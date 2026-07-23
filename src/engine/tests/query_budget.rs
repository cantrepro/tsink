use super::*;
use crate::{
    Aggregation, BytesAggregation, QueryBudgetError, QueryBudgetLimits, QueryCancellationToken,
    QueryLimitReason, QueryOptions, QueryWorkLimits, Result,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn storage_with_query_limits(limits: QueryBudgetLimits) -> ChunkStorage {
    let options = ChunkStorageOptions {
        retention_enforced: false,
        background_threads_enabled: false,
        background_fail_fast: false,
        ..ChunkStorageOptions::default()
    };
    ChunkStorage::new_with_data_path_and_options_and_disk_budget_and_query_budget(
        64, None, None, None, 1, options, None, limits,
    )
    .unwrap()
}

fn insert_two_points(storage: &ChunkStorage) {
    storage
        .insert_rows(&[
            Row::new("cpu", DataPoint::new(1, 1.0)),
            Row::new("cpu", DataPoint::new(2, 2.0)),
        ])
        .unwrap();
}

fn insert_four_points(storage: &ChunkStorage) {
    storage
        .insert_rows(&[
            Row::new("cpu", DataPoint::new(1, 1.0)),
            Row::new("cpu", DataPoint::new(2, 2.0)),
            Row::new("cpu", DataPoint::new(11, 3.0)),
            Row::new("cpu", DataPoint::new(12, 4.0)),
        ])
        .unwrap();
}

struct CountingCustomAggregation {
    calls: Arc<AtomicUsize>,
}

impl BytesAggregation for CountingCustomAggregation {
    fn aggregate_series(&self, points: &[DataPoint]) -> Result<Option<DataPoint>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(points.first().cloned())
    }

    fn aggregate_bucket(
        &self,
        points: &[DataPoint],
        bucket_start: i64,
    ) -> Result<Option<DataPoint>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(points
            .first()
            .map(|point| DataPoint::new(bucket_start, point.value.clone())))
    }
}

fn assert_query_limit(error: TsinkError, reason: QueryLimitReason) {
    assert!(matches!(
        error,
        TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
            if exceeded.reason == reason
    ));
}

#[test]
fn direct_select_accepts_exact_sample_bound_and_releases_slot() {
    let storage = storage_with_query_limits(QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        per_query: QueryWorkLimits {
            max_samples_scanned: Some(2),
            max_samples_returned: Some(2),
            max_intermediate_vector_size: Some(2),
            max_memory_bytes: Some(4_096),
            ..QueryWorkLimits::default()
        },
        max_shared_memory_bytes: Some(4_096),
    });
    insert_two_points(&storage);

    let points = storage.select("cpu", &[], 0, 3).unwrap();
    assert_eq!(points.len(), 2);

    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
    assert_eq!(snapshot.peak_active_queries, 1);
}

#[test]
fn tiny_return_limit_rejects_before_read_materialization_hook() {
    let storage = storage_with_query_limits(QueryBudgetLimits {
        per_query: QueryWorkLimits {
            max_samples_returned: Some(1),
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    });
    insert_two_points(&storage);
    let materializations = Arc::new(AtomicUsize::new(0));
    storage.set_query_merge_in_memory_source_snapshot_hook({
        let materializations = Arc::clone(&materializations);
        move || {
            materializations.fetch_add(1, Ordering::SeqCst);
        }
    });

    assert_query_limit(
        storage.select("cpu", &[], 0, 3).unwrap_err(),
        QueryLimitReason::SamplesReturned,
    );
    assert_eq!(materializations.load(Ordering::SeqCst), 0);
    assert_eq!(storage.query_budget_snapshot().active_queries, 0);
}

#[test]
fn nested_reads_share_one_slot_and_concurrent_admission_is_rejected() {
    let storage = storage_with_query_limits(QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        ..QueryBudgetLimits::default()
    });
    insert_two_points(&storage);
    let execution = storage
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();

    assert_query_limit(
        storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap_err(),
        QueryLimitReason::ConcurrentQueries,
    );
    let result = storage
        .select_many_with_execution(
            &[MetricSeries {
                name: "cpu".to_string(),
                labels: Vec::new(),
            }],
            0,
            3,
            &execution,
        )
        .unwrap();
    assert_eq!(result[0].points.len(), 2);
    assert_eq!(storage.query_budget_snapshot().queries_started_total, 1);

    drop(execution);
    assert_eq!(storage.query_budget_snapshot().active_queries, 0);
}

#[test]
fn cancelled_execution_stops_inside_scan_and_releases_after_drop() {
    let storage = storage_with_query_limits(QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        ..QueryBudgetLimits::default()
    });
    insert_two_points(&storage);
    let token = QueryCancellationToken::new();
    let execution = storage
        .begin_query_execution(QueryWorkLimits::default(), token.clone())
        .unwrap()
        .unwrap();
    storage.set_query_merge_in_memory_source_snapshot_hook(move || token.cancel());

    assert!(matches!(
        storage.select_with_execution("cpu", &[], 0, 3, &execution),
        Err(TsinkError::QueryBudget(QueryBudgetError::Cancelled))
    ));
    assert_eq!(storage.query_budget_snapshot().active_queries, 1);
    drop(execution);
    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.cancellations_total, 1);
}

#[test]
fn expired_deadline_is_rejected_before_admission() {
    let storage = storage_with_query_limits(QueryBudgetLimits::default());
    let token = QueryCancellationToken::new().with_timeout(Duration::ZERO);
    assert!(matches!(
        storage.begin_query_execution(QueryWorkLimits::default(), token),
        Err(TsinkError::QueryBudget(QueryBudgetError::DeadlineExceeded))
    ));
    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.queries_started_total, 0);
    assert_eq!(snapshot.deadline_exceeded_total, 1);
}

#[test]
fn failed_direct_query_releases_slot_and_observability_reports_limits() {
    let limits = QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        per_query: QueryWorkLimits {
            max_samples_scanned: Some(1),
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    };
    let storage = storage_with_query_limits(limits);
    insert_two_points(&storage);

    assert_query_limit(
        storage.select("cpu", &[], 0, 3).unwrap_err(),
        QueryLimitReason::SamplesScanned,
    );
    let snapshot = storage.observability_snapshot().query_budget;
    assert_eq!(snapshot.limits, limits);
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
}

#[test]
fn pattern_expansion_limit_rejects_metadata_materialization() {
    let storage = storage_with_query_limits(QueryBudgetLimits {
        per_query: QueryWorkLimits {
            max_pattern_expansion: Some(1),
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    });
    storage
        .insert_rows(&[
            Row::new("cpu", DataPoint::new(1, 1.0)),
            Row::new("memory", DataPoint::new(1, 1.0)),
        ])
        .unwrap();
    let planner_materializations = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_all_series_postings_hook({
        let planner_materializations = Arc::clone(&planner_materializations);
        move || {
            planner_materializations.fetch_add(1, Ordering::SeqCst);
        }
    });

    assert_query_limit(
        storage
            .select_series(&SeriesSelection::default())
            .unwrap_err(),
        QueryLimitReason::PatternExpansion,
    );
    assert_eq!(planner_materializations.load(Ordering::SeqCst), 0);
    assert_eq!(storage.query_budget_snapshot().active_queries, 0);
}

#[test]
fn custom_downsample_preflights_exact_bucket_count_before_calling_user_code() {
    let success_calls = Arc::new(AtomicUsize::new(0));
    let success = storage_with_query_limits(QueryBudgetLimits {
        per_query: QueryWorkLimits {
            max_samples_returned: Some(2),
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    });
    insert_four_points(&success);
    let mut options = QueryOptions::new(0, 20).with_downsample(10, Aggregation::Last);
    options.custom_aggregation = Some(Arc::new(CountingCustomAggregation {
        calls: Arc::clone(&success_calls),
    }));
    let result = success.select_with_options("cpu", options).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(success_calls.load(Ordering::SeqCst), 2);
    assert_eq!(success.query_budget_snapshot().active_queries, 0);

    let rejected_calls = Arc::new(AtomicUsize::new(0));
    let rejected = storage_with_query_limits(QueryBudgetLimits {
        per_query: QueryWorkLimits {
            max_samples_returned: Some(1),
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    });
    insert_four_points(&rejected);
    let mut options = QueryOptions::new(0, 20).with_downsample(10, Aggregation::Last);
    options.custom_aggregation = Some(Arc::new(CountingCustomAggregation {
        calls: Arc::clone(&rejected_calls),
    }));
    assert_query_limit(
        rejected.select_with_options("cpu", options).unwrap_err(),
        QueryLimitReason::SamplesReturned,
    );
    assert_eq!(rejected_calls.load(Ordering::SeqCst), 0);
    assert_eq!(rejected.query_budget_snapshot().active_queries, 0);
}

fn observed_returned_bytes(
    storage: &ChunkStorage,
    query: impl FnOnce(&ChunkStorage, &crate::QueryExecution) -> Result<()>,
) -> u64 {
    let execution = storage
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    query(storage, &execution).unwrap();
    let bytes = execution.snapshot().returned_bytes;
    drop(execution);
    bytes
}

fn insert_metadata_limit_series(storage: &ChunkStorage) {
    storage
        .insert_rows(&[
            Row::with_labels("cpu", vec![Label::new("host", "a")], DataPoint::new(1, 1.0)),
            Row::with_labels(
                "memory",
                vec![Label::new("host", "b")],
                DataPoint::new(1, 2.0),
            ),
            Row::with_labels(
                "disk",
                vec![Label::new("host", "c")],
                DataPoint::new(1, 3.0),
            ),
        ])
        .unwrap();
}

const COLD_VISIBILITY_RANGE_COUNT: usize = 12;

fn cold_visibility_metadata_storage(limits: QueryBudgetLimits) -> ChunkStorage {
    let storage = storage_with_query_limits(limits);
    let rows = (0..COLD_VISIBILITY_RANGE_COUNT)
        .map(|index| {
            Row::new(
                "cold_visibility",
                DataPoint::new(i64::try_from(index).unwrap() * 2, 1.0),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();
    let series_ids = storage.materialized_series_snapshot();
    assert_eq!(series_ids.len(), 1);
    storage.clear_series_visible_timestamp_cache(series_ids.iter().copied());
    assert_eq!(
        storage
            .missing_visibility_summary_series_ids(series_ids.iter().copied())
            .len(),
        1
    );
    storage
}

const DEAD_METADATA_SERIES_COUNT: usize = 16_384;

fn dead_metadata_storage(limits: QueryBudgetLimits) -> ChunkStorage {
    let options = ChunkStorageOptions {
        timestamp_precision: TimestampPrecision::Seconds,
        retention_window: 10,
        future_skew_window: 0,
        retention_enforced: true,
        background_threads_enabled: false,
        background_fail_fast: false,
        current_time_override: Some(1),
        ..ChunkStorageOptions::default()
    };
    let storage = ChunkStorage::new_with_data_path_and_options_and_disk_budget_and_query_budget(
        64, None, None, None, 1, options, None, limits,
    )
    .unwrap();
    storage
        .insert_rows(&[Row::new("retention_anchor", DataPoint::new(1, 1.0))])
        .unwrap();

    let synthetic_ids = (0..DEAD_METADATA_SERIES_COUNT - 1)
        .map(|index| 1_000_000u64 + u64::try_from(index).unwrap())
        .collect::<Vec<_>>();
    storage.mark_materialized_series_ids(synthetic_ids);
    let series_ids = storage.materialized_series_snapshot();
    assert_eq!(series_ids.len(), DEAD_METADATA_SERIES_COUNT);
    storage
        .refresh_series_visible_timestamp_cache(series_ids.iter().copied())
        .unwrap();
    storage.set_current_time_override(100);
    storage
}

#[test]
fn list_metrics_uses_one_execution_and_preflights_exact_result_and_memory_limits() {
    let calibration = storage_with_query_limits(QueryBudgetLimits::default());
    insert_metadata_limit_series(&calibration);
    let execution = calibration
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    let listed = calibration.list_metrics_with_execution(&execution).unwrap();
    assert_eq!(listed.len(), 3);
    let execution_snapshot = execution.snapshot();
    assert_eq!(execution_snapshot.series_matched, 3);
    let exact_returned_bytes = execution_snapshot.returned_bytes;
    drop(execution);
    let exact_memory_bytes = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_returned_bytes > 0);
    assert!(exact_memory_bytes > 0);

    for (series_limit, should_succeed) in [(3, true), (2, false)] {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_series_matched: Some(series_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        insert_metadata_limit_series(&storage);
        let result = storage.list_metrics();
        if should_succeed {
            assert_eq!(result.unwrap().len(), 3);
        } else {
            assert_query_limit(result.unwrap_err(), QueryLimitReason::SeriesMatched);
        }
        assert_eq!(storage.query_budget_snapshot().active_queries, 0);
    }

    for (returned_limit, should_succeed) in [
        (exact_returned_bytes, true),
        (exact_returned_bytes - 1, false),
    ] {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_returned_bytes: Some(returned_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        insert_metadata_limit_series(&storage);
        let result = storage.list_metrics();
        if should_succeed {
            assert_eq!(result.unwrap().len(), 3);
        } else {
            assert_query_limit(result.unwrap_err(), QueryLimitReason::ReturnedBytes);
        }
        assert_eq!(storage.query_budget_snapshot().active_queries, 0);
    }

    for (memory_limit, should_succeed) in
        [(exact_memory_bytes, true), (exact_memory_bytes - 1, false)]
    {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            max_shared_memory_bytes: Some(memory_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(memory_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        insert_metadata_limit_series(&storage);
        let result = storage.list_metrics();
        if should_succeed {
            assert_eq!(result.unwrap().len(), 3);
        } else {
            assert_query_limit(result.unwrap_err(), QueryLimitReason::PerQueryMemoryBytes);
        }
        assert_eq!(storage.query_budget_snapshot().active_queries, 0);
    }
}

#[test]
fn list_metrics_cold_visibility_repair_preflights_exact_memory_and_range_vector_limits() {
    let warm = storage_with_query_limits(QueryBudgetLimits::default());
    let warm_rows = (0..COLD_VISIBILITY_RANGE_COUNT)
        .map(|index| {
            Row::new(
                "cold_visibility",
                DataPoint::new(i64::try_from(index).unwrap() * 2, 1.0),
            )
        })
        .collect::<Vec<_>>();
    warm.insert_rows(&warm_rows).unwrap();
    assert_eq!(warm.list_metrics().unwrap().len(), 1);
    let warm_peak = warm
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;

    let calibration = cold_visibility_metadata_storage(QueryBudgetLimits::default());
    let execution = calibration
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    assert_eq!(
        calibration
            .list_metrics_with_execution(&execution)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        execution.snapshot().intermediate_vector_size,
        COLD_VISIBILITY_RANGE_COUNT as u64
    );
    drop(execution);
    let exact_memory = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_memory > warm_peak);
    assert_eq!(
        calibration
            .query_budget_snapshot()
            .shared_reserved_memory_bytes,
        0
    );

    for (memory_limit, should_succeed) in [(exact_memory, true), (exact_memory - 1, false)] {
        let storage = cold_visibility_metadata_storage(QueryBudgetLimits {
            max_shared_memory_bytes: Some(memory_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(memory_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let series_ids = storage.materialized_series_snapshot();
        let result = storage.list_metrics();
        if should_succeed {
            assert_eq!(result.unwrap().len(), 1);
            assert!(storage
                .missing_visibility_summary_series_ids(series_ids.iter().copied())
                .is_empty());
        } else {
            assert_query_limit(result.unwrap_err(), QueryLimitReason::PerQueryMemoryBytes);
            assert_eq!(
                storage
                    .missing_visibility_summary_series_ids(series_ids.iter().copied())
                    .len(),
                1
            );
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    for (vector_limit, should_succeed) in [
        (COLD_VISIBILITY_RANGE_COUNT as u64, true),
        (COLD_VISIBILITY_RANGE_COUNT as u64 - 1, false),
    ] {
        let storage = cold_visibility_metadata_storage(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_intermediate_vector_size: Some(vector_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let result = storage.list_metrics();
        if should_succeed {
            assert_eq!(result.unwrap().len(), 1);
        } else {
            assert_query_limit(
                result.unwrap_err(),
                QueryLimitReason::IntermediateVectorSize,
            );
        }
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            0
        );
    }
}

#[test]
fn list_metrics_cold_visibility_repair_readmits_post_preflight_ingest_growth() {
    let calibration = cold_visibility_metadata_storage(QueryBudgetLimits::default());
    assert_eq!(calibration.list_metrics().unwrap().len(), 1);
    let preflight_exact_memory = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;

    let storage = Arc::new(cold_visibility_metadata_storage(QueryBudgetLimits {
        max_shared_memory_bytes: Some(preflight_exact_memory),
        per_query: QueryWorkLimits {
            max_memory_bytes: Some(preflight_exact_memory),
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    }));
    let weak_storage = Arc::downgrade(&storage);
    storage.set_metadata_visibility_refresh_post_preflight_hook(move || {
        let storage = weak_storage
            .upgrade()
            .expect("racing metadata storage must remain alive");
        let rows = (0..512)
            .map(|index| {
                Row::new(
                    "cold_visibility",
                    DataPoint::new(10_000 + i64::from(index), 2.0),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
    });

    assert_query_limit(
        storage.list_metrics().unwrap_err(),
        QueryLimitReason::PerQueryMemoryBytes,
    );
    storage.clear_metadata_visibility_refresh_post_preflight_hook();
    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(snapshot.accounting_invariant_violations_total, 0);
}

#[test]
fn list_metrics_dead_pruning_accounts_companion_ids_at_exact_boundaries() {
    let calibration = dead_metadata_storage(QueryBudgetLimits::default());
    let execution = calibration
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    assert!(calibration
        .list_metrics_with_execution(&execution)
        .unwrap()
        .is_empty());
    assert_eq!(
        execution.snapshot().intermediate_vector_size,
        DEAD_METADATA_SERIES_COUNT as u64
    );
    drop(execution);
    let calibration_snapshot = calibration.query_budget_snapshot();
    let exact_memory = calibration_snapshot.peak_shared_reserved_memory_bytes;
    assert_eq!(
        calibration_snapshot.intermediate_vector_size_rejections_total,
        0
    );
    assert!(calibration.materialized_series_snapshot().is_empty());

    for (memory_limit, should_succeed) in [(exact_memory, true), (exact_memory - 1, false)] {
        let storage = dead_metadata_storage(QueryBudgetLimits {
            max_shared_memory_bytes: Some(memory_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(memory_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let result = storage.list_metrics();
        if should_succeed {
            assert!(result.unwrap().is_empty());
            assert!(storage.materialized_series_snapshot().is_empty());
        } else {
            assert_query_limit(result.unwrap_err(), QueryLimitReason::PerQueryMemoryBytes);
            assert_eq!(
                storage.materialized_series_snapshot().len(),
                DEAD_METADATA_SERIES_COUNT
            );
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    for (vector_limit, should_succeed) in [
        (DEAD_METADATA_SERIES_COUNT as u64, true),
        (DEAD_METADATA_SERIES_COUNT as u64 - 1, false),
    ] {
        let storage = dead_metadata_storage(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_intermediate_vector_size: Some(vector_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let result = storage.list_metrics();
        if should_succeed {
            assert!(result.unwrap().is_empty());
            assert!(storage.materialized_series_snapshot().is_empty());
        } else {
            assert_query_limit(
                result.unwrap_err(),
                QueryLimitReason::IntermediateVectorSize,
            );
            assert_eq!(
                storage.materialized_series_snapshot().len(),
                DEAD_METADATA_SERIES_COUNT
            );
        }
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            0
        );
    }
}

#[test]
fn select_many_returned_bytes_include_series_identity_at_exact_boundary() {
    let series = vec![MetricSeries {
        name: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    }];
    let calibration = storage_with_query_limits(QueryBudgetLimits::default());
    calibration
        .insert_rows(&[Row::with_labels(
            "cpu",
            series[0].labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    let exact = observed_returned_bytes(&calibration, |storage, execution| {
        storage
            .select_many_with_execution(&series, 0, 2, execution)
            .map(|_| ())
    });
    assert!(exact > std::mem::size_of::<DataPoint>() as u64);

    for (limit, should_succeed) in [(exact, true), (exact - 1, false)] {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_returned_bytes: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        storage
            .insert_rows(&[Row::with_labels(
                "cpu",
                series[0].labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();
        let result = storage.select_many(&series, 0, 2);
        if should_succeed {
            assert_eq!(result.unwrap().len(), 1);
        } else {
            assert_query_limit(result.unwrap_err(), QueryLimitReason::ReturnedBytes);
        }
        assert_eq!(storage.query_budget_snapshot().active_queries, 0);
    }
}

#[test]
fn select_all_returned_bytes_include_metric_and_labels_at_exact_boundary() {
    let labels = vec![Label::new("host", "a")];
    let calibration = storage_with_query_limits(QueryBudgetLimits::default());
    calibration
        .insert_rows(&[Row::with_labels(
            "cpu",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    let exact = observed_returned_bytes(&calibration, |storage, execution| {
        storage
            .select_all_with_execution("cpu", 0, 2, execution)
            .map(|_| ())
    });
    assert!(exact > std::mem::size_of::<DataPoint>() as u64);

    for (limit, should_succeed) in [(exact, true), (exact - 1, false)] {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_returned_bytes: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        storage
            .insert_rows(&[Row::with_labels(
                "cpu",
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();
        let result = storage.select_all("cpu", 0, 2);
        if should_succeed {
            assert_eq!(result.unwrap().len(), 1);
        } else {
            assert_query_limit(result.unwrap_err(), QueryLimitReason::ReturnedBytes);
        }
        assert_eq!(storage.query_budget_snapshot().active_queries, 0);
    }
}

#[test]
fn sync_and_async_builders_validate_query_limits_before_startup() {
    let invalid = QueryBudgetLimits {
        max_concurrent_queries: Some(0),
        ..QueryBudgetLimits::default()
    };
    assert!(matches!(
        StorageBuilder::new()
            .with_query_budget_limits(invalid)
            .build(),
        Err(TsinkError::QueryBudget(QueryBudgetError::InvalidLimits(_)))
    ));
    assert!(matches!(
        crate::AsyncStorageBuilder::new()
            .with_query_budget_limits(invalid)
            .build(),
        Err(TsinkError::QueryBudget(QueryBudgetError::InvalidLimits(_)))
    ));
}
