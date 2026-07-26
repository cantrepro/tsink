use super::*;
use crate::engine::series::SeriesId;
use crate::engine::tombstone::{self, TombstoneMap};
use crate::{
    Aggregation, BytesAggregation, QueryBudgetError, QueryBudgetLimits, QueryCancellationToken,
    QueryExecutionAccounting, QueryLimitReason, QueryOptions, QueryWorkLimits, Result,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

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

fn storage_with_query_limits_and_metadata_shards(limits: QueryBudgetLimits) -> ChunkStorage {
    let options = ChunkStorageOptions {
        retention_enforced: false,
        background_threads_enabled: false,
        background_fail_fast: false,
        metadata_shard_count: Some(1),
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

fn install_mixed_query_tombstones(storage: &ChunkStorage, series_id: SeriesId) {
    storage.visibility.tombstones.write().insert(
        series_id,
        vec![tombstone::TombstoneRange { start: 0, end: 10 }],
    );
    let shard_index = tombstone::ImmutableTombstoneSnapshot::shard_index(series_id);
    let mut shards = (0..tombstone::LIVE_TOMBSTONE_SHARD_COUNT)
        .map(|_| {
            tombstone::ImmutableTombstoneShard::from_map_with_memory_usage(TombstoneMap::new(), 0)
        })
        .collect::<Vec<_>>();
    let mut remote = TombstoneMap::new();
    remote.insert(
        series_id,
        vec![tombstone::TombstoneRange { start: 5, end: 20 }],
    );
    shards[shard_index] = tombstone::ImmutableTombstoneShard::from_map_with_memory_usage(remote, 0);
    let _visibility_guard = storage.visibility_write_fence();
    storage
        .tombstone_publication_context()
        .publish_remote_tombstones_locked(
            storage,
            Arc::new(tombstone::ImmutableTombstoneSnapshot::from_shards(shards)),
        )
        .unwrap();
}

#[test]
fn mixed_tombstone_union_has_exact_memory_and_intermediate_vector_boundaries() {
    let series_id = 77;
    let range_count = 2usize;
    let exact_memory = super::super::query_exec::modeled_vec_capacity_bytes::<
        tombstone::TombstoneRange,
    >(range_count);

    for (memory_limit, should_succeed) in [(exact_memory, true), (exact_memory - 1, false)] {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            max_shared_memory_bytes: Some(memory_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(memory_limit),
                max_intermediate_vector_size: Some(range_count as u64),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        install_mixed_query_tombstones(&storage, series_id);
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let result = storage
            .tombstone_read_context()
            .with_series_tombstone_ranges_for_query(series_id, Some(&execution), |ranges| {
                assert_eq!(
                    ranges,
                    Some([tombstone::TombstoneRange { start: 0, end: 20 }].as_slice()),
                );
                Ok(())
            });
        if should_succeed {
            result.expect("the exact mixed-overlay Vec model must pass");
        } else {
            assert_query_limit(
                result.expect_err("one byte below the mixed-overlay Vec model must fail"),
                QueryLimitReason::PerQueryMemoryBytes,
            );
        }
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            0,
            "the short-lived tombstone union reservation must always release",
        );
        drop(execution);
        storage.close().unwrap();
    }

    let storage = storage_with_query_limits(QueryBudgetLimits {
        max_shared_memory_bytes: Some(exact_memory),
        per_query: QueryWorkLimits {
            max_memory_bytes: Some(exact_memory),
            max_intermediate_vector_size: Some((range_count - 1) as u64),
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    });
    install_mixed_query_tombstones(&storage, series_id);
    let execution = storage
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    let error = storage
        .tombstone_read_context()
        .with_series_tombstone_ranges_for_query(series_id, Some(&execution), |_| Ok(()))
        .unwrap_err();
    assert_query_limit(error, QueryLimitReason::IntermediateVectorSize);
    assert_eq!(
        storage.query_budget_snapshot().shared_reserved_memory_bytes,
        0,
    );
    drop(execution);
    storage.close().unwrap();
}

fn shard_window_sort_fixture() -> Vec<DataPoint> {
    let histogram = crate::NativeHistogram {
        count: Some(crate::HistogramCount::Int(9)),
        sum: 42.5,
        schema: 2,
        zero_threshold: 0.001,
        zero_count: Some(crate::HistogramCount::Float(1.5)),
        negative_spans: vec![crate::HistogramBucketSpan {
            offset: -3,
            length: 2,
        }],
        negative_deltas: vec![-2, 4],
        negative_counts: vec![1.0, 3.0],
        positive_spans: vec![crate::HistogramBucketSpan {
            offset: 1,
            length: 3,
        }],
        positive_deltas: vec![1, 2, 3],
        positive_counts: vec![2.0, 4.0, 8.0],
        reset_hint: crate::HistogramResetHint::Gauge,
        custom_values: (0..2_048).map(f64::from).collect(),
    };
    vec![
        DataPoint::new(9, Value::String("z".repeat(32 * 1024))),
        DataPoint::new(9, Value::Histogram(Box::new(histogram))),
        DataPoint::new(8, Value::String("a".repeat(16 * 1024))),
    ]
}

#[test]
fn shard_window_value_json_sort_scratch_has_exact_memory_boundary() {
    let calibration = storage_with_query_limits_and_metadata_shards(QueryBudgetLimits::default());
    let calibration_execution = calibration
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    let mut calibration_points = shard_window_sort_fixture();
    super::super::query_exec::sort_data_points_for_shard_window_with_execution(
        &mut calibration_points,
        &calibration_execution,
    )
    .unwrap();
    drop(calibration_execution);
    let exact_memory = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_memory > 32 * 1024);

    for (memory_limit, should_succeed) in [(exact_memory, true), (exact_memory - 1, false)] {
        let storage = storage_with_query_limits_and_metadata_shards(QueryBudgetLimits {
            max_shared_memory_bytes: Some(memory_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(memory_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let mut points = shard_window_sort_fixture();
        let result = super::super::query_exec::sort_data_points_for_shard_window_with_execution(
            &mut points,
            &execution,
        );
        if should_succeed {
            result.expect("the exact JSON-key scratch boundary should pass");
            assert_eq!(points[0].timestamp, 8);
        } else {
            assert_query_limit(
                result.expect_err("one byte below JSON-key scratch must fail"),
                QueryLimitReason::PerQueryMemoryBytes,
            );
        }
        drop(execution);
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }
}

fn insert_long_identity_series(storage: &ChunkStorage, count: usize) {
    let rows = (0..count)
        .map(|index| {
            Row::with_labels(
                format!("shard_scan_metric_{index}_{}", "m".repeat(512)),
                vec![
                    Label::new("host", format!("{index}-{}", "h".repeat(2_048))),
                    Label::new("zone", format!("{index}-{}", "z".repeat(2_048))),
                ],
                DataPoint::new(10, index as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();
}

#[test]
fn shard_window_series_entry_materialization_has_exact_memory_boundary() {
    const SERIES_COUNT: usize = 32;

    let calibration = storage_with_query_limits_and_metadata_shards(QueryBudgetLimits::default());
    insert_long_identity_series(&calibration, SERIES_COUNT);
    let calibration_execution = calibration
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    let (entries, candidate_reservation, entries_reservation) = calibration
        .shard_scan_series_entries(0, 1, "test_shard_scan_entries", &calibration_execution)
        .unwrap();
    assert_eq!(entries.len(), SERIES_COUNT);
    drop(entries);
    drop(entries_reservation);
    drop(candidate_reservation);
    drop(calibration_execution);
    let exact_memory = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_memory > 128 * 1024);

    for (memory_limit, should_succeed) in [(exact_memory, true), (exact_memory - 1, false)] {
        let storage = storage_with_query_limits_and_metadata_shards(QueryBudgetLimits {
            max_shared_memory_bytes: Some(memory_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(memory_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        insert_long_identity_series(&storage, SERIES_COUNT);
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let result = storage.shard_scan_series_entries(0, 1, "test_shard_scan_entries", &execution);
        if should_succeed {
            let (entries, candidate_reservation, entries_reservation) =
                result.expect("the exact series-entry memory boundary should pass");
            assert_eq!(entries.len(), SERIES_COUNT);
            drop(entries);
            drop(entries_reservation);
            drop(candidate_reservation);
        } else {
            assert_query_limit(
                result.expect_err("one byte below series-entry materialization must fail"),
                QueryLimitReason::PerQueryMemoryBytes,
            );
        }
        drop(execution);
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }
}

#[test]
fn shard_window_point_hashes_are_stable_for_every_value_variant() {
    let histogram = crate::NativeHistogram {
        count: Some(crate::HistogramCount::Int(3)),
        sum: 6.5,
        schema: -1,
        zero_threshold: 0.01,
        zero_count: Some(crate::HistogramCount::Float(0.5)),
        negative_spans: vec![crate::HistogramBucketSpan {
            offset: -2,
            length: 1,
        }],
        negative_deltas: vec![-1],
        negative_counts: vec![1.25],
        positive_spans: vec![crate::HistogramBucketSpan {
            offset: 2,
            length: 2,
        }],
        positive_deltas: vec![1, 2],
        positive_counts: vec![2.5, 3.5],
        reset_hint: crate::HistogramResetHint::No,
        custom_values: vec![0.25, 0.75],
    };
    let points = [
        DataPoint::new(123, Value::F64(1.5)),
        DataPoint::new(123, Value::I64(-7)),
        DataPoint::new(123, Value::U64(9)),
        DataPoint::new(123, Value::Bool(true)),
        DataPoint::new(123, Value::Bytes(vec![0, 1, 255])),
        DataPoint::new(123, Value::String("snowman-☃".to_string())),
        DataPoint::new(123, Value::Histogram(Box::new(histogram))),
    ];
    let hashes = points
        .iter()
        .map(crate::storage::shard_window_hash_data_point)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        hashes,
        vec![
            683_224_143_682_936_456,
            10_120_950_658_361_785_947,
            12_532_566_515_122_182_258,
            14_305_959_779_853_542_798,
            5_120_379_721_296_883_712,
            9_948_920_761_892_448_446,
            1_010_851_430_309_354_837,
        ]
    );
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

fn bounded_regex_selection() -> SeriesSelection {
    SeriesSelection::new().with_matcher(SeriesMatcher::regex_match("host", "web-[0-9]+"))
}

#[test]
fn matcher_preparation_memory_has_an_exact_fail_before_compile_boundary() {
    let selection = bounded_regex_selection();
    let calibration = storage_with_query_limits(QueryBudgetLimits::default());
    let calibration_compiles = Arc::new(AtomicUsize::new(0));
    calibration.set_metadata_matcher_regex_compile_hook({
        let calibration_compiles = Arc::clone(&calibration_compiles);
        move || {
            calibration_compiles.fetch_add(1, Ordering::SeqCst);
        }
    });
    assert!(calibration.select_series(&selection).unwrap().is_empty());
    assert_eq!(calibration_compiles.load(Ordering::SeqCst), 1);
    calibration.clear_metadata_matcher_regex_compile_hook();
    let exact_memory = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(
        exact_memory
            > crate::query_matcher::modeled_series_matcher_preparation_bytes(&selection.matchers)
    );

    for (memory_limit, should_succeed) in [(exact_memory, true), (exact_memory - 1, false)] {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            max_shared_memory_bytes: Some(memory_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(memory_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let compiles = Arc::new(AtomicUsize::new(0));
        storage.set_metadata_matcher_regex_compile_hook({
            let compiles = Arc::clone(&compiles);
            move || {
                compiles.fetch_add(1, Ordering::SeqCst);
            }
        });

        let result = storage.select_series(&selection);
        if should_succeed {
            assert!(result.unwrap().is_empty());
            assert_eq!(compiles.load(Ordering::SeqCst), 1);
        } else {
            assert_query_limit(
                result.expect_err("one byte below the preparation peak must fail"),
                QueryLimitReason::PerQueryMemoryBytes,
            );
            assert_eq!(
                compiles.load(Ordering::SeqCst),
                0,
                "memory rejection must happen before regex compilation"
            );
        }
        storage.clear_metadata_matcher_regex_compile_hook();
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }
}

#[test]
fn matcher_preparation_reservation_is_held_through_regex_use() {
    let storage = storage_with_query_limits(QueryBudgetLimits::default());
    storage
        .insert_rows(&[Row::with_labels(
            "cpu",
            vec![Label::new("host", "web-1")],
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    let selection = bounded_regex_selection();
    let preparation_bytes =
        crate::query_matcher::modeled_series_matcher_preparation_bytes(&selection.matchers);
    let execution = storage
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    let observed_use = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_direct_candidate_scan_hook({
        let observed_use = Arc::clone(&observed_use);
        let execution = execution.clone();
        move || {
            assert!(execution.snapshot().memory_reserved_bytes >= preparation_bytes);
            observed_use.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = storage
        .select_series_with_execution(&selection, &execution)
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(observed_use.load(Ordering::SeqCst), 1);
    storage.clear_metadata_direct_candidate_scan_hook();
    assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
    assert_eq!(
        storage.query_budget_snapshot().shared_reserved_memory_bytes,
        0
    );
    drop(execution);
    assert_eq!(storage.query_budget_snapshot().active_queries, 0);
}

#[test]
fn matcher_preparation_cancellation_wins_before_regex_compile_and_releases_memory() {
    let storage = storage_with_query_limits(QueryBudgetLimits::default());
    let token = QueryCancellationToken::new();
    let execution = storage
        .begin_query_execution(QueryWorkLimits::default(), token.clone())
        .unwrap()
        .unwrap();
    let hook_calls = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_matcher_regex_compile_hook({
        let hook_calls = Arc::clone(&hook_calls);
        move || {
            hook_calls.fetch_add(1, Ordering::SeqCst);
            token.cancel();
        }
    });
    let invalid = SeriesSelection::new().with_matcher(SeriesMatcher::regex_match("host", "("));

    assert!(matches!(
        storage.select_series_with_execution(&invalid, &execution),
        Err(TsinkError::QueryBudget(QueryBudgetError::Cancelled))
    ));
    assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
    storage.clear_metadata_matcher_regex_compile_hook();
    assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
    assert_eq!(
        storage.query_budget_snapshot().shared_reserved_memory_bytes,
        0
    );
    drop(execution);
    assert_eq!(storage.query_budget_snapshot().active_queries, 0);
}

#[test]
fn matcher_preparation_deadline_wins_before_regex_compile_and_releases_memory() {
    let storage = storage_with_query_limits(QueryBudgetLimits::default());
    let deadline = Instant::now() + Duration::from_millis(50);
    let token = QueryCancellationToken::new().with_deadline(deadline);
    let execution = storage
        .begin_query_execution(QueryWorkLimits::default(), token)
        .unwrap()
        .unwrap();
    let hook_calls = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_matcher_regex_compile_hook({
        let hook_calls = Arc::clone(&hook_calls);
        move || {
            hook_calls.fetch_add(1, Ordering::SeqCst);
            while Instant::now() < deadline {
                std::hint::spin_loop();
            }
        }
    });
    let invalid = SeriesSelection::new().with_matcher(SeriesMatcher::regex_match("host", "("));

    assert!(matches!(
        storage.select_series_with_execution(&invalid, &execution),
        Err(TsinkError::QueryBudget(QueryBudgetError::DeadlineExceeded))
    ));
    assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
    storage.clear_metadata_matcher_regex_compile_hook();
    assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
    assert_eq!(
        storage.query_budget_snapshot().shared_reserved_memory_bytes,
        0
    );
    drop(execution);
    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.deadline_exceeded_total, 1);
}

#[test]
fn matcher_shape_rejection_precedes_compile_and_leaves_no_query_residue() {
    let storage = storage_with_query_limits(QueryBudgetLimits::default());
    let compiles = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_matcher_regex_compile_hook({
        let compiles = Arc::clone(&compiles);
        move || {
            compiles.fetch_add(1, Ordering::SeqCst);
        }
    });
    let mut matchers = vec![SeriesMatcher::regex_match("host", "web-.*")];
    matchers.extend(
        (1..=crate::MAX_SERIES_SELECTION_MATCHERS)
            .map(|index| SeriesMatcher::equal(format!("label_{index}"), "")),
    );

    let selection = SeriesSelection::new().with_matchers(matchers);
    let error = storage
        .select_series(&selection)
        .expect_err("one-over matcher count must fail");
    assert!(matches!(error, TsinkError::InvalidConfiguration(_)));
    let scoped_error = storage
        .select_series_in_shards(&selection, &crate::MetadataShardScope::new(1, vec![0]))
        .expect_err("one-over matcher count must fail before shard materialization");
    assert!(matches!(scoped_error, TsinkError::InvalidConfiguration(_)));
    assert_eq!(compiles.load(Ordering::SeqCst), 0);
    storage.clear_metadata_matcher_regex_compile_hook();
    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
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

#[test]
fn select_series_execution_result_retains_memory_until_result_drop() {
    let storage = storage_with_query_limits(QueryBudgetLimits {
        max_shared_memory_bytes: Some(1_000_000),
        per_query: QueryWorkLimits {
            max_memory_bytes: Some(1_000_000),
            ..QueryWorkLimits::default()
        },
        ..QueryBudgetLimits::default()
    });
    insert_metadata_limit_series(&storage);
    let execution = storage
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();

    assert_eq!(
        storage.select_series_execution_accounting(),
        QueryExecutionAccounting::Complete
    );
    let selected = storage
        .select_series_with_execution_result(&SeriesSelection::new(), &execution)
        .unwrap();
    assert_eq!(selected.series.len(), 3);
    let reserved = selected.reserved_memory_bytes();
    assert!(reserved > 0);
    assert_eq!(execution.snapshot().memory_reserved_bytes, reserved);
    assert_eq!(
        storage.query_budget_snapshot().shared_reserved_memory_bytes,
        reserved
    );

    drop(selected);
    assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
    assert_eq!(
        storage.query_budget_snapshot().shared_reserved_memory_bytes,
        0
    );
    drop(execution);
    assert_eq!(storage.query_budget_snapshot().active_queries, 0);
}

#[test]
fn select_series_execution_result_memory_limit_has_an_exact_boundary() {
    let calibration = storage_with_query_limits(QueryBudgetLimits::default());
    insert_metadata_limit_series(&calibration);
    let execution = calibration
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    let selected = calibration
        .select_series_with_execution_result(&SeriesSelection::new(), &execution)
        .unwrap();
    assert_eq!(selected.series.len(), 3);
    drop(selected);
    drop(execution);
    let exact_memory = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_memory > 0);

    for (limit, should_succeed) in [(exact_memory, true), (exact_memory - 1, false)] {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            max_shared_memory_bytes: Some(limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        insert_metadata_limit_series(&storage);
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let result =
            storage.select_series_with_execution_result(&SeriesSelection::new(), &execution);
        if should_succeed {
            assert_eq!(result.unwrap().series.len(), 3);
        } else {
            assert_query_limit(
                result.expect_err("one byte below the observed peak must fail"),
                QueryLimitReason::PerQueryMemoryBytes,
            );
        }
        drop(execution);
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    }
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

fn arm_one_shot_time_range_visibility_cache_clear(storage: &Arc<ChunkStorage>) {
    let weak_storage = Arc::downgrade(storage);
    let calls = Arc::new(AtomicUsize::new(0));
    let hook_calls = Arc::clone(&calls);
    storage.set_metadata_time_range_summary_hook(move || {
        if hook_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let storage = weak_storage
                .upgrade()
                .expect("time-range metadata storage must remain alive");
            let series_ids = storage.materialized_series_snapshot();
            storage.clear_series_visible_timestamp_cache(series_ids);
        }
    });
}

#[test]
fn select_series_cold_visibility_repair_is_inside_the_execution_envelope() {
    let calibration = cold_visibility_metadata_storage(QueryBudgetLimits::default());
    let execution = calibration
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    let selected = calibration
        .select_series_with_execution_result(&SeriesSelection::new(), &execution)
        .unwrap();
    assert_eq!(selected.series.len(), 1);
    assert_eq!(
        execution.snapshot().intermediate_vector_size,
        COLD_VISIBILITY_RANGE_COUNT as u64
    );
    drop(selected);
    drop(execution);
    let exact_memory = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_memory > 0);
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
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let result =
            storage.select_series_with_execution_result(&SeriesSelection::new(), &execution);
        if should_succeed {
            assert_eq!(result.unwrap().series.len(), 1);
        } else {
            assert_query_limit(
                result.expect_err("one byte below the cold-query peak must fail"),
                QueryLimitReason::PerQueryMemoryBytes,
            );
        }
        drop(execution);
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
        let result = storage.select_series(&SeriesSelection::new());
        if should_succeed {
            assert_eq!(result.unwrap().len(), 1);
        } else {
            assert_query_limit(
                result.unwrap_err(),
                QueryLimitReason::IntermediateVectorSize,
            );
        }
        assert_eq!(storage.query_budget_snapshot().active_queries, 0);
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            0
        );
    }
}

#[test]
fn select_series_cold_visibility_repair_readmits_post_preflight_growth() {
    let calibration = cold_visibility_metadata_storage(QueryBudgetLimits::default());
    assert_eq!(
        calibration
            .select_series(&SeriesSelection::new())
            .unwrap()
            .len(),
        1
    );
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
        storage
            .select_series(&SeriesSelection::new())
            .expect_err("post-preflight growth must be readmitted"),
        QueryLimitReason::PerQueryMemoryBytes,
    );
    storage.clear_metadata_visibility_refresh_post_preflight_hook();
    let snapshot = storage.query_budget_snapshot();
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(snapshot.accounting_invariant_violations_total, 0);
}

#[test]
fn time_range_visibility_repair_uses_the_same_query_memory_and_vector_limits() {
    let selection = SeriesSelection::new().with_time_range(0, 100);
    let calibration = Arc::new(cold_visibility_metadata_storage(
        QueryBudgetLimits::default(),
    ));
    arm_one_shot_time_range_visibility_cache_clear(&calibration);
    assert_eq!(calibration.select_series(&selection).unwrap().len(), 1);
    calibration.clear_metadata_time_range_summary_hook();
    let exact_memory = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_memory > 0);

    for (memory_limit, should_succeed) in [(exact_memory, true), (exact_memory - 1, false)] {
        let storage = Arc::new(cold_visibility_metadata_storage(QueryBudgetLimits {
            max_shared_memory_bytes: Some(memory_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(memory_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        }));
        arm_one_shot_time_range_visibility_cache_clear(&storage);
        let result = storage.select_series(&selection);
        if should_succeed {
            assert_eq!(result.unwrap().len(), 1);
        } else {
            assert_query_limit(
                result.expect_err("one byte below the time-range cold-repair peak must fail"),
                QueryLimitReason::PerQueryMemoryBytes,
            );
        }
        storage.clear_metadata_time_range_summary_hook();
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    for (vector_limit, should_succeed) in [
        (COLD_VISIBILITY_RANGE_COUNT as u64, true),
        (COLD_VISIBILITY_RANGE_COUNT as u64 - 1, false),
    ] {
        let storage = Arc::new(cold_visibility_metadata_storage(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_intermediate_vector_size: Some(vector_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        }));
        arm_one_shot_time_range_visibility_cache_clear(&storage);
        let result = storage.select_series(&selection);
        if should_succeed {
            assert_eq!(result.unwrap().len(), 1);
        } else {
            assert_query_limit(
                result.unwrap_err(),
                QueryLimitReason::IntermediateVectorSize,
            );
        }
        storage.clear_metadata_time_range_summary_hook();
        assert_eq!(storage.query_budget_snapshot().active_queries, 0);
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            0
        );
    }
}

#[test]
fn guarded_row_scan_preflights_resolved_identity_clones_at_exact_boundaries() {
    const REQUESTED_SERIES: usize = 128;
    let requested = (0..REQUESTED_SERIES)
        .map(|index| MetricSeries {
            name: format!("absent_metric_{index:04}"),
            labels: vec![Label::new("host", format!("host-{index:04}"))],
        })
        .collect::<Vec<_>>();

    let calibration = storage_with_query_limits(QueryBudgetLimits::default());
    let execution = calibration
        .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
        .unwrap()
        .unwrap();
    let result = calibration
        .scan_series_rows_with_execution_result(
            &requested,
            0,
            1,
            crate::QueryRowsScanOptions::default(),
            &execution,
        )
        .unwrap();
    assert!(result.page.rows.is_empty());
    assert_eq!(result.reserved_memory_bytes(), 0);
    assert_eq!(
        execution.snapshot().intermediate_vector_size,
        REQUESTED_SERIES as u64
    );
    drop(result);
    drop(execution);
    let exact_memory = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_memory > 0);

    for (memory_limit, should_succeed) in [(exact_memory, true), (exact_memory - 1, false)] {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            max_shared_memory_bytes: Some(memory_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(memory_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let result = storage.scan_series_rows_with_execution_result(
            &requested,
            0,
            1,
            crate::QueryRowsScanOptions::default(),
            &execution,
        );
        if should_succeed {
            assert!(result.unwrap().page.rows.is_empty());
        } else {
            assert_query_limit(
                result.expect_err("one byte below the resolved-identity peak must fail"),
                QueryLimitReason::PerQueryMemoryBytes,
            );
        }
        drop(execution);
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    for (vector_limit, should_succeed) in [
        (REQUESTED_SERIES as u64, true),
        (REQUESTED_SERIES as u64 - 1, false),
    ] {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_intermediate_vector_size: Some(vector_limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let result = storage.scan_series_rows_with_execution_result(
            &requested,
            0,
            1,
            crate::QueryRowsScanOptions::default(),
            &execution,
        );
        if should_succeed {
            assert!(result.unwrap().page.rows.is_empty());
        } else {
            assert_query_limit(
                result.unwrap_err(),
                QueryLimitReason::IntermediateVectorSize,
            );
        }
        drop(execution);
        assert_eq!(storage.query_budget_snapshot().active_queries, 0);
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            0
        );
    }
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
fn select_many_preflights_outer_vector_and_both_batch_slot_buffers() {
    let series = vec![
        MetricSeries {
            name: "missing".to_string(),
            labels: vec![Label::new("host", "a")],
        },
        MetricSeries {
            name: "missing".to_string(),
            labels: vec![Label::new("host", "b")],
        },
    ];

    for (limit, should_succeed) in [(2, true), (1, false)] {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_intermediate_vector_size: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let result = storage.select_many(&series, 0, 2);
        if should_succeed {
            let selected = result.expect("exact outer-vector limit should succeed");
            assert_eq!(selected.len(), 2);
            assert!(selected.iter().all(|item| item.points.is_empty()));
        } else {
            assert_query_limit(
                result.expect_err("one-under outer-vector limit should fail"),
                QueryLimitReason::IntermediateVectorSize,
            );
        }
        assert_eq!(storage.query_budget_snapshot().active_queries, 0);
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            0
        );
    }

    let calibration = storage_with_query_limits(QueryBudgetLimits::default());
    assert_eq!(calibration.select_many(&series, 0, 2).unwrap().len(), 2);
    let exact_memory = calibration
        .query_budget_snapshot()
        .peak_shared_reserved_memory_bytes;
    assert!(exact_memory > 0);

    for (limit, should_succeed) in [(exact_memory, true), (exact_memory - 1, false)] {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            max_shared_memory_bytes: Some(limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let result = storage.select_many(&series, 0, 2);
        if should_succeed {
            assert_eq!(result.unwrap().len(), 2);
        } else {
            assert_query_limit(result.unwrap_err(), QueryLimitReason::PerQueryMemoryBytes);
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }
}

#[test]
fn point_returned_bytes_are_capacity_independent_at_exact_boundary() {
    let compact = vec![
        DataPoint::new(1, Value::Bytes(vec![1, 2, 3])),
        DataPoint::new(2, Value::String("abc".to_string())),
    ];
    let mut roomy_bytes = Vec::with_capacity(128);
    roomy_bytes.extend([1, 2, 3]);
    let mut roomy_text = String::with_capacity(128);
    roomy_text.push_str("abc");
    let roomy = vec![
        DataPoint::new(1, Value::Bytes(roomy_bytes)),
        DataPoint::new(2, Value::String(roomy_text)),
    ];
    assert_eq!(compact, roomy);

    let calibration = storage_with_query_limits(QueryBudgetLimits::default());
    let compact_bytes = observed_returned_bytes(&calibration, |storage, execution| {
        storage.charge_point_query_result(execution, 0, 0, &compact)
    });
    let roomy_bytes = observed_returned_bytes(&calibration, |storage, execution| {
        storage.charge_point_query_result(execution, 0, 0, &roomy)
    });
    assert_eq!(compact_bytes, roomy_bytes);
    assert!(roomy_bytes > 0);

    for (limit, should_succeed) in [(roomy_bytes, true), (roomy_bytes - 1, false)] {
        let storage = storage_with_query_limits(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_returned_bytes: Some(limit),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        });
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let result = storage.charge_point_query_result(&execution, 0, 0, &roomy);
        if should_succeed {
            result.expect("the exact logical returned-byte limit should pass");
            assert_eq!(execution.snapshot().returned_bytes, roomy_bytes);
        } else {
            assert_query_limit(
                result.expect_err("one byte below the logical result size must fail"),
                QueryLimitReason::ReturnedBytes,
            );
        }
        drop(execution);
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
