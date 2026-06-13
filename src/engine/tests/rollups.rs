use super::*;
use crate::{Aggregation, QueryOptions};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

fn cpu_rollup_policy(id: &str, interval: i64) -> crate::storage::RollupPolicy {
    crate::storage::RollupPolicy {
        id: id.to_string(),
        metric: "cpu_usage".to_string(),
        match_labels: Vec::new(),
        interval,
        aggregation: Aggregation::Avg,
        bucket_origin: 0,
    }
}

fn write_rollup_state(
    data_path: &std::path::Path,
    policy_id: &str,
    generation: u64,
    checkpoint: Option<(&str, i64)>,
    pending: Option<(&str, i64, i64)>,
) {
    let state_path = data_path.join(".rollups").join("state.json");
    let checkpoints = checkpoint
        .into_iter()
        .map(|(source_key, materialized_through)| {
            json!({
                "policy_id": policy_id,
                "source_key": source_key,
                "materialized_through": materialized_through,
            })
        })
        .collect::<Vec<_>>();
    let pending_materializations = pending
        .into_iter()
        .map(|(source_key, checkpoint, materialized_through)| {
            json!({
                "policy_id": policy_id,
                "source_key": source_key,
                "checkpoint": checkpoint,
                "materialized_through": materialized_through,
                "generation": generation,
            })
        })
        .collect::<Vec<_>>();
    let state = json!({
        "magic": "tsink-rollup-state",
        "version": 1,
        "checkpoints": checkpoints,
        "pending_materializations": pending_materializations,
        "generations": [{
            "policy_id": policy_id,
            "generation": generation,
        }],
    });
    std::fs::write(state_path, serde_json::to_vec_pretty(&state).unwrap()).unwrap();
}

fn rollup_metric_name(policy_id: &str, generation: u64, metric: &str) -> String {
    if generation == 0 {
        format!("__tsink_rollup__:{policy_id}:{metric}")
    } else {
        format!("__tsink_rollup__:{policy_id}:g{generation}:{metric}")
    }
}

fn block_next_rollup_policy_run(
    storage: &ChunkStorage,
    policy_id: &str,
) -> (Arc<Barrier>, Arc<Barrier>) {
    let started = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let blocked = Arc::new(AtomicBool::new(false));
    let target_policy_id = policy_id.to_string();

    storage.set_rollup_policy_start_hook({
        let started = Arc::clone(&started);
        let resume = Arc::clone(&resume);
        let blocked = Arc::clone(&blocked);
        move |policy| {
            if policy.id != target_policy_id || blocked.swap(true, Ordering::SeqCst) {
                return;
            }
            started.wait();
            resume.wait();
        }
    });

    (started, resume)
}

fn read_rollup_policies_file(data_path: &std::path::Path) -> serde_json::Value {
    serde_json::from_slice(
        &std::fs::read(data_path.join(".rollups").join("policies.json")).unwrap(),
    )
    .unwrap()
}

fn read_rollup_state_file(data_path: &std::path::Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(data_path.join(".rollups").join("state.json")).unwrap())
        .unwrap()
}

#[test]
fn durable_rollups_survive_restart_and_power_aligned_downsample_queries() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 2.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 3.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 4.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 5.0)),
        ])
        .unwrap();

    let rollup_snapshot = storage
        .apply_rollup_policies(vec![crate::storage::RollupPolicy {
            id: "cpu_1s_avg".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 1_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        }])
        .unwrap();
    assert_eq!(rollup_snapshot.policies.len(), 1);
    assert_eq!(rollup_snapshot.policies[0].matched_series, 1);
    assert_eq!(rollup_snapshot.policies[0].materialized_series, 1);
    assert_eq!(
        rollup_snapshot.policies[0].materialized_through,
        Some(4_000)
    );
    assert_eq!(rollup_snapshot.policies[0].lag, Some(0));

    let points = storage
        .select_with_options(
            "cpu_usage",
            QueryOptions::new(0, 5_000)
                .with_labels(labels.clone())
                .with_downsample(1_000, Aggregation::Avg),
        )
        .unwrap();
    assert_eq!(
        points,
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 2.0),
            DataPoint::new(2_000, 3.0),
            DataPoint::new(3_000, 4.0),
            DataPoint::new(4_000, 5.0),
        ]
    );

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.query.rollup_query_plans_total, 1);
    assert_eq!(snapshot.query.partial_rollup_query_plans_total, 1);
    assert!(snapshot.query.rollup_points_read_total >= 4);
    assert!(storage
        .list_metrics()
        .unwrap()
        .iter()
        .all(|series| !series.name.starts_with("__tsink_rollup__:")));

    storage.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    let reopened_points = reopened
        .select_with_options(
            "cpu_usage",
            QueryOptions::new(0, 5_000)
                .with_labels(labels)
                .with_downsample(1_000, Aggregation::Avg),
        )
        .unwrap();
    assert_eq!(reopened_points, points);

    let reopened_snapshot = reopened.observability_snapshot();
    assert_eq!(reopened_snapshot.rollups.policies.len(), 1);
    assert_eq!(
        reopened_snapshot.rollups.policies[0].materialized_through,
        Some(4_000)
    );
}

#[test]
fn crash_recovery_does_not_rewrite_already_materialized_rollup_buckets() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let policy = cpu_rollup_policy("cpu_1s_avg", 1_000);
    let source_key = crate::storage::shard_window_series_identity_key("cpu_usage", &labels);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 2.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 3.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 4.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 5.0)),
        ])
        .unwrap();
    storage.apply_rollup_policies(vec![policy.clone()]).unwrap();
    storage.close().unwrap();

    write_rollup_state(
        temp_dir.path(),
        &policy.id,
        0,
        None,
        Some((source_key.as_str(), i64::MIN, 4_000)),
    );

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    let rebuild_snapshot = reopened.trigger_rollup_run().unwrap();
    assert_eq!(rebuild_snapshot.buckets_materialized_total, 0);
    assert_eq!(rebuild_snapshot.points_materialized_total, 0);
    assert_eq!(
        rebuild_snapshot.policies[0].materialized_through,
        Some(4_000)
    );
    assert_eq!(
        reopened
            .select(
                &rollup_metric_name(&policy.id, 0, &policy.metric),
                &labels,
                0,
                5_000
            )
            .unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 2.0),
            DataPoint::new(2_000, 3.0),
            DataPoint::new(3_000, 4.0),
        ]
    );
    assert_eq!(
        reopened
            .select_with_options(
                "cpu_usage",
                QueryOptions::new(0, 5_000)
                    .with_labels(labels)
                    .with_downsample(1_000, Aggregation::Avg),
            )
            .unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 2.0),
            DataPoint::new(2_000, 3.0),
            DataPoint::new(3_000, 4.0),
            DataPoint::new(4_000, 5.0),
        ]
    );
    reopened.close().unwrap();
}

#[test]
fn crash_recovery_finishes_partial_rollup_materialization_without_duplicate_rewrites() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let policy = cpu_rollup_policy("cpu_1s_avg", 1_000);
    let source_key = crate::storage::shard_window_series_identity_key("cpu_usage", &labels);
    let recovery_metric = rollup_metric_name(&policy.id, 1, &policy.metric);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 2.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 3.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 4.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 5.0)),
        ])
        .unwrap();
    storage.apply_rollup_policies(vec![policy.clone()]).unwrap();
    storage
        .insert_rows(&[
            Row::with_labels(
                recovery_metric.clone(),
                labels.clone(),
                DataPoint::new(0, 1.0),
            ),
            Row::with_labels(
                recovery_metric.clone(),
                labels.clone(),
                DataPoint::new(1_000, 2.0),
            ),
        ])
        .unwrap();
    storage.close().unwrap();

    write_rollup_state(
        temp_dir.path(),
        &policy.id,
        1,
        None,
        Some((source_key.as_str(), i64::MIN, 4_000)),
    );

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    let rebuild_snapshot = reopened.trigger_rollup_run().unwrap();
    assert_eq!(rebuild_snapshot.buckets_materialized_total, 2);
    assert_eq!(rebuild_snapshot.points_materialized_total, 2);
    assert_eq!(
        rebuild_snapshot.policies[0].materialized_through,
        Some(4_000)
    );
    assert_eq!(
        reopened
            .select(&recovery_metric, &labels, 0, 5_000)
            .unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 2.0),
            DataPoint::new(2_000, 3.0),
            DataPoint::new(3_000, 4.0),
        ]
    );
    assert_eq!(
        reopened
            .select_with_options(
                "cpu_usage",
                QueryOptions::new(0, 5_000)
                    .with_labels(labels)
                    .with_downsample(1_000, Aggregation::Avg),
            )
            .unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 2.0),
            DataPoint::new(2_000, 3.0),
            DataPoint::new(3_000, 4.0),
            DataPoint::new(4_000, 5.0),
        ]
    );
    reopened.close().unwrap();
}

#[test]
fn historical_backfill_behind_pending_rollup_recovery_state_forces_generation_rebuild() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let policy = cpu_rollup_policy("cpu_2s_avg", 2_000);
    let source_key = crate::storage::shard_window_series_identity_key("cpu_usage", &labels);
    let rebuilt_metric = rollup_metric_name(&policy.id, 1, &policy.metric);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 5.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 7.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 9.0)),
        ])
        .unwrap();
    storage.apply_rollup_policies(vec![policy.clone()]).unwrap();
    storage.close().unwrap();

    write_rollup_state(
        temp_dir.path(),
        &policy.id,
        0,
        None,
        Some((source_key.as_str(), i64::MIN, 4_000)),
    );

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    reopened
        .insert_rows(&[Row::with_labels(
            "cpu_usage",
            labels.clone(),
            DataPoint::new(1_500, 11.0),
        )])
        .unwrap();

    let rebuild_snapshot = reopened.trigger_rollup_run().unwrap();
    assert_eq!(
        rebuild_snapshot.policies[0].materialized_through,
        Some(4_000)
    );
    assert_eq!(
        reopened.select(&rebuilt_metric, &labels, 0, 5_000).unwrap(),
        vec![DataPoint::new(0, 5.0), DataPoint::new(2_000, 6.0)]
    );
    assert_eq!(
        reopened
            .select_with_options(
                "cpu_usage",
                QueryOptions::new(0, 5_000)
                    .with_labels(labels)
                    .with_downsample(2_000, Aggregation::Avg),
            )
            .unwrap(),
        vec![
            DataPoint::new(0, 5.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 9.0),
        ]
    );
    reopened.close().unwrap();
}

#[test]
fn out_of_order_writes_ahead_of_the_checkpoint_keep_rollup_coverage() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 5.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 7.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 9.0)),
        ])
        .unwrap();

    storage
        .apply_rollup_policies(vec![crate::storage::RollupPolicy {
            id: "cpu_2s_avg".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 2_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        }])
        .unwrap();

    storage
        .insert_rows(&[Row::with_labels(
            "cpu_usage",
            labels.clone(),
            DataPoint::new(4_500, 11.0),
        )])
        .unwrap();

    let rollup_status = storage.observability_snapshot().rollups.policies;
    assert_eq!(rollup_status.len(), 1);
    assert_eq!(rollup_status[0].materialized_series, 1);
    assert_eq!(rollup_status[0].materialized_through, Some(4_000));

    let query = QueryOptions::new(0, 6_000)
        .with_labels(labels.clone())
        .with_downsample(2_000, Aggregation::Avg);
    assert_eq!(
        storage
            .select_with_options("cpu_usage", query.clone())
            .unwrap(),
        vec![
            DataPoint::new(0, 2.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 10.0),
        ]
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .query
            .rollup_query_plans_total,
        1
    );

    storage
        .insert_rows(&[Row::with_labels(
            "cpu_usage",
            labels.clone(),
            DataPoint::new(6_000, 13.0),
        )])
        .unwrap();

    let rebuild_snapshot = storage.trigger_rollup_run().unwrap();
    assert_eq!(rebuild_snapshot.policies.len(), 1);
    assert_eq!(rebuild_snapshot.policies[0].materialized_series, 1);
    assert_eq!(
        rebuild_snapshot.policies[0].materialized_through,
        Some(6_000)
    );

    let rebuilt_query = QueryOptions::new(0, 7_000)
        .with_labels(labels)
        .with_downsample(2_000, Aggregation::Avg);
    assert_eq!(
        storage
            .select_with_options("cpu_usage", rebuilt_query)
            .unwrap(),
        vec![
            DataPoint::new(0, 2.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 10.0),
            DataPoint::new(6_000, 13.0),
        ]
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .query
            .rollup_query_plans_total,
        2
    );
    storage.close().unwrap();
}

#[test]
fn historical_backfill_behind_the_checkpoint_invalidates_rollups_until_rebuilt() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];

    {
        let storage = persistent_rollup_storage(temp_dir.path());

        storage
            .insert_rows(&[
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 5.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 7.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 9.0)),
            ])
            .unwrap();

        storage
            .apply_rollup_policies(vec![crate::storage::RollupPolicy {
                id: "cpu_2s_avg".to_string(),
                metric: "cpu_usage".to_string(),
                match_labels: Vec::new(),
                interval: 2_000,
                aggregation: Aggregation::Avg,
                bucket_origin: 0,
            }])
            .unwrap();

        let query = QueryOptions::new(0, 5_000)
            .with_labels(labels.clone())
            .with_downsample(2_000, Aggregation::Avg);
        assert_eq!(
            storage
                .select_with_options("cpu_usage", query.clone())
                .unwrap(),
            vec![
                DataPoint::new(0, 2.0),
                DataPoint::new(2_000, 6.0),
                DataPoint::new(4_000, 9.0),
            ]
        );
        assert_eq!(
            storage
                .observability_snapshot()
                .query
                .rollup_query_plans_total,
            1
        );

        storage
            .insert_rows(&[Row::with_labels(
                "cpu_usage",
                labels.clone(),
                DataPoint::new(1_500, 11.0),
            )])
            .unwrap();

        let invalidated_status = storage.observability_snapshot().rollups.policies;
        assert_eq!(invalidated_status.len(), 1);
        assert_eq!(invalidated_status[0].materialized_series, 0);
        assert_eq!(invalidated_status[0].materialized_through, None);

        assert_eq!(
            storage.select_with_options("cpu_usage", query).unwrap(),
            vec![
                DataPoint::new(0, 5.0),
                DataPoint::new(2_000, 6.0),
                DataPoint::new(4_000, 9.0),
            ]
        );
        assert_eq!(
            storage
                .observability_snapshot()
                .query
                .rollup_query_plans_total,
            1
        );
        storage.close().unwrap();
    }

    let reopened = reopen_persistent_rollup_storage(temp_dir.path());

    let query = QueryOptions::new(0, 5_000)
        .with_labels(labels)
        .with_downsample(2_000, Aggregation::Avg);
    let invalidated_status = reopened.observability_snapshot().rollups.policies;
    assert_eq!(invalidated_status.len(), 1);
    assert_eq!(invalidated_status[0].materialized_series, 0);
    assert_eq!(invalidated_status[0].materialized_through, None);

    assert_eq!(
        reopened
            .select_with_options("cpu_usage", query.clone())
            .unwrap(),
        vec![
            DataPoint::new(0, 5.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 9.0),
        ]
    );

    let rebuild_snapshot = reopened.trigger_rollup_run().unwrap();
    assert_eq!(rebuild_snapshot.policies.len(), 1);
    assert_eq!(rebuild_snapshot.policies[0].materialized_series, 1);
    assert_eq!(
        rebuild_snapshot.policies[0].materialized_through,
        Some(4_000)
    );

    assert_eq!(
        reopened.select_with_options("cpu_usage", query).unwrap(),
        vec![
            DataPoint::new(0, 5.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 9.0),
        ]
    );
    assert_eq!(
        reopened
            .observability_snapshot()
            .query
            .rollup_query_plans_total,
        1
    );
    reopened.close().unwrap();
}

#[test]
fn invalidated_rollups_stay_raw_for_later_windows_until_rebuilt() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = persistent_rollup_storage(temp_dir.path());

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 5.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 7.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 9.0)),
        ])
        .unwrap();

    storage
        .apply_rollup_policies(vec![crate::storage::RollupPolicy {
            id: "cpu_2s_avg".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 2_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        }])
        .unwrap();

    let later_window_query = QueryOptions::new(2_000, 5_000)
        .with_labels(labels.clone())
        .with_downsample(2_000, Aggregation::Avg);
    assert_eq!(
        storage
            .select_with_options("cpu_usage", later_window_query.clone())
            .unwrap(),
        vec![DataPoint::new(2_000, 6.0), DataPoint::new(4_000, 9.0)]
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .query
            .rollup_query_plans_total,
        1
    );

    storage
        .insert_rows(&[Row::with_labels(
            "cpu_usage",
            labels.clone(),
            DataPoint::new(1_500, 11.0),
        )])
        .unwrap();

    let invalidated_status = storage.observability_snapshot().rollups.policies;
    assert_eq!(invalidated_status.len(), 1);
    assert_eq!(invalidated_status[0].materialized_series, 0);
    assert_eq!(invalidated_status[0].materialized_through, None);
    assert_eq!(
        storage
            .select_with_options("cpu_usage", later_window_query.clone())
            .unwrap(),
        vec![DataPoint::new(2_000, 6.0), DataPoint::new(4_000, 9.0)]
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .query
            .rollup_query_plans_total,
        1,
        "later windows must stay on the raw path until the invalidated rollup policy is rebuilt",
    );

    storage.trigger_rollup_run().unwrap();
    assert_eq!(
        storage
            .select_with_options("cpu_usage", later_window_query)
            .unwrap(),
        vec![DataPoint::new(2_000, 6.0), DataPoint::new(4_000, 9.0)]
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .query
            .rollup_query_plans_total,
        2
    );
    storage.close().unwrap();
}

#[test]
fn historical_backfill_rollup_state_persist_failure_aborts_before_raw_commit() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let query = QueryOptions::new(0, 5_000)
        .with_labels(labels.clone())
        .with_downsample(2_000, Aggregation::Avg);
    let source_key = crate::storage::shard_window_series_identity_key("cpu_usage", &labels);

    let storage = persistent_rollup_storage(temp_dir.path());

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 5.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 7.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 9.0)),
        ])
        .unwrap();

    storage
        .apply_rollup_policies(vec![crate::storage::RollupPolicy {
            id: "cpu_2s_avg".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 2_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        }])
        .unwrap();

    storage.set_rollup_state_persist_hook(|| {
        Err(TsinkError::Other(
            "injected rollup state persist failure".to_string(),
        ))
    });

    let err = storage
        .insert_rows(&[Row::with_labels(
            "cpu_usage",
            labels.clone(),
            DataPoint::new(1_500, 11.0),
        )])
        .unwrap_err();
    storage.clear_rollup_state_persist_hook();

    assert!(
        err.to_string()
            .contains("injected rollup state persist failure"),
        "unexpected rollup state error: {err:?}"
    );
    assert_eq!(
        storage.select("cpu_usage", &labels, 0, 5_000).unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 3.0),
            DataPoint::new(2_000, 5.0),
            DataPoint::new(3_000, 7.0),
            DataPoint::new(4_000, 9.0),
        ]
    );
    assert_eq!(
        storage
            .select_with_options("cpu_usage", query.clone())
            .unwrap(),
        vec![
            DataPoint::new(0, 2.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 9.0),
        ]
    );

    let rollup_status = storage.observability_snapshot().rollups.policies;
    assert_eq!(rollup_status.len(), 1);
    assert_eq!(rollup_status[0].materialized_series, 1);
    assert_eq!(rollup_status[0].materialized_through, Some(4_000));

    let state = read_rollup_state_file(temp_dir.path());
    let checkpoints = state["checkpoints"].as_array().unwrap();
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0]["policy_id"], "cpu_2s_avg");
    assert_eq!(checkpoints[0]["source_key"], source_key);
    assert_eq!(checkpoints[0]["materialized_through"], 4_000);

    storage.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    let reopened_status = reopened.observability_snapshot().rollups.policies;
    assert_eq!(reopened_status.len(), 1);
    assert_eq!(reopened_status[0].materialized_series, 1);
    assert_eq!(reopened_status[0].materialized_through, Some(4_000));
    assert_eq!(
        reopened.select("cpu_usage", &labels, 0, 5_000).unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 3.0),
            DataPoint::new(2_000, 5.0),
            DataPoint::new(3_000, 7.0),
            DataPoint::new(4_000, 9.0),
        ]
    );
    assert_eq!(
        reopened.select_with_options("cpu_usage", query).unwrap(),
        vec![
            DataPoint::new(0, 2.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 9.0),
        ]
    );

    reopened.close().unwrap();
}

#[test]
fn ranged_delete_invalidates_rollups_until_the_policy_is_rebuilt() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = persistent_rollup_storage(temp_dir.path());

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 5.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 7.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 9.0)),
        ])
        .unwrap();

    storage
        .apply_rollup_policies(vec![crate::storage::RollupPolicy {
            id: "cpu_2s_avg".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 2_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        }])
        .unwrap();

    let query = QueryOptions::new(0, 5_000)
        .with_labels(labels.clone())
        .with_downsample(2_000, Aggregation::Avg);
    assert_eq!(
        storage
            .select_with_options("cpu_usage", query.clone())
            .unwrap(),
        vec![
            DataPoint::new(0, 2.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 9.0),
        ]
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .query
            .rollup_query_plans_total,
        1
    );

    storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("cpu_usage")
                .with_matcher(SeriesMatcher::equal("host", "a"))
                .with_time_range(1_000, 2_000),
        )
        .unwrap();

    assert_eq!(
        storage
            .select_with_options("cpu_usage", query.clone())
            .unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 9.0),
        ]
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .query
            .rollup_query_plans_total,
        1
    );

    storage.trigger_rollup_run().unwrap();
    assert_eq!(
        storage.select_with_options("cpu_usage", query).unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 9.0),
        ]
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .query
            .rollup_query_plans_total,
        2
    );
    storage.close().unwrap();
}

#[test]
fn repeated_rollup_invalidations_survive_restart() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 5.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 7.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 9.0)),
            ])
            .unwrap();

        storage
            .apply_rollup_policies(vec![crate::storage::RollupPolicy {
                id: "cpu_2s_avg".to_string(),
                metric: "cpu_usage".to_string(),
                match_labels: Vec::new(),
                interval: 2_000,
                aggregation: Aggregation::Avg,
                bucket_origin: 0,
            }])
            .unwrap();

        storage
            .delete_series(
                &SeriesSelection::new()
                    .with_metric("cpu_usage")
                    .with_matcher(SeriesMatcher::equal("host", "a"))
                    .with_time_range(1_000, 2_000),
            )
            .unwrap();
        storage.trigger_rollup_run().unwrap();

        storage
            .delete_series(
                &SeriesSelection::new()
                    .with_metric("cpu_usage")
                    .with_matcher(SeriesMatcher::equal("host", "a"))
                    .with_time_range(3_000, 4_000),
            )
            .unwrap();
        storage.trigger_rollup_run().unwrap();

        let query = QueryOptions::new(0, 5_000)
            .with_labels(labels.clone())
            .with_downsample(2_000, Aggregation::Avg);
        assert_eq!(
            storage.select_with_options("cpu_usage", query).unwrap(),
            vec![
                DataPoint::new(0, 1.0),
                DataPoint::new(2_000, 5.0),
                DataPoint::new(4_000, 9.0),
            ]
        );
        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    let query = QueryOptions::new(0, 5_000)
        .with_labels(labels)
        .with_downsample(2_000, Aggregation::Avg);
    assert_eq!(
        reopened.select_with_options("cpu_usage", query).unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(2_000, 5.0),
            DataPoint::new(4_000, 9.0),
        ]
    );
    reopened.close().unwrap();
}

#[test]
fn policy_apply_waits_for_an_active_worker_and_persists_the_new_set() {
    use std::sync::mpsc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_rollup_storage(temp_dir.path());
    let labels = vec![Label::new("host", "a")];
    let policy = cpu_rollup_policy("cpu_1s_avg", 1_000);
    let extra_policy = cpu_rollup_policy("cpu_2s_avg", 2_000);

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 2.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 3.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 4.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 5.0)),
        ])
        .unwrap();
    storage.apply_rollup_policies(vec![policy.clone()]).unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "cpu_usage",
            labels.clone(),
            DataPoint::new(5_000, 6.0),
        )])
        .unwrap();

    let (started, resume) = block_next_rollup_policy_run(storage.as_ref(), &policy.id);
    let worker_storage = Arc::clone(&storage);
    let worker = thread::spawn(move || worker_storage.trigger_rollup_run());
    started.wait();

    let (tx, rx) = mpsc::channel();
    let apply_storage = Arc::clone(&storage);
    let apply_policy = policy.clone();
    let apply_extra_policy = extra_policy.clone();
    let apply = thread::spawn(move || {
        tx.send(apply_storage.apply_rollup_policies(vec![apply_policy, apply_extra_policy]))
            .unwrap();
    });

    assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());

    resume.wait();
    worker.join().unwrap().unwrap();
    let snapshot = rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
    apply.join().unwrap();
    storage.clear_rollup_policy_start_hook();

    let policy_ids = snapshot
        .policies
        .iter()
        .map(|status| status.policy.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(policy_ids, vec!["cpu_1s_avg", "cpu_2s_avg"]);
    let applied_policy = snapshot
        .policies
        .iter()
        .find(|status| status.policy.id == extra_policy.id)
        .unwrap();
    assert_eq!(applied_policy.materialized_series, 1);
    assert_eq!(applied_policy.materialized_through, Some(4_000));

    let persisted_policies = read_rollup_policies_file(temp_dir.path());
    assert_eq!(
        persisted_policies["policies"]
            .as_array()
            .unwrap()
            .iter()
            .map(|policy| policy["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["cpu_1s_avg", "cpu_2s_avg"]
    );

    let persisted_state = read_rollup_state_file(temp_dir.path());
    assert_eq!(
        persisted_state["generations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["policy_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["cpu_1s_avg", "cpu_2s_avg"]
    );
    assert!(persisted_state["checkpoints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|checkpoint| {
            checkpoint["policy_id"] == extra_policy.id
                && checkpoint["materialized_through"] == 4_000
        }));
}

#[test]
fn policy_remove_waits_for_an_active_worker_and_discards_stale_state() {
    use std::sync::mpsc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_rollup_storage(temp_dir.path());
    let labels = vec![Label::new("host", "a")];
    let policy = cpu_rollup_policy("cpu_1s_avg", 1_000);

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 2.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 3.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 4.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 5.0)),
        ])
        .unwrap();
    storage.apply_rollup_policies(vec![policy.clone()]).unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "cpu_usage",
            labels.clone(),
            DataPoint::new(5_000, 6.0),
        )])
        .unwrap();

    let (started, resume) = block_next_rollup_policy_run(storage.as_ref(), &policy.id);
    let worker_storage = Arc::clone(&storage);
    let worker = thread::spawn(move || worker_storage.trigger_rollup_run());
    started.wait();

    let (tx, rx) = mpsc::channel();
    let remove_storage = Arc::clone(&storage);
    let remove = thread::spawn(move || {
        tx.send(remove_storage.apply_rollup_policies(Vec::new()))
            .unwrap();
    });

    assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());

    resume.wait();
    worker.join().unwrap().unwrap();
    let snapshot = rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
    remove.join().unwrap();
    storage.clear_rollup_policy_start_hook();

    assert!(snapshot.policies.is_empty());
    assert!(storage.observability_snapshot().rollups.policies.is_empty());

    let persisted_policies = read_rollup_policies_file(temp_dir.path());
    assert!(persisted_policies["policies"]
        .as_array()
        .unwrap()
        .is_empty());

    let persisted_state = read_rollup_state_file(temp_dir.path());
    assert!(persisted_state["checkpoints"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(persisted_state["pending_materializations"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(persisted_state["generations"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn close_waits_for_inflight_background_rollup_before_final_persist() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_rollup_storage(temp_dir.path());
    let labels = vec![Label::new("host", "a")];
    let policy = cpu_rollup_policy("cpu_1s_avg", 1_000);

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 2.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 3.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 4.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 5.0)),
        ])
        .unwrap();
    storage.apply_rollup_policies(vec![policy.clone()]).unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "cpu_usage",
            labels,
            DataPoint::new(5_000, 6.0),
        )])
        .unwrap();

    let (background_entered_tx, background_entered_rx) = mpsc::channel();
    let (close_persist_tx, close_persist_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let blocked = Arc::new(AtomicBool::new(false));
    let target_policy_id = policy.id.clone();

    storage.set_rollup_policy_start_hook({
        let blocked = Arc::clone(&blocked);
        let release_rx = Arc::clone(&release_rx);
        move |started_policy| {
            if started_policy.id != target_policy_id || blocked.swap(true, Ordering::SeqCst) {
                return;
            }
            background_entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }
    });
    storage.set_persist_post_publish_hook(move |_| {
        close_persist_tx.send(()).unwrap();
    });

    storage
        .start_background_rollup_thread(Duration::from_secs(60))
        .unwrap();
    storage.notify_rollup_thread();
    background_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("background rollup worker did not reach the policy start hook");

    let close_storage = Arc::clone(&storage);
    let (close_tx, close_rx) = mpsc::channel();
    let close_thread = thread::spawn(move || {
        close_tx.send(close_storage.close()).unwrap();
    });

    assert!(
        close_persist_rx
            .recv_timeout(Duration::from_millis(200))
            .is_err(),
        "close should not publish its final persisted segment while background rollup is mid-pass",
    );

    release_tx.send(()).unwrap();
    close_persist_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("close did not reach its final persist after the background rollup finished");
    assert!(close_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    close_thread.join().unwrap();
}

#[test]
fn policy_update_waits_for_an_active_worker_and_rebuilds_under_a_new_generation() {
    use std::sync::mpsc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_rollup_storage(temp_dir.path());
    let labels = vec![Label::new("host", "a")];
    let original_policy = cpu_rollup_policy("cpu_rollup", 1_000);
    let updated_policy = cpu_rollup_policy("cpu_rollup", 2_000);

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 5.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3_000, 7.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_000, 9.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(5_000, 11.0)),
        ])
        .unwrap();
    storage
        .apply_rollup_policies(vec![original_policy.clone()])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "cpu_usage",
            labels.clone(),
            DataPoint::new(6_000, 13.0),
        )])
        .unwrap();

    let (started, resume) = block_next_rollup_policy_run(storage.as_ref(), &original_policy.id);
    let worker_storage = Arc::clone(&storage);
    let worker = thread::spawn(move || worker_storage.trigger_rollup_run());
    started.wait();

    let (tx, rx) = mpsc::channel();
    let update_storage = Arc::clone(&storage);
    let updated_policy_clone = updated_policy.clone();
    let update = thread::spawn(move || {
        tx.send(update_storage.apply_rollup_policies(vec![updated_policy_clone]))
            .unwrap();
    });

    assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());

    resume.wait();
    worker.join().unwrap().unwrap();
    let snapshot = rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
    update.join().unwrap();
    storage.clear_rollup_policy_start_hook();

    assert_eq!(snapshot.policies.len(), 1);
    assert_eq!(snapshot.policies[0].policy.interval, 2_000);
    assert_eq!(snapshot.policies[0].materialized_series, 1);
    assert_eq!(snapshot.policies[0].materialized_through, Some(6_000));

    let rebuilt_metric = rollup_metric_name(&updated_policy.id, 1, &updated_policy.metric);
    assert_eq!(
        storage.select(&rebuilt_metric, &labels, 0, 7_000).unwrap(),
        vec![
            DataPoint::new(0, 2.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 10.0),
        ]
    );
    assert_eq!(
        storage
            .select_with_options(
                "cpu_usage",
                QueryOptions::new(0, 7_000)
                    .with_labels(labels)
                    .with_downsample(2_000, Aggregation::Avg),
            )
            .unwrap(),
        vec![
            DataPoint::new(0, 2.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 10.0),
            DataPoint::new(6_000, 13.0),
        ]
    );

    let persisted_policies = read_rollup_policies_file(temp_dir.path());
    assert_eq!(
        persisted_policies["policies"][0]["interval"].as_i64(),
        Some(2_000)
    );

    let persisted_state = read_rollup_state_file(temp_dir.path());
    assert_eq!(
        persisted_state["generations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| (
                entry["policy_id"].as_str().unwrap(),
                entry["generation"].as_u64().unwrap()
            ))
            .collect::<Vec<_>>(),
        vec![("cpu_rollup", 1)]
    );
    assert_eq!(
        persisted_state["checkpoints"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| (
                entry["policy_id"].as_str().unwrap(),
                entry["materialized_through"].as_i64().unwrap()
            ))
            .collect::<Vec<_>>(),
        vec![("cpu_rollup", 6_000)]
    );
}
