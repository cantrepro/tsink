use super::super::rollups::BackgroundRollupCursor;
use super::super::SealedChunkKey;
use super::*;
use crate::{
    Aggregation, QueryBudgetError, QueryBudgetLimits, QueryLimitReason, QueryOptions,
    QueryWorkLimits,
};
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

fn single_writer_persistent_rollup_storage(root: &std::path::Path) -> Arc<ChunkStorage> {
    single_writer_persistent_rollup_storage_with_timeout(root, Duration::from_secs(5))
}

fn single_writer_persistent_rollup_storage_with_timeout(
    root: &std::path::Path,
    write_timeout: Duration,
) -> Arc<ChunkStorage> {
    let wal = FramedWal::open(root.join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.retention_enforced = false;
    options.max_writers = 1;
    options.write_timeout = write_timeout;

    Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            Some(wal),
            Some(root.join(NUMERIC_LANE_ROOT)),
            Some(root.join(BLOB_LANE_ROOT)),
            1,
            options,
        )
        .unwrap(),
    )
}

fn wait_for_sole_writer_permit(storage: &ChunkStorage, owner: &str) {
    assert_eq!(storage.runtime.write_limiter.capacity(), 1);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while storage.runtime.write_limiter.available_permits() != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "{owner} did not acquire the sole writer permit before waiting for rollup serialization"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn seed_materialized_cpu_rollup(
    storage: &ChunkStorage,
    labels: &[Label],
    policy: &crate::storage::RollupPolicy,
) {
    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.to_vec(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.to_vec(), DataPoint::new(1_000, 3.0)),
            Row::with_labels("cpu_usage", labels.to_vec(), DataPoint::new(2_000, 5.0)),
            Row::with_labels("cpu_usage", labels.to_vec(), DataPoint::new(3_000, 7.0)),
            Row::with_labels("cpu_usage", labels.to_vec(), DataPoint::new(4_000, 9.0)),
        ])
        .unwrap();
    let snapshot = storage.apply_rollup_policies(vec![policy.clone()]).unwrap();
    assert_eq!(snapshot.policies.len(), 1);
    assert_eq!(snapshot.policies[0].materialized_through, Some(4_000));
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
    super::super::rollups::load_rollup_state_json(&data_path.join(".rollups").join("state.json"))
        .unwrap()
}

#[test]
fn finite_profile_first_materialization_publishes_managed_state_journal() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "finite")];
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_resource_profile(crate::ResourceProfile::Test)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();
    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
        ])
        .unwrap();
    storage
        .apply_rollup_policies(vec![cpu_rollup_policy("finite-policy", 1_000)])
        .unwrap();

    let rollup_dir = temp_dir.path().join(".rollups");
    let journal_batch = std::fs::read_dir(&rollup_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("state-journal-batch-") && name.ends_with(".d")
                })
        })
        .expect("first materialization should publish a batch journal generation");
    assert!(journal_batch.is_dir());
    assert!(std::fs::read_dir(journal_batch).unwrap().next().is_some());
    let status = storage.observability_snapshot().rollups.policies;
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].materialized_series, 1);
    let disk = storage.observability_snapshot().local_disk.unwrap();
    assert_eq!(disk.active_reservations, 0);
    assert_eq!(disk.reserved_bytes, 0);
    storage.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_resource_profile(crate::ResourceProfile::Test)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();
    assert_eq!(
        reopened.select("cpu_usage", &labels, 0, 2_000).unwrap(),
        vec![DataPoint::new(0, 1.0), DataPoint::new(1_000, 3.0)]
    );
    reopened.close().unwrap();
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
    let reopened_rollups = if reopened_snapshot.rollups.source_traversal_complete {
        reopened_snapshot.rollups
    } else {
        reopened.trigger_rollup_run().unwrap()
    };
    assert_eq!(
        reopened_rollups.policies[0].materialized_through,
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
    if rollup_status[0].source_traversal_complete {
        assert_eq!(rollup_status[0].materialized_series, 1);
        assert_eq!(rollup_status[0].materialized_through, Some(4_000));
    }

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
fn historical_backfill_state_first_interruption_aborts_before_raw_commit() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let query = QueryOptions::new(0, 5_000)
        .with_labels(labels.clone())
        .with_downsample(2_000, Aggregation::Avg);
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
    assert!(
        checkpoints.is_empty(),
        "the durable state-first candidate must retain the safe rollup invalidation"
    );

    storage.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    let reopened_status = reopened.observability_snapshot().rollups.policies;
    assert_eq!(reopened_status.len(), 1);
    assert_eq!(reopened_status[0].materialized_series, 0);
    assert_eq!(reopened_status[0].materialized_through, None);
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
    assert!(!snapshot.source_traversal_complete);
    assert_eq!(
        snapshot.continuation_policy_id.as_deref(),
        Some(extra_policy.id.as_str())
    );
    assert_eq!(applied_policy.materialized_series, 0);
    assert_eq!(applied_policy.materialized_through, None);
    let completed = storage.trigger_rollup_run().unwrap();
    let applied_policy = completed
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
fn committed_policy_apply_surfaces_initial_materialization_failure_and_reopens() {
    use std::sync::atomic::AtomicUsize;

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
            Row::with_labels("cpu_usage", labels, DataPoint::new(4_000, 5.0)),
        ])
        .unwrap();

    let persist_calls = Arc::new(AtomicUsize::new(0));
    storage.set_rollup_state_persist_hook({
        let persist_calls = Arc::clone(&persist_calls);
        move || {
            let call = persist_calls.fetch_add(1, Ordering::SeqCst);
            if call == 1 {
                return Err(TsinkError::Other(
                    "injected post-commit materialization state failure".to_string(),
                ));
            }
            Ok(())
        }
    });

    let snapshot = storage
        .apply_rollup_policies(vec![policy.clone()])
        .expect("a durable policy apply must not be reported as rejected");
    storage.clear_rollup_state_persist_hook();

    assert_eq!(persist_calls.load(Ordering::SeqCst), 2);
    assert_eq!(snapshot.policies.len(), 1);
    assert_eq!(snapshot.policies[0].policy, policy);
    assert!(
        snapshot.policies[0].last_error.as_deref().is_some_and(
            |error| error.contains("injected post-commit materialization state failure")
        ),
        "committed apply should expose the deferred materialization failure: {:?}",
        snapshot.policies[0].last_error
    );
    assert_eq!(snapshot.worker_errors_total, 1);
    assert_eq!(
        read_rollup_policies_file(temp_dir.path())["policies"][0]["id"],
        policy.id
    );

    storage.close().unwrap();
    drop(storage);

    let reopened = reopen_persistent_rollup_storage(temp_dir.path());
    let reopened_status = reopened.observability_snapshot().rollups.policies;
    assert_eq!(reopened_status.len(), 1);
    assert_eq!(reopened_status[0].policy, policy);

    let retry = reopened.trigger_rollup_run().unwrap();
    assert_eq!(retry.policies.len(), 1);
    assert_eq!(retry.policies[0].policy, policy);
    assert_eq!(retry.policies[0].last_error, None);
    reopened.close().unwrap();
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

#[test]
fn close_waits_for_single_writer_historical_invalidation_without_lock_inversion() {
    use std::sync::mpsc;
    use std::sync::Mutex;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let storage = single_writer_persistent_rollup_storage(temp_dir.path());
    let labels = vec![Label::new("host", "a")];
    let policy = cpu_rollup_policy("cpu_1s_avg", 1_000);
    seed_materialized_cpu_rollup(storage.as_ref(), &labels, &policy);

    let historical = Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_500, 11.0));
    assert_eq!(
        storage
            .rollup_policy_ids_needing_rebuild_for_rows(std::slice::from_ref(&historical))
            .into_iter()
            .collect::<Vec<_>>(),
        vec![policy.id.clone()]
    );

    let (invalidation_entered_tx, invalidation_entered_rx) = mpsc::channel();
    let (release_invalidation_tx, release_invalidation_rx) = mpsc::channel();
    let release_invalidation_rx = Arc::new(Mutex::new(release_invalidation_rx));
    storage.set_rollup_state_persist_hook({
        let release_invalidation_rx = Arc::clone(&release_invalidation_rx);
        move || {
            invalidation_entered_tx.send(()).unwrap();
            release_invalidation_rx.lock().unwrap().recv().unwrap();
            Ok(())
        }
    });

    let (historical_tx, historical_rx) = mpsc::channel();
    let historical_storage = Arc::clone(&storage);
    let historical_thread = thread::spawn(move || {
        historical_tx
            .send(historical_storage.insert_rows(&[historical]))
            .unwrap();
    });
    invalidation_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("historical ingest did not reach durable rollup invalidation");
    wait_for_sole_writer_permit(storage.as_ref(), "historical ingest");

    let (close_tx, close_rx) = mpsc::channel();
    let close_storage = Arc::clone(&storage);
    let close_thread = thread::spawn(move || {
        close_tx.send(close_storage.close()).unwrap();
    });
    assert!(
        close_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "close must wait for the historical writer that owns the sole permit"
    );

    release_invalidation_tx.send(()).unwrap();
    historical_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("historical ingest remained blocked after rollup invalidation was released")
        .unwrap();
    close_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("close remained blocked after the historical ingest released its permit")
        .unwrap();
    historical_thread.join().unwrap();
    close_thread.join().unwrap();
    storage.clear_rollup_state_persist_hook();
    drop(storage);

    let reopened = reopen_persistent_rollup_storage(temp_dir.path());
    let status = reopened.observability_snapshot().rollups.policies;
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].materialized_series, 0);
    assert_eq!(status[0].materialized_through, None);
    assert_eq!(
        reopened.select("cpu_usage", &labels, 0, 5_000).unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 3.0),
            DataPoint::new(1_500, 11.0),
            DataPoint::new(2_000, 5.0),
            DataPoint::new(3_000, 7.0),
            DataPoint::new(4_000, 9.0),
        ]
    );
    reopened.close().unwrap();
}

#[test]
fn idle_shared_background_rollup_does_not_wait_for_writer_permits() {
    let temp_dir = TempDir::new().unwrap();
    let storage =
        single_writer_persistent_rollup_storage_with_timeout(temp_dir.path(), Duration::ZERO);
    let held_permit = storage.runtime.write_limiter.acquire();
    let before = storage.observability_snapshot().rollups;

    storage
        .run_shared_background_rollup_pipeline_once()
        .unwrap();

    let after = storage.observability_snapshot().rollups;
    assert_eq!(after.worker_runs_total, before.worker_runs_total + 1);
    assert_eq!(after.worker_success_total, before.worker_success_total + 1);
    assert_eq!(after.worker_errors_total, before.worker_errors_total);
    assert!(after.source_traversal_complete);
    assert_eq!(storage.runtime.write_limiter.available_permits(), 0);

    drop(held_permit);
    storage.close().unwrap();
}

#[test]
fn rollup_materialization_uses_permit_before_run_lock_with_historical_ingest() {
    use std::sync::mpsc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let storage = single_writer_persistent_rollup_storage(temp_dir.path());
    let labels = vec![Label::new("host", "a")];
    let policy = cpu_rollup_policy("cpu_1s_avg", 1_000);
    seed_materialized_cpu_rollup(storage.as_ref(), &labels, &policy);
    storage
        .insert_rows(&[Row::with_labels(
            "cpu_usage",
            labels.clone(),
            DataPoint::new(5_000, 13.0),
        )])
        .unwrap();

    let historical = Row::with_labels("cpu_usage", labels, DataPoint::new(1_500, 11.0));
    assert_eq!(
        storage
            .rollup_policy_ids_needing_rebuild_for_rows(std::slice::from_ref(&historical))
            .into_iter()
            .collect::<Vec<_>>(),
        vec![policy.id.clone()]
    );

    let run_guard = storage.rollups.run_lock.lock();
    let (rollup_tx, rollup_rx) = mpsc::channel();
    let rollup_storage = Arc::clone(&storage);
    let rollup_thread = thread::spawn(move || {
        rollup_tx.send(rollup_storage.trigger_rollup_run()).unwrap();
    });
    wait_for_sole_writer_permit(storage.as_ref(), "rollup materialization");

    let (historical_tx, historical_rx) = mpsc::channel();
    let historical_storage = Arc::clone(&storage);
    let historical_thread = thread::spawn(move || {
        historical_tx
            .send(historical_storage.insert_rows(&[historical]))
            .unwrap();
    });
    assert!(rollup_rx.recv_timeout(Duration::from_millis(100)).is_err());
    assert!(historical_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());

    drop(run_guard);
    rollup_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("rollup materialization remained blocked")
        .unwrap();
    historical_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("historical ingest remained blocked behind rollup materialization")
        .unwrap();
    rollup_thread.join().unwrap();
    historical_thread.join().unwrap();

    let status = storage.observability_snapshot().rollups.policies;
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].materialized_series, 0);
    assert_eq!(status[0].materialized_through, None);
    storage.close().unwrap();
}

#[test]
fn rollup_source_snapshot_excludes_concurrent_raw_commits_until_checkpoint_publication() {
    use std::sync::mpsc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_rollup_storage(temp_dir.path());
    assert_eq!(storage.runtime.write_limiter.capacity(), 2);

    let labels = vec![Label::new("host", "a")];
    let policy = cpu_rollup_policy("cpu_1s_avg", 1_000);
    seed_materialized_cpu_rollup(storage.as_ref(), &labels, &policy);
    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(5_000, 13.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(6_000, 15.0)),
        ])
        .unwrap();

    let source_read = Arc::new(Barrier::new(2));
    let resume_rollup = Arc::new(Barrier::new(2));
    let blocked = Arc::new(AtomicBool::new(false));
    storage.set_rollup_source_read_hook({
        let source_read = Arc::clone(&source_read);
        let resume_rollup = Arc::clone(&resume_rollup);
        let blocked = Arc::clone(&blocked);
        let policy_id = policy.id.clone();
        move |candidate, _series_id| {
            if candidate.id != policy_id || blocked.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            source_read.wait();
            resume_rollup.wait();
            Ok(())
        }
    });

    let (rollup_tx, rollup_rx) = mpsc::channel();
    let rollup_storage = Arc::clone(&storage);
    let rollup_thread = thread::spawn(move || {
        rollup_tx.send(rollup_storage.trigger_rollup_run()).unwrap();
    });
    source_read.wait();
    assert_eq!(
        storage.runtime.write_limiter.available_permits(),
        0,
        "rollup must fence every raw writer between source read and checkpoint publication"
    );

    let late_row = Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(4_500, 11.0));
    let (writer_started_tx, writer_started_rx) = mpsc::channel();
    let (writer_tx, writer_rx) = mpsc::channel();
    let writer_storage = Arc::clone(&storage);
    let writer_thread = thread::spawn(move || {
        writer_started_tx.send(()).unwrap();
        writer_tx
            .send(writer_storage.insert_rows(&[late_row]))
            .unwrap();
    });
    writer_started_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(
        writer_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "a raw writer must not commit against the source snapshot being checkpointed"
    );

    resume_rollup.wait();
    rollup_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("rollup remained blocked after its source read was released")
        .unwrap();
    writer_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("raw writer remained blocked after rollup checkpoint publication")
        .unwrap();
    rollup_thread.join().unwrap();
    writer_thread.join().unwrap();
    storage.clear_rollup_source_read_hook();

    let invalidated = storage.observability_snapshot().rollups.policies;
    assert_eq!(invalidated.len(), 1);
    assert_eq!(invalidated[0].materialized_series, 0);
    assert_eq!(invalidated[0].materialized_through, None);

    storage.trigger_rollup_run().unwrap();
    assert_eq!(
        storage
            .select_with_options(
                "cpu_usage",
                QueryOptions::new(4_000, 6_000)
                    .with_labels(labels)
                    .with_downsample(1_000, Aggregation::Avg),
            )
            .unwrap(),
        vec![DataPoint::new(4_000, 10.0), DataPoint::new(5_000, 13.0)]
    );
    storage.close().unwrap();
}

#[test]
fn policy_apply_uses_permit_before_run_lock_with_historical_ingest() {
    use std::sync::mpsc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let storage = single_writer_persistent_rollup_storage(temp_dir.path());
    let labels = vec![Label::new("host", "a")];
    let policy = cpu_rollup_policy("cpu_1s_avg", 1_000);
    let extra_policy = cpu_rollup_policy("cpu_2s_avg", 2_000);
    seed_materialized_cpu_rollup(storage.as_ref(), &labels, &policy);

    let historical = Row::with_labels("cpu_usage", labels, DataPoint::new(1_500, 11.0));
    assert_eq!(
        storage
            .rollup_policy_ids_needing_rebuild_for_rows(std::slice::from_ref(&historical))
            .into_iter()
            .collect::<Vec<_>>(),
        vec![policy.id.clone()]
    );

    let run_guard = storage.rollups.run_lock.lock();
    let (apply_tx, apply_rx) = mpsc::channel();
    let apply_storage = Arc::clone(&storage);
    let apply_policy = policy.clone();
    let apply_extra_policy = extra_policy.clone();
    let apply_thread = thread::spawn(move || {
        apply_tx
            .send(apply_storage.apply_rollup_policies(vec![apply_policy, apply_extra_policy]))
            .unwrap();
    });
    wait_for_sole_writer_permit(storage.as_ref(), "rollup policy apply");

    let (historical_tx, historical_rx) = mpsc::channel();
    let historical_storage = Arc::clone(&storage);
    let historical_thread = thread::spawn(move || {
        historical_tx
            .send(historical_storage.insert_rows(&[historical]))
            .unwrap();
    });
    assert!(apply_rx.recv_timeout(Duration::from_millis(100)).is_err());
    assert!(historical_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());

    drop(run_guard);
    apply_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("rollup policy apply remained blocked")
        .unwrap();
    historical_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("historical ingest remained blocked behind policy apply")
        .unwrap();
    apply_thread.join().unwrap();
    historical_thread.join().unwrap();

    let status = storage.observability_snapshot().rollups.policies;
    assert_eq!(status.len(), 2);
    assert!(status.iter().all(|entry| entry.materialized_series == 0));
    assert!(status
        .iter()
        .all(|entry| entry.materialized_through.is_none()));
    assert_eq!(
        status
            .iter()
            .map(|entry| entry.policy.id.as_str())
            .collect::<Vec<_>>(),
        vec![policy.id.as_str(), extra_policy.id.as_str()]
    );
    storage.close().unwrap();
}

#[test]
fn snapshot_uses_permit_before_run_lock_with_historical_ingest() {
    use std::sync::mpsc;
    use std::thread;

    let data_dir = TempDir::new().unwrap();
    let artifact_dir = TempDir::new().unwrap();
    let snapshot_path = artifact_dir.path().join("snapshot");
    let restore_path = artifact_dir.path().join("restore");
    let storage = single_writer_persistent_rollup_storage(data_dir.path());
    let fixture_manifest_builder = StorageBuilder::new()
        .with_data_path(data_dir.path())
        .with_chunk_points(8)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .with_partition_duration(Duration::MAX);
    super::super::data_directory_manifest::install_current_manifest_for_test(
        &fixture_manifest_builder,
    )
    .unwrap();
    let labels = vec![Label::new("host", "a")];
    let policy = cpu_rollup_policy("cpu_1s_avg", 1_000);
    seed_materialized_cpu_rollup(storage.as_ref(), &labels, &policy);

    let historical = Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_500, 11.0));
    assert_eq!(
        storage
            .rollup_policy_ids_needing_rebuild_for_rows(std::slice::from_ref(&historical))
            .into_iter()
            .collect::<Vec<_>>(),
        vec![policy.id]
    );

    let run_guard = storage.rollups.run_lock.lock();
    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let snapshot_storage = Arc::clone(&storage);
    let snapshot_path_for_thread = snapshot_path.clone();
    let snapshot_thread = thread::spawn(move || {
        snapshot_tx
            .send(snapshot_storage.snapshot(&snapshot_path_for_thread))
            .unwrap();
    });
    wait_for_sole_writer_permit(storage.as_ref(), "snapshot");

    let (historical_tx, historical_rx) = mpsc::channel();
    let historical_storage = Arc::clone(&storage);
    let historical_thread = thread::spawn(move || {
        historical_tx
            .send(historical_storage.insert_rows(&[historical]))
            .unwrap();
    });
    assert!(snapshot_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    assert!(historical_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());

    drop(run_guard);
    snapshot_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("snapshot remained blocked")
        .unwrap();
    historical_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("historical ingest remained blocked behind snapshot")
        .unwrap();
    snapshot_thread.join().unwrap();
    historical_thread.join().unwrap();

    assert_eq!(
        storage.select("cpu_usage", &labels, 0, 5_000).unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 3.0),
            DataPoint::new(1_500, 11.0),
            DataPoint::new(2_000, 5.0),
            DataPoint::new(3_000, 7.0),
            DataPoint::new(4_000, 9.0),
        ]
    );

    StorageBuilder::restore_from_snapshot(&snapshot_path, &restore_path).unwrap();
    let restored = StorageBuilder::new()
        .with_data_path(&restore_path)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .with_chunk_points(8)
        .with_partition_duration(Duration::MAX)
        .build()
        .unwrap();
    assert_eq!(
        restored.select("cpu_usage", &labels, 0, 5_000).unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 3.0),
            DataPoint::new(2_000, 5.0),
            DataPoint::new(3_000, 7.0),
            DataPoint::new(4_000, 9.0),
        ],
        "the snapshot must precede the backfill that was waiting for its drained permit"
    );

    restored.close().unwrap();
    storage.close().unwrap();
}

#[test]
fn background_rollup_pages_matching_series_at_the_maintenance_limit() {
    let data_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(data_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.retention_enforced = false;
    options.max_writers = 1;
    options.maintenance_max_items_per_pass = 1;
    options.maintenance_max_bytes_per_pass = 256 * 1024 * 1024;
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        Some(wal),
        Some(data_dir.path().join(NUMERIC_LANE_ROOT)),
        Some(data_dir.path().join(BLOB_LANE_ROOT)),
        1,
        options,
    )
    .unwrap();
    let policy = cpu_rollup_policy("bounded-background", 1_000);
    storage.apply_rollup_policies(vec![policy.clone()]).unwrap();

    let labels = (0..3)
        .map(|host| vec![Label::new("host", host.to_string())])
        .collect::<Vec<_>>();
    let rows = labels
        .iter()
        .flat_map(|labels| {
            [
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
            ]
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let metric = "__tsink_rollup__:bounded-background:cpu_usage";
    let materialized_sources = || {
        labels
            .iter()
            .filter(|labels| {
                !storage
                    .select(metric, labels, i64::MIN, i64::MAX)
                    .unwrap()
                    .is_empty()
            })
            .count()
    };
    let mut cursor = BackgroundRollupCursor::default();

    storage
        .run_background_rollup_pipeline_once(&mut cursor)
        .unwrap();
    assert_eq!(materialized_sources(), 1);
    let status = storage.observability_snapshot().rollups.policies;
    assert_eq!(status[0].materialized_series, 1);
    assert_eq!(status[0].materialized_through, None);
    storage
        .run_background_rollup_pipeline_once(&mut cursor)
        .unwrap();
    assert_eq!(materialized_sources(), 2);
    let status = storage.observability_snapshot().rollups.policies;
    assert_eq!(status[0].materialized_series, 2);
    assert_eq!(status[0].materialized_through, None);
    storage
        .run_background_rollup_pipeline_once(&mut cursor)
        .unwrap();
    assert_eq!(materialized_sources(), 3);
    let status = storage.observability_snapshot().rollups.policies;
    assert_eq!(status[0].materialized_series, 3);
    assert_eq!(status[0].materialized_through, None);

    // A full exact-multiple page cannot inspect posting N+1, so one empty terminal page proves
    // complete coverage without duplicating already checkpointed buckets.
    storage
        .run_background_rollup_pipeline_once(&mut cursor)
        .unwrap();
    assert_eq!(materialized_sources(), 3);
    let status = storage.observability_snapshot().rollups.policies;
    assert_eq!(status[0].materialized_through, Some(1_000));
    assert!(status[0].source_traversal_complete);
    for labels in labels {
        assert_eq!(
            storage.select(metric, &labels, i64::MIN, i64::MAX).unwrap(),
            vec![DataPoint::new(0, 1.0)]
        );
    }

    storage.close().unwrap();
}

#[test]
fn background_rollup_page_error_does_not_starve_later_series() {
    let data_dir = TempDir::new().unwrap();
    let wal = FramedWal::open(data_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.retention_enforced = false;
    options.max_writers = 1;
    options.maintenance_max_items_per_pass = 3;
    options.maintenance_max_bytes_per_pass = 256 * 1024 * 1024;
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        Some(wal),
        Some(data_dir.path().join(NUMERIC_LANE_ROOT)),
        Some(data_dir.path().join(BLOB_LANE_ROOT)),
        1,
        options,
    )
    .unwrap();
    let policy = cpu_rollup_policy("bounded-error-progress", 1_000);
    storage.apply_rollup_policies(vec![policy]).unwrap();

    let labels = (0..3)
        .map(|host| vec![Label::new("host", host.to_string())])
        .collect::<Vec<_>>();
    let rows = labels
        .iter()
        .flat_map(|labels| {
            [
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
            ]
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let fail_first_source = Arc::new(AtomicBool::new(true));
    storage.set_rollup_source_read_hook({
        let fail_first_source = Arc::clone(&fail_first_source);
        move |_policy, _series_id| {
            if fail_first_source.swap(false, Ordering::SeqCst) {
                return Err(TsinkError::Other(
                    "injected first-source read failure".to_string(),
                ));
            }
            Ok(())
        }
    });

    let metric = "__tsink_rollup__:bounded-error-progress:cpu_usage";
    let has_materialized = |labels: &[Label]| {
        !storage
            .select(metric, labels, i64::MIN, i64::MAX)
            .unwrap()
            .is_empty()
    };
    let mut cursor = BackgroundRollupCursor::default();

    let err = storage
        .run_background_rollup_pipeline_once(&mut cursor)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("injected first-source read failure"));
    storage.clear_rollup_source_read_hook();
    assert!(!has_materialized(&labels[0]));
    assert!(
        has_materialized(&labels[1]) && has_materialized(&labels[2]),
        "an invalid first source must not starve later sources in the same bounded page"
    );

    // A full exact-multiple page conservatively requires one empty terminal pass before wrap.
    storage
        .run_background_rollup_pipeline_once(&mut cursor)
        .unwrap();
    assert!(!has_materialized(&labels[0]));
    storage
        .run_background_rollup_pipeline_once(&mut cursor)
        .unwrap();
    assert!(has_materialized(&labels[0]));

    storage.close().unwrap();
}

#[test]
fn background_rollup_rejects_oversized_append_sort_source_before_allocating_and_continues_page() {
    let data_dir = TempDir::new().unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.retention_enforced = false;
    options.max_writers = 1;
    options.max_labels_per_series = 1;
    options.max_series_identity_bytes = 64;
    options.maintenance_max_items_per_pass = 2;
    let point_bytes = u64::try_from(std::mem::size_of::<DataPoint>()).unwrap();
    // Sixty-four points fit the derived sample/returned-row ceiling of eighty, but the append-sort
    // working-set model also includes its full output plus one chunk-sized decode scratch buffer.
    // The read must therefore reach the append-sort preflight and fail its memory reservation.
    options.maintenance_max_bytes_per_pass = point_bytes.saturating_mul(80);
    let storage = ChunkStorage::new_with_data_path_and_options(
        256,
        None,
        Some(data_dir.path().join(NUMERIC_LANE_ROOT)),
        Some(data_dir.path().join(BLOB_LANE_ROOT)),
        1,
        options,
    )
    .unwrap();

    let oversized_labels = vec![Label::new("host", "oversized")];
    let small_labels = vec![Label::new("host", "small")];
    storage
        .insert_rows(&[Row::with_labels(
            "cpu_usage",
            oversized_labels.clone(),
            DataPoint::new(1_000, 1.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", small_labels.clone(), DataPoint::new(0, 2.0)),
            Row::with_labels(
                "cpu_usage",
                small_labels.clone(),
                DataPoint::new(1_000, 4.0),
            ),
        ])
        .unwrap();

    let oversized_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("cpu_usage", &oversized_labels)
        .unwrap()
        .series_id;
    let manual_chunk = Chunk {
        header: ChunkHeader {
            series_id: oversized_series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::F64),
            point_count: 64,
            min_ts: 0,
            max_ts: 63,
            ts_codec: TimestampCodecId::DeltaVarint,
            value_codec: ValueCodecId::ConstantRle,
        },
        points: (0..64i64)
            .rev()
            .map(|timestamp| ChunkPoint {
                ts: timestamp,
                value: Value::F64(timestamp as f64),
            })
            .collect(),
        encoded_payload: Vec::new(),
        wal_lowwater: WalHighWatermark::default(),
        wal_highwater: WalHighWatermark::default(),
    };
    let manual_sequence = storage
        .chunks
        .next_chunk_sequence
        .fetch_add(1, Ordering::SeqCst);
    let manual_key = SealedChunkKey::from_chunk(&manual_chunk, manual_sequence);
    storage
        .sealed_shard(oversized_series_id)
        .write()
        .entry(oversized_series_id)
        .or_default()
        .insert(manual_key, Arc::new(manual_chunk));

    let policy = cpu_rollup_policy("bounded-source-read", 1_000);
    let initial = storage.apply_rollup_policies(vec![policy]).unwrap();
    assert!(initial.policies[0]
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("per_query_memory_bytes")));

    let append_sort_before = storage
        .observability_snapshot()
        .query
        .append_sort_path_queries_total;
    let mut cursor = BackgroundRollupCursor::default();
    let error = storage
        .run_background_rollup_pipeline_once(&mut cursor)
        .unwrap_err();
    assert!(matches!(
        error,
        TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
            if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
                && exceeded.limit == point_bytes.saturating_mul(80)
    ));
    assert_eq!(
        storage
            .observability_snapshot()
            .query
            .append_sort_path_queries_total,
        append_sort_before.saturating_add(1),
        "the oversized source must be rejected by append-sort's pre-allocation reservation"
    );

    let rollup_metric = "__tsink_rollup__:bounded-source-read:cpu_usage";
    assert!(storage
        .select(rollup_metric, &oversized_labels, i64::MIN, i64::MAX)
        .unwrap()
        .is_empty());
    assert_eq!(
        storage
            .select(rollup_metric, &small_labels, i64::MIN, i64::MAX)
            .unwrap(),
        vec![DataPoint::new(0, 2.0)],
        "a bounded rejection must remain source-local and not starve the next page member"
    );
    assert_eq!(storage.query_budget_snapshot().active_queries, 0);
    storage
        .sealed_shard(oversized_series_id)
        .write()
        .remove(&oversized_series_id);
    storage.close().unwrap();
}

#[test]
fn background_rollup_source_execution_preserves_instance_scan_limit() {
    let data_dir = TempDir::new().unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.retention_enforced = false;
    options.max_writers = 1;
    options.maintenance_max_items_per_pass = 2;
    let storage = ChunkStorage::new_with_data_path_and_options_and_disk_budget_and_query_budget(
        256,
        None,
        Some(data_dir.path().join(NUMERIC_LANE_ROOT)),
        Some(data_dir.path().join(BLOB_LANE_ROOT)),
        1,
        options,
        None,
        QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_samples_scanned: Some(2),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        },
    )
    .unwrap();

    let oversized_labels = vec![Label::new("host", "oversized-scan")];
    let small_labels = vec![Label::new("host", "small-scan")];
    storage
        .insert_rows(&[
            Row::with_labels(
                "cpu_usage",
                oversized_labels.clone(),
                DataPoint::new(0, 1.0),
            ),
            Row::with_labels(
                "cpu_usage",
                oversized_labels.clone(),
                DataPoint::new(500, 2.0),
            ),
            Row::with_labels(
                "cpu_usage",
                oversized_labels.clone(),
                DataPoint::new(1_000, 3.0),
            ),
        ])
        .unwrap();
    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", small_labels.clone(), DataPoint::new(0, 2.0)),
            Row::with_labels(
                "cpu_usage",
                small_labels.clone(),
                DataPoint::new(1_000, 4.0),
            ),
        ])
        .unwrap();

    let initial = storage
        .apply_rollup_policies(vec![cpu_rollup_policy("bounded-source-scan", 1_000)])
        .unwrap();
    assert!(initial.policies[0]
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("samples_scanned")));

    let mut cursor = BackgroundRollupCursor::default();
    let error = storage
        .run_background_rollup_pipeline_once(&mut cursor)
        .unwrap_err();
    assert!(matches!(
        error,
        TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
            if exceeded.reason == QueryLimitReason::SamplesScanned && exceeded.limit == 2
    ));
    assert_eq!(
        storage
            .select(
                "__tsink_rollup__:bounded-source-scan:cpu_usage",
                &small_labels,
                i64::MIN,
                i64::MAX,
            )
            .unwrap(),
        vec![DataPoint::new(0, 2.0)],
        "the inherited scan rejection must remain source-local"
    );
    assert_eq!(storage.query_budget_snapshot().active_queries, 0);
    storage.close().unwrap();
}

#[test]
fn background_rollup_global_page_failure_retries_the_same_cursor() {
    use std::sync::atomic::AtomicUsize;

    let data_dir = TempDir::new().unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.retention_enforced = false;
    options.max_writers = 1;
    options.maintenance_max_items_per_pass = 1;
    options.maintenance_max_bytes_per_pass = 256 * 1024 * 1024;
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(data_dir.path().join(NUMERIC_LANE_ROOT)),
        Some(data_dir.path().join(BLOB_LANE_ROOT)),
        1,
        options,
    )
    .unwrap();
    storage
        .apply_rollup_policies(vec![cpu_rollup_policy("global-error-retry", 1_000)])
        .unwrap();

    let labels = [vec![Label::new("host", "0")], vec![Label::new("host", "1")]];
    storage
        .insert_rows(
            &labels
                .iter()
                .flat_map(|labels| {
                    [
                        Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
                        Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
                    ]
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();

    let persist_calls = Arc::new(AtomicUsize::new(0));
    storage.set_rollup_state_persist_hook({
        let persist_calls = Arc::clone(&persist_calls);
        move || {
            if persist_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(TsinkError::Other(
                    "injected global rollup state failure".to_string(),
                ));
            }
            Ok(())
        }
    });

    let mut cursor = BackgroundRollupCursor::default();
    let err = storage
        .run_background_rollup_pipeline_once(&mut cursor)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("injected global rollup state failure"));
    storage.clear_rollup_state_persist_hook();

    storage
        .run_background_rollup_pipeline_once(&mut cursor)
        .unwrap();
    let metric = "__tsink_rollup__:global-error-retry:cpu_usage";
    assert!(!storage
        .select(metric, &labels[0], i64::MIN, i64::MAX)
        .unwrap()
        .is_empty());
    assert!(storage
        .select(metric, &labels[1], i64::MIN, i64::MAX)
        .unwrap()
        .is_empty());
    storage.close().unwrap();
}

#[test]
fn zero_source_page_capacity_succeeds_for_empty_policy_and_rejects_real_work() {
    const PAGE_BYTES: u64 = 256 * 1024 * 1024;
    let data_dir = TempDir::new().unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.retention_enforced = false;
    options.max_writers = 1;
    options.max_series_identity_bytes = 64 * 1024 * 1024;
    options.maintenance_max_items_per_pass = 1;
    options.maintenance_max_bytes_per_pass = PAGE_BYTES;
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(data_dir.path().join(NUMERIC_LANE_ROOT)),
        Some(data_dir.path().join(BLOB_LANE_ROOT)),
        1,
        options,
    )
    .unwrap();
    storage
        .apply_rollup_policies(vec![cpu_rollup_policy("zero-capacity", 1_000)])
        .unwrap();

    let mut cursor = BackgroundRollupCursor::default();
    storage
        .run_background_rollup_pipeline_once(&mut cursor)
        .expect("an empty policy has no source identity to admit");

    storage
        .insert_rows(&[
            Row::new("cpu_usage", DataPoint::new(0, 1.0)),
            Row::new("cpu_usage", DataPoint::new(1_000, 3.0)),
        ])
        .unwrap();
    assert!(matches!(
        storage.run_background_rollup_pipeline_once(&mut cursor),
        Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "rollup source traversal",
            limit: PAGE_BYTES,
            ..
        })
    ));
    assert!(matches!(
        storage.trigger_rollup_run(),
        Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "rollup source traversal",
            limit: PAGE_BYTES,
            ..
        })
    ));
    storage.close().unwrap();
}

#[test]
fn manual_rollup_reports_n_minus_one_and_exact_n_page_progress() {
    let data_dir = TempDir::new().unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.retention_enforced = false;
    options.max_writers = 1;
    options.maintenance_max_items_per_pass = 2;
    options.maintenance_max_bytes_per_pass = 256 * 1024 * 1024;
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(data_dir.path().join(NUMERIC_LANE_ROOT)),
        Some(data_dir.path().join(BLOB_LANE_ROOT)),
        1,
        options,
    )
    .unwrap();
    storage
        .apply_rollup_policies(vec![cpu_rollup_policy("manual-page-boundary", 1_000)])
        .unwrap();

    let first_labels = vec![Label::new("host", "0")];
    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", first_labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels(
                "cpu_usage",
                first_labels.clone(),
                DataPoint::new(1_000, 3.0),
            ),
        ])
        .unwrap();
    let n_minus_one = storage.trigger_rollup_run().unwrap();
    assert!(n_minus_one.source_traversal_complete);
    assert_eq!(n_minus_one.continuation_policy_id, None);
    assert_eq!(n_minus_one.policies[0].matched_series, 1);
    assert!(n_minus_one.policies[0].source_traversal_complete);

    let second_labels = vec![Label::new("host", "1")];
    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", second_labels.clone(), DataPoint::new(0, 2.0)),
            Row::with_labels(
                "cpu_usage",
                second_labels.clone(),
                DataPoint::new(1_000, 4.0),
            ),
        ])
        .unwrap();
    let exact_n = storage.trigger_rollup_run().unwrap();
    assert!(
        !exact_n.source_traversal_complete,
        "a full page must not inspect posting N+1 to claim terminal coverage"
    );
    assert_eq!(
        exact_n.continuation_policy_id.as_deref(),
        Some("manual-page-boundary")
    );
    assert!(exact_n.continuation_after_series_id.is_some());
    assert_eq!(exact_n.policies[0].matched_series, 2);
    assert!(!exact_n.policies[0].source_traversal_complete);

    let terminal = storage.trigger_rollup_run().unwrap();
    assert!(terminal.source_traversal_complete);
    assert_eq!(terminal.continuation_policy_id, None);
    assert_eq!(terminal.continuation_after_series_id, None);
    assert_eq!(terminal.policies[0].matched_series, 2);
    assert_eq!(terminal.policies[0].materialized_series, 2);
    assert!(terminal.policies[0].source_traversal_complete);
    storage.close().unwrap();
}

#[test]
fn manual_rollup_global_failure_retries_the_same_cursor_page() {
    let data_dir = TempDir::new().unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.retention_enforced = false;
    options.max_writers = 1;
    options.maintenance_max_items_per_pass = 1;
    options.maintenance_max_bytes_per_pass = 256 * 1024 * 1024;
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(data_dir.path().join(NUMERIC_LANE_ROOT)),
        Some(data_dir.path().join(BLOB_LANE_ROOT)),
        1,
        options,
    )
    .unwrap();
    storage
        .apply_rollup_policies(vec![cpu_rollup_policy("manual-retry", 1_000)])
        .unwrap();

    let labels = [vec![Label::new("host", "0")], vec![Label::new("host", "1")]];
    storage
        .insert_rows(
            &labels
                .iter()
                .flat_map(|labels| {
                    [
                        Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
                        Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
                    ]
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();

    let fail_once = Arc::new(AtomicBool::new(true));
    storage.set_rollup_state_persist_hook({
        let fail_once = Arc::clone(&fail_once);
        move || {
            if fail_once.swap(false, Ordering::SeqCst) {
                return Err(TsinkError::Other(
                    "injected manual rollup state failure".to_string(),
                ));
            }
            Ok(())
        }
    });
    let error = storage.trigger_rollup_run().unwrap_err();
    assert!(error
        .to_string()
        .contains("injected manual rollup state failure"));
    storage.clear_rollup_state_persist_hook();
    let failed_progress = storage.rollup_observability_snapshot();
    assert!(!failed_progress.source_traversal_complete);
    assert_eq!(
        failed_progress.continuation_policy_id.as_deref(),
        Some("manual-retry")
    );
    assert_eq!(failed_progress.continuation_after_series_id, None);

    let retried = storage.trigger_rollup_run().unwrap();
    assert!(!retried.source_traversal_complete);
    assert!(retried.continuation_after_series_id.is_some());
    let metric = "__tsink_rollup__:manual-retry:cpu_usage";
    assert!(!storage
        .select(metric, &labels[0], i64::MIN, i64::MAX)
        .unwrap()
        .is_empty());
    assert!(storage
        .select(metric, &labels[1], i64::MIN, i64::MAX)
        .unwrap()
        .is_empty());

    assert!(
        !storage
            .trigger_rollup_run()
            .unwrap()
            .source_traversal_complete
    );
    assert!(
        storage
            .trigger_rollup_run()
            .unwrap()
            .source_traversal_complete
    );
    storage.close().unwrap();
}

#[test]
fn expert_unlimited_manual_rollup_drains_the_complete_cycle() {
    let data_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(data_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .with_resource_profile(crate::ResourceProfile::ExpertUnlimited)
        .with_background_threads_enabled_for_tests(false)
        .build()
        .unwrap();

    let labels = (0..3)
        .map(|host| vec![Label::new("host", host.to_string())])
        .collect::<Vec<_>>();
    storage
        .insert_rows(
            &labels
                .iter()
                .flat_map(|labels| {
                    [
                        Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
                        Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 3.0)),
                    ]
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();

    let snapshot = storage
        .apply_rollup_policies(vec![
            cpu_rollup_policy("expert-first", 1_000),
            cpu_rollup_policy("expert-second", 1_000),
        ])
        .unwrap();
    assert!(snapshot.source_traversal_complete);
    assert_eq!(snapshot.continuation_policy_id, None);
    assert_eq!(snapshot.continuation_after_series_id, None);
    assert_eq!(snapshot.policies.len(), 2);
    for status in snapshot.policies {
        assert!(status.source_traversal_complete);
        assert_eq!(status.matched_series, 3);
        assert_eq!(status.materialized_series, 3);
    }
    storage.close().unwrap();
}
