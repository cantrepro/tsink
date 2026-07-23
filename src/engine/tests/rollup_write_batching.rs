use super::*;
use crate::{Aggregation, WriteBatchLimits};

fn rollup_storage_with_write_limits(
    root: &std::path::Path,
    limits: WriteBatchLimits,
) -> ChunkStorage {
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.retention_enforced = false;
    options.max_writers = 1;
    options.write_batch_limits = limits;
    options.background_threads_enabled = false;

    ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(root.join(NUMERIC_LANE_ROOT)),
        Some(root.join(BLOB_LANE_ROOT)),
        1,
        options,
    )
    .unwrap()
}

fn long_history_policy(id: &str) -> crate::storage::RollupPolicy {
    crate::storage::RollupPolicy {
        id: id.to_string(),
        metric: "long_history".to_string(),
        match_labels: Vec::new(),
        interval: 1_000,
        aggregation: Aggregation::Avg,
        bucket_origin: 0,
    }
}

fn seed_long_history(storage: &ChunkStorage, labels: &[Label]) {
    for (index, timestamp) in [0, 1_000, 2_000, 3_000, 4_000, 5_000]
        .into_iter()
        .enumerate()
    {
        storage
            .insert_rows(&[Row::with_labels(
                "long_history",
                labels.to_vec(),
                DataPoint::new(timestamp, (index + 1) as f64),
            )])
            .unwrap();
    }
}

fn expected_materialized_rows(metric: &str, labels: &[Label]) -> Vec<Row> {
    [0, 1_000, 2_000, 3_000, 4_000]
        .into_iter()
        .enumerate()
        .map(|(index, timestamp)| {
            Row::with_labels(
                metric,
                labels.to_vec(),
                DataPoint::new(timestamp, (index + 1) as f64),
            )
        })
        .collect()
}

fn assert_long_history_materialized(
    storage: &ChunkStorage,
    policy: crate::storage::RollupPolicy,
    labels: &[Label],
) {
    seed_long_history(storage, labels);

    let snapshot = storage.apply_rollup_policies(vec![policy.clone()]).unwrap();
    assert_eq!(snapshot.policies.len(), 1);
    assert_eq!(snapshot.policies[0].materialized_series, 1);
    assert_eq!(snapshot.policies[0].materialized_through, Some(5_000));

    let rollup_metric = format!("__tsink_rollup__:{}:long_history", policy.id);
    assert_eq!(
        storage
            .select(&rollup_metric, labels, i64::MIN, i64::MAX)
            .unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 2.0),
            DataPoint::new(2_000, 3.0),
            DataPoint::new(3_000, 4.0),
            DataPoint::new(4_000, 5.0),
        ]
    );
}

#[test]
fn rollup_materialization_chunks_output_at_the_write_batch_row_limit() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "row-bounded")];
    let policy = long_history_policy("row-bounded");
    let rollup_metric = "__tsink_rollup__:row-bounded:long_history";
    let rows = expected_materialized_rows(rollup_metric, &labels);
    let limits = WriteBatchLimits {
        max_rows: Some(2),
        max_modeled_input_bytes: Some(usize::MAX),
    };
    assert!(rows.len() > limits.max_rows.unwrap());

    let storage = rollup_storage_with_write_limits(temp_dir.path(), limits);
    assert_long_history_materialized(&storage, policy, &labels);
    storage.close().unwrap();
}

#[test]
fn rollup_materialization_chunks_output_at_the_modeled_byte_limit() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "byte-bounded")];
    let policy = long_history_policy("byte-bounded");
    let rollup_metric = "__tsink_rollup__:byte-bounded:long_history";
    let rows = expected_materialized_rows(rollup_metric, &labels);
    let two_row_bytes = crate::modeled_write_batch_input_bytes(&rows[..2]).unwrap();
    let limits = WriteBatchLimits {
        max_rows: Some(3),
        max_modeled_input_bytes: Some(two_row_bytes),
    };
    assert!(rows.len() > limits.max_rows.unwrap());
    assert!(crate::modeled_write_batch_input_bytes(&rows).unwrap() > two_row_bytes);
    assert_eq!(
        crate::modeled_write_batch_input_bytes(&rows[..2]).unwrap(),
        two_row_bytes
    );
    assert!(crate::modeled_write_batch_input_bytes(&rows[..3]).unwrap() > two_row_bytes);

    let storage = rollup_storage_with_write_limits(temp_dir.path(), limits);
    assert_long_history_materialized(&storage, policy, &labels);
    storage.close().unwrap();
}
