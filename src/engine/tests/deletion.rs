use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::engine::tombstone::{
    referenced_tombstone_shard_files, tombstone_store_sidecar_path, TombstoneRange,
    TOMBSTONES_FILE_NAME,
};
use crate::label::stable_series_identity_hash;
use crate::storage::MetadataShardScope;
use crate::{Aggregation, QueryOptions};

fn labels_for_shard(metric: &str, shard_count: u32, target_shard: u32, prefix: &str) -> Vec<Label> {
    (0..4096u32)
        .map(|idx| vec![Label::new("host", format!("{prefix}-{idx}"))])
        .find(|labels| {
            (stable_series_identity_hash(metric, labels) % u64::from(shard_count)) as u32
                == target_shard
        })
        .unwrap()
}

fn clear_persisted_segments_but_keep_tombstones(data_path: &Path) {
    for lane_root in [NUMERIC_LANE_ROOT, BLOB_LANE_ROOT] {
        let lane_path = data_path.join(lane_root);
        let tombstone_store_name =
            tombstone_store_sidecar_path(&lane_path.join(TOMBSTONES_FILE_NAME))
                .file_name()
                .unwrap()
                .to_os_string();
        let Ok(entries) = std::fs::read_dir(&lane_path) else {
            continue;
        };
        for entry in entries {
            let entry = entry.unwrap();
            if entry.file_name() == std::ffi::OsStr::new(TOMBSTONES_FILE_NAME)
                || entry.file_name() == tombstone_store_name
            {
                continue;
            }
            let path = entry.path();
            if path.is_dir() {
                std::fs::remove_dir_all(path).unwrap();
            } else {
                std::fs::remove_file(path).unwrap();
            }
        }
    }
}

fn read_rollup_state_file(data_path: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(data_path.join(".rollups").join("state.json")).unwrap())
        .unwrap()
}

fn current_unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn retention_storage_at_time(root: Option<&Path>, now: i64, retention_window: i64) -> ChunkStorage {
    let mut options = super::base_storage_test_options(TimestampPrecision::Seconds, Some(now));
    options.retention_window = retention_window;
    ChunkStorage::new_with_data_path_and_options(
        1,
        None,
        root.map(|path| path.join(NUMERIC_LANE_ROOT)),
        root.map(|path| path.join(BLOB_LANE_ROOT)),
        1,
        options,
    )
    .unwrap()
}

#[test]
fn delete_series_tombstones_selected_time_range() {
    let storage = builder_at_time(3)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();

    let selection = SeriesSelection::new()
        .with_metric("cpu_usage")
        .with_matcher(SeriesMatcher::equal("host", "a"))
        .with_time_range(2, 4);
    let result = storage
        .delete_series(&selection)
        .expect("delete_series should succeed");
    assert_eq!(result.matched_series, 1);
    assert_eq!(result.tombstones_applied, 1);

    let points = storage.select("cpu_usage", &labels, 0, 10).unwrap();
    assert_eq!(points, vec![DataPoint::new(1, 1.0)]);
}

#[test]
fn delete_series_without_time_range_tombstones_all_matching_series() {
    let storage = builder_at_time(1)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();
    let labels_a = vec![Label::new("host", "a")];
    let labels_b = vec![Label::new("host", "b")];

    storage
        .insert_rows(&[
            Row::with_labels(
                "http_requests_total",
                labels_a.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "http_requests_total",
                labels_b.clone(),
                DataPoint::new(1, 2.0),
            ),
            Row::with_labels("other_metric", labels_a.clone(), DataPoint::new(1, 3.0)),
        ])
        .unwrap();

    let selection = SeriesSelection::new().with_metric("http_requests_total");
    let result = storage
        .delete_series(&selection)
        .expect("delete_series should succeed");
    assert_eq!(result.matched_series, 2);

    assert!(storage
        .select("http_requests_total", &labels_a, 0, 10)
        .unwrap()
        .is_empty());
    assert!(storage
        .select("http_requests_total", &labels_b, 0, 10)
        .unwrap()
        .is_empty());
    assert_eq!(
        storage.select("other_metric", &labels_a, 0, 10).unwrap(),
        vec![DataPoint::new(1, 3.0)]
    );
}

#[test]
fn delete_series_reports_zero_tombstones_applied_for_idempotent_matches() {
    let storage = retention_storage_at_time(None, 1, i64::MAX);
    let labels = vec![Label::new("host", "a")];

    storage
        .insert_rows(&[Row::with_labels(
            "idempotent_delete_metric",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("idempotent_delete_metric", &labels)
        .unwrap()
        .series_id;
    storage
        .refresh_series_visible_timestamp_cache(std::iter::once(series_id))
        .unwrap();

    // Seed an already-covered tombstone without republishing visibility so the selector still
    // matches and the result accounting has to distinguish matches from applied changes.
    storage.visibility.tombstones.write().insert(
        series_id,
        vec![TombstoneRange {
            start: i64::MIN,
            end: i64::MAX,
        }],
    );

    let result = storage
        .delete_series(&SeriesSelection::new().with_metric("idempotent_delete_metric"))
        .unwrap();
    assert_eq!(result.matched_series, 1);
    assert_eq!(result.tombstones_applied, 0);
}

#[test]
fn delete_series_reports_only_changed_tombstones_for_mixed_matches() {
    let storage = retention_storage_at_time(None, 1, i64::MAX);
    let labels_a = vec![Label::new("host", "a")];
    let labels_b = vec![Label::new("host", "b")];

    storage
        .insert_rows(&[
            Row::with_labels(
                "mixed_delete_metric",
                labels_a.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "mixed_delete_metric",
                labels_b.clone(),
                DataPoint::new(1, 2.0),
            ),
        ])
        .unwrap();

    let series_id_a = storage
        .catalog
        .registry
        .read()
        .resolve_existing("mixed_delete_metric", &labels_a)
        .unwrap()
        .series_id;
    let series_id_b = storage
        .catalog
        .registry
        .read()
        .resolve_existing("mixed_delete_metric", &labels_b)
        .unwrap()
        .series_id;
    storage
        .refresh_series_visible_timestamp_cache([series_id_a, series_id_b])
        .unwrap();

    storage.visibility.tombstones.write().insert(
        series_id_a,
        vec![TombstoneRange {
            start: i64::MIN,
            end: i64::MAX,
        }],
    );

    let result = storage
        .delete_series(&SeriesSelection::new().with_metric("mixed_delete_metric"))
        .unwrap();
    assert_eq!(result.matched_series, 2);
    assert_eq!(result.tombstones_applied, 1);
}

#[test]
fn deleting_newest_bounded_sample_repairs_recency_reference() {
    let now = current_unix_seconds();
    let older = now - 16;
    let current = now;
    let future = now + 30 * 24 * 3600;
    let storage = retention_storage_at_time(None, older, 15);

    storage.set_current_time_override(older);
    storage
        .insert_rows(&[Row::new(
            "delete_recency_metric",
            DataPoint::new(older, 1.0),
        )])
        .unwrap();
    storage.set_current_time_override(current);
    storage
        .insert_rows(&[Row::new(
            "delete_recency_metric",
            DataPoint::new(current, 2.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::new(
            "delete_recency_metric",
            DataPoint::new(future, 3.0),
        )])
        .unwrap();

    assert_eq!(
        storage
            .select("delete_recency_metric", &[], older - 1, future + 1)
            .unwrap(),
        vec![DataPoint::new(current, 2.0), DataPoint::new(future, 3.0)]
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .retention
            .recency_reference_timestamp,
        Some(now)
    );

    storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("delete_recency_metric")
                .with_time_range(current, current + 1),
        )
        .unwrap();

    assert_eq!(
        storage
            .select("delete_recency_metric", &[], older - 1, future + 1)
            .unwrap(),
        vec![DataPoint::new(future, 3.0)]
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .retention
            .recency_reference_timestamp,
        Some(now)
    );
}

#[test]
fn delete_series_tombstones_persist_across_restart() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let data_path = temp_dir.path().join("data");

    {
        let storage = builder_at_time(2)
            .with_data_path(&data_path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("restart_metric", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("restart_metric", labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();

        storage
            .delete_series(
                &SeriesSelection::new()
                    .with_metric("restart_metric")
                    .with_matcher(SeriesMatcher::equal("host", "a"))
                    .with_time_range(1, 2),
            )
            .unwrap();
        storage.close().unwrap();
    }

    let numeric_tombstones_path = data_path.join(NUMERIC_LANE_ROOT).join(TOMBSTONES_FILE_NAME);
    let blob_tombstones_path = data_path.join(BLOB_LANE_ROOT).join(TOMBSTONES_FILE_NAME);
    assert!(numeric_tombstones_path.exists());
    assert!(blob_tombstones_path.exists());
    let reopened = builder_at_time(2)
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();
    let points = reopened.select("restart_metric", &labels, 0, 10).unwrap();
    assert_eq!(points, vec![DataPoint::new(2, 2.0)]);
    reopened.close().unwrap();
}

#[test]
fn repeated_deletes_rewrite_only_changed_tombstone_shards_and_survive_restart() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let labels_a = vec![Label::new("host", "a")];
    let labels_b = vec![Label::new("host", "b")];

    {
        let storage = builder_at_time(4)
            .with_data_path(&data_path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels(
                    "repeat_delete_metric",
                    labels_a.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "repeat_delete_metric",
                    labels_a.clone(),
                    DataPoint::new(2, 2.0),
                ),
                Row::with_labels(
                    "repeat_delete_metric",
                    labels_b.clone(),
                    DataPoint::new(1, 3.0),
                ),
                Row::with_labels(
                    "repeat_delete_metric",
                    labels_b.clone(),
                    DataPoint::new(2, 4.0),
                ),
            ])
            .unwrap();

        storage
            .delete_series(
                &SeriesSelection::new()
                    .with_metric("repeat_delete_metric")
                    .with_matcher(SeriesMatcher::equal("host", "a"))
                    .with_time_range(0, 3),
            )
            .unwrap();

        let tombstones_path = data_path.join(NUMERIC_LANE_ROOT).join(TOMBSTONES_FILE_NAME);
        let shard_files_after_first = referenced_tombstone_shard_files(&tombstones_path).unwrap();
        let first_touched_shard = shard_files_after_first
            .iter()
            .position(Option::is_some)
            .expect("first delete should materialize exactly one shard");
        assert_eq!(
            shard_files_after_first.iter().flatten().count(),
            1,
            "first delete should only materialize one shard"
        );

        storage
            .delete_series(
                &SeriesSelection::new()
                    .with_metric("repeat_delete_metric")
                    .with_matcher(SeriesMatcher::equal("host", "b"))
                    .with_time_range(2, 3),
            )
            .unwrap();

        let shard_files_after_second = referenced_tombstone_shard_files(&tombstones_path).unwrap();
        let second_touched_shard = shard_files_after_second
            .iter()
            .enumerate()
            .find_map(|(idx, file_name)| {
                (file_name.is_some() && shard_files_after_first[idx].is_none()).then_some(idx)
            })
            .expect("second delete should only touch one additional shard");
        assert_eq!(
            shard_files_after_second.iter().flatten().count(),
            2,
            "second delete should only add the newly touched shard"
        );
        assert_eq!(
            shard_files_after_second[first_touched_shard],
            shard_files_after_first[first_touched_shard],
            "untouched shard should keep the same file"
        );
        assert_ne!(
            shard_files_after_second[second_touched_shard],
            shard_files_after_first[second_touched_shard],
            "newly touched shard should get a new file"
        );

        let repeated = storage
            .delete_series(
                &SeriesSelection::new()
                    .with_metric("repeat_delete_metric")
                    .with_matcher(SeriesMatcher::equal("host", "b"))
                    .with_time_range(2, 3),
            )
            .unwrap();
        assert_eq!(repeated.matched_series, 0);
        assert_eq!(repeated.tombstones_applied, 0);
        assert_eq!(
            referenced_tombstone_shard_files(&tombstones_path).unwrap(),
            shard_files_after_second,
            "repeating an already-covered delete should not rewrite shard files"
        );
        storage.close().unwrap();
    }

    let reopened = builder_at_time(4)
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();
    assert!(reopened
        .select("repeat_delete_metric", &labels_a, 0, 10)
        .unwrap()
        .is_empty());
    assert_eq!(
        reopened
            .select("repeat_delete_metric", &labels_b, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 3.0)]
    );
    reopened.close().unwrap();
}

#[test]
fn delete_series_persists_object_store_tombstones_for_compute_only_restart() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let expected_series = MetricSeries {
        name: "remote_delete_metric".to_string(),
        labels: labels.clone(),
    };

    {
        let storage = builder_at_time(3)
            .with_data_path(data_dir.path())
            .with_object_store_path(object_store_dir.path())
            .with_mirror_hot_segments_to_object_store(true)
            .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
            .with_retention(Duration::from_secs(100))
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_wal_enabled(false)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels(
                    "remote_delete_metric",
                    labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "remote_delete_metric",
                    labels.clone(),
                    DataPoint::new(2, 2.0),
                ),
                Row::with_labels(
                    "remote_delete_metric",
                    labels.clone(),
                    DataPoint::new(3, 3.0),
                ),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    {
        let storage = builder_at_time(3)
            .with_data_path(data_dir.path())
            .with_object_store_path(object_store_dir.path())
            .with_mirror_hot_segments_to_object_store(true)
            .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
            .with_retention(Duration::from_secs(100))
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_wal_enabled(false)
            .build()
            .unwrap();

        storage
            .delete_series(
                &SeriesSelection::new()
                    .with_metric("remote_delete_metric")
                    .with_matcher(SeriesMatcher::equal("host", "a"))
                    .with_time_range(2, 4),
            )
            .unwrap();

        assert!(object_store_dir
            .path()
            .join("hot")
            .join(NUMERIC_LANE_ROOT)
            .join(TOMBSTONES_FILE_NAME)
            .exists());

        storage.close().unwrap();
    }

    for _ in 0..2 {
        let storage = builder_at_time(3)
            .with_object_store_path(object_store_dir.path())
            .with_runtime_mode(StorageRuntimeMode::ComputeOnly)
            .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
            .with_retention(Duration::from_secs(100))
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_wal_enabled(false)
            .build()
            .unwrap();

        assert_eq!(
            storage
                .select("remote_delete_metric", &labels, 0, 10)
                .unwrap(),
            vec![DataPoint::new(1, 1.0)]
        );
        assert_eq!(
            storage.list_metrics().unwrap(),
            vec![expected_series.clone()]
        );
        assert_eq!(
            storage
                .select_series(
                    &SeriesSelection::new()
                        .with_metric("remote_delete_metric")
                        .with_time_range(0, 2),
                )
                .unwrap(),
            vec![expected_series.clone()]
        );
        assert!(storage
            .select_series(
                &SeriesSelection::new()
                    .with_metric("remote_delete_metric")
                    .with_time_range(2, 10),
            )
            .unwrap()
            .is_empty());
        storage.close().unwrap();
    }
}

#[test]
fn restart_without_registry_snapshot_keeps_tombstoned_series_ids_reserved() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let deleted_labels = vec![Label::new("host", "deleted")];
    let replacement_labels = vec![Label::new("host", "replacement")];

    {
        let storage = builder_at_time(10)
            .with_data_path(&data_path)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_wal_enabled(false)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::with_labels(
                "deleted_identity_metric",
                deleted_labels.clone(),
                DataPoint::new(10, 1.0),
            )])
            .unwrap();
        storage
            .delete_series(
                &SeriesSelection::new()
                    .with_metric("deleted_identity_metric")
                    .with_matcher(SeriesMatcher::equal("host", "deleted")),
            )
            .unwrap();
        storage.close().unwrap();
    }

    let numeric_tombstones_path = data_path.join(NUMERIC_LANE_ROOT).join(TOMBSTONES_FILE_NAME);
    assert!(numeric_tombstones_path.exists());
    clear_persisted_segments_but_keep_tombstones(&data_path);
    let checkpoint_path = data_path.join(SERIES_INDEX_FILE_NAME);
    let delta_path = SeriesRegistry::incremental_path(&checkpoint_path);
    let delta_dir = SeriesRegistry::incremental_dir(&checkpoint_path);
    let _ = std::fs::remove_file(&checkpoint_path);
    let _ = std::fs::remove_file(&delta_path);
    let _ = std::fs::remove_dir_all(&delta_dir);

    let reopened = builder_at_time(11)
        .with_data_path(&data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();
    reopened
        .insert_rows(&[Row::with_labels(
            "replacement_identity_metric",
            replacement_labels.clone(),
            DataPoint::new(11, 2.0),
        )])
        .unwrap();

    assert_eq!(
        reopened
            .select("replacement_identity_metric", &replacement_labels, 0, 20)
            .unwrap(),
        vec![DataPoint::new(11, 2.0)]
    );
    reopened.close().unwrap();
}

#[test]
fn compute_only_delete_series_is_rejected_without_mutating_remote_state() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let expected_points = vec![
        DataPoint::new(1, 1.0),
        DataPoint::new(2, 2.0),
        DataPoint::new(3, 3.0),
    ];

    {
        let storage = builder_at_time(3)
            .with_data_path(data_dir.path())
            .with_object_store_path(object_store_dir.path())
            .with_mirror_hot_segments_to_object_store(true)
            .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
            .with_retention(Duration::from_secs(100))
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_wal_enabled(false)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels(
                    "remote_delete_metric",
                    labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "remote_delete_metric",
                    labels.clone(),
                    DataPoint::new(2, 2.0),
                ),
                Row::with_labels(
                    "remote_delete_metric",
                    labels.clone(),
                    DataPoint::new(3, 3.0),
                ),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let storage = builder_at_time(3)
        .with_object_store_path(object_store_dir.path())
        .with_runtime_mode(StorageRuntimeMode::ComputeOnly)
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        storage
            .select("remote_delete_metric", &labels, 0, 10)
            .unwrap(),
        expected_points
    );

    let err = storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("remote_delete_metric")
                .with_matcher(SeriesMatcher::equal("host", "a"))
                .with_time_range(2, 4),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::UnsupportedOperation {
            operation: "delete_series",
            reason,
        } if reason.contains("compute-only storage mode")
    ));
    assert_eq!(
        storage
            .select("remote_delete_metric", &labels, 0, 10)
            .unwrap(),
        expected_points
    );
    storage.close().unwrap();

    let reopened = builder_at_time(3)
        .with_object_store_path(object_store_dir.path())
        .with_runtime_mode(StorageRuntimeMode::ComputeOnly)
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();
    assert_eq!(
        reopened
            .select("remote_delete_metric", &labels, 0, 10)
            .unwrap(),
        expected_points
    );
    reopened.close().unwrap();
}

#[test]
fn deleted_bounded_anchor_rebuilds_recency_across_restart() {
    let temp_dir = TempDir::new().unwrap();
    let now = current_unix_seconds();
    let older = now - 16;
    let current = now;
    let future = now + 30 * 24 * 3600;

    {
        let storage = retention_storage_at_time(Some(temp_dir.path()), older, 15);

        storage.set_current_time_override(older);
        storage
            .insert_rows(&[Row::new(
                "restart_delete_recency_metric",
                DataPoint::new(older, 1.0),
            )])
            .unwrap();
        storage.set_current_time_override(current);
        storage
            .insert_rows(&[Row::new(
                "restart_delete_recency_metric",
                DataPoint::new(current, 2.0),
            )])
            .unwrap();
        storage
            .insert_rows(&[Row::new(
                "restart_delete_recency_metric",
                DataPoint::new(future, 3.0),
            )])
            .unwrap();
        storage
            .delete_series(
                &SeriesSelection::new()
                    .with_metric("restart_delete_recency_metric")
                    .with_time_range(current, current + 1),
            )
            .unwrap();

        assert_eq!(
            storage
                .observability_snapshot()
                .retention
                .recency_reference_timestamp,
            Some(current)
        );
        storage.close().unwrap();
    }

    let reopened = builder_at_time(current)
        .with_data_path(temp_dir.path())
        .with_retention(Duration::from_secs(15))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();

    assert_eq!(
        reopened
            .select("restart_delete_recency_metric", &[], older - 1, future + 1)
            .unwrap(),
        vec![DataPoint::new(future, 3.0)]
    );
    assert_eq!(
        reopened
            .observability_snapshot()
            .retention
            .recency_reference_timestamp,
        Some(current)
    );
    reopened.close().unwrap();
}

#[test]
fn metadata_queries_reuse_cached_tombstone_liveness_without_persisted_payloads() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let expected_series = MetricSeries {
        name: "cpu_usage".to_string(),
        labels: labels.clone(),
    };
    let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Milliseconds, 1);

    storage
        .insert_rows(&[
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();
    storage.flush_all_active().unwrap();
    assert!(storage.persist_segment_with_outcome().unwrap().persisted);

    storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("cpu_usage")
                .with_matcher(SeriesMatcher::equal("host", "a"))
                .with_time_range(2, 4),
        )
        .unwrap();

    for shard in &storage.chunks.sealed_chunks {
        shard.write().clear();
    }
    storage
        .persisted
        .persisted_index
        .write()
        .segment_maps
        .clear();

    assert_eq!(
        storage.list_metrics().unwrap(),
        vec![expected_series.clone()]
    );
    assert_eq!(
        storage
            .select_series(&SeriesSelection::new().with_metric("cpu_usage"))
            .unwrap(),
        vec![expected_series],
    );
}

#[test]
fn metadata_queries_rebuild_tombstone_liveness_cache_across_restart() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let expected_series = MetricSeries {
        name: "cpu_usage".to_string(),
        labels: labels.clone(),
    };

    {
        let storage =
            persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Milliseconds, 1);

        storage
            .insert_rows(&[
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(3, 3.0)),
            ])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);

        storage
            .delete_series(
                &SeriesSelection::new()
                    .with_metric("cpu_usage")
                    .with_matcher(SeriesMatcher::equal("host", "a"))
                    .with_time_range(2, 4),
            )
            .unwrap();
        storage.close().unwrap();
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Milliseconds, 1);
    for shard in &reopened.chunks.sealed_chunks {
        shard.write().clear();
    }
    reopened
        .persisted
        .persisted_index
        .write()
        .segment_maps
        .clear();

    assert_eq!(
        reopened.list_metrics().unwrap(),
        vec![expected_series.clone()]
    );
    assert_eq!(
        reopened
            .select_series(&SeriesSelection::new().with_metric("cpu_usage"))
            .unwrap(),
        vec![expected_series],
    );
    reopened.close().unwrap();
}

#[test]
fn delete_series_blocks_point_queries_until_visibility_refresh_commits() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Milliseconds,
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );
    let labels = vec![Label::new("host", "a")];
    let expected_series = MetricSeries {
        name: "visibility_delete_metric".to_string(),
        labels: labels.clone(),
    };

    storage
        .insert_rows(&[
            Row::with_labels(
                "visibility_delete_metric",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "visibility_delete_metric",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
            Row::with_labels(
                "visibility_delete_metric",
                labels.clone(),
                DataPoint::new(3, 3.0),
            ),
        ])
        .unwrap();

    let (delete_entered_tx, delete_entered_rx) = mpsc::channel();
    let (delete_release_tx, delete_release_rx) = mpsc::channel();
    let delete_release_rx = Arc::new(Mutex::new(delete_release_rx));
    storage.set_tombstone_post_swap_pre_visibility_hook(move || {
        delete_entered_tx.send(()).unwrap();
        delete_release_rx.lock().unwrap().recv().unwrap();
    });

    let delete_storage = Arc::clone(&storage);
    let delete_thread = thread::spawn(move || {
        delete_storage
            .delete_series(&SeriesSelection::new().with_metric("visibility_delete_metric"))
    });

    delete_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("delete did not reach the tombstone publication hook");

    let point_query_storage = Arc::clone(&storage);
    let point_query_labels = labels.clone();
    let (point_query_tx, point_query_rx) = mpsc::channel();
    let point_query = thread::spawn(move || {
        let result =
            point_query_storage.select("visibility_delete_metric", &point_query_labels, 0, 10);
        point_query_tx.send(result).unwrap();
    });

    let point_query_blocked = point_query_rx
        .recv_timeout(Duration::from_millis(200))
        .is_err();
    assert_eq!(
        storage
            .select_series(&SeriesSelection::new().with_metric("visibility_delete_metric"))
            .unwrap(),
        vec![expected_series],
        "metadata should keep serving the last published visibility snapshot until delete commits",
    );

    delete_release_tx.send(()).unwrap();
    assert!(
        point_query_blocked,
        "point query should wait for tombstone visibility publication",
    );

    let delete_result = delete_thread.join().unwrap().unwrap();
    assert_eq!(delete_result.matched_series, 1);
    assert_eq!(delete_result.tombstones_applied, 1);
    assert_eq!(
        point_query_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap(),
        Vec::<DataPoint>::new(),
    );
    point_query.join().unwrap();

    assert!(storage
        .select_series(&SeriesSelection::new().with_metric("visibility_delete_metric"))
        .unwrap()
        .is_empty());
    storage.clear_tombstone_post_swap_pre_visibility_hook();
}

#[test]
fn shard_scoped_metadata_keeps_last_visibility_snapshot_until_delete_commits() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let shard_count = 8;
    let target_shard = 3;
    let labels = labels_for_shard("cpu_usage", shard_count, target_shard, "blocked");
    let expected_series = MetricSeries {
        name: "cpu_usage".to_string(),
        labels: labels.clone(),
    };
    let scope = MetadataShardScope::new(shard_count, vec![target_shard]);
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Milliseconds,
                retention_enforced: false,
                background_threads_enabled: false,
                metadata_shard_count: Some(shard_count),
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );

    storage
        .insert_rows(&[Row::with_labels(
            "cpu_usage",
            labels.clone(),
            DataPoint::new(1_000, 1.0),
        )])
        .unwrap();

    let (delete_entered_tx, delete_entered_rx) = mpsc::channel();
    let (delete_release_tx, delete_release_rx) = mpsc::channel();
    let delete_release_rx = Arc::new(Mutex::new(delete_release_rx));
    storage.set_tombstone_post_swap_pre_visibility_hook(move || {
        delete_entered_tx.send(()).unwrap();
        delete_release_rx.lock().unwrap().recv().unwrap();
    });

    let delete_storage = Arc::clone(&storage);
    let delete_thread = thread::spawn(move || {
        delete_storage.delete_series(
            &SeriesSelection::new()
                .with_metric("cpu_usage")
                .with_matcher(SeriesMatcher::equal("host", labels[0].value.clone())),
        )
    });

    delete_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("delete did not reach the tombstone publication hook");

    assert_eq!(
        storage.list_metrics_in_shards(&scope).unwrap(),
        vec![expected_series.clone()],
        "shard-scoped metadata should keep serving the last published snapshot until delete commits",
    );
    assert_eq!(
        storage
            .select_series_in_shards(&SeriesSelection::new().with_metric("cpu_usage"), &scope)
            .unwrap(),
        vec![expected_series],
        "shard-scoped series selection should stay on the last published snapshot until delete commits",
    );

    delete_release_tx.send(()).unwrap();

    let delete_result = delete_thread.join().unwrap().unwrap();
    assert_eq!(delete_result.matched_series, 1);
    assert_eq!(delete_result.tombstones_applied, 1);
    assert!(storage.list_metrics_in_shards(&scope).unwrap().is_empty());
    assert!(storage
        .select_series_in_shards(&SeriesSelection::new().with_metric("cpu_usage"), &scope)
        .unwrap()
        .is_empty());
    storage.clear_tombstone_post_swap_pre_visibility_hook();
}

#[test]
fn delete_series_blocks_rollup_queries_until_delete_visibility_commits() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

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

    let (delete_entered_tx, delete_entered_rx) = mpsc::channel();
    let (delete_release_tx, delete_release_rx) = mpsc::channel();
    let delete_release_rx = Arc::new(Mutex::new(delete_release_rx));
    storage.set_tombstone_post_swap_pre_visibility_hook(move || {
        delete_entered_tx.send(()).unwrap();
        delete_release_rx.lock().unwrap().recv().unwrap();
    });

    let delete_storage = Arc::clone(&storage);
    let delete_thread = thread::spawn(move || {
        delete_storage.delete_series(
            &SeriesSelection::new()
                .with_metric("cpu_usage")
                .with_matcher(SeriesMatcher::equal("host", "a"))
                .with_time_range(1_000, 2_000),
        )
    });

    delete_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("delete did not reach the tombstone publication hook");

    let query_storage = Arc::clone(&storage);
    let query_clone = query.clone();
    let (query_tx, query_rx) = mpsc::channel();
    let query_thread = thread::spawn(move || {
        let result = query_storage.select_with_options("cpu_usage", query_clone);
        query_tx.send(result).unwrap();
    });

    assert!(
        query_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "rollup-backed query should wait for delete visibility publication",
    );

    delete_release_tx.send(()).unwrap();

    let delete_result = delete_thread.join().unwrap().unwrap();
    assert_eq!(delete_result.matched_series, 1);
    assert_eq!(delete_result.tombstones_applied, 1);
    assert_eq!(
        query_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 9.0),
        ]
    );
    query_thread.join().unwrap();

    assert_eq!(
        storage
            .observability_snapshot()
            .query
            .rollup_query_plans_total,
        1
    );
    storage.clear_tombstone_post_swap_pre_visibility_hook();
}

#[test]
fn full_series_delete_removes_rollup_backed_results() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = builder_at_time(3_000)
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

    let query = QueryOptions::new(0, 4_000)
        .with_labels(labels.clone())
        .with_downsample(2_000, Aggregation::Avg);
    assert_eq!(
        storage
            .select_with_options("cpu_usage", query.clone())
            .unwrap(),
        vec![DataPoint::new(0, 2.0), DataPoint::new(2_000, 6.0)]
    );

    storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("cpu_usage")
                .with_matcher(SeriesMatcher::equal("host", "a")),
        )
        .unwrap();

    assert!(storage
        .select_with_options("cpu_usage", query.clone())
        .unwrap()
        .is_empty());

    storage.trigger_rollup_run().unwrap();
    assert!(storage
        .select_with_options("cpu_usage", query)
        .unwrap()
        .is_empty());
    storage.close().unwrap();
}

#[test]
fn delete_series_repairs_pending_rollup_invalidation_after_restart() {
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    let persist_calls = Arc::new(AtomicUsize::new(0));
    storage.set_rollup_state_persist_hook({
        let persist_calls = Arc::clone(&persist_calls);
        move || {
            let call = persist_calls.fetch_add(1, Ordering::SeqCst);
            if call == 1 {
                return Err(TsinkError::Other(
                    "injected post-tombstone rollup state persist failure".to_string(),
                ));
            }
            Ok(())
        }
    });

    let result = storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("cpu_usage")
                .with_matcher(SeriesMatcher::equal("host", "a"))
                .with_time_range(1_000, 2_000),
        )
        .unwrap();
    storage.clear_rollup_state_persist_hook();

    assert_eq!(result.matched_series, 1);
    assert_eq!(result.tombstones_applied, 1);
    assert_eq!(persist_calls.load(Ordering::SeqCst), 2);
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

    let pending_state = read_rollup_state_file(temp_dir.path());
    assert_eq!(
        pending_state["pending_delete_invalidations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(pending_state["checkpoints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry["policy_id"] == "cpu_2s_avg"));

    storage.close().unwrap();

    let reopened = builder_at_time(4_000)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    assert_eq!(
        reopened
            .select_with_options("cpu_usage", query.clone())
            .unwrap(),
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(2_000, 6.0),
            DataPoint::new(4_000, 9.0),
        ]
    );
    assert_eq!(
        reopened
            .observability_snapshot()
            .query
            .rollup_query_plans_total,
        0
    );
    assert_eq!(reopened.observability_snapshot().rollups.policies.len(), 1);
    assert_eq!(
        reopened.observability_snapshot().rollups.policies[0].materialized_series,
        0
    );
    assert_eq!(
        reopened.observability_snapshot().rollups.policies[0].materialized_through,
        None
    );

    let repaired_state = read_rollup_state_file(temp_dir.path());
    assert!(repaired_state["pending_delete_invalidations"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(repaired_state["checkpoints"].as_array().unwrap().is_empty());

    reopened.close().unwrap();
}

#[test]
fn delete_series_reconciles_metadata_and_rollup_sources_before_and_after_restart() {
    let temp_dir = TempDir::new().unwrap();
    let shard_count = 8;
    let target_shard = 3;
    let deleted_labels = labels_for_shard("cpu_usage", shard_count, target_shard, "deleted");
    let retained_labels = labels_for_shard("memory_usage", shard_count, target_shard, "retained");
    let scope = MetadataShardScope::new(shard_count, vec![target_shard]);

    {
        let storage = builder_at_time(1_000)
            .with_data_path(temp_dir.path())
            .with_metadata_shard_count(shard_count)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("cpu_usage", deleted_labels.clone(), DataPoint::new(0, 1.0)),
                Row::with_labels(
                    "cpu_usage",
                    deleted_labels.clone(),
                    DataPoint::new(1_000, 3.0),
                ),
                Row::with_labels(
                    "memory_usage",
                    retained_labels.clone(),
                    DataPoint::new(0, 5.0),
                ),
            ])
            .unwrap();

        storage
            .apply_rollup_policies(vec![crate::storage::RollupPolicy {
                id: "cpu_1s_avg".to_string(),
                metric: "cpu_usage".to_string(),
                match_labels: Vec::new(),
                interval: 1_000,
                aggregation: Aggregation::Avg,
                bucket_origin: 0,
            }])
            .unwrap();

        storage
            .delete_series(
                &SeriesSelection::new()
                    .with_metric("cpu_usage")
                    .with_matcher(SeriesMatcher::equal(
                        "host",
                        deleted_labels[0].value.clone(),
                    )),
            )
            .unwrap();

        assert_eq!(
            storage.list_metrics().unwrap(),
            vec![MetricSeries {
                name: "memory_usage".to_string(),
                labels: retained_labels.clone(),
            }]
        );
        assert!(storage
            .select_series(&SeriesSelection::new().with_metric("cpu_usage"))
            .unwrap()
            .is_empty());
        assert_eq!(
            storage.list_metrics_in_shards(&scope).unwrap(),
            vec![MetricSeries {
                name: "memory_usage".to_string(),
                labels: retained_labels.clone(),
            }]
        );
        assert!(storage
            .select_series_in_shards(&SeriesSelection::new().with_metric("cpu_usage"), &scope)
            .unwrap()
            .is_empty());

        let rollup_snapshot = storage.trigger_rollup_run().unwrap();
        assert_eq!(rollup_snapshot.policies[0].matched_series, 0);
        assert_eq!(rollup_snapshot.policies[0].materialized_series, 0);

        storage.close().unwrap();
    }

    let reopened = builder_at_time(1_000)
        .with_data_path(temp_dir.path())
        .with_metadata_shard_count(shard_count)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()
        .unwrap();

    assert_eq!(
        reopened.list_metrics().unwrap(),
        vec![MetricSeries {
            name: "memory_usage".to_string(),
            labels: retained_labels.clone(),
        }]
    );
    assert!(reopened
        .select_series(&SeriesSelection::new().with_metric("cpu_usage"))
        .unwrap()
        .is_empty());
    assert_eq!(
        reopened.list_metrics_in_shards(&scope).unwrap(),
        vec![MetricSeries {
            name: "memory_usage".to_string(),
            labels: retained_labels,
        }]
    );
    assert!(reopened
        .select_series_in_shards(&SeriesSelection::new().with_metric("cpu_usage"), &scope)
        .unwrap()
        .is_empty());

    let reopened_rollup_snapshot = reopened.trigger_rollup_run().unwrap();
    assert_eq!(reopened_rollup_snapshot.policies[0].matched_series, 0);
    assert_eq!(reopened_rollup_snapshot.policies[0].materialized_series, 0);
    reopened.close().unwrap();
}
