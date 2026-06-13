use super::*;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};

fn replace_numeric_lane_with_file(root: &Path) {
    replace_path_with_file(root);
}

fn replace_path_with_file(root: &Path) {
    let mut last_err = None;
    for _ in 0..20 {
        if let Err(err) = std::fs::remove_dir_all(root) {
            match err.kind() {
                std::io::ErrorKind::NotFound => {}
                std::io::ErrorKind::PermissionDenied => {
                    last_err = Some(format!(
                        "failed to remove directory root {}: {err}",
                        root.display()
                    ));
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                _ => panic!("failed to remove directory root {}: {err}", root.display()),
            }
        }
        if let Err(err) = std::fs::remove_file(root) {
            if err.kind() != std::io::ErrorKind::NotFound {
                panic!("failed to remove file root {}: {err}", root.display());
            }
        }

        if let Some(parent) = root.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        match std::fs::write(root, b"not-a-directory") {
            Ok(()) => return,
            Err(err) if err.kind() == std::io::ErrorKind::IsADirectory => {
                last_err = Some(format!(
                    "failed to replace directory root {}: {err}",
                    root.display()
                ));
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => panic!("failed to write file root {}: {err}", root.display()),
        }
    }
    panic!(
        "{}",
        last_err.unwrap_or_else(|| format!("failed to replace directory root {}", root.display()))
    );
}

fn wait_for_condition<F>(timeout: Duration, poll_interval: Duration, condition: F) -> bool
where
    F: Fn() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(poll_interval);
    }
    condition()
}

fn write_runtime_refresh_segment(data_path: &Path, metric: &str) -> std::path::PathBuf {
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "startup")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;
    let mut chunks_by_series = HashMap::new();
    chunks_by_series.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0)],
        )],
    );

    let writer = SegmentWriter::new(&lane_path, 0, 1).unwrap();
    writer.write_segment(&registry, &chunks_by_series).unwrap();
    writer.layout().root.clone()
}

fn flip_first_byte(path: &Path) {
    let mut bytes = std::fs::read(path).unwrap();
    assert!(
        !bytes.is_empty(),
        "expected {} to be non-empty",
        path.display()
    );
    bytes[0] ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

fn write_numeric_segment_to_path(
    lane_path: &Path,
    registry: &SeriesRegistry,
    series_id: crate::engine::series::SeriesId,
    level: u8,
    segment_id: u64,
    points: &[(i64, f64)],
) -> PathBuf {
    let mut chunks_by_series = HashMap::new();
    chunks_by_series.insert(
        series_id,
        vec![make_persisted_numeric_chunk(series_id, points)],
    );

    let writer = SegmentWriter::new(lane_path, level, segment_id).unwrap();
    writer.write_segment(registry, &chunks_by_series).unwrap();
    writer.layout().root.clone()
}

fn staged_dir_prefix(purpose: &str) -> String {
    format!(".tmp-tsink-{purpose}-")
}

fn quarantined_segment_paths(level_root: &Path, purpose: &str) -> Vec<PathBuf> {
    let prefix = staged_dir_prefix(purpose);
    let mut roots = match std::fs::read_dir(level_root) {
        Ok(entries) => entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_type = entry.file_type().ok()?;
                if !file_type.is_dir() {
                    return None;
                }
                let name = entry.file_name();
                let name = name.to_str()?;
                name.starts_with(&prefix).then_some(entry.path())
            })
            .collect::<Vec<_>>(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(err) => panic!("failed to list {level_root:?}: {err}"),
    };
    roots.sort();
    roots
}

#[test]
fn data_path_lock_rejects_second_open_while_first_is_alive() {
    let temp_dir = TempDir::new().unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let err = match StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
    {
        Ok(_) => panic!("expected second open to fail due to held data-path lock"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        TsinkError::InvalidConfiguration(message) if message.contains("already locked")
    ));

    storage.close().unwrap();
}

#[test]
fn data_path_lock_releases_on_close_and_allows_reopen() {
    let temp_dir = TempDir::new().unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();
    storage.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();
    reopened.close().unwrap();
}

#[test]
fn data_path_lock_retries_until_last_handle_drops() {
    let temp_dir = TempDir::new().unwrap();

    let lock = Arc::new(
        super::super::process_lock::DataPathProcessLock::acquire(temp_dir.path()).unwrap(),
    );
    let delayed_handle = Arc::clone(&lock);

    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        drop(delayed_handle);
    });

    drop(lock);

    let reopened =
        super::super::process_lock::DataPathProcessLock::acquire(temp_dir.path()).unwrap();
    drop(reopened);
    releaser.join().unwrap();
}

#[test]
fn background_flush_failures_only_degrade_when_fail_fast_is_opted_out() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_root = temp_dir.path().join(NUMERIC_LANE_ROOT);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .with_background_fail_fast(false)
        .build()
        .unwrap();

    replace_numeric_lane_with_file(&numeric_root);
    storage
        .insert_rows(&[Row::new("background_health", DataPoint::new(1, 1.0))])
        .unwrap();

    let observed = wait_for_condition(Duration::from_secs(3), Duration::from_millis(25), || {
        storage
            .observability_snapshot()
            .health
            .background_errors_total
            > 0
    });
    assert!(
        observed,
        "expected background worker error to be visible in health snapshot"
    );

    let health = storage.observability_snapshot().health;
    assert!(health.degraded);
    assert!(!health.fail_fast_enabled);
    assert!(!health.fail_fast_triggered);
    assert!(health.last_background_error.is_some());

    storage
        .insert_rows(&[Row::new("background_health", DataPoint::new(2, 2.0))])
        .unwrap();
}

#[test]
fn background_flush_failures_fence_new_work_by_default() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_root = temp_dir.path().join(NUMERIC_LANE_ROOT);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .build()
        .unwrap();

    replace_numeric_lane_with_file(&numeric_root);
    storage
        .insert_rows(&[Row::new("background_fail_fast", DataPoint::new(1, 1.0))])
        .unwrap();

    let triggered = wait_for_condition(Duration::from_secs(3), Duration::from_millis(25), || {
        storage.observability_snapshot().health.fail_fast_triggered
    });
    assert!(
        triggered,
        "expected the default background durability policy to fence service"
    );

    let health = storage.observability_snapshot().health;
    assert!(health.degraded);
    assert!(health.fail_fast_enabled);
    assert!(health.fail_fast_triggered);
    assert!(health
        .last_background_error
        .as_deref()
        .is_some_and(|message| message.contains("flush worker error")));

    let err = storage
        .insert_rows(&[Row::new("background_fail_fast", DataPoint::new(2, 2.0))])
        .unwrap_err();
    assert!(matches!(err, TsinkError::StorageShuttingDown));
}

#[test]
fn background_compaction_failures_fence_new_work_by_default() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let metric = "background_compaction_fail_fast_default";
    let labels = vec![Label::new("host", "compaction")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;

    for segment_id in 1..=4 {
        write_numeric_segment_to_path(
            &lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        );
    }

    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(lane_path.clone()),
        None,
        5,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: true,
            compaction_interval: Duration::from_secs(60),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    replace_numeric_lane_with_file(&lane_path);
    storage.notify_compaction_thread();

    let triggered = wait_for_condition(Duration::from_secs(3), Duration::from_millis(25), || {
        let health = storage.observability_snapshot().health;
        health.fail_fast_triggered
            && health
                .last_background_error
                .as_deref()
                .is_some_and(|message| message.contains("compaction worker error"))
    });
    assert!(
        triggered,
        "expected compaction failures to fence service under the default policy"
    );

    let health = storage.observability_snapshot().health;
    assert!(health.degraded);
    assert!(health.fail_fast_enabled);
    assert!(health.fail_fast_triggered);
    assert_eq!(
        storage.coordination.lifecycle.load(Ordering::SeqCst),
        super::super::STORAGE_OPEN
    );

    let err = storage
        .insert_rows(&[Row::new(metric, DataPoint::new(5, 5.0))])
        .unwrap_err();
    assert!(matches!(err, TsinkError::StorageShuttingDown));

    storage
        .coordination
        .lifecycle
        .store(super::super::STORAGE_CLOSED, Ordering::SeqCst);
}

#[test]
fn background_persisted_refresh_failures_fence_new_work_by_default() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let metric = "background_runtime_refresh_manifest_corrupt";
    let segment_root = write_runtime_refresh_segment(temp_dir.path(), metric);
    let storage = Arc::new(
        open_raw_numeric_storage_from_data_path_with_background_fail_fast(temp_dir.path(), true),
    );

    flip_first_byte(&segment_root.join("manifest.bin"));
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage.start_background_persisted_refresh_thread().unwrap();
    storage.notify_persisted_refresh_thread();

    let triggered = wait_for_condition(Duration::from_secs(3), Duration::from_millis(25), || {
        let snapshot = storage.observability_snapshot();
        snapshot.health.fail_fast_triggered
            && snapshot
                .health
                .last_background_error
                .as_deref()
                .is_some_and(|message| {
                    message.contains("runtime refresh")
                        && message.contains(&segment_root.display().to_string())
                })
    });
    assert!(
        triggered,
        "expected persisted-refresh failures to fence service under the default policy"
    );

    let health = storage.observability_snapshot().health;
    assert!(health.degraded);
    assert!(health.fail_fast_enabled);
    assert!(health.fail_fast_triggered);
    assert!(health
        .last_background_error
        .as_deref()
        .is_some_and(|message| {
            message.contains("runtime refresh")
                && message.contains("manifest crc32 mismatch")
                && message.contains(&segment_root.display().to_string())
        }));
    assert_eq!(
        storage.coordination.lifecycle.load(Ordering::SeqCst),
        super::super::STORAGE_OPEN
    );

    let err = storage
        .insert_rows(&[Row::new(metric, DataPoint::new(3, 3.0))])
        .unwrap_err();
    assert!(matches!(err, TsinkError::StorageShuttingDown));

    storage
        .coordination
        .lifecycle
        .store(super::super::STORAGE_CLOSED, Ordering::SeqCst);
}

#[test]
fn startup_defers_retention_maintenance_failures_to_background_worker() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let lane_path = data_dir.path().join(NUMERIC_LANE_ROOT);
    let warm_numeric_path = object_store_dir.path().join("warm").join(NUMERIC_LANE_ROOT);
    let metric = "startup_deferred_retention_failure";
    let labels = vec![Label::new("host", "startup")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;

    write_numeric_segment_to_path(
        &lane_path,
        &registry,
        series_id,
        2,
        1,
        &[(40, 40.0), (41, 41.0)],
    );

    let storage = builder_at_time(65)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_background_fail_fast(false)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        storage.select(metric, &labels, 0, 200).unwrap(),
        vec![DataPoint::new(40, 40.0), DataPoint::new(41, 41.0)],
    );

    replace_path_with_file(&warm_numeric_path);
    let observed = wait_for_condition(Duration::from_secs(3), Duration::from_millis(25), || {
        let snapshot = storage.observability_snapshot();
        snapshot.health.background_errors_total > 0
            && snapshot
                .health
                .last_background_error
                .as_deref()
                .is_some_and(|message| message.contains("flush_maintenance worker error"))
    });
    assert!(
        observed,
        "expected deferred startup retention maintenance to fail in the background worker"
    );

    let health = storage.observability_snapshot().health;
    assert!(health.degraded);
    assert!(!health.fail_fast_enabled);
    assert!(!health.fail_fast_triggered);

    std::fs::remove_file(&warm_numeric_path).unwrap();
    std::fs::create_dir_all(&warm_numeric_path).unwrap();
    storage.close().unwrap();
}

#[test]
fn startup_repair_reconciles_expired_segments_in_background() {
    let data_dir = TempDir::new().unwrap();
    let lane_path = data_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "startup")];
    let expired_metric = "startup_background_expired";
    let healthy_metric = "startup_background_healthy";
    let registry = SeriesRegistry::new();
    let expired_series_id = registry
        .resolve_or_insert(expired_metric, &labels)
        .unwrap()
        .series_id;
    let healthy_series_id = registry
        .resolve_or_insert(healthy_metric, &labels)
        .unwrap()
        .series_id;

    let expired_root = write_numeric_segment_to_path(
        &lane_path,
        &registry,
        expired_series_id,
        2,
        1,
        &[(1, 1.0), (2, 2.0)],
    );
    write_numeric_segment_to_path(
        &lane_path,
        &registry,
        healthy_series_id,
        2,
        2,
        &[(95, 95.0), (96, 96.0)],
    );

    let storage = builder_at_time(100)
        .with_data_path(data_dir.path())
        .with_retention(Duration::from_secs(10))
        .with_background_fail_fast(false)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        storage.select(healthy_metric, &labels, 0, 200).unwrap(),
        vec![DataPoint::new(95, 95.0), DataPoint::new(96, 96.0)],
    );

    let repaired = wait_for_condition(Duration::from_secs(3), Duration::from_millis(25), || {
        !expired_root.exists()
            && storage
                .observability_snapshot()
                .flush
                .expired_segments_total
                > 0
            && storage
                .select(expired_metric, &labels, 0, 200)
                .unwrap()
                .is_empty()
            && storage
                .select_series(&SeriesSelection::new().with_metric(expired_metric))
                .unwrap()
                .is_empty()
    });
    assert!(
        repaired,
        "expected deferred startup maintenance to retire expired segments and reconcile metadata"
    );

    storage.close().unwrap();
}

#[test]
fn startup_tiering_runs_synchronously_when_background_fail_fast_is_enabled() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let lane_path = data_dir.path().join(NUMERIC_LANE_ROOT);
    let warm_lane = object_store_dir.path().join("warm").join(NUMERIC_LANE_ROOT);
    let metric = "startup_fail_fast_tiering";
    let labels = vec![Label::new("host", "startup")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;

    write_numeric_segment_to_path(
        &lane_path,
        &registry,
        series_id,
        2,
        1,
        &[(95, 95.0), (96, 96.0)],
    );

    let storage = builder_at_time(110)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_background_fail_fast(true)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();

    assert_eq!(
        storage.select(metric, &labels, 0, 200).unwrap(),
        vec![DataPoint::new(95, 95.0), DataPoint::new(96, 96.0)],
    );

    let visible = storage
        .select_series(&SeriesSelection::new().with_metric(metric))
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].name, metric);
    assert_eq!(visible[0].labels, labels);

    assert!(
        load_segments_for_level(&lane_path, 2).unwrap().is_empty(),
        "fail-fast startup should tier segments before build returns instead of waiting for deferred maintenance",
    );
    assert_eq!(load_segments_for_level(&warm_lane, 2).unwrap().len(), 1);

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.flush.hot_segments_visible, 0);
    assert_eq!(snapshot.flush.warm_segments_visible, 1);
    assert_eq!(snapshot.flush.cold_segments_visible, 0);

    storage.close().unwrap();
}

#[test]
fn background_runtime_refresh_keeps_last_visible_segments_when_manifest_is_corrupted() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let metric = "background_runtime_refresh_manifest_corrupt";
    let labels = vec![Label::new("host", "startup")];
    let segment_root = write_runtime_refresh_segment(temp_dir.path(), metric);
    let storage = Arc::new(
        open_raw_numeric_storage_from_data_path_with_background_fail_fast(temp_dir.path(), false),
    );

    flip_first_byte(&segment_root.join("manifest.bin"));
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage.start_background_persisted_refresh_thread().unwrap();
    storage.notify_persisted_refresh_thread();

    let observed = wait_for_condition(Duration::from_secs(3), Duration::from_millis(25), || {
        let snapshot = storage.observability_snapshot();
        snapshot.health.background_errors_total > 0
            && snapshot
                .health
                .last_background_error
                .as_deref()
                .is_some_and(|message| {
                    message.contains("runtime refresh")
                        && message.contains(&segment_root.display().to_string())
                })
    });
    assert!(
        observed,
        "expected background runtime refresh corruption to surface in health snapshot"
    );

    let health = storage.observability_snapshot().health;
    assert!(health.degraded);
    assert!(!health.fail_fast_enabled);
    assert!(!health.fail_fast_triggered);
    assert!(health
        .last_background_error
        .as_deref()
        .is_some_and(|message| {
            message.contains("runtime refresh")
                && message.contains("manifest crc32 mismatch")
                && message.contains(&segment_root.display().to_string())
        }));
    assert_eq!(
        storage.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&segment_root));
}

#[test]
fn background_persisted_refresh_retries_after_repair_when_fail_fast_is_disabled() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let metric = "background_runtime_refresh_retry";
    let labels = vec![Label::new("host", "startup")];
    let segment_root = write_runtime_refresh_segment(temp_dir.path(), metric);
    let manifest_path = segment_root.join("manifest.bin");
    let manifest_bytes = std::fs::read(&manifest_path).unwrap();
    let storage = Arc::new(
        open_raw_numeric_storage_from_data_path_with_background_fail_fast(temp_dir.path(), false),
    );

    flip_first_byte(&manifest_path);
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage.start_background_persisted_refresh_thread().unwrap();
    storage.notify_persisted_refresh_thread();

    let failed = wait_for_condition(Duration::from_secs(3), Duration::from_millis(25), || {
        let snapshot = storage.observability_snapshot();
        storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst)
            && snapshot.health.background_errors_total > 0
            && snapshot
                .health
                .last_background_error
                .as_deref()
                .is_some_and(|message| {
                    message.contains("runtime refresh")
                        && message.contains("manifest crc32 mismatch")
                })
    });
    assert!(
        failed,
        "expected the background refresh worker to surface the corruption and leave the catalog dirty for retry",
    );
    assert_eq!(
        storage.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)],
        "the last published persisted view should stay queryable after a refresh failure",
    );

    std::fs::write(&manifest_path, manifest_bytes).unwrap();
    storage.notify_persisted_refresh_thread();

    // Invariant: a background refresh failure may leave the engine dirty, but a later
    // retry should be able to publish the repaired view without recreating the storage.
    let recovered = wait_for_condition(Duration::from_secs(3), Duration::from_millis(25), || {
        !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst)
    });
    assert!(
        recovered,
        "expected the background refresh worker to clear the dirty flag after the segment was repaired",
    );
    assert_eq!(
        storage.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
}

#[test]
fn retention_maintenance_rejects_corrupt_runtime_inventory_before_tiering_other_segments() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = data_dir.path().join(NUMERIC_LANE_ROOT);
    let warm_lane = object_store_dir.path().join("warm").join(NUMERIC_LANE_ROOT);
    let metric = "runtime_maintenance_manifest_corrupt";
    let labels = vec![Label::new("host", "maintenance")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;
    let corrupt_root = write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        2,
        1,
        &[(60, 60.0), (61, 61.0)],
    );
    let movable_root = write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        2,
        2,
        &[(62, 62.0), (63, 63.0)],
    );
    let loaded_segments = load_segment_indexes(&hot_lane).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(hot_lane.clone()),
        None,
        loaded_segments.next_segment_id,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: 100,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            retention_enforced: true,
            background_threads_enabled: false,
            tiered_storage: Some(super::super::config::TieredStorageConfig {
                object_store_root: object_store_dir.path().to_path_buf(),
                segment_catalog_path: None,
                mirror_hot_segments: false,
                hot_retention_window: 10,
                warm_retention_window: 50,
            }),
            #[cfg(test)]
            current_time_override: Some(65),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    storage
        .apply_loaded_segment_indexes(loaded_segments, false)
        .unwrap();
    storage.refresh_segment_catalog_and_observability().unwrap();

    flip_first_byte(&corrupt_root.join("manifest.bin"));
    storage.set_current_time_override(100);
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    let err = storage.sweep_expired_persisted_segments().unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("failed maintenance validation"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("manifest crc32 mismatch"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains(&corrupt_root.display().to_string()),
        "unexpected error: {message}"
    );

    let warm_root = warm_lane
        .join("segments")
        .join("L2")
        .join("seg-0000000000000002");
    assert!(
        !warm_root.exists(),
        "maintenance must not tier a healthy segment while a scanned peer is corrupt"
    );
    assert!(
        corrupt_root.exists(),
        "corrupt root should remain visible on disk"
    );
    assert!(
        movable_root.exists(),
        "healthy root should remain in place when maintenance aborts"
    );
    assert_eq!(
        storage.select(metric, &labels, 0, 200).unwrap(),
        vec![
            DataPoint::new(60, 60.0),
            DataPoint::new(61, 61.0),
            DataPoint::new(62, 62.0),
            DataPoint::new(63, 63.0),
        ]
    );
}

#[test]
fn tier_move_staging_sync_failure_keeps_source_visible_and_avoids_destination_publish() {
    let hot_dir = TempDir::new().unwrap();
    let warm_dir = TempDir::new().unwrap();
    let hot_lane = hot_dir.path().join(NUMERIC_LANE_ROOT);
    let warm_lane = warm_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "tier-copy")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("tier_copy_failure_metric", &labels)
        .unwrap()
        .series_id;
    let source_root = write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        2,
        1,
        &[(60, 60.0), (61, 61.0)],
    );
    let destination_root = warm_lane
        .join("segments")
        .join("L2")
        .join("seg-0000000000000001");
    let destination_parent = destination_root.parent().unwrap().to_path_buf();
    let staging_prefix = staged_dir_prefix("tier-segment");
    let _guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
        move |candidate| {
            candidate.parent() == Some(destination_parent.as_path())
                && candidate
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(&staging_prefix))
        },
        "injected tier destination staging sync failure",
    );

    let err =
        super::super::tiering::move_segment_to_tier(&source_root, &destination_root).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("injected tier destination staging sync failure"),
        "unexpected error: {message}"
    );
    assert!(source_root.exists(), "source segment must remain visible");
    assert!(
        load_segments_for_level(&warm_lane, 2).unwrap().is_empty(),
        "destination tier must not publish a partially copied segment"
    );
}

#[test]
fn tier_move_quarantines_source_after_destination_publish_when_cleanup_sync_fails() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = data_dir.path().join(NUMERIC_LANE_ROOT);
    let warm_lane = object_store_dir.path().join("warm").join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "tier-quarantine")];
    let metric = "tier_cleanup_quarantine_metric";
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;
    let source_root = write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        2,
        1,
        &[(60, 60.0), (61, 61.0)],
    );
    let hot_level_root = source_root.parent().unwrap().to_path_buf();
    let warm_root = warm_lane
        .join("segments")
        .join("L2")
        .join("seg-0000000000000001");
    super::super::tiering::move_segment_to_tier(&source_root, &warm_root).unwrap();
    let guard = crate::engine::fs_utils::fail_directory_sync_once(
        hot_level_root.clone(),
        "injected tier source cleanup sync failure",
    );

    let quarantined =
        crate::engine::segment::quarantine_segment_root(&source_root, "tier-source-cleanup")
            .unwrap();
    assert!(quarantined.sync_failed);
    assert!(!source_root.exists(), "source root should have been hidden");
    assert!(
        warm_root.exists(),
        "verified destination should remain published"
    );

    let quarantined_paths = quarantined_segment_paths(&hot_level_root, "tier-source-cleanup");
    assert_eq!(quarantined_paths, vec![quarantined.path.clone()]);

    drop(guard);

    let reopened = builder_at_time(100)
        .with_data_path(data_dir.path())
        .with_object_store_path(object_store_dir.path())
        .with_tiered_retention_policy(Duration::from_secs(10), Duration::from_secs(50))
        .with_retention(Duration::from_secs(100))
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_wal_enabled(false)
        .build()
        .unwrap();
    assert_eq!(
        reopened.select(metric, &labels, 0, 200).unwrap(),
        vec![DataPoint::new(60, 60.0), DataPoint::new(61, 61.0)]
    );
    reopened.close().unwrap();
}

#[test]
fn flush_worker_shutdown_skips_self_join_panics() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            1,
            None,
            Some(lane_path),
            None,
            1,
            ChunkStorageOptions {
                background_threads_enabled: false,
                retention_enforced: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );

    storage
        .start_background_flush_thread(Duration::from_millis(5))
        .unwrap();

    let (result_tx, result_rx) = mpsc::channel();
    let sender_slot = Arc::new(Mutex::new(Some(result_tx)));
    let weak_storage = Arc::downgrade(&storage);

    storage.set_persist_post_publish_hook({
        let sender_slot = Arc::clone(&sender_slot);
        move |_| {
            let Some(storage) = weak_storage.upgrade() else {
                return;
            };
            let result = catch_unwind(AssertUnwindSafe(|| storage.join_background_threads()));
            if let Some(sender) = sender_slot.lock().take() {
                let _ = sender.send(result);
            }
        }
    });

    storage
        .insert_rows(&[Row::new("flush_self_join_guard", DataPoint::new(1, 1.0))])
        .unwrap();

    let result = result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("flush worker should exercise self-join path");
    let join_result = result.expect("self-join guard should avoid panicking");
    join_result.expect("self-join guard should return success");

    storage.clear_persist_post_publish_hook();
    storage
        .coordination
        .lifecycle
        .store(super::super::STORAGE_CLOSED, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(25));
}
