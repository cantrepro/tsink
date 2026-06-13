use super::*;
use std::path::Path;

fn replace_numeric_lane_with_file(root: &Path) {
    if let Err(err) = std::fs::remove_dir_all(root) {
        if err.kind() != std::io::ErrorKind::NotFound {
            panic!(
                "failed to remove numeric lane root {}: {err}",
                root.display()
            );
        }
    }
    if let Err(err) = std::fs::remove_file(root) {
        if err.kind() != std::io::ErrorKind::NotFound {
            panic!(
                "failed to remove numeric lane file {}: {err}",
                root.display()
            );
        }
    }
    std::fs::write(root, b"not-a-directory").unwrap();
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

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();
    let delayed_handle = storage.clone();

    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        drop(delayed_handle);
    });

    drop(storage);

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();
    reopened.close().unwrap();
    releaser.join().unwrap();
}

#[test]
fn background_errors_surface_in_health_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_root = temp_dir.path().join(NUMERIC_LANE_ROOT);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
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
    assert!(!health.fail_fast_triggered);
    assert!(health.last_background_error.is_some());

    storage
        .insert_rows(&[Row::new("background_health", DataPoint::new(2, 2.0))])
        .unwrap();
}

#[test]
fn background_fail_fast_blocks_new_operations_after_worker_error() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_root = temp_dir.path().join(NUMERIC_LANE_ROOT);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(1)
        .with_background_fail_fast(true)
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
        "expected fail-fast mode to trip after background worker error"
    );

    let err = storage
        .insert_rows(&[Row::new("background_fail_fast", DataPoint::new(2, 2.0))])
        .unwrap_err();
    assert!(matches!(err, TsinkError::StorageShuttingDown));
}
