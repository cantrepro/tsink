use std::fs;
use tempfile::TempDir;
use tsink::{DataPoint, Row, StorageBuilder};

#[test]
fn test_memory_mapped_bounds_checking() {
    let temp_dir = TempDir::new().unwrap();
    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .build()
            .unwrap();

        // Insert data
        let rows = vec![
            Row::new("test_metric", DataPoint::new(1000, 1.0)),
            Row::new("test_metric", DataPoint::new(2000, 2.0)),
            Row::new("test_metric", DataPoint::new(3000, 3.0)),
        ];
        storage.insert_rows(&rows).unwrap();
        storage.close().unwrap();
    }

    // Corrupt the first persisted partition data file.
    let partition_dir = fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .map(|name| name.starts_with("p-"))
                    .unwrap_or(false)
        })
        .expect("expected at least one persisted partition");

    let data_file = partition_dir.join("data");
    fs::write(&data_file, vec![0u8; 1]).unwrap();

    // Reopen and try to query - should handle bounds error gracefully
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let result = storage.select("test_metric", &[], 1, 4000);
    assert!(
        result.is_err(),
        "corrupted on-disk data should surface as an error"
    );
}

#[test]
fn test_large_offset_bounds_check() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_partition_duration(std::time::Duration::from_secs(1))
        .build()
        .unwrap();

    // Insert enough data to create multiple partitions
    let mut rows = Vec::new();
    for i in 0..200 {
        rows.push(Row::new(
            format!("metric_{}", i % 10),
            DataPoint::new(i as i64 * 1000, i as f64),
        ));
    }
    storage.insert_rows(&rows).unwrap();

    // Query with large time range to test bounds
    let result = storage.select("metric_0", &[], i64::MIN, i64::MAX);
    assert!(result.is_ok());
}

#[test]
fn test_empty_partition_handling() {
    let temp_dir = TempDir::new().unwrap();

    // Create empty data file
    let data_dir = temp_dir.path().join("p-0-1000");
    fs::create_dir_all(&data_dir).unwrap();
    fs::write(data_dir.join("data"), []).unwrap();

    // Write valid but minimal metadata
    let meta = r#"{
        "min_timestamp": 0,
        "max_timestamp": 1000,
        "num_data_points": 0,
        "metrics": {},
        "created_at": {"secs_since_epoch": 0, "nanos_since_epoch": 0}
    }"#;
    fs::write(data_dir.join("meta.json"), meta).unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Should handle empty partition gracefully
    let result = storage.select("any_metric", &[], 1, 2000);
    match result {
        Ok(points) => assert_eq!(points.len(), 0),
        Err(e) => panic!("Query failed with error: {:?}", e),
    }
}

#[test]
fn test_malformed_metadata_handling() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("p-0-1000");
    fs::create_dir_all(&data_dir).unwrap();

    // Write malformed metadata
    fs::write(data_dir.join("meta.json"), b"not valid json").unwrap();
    fs::write(data_dir.join("data"), vec![0u8; 100]).unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build();

    assert!(
        storage.is_err(),
        "malformed partition metadata should fail open"
    );
}
