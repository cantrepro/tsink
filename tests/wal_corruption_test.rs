use std::fs;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;
use tsink::wal::{DiskWal, Wal};
use tsink::{DataPoint, Row, StorageBuilder};

#[test]
fn test_wal_recovery_with_corruption() {
    let temp_dir = TempDir::new().unwrap();

    // Create storage with WAL enabled
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .with_wal_buffer_size(1024)
        .build()
        .unwrap();

    // Insert data
    for i in 0..100 {
        let rows = vec![Row::new(
            "wal_metric",
            DataPoint::new(i as i64 * 1000, i as f64),
        )];
        storage.insert_rows(&rows).unwrap();
    }

    drop(storage);

    // Corrupt WAL file
    let wal_dir = temp_dir.path().join("wal");
    if wal_dir.exists() {
        for entry in fs::read_dir(&wal_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("wal") {
                // Corrupt middle of file
                let mut data = fs::read(&path).unwrap();
                if data.len() > 50 {
                    // Inject garbage in the middle
                    for byte in data.iter_mut().take(50).skip(30) {
                        *byte = 0xFF;
                    }
                    fs::write(&path, data).unwrap();
                    break;
                }
            }
        }
    }

    // Reopen - should recover what it can
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build()
        .unwrap();

    // Should have recovered some data
    let points = storage.select("wal_metric", &[], 1, i64::MAX).unwrap();
    assert!(
        !points.is_empty(),
        "Should recover some data despite corruption"
    );
}

#[test]
fn test_wal_with_incomplete_writes() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create WAL segment with incomplete write
    let segment_path = wal_dir.join("000001.wal");
    let mut file = fs::File::create(&segment_path).unwrap();

    // Write valid operation type
    file.write_all(&[1u8]).unwrap(); // Insert operation

    // Write partial metric name length (incomplete varint)
    file.write_all(&[0x80]).unwrap(); // Start of varint but no continuation

    drop(file);

    // Open storage - should handle incomplete WAL gracefully
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build()
        .unwrap();

    // Should open successfully despite incomplete WAL
    let rows = vec![Row::new("test", DataPoint::new(1000, 1.0))];
    assert!(storage.insert_rows(&rows).is_ok());
}

#[test]
fn test_wal_with_invalid_operations() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create WAL with invalid operation codes
    let segment_path = wal_dir.join("000001.wal");
    let mut file = fs::File::create(&segment_path).unwrap();

    // Write some valid data first
    file.write_all(&[1u8]).unwrap(); // Valid Insert operation
    file.write_all(&[4u8]).unwrap(); // metric length
    file.write_all(b"test").unwrap();
    file.write_all(&[8, 0, 0, 0, 0, 0, 0, 0]).unwrap(); // timestamp
    file.write_all(&[0, 0, 0, 0, 0, 0, 0xF0, 0x3F]).unwrap(); // value 1.0

    // Write invalid operation
    file.write_all(&[99u8]).unwrap(); // Invalid operation code

    // Write more valid data
    file.write_all(&[1u8]).unwrap(); // Valid Insert operation
    file.write_all(&[4u8]).unwrap(); // metric length
    file.write_all(b"test").unwrap();
    file.write_all(&[16, 0, 0, 0, 0, 0, 0, 0]).unwrap(); // timestamp
    file.write_all(&[0, 0, 0, 0, 0, 0, 0, 0x40]).unwrap(); // value 2.0

    drop(file);

    // Open storage - should skip invalid operation
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build()
        .unwrap();

    let points = storage.select("test", &[], 1, i64::MAX).unwrap();
    // Should have recovered valid entries
    assert!(!points.is_empty(), "Should recover valid entries");
}

#[test]
fn test_wal_with_multiple_corrupted_segments() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create multiple WAL segments with varying corruption
    for i in 1..=3 {
        let segment_path = wal_dir.join(format!("{:06}.wal", i));
        let mut file = fs::File::create(&segment_path).unwrap();

        if i == 2 {
            // Completely corrupt segment
            file.write_all(&[0xFF; 100]).unwrap();
        } else {
            // Valid segment
            file.write_all(&[1u8]).unwrap(); // Insert operation
            file.write_all(&[7u8]).unwrap(); // metric length
            file.write_all(format!("metric{}", i).as_bytes()).unwrap();
            file.write_all(&[i * 8, 0, 0, 0, 0, 0, 0, 0]).unwrap(); // timestamp
            file.write_all(&[0, 0, 0, 0, 0, 0, 0xF0, 0x3F]).unwrap(); // value
        }
    }

    // Should handle multiple corrupted segments
    let result = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build();

    // Storage should still open (may have partial data)
    assert!(result.is_ok());
}

#[test]
fn test_wal_recovery_tolerates_more_than_one_failed_segment() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();

    // Two segments that should fail parsing.
    fs::write(wal_dir.join("000001.wal"), vec![0xFF; 32]).unwrap();
    fs::write(wal_dir.join("000002.wal"), vec![0xFF; 32]).unwrap();

    // One valid segment written using the real WAL encoder.
    let wal = DiskWal::new(&wal_dir, 0).unwrap();
    let wal_trait: Arc<dyn Wal> = wal;
    wal_trait
        .append_rows(&[Row::new("recoverable", DataPoint::new(7, 7.0))])
        .unwrap();
    wal_trait.flush().unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build()
        .unwrap();

    let points = storage.select("recoverable", &[], 0, 10).unwrap();
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].timestamp, 7);
    assert!((points[0].value - 7.0).abs() < 1e-12);
}

#[test]
fn test_wal_with_empty_segments() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create empty WAL segment
    fs::write(wal_dir.join("000001.wal"), []).unwrap();

    // Create very small WAL segment (less than 8 bytes)
    fs::write(wal_dir.join("000002.wal"), [1, 2, 3]).unwrap();

    // Create valid segment
    let mut file = fs::File::create(wal_dir.join("000003.wal")).unwrap();
    file.write_all(&[1u8]).unwrap(); // Insert operation
    file.write_all(&[4u8]).unwrap(); // metric length
    file.write_all(b"test").unwrap();
    file.write_all(&[8, 0, 0, 0, 0, 0, 0, 0]).unwrap(); // timestamp
    file.write_all(&[0, 0, 0, 0, 0, 0, 0xF0, 0x3F]).unwrap(); // value

    // Should skip empty/small segments and recover valid one
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build()
        .unwrap();

    let points = storage.select("test", &[], 1, i64::MAX).unwrap();
    assert_eq!(points.len(), 1, "Should recover valid segment");
}
