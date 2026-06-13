use std::fs;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;
use tsink::wal::{DiskWal, Wal};
use tsink::{DataPoint, Row, StorageBuilder};

fn encode_uvarint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
    out
}

fn encode_varint(value: i64) -> Vec<u8> {
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    encode_uvarint(zigzag)
}

fn wal_insert_record(metric: &str, timestamp: i64, value: f64) -> Vec<u8> {
    let mut out = vec![1u8];
    let metric_name = tsink::label::marshal_metric_name(metric, &[]);
    out.extend(encode_uvarint(metric_name.len() as u64));
    out.extend(metric_name);
    out.extend(encode_varint(timestamp));
    out.extend(encode_uvarint(value.to_bits()));
    out
}

#[test]
fn test_wal_recovery_with_corruption() {
    let temp_dir = TempDir::new().unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .with_wal_buffer_size(1024)
        .build()
        .unwrap();

    for i in 0..100 {
        let rows = vec![Row::new(
            "wal_metric",
            DataPoint::new(i as i64 * 1000, i as f64),
        )];
        storage.insert_rows(&rows).unwrap();
    }

    drop(storage);

    let wal_dir = temp_dir.path().join("wal");
    if wal_dir.exists() {
        for entry in fs::read_dir(&wal_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("wal") {
                let mut data = fs::read(&path).unwrap();
                if data.len() > 50 {
                    for byte in data.iter_mut().take(50).skip(30) {
                        *byte = 0xFF;
                    }
                    fs::write(&path, data).unwrap();
                    break;
                }
            }
        }
    }

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build()
        .unwrap();

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

    let segment_path = wal_dir.join("000001.wal");
    let mut file = fs::File::create(&segment_path).unwrap();

    file.write_all(&[1u8]).unwrap(); // Insert operation

    file.write_all(&[0x80]).unwrap(); // Start of varint but no continuation

    drop(file);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build()
        .unwrap();

    let rows = vec![Row::new("test", DataPoint::new(1000, 1.0))];
    assert!(storage.insert_rows(&rows).is_ok());
}

#[test]
fn test_wal_with_invalid_operations() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();

    let segment_path = wal_dir.join("000001.wal");
    let mut file = fs::File::create(&segment_path).unwrap();

    file.write_all(&wal_insert_record("test", 8, 1.0)).unwrap();

    file.write_all(&[99u8]).unwrap(); // Invalid operation code

    file.write_all(&wal_insert_record("test", 16, 2.0)).unwrap();

    drop(file);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build()
        .unwrap();

    let points = storage.select("test", &[], 1, i64::MAX).unwrap();
    assert!(!points.is_empty(), "Should recover valid entries");
}

#[test]
fn test_wal_recovery_keeps_trailing_valid_rows_after_malformed_record() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();

    // Segment layout: valid -> malformed -> valid.
    let segment_path = wal_dir.join("000001.wal");
    let mut file = fs::File::create(&segment_path).unwrap();
    file.write_all(&wal_insert_record("test", 8, 1.0)).unwrap();

    file.write_all(&[1u8]).unwrap();
    file.write_all(&[0x80; 10]).unwrap();

    file.write_all(&wal_insert_record("test", 16, 2.0)).unwrap();
    drop(file);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build()
        .unwrap();

    let points = storage.select("test", &[], 0, i64::MAX).unwrap();
    assert_eq!(points.len(), 2, "Both valid rows should be recovered");

    let mut timestamps: Vec<i64> = points.iter().map(|p| p.timestamp).collect();
    timestamps.sort_unstable();
    assert_eq!(timestamps, vec![8, 16]);
}

#[test]
fn test_wal_with_multiple_corrupted_segments() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();

    for i in 1..=3 {
        let segment_path = wal_dir.join(format!("{:06}.wal", i));
        let mut file = fs::File::create(&segment_path).unwrap();

        if i == 2 {
            file.write_all(&[0xFF; 100]).unwrap();
        } else {
            let metric = format!("metric{}", i);
            file.write_all(&wal_insert_record(&metric, (i * 8) as i64, 1.0))
                .unwrap();
        }
    }

    let result = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build();

    assert!(result.is_ok());
}

#[test]
fn test_wal_recovery_tolerates_more_than_one_failed_segment() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();

    fs::write(wal_dir.join("000001.wal"), vec![0xFF; 32]).unwrap();
    fs::write(wal_dir.join("000002.wal"), vec![0xFF; 32]).unwrap();

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

    fs::write(wal_dir.join("000001.wal"), []).unwrap();

    fs::write(wal_dir.join("000002.wal"), [1, 2, 3]).unwrap();

    let mut file = fs::File::create(wal_dir.join("000003.wal")).unwrap();
    file.write_all(&wal_insert_record("test", 8, 1.0)).unwrap();

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .build()
        .unwrap();

    let points = storage.select("test", &[], 1, i64::MAX).unwrap();
    assert_eq!(points.len(), 1, "Should recover valid segment");
}
