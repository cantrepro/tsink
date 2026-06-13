use std::fs;
use std::sync::Arc;

use tempfile::TempDir;
use tsink::wal::{DiskWal, Wal, WalReader};
use tsink::{DataPoint, Label, Row};

fn wal_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map(|ext| ext == "wal").unwrap_or(false) {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

#[test]
fn disk_wal_persists_rotates_and_recovers() {
    let temp_dir = TempDir::new().unwrap();

    let wal = DiskWal::new(temp_dir.path(), 0).unwrap();
    let wal_trait: Arc<dyn Wal> = wal.clone();

    let batch_one = vec![
        Row::new("wal_metric", DataPoint::new(1, 1.0)),
        Row::with_labels(
            "wal_metric",
            vec![Label::new("host", "a")],
            DataPoint::new(2, 2.5),
        ),
    ];

    wal_trait.append_rows(&batch_one).unwrap();
    wal_trait.flush().unwrap();

    let recovered = WalReader::new(temp_dir.path()).unwrap().read_all().unwrap();
    assert_eq!(recovered.len(), batch_one.len());
    assert!(recovered.iter().any(|row| row.metric() == "wal_metric"
        && row.labels().is_empty()
        && row.data_point().timestamp == 1
        && (row.data_point().value - 1.0).abs() < 1e-12));

    let expected_label = vec![Label::new("host", "a")];
    assert!(recovered.iter().any(|row| {
        row.metric() == "wal_metric"
            && row.labels() == expected_label.as_slice()
            && row.data_point().timestamp == 2
            && (row.data_point().value - 2.5).abs() < 1e-12
    }));

    // Rotate and add a second batch
    wal_trait.punctuate().unwrap();
    let batch_two = vec![Row::new("wal_metric", DataPoint::new(3, 5.0))];
    wal_trait.append_rows(&batch_two).unwrap();
    wal_trait.flush().unwrap();

    let all_rows = WalReader::new(temp_dir.path()).unwrap().read_all().unwrap();
    assert_eq!(all_rows.len(), batch_one.len() + batch_two.len());
    assert!(all_rows.iter().any(|row| row.data_point().timestamp == 3));

    // Remove oldest segment and confirm only the newer entries remain
    wal_trait.remove_oldest().unwrap();
    let files_after_removal = wal_files(temp_dir.path());
    assert_eq!(files_after_removal.len(), 1);
    let remaining_rows = WalReader::new(temp_dir.path()).unwrap().read_all().unwrap();
    assert_eq!(remaining_rows.len(), batch_two.len());
    assert!(
        remaining_rows
            .iter()
            .all(|row| row.data_point().timestamp >= 3)
    );

    // Refresh should clear all segments
    wal_trait.refresh().unwrap();
    assert!(wal_files(temp_dir.path()).is_empty());
}

#[test]
fn disk_wal_flush_with_buffer_persists() {
    let temp_dir = TempDir::new().unwrap();
    let wal = DiskWal::new(temp_dir.path(), 4096).unwrap();
    let wal_trait: Arc<dyn Wal> = wal.clone();

    let batch = vec![Row::new("buffered_metric", DataPoint::new(1, 1.5))];
    wal_trait.append_rows(&batch).unwrap();
    wal_trait.flush().unwrap();

    // Ensure a wal file was written and can be recovered
    let files = wal_files(temp_dir.path());
    assert_eq!(files.len(), 1, "wal segment should exist after flush");

    let recovered = WalReader::new(temp_dir.path()).unwrap().read_all().unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].metric(), "buffered_metric");
    assert_eq!(recovered[0].data_point().timestamp, 1);
    assert!((recovered[0].data_point().value - 1.5).abs() < 1e-12);
}
