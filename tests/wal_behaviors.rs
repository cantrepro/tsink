use std::fs;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tsink::wal::{DiskWal, Wal, WalReader};
use tsink::{DataPoint, Label, Row, StorageBuilder, WalSyncMode};

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

fn total_wal_size(dir: &std::path::Path) -> u64 {
    wal_files(dir)
        .iter()
        .map(|path| fs::metadata(path).unwrap().len())
        .sum()
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

    let files = wal_files(temp_dir.path());
    assert_eq!(files.len(), 1);
    let first_segment = fs::read(&files[0]).unwrap();
    assert_eq!(first_segment.first().copied(), Some(1));

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

#[test]
fn wal_reader_accepts_valid_short_segments() {
    let temp_dir = TempDir::new().unwrap();
    let segment = temp_dir.path().join("000001.wal");

    // op=insert, metric_len=1, metric='a', ts=0, value=0.0 bits
    fs::write(segment, vec![1, 1, b'a', 0, 0]).unwrap();

    let recovered = WalReader::new(temp_dir.path()).unwrap().read_all().unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].metric(), "a");
}

#[test]
fn wal_reader_ignores_non_wal_files_even_if_numeric() {
    let temp_dir = TempDir::new().unwrap();
    fs::write(temp_dir.path().join("000001"), vec![1, 1, b'a', 0, 0]).unwrap();
    fs::write(temp_dir.path().join("000002.log"), vec![1, 1, b'b', 0, 0]).unwrap();

    let wal = DiskWal::new(temp_dir.path(), 0).unwrap();
    let wal_trait: Arc<dyn Wal> = wal;
    wal_trait
        .append_rows(&[Row::new("valid", DataPoint::new(9, 9.0))])
        .unwrap();
    wal_trait.flush().unwrap();

    let recovered = WalReader::new(temp_dir.path()).unwrap().read_all().unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].metric(), "valid");
}

#[test]
fn wal_recovery_does_not_reappend_rows_when_replaying_multiple_windows() {
    let temp_dir = TempDir::new().unwrap();

    // Seed one persisted disk partition so replay includes both on-disk and in-memory partitions.
    let seeded = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_partition_duration(Duration::from_secs(1))
        .build()
        .unwrap();
    seeded
        .insert_rows(&[Row::new("seed_metric", DataPoint::new(10, 1.0))])
        .unwrap();
    seeded.close().unwrap();

    let wal_dir = temp_dir.path().join("wal");
    let wal = DiskWal::new(&wal_dir, 0).unwrap();
    let wal_trait: Arc<dyn Wal> = wal.clone();

    let mut valid_rows = Vec::new();
    for i in 0..500 {
        valid_rows.push(Row::new(
            "recover_metric",
            DataPoint::new(10_000_000_000 + i as i64, i as f64),
        ));
    }
    for i in 0..500 {
        valid_rows.push(Row::new(
            "recover_metric",
            DataPoint::new(8_000_000_000 + i as i64, (i + 500) as f64),
        ));
    }
    wal_trait.append_rows(&valid_rows).unwrap();

    // Include a much older row so recovery must create additional writable windows.
    wal_trait
        .append_rows(&[Row::new("recover_metric", DataPoint::new(1, 1.0))])
        .unwrap();
    wal_trait.flush().unwrap();

    let before = total_wal_size(&wal_dir);

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .with_partition_duration(Duration::from_secs(1))
        .build()
        .unwrap();

    let points = reopened.select("recover_metric", &[], 0, i64::MAX).unwrap();
    assert_eq!(points.len(), 1001);

    // Recovery must not append replayed rows back into WAL.
    let after_first_reopen = total_wal_size(&wal_dir);
    assert!(after_first_reopen <= before);

    reopened.close().unwrap();

    // Reopen again and ensure replay does not duplicate recovered rows.
    let reopened_again = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(true)
        .with_partition_duration(Duration::from_secs(1))
        .build()
        .unwrap();
    let points_again = reopened_again
        .select("recover_metric", &[], 0, i64::MAX)
        .unwrap();
    assert_eq!(points_again.len(), 1001);
}

#[test]
fn disk_wal_supports_per_append_and_periodic_sync_modes() {
    for sync_mode in [
        WalSyncMode::PerAppend,
        WalSyncMode::Periodic(Duration::from_secs(3600)),
    ] {
        let temp_dir = TempDir::new().unwrap();
        let wal = DiskWal::new_with_sync_mode(temp_dir.path(), 4096, sync_mode).unwrap();
        let wal_trait: Arc<dyn Wal> = wal;

        wal_trait
            .append_rows(&[Row::new("mode_metric", DataPoint::new(1, 1.0))])
            .unwrap();
        wal_trait.flush().unwrap();

        let recovered = WalReader::new(temp_dir.path()).unwrap().read_all().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].metric(), "mode_metric");
        assert_eq!(recovered[0].data_point().timestamp, 1);
    }
}
