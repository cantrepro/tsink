use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use tsink::{DataPoint, Label, Row, StorageBuilder};

#[test]
fn test_high_contention_concurrent_writes() {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_max_writers(4) // Limit writers to force contention
            .with_write_timeout(Duration::from_secs(10))
            .build()
            .unwrap(),
    );

    let num_threads = 20;
    let writes_per_thread = 100;
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage = storage.clone();
        let handle = thread::spawn(move || {
            for i in 0..writes_per_thread {
                let timestamp = (thread_id * writes_per_thread + i + 1) as i64;
                let rows = vec![Row::new(
                    "contention_metric",
                    DataPoint::new(timestamp, thread_id as f64 + i as f64 / 100.0),
                )];
                storage.insert_rows(&rows).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all writes succeeded
    let points = storage
        .select("contention_metric", &[], 1, i64::MAX)
        .unwrap();
    assert_eq!(
        points.len(),
        num_threads * writes_per_thread,
        "Some writes were lost under contention"
    );
}

#[test]
fn test_concurrent_reads_during_writes() {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .build()
            .unwrap(),
    );

    // Start writer thread
    let storage_writer = storage.clone();
    let writer = thread::spawn(move || {
        for i in 0..1000 {
            let rows = vec![Row::new(
                "concurrent_metric",
                DataPoint::new((i + 1) as i64 * 100, i as f64),
            )];
            storage_writer.insert_rows(&rows).unwrap();
            if i % 10 == 0 {
                thread::sleep(Duration::from_micros(100));
            }
        }
    });

    // Start multiple reader threads
    let mut readers = vec![];
    for reader_id in 0..5 {
        let storage = storage.clone();
        let handle = thread::spawn(move || {
            let mut last_count = 0;
            for _ in 0..20 {
                thread::sleep(Duration::from_millis(5));
                let points = storage
                    .select("concurrent_metric", &[], 1, i64::MAX)
                    .unwrap();
                // Points should only increase or stay same, never decrease
                assert!(
                    points.len() >= last_count,
                    "Reader {} saw decreasing points: {} -> {}",
                    reader_id,
                    last_count,
                    points.len()
                );
                last_count = points.len();
            }
            last_count
        });
        readers.push(handle);
    }

    writer.join().unwrap();
    for handle in readers {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_different_metrics() {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .build()
            .unwrap(),
    );

    let mut handles = vec![];

    // Each thread writes to its own metric
    for thread_id in 0..10 {
        let storage = storage.clone();
        let handle = thread::spawn(move || {
            let metric_name = format!("metric_{}", thread_id);
            for i in 0..100 {
                let rows = vec![Row::new(
                    &metric_name,
                    DataPoint::new((i + 1) as i64, thread_id as f64 * 100.0 + i as f64),
                )];
                storage.insert_rows(&rows).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify each metric has exactly 100 points
    for thread_id in 0..10 {
        let metric_name = format!("metric_{}", thread_id);
        let points = storage.select(&metric_name, &[], 1, i64::MAX).unwrap();
        assert_eq!(points.len(), 100, "Metric {} has wrong count", metric_name);
    }
}

#[test]
fn test_concurrent_labeled_metrics() {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .build()
            .unwrap(),
    );

    let mut handles = vec![];

    for thread_id in 0..8 {
        let storage = storage.clone();
        let handle = thread::spawn(move || {
            let labels = vec![
                Label::new("thread", thread_id.to_string()),
                Label::new("type", if thread_id % 2 == 0 { "even" } else { "odd" }),
            ];

            for i in 0..50 {
                let rows = vec![Row::with_labels(
                    "labeled_metric",
                    labels.clone(),
                    DataPoint::new((i + 1) as i64, thread_id as f64 * 10.0 + i as f64),
                )];
                storage.insert_rows(&rows).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Query with different label combinations
    for thread_id in 0..8 {
        let labels = vec![
            Label::new("thread", thread_id.to_string()),
            Label::new("type", if thread_id % 2 == 0 { "even" } else { "odd" }),
        ];
        let points = storage
            .select("labeled_metric", &labels, 1, i64::MAX)
            .unwrap();
        assert_eq!(
            points.len(),
            50,
            "Thread {} labeled metric has wrong count",
            thread_id
        );
    }
}
