//! Production-ready example of using tsink with all features.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};
use tsink::{DataPoint, Label, Row, StorageBuilder, TimestampPrecision};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting tsink production example");

    let storage = Arc::new(
        StorageBuilder::new()
            .with_data_path("./tsink-data")
            .with_retention(Duration::from_secs(7 * 24 * 3600)) // 7 days
            .with_partition_duration(Duration::from_secs(3600)) // 1 hour
            .with_timestamp_precision(TimestampPrecision::Nanoseconds)
            .with_write_timeout(Duration::from_secs(30))
            .with_wal_buffer_size(4096)
            .with_max_writers(8)
            .build()?,
    );

    info!("Starting production workload simulation");

    let mut writer_handles = vec![];
    for thread_id in 0..4 {
        let storage_clone = Arc::clone(&storage);

        let handle = thread::spawn(move || {
            info!("Writer thread {} started", thread_id);

            for batch in 0..100 {
                let start = std::time::Instant::now();
                let mut rows = Vec::new();

                for i in 0..100 {
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64;

                    rows.push(Row::with_labels(
                        "cpu_usage",
                        vec![
                            Label::new("host", format!("server-{}", thread_id)),
                            Label::new("core", format!("core-{}", i % 4)),
                        ],
                        DataPoint::new(timestamp, 50.0 + (i as f64) * 0.1),
                    ));

                    rows.push(Row::with_labels(
                        "memory_usage",
                        vec![
                            Label::new("host", format!("server-{}", thread_id)),
                            Label::new("type", "used"),
                        ],
                        DataPoint::new(timestamp, 1024.0 * (1000.0 + i as f64)),
                    ));

                    rows.push(Row::with_labels(
                        "network_bytes",
                        vec![
                            Label::new("host", format!("server-{}", thread_id)),
                            Label::new("interface", "eth0"),
                            Label::new("direction", if i % 2 == 0 { "rx" } else { "tx" }),
                        ],
                        DataPoint::new(timestamp, 1000.0 * (i as f64)),
                    ));
                }

                match storage_clone.insert_rows(&rows) {
                    Ok(_) => {
                        let duration = start.elapsed();
                        if batch % 10 == 0 {
                            info!(
                                "Thread {} inserted batch {} ({} rows) in {:?}",
                                thread_id,
                                batch,
                                rows.len(),
                                duration
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "Thread {} failed to insert batch {}: {}",
                            thread_id, batch, e
                        );
                    }
                }

                thread::sleep(Duration::from_millis(100));
            }

            info!("Writer thread {} completed", thread_id);
        });

        writer_handles.push(handle);
    }

    let storage_reader = Arc::clone(&storage);

    let reader_handle = thread::spawn(move || {
        info!("Reader thread started");

        thread::sleep(Duration::from_secs(2)); // Wait for some data

        for iteration in 0..20 {
            let start = std::time::Instant::now();
            let end_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64;
            let start_time = end_time - 60_000_000_000; // Last 60 seconds

            match storage_reader.select(
                "cpu_usage",
                &[Label::new("host", "server-0")],
                start_time,
                end_time,
            ) {
                Ok(points) => {
                    let duration = start.elapsed();
                    if !points.is_empty() {
                        let avg_value: f64 = points
                            .iter()
                            .map(|p| p.value_as_f64().unwrap_or(f64::NAN))
                            .sum::<f64>()
                            / points.len() as f64;
                        info!(
                            "Query {} returned {} CPU data points, avg value: {:.2}, duration: {:?}",
                            iteration,
                            points.len(),
                            avg_value,
                            duration
                        );
                    }
                }
                Err(e) => {
                    warn!("Query {} failed: {}", iteration, e);
                }
            }

            match storage_reader.select(
                "memory_usage",
                &[Label::new("host", "server-1"), Label::new("type", "used")],
                start_time,
                end_time,
            ) {
                Ok(points) => {
                    if !points.is_empty() {
                        let latest = points.last().unwrap();
                        info!(
                            "Latest memory usage for server-1: {:.2} MB",
                            latest.value_as_f64().unwrap_or(f64::NAN) / 1024.0
                        );
                    }
                }
                Err(e) => {
                    warn!("Memory query failed: {}", e);
                }
            }

            thread::sleep(Duration::from_secs(3));
        }

        info!("Reader thread completed");
    });

    for handle in writer_handles {
        handle.join().expect("Writer thread panicked");
    }

    reader_handle.join().expect("Reader thread panicked");

    info!("Shutting down storage");
    storage.close()?;

    info!("tsink production example completed successfully");
    Ok(())
}
