//! Basic usage example for tsink.

use tsink::{DataPoint, Label, Row, StorageBuilder, TimestampPrecision};

fn main() -> anyhow::Result<()> {
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()?;

    let rows = vec![
        Row::new("cpu_usage", DataPoint::new(1600000000, 45.5)),
        Row::new("cpu_usage", DataPoint::new(1600000060, 47.2)),
        Row::new("cpu_usage", DataPoint::new(1600000120, 46.8)),
        Row::new("memory_usage", DataPoint::new(1600000000, 1024.0)),
        Row::new("memory_usage", DataPoint::new(1600000060, 1156.0)),
    ];

    storage.insert_rows(&rows)?;

    let cpu_points = storage.select("cpu_usage", &[], 1600000000, 1600000121)?;
    println!("CPU usage data points:");
    for point in &cpu_points {
        println!("  Timestamp: {}, Value: {}", point.timestamp, point.value);
    }

    let labeled_rows = vec![
        Row::with_labels(
            "http_requests",
            vec![Label::new("method", "GET"), Label::new("status", "200")],
            DataPoint::new(1600000000, 150.0),
        ),
        Row::with_labels(
            "http_requests",
            vec![Label::new("method", "POST"), Label::new("status", "201")],
            DataPoint::new(1600000000, 50.0),
        ),
    ];

    storage.insert_rows(&labeled_rows)?;

    let get_requests = storage.select(
        "http_requests",
        &[Label::new("method", "GET"), Label::new("status", "200")],
        1600000000,
        1600000100,
    )?;

    println!("\nHTTP GET requests:");
    for point in &get_requests {
        println!("  Timestamp: {}, Count: {}", point.timestamp, point.value);
    }

    storage.close()?;

    Ok(())
}
