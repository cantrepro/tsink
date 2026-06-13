//! Example showing persistent storage with disk partitions.

use std::time::Duration;
use tempfile::TempDir;
use tsink::{DataPoint, Row, StorageBuilder, TimestampPrecision};

fn main() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let data_path = temp_dir.path();

    println!("Using data directory: {:?}", data_path);

    let storage = StorageBuilder::new()
        .with_data_path(data_path)
        .with_partition_duration(Duration::from_secs(3600)) // 1 hour partitions
        .with_retention(Duration::from_secs(24 * 3600)) // 24 hour retention
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()?;

    let mut current_time = 1600000000i64;
    let mut rows = Vec::new();

    for i in 0..100 {
        rows.push(Row::new(
            "temperature",
            DataPoint::new(current_time, 20.0 + (i as f64 * 0.1)),
        ));
        rows.push(Row::new(
            "humidity",
            DataPoint::new(current_time, 60.0 + (i as f64 * 0.05)),
        ));
        current_time += 60; // Add 60 milliseconds
    }

    storage.insert_rows(&rows)?;
    println!("Inserted {} rows", rows.len());

    let start = 1600000000;
    let end = current_time;

    let temp_points = storage.select("temperature", &[], start, end)?;
    println!("\nTemperature readings: {} points", temp_points.len());

    if let Some(first) = temp_points.first() {
        println!(
            "  First: timestamp={}, value={:.2}",
            first.timestamp, first.value
        );
    }
    if let Some(last) = temp_points.last() {
        println!(
            "  Last:  timestamp={}, value={:.2}",
            last.timestamp, last.value
        );
    }

    storage.close()?;
    println!("\nStorage closed and data persisted to disk");

    let storage2 = StorageBuilder::new()
        .with_data_path(data_path)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()?;

    let temp_points2 = storage2.select("temperature", &[], start, end)?;
    println!(
        "\nAfter reopening, found {} temperature points",
        temp_points2.len()
    );

    storage2.close()?;

    Ok(())
}
