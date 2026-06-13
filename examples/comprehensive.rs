//! Comprehensive examples demonstrating various tsink features
//!
//! Run with: cargo run --example comprehensive

use std::thread;
use std::time::Duration;
use tsink::{DataPoint, Label, Row, StorageBuilder, TimestampPrecision};

/// Example: Basic storage with data path persistence
fn example_with_data_path() -> tsink::Result<()> {
    println!("\n=== Example: Storage with Data Path ===");

    let storage = StorageBuilder::new()
        .with_data_path("/tmp/tsink-data")
        .build()?;

    // Insert some data
    let row = Row::new("temperature", DataPoint::new(1600000000, 23.5));
    storage.insert_rows(&[row])?;

    // Data will be persisted to /tmp/tsink-data
    storage.close()?;

    println!("Data persisted to /tmp/tsink-data");
    Ok(())
}

/// Example: Custom partition duration
fn example_with_partition_duration() -> tsink::Result<()> {
    println!("\n=== Example: Custom Partition Duration ===");

    let storage = StorageBuilder::new()
        .with_partition_duration(Duration::from_secs(3600 * 5)) // 5 hours
        .build()?;

    // Insert data across multiple partitions
    for i in 0..10 {
        let timestamp = 1600000000 + (i * 3600 * 2); // 2 hour intervals
        let row = Row::new("metric", DataPoint::new(timestamp, i as f64));
        storage.insert_rows(&[row])?;
    }

    storage.close()?;
    println!("Created partitions with 5-hour duration");
    Ok(())
}

/// Example: Basic insert and select operations
fn example_insert_and_select() -> tsink::Result<()> {
    println!("\n=== Example: Insert and Select ===");

    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()?;

    // Insert multiple data points
    let rows = vec![
        Row::new("cpu_usage", DataPoint::new(1600000000, 45.2)),
        Row::new("cpu_usage", DataPoint::new(1600000010, 47.8)),
        Row::new("cpu_usage", DataPoint::new(1600000020, 52.1)),
        Row::with_labels(
            "cpu_usage",
            vec![Label::new("host", "server1")],
            DataPoint::new(1600000030, 49.5),
        ),
    ];

    storage.insert_rows(&rows)?;

    // Select data
    let points = storage.select("cpu_usage", &[], 1600000000, 1600000025)?;

    println!("Selected {} data points:", points.len());
    for point in points {
        println!("  Timestamp: {}, Value: {}", point.timestamp, point.value);
    }

    storage.close()?;
    Ok(())
}

/// Example: Concurrent insert and select operations
fn example_concurrent_operations() -> tsink::Result<()> {
    println!("\n=== Example: Concurrent Operations ===");

    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()?;

    let mut handles = vec![];

    // Start write workers
    for worker_id in 0..3 {
        let storage = storage.clone();
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let timestamp = 1600000000 + i;
                let row = Row::with_labels(
                    "concurrent_metric",
                    vec![Label::new("worker", worker_id.to_string())],
                    DataPoint::new(timestamp, (worker_id * 100 + i) as f64),
                );

                if let Err(e) = storage.insert_rows(&[row]) {
                    eprintln!("Worker {} insert error: {}", worker_id, e);
                }
            }
            println!("Worker {} completed 100 inserts", worker_id);
        });
        handles.push(handle);
    }

    // Start read workers
    for reader_id in 0..2 {
        let storage = storage.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50)); // Let some writes happen first

            for _ in 0..10 {
                match storage.select("concurrent_metric", &[], 1600000000, 1600000100) {
                    Ok(points) => {
                        println!("Reader {} read {} points", reader_id, points.len());
                    }
                    Err(tsink::TsinkError::NoDataPoints { .. }) => {
                        println!("Reader {} found no data points yet", reader_id);
                    }
                    Err(e) => {
                        eprintln!("Reader {} error: {}", reader_id, e);
                    }
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        handles.push(handle);
    }

    // Wait for all workers
    for handle in handles {
        handle.join().unwrap();
    }

    storage.close()?;
    println!("Concurrent operations completed");
    Ok(())
}

/// Example: Out-of-order insertion
fn example_out_of_order_insertion() -> tsink::Result<()> {
    println!("\n=== Example: Out-of-Order Insertion ===");

    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()?;

    // Insert data points out of order
    let rows = vec![
        Row::new("unordered", DataPoint::new(1600000500, 5.0)),
        Row::new("unordered", DataPoint::new(1600000100, 1.0)),
        Row::new("unordered", DataPoint::new(1600000300, 3.0)),
        Row::new("unordered", DataPoint::new(1600000200, 2.0)),
        Row::new("unordered", DataPoint::new(1600000400, 4.0)),
    ];

    for row in rows {
        storage.insert_rows(&[row])?;
    }

    // Select and verify ordering
    let points = storage.select("unordered", &[], 1600000000, 1600001000)?;

    println!("Points retrieved (should be in timestamp order):");
    for point in &points {
        println!("  Timestamp: {}, Value: {}", point.timestamp, point.value);
    }

    // Verify they're actually sorted
    for i in 1..points.len() {
        assert!(
            points[i].timestamp >= points[i - 1].timestamp,
            "Points not sorted!"
        );
    }

    storage.close()?;
    println!("Out-of-order insertion handled correctly");
    Ok(())
}

/// Example: Handling expired data with retention
fn example_expired_data() -> tsink::Result<()> {
    println!("\n=== Example: Expired Data Handling ===");

    let storage = StorageBuilder::new()
        .with_retention(Duration::from_secs(3600)) // 1 hour retention
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Try to insert old data (beyond retention)
    let old_timestamp = now - 7200; // 2 hours ago
    let old_row = Row::new("expired_metric", DataPoint::new(old_timestamp, 100.0));

    // This might be rejected or stored temporarily
    match storage.insert_rows(&[old_row]) {
        Ok(_) => println!("Old data inserted (may be cleaned up later)"),
        Err(e) => println!("Old data rejected: {}", e),
    }

    // Insert current data
    let current_row = Row::new("current_metric", DataPoint::new(now, 200.0));
    storage.insert_rows(&[current_row])?;

    storage.close()?;
    println!("Retention policy demonstrated");
    Ok(())
}

/// Example: Using labels for multi-dimensional data
fn example_with_labels() -> tsink::Result<()> {
    println!("\n=== Example: Multi-dimensional Data with Labels ===");

    let storage = StorageBuilder::new().build()?;

    // Insert metrics with different label combinations
    let rows = vec![
        Row::with_labels(
            "http_requests",
            vec![Label::new("method", "GET"), Label::new("status", "200")],
            DataPoint::new(1600000000, 150.0),
        ),
        Row::with_labels(
            "http_requests",
            vec![Label::new("method", "POST"), Label::new("status", "200")],
            DataPoint::new(1600000000, 50.0),
        ),
        Row::with_labels(
            "http_requests",
            vec![Label::new("method", "GET"), Label::new("status", "404")],
            DataPoint::new(1600000000, 10.0),
        ),
    ];

    for row in rows {
        storage.insert_rows(&[row])?;
    }

    // Query specific label combination
    let get_200 = storage.select(
        "http_requests",
        &[Label::new("method", "GET"), Label::new("status", "200")],
        0,
        i64::MAX,
    )?;

    println!(
        "GET requests with 200 status: {} requests",
        get_200[0].value
    );

    storage.close()?;
    Ok(())
}

/// Example: High-throughput concurrent writes
#[tokio::main]
async fn example_high_throughput() -> tsink::Result<()> {
    println!("\n=== Example: High-Throughput Writes ===");

    let storage = StorageBuilder::new().build()?;
    let start = std::time::Instant::now();

    let mut tasks = vec![];

    // Launch many concurrent write tasks
    for task_id in 0..10 {
        let storage = storage.clone();
        let task = tokio::spawn(async move {
            for i in 0..1000 {
                let timestamp = 1600000000 + (task_id * 1000 + i) as i64;
                let row = Row::new(
                    "high_throughput",
                    DataPoint::new(timestamp, (task_id * 1000 + i) as f64),
                );

                if let Err(e) = storage.insert_rows(&[row]) {
                    eprintln!("Task {} error: {}", task_id, e);
                }
            }
        });
        tasks.push(task);
    }

    // Wait for all tasks
    for task in tasks {
        task.await.unwrap();
    }

    let elapsed = start.elapsed();
    let total_points = 10 * 1000;
    let throughput = total_points as f64 / elapsed.as_secs_f64();

    println!("Inserted {} points in {:?}", total_points, elapsed);
    println!("Throughput: {:.0} points/second", throughput);

    storage.close()?;
    Ok(())
}

fn main() -> tsink::Result<()> {
    println!("tsink Comprehensive Examples");
    println!("============================");

    // Run all examples
    example_with_data_path()?;
    example_with_partition_duration()?;
    example_insert_and_select()?;
    example_concurrent_operations()?;
    example_out_of_order_insertion()?;
    example_expired_data()?;
    example_with_labels()?;
    example_high_throughput()?;

    println!("\n✅ All examples completed successfully!");
    Ok(())
}
