# tsink

<div align="center">

<p align="right">
  <img src="https://raw.githubusercontent.com/h2337/tsink/refs/heads/master/logo.svg" width="250" height="250">
</p>

**A high-performance embedded time-series database for Rust**

</div>

## Overview

tsink is a lightweight, high-performance time-series database engine written in Rust. It provides efficient storage and retrieval of time-series data with automatic compression, time-based partitioning, and thread-safe operations.

### Key Features

- **🚀 High Performance**: Gorilla compression achieves ~1.37 bytes per data point
- **🔒 Thread-Safe**: Lock-free reads and concurrent writes with configurable worker pools
- **💾 Flexible Storage**: Choose between in-memory or persistent disk storage
- **📊 Time Partitioning**: Automatic data organization by configurable time ranges
- **🏷️ Label Support**: Multi-dimensional metrics with key-value labels
- **📝 WAL Support**: Write-ahead logging for durability and crash recovery
- **🗑️ Auto-Retention**: Configurable automatic data expiration
- **🐳 Container-Aware**: cgroup support for optimal resource usage in containers
- **⚡ Zero-Copy Reads**: Memory-mapped files for efficient disk operations

## Installation

Add tsink to your `Cargo.toml`:

```toml
[dependencies]
tsink = "0.4.2"
```

## Quick Start

### Basic Usage

```rust
use tsink::{DataPoint, Row, StorageBuilder, Storage, TimestampPrecision};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create storage with default settings
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()?;

    // Insert data points
    let rows = vec![
        Row::new("cpu_usage", DataPoint::new(1600000000, 45.5)),
        Row::new("cpu_usage", DataPoint::new(1600000060, 47.2)),
        Row::new("cpu_usage", DataPoint::new(1600000120, 46.8)),
    ];
    storage.insert_rows(&rows)?;

    // Note: Using timestamp 0 will automatically use the current timestamp
    // let row = Row::new("cpu_usage", DataPoint::new(0, 50.0));  // timestamp = current time

    // Query data points
    let points = storage.select("cpu_usage", &[], 1600000000, 1600000121)?;
    for point in points {
        println!("Timestamp: {}, Value: {}", point.timestamp, point.value);
    }

    storage.close()?;
    Ok(())
}
```

### Persistent Storage

```rust
use tsink::{StorageBuilder, Storage};
use std::time::Duration;

let storage = StorageBuilder::new()
    .with_data_path("./tsink-data")              // Enable disk persistence
    .with_partition_duration(Duration::from_secs(3600))  // 1-hour partitions
    .with_retention(Duration::from_secs(7 * 24 * 3600))  // 7-day retention
    .with_wal_buffer_size(8192)                  // 8KB WAL buffer
    .build()?;
```

### Multi-Dimensional Metrics with Labels

```rust
use tsink::{DataPoint, Label, Row};

// Create metrics with labels for detailed categorization
let rows = vec![
    Row::with_labels(
        "http_requests",
        vec![
            Label::new("method", "GET"),
            Label::new("status", "200"),
            Label::new("endpoint", "/api/users"),
        ],
        DataPoint::new(1600000000, 150.0),
    ),
    Row::with_labels(
        "http_requests",
        vec![
            Label::new("method", "POST"),
            Label::new("status", "201"),
            Label::new("endpoint", "/api/users"),
        ],
        DataPoint::new(1600000000, 25.0),
    ),
];

storage.insert_rows(&rows)?;

// Query specific label combinations
let points = storage.select(
    "http_requests",
    &[
        Label::new("method", "GET"),
        Label::new("status", "200"),
    ],
    1600000000,
    1600000100,
)?;

// Query all label combinations for a metric
let all_results = storage.select_all("http_requests", 1600000000, 1600000100)?;
for (labels, points) in all_results {
    println!("Labels: {:?}, Points: {}", labels, points.len());
}
```

### Query Options: Downsampling, Aggregation, Pagination

Use `select_with_options` to shape query results without post-processing:

```rust
use tsink::{
    Aggregation, DataPoint, Label, QueryOptions, Row,
    StorageBuilder, Storage,
};

let storage = StorageBuilder::new().with_data_path("./tsink-data").build()?;

// Insert some points
storage.insert_rows(&[
    Row::new("cpu", DataPoint::new(1_000, 1.0)),
    Row::new("cpu", DataPoint::new(2_000, 2.0)),
    Row::new("cpu", DataPoint::new(3_000, 3.0)),
    Row::new("cpu", DataPoint::new(4_500, 1.5)),
])?;

// Downsample into 2s buckets using average, with pagination
let opts = QueryOptions::new(1_000, 5_000)
    .with_downsample(2_000, Aggregation::Avg)
    .with_pagination(0, Some(2));

let buckets = storage.select_with_options("cpu", opts)?;
// buckets: [ (t=1000, avg=1.5), (t=3000, avg=2.25) ]
```

## Architecture

tsink uses a linear-order partition model that divides time-series data into time-bounded chunks:

```
┌─────────────────────────────────────────┐
│             tsink Storage               │
├─────────────────────────────────────────┤
│                                         │
│  ┌───────────────┐  Active Partition    │
│  │ Memory Part.  │◄─ (Writable)         │
│  └───────────────┘                      │
│                                         │
│  ┌───────────────┐  Buffer Partition    │
│  │ Memory Part.  │◄─ (Out-of-order)     │
│  └───────────────┘                      │
│                                         │
│  ┌───────────────┐                      │
│  │ Disk Part. 1  │◄─ Read-only          │
│  └───────────────┘   (Memory-mapped)    │
│                                         │
│  ┌───────────────┐                      │
│  │ Disk Part. 2  │◄─ Read-only          │
│  └───────────────┘                      │
│         ...                             │
└─────────────────────────────────────────┘
```

### Partition Lifecycle

1. **Active Partition**: Accepts new writes, kept in memory
2. **Buffer Partition**: Handles out-of-order writes within recent time window
3. **Flushing**: When active partition is full, it's flushed to disk
4. **Disk Partitions**: Read-only, memory-mapped for efficient queries
5. **Expiration**: Old partitions are automatically removed based on retention

### Benefits

- **Fast Queries**: Skip irrelevant partitions based on time range
- **Efficient Memory**: Only recent data stays in RAM
- **Low Write Amplification**: Sequential writes, no compaction needed
- **SSD-Friendly**: Minimal random I/O patterns

## Configuration

### StorageBuilder Options

| Option | Description | Default |
|--------|-------------|---------|
| `with_data_path` | Directory for persistent storage | None (in-memory) |
| `with_retention` | How long to keep data | 14 days |
| `with_timestamp_precision` | Timestamp precision (ns/μs/ms/s) | Nanoseconds |
| `with_max_writers` | Maximum concurrent write workers | CPU count |
| `with_write_timeout` | Timeout for write operations | 30 seconds |
| `with_partition_duration` | Time range per partition | 1 hour |
| `with_wal_enabled` | Enable write-ahead logging | true |
| `with_wal_buffer_size` | WAL buffer size in bytes | 4096 |

### Example Configuration

```rust
let storage = StorageBuilder::new()
    .with_data_path("/var/lib/tsink")
    .with_retention(Duration::from_secs(30 * 24 * 3600))  // 30 days
    .with_timestamp_precision(TimestampPrecision::Milliseconds)
    .with_max_writers(16)
    .with_write_timeout(Duration::from_secs(60))
    .with_partition_duration(Duration::from_secs(6 * 3600))  // 6 hours
    .with_wal_buffer_size(16384)  // 16KB
    .build()?;
```

## Compression

tsink uses the Gorilla compression algorithm, which is specifically designed for time-series data:

- **Delta-of-delta encoding** for timestamps
- **XOR compression** for floating-point values
- Typical compression ratio: **~1.37 bytes per data point**

This means a data point that would normally take 16 bytes (8 bytes timestamp + 8 bytes value) is compressed to less than 2 bytes on average.

## Performance

Benchmarks on AMD Ryzen 7940HS (single core):

| Operation | Throughput | Latency |
|-----------|------------|---------|
| Insert single point | 10M ops/sec | ~100ns |
| Batch insert (1000) | 15M points/sec | ~67μs/batch |
| Select 1K points | 4.5M queries/sec | ~220ns |
| Select 1M points | 3.4M queries/sec | ~290ns |

Run benchmarks yourself:
```bash
cargo bench
```

## Module Overview

### Core Modules

| Module | Description |
|--------|-------------|
| `storage` | Main storage engine with builder pattern configuration |
| `partition` | Time-based data partitioning (memory and disk implementations) |
| `encoding` | Gorilla compression for efficient time-series storage |
| `wal` | Write-ahead logging for durability and crash recovery |
| `label` | Multi-dimensional metric labeling and marshaling |

### Infrastructure Modules

| Module | Description |
|--------|-------------|
| `cgroup` | Container-aware CPU and memory limit detection |
| `mmap` | Platform-optimized memory-mapped file operations |
| `concurrency` | Worker pools, semaphores, and rate limiters |
| `bstream` | Bit-level streaming for compression algorithms |
| `list` | Thread-safe partition list management |

### Utility Modules

| Module | Description |
|--------|-------------|
| `error` | Comprehensive error types with context |

## Advanced Usage

### Label Querying

tsink provides querying capabilities for metrics with labels:

```rust
use tsink::{DataPoint, Label, Row};

// Insert metrics with various label combinations
let rows = vec![
    Row::with_labels(
        "cpu_usage",
        vec![Label::new("host", "server1"), Label::new("region", "us-west")],
        DataPoint::new(1600000000, 45.5),
    ),
    Row::with_labels(
        "cpu_usage",
        vec![Label::new("host", "server2"), Label::new("region", "us-east")],
        DataPoint::new(1600000000, 52.1),
    ),
    Row::with_labels(
        "cpu_usage",
        vec![Label::new("host", "server3")],  // Different label set
        DataPoint::new(1600000000, 48.3),
    ),
];
storage.insert_rows(&rows)?;

// Method 1: Query with exact label match
let points = storage.select(
    "cpu_usage",
    &[Label::new("host", "server1"), Label::new("region", "us-west")],
    1600000000,
    1600000100,
)?;

// Method 2: Query all label combinations (discovers all variations)
let all_results = storage.select_all("cpu_usage", 1600000000, 1600000100)?;

// Process results grouped by labels
for (labels, points) in all_results {
    // Find which hosts have the most data points
    if let Some(host_label) = labels.iter().find(|l| l.name == "host") {
        println!("Host {} has {} data points", host_label.value, points.len());
    }

    // Aggregate metrics across regions
    if let Some(region_label) = labels.iter().find(|l| l.name == "region") {
        let avg: f64 = points.iter().map(|p| p.value).sum::<f64>() / points.len() as f64;
        println!("Region {} average: {:.2}", region_label.value, avg);
    }
}
```

### Concurrent Operations

tsink is designed for high-concurrency scenarios:

```rust
use std::thread;
use std::sync::Arc;

let storage = Arc::new(StorageBuilder::new().build()?);

// Spawn multiple writer threads
let mut handles = vec![];
for worker_id in 0..10 {
    let storage = storage.clone();
    let handle = thread::spawn(move || {
        for i in 0..1000 {
            let row = Row::new(
                "concurrent_metric",
                DataPoint::new(1600000000 + i, i as f64),
            );
            storage.insert_rows(&[row]).unwrap();
        }
    });
    handles.push(handle);
}

// Wait for all threads
for handle in handles {
    handle.join().unwrap();
}
```

### Out-of-Order Insertion

tsink handles out-of-order data points automatically:

```rust
// Insert data points in random order
let rows = vec![
    Row::new("metric", DataPoint::new(1600000500, 5.0)),
    Row::new("metric", DataPoint::new(1600000100, 1.0)),  // Earlier timestamp
    Row::new("metric", DataPoint::new(1600000300, 3.0)),
    Row::new("metric", DataPoint::new(1600000200, 2.0)),  // Out of order
];

storage.insert_rows(&rows)?;

// Query returns points in correct chronological order
let points = storage.select("metric", &[], 1600000000, 1600001000)?;
assert!(points.windows(2).all(|w| w[0].timestamp <= w[1].timestamp));
```

### Container Deployment

tsink automatically detects container resource limits:

```rust
// tsink reads cgroup limits automatically
let storage = StorageBuilder::new()
    .with_max_writers(0)  // 0 = auto-detect from cgroup
    .build()?;

// In a container with 2 CPU limit, this will use 2 workers
// even if the host has 16 CPUs
```

### WAL Recovery

After a crash, tsink automatically recovers from WAL:

```rust
// First run - data is written to WAL
let storage = StorageBuilder::new()
    .with_data_path("/data/tsink")
    .build()?;
storage.insert_rows(&rows)?;
// Crash happens here...

// Next run - data is recovered from WAL automatically
let storage = StorageBuilder::new()
    .with_data_path("/data/tsink")  // Same path
    .build()?;  // Recovery happens here

// Previously inserted data is available
let points = storage.select("metric", &[], 0, i64::MAX)?;
```

## Examples

Run the comprehensive example showcasing all features:

```bash
cargo run --example comprehensive
```

Other examples:
- `basic_usage` - Simple insert and query operations
- `persistent_storage` - Disk-based storage with WAL
- `production_example` - Production-ready configuration

## Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run with verbose output
cargo test -- --nocapture

# Run specific test module
cargo test storage::tests
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/h2337/tsink.git
cd tsink

# Run tests
cargo test

# Run benchmarks
cargo bench

# Check formatting
cargo fmt -- --check

# Run clippy
cargo clippy -- -D warnings
```

## License

- MIT License
