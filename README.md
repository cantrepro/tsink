# tsink

<div align="center">
  <img src="https://raw.githubusercontent.com/h2337/tsink/refs/heads/master/logo.svg" width="220" height="220" alt="tsink logo">
  <p><strong>Embedded time-series storage for Rust</strong></p>
  <a href="https://crates.io/crates/tsink"><img src="https://img.shields.io/crates/v/tsink.svg" alt="crates.io"></a>
  <a href="https://docs.rs/tsink"><img src="https://docs.rs/tsink/badge.svg" alt="docs.rs"></a>
  <a href="https://github.com/h2337/tsink/blob/master/LICENSE"><img src="https://img.shields.io/crates/l/tsink.svg" alt="MIT license"></a>
</div>

---

`tsink` is a lightweight, in-process time-series database engine for Rust applications.
It stores time-series data in compressed chunks, persists immutable segment files to disk, and can replay a write-ahead log (WAL) after crashes — all without requiring an external server.

## Features

- **Embedded API** — no external server, network protocol, or daemon required.
- **Thread-safe** — the storage handle is an `Arc<dyn Storage>`, safe to share across threads.
- **Multi-series model** — series identity is metric name + exact label set.
- **Typed values** — `f64`, `i64`, `u64`, `bool`, `bytes`, and `string`.
- **Rich queries** — downsampling, aggregation (12 built-in functions), pagination, and custom bytes aggregation via the `Codec`/`Aggregator` traits.
- **Disk persistence** — immutable segment files with a crash-safe commit protocol.
- **WAL durability** — selectable sync mode (`Periodic` or `PerAppend`) with idempotent replay on recovery.
- **Out-of-order writes** — data is returned sorted by timestamp regardless of insertion order.
- **Concurrent writers** — multiple threads can insert simultaneously with sharded internal locking.
- **Optional PromQL engine** — instant and range queries with 20+ built-in functions; enable with the `promql` Cargo feature.
- **LSM-style compaction** — tiered L0 → L1 → L2 segment compaction reduces read amplification.
- **Gorilla compression** — Gorilla XOR encoding for floats, delta-of-delta for timestamps, and per-type codecs for other value types.
- **Cgroup-aware defaults** — worker thread counts and memory limits respect container CPU/memory quotas.
- **Resource limits** — configurable memory budget, series cardinality cap, and WAL size limit with admission backpressure.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Async Usage](#async-usage)
- [Server Mode](#server-mode-prometheus-wire-compatible)
- [Query APIs](#query-apis)
- [Series Discovery](#series-discovery)
- [Value Model](#value-model)
- [Label Constraints](#label-constraints)
- [PromQL Engine](#promql-engine)
- [Persistence and WAL](#persistence-and-wal)
- [On-Disk Layout](#on-disk-layout)
- [Compression and Encoding](#compression-and-encoding)
- [Performance](#performance)
- [Architecture](#architecture)
- [StorageBuilder Options](#storagebuilder-options)
- [Resource Limits and Backpressure](#resource-limits-and-backpressure)
- [Container Support](#container-support)
- [Error Handling](#error-handling)
- [Advanced Usage](#advanced-usage)
- [Examples](#examples)
- [Benchmarks and Tests](#benchmarks-and-tests)
- [Development Scripts](#development-scripts)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [Minimum Supported Rust Version](#minimum-supported-rust-version)
- [License](#license)

## Installation

```toml
[dependencies]
tsink = "0.8.0"
```

Enable PromQL support:

```toml
[dependencies]
tsink = { version = "0.8.0", features = ["promql"] }
```

Enable async storage facade (dedicated worker threads, runtime-agnostic futures):

```toml
[dependencies]
tsink = { version = "0.8.0", features = ["async-storage"] }
```

## Quick Start

```rust
use std::error::Error;
use tsink::{DataPoint, Label, Row, StorageBuilder};

fn main() -> Result<(), Box<dyn Error>> {
    let storage = StorageBuilder::new().build()?;

    storage.insert_rows(&[
        Row::new("cpu_usage", DataPoint::new(1_700_000_000, 42.5)),
        Row::new("cpu_usage", DataPoint::new(1_700_000_010, 43.1)),
        Row::with_labels(
            "http_requests",
            vec![Label::new("method", "GET"), Label::new("status", "200")],
            DataPoint::new(1_700_000_000, 120u64),
        ),
    ])?;

    // Time range is [start, end) (end-exclusive).
    let cpu = storage.select("cpu_usage", &[], 1_700_000_000, 1_700_000_100)?;
    assert_eq!(cpu.len(), 2);

    // Label order does not matter for series identity.
    let get_200 = storage.select(
        "http_requests",
        &[Label::new("status", "200"), Label::new("method", "GET")],
        1_700_000_000,
        1_700_000_100,
    )?;
    assert_eq!(get_200.len(), 1);

    storage.close()?;
    Ok(())
}
```

## Async Usage

`async-storage` exposes `AsyncStorage` and `AsyncStorageBuilder`.
The async API routes requests through bounded queues to dedicated worker threads, while reusing the existing synchronous engine implementation. It is runtime-agnostic — no dependency on tokio, async-std, or any specific executor.

```rust
use tsink::{AsyncStorageBuilder, DataPoint, Row};

# async fn run() -> tsink::Result<()> {
let storage = AsyncStorageBuilder::new()
    .with_queue_capacity(1024)
    .with_read_workers(4)
    .build()?;

storage
    .insert_rows(vec![Row::new("cpu", DataPoint::new(1, 42.0))])
    .await?;

let points = storage.select("cpu", vec![], 0, 10).await?;
assert_eq!(points.len(), 1);

storage.close().await?;
# Ok(())
# }
```

`AsyncStorage` also provides synchronous accessors for introspection:

| Method | Description |
|---|---|
| `memory_used()` | Current in-memory usage in bytes. |
| `memory_budget()` | Configured memory budget. |
| `inner()` | Access the underlying `Arc<dyn Storage>`. |
| `into_inner(self)` | Unwrap the underlying storage handle. |

## Server Mode (Prometheus Wire Compatible)

This workspace includes a binary crate at `crates/tsink-server` that runs tsink as a network service and accepts Prometheus remote storage wire format (protobuf + snappy).

Run the server:

```bash
cargo run -p tsink-server -- server --listen 127.0.0.1:9201 --data-path ./tsink-data
```

Full CLI options:

| Flag | Default | Description |
|---|---|---|
| `--listen <ADDR>` | `127.0.0.1:9201` | Bind address. |
| `--data-path <PATH>` | None (in-memory) | Persist tsink data under PATH. |
| `--wal-enabled <BOOL>` | `true` | Enable WAL. |
| `--no-wal` | — | Disable WAL (shorthand). |
| `--timestamp-precision <s\|ms\|us\|ns>` | `ms` | Timestamp precision (server defaults to milliseconds). |

Supported endpoints:
- `GET /healthz` — health check (returns `ok`)
- `POST /api/v1/write` — Prometheus remote write
- `POST /api/v1/read` — Prometheus remote read

Example Prometheus config:

```yaml
remote_write:
  - url: http://127.0.0.1:9201/api/v1/write

remote_read:
  - url: http://127.0.0.1:9201/api/v1/read
```

## Query APIs

| Method | Description |
|---|---|
| `select(metric, labels, start, end)` | Returns points sorted by timestamp for one series. |
| `select_into(metric, labels, start, end, &mut buf)` | Same as `select`, but writes into a caller-provided buffer for allocation reuse. |
| `select_all(metric, start, end)` | Returns grouped results for all label sets of a metric. |
| `select_with_options(metric, QueryOptions)` | Supports downsampling, aggregation, custom bytes aggregation, and pagination. |
| `list_metrics()` | Lists all known metric + label-set series. |
| `list_metrics_with_wal()` | Like `list_metrics`, but also includes series only present in the WAL. |
| `select_series(SeriesSelection)` | Matcher-based series discovery (`=`, `!=`, `=~`, `!~`) with optional time-window filtering. |

All time ranges are half-open: `[start, end)`.

### Downsampling and Aggregation

```rust
use tsink::{Aggregation, DataPoint, QueryOptions, Row, StorageBuilder};

let storage = StorageBuilder::new().build()?;
storage.insert_rows(&[
    Row::new("cpu", DataPoint::new(1_000, 1.0)),
    Row::new("cpu", DataPoint::new(2_000, 2.0)),
    Row::new("cpu", DataPoint::new(3_000, 3.0)),
    Row::new("cpu", DataPoint::new(4_500, 1.5)),
])?;

let opts = QueryOptions::new(1_000, 5_000)
    .with_downsample(2_000, Aggregation::Avg)
    .with_pagination(0, Some(2));

let buckets = storage.select_with_options("cpu", opts)?;
assert_eq!(buckets.len(), 2);
```

Built-in aggregation functions:
`None`, `Sum`, `Min`, `Max`, `Avg`, `First`, `Last`, `Count`, `Median`, `Range`, `Variance`, `StdDev`.

### Custom Bytes Aggregation

For non-numeric data, implement the `Codec` and `Aggregator` traits to define custom aggregation logic over `bytes`-encoded values:

```rust
use tsink::{Codec, Aggregator, QueryOptions, Aggregation};

struct MyCodec;
impl Codec for MyCodec {
    type Item = MyStruct;
    fn encode(&self, value: &MyStruct) -> tsink::Result<Vec<u8>> { /* ... */ }
    fn decode(&self, bytes: &[u8]) -> tsink::Result<MyStruct> { /* ... */ }
}

struct MyAggregator;
impl Aggregator<MyStruct> for MyAggregator {
    fn aggregate(&self, values: &[MyStruct]) -> Option<MyStruct> { /* ... */ }
}

let opts = QueryOptions::new(start, end)
    .with_custom_bytes_aggregation(MyCodec, MyAggregator);
```

## Series Discovery

Use `select_series` with matcher-based filtering to discover series dynamically:

```rust
use tsink::{SeriesSelection, SeriesMatcher};

let selection = SeriesSelection::new()
    .with_metric("http_requests")
    .with_matcher(SeriesMatcher::equal("method", "GET"))
    .with_matcher(SeriesMatcher::regex_match("status", "2.."))
    .with_time_range(start, end);

let series = storage.select_series(&selection)?;
```

Supported matcher operators:

| Operator | Constructor | Description |
|---|---|---|
| `=` | `SeriesMatcher::equal(name, value)` | Exact label match. |
| `!=` | `SeriesMatcher::not_equal(name, value)` | Negated exact match. |
| `=~` | `SeriesMatcher::regex_match(name, pattern)` | Regex label match. |
| `!~` | `SeriesMatcher::regex_no_match(name, pattern)` | Negated regex match. |

## Value Model

`DataPoint` stores a `timestamp: i64` and a `value: Value`.

| Variant | Rust type |
|---|---|
| `Value::F64(f64)` | `f64` |
| `Value::I64(i64)` | `i64` |
| `Value::U64(u64)` | `u64` |
| `Value::Bool(bool)` | `bool` |
| `Value::Bytes(Vec<u8>)` | raw bytes |
| `Value::String(String)` | UTF-8 string |

Notes:
- A series (same metric + labels) must keep a consistent value type family.
- `bytes` and `string` data uses blob-lane encoding on disk.
- Convenience conversions are provided: `DataPoint::new(ts, 42.5)` auto-converts via `Into<Value>`.
- Accessor methods: `value.as_f64()`, `value.as_i64()`, `value.as_u64()`, `value.as_bool()`, `value.as_bytes()`, `value.as_str()`.
- `value.kind()` returns a `&'static str` tag: `"f64"`, `"i64"`, `"u64"`, `"bool"`, `"bytes"`, or `"string"`.
- `Value::F64(NAN)` compares equal to itself, unlike standard `f64`, for consistent equality semantics in collections.

Automatic `From` conversions are provided for: `f64`, `i64`, `i32`, `u64`, `u32`, `usize`, `bool`, `Vec<u8>`, `&[u8]`, `String`, and `&str`.

## Label Constraints

Labels are key-value pairs that identify a series alongside the metric name. Labels are automatically sorted for consistent series identity — insertion order does not matter.

| Constraint | Limit |
|---|---|
| Label name length | 256 bytes |
| Label value length | 16,384 bytes (16 KB) |
| Metric name length | 65,535 bytes |

Empty label names or values are rejected. Oversized values are truncated at the marshaling boundary.

## PromQL Engine

Enable with the `promql` feature. The engine supports instant and range queries over data stored in tsink.

```rust
use std::sync::Arc;
use tsink::{StorageBuilder, DataPoint, Row};
use tsink::promql::Engine;

let storage = StorageBuilder::new().build()?;
storage.insert_rows(&[
    Row::new("http_requests_total", DataPoint::new(1_000, 10.0)),
    Row::new("http_requests_total", DataPoint::new(2_000, 25.0)),
    Row::new("http_requests_total", DataPoint::new(3_000, 50.0)),
])?;

let engine = Engine::new(storage.clone());

// Instant query — evaluates at a single point in time.
let result = engine.instant_query("http_requests_total", 3_000)?;

// Range query — evaluates at each step across a time window.
let result = engine.range_query("http_requests_total", 1_000, 3_000, 1_000)?;
```

Use `Engine::with_precision(storage, precision)` if your timestamps are not in nanoseconds.

Supported functions:

| Category | Functions |
|---|---|
| Rate/counter | `rate`, `irate`, `increase` |
| Over-time | `avg_over_time`, `sum_over_time`, `min_over_time`, `max_over_time`, `count_over_time` |
| Math | `abs`, `ceil`, `floor`, `round`, `clamp`, `clamp_min`, `clamp_max` |
| Type conversion | `scalar`, `vector` |
| Time | `time`, `timestamp` |
| Sorting | `sort`, `sort_desc` |
| Label manipulation | `label_replace`, `label_join` |

Aggregation operators: `sum`, `avg`, `min`, `max`, `count`, `topk`, `bottomk` — with `by`/`without` grouping.

Binary operators: `+`, `-`, `*`, `/`, `%`, `^`, `==`, `!=`, `<`, `>`, `<=`, `>=`, `and`, `or`, `unless` — with `on`/`ignoring` vector matching and `bool` modifier.

## Persistence and WAL

Set `with_data_path(...)` to enable persistence:

```rust
use std::time::Duration;
use tsink::{StorageBuilder, WalSyncMode};

let storage = StorageBuilder::new()
    .with_data_path("./tsink-data")
    .with_chunk_points(2048)
    .with_wal_enabled(true)
    .with_wal_sync_mode(WalSyncMode::Periodic(Duration::from_secs(1)))
    .build()?;
```

Behavior:
- `close()` flushes active chunks and writes immutable segment files.
- With WAL enabled, reopening the same path replays durable WAL frames automatically.
- Recovery is idempotent — a high-water mark prevents double-apply of WAL frames.

### Sync Modes

| Mode | Trade-off |
|---|---|
| `WalSyncMode::Periodic(Duration)` | Lower fsync overhead; small recent-write loss window on crash. |
| `WalSyncMode::PerAppend` | Strongest durability for acknowledged writes; higher fsync cost. |

## On-Disk Layout

When persistence is enabled, tsink writes separate numeric/blob lane segment families:

```text
<data_path>/
  lane_numeric/
    segments/
      L0/...
      L1/...
      L2/...
  lane_blob/
    segments/
      L0/...
      L1/...
      L2/...
  wal/
    wal-0000000000000000.log
    wal-0000000000000001.log
    ...
```

Each segment directory contains:
`manifest.bin`, `chunks.bin`, `chunk_index.bin`, `series.bin`, `postings.bin`.

The storage format uses CRC32c and XXH64 checksums for corruption detection and a crash-safe commit protocol (write temps, fsync, rename atomically).

### Compaction

tsink uses tiered LSM-style compaction across three levels:

| Level | Trigger | Description |
|---|---|---|
| L0 | Every flush | Newly flushed segments land here. |
| L1 | 4 L0 segments | L0 segments are merged and re-chunked into L1. |
| L2 | 4 L1 segments | L1 segments are merged into larger L2 segments. |

Compaction runs automatically in the background and is transparent to reads and writes.

## Compression and Encoding

tsink uses two parallel encoding lanes based on value type:

### Numeric Lane

Timestamps and numeric values (`f64`, `i64`, `u64`, `bool`) are encoded with specialized codecs. The encoder tries all applicable candidates and picks the most compact.

**Timestamp codecs:**

| Codec | Strategy |
|---|---|
| Fixed-step RLE | Run-length encoding for fixed-interval timestamps. |
| Delta-of-delta bitpack | Delta-of-delta encoding with bit-packing (primary strategy). |
| Delta varint | Varint-encoded deltas for irregular intervals. |

**Value codecs:**

| Codec | Type | Strategy |
|---|---|---|
| Gorilla XOR | `f64` | Gorilla-style XOR of IEEE 754 floats. |
| Zigzag delta bitpack | `i64` | Zigzag encoding + delta + bit-packing. |
| Delta bitpack | `u64` | Delta encoding + bit-packing. |
| Constant RLE | any numeric | Run-length encoding for constant values. |
| Bool bitpack | `bool` | Bit-level packing (1 bit per value). |

### Blob Lane

`bytes` and `string` values are encoded with delta block compression in a separate blob lane.

## Performance

### Compression

The adaptive codec selection (Gorilla XOR, delta-of-delta, RLE, bitpacking) achieves **~0.68 bytes per data point** for typical `f64` time-series workloads — down from 16 bytes uncompressed (8-byte timestamp + 8-byte value), a **~23x** compression ratio.

### Throughput

Insert throughput (single-series, in-memory):

| Batch size | Latency | Throughput |
|---|---|---|
| 1 | ~1.7 us | ~577K points/sec |
| 10 | ~5.3 us | ~1.89M points/sec |
| 1,000 | ~155 us | ~6.4M points/sec |

Select throughput (single-series, in-memory):

| Result size | Latency | Throughput |
|---|---|---|
| 1 point | ~114 ns | ~8.8M queries/sec |
| 10 points | ~296 ns | ~33.6M points/sec |
| 1,000 points | ~15.4 us | ~64M points/sec |
| 1,000,000 points | ~20.9 ms | ~48M points/sec |

Numbers above are ballpark figures from a single run (`--quick` mode). Run benchmarks on your hardware:

```bash
cargo bench
scripts/measure_bpp.sh quick   # Measure bytes-per-point
scripts/measure_perf.sh quick  # Criterion insert/select matrix
```

## Architecture

```text
┌─────────────────────────────────────────────────────┐
│                    Public API                       │
│   StorageBuilder / Storage / AsyncStorage / PromQL  │
├────────────┬──────────────┬─────────────────────────┤
│  Writers   │   Readers    │       Compactor         │
│ (N threads)│  (concurrent)│   (background merges)   │
├────────────┴──────────────┴─────────────────────────┤
│               Engine (partitioned by time)          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │ Active   │  │ Immutable│  │ Segments │           │
│  │ Chunks   │→ │ Chunks   │→ │ (L0/L1/  │           │
│  │ (memory) │  │ (memory) │  │  L2 disk)│           │
│  └──────────┘  └──────────┘  └──────────┘           │
├─────────────────────────────────────────────────────┤
│  WAL (write-ahead log)  │  Series Registry + Index  │
└─────────────────────────┴───────────────────────────┘
```

Key internals:
- **Time partitions** split data by wall-clock intervals (default: 1 hour).
- **Chunks** group data points (default: 2048 per chunk) with delta-of-delta timestamp encoding and per-lane value encoding (numeric vs. blob).
- **Series registry** maps metric name + label set → series ID, with inverted postings for label-based lookups.
- **Segment files** are immutable, CRC32c + XXH64 checksummed, and consist of: `manifest.bin`, `chunks.bin`, `chunk_index.bin`, `series.bin`, `postings.bin`.
- **Sharded locking** (64 internal shards) reduces write contention under high concurrency.
- **Background flush** periodically converts active chunks into immutable chunks (default: every 250ms).
- **Background compaction** merges segments across levels (default: every 5s).

## StorageBuilder Options

| Method | Default | Description |
|---|---|---|
| `with_data_path(path)` | `None` (in-memory only) | Directory for segment files and WAL. |
| `with_chunk_points(n)` | `2048` | Target data points per chunk before flushing. |
| `with_wal_enabled(bool)` | `true` | Enable/disable write-ahead logging. |
| `with_wal_sync_mode(mode)` | `Periodic(1s)` | WAL fsync policy. |
| `with_wal_size_limit(bytes)` | Unlimited | Hard cap on total WAL bytes across all WAL segments. |
| `with_wal_buffer_size(n)` | 4096 | WAL buffer size in bytes. |
| `with_retention(duration)` | 14 days | Data retention window. |
| `with_retention_enforced(bool)` | `true` | Enforce retention window (`false` keeps data forever). |
| `with_timestamp_precision(p)` | `Nanoseconds` | Timestamp unit (`Seconds`, `Milliseconds`, `Microseconds`, `Nanoseconds`). |
| `with_max_writers(n)` | Available CPUs (cgroup-aware) | Maximum concurrent writer threads. |
| `with_write_timeout(duration)` | 30s | Timeout for write operations. |
| `with_partition_duration(duration)` | 1 hour | Time partition granularity. |
| `with_memory_limit(bytes)` | Unlimited | Hard in-memory budget with admission backpressure before writes. |
| `with_cardinality_limit(series)` | Unlimited | Hard cap on total metric+label series cardinality. |

## Resource Limits and Backpressure

tsink provides three configurable resource limits that protect against unbounded growth:

### Memory Limit

```rust
let storage = StorageBuilder::new()
    .with_memory_limit(512 * 1024 * 1024) // 512 MB
    .build()?;
```

When the memory budget is reached, new writes block until a background flush frees memory. This provides admission backpressure rather than OOM crashes.

### Cardinality Limit

```rust
let storage = StorageBuilder::new()
    .with_cardinality_limit(100_000)
    .build()?;
```

Caps the total number of unique metric + label-set combinations. Writes that would create new series beyond this limit return `TsinkError::CardinalityLimitExceeded`.

### WAL Size Limit

```rust
let storage = StorageBuilder::new()
    .with_wal_size_limit(1024 * 1024 * 1024) // 1 GB
    .build()?;
```

Caps the total WAL bytes on disk. Writes that would exceed this limit return `TsinkError::WalSizeLimitExceeded`.

## Container Support

tsink automatically detects cgroup v1/v2 CPU and memory quotas when running inside containers (Docker, Kubernetes, etc.). This affects:

- **Writer thread count** — defaults to available CPUs within the cgroup quota, not the host CPU count.
- **Rayon thread pool** — sized to respect container limits.

Override with the `TSINK_MAX_CPUS` environment variable:

```bash
TSINK_MAX_CPUS=4 cargo run --example production_example
```

## Error Handling

All fallible operations return `tsink::Result<T>`, which wraps `TsinkError`. Key error variants:

| Error | Cause |
|---|---|
| `InvalidTimeRange` | `start >= end` in a query. |
| `WriteTimeout` | Writer could not acquire a slot within the configured timeout. |
| `MemoryBudgetExceeded` | Write blocked and memory budget was not freed in time. |
| `CardinalityLimitExceeded` | Too many unique series. |
| `WalSizeLimitExceeded` | WAL disk usage reached the configured cap. |
| `ValueTypeMismatch` | Inserting a different value type into an existing series. |
| `OutOfRetention` | Data point timestamp is outside the retention window. |
| `DataCorruption` | Checksum mismatch during segment read. |
| `StorageClosed` | Operation attempted after `close()` was called. |

## Advanced Usage

### Concurrent Operations

The storage handle is `Arc`-based and safe to share across threads:

```rust
use std::sync::Arc;
use std::thread;
use tsink::{DataPoint, Row, StorageBuilder};

let storage = StorageBuilder::new()
    .with_max_writers(8)
    .build()?;

let mut handles = vec![];
for worker_id in 0..8 {
    let storage = storage.clone();
    handles.push(thread::spawn(move || {
        for i in 0..10_000 {
            let row = Row::new(
                format!("worker_{worker_id}"),
                DataPoint::new(1_700_000_000 + i, i as f64),
            );
            storage.insert_rows(&[row]).unwrap();
        }
    }));
}

for handle in handles {
    handle.join().unwrap();
}
```

### Out-of-Order Writes

tsink accepts data points in any order and returns them sorted by timestamp on read:

```rust
use tsink::{DataPoint, Row};

storage.insert_rows(&[
    Row::new("metric", DataPoint::new(1_700_000_500, 5.0)),
    Row::new("metric", DataPoint::new(1_700_000_100, 1.0)),
    Row::new("metric", DataPoint::new(1_700_000_300, 3.0)),
    Row::new("metric", DataPoint::new(1_700_000_200, 2.0)),
])?;

let points = storage.select("metric", &[], 1_700_000_000, 1_700_001_000)?;
// points are returned in chronological order: 1.0, 2.0, 3.0, 5.0
assert!(points.windows(2).all(|w| w[0].timestamp <= w[1].timestamp));
```

### WAL Recovery

After a crash, tsink automatically replays the WAL on the next open:

```rust
use tsink::StorageBuilder;

// First run — data is written and WAL-protected
let storage = StorageBuilder::new()
    .with_data_path("/data/tsink")
    .build()?;
storage.insert_rows(&rows)?;
// Crash happens here — close() was never called

// Next run — recovery is automatic
let storage = StorageBuilder::new()
    .with_data_path("/data/tsink")  // Same path
    .build()?;  // WAL replay happens here

// Previously inserted data is available
let points = storage.select("metric", &[], 0, i64::MAX)?;
```

Recovery is idempotent — a high-water mark ensures WAL frames are never applied twice.

### Multi-Dimensional Label Querying

```rust
use tsink::{DataPoint, Label, Row};

storage.insert_rows(&[
    Row::with_labels(
        "http_requests",
        vec![Label::new("method", "GET"), Label::new("status", "200")],
        DataPoint::new(1_700_000_000, 150.0),
    ),
    Row::with_labels(
        "http_requests",
        vec![Label::new("method", "POST"), Label::new("status", "201")],
        DataPoint::new(1_700_000_000, 25.0),
    ),
])?;

// Query all label combinations for a metric
let all_results = storage.select_all("http_requests", 1_700_000_000, 1_700_000_100)?;
for (labels, points) in all_results {
    println!("Labels: {:?}, Points: {}", labels, points.len());
}

// Discover all known series
let all_series = storage.list_metrics()?;
for series in all_series {
    println!("Metric: {}, Labels: {:?}", series.name, series.labels);
}
```

### Production Configuration

```rust
use std::time::Duration;
use tsink::{StorageBuilder, WalSyncMode, TimestampPrecision};

let storage = StorageBuilder::new()
    .with_data_path("/var/lib/tsink")
    .with_timestamp_precision(TimestampPrecision::Milliseconds)
    .with_retention(Duration::from_secs(30 * 24 * 3600))        // 30 days
    .with_partition_duration(Duration::from_secs(6 * 3600))     // 6-hour partitions
    .with_chunk_points(4096)
    .with_max_writers(16)
    .with_write_timeout(Duration::from_secs(60))
    .with_memory_limit(1024 * 1024 * 1024)                      // 1 GB
    .with_cardinality_limit(500_000)
    .with_wal_sync_mode(WalSyncMode::Periodic(Duration::from_secs(1)))
    .with_wal_buffer_size(16384)                                // 16 KB
    .build()?;
```

## Examples

```bash
cargo run --example basic_usage
cargo run --example persistent_storage
cargo run --example production_example
cargo run --example comprehensive
```

| Example | Description |
|---|---|
| `basic_usage` | Simple insert and select with labels. |
| `persistent_storage` | Disk persistence and WAL recovery. |
| `production_example` | Resource limits, query options, and custom aggregation. |
| `comprehensive` | Multiple features: concurrent ops, retention, downsampling, and aggregation. |

## Benchmarks and Tests

```bash
cargo test                          # Run all tests
cargo test --features promql        # Include PromQL tests
cargo test --features async-storage # Include async tests

cargo bench                         # Run all benchmarks
cargo bench --bench storage_benchmarks -- '^concurrent_rw/' --quick --noplot
```

### Benchmark Suites

| Benchmark | Description |
|---|---|
| `storage_benchmarks` | Criterion-based matrix of insert/select operations at various scales. |
| `workload` | Realistic workload simulation with multiple series, out-of-order writes, and bytes-per-point measurement. |

## Development Scripts

| Script | Description |
|---|---|
| `scripts/measure_perf.sh <quick\|full>` | Run Criterion benchmarks with quick or full sample sizes. |
| `scripts/measure_bpp.sh <quick\|full>` | Measure bytes-per-point compression efficiency. |
| `scripts/check_bench_regression.sh [threshold]` | Detect Criterion benchmark regressions beyond a threshold (default: 50%). |

The `measure_bpp.sh` script accepts environment variables for workload tuning:

| Variable | Description |
|---|---|
| `TSINK_ACTIVE_SERIES` | Number of concurrent series. |
| `TSINK_WARMUP_POINTS` / `TSINK_MEASURE_POINTS` | Points ingested during warmup and measurement phases. |
| `TSINK_BATCH_SIZE` | Insert batch size. |
| `TSINK_OOO_MAX_SECONDS` / `TSINK_OOO_PERMILLE` | Out-of-order write tuning. |
| `TSINK_RETENTION_SECONDS` / `TSINK_PARTITION_SECONDS` | Retention and partition windows. |
| `TSINK_FAIL_ON_TARGET` | Fail if compression target is not met. |

## Project Structure

```text
tsink/
├── src/
│   ├── lib.rs                  # Public API re-exports
│   ├── storage.rs              # Storage trait and StorageBuilder
│   ├── value.rs                # Value types, Codec, Aggregator traits
│   ├── label.rs                # Label handling and marshaling
│   ├── error.rs                # TsinkError and Result type
│   ├── wal.rs                  # WalSyncMode
│   ├── async_storage.rs        # Async facade (feature-gated)
│   ├── cgroup.rs               # Container resource detection
│   ├── concurrency.rs          # Concurrency primitives
│   ├── mmap.rs                 # Memory-mapped I/O
│   ├── engine/
│   │   ├── engine.rs           # Core storage engine
│   │   ├── compactor.rs        # LSM compaction
│   │   ├── segment.rs          # Segment file I/O
│   │   ├── chunk.rs            # Chunk structures
│   │   ├── encoder.rs          # Compression codecs
│   │   ├── query.rs            # Query execution
│   │   ├── series_registry.rs  # Series tracking and indexing
│   │   ├── index.rs            # Index structures
│   │   └── wal.rs              # WAL implementation
│   └── promql/                 # PromQL engine (feature-gated)
│       ├── ast.rs              # Abstract syntax tree
│       ├── lexer.rs            # Tokenizer
│       ├── parser.rs           # Parser
│       └── eval/               # Evaluation engine
├── crates/
│   └── tsink-server/           # Prometheus-compatible network server
├── tests/                      # Integration tests
├── benches/                    # Criterion benchmarks
├── examples/                   # Usage examples
└── scripts/                    # Development and CI scripts
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

```bash
git clone https://github.com/h2337/tsink.git
cd tsink

cargo test                    # Run tests
cargo test --features promql  # Include PromQL tests
cargo bench                   # Run benchmarks
cargo fmt -- --check          # Check formatting
cargo clippy -- -D warnings   # Lint
```

## Minimum Supported Rust Version

Rust **2021 edition**. Tested on stable.

## License

MIT
