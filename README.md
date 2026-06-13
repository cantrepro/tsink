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
- **Concurrent writers** — multiple threads can insert simultaneously.
- **Optional PromQL engine** — instant and range queries with 20+ built-in functions; enable with the `promql` Cargo feature.
- **LSM-style compaction** — tiered L0 → L1 → L2 segment compaction reduces read amplification.
- **Cgroup-aware defaults** — worker thread counts respect container CPU limits.

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
The async API routes requests through bounded queues to dedicated worker threads, while reusing the existing synchronous engine implementation.

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
- `GET /healthz`
- `POST /api/v1/write` (Prometheus remote write)
- `POST /api/v1/read` (Prometheus remote read)

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
    wal.log
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

## Architecture

```text
┌─────────────────────────────────────────────────────┐
│                    Public API                       │
│   StorageBuilder / Storage / AsyncStorage / PromQL  │
├────────────┬──────────────┬─────────────────────────┤
│  Writers   │   Readers    │       Compactor         │
│ (N threads)│  (concurrent)│   (background merges)   │
├────────────┴──────────────┴─────────────────────────┤
│               Engine (partitioned by time)           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │ Active   │  │ Immutable│  │ Segments │          │
│  │ Chunks   │→ │ Chunks   │→ │ (L0/L1/  │          │
│  │ (memory) │  │ (memory) │  │  L2 disk)│          │
│  └──────────┘  └──────────┘  └──────────┘          │
├─────────────────────────────────────────────────────┤
│  WAL (write-ahead log)  │  Series Registry + Index  │
└─────────────────────────┴───────────────────────────┘
```

Key internals:
- **Time partitions** split data by wall-clock intervals (default: 1 hour).
- **Chunks** group data points (default: 2048 per chunk) with delta-of-delta timestamp encoding and per-lane value encoding (numeric vs. blob).
- **Series registry** maps metric name + label set → series ID, with inverted postings for label-based lookups.
- **Segment files** are immutable, CRC32c + XXH64 checksummed, and consist of: `manifest.bin`, `chunks.bin`, `chunk_index.bin`, `series.bin`, `postings.bin`.

## StorageBuilder Options

| Method | Default | Description |
|---|---|---|
| `with_data_path(path)` | `None` (in-memory only) | Directory for segment files and WAL. |
| `with_chunk_points(n)` | `2048` | Target data points per chunk before flushing. |
| `with_wal_enabled(bool)` | `true` | Enable/disable write-ahead logging. |
| `with_wal_sync_mode(mode)` | `Periodic(1s)` | WAL fsync policy. |
| `with_retention(duration)` | 14 days | Data retention window. |
| `with_retention_enforced(bool)` | `true` | Enforce retention window (`false` keeps data forever). |
| `with_timestamp_precision(p)` | `Nanoseconds` | Timestamp unit (`Seconds`, `Milliseconds`, `Microseconds`, `Nanoseconds`). |
| `with_max_writers(n)` | Available CPUs (cgroup-aware) | Maximum concurrent writer threads. |
| `with_write_timeout(duration)` | 30s | Timeout for write operations. |
| `with_partition_duration(duration)` | 1 hour | Time partition granularity. |
| `with_wal_buffer_size(n)` | 4096 | WAL buffer size in bytes. |

## Examples

```bash
cargo run --example basic_usage
cargo run --example persistent_storage
cargo run --example production_example
cargo run --example comprehensive
```

## Benchmarks and Tests

```bash
cargo test
cargo bench
scripts/measure_bpp.sh quick   # Quick bytes-per-point measurement
scripts/measure_bpp.sh full    # Full bytes-per-point measurement
```

## Minimum Supported Rust Version

Rust **2021 edition**. Tested on stable.

## License

MIT
