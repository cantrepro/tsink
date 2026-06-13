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
- **Optional PromQL engine** — enable with the `promql` Cargo feature.

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

## Query APIs

| Method | Description |
|---|---|
| `select(metric, labels, start, end)` | Returns points sorted by timestamp for one series. |
| `select_into(metric, labels, start, end, &mut buf)` | Same as `select`, but writes into a caller-provided buffer for allocation reuse. |
| `select_all(metric, start, end)` | Returns grouped results for all label sets of a metric. |
| `select_with_options(metric, QueryOptions)` | Supports downsampling, aggregation, custom bytes aggregation, and pagination. |
| `list_metrics()` | Lists all known metric + label-set series. |

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

The storage format uses CRC32c and XXH64 checksums for corruption detection and a crash-safe commit protocol (write temps, fsync, rename atomically). See [`docs/storage.md`](docs/storage.md) for the full binary format specification.

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
| `with_max_writers(n)` | CPU count | Maximum concurrent writer threads. |
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

## License

MIT
