# Embedded library guide

Use tsink as an in-process time-series engine by adding it as a Rust dependency.
You get WAL durability, compaction, tiered retention, and queries — no separate process needed.

```toml
[dependencies]
tsink = "0.10"
```

---

## Quick start

```rust
use std::sync::Arc;
use tsink::{DataPoint, Label, Row, Storage, StorageBuilder, TimestampPrecision};

fn main() -> tsink::Result<()> {
    let storage = StorageBuilder::new()
        .with_data_path("./tsink-data")
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()?;

    // Write a single point.
    storage.insert_rows(&[
        Row::new("cpu_usage", DataPoint::new(1_700_000_000_000_i64, 42.0)),
    ])?;

    // Write with labels.
    storage.insert_rows(&[
        Row::with_labels(
            "http_requests_total",
            vec![Label::new("method", "GET"), Label::new("status", "200")],
            DataPoint::new(1_700_000_000_000_i64, 1027.0),
        ),
    ])?;

    // Read back.
    let points = storage.select("cpu_usage", &[], 0, i64::MAX)?;
    println!("{points:?}");

    storage.close()?;
    Ok(())
}
```

---

## StorageBuilder

`StorageBuilder` configures and constructs a `Storage` instance.
Every setting has a sensible default; only `with_data_path` is required for persistent storage.

```rust
let storage = StorageBuilder::new()
    .with_data_path("/var/lib/tsink")
    .with_retention(Duration::from_secs(30 * 24 * 3600))   // 30 days
    .with_timestamp_precision(TimestampPrecision::Milliseconds)
    .with_memory_limit(512 * 1024 * 1024)                  // 512 MiB
    .with_cardinality_limit(1_000_000)
    .build()?;
```

### Data & persistence

| Method | Default | Description |
|---|---|---|
| `with_data_path(path)` | *none* | Directory for WAL, segments, and metadata. Required for persistence. |
| `with_object_store_path(path)` | *none* | Directory (or object-store prefix) for warm/cold tier segments. |

### Retention

| Method | Default | Description |
|---|---|---|
| `with_retention(duration)` | 14 days | Global retention window. Data older than this is eligible for removal. |
| `with_retention_enforced(bool)` | `false` | When `true`, out-of-retention writes are rejected immediately. |
| `with_tiered_retention_policy(hot, warm)` | *none* | Set distinct retention durations for the hot and warm tiers. |

### Timestamp precision

| Method | Default | Description |
|---|---|---|
| `with_timestamp_precision(precision)` | `Nanoseconds` | Interpret ingested timestamps as one of `Nanoseconds`, `Microseconds`, `Milliseconds`, or `Seconds`. |

### Chunks & partitions

| Method | Default | Description |
|---|---|---|
| `with_chunk_points(n)` | 2048 | Number of data points per in-memory chunk before sealing. |
| `with_partition_duration(duration)` | 1 hour | Time range covered by each partition. Affects compaction and query granularity. |
| `with_max_active_partition_heads_per_series(n)` | 8 | Maximum concurrently active partition heads per series. Controls out-of-order write fanout. |

### Memory & cardinality

| Method | Default | Description |
|---|---|---|
| `with_memory_limit(bytes)` | `usize::MAX` | Memory budget for in-memory data. Exceeding this triggers admission backpressure. |
| `with_cardinality_limit(series)` | `usize::MAX` | Maximum number of unique series. New series are rejected once the limit is reached. |

### Concurrency

| Method | Default | Description |
|---|---|---|
| `with_max_writers(n)` | cgroup-detected CPU count | Parallel writer threads for ingestion. |
| `with_write_timeout(duration)` | 30 s | Maximum time a write waits for a writer slot before returning `WriteTimeout`. |

### WAL

| Method | Default | Description |
|---|---|---|
| `with_wal_enabled(bool)` | `true` | Enable or disable the write-ahead log. Disabling trades durability for speed. |
| `with_wal_size_limit(bytes)` | *unlimited* | Cap total WAL size on disk. Exceeding this returns `WalSizeLimitExceeded`. |
| `with_wal_buffer_size(size)` | *default* | In-memory buffer size for WAL frame batching before flush. |
| `with_wal_sync_mode(mode)` | `PerAppend` | Durability policy — see [WAL sync modes](#wal-sync-modes). |
| `with_wal_replay_mode(mode)` | `Strict` | Corruption handling during WAL replay — see [WAL replay modes](#wal-replay-modes). |

### Remote segments

| Method | Default | Description |
|---|---|---|
| `with_remote_segment_cache_policy(policy)` | `MetadataOnly` | Caching strategy for remote tier segments. |
| `with_remote_segment_refresh_interval(duration)` | *default* | How often to refresh the remote segment catalog. |
| `with_mirror_hot_segments_to_object_store(bool)` | `false` | Copy hot-tier segments to the object store for disaster recovery. |

### Runtime

| Method | Default | Description |
|---|---|---|
| `with_runtime_mode(mode)` | `ReadWrite` | `ReadWrite` for local persistence, `ComputeOnly` for remote-only metadata. |
| `with_background_fail_fast(bool)` | `true` | Halt the engine on unrecoverable background errors (compaction, flush). |
| `with_metadata_shard_count(n)` | *auto* | Number of metadata shards for series routing. |

---

## Writing data

### Value types

tsink supports multiple value types through the `Value` enum:

```rust
use tsink::Value;

Value::F64(42.0);
Value::I64(-1);
Value::U64(100);
Value::Bool(true);
Value::Bytes(vec![0xCA, 0xFE]);
Value::String("hello".into());
Value::Histogram(Box::new(histogram));   // NativeHistogram
```

`DataPoint::new(timestamp, value)` accepts anything that implements `Into<Value>`.
For convenience, `f64` and `i64` convert automatically:

```rust
DataPoint::new(ts, 42.0);   // Value::F64
DataPoint::new(ts, -1_i64); // Value::I64
```

### Batch inserts

Pass a slice of `Row` values to `insert_rows`:

```rust
let rows: Vec<Row> = metrics
    .iter()
    .map(|m| Row::with_labels(&m.name, m.labels.clone(), DataPoint::new(m.ts, m.value)))
    .collect();

storage.insert_rows(&rows)?;
```

### Write acknowledgement

Use `insert_rows_with_result` to inspect durability guarantees:

```rust
let result = storage.insert_rows_with_result(&rows)?;

match result.acknowledgement {
    WriteAcknowledgement::Durable  => { /* crash-safe */ }
    WriteAcknowledgement::Appended => { /* in WAL buffer, not yet fsync'd */ }
    WriteAcknowledgement::Volatile => { /* in memory only */ }
}
```

The acknowledgement level depends on the configured WAL sync mode:

- **`PerAppend`** — every write returns `Durable`.
- **`Periodic(interval)`** — writes return `Appended`; they become durable after the next periodic sync.
- **WAL disabled** — writes return `Volatile`.

---

## Querying data

### Simple select

Retrieve all points for a metric within a time range:

```rust
let points: Vec<DataPoint> = storage.select("cpu_usage", &[], start, end)?;

for p in &points {
    println!("t={} v={:?}", p.timestamp, p.value);
}
```

Pass labels to select a specific series:

```rust
let labels = vec![Label::new("host", "web-1")];
let points = storage.select("cpu_usage", &labels, start, end)?;
```

### Select all label combinations

```rust
let all: Vec<(Vec<Label>, Vec<DataPoint>)> =
    storage.select_all("http_requests_total", start, end)?;

for (labels, points) in &all {
    println!("{labels:?} => {} points", points.len());
}
```

### Select multiple series at once

```rust
use tsink::{MetricSeries, SeriesPoints};

let series = vec![
    MetricSeries { name: "cpu_usage".into(), labels: vec![Label::new("host", "web-1")] },
    MetricSeries { name: "cpu_usage".into(), labels: vec![Label::new("host", "web-2")] },
];

let results: Vec<SeriesPoints> = storage.select_many(&series, start, end)?;
```

### Advanced queries with QueryOptions

`select_with_options` provides aggregation, downsampling, and pagination:

```rust
use tsink::{Aggregation, QueryOptions};

let opts = QueryOptions::new(start, end)
    .with_labels(vec![Label::new("host", "web-1")])
    .with_aggregation(Aggregation::Avg)
    .with_downsample(60_000, Aggregation::Sum)   // 1-minute buckets
    .with_pagination(0, Some(1000));              // first 1000 points

let points = storage.select_with_options("cpu_usage", opts)?;
```

Available aggregations:

| Aggregation | Description |
|---|---|
| `None` | No aggregation (default) |
| `Sum` | Sum of values |
| `Min` / `Max` | Minimum / maximum |
| `Avg` | Arithmetic mean |
| `First` / `Last` | First / last point in window |
| `Count` | Number of points |
| `Median` | Median value |
| `Range` | Max − Min |
| `Variance` / `StdDev` | Statistical variance / standard deviation |

### Paginated row scanning

For large result sets, use cursor-based scanning:

```rust
use tsink::QueryRowsScanOptions;

let mut offset = None;
loop {
    let page = storage.scan_metric_rows(
        "cpu_usage", start, end,
        QueryRowsScanOptions { max_rows: Some(10_000), row_offset: offset },
    )?;

    process(&page.rows);

    if page.truncated {
        offset = page.next_row_offset;
    } else {
        break;
    }
}
```

### Series discovery

List all known series:

```rust
let all_series: Vec<MetricSeries> = storage.list_metrics()?;
```

Filter with matchers:

```rust
use tsink::{SeriesMatcher, SeriesMatcherOp, SeriesSelection};

let selection = SeriesSelection::new()
    .with_metric("http_requests_total")
    .with_matcher(SeriesMatcher::equal("method", "GET"))
    .with_matcher(SeriesMatcher::regex_match("status", "2.."))
    .with_time_range(start, end);

let matched: Vec<MetricSeries> = storage.select_series(&selection)?;
```

Matcher operators:

| Constructor | Operator | Example |
|---|---|---|
| `SeriesMatcher::equal(name, value)` | `=` | `method="GET"` |
| `SeriesMatcher::not_equal(name, value)` | `!=` | `status!="500"` |
| `SeriesMatcher::regex_match(name, pattern)` | `=~` | `host=~"web-.*"` |
| `SeriesMatcher::regex_no_match(name, pattern)` | `!~` | `env!~"staging\|dev"` |

---

## Deleting series

```rust
let selection = SeriesSelection::new()
    .with_metric("deprecated_metric");

let result = storage.delete_series(&selection)?;
println!("matched={} tombstones={}", result.matched_series, result.tombstones_applied);
```

Deletion is tombstone-based. Tombstones are merged during compaction.

---

## Rollup policies

Define materialized downsampled views that the engine maintains automatically:

```rust
use tsink::RollupPolicy;

let policies = vec![
    RollupPolicy {
        id: "cpu_5m_avg".into(),
        metric: "cpu_usage".into(),
        match_labels: vec![],
        interval: 300_000,             // 5 minutes in ms
        aggregation: Aggregation::Avg,
        bucket_origin: 0,
    },
];

let snapshot = storage.apply_rollup_policies(policies)?;
for status in &snapshot.policies {
    println!("{}: materialized through {:?}", status.policy.id, status.materialized_through);
}
```

Trigger an immediate rollup run:

```rust
let snapshot = storage.trigger_rollup_run()?;
```

---

## Snapshots

Create an atomic, point-in-time snapshot of the entire database:

```rust
use std::path::Path;

storage.snapshot(Path::new("/backups/tsink-2026-03-12"))?;
```

Restore from a snapshot:

```rust
StorageBuilder::restore_from_snapshot("/backups/tsink-2026-03-12", "/var/lib/tsink")?;
```

---

## Async API

`AsyncStorage` wraps the sync engine with a dedicated thread pool and async channels,
making it safe to use from a Tokio runtime without blocking the executor.

```rust
use tsink::{AsyncStorageBuilder, TimestampPrecision};

#[tokio::main]
async fn main() -> tsink::Result<()> {
    let storage = AsyncStorageBuilder::new()
        .with_data_path("./tsink-data")
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .with_queue_capacity(2048)
        .with_read_workers(4)
        .build()?;

    storage.insert_rows(vec![
        Row::new("cpu_usage", DataPoint::new(1_700_000_000_000_i64, 42.0)),
    ]).await?;

    let points = storage.select("cpu_usage", vec![], 0, i64::MAX).await?;
    println!("{points:?}");

    storage.close().await?;
    Ok(())
}
```

### Wrapping an existing Storage

```rust
use tsink::{AsyncStorage, AsyncRuntimeOptions, StorageBuilder};

let sync_storage = StorageBuilder::new()
    .with_data_path("./tsink-data")
    .build()?;

let async_storage = AsyncStorage::from_storage_with_options(
    sync_storage,
    AsyncRuntimeOptions {
        queue_capacity: 4096,
        read_workers: 8,
    },
)?;
```

### Async-specific options

| Option | Default | Description |
|---|---|---|
| `queue_capacity` | 1024 | Maximum in-flight requests per internal queue. |
| `read_workers` | cgroup CPU count | Number of dedicated reader threads. |

All async methods mirror the sync API. The underlying `Arc<dyn Storage>` is accessible
via `inner()` or recoverable with `into_inner()`.

---

## Custom codecs and aggregators

Store and aggregate arbitrary types by implementing `Codec` and `Aggregator`:

```rust
use tsink::{Codec, Aggregator, Value, CodecAggregator, QueryOptions, Aggregation};

struct MyCodec;

impl Codec for MyCodec {
    type Item = MyStruct;
    fn encode(&self, value: &MyStruct) -> tsink::Result<Vec<u8>> {
        Ok(serde_json::to_vec(value).unwrap())
    }
    fn decode(&self, bytes: &[u8]) -> tsink::Result<MyStruct> {
        Ok(serde_json::from_slice(bytes).unwrap())
    }
}

struct MyAggregator;

impl Aggregator<MyStruct> for MyAggregator {
    fn aggregate(&self, values: &[MyStruct]) -> Option<MyStruct> {
        // your merge logic
        values.first().cloned()
    }
}

// Encode a value for storage.
let encoded = Value::encode_with(&my_struct, &MyCodec)?;
storage.insert_rows(&[Row::new("custom_metric", DataPoint::new(ts, encoded))])?;

// Query with custom aggregation.
let bridge = CodecAggregator::new(MyCodec, MyAggregator);
let opts = QueryOptions::new(start, end)
    .with_custom_bytes_aggregation(MyCodec, MyAggregator);
let points = storage.select_with_options("custom_metric", opts)?;

// Decode results.
for p in &points {
    let item: MyStruct = p.value.decode_with(&MyCodec)?;
}
```

---

## WAL sync modes

| Mode | Acknowledgement | Trade-off |
|---|---|---|
| `WalSyncMode::PerAppend` (default) | `Durable` | Every write is fsync'd before returning. Crash-safe but higher latency. |
| `WalSyncMode::Periodic(interval)` | `Appended` | Writes are buffered and fsync'd on a timer. Lower latency; a crash may lose up to one interval of writes. |

```rust
use std::time::Duration;
use tsink::WalSyncMode;

StorageBuilder::new()
    .with_data_path("./data")
    .with_wal_sync_mode(WalSyncMode::Periodic(Duration::from_secs(1)))
    .build()?;
```

## WAL replay modes

| Mode | Behaviour |
|---|---|
| `WalReplayMode::Strict` (default) | Abort replay on any corruption. Use for production where data integrity is paramount. |
| `WalReplayMode::Salvage` | Skip corrupted frames/segments and continue. Useful for disaster recovery when some data loss is acceptable. |

---

## Observability

Inspect engine internals at runtime:

```rust
let snap = storage.observability_snapshot();

println!("memory: {} / {} bytes",
    snap.memory.active_and_sealed_bytes,
    snap.memory.budgeted_bytes);

println!("WAL: {} segments, {} bytes",
    snap.wal.segment_count, snap.wal.size_bytes);

println!("compaction: {} runs, {} errors",
    snap.compaction.runs_total, snap.compaction.errors_total);

println!("queries: {} selects",
    snap.query.select_calls_total);

if snap.health.degraded {
    eprintln!("engine degraded: {:?}", snap.health.last_background_error);
}
```

The snapshot covers memory, WAL, retention, flush pipeline, compaction, queries, rollups,
remote storage, and overall health.

---

## Error handling

All fallible operations return `tsink::Result<T>`, which is `Result<T, TsinkError>`.
Key error variants to handle:

| Variant | When it occurs |
|---|---|
| `MemoryBudgetExceeded` | Write would push memory usage past the configured limit. |
| `CardinalityLimitExceeded` | A new series would exceed the cardinality cap. |
| `WalSizeLimitExceeded` | WAL on-disk size exceeds the configured limit. |
| `WriteTimeout` | No writer slot became available within `write_timeout`. |
| `InvalidTimeRange` | `start > end` in a query. |
| `InvalidMetricName` / `InvalidLabel` | Metric or label name/value violates naming rules. |
| `StorageClosed` / `StorageShuttingDown` | Operation attempted after `close()`. |
| `DataCorruption` | Segment or WAL integrity check failed. |
| `InsufficientDiskSpace` | Not enough disk space for WAL or segment writes. |
| `OutOfRetention` | Write timestamp falls outside the retention window (when enforcement is enabled). |

---

## Labels & naming rules

- Metric names: up to 65 535 bytes, validated on write.
- Label names: up to 256 bytes.
- Label values: up to 16 384 bytes.
- Labels are automatically sorted by name to produce a canonical series identity.

---

## Lifecycle

1. **Build** — `StorageBuilder::new()...build()` opens (or creates) the data directory, replays the WAL, and starts background threads.
2. **Use** — `insert_rows`, `select`, `select_with_options`, etc.  The `Storage` trait object is `Send + Sync` and safe to share across threads via `Arc`.
3. **Close** — `storage.close()` flushes remaining data, syncs the WAL, and shuts down background workers. Calling any method after `close()` returns `StorageClosed`.
