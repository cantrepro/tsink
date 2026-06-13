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

> `0.9.0` upgrade note: persistent on-disk data from `0.8.x` is not wire-compatible with `0.9.0` (`STORAGE_FORMAT_VERSION=2` with disk-backed series index + roaring postings).

## Features

- **Embedded API** — no external server, network protocol, or daemon required.
- **Thread-safe** — the storage handle is an `Arc<dyn Storage>`, safe to share across threads.
- **Multi-series model** — series identity is metric name + exact label set.
- **Typed values** — `f64`, `i64`, `u64`, `bool`, `bytes`, and `string`.
- **Rich queries** — downsampling, aggregation (12 built-in functions), pagination, and custom bytes aggregation via the `Codec`/`Aggregator` traits.
- **Disk persistence (v2)** — immutable segment files + disk-backed global `series_index.bin` and roaring-bitmaps postings.
- **WAL durability** — selectable sync mode (`Periodic` or `PerAppend`) and replay policy (`Salvage` default or `Strict`).
- **Atomic snapshot/restore** — create segment-consistent, WAL-aware backups and restore them atomically.
- **Out-of-order writes** — data is returned sorted by timestamp regardless of insertion order.
- **Concurrent writers** — multiple threads can insert simultaneously with sharded internal locking.
- **Optional PromQL engine** — instant and range queries with 20+ built-in functions; enable with the `promql` Cargo feature.
- **LSM-style compaction** — tiered L0 → L1 → L2 segment compaction reduces read amplification.
- **Adaptive compression** — Gorilla/delta codecs plus payload-level zstd compression when beneficial.
- **Observability snapshots** — structured WAL/flush/compaction/query counters and health state via `observability_snapshot()`.
- **Single-process safety** — per-`data_path` lock file (`.tsink.lock`) blocks concurrent writers in multiple processes.
- **cgroup-aware defaults** — worker thread defaults respect container CPU quotas.
- **Resource limits** — configurable memory budget, series cardinality cap, and WAL size limit with admission backpressure.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Async Usage](#async-usage)
- [Server Mode](#server-mode-prometheus-wire-compatible)
- [Python Bindings](#python-bindings)
- [Query APIs](#query-apis)
- [Series Discovery](#series-discovery)
- [Value Model](#value-model)
- [Label Constraints](#label-constraints)
- [PromQL Engine](#promql-engine)
- [Persistence and WAL](#persistence-and-wal)
- [Observability](#observability)
- [Snapshots and Restore](#snapshots-and-restore)
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
tsink = "0.9"
```

Enable PromQL support:

```toml
[dependencies]
tsink = { version = "0.9", features = ["promql"] }
```

Enable async storage facade (dedicated worker threads, runtime-agnostic futures):

```toml
[dependencies]
tsink = { version = "0.9", features = ["async-storage"] }
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

`AsyncStorageBuilder` forwards all core `StorageBuilder` configuration, including WAL replay mode (`with_wal_replay_mode`) and background fail-fast (`with_background_fail_fast`).

`AsyncStorage` also provides synchronous accessors for introspection:

| Method | Description |
|---|---|
| `memory_used()` | Current in-memory usage in bytes. |
| `memory_budget()` | Configured memory budget. |
| `snapshot(path).await` | Create an atomic snapshot using the async write worker. |
| `inner()` | Access the underlying `Arc<dyn Storage>`. |
| `into_inner(self)` | Unwrap the underlying storage handle. |

## Server Mode (Prometheus Wire Compatible)

> **Experimental:** tsink-server is still experimental and under development.

This workspace includes a binary crate at `crates/tsink-server` that runs tsink as an async network service (tokio-based) with Prometheus remote storage wire format, PromQL HTTP API, TLS, and Bearer token authentication.

Run the server:

```bash
cargo run -p tsink-server -- server --listen 127.0.0.1:9201 --data-path ./tsink-data
```

### CLI Options

| Flag | Default | Description |
|---|---|---|
| `--listen <ADDR>` | `127.0.0.1:9201` | Bind address. |
| `-V, --version` | — | Print version and exit. |
| `--data-path <PATH>` | None (in-memory) | Persist tsink data under PATH. |
| `--wal-enabled <BOOL>` | `true` | Enable WAL. |
| `--no-wal` | — | Disable WAL (shorthand). |
| `--timestamp-precision <s\|ms\|us\|ns>` | `ms` | Timestamp precision (server defaults to milliseconds). |
| `--retention <DURATION>` | 14d | Data retention period (e.g. `14d`, `720h`). |
| `--memory-limit <BYTES>` | Unlimited | Memory budget (e.g. `1G`, `1073741824`). |
| `--cardinality-limit <N>` | Unlimited | Max unique series. |
| `--chunk-points <N>` | 2048 | Target points per chunk. |
| `--max-writers <N>` | Available CPUs | Concurrent writer threads. |
| `--wal-sync-mode <MODE>` | `periodic` | WAL fsync policy (`per-append` or `periodic`). |
| `--tls-cert <PATH>` | — | TLS certificate file (PEM). Requires `--tls-key`. |
| `--tls-key <PATH>` | — | TLS private key file (PEM). Requires `--tls-cert`. |
| `--auth-token <TOKEN>` | — | Require Bearer token on all endpoints except health probes. |
| `--enable-admin-api` | `false` | Enable admin snapshot/restore endpoints. |
| `--admin-path-prefix <PATH>` | — | Restrict admin snapshot/restore paths under a canonical root. Requires `--enable-admin-api`. |

### Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/healthz` | Health check (returns `ok`). |
| GET | `/ready` | Readiness probe (returns `ready`). |
| GET | `/metrics` | Self-monitoring metrics (Prometheus exposition format). |
| GET/POST | `/api/v1/query` | PromQL instant query. |
| GET/POST | `/api/v1/query_range` | PromQL range query. |
| GET | `/api/v1/series` | Series metadata (accepts `match[]` selectors). |
| GET | `/api/v1/labels` | All label names. |
| GET | `/api/v1/label/<name>/values` | Values for a given label. |
| POST | `/api/v1/write` | Prometheus remote write (protobuf + snappy). |
| POST | `/api/v1/read` | Prometheus remote read (protobuf + snappy). |
| POST | `/api/v1/import/prometheus` | Prometheus text exposition format ingestion. |
| GET | `/api/v1/status/tsdb` | TSDB stats (JSON). |
| POST | `/api/v1/admin/snapshot` | Admin-only endpoint (disabled by default): create snapshot (`{\"path\":\"...\"}`). |
| POST | `/api/v1/admin/restore` | Admin-only endpoint (disabled by default): restore snapshot (`{\"snapshotPath\":\"...\",\"dataPath\":\"...\"}`). |
| POST | `/api/v1/admin/delete_series` | Admin-only endpoint (disabled by default, currently stubbed with `501`). |

### TLS

Provide both `--tls-cert` and `--tls-key` to enable TLS:

```bash
cargo run -p tsink-server -- server \
  --tls-cert /path/to/cert.pem \
  --tls-key /path/to/key.pem
```

### Authentication

When `--auth-token` is set, all requests except `GET /healthz` and `GET /ready` must include the header `Authorization: Bearer <TOKEN>`. Unauthenticated requests receive a `401 Unauthorized` response.

### Admin API

Admin endpoints are disabled by default. Enabling requires both `--enable-admin-api` and `--auth-token`:

```bash
cargo run -p tsink-server -- server \
  --data-path ./tsink-data \
  --auth-token secret-token \
  --enable-admin-api
```

To constrain snapshot/restore destinations to a fixed root:

```bash
cargo run -p tsink-server -- server \
  --data-path ./tsink-data \
  --auth-token secret-token \
  --enable-admin-api \
  --admin-path-prefix /srv/tsink-admin
```

### Graceful Shutdown

The server handles `SIGTERM` and `SIGINT` signals. On receipt it stops accepting new connections, waits up to 10 seconds for in-flight requests to complete, then closes storage cleanly.

### PromQL HTTP API

The query endpoints follow the [Prometheus HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) response format:

```bash
# Instant query
curl 'http://localhost:9201/api/v1/query?query=up&time=1700000000'

# Range query
curl 'http://localhost:9201/api/v1/query_range?query=up&start=1700000000&end=1700000060&step=15s'
```

### Prometheus Integration

```yaml
remote_write:
  - url: http://127.0.0.1:9201/api/v1/write

remote_read:
  - url: http://127.0.0.1:9201/api/v1/read
```

### Text Format Ingestion

Post Prometheus exposition format text directly:

```bash
curl -X POST http://localhost:9201/api/v1/import/prometheus \
  -H 'Content-Type: text/plain' \
  --data-binary @metrics.txt
```

## Python Bindings

The [`tsink_uniffi`](https://pypi.org/project/tsink-uniffi/) package provides Python bindings for tsink via [UniFFI](https://mozilla.github.io/uniffi-rs/). It exposes the core storage engine to Python with native performance.

### Installation

```bash
pip install tsink_uniffi
```

### Quick Start

```python
import time
from tsink_uniffi import (
    TsinkStorageBuilder,
    UDataPoint,
    ULabel,
    URow,
    UValue,
    UTimestampPrecision,
    UAggregation,
    UQueryOptions,
)

# Build an in-memory store
builder = TsinkStorageBuilder()
builder.with_timestamp_precision(UTimestampPrecision.SECONDS)
builder.with_memory_limit(64 * 1024 * 1024)  # 64 MB
db = builder.build()

now = int(time.time())

# Insert rows
db.insert_rows([
    URow(
        metric="cpu_usage",
        labels=[
            ULabel(name="host", value="server-1"),
            ULabel(name="region", value="us-east"),
        ],
        data_point=UDataPoint(
            value=UValue.F64(45.0),
            timestamp=now,
        ),
    ),
])

# Query a single series
points = db.select(
    metric="cpu_usage",
    labels=[
        ULabel(name="host", value="server-1"),
        ULabel(name="region", value="us-east"),
    ],
    start=now - 60,
    end=now + 1,
)
for p in points:
    print(f"ts={p.timestamp}  value={p.value}")
```

### Series Discovery and Aggregation

```python
# List all known metric + label-set combinations
for m in db.list_metrics():
    label_str = ", ".join(f"{l.name}={l.value}" for l in m.labels)
    print(f"  {m.name} {{ {label_str} }}")

# Query all label sets for a metric
for series in db.select_all(metric="cpu_usage", start=now - 60, end=now + 1):
    label_str = ", ".join(f"{l.name}={l.value}" for l in series.labels)
    print(f"  {{ {label_str} }}  →  {len(series.data_points)} points")

# Aggregation query
avg_points = db.select_with_options(
    metric="cpu_usage",
    options=UQueryOptions(
        labels=[ULabel(name="host", value="server-1")],
        start=now - 60,
        end=now + 1,
        aggregation=UAggregation.AVG,
        downsample=None,
        limit=None,
        offset=0,
    ),
)
```

### Memory Introspection

```python
print(f"Memory used:   {db.memory_used():,} bytes")
print(f"Memory budget: {db.memory_budget():,} bytes")

db.close()
```

The Python API mirrors the Rust API — see the sections below for details on query options, aggregation functions, and value types.

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
Matcher-based `select_series` queries are resolved via persisted inverted postings (roaring bitmaps) when available, not by full scan.

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

Empty label names or values are rejected. Oversized metric/label names and values are rejected with validation errors (`InvalidMetricName` / `InvalidLabel`); they are not silently truncated on write.

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

Recent `0.9.0` alignment updates include metricless-selector matcher semantics, stricter one-to-one vector matching for binary ops, and metric-name dropping behavior for vector binary results.

## Persistence and WAL

Set `with_data_path(...)` to enable persistence:

```rust
use std::time::Duration;
use tsink::{StorageBuilder, WalReplayMode, WalSyncMode};

let storage = StorageBuilder::new()
    .with_data_path("./tsink-data")
    .with_chunk_points(2048)
    .with_wal_enabled(true)
    .with_wal_sync_mode(WalSyncMode::Periodic(Duration::from_secs(1)))
    .with_wal_replay_mode(WalReplayMode::Salvage) // default
    .build()?;
```

Behavior:
- `close()` flushes active chunks and writes immutable segment files.
- With WAL enabled, reopening the same path replays durable WAL frames automatically.
- Recovery is idempotent — a high-water mark prevents double-apply of WAL frames.
- Persistent flush/compaction maintenance runs for `data_path` storage even when WAL is disabled.
- A per-path process lock file (`.tsink.lock`) prevents concurrent opens of the same `data_path`.

### Sync Modes

| Mode | Trade-off |
|---|---|
| `WalSyncMode::Periodic(Duration)` | Lower fsync overhead; small recent-write loss window on crash. |
| `WalSyncMode::PerAppend` | Strongest durability for acknowledged writes; higher fsync cost. |

### Replay Modes

| Mode | Behavior on mid-log WAL corruption/truncation |
|---|---|
| `WalReplayMode::Salvage` (default) | Replays valid prefix and stops at first broken frame. |
| `WalReplayMode::Strict` | Fails startup replay immediately with corruption error. |

## Observability

Use `observability_snapshot()` to inspect structured runtime internals:

```rust
let obs = storage.observability_snapshot();
println!("wal bytes={}", obs.wal.size_bytes);
println!("flush runs={}", obs.flush.pipeline_runs_total);
println!("compaction runs={}", obs.compaction.runs_total);
println!("select calls={}", obs.query.select_calls_total);
println!("degraded={}", obs.health.degraded);
```

`obs.health` also exposes `background_errors_total`, `fail_fast_enabled`, `fail_fast_triggered`, and `last_background_error`.

## Snapshots and Restore

Create an atomic, segment-consistent, WAL-aware snapshot from a live storage:

```rust
storage.snapshot(std::path::Path::new("/backups/tsink-snap-001"))?;
```

Restore atomically into another data directory:

```rust
tsink::StorageBuilder::restore_from_snapshot(
    std::path::Path::new("/backups/tsink-snap-001"),
    std::path::Path::new("/data/tsink-restore"),
)?;
```

`restore_from_snapshot` replaces the destination through a staged publish (with rollback on activation failure) and preserves WAL replay semantics on reopen.

## On-Disk Layout

When persistence is enabled, tsink writes separate numeric/blob lane segment families plus a global series index:

```text
<data_path>/
  series_index.bin
  .tsink.lock
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
  wal/                      # Present when WAL is enabled
    wal-0000000000000000.log
    wal-0000000000000001.log
    ...
```

Each segment directory contains:
`manifest.bin`, `chunks.bin`, `chunk_index.bin`, `series.bin`, `postings.bin`.

Storage format notes:
- `0.9.0` uses segment format v2 (`TSM2/CHK2/CID2/SRS2/PST2` headers).
- `postings.bin` stores roaring bitmap payloads for matcher acceleration.
- `chunks.bin` records may be stored uncompressed or zstd-compressed (flagged per chunk).
- CRC32 + XXH64 checksums and crash-safe staged publish (`write tmp -> fsync -> atomic rename`) are used for integrity and durability.

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

After lane encoding, chunk payloads are optionally compressed with fast zstd (level 1) when that reduces size. Read paths transparently decode both compressed and raw payloads.

## Performance

### Compression

The adaptive codec selection (Gorilla XOR, delta-of-delta, RLE, bitpacking) achieves **~0.4 bytes per data point** for typical `f64` time-series workloads — down from 16 bytes uncompressed (8-byte timestamp + 8-byte value), a **~40x** compression ratio.

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
- **Series registry + global index** maps metric name + label set → series ID and persists dictionaries in `series_index.bin` for faster reopen/startup.
- **Inverted postings** use roaring bitmaps for metric/label matcher candidate resolution.
- **Segment files** are immutable, CRC32 + XXH64 checksummed, and consist of: `manifest.bin`, `chunks.bin`, `chunk_index.bin`, `series.bin`, `postings.bin`.
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
| `with_wal_replay_mode(mode)` | `Salvage` | WAL corruption handling during replay (`Salvage` / `Strict`). |
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
| `with_background_fail_fast(bool)` | `false` | Stop new operations after background flush/compaction worker failures. |

## Resource Limits and Backpressure

tsink provides three configurable resource limits that protect against unbounded growth:

### Memory Limit

```rust
let storage = StorageBuilder::new()
    .with_memory_limit(512 * 1024 * 1024) // 512 MB
    .build()?;
```

When the memory budget is reached, new writes block until a background flush frees memory. This provides admission backpressure rather than OOM crashes. `0.9.0` also switched write-path memory checks to incremental shard accounting to avoid full memory scans on each admission decision.

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

tsink automatically detects cgroup CPU quotas when running inside containers (Docker, Kubernetes, etc.). This affects:

- **Writer thread count** — defaults to available CPUs within the cgroup quota, not the host CPU count.
- **Async read worker default** (when using `async-storage`) — also derived from cgroup CPU availability.

`tsink::cgroup::get_memory_limit()` is available if you want to derive an explicit `with_memory_limit(...)` value from container limits.

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
| `StorageShuttingDown` | Background fail-fast was triggered after a worker failure. |
| `InvalidConfiguration` | Invalid builder/runtime settings (for example concurrent `data_path` lock conflict, invalid snapshot/restore paths, or unsupported mode combinations). |
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
use tsink::{StorageBuilder, WalReplayMode};

// First run — data is written and WAL-protected
let storage = StorageBuilder::new()
    .with_data_path("/data/tsink")
    .build()?;
storage.insert_rows(&rows)?;
// Crash happens here — close() was never called

// Next run — recovery is automatic
let storage = StorageBuilder::new()
    .with_data_path("/data/tsink") // Same path
    .with_wal_replay_mode(WalReplayMode::Salvage)
    .build()?; // WAL replay happens here

// Previously inserted data is available
let points = storage.select("metric", &[], 0, i64::MAX)?;
```

Recovery is idempotent — a high-water mark ensures WAL frames are never applied twice. Use `WalReplayMode::Strict` if you prefer startup failure over salvage when a WAL tail is corrupted.

### Snapshot Workflow Example

Use `snapshot()` for an atomic, segment-consistent, WAL-aware backup of a live storage:

```rust
use tsink::{DataPoint, Row, StorageBuilder};

let storage = StorageBuilder::new()
    .with_data_path("/data/tsink-primary")
    .build()?;

storage.insert_rows(&[
    Row::new("cpu", DataPoint::new(1, 0.5)),
    Row::new("cpu", DataPoint::new(2, 0.7)),
])?;

storage.snapshot(std::path::Path::new("/backups/tsink-snap-001"))?;
storage.close()?;
```

Restore the snapshot atomically into another data path:

```rust
use tsink::StorageBuilder;

StorageBuilder::restore_from_snapshot(
    std::path::Path::new("/backups/tsink-snap-001"),
    std::path::Path::new("/data/tsink-restore"),
)?;

let restored = StorageBuilder::new()
    .with_data_path("/data/tsink-restore")
    .build()?;
```

`restore_from_snapshot` replaces the target data directory in a single staged publish step and preserves WAL replay semantics on reopen.

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
| `production_example` | Multi-threaded ingest/query workload with tracing-friendly logging and persistent config knobs. |
| `comprehensive` | Multiple features: concurrent ops, retention, downsampling, and aggregation. |

## Benchmarks and Tests

```bash
cargo test                          # Run all tests
cargo test --features promql        # Include PromQL tests
cargo test --features async-storage # Include async tests

cargo bench                         # Run all benchmarks
cargo bench --bench storage_benchmarks -- '^(insert_rows|select|concurrent_rw)/' --quick --noplot
scripts/measure_perf.sh quick       # Focused insert/select matrix
scripts/measure_bpp.sh quick        # Workload bytes-per-point
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
| `TSINK_BPP_RUNS` | Number of workload benchmark repetitions in `measure_bpp.sh`. |
| `TSINK_ACTIVE_SERIES` | Number of concurrent series. |
| `TSINK_PRIME_ALL_SERIES` | Prime all series before measurement (`1`/`true` to enable). |
| `TSINK_MIN_POINTS_PER_SERIES` | Minimum target points per series before measuring BPP. |
| `TSINK_WARMUP_POINTS` / `TSINK_MEASURE_POINTS` | Points ingested during warmup and measurement phases. |
| `TSINK_BATCH_SIZE` | Insert batch size. |
| `TSINK_OOO_MAX_SECONDS` / `TSINK_OOO_PERMILLE` | Out-of-order write tuning. |
| `TSINK_SPARSE_EMIT_PERMILLE` / `TSINK_SHORT_LIVED_LIFETIME_STEPS` | Sparse and short-lived series workload tuning. |
| `TSINK_RETENTION_SECONDS` / `TSINK_PARTITION_SECONDS` | Retention and partition windows. |
| `TSINK_STEP_SECONDS` / `TSINK_SETTLE_MILLIS` | Workload step spacing and settle delay tuning. |
| `TSINK_FAIL_ON_TARGET` | Fail if compression target is not met. |

## Project Structure

```text
.
├── src/                    # Core tsink engine + public API
├── crates/
│   ├── tsink-server/       # Prometheus wire-compatible server binary
│   └── tsink-uniffi/       # Python bindings (UniFFI)
├── examples/               # Runnable usage examples
├── benches/                # Criterion benchmark suites
├── tests/                  # Integration tests
├── scripts/                # Benchmark/perf helper scripts
├── CHANGELOG.md            # Release notes
└── README.md
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

No explicit MSRV is pinned yet. The crate uses Rust 2021 edition and is tested on stable.

## License

MIT
