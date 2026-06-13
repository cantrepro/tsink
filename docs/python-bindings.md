# Python bindings guide

tsink ships Python bindings via [UniFFI](https://mozilla.github.io/uniffi-rs/).
The `tsink` package gives you the full storage engine — writes, queries,
aggregation, rollups, snapshots — from Python with no server process required.
After installation, import it as `tsink`.
UniFFI record types such as `Label`, `Row`, and `MetricSeries` use keyword-only
constructors in Python.

## Installation

```bash
pip install tsink
```

Requires Python 3.8+. The wheel includes the native Rust library; no Rust toolchain needed at runtime.

### Building from source

If a pre-built wheel is not available for your platform:

```bash
pip install maturin
cd crates/tsink-uniffi
maturin develop --release
```

---

## Quick start

```python
from tsink import TsinkStorageBuilder, DataPoint, Label, Row, Value

builder = TsinkStorageBuilder()
builder.with_data_path("./tsink-data")
db = builder.build()

db.insert_rows([
    Row(
        metric="cpu_usage",
        labels=[Label(name="host", value="web-1")],
        data_point=DataPoint(timestamp=1_700_000_000_000, value=Value.F64(v=42.0)),
    ),
])

points = db.select(
    "cpu_usage",
    [Label(name="host", value="web-1")],
    0,
    2_000_000_000_000,
)
for p in points:
    print(f"ts={p.timestamp} value={p.value}")

db.close()
```

---

## TsinkStorageBuilder

Create a builder, call configuration methods, then call `build()` once to get a
`TsinkDB` handle. The builder is consumed by `build()` and cannot be reused.

```python
from datetime import timedelta
from tsink import (
    TsinkStorageBuilder,
    TimestampPrecision,
    WalSyncMode,
    WalReplayMode,
    StorageRuntimeMode,
)

builder = TsinkStorageBuilder()
builder.with_data_path("/var/lib/tsink")
builder.with_retention(timedelta(days=30))
builder.with_timestamp_precision(TimestampPrecision.MILLISECONDS)
builder.with_memory_limit(512 * 1024 * 1024)       # 512 MiB
builder.with_cardinality_limit(1_000_000)
builder.with_wal_sync_mode(WalSyncMode.PERIODIC(interval=timedelta(seconds=1)))
db = builder.build()
```

### Configuration methods

All methods return `None` and mutate the builder in place.

#### Data & persistence

| Method | Default | Description |
|---|---|---|
| `with_data_path(path)` | *none* | Directory for WAL, segments, and metadata. |
| `with_object_store_path(path)` | *none* | Path (or object-store prefix) for warm/cold tier segments. |

#### Retention

| Method | Default | Description |
|---|---|---|
| `with_retention(duration)` | 14 days | Global retention window. |
| `with_retention_enforced(bool)` | `False` | Reject writes outside the retention window. |
| `with_tiered_retention_policy(hot, warm)` | *none* | Separate retention for hot and warm tiers. |

#### Timestamp precision

| Method | Default | Description |
|---|---|---|
| `with_timestamp_precision(precision)` | `Nanoseconds` | One of `Nanoseconds`, `Microseconds`, `Milliseconds`, `Seconds`. |

#### Chunks & partitions

| Method | Default | Description |
|---|---|---|
| `with_chunk_points(n)` | 2048 | Points per in-memory chunk before sealing. |
| `with_partition_duration(duration)` | 1 hour | Time range per partition. |
| `with_max_active_partition_heads_per_series(n)` | 8 | Active partition heads per series (out-of-order fanout). |

#### Memory & cardinality

| Method | Default | Description |
|---|---|---|
| `with_memory_limit(bytes)` | unlimited | Memory budget; exceeding triggers backpressure. |
| `with_cardinality_limit(series)` | unlimited | Maximum unique series count. |

#### Concurrency

| Method | Default | Description |
|---|---|---|
| `with_max_writers(n)` | CPU count | Parallel writer threads. |
| `with_write_timeout(duration)` | 30 s | Maximum wait for a writer slot. |

#### WAL

| Method | Default | Description |
|---|---|---|
| `with_wal_enabled(bool)` | `True` | Enable/disable the write-ahead log. |
| `with_wal_size_limit(bytes)` | unlimited | Maximum WAL size on disk. |
| `with_wal_buffer_size(size)` | *default* | In-memory WAL buffer size. |
| `with_wal_sync_mode(mode)` | `PerAppend` | `WalSyncMode.PER_APPEND` (crash-safe) or `WalSyncMode.PERIODIC(interval)`. |
| `with_wal_replay_mode(mode)` | `Strict` | `WalReplayMode.STRICT` or `WalReplayMode.SALVAGE`. |

#### Remote segments

| Method | Default | Description |
|---|---|---|
| `with_remote_segment_cache_policy(policy)` | `MetadataOnly` | Caching strategy for remote tier segments. |
| `with_remote_segment_refresh_interval(duration)` | *default* | Refresh interval for the remote segment catalog. |
| `with_mirror_hot_segments_to_object_store(bool)` | `False` | Mirror hot segments to the object store. |

#### Runtime

| Method | Default | Description |
|---|---|---|
| `with_runtime_mode(mode)` | `ReadWrite` | `StorageRuntimeMode.READ_WRITE` or `COMPUTE_ONLY`. |
| `with_background_fail_fast(bool)` | `True` | Halt on unrecoverable background errors. |
| `with_metadata_shard_count(n)` | *auto* | Number of metadata shards. |

---

## TsinkDB

`TsinkDB` is the main database handle returned by `builder.build()`. It is
thread-safe and can be shared across Python threads.

### Writing data

#### Value types

Values are represented by the `Value` enum:

```python
Value.F64(v=3.14)              # 64-bit float
Value.I64(v=-42)               # 64-bit signed integer
Value.U64(v=100)               # 64-bit unsigned integer
Value.BOOL(v=True)             # boolean
Value.BYTES(v=b"\xCA\xFE")    # raw bytes
Value.STR(v="hello")           # UTF-8 string
Value.HISTOGRAM(v=histogram)   # native Prometheus histogram
```

#### Insert rows

```python
rows = [
    Row(
        metric="http_requests_total",
        labels=[
            Label(name="method", value="GET"),
            Label(name="status", value="200"),
        ],
        data_point=DataPoint(timestamp=1_700_000_000_000, value=Value.F64(v=1027.0)),
    ),
    Row(
        metric="memory_free_bytes",
        labels=[Label(name="host", value="web-1")],
        data_point=DataPoint(timestamp=1_700_000_000_000, value=Value.I64(v=8_589_934_592)),
    ),
]

db.insert_rows(rows)
```

#### Write acknowledgement

`insert_rows_with_result` returns a `WriteResult` so you can inspect the
durability guarantee:

```python
result = db.insert_rows_with_result(rows)
print(result.acknowledgement)
# WriteAcknowledgement.DURABLE   — fsync'd (PerAppend WAL mode)
# WriteAcknowledgement.APPENDED  — in WAL buffer (Periodic mode)
# WriteAcknowledgement.VOLATILE  — in memory only (WAL disabled)
```

---

### Querying data

#### Simple select

```python
points = db.select(
    "cpu_usage",
    [Label(name="host", value="web-1")],
    start=0,
    end=2_000_000_000_000,
)

for p in points:
    print(p.timestamp, p.value)
```

Pass an empty label list to match the bare metric (no labels):

```python
points = db.select("cpu_usage", [], start=0, end=2_000_000_000_000)
```

#### Select all label combinations

```python
all_series = db.select_all("http_requests_total", start=0, end=2_000_000_000_000)

for labeled in all_series:
    tag_str = ", ".join(f"{l.name}={l.value}" for l in labeled.labels)
    print(f"[{tag_str}]: {len(labeled.data_points)} points")
```

Returns a list of `LabeledDataPoints`, each carrying `labels` and `data_points`.

#### Select multiple series at once

```python
from tsink import MetricSeries

series = [
    MetricSeries(name="cpu_usage", labels=[Label(name="host", value="web-1")]),
    MetricSeries(name="cpu_usage", labels=[Label(name="host", value="web-2")]),
]

results = db.select_many(series, start=0, end=2_000_000_000_000)
for sp in results:
    print(f"{sp.series.name} {sp.series.labels}: {len(sp.points)} points")
```

#### Advanced queries with QueryOptions

```python
from tsink import Aggregation, DownsampleOptions, QueryOptions

options = QueryOptions(
    labels=[Label(name="host", value="web-1")],
    start=0,
    end=2_000_000_000_000,
    aggregation=Aggregation.AVG,
    downsample=DownsampleOptions(interval=60_000),  # 1-minute buckets
    limit=1000,
    offset=0,
)

points = db.select_with_options("cpu_usage", options)
```

Available aggregations:

| `Aggregation` variant | Description |
|---|---|
| `NONE` | No aggregation (default) |
| `SUM` | Sum of values |
| `MIN` / `MAX` | Minimum / maximum |
| `AVG` | Arithmetic mean |
| `FIRST` / `LAST` | First / last point in window |
| `COUNT` | Number of points |
| `MEDIAN` | Median value |
| `RANGE` | Max − Min |
| `VARIANCE` / `STD_DEV` | Statistical variance / standard deviation |

#### Paginated row scanning

For large result sets, scan in pages:

```python
from tsink import QueryRowsScanOptions

offset = None
while True:
    page = db.scan_metric_rows(
        "cpu_usage",
        start=0,
        end=2_000_000_000_000,
        options=QueryRowsScanOptions(max_rows=10_000, row_offset=offset),
    )
    process(page.rows)

    if page.truncated:
        offset = page.next_row_offset
    else:
        break
```

`scan_series_rows` works the same way but accepts a list of `MetricSeries`
instead of a metric name.

---

### Series discovery

#### List all metrics

```python
all_metrics = db.list_metrics()
for m in all_metrics:
    print(m.name, m.labels)
```

`list_metrics_with_wal()` includes series that exist only in the WAL (not yet
flushed).

#### Filter with matchers

```python
from tsink import SeriesMatcher, SeriesMatcherOp, SeriesSelection

selection = SeriesSelection(
    metric="http_requests_total",
    matchers=[
        SeriesMatcher(name="method", op=SeriesMatcherOp.EQUAL, value="GET"),
        SeriesMatcher(name="status", op=SeriesMatcherOp.REGEX_MATCH, value="2.."),
    ],
    start=None,
    end=None,
)

matched = db.select_series(selection)
```

| `SeriesMatcherOp` | Equivalent | Example |
|---|---|---|
| `EQUAL` | `=` | `method="GET"` |
| `NOT_EQUAL` | `!=` | `status!="500"` |
| `REGEX_MATCH` | `=~` | `host=~"web-.*"` |
| `REGEX_NO_MATCH` | `!~` | `env!~"staging\|dev"` |

---

### Deleting series

```python
result = db.delete_series(
    SeriesSelection(metric="old_metric", matchers=[], start=None, end=None)
)
print(f"matched={result.matched_series}, tombstones={result.tombstones_applied}")
```

Deletion is tombstone-based. Tombstones are merged during compaction.

---

### Rollup policies

Define materialized downsampled views:

```python
from tsink import RollupPolicy

policies = [
    RollupPolicy(
        id="cpu_5m_avg",
        metric="cpu_usage",
        match_labels=[],
        interval=300_000,             # 5 minutes in ms
        aggregation=Aggregation.AVG,
        bucket_origin=0,
    ),
]

snapshot = db.apply_rollup_policies(policies)
for status in snapshot.policies:
    print(f"{status.policy.id}: materialized through {status.materialized_through}")
```

Trigger an immediate rollup run:

```python
snapshot = db.trigger_rollup_run()
```

---

### Snapshots

```python
db.snapshot("/backups/tsink-2026-03-12")

# Restore to a new data directory.
TsinkStorageBuilder.restore_from_snapshot("/backups/tsink-2026-03-12", "/var/lib/tsink")
```

---

### Observability

Inspect engine internals at runtime:

```python
snap = db.observability_snapshot()

print(f"memory: {snap.memory.active_and_sealed_bytes} / {snap.memory.budgeted_bytes} bytes")
print(f"WAL: {snap.wal.segment_count} segments, {snap.wal.size_bytes} bytes")
print(f"compaction: {snap.compaction.runs_total} runs, {snap.compaction.errors_total} errors")
print(f"queries: {snap.query.select_calls_total} selects")

if snap.health.degraded:
    print(f"engine degraded: {snap.health.last_background_error}")
```

The snapshot covers memory, WAL, retention, flush pipeline, compaction, queries,
rollups, remote storage, and overall health.

---

### Memory inspection

```python
print(f"used:   {db.memory_used()} bytes")
print(f"budget: {db.memory_budget()} bytes")
```

---

### Closing the database

```python
db.close()
```

Flushes remaining data, syncs the WAL, and shuts down background workers.
Any method called after `close()` raises `TsinkUniFFIError`.

---

## Native histograms

Store Prometheus-style native histograms:

```python
from tsink import (
    NativeHistogram,
    HistogramBucketSpan,
    HistogramCount,
    HistogramResetHint,
)

histogram = NativeHistogram(
    count=HistogramCount.INT(v=10),
    sum=55.0,
    schema=3,
    zero_threshold=1e-128,
    zero_count=HistogramCount.INT(v=1),
    negative_spans=[],
    negative_deltas=[],
    negative_counts=[],
    positive_spans=[HistogramBucketSpan(offset=0, length=3)],
    positive_deltas=[2, 1, -1],
    positive_counts=[],
    reset_hint=HistogramResetHint.NO,
    custom_values=[],
)

db.insert_rows([
    Row(
        metric="request_duration",
        labels=[],
        data_point=DataPoint(
            timestamp=1_700_000_000_000,
            value=Value.HISTOGRAM(v=histogram),
        ),
    ),
])
```

---

## Shard window operations

These methods support distributed deployments where data is partitioned across
nodes. They are used internally by the cluster layer but are available for
advanced use cases.

```python
from tsink import MetadataShardScope, ShardWindowScanOptions

# Compute a digest (fingerprint) for a shard window.
digest = db.compute_shard_window_digest(
    shard=0, shard_count=16,
    window_start=0, window_end=2_000_000_000_000,
)
print(f"series={digest.series_count} points={digest.point_count} fp={digest.fingerprint}")

# Scan rows for a shard window with pagination.
page = db.scan_shard_window_rows(
    shard=0, shard_count=16,
    window_start=0, window_end=2_000_000_000_000,
    options=ShardWindowScanOptions(max_series=100, max_rows=10_000, row_offset=None),
)

# List metrics limited to specific shards.
scope = MetadataShardScope(shard_count=16, shards=[0, 1, 2])
metrics = db.list_metrics_in_shards(scope)
```

---

## Error handling

All methods raise `TsinkUniFFIError` on failure. The exception carries a string
message and is one of these variants:

| Variant | When it occurs |
|---|---|
| `NoDataPoints` | No data found for the given metric/time range. |
| `InvalidTimeRange` | `start > end` or otherwise invalid range. |
| `StorageClosed` | Operation attempted after `close()`. |
| `InvalidInput` | Bad metric name, label, or unsupported operation. |
| `IoError` | File-system or disk I/O failure. |
| `DataCorruption` | Checksum mismatch or parse error. |
| `ResourceExhausted` | Memory budget, cardinality limit, or WAL size limit exceeded. |
| `Other` | Lock poisoning, channel errors, WAL issues, etc. |

```python
from tsink import TsinkUniFFIError

try:
    db.insert_rows(rows)
except TsinkUniFFIError as e:
    print(f"write failed: {e}")
```

---

## Type reference

### Records (dataclasses)

| Python type | Fields | Notes |
|---|---|---|
| `Label` | `name: str`, `value: str` | Metric label key-value pair. |
| `DataPoint` | `timestamp: int`, `value: Value` | Single data point. |
| `Row` | `metric: str`, `labels: list[Label]`, `data_point: DataPoint` | Complete write record. |
| `MetricSeries` | `name: str`, `labels: list[Label]` | Series identity. |
| `SeriesPoints` | `series: MetricSeries`, `points: list[DataPoint]` | Series with its data. |
| `LabeledDataPoints` | `labels: list[Label]`, `data_points: list[DataPoint]` | Labels with points (from `select_all`). |
| `QueryOptions` | `labels`, `start`, `end`, `aggregation`, `downsample`, `limit`, `offset` | Advanced query configuration. |
| `DownsampleOptions` | `interval: int` | Downsample bucket width. |
| `SeriesSelection` | `metric: Optional[str]`, `matchers`, `start`, `end` | Series filter. |
| `SeriesMatcher` | `name: str`, `op: SeriesMatcherOp`, `value: str` | Single label matcher. |
| `RollupPolicy` | `id`, `metric`, `match_labels`, `interval`, `aggregation`, `bucket_origin` | Rollup definition. |
| `WriteResult` | `acknowledgement: WriteAcknowledgement` | Write durability level. |
| `DeleteSeriesResult` | `matched_series: int`, `tombstones_applied: int` | Deletion outcome. |

### Enums

| Python type | Variants |
|---|---|
| `Value` | `F64`, `I64`, `U64`, `BOOL`, `BYTES`, `STR`, `HISTOGRAM` |
| `Aggregation` | `NONE`, `SUM`, `MIN`, `MAX`, `AVG`, `FIRST`, `LAST`, `COUNT`, `MEDIAN`, `RANGE`, `VARIANCE`, `STD_DEV` |
| `TimestampPrecision` | `NANOSECONDS`, `MICROSECONDS`, `MILLISECONDS`, `SECONDS` |
| `StorageRuntimeMode` | `READ_WRITE`, `COMPUTE_ONLY` |
| `RemoteSegmentCachePolicy` | `METADATA_ONLY` |
| `WalSyncMode` | `PER_APPEND`, `PERIODIC(interval)` |
| `WalReplayMode` | `STRICT`, `SALVAGE` |
| `WriteAcknowledgement` | `VOLATILE`, `APPENDED`, `DURABLE` |
| `SeriesMatcherOp` | `EQUAL`, `NOT_EQUAL`, `REGEX_MATCH`, `REGEX_NO_MATCH` |
| `HistogramCount` | `INT(v)`, `FLOAT(v)` |
| `HistogramResetHint` | `UNKNOWN`, `YES`, `NO`, `GAUGE` |
