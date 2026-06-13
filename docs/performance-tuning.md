# Performance Tuning

This guide explains which knobs most affect throughput and latency in tsink and how to choose values for your workload. It assumes you have already read the [storage engine internals](storage-engine.md) and the [configuration reference](configuration.md).

Sections are ordered from the highest-leverage tunables (memory, write pipeline) to lower-level ones (compaction, encoding, containers).

---

## Contents

1. [Understanding the write path](#1-understanding-the-write-path)
2. [Memory budget](#2-memory-budget)
3. [Write pipeline](#3-write-pipeline)
4. [WAL sync mode](#4-wal-sync-mode)
5. [Chunk and partition sizing](#5-chunk-and-partition-sizing)
6. [Compaction tuning](#6-compaction-tuning)
7. [Metadata sharding](#7-metadata-sharding)
8. [Query performance](#8-query-performance)
9. [Rollups for read-heavy workloads](#9-rollups-for-read-heavy-workloads)
10. [Server-level concurrency limits](#10-server-level-concurrency-limits)
11. [Cgroup-aware scheduling](#11-cgroup-aware-scheduling)
12. [Monitoring performance](#12-monitoring-performance)
13. [Quick-reference table](#13-quick-reference-table)

---

## 1. Understanding the write path

Every call to `insert_rows` travels through five ordered stages before the write is acknowledged:

```
insert_rows(rows)
    │
    ├─ 1. Resolve     — look up or create series IDs
    ├─ 2. Prepare     — validate, check memory budget, pick WAL codec
    ├─ 3. Stage       — encode and append to WAL (fsync if PerAppend)
    ├─ 4. Apply       — append points to the active ChunkBuilder
    └─ 5. Publish     — advance query visibility
```

The dominant cost varies by workload:

- **High series cardinality** — stage 1 (registry lookup) dominates. Use [metadata sharding](#7-metadata-sharding).
- **Durability-sensitive** — stage 3 (WAL fsync) dominates. Switch to [periodic WAL sync](#4-wal-sync-mode).
- **Memory pressure** — stage 2 (admission control) dominates. Increase the [memory budget](#2-memory-budget) or reduce the ingestion rate.
- **High concurrent throughput** — writer pool saturation. Increase [`max_writers`](#3-write-pipeline).

---

## 2. Memory budget

The memory budget is the single most important tunable for sustained throughput. The engine tracks all active and sealed chunk bytes and holds writes in admission control when the budget is exhausted.

### Setting the budget

**Embedded library:**

```rust
let storage = StorageBuilder::new()
    .with_data_path("./tsink-data")
    .with_memory_limit(2 * 1024 * 1024 * 1024) // 2 GiB
    .build()?;
```

**Server:**

```
tsink-server --memory-limit 2G --data-path ./var/tsink
```

### Sizing guidelines

| Deployment | Recommended budget |
|---|---|
| Laptop / development | 256 MiB – 512 MiB |
| Single-node server, moderate cardinality (< 100 k series) | 1 GiB – 4 GiB |
| Single-node server, high cardinality (> 500 k series) | 4 GiB – 16 GiB |
| Cluster node | Set to 50–70 % of available RAM; leave room for the OS page cache |

### What uses memory

The budget covers active chunk builders and sealed chunks pending flush. Several other categories consume memory but are **excluded** from the budget:

| Category | Visibility |
|---|---|
| Series registry (metadata) | `tsink_memory_registry_bytes` |
| Metadata caches and indexes | `tsink_memory_metadata_cache_bytes` |
| Persisted chunk refs and timestamp search indexes | `tsink_memory_persisted_index_bytes` |
| mmap-mapped segment payloads | `tsink_memory_persisted_mmap_bytes` |
| Tombstone state | `tsink_memory_tombstone_bytes` |

Add a proportional allowance for these when calculating total process memory.

### Backpressure behaviour

When active + sealed bytes exceeds the budget:

1. The flush pipeline is triggered immediately to persist sealed chunks to L0 segments and free their memory.
2. Incoming writes poll every 10 ms waiting for budget recovery.
3. If the budget is still exhausted after `write_timeout` (default 30 s), the write returns an error.

If `tsink_flush_admission_backpressure_delays_total` is climbing in `/metrics`, the budget is too small for your write rate. Either increase the budget or add a `--data-path` to enable background persistence.

---

## 3. Write pipeline

Writes are gated by a semaphore sized to `max_writers`. Each writer acquires a permit for the duration of stages 1–5 and releases it on exit.

### Setting max_writers

**Embedded library:**

```rust
let storage = StorageBuilder::new()
    .with_max_writers(8)
    .build()?;
```

**Server:**

```
tsink-server --max-writers 8
```

The default is the cgroup-visible CPU count (see [cgroup-aware scheduling](#11-cgroup-aware-scheduling)).

### Tuning guidance

- **Low-latency single-writer workloads** — keep `max_writers` at the CPU count or lower. Adding writers above CPU count introduces lock contention in the series registry.
- **Fan-out write workloads** (many HTTP clients writing many small batches) — increase `max_writers` to 2× CPU count. Each write spends time blocked on WAL I/O, leaving CPU headroom for other writers.
- **Batch ingestion** (few large batches per second) — lower `max_writers` to 1–2. Large batches saturate the registry faster; more writers increase memory usage without proportional throughput gains.

### Write timeout

If all writer slots are occupied, new writes wait up to `write_timeout` (default 30 s). On a healthy system this should never trigger. If you see `WriteTimeout` errors, either increase `max_writers` or reduce the ingest rate.

```rust
StorageBuilder::new()
    .with_write_timeout(Duration::from_secs(10))
    .build()?
```

---

## 4. WAL sync mode

WAL sync mode is the primary durability vs. throughput tradeoff.

| Mode | Durability | Typical overhead |
|---|---|---|
| `PerAppend` (default) | `fsync` per write — crash-safe | High latency per write, excellent for low-rate durable ingestion |
| `Periodic(interval)` | OS-buffered; data in the crash window can be lost | 5–20× higher write throughput at equivalent concurrency |

### Disabling crash-safety for maximum throughput

```rust
use tsink::wal::WalSyncMode;
use std::time::Duration;

let storage = StorageBuilder::new()
    .with_wal_sync_mode(WalSyncMode::Periodic(Duration::from_millis(500)))
    .build()?;
```

**Server:**

```
tsink-server --wal-sync-mode periodic
```

With `Periodic`, `insert_rows_with_result` returns `WriteAcknowledgement::Appended` instead of `::Durable`. A background thread fsync's the WAL at the configured interval; data written within the last interval window can be lost on process crash.

### Disabling the WAL entirely

For in-memory or ephemeral deployments where crash-safety is not needed:

```rust
let storage = StorageBuilder::new()
    .with_wal_enabled(false)
    .build()?;
```

No WAL means no durability guarantee at all. All in-flight data is lost on process exit until the next compaction and flush cycle makes data available in recovery.

### WAL buffer size

The default per-write I/O buffer is 4096 bytes. On high-throughput workloads with many small writes, increasing this can reduce syscall overhead:

```rust
StorageBuilder::new()
    .with_wal_buffer_size(65536)
    .build()?
```

---

## 5. Chunk and partition sizing

### Chunk point capacity (`chunk_points`)

A chunk is the atomic encoded unit. The encoder selects a codec for each chunk independently. The default capacity is **2048 points**.

| Larger chunks | Smaller chunks |
|---|---|
| Higher compression ratio (more context for delta/XOR) | Lower read amplification on recent data |
| Lower flush I/O overhead | Faster availability after write (each flush covers a smaller unit) |
| More memory held per series before sealed | Shorter WAL high-watermark advancement |

For Prometheus-style scrape workloads with a 15 s interval and regular timestamps, `FixedStepRle` cuts timestamp storage to a constant regardless of chunk size — here larger chunks mainly improve the float XOR compression. A value of **2048–8192** is effective.

For irregular or heavily-jittered ingestion (InfluxDB, StatsD, OTLP), smaller chunks (512–1024) reduce merge amplification during compaction.

```rust
StorageBuilder::new()
    .with_chunk_points(4096)
    .build()?
```

**Server:**

```
tsink-server --chunk-points 4096
```

### Partition duration

Partitions group data in time windows (default: **1 hour**). One `ChunkBuilder` per partition head is kept open per series.

- Shorter partitions (e.g., 15 m) reduce the amount of active memory per series but increase the number of small L0 segments, leading to more compaction.
- Longer partitions (e.g., 6 h) improve compression across more points per series but hold more data in memory before the partition head seals.

Match the partition duration to your typical query range. A common pattern is:

| Scrape interval | Retention | Recommended partition |
|---|---|---|
| 15 s | 7–30 d | 1 h (default) |
| 1 m | 90+ d | 6 h |
| 5 m | 1 year+ | 24 h |

### Max active partition heads

`max_active_partition_heads_per_series` (default: **8**) controls how many time-windows can be open simultaneously for a single series. Increase this only when you have significant backfill or unordered ingestion spanning many different historical windows. Keeping it low reduces memory usage.

---

## 6. Compaction tuning

Compaction merges L0 segments into L1, then L1 into L2. It runs in a background thread every **5 seconds**.

### Trigger thresholds

Two thresholds govern when a compaction pass fires for each level transition:

| Threshold | Default | Effect |
|---|---|---|
| `l0_trigger` | 4 | Compact L0 → L1 when ≥ 4 L0 segments exist |
| `l1_trigger` | 4 | Compact L1 → L2 when ≥ 4 L1 segments exist |

Additionally, a compaction pass fires immediately for a level if any two segments in that level have **overlapping time ranges**, regardless of the count trigger. This keeps query read amplification low.

These thresholds are not yet exposed as public API — increasing the flush frequency relative to the ingest rate is the easiest way to control L0 growth.

### Source window limit

Each compaction pass merges at most **8 source segments** at a time. For burst-heavy workloads this keeps individual compaction jobs bounded. Back-to-back passes converge the level quickly.

### Monitoring compaction health

```
tsink_compaction_runs_total
tsink_compaction_success_total
tsink_compaction_errors_total
tsink_compaction_source_segments_total    # segments consumed
tsink_compaction_output_segments_total   # segments produced
tsink_compaction_duration_nanoseconds_total
```

A healthy deployment sees `source_segments / output_segments ≈ 4` (matching the trigger threshold). A ratio close to 1 means compaction is not converging — investigate whether flush is running too frequently or the ingest rate exceeds compaction bandwidth.

### Compaction and tombstones

Tombstones are applied lazily during compaction: deleted time ranges are trimmed from merged chunks so that physical disk space is reclaimed. If you issue many deletes, compaction may briefly spike in duration while re-encoding chunks.

---

## 7. Metadata sharding

The in-memory series registry is split across **64 shards** by default (locked independently, keyed by `series_id % 64`). For workloads with hundreds of thousands of distinct series, write-transaction lock contention on the registry can become a bottleneck.

`with_metadata_shard_count` enables an additional metadata partitioning layer for series index scans and postings lookups:

```rust
StorageBuilder::new()
    .with_metadata_shard_count(16)
    .build()?
```

Useful values are powers-of-two between **8** and **64**. Higher counts reduce per-shard scan range, improving label-matcher query latency at high cardinality. There is no strong reason to exceed 64 for the built-in 64-shard write-path sharding.

When using the server, this can be tuned per deployment but is not yet exposed as a CLI flag — use the embedded library or Python bindings for fine-grained control.

---

## 8. Query performance

### mmap reads

Segment chunk files are opened as read-only memory maps and decoded directly from the mapped address space without a user-space copy. The OS page cache serves as the effective read buffer. Performance improves proportionally with available free RAM.

For read-heavy deployments:

- Leave at least 30–50 % of RAM unallocated so the page cache can hold recently-accessed segment files.
- Avoid the memory budget consuming all available RAM — `tsink_memory_persisted_mmap_bytes` contributes to the overhead.

### Block-level seek index

Each chunk carries an in-memory timestamp search index that allows the decoder to seek directly to the 64-point block containing the desired timestamp without decompressing the whole chunk. Narrower time-range queries benefit most from this.

### Series selection and label matchers

The postings bitmap indexes (one per metric name, per label name, per label name+value pair) are loaded into memory on startup and used to resolve label-matchers before any chunk data is read. Regex matchers compile once and are applied against the string dictionary, not against raw label values.

For high-cardinality label spaces, prefer exact-match (`=`) and not-equal (`!=`) matchers over regex (`=~`) wherever possible — exact matches walk the postings index in O(1) while regex requires scanning the label dictionary.

### Metadata sharding for range queries

For `select_series` queries with time-range filters applied to servers with large histories (many persisted segments), increasing `metadata_shard_count` reduces the per-shard scan range and can cut latency by 2–5× at > 500 k series.

### Rollup materialization

For dashboard-style repeated queries over long time ranges, configure [rollup policies](rollups.md). The query engine checks for materialized series before falling back to a raw scan. At query time a rollup hit saves reading and merging the underlying raw chunks entirely.

---

## 9. Rollups for read-heavy workloads

If the same long-range aggregation query runs repeatedly (e.g., Grafana dashboards with `rate`, `sum`, `avg`), define a rollup policy to pre-materialize the result at a lower resolution:

```bash
curl -X POST http://127.0.0.1:9201/api/v1/rollups \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "http_req_1m",
    "metric": "http_requests_total",
    "interval": 60000,
    "aggregation": "sum"
  }'
```

Materialized rollup series are stored as regular time series and resolved at query time with no special code path — the engine simply notices the materialized resolution matches the query step and short-circuits the raw scan.

---

## 10. Server-level concurrency limits

The tsink-server adds an HTTP-layer admission tier on top of the engine-level `max_writers` semaphore. These environment variables are read once at process startup:

| Variable | Default | Description |
|---|---|---|
| `TSINK_SERVER_WRITE_MAX_INFLIGHT_REQUESTS` | `64` | Max concurrent write HTTP requests |
| `TSINK_SERVER_WRITE_MAX_INFLIGHT_ROWS` | `200000` | Max total in-flight rows across all write requests |
| `TSINK_SERVER_WRITE_RESOURCE_ACQUIRE_TIMEOUT_MS` | `25` | ms to wait for a write slot before returning 429 |
| `TSINK_SERVER_READ_MAX_INFLIGHT_REQUESTS` | `64` | Max concurrent read HTTP requests |
| `TSINK_SERVER_READ_MAX_INFLIGHT_QUERIES` | `128` | Max total in-flight query slots |
| `TSINK_SERVER_READ_MAX_INFLIGHT_ROWS` | *(unlimited)* | Optionally cap total rows returned across concurrent reads |

### Tuning for high write throughput

For Prometheus remote-write or OTLP workloads where each request carries 1000–10 000 rows:

```bash
export TSINK_SERVER_WRITE_MAX_INFLIGHT_REQUESTS=128
export TSINK_SERVER_WRITE_MAX_INFLIGHT_ROWS=500000
export TSINK_SERVER_WRITE_RESOURCE_ACQUIRE_TIMEOUT_MS=100
```

### Tuning for many concurrent queries

For dashboard-heavy setups with many Grafana panels refreshing simultaneously:

```bash
export TSINK_SERVER_READ_MAX_INFLIGHT_REQUESTS=256
export TSINK_SERVER_READ_MAX_INFLIGHT_QUERIES=512
export TSINK_SERVER_READ_RESOURCE_ACQUIRE_TIMEOUT_MS=200
```

---

## 11. Cgroup-aware scheduling

tsink reads container CPU and memory limits from the cgroup v2 interface (`/sys/fs/cgroup/cpu.max` and `/sys/fs/cgroup/memory.max`) at startup and uses those values to size the default worker pools.

Internally:

- `available_cpus()` returns the smaller of the cgroup CPU quota (rounded up) and the host CPU count.
- `max_writers` defaults to `available_cpus()`.
- The async storage read worker pool also defaults to `available_cpus()`.

### Overriding the detected CPU count

If the autodetection is wrong or you want to pin the worker count regardless of the container limit:

```bash
export TSINK_MAX_CPUS=4
```

This overrides `available_cpus()` globally. All worker pools that default to CPU count will respect this override.

### Memory limit awareness

The cgroup memory limit is exposed through `cgroup::get_memory_limit()` but is not currently applied automatically as the storage memory budget — you must set `--memory-limit` or `with_memory_limit()` explicitly. A reasonable starting point is 60–70 % of the container memory limit:

```bash
# cgroup limit = 4 GiB → use 2.5 GiB for tsink data
tsink-server --memory-limit 2684354560 --data-path ./var/tsink
```

---

## 12. Monitoring performance

All internal counters are exposed at `GET /metrics` in Prometheus format. The most useful ones for performance diagnosis:

### Write throughput and latency

```
# Rate of points ingested
rate(tsink_wal_append_points_total[1m])

# Write admission delays (backpressure events)
tsink_flush_admission_backpressure_delays_total
tsink_flush_admission_backpressure_delay_nanoseconds_total

# Write timeout errors
tsink_flush_pipeline_timeout_total
```

### Memory pressure

```
# Budget usage
tsink_memory_used_bytes / tsink_memory_budget_bytes

# mmap footprint (not counted in budget)
tsink_memory_persisted_mmap_bytes
```

### Compaction health

```
# Compaction rate — should be ≥ flush rate to avoid L0 accumulation
rate(tsink_compaction_success_total[5m])

# L0 segment count (should stay low — near the l0_trigger)
tsink_flush_hot_segments_visible
```

### Flush pipeline

```
# Segments landing on disk per minute
rate(tsink_flush_persisted_segments_total[1m])

# Seal-to-persist pipeline duration
rate(tsink_flush_persist_duration_nanoseconds_total[1m])
  / rate(tsink_flush_persist_success_total[1m])
```

### Query latency

```
# Select call duration
rate(tsink_query_select_duration_nanoseconds_total[1m])
  / rate(tsink_query_select_calls_total[1m])

# Rollup hit rate vs raw scans
rate(tsink_query_rollup_query_plans_total[5m])
  / rate(tsink_query_select_with_options_calls_total[5m])
```

---

## 13. Quick-reference table

| Goal | Parameter | Recommended change |
|---|---|---|
| Maximise write throughput | `wal_sync_mode` | `Periodic(500ms)` |
| Reduce write latency at high concurrency | `max_writers` | 2× CPU count |
| Prevent OOM on memory spike | `memory_limit` | 60–70 % of available RAM |
| Improve compression ratio | `chunk_points` | 4096–8192 |
| Reduce read latency on large label spaces | `metadata_shard_count` | 16–64 |
| Reduce read latency on long time ranges | Rollup policies | 1 m / 5 m resolution |
| Handle burst write traffic | `TSINK_SERVER_WRITE_MAX_INFLIGHT_REQUESTS` | 2× concurrent writers |
| Handle many concurrent queries | `TSINK_SERVER_READ_MAX_INFLIGHT_QUERIES` | 4–8× CPU count |
| Cap worker pool in containers | `TSINK_MAX_CPUS` | Container CPU quota |
| Survive background failures silently | `background_fail_fast` | `false` (dev/test only) |
| Fast startup after crash | `wal_replay_mode` | `Salvage` (accept potential data loss) |
