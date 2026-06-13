# tsink — Architecture Overview

This document describes the high-level design of tsink, the component interactions, and the end-to-end data flow for writes and reads.

---

## Table of Contents

1. [Deployment modes](#1-deployment-modes)
2. [Repository layout](#2-repository-layout)
3. [Public API surface](#3-public-api-surface)
4. [Storage engine overview](#4-storage-engine-overview)
5. [Write path](#5-write-path)
6. [Read path](#6-read-path)
7. [Write-Ahead Log (WAL)](#7-write-ahead-log-wal)
8. [Chunk encoding](#8-chunk-encoding)
9. [Segments and on-disk index](#9-segments-and-on-disk-index)
10. [Series registry](#10-series-registry)
11. [Compaction](#11-compaction)
12. [Tiered storage and lifecycle](#12-tiered-storage-and-lifecycle)
13. [Visibility and MVCC fence](#13-visibility-and-mvcc-fence)
14. [PromQL engine](#14-promql-engine)
15. [Rollups and downsampling](#15-rollups-and-downsampling)
16. [Server mode](#16-server-mode)
17. [Clustering and replication](#17-clustering-and-replication)
18. [Security model](#18-security-model)
19. [Observability and background runtime](#19-observability-and-background-runtime)
20. [Configuration reference summary](#20-configuration-reference-summary)

---

## 1. Deployment modes

tsink ships as three interconnected Rust crates in a single workspace.

| Crate | Purpose |
|---|---|
| `tsink` (root `src/`) | Embeddable library — the complete storage engine with no async runtime dependency. |
| `tsink-server` (`crates/tsink-server/`) | Standalone HTTP server binary with protocol ingest, clustering, and RBAC. |
| `tsink-uniffi` (`crates/tsink-uniffi/`) | UniFFI-generated Python bindings that wrap the library crate. |

All three share the same engine code. The server and Python bindings call into the same `Storage` trait that application code uses directly.

---

## 2. Repository layout

```
src/
  lib.rs                 — public re-exports
  storage.rs             — Storage trait, StorageBuilder, public DTOs
  async.rs               — runtime-agnostic async façade (AsyncStorage)
  value.rs               — Value enum, NativeHistogram
  label.rs               — Label, canonical identity, stable hash
  wal.rs                 — WalSyncMode, WalReplayMode
  mmap.rs                — PlatformMmap (memmap2 wrapper)
  concurrency.rs         — Semaphore (write gate)
  cgroup.rs              — container-aware CPU/memory detection
  validation.rs          — metric/label boundary checks
  query_aggregation.rs   — Aggregation enum, downsampling
  query_matcher.rs       — CompiledSeriesMatcher (regex → postings)
  query_selection.rs     — PreparedSeriesSelection execution
  error.rs               — StorageError
  engine/                — all internal engine modules (see §4)
  promql/                — PromQL lexer, parser, AST, evaluator

crates/
  tsink-server/src/      — HTTP server, protocol adapters, cluster logic
  tsink-uniffi/src/      — UniFFI bindings
```

---

## 3. Public API surface

### Core interface — `Storage` trait (`src/storage.rs`)

All interaction with the engine goes through the `Storage` trait. Key methods:

| Method | Description |
|---|---|
| `insert_rows(&[Row])` | Synchronous batch write. |
| `insert_rows_with_result` | Batch write returning per-row `WriteResult`. |
| `select(metric, labels, start, end)` | Range read for a single series. |
| `select_with_options` | Range read with `QueryOptions` (aggregation, downsampling, pagination). |
| `select_all` | Bulk range read across all series matching a selector. |
| `list_metrics` | Enumerate stored metric names. |
| `select_series` | Enumerate series matching a label selector with time-range filtering. |
| `scan_series_rows` / `scan_metric_rows` | Paginated scan for large result sets. |
| `delete_series` | Tombstone-based deletion with optional time range. |
| `snapshot` | Atomic directory snapshot. |
| `add_rollup_policy` / `run_rollup_pass` | Manage persistent downsampling policies. |
| `observability_snapshot` | Current internal counters and health state. |

### Key data types

- **`Row`** — `(metric: String, labels: Vec<Label>, data_point: DataPoint)`.
- **`DataPoint`** — `(timestamp: i64, value: Value)`.
- **`Value`** — `F64(f64) | I64(i64) | U64(u64) | Bool(bool) | Bytes(Vec<u8>) | Histogram(Box<NativeHistogram>)`.
- **`Label`** — `(name: String, value: String)`.
- **`TimestampPrecision`** — `Nanoseconds | Microseconds | Milliseconds | Seconds`.

### Async façade — `AsyncStorage` (`src/async.rs`)

`AsyncStorage` wraps `Storage` without requiring Tokio. It spawns OS threads internally:

- One dedicated write worker receives `WriteCommand` messages over an `async-channel`.
- A pool of read workers (sized by `cgroup::default_workers_limit()`) handles `ReadCommand` messages concurrently.
- Uses `parking_lot::Mutex` and `std::thread` — suitable for embedding in any async runtime.

---

## 4. Storage engine overview

The engine lives in `src/engine/` and is structured in three layers.

### Layer 1 — Foundation libraries
Pure data-structure and codec modules with no cross-cutting concerns:

| Module | Role |
|---|---|
| `chunk` | In-memory chunk builder and sealed `EncodedChunk`. |
| `encoder` | Timestamp and value codec selection and execution. |
| `segment` | On-disk segment format, reader, writer, index postings. |
| `series` | Series identity, interned string dictionaries, Roaring Bitmap postings. |
| `wal` | WAL frame format, segmented log files, replay stream. |
| `index` | In-memory `ChunkIndex` and `PostingsIndex`. |
| `query` | Core query primitives: `QueryPlan`, `ChunkSeriesCursor`, chunk decoding. |
| `compactor` | L0/L1/L2 compaction planning and execution. |

### Layer 2 — Shared shell
State containers and cross-cutting services that the owner modules operate on:

| Module | Role |
|---|---|
| `engine.rs` | `ChunkStorage` — the top-level struct with five state buckets. |
| `state` | All internal state type definitions. |
| `visibility` | MVCC fence and per-series visibility summaries. |
| `runtime` | Background thread management and supervision. |
| `metrics` / `observability` | Lockless counters and snapshot generation. |
| `shard_routing` | Maps series IDs to 1-of-64 shards; dead-lock-safe lock ordering. |
| `metadata_lookup` | Distributed-shard series resolution. |
| `process_lock` | Exclusive-open file lock. |

### Layer 3 — Owner modules
Higher-level operations that drive the shell:

| Module | Role |
|---|---|
| `bootstrap` | Six-phase startup: discovery → recovery → WAL open → hydrate → replay → finalize. |
| `ingest` + `ingest_pipeline` | Five-phase write pipeline (resolve → prepare → apply → commit). |
| `write_buffer` | Active-head flush policies. |
| `lifecycle` | Flush, persist, replay, and shutdown orchestration. |
| `query_exec` + `query_read` | High-level query execution and low-level read path. |
| `deletion` | Tombstone-based series deletion. |
| `rollups` | Persistent downsampling materialization. |
| `maintenance` + `tiering` + `registry_catalog` | Tier lifecycle, catalog validation. |

### State buckets in `ChunkStorage`

To contain lock contention, `ChunkStorage` organises mutable state into five separate structs:

| Struct | Contents |
|---|---|
| `CatalogState` | `SeriesRegistry`, 64 `write_txn_shards` (`Mutex`), pending series IDs, persistence lock. |
| `ChunkBufferState` | 64-shard active builders, 64-shard sealed chunks, persisted-chunk watermarks, monotone chunk sequence counter. |
| `VisibilityState` | Tombstones, materialized series, per-series visibility summaries, flush visibility RwLock. |
| `PersistedStorageState` | `PersistedIndexState`, WAL handle, compactors, tiered-storage config, pending segment diff. |
| `RuntimeConfigState` | Retention/partition/skew windows, memory budget, cardinality limit, WAL size limit, concurrency bounds. |

---

## 5. Write path

A single `insert_rows` call traverses six steps before returning to the caller.

```
insert_rows(&[Row])
    │
    ▼
[1] Semaphore acquire          (max_writers permits, write_timeout deadline)
    │
    ▼
[2] Shard lock acquisition     (minimal set of write_txn_shards, ascending order)
    │
    ▼
[3] WriteResolver              — validate labels, resolve or mint SeriesId,
    │                            append WAL SeriesDefinitionFrame for new series
    ▼
[4] WritePreparer              — normalise timestamps, reject future skew,
    │                            reject below retention floor,
    │                            admission control: memory budget, cardinality limit, WAL size
    ▼
[5] WriteApplier               — append ChunkPoint to ActivePartitionHead (ChunkBuilder),
    │                            append WAL SamplesBatchFrame
    ▼
[6] WriteCommitter             — seal full chunks, publish visibility,
                                 update tombstone/rollup cross-references,
                                 notify background flush/compaction workers
```

Each `Row` carries a `metric`, `Vec<Label>`, and a `DataPoint`. The 64-shard split means that independent series in the same batch can be admitted concurrently.

**Admission control** in the prepare phase enforces:
- `memory_budget_bytes` — total in-memory chunk bytes.
- `cardinality_limit` — maximum unique series count.
- WAL size limit — maximum outstanding un-flushed WAL bytes.

Admission failures return a per-Row `WriteResult` when calling `insert_rows_with_result`; the synchronous `insert_rows` drops rejected rows silently.

---

## 6. Read path

```
select / select_all / select_with_options
    │
    ▼
[1] candidate_planner          — resolve metric + label matchers → Vec<SeriesId>
    │                            via PostingsIndex (postings bitmaps, regex → finite literal set)
    ▼
[2] TieredQueryPlan            — compute which tiers to include (hot-only if within hot window,
    │                            else hot + warm + cold)
    ▼
[3] QuerySnapshotContext       — take visibility-fenced snapshot:
    │                            acquire flush_visibility_lock (read)
    │                            snapshot active chunks, sealed chunks, persisted index
    │                            release lock
    ▼
[4] Per-series read            — for each SeriesId:
    │    a. Scan active ChunkBuilders (in-memory, unsorted tail)
    │    b. Scan sealed EncodedChunks (flushed from active, pending segment write)
    │    c. Scan PersistedIndexState (mmap'd segment files on disk, hot tier)
    │    d. Optionally scan warm/cold tiers (object store or additional paths)
    ▼
[5] Merge and decode           — ChunkSeriesCursor binary-searches each chunk's
    │                            TimestampSearchIndex, decodes only the needed range,
    │                            merge-sorts streams from all sources
    ▼
[6] Aggregation / downsampling — optional bucket aggregation via query_aggregation
    │
    ▼
[7] Return SeriesPoints
```

The `flush_visibility_lock` is the consistency fence: writers hold a write lock during flush publication; readers hold a shared read lock for the duration of snapshot capture, guaranteeing a consistent view.

---

## 7. Write-Ahead Log (WAL)

The WAL lives under `<data_path>/wal/` and is managed by the `FramedWal` struct.

### Frame wire format

```
Offset  Size  Field
     0     4  Magic = b"TSFR"
     4     1  Frame type (1 = SeriesDef, 2 = Samples)
     5     4  CRC-32 of payload
     9     4  Payload length (max 64 MiB)
    13    11  Sequence number (monotone u64 + padding)
    24     N  Payload
```

**`SeriesDefinitionFrame`** — written once per new series: `(series_id, metric, labels)`.

**`SamplesBatchFrame`** — written per chunk flush: `(series_id, lane, timestamp_codec_id, value_codec_id, point_count, base_timestamp, encoded_timestamps, encoded_values)`.

### Durability modes

| Mode | Behavior |
|---|---|
| `WalSyncMode::PerAppend` | `fsync` on every frame append. Crash-safe; throughput-limited. |
| `WalSyncMode::Periodic(d)` | Background `fsync` every duration `d`. Higher throughput; up to `d` of data can be lost on hard crash. |

### Replay modes

| Mode | Behavior |
|---|---|
| `WalReplayMode::Strict` | Abort if any WAL segment is corrupted or unreadable. |
| `WalReplayMode::Salvage` | Skip damaged segments and continue replay. |

### High-water mark

`WalHighWatermark { segment: u64, frame: u64 }` tracks how far replay has been committed. Each persisted segment stores the WAL high-water mark at the time it was written, so recovery knows which WAL frames to replay and which to skip.

The published high-water file (`wal.published`) uses magic `b"TSHW"`.

---

## 8. Chunk encoding

### Chunk lifecycle

```
ChunkBuilder (active)
    │  accumulates ChunkPoints in 64-point frozen blocks + mutable tail
    │  sealed when point count reaches DEFAULT_CHUNK_POINTS (2048)
    ▼
EncodedChunk (sealed)
    │  raw points cleared; only encoded payload survives
    │  held in sealed_chunks until background flush
    ▼
SegmentWriter → persisted segment file
```

### Timestamp codecs

| Codec | Use case |
|---|---|
| `FixedStepRle` | Constant-interval series (e.g. scrape-interval metrics). |
| `DeltaVarint` | Variable-interval series with small deltas. |
| `DeltaOfDeltaBitpack` | Gorilla-style double-delta with bit-packing for irregular series. |

The encoder auto-selects the best codec via `choose_best_timestamp_codec` after inspecting the actual delta distribution.

### Value codecs

| Codec | Use case |
|---|---|
| `GorillaXorF64` | XOR floating-point compression (Gorilla paper). |
| `ZigZagDeltaBitpackI64` | Signed integer delta + bit-packing. |
| `DeltaBitpackU64` | Unsigned integer delta + bit-packing. |
| `ConstantRle` | Run-length encoding for constant-value series. |
| `BoolBitpack` | 1-bit packing for boolean streams. |
| `BytesDeltaBlock` | Block delta encoding for byte-string payloads. |

### Timestamp search index

Each sealed chunk builds a `TimestampSearchIndex` — an array of anchor entries every 64 points. Queries binary-search this index to seek directly into the encoded payload without full decompression, giving O(log N) range access.

---

## 9. Segments and on-disk index

### Directory structure

```
<data_path>/
  lane_numeric/          — float/int/bool/histogram chunks
    l0/                  — freshly flushed segments
    l1/                  — first compaction level
    l2/                  — final compaction level
  lane_blob/             — bytes-value chunks (same level layout)
  wal/                   — WAL segment files
  series_index.bin       — binary RIDX v2 series registry snapshot
  series_index.delta.d/  — incremental series registry delta checkpoints
  series_index.catalog.json — fingerprint catalog for fast registry reload
  tombstones.json.store/ — sharded binary tombstone store (256 shards)
  .rollups/              — rollup state JSON (policies.json, state.json)
  .process.lock          — exclusive-open process lock
```

### Segment manifest

Each segment directory contains:
- Encoded chunk files (one per series × value lane).
- A `SegmentManifest` (`segment_id, level, chunk/point/series counts, min/max timestamps, wal_highwater`).
- A `SegmentPostingsIndex` for label/metric lookups within the segment.
- Checksums; corrupted segments are quarantined automatically.

### In-memory index

`ChunkIndex` — a flat sorted `Vec<ChunkIndexEntry>`, binary-searched for time-range queries. Each entry stores `(series_id, min_ts, max_ts, chunk_offset, chunk_len, point_count, lane, codec IDs, level)`.

`PostingsIndex` — a `BTreeMap<label_name → BTreeMap<label_value → BTreeSet<SeriesId>>>` used to resolve label-matcher queries to candidate series lists.

---

## 10. Series registry

The registry maps `(metric, labels)` pairs to stable numeric `SeriesId` values and maintains postings for fast label-based lookup.

### Internals

- **64-shard split** — `series_shards`, `metric_postings_shards`, `label_postings_shards`, `all_series_shards`; each shard protected by an independent `RwLock`. Reads and writes on different series never contend.
- **Interned string dictionaries** — `metric_dict`, `label_name_dict`, `label_value_dict` store unique strings once; `DictionaryId (u32)` is used internally. Reduces memory for high-cardinality label sets.
- **Roaring Bitmaps** — postings lists are `RoaringTreemap` — compact, fast union/intersection for label-matcher resolution.
- **`SeriesValueFamily`** — inferred at first write (`F64 | I64 | U64 | Bool | Blob | Histogram`); used to select the appropriate codec path for all subsequent writes.

### Series identity

`canonical_series_identity(metric, labels)` produces a deterministic binary key: labels are sorted lexicographically, then length-prefixed and concatenated with the metric name. `stable_series_identity_hash` applies xxh64 to this key for O(1) shard dispatch.

### Persistence

The registry is persisted as a binary RIDX v2 file (`series_index.bin`) with incremental delta checkpoints in `series_index.delta.d/`. On startup, `registry_catalog.rs` validates per-segment xxh64 fingerprints against the catalog; a mismatch triggers a full registry rebuild from segment postings files.

---

## 11. Compaction

tsink uses a three-level LSM-inspired compaction scheme.

```
Active chunks
    │  background flush (250 ms default)
    ▼
L0 segments    ← trigger: 4 L0 segments → compact → L1
    ▼
L1 segments    ← trigger: 4 L1 segments → compact → L2
    ▼
L2 segments    (cold-compacted, largest, longest time span)
```

Each compaction pass:
1. **Planning** (`compactor/planning.rs`) — selects a window of up to 8 source segments at the triggering level.
2. **Execution** (`compactor/execution.rs`) — merge-sorts all chunks, deduplicates overlapping samples, re-encodes with optimal codecs, writes new target-level segment.
3. **Atomic replacement** — the replacement is staged in `.compaction-replacements/` and renamed into place. If the process crashes mid-replacement, `finalize_pending_compaction_replacements` cleans up the staging area at next startup.

Compaction is tombstone-aware: tombstoned time ranges are excluded from the output segment, physically removing deleted data from disk.

---

## 12. Tiered storage and lifecycle

### Tiers

| Tier | Location | Access |
|---|---|---|
| Hot | Local filesystem (`lane_numeric/`, `lane_blob/`) | Direct mmap reads. |
| Warm | Configurable local path or object store | Loaded on demand. |
| Cold | Object store | Loaded on demand; full fetch before query. |

### Tier lifecycle

A background maintenance pass computes a `PostFlushMaintenancePolicyPlan` from `RetentionTierPolicy`:
- **`expired_actions`** — delete segments that fall outside the retention window.
- **`rewrite_actions`** — trim segment boundaries at time-split points.
- **`move_actions`** — promote segments from Hot → Warm or Warm → Cold when they cross the configured retention cutoffs.

### Query tier selection

`TieredQueryPlan { start, end, include_warm, include_cold }` is computed from the query time range vs. tier cutoffs. A query entirely within the hot window skips all remote tier I/O.

---

## 13. Visibility and MVCC fence

### The problem

A flush publishes many new segment entries and visibility summaries at once. Without a fence, a reader could see a partially-published flush: some new segments visible, others not.

### The solution

`flush_visibility_lock` is a `RwLock<()>`:
- **Writers** acquire the exclusive `write` lock for the entire duration of flush publication.
- **Readers** acquire a shared `read` lock during snapshot capture (step 3 of the read path).

This is a brief, bounded mutual-exclusion window — not a per-operation lock. Normal write ingest does not touch this lock.

### Per-series visibility summaries

`SeriesVisibilitySummary` tracks up to `SERIES_VISIBILITY_SUMMARY_MAX_RANGES = 32` disjoint time ranges per series. This handles series with gaps (e.g. intermittently reporting nodes) efficiently: readers can skip time ranges that are known to be empty without scanning segment files.

Fields per summary:
- `latest_visible_timestamp` — highest timestamp ingested for this series.
- `latest_bounded_visible_timestamp` — highest timestamp whose containing chunk has been sealed and persisted.
- `exhaustive_floor_inclusive` — data below this timestamp is complete (no more writes expected).
- `truncated_before_floor` — all data before this point has been tombstoned.
- `ranges` — list of `(min, max)` tuples defining continuous data windows.

---

## 14. PromQL engine

tsink includes a full in-process PromQL evaluator in `src/promql/`.

### Components

| Module | Role |
|---|---|
| `lexer.rs` | Hand-written lexer; produces `Vec<Token>`. |
| `parser.rs` | Recursive-descent Pratt parser; produces `ast::Expr`. |
| `ast.rs` | Complete AST: `VectorSelector`, `MatrixSelector`, `SubqueryExpr`, `BinaryExpr`, `AggregationExpr`, `CallExpr`, `AtModifier`, all operators. |
| `eval/` | Evaluator; implements all PromQL functions and aggregations. |

### Evaluator

`Engine { storage, default_lookback_delta, timestamp_units_per_second }`:

- **`instant_query(query, time)`** — evaluates at a single timestamp.
- **`range_query(query, start, end, step)`** — vectorized evaluation over a step range.

A `PrefetchCache` is keyed by metric name and populated via `select_all` before evaluation, amortising storage round-trips across all eval timestamps in a range query.

Supported features: `rate`, `irate`, `increase`, `delta`, `histogram_quantile`, all standard aggregations (`sum`, `avg`, `count`, `min`, `max`, `topk`, `bottomk`, `quantile`), binary operators with `on`/`ignoring`/`group_left`/`group_right`, subqueries, `@` modifier.

---

## 15. Rollups and downsampling

Rollup policies define persistent, automated downsampling of raw data.

### Policy structure

`RollupPolicy`:
- `policy_id` — stable identifier.
- Source selector — metric name + optional label matchers.
- `aggregation` — `Sum | Min | Max | Avg | Count | ...`.
- `resolution` — output bucket width.
- Retention rules — when to expire rolled-up data.

### Materialization

Materialized series are stored under synthetic metric names of the form `__tsink_rollup__:<policy_id>:<original_metric>`. A rollup worker runs every 5 seconds, reads raw data for each pending policy/source pair, computes aggregated buckets, and writes the result back via a normal `insert_rows` call.

Rollup state is checkpointed as JSON files in `.rollups/` with per-policy per-source progress cursors. When a tombstone is written to a source series, any rolled-up data covering the deleted range is invalidated and scheduled for re-materialization.

---

## 16. Server mode

The `tsink-server` crate wraps the library engine in a full HTTP server.

### Protocol support

| Protocol | Endpoint |
|---|---|
| Prometheus Remote Write | `POST /api/v1/write` (Snappy-framed protobuf) |
| Prometheus Remote Read | `POST /api/v1/read` |
| Prometheus Text Exposition | `POST /api/v1/import/prometheus` |
| InfluxDB Line Protocol v1/v2 | `POST /write`, `POST /api/v2/write` |
| OTLP HTTP | `POST /v1/metrics` (protobuf; gauges, sums, histograms, summaries) |
| StatsD | UDP (counter, gauge, timer, set) |
| Graphite | TCP (plaintext) |
| PromQL instant query | `GET /api/v1/query` |
| PromQL range query | `GET /api/v1/query_range` |

### Connection handling

- `MAX_CONNECTIONS = 1024` simultaneous TCP connections.
- `KEEP_ALIVE_TIMEOUT = 30 s`, `HANDSHAKE_TIMEOUT = 10 s`, `SHUTDOWN_GRACE_PERIOD = 10 s`.
- TLS via `tokio-rustls`; HTTP/1.1 framing.

### Multi-tenancy

Tenant identity is propagated via the `x-tsink-tenant` HTTP header and injected as a `__tsink_tenant__` synthetic label on all writes and reads. Per-tenant admission semaphores govern ingest, query, metadata, and retention concurrency independently.

---

## 17. Clustering and replication

Clustering is an opt-in layer over the single-node server. All cluster logic lives in `crates/tsink-server/src/cluster/`.

### Topology

```
Client
  │  writes / reads
  ▼
Any cluster node (HTTP)
  │
  ├── WriteRouter        — splits batch by consistent hash ring → local + remote shards
  │     │
  │     ├── local write  — directly into local ChunkStorage
  │     └── remote write — parallel RPC fanout to replica owners
  │
  └── ReadFanoutExecutor — fans query to owner shards, merges responses
```

### Consistent hash ring (`cluster/ring.rs`)

`ShardRing { shard_count, replication_factor, virtual_nodes_per_node = 128 }`:
- Each node contributes 128 virtual tokens distributed by xxh64 hash around the ring.
- Each shard is pre-assigned to `replication_factor` owner nodes.
- `stable_series_identity_hash(metric, labels)` → deterministic shard selection — the same series always lands on the same shard regardless of which node receives the write.

### Write routing (`cluster/replication.rs`)

`WriteRouter` decomposes an incoming batch into a `WritePlan { local_rows, remote_batches }`. Remote batches are sent in parallel via `tokio::task::JoinSet` (max 32 in-flight batches, max 1024 rows per batch). Failed remote writes are enqueued to `HintedHandoffOutbox` for retry.

**Consistency levels** — `ClusterWriteConsistency { One | Quorum | All }` — control how many replicas must acknowledge before the write is considered successful.

### Control plane (`cluster/consensus.rs`)

A custom Raft-like log replication layer manages cluster membership and control state:
- Tick interval: 2 s; max append entries: 64; snapshot every 128 entries.
- Leader election with suspect timeout: 6 s; dead timeout: 20 s; leader lease: 6 s.
- Control log persisted as NDJSON to `tsink-control-log`.

### Anti-entropy repair (`cluster/repair.rs`)

`DigestExchangeRuntime` runs background anti-entropy:
- Every 30 s, each node exchanges window digest hashes with its peers for up to 64 shards per tick.
- A digest mismatch triggers a `InternalRepairBackfillRequest` to re-replicate missing data.
- Rebalance moves shard ownership between nodes in 5 s intervals with configurable row-per-tick limits.

### Deduplication

`DedupeWindowStore` provides idempotency-key deduplication to prevent re-ingestion of retried writes during hinted handoff re-delivery.

---

## 18. Security model

### TLS

All external HTTP and peer-to-peer traffic can be protected by TLS. Certificates are loaded via `tokio-rustls` and support hot rotation without server restart via `SecurityManager`.

Internal cluster traffic uses mTLS with a dedicated cluster CA to authenticate peer nodes.

### Authentication

Two methods are supported:
1. **Bearer tokens** — loaded from a file or via an exec command; verified on every request.
2. **OIDC JWT** — RS256, ES256, or HS256 tokens issued by a configured identity provider; JWKS fetched at startup with 60 s clock skew tolerance.

### RBAC (`src/rbac.rs`)

`RbacRegistry` assigns roles to principals. Actions are `Read | Write`; resource kinds are `Tenant | Admin | System`. Service accounts carry 32-byte random tokens (base64url-encoded, HMAC-backed) with configurable rotation schedules.

An in-memory ring buffer retains the last 256 RBAC decisions for audit review.

### Secret rotation

`SecurityManager` supports runtime rotation of:
- Public and admin auth tokens.
- Cluster internal auth token.
- TLS listener certificate/key.
- Cluster mTLS certificate/key.

A 300 s overlap grace period (configurable) allows in-flight requests to complete with the old credential before it is invalidated.

---

## 19. Observability and background runtime

### Self-instrumentation

`StorageObservabilityCounters` provides lockless `AtomicU64` counters grouped by subsystem:

| Counter group | What it tracks |
|---|---|
| `WalObservabilityCounters` | Replay runs, frames, points, errors, durations. |
| `FlushObservabilityCounters` | Flush runs, backpressure events, active chunk stats. |
| `CompactionObservabilityCounters` | Runs, source/output segment/chunk/point counts. |
| `QueryObservabilityCounters` | Hot/warm/cold tier plans, chunks read, fetch durations. |
| `RollupObservabilityCounters` | Materialization runs, points produced. |
| `RetentionObservabilityCounters` | Expired series, segments, points. |
| `HealthObservabilityState` | `fail_fast_triggered`, last error strings. |

`observability_snapshot()` converts raw counters + runtime state into the public `StorageObservabilitySnapshot` DTO.

The server exposes `/metrics` in Prometheus exposition format and `/healthz` / `/ready` Kubernetes-compatible probes.

### Background threads

The runtime (`src/engine/runtime.rs`) manages three background workers:

| Worker | Interval | Activity |
|---|---|---|
| Flush | 250 ms | Flushes `BackgroundEligible` or `BackgroundBounded` active chunks to the sealed queue. |
| Compaction | 5 s (+ triggered) | Runs `Compactor::compact_once` for numeric and blob lanes. |
| Rollup | 5 s | Executes pending rollup materializations. |

Workers check three lifecycle states (`STORAGE_OPEN / CLOSING / CLOSED`) and stop themselves when the engine begins shutdown. If `background_fail_fast = true` (default), a background worker panic sets `fail_fast_triggered` and causes subsequent writes to return an error.

---

## 20. Configuration reference summary

Key engine knobs and their defaults:

| Option | Default | Notes |
|---|---|---|
| `timestamp_precision` | Nanoseconds | `Ns | µs | ms | s` |
| `retention_window` | 14 days | Data older than this is eligible for expiry. |
| `future_skew_window` | 15 min | Writes with future timestamps beyond this are rejected. |
| `partition_window` | 1 hour | Time-bucket width for active partition heads. |
| `max_active_partition_heads_per_series` | 8 | Maximum concurrent open partitions per series. |
| `max_writers` | cgroup CPU count | Write parallelism gate (`Semaphore` permits). |
| `write_timeout` | 30 s | Maximum wait time for a write permit. |
| `memory_budget_bytes` | `u64::MAX` (no limit) | Total in-memory chunk budget. |
| `cardinality_limit` | `usize::MAX` (no limit) | Maximum unique series count. |
| `chunk_points` | 2048 | Points per sealed chunk. |
| `compaction_interval` | 5 s | Background compaction frequency. |
| `flush_interval` | 250 ms | Background flush frequency. |
| `background_fail_fast` | `true` | Worker panic triggers engine failure mode. |

Container-aware defaults: `cgroup.rs` reads `/sys/fs/cgroup/cpu.max` and `/sys/fs/cgroup/memory.max` to detect CPU and memory limits. The `TSINK_MAX_CPUS` environment variable overrides the detected CPU count.
