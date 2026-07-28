# Architecture Overview

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

tsink ships as five interconnected Rust crates in a single workspace.

| Crate                                       | Purpose                                                                            |
| ------------------------------------------- | ---------------------------------------------------------------------------------- |
| `tsink` (root `src/`)                       | Embeddable library — the complete storage engine with no async runtime dependency. |
| `tsink-protocol` (`crates/tsink-protocol/`) | Reusable generated Prometheus and OTLP protobuf models; no server or engine.        |
| `tsink-server` (`crates/tsink-server/`)     | Standalone HTTP server binary with protocol ingest, clustering, and RBAC.          |
| `tsink-test` (`crates/tsink-test/`)         | In-process test fixtures and optional protocol test-data helpers.                  |
| `tsink-uniffi` (`crates/tsink-uniffi/`)     | UniFFI-generated Python bindings that wrap the library crate.                      |

The server, testkit, and Python bindings use the same `Storage` trait that application code calls
directly. `tsink-protocol` owns shared wire models without depending on the engine, and the default
testkit does not depend on it unless a protocol feature is enabled.

---

## 2. Repository layout

```text
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
  tsink-protocol/        — shared Prometheus and OTLP protobuf models
  tsink-server/src/      — HTTP server, protocol adapters, cluster logic
  tsink-test/src/        — in-process test fixtures and assertion helpers
  tsink-uniffi/src/      — UniFFI bindings
```

---

## 3. Public API surface

### Core interface — `Storage` trait (`src/storage.rs`)

All interaction with the engine goes through the `Storage` trait. Key methods:

| Method                                  | Description                                                             |
| --------------------------------------- | ----------------------------------------------------------------------- |
| `insert_rows(&[Row])`                   | Synchronous batch write.                                                |
| `insert_rows_with_result`               | Batch write returning one batch-level durability `WriteResult`.         |
| `write_batch(&[Row], WriteMode)`        | Canonical indexed outcomes with explicit atomic or best-effort policy.  |
| `select(metric, labels, start, end)`    | Range read for a single series.                                         |
| `select_with_options`                   | Range read with `QueryOptions` (aggregation, downsampling, pagination). |
| `select_all`                            | Bulk range read across all series matching a selector.                  |
| `list_metrics`                          | Enumerate stored metric names.                                          |
| `select_series`                         | Enumerate series matching a label selector with time-range filtering.   |
| `scan_series_rows` / `scan_metric_rows` | Paginated scan for large result sets.                                   |
| `delete_series`                         | Tombstone-based deletion with optional time range.                      |
| `snapshot`                              | Atomic directory snapshot.                                              |
| `add_rollup_policy` / `run_rollup_pass` | Manage persistent downsampling policies.                                |
| `observability_snapshot`                | Current internal counters and health state.                             |

### Key data types

- **`Row`** — `(metric: String, labels: Vec<Label>, data_point: DataPoint)`.
- **`DataPoint`** — `(timestamp: i64, value: Value)`.
- **`Value`** — `F64(f64) | I64(i64) | U64(u64) | Bool(bool) | Bytes(Vec<u8>) | Histogram(Box<NativeHistogram>)`.
- **`Label`** — `(name: String, value: String)`.
- **`TimestampPrecision`** — `Nanoseconds | Microseconds | Milliseconds | Seconds`.

### Async façade — `AsyncStorage` (`src/async.rs`)

`AsyncStorage` wraps `Storage` without requiring Tokio. It spawns OS threads internally:

- One dedicated write worker receives `WriteCommand` messages over an `async-channel`.
- A pool of read workers handles `ReadCommand` messages concurrently. Standard profiles use their
  finite configured count (the default Embedded profile uses four). The `ExpertUnlimited` profile's
  runtime fallback selects `cgroup::default_workers_limit()`; an explicit
  `with_read_workers(0)` request is normalized to one worker.
- Each channel has a 1,024-command default bound plus independent atomic payload-byte admission
  (64 MiB for writes and 16 MiB for reads). The byte ledger also covers producers waiting to enter
  a full channel and releases each reservation when a worker receives the command.
- Every read command carries a query cancellation token. Dropping the awaiting future cancels
  running built-in query work at cooperative checkpoints; accepted writes retain side-effecting
  completion semantics.
- `list_metrics`, `select_series`, `scan_series_rows`, and `scan_metric_rows` carry their detailed
  result guards through the worker reply, retaining the query permit and result-memory reservation
  until receipt or cancellation. Finite executions reject a backend that advertises `Unaccounted`
  accounting for the requested operation, or returns a missing or undersized guard, before
  accepting its result.
- Uses `parking_lot::Mutex` and `std::thread` — suitable for embedding in any async runtime.

---

## 4. Storage engine overview

The engine lives in `src/engine/` and is structured in three layers.

### Layer 1 — Foundation libraries

Pure data-structure and codec modules with no cross-cutting concerns:

| Module      | Role                                                                     |
| ----------- | ------------------------------------------------------------------------ |
| `chunk`     | In-memory chunk builder and sealed `EncodedChunk`.                       |
| `encoder`   | Timestamp and value codec selection and execution.                       |
| `segment`   | On-disk segment format, reader, writer, index postings.                  |
| `series`    | Series identity, interned string dictionaries, Roaring Bitmap postings.  |
| `wal`       | WAL frame format, segmented log files, replay stream.                    |
| `index`     | In-memory `ChunkIndex` and `PostingsIndex`.                              |
| `query`     | Core query primitives: `QueryPlan`, `ChunkSeriesCursor`, chunk decoding. |
| `compactor` | L0/L1/L2 compaction planning and execution.                              |

### Layer 2 — Shared shell

State containers and cross-cutting services that the owner modules operate on:

| Module                      | Role                                                             |
| --------------------------- | ---------------------------------------------------------------- |
| `engine.rs`                 | `ChunkStorage` — the top-level struct with five state buckets.   |
| `state`                     | All internal state type definitions.                             |
| `visibility`                | MVCC fence and per-series visibility summaries.                  |
| `runtime`                   | Background thread management and supervision.                    |
| `metrics` / `observability` | Lockless counters and snapshot generation.                       |
| `shard_routing`             | Maps series IDs to 1-of-64 shards; dead-lock-safe lock ordering. |
| `metadata_lookup`           | Distributed-shard series resolution.                             |
| `process_lock`              | Exclusive-open file lock.                                        |

### Layer 3 — Owner modules

Higher-level operations that drive the shell:

| Module                                         | Role                                                                              |
| ---------------------------------------------- | --------------------------------------------------------------------------------- |
| `bootstrap`                                    | Six-phase startup: discovery → recovery → WAL open → hydrate → replay → finalize. |
| `ingest` + `ingest_pipeline`                   | Five-phase write pipeline (resolve → prepare → stage → apply → publish).          |
| `write_buffer`                                 | Active-head flush policies.                                                       |
| `lifecycle`                                    | Flush, persist, replay, and shutdown orchestration.                               |
| `query_exec` + `query_read`                    | High-level query execution and low-level read path.                               |
| `deletion`                                     | Tombstone-based series deletion.                                                  |
| `rollups`                                      | Persistent downsampling materialization.                                          |
| `maintenance` + `tiering` + `registry_catalog` | Tier lifecycle, catalog validation.                                               |

### State buckets in `ChunkStorage`

To contain lock contention, `ChunkStorage` organises mutable state into five separate structs:

| Struct                  | Contents                                                                                                       |
| ----------------------- | -------------------------------------------------------------------------------------------------------------- |
| `CatalogState`          | `SeriesRegistry`, 64 `write_txn_shards` (`Mutex`), pending series IDs, persistence lock.                       |
| `ChunkBufferState`      | 64-shard active builders, 64-shard sealed chunks, persisted-chunk watermarks, monotone chunk sequence counter. |
| `VisibilityState`       | Tombstones, materialized series, per-series visibility summaries, flush visibility RwLock.                     |
| `PersistedStorageState` | `PersistedIndexState`, WAL handle, compactors, tiered-storage config, pending segment diff.                    |
| `RuntimeConfigState`    | Retention/partition/skew windows, memory budget, cardinality limit, WAL size limit, concurrency bounds.        |

---

## 5. Write path

A non-empty `insert_rows` call passes two entry gates and then traverses five ingest phases before
returning to the caller.

```text
insert_rows(&[Row])
    │
    ▼
[1] Semaphore acquire          (max_writers permits, write_timeout deadline)
    │
    ▼
[2] Shard lock acquisition     (minimal set of write_txn_shards, ascending order)
    │
    ▼
[3] Resolve                    — validate metrics/labels, resolve or provisionally mint SeriesId
    ▼
[4] Prepare                    — validate per-series lane/value family, retention and partition
    │                            constraints; enforce admission limits; encode WAL payloads
    ▼
[5] Stage                      — persist series definitions and samples as one unpublished
    │                            logical WAL write (when enabled)
    ▼
[6] Apply                      — install every point in the active write buffers
    │
    ▼
[7] Publish                    — commit the logical WAL boundary when present,
                                 establish one batch-level acknowledgement, and notify workers
```

Each `Row` carries a `metric`, `Vec<Label>`, and a `DataPoint`. A call is intended to remain one
atomic acceptance unit; the 64-shard split limits interference between concurrent calls that touch
independent series.

**Admission control** across resolve and prepare enforces:

- `memory_budget_bytes` — total in-memory chunk bytes.
- `cardinality_limit` — maximum unique series count.
- WAL size limit — maximum outstanding un-flushed WAL bytes.
- optional maximum future skew — rejects timestamps beyond the configured clock-relative cutoff.

Any validation or admission failure returns a structured `TsinkError` for the whole batch, and no
row from that batch is committed. `insert_rows` propagates the same error rather than silently
dropping rejected rows. `insert_rows_with_result` returns one durability acknowledgement only after
the whole batch succeeds. See [ADR 0001: Core batch write contract](adr/0001-write-contract.md).

The canonical `write_batch` surface adds explicit `Atomic` and `BestEffort` modes. It returns one
indexed outcome per row, structured rejection categories, and the weakest acknowledgement among
accepted rows. Best effort deliberately submits singleton atomic writes in order; it is never an
implicit fallback from an atomic call. Safe pre-commit admission failures produce a complete
rejected outcome set; configured top-level row/input bounds return an outer error before allocating
an oversized outcome vector and commit nothing. If full write-memory admission fails, the engine
separately admits the bounded rejection-result envelope; inability to admit even that response is
also an outer memory error. Legacy `Storage` implementations must opt in rather than inheriting
assumed semantics.

Apply holds every affected active-shard lock and runs fallible rotations and encoding against
staged active-series states. Only after all affected shards succeed are those states and any sealed
chunks published. A later-shard encoding error therefore cannot expose an earlier shard from the
rejected batch. Active-head flush finalization likewise leaves the live head intact when encoding
fails.

---

## 6. Read path

```text
select / select_all / select_with_options
    │
    ▼
[0] QueryExecution            — acquire one shared concurrency slot; establish effective
    │                            work, memory, cancellation, and deadline controls
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

One logical core query owns one `QueryExecution`; each query inside a batched Prometheus remote-read
request gets its own sequential execution. Nested reads use the `*_with_execution` methods rather
than acquiring additional permits. Candidate planning, exact metadata scans, chunk decode/merge,
result materialization, and built-in aggregation charge the applicable matched-series,
pattern-expansion, scanned/returned-sample, returned-byte, intermediate-vector, and modeled-memory
limits. Long loops checkpoint cooperative cancellation and the effective wall-time deadline. Permit
and memory leases are RAII-owned, so owned entrypoints release them on every success or error path.
A caller-supplied execution remains admitted until its last clone and reservation are dropped. The
remote-read adapter encodes each query result before releasing its execution and uses a separate
64 MiB aggregate protobuf envelope with bounded Snappy output allocation. All query limits remain
optional at the low-level API. The core's `Embedded` and server's `Server` profiles populate finite
values; the all-`None` legacy configuration is selected explicitly with `ExpertUnlimited`.

---

## 7. Write-Ahead Log (WAL)

The WAL lives under `<data_path>/wal/` and is managed by the `FramedWal` struct.

### Frame wire format

```text
Offset  Size  Field
     0     4  Magic = b"TSFR"
     4     1  Frame type (1 = SeriesDef, 2 = Samples)
     5     4  CRC-32 of payload
     9     4  Payload length (max 64 MiB)
    13    11  Sequence number (monotone u64 + padding)
    24     N  Payload
```

**`SeriesDefinitionFrame`** — written once per new series: `(series_id, metric, labels)`.

**`SamplesBatchFrame`** — staged during a live logical batch write; each item contains
`(series_id, lane, timestamp_codec_id, value_codec_id, point_count, base_timestamp, encoded_timestamps, encoded_values)`.

### Durability modes

| Mode                       | Behavior                                                                                               |
| -------------------------- | ------------------------------------------------------------------------------------------------------ |
| `WalSyncMode::PerAppend`   | Sync each staged batch; successful WAL publication normally establishes `Durable`.                   |
| `WalSyncMode::Periodic(d)` | Check elapsed time on append; a batch can be `Appended` or, when that append syncs, `Durable`.          |

Either mode can return a successful `Volatile` acknowledgement if logical WAL publication fails
after in-memory apply. `Periodic` does not run an autonomous WAL-sync timer; later writes and
lifecycle/persistence work can advance durability. See
[the durability contract](durability.md) and
[ADR 0001](adr/0001-write-contract.md#durability-acknowledgement-is-batch-level).

### Replay modes

| Mode                     | Behavior                                             |
| ------------------------ | ---------------------------------------------------- |
| `WalReplayMode::Strict`  | Abort logical replay if a frame is corrupted or unreadable. |
| `WalReplayMode::Salvage` | Continue logical replay past recoverable corruption only after the WAL passed open-time validation. |

Persistent open always validates the complete published WAL prefix strictly before applying either
logical replay policy. `Salvage` therefore cannot bypass published corruption, rewrite or
quarantine corrupt WAL data, or recover it in place; the supported recovery path writes a separate
destination through `tsink-inspect salvage`. When a supported legacy WAL has no `wal.published`
marker, every existing segment byte is treated as published and must pass the same streaming
validation before tsink derives or installs a marker. The legacy root format identity may already
have been installed with a null successful-open version before this normal recovery check fails.

### High-water mark

`WalHighWatermark { segment: u64, frame: u64 }` tracks how far replay has been committed. Each persisted segment stores the WAL high-water mark at the time it was written, so recovery knows which WAL frames to replay and which to skip.

The published high-water file (`wal.published`) accepts a legacy 24-byte `TSHW` record containing
the published boundary `H`, or a 40-byte `TSH2` record containing `H` and a checksummed
reset-through floor `R`. Both records end in a CRC-32; `TSH2` is rejected unless `R <= H`.
Legacy `TSHW` carries no reset authorization.

Recovery uses the effective floor `F = max(clean persisted replay floor, R)`. When `H > F`, each
logical segment ID from `F.segment` through `H.segment` must have one contiguous physical
representation and validation must reach the exact `H` frame. An empty or short boundary segment,
or one whose first frame is above `H`, is corruption. The frame may be absent only when `F >= H`;
gaps below `F` are already checkpointed or reset. Reads accept only an exact 24- or 40-byte regular
non-link marker, use no-follow open where supported, and verify file identity around the bounded
read.

A reset durably publishes `TSH2` with
`H = R = max(last appended high-water mark, (active segment, 0))` before truncating or removing
WAL files. Future commits preserve `R` while advancing `H`. After recovery validates the marker and
required prefix, it restores the runtime append and durable floors through `H`. Publication
removes only a stale `wal.published.tmp` directory entry without following its target, rejects a
directory, creates the temporary file exclusively with no-follow protection, identity-checks it
before rename, and semantically revalidates the installed marker after rename.

---

## 8. Chunk encoding

### Chunk lifecycle

```text
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

| Codec                 | Use case                                                          |
| --------------------- | ----------------------------------------------------------------- |
| `FixedStepRle`        | Constant-interval series (e.g. scrape-interval metrics).          |
| `DeltaVarint`         | Variable-interval series with small deltas.                       |
| `DeltaOfDeltaBitpack` | Gorilla-style double-delta with bit-packing for irregular series. |

The encoder auto-selects the best codec via `choose_best_timestamp_codec` after inspecting the actual delta distribution.

### Value codecs

| Codec                   | Use case                                        |
| ----------------------- | ----------------------------------------------- |
| `GorillaXorF64`         | XOR floating-point compression (Gorilla paper). |
| `ZigZagDeltaBitpackI64` | Signed integer delta + bit-packing.             |
| `DeltaBitpackU64`       | Unsigned integer delta + bit-packing.           |
| `ConstantRle`           | Run-length encoding for constant-value series.  |
| `BoolBitpack`           | 1-bit packing for boolean streams.              |
| `BytesDeltaBlock`       | Block delta encoding for byte-string payloads.  |

### Timestamp search index

Each sealed chunk builds a `TimestampSearchIndex` — an array of anchor entries every 64 points. Queries binary-search this index to seek directly into the encoded payload without full decompression, giving O(log N) range access.

---

## 9. Segments and on-disk index

### Directory structure

```text
<data_path>/
  lane_numeric/          — float/int/uint/bool chunks
    segments/
      L0/                — freshly flushed segments
      L1/                — first compaction level
      L2/                — final compaction level
    tombstones.json      — legacy tombstone snapshot when present
    tombstones.json.store/ — sharded binary tombstone store (256 shards)
  lane_blob/             — bytes/string/native-histogram chunks (same segment/tombstone layout)
  wal/                   — WAL segment files
  series_index.bin       — binary RIDX v2 series registry snapshot
  series_index.delta.d/  — incremental series registry delta checkpoints
  series_index.catalog.json — fingerprint catalog for fast registry reload
  series_index.catalog.d/ — bounded per-segment registry fingerprint store
  segment_catalog.json   — tiered segment inventory when configured
  .tombstone-transactions/ — crash-recovery record for multi-lane deletes
  .post-flush-replacements/ — crash-recovery records for segment replacement
  .rollups/              — rollup policies/base state JSON plus bounded state-journal generations
  .tsink.lock            — exclusive-open process lock
```

### Segment manifest

Each segment directory contains:

- Five aggregate binary files for the segment: `manifest.bin`, `chunks.bin`,
  `chunk_index.bin`, `series.bin`, and `postings.bin`.
- A `SegmentManifest` (`segment_id, level, chunk/point/series counts, min/max timestamps, wal_highwater`).
- A `SegmentPostingsIndex` for label/metric lookups within the segment.
- Checksums; invalid persisted segments are quarantined during startup recovery.

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

The registry is persisted as a binary RIDX v2 file (`series_index.bin`) with incremental delta
checkpoints in `series_index.delta.d/`. Per-segment xxh64 validation entries live in
`series_index.catalog.d/`; its bounded manifest intent makes ordinary root deltas idempotent without
rewriting every entry. Complete checkpoints retain the legacy `series_index.catalog.json` snapshot
for downgrade compatibility. On startup, `registry_catalog.rs` requires the catalog entry set and
fingerprints to match the visible inventory exactly; an incomplete intent or mismatch triggers a
full registry rebuild from segment postings files.

---

## 11. Compaction

tsink uses a three-level LSM-inspired compaction scheme.

```text
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

| Tier | Location                                         | Access                                     |
| ---- | ------------------------------------------------ | ------------------------------------------ |
| Hot  | Local filesystem (`lane_numeric/`, `lane_blob/`) | Direct mmap reads.                         |
| Warm | Configured mounted filesystem path               | Loaded on demand.                          |
| Cold | Configured mounted filesystem path               | Loaded on demand; full fetch before query. |

The mounted path may be local, FUSE-backed, or a network filesystem. The core does not speak a
native S3/GCS object-store API.

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

| Module      | Role                                                                                                                                        |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `lexer.rs`  | Hand-written lexer; produces `Vec<Token>`.                                                                                                  |
| `parser.rs` | Recursive-descent Pratt parser; produces `ast::Expr`.                                                                                       |
| `ast.rs`    | Complete AST: `VectorSelector`, `MatrixSelector`, `SubqueryExpr`, `BinaryExpr`, `AggregationExpr`, `CallExpr`, `AtModifier`, all operators. |
| `eval/`     | Evaluator; implements all PromQL functions and aggregations.                                                                                |

### Evaluator

`Engine { storage, default_lookback_delta, timestamp_units_per_second }`:

- **`instant_query(query, time)`** — evaluates at a single timestamp.
- **`range_query(query, start, end, step)`** — vectorized evaluation over a step range.
- **`instant_query_with_control(...)` / `range_query_with_control(...)`** — applies
  request-specific limit tightening and a cancellation token.
- **`*_with_execution(...)`** — evaluates under an already-admitted core execution.

A `PrefetchCache` is keyed by metric name and populated via `select_all` before evaluation,
amortising storage round-trips across all eval timestamps in a range query. The complete PromQL
request carries one `QueryExecution` through prefetch, selectors, subqueries, all evaluation steps,
binary and aggregation intermediates, and final result accounting; nested work does not consume a
second query slot.

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

Rollup policies and the rebased state snapshot live in `.rollups/policies.json` and
`.rollups/state.json`. Per-source checkpoint/pending replacements use checksummed
`state-journal-active.bin` / sealed `state-journal-*.bin` generations, so ordinary materialization
does not rewrite the complete cursor map. Full policy/delete state-first snapshots advance the
journal epoch and absorb old generations. When a tombstone is written to a source series, any
rolled-up data covering the deleted range is invalidated and scheduled for re-materialization.

---

## 16. Server mode

The `tsink-server` crate wraps the library engine in a full HTTP server.

### Protocol support

| Protocol                     | Endpoint                                                           |
| ---------------------------- | ------------------------------------------------------------------ |
| Prometheus Remote Write      | `POST /api/v1/write` (Snappy block-compressed protobuf)            |
| Prometheus Remote Read       | `POST /api/v1/read`                                                |
| Prometheus Text Exposition   | `POST /api/v1/import/prometheus`                                   |
| InfluxDB Line Protocol v1/v2 | `POST /write`, `POST /api/v2/write`                                |
| OTLP HTTP                    | `POST /v1/metrics` (protobuf; gauges, sums, histograms, summaries) |
| StatsD                       | UDP (counter, gauge, timer, set)                                   |
| Graphite                     | TCP (plaintext)                                                    |
| PromQL instant query         | `GET /api/v1/query`                                                |
| PromQL range query           | `GET /api/v1/query_range`                                          |

### Connection handling

- `MAX_CONNECTIONS = 1024` simultaneous TCP connections.
- `KEEP_ALIVE_TIMEOUT = 30 s`, `HANDSHAKE_TIMEOUT = 10 s`, and a 10 s shutdown grace
  threshold. Crossing the shutdown threshold logs a warning; owned connection tasks continue
  draining before persistent storage and its process lease are released.
- TLS via `tokio-rustls`; HTTP/1.1 framing.

### Multi-tenancy

Tenant identity is propagated via the `x-tsink-tenant` HTTP header and injected as a `__tsink_tenant__` synthetic label on all writes and reads. Per-tenant admission semaphores govern ingest, query, metadata, and retention concurrency independently.

---

## 17. Clustering and replication

Clustering is an opt-in layer over the single-node server. All cluster logic lives in `crates/tsink-server/src/cluster/`.

### Topology

```text
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

### RBAC (`crates/tsink-server/src/rbac.rs`)

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

| Counter group                     | What it tracks                                          |
| --------------------------------- | ------------------------------------------------------- |
| `WalObservabilityCounters`        | Replay runs, frames, points, errors, durations.         |
| `FlushObservabilityCounters`      | Flush runs, backpressure events, active chunk stats.    |
| `CompactionObservabilityCounters` | Runs, source/output segment/chunk/point counts.         |
| `QueryObservabilityCounters`      | Hot/warm/cold tier plans, chunks read, fetch durations. |
| `RollupObservabilityCounters`     | Materialization runs, points produced.                  |
| `RetentionObservabilityCounters`  | Expired series, segments, points.                       |
| `HealthObservabilityState`        | `fail_fast_triggered`, last error strings.              |

`observability_snapshot()` converts raw counters + runtime state into the public `StorageObservabilitySnapshot` DTO.

The server exposes `/metrics` in Prometheus exposition format and `/healthz` / `/ready` Kubernetes-compatible probes.

### Background threads

The runtime (`src/engine/runtime.rs`) owns at most four named threads per storage instance. There is
no process-global maintenance pool and none of these workers calls embedder code.

| Thread name | When created | Concurrency | Cadence and wakeups | Queue/work bound |
|---|---|---:|---|---|
| `tsink-flush` | Persistent read-write lanes | 1 | 250 ms; ingest/admission may unpark it sooner | One coalescing atomic wake bit. A cursor visits at most the configured maintenance items and modeled input bytes without wrapping through the active catalog twice in one pass. Healthy WAL-backed non-tiered current heads require half an initial point block; no-WAL/tiered durability and memory/WAL pressure override that fill threshold. |
| `tsink-compaction` | At least one local lane compactor | 1 | 5 s; delete publication may unpark it sooner | `park` provides one coalescing wake token. One pass runs at most one numeric and one blob compactor operation; each operation selects at most eight source segments, although inventory discovery still visits the level. |
| `tsink-persisted-refresh` | Persistent lanes, or a compute-only tiered reader | 1 | 250 ms for local state; for compute-only tiering, `min(remote_refresh_interval, 250 ms)`, never below 1 ms; dirty publication unparks it | Retention/tiering, post-flush metadata reconciliation, local catalog refresh, and remote catalog refresh share this single serialized slot. Pending local maintenance is represented by coalescing booleans, not an item queue. Retention/tiering visits one root/action page per wake and applies its ordinary registry-catalog delta through a bounded intent. Finite non-tiered unknown-dirty refresh charges lane/level scan pages, then stable add/prune root deltas, and clears dirty state only after terminal probes. Tiered segment-catalog snapshots remain complete-inventory operations. |
| `tsink-rollups` | Persistent storage (the rollup state directory is available) | 1 | 5 s; writes, deletes, and policy changes may unpark it sooner | `park` provides one coalescing wake token. One background pass visits one policy and one cursor-seeked metric-postings page bounded by maintenance item and modeled identity-byte ceilings. Every raw or existing-materialized source read inherits the instance query work/deadline limits, while a finite maintenance byte ceiling further tightens its memory, scan, result, and intermediate-vector envelope; an oversized source is rejected before append-sort allocation without truncating its checkpoint window. Internal output writes are chunked to finite write-batch limits. Downsample and row assembly still retain proportional cloned data outside the source query reservation. |

All periodic intervals are normalized to a 1 ms minimum before a thread starts, and idle loops use
`park_timeout` rather than polling. `StorageObservabilitySnapshot::background` reports installed and
running threads, the effective interval, notifications, idle waits, passes, exits, and shutdown
joins. It also reports close attempts/results, coordination wait and timeout totals, compaction
passes against the fixed 128-pass close ceiling, whole-close duration, and worker-join wait. The
server exports the same state with fixed worker/event labels; metrics collection cannot create
unbounded labels.

The single persisted-refresh slot is the retention/tiering and remote-catalog concurrency bound; it
is not three hidden threads. Remote tier payload reads occur synchronously on query callers. Their
finite concurrency is therefore the configured shared query concurrency, exposed as
`max_remote_tier_fetch_concurrency`; it remains `None` when queries are explicitly left unbounded.

Workers check three lifecycle states (`STORAGE_OPEN / CLOSING / CLOSED`) before and after acquiring
their outer maintenance gate. Close changes the lifecycle state, unparks every installed worker,
enters the shared maintenance gate, drains writer permits, and preflights the compaction gate. Each
of those coordination waits is capped by the configured write/lifecycle timeout. A timeout returns
`LifecycleTimeout` and restores `STORAGE_OPEN`, so the caller can retry without discarding accepted
WAL or in-memory state. Once the compaction preflight completes, `CLOSING` prevents the owned
compaction worker from beginning another filesystem pass.

Close then drains every accepted active head, publishes and verifies pending segments, runs the
complete retention pass, settles compaction for at most 128 passes, refreshes dirty catalog state,
and checkpoints tombstones and the series registry. Only a successful pipeline changes the state
to `CLOSED`; worker handles are then joined in the fixed compaction → flush → persisted-refresh →
rollup order before the process lease is released. A panic is returned with the worker name, but
shutdown still joins the remaining handles instead of returning early.

This is not a portable whole-call deadline. Once a durability stage has entered `write`, `fsync`,
directory sync, atomic rename, or removal, Rust's blocking filesystem APIs cannot be safely
preempted without abandoning an unknown durability outcome. Complete active/pending/catalog loops
are finite snapshots after writers and maintenance are quiesced, but their duration scales with
accepted state; close intentionally uses complete unknown-dirty reconciliation rather than the
finite background cursor. Worker join
normally has no pass left to execute after the gates drain, but thread scheduling and a blocked
kernel filesystem call have no portable timeout. If `background_fail_fast = true` (default), a
background execution error sets `fail_fast_triggered` and fences subsequent writes.

---

## 20. Configuration reference summary

Key engine knobs and their defaults:

| Option                                  | Default                 | Notes                                                                                  |
| --------------------------------------- | ----------------------- | -------------------------------------------------------------------------------------- |
| `timestamp_precision`                   | Nanoseconds             | Timestamp unit: ns, µs, ms, or s.                                                      |
| `retention_window`                      | 14 days                 | Age window used when retention enforcement is explicitly enabled.                      |
| `future_skew_window`                    | 15 min                  | Observability/bounded-recency window; it does not reject future writes by itself.      |
| `max_future_skew_window`                | unset                   | Optional admission cutoff configured with `with_max_future_skew`.                     |
| `partition_window`                      | 1 hour                  | Time-bucket width for active partition heads.                                          |
| `max_active_partition_heads_per_series` | 8                       | Maximum concurrent open partitions per series.                                         |
| `max_writers`                           | 4                       | `Embedded` write parallelism gate (`Semaphore` permits); `Server` uses 16.              |
| `write_timeout`                         | 30 s                    | Per-acquisition wait for writer admission and close coordination.                     |
| `memory_budget_bytes`                   | 512 MiB                 | `Embedded` modeled storage-memory budget; this is not a process-RSS cap.                |
| `cardinality_limit`                     | 1,000,000               | `Embedded` maximum unique series count.                                                 |
| `chunk_points`                          | 2048                    | Points per sealed chunk.                                                               |
| `compaction_interval`                   | 5 s                     | Background compaction frequency.                                                       |
| `flush_interval`                        | 250 ms                  | Background flush frequency.                                                            |
| persisted-refresh poll interval         | 250 ms or remote minimum | Serialized local/remote catalog, retention, and tiering worker cadence.                 |
| rollup interval                         | 5 s                     | Background rollup frequency.                                                           |
| `background_fail_fast`                  | `true`                  | A background durability failure fences new writes.                                     |

The values above are the default `Embedded` profile; `Test`, `Edge`, and `Server` have their own
finite constants, and `ExpertUnlimited` must be selected explicitly for legacy unbounded limits.
`cgroup.rs` still exposes CPU/memory detection. `TSINK_MAX_CPUS` affects controls that explicitly
request the cgroup-aware worker fallback, such as `with_max_writers(0)`, but does not rewrite a
standard profile.
