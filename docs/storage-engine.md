# Storage Engine Internals

This document describes the internal architecture of the tsink storage engine: how data moves from a write call through the WAL, write buffer, and flush pipeline to immutable on-disk segments, how those segments are compacted and tiered, and how reads traverse the resulting data layout.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Types and Value Lanes](#data-types-and-value-lanes)
3. [Write Path](#write-path)
4. [Write-Ahead Log (WAL)](#write-ahead-log-wal)
5. [In-Memory Write Buffer](#in-memory-write-buffer)
6. [Chunks and Encoding](#chunks-and-encoding)
7. [Encoding Codecs](#encoding-codecs)
8. [Flush Pipeline](#flush-pipeline)
9. [On-Disk Segment Format](#on-disk-segment-format)
10. [LSM-Style Compaction](#lsm-style-compaction)
11. [Series Registry](#series-registry)
12. [Query Execution](#query-execution)
13. [Tiered Storage](#tiered-storage)
14. [Tombstones and Deletion](#tombstones-and-deletion)
15. [Memory Budget and Backpressure](#memory-budget-and-backpressure)
16. [Directory Layout](#directory-layout)

---

## Architecture Overview

The engine is split into three layers:

| Layer | Modules |
|---|---|
| **Foundation libraries** | `chunk`, `compactor`, `encoder`, `query`, `segment`, `series`, `wal`, `binio`, `index`, `tombstone` |
| **Shared shell / state** | `engine` (`engine.rs`), `construction`, `core_impl`, `state`, `visibility`, `runtime`, `metadata_lookup`, `shard_routing`, `metrics`, `observability`, `process_lock` |
| **Owner modules** | `bootstrap`, `ingest` + `ingest_pipeline`, `write_buffer`, `lifecycle`, `maintenance` + `tiering` + `registry_catalog`, `query_exec` + `query_read`, `deletion`, `rollups` |

The engine state is partitioned into five independent structs that can be reasoned about and locked separately:

- **`CatalogState`** — series registry, write-transaction shards, and registry-persistence coordination.
- **`ChunkBufferState`** — 64-shard active builders, sealed chunk buffers, and per-series persisted watermarks.
- **`VisibilityState`** — tombstone map, materialized series set, visibility summaries, and publication fencing.
- **`PersistedStorageState`** — on-disk segment inventory, WAL handles, compactors, and tiering configuration.
- **`RuntimeConfigState`** — immutable options fixed for the lifetime of a storage instance (precision, retention, memory budget, etc.).

---

## Data Types and Value Lanes

Every data point carries a typed `Value`:

| Rust type | Codec family | Lane |
|---|---|---|
| `f64` | Gorilla XOR | Numeric |
| `i64` | ZigZag delta bitpack | Numeric |
| `u64` | Delta bitpack | Numeric |
| `bool` | Bit-pack | Numeric |
| `bytes` / `string` | Bytes delta block | Blob |
| `NativeHistogram` | Bytes delta block | Blob |

Points for a series are written to one of two **value lanes**: `Numeric` or `Blob`. The lane is determined once per batch by inspecting the first value; mixing types within a batch returns an error. Lanes are stored in separate directory trees on disk (`lane_numeric/` and `lane_blob/`), which keeps numeric and blob compaction independent.

---

## Write Path

A call to `insert_rows` is processed through a five-stage ingest pipeline:

```
insert_rows(rows)
    │
    ├─ 1. Resolve     — look up or create series IDs in the registry
    ├─ 2. Prepare     — validate metrics/labels, check memory budget, pick WAL codec
    ├─ 3. Stage       — write encoded frames to the WAL (fsync depending on sync mode)
    ├─ 4. Apply       — append points to the active ChunkBuilder for each series
    └─ 5. Publish     — advance visibility so new points become query-visible
```

Stages 3–5 run inside a registry write-transaction shard lock scoped to the set of unique series being written. This prevents a series definition from becoming query-visible before its WAL frame is committed. The lock is a fine-grained shard lock (one of 64 shards, keyed by series ID) rather than a global mutex.

A write permit is acquired from a semaphore bounded by `max_writers` before entering stage 1. If the memory budget is exhausted, admission control parks the writer until flushing reclaims budget.

---

## Write-Ahead Log (WAL)

### Segmented design

The WAL is a sequence of rotating binary files in the `wal/` subdirectory. Each file is named `wal-{id}.log`. A new segment is created when the active segment reaches the configurable size limit (default **64 MiB**). Completed segments are deleted after the flush pipeline raises the published high-watermark above every frame they contain.

### Frame format

Each WAL entry is a **framed record** with a 24-byte header:

```
Offset  Size  Field
0       4     Magic: 0x54534652 ("TSFR")
4       1     Frame type (1=series_def, 2=samples)
5       1     Flags
6       2     Reserved
8       4     Payload length (bytes)
12      8     Monotonic sequence number
20      4     CRC-32 checksum over header+payload
```

There are two frame types:

- **`SERIES_DEF` (1)** — records a new series ID together with its metric name and label set. Written once on first use; replayed to reconstruct the registry.
- **`SAMPLES` (2)** — carries an encoded batch of data points for one series. Contains series ID, value lane, timestamp codec ID, value codec ID, point count, base timestamp, and separate timestamp/value payloads.

Both frame types use the same length-prefixed codec as the on-disk chunk format, so replay reuses the same decoder as normal reads.

### Sync modes

| Mode | Durability | Notes |
|---|---|---|
| `PerAppend` (default) | Crash-safe | Each append calls `fsync` before acknowledging the write. |
| `Periodic(interval)` | OS-buffered | Frames are flushed to the OS; a periodic background task calls `fsync`. Writes in the crash window since the last sync can be lost. |

`WriteResult` from `insert_rows_with_result` reflects whether the write is already durable.

### WAL high-watermark

A separate file `wal.published` records the highest `(segment, frame)` pair that has been durably flushed to a segment on disk. On crash recovery this watermark determines which WAL frames have already been persisted and can be skipped during replay.

### Replay modes

| Mode | Behavior |
|---|---|
| `Strict` (default) | Any checksum mismatch or truncation fails the open call immediately. |
| `Salvage` | Skips corrupted frames whose boundaries are intact; quarantines the corrupt segment and continues from the next. |

On open, if the last active segment is found corrupt, it is quarantined (left in place) and a fresh segment is started.

---

## In-Memory Write Buffer

Incoming data lives in two stages before it reaches disk:

### Active builders

Each series has one or more `ChunkBuilder` instances, one per open **partition head** (default partition duration: **1 hour**). A builder accumulates `ChunkPoint` objects in memory. Points are stored in two tiers: a **tail** of recent raw points plus a list of **frozen point blocks** (64 points each) that are snapshot-friendly and shareable via `Arc`.

When a builder reaches the chunk point capacity (default **2048** points), it is finalized into an immutable `Chunk` and moved to the sealed buffer.

Up to `max_active_partition_heads_per_series` (default **8**) partition heads can be open simultaneously per series. This accommodates backfill and unordered ingestion across different time ranges.

### Sealed chunks

Finalized chunks waiting to be flushed to disk are held in a 64-shard `RwLock<HashMap<SeriesId, BTreeMap<SealedChunkKey, Arc<Chunk>>>>`. The `SealedChunkKey` combines the chunk's min timestamp and a monotonic sequence number so chunks are naturally time-ordered within each series.

When a chunk is moved to sealed storage its raw point list is dropped (`into_sealed_storage`), keeping only the encoded payload in memory.

### Sharding

Both active builders and sealed chunks are split across **64 shards** keyed by `series_id % 64`. This eliminates most write contention for high-cardinality workloads.

---

## Chunks and Encoding

A `Chunk` is the atomic unit of storage throughout the engine:

```rust
pub struct Chunk {
    pub header: ChunkHeader,   // series_id, lane, value_family, point_count,
                                // min_ts, max_ts, ts_codec, value_codec
    pub points: Vec<ChunkPoint>,        // non-empty only in active builders
    pub encoded_payload: Vec<u8>,       // non-empty in sealed/persisted chunks
    pub wal_highwater: WalHighWatermark,
}
```

The `wal_highwater` records the WAL position of the last frame whose data is included in this chunk. The flush pipeline uses it to determine the safe high-watermark for WAL trimming.

Encoding always produces a compact binary payload from which the original points can be reconstructed. The encoder selects the best codec independently for timestamps and values by trying all applicable candidates and keeping the smallest output.

---

## Encoding Codecs

### Timestamp codecs

| Codec | ID | Description | Best for |
|---|---|---|---|
| `FixedStepRle` | 1 | Stores only the first timestamp and fixed step. O(1) decode. | Regular scrape intervals |
| `DeltaOfDeltaBitpack` | 2 | Delta-of-delta with signed varint (Gorilla-style). | Near-regular with occasional jitter |
| `DeltaVarint` | 3 | Raw delta with signed varint. Guaranteed fallback. | Irregular timestamps |

The encoder tries all three codecs (skipping `FixedStepRle` if the series is not perfectly regular, and skipping `DeltaOfDeltaBitpack` if deltas could overflow) and picks the smallest result.

### Value codecs

| Codec | ID | Description | Best for |
|---|---|---|---|
| `ConstantRle` | 4 | Single value stored once. Checked first, always wins if applicable. | Constant gauges |
| `GorillaXorF64` | 1 | XOR of successive IEEE-754 doubles with leading/trailing zero elision. | Floating point metrics |
| `ZigZagDeltaBitpackI64` | 2 | Delta encoding + ZigZag to bring small negatives near zero, then bitpack. | Monotonically changing signed integers |
| `DeltaBitpackU64` | 3 | Delta encoding then bitpack for unsigned integers. | Counters |
| `BoolBitpack` | 5 | One bit per sample. | Boolean flags |
| `BytesDeltaBlock` | 6 | Length-prefixed byte blocks with optional prefix deduplication. | Histograms, blobs, strings |

### On-disk zstd compression

When a segment file is written, each chunk payload is optionally recompressed with **zstd level 1** (`CHUNK_FLAG_PAYLOAD_ZSTD` flag in the chunk record). This second compression pass is applied per-chunk and its output is accepted only when it is smaller than the raw encoded payload.

### Block-level timestamp search index

To support sub-chunk time range seeks without decompressing the whole payload, the encoder builds an in-memory **search index** over 64-point anchor blocks:

- `FixedStep` — arithmetic on `(first_ts, step)` directly computes the block.
- `DeltaVarint` / `DeltaOfDelta` — one anchor per 64 points stores `(point_idx, timestamp, payload_offset)`.

Queries use the search index to identify the candidate block and then decompress only that block, giving O(log n / 64) decode cost for a point lookup.

---

## Flush Pipeline

A background thread runs the flush pipeline on a configurable interval (default **250 ms**):

1. **Snapshot sealed chunks** — collect all sealed chunks whose sequence numbers exceed the persisted watermark for each series.
2. **Write segment files** — group chunks by lane and write a new L0 segment directory using `SegmentWriter`.
3. **Advance WAL high-watermark** — update `wal.published` to the maximum `wal_highwater` across all flushed chunks.
4. **Trim WAL** — delete WAL segments entirely behind the new high-watermark.
5. **Publish persisted catalog** — atomically swap the in-memory persisted index to include the new segment, making it visible to queries.
6. **Release memory** — update persisted watermarks so sealed chunks can be evicted to free memory budget.

---

## On-Disk Segment Format

Every segment is a **directory** containing exactly four files. Format version: **2** (magic bytes embedded in each file header).

```
{lane_numeric|lane_blob}/
  L0/                    ← compaction level directory
    {segment_id}/        ← one segment directory
      manifest           ← segment metadata and file integrity table
      chunks             ← binary payload of all chunk records
      chunk_index        ← sorted lookup index per (series, time range)
      series             ← metric/label dictionary and series definitions
      postings           ← inverted index for label-based series selection
```

### `manifest` (magic `TSM2`)

80-byte header followed by four 20-byte file entries:

| Field | Description |
|---|---|
| `segment_id` | Monotonically incrementing u64 allocated at flush time. |
| `level` | Compaction level (0, 1, or 2). |
| `chunk_count` | Total number of chunk records. |
| `point_count` | Total number of data points. |
| `series_count` | Number of distinct series. |
| `min_ts` / `max_ts` | Inclusive time range of all chunks. |
| `wal_highwater` | `(segment, frame)` high-watermark of the last WAL frame included. |
| File entries × 4 | `kind`, `file_len` (bytes), `hash64` (xxHash or FNV-1a) for integrity verification. |

### `chunks` (magic `CHK2`)

Binary concatenation of variable-length chunk records. Each record contains a header with codec IDs, point count and timestamp bounds, followed by the encoded (and optionally zstd-compressed) payload.

### `chunk_index` (magic `CID2`)

Fixed-size entries sorted by `(series_id, min_ts, max_ts, chunk_offset)`. Each entry records:

- `series_id`, `min_ts`, `max_ts`
- `chunk_offset` and `chunk_len` — location within the `chunks` file
- `point_count`, `lane`, `ts_codec`, `value_codec`

Range queries binary-search this index instead of scanning the chunks file.

### `series` (magic `SRS2`)

A compact string dictionary (metric names, label names, label values) followed by series definition records. Each record maps a `series_id` to a `metric_id` and a list of `LabelPairId` values into the dictionary.

### `postings` (magic `PST2`)

Three inverted-index sections:

- **By metric name** — maps metric name → `RoaringTreemap` of series IDs.
- **By label name** — maps label name → `RoaringTreemap`.
- **By label name+value pair** — maps (name, value) → `RoaringTreemap`.

Label-matcher queries intersect and difference these bitmaps to identify candidate series IDs before reading any chunks.

---

## LSM-Style Compaction

Compaction runs in a background loop (default **every 5 seconds**) and follows an LSM-style level hierarchy.

### Levels

| Level | Directory | Description |
|---|---|---|
| L0 | `L0/` | Segments written directly by the flush pipeline. May overlap in time. |
| L1 | `L1/` | L0 segments merged together. Smaller overlap. |
| L2 | `L2/` | L1 segments merged together. Minimal overlap, highest compaction ratio. |

### Trigger conditions

A compaction pass runs L0→L1 if either:
- The number of L0 segments reaches `l0_trigger` (default **4**), or
- Any two L0 segments have overlapping time ranges.

Likewise for L1→L2 with `l1_trigger` (default **4**). A compaction window covers at most `source_window_segments` (default **8**) source segments per pass.

### Merge process

1. Load source segment chunk payloads.
2. Group chunks by series ID and merge/sort across sources.
3. Apply tombstone ranges — trim or drop chunks that overlap deleted ranges.
4. Re-encode merged chunks, choosing the best codec for the merged data.
5. Write output L-target segments under `.compaction-replacements/`.
6. Atomically rename output directories into place and delete source directories.

The `.compaction-replacements/` manifest is written before any renames so an interrupted compaction can be detected and completed on the next open.

### Point capacity

The compactor clips chunk `point_count` to the configured `point_cap` (default **2048**, clamped to `[1, 65535]`). Chunks that would exceed the cap are split.

---

## Series Registry

The series registry maps `(metric, labels)` → `series_id`. It is an in-memory structure backed by an on-disk checkpoint file.

### In-memory layout

The registry is split across **64 shards** (hash-partitioned by metric+label key). Each shard maintains:

- A `HashMap<SeriesKeyIds, SeriesId>` for forward lookups (write path).
- A `HashMap<SeriesId, SeriesDefinition>` for reverse lookups (read path).
- A `HashMap<SeriesId, SeriesValueFamily>` for type inference.
- A `StringDictionary` that interns metric names, label names, and label values.

### Persistence

The registry is checkpointed to `series_index.bin` (magic `RIDX`, version 2). Incremental changes since the last full checkpoint are appended to `series_index.delta.bin` or sharded files under `series_index.delta.d/`. On startup the base checkpoint is loaded first, then delta files are replayed in order.

Series IDs are `u64` values assigned from a monotonically increasing counter (backed by an `AtomicU64`).

---

## Query Execution

A query traverses four data sources in order and merges the results:

```
1. Active builders   — points in ChunkBuilder not yet sealed
2. Sealed chunks     — finalized but not yet flushed to disk
3. Hot segments      — local on-disk segments
4. Warm/cold tiers   — remote or tiered object-store segments (if time range requires)
```

### Series selection

Label matchers are resolved against the in-memory series registry and/or the persisted postings indexes. Regex matchers are compiled once and applied against the string dictionary. The result is a set of `SeriesId` values that are then used to drive chunk lookups.

### Time range planning

Before touching any data, the engine computes a **tiered query plan** from the requested time range and retention configuration:

- **hot-only** — query fits within the hot tier's retention window.
- **hot + warm** — query extends into the warm tier.
- **hot + warm + cold** — query spans all tiers.

Tiers not needed by the plan are skipped entirely.

### Chunk index scan

For persisted segments the chunk index entries for each series are binary-searched to find overlapping `(min_ts, max_ts)` ranges. Only matching chunk records are loaded from the `chunks` file (via mmap).

### mmap reads

Segment chunk files are opened as read-only memory maps (`PlatformMmap` backed by `memmap2`). The chunk payload is decoded directly from the mapped slice without a user-space copy. Architecture-specific size limits apply (unrestricted on x86-64 and AArch64; 2 GiB on 32-bit targets).

### Result merging

Points from multiple sources are merged and de-duplicated when necessary. Rollup materialization is checked before falling back to raw scan; if a matching rollup policy covers the query's resolution, the materialized downsampled series is used instead.

---

## Tiered Storage

Three tiers govern where segments live and how long they are retained:

| Tier | Storage | Typical retention |
|---|---|---|
| **Hot** | Local disk | Configurable `hot_retention_window` |
| **Warm** | Object store | `hot_retention_window` → `warm_retention_window` |
| **Cold** | Object store | `warm_retention_window` → full `retention_window` |

After flushing a new segment, the post-flush maintenance policy plan (`PostFlushMaintenancePolicyPlan`) determines which existing segments to move, rewrite, or expire:

- **Move** — copy a hot segment to the object store and delete the local copy.
- **Rewrite** — re-encode a segment before moving (e.g., apply pending tombstones).
- **Expire** — delete segments whose `max_ts` has aged out of the retention window.

A `segment_catalog.bin` file on the object store serves as the authoritative inventory of remote segments so the local node can rebuild its view on startup or after a remote catalog refresh (default every **5 seconds**).

---

## Tombstones and Deletion

Deleting a series or a time range writes a `TombstoneRange { start, end }` record to `tombstones.json` (version 1 format) or the sharded `tombstones.store/` store (version 2, 256 shards). Tombstones are **not** applied inline at write time; instead:

- **Query path** — active and sealed chunks are filtered at read time against the in-memory tombstone map.
- **Compaction path** — tombstone ranges are applied during the merge step so compacted output segments no longer contain deleted data.

This design keeps the write path unaffected by deletions while ensuring deleted data is eventually reclaimed during compaction.

---

## Memory Budget and Backpressure

The engine tracks two memory categories: active builder bytes and sealed chunk bytes. Both are accounted through `MemoryDeltaBytes` deltas applied atomically to a per-shard counter.

When the total exceeds `memory_budget_bytes`:

1. The flush pipeline is triggered immediately to convert sealed chunks to disk segments.
2. New writes are held in admission control, polling every `admission_poll_interval` (default **10 ms**) until the budget recovers.
3. If the budget is still exhausted after the write timeout (default **30 s**), the write returns an error.

A separate `cardinality_limit` caps the number of unique series that can be registered. Writes that would exceed the limit are rejected.

---

## Directory Layout

A fully configured storage instance on disk:

```
{data_path}/
  wal/
    wal-0.log            ← WAL segments (oldest to active)
    wal-1.log
    wal.published        ← flush high-watermark checkpoint
  lane_numeric/
    L0/
      {segment_id}/      ← newly flushed segments
        manifest
        chunks
        chunk_index
        series
        postings
    L1/
      {segment_id}/      ← L0→L1 compacted segments
    L2/
      {segment_id}/      ← L1→L2 compacted segments
    .compaction-replacements/   ← crash-recovery marker for in-progress compactions
  lane_blob/
    L0/  L1/  L2/        ← same structure, blob-lane segments
  series_index.bin       ← series registry full checkpoint
  series_index.delta.d/  ← incremental registry deltas
  tombstones.store/      ← sharded tombstone records
  tsink.lock             ← exclusive process lock (prevents double-open)
```

When tiered storage is enabled, warm and cold segments are under the configured `object_store_root`:

```
{object_store_root}/
  hot/lane_numeric/      ← mirrored hot segments (optional)
  warm/lane_numeric/     ← warm-tier segments
  cold/lane_numeric/     ← cold-tier segments
  segment_catalog.bin    ← remote segment inventory
```
