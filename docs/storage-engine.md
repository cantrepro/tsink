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
16. [Close and Shutdown](#close-and-shutdown)
17. [Directory Layout](#directory-layout)

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

Every point is classified as `Numeric` or `Blob` from its own value. A batch may contain different
lanes and value families when they belong to different series. Within one series, however, the
engine enforces one lane and one compatible value family across the batch and existing history;
an incompatible value returns `TsinkError::ValueTypeMismatch` for the whole batch. Lanes are stored
in separate directory trees on disk (`lane_numeric/` and `lane_blob/`), which keeps numeric and blob
compaction independent. The batch failure boundary is specified in
[ADR 0001](adr/0001-write-contract.md).

---

## Write Path

A non-empty call to `insert_rows` is processed through a five-stage ingest pipeline:

```
insert_rows(rows)
    │
    ├─ 0. Admit       — check row/input limits and atomically reserve modeled transient memory
    ├─ 1. Resolve     — validate metrics/labels; look up or provisionally create series IDs
    ├─ 2. Prepare     — validate lanes, value families, retention/partitions, and admission;
    │                  encode the candidate WAL payloads
    ├─ 3. Stage       — persist an unpublished logical WAL write (depending on WAL configuration)
    ├─ 4. Apply       — install all points in the active write buffers
    └─ 5. Publish     — commit the WAL boundary when present and establish the batch acknowledgement
```

All five phases run inside registry write-transaction shard locks scoped to the unique series in the
batch. These locks keep provisional series identity, staged WAL state, and in-memory application
inside one intended write boundary. They are fine-grained shards rather than one global mutex.

For a non-empty batch, row-count and modeled-input-byte checks run before tsink clones an identity
or value. Admission then reserves the conservative peak for coexisting preparation structures, WAL
encoding, indexed outcomes, and retained-growth overlap. A write permit is acquired from a semaphore
bounded by `max_writers` before entering stage 1. If the memory budget is exhausted, admission
control parks the writer until flushing reclaims budget. The cloneable lease is shared by retry and
best-effort sub-operations and its final `Drop` releases it on every exit path.

Both public insert methods return one batch error, rather than per-row outcomes, when the input is
rejected; none of those rows is accepted. Apply stages fallible active-state rotations and chunk
encoding for every affected shard before publishing any of them, so a later-shard failure cannot
leave an earlier shard installed. See [ADR 0001](adr/0001-write-contract.md) for the atomic batch,
acknowledgement, and empty-batch contract.

`Storage::write_batch` is the canonical result-bearing entry point. `Atomic` uses the pipeline once
and reports every row accepted or every row rejected. `BestEffort` uses ordered singleton pipeline
calls, preserves each original index, and reports the weakest acknowledgement among accepted rows.
The default trait implementation is unsupported so third-party backends cannot accidentally claim
these semantics through a legacy adapter.

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
| `PerAppend` (default) | Synchronized software path when published | Syncs the staged batch; successful WAL publication normally establishes `Durable`. |
| `Periodic(interval)` | Append-driven sync | Checks elapsed time during append; the batch is normally `Appended`, or `Durable` if that append performs a sync. |

`insert_rows_with_result` returns one `WriteResult` for the entire successful batch. It reflects
whether that batch is already durable; it is not a list of per-row outcomes. On an open read-write
engine, an empty batch is a successful no-op acknowledged as `Durable`. See
[the durability contract](durability.md) and [ADR 0001](adr/0001-write-contract.md).

If logical WAL publication degrades after in-memory apply, either sync mode can instead return a
successful `Volatile` acknowledgement. Periodic mode has no autonomous timer; another append or
lifecycle/persistence work is needed to advance durability.

### WAL high-watermark

A separate file `wal.published` records the highest `(segment, frame)` pair that has been durably flushed to a segment on disk. On crash recovery this watermark determines which WAL frames have already been persisted and can be skipped during replay.

### Replay modes

| Mode | Behavior |
|---|---|
| `Strict` (default) | Any checksum mismatch or truncation fails the open call immediately. |
| `Salvage` | Skips corrupted frames whose boundaries are intact; quarantines the corrupt segment and continues from the next. |

On open, if the last active segment is found corrupt, it is quarantined (left in place) and a fresh segment is started.

Lifecycle replay does not collect the WAL into one pending write. It reserves each bounded frame
before payload allocation, applies definitions singly, and decodes one sample batch at a time under
that same lease. Decoded sample scratch has an intrinsic 256 MiB modeled ceiling in addition to
optional configured write-batch limits.

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

Persisted decode has format-level safety ceilings in addition to configurable resource budgets.
Decoded `series`, `postings`, `chunk_index`, and registry checkpoint files are limited to 256 MiB;
one decoded chunk payload and one modeled full-chunk decode peak are also limited to 256 MiB.
`chunks.bin` is limited to 1 GiB, including the aggregate decoded payload retained by a full segment
load, and a chunk remains limited to 65,535 points by its `u16` header field. Writers reject output
above the same compression-input ceilings.

Before reserving count-driven vectors, readers prove fixed-width tables and label-pair blocks fit in
the bounded file. Zstd output is decoded through a fixed 16 KiB transfer buffer into one preflighted
destination, stops if actual output exceeds the declared length, and requires the final length to
match exactly. Uncompressed framed metadata is borrowed from the already bounded input instead of
being copied. A full segment load reads and releases its metadata inputs one file at a time, then
moves decoded chunk payloads into their final series groups instead of cloning them. These are
corruption and allocation-amplification guards, not a claim that the process
uses at most those values; resource profiles may enforce lower limits and other simultaneously
retained structures still contribute to memory use.

The version-2 byte layout did not change. Existing files within these ceilings remain readable; a
previously representable oversized file is now rejected explicitly rather than being allowed to
drive an unchecked allocation.

Before persistent startup materializes its registry, inventory, or loaded segment indexes, a
single conservative admission ledger covers the base registry plus all incremental files and every
numeric/blob/tier segment. It charges physical mapping lengths and expanded decoded/index state,
and uses the lower of the configured startup budget and the format ceiling for individual metadata
decodes. Admission is completed before corrupt-segment quarantine or compaction recovery mutates
durable names; `MemoryBudgetExceeded.required` reports the next exact modeled threshold.

### Block-level timestamp search index

To support sub-chunk time range seeks without decompressing the whole payload, the encoder builds an in-memory **search index** over 64-point anchor blocks:

- `FixedStep` — arithmetic on `(first_ts, step)` directly computes the block.
- `DeltaVarint` / `DeltaOfDelta` — one anchor per 64 points stores `(point_idx, timestamp, payload_offset)`.

Queries use the search index to identify the candidate block and then decompress only that block, giving O(log n / 64) decode cost for a point lookup.

---

## Flush Pipeline

A background thread runs the flush pipeline on a configurable interval (default **250 ms**):

For WAL-backed, non-tiered storage under normal memory and WAL pressure, the timed selector defers
the current partition head until it contains at least half of its currently allocated point block.
Older/non-current and already-sealed chunks remain immediately eligible. No-WAL storage, tiered
publication, retained-memory pressure, and finite-WAL pressure admit younger current heads; an
explicit flush and close always drain them. This fill-aware rule keeps the WAL as the durable copy
of low-rate recent points without fragmenting the persisted index every 250 ms.

1. **Select replay-closed work** — bounded background passes take a global sealed-chunk sequence prefix subject to item and modeled-byte limits. With a WAL, the selected maximum frame must be strictly before every deferred sealed/active floor.
2. **Write and verify segment files** — group selected chunks by lane, atomically publish new L0 directories, then load and validate their indexes.
3. **Persist recovery metadata** — persist the selected registry delta required to decode the new roots. A bounded pass does not rebuild the complete registry catalog; the serialized persisted-refresh owner later applies the exact root delta to its per-segment sidecar.
4. **Publish persisted visibility** — under the catalog visibility fence, install the verified indexes and make the roots query-visible as one transition.
5. **Release sealed memory** — only after successful visibility publication, advance persisted chunk watermarks, remove pending locators, and evict covered sealed chunks.
6. **Advance durability and trim safely** — mark the selected replay-closed WAL high-water mark durable, then reset/trim only when no newer committed write makes that reset unsafe.

Any failure before step 4 rolls back the staged roots and leaves WAL durability and the pending
sealed prefix unchanged for retry. Lowering a segment checkpoint below selected data would duplicate
it on replay, while advancing through a deferred floor would skip unpersisted data; the closure test
rejects both outcomes.

For non-tiered storage, step 4 accounts the exact added/removed roots and updates segment counters
without rebuilding the live segment inventory. Registry validation state lives in
`series_index.catalog.d/`: one bounded manifest plus one fingerprint entry per segment.
A root-changing maintenance page first publishes its bounded intent in the manifest, applies only
the named removes/upserts, and then marks that intent complete. The aggregate series fingerprint is
cleared before mutation, so an interrupted page cannot falsely enable the registry fast path;
retry replays the same intent idempotently. Complete foreground/startup checkpoints rebuild that
fingerprint and retain the v2 JSON compatibility snapshot. Tiered storage still publishes complete
local and shared segment-catalog snapshots; that monolithic catalog format remains proportional to
the live catalog.

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

The registry is checkpointed to `series_index.bin` (magic `RIDX`, version 2). Startup remains
compatible with the legacy `series_index.delta.bin` sidecar and immutable
`series_index.delta.d/delta-<nonce>.bin` files. New incremental publications use a checksummed
`RJNL` wrapper: small registry subsets are merged into `journal-active.bin` up to 1,024 series and
4 MiB of stored/decoded payload, then the active file is parent-synced under an immutable
`journal-<nonce>.bin` name before a replacement active generation is published. A crash between
those two publications leaves the sealed generation replayable, and a torn or checksum-invalid
managed journal fails closed. On startup the base checkpoint is loaded first, then all recognized
legacy and journal generations are merged by stable series ID; unknown directory entries are never
treated as registry data or removed by rollover/checkpoint cleanup.

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

Rollup maintenance source reads use the same `QueryExecution` accounting as foreground reads. They
inherit every finite instance scan, result, intermediate-memory, concurrency, and deadline limit;
the finite maintenance byte ceiling can only tighten that envelope. Snapshot and append-sort
working sets reserve before allocation, so a source that cannot fit returns a structured query
limit error instead of a partial vector. Background rollup treats that error as source-local and
continues later sources in its admitted postings page. Finite explicit rollup triggers share the
same seek cursor and process one item/byte-bounded page; their snapshot reports whether another
call is required and the next policy/series position. Building status uses accumulated traversal
counters instead of a fresh full source scan. `ExpertUnlimited` retains its explicit unbounded
full-cycle manual behavior unless the query or maintenance controls are overridden.

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

The persisted-refresh worker evaluates one root-ordered inventory page per wake. Each inspected
descriptor consumes the maintenance item/byte allowance; every selected rewrite, move, or expiry
also charges the logical source bytes declared by the segment manifest. A page publishes only its exact root delta,
and its cursor advances only after the two-phase replacement finishes. A publication error therefore
retries the same cursor, while new roots behind the cursor are picked up after the finite cycle wraps.
Lifecycle startup, close, and capacity-reclamation sweeps remain complete strict scans.

Moves, rewrites, and expiry are published through the two-phase post-flush replacement protocol in
[ADR 0004](adr/0004-post-flush-segment-replacement.md). `Prepared` is rollback-safe; `Committing`
keeps outputs and converges source retirement. A pending marker fences compaction, snapshot export,
and inventory scans until runtime or startup recovery completes.

A `segment_catalog.bin` file on the object store serves as the authoritative inventory of remote segments so the local node can rebuild its view on startup or after a remote catalog refresh (default every **5 seconds**).

The ordinary local registry sidecar is incremental: a root/action page touches only its bounded
manifest intent and the named segment entry files. Its namespace is capped at 16,382 live entries,
its serialized pending intent at 16 MiB, its manifest/entry decodes at 17 MiB/16 KiB, and complete
checkpoints prune recognized stale entries. Legacy v2 JSON remains readable behind a 64 MiB decode
ceiling and is migrated by the next startup/complete checkpoint.

A finite non-tiered unknown-dirty runtime refresh scans in charged lane/level pages and retains a
deduplicated snapshot within the maintenance byte ceiling and fixed 16,384-entry namespace. It
publishes no partial inventory. Once scanning reaches its terminal probe, stable add and prune
cursors publish exact root deltas; failure retries the same intent, visibility changes restart the
cycle, and startup recovery discards an interrupted cursor and hydrates strictly from disk. The
tiered local/shared segment catalog remains a monolithic crash-safe snapshot. Tiered publication,
`ExpertUnlimited`, startup, and close therefore retain complete-inventory behavior.

The object-store root has a single-writer invariant. One read-write engine holds
`.tsink-writer.lock` for its lifetime and revalidates that pathname's file identity before shared
tier or catalog mutation; a second read-write engine is rejected even when it uses a different
local data path. Compute-only engines do not take the writer lease and may share the root. See
[ADR 0005](adr/0005-cross-filesystem-tombstone-transactions.md) for the lease and delete-publication
protocol.

---

## Tombstones and Deletion

Deleting a series or a time range writes a `TombstoneRange { start, end }` record to `tombstones.json` (version 1 format) or the sharded `tombstones.store/` store (version 2, 256 shards). A local two-phase coordinator makes multi-lane publication crash recoverable. With tiered storage, one complete remote manifest is published first as the compute-only visibility anchor; the API does not acknowledge the delete as committed before that anchor is durable. Tombstones are **not** applied inline at write time; instead:

- **Query path** — active and sealed chunks are filtered at read time against the in-memory tombstone map.
- **Compaction path** — tombstone ranges are applied during the merge step so compacted output segments no longer contain deleted data.

Catalog refresh publishes recovered tombstones before segment changes, and compaction reads the
current authoritative tombstone map. This keeps deleted samples hidden during retries and ensures
they are eventually reclaimed. The full protocol is specified in
[ADR 0005](adr/0005-cross-filesystem-tombstone-transactions.md).

---

## Memory Budget and Backpressure

The modeled storage budget covers active and sealed chunks, registry and metadata state, persisted
indexes and mapping lengths, tombstones, and conservative transient write/replay reservations.
Retained components use incremental deltas; transient leases use one shared atomic current counter
so concurrent writers cannot all pass a stale budget check.

When the total exceeds `memory_budget_bytes`:

1. The flush pipeline is triggered immediately to convert sealed chunks to disk segments.
2. New writes are held in admission control, polling every `admission_poll_interval` (default **10 ms**) until the budget recovers.
3. If the budget is still exhausted after the write timeout (default **30 s**), the write returns an error.

A separate `cardinality_limit` caps the number of unique series that can be registered. Writes that would exceed the limit are rejected.

The caller-owned input slice and the fixed WAL `BufWriter` remain outside this budget. Effective
limits report the writer-buffer capacity, while memory observability reports current/peak transient
bytes, admitted leases, rejected leases, and the named excluded categories. This is modeled engine
memory, not a process-RSS cap.

---

## Close and Shutdown

`Storage::close()` first changes the lifecycle from `OPEN` to `CLOSING` and unparks every owned
worker. It then takes the shared maintenance gate, drains every writer permit, takes the rollup
transaction fence, and preflights the compaction gate. The maintenance, writer-drain, and
compaction waits each use the configured `write_timeout`; a contended lifecycle gate returns
`LifecycleTimeout`, restores `OPEN`, and leaves WAL and in-memory state intact for retry.

After quiescence, close finalizes every active partition head, persists and verifies all pending
sealed chunks, completes retention, runs at most 128 compaction settling passes, refreshes dirty
persisted state, and checkpoints tombstone and series-registry recovery metadata. It changes the
lifecycle to `CLOSED` only after those durability stages succeed. Owned workers are then joined in
the order compaction, flush, persisted refresh, and rollup; the data-path process lease is released
after the last join.

This procedure bounds the outer lifecycle waits and compaction pass count, but it is not a portable
wall-clock deadline. Complete drains scale with the accepted state captured after writer
quiescence, and close intentionally completes unknown-dirty catalog reconciliation rather than
resuming the finite background cursor.
Inner publication locks are not individually timed; production mutators are fenced by the outer
drain, while a concurrent query can briefly retain the visibility read fence during its bounded
source snapshot.
More importantly, blocking file writes, `fsync`, directory sync, rename, and removal cannot be
cancelled safely after they enter the kernel. Returning early could claim failure while durability
actually commits later, or release the data-path lease over an in-flight mutation. Close therefore
waits for an entered filesystem operation and reports its actual error. The background
observability snapshot reports close outcomes, coordination waits/timeouts, compaction passes,
whole-call duration, and join duration.

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
