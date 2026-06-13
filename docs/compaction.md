# Compaction

tsink uses LSM-style leveled compaction to merge small L0 segments produced by the
write pipeline into progressively larger L1 and L2 segments. Compaction reduces the
number of segment files, eliminates duplicate and tombstoned data points, and keeps
query fan-out bounded as data accumulates over time.

---

## L0 / L1 / L2 levels

Every segment carries a level tag written into its manifest at creation time. There are
three levels:

| Level | Written by | Compacted from |
|---|---|---|
| **L0** | Flush pipeline (WAL → segments) | — |
| **L1** | Compactor | L0 |
| **L2** | Compactor | L1 |

Segments at the same level are independent: they may cover overlapping or non-overlapping
time ranges and may belong to different series. The compactor merges segments within a
level and writes the output one level higher. There is no compaction out of L2; L2
segments age out only through retention expiry.

The engine maintains two separate compactors — one for the **numeric lane** (float64
series) and one for the **blob lane** (bytes and native histograms). Each compactor
operates on its own directory subtree and carries its own segment-ID allocator backed by
a shared atomic counter.

---

## Trigger conditions

A compaction pass fires for a given level when either of the following is true:

- **Count trigger** — the number of eligible segments at the source level reaches the
  configured threshold (default **4** for both L0→L1 and L1→L2).
- **Time overlap** — any two segments at the source level have overlapping time ranges,
  regardless of segment count.

The overlap trigger exists because overlapping segments break the sorted-segments
assumption that queries rely on: merging them eagerly keeps the persisted view coherent
and avoids returning duplicates at query time.

Each `compact_once` call checks L0 first, then L1. Only one level is compacted per call.

---

## Window selection

Rather than compacting all available source segments at once, each pass selects a
**window** of up to `DEFAULT_SOURCE_WINDOW_SEGMENTS` (8) segments. The selection
algorithm:

1. If any segments have overlapping time ranges, the algorithm identifies the smallest
   cluster of overlapping segments and selects up to 8 of them by their original storage
   order. This ensures overlaps are resolved before anything else.
2. If there are no overlaps, the algorithm takes the oldest `count_trigger` (≥ 2)
   segments sorted by segment ID.

A window must contain at least two segments; a single-segment window is always a no-op.

---

## Merge algorithm

The merge is a k-way heap merge that streams all source data points in timestamp order
across all input chunks for the same series:

1. Each input chunk is decoded into individual data points and placed behind a cursor.
2. A min-heap ordered by `(timestamp, chunk_order)` drives the merge — the earliest
   timestamp across all cursors is drained first.
3. A point is **skipped** if:
   - It is an exact duplicate of the previously emitted point — same `(ts, value)` pair.
   - Its timestamp falls within a tombstoned range.
   - Its timestamp is older than the retention cutoff (when compaction is used to enforce
     retention).
4. Surviving points are accumulated into output chunks. When a chunk reaches
   `chunk_point_cap` points it is closed and a new one is started.
5. When accumulated output points reach the **segment point budget** —
   `chunk_point_cap × 512` — the current set of chunks is written as a new output
   segment and the accumulator is reset. This caps output segment size even when many
   source segments are merged at once.

Chunk encoding for output chunks uses the same adaptive codec pipeline as all other
segments: delta/XOR encoding for timestamps and values, with optional zstd compression.

The WAL highwater carried by an output segment is the **maximum** highwater across all
source segments, allowing the WAL to reclaim space for any data already compacted to
persistent segments.

---

## Tombstone handling

Before each compaction pass the tombstone store is loaded from disk. A tombstone is a
series-scoped time range (`start` inclusive, `end` exclusive). Any point whose timestamp
falls within a tombstone range for its series is silently discarded during the merge.
Points tombstoned in one compaction pass will never reappear in subsequent reads because
they are permanently excluded from the output segments.

---

## Atomic segment replacement

Compaction writes output segments to disk first, then atomically swaps them in to replace
the source segments. This two-phase protocol makes interruptions safe:

1. **Write outputs** — each output segment is written to a new directory under the lane
   root. If writing fails, any partial output directories are removed (rollback).
2. **Write replacement marker** — a JSON file is written atomically to
   `.compaction-replacements/replace-<ts>-<nonce>.json`. The marker lists the relative
   paths of both source and output segments.
3. **Apply replacement** — the source segment directories are removed and the marker file
   is deleted.

If the process is interrupted between steps 2 and 3, the marker file survives on disk.
The next `compact_once` call (or startup) calls `finalize_pending_compaction_replacements`
which replays all pending markers in sorted order, completing any interrupted
replacements. An output-side manifest must exist for the replacement to be applied; if
it does not, the replacement is treated as a corruption error.

---

## Background thread

The compaction background thread runs continuously while the storage engine is open. It
sleeps for `compaction_interval` (default **5 seconds**) between passes and wakes
immediately when the flush pipeline signals that new segments have been written.

The thread runs both the numeric compactor and the blob compactor in sequence on each
wakeup. A mutex (`compaction_lock`) serialises the thread against manual compaction
calls and against snapshot operations.

On `close()`, the engine acquires all write permits, flushes active state to segments,
and then runs up to **128** compaction passes to drain any remaining work before
shutting down background threads.

When `background_fail_fast` is enabled (the default), a compaction error marks the
storage engine as unhealthy and causes subsequent write and flush operations to return
an error.

---

## Observability

The `observability_snapshot()` method exposes a `CompactionObservabilitySnapshot` with
cumulative counters:

| Field | Description |
|---|---|
| `runs_total` | Total `compact_once` calls |
| `success_total` | Runs that produced at least one output segment |
| `noop_total` | Runs where no compaction was needed |
| `errors_total` | Runs that returned an error |
| `source_segments_total` | Cumulative source segments consumed |
| `output_segments_total` | Cumulative output segments produced |
| `source_chunks_total` | Cumulative source chunks processed |
| `output_chunks_total` | Cumulative output chunks written |
| `source_points_total` | Cumulative input data points before dedup/tombstone filtering |
| `output_points_total` | Cumulative output data points after filtering |
| `duration_nanos_total` | Cumulative wall-clock time spent in compaction |

The ratio `output_points_total / source_points_total` indicates how much deduplication
or tombstone removal is occurring over time.

---

## Tuning

| `StorageBuilder` method | Default | Effect |
|---|---|---|
| `with_chunk_points(n)` | 2048 | Maximum points per chunk. Also controls the output segment size budget (`n × 512` points per output segment). Larger values produce fewer, bigger segments and reduce compaction frequency but increase per-segment memory and I/O cost. |

There are no builder methods for the L0/L1 count triggers or the source window size; they
are fixed at 4 and 8 respectively. The compaction interval is also fixed at 5 seconds in
the engine defaults.

---

## Interaction with retention

Compaction and retention are separate operations. The retention sweep
(`sweep_expired_persisted_segments`) deletes entire segments whose time range falls
entirely below the retention cutoff. When a segment partially overlaps the cutoff, the
compactor rewrites it (`stage_segment_rewrite_with_retention`) and drops points below
the cutoff during the merge, producing a smaller output segment at the same level rather
than advancing it upward.
