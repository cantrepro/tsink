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

Each `compact_once` call visits one level first and then the other, alternating the starting level
across calls. Only one level is compacted per call. This prevents a continuously eligible L0
backlog from starving L1.

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

### Bounded planning and source loading

Compaction does not build a complete directory inventory or load every segment body before
selecting a window. Each lane owns a persistent directory cursor, shared by cloned `Compactor`
handles, and retains at most eight candidate manifests. A pass stops at its configured directory
entry and manifest-inspection ceilings. On the next pass the cursor resumes from that point; after
reaching the end it wraps so new roots are eventually discovered. Candidate eviction and rejected
windows do not remove durable roots.

Only the selected window is fully loaded. Before that load, the planner checks the aggregate
manifest chunk and point counts, persisted file lengths, declared decoded chunk-payload lengths,
and a fixed `ChunkPoint` model against the pass limits. The decoded-payload inspection validates
chunk framing and checksums without decompressing payloads. If any item or byte ceiling would be
crossed, the pass yields as a no-op and publishes no output or replacement marker.

The default standalone limits are 1,024 directory entries/manifests/chunks and 256 MiB of modeled
source bytes per pass; the source window remains capped at eight. Storage instances pass their
effective `maintenance_max_items_per_pass` and `maintenance_max_bytes_per_pass` settings to both
lane compactors.

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

Before the first output-side filesystem mutation, compaction deterministically encodes the
selected data without publishing it and measures the complete operation peak. The model includes
every output file, simultaneous Preparing/Ready marker payloads, a destination-allocation-unit
allowance for every staged file and directory entry, missing owned ancestry, and one retirement
entry per source. One shared `Maintenance` reservation covers that complete peak. Competing
reservations participate in the same atomic admission decision, so an exact-boundary operation
succeeds and the same operation plus one concurrently reserved byte returns
`InsufficientCompactionHeadroom` without consuming a segment ID or creating a marker/output.
Physical-headroom rejection similarly returns `InsufficientDiskSpace` before publication.

After admission, replacement uses a crash-replayable marker protocol:

1. **Publish Preparing intent** — a JSON marker is written atomically to
   `.compaction-replacements/replace-<ts>-<nonce>.json` with the exact source and planned output
   roots. Recovery of this phase removes only those planned outputs and leaves every source visible.
2. **Write outputs** — every planned output segment is written to its new directory. A normal
   failure rolls all planned outputs back before removing the Preparing marker.
3. **Publish Ready intent** — the marker is atomically replaced with its Ready phase only after
   every planned output has been completely written and synchronized.
4. **Apply replacement** — each validated source is atomically moved to a marker-owned retirement
   path before recursive cleanup. The marker is removed after all sources have disappeared from the
   loader-visible namespace.

The next `compact_once` call (or startup) replays pending markers in sorted order. Preparing replay
rolls outputs back; Ready replay validates every complete output and finishes source retirement.
During a live aggregate operation, individual segment writes do not make nested per-output
reservations. Completion instead installs an exact full-tree reconciliation, preserving the
existing per-file/category accounting. The aggregate reservation is RAII-owned: ordinary error
releases it after rollback. If the exact scan fails, or if the operation unwinds, the guard leaves
no active reservation and conservatively accounts the admitted peak; restart recovery and a later
successful reconciliation remove that overcharge.

With a finite local-disk limit this is a hard logical envelope and the configured maintenance
reserve remains available to compaction. `ExpertUnlimited` deliberately removes the logical
local-disk ceiling, but the same aggregate preflight still checks actual filesystem availability
and any explicitly configured free-space headroom.

---

## Background thread

The compaction background thread runs continuously while the storage engine is open. It
sleeps for `compaction_interval` (default **5 seconds**) between passes and wakes
immediately when the flush pipeline signals that new segments have been written.

The thread runs both the numeric compactor and the blob compactor in sequence on each
wakeup. A mutex (`compaction_lock`) serialises the thread against manual compaction
calls and snapshot operations. Flush holds the same gate from before a segment root
becomes directory-visible through index verification, recovery-metadata persistence,
and the catalog visibility swap. A directory-scanning compactor therefore cannot
consume and retire a newly staged root before its flush transaction commits.

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
| `planning_directory_entries_inspected_total` | Directory entries inspected by bounded planning |
| `planning_manifests_inspected_total` | Segment manifests inspected by bounded planning |
| `planning_candidates_observed_total` | Candidate-cache entries observed across passes |
| `planning_source_bytes_total` | Modeled selected-source bytes admitted before full load |
| `planning_backlog_observed_total` | Passes that observed deferred planning/source work |
| `planning_budget_exhaustions_total` | Passes stopped by an item or byte ceiling |
| `duration_nanos_total` | Cumulative wall-clock time spent in compaction |

The ratio `output_points_total / source_points_total` indicates how much deduplication
or tombstone removal is occurring over time.

---

## Tuning

| `StorageBuilder` method | Default | Effect |
|---|---|---|
| `with_chunk_points(n)` | 2048 | Maximum points per chunk. Also controls the output segment size budget (`n × 512` points per output segment). Larger values produce fewer, bigger segments and reduce compaction frequency but increase per-segment memory and I/O cost. |
| `with_maintenance_max_items_per_pass(n)` | profile-specific; Embedded is 100,000 | Caps directory entries, manifest inspections, and source chunks considered by one pass. A finite value must cover the effective write-batch row limit so an admitted batch cannot be stranded. |
| `with_maintenance_max_bytes_per_pass(n)` | profile-specific; Embedded is 512 MiB | Caps modeled persisted input, declared decoded payloads, and fixed point storage selected by one pass. A finite value must be at least the accounted-memory limit because one sealed chunk may conservatively grow to that envelope. |

There are no builder methods for the L0/L1 count triggers or the source window size; they are fixed
at 4 and 8 respectively. The compaction interval is also fixed at 5 seconds in the engine
defaults.

These pass ceilings bound discovery and selected input. They do not yet charge compaction working
memory to `accounted_memory_bytes`. In particular, all selected chunks for one series may be
decompressed into per-chunk merge cursors at once, and output encoding can retain up to the
output-segment point budget (`chunk_points × 512`). The limits and hard format ceilings keep those
inputs finite, but a future accounting pass must reserve the output and codec scratch explicitly
before they can be described as part of the storage-memory budget.

---

## Interaction with retention

Compaction and retention are separate operations. The retention sweep
(`sweep_expired_persisted_segments`) deletes entire segments whose time range falls
entirely below the retention cutoff. When a segment partially overlaps the cutoff, the
compactor rewrites it (`stage_segment_rewrite_with_retention`) and drops points below
the cutoff during the merge, producing a smaller output segment at the same level rather
than advancing it upward.
