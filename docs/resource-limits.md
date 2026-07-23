# Resource limits and profiles

This document is the implementation inventory for Phase 2 of the execution charter. The design is
recorded in [ADR 0002](adr/0002-resource-profiles-and-budgets.md).

## Current status

tsink ships finite `Test`, `Embedded`, `Edge`, and `Server` profiles plus fully specified
`Custom(ResourceLimits)` profiles. `StorageBuilder::new()` selects `Embedded`; the server selects
`Server` unless `--resource-profile` or programmatic configuration chooses another base. Existing
applications that need the pre-profile unbounded storage/query defaults must select
`ResourceProfile::ExpertUnlimited` explicitly.

The initial constants are conservative and **provisional** until the final clean workload matrix in
[Resource-profile measurements](resource-profile-measurements.md) is complete. They are enforceable
limits, not capacity promises or measured maximum throughput claims:

| Profile | Accounted storage memory | Local disk | WAL | Cardinality | Shared queries | Writers |
|---|---:|---:|---:|---:|---:|---:|
| `Test` | 64 MiB | 256 MiB | 32 MiB | 10,000 | 2 | 2 |
| `Embedded` | 512 MiB | 16 GiB | 512 MiB | 1,000,000 | 8 | 4 |
| `Edge` | 256 MiB | 4 GiB | 256 MiB | 250,000 | 4 | 4 |
| `Server` | 2 GiB | 256 GiB | 8 GiB | 10,000,000 | 32 | 16 |

The complete values, including identity, batch, query-work, async queue, disk-reserve, cadence, and
maintenance-pass limits, are returned by `ResourceProfile::finite_limits()` and validated before
storage opens. Profile disk limits are dormant for in-memory storage; an explicit disk override
without a persistent read-write data path remains an error.

After build, the canonical inspection API is `Storage::effective_storage_limits()`. The same value
is included at `Storage::observability_snapshot().limits`, is available directly from
`AsyncStorage`, and is preserved by the UniFFI/Python binding. The built-in server publishes it as
`data.effectiveStorageLimits` from `GET /api/v1/status/tsdb`.

`Storage::resource_configuration_snapshot()` is the complete versioned inspection API. It contains
the selected profile name, resolved storage/query/async/maintenance limits, and stable low-level
override provenance. It is also embedded in `observability_snapshot().resource_configuration`,
available from `AsyncStorage`, represented as typed UniFFI/Python records, and published by the
server as `data.resourceConfiguration`.

```rust
let storage = tsink::StorageBuilder::new()
    .with_resource_profile(tsink::ResourceProfile::Embedded)
    .with_memory_limit(64 * 1024 * 1024)
    .with_cardinality_limit(100_000)
    .with_max_labels_per_series(32)
    .with_max_series_identity_bytes(16 * 1024)
    .with_series_creation_rate_limit(1_000, std::time::Duration::from_secs(60))
    .with_write_batch_limits(tsink::WriteBatchLimits {
        max_rows: Some(10_000),
        max_modeled_input_bytes: Some(8 * 1024 * 1024),
    })
    .build()?;

let limits = storage.effective_storage_limits();
assert_eq!(limits.accounted_memory_bytes, Some(64 * 1024 * 1024));
assert_eq!(limits.cardinality, Some(100_000));
assert_eq!(limits.max_labels_per_series, Some(32));
assert_eq!(limits.max_series_identity_bytes, Some(16 * 1024));
assert_eq!(limits.max_new_series_per_window, Some(1_000));
assert_eq!(limits.max_write_batch_rows, Some(10_000));
assert_eq!(limits.max_write_batch_input_bytes, Some(8 * 1024 * 1024));
assert_eq!(storage.observability_snapshot().limits, limits);
let resources = storage.resource_configuration_snapshot();
assert_eq!(resources.selected_profile, tsink::ResourceProfileName::Embedded);
# Ok::<(), tsink::TsinkError>(())
```

Low-level methods are sparse overrides and win regardless of call order. A later
`with_resource_profile(...)` changes only the base. Use
`clear_resource_limit_override(ResourceLimitOverride::...)` for one field group or
`clear_resource_limit_overrides()` to return to pure profile values.

For the built-in backend, `None` means no finite limit is enforced for that field. For a third-party
backend with `reported_by_backend == false`, optional values are unknown and must not be interpreted
as either finite or unlimited. Durations are reported in nanoseconds so sub-millisecond builder
values remain inspectable.

## Enforced storage-side controls

| Effective field | Builder control | Legacy default | Current enforcement scope |
|---|---|---|---|
| `accounted_memory_bytes` | `with_memory_limit(bytes)` | `None` | Modeled bytes for active and sealed chunks, the series registry, metadata caches, persisted indexes, full persisted mapping lengths, tombstones, foreground write preparation/WAL encoding, streamed startup WAL replay, and conservative pre-live registry/inventory/index hydration. Writes apply backpressure and eventually return `MemoryBudgetExceeded`. This is not a process-RSS cap. |
| `cardinality` | `with_cardinality_limit(series)` | `None` | Total registered metric-and-label identities. A write that must create too many series returns `CardinalityLimitExceeded`. |
| `max_labels_per_series` | `with_max_labels_per_series(labels)` | 128 | Maximum labels in every submitted series identity. The storage format has a separate hard maximum of 65,535. Violations are rejected as `InvalidLabel` before registry allocation. |
| `max_series_identity_bytes` | `with_max_series_identity_bytes(bytes)` | 64 KiB | Maximum cumulative UTF-8 bytes across the metric name and every label name and value. Violations are rejected as `InvalidLabel` before registry allocation. |
| `max_new_series_per_window` | `with_series_creation_rate_limit(series, window)` | `None` | New series allowed to publish in one fixed storage-clock window. Concurrent pending reservations count against the limit; failures release them and existing-series writes do not consume them. Rejection returns `CardinalityCreationRateExceeded`. |
| `new_series_window_nanos` | same as above | `None` | Effective creation-rate window in nanoseconds after the engine rounds a sub-precision duration up to one storage timestamp unit. |
| `max_write_batch_rows` | `with_write_batch_limits(...)` | `None` | Maximum rows in one top-level write. The check runs before tsink clones row identities or values and also bounds the indexed outcome allocation for `BestEffort`; row-wise execution cannot bypass it. Replay applies the same bound to one committed sample frame. |
| `max_write_batch_input_bytes` | `with_write_batch_limits(...)` | `None` | Maximum checked logical input model: `Row` and `Label` storage, metric/label UTF-8, bytes/string payloads, and native-histogram structures and vectors. `modeled_write_batch_input_bytes` exposes the exact calculation. Violations return `WriteBatchInputLimitExceeded` before a transient lease or clone. |
| `wal_bytes` | `with_wal_size_limit(bytes)` | `None` | Recognized WAL segment bytes when a local WAL is active. The definitive quota check is serialized with the WAL writer, so concurrent logical writes cannot collectively pass a stale check. This is a WAL sublimit, not a data-directory quota. |
| `wal_write_buffer_bytes` | `with_wal_buffer_size(bytes)` | 4 KiB when WAL is active | Inspected finite capacity of the WAL `BufWriter`. It is reported but deliberately not charged to `accounted_memory_bytes`. |
| `local_disk_bytes` | `with_local_disk_limit(bytes)` | `None` | Persistent core data-directory bytes admitted through the shared coordinator. Normal growth returns `DiskQuotaExceeded` at the boundary; reopening existing over-limit data remains possible. |
| `filesystem_free_headroom_bytes` | `with_filesystem_free_headroom(bytes)` | 0 for persistent storage | Physical free space that both normal and recovery work must leave available. A failed reservation returns `InsufficientDiskSpace`. |
| `maintenance_temp_reserve_bytes` | `with_maintenance_temp_reserve(bytes)` | 0 for persistent storage | Logical capacity withheld from normal growth but available to bounded maintenance output. Exhaustion returns `InsufficientCompactionHeadroom`. |
| `max_concurrent_writers` | `with_max_writers(n)` | cgroup-aware worker count | Synchronous engine writer permits. Zero passed to the builder selects the cgroup-aware default. |
| `write_timeout_nanos` | `with_write_timeout(duration)` | 30 seconds | Maximum wait for a writer permit and related lifecycle drains. |
| `max_background_threads` | fixed engine ownership | 0 in memory; up to 4 with persistent capabilities | Sum of the flush, compaction, persisted-refresh, and rollup thread slots that this built instance can own. |
| `max_flush_concurrency` | fixed serialized slot | 0 or 1 | One instance-owned flush worker for persistent lanes. |
| `max_compaction_concurrency` | fixed serialized slot | 0 or 1 | One compaction pass at a time across the instance. Numeric and blob compactor calls run serially within that pass. |
| `max_retention_tiering_concurrency` | fixed serialized slot | 0 or 1 | Retention/tiering is serialized through the persisted-refresh worker and compaction gate. |
| `max_remote_catalog_refresh_concurrency` | fixed serialized slot | 0 or 1 | Compute-only remote catalog refresh uses the same single persisted-refresh worker. |
| `max_remote_tier_fetch_concurrency` | `with_query_budget_limits(...)` | 0 without tiering; otherwise the query setting | Payload fetches are synchronous within a query, so a finite shared query concurrency is also the remote-fetch concurrency bound. `None` honestly means that tiered query/fetch concurrency remains unbounded. |
| `max_rollup_concurrency` | fixed serialized slot | 0 or 1 | One rollup pass at a time, additionally serialized with policy replacement. |
| `flush_interval_nanos` | fixed current runtime cadence | 250 ms | Periodic flush cadence; a coalescing work notification may wake it sooner. Healthy WAL-backed non-tiered passes defer a current head below half of its allocated point block; no-WAL/tiered durability and memory/WAL pressure override that fill threshold. |
| `compaction_interval_nanos` | fixed current runtime cadence | 5 s | Periodic compaction cadence; delete publication may wake it sooner. |
| `persisted_refresh_poll_interval_nanos` | remote refresh builder value plus fixed local poll | 250 ms locally | Local polling is 250 ms. Compute-only tiering uses the smaller of the configured remote refresh interval and 250 ms, with a 1 ms floor. Notifications may wake it sooner. |
| `rollup_interval_nanos` | fixed current runtime cadence | 5 s | Periodic rollup cadence; relevant writes and policy changes may wake it sooner. |
| `max_active_partition_heads_per_series` | `with_max_active_partition_heads_per_series(n)` | 8 | Maximum simultaneous time-partition heads for one series. Writes that would exceed it are rejected rather than opening unbounded late-write state. |
| `persistent` | `with_data_path(path)` and runtime mode | `false` | Reports whether the built backend owns local persistent lanes. It is descriptive, not a limit. |
| `wal_enabled` | `with_wal_enabled(bool)` plus persistence | `false` in memory | Reports whether a local WAL was actually opened. Enabling WAL without a persistent read-write data path does not fabricate one. |

Retention, future-skew, timestamp precision, and tier cutoffs are data-policy controls. They remain
visible through their existing configuration and observability APIs but are not resource budgets.

### Memory accounting boundary

`MemoryObservabilitySnapshot::accounted_bytes` is the modeled total admitted against
`accounted_memory_bytes`; `budgeted_bytes` remains as a compatibility alias. Component fields expose
the split. `estimated_accounted_bytes` is the portion modeled from owned allocations, while
`persisted_mmap_bytes` is the full virtual file-mapping extent charged to the same budget. Neither
is an RSS measurement.

Persisted mapping bytes are virtual file mapping lengths, not measured resident pages. Foreground
admission deliberately excludes the caller-owned row slice, then reserves a conservative envelope
for all coexisting tsink-owned identity/value clones, resolution and grouping collections,
`BestEffort` outcomes, encoded WAL payloads, and the brief overlap while modeled retained growth is
published. One top-level lease is shared across `BestEffort` rows and a disk-cleanup retry; sizing
from its immutable base prevents either path from double-reserving. Final-handle `Drop` releases the
lease on success, error, rollback, and unwind.

Startup WAL recovery reserves each bounded frame before allocating its payload, processes series
definitions one at a time, and enlarges that frame lease for only one decoded sample batch at a
time. The format also has a 256 MiB modeled decoded-batch safety ceiling, including constant-RLE
expansion. It never accumulates all committed definitions, frames, or decoded points in the
lifecycle replay path.

Persistent startup also owns a monotonic hydration-admission ledger before live
`ChunkStorage` accounting exists. The base registry checkpoint, legacy delta, and every canonical
incremental-registry file share that ledger; each compressed/stored input is inspected first, then
its physical bytes plus a conservative decoded/registry/postings expansion are admitted before the
body is read. Incremental paths are streamed and charged before retention. Segment discovery uses
one namespace counter and one memory ledger across numeric, blob, hot, warm, and cold roots. For
every valid segment it admits repeated inventory/path representations, the complete `chunks.bin`
mapping length, and a conservative expansion of decoded series, postings, chunk-index,
registry-rebuild, and eventual persisted-index state. The finite builder limit is also passed as an
additional per-file metadata decode ceiling, capped by the 256 MiB format ceiling.

These checks are read-only and finish before compaction-replacement recovery or corrupt-segment
quarantine can rename durable state. A rejection is `MemoryBudgetExceeded`; its `required` value is
the exact threshold for that admission step (the exact value admits the step, one byte less does
not; a retry can then expose a later, higher step). The reservation is intentionally conservative
and the bootstrap-only ledger is discarded after the admitted state is handed into construction,
so it is not a retained component in the post-build observability snapshot. Process leases exclude
cooperating writers between preflight and
materialization, but this is not descriptor-relative protection from an uncooperative process
replacing files after admission.

`write_transient_bytes` is included in both `accounted_bytes` and
`estimated_accounted_bytes`; `peak_write_transient_bytes`, admitted-reservation and budget-rejection
counters, and `write_transient_bytes_estimated = true` disclose the model. The retained WAL
series-definition cache is charged separately as `wal_series_definition_cache_bytes`; foreground
publication and one-frame-at-a-time startup replay transfer that modeled retained growth into the
shared budget before releasing the transient lease. Every built `ChunkStorage` eagerly initializes
the cache, so its metadata path does not trigger the standalone WAL helper's lazy rebuild. A
standalone `FramedWal` has no storage-memory budget of its own. The fixed WAL writer buffer,
caller-owned inputs, and collections returned by public WAL inspection helpers remain named
exclusions.

The storage-memory budget still does not account for all process memory. Query results, general
runtime decode/intermediate vectors outside the bounded startup hydration path, and caller-provided
aggregator internals are outside this budget,
although tsink-owned query allocations are separately admitted by the query budget described below.
Persisted metadata and chunk decode now have last-resort 256 MiB format ceilings (plus a 1 GiB
`chunks.bin`/full-load aggregate ceiling), exact-length streaming zstd validation, and structural
preflight before count-driven allocation. Those fixed corruption guards do not charge the live
storage-memory counter and therefore do not close this accounting exclusion.
Background-worker stacks, server request bodies, allocator/runtime overhead, and adapter state also
remain outside a complete process envelope. `excluded_categories` names these gaps.
`excluded_bytes_known` is `false`, so the compatibility `excluded_bytes` zero is not a measured
total and must not be interpreted as “nothing excluded.”

Startup local-disk reconciliation, exact owned-orphan planning, tombstone recovery/cleanup
preflight, post-flush replacement-marker recovery, and registry/inventory/index hydration are no
longer part of that unbounded gap.
They use conservative transient peak models and reject with `MemoryBudgetExceeded` before their
first durable mutation when the configured limit is too small. Because those allocations are
released before `StorageBuilder::build()` returns, they are not retained components of a later
`MemoryObservabilitySnapshot::accounted_bytes`; the startup error's `required` field is their
inspectable admission signal.

`memory.pressure` reports pressure on this modeled scope, with `Normal`, `ApproachingLimit`,
`Backpressured`, `Rejecting`, and `Degraded` states. The approaching threshold is inspectable and is
currently 9,000 basis points (90%) of a finite budget. Active-writer, event, and rejection counters
are memory-specific; older flush admission counters combine memory and WAL pressure. `Degraded`
means the storage instance is degraded and does not assert that memory caused it. Atomic byte
reservations prevent concurrent foreground writers and replay work from collectively passing a
stale budget check.

### Query-budget boundary

One storage instance owns a shared `QueryBudget`. Configure it independently of the retained
storage-memory budget:

```rust
use std::time::Duration;
use tsink::{QueryBudgetLimits, QueryWorkLimits};

let query_limits = QueryBudgetLimits {
    max_concurrent_queries: Some(8),
    max_shared_memory_bytes: Some(64 * 1024 * 1024),
    per_query: QueryWorkLimits {
        max_series_matched: Some(100_000),
        max_samples_scanned: Some(5_000_000),
        max_samples_returned: Some(1_000_000),
        max_returned_bytes: Some(64 * 1024 * 1024),
        max_pattern_expansion: Some(250_000),
        max_steps: Some(20_000),
        max_intermediate_vector_size: Some(100_000),
        max_memory_bytes: Some(32 * 1024 * 1024),
        max_wall_time: Some(Duration::from_secs(15)),
    },
};

let storage = tsink::StorageBuilder::new()
    .with_query_budget_limits(query_limits)
    .build()?;

assert_eq!(storage.query_budget_snapshot().limits, query_limits);
# Ok::<(), tsink::TsinkError>(())
```

Every field is optional. `None` means that the query-budget layer enforces no finite value for that
dimension; the all-`None` legacy default is therefore unbounded, not a named profile. Zero is not a
valid finite value, and a per-query memory limit cannot exceed the shared query-memory limit.
Request-specific `QueryWorkLimits` can only tighten instance limits. The admitted
`QueryExecution::limits()` value exposes the fieldwise effective result.

The work dimensions cover matched series, raw samples visited or decoded, returned samples and
modeled result bytes, regex/pattern candidate expansion, range and subquery steps, maximum
simultaneously materialized intermediate-vector length, per-query modeled memory, and wall time.
Concurrency and shared modeled query memory are instance-wide. Storage reads pre-admit predictable
work and checkpoint long loops; a limit failure is a structured `QueryBudgetError` and does not
silently truncate the result.

Legacy direct storage reads admit one execution internally. Nested engine callers can carry the
same execution through the `*_with_execution` methods so they do not acquire another concurrency
slot. PromQL instant and range requests likewise use one execution for selector planning and
prefetch, subqueries, all steps, binary operations, aggregations, and final result accounting.
`instant_query_with_control` and `range_query_with_control` accept request-specific limits and a
`QueryCancellationToken`; callers that already own an execution can use the corresponding
`*_with_execution` methods. Owned entrypoints release the query permit and modeled-memory
reservations through RAII on every exit path. A caller-supplied execution intentionally remains
admitted until its last clone and reservation are dropped, while temporary reservations created by
the failed operation are still released immediately.

The portable query-memory model charges tsink-owned collection capacities and value payloads plus a
named per-allocation allowance. It covers storage decode buffers, snapshots, candidate sets,
built-in aggregation working sets, and PromQL intermediates. It is not RSS, does not include private
global-allocator metadata, and cannot charge memory allocated internally by caller-provided
`Aggregator` or `CodecAggregator` implementations; their tsink-owned inputs and returned values are
still accounted.

`Storage::query_budget_snapshot()` and `Storage::observability_snapshot().query_budget` report the
configured limits, active and peak permits, active and peak shared memory, lifecycle counters,
fixed-reason rejection counters, cancellations, deadlines, and accounting-invariant violations.
The async facade exposes the same underlying snapshot and base enforcement, and UniFFI/Python
preserves both the builder limits and snapshot. Each queued async read now owns a cancellation
token. Dropping its awaiting future cancels that token; a running built-in selector or scan observes
it at the core query checkpoints and releases its query permit and reservations. A canceled command
still waiting in the channel is discarded when a reader receives it. Accepted writes remain
side-effecting and continue after their awaiting future is dropped.

The built-in server publishes this snapshot as `data.queryBudget` from
`GET /api/v1/status/tsdb` and as fixed-cardinality `tsink_query_budget_*` metrics. PromQL budget
errors use stable `X-Tsink-Read-Error-Code` values: invalid limits return 400, concurrency or shared
memory saturation returns 429 with `Retry-After: 1`, other finite-limit rejections return 413, and
observed cancellation or deadline expiry returns 503. These status and adapter surfaces do not
change an explicit `ExpertUnlimited` configuration. The embedded core and async builders otherwise
select finite `Embedded` query limits, while the server selects finite `Server` limits.

### Async facade boundary

`AsyncRuntimeOptions` independently bounds each command queue (1,024 entries by default), modeled
write payloads (64 MiB by default), modeled read payloads (16 MiB by default), and the number of
reader worker threads; writes use one serialized worker. Byte reservations use atomic compare-and-
exchange admission, include commands held by producers waiting for a full channel, and are released
when a worker receives the command. A send failure or a future dropped while its send is still
pending drops the same RAII reservation. Zero byte capacity permits only commands with no owned
input payload.

The byte model covers logical payload owned while queued: row/metric/label/value contents using the
same checked write-input model as foreground admission; metric, matcher, series, and rollup-policy
UTF-8 contents plus their vector element representations; and snapshot path encoded bytes. Fixed
command envelopes and reply handles are bounded by the command-count limit. Spare vector/string
capacity, allocator rounding, channel implementation overhead, and the pointee behind a caller-
provided custom aggregation `Arc` are not claimed as measured bytes. Query results and working
memory are separately governed by the core query budget.

`AsyncStorage::async_runtime_snapshot()` reports the configured command and byte caps, channel
depths, current and peak reserved bytes, byte-admission rejections, and worker counts. These are
facade settings, not storage-engine fields, so they remain outside the compatibility
`EffectiveStorageLimits` projection. `AsyncStorage::resource_configuration_snapshot()` resolves
them alongside the underlying storage/query limits and records async-specific overrides.

### Background-work boundary

`StorageObservabilitySnapshot::background` reports each of the four fixed worker slots, including
whether a join handle is installed, whether the thread is alive, its effective cadence, coalesced
notifications, idle parks, passes, exits, and shutdown joins. Aggregate close fields report
attempts, success/error outcomes, coordination wait/timeouts, compaction passes against the fixed
128-pass limit, whole-call duration, and worker-join wait. The built-in server exposes the same
snapshot from `GET /api/v1/status/tsdb` and fixed-cardinality labels from `/metrics`.
`close_attempts_total > close_success_total + close_errors_total` identifies a close that is
currently inside coordination or durability work, including a filesystem call whose duration
cannot yet be finalized.
Retention/tiering and remote catalog refresh share the one persisted-refresh worker rather than
creating hidden threads.

`with_maintenance_max_items_per_pass` and `with_maintenance_max_bytes_per_pass` resolve from every
finite profile and are included in `ResourceConfigurationSnapshot`. The compactor consumes both:
directory/manifest inspection, candidate selection, and modeled source bytes stop at the pass
boundary and continue from a persistent cursor on a later pass. Sealed-chunk persistence also stops
at the item or modeled-byte boundary and resumes from the global sealed-chunk sequence prefix. With
a WAL, a partial prefix is publishable only when its maximum WAL frame is strictly before every
deferred sealed chunk and active-head floor. An older active head causes a safe no-op until bounded
active finalization moves it into the prefix; a dependency group that cannot fit the configured
pass returns structured `MaintenanceDependencyWindowExceeded` instead of publishing a checkpoint
that could duplicate or skip data after restart. Because repeated accepted writes can
conservatively grow one chunk to the full accounted-memory envelope, build validation requires
every finite maintenance byte cap to be at least the effective finite memory limit. A finite cap
paired with unbounded memory is rejected rather than risking maintenance livelock. Likewise, every
finite maintenance item cap must be at least the effective finite write-batch row limit so all
chunks produced by one accepted batch can fit the pass envelope.
`ExpertUnlimited` maps both controls to internal unlimited sentinels for compatibility.

Background retention/tiering uses the same pair for one deterministic persisted-root page per wake.
Every visited root charges one descriptor item and its retained descriptor/path bytes. Every selected
rewrite, move, or expiry additionally charges the logical lengths of all canonical source files
declared by its verified manifest because replacement preparation validates the complete source.
The replacement marker's 16,384-record recovery ceiling conservatively tightens a page to 8,192
source roots because a move can add one output record per source. A candidate larger than the complete byte envelope returns
`MaintenanceDependencyWindowExceeded`. The cursor commits only after staging, `Prepared`/`Committing`
publication, catalog visibility, and source retirement succeed. Failure keeps the cursor for durable
recovery; reaching an item boundary conservatively schedules another page, so an exact multiple needs
one empty terminal page before the worker may claim a clean full cycle. Startup, close, and
capacity-reclamation retention paths remain complete lifecycle operations rather than background
pages.

Incremental series-registry publications also stay within the startup recovery namespace bound.
Small publications are merged into one active `RJNL` generation with at most 1,024 series and
4 MiB of stored and decoded registry payload. Crossing either threshold first seals the active
generation with an atomic rename, then publishes a new active generation; an individually larger
bounded batch is retained as one dedicated generation and is sealed before the following merge.
Thus repeated one-series flushes create roughly one file per 1,024 series rather than one file per
flush, while each background merge remains bounded. Before every atomic publication the writer
counts every existing entry in the owned delta directory, including unknown files and the
crash-visible replacement temporary, and returns structured
`MaintenanceNamespaceLimitExceeded` if the operation could create a namespace that a later bounded
startup cannot enumerate. Startup accepts legacy checkpoint/delta files and both sealed and active
journal generations; an incomplete managed journal is corruption, while exact owned atomic-write
temporaries are removed and unknown entries are preserved. Full-checkpoint cleanup likewise removes
only recognized regular registry generations and leaves host-owned entries untouched.

Rollup checkpoint and pending-materialization updates use checksummed replacement records instead
of rewriting the complete `.rollups/state.json` map per source. One active generation contains at
most 1,024 latest-per-source records and 4 MiB of JSON payload. Rollover seals it before publishing
the next active file; one bounded adjacent-generation compaction may replace two sealed inputs with
their latest source states, and duplicate replay after interrupted cleanup is idempotent. At most
1,024 recognized journal generations are allowed (at most 1,048,576 distinct records before
supersession/compaction); the next non-compacting rollover returns
`MaintenanceNamespaceLimitExceeded`. Startup enumerates at most the fixed recovery namespace and
reads each generation within its own 4 MiB bound.

Policy replacement and delete invalidation still require a complete state-first snapshot. Before
cloning the live maps or cloning/encoding policies, tsink admits policies, labels, checkpoints,
pending records, generations, and delete-marker members against one 65,536-item and 64 MiB
conservative modeled-JSON envelope. `MaintenanceDependencyWindowExceeded` rejects a larger admin
mutation before publication. A committed full snapshot advances a journal epoch, making every old
source record ineligible for replay, then removes recognized obsolete generations; unknown files
remain untouched. These explicit ceilings prevent hidden unbounded maintenance but currently mean
a Server deployment with more live rollup state than the full-snapshot envelope must partition or
reduce that state before policy replacement or source deletion can publish.

A retention/tiering wake whose persisted catalog is already clean now evaluates only its bounded
root page. A clean page advances the cursor without catalog publication; only the empty terminal page
claims a clean full cycle. Unknown dirty state is left pending for the catalog-refresh phase instead
of being mistaken for a no-op, and completed replacement recovery remains authoritative before page
selection.

Ordinary persisted-index add/remove accounting measures only the roots, per-series reference
vectors, and posting keys named by the transition; it no longer walks every live segment before
and after a bounded flush. A non-tiered transition likewise updates visible-segment counters from
that exact root delta without constructing a complete inventory. The ordinary registry-catalog
sidecar is also incremental: `series_index.catalog.d/` has one entry per live segment and one
manifest containing at most one pending page intent. An intent is capped at 16 MiB, the directory
at 16,382 live entries plus its manifest and atomic-write allowance, and retry replays the same
remove/upsert set before publishing its exact final count. Native manifest and entry reads reject
more than 17 MiB and 16 KiB before allocation; legacy JSON rejects more than 64 MiB. The aggregate
series fingerprint is invalidated before entry mutation, so a torn page cannot claim the registry
fast path. A complete checkpoint rebuilds that fingerprint, prunes recognized stale entries, and
writes the legacy v2 JSON compatibility snapshot; startup migrates a legacy-only catalog through
that complete path.

For finite non-tiered runtime refresh, unknown-dirty reconciliation now has its own process-local
continuation. Fixed lane/level traversal charges directory opens and every raw directory entry,
including ignored names; candidate manifest reads are charged separately. The deduplicated scan
snapshot is keyed in stable lane/level/segment order, its retained path/bookkeeping model cannot
exceed the configured maintenance byte ceiling, and the complete scan namespace cannot exceed
16,384 raw entries. The scan publishes nothing. After its terminal probe, stable add-key and
persisted-root cursors apply exact registry/visibility deltas, with manifest-declared source bytes
and conservative removal work charged to the same pass. The first delta refreshes authoritative
tombstones; dirty state clears only after the terminal prune probe. A publication failure retains
the exact root intent, a visibility-generation change or known root delta discards the stale scan,
and process restart discards the cursor because startup strict hydration is authoritative.
`MaintenanceWorkItemTooLarge`, `MaintenanceDependencyWindowExceeded`, and
`MaintenanceNamespaceLimitExceeded` are explicit rather than truncating a catalog.

Tiered local/shared segment catalogs remain monolithic snapshot files: a tiered publication must
still materialize and rewrite the complete final inventory to preserve their current crash-safe
replacement contract. Lifecycle startup and close also intentionally use complete strict
reconciliation, and `ExpertUnlimited` explicitly keeps the complete runtime path. The scoped
accounting path can still traverse a touched Roaring posting bitmap and an invalidated missing-label
cache; removing those key-local cardinality costs requires incremental bitmap/cache byte counters.

When both numeric and blob lanes are persistent, the background compaction worker alternates one
lane per wake. Each compactor therefore consumes the configured pass limits at most once per
worker pass instead of independently doubling the shared item and byte envelope. Explicit close
drain retains its multi-lane settling loop because it is a finite lifecycle operation rather than
an idle background wake.

For a governed compaction, output encoding first measures the complete staged disk peak without
publishing: all output-file bytes, simultaneous Preparing/Ready replacement-marker payloads,
destination-allocation-unit allowances for staged entries and missing ancestry, and one new
retirement entry per source. The shared coordinator admits that total as one `Maintenance`
reservation before allocating output IDs or mutating the filesystem. Concurrent reservations are
included atomically. A logical N+1 failure returns `InsufficientCompactionHeadroom`; physical
free-space/headroom failure returns `InsufficientDiskSpace`; neither creates a marker, output, or
source-retirement side effect. Preparing recovery rolls planned outputs back, Ready recovery
finishes validated source retirement, and successful/error completion reconciles the exact
per-file/category totals when the scan succeeds. A failed scan or unwind leaves no live reservation
and conservatively charges the admitted peak until restart recovery and reconciliation.
`ExpertUnlimited` removes the logical disk ceiling but not the aggregate
actual-free-space/headroom check.

Core close keeps the exclusive data-path process lease through background-worker join. The lease
is released only after every owned worker has stopped, so a worker finishing filesystem I/O cannot
overlap a second process opening the same directory. Before durability work starts, close waits for
the background-maintenance gate, every writer permit, and a compaction-drain preflight. Each wait
uses `write_timeout`; contention returns a structured `LifecycleTimeout` (writer drain retains
`WriteTimeout`), restores the open lifecycle, and leaves accepted state available for retry. These
are per-acquisition bounds, not one cumulative close deadline.

The timeout applies to those three outer coordination classes, not every inner publication
`Mutex`/`RwLock`. Draining writers and maintenance removes engine-owned mutator contention before
the durability pipeline starts. A concurrent query may still briefly retain the visibility read
fence while it snapshots one source; finite profiles give that query cooperative work/deadline
limits, while `ExpertUnlimited` deliberately does not.

After quiescence, close deliberately performs complete work: every accepted active head and pending
sealed chunk is persisted, retention observes the complete admitted inventory, compaction runs at
most 128 settling passes, dirty persisted state is refreshed, and tombstone/registry recovery
indexes are checkpointed. The active and sealed loops are finite because writer permits remain
drained. Standard finite profiles also bound admitted series, WAL, and local-disk state; explicit
`ExpertUnlimited` does not. Close intentionally completes any unknown-dirty scan in its strict
lifecycle path, while tiered segment-catalog formats still require proportional complete-catalog
publication.

Blocking filesystem calls are the portability boundary. A call already executing file write/sync,
directory sync, atomic rename, or removal cannot be safely cancelled by portable Rust APIs without
making the durability result unknowable. Close therefore has no whole-call wall-clock deadline
after durability I/O begins. Worker joins occur only after maintenance/compaction gates have
drained and the lifecycle is closed, so no new engine-owned pass can start; scheduling delay or a
kernel-blocked call is nevertheless not preemptible. Whole-close and join durations make that
boundary measurable.

These fields are work/concurrency/cadence bounds, not a CPU quota. Active flush and rollup postings
traversal consume the shared item/byte controls, and each rollup source read is additionally bounded
by an internal `QueryExecution` whose modeled memory and work can only be tightened by the finite
maintenance byte ceiling. That same source-scoped execution remains live through raw and
existing-materialized reads, downsampling, duplicate filtering, and output-row construction. It
preflights retained point vectors, downsample output/value payload, numeric scratch, and cloned row
identities against the per-query and shared query-memory ceilings; failure precedes pending state,
writes, and checkpoint progress, and every reservation is released on success or error.
Finite explicit/manual rollup calls now advance the shared source cursor by one item/byte-bounded
page and return continuation state; status uses the accumulated traversal counters rather than a
fresh complete enumeration. Tiered segment-catalog publication and `ExpertUnlimited` manual rollup
drains retain complete work, so the snapshot must not be interpreted as a universal per-worker
CPU/work guarantee. `close()` unparks and joins all owned workers—even if an earlier join reports a
panic—but cannot portably interrupt a filesystem operation that has already entered the kernel.
The remaining pass-budget integrations are residual Phase-2 work.

### Local-disk accounting boundary

Persistent storage always reconciles one core data-directory coordinator. Callers opt into a
finite logical and physical envelope with the builder controls:

```rust
let storage = tsink::StorageBuilder::new()
    .with_data_path("./tsink-data")
    .with_local_disk_limit(1024 * 1024 * 1024)
    .with_filesystem_free_headroom(128 * 1024 * 1024)
    .with_maintenance_temp_reserve(64 * 1024 * 1024)
    .build()?;

let limits = storage.effective_storage_limits();
assert_eq!(limits.local_disk_bytes, Some(1024 * 1024 * 1024));
let disk = storage.observability_snapshot().local_disk.unwrap();
assert_eq!(disk.limits.max_bytes, limits.local_disk_bytes);
# Ok::<(), tsink::TsinkError>(())
```

The coordinator scans the root without following symlinks, includes unknown entries in the total,
and exposes WAL, segments, registry/catalog, tombstone, rollup, metadata, exemplar, server-state,
temporary, and unknown categories. It uses atomic peak-byte reservations so concurrent managed
writers cannot collectively admit the same remaining capacity. Compaction and retention reserve
their complete governed output group and missing publication parents before the first promotion.
Post-flush replacement markers separately reserve their bounded payload, marker entry, and missing
marker-directory entries. Recovery source/output renames use one aggregate Recovery reservation:
they may bypass the logical cap but still honor physical free-space headroom.
Required startup, cleanup, and shutdown recovery work may
proceed while the logical cap is already exceeded, but still honors the physical free-space floor.
Cleanup that removes an entry is followed by an exclusive scan so category credits cannot
undercount surviving files; a no-op orphan pass does not rescan the tree.

Persistent read-write startup derives `accounted_memory_bytes` before opening the disk coordinator.
Initial reconciliation is a no-follow, depth-first streaming walk with one 16,384-entry global work
cap and depth 128; it retains only the active path stack and a fixed twelve-category accumulator,
not a directory-wide `Vec<DirEntry>` or pending sibling stack. Path/component and traversal-frame
capacity is admitted against the configured modeled-memory limit. A startup that cannot admit this
work returns `MemoryBudgetExceeded` before recovery cleanup can remove or rename durable entries.
Consequently, a memory limit that is valid for steady-state data can still be too small to open an
existing persistent namespace; the error's `required` value reports the next modeled startup
threshold.
Managed file writers validate the final directory entry without following it and reject symlinks or
other non-regular files, including dangling symlinks. This is static path validation: descriptor-
relative traversal that remains safe across a hostile concurrent namespace swap is not yet
implemented. Writers also synchronize every newly created nested-directory entry.
Budget-integrated atomic server stores clean up only the generated
`.<target>.tmp-<pid>-<nonce>` shape, where `pid` is a canonical decimal `u32` and `nonce` is exactly
16 lowercase hexadecimal digits. Matching regular-file or symlink entries can be removed;
ambiguous matching directories fail startup instead. Core startup additionally rejects symlinked
owned directory namespaces and removes only exact current-format atomic-write temporaries for
registry, rollup, tombstone, and compaction-replacement-marker targets, plus
`.tmp-seg-<16-lowercase-hex>` staging directories. Variable target names are also strict: legacy
registry deltas use `delta-<16-lowercase-hex>.bin`, registry journals use the fixed
`journal-active.bin` target and sealed `journal-<16-lowercase-hex>.bin` generations, tombstone shards use
`shard-<three-decimal-digits-000-through-255>-<16-lowercase-hex>.bin`, and compaction replacement
markers use `replace-<16-lowercase-hex>-<16-lowercase-hex>.json`. Pending final compaction markers,
unknown files, and lookalikes are preserved. Tombstone shard cleanup first validates the complete
current or legacy manifest and fails closed on unrecognized bytes; it removes only exact canonical
shard names that the valid manifest does not reference. Filesystem-backed tier tombstone
directories receive the same durable ancestry linking even though they are intentionally outside
the local quota.

Post-flush startup cleanup, while holding the read-write data-path lease, removes exact atomic
temporaries for `transaction-<16hex>-<16hex>.json` and exact current-format rewrite/copy staging
directories before replacement recovery. These paths are not referenced by the durable marker and
can otherwise consume physical headroom needed for a recovery rename. Compute-only startup never
runs this mutation against a configured writer or object-store path. Unknown entries and lookalikes
remain untouched. The cleanup first preflights every candidate tree across all configured lane/tier
roots with one namespace cap and a modeled-memory admission; no blocker is deleted if any later
candidate fails validation or admission. Generic registry, rollup, tombstone, compaction-marker,
and segment-staging cleanup likewise preflights every category and every tombstone lane before its
first deletion.

Replacement recovery enumerates all marker-directory entries before mutation, counts unknown and
non-UTF-8 names toward the same finite namespace work envelope, and admits the retained marker-path
set. It then admits the complete peak for the bounded 4 MiB framed read, serde-owned marker state,
at most 16,384 source/output records, validated paths, and collected rollback/commit states. Every
marker and segment state is validated before the first recovery rename or deletion. Memory or
validation rejection preserves all marker files, published source/output names, atomic
temporaries, and staging directories for a fully-admitted retry.

Opening an existing disk-over-limit directory is supported once the separately configured startup
memory work is admitted: existing reads remain available,
`over_limit` is reported, and new normal growth is rejected. Unknown or host-created entries are
counted and retained. A snapshot-export destination resolving inside the live managed tree is
rejected; an external export destination is outside that quota and reports its own I/O failure.
Object-store roots are also outside the live quota, and any configuration that overlaps one with
the managed root is rejected.

Offline restore has a separate, explicit core envelope.
`StorageBuilder::restore_from_snapshot_with_disk_budget` accepts a caller-owned
`LocalDiskBudget` rooted above the target; the target must be a strict descendant and the snapshot
must not overlap that root. The restore tree is measured before destination mutation, with the root
counting toward a 100,000-entry limit and descendant-directory depth capped at 128. Static
symlinks, Windows reparse points, special entries, and resolved source/target overlap in either
direction are rejected. The source must nevertheless be trusted and immutable for the call:
portable path-based traversal cannot close a concurrent namespace-swap race between validation and
open.

The budgeted restore reserves
`logical_file_bytes + snapshot_entries * entry_allowance + missing_target_ancestors * entry_allowance`,
where `entry_allowance` is the greater of the 4 KiB policy floor and the destination filesystem's
reported allocation unit. The entry allowance is a conservative admission policy for entry
metadata and minimum allocation, not a claim that every filesystem's physical footprint is exactly
that value. Target activation is serialized with cooperating managed mutations. An exclusive scan
replaces the reservation with exact logical accounting before admission resumes; if reconciliation
fails, the error is explicit and the full reservation remains charged conservatively. In
particular, a scan failure after activation is reported explicitly as a committed restore whose
accounting reconciliation failed, not as a clean rollback.

The compatibility `StorageBuilder::restore_from_snapshot` path remains caller-unbudgeted. It now
uses the same finite entry/depth measurement, static link-like and special-entry rejection,
resolved overlap check, bounded copy, durable missing-ancestor creation, and rollback-aware staged
activation. Callers that need a finite restore boundary must use the budgeted API and keep its
offline coordinator alive for the complete operation; the restored storage subsequently opens its
normal live coordinator at the target.

When a typed disk-capacity rejection occurs during rollback-safe segment-flush staging or foreground
WAL Growth admission, the engine makes one cleanup attempt that can retire only fully expired owned
segment roots and then retries the rejected operation once. Rewrite and tier-move actions are
disabled for this path, so mixed-age segments are not rewritten merely to make room. Unknown and
host-created files are counted but never selected for reclamation. If cleanup cannot reclaim
capacity, or cleanup itself receives a capacity rejection, the original typed rejection is
preserved. That cleanup still needs enough maintenance headroom to publish its durable replacement
marker before it can retire a segment.

The built-in server opens this coordinator before constructing persistent stores and shares it with
the core, metric metadata, exemplars, rules, the usage ledger, managed control-plane state, the
experimental hinted-handoff outbox, paired cluster control state and consensus log, cluster audit
log, cluster dedupe markers, edge source queue, and standalone edge-accept dedupe markers.
Cluster mode requires an explicit data path, so these cluster writers cannot fall back to an
unleased, unbudgeted temporary root. Omitting `--data-path` is supported only for non-cluster
in-memory operation.

Those writers reserve exact growth and synchronize successful publication. Most publish in-memory
state only after persistence; dedupe is the explicit exception, retaining an exact completed result
after a marker failure so same-process retries remain idempotent while new keys are fenced.
Read-write storage holds the core data-path lease; server modes that
do not open a read-write core hold the same canonical process lease themselves. A final startup
reconciliation waits for outstanding reservations and makes status and metrics reflect every file
created during bootstrap. On shutdown, HTTP and Graphite request tasks remain owned and are drained
before storage closes or the process lease is released; the ten-second grace period is a warning
threshold, not permission to detach a still-running write.

The server flags are `--local-disk-limit`, `--filesystem-free-headroom`, and
`--maintenance-temp-reserve`; any of them requires `--data-path`, as does `--cluster-enabled` even
when those limits are left at their defaults. The shared snapshot is reported even for compute-only
server storage. Direct and internal metadata/exemplar quota failures retain their structured
resource category and map to HTTP 413, disclosing partial row progress when rows
already committed. A hinted-handoff Put that cannot reserve shared local-disk growth likewise
returns a structured disk resource failure; the cluster write surface maps it to HTTP 413. A
dedupe marker quota failure after the primary write committed returns the stable
`write_disk_quota_exceeded` error code in a partial HTTP 413 and retains the exact completion in
memory for same-process replay. Edge source queue growth rejection uses that same partial HTTP 413
after the local rows commit. Audit quota/headroom errors remain typed internally and are logged;
because auditing occurs after a control-plane response is determined, they do not roll back or
reclassify the completed mutation. Before consensus requires a control candidate, resource
rejection ahead of authoritative log publication maps to HTTP 413 `write_disk_quota_exceeded`
without publishing it. Once quorum or a leader commit establishes the candidate, a pre-log
persistence failure becomes fenced HTTP 503 `control_persistence_indeterminate` instead of a false
definitive 413. Both stable error codes are exposed in `X-Tsink-Write-Error-Code` and are
non-retryable on internal control responses. A mirror failure after the log replacement and parent-
directory sync is reported as a committed checkpoint pending.

The coupled experimental cluster control-state and consensus-log files now share one grouped
`Cluster` reservation. Both complete replacement files are staged before publication; the schema-v2
log, including its authoritative `checkpointState` and restart-durable `steppedDownTerm`, is
synchronized and published before the control-state mirror. Before consensus requires a candidate,
a pre-log failure releases the grouped reservation and leaves live state unchanged. A required
candidate that cannot yet publish the log is retained in memory, fenced, and retried as pending
durability. Once the log and parent sync are durable, a mirror failure installs the committed
candidate and fences later control mutations until authoritative Recovery repair completes. Startup
uses a valid v2 log to attempt repair of a stale, missing, or invalid mirror and opens fenced with
the checkpoint pending if that repair fails; legacy v1 migration still needs a valid mirror.
Authoritative Recovery may recreate a missing mirror or grow a stale mirror while reconciled usage
is at the logical quota because the durable log already represents that logical state. It does not
waive the physical-space or filesystem-headroom reservation for the complete temporary peak.
Failure after both pair members are durable is cleanup debt for finalization, owned-temp cleanup,
or exact reconciliation, not a fence; that cleanup runs before any separate authority repair.
If a quorum-committed command later observes a higher commit-notice term that cannot yet be
persisted, it returns degraded `committed_persistence_pending`, adopts the term in memory, and
fences leadership while retrying the required log-only publication. Because the command is already
committed, this is not reclassified as a definitive quota rejection.

This is not yet a complete **server data-directory** contract. The hinted-handoff outbox reserves
Put growth in the `Cluster` category, reconciles exact bytes on
restart, and uses Recovery admission for the Ack append plus an immediate cleanup attempt when the
logical quota is exhausted. Cluster dedupe marker growth uses the same `Cluster` category;
standalone edge-accept markers use `EdgeSync`. Their non-growing compactions use Recovery admission
and bounded atomic replacement, while a growing legacy normalization still requires Growth
admission. Current-format outbox logs likewise shrink through Recovery compaction; a legacy record
whose explicit defaults make the replacement grow requires normal Growth admission. A failed
post-record outbox compaction is recorded as cleanup debt and retried without changing the durable
Ack or reschedule outcome.
Cluster audit appends reserve `Cluster` growth and publish memory only after the record is
synchronized. Edge source Put records do the same in `EdgeSync`; Ack and batched expiry records can
use Recovery admission at the growth limit before an exact shrinking compaction. Post-record audit
or edge compaction failure is cleanup debt and does not reverse the already-durable logical result.
Clustered metadata/exemplar routing preserves a local or peer's structured disk-quota failure as
HTTP 413 when the requested acknowledgement count is not met. Online restore targets that overlap
the live root are rejected. The server now requires a second, finite, cross-process-leased offline
coordinator for every restore. Standalone restore and both internal routes use the budgeted core
API; cluster coordination requires `budgeted_restore_v1`, uses that same envelope for every local
target and the post-restore report, and has no unbudgeted peer fallback. Report/source/target
overlaps are rejected before the first restore mutation. External snapshot-export destinations
remain caller-governed and are rejected beneath the offline root. Concurrent arbitrary host
mutations cannot be prevented: either coordinator tolerates and counts them when reconciled but is
not a quota on other processes.

## Remaining hard-budget work

### Server-wide local disk

The core now coordinates WAL, segments and indexes, compaction and retention staging,
registry/catalog files, tombstones, and rollup state. The server additionally coordinates metadata,
exemplars, rules, usage, managed state, the hinted-handoff outbox, cluster audit log, cluster dedupe
markers, the paired cluster control state and consensus log, edge source queue, and standalone
edge-accept dedupe markers through the same live root. A distinct finite offline root now covers
server restore staging/targets and cluster reports without weakening tenant or live-root isolation.

Rollup policy/state changes reserve the complete two-file staging peak, one allocation-unit
allowance per temporary entry, and missing parent entries before mutation. Both files are staged and
synchronized before state-first publication. Partial or ambiguous publication fences later rollup
mutation until reopen, while a proven complete pair can report cleanup debt without becoming a
false rejection.

A complete server-wide model still needs:

- a stable, leased crash-recovery coordinator for cross-lane and cross-filesystem tombstone
  manifest publication;
- a measured policy for batching exclusive cleanup reconciliation, which currently favors exactness
  over cleanup throughput.

Core tests now cover over-limit recovery, restart reconciliation, unknown files, concurrent
reservations, whole-operation multi-output compaction preflight at exact N/N+1, competing
reservation rejection before mutation, Preparing/Ready restart replay, unwind release, later-output
rollback, WAL reset, catalog repair, symlink-safe entry replacement, publication rollback after
directory-sync failure, and injected partial filesystem-full writes.
Offline-restore tests cover resolved overlap and static-link rejection, finite depth and bounded-copy
enforcement, allocation-aware tiny-entry admission, missing-ancestor accounting, successful exact
reconciliation, and explicit committed reconciliation-failure classification. Retention tests cover
a single fully-expired-only reclaim-and-retry before both flush staging and foreground WAL rejection
while preserving mixed-age segments and external files.
Server tests cover tiny-limit no-publication behavior, exact category accounting, concurrent usage
append ordering, torn-tail rejection, restart reconciliation across sidecars, compute-only process
leasing and observability, structured 413 responses with partial progress, and rejection of online
restore targets that overlap the live root. Hinted-handoff tests additionally cover tiny-quota
enqueue rejection without queue publication, exact restart reconciliation, competing outboxes for
the final bytes, and Ack recovery that compacts a quota-full log to an empty restart state.
Dedupe tests cover typed quota failures and physical-headroom error classification, exact replay and
new-key fencing after a failed marker append, exact `Cluster` and `EdgeSync` restart categories,
competing marker stores for the
final bytes, quota-full Recovery compaction, continued append after nonempty atomic replacement,
and exact owned-temporary cleanup with lookalike preservation.
Audit tests cover typed quota/headroom rejection without publication, exact restart accounting,
concurrent final-byte admission, quota-full Recovery compaction, durable cleanup debt, monotonic IDs
after expired-log cleanup, torn-tail rejection, and exact owned-temp cleanup. Edge tests cover typed
quota rejection without publication, exact restart accounting, Recovery Ack at the growth limit,
all-or-nothing expiry append failure, durable post-Put cleanup debt, invalid/torn-log rejection, and
owned-temp cleanup.
Control consensus tests cover grouped quota admission and fenced retention of a consensus-required
candidate, log-first checkpoint failure and restart repair, authoritative stale/missing/invalid
mirror repair, pre- versus post-quorum quota classification, corrupt-log fail-closed validation,
higher-term durability fencing across restart/restore, and legacy schema-v1 migration to the
required schema-v2 checkpoint.
The internal exact-reconciliation helper assumes its caller does not hold a second reservation on
the same coordinator; batching or a deferred-reconciliation protocol should replace that
correctness-first constraint before advertising high cleanup throughput.

### Usage accounting

Ordinary durable usage appends run through one bounded blocking lane. Storage reconciliation scans
on a separate one-permit lane, so a long scan does not occupy the ordinary append lane; its final
multi-tenant result is appended as one physical batch frame. Handlers await metering, so ledger I/O
can add response latency, but a ledger failure does not reclassify already-completed primary work as
a retryable data failure.

Usage state is finite and inspectable. The ledger stores exact all-time record/status counters and
per-tenant category summaries, with tenant cardinality rejected at a configured maximum. Only the
newest configured record window remains in memory. Reopen streams a fixed-length file snapshot;
line growth is checked before extending the line buffer, and record, frame, line, batch, startup
scratch, and disjoint legacy sequence-range limits are validated. Legacy single-record lines,
sequence gaps, and bounded out-of-order ranges remain accepted; zero/duplicate sequences and torn
tails remain fail-closed.

Time-filtered and bucketed reports aggregate at most a configured number of recent records and have
an encoded response-byte ceiling. Raw export uses the same bounded recent window with independent
record and NDJSON byte ceilings. Both publish a snapshot sequence, earliest retained sequence,
exclusive continuation cursor, and `hasMore`; continuations pin the initial snapshot so concurrent
appends create neither duplicates nor moving-page ambiguity. A cursor lost to retention returns a
structured 410, and an individual record or aggregate response that cannot fit its byte envelope
returns a structured 413. Unfiltered `bucket=none` reports and the support bundle use the exact
all-time aggregate without a history scan. Effective limits, retained count, and earliest sequence
are exposed in status; retained and configured record/tenant bounds are exported as metrics.
These server-side usage-state and read limits remain finite even when the core storage profile is
`ExpertUnlimited`; there is no unlimited usage-ledger CLI sentinel. That explicit profile can
remove the separate local-disk envelope, so durable replay can again become proportional to an
unbounded ledger file. Complete storage reconciliation is concurrency-isolated but still performs a
full database scan and remains residual work for a cursor-bounded reconciliation design.

### Residual query-envelope work

`QueryOptions::limit` and `offset`, `QueryRowsScanOptions::max_rows`, and
`ShardWindowScanOptions::{max_series,max_rows}` remain caller-requested pagination controls rather
than resource limits. The shared `QueryExecution` separately enforces the profile's concurrency,
work, deadline, and modeled-memory limits across direct, async, PromQL, and HTTP entry points.
The built-in `list_metrics` entry point now admits that execution directly; async metadata reads
forward the already-admitted execution instead of acquiring a second slot. Registry IDs are read
in fixed 4,096-entry pages, and the page scratch, growing returned identities, and retained
dead-series pruning IDs are reserved before allocation. A cold visibility-summary repair reserves
its update, normalized-range, and cache-publication staging, checkpoints source traversal, and
re-admits the actual active/sealed range count under the same read guards used for rebuilding; a
post-estimate concurrent write therefore cannot grow the vector past its admitted envelope. Stable
dead-series pruning observes the dead-ID length and reserves the one simultaneously live companion
vector reused by removal, delta reconciliation, and shard unpublication. The default-tenant server
wrapper admits one execution across both its scoped and legacy selections, observes their combined
intermediate length, and pre-reserves an in-place merge/label-stripping path. Series count,
returned-byte, intermediate-length, and per-query/shared-memory failures remain structured and
release the slot on every exit. WAL-definition merging, other metadata entry points,
adapter-owned distributed merge buffers, and caller-owned returned vectors remain separate named
boundaries.
Rollup maintenance creates one internal execution per source read, preserving those instance limits
and tightening memory, scanned/returned samples, returned bytes, and intermediate length to the
finite maintenance ceiling. It never paginates or truncates a source into a false checkpoint: an
append-sort working set that cannot fit is rejected before allocation and retried on a later policy
cycle. Explicit `ExpertUnlimited` leaves both query and maintenance controls unbounded unless the
embedder supplies an override.
Caller-provided aggregator internals and allocator/runtime overhead remain outside that portable
model, and profile constants still require calibration across those entry points.

### Cardinality shape and background work

The core enforces total series, concurrent new-series creation per fixed time window, label count,
metric and label name/value format lengths, cumulative identity bytes, duplicate-label rejection,
and canonical label ordering. Shape validation runs before dictionary or registry growth. A
creation-rate reservation is retained through WAL/application publication; failed writes and
registry races release unused capacity, so only successfully published new identities become
committed window usage. `observability_snapshot().cardinality` reports current series, pending and
committed window usage, the storage-clock window start, lifetime admissions/commits, and rejections.

Per-metric budgets are intentionally not part of the embedded core. Server operators may layer
tenant policy on top of the canonical structured rejection path, but no standard per-metric or
per-tenant cardinality profile is published. Background workers have fixed inspectable topology
and cadence. The shared maintenance item/byte pair currently bounds compaction, sealed-chunk
persistence, active-flush discovery, retention/tiering root/action pagination, finite non-tiered
unknown-dirty catalog reconciliation, and rollup postings traversal; each rollup source read and its
downstream transform/row assembly share one finite query/maintenance envelope. Tiered
segment-catalog publication retains complete snapshots. Finite explicit/manual rollup calls share
the background cursor and advance at most one item/byte-bounded policy/source page; bounded status
counters plus the continuation policy and exclusive series ID distinguish partial progress until a
terminal page proves the cycle complete. Only explicit `ExpertUnlimited` drains that complete cycle
in one manual call.

## Why profile constants remain provisional

The named profiles are available so constrained hosts no longer inherit accidental unbounded
defaults, but their initial constants are not capacity or throughput claims. They remain
provisional until every workload in `resource-profile-measurements.md` is rerun on a clean tree and
the residual background-pass and excluded-memory classes above are qualified. The versioned
configuration snapshot makes later tuning visible, while low-level overrides and
`ExpertUnlimited` provide explicit migration paths.
