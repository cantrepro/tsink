# Resource limits and profiles

This document is the implementation inventory for Phase 2 of the execution charter. The design is
recorded in [ADR 0002](adr/0002-resource-profiles-and-budgets.md).

## Current status

tsink does **not** yet ship the named `Test`, `Embedded`, `Edge`, or `Server` resource profiles.
`StorageBuilder::new()` retains the legacy defaults: accounted memory, cardinality, and WAL bytes
have no finite limit unless the caller sets one. Relabeling those defaults as an embedded profile
would imply disk and query guarantees that the engine does not yet enforce.

After build, the canonical inspection API is `Storage::effective_storage_limits()`. The same value
is included at `Storage::observability_snapshot().limits`, is available directly from
`AsyncStorage`, and is preserved by the UniFFI/Python binding. The built-in server publishes it as
`data.effectiveStorageLimits` from `GET /api/v1/status/tsdb`.

```rust
let storage = tsink::StorageBuilder::new()
    .with_memory_limit(64 * 1024 * 1024)
    .with_cardinality_limit(100_000)
    .build()?;

let limits = storage.effective_storage_limits();
assert_eq!(limits.accounted_memory_bytes, Some(64 * 1024 * 1024));
assert_eq!(limits.cardinality, Some(100_000));
assert_eq!(storage.observability_snapshot().limits, limits);
# Ok::<(), tsink::TsinkError>(())
```

For the built-in backend, `None` means no finite limit is enforced for that field. For a third-party
backend with `reported_by_backend == false`, optional values are unknown and must not be interpreted
as either finite or unlimited. Durations are reported in nanoseconds so sub-millisecond builder
values remain inspectable.

## Enforced storage-side controls

| Effective field | Builder control | Legacy default | Current enforcement scope |
|---|---|---|---|
| `accounted_memory_bytes` | `with_memory_limit(bytes)` | `None` | Modeled bytes for active and sealed chunks, the series registry, metadata caches, persisted indexes, full persisted mapping lengths, and tombstones. Writes apply backpressure and eventually return `MemoryBudgetExceeded`. This is not a process-RSS cap. |
| `cardinality` | `with_cardinality_limit(series)` | `None` | Total registered metric-and-label identities. A write that must create too many series returns `CardinalityLimitExceeded`. There is no creation-rate or per-metric limit yet. |
| `wal_bytes` | `with_wal_size_limit(bytes)` | `None` | Recognized WAL segment bytes when a local WAL is active. The definitive quota check is serialized with the WAL writer, so concurrent logical writes cannot collectively pass a stale check. This is a WAL sublimit, not a data-directory quota. |
| `local_disk_bytes` | `with_local_disk_limit(bytes)` | `None` | Persistent core data-directory bytes admitted through the shared coordinator. Normal growth returns `DiskQuotaExceeded` at the boundary; reopening existing over-limit data remains possible. |
| `filesystem_free_headroom_bytes` | `with_filesystem_free_headroom(bytes)` | 0 for persistent storage | Physical free space that both normal and recovery work must leave available. A failed reservation returns `InsufficientDiskSpace`. |
| `maintenance_temp_reserve_bytes` | `with_maintenance_temp_reserve(bytes)` | 0 for persistent storage | Logical capacity withheld from normal growth but available to bounded maintenance output. Exhaustion returns `InsufficientCompactionHeadroom`. |
| `max_concurrent_writers` | `with_max_writers(n)` | cgroup-aware worker count | Synchronous engine writer permits. Zero passed to the builder selects the cgroup-aware default. |
| `write_timeout_nanos` | `with_write_timeout(duration)` | 30 seconds | Maximum wait for a writer permit and related lifecycle drains. |
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

Persisted mapping bytes are virtual file mapping lengths, not measured resident pages. The current
memory budget does not account for all process memory. Query result and intermediate
vectors, prepared write batches before publication, WAL userspace buffers, compression and
decompression scratch space, background-worker stacks, server request bodies, and allocator or
runtime overhead are outside the hard admission calculation. `excluded_categories` names these
known gaps. `excluded_bytes_known` is currently `false`, so the compatibility `excluded_bytes`
value of zero is not a measured total and must not be interpreted as “nothing excluded.”

`memory.pressure` reports pressure on this modeled scope, with `Normal`, `ApproachingLimit`,
`Backpressured`, `Rejecting`, and `Degraded` states. The approaching threshold is inspectable and is
currently 9,000 basis points (90%) of a finite budget. Active-writer, event, and rejection counters
are memory-specific; older flush admission counters combine memory and WAL pressure. `Degraded`
means the storage instance is degraded and does not assert that memory caused it. Atomic byte
reservations and the excluded work classes are still required before Phase 2 memory work is
complete.

### Async facade boundary

`AsyncRuntimeOptions` independently bounds each command queue (1,024 entries by default) and the
number of reader worker threads; writes use one serialized worker. Those are facade settings, not
storage-engine limits, and therefore are not currently included in `EffectiveStorageLimits`. The
final profile model must resolve both sets of controls together.

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
each temporary output before publication; a whole-operation compaction estimate is still pending.
Required startup, cleanup, and shutdown recovery work may
proceed while the logical cap is already exceeded, but still honors the physical free-space floor.
Cleanup that removes an entry is followed by an exclusive scan so category credits cannot
undercount surviving files; a no-op orphan pass does not rescan the tree.
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
`.tmp-seg-<16-lowercase-hex>` staging directories. Variable target names are also strict: registry
deltas use `delta-<16-lowercase-hex>.bin`, tombstone shards use
`shard-<three-decimal-digits-000-through-255>-<16-lowercase-hex>.bin`, and compaction replacement
markers use `replace-<16-lowercase-hex>-<16-lowercase-hex>.json`. Pending final compaction markers,
unknown files, and lookalikes are preserved. Tombstone shard cleanup first validates the complete
current or legacy manifest and fails closed on unrecognized bytes; it removes only exact canonical
shard names that the valid manifest does not reference. Filesystem-backed tier tombstone
directories receive the same durable ancestry linking even though they are intentionally outside
the local quota.

Opening an existing over-limit directory is supported: existing reads remain available,
`over_limit` is reported, and new normal growth is rejected. Unknown or host-created entries are
counted and retained. A snapshot destination resolving inside the managed tree is rejected; an
external snapshot destination is outside this quota and reports its own I/O failure. Online restore
targets that overlap the live managed root are rejected; external restore staging and target
directories remain unbudgeted. Object-store roots are outside the quota, and any configuration that
overlaps one with the managed root is rejected.

The built-in server opens this coordinator before constructing persistent stores and shares it with
the core, metric metadata, exemplars, rules, the usage ledger, and managed control-plane state.
Those writers reserve exact growth, synchronize successful publication, and publish in-memory
state only after persistence. Read-write storage holds the core data-path lease; server modes that
do not open a read-write core hold the same canonical process lease themselves. A final startup
reconciliation waits for outstanding reservations and makes status and metrics reflect every file
created during bootstrap. On shutdown, HTTP and Graphite request tasks remain owned and are drained
before storage closes or the process lease is released; the ten-second grace period is a warning
threshold, not permission to detach a still-running write.

The server flags are `--local-disk-limit`, `--filesystem-free-headroom`, and
`--maintenance-temp-reserve`; any of them requires `--data-path`. The shared snapshot is reported
even for compute-only server storage. Direct and internal metadata/exemplar quota failures retain
their structured resource category and map to HTTP 413, disclosing partial row progress when rows
already committed.

This is not yet a complete **server data-directory** contract. Experimental cluster control,
consensus, audit, dedupe and outbox files, plus edge-sync queues, are counted if they live beneath
the root and startup reconciliation sees them, but their runtime writers do not reserve capacity.
Clustered metadata/exemplar routing preserves a local or peer's structured disk-quota failure as
HTTP 413 when the requested acknowledgement count is not met. Online restore targets that overlap
the live root are rejected; external restore staging or targets and external snapshot destinations
remain outside this quota. Concurrent arbitrary host mutations cannot be prevented: the coordinator
tolerates and counts them when reconciled but is not a quota on other processes.

## Remaining hard-budget work

### Server-wide local disk

The core now coordinates WAL, segments and indexes, compaction and retention staging,
registry/catalog files, tombstones, and rollup state. The server additionally coordinates metadata,
exemplars, rules, usage, and managed state through the same root. Experimental cluster and edge
subsystems still write control/consensus/audit logs, dedupe and outbox logs, and edge queues through
separate persistence paths. Restore staging outside the live root is also unbudgeted. A complete
server profile needs reservations or explicitly documented sub-budgets for those remaining paths
without weakening tenant isolation.

A complete server-wide model still needs:

- atomic reservations in the remaining cluster and edge writers;
- startup, concurrent-admission, and failure-path tests for those additional categories;
- explicit restore-staging accounting;
- cleanup of eligible expired data before rejecting normal growth;
- whole-operation compaction headroom estimation and rollback coverage;
- a crash-durable coordinator for logically atomic multi-file tombstone and rollup publication;
- a measured policy for batching exclusive cleanup reconciliation, which currently favors exactness
  over cleanup throughput.

Core tests now cover over-limit recovery, restart reconciliation, unknown files, concurrent
reservations, per-output compaction preflight, WAL reset, catalog repair, symlink-safe entry replacement,
publication rollback after directory-sync failure, and injected partial filesystem-full writes.
Server tests cover tiny-limit no-publication behavior, exact category accounting, concurrent usage
append ordering, torn-tail rejection, restart reconciliation across sidecars, compute-only process
leasing and observability, structured 413 responses with partial progress, and rejection of online
restore targets that overlap the live root.
The internal exact-reconciliation helper assumes its caller does not hold a second reservation on
the same coordinator; batching or a deferred-reconciliation protocol should replace that
correctness-first constraint before advertising high cleanup throughput.

### Usage accounting

Ordinary durable usage appends run through one bounded blocking lane. Storage reconciliation scans
on a separate one-permit lane, so a long scan does not occupy the ordinary append lane; its final
multi-tenant result is appended as one physical batch frame. Handlers await metering, so ledger I/O
can add response latency, but a ledger failure does not reclassify already-completed primary work as
a retryable data failure.

The in-memory record history is not bounded. Status, report, support-bundle, and export paths scan
that history synchronously, and export has no bounded pagination or streaming cursor. An incremental
aggregate plus bounded read/export APIs is still required before a finite server profile can cover
usage accounting.

### Embedded queries

`QueryOptions::limit` and `offset`, `QueryRowsScanOptions::max_rows`, and
`ShardWindowScanOptions::{max_series,max_rows}` are caller-requested pagination controls. They do
not bound series discovery, samples scanned, intermediate allocations, returned bytes, pattern
expansion, wall time, or concurrent direct queries. Reaching a future resource budget must produce
a structured rejection, not silent pagination.

The HTTP server has separate finite request admission defaults (64 concurrent read requests and
128 in-flight query units) and request-shape policies, but a caller using the Rust or Python library
bypasses those server guards. Phase 2 therefore requires a runtime-independent shared core query
context with concurrency, deadline/cancellation, scan, result, and intermediate-memory budgets.

### Cardinality shape and background work

The current cardinality cap is total-series only. Per-metric series, label-pair counts, label
lengths, labels per series, and series-creation rate do not yet share a profile budget. Background
workers are fixed in number and some individual jobs are bounded, but there is no common budget for
compaction, tier fetch, retention, rollup, remote refresh, or maintenance queues.

## Why named profiles remain withheld

A finite memory number plus a WAL cap is not a finite database envelope. Standard profile values
will be published only after the disk, query, cardinality-shape, and background-work controls above
are enforced, their effective values are inspectable, and the values are selected from recorded
measurements and tiny-limit boundary tests. The existing low-level builder methods remain available
in the meantime.
