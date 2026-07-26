# Configuration reference

Reference for the supported embedded builder, server CLI, and environment controls that tune
ingestion, clustering, and background workers.

Sections are ordered from most commonly used to most advanced.

`StorageBuilder::new()` selects the finite `Embedded` resource profile. Individual memory,
cardinality, WAL, disk, query, async, and maintenance controls below become sparse overrides; they
win even if `with_resource_profile(...)` is called later. The server selects the finite `Server`
profile by default. Exact enforcement scope, versioned post-build inspection, provisional profile
values, and migration guidance are documented in
[Resource limits and profiles](resource-limits.md).

---

## Contents

1. [Embedded library — `StorageBuilder`](#1-embedded-library--storagebuilder)
2. [Server CLI flags](#2-server-cli-flags)
   - [Networking & listeners](#21-networking--listeners)
   - [Storage & WAL](#22-storage--wal)
   - [Memory & cardinality](#23-memory--cardinality)
   - [Security & auth](#24-security--auth)
   - [Cluster (experimental)](#25-cluster-experimental)
   - [Edge sync](#26-edge-sync)
3. [Environment variables — server admission](#3-environment-variables--server-admission)
4. [Environment variables — ingestion protocols](#4-environment-variables--ingestion-protocols)
5. [Environment variables — rules engine](#5-environment-variables--rules-engine)
6. [Environment variables — cluster (experimental)](#6-environment-variables--cluster-experimental)
   - [RPC & writes](#61-rpc--writes)
   - [Reads](#62-reads)
   - [Hinted handoff outbox](#63-hinted-handoff-outbox)
   - [Digest exchange / anti-entropy](#64-digest-exchange--anti-entropy)
   - [Repair](#65-repair)
   - [Rebalance](#66-rebalance)
   - [Control plane (Raft)](#67-control-plane-raft)
   - [Write deduplication](#68-write-deduplication)

---

## 1. Embedded library — `StorageBuilder`

These are options on the Rust `StorageBuilder` API. The Python builder exposes a documented subset;
see the [embedded library guide](embedded-library.md) and
[Python bindings guide](python-bindings.md) for the exact surface and usage examples.

### Storage & persistence

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_resource_profile(profile)` | `ResourceProfile` | `Embedded` | Select `Test`, `Embedded`, `Edge`, `Server`, `Custom(ResourceLimits)`, or the legacy-migration `ExpertUnlimited` base. Existing low-level overrides are preserved. |
| `clear_resource_limit_override(field)` / `clear_resource_limit_overrides()` | `ResourceLimitOverride` | — | Clear one override group or all groups and restore values from the selected base. |
| `with_data_path(path)` | `PathBuf` | *(none)* | Root directory for all on-disk data (WAL, segments, metadata). Required for durable storage. |
| `with_object_store_path(path)` | `PathBuf` | *(none)* | Root directory for tiered segment lanes (`hot/`, `warm/`, `cold/`). Required for tiered storage. At most one `ReadWrite` process may own a root; `ComputeOnly` readers may share it. |
| `with_runtime_mode(mode)` | `StorageRuntimeMode` | `ReadWrite` | `ReadWrite` — full local instance. `ComputeOnly` — query node that reads from object store without persisting locally. |
| `with_timestamp_precision(p)` | `TimestampPrecision` | `Nanoseconds` | Interpretation of raw integer timestamps: `Seconds`, `Milliseconds`, `Microseconds`, or `Nanoseconds`. Must match the precision of all ingested data. |

### Retention & tiering

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_retention(duration)` | `Duration` | `14 days` | How long data is retained. Writes outside this window are rejected when `retention_enforced` is set (which `with_retention` enables automatically). |
| `with_retention_enforced(enabled)` | `bool` | `false` | Enable or disable retention rejection and filtering independently of the configured window. |
| `with_tiered_retention_policy(hot, warm)` | `(Duration, Duration)` | *(both fall back to `retention`)* | Sets the ages at which data moves from local hot storage to the warm tier and then to the cold tier. Calling it also enables retention enforcement. |
| `with_mirror_hot_segments_to_object_store(bool)` | `bool` | `false` | Copy freshly-persisted hot segments into `<object_store_path>/hot/` in addition to writing locally. Useful for cross-node availability. |
| `with_remote_segment_cache_policy(policy)` | `RemoteSegmentCachePolicy` | `MetadataOnly` | Selects the remote segment cache policy. `MetadataOnly` is the only policy currently available. |
| `with_remote_segment_refresh_interval(duration)` | `Duration` | `5s` | How often a `ComputeOnly` node refreshes its view of remote segment metadata. |

### Chunk & partition tuning

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_chunk_points(n)` | `usize` | `2048` | Target number of data points per chunk before the chunk is sealed. Clamped to `1..=65535`. Larger values improve compression; smaller values reduce read amplification on recent data. |
| `with_partition_duration(duration)` | `Duration` | `1 hour` | Time window covered by a single partition. All series data within this window is co-located. |
| `with_max_active_partition_heads_per_series(n)` | `usize` | `8` | Maximum simultaneously open partition heads per series. A newer partition can seal the oldest head to make room; a write that would open another older partition is rejected once the bound is full. |

### Write pipeline

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_max_writers(n)` | `usize` | `4` (`Embedded`) | Maximum writes admitted concurrently by the synchronous engine. |
| `with_write_timeout(duration)` | `Duration` | `30s` | Maximum time a write call will wait for a writer slot before returning a backpressure error. |
| `with_max_future_skew(duration)` | `Duration` | *(unset)* | Opt-in clock-relative admission cutoff. A timestamp exactly at `now + duration` is accepted; a later timestamp is rejected before series or WAL state is created. |
| `with_write_batch_limits(limits)` | `WriteBatchLimits` | 100,000 rows / 64 MiB (`Embedded`) | Pre-clone top-level row and modeled input bounds. Violations commit nothing and return a structured batch-limit error before allocating indexed outcomes. |

### Memory & cardinality

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_memory_limit(n)` | `usize` | `512 MiB` (`Embedded`) | Budget for the engine's accounted storage memory. This is not a hard total-process RSS cap; inspect `observability_snapshot()` for accounted and excluded categories. |
| `with_cardinality_limit(n)` | `usize` | `1,000,000` (`Embedded`) | Hard cap on the total number of unique series. Writes that would create a new series beyond this limit are rejected with a cardinality error. |
| `with_max_labels_per_series(n)` | `usize` | `128` | Maximum labels in a submitted series identity. Values above the 65,535-label storage-format maximum fail during build. |
| `with_max_series_identity_bytes(n)` | `usize` | `65536` | Maximum cumulative UTF-8 bytes in the metric name and all label names and values. |
| `with_series_creation_rate_limit(n, window)` | `usize`, `Duration` | 100,000 / 60 s (`Embedded`) | Maximum successfully published new series in a fixed storage-clock window. Concurrent in-flight reservations count; failed writes release them. |

### WAL

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_wal_enabled(bool)` | `bool` | `true` | Enable or disable the write-ahead log. Disabling removes write-time WAL recovery guarantees. |
| `with_wal_size_limit(n)` | `usize` | `512 MiB` (`Embedded`) | Maximum projected on-disk WAL size. A batch that would exceed the limit is rejected; WAL space is reclaimed after persisted state makes older records unnecessary. |
| `with_wal_buffer_size(n)` | `usize` | `4096` | I/O buffer size for WAL writes. Larger buffers reduce syscall overhead on high-throughput workloads. |
| `with_wal_sync_mode(mode)` | `WalSyncMode` | `PerAppend` | `PerAppend` synchronizes each non-empty batch. `Periodic(duration)` checks the elapsed interval during a later append; it has no autonomous timer, so successful writes may be `Appended` until another write or lifecycle action synchronizes them. |
| `with_wal_replay_mode(mode)` | `WalReplayMode` | `Strict` | `Strict` — abort recovery on any corrupted WAL frame. `Salvage` — skip corrupted frames and recover as much data as possible. |

### Local disk

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_local_disk_limit(n)` | `u64` | `16 GiB` (`Embedded`) | Logical byte quota beneath the persistent data path. Existing over-limit data can reopen, but new growth is rejected. |
| `with_filesystem_free_headroom(n)` | `u64` | `256 MiB` (`Embedded`) | Filesystem free space reserved for the host. |
| `with_maintenance_temp_reserve(n)` | `u64` | `1 GiB` (`Embedded`) | Portion of the logical quota reserved for maintenance and recovery admission. |

### Query budget

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_query_budget_limits(limits)` | `QueryBudgetLimits` | finite (`Embedded`) | Override shared query concurrency/memory and per-query work, result, deadline, and memory limits. Exact resolved values are exposed by `resource_configuration_snapshot()` and listed in [resource limits](resource-limits.md). |

### Background workers

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_background_fail_fast(bool)` | `bool` | `true` | When `true`, a failure in any background worker (flush, compaction, remote segment refresh) immediately fences all further writes with an error. When `false`, the error is logged but writes continue. |
| `with_maintenance_max_items_per_pass(n)` | `usize` | `100,000` (`Embedded`) | Maximum logical inventory items selected by one bounded maintenance pass, including retention/tiering roots inspected in one worker wake. A finite cap must cover the effective write-batch row limit. |
| `with_maintenance_max_bytes_per_pass(n)` | `u64` | `512 MiB` (`Embedded`) | Maximum modeled source bytes selected by one bounded maintenance pass. Retention/tiering charges descriptor bytes and manifest-declared source bytes for every selected replacement action. A finite cap must be at least the effective accounted-memory limit so one admitted sealed chunk cannot be stranded. |

### Cluster / metadata sharding

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_metadata_shard_count(n)` | `u32` | *(none, no sharding)* | Partition the in-memory series metadata into N shards to reduce lock contention on high-cardinality workloads. |

---

## 2. Server CLI flags

All flags are passed on the command line to the `tsink-server` binary. Defaults shown are the compiled-in values; they may be overridden at any time with the corresponding flag.

```
tsink-server --help
```

### 2.1 Networking & listeners

| Flag | Default | Description |
|---|---|---|
| `--listen <HOST:PORT>` | `127.0.0.1:9201` | TCP address for the HTTP/HTTPS listener. |
| `--statsd-listen <HOST:PORT>` | *(disabled)* | UDP address for the StatsD listener. Omit to disable. |
| `--statsd-tenant <ID>` | `default` | Tenant that receives StatsD writes. |
| `--graphite-listen <HOST:PORT>` | *(disabled)* | TCP address for the Graphite plaintext listener. Omit to disable. |
| `--graphite-tenant <ID>` | `default` | Tenant that receives Graphite writes. |

### 2.2 Storage & WAL

| Flag | Default | Description |
|---|---|---|
| `--resource-profile <PROFILE>` | `server` | Core base profile: `test`, `embedded`, `edge`, `server`, or `expert-unlimited`. Explicit low-level flags override the selected base. |
| `--data-path <PATH>` | *(none)* | Persist data under PATH. Required by `--cluster-enabled`; without it, only non-cluster storage can run purely in memory. |
| `--object-store-path <PATH>` | *(none)* | Object-store root for tiered segment lanes (`hot/`, `warm/`, `cold/`). Must not overlap `--data-path`; at most one read-write process may own the root. |
| `--local-disk-limit <BYTES>` | `256 GiB` (`Server`, when persistent) | Shared logical byte limit for budget-integrated writers under `--data-path`. Must be greater than zero when set. |
| `--filesystem-free-headroom <BYTES>` | `2 GiB` (`Server`, when persistent) | Filesystem free space that budget-integrated writes must leave available. |
| `--maintenance-temp-reserve <BYTES>` | `16 GiB` (`Server`, when persistent) | Capacity withheld from normal growth for maintenance temporary output. Must be smaller than `--local-disk-limit` when that limit is set. |
| `--offline-restore-root <PATH>` | *(none)* | Dedicated parent directory for bounded offline restore targets. Must be paired with `--offline-restore-disk-limit`, isolated from the live data and object-store roots, and a strict descendant of `--admin-path-prefix` when that prefix is set. |
| `--offline-restore-disk-limit <BYTES>` | *(none)* | Finite logical limit shared by restore staging, restored targets, and cluster restore reports beneath `--offline-restore-root`. |
| `--offline-restore-filesystem-free-headroom <BYTES>` | `0` | Filesystem free space that offline restore work must leave available. Requires the other two offline-restore flags. |
| `--timestamp-precision <PRECISION>` | `ms` | Units for raw timestamps: `s`, `ms`, `us`, `ns`. |
| `--retention <DURATION>` | `14d` | Data retention window (e.g. `7d`, `24h`, `90d`). |
| `--hot-tier-retention <DURATION>` | *(same as `--retention`)* | Age at which local segments move to the warm object-store tier. |
| `--warm-tier-retention <DURATION>` | *(same as `--retention`)* | Age at which warm segments move to the cold object-store tier. |
| `--storage-mode <MODE>` | `read-write` | `read-write` — normal full node. `compute-only` — query-only node backed by object store. |
| `--remote-segment-refresh-interval <DURATION>` | `5s` | Metadata refresh interval for `compute-only` nodes. |
| `--mirror-hot-segments-to-object-store <BOOL>` | `false` | Copy hot segments to object store as they are sealed. |
| `--wal-enabled <BOOL>` | `true` | Enable (`true`) or disable (`false`) the WAL. |
| `--wal-sync-mode <MODE>` | `per-append` | `per-append` (synchronize each non-empty write) or `periodic` (append-driven interval, higher throughput). |
| `--chunk-points <N>` | `2048` | Target data points per chunk (1–65535). |

Usage accounting has a separate finite memory/startup/read envelope:

| Flag | Default | Description |
|---|---:|---|
| `--usage-ledger-recent-records <N>` | `8192` | Recent sequence-ordered records retained for raw export and time-bucket reports. Exact all-time summaries are stored separately. |
| `--usage-ledger-max-tenants <N>` | `4096` | Maximum tenant keys in exact all-time summaries; an N+1 tenant record is rejected before ledger publication. |
| `--usage-ledger-max-record-bytes <BYTES>` | `64K` | Maximum canonical JSON bytes in one record. |
| `--usage-ledger-max-frame-bytes <BYTES>` | `8M` | Maximum JSON bytes in one single-record or atomic batch frame. |
| `--usage-ledger-max-line-bytes <BYTES>` | `8388609` | Maximum frame plus line terminator read during startup. Must exceed the frame limit. |
| `--usage-ledger-max-batch-records <N>` | `4096` | Maximum records in one atomic batch frame. |
| `--usage-ledger-startup-scratch-bytes <BYTES>` | `32M` | Startup parsing envelope. Validation requires space for the line, decoded frame, and configured batch slots. |
| `--usage-ledger-max-sequence-ranges <N>` | `4096` | Maximum disjoint legacy sequence ranges used for bounded duplicate validation. |
| `--usage-report-default-records <N>` | `1000` | Default records aggregated by a time-filtered or bucketed report page. |
| `--usage-report-max-records <N>` | `4096` | Maximum records aggregated by one report page. |
| `--usage-report-max-response-bytes <BYTES>` | `4M` | Maximum encoded report response; overflow is a structured HTTP 413. |
| `--usage-export-default-records <N>` | `1000` | Default raw records returned per export page. |
| `--usage-export-max-records <N>` | `4096` | Maximum raw records returned per export page. |
| `--usage-export-max-response-bytes <BYTES>` | `4M` | Maximum NDJSON bytes returned per export page. |

All usage limits must be nonzero. The frame must fit a record, the line must fit a frame plus its
newline, the atomic batch bound must cover the tenant bound, and the export byte maximum must fit
one maximum-size record. Default page sizes cannot exceed their maxima, and the startup scratch
value must satisfy the line/frame/batch relationship checked at startup. These effective values are
returned in the usage journal under `limits`. They remain finite when the core resource profile is
`ExpertUnlimited`; the server has no unlimited usage-ledger flag or zero sentinel.

Disk values accept an integer byte count or a case-insensitive binary `K`, `M`, `G`, or `T` suffix
(for example, `512M` or `1.5G`). Setting `--local-disk-limit`, a non-zero
`--filesystem-free-headroom`, or a non-zero `--maintenance-temp-reserve` requires `--data-path`.
Experimental cluster mode also requires `--data-path`, even when all three disk limits retain their
defaults and even for a `query`-role node. There is no unleased temporary-root fallback for cluster
persistence. Data-path-free operation is limited to non-cluster in-memory storage.
Each read-write tiered engine also holds `<object-store-path>/.tsink-writer.lock` for its lifetime,
independent of its local data-path lease. This prevents two distinct local stores from publishing
incompatible shared tombstone or segment-catalog histories. Compute-only mode is exempt because it
does not recover or mutate shared state.
The maintenance reserve is unavailable to normal growth; maintenance may use it while still leaving
the configured filesystem headroom. The headroom plus reserve must fit in the supported 64-bit byte
range.

Quota-aware admission currently covers core storage, metric metadata, exemplars, rules, the usage
ledger, managed control-plane state, the experimental hinted-handoff outbox, cluster deduplication
markers, the cluster audit log, the paired cluster control state and consensus log, and both the
edge source queue and standalone edge-accept deduplication markers. The control pair stages both
complete replacements under one `Cluster` reservation and publishes the authoritative log first.
External snapshot destinations remain outside the shared live-data budget. Restore is fail-closed
unless the paired offline-root and finite-limit flags are configured. The server holds a distinct
cross-process lease on that root and uses one coordinator for standalone restore, both internal
restore routes, cluster node targets, and the post-restore report. Targets must be strict
descendants; the offline root must not overlap `--data-path` or `--object-store-path`. Snapshot
destinations inside the offline root are rejected so export bytes cannot consume its separately
bounded capacity. Online restore targets that overlap the live `--data-path` are also rejected.
Files beneath `--data-path` can still appear
in reconciled usage, but that accounting does not make excluded writers quota-safe.

### 2.3 Memory & cardinality

| Flag | Default | Description |
|---|---|---|
| `--memory-limit <BYTES>` | `2 GiB` (`Server`) | Global accounted storage-memory budget, in bytes. Supports suffixes such as `1G`, `512M`. |
| `--maintenance-max-items-per-pass <N>` | `500,000` (`Server`) | Maximum logical items selected by one bounded maintenance pass. A finite override must be at least the effective write-batch row limit. |
| `--maintenance-max-bytes-per-pass <BYTES>` | `2 GiB` (`Server`) | Maximum modeled bytes selected by one bounded maintenance pass. A finite override must be at least `--memory-limit`; raise both together when increasing memory. |
| `--cardinality-limit <N>` | `10,000,000` (`Server`) | Maximum number of unique series. New series are rejected once the limit is reached. |
| `--max-labels-per-series <N>` | `128` (core default) | Maximum labels in each submitted series identity. |
| `--max-series-identity-bytes <BYTES>` | `65536` (core default) | Maximum cumulative metric and label UTF-8 bytes in one identity. |
| `--max-new-series-per-window <N>` | `1,000,000` per profile window (`Server`) | Maximum new series that may publish per creation-rate window. Supplying this low-level override requires `--new-series-window`. |
| `--new-series-window <DURATION>` | `60s` (`Server`) | Fixed storage-clock window for new-series admission. Supplying this low-level override requires `--max-new-series-per-window`. |
| `--max-writers <N>` | `16` (`Server`) | Concurrent writer permits. |

### 2.4 Security & auth

| Flag | Default | Description |
|---|---|---|
| `--tls-cert <PATH>` | *(none)* | PEM-encoded TLS certificate. Both `--tls-cert` and `--tls-key` must be set to enable TLS. |
| `--tls-key <PATH>` | *(none)* | PEM-encoded TLS private key. |
| `--auth-token <TOKEN>` | *(none)* | Static bearer token required on all non-admin requests. |
| `--auth-token-file <PATH>` | *(none)* | File or exec-based token manifest (JSON). Mutually exclusive with `--auth-token`. |
| `--admin-auth-token <TOKEN>` | *(none)* | Static bearer token required on `/api/v1/admin/*` endpoints. |
| `--admin-auth-token-file <PATH>` | *(none)* | File or exec-based admin token manifest. Mutually exclusive with `--admin-auth-token`. |
| `--tenant-config <PATH>` | *(none)* | JSON file defining per-tenant auth, quotas, and policies. See [Multi-tenancy](multi-tenancy.md). |
| `--rbac-config <PATH>` | *(none)* | JSON file defining RBAC roles, service accounts, and OIDC settings. See [Security model](security.md). |
| `--enable-admin-api` | `false` | Expose admin snapshot, restore, and experimental cluster management endpoints. |
| `--admin-path-prefix <PATH>` | *(none)* | Restrict admin file I/O operations to this directory prefix. |

### 2.5 Cluster (experimental)

Cluster mode is an experimental advanced capability, not part of tsink's primary embedded, single-node product. These flags are only relevant when `--cluster-enabled` is set. See [Cluster setup](cluster-setup.md) and [Clustering internals](clustering-internals.md) for deployment guidance.

| Flag | Default | Description |
|---|---|---|
| `--cluster-enabled <BOOL>` | `false` | Enable experimental cluster mode. Requires `--data-path`. |
| `--cluster-node-id <ID>` | *(required)* | Stable, unique identifier for this node. Must not change after initial startup. |
| `--cluster-bind <HOST:PORT>` | *(none)* | Internal RPC bind/advertise address. Peers will connect to this address. |
| `--cluster-node-role <ROLE>` | `hybrid` | `storage` — data only; `query` — query fan-out only; `hybrid` — both. |
| `--cluster-seeds <LIST>` | *(none)* | Comma-separated `HOST:PORT` addresses of seed peers for cluster bootstrap. |
| `--cluster-shards <N>` | `128` | Number of logical hash-ring shards. Changing this after data is stored requires a full rebalance. |
| `--cluster-replication-factor <N>` | `1` | Number of replicas for each shard. |
| `--cluster-write-consistency <LEVEL>` | `quorum` | `one`, `quorum`, or `all` — how many replicas must acknowledge a write. |
| `--cluster-read-consistency <LEVEL>` | `eventual` | `eventual`, `quorum`, or `strict` — read consistency level. |
| `--cluster-read-partial-response <POLICY>` | `allow` | `allow` — return partial results when some shards are unavailable; `deny` — fail the query. |
| `--cluster-internal-auth-token <TOKEN>` | *(none)* | Shared secret for internal RPC authentication (used when mTLS is not enabled). |
| `--cluster-internal-auth-token-file <PATH>` | *(none)* | File/exec manifest for the internal RPC token. |
| `--cluster-internal-mtls-enabled <BOOL>` | `false` | Enable mTLS for all internal peer-to-peer RPC. |
| `--cluster-internal-mtls-ca-cert <PATH>` | *(none)* | PEM CA bundle for internal mTLS. |
| `--cluster-internal-mtls-cert <PATH>` | *(none)* | PEM client certificate for internal mTLS. |
| `--cluster-internal-mtls-key <PATH>` | *(none)* | PEM client key for internal mTLS. |

### 2.6 Edge sync

Queues locally accepted row batches and replays them to an upstream tsink instance. Metadata and
exemplar sidecars are not included in the source queue. This is useful for edge deployments or row
write aggregation.

| Flag | Default | Description |
|---|---|---|
| `--edge-sync-upstream <HOST:PORT>` | *(disabled)* | Upstream server to replay writes to. Omit to disable edge sync. |
| `--edge-sync-auth-token <TOKEN>` | *(none)* | Bearer token used when writing to the upstream server. |
| `--edge-sync-source-id <ID>` | `--listen` address | Stable identifier for this edge node, used to generate idempotency keys. |
| `--edge-sync-static-tenant <ID>` | *(none)* | Rewrite all tenant labels to this value before forwarding writes upstream. |

---

## 3. Environment variables — server admission

These variables cap the number of concurrent HTTP requests and in-flight rows to protect the server under sudden load. They are read once at process start.

| Variable | Default | Description |
|---|---|---|
| `TSINK_SERVER_WRITE_MAX_INFLIGHT_REQUESTS` | `64` | Maximum number of concurrent write HTTP requests accepted by the server. |
| `TSINK_SERVER_WRITE_MAX_INFLIGHT_ROWS` | `200000` | Maximum total rows across all active write requests. New requests block until below the threshold. |
| `TSINK_SERVER_WRITE_RESOURCE_ACQUIRE_TIMEOUT_MS` | `25` | Milliseconds to wait for a write slot before returning HTTP 429. |
| `TSINK_SERVER_READ_MAX_INFLIGHT_REQUESTS` | `64` | Maximum number of concurrent read HTTP requests. |
| `TSINK_SERVER_READ_MAX_INFLIGHT_QUERIES` | `128` | Maximum total in-flight queries across all read requests. |
| `TSINK_SERVER_READ_RESOURCE_ACQUIRE_TIMEOUT_MS` | `25` | Milliseconds to wait for a read slot before returning HTTP 429. |

---

## 4. Environment variables — ingestion protocols

These variables control per-protocol feature flags and per-request limits.

| Variable | Default | Description |
|---|---|---|
| `TSINK_REMOTE_WRITE_METADATA_ENABLED` | `true` | Accept metric metadata in Prometheus remote-write requests (capped at 512 metadata entries per request). When `false`, a request containing metadata is rejected explicitly. |
| `TSINK_REMOTE_WRITE_EXEMPLARS_ENABLED` | `true` | Accept exemplar records in Prometheus remote-write requests. |
| `TSINK_REMOTE_WRITE_HISTOGRAMS_ENABLED` | `true` | Accept native histogram samples in Prometheus remote-write requests (capped at 16,384 bucket entries per request). |
| `TSINK_INFLUX_LINE_PROTOCOL_ENABLED` | `true` | Enable the InfluxDB line-protocol endpoints (`POST /write`, `POST /api/v2/write`). |
| `TSINK_INFLUX_LINE_PROTOCOL_MAX_LINES_PER_REQUEST` | `4096` | Maximum number of lines accepted in a single InfluxDB line-protocol request. |
| `TSINK_OTLP_METRICS_ENABLED` | `true` | Enable the OTLP HTTP/protobuf metrics ingestion endpoint (`POST /v1/metrics`). |
| `TSINK_STATSD_MAX_PACKET_BYTES` | `8192` | Maximum UDP packet size for the StatsD listener. |
| `TSINK_STATSD_MAX_EVENTS_PER_PACKET` | `1024` | Maximum number of StatsD events parsed from a single UDP packet. |
| `TSINK_GRAPHITE_MAX_LINE_BYTES` | `8192` | Maximum byte length of a single Graphite plaintext line. |

---

## 5. Environment variables — rules engine

| Variable | Default | Description |
|---|---|---|
| `TSINK_RULES_SCHEDULER_TICK_MS` | `1000` | Interval in milliseconds between rules-engine scheduler evaluations. |
| `TSINK_RULES_MAX_RECORDING_ROWS_PER_EVAL` | `10000` | Maximum rows written by a single recording rule evaluation. Evaluations that would exceed this are rejected before row construction. |
| `TSINK_RULES_MAX_ALERT_INSTANCES_PER_RULE` | `10000` | Maximum number of alert instances tracked per alerting rule. |

Embedded callers can configure the finite rules-sidecar count, input-byte, retained, durable,
startup, replacement, runtime-update, and status ceilings through
`RulesRuntime::open_with_config`; see [Rules](rules.md#environment-variables).

---

## 6. Environment variables — cluster (experimental)

These variables configure the experimental cluster subsystem. They are all read once at startup unless otherwise noted.

### 6.1 RPC & writes

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_RPC_TIMEOUT_MS` | `2000` | Timeout in milliseconds for a single internal RPC call. |
| `TSINK_CLUSTER_RPC_MAX_RETRIES` | `2` | Number of retries on transient RPC failures before giving up. |
| `TSINK_CLUSTER_WRITE_MAX_BATCH_ROWS` | `1024` | Maximum rows per remote-write batch sent to a replica. |
| `TSINK_CLUSTER_WRITE_MAX_INFLIGHT_BATCHES` | `32` | Maximum number of concurrent write batches in flight to all replicas combined. |
| `TSINK_CLUSTER_FANOUT_CONCURRENCY` | `16` | Maximum concurrent sub-requests when fanning a write out to multiple shards. |

### 6.2 Reads

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_READ_MAX_MERGED_SERIES` | `250000` | Maximum unique series returned by a distributed query. |
| `TSINK_CLUSTER_READ_MAX_MERGED_POINTS_PER_SERIES` | `1000000` | Maximum data points per series in a distributed query result. |
| `TSINK_CLUSTER_READ_MAX_MERGED_POINTS_TOTAL` | `5000000` | Maximum total data points across all series in a distributed query result. |
| `TSINK_CLUSTER_READ_MAX_INFLIGHT_QUERIES` | `64` | Maximum concurrent distributed read queries across the node. |
| `TSINK_CLUSTER_READ_MAX_INFLIGHT_MERGED_POINTS` | `20000000` | Maximum total in-flight merged points across all concurrent distributed reads. |
| `TSINK_CLUSTER_READ_RESOURCE_ACQUIRE_TIMEOUT_MS` | `25` | Milliseconds to wait for a distributed-read concurrency slot before returning an error. |

### 6.3 Hinted handoff outbox

When a replica is temporarily unreachable, writes are queued in an on-disk outbox (backed by a WAL) and replayed once the replica recovers.

When a shared local-disk coordinator is configured, each Put reserves growth in the `Cluster`
category before publishing queue state. A rejected reservation is reported as a disk resource limit
and maps to HTTP 413. The Ack append can use Recovery admission at the logical quota and then
attempts bounded, parent-synchronized cleanup compaction. A compaction failure remains observable
cleanup debt for the background worker to retry; it does not undo a durable Ack or reschedule.

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_OUTBOX_MAX_ENTRIES` | `100000` | Maximum queued entries across all unreachable peers combined. |
| `TSINK_CLUSTER_OUTBOX_MAX_BYTES` | `536870912` (512 MiB) | Total in-memory size cap for the outbox. |
| `TSINK_CLUSTER_OUTBOX_MAX_PEER_BYTES` | `268435456` (256 MiB) | Per-peer in-memory size cap for the outbox. |
| `TSINK_CLUSTER_OUTBOX_MAX_LOG_BYTES` | `2147483648` (2 GiB) | Maximum on-disk WAL size for the outbox log. |
| `TSINK_CLUSTER_OUTBOX_MAX_RECORD_BYTES` | `2097152` (2 MiB) | Maximum size of a single outbox record. |
| `TSINK_CLUSTER_OUTBOX_REPLAY_INTERVAL_SECS` | `2` | Interval in seconds between replay attempts for queued entries. |
| `TSINK_CLUSTER_OUTBOX_REPLAY_BATCH_SIZE` | `256` | Maximum queued outbox entries considered per replay pass. |
| `TSINK_CLUSTER_OUTBOX_MAX_BACKOFF_SECS` | `30` | Maximum backoff in seconds between replay attempts when the peer remains unresponsive. |
| `TSINK_CLUSTER_OUTBOX_CLEANUP_INTERVAL_SECS` | `30` | Interval at which stale delivered records are pruned from the outbox log. |
| `TSINK_CLUSTER_OUTBOX_CLEANUP_MIN_STALE_RECORDS` | `1024` | Minimum number of stale records required to trigger an early cleanup pass. |
| `TSINK_CLUSTER_OUTBOX_STALLED_PEER_AGE_SECS` | `300` | Seconds of outbox age before a peer is flagged as stalled. |
| `TSINK_CLUSTER_OUTBOX_STALLED_PEER_MIN_ENTRIES` | `1` | Minimum queued entries for a peer to be considered stalled. |
| `TSINK_CLUSTER_OUTBOX_STALLED_PEER_MIN_BYTES` | `1` | Minimum queued bytes for a peer to be considered stalled. |

### 6.4 Digest exchange / anti-entropy

Nodes periodically exchange fingerprint digests to detect and repair missing data without full scans.

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_DIGEST_INTERVAL_SECS` | `30` | Interval in seconds between digest exchange rounds per node. |
| `TSINK_CLUSTER_DIGEST_WINDOW_SECS` | `300` | Time lookback window covered by each digest exchange. |
| `TSINK_CLUSTER_DIGEST_MAX_SHARDS_PER_TICK` | `64` | Maximum shards compared in a single digest tick. |
| `TSINK_CLUSTER_DIGEST_MAX_MISMATCH_REPORTS` | `128` | Maximum mismatch records held in memory before older ones are evicted. |
| `TSINK_CLUSTER_DIGEST_MAX_BYTES_PER_TICK` | `262144` (256 KiB) | Maximum payload size of the digest message sent per tick. |

### 6.5 Repair

Repair uses the mismatch records found during digest exchange to transfer missing data between nodes.

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_REPAIR_MAX_MISMATCHES_PER_TICK` | `2` | Maximum diverged shards repaired per tick. |
| `TSINK_CLUSTER_REPAIR_MAX_SERIES_PER_TICK` | `256` | Maximum series scanned per repair tick. |
| `TSINK_CLUSTER_REPAIR_MAX_ROWS_PER_TICK` | `16384` | Maximum rows transferred per repair tick. |
| `TSINK_CLUSTER_REPAIR_MAX_RUNTIME_MS_PER_TICK` | `100` | Wall-clock budget in milliseconds per repair tick. |
| `TSINK_CLUSTER_REPAIR_FAILURE_BACKOFF_SECS` | `30` | Backoff in seconds after a failed repair attempt before retrying. |

### 6.6 Rebalance

Rebalance migrates shard ownership when nodes are added or removed.

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_REBALANCE_INTERVAL_SECS` | `5` | Interval in seconds between rebalance loop ticks. |
| `TSINK_CLUSTER_REBALANCE_MAX_ROWS_PER_TICK` | `10000` | Maximum rows migrated per rebalance tick. |
| `TSINK_CLUSTER_REBALANCE_MAX_SHARDS_PER_TICK` | `4` | Maximum shards processed per rebalance tick. |

### 6.7 Control plane (Raft)

The control plane uses a Raft-based consensus protocol to manage cluster membership and shard assignments.

Its on-disk consensus log uses schema v2 with a required authoritative `checkpointState` and
restart-durable `steppedDownTerm`; the separate control-state file remains a repairable schema-v1
mirror. Both files use the shared local-disk settings from section 2.2 and are charged to `Cluster`;
there is no separate control-plane quota flag. Legacy v1 logs are rewritten to v2 on open, which
creates a storage-format downgrade boundary for older binaries. A typed disk rejection is
definitive only before consensus requires the candidate; a required candidate that cannot yet be
made durable is retained behind the persistence fence until repair.

Only committed `Active` members vote or assert leadership. Activation must follow control-log
catch-up, and leadership transfer must follow committed activation; proof of newly activated
leader eligibility to a lagging voter remains incomplete Phase 2 work. Authoritative repair may
recreate or grow the mirror at the logical quota because the log already holds authority, while
the complete temporary peak still observes physical-space and filesystem-headroom limits.
An Active leader must transfer leadership before its own leave can be committed.

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_CONTROL_TICK_INTERVAL_SECS` | `2` | Consensus heartbeat interval in seconds. |
| `TSINK_CLUSTER_CONTROL_MAX_APPEND_ENTRIES` | `64` | Maximum log entries per Raft AppendEntries RPC round. |
| `TSINK_CLUSTER_CONTROL_SNAPSHOT_INTERVAL_ENTRIES` | `128` | Compact the Raft log into a snapshot every N committed entries. |
| `TSINK_CLUSTER_CONTROL_SUSPECT_TIMEOUT_SECS` | `6` | Seconds of missed heartbeats before a peer is marked suspect. |
| `TSINK_CLUSTER_CONTROL_DEAD_TIMEOUT_SECS` | `20` | Seconds after which a suspect peer is declared dead and removed from routing. |
| `TSINK_CLUSTER_CONTROL_LEADER_LEASE_SECS` | `6` | Leader lease duration in seconds. |

### 6.8 Write deduplication

Cluster writes carry idempotency keys to suppress and replay duplicate internal requests within a
bounded window. This is a retry aid, not an exactly-once guarantee: entries can expire or be
evicted, and a failure can occur after rows are applied but before the completion marker is synced.

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_DEDUPE_WINDOW_SECS` | `900` (15 min) | How long idempotency keys are retained. Retries arriving after this window may be re-applied. |
| `TSINK_CLUSTER_DEDUPE_MAX_ENTRIES` | `250000` | Maximum number of idempotency keys held in memory. Oldest entries are evicted when the limit is reached. |
