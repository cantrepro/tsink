# Configuration reference

Complete listing of every configuration knob in tsink — the embedded library API, the server CLI flags, and the environment variables that tune ingestion, clustering, and background workers.

Sections are ordered from most commonly used to most advanced.

---

## Contents

1. [Embedded library — `StorageBuilder`](#1-embedded-library--storagebuilder)
2. [Server CLI flags](#2-server-cli-flags)
   - [Networking & listeners](#21-networking--listeners)
   - [Storage & WAL](#22-storage--wal)
   - [Memory & cardinality](#23-memory--cardinality)
   - [Security & auth](#24-security--auth)
   - [Cluster](#25-cluster)
   - [Edge sync](#26-edge-sync)
3. [Environment variables — server admission](#3-environment-variables--server-admission)
4. [Environment variables — ingestion protocols](#4-environment-variables--ingestion-protocols)
5. [Environment variables — rules engine](#5-environment-variables--rules-engine)
6. [Environment variables — cluster](#6-environment-variables--cluster)
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

These are the options exposed through the `StorageBuilder` Rust API and the equivalent `TsinkStorageBuilder` Python bindings. See the [embedded library guide](embedded-library.md) and [Python bindings guide](python-bindings.md) for usage examples.

### Storage & persistence

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_data_path(path)` | `PathBuf` | *(none)* | Root directory for all on-disk data (WAL, segments, metadata). Required for durable storage. |
| `with_object_store_path(path)` | `PathBuf` | *(none)* | Root directory for tiered segment lanes (`hot/`, `warm/`, `cold/`). Required for tiered storage. |
| `with_runtime_mode(mode)` | `StorageRuntimeMode` | `ReadWrite` | `ReadWrite` — full local instance. `ComputeOnly` — query node that reads from object store without persisting locally. |
| `with_timestamp_precision(p)` | `TimestampPrecision` | `Nanoseconds` | Interpretation of raw integer timestamps: `Seconds`, `Milliseconds`, `Microseconds`, or `Nanoseconds`. Must match the precision of all ingested data. |

### Retention & tiering

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_retention(duration)` | `Duration` | `14 days` | How long data is retained. Writes outside this window are rejected when `retention_enforced` is set (which `with_retention` enables automatically). |
| `with_hot_tier_retention(duration)` | `Duration` | *(falls back to `retention`)* | Age at which data moves from local (hot) to object-store warm tier. |
| `with_warm_tier_retention(duration)` | `Duration` | *(falls back to `retention`)* | Age at which data moves from warm to cold tier. |
| `with_mirror_hot_segments_to_object_store(bool)` | `bool` | `false` | Copy freshly-persisted hot segments into `<object_store_path>/hot/` in addition to writing locally. Useful for cross-node availability. |
| `with_remote_segment_cache_policy(policy)` | `RemoteSegmentCachePolicy` | `MetadataOnly` | What to hold in memory for remote (object-store) segments: `MetadataOnly` or `Full`. |
| `with_remote_segment_refresh_interval(duration)` | `Duration` | `5s` | How often a `ComputeOnly` node refreshes its view of remote segment metadata. |

### Chunk & partition tuning

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_chunk_points(n)` | `usize` | `2048` | Target number of data points per chunk before the chunk is sealed. Clamped to `1..=65535`. Larger values improve compression; smaller values reduce read amplification on recent data. |
| `with_partition_duration(duration)` | `Duration` | `1 hour` | Time window covered by a single partition. All series data within this window is co-located. |
| `with_max_active_partition_heads_per_series(n)` | `usize` | `8` | Maximum number of simultaneously open partition heads per series. When the limit is reached the oldest head is sealed and compacted. |

### Write pipeline

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_max_writers(n)` | `usize` | CPU-count (cgroup-aware) | Size of the writer thread pool. Higher values increase throughput under concurrent write load at the cost of memory. |
| `with_write_timeout(duration)` | `Duration` | `30s` | Maximum time a write call will wait for a writer slot before returning a backpressure error. |

### Memory & cardinality

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_memory_limit_bytes(n)` | `usize` | `usize::MAX` (unlimited) | Global byte budget for all in-memory chunks (active + sealed). New writes are back-pressured when the budget is exhausted. |
| `with_cardinality_limit(n)` | `usize` | `usize::MAX` (unlimited) | Hard cap on the total number of unique series. Writes that would create a new series beyond this limit are rejected with a cardinality error. |

### WAL

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_wal_enabled(bool)` | `bool` | `true` | Enable or disable the write-ahead log. Disabling removes crash-safety guarantees. |
| `with_wal_size_limit_bytes(n)` | `usize` | `usize::MAX` (unlimited) | Maximum total on-disk size for WAL files. Oldest segments are pruned when the limit is reached. |
| `with_wal_buffer_size(n)` | `usize` | `4096` | I/O buffer size for WAL writes. Larger buffers reduce syscall overhead on high-throughput workloads. |
| `with_wal_sync_mode(mode)` | `WalSyncMode` | `PerAppend` | `PerAppend` — `fsync` after every write (crash-safe, higher latency). `Periodic(duration)` — flush without fsync on a fixed interval (higher throughput, potential data loss on crash). |
| `with_wal_replay_mode(mode)` | `WalReplayMode` | `Strict` | `Strict` — abort recovery on any corrupted WAL frame. `Salvage` — skip corrupted frames and recover as much data as possible. |

### Background workers

| Builder method | Type | Default | Description |
|---|---|---|---|
| `with_background_fail_fast(bool)` | `bool` | `true` | When `true`, a failure in any background worker (flush, compaction, remote segment refresh) immediately fences all further writes with an error. When `false`, the error is logged but writes continue. |

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
| `--data-path <PATH>` | *(none)* | Persist data under PATH. Without this flag, storage is purely in-memory. |
| `--object-store-path <PATH>` | *(none)* | Object-store root for tiered segment lanes (`hot/`, `warm/`, `cold/`). |
| `--timestamp-precision <PRECISION>` | `ms` | Units for raw timestamps: `s`, `ms`, `us`, `ns`. |
| `--retention <DURATION>` | `14d` | Data retention window (e.g. `7d`, `24h`, `90d`). |
| `--hot-tier-retention <DURATION>` | *(same as `--retention`)* | Age at which local segments move to the warm object-store tier. |
| `--warm-tier-retention <DURATION>` | *(same as `--retention`)* | Age at which warm segments move to the cold object-store tier. |
| `--storage-mode <MODE>` | `read-write` | `read-write` — normal full node. `compute-only` — query-only node backed by object store. |
| `--remote-segment-refresh-interval <DURATION>` | `5s` | Metadata refresh interval for `compute-only` nodes. |
| `--mirror-hot-segments-to-object-store` | `false` | Copy hot segments to object store as they are sealed. |
| `--wal-enabled <BOOL>` | `true` | Enable (`true`) or disable (`false`) the WAL. |
| `--wal-sync-mode <MODE>` | `per-append` | `per-append` (crash-safe) or `periodic` (higher throughput). |
| `--chunk-points <N>` | `2048` | Target data points per chunk (1–65535). |

### 2.3 Memory & cardinality

| Flag | Default | Description |
|---|---|---|
| `--memory-limit <BYTES>` | *(unlimited)* | Global in-memory chunk budget, in bytes. Supports suffixes such as `1G`, `512M`. |
| `--cardinality-limit <N>` | *(unlimited)* | Maximum number of unique series. New series are rejected once the limit is reached. |
| `--max-writers <N>` | *(CPU count)* | Concurrent writer threads. |

### 2.4 Security & auth

| Flag | Default | Description |
|---|---|---|
| `--tls-cert <PATH>` | *(none)* | PEM-encoded TLS certificate. Both `--tls-cert` and `--tls-key` must be set to enable TLS. |
| `--tls-key <PATH>` | *(none)* | PEM-encoded TLS private key. |
| `--auth-token <TOKEN>` | *(none)* | Static bearer token required on all non-admin requests. |
| `--auth-token-file <PATH>` | *(none)* | File or exec-based token manifest (JSON). Takes precedence over `--auth-token`. |
| `--admin-auth-token <TOKEN>` | *(none)* | Static bearer token required on `/api/v1/admin/*` endpoints. |
| `--admin-auth-token-file <PATH>` | *(none)* | File or exec-based admin token manifest. Takes precedence over `--admin-auth-token`. |
| `--tenant-config <PATH>` | *(none)* | JSON file defining per-tenant auth, quotas, and policies. See [Multi-tenancy](multi-tenancy.md). |
| `--rbac-config <PATH>` | *(none)* | JSON file defining RBAC roles, service accounts, and OIDC settings. See [Security model](security.md). |
| `--enable-admin-api` | `false` | Expose admin snapshot, restore, and cluster management endpoints. |
| `--admin-path-prefix <PATH>` | *(none)* | Restrict admin file I/O operations to this directory prefix. |

### 2.5 Cluster

These flags are only relevant when `--cluster-enabled` is set. See [Cluster setup](cluster-setup.md) and [Clustering internals](clustering-internals.md) for deployment guidance.

| Flag | Default | Description |
|---|---|---|
| `--cluster-enabled` | `false` | Enable cluster mode. |
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
| `--cluster-internal-mtls-enabled` | `false` | Enable mTLS for all internal peer-to-peer RPC. |
| `--cluster-internal-mtls-ca-cert <PATH>` | *(none)* | PEM CA bundle for internal mTLS. |
| `--cluster-internal-mtls-cert <PATH>` | *(none)* | PEM client certificate for internal mTLS. |
| `--cluster-internal-mtls-key <PATH>` | *(none)* | PEM client key for internal mTLS. |

### 2.6 Edge sync

Replays locally-written data to an upstream tsink instance. Useful for edge deployments or write aggregation.

| Flag | Default | Description |
|---|---|---|
| `--edge-sync-upstream <HOST:PORT>` | *(disabled)* | Upstream server to replay writes to. Omit to disable edge sync. |
| `--edge-sync-auth-token <TOKEN>` | *(none)* | Bearer token used when writing to the upstream server. |
| `--edge-sync-source-id <ID>` | *(none)* | Stable identifier for this edge node, used to generate idempotency keys. |
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
| `TSINK_REMOTE_WRITE_METADATA_ENABLED` | `true` | Accept metric metadata in Prometheus remote-write requests (capped at 512 metadata entries per request). Set to `false` to ignore all metadata. |
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
| `TSINK_RULES_MAX_RECORDING_ROWS_PER_EVAL` | `10000` | Maximum rows written by a single recording rule evaluation. Evaluations that would exceed this produce a partial result and log a warning. |
| `TSINK_RULES_MAX_ALERT_INSTANCES_PER_RULE` | `10000` | Maximum number of alert instances tracked per alerting rule. |

---

## 6. Environment variables — cluster

These variables control every aspect of cluster internals. They are all read once at startup unless otherwise noted.

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

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_OUTBOX_MAX_ENTRIES` | `100000` | Maximum queued entries across all unreachable peers combined. |
| `TSINK_CLUSTER_OUTBOX_MAX_BYTES` | `536870912` (512 MiB) | Total in-memory size cap for the outbox. |
| `TSINK_CLUSTER_OUTBOX_MAX_PEER_BYTES` | `268435456` (256 MiB) | Per-peer in-memory size cap for the outbox. |
| `TSINK_CLUSTER_OUTBOX_MAX_LOG_BYTES` | `2147483648` (2 GiB) | Maximum on-disk WAL size for the outbox log. |
| `TSINK_CLUSTER_OUTBOX_MAX_RECORD_BYTES` | `2097152` (2 MiB) | Maximum size of a single outbox record. |
| `TSINK_CLUSTER_OUTBOX_REPLAY_INTERVAL_SECS` | `2` | Interval in seconds between replay attempts for queued entries. |
| `TSINK_CLUSTER_OUTBOX_REPLAY_BATCH_SIZE` | `256` | Rows per replay batch sent to a recovering replica. |
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

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_CONTROL_TICK_INTERVAL_SECS` | `2` | Consensus heartbeat interval in seconds. |
| `TSINK_CLUSTER_CONTROL_MAX_APPEND_ENTRIES` | `64` | Maximum log entries per Raft AppendEntries RPC round. |
| `TSINK_CLUSTER_CONTROL_SNAPSHOT_INTERVAL_ENTRIES` | `128` | Compact the Raft log into a snapshot every N committed entries. |
| `TSINK_CLUSTER_CONTROL_SUSPECT_TIMEOUT_SECS` | `6` | Seconds of missed heartbeats before a peer is marked suspect. |
| `TSINK_CLUSTER_CONTROL_DEAD_TIMEOUT_SECS` | `20` | Seconds after which a suspect peer is declared dead and removed from routing. |
| `TSINK_CLUSTER_CONTROL_LEADER_LEASE_SECS` | `6` | Leader lease duration in seconds. |

### 6.8 Write deduplication

Cluster writes carry idempotency keys so that retried requests from the client or from hinted handoff replay are not applied twice.

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_DEDUPE_WINDOW_SECS` | `900` (15 min) | How long idempotency keys are retained. Retries arriving after this window may be re-applied. |
| `TSINK_CLUSTER_DEDUPE_MAX_ENTRIES` | `250000` | Maximum number of idempotency keys held in memory. Oldest entries are evicted when the limit is reached. |
