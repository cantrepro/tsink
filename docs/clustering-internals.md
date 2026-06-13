# Clustering internals

This document describes how tsink distributes data across nodes: how the hash ring is built, how writes are routed and replicated, how the control plane reaches consensus, how temporarily unreachable replicas are handled via hinted handoff, how background digest-based repair keeps replicas consistent, and how queries fan out and merge across shards.

---

## Table of contents

- [Architecture overview](#architecture-overview)
- [Consistent hash-ring sharding](#consistent-hash-ring-sharding)
- [Node membership](#node-membership)
- [Node roles](#node-roles)
- [Write path and replication](#write-path-and-replication)
  - [Consistency levels](#consistency-levels)
  - [Write routing and batching](#write-routing-and-batching)
  - [Idempotency and deduplication](#idempotency-and-deduplication)
- [Control plane consensus](#control-plane-consensus)
  - [Node lifecycle states](#node-lifecycle-states)
  - [Log replication and snapshots](#log-replication-and-snapshots)
- [Hinted handoff](#hinted-handoff)
- [Digest-based repair](#digest-based-repair)
  - [Digest exchange](#digest-exchange)
  - [Repair execution and budget](#repair-execution-and-budget)
- [Shard rebalance and handoff](#shard-rebalance-and-handoff)
- [Distributed query fan-out](#distributed-query-fan-out)
  - [Read consistency levels](#read-consistency-levels)
  - [Shard-aware query planning](#shard-aware-query-planning)
  - [Result merging and safety limits](#result-merging-and-safety-limits)
  - [Resource guardrails](#resource-guardrails)
- [Internal RPC layer](#internal-rpc-layer)
  - [Protocol versioning and capabilities](#protocol-versioning-and-capabilities)
  - [mTLS and authentication](#mtls-and-authentication)
- [Hotspot detection](#hotspot-detection)
- [Cluster snapshots](#cluster-snapshots)
- [Cluster audit log](#cluster-audit-log)
- [Environment variable reference](#environment-variable-reference)

---

## Architecture overview

A tsink cluster is a set of independent server processes that each run a full embedded storage engine. Each node is identified by a unique string `node_id` and exposes an internal HTTP endpoint for peer-to-peer RPC. There is no shared storage or external coordination service: membership, sharding, and replication are all managed in-process.

The main runtime types are:

| Type | Responsibility |
|---|---|
| `ShardRing` | Maps series to shards and shards to owner nodes |
| `MembershipView` | Tracks the current set of nodes and their endpoints |
| `WriteRouter` | Routes incoming write batches to the correct replicas |
| `ReadFanoutExecutor` | Fans read requests out to the relevant shards and merges results |
| `ControlConsensusRuntime` | Replicates the cluster control state (membership, shard assignments) via a Raft-like log |
| `HintedHandoffOutbox` | Queues writes destined for temporarily-unavailable replicas |
| `DigestExchangeRuntime` | Runs background fingerprint comparison and repairs diverged replicas |

---

## Consistent hash-ring sharding

Series are assigned to shards deterministically. The ring is built at startup and stored in `ShardRing`.

**Shard assignment:**

1. Each series is identified by a stable 64-bit hash (`stable_series_identity_hash`) of its metric name and label set.
2. The shard index is computed as:

   ```
   shard = series_id % shard_count
   ```

3. For each shard a token is computed:

   ```
   shard_token = xxh64("shard:<N>")
   ```

4. Virtual node tokens are placed on a ring for each physical node. Each node gets `virtual_nodes_per_node` (default 128) virtual nodes, each with token:

   ```
   token = xxh64("<node_id>#<vnode_index>")
   ```

5. A shard's owner list is determined by walking the ring clockwise from the shard token and collecting the first `min(replication_factor, node_count)` distinct physical nodes. This gives load distribution across nodes proportional to their virtual node count.

The ring is versioned (`hash_version`, currently `STABLE_RING_HASH_VERSION_V1 = 1`). The version is embedded in ring snapshots so that hash algorithm changes can be detected. The ring can be serialised to a `ShardRingSnapshot` and restored without rebuilding from membership.

**Defaults:**

| Parameter | Default |
|---|---|
| `shard_count` | 128 |
| `replication_factor` | 1 |
| `virtual_nodes_per_node` | 128 |

---

## Node membership

`MembershipView` is the static view of the cluster that is used to build the `ShardRing` and route RPC calls. It is constructed from the `ClusterConfig` at startup.

Each known node is represented as a `ClusterNode`:

```
ClusterNode { id: String, endpoint: String }
```

The local node is always included. The remaining nodes come from the `--cluster-peers` list (seed nodes).

**Auto-join:** `AutoJoinRuntime` runs on startup when seed nodes are configured. It periodically calls `control_auto_join` on each seed endpoint until one acknowledges the join, then stops. The probe interval defaults to 3 seconds (`TSINK_CLUSTER_AUTO_JOIN_INTERVAL_SECS`).

---

## Node roles

Each node has one of three roles:

| Role | Owns shards | Serves queries |
|---|---|---|
| `Storage` | Yes | No |
| `Query` | No | Yes |
| `Hybrid` (default) | Yes | Yes |

Storage and Hybrid nodes are included in the ring and receive data writes. Query-only nodes do not own shards; they are excluded from the ring when it is built for ownership purposes and only fan out read requests to storage nodes.

---

## Write path and replication

When a write arrives at any node, the `WriteRouter` determines which shards the rows belong to, groups them by replica owner, and either commits them locally or forwards them to the responsible nodes over RPC.

### Consistency levels

Write consistency governs how many replicas must acknowledge before the write is reported as successful. The configured level can be overridden per-request with the `x-tsink-write-consistency` header (only to a weaker level; strengthening above the node's configured mode is rejected).

| Level | Required acks |
|---|---|
| `one` | 1 |
| `quorum` (default) | ⌊RF/2⌋ + 1 |
| `all` | RF (replication factor) |

The coordinator tracks per-shard acknowledgements. If the required ack count cannot mathematically be reached given the remaining pending replicas, the write fails immediately with `InsufficientReplicas`. Slow replicas that exceed the RPC timeout contribute `ConsistencyTimeout` errors; these are retryable.

### Write routing and batching

`WriteRouter` splits a batch of rows into:

- **Local rows** — rows whose primary shard is owned by the local node.
- **Remote batches** — rows grouped by destination node. Remote batches are bounded by `TSINK_CLUSTER_WRITE_MAX_BATCH_ROWS` (default 1,024 rows) and sent concurrently with a cap of `TSINK_CLUSTER_WRITE_MAX_INFLIGHT_BATCHES` (default 32) in-flight batches.

Each remote batch carries a stable idempotency key so that exactly-once delivery can be guaranteed end-to-end through the deduplication window on the receiver.

For a replica that is currently unreachable the write is handed off to `HintedHandoffOutbox` rather than immediately failing, as long as the consistency quorum was already satisfied by successful acks.

### Idempotency and deduplication

`DedupeWindowStore` maintains a durable, append-only log of accepted idempotency keys with expiry timestamps. On each incoming internal write, the key is looked up:

- **Accepted** — key is new; the write proceeds and the key is committed on success.
- **InFlight** — a write with the same key is already in progress; the request is rejected to prevent concurrent processing of the same batch.
- **Duplicate** — key was seen within the window and already committed; the request is silently dropped and a success is returned.

Keys expire after `TSINK_CLUSTER_DEDUPE_WINDOW_SECS` (default 15 minutes). The log is compacted periodically to reclaim space.

| Parameter | Default |
|---|---|
| Window | 15 minutes |
| Max entries | 250,000 |
| Max log size | 64 MiB |
| Cleanup interval | 30 s |

---

## Control plane consensus

The cluster control plane (membership, shard ring, handoff state) is managed by `ControlConsensusRuntime`, which implements a Raft-like replicated log. Every mutation to the control state goes through a `propose` call that replicates the log entry to all peers and waits until a quorum has persisted it.

The control state tracks:
- The `ShardRingSnapshot` (current shard assignments).
- Each node's `ControlNodeStatus` (`Joining`, `Active`, `Leaving`, `Removed`).
- Active shard handoffs and their `ShardHandoffPhase`.

**Tuning parameters (all via environment variables):**

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_CONTROL_TICK_INTERVAL_SECS` | 2 | Heartbeat and liveness check interval |
| `TSINK_CLUSTER_CONTROL_MAX_APPEND_ENTRIES` | 64 | Max log entries per append RPC |
| `TSINK_CLUSTER_CONTROL_SNAPSHOT_INTERVAL_ENTRIES` | 128 | Compact log into snapshot after N entries |
| `TSINK_CLUSTER_CONTROL_SUSPECT_TIMEOUT_SECS` | 6 | Seconds without contact before peer is suspect |
| `TSINK_CLUSTER_CONTROL_DEAD_TIMEOUT_SECS` | 20 | Seconds without contact before peer is declared dead |
| `TSINK_CLUSTER_CONTROL_LEADER_LEASE_SECS` | 6 | Lease duration for the current leader |

### Node lifecycle states

```
Joining → Active → Leaving → Removed
```

- **Joining** — node has sent an auto-join request but has not yet had its membership committed by the consensus leader.
- **Active** — node is a full participant; it owns shards and its writes count toward consistency quorums.
- **Leaving** — node has requested removal; shard handoffs are initiated before the node transitions to Removed.
- **Removed** — node is no longer in the ring and will be ignored by routing.

### Log replication and snapshots

The control log is written to a JSON file on disk (`tsink-control-log`). The schema version is embedded (`CONTROL_LOG_SCHEMA_VERSION = 1`). When the number of uncommitted entries reaches `snapshot_interval_entries`, the current state is folded into a snapshot and the log is truncated, keeping only the entries that have not yet been applied by all peers.

---

## Hinted handoff

When a remote write fails for a replica that is only temporarily unavailable, `HintedHandoffOutbox` stores the rows in a durable per-peer queue so they can be replayed once the peer recovers.

The outbox maintains:
- An in-memory queue per destination node, bounded by `TSINK_CLUSTER_OUTBOX_MAX_PEER_BYTES` (default 256 MiB).
- A total in-memory cap of `TSINK_CLUSTER_OUTBOX_MAX_ENTRIES` (default 100,000) entries and `TSINK_CLUSTER_OUTBOX_MAX_BYTES` (default 512 MiB).
- A persistent append-only log capped at `TSINK_CLUSTER_OUTBOX_MAX_LOG_BYTES` (default 2 GiB).

**Replay loop:** A background task retries the backlog every `TSINK_CLUSTER_OUTBOX_REPLAY_INTERVAL_SECS` (default 2 seconds) in batches of up to `TSINK_CLUSTER_OUTBOX_REPLAY_BATCH_SIZE` (default 256) entries. Failed retries are subject to exponential backoff capped at `TSINK_CLUSTER_OUTBOX_MAX_BACKOFF_SECS` (default 30 seconds).

**Stalled peer detection:** A peer is flagged as stalled when its oldest undelivered entry is older than `TSINK_CLUSTER_OUTBOX_STALLED_PEER_AGE_SECS` (default 300 seconds) and at least `TSINK_CLUSTER_OUTBOX_STALLED_PEER_MIN_ENTRIES` entries are queued for it. Stalled alerts are counted in metrics.

**Log compaction:** The background cleanup task runs every `TSINK_CLUSTER_OUTBOX_CLEANUP_INTERVAL_SECS` (default 30 seconds) and rewrites the log file to remove delivered (stale) records when at least `TSINK_CLUSTER_OUTBOX_CLEANUP_MIN_STALE_RECORDS` (default 1,024) stale records have accumulated.

---

## Digest-based repair

Digest-based repair is the background process that detects and corrects divergence between replicas without blocking foreground I/O.

### Digest exchange

`DigestExchangeRuntime` runs periodically every `TSINK_CLUSTER_DIGEST_INTERVAL_SECS` (default 30 seconds). On each tick it selects up to `TSINK_CLUSTER_DIGEST_MAX_SHARDS_PER_TICK` (default 64) shards and, for each shard, exchanges window fingerprints with each replica peer.

A fingerprint window covers the time range `[now - window, now]` where `window = TSINK_CLUSTER_DIGEST_WINDOW_SECS` (default 300 seconds). For each shard within the window the runtime computes:
- **Series count** — number of distinct series with data in the window.
- **Point count** — total number of data points.
- **Fingerprint** — a xxhash64 digest over the canonical sorted representation of all (series_id, timestamp, value) tuples.

A `DigestMismatchReport` is recorded whenever the local fingerprint differs from a peer's fingerprint for the same shard and window. Mismatches include both fingerprints, series counts, and point counts so the operator can reason about the direction and magnitude of divergence.

The digest exchange enforces a per-tick byte budget (`TSINK_CLUSTER_DIGEST_MAX_BYTES_PER_TICK`, default 256 KiB) on the response payload transferred from peers, preventing excessive network usage during recovery.

### Repair execution and budget

When a mismatch is detected, the runtime initiates an additive repair: it fetches the rows that are present on the peer but missing locally and inserts them. Only additive repairs are performed; conflicting or extra-local data is not deleted.

Repairs are throttled by a per-tick budget:

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_REPAIR_MAX_MISMATCHES_PER_TICK` | 2 | Max mismatch reports processed per tick |
| `TSINK_CLUSTER_REPAIR_MAX_SERIES_PER_TICK` | 256 | Max series scanned per tick |
| `TSINK_CLUSTER_REPAIR_MAX_ROWS_PER_TICK` | 16,384 | Max rows inserted per tick |
| `TSINK_CLUSTER_REPAIR_MAX_RUNTIME_MS_PER_TICK` | 100 ms | Wall-clock time budget per tick |
| `TSINK_CLUSTER_REPAIR_FAILURE_BACKOFF_SECS` | 30 s | Backoff after a repair attempt fails |

Repair can be paused and cancelled at runtime via the control API. Cancellation increments the cancel generation so in-progress repair tasks can detect and abort.

If `TSINK_CLUSTER_REBALANCE_INTERVAL_SECS` (default 5 seconds) is reached during rebalance, the repair runtime also drives shard handoff progress, copying up to `TSINK_CLUSTER_REBALANCE_MAX_ROWS_PER_TICK` (default 10,000) rows and advancing up to `TSINK_CLUSTER_REBALANCE_MAX_SHARDS_PER_TICK` (default 4) shards per tick.

---

## Shard rebalance and handoff

When nodes are added or removed, shards that change ownership go through a structured handoff protocol managed by the control plane.

A `ShardHandoffProgress` record tracks each active handoff with the following phases:

```
Warmup → Cutover → FinalSync → Completed
                 ↘ Failed → Warmup (resume)
```

| Phase | Description |
|---|---|
| `Warmup` | New owner begins receiving writes and copying historical data. |
| `Cutover` | Ring is updated; new owner becomes primary. Old owner continues serving stale reads. |
| `FinalSync` | Remaining data gap is closed; old owner drains. |
| `Completed` | Handoff is done; old owner releases shard ownership. |
| `Failed` | Handoff failed; can be retried by transitioning back to `Warmup`. |

The handoff record tracks `copied_rows`, `pending_rows`, `resumed_count` (number of retries after failure), and timestamps for `started_unix_ms` / `updated_unix_ms`.

---

## Distributed query fan-out

Read requests are handled by `ReadFanoutExecutor`, which broadcasts queries to all relevant shard owners and merges the results.

### Read consistency levels

| Level | Description | Shards queried per shard |
|---|---|---|
| `eventual` (default) | Best-effort; queries any single replica | Primary only |
| `quorum` | Majority of replicas; results are deduped | All replicas |
| `strict` | All replicas; results are deduped | All replicas |

The `x-tsink-write-consistency` equivalent for reads is controlled at configuration time via `--cluster-read-consistency`.

When partial-response mode is `allow` (default), a failed shard sub-request degrades the result with a warning rather than failing the entire query. When set to `deny`, any shard failure causes the query to fail.

### Shard-aware query planning

`ShardAwareQueryPlanner` builds a `ReadExecutionPlan` for each query:

1. **Candidate shard selection** — all shards or, for time-bounded queries, a pruned subset if future shard pruning is applicable.
2. **Owner resolution** — for each candidate shard, the ring is consulted to obtain the owner list. The plan records which shards are local (served in-process) and which require remote RPC calls.
3. **Remote target grouping** — shards are grouped by destination node to minimise the number of RPC round-trips. Remote batch size for select operations is 128 shards per call.

Fanout concurrency is bounded by `TSINK_CLUSTER_FANOUT_CONCURRENCY` (default 16 concurrent shard sub-requests).

### Result merging and safety limits

Query results from multiple shards can overlap when `replication_factor > 1` and read consistency is `quorum` or `strict`. `SeriesMetadataMerger` and `SeriesPointsMerger` deduplicate series and data points by their stable identities before returning results to the client.

Hard limits are enforced during merging to prevent a single query from exhausting memory:

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_READ_MAX_MERGED_SERIES` | 250,000 | Max unique series per query |
| `TSINK_CLUSTER_READ_MAX_MERGED_POINTS_PER_SERIES` | 1,000,000 | Max points per series |
| `TSINK_CLUSTER_READ_MAX_MERGED_POINTS_TOTAL` | 5,000,000 | Max total points per query |

### Resource guardrails

`ReadResourceGuardrails` enforces cluster-wide concurrency limits using semaphores:

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_READ_MAX_INFLIGHT_QUERIES` | 64 | Max concurrent fan-out queries |
| `TSINK_CLUSTER_READ_MAX_INFLIGHT_MERGED_POINTS` | 20,000,000 | Max points across all active fan-out queries |
| `TSINK_CLUSTER_READ_RESOURCE_ACQUIRE_TIMEOUT_MS` | 25 ms | Max wait to acquire a query slot |

Queries that cannot acquire a slot within the timeout return a resource-exhaustion error.

---

## Internal RPC layer

All peer-to-peer communication goes through `RpcClient`, which makes HTTP requests to the internal API endpoint of each node. The client handles serialisation, retries, and protocol negotiation.

### Protocol versioning and capabilities

Each RPC request carries the header `x-tsink-rpc-version: 1`. Responses that indicate a temporary failure (`500`, `502`, `503`, `504`) are retried up to `TSINK_CLUSTER_RPC_MAX_RETRIES` times (default 2) with the same idempotency key.

Nodes advertise a set of capabilities in the `x-tsink-peer-capabilities` header. The current capability set includes:

| Capability | Description |
|---|---|
| `cluster_rpc_v1` | Base internal RPC protocol |
| `control_replication_v1` | Raft-style log append RPC |
| `control_snapshot_rpc_v1` | Snapshot install RPC |
| `control_state_v1` | Control state read RPC |
| `control_log_v1` | Control log read RPC |
| `control_recovery_snapshot_v1` | Recovery snapshot RPC |
| `cluster_snapshot_v1` | Cluster data snapshot RPC |
| `metadata_ingest_v1` | Metadata ingest payload support |
| `metadata_store_v1` | Metadata store read support |
| `exemplar_ingest_v1` | Exemplar data ingest |
| `exemplar_query_v1` | Exemplar query support |
| `histogram_ingest_v1` | Native histogram ingest |
| `histogram_storage_v1` | Native histogram storage |

Writes containing native histograms require `histogram_ingest_v1` and `histogram_storage_v1` on the destination node; the router skips capabilities that the target does not support.

### mTLS and authentication

Internal API endpoints are protected by two complementary mechanisms:

- **Bearer token** (`x-tsink-internal-auth` header) — a shared secret loaded from `--cluster-internal-auth-token` or a file. All peer requests must include this token.
- **mTLS** — when `--cluster-internal-mtls-enabled` is set, `RpcClient` uses a dedicated CA, certificate, and private key for peer connections. The verified peer node ID is transmitted in the `x-tsink-verified-node-id` header and cross-checked against the membership view.

TLS is implemented with `rustls` (no OpenSSL). The crypto provider is installed once per process via a `OnceLock`.

**RPC timeout:** `TSINK_CLUSTER_RPC_TIMEOUT_MS` (default 2,000 ms).

---

## Hotspot detection

`HotspotTracker` accumulates per-shard and per-tenant counters in memory:

- Ingest rows
- Query requests and shard hits
- Repair mismatches, series and point gaps, and rows inserted

`build_cluster_hotspot_snapshot` computes a ranked list of hot shards and tenants by combining these counters with current storage series counts from the local engine. Each shard receives a `pressure_score` based on its workload, a `movement_cost_score` based on pending handoff rows, and a `skew_factor` normalised against the mean across all shards. Shards whose `skew_factor` exceeds `SHARD_SKEW_THRESHOLD` (4×) are flagged with `recommend_move = true`. Tenant skew uses `TENANT_SKEW_THRESHOLD` (4×) in the same way.

The hotspot snapshot is exposed through the cluster status API and can be used to guide manual or automatic shard rebalancing decisions.

---

## Cluster snapshots

Cluster snapshots capture a consistent point-in-time backup of both the data plane and control plane. The `cluster_snapshot_v1` capability enables the `InternalDataSnapshotRequest` / `InternalDataSnapshotResponse` RPC pair. A coordinated snapshot:

1. Suspends compaction on participating nodes.
2. Flushes the write buffer.
3. Copies the current segment files and WAL to the snapshot destination.
4. Records the control-log commit index and ring snapshot at the time of the backup.

Restore replays the control log from the snapshot point and imports segment files before the engine resumes serving requests.

---

## Cluster audit log

`ClusterAuditLog` records a tamper-evident append-only log of all cluster control-plane mutations (membership changes, shard handoffs, snapshot operations, etc.). Each record contains:

- A monotonic `id`
- A UTC `timestamp_unix_ms`
- An `operation` name
- An `actor` (identity and auth scope)
- A `target` (the resource being mutated, as a JSON object)
- An `outcome` (HTTP status, result or error type)

**Defaults:**

| Variable | Default |
|---|---|
| `TSINK_CLUSTER_AUDIT_RETENTION_SECS` | 30 days |
| `TSINK_CLUSTER_AUDIT_MAX_LOG_BYTES` | 128 MiB |
| `TSINK_CLUSTER_AUDIT_MAX_QUERY_LIMIT` | 1,000 records per query |

Audit records are persisted to an append-only file and can be queried via the cluster status API filtered by operation name or actor identity.

---

## Environment variable reference

All environment variables related to clustering are listed below with their defaults.

### Write path

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_WRITE_MAX_BATCH_ROWS` | 1,024 | Max rows per remote write batch |
| `TSINK_CLUSTER_WRITE_MAX_INFLIGHT_BATCHES` | 32 | Max concurrent remote write batches |
| `TSINK_CLUSTER_RPC_TIMEOUT_MS` | 2,000 | RPC call timeout (ms) |
| `TSINK_CLUSTER_RPC_MAX_RETRIES` | 2 | RPC retry count on transient failures |

### Read path

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_FANOUT_CONCURRENCY` | 16 | Max concurrent shard sub-requests |
| `TSINK_CLUSTER_READ_MAX_MERGED_SERIES` | 250,000 | Max unique series per fan-out query |
| `TSINK_CLUSTER_READ_MAX_MERGED_POINTS_PER_SERIES` | 1,000,000 | Max data points per series |
| `TSINK_CLUSTER_READ_MAX_MERGED_POINTS_TOTAL` | 5,000,000 | Max total data points per fan-out query |
| `TSINK_CLUSTER_READ_MAX_INFLIGHT_QUERIES` | 64 | Max concurrent fan-out queries |
| `TSINK_CLUSTER_READ_MAX_INFLIGHT_MERGED_POINTS` | 20,000,000 | Max merged points across all active queries |
| `TSINK_CLUSTER_READ_RESOURCE_ACQUIRE_TIMEOUT_MS` | 25 | Timeout to acquire query slot (ms) |

### Hinted handoff

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_OUTBOX_MAX_ENTRIES` | 100,000 | Max total queued entries |
| `TSINK_CLUSTER_OUTBOX_MAX_BYTES` | 512 MiB | Max total in-memory payload |
| `TSINK_CLUSTER_OUTBOX_MAX_PEER_BYTES` | 256 MiB | Max payload per destination node |
| `TSINK_CLUSTER_OUTBOX_MAX_LOG_BYTES` | 2 GiB | Max persistent log file size |
| `TSINK_CLUSTER_OUTBOX_REPLAY_INTERVAL_SECS` | 2 | Replay loop interval (s) |
| `TSINK_CLUSTER_OUTBOX_REPLAY_BATCH_SIZE` | 256 | Entries per replay attempt |
| `TSINK_CLUSTER_OUTBOX_MAX_BACKOFF_SECS` | 30 | Max retry backoff (s) |
| `TSINK_CLUSTER_OUTBOX_MAX_RECORD_BYTES` | 2 MiB | Max size for a single outbox record |
| `TSINK_CLUSTER_OUTBOX_CLEANUP_INTERVAL_SECS` | 30 | Log compaction check interval (s) |
| `TSINK_CLUSTER_OUTBOX_CLEANUP_MIN_STALE_RECORDS` | 1,024 | Stale records before compaction runs |
| `TSINK_CLUSTER_OUTBOX_STALLED_PEER_AGE_SECS` | 300 | Age threshold for stalled peer alert (s) |
| `TSINK_CLUSTER_OUTBOX_STALLED_PEER_MIN_ENTRIES` | 1 | Min queued entries to trigger stall alert |
| `TSINK_CLUSTER_OUTBOX_STALLED_PEER_MIN_BYTES` | 1 | Min queued bytes to trigger stall alert |

### Deduplication

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_DEDUPE_WINDOW_SECS` | 900 (15 min) | Idempotency key retention window |
| `TSINK_CLUSTER_DEDUPE_MAX_ENTRIES` | 250,000 | Max tracked idempotency keys |
| `TSINK_CLUSTER_DEDUPE_MAX_LOG_BYTES` | 64 MiB | Max dedupe log file size |
| `TSINK_CLUSTER_DEDUPE_CLEANUP_INTERVAL_SECS` | 30 | Cleanup interval (s) |

### Digest repair

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_DIGEST_INTERVAL_SECS` | 30 | Digest exchange tick interval (s) |
| `TSINK_CLUSTER_DIGEST_WINDOW_SECS` | 300 | Time window evaluated per digest tick (s) |
| `TSINK_CLUSTER_DIGEST_MAX_SHARDS_PER_TICK` | 64 | Shards compared per tick |
| `TSINK_CLUSTER_DIGEST_MAX_MISMATCH_REPORTS` | 128 | Max mismatch reports retained |
| `TSINK_CLUSTER_DIGEST_MAX_BYTES_PER_TICK` | 256 KiB | Max bytes received from peers per tick |
| `TSINK_CLUSTER_REPAIR_MAX_MISMATCHES_PER_TICK` | 2 | Mismatches actioned per tick |
| `TSINK_CLUSTER_REPAIR_MAX_SERIES_PER_TICK` | 256 | Series scanned per repair tick |
| `TSINK_CLUSTER_REPAIR_MAX_ROWS_PER_TICK` | 16,384 | Rows inserted per repair tick |
| `TSINK_CLUSTER_REPAIR_MAX_RUNTIME_MS_PER_TICK` | 100 | Wall-clock budget per repair tick (ms) |
| `TSINK_CLUSTER_REPAIR_FAILURE_BACKOFF_SECS` | 30 | Backoff after failed repair (s) |
| `TSINK_CLUSTER_REBALANCE_INTERVAL_SECS` | 5 | Rebalance check interval (s) |
| `TSINK_CLUSTER_REBALANCE_MAX_ROWS_PER_TICK` | 10,000 | Rows migrated per rebalance tick |
| `TSINK_CLUSTER_REBALANCE_MAX_SHARDS_PER_TICK` | 4 | Shards advanced per rebalance tick |

### Control plane consensus

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_CONTROL_TICK_INTERVAL_SECS` | 2 | Heartbeat tick interval (s) |
| `TSINK_CLUSTER_CONTROL_MAX_APPEND_ENTRIES` | 64 | Max entries per append-entries RPC |
| `TSINK_CLUSTER_CONTROL_SNAPSHOT_INTERVAL_ENTRIES` | 128 | Log entries before snapshot compaction |
| `TSINK_CLUSTER_CONTROL_SUSPECT_TIMEOUT_SECS` | 6 | Seconds before peer is marked suspect |
| `TSINK_CLUSTER_CONTROL_DEAD_TIMEOUT_SECS` | 20 | Seconds before peer is marked dead |
| `TSINK_CLUSTER_CONTROL_LEADER_LEASE_SECS` | 6 | Leader lease duration (s) |

### Membership

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_AUTO_JOIN_INTERVAL_SECS` | 3 | Auto-join probe interval (s) |

### Audit log

| Variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_AUDIT_RETENTION_SECS` | 2,592,000 (30 days) | Audit record retention |
| `TSINK_CLUSTER_AUDIT_MAX_LOG_BYTES` | 128 MiB | Max audit log size |
| `TSINK_CLUSTER_AUDIT_MAX_QUERY_LIMIT` | 1,000 | Max records returned per audit query |
