# Cluster Setup

> [!WARNING]
> **Experimental, non-primary capability.** Cluster mode is not tsink's primary product. This guide documents an advanced subsystem and is not a production-readiness claim. Validate correctness, failure handling, and data safety for your workload before relying on it.

This guide covers running tsink as a replicated multi-node cluster. The same engine binary that runs single-node also runs in cluster mode — clustering is enabled with a flag.

---

## Overview

A tsink cluster is a group of nodes that share data via consistent-hash-ring sharding and configurable replication. The cluster handles:

- **Shard routing** — series are hashed to a logical shard, and each shard is owned by one or more replica nodes.
- **Replication** — writes fan out to all replica owners; consistency guarantees are tunable.
- **Hinted handoff** — writes destined for a temporarily unavailable peer are placed in a bounded,
  on-disk queue and replayed when it recovers.
- **Anti-entropy repair** — periodic digest exchange detects and repairs diverged shards.
- **Online rebalance** — shard ownership migrates automatically when nodes join or leave.
- **Control-plane consensus** — membership and ring state are managed by an internal Raft-like log replicated across all storage-capable nodes.

---

## Quick start: three-node cluster

Start three nodes, each binding to its own data path and internal RPC endpoint:

```bash
# Node 1
tsink-server \
  --listen 0.0.0.0:9201 \
  --data-path ./var/node1 \
  --cluster-enabled \
  --cluster-node-id node-1 \
  --cluster-bind 0.0.0.0:9211 \
  --cluster-seeds node-2:9212,node-3:9213 \
  --cluster-replication-factor 3

# Node 2
tsink-server \
  --listen 0.0.0.0:9202 \
  --data-path ./var/node2 \
  --cluster-enabled \
  --cluster-node-id node-2 \
  --cluster-bind 0.0.0.0:9212 \
  --cluster-seeds node-1:9211,node-3:9213 \
  --cluster-replication-factor 3

# Node 3
tsink-server \
  --listen 0.0.0.0:9203 \
  --data-path ./var/node3 \
  --cluster-enabled \
  --cluster-node-id node-3 \
  --cluster-bind 0.0.0.0:9213 \
  --cluster-seeds node-1:9211,node-2:9212 \
  --cluster-replication-factor 3
```

Each node bootstraps by contacting the seed list until the control plane accepts its join. Once all three nodes are active, shard ownership is distributed across them.

`--data-path` is mandatory in cluster mode, including for `query`-role nodes. Give every process a
stable, node-specific directory on persistent local storage. Startup rejects
`--cluster-enabled` without it; there is no temporary-root fallback for control, audit, dedupe, or
handoff files.

---

## CLI flags reference

All cluster flags are only meaningful when `--cluster-enabled` is set.

| Flag | Default | Description |
|---|---|---|
| `--cluster-enabled` | `false` | Enable cluster mode. Requires `--data-path`. |
| `--cluster-node-id <ID>` | — | **Required.** Stable, unique identifier for this node (e.g. `node-1`). Must not be `"unknown"`. |
| `--cluster-bind <HOST:PORT>` | — | **Required.** Internal RPC listen/advertise address. Peers connect here. |
| `--cluster-node-role <ROLE>` | `hybrid` | Node role: `storage`, `query`, or `hybrid`. See [node roles](#node-roles). |
| `--cluster-seeds <HOST:PORT,...>` | — | Comma-separated list of peer endpoints used for initial join. Format: `host:port` or `node-id@host:port`. |
| `--cluster-shards <N>` | `128` | Number of logical shards. Must be > 0. Set this once at cluster creation and never change it. |
| `--cluster-replication-factor <N>` | `1` | Number of replicas per shard. Must be > 0 and ≤ number of storage-capable nodes. |
| `--cluster-write-consistency <MODE>` | `quorum` | Write consistency level: `one`, `quorum`, or `all`. |
| `--cluster-read-consistency <MODE>` | `eventual` | Read consistency level: `eventual`, `quorum`, or `strict`. |
| `--cluster-read-partial-response <MODE>` | `allow` | Whether to allow partial results when some shards are unavailable: `allow` or `deny`. |
| `--cluster-internal-auth-token <TOKEN>` | — | Shared-secret token sent on all internal RPC calls. Mutually exclusive with `--cluster-internal-auth-token-file`. |
| `--cluster-internal-auth-token-file <PATH>` | — | Path to a file containing the shared-secret token. Mutually exclusive with `--cluster-internal-auth-token`. |
| `--cluster-internal-mtls-enabled` | `false` | Enable mTLS for internal RPC. When set, token auth is disabled. Requires the three flags below. |
| `--cluster-internal-mtls-ca-cert <PATH>` | — | PEM CA bundle used to verify peer certificates. |
| `--cluster-internal-mtls-cert <PATH>` | — | PEM client certificate presented on outbound RPC. |
| `--cluster-internal-mtls-key <PATH>` | — | PEM private key for `--cluster-internal-mtls-cert`. |

---

## Node roles

Each node has a role that determines whether it owns data shards and whether it participates in query fanout.

| Role | Owns shards | Handles queries |
|---|---|---|
| `hybrid` (default) | Yes | Yes |
| `storage` | Yes | Via RPC from query nodes |
| `query` | No | Yes (routes to storage/hybrid) |

**`hybrid`** is appropriate for homogeneous clusters where every node is equal. **`storage` + `query`** separation is useful when you want dedicated query nodes with more memory for merging results, while storage nodes focus on ingest and retention.

A `query`-role node requires at least one `storage` or `hybrid` node in its seed list:

```bash
tsink-server \
  --cluster-node-role query \
  --cluster-seeds storage-1:9211,storage-2:9212 \
  ...
```

---

## Sharding

Series are mapped to shards, and shards are mapped to replica owners, using a consistent hash ring:

1. **Series → shard**: `shard_index = stable_series_identity_hash(metric, sorted_labels) % shard_count`
2. **Shard → owners**: virtual-node consistent hash ring (xxHash64, seed 0). Each physical node gets 128 virtual nodes on the ring. The `replication_factor` clockwise-unique physical nodes are the shard's replica set.

The `--cluster-shards` value determines the total number of logical shards and **cannot be changed after the cluster is created**. The default of `128` is sufficient for most deployments. Use a higher value (e.g. `512` or `1024`) if you plan to grow to many nodes and want fine-grained shard migration.

---

## Consistency levels

### Write consistency

Controls how many replica acks are required before a write is confirmed.

| Level | Acks required | Formula |
|---|---|---|
| `one` | 1 | — |
| `quorum` (default) | majority | `⌊replication_factor / 2⌋ + 1` |
| `all` | all replicas | `replication_factor` |

The write consistency can be overridden per-request using the HTTP header:

```
x-tsink-write-consistency: one
```

Per-request overrides may only **weaken** (not strengthen) the server-configured level.

### Read consistency

Controls how many replica responses are required and whether all replicas are queried.

| Level | Replicas queried | Acks required |
|---|---|---|
| `eventual` (default) | Primary only | 1 |
| `quorum` | All replicas | `⌊replicas / 2⌋ + 1` |
| `strict` | All replicas | All replicas |

### Partial response

When `--cluster-read-partial-response allow` (default), a query can succeed and return data even if
some shards are temporarily unavailable. The JSON response includes
`"partialResponse": {"enabled": true, ...}` plus warnings, and the HTTP headers report the same
partial state.

Set `--cluster-read-partial-response deny` to return an error instead when any shard is unavailable.

---

## Hinted handoff

When a write cannot be delivered to a replica because the peer is unreachable, tsink places the
data in a bounded, per-peer **hinted handoff outbox** backed by an append-only log. When the peer
recovers, the queued data is replayed automatically. The current outbox flushes records for
process-restart recovery and calls `sync_data` before publishing Put or Ack state. Compaction syncs
its replacement file before rename and then synchronizes the parent directory. With a shared local
disk budget, Put records reserve `Cluster` growth, while an Ack append using Recovery admission can
trigger an immediate cleanup attempt at the logical quota. A failed post-record compaction remains
cleanup debt for retry without changing the durable Ack or reschedule outcome. The platform limits
in the [durability contract](durability.md) still apply.

Handoff behavior is tunable via environment variables:

| Environment variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_OUTBOX_MAX_ENTRIES` | `100000` | Maximum number of queued entries across all peers. |
| `TSINK_CLUSTER_OUTBOX_MAX_BYTES` | `536870912` (512 MiB) | Total size cap for the handoff queue. |
| `TSINK_CLUSTER_OUTBOX_MAX_PEER_BYTES` | `268435456` (256 MiB) | Per-peer size cap. |
| `TSINK_CLUSTER_OUTBOX_MAX_LOG_BYTES` | `2147483648` (2 GiB) | Maximum WAL size for the outbox log. |
| `TSINK_CLUSTER_OUTBOX_REPLAY_INTERVAL_SECS` | `2` | How often to attempt replay to recovered peers (seconds). |
| `TSINK_CLUSTER_OUTBOX_REPLAY_BATCH_SIZE` | `256` | Maximum queued outbox entries considered per replay pass. |
| `TSINK_CLUSTER_OUTBOX_MAX_BACKOFF_SECS` | `30` | Maximum backoff between replay attempts. |
| `TSINK_CLUSTER_OUTBOX_MAX_RECORD_BYTES` | `2097152` (2 MiB) | Maximum size of a single outbox record. |
| `TSINK_CLUSTER_OUTBOX_CLEANUP_INTERVAL_SECS` | `30` | How often stale outbox records are cleaned up. |
| `TSINK_CLUSTER_OUTBOX_CLEANUP_MIN_STALE_RECORDS` | `1024` | Minimum stale records before cleanup runs. |
| `TSINK_CLUSTER_OUTBOX_STALLED_PEER_AGE_SECS` | `300` | Age threshold (seconds) at which a peer's outbox queue is considered stalled. |
| `TSINK_CLUSTER_OUTBOX_STALLED_PEER_MIN_ENTRIES` | `1` | Minimum entries for a peer queue to be considered stalled. |
| `TSINK_CLUSTER_OUTBOX_STALLED_PEER_MIN_BYTES` | `1` | Minimum bytes for a peer queue to be considered stalled. |

---

## Anti-entropy repair

Repair uses a **digest exchange**: each node periodically computes per-shard fingerprints and compares them with peers. Diverged shards trigger a targeted data backfill.

| Environment variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_DIGEST_INTERVAL_SECS` | `30` | How often digest exchange runs per node. |
| `TSINK_CLUSTER_DIGEST_WINDOW_SECS` | `300` | Lookback window included in each digest (seconds). |
| `TSINK_CLUSTER_DIGEST_MAX_SHARDS_PER_TICK` | `64` | Max shards compared per tick. |
| `TSINK_CLUSTER_DIGEST_MAX_MISMATCH_REPORTS` | `128` | Max mismatch records tracked in memory. |
| `TSINK_CLUSTER_DIGEST_MAX_BYTES_PER_TICK` | `262144` (256 KiB) | Max digest payload size per tick. |
| `TSINK_CLUSTER_REPAIR_MAX_MISMATCHES_PER_TICK` | `2` | Max diverged shards repaired per tick. |
| `TSINK_CLUSTER_REPAIR_MAX_SERIES_PER_TICK` | `256` | Max series included in a repair round. |
| `TSINK_CLUSTER_REPAIR_MAX_ROWS_PER_TICK` | `16384` | Max rows transferred per repair tick. |
| `TSINK_CLUSTER_REPAIR_MAX_RUNTIME_MS_PER_TICK` | `100` | Max wall time per repair tick (ms). |
| `TSINK_CLUSTER_REPAIR_FAILURE_BACKOFF_SECS` | `30` | Backoff after a failed repair attempt. |

Repair can be paused, resumed, cancelled, or triggered on-demand via the admin API:

```bash
# Pause repair
curl -X POST http://node-1:9201/api/v1/admin/cluster/repair/pause

# Resume repair
curl -X POST http://node-1:9201/api/v1/admin/cluster/repair/resume

# Run repair immediately
curl -X POST http://node-1:9201/api/v1/admin/cluster/repair/run

# Check repair status
curl http://node-1:9201/api/v1/admin/cluster/repair/status
```

---

## Rebalance

When shard ownership changes (node joining, leaving, or ring update), data migrates to new owners. Rebalance runs automatically in the background and is rate-limited to avoid impacting foreground traffic.

| Environment variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_REBALANCE_INTERVAL_SECS` | `5` | How often the rebalance loop ticks. |
| `TSINK_CLUSTER_REBALANCE_MAX_ROWS_PER_TICK` | `10000` | Max rows migrated per rebalance tick. |
| `TSINK_CLUSTER_REBALANCE_MAX_SHARDS_PER_TICK` | `4` | Max shards processed per rebalance tick. |

Rebalance management endpoints:

```bash
# Pause rebalance
curl -X POST http://node-1:9201/api/v1/admin/cluster/rebalance/pause

# Resume rebalance
curl -X POST http://node-1:9201/api/v1/admin/cluster/rebalance/resume

# Run rebalance immediately
curl -X POST http://node-1:9201/api/v1/admin/cluster/rebalance/run

# Check rebalance status
curl http://node-1:9201/api/v1/admin/cluster/rebalance/status
```

---

## Adding and removing nodes

### Adding a node

1. Start the new node with `--cluster-enabled`, a unique `--cluster-node-id`, and `--cluster-seeds` pointing at existing nodes.
2. The new node contacts the seed list and sends a `join` request to the control plane.
3. Once accepted, the control plane updates the ring and rebalance begins migrating shards to the new node.

You can also trigger the join manually:

```bash
curl -X POST http://new-node:9201/api/v1/admin/cluster/join
```

### Removing a node

Signal the node to drain its shards before stopping:

```bash
curl -X POST http://node-to-remove:9201/api/v1/admin/cluster/leave
```

This initiates a graceful handoff: the node transfers its shard data to the remaining owners before marking itself as removed. Check handoff status:

```bash
curl http://node-to-remove:9201/api/v1/admin/cluster/handoff/status
```

Once the handoff is complete, the node can be stopped safely.

### Shard handoff phases

When a shard is migrated, it goes through the following phases:

| Phase | Description |
|---|---|
| `Warmup` | New owner receives data; old owner remains primary. |
| `Cutover` | Traffic switches to the new owner. |
| `FinalSync` | Final data sync before the old owner relinquishes the shard. |
| `Completed` | Migration done. |
| `Failed` | An error occurred; the handoff can be resumed from `Warmup`. |

---

## Control-plane consensus

Cluster membership and ring state are managed by an internal Raft-like consensus log replicated across all storage-capable nodes. The leader coordinates ring updates, join/leave decisions, and snapshots.

| Environment variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_CONTROL_TICK_INTERVAL_SECS` | `2` | Consensus tick interval. |
| `TSINK_CLUSTER_CONTROL_MAX_APPEND_ENTRIES` | `64` | Max log entries per append round. |
| `TSINK_CLUSTER_CONTROL_SNAPSHOT_INTERVAL_ENTRIES` | `128` | Log entries between automatic snapshots. |
| `TSINK_CLUSTER_CONTROL_SUSPECT_TIMEOUT_SECS` | `6` | Seconds before a silent peer is marked suspect. |
| `TSINK_CLUSTER_CONTROL_DEAD_TIMEOUT_SECS` | `20` | Seconds before a suspect peer is declared dead. |
| `TSINK_CLUSTER_CONTROL_LEADER_LEASE_SECS` | `6` | Leader lease duration. |

Peer liveness transitions: `unknown → healthy → suspect → dead`.

Only nodes with committed membership status `Active` vote in the control quorum or may assert
control leadership. Joining and Leaving peers can still receive replication while a membership
change converges, but their responses do not count toward quorum and they cannot raise a receiver's
term. If the recorded leader is no longer Active, it is ineligible and deterministic failover
selects from the remaining Active voters.

This strict receiver-local eligibility gate requires activation and leadership transfer not to
outrun control-log catch-up: activate a Joining node only after it has caught up, and transfer
leadership only after activation commits. A membership-certificate or joint-configuration proof
for accepting a newly activated leader on a lagging voter remains Phase 2 work.

An Active leader cannot leave itself directly. Move leadership to another Active voter first, then
submit the former leader's leave through the new leader. This is a crash-safe near-term rule that
keeps an eligible leader available to finish commit propagation.

### Control persistence health

The consensus log and control-state mirror share the server local-disk budget when `--data-path`
is configured. A checkpoint stages both replacements in the `Cluster` category and publishes the
schema-v2 log, including its authoritative `checkpointState`, before the mirror. A quota or
filesystem-headroom rejection before consensus requires the candidate returns
`413 write_disk_quota_exceeded` without publishing it. If quorum or a leader commit already makes
the candidate required, failure before durable log publication is instead fenced HTTP 503
`control_persistence_indeterminate`; status shows the detail while `pendingCheckpoint` remains
null. Both failures set `X-Tsink-Write-Error-Code` and internal control responses mark them
non-retryable.

If the log replacement and its parent-directory sync succeed but the mirror cannot be published,
membership and handoff mutations return HTTP 200 with
`result: "committed_checkpoint_pending"`, `degraded: true`, and the committed log index and term.
The mutation is committed, not safe to retry as though it failed. The node fences
later control mutations until it can repair the mirror from the log. Check
`data.cluster.control.persistence` in `/api/v1/status/tsdb` and
`tsink_cluster_control_persistence_health` in `/metrics`; resolve disk quota or free-space pressure
instead of deleting either control file. Startup uses a valid v2 log to attempt repair of a stale,
missing, or invalid mirror; if repair fails, the runtime opens fenced with its checkpoint pending. A
legacy v1 log still requires a valid mirror because it has no embedded checkpoint.

Authoritative mirror repair may recreate a missing mirror or grow a stale mirror even at the
logical quota: the schema-v2 log already proves that logical state. The repair still reserves its
complete temporary peak against physical free space and filesystem headroom.

If both files are durable but replacement finalization, owned-temp cleanup, or disk-accounting
reconciliation fails, a membership or handoff mutation returns HTTP 200 with
`result: "committed_cleanup_pending"` and `degraded: true`. Status reports `cleanupDebt` without
setting a persistence fence solely for that condition. The next repair attempt retries cleanup and
reconciliation before any separate fence repair; the committed mutation must not be retried as a
rejection.

If a command is already quorum-committed but a commit-notice response reveals a higher term that
cannot yet be persisted, membership and handoff surfaces return HTTP 200 with
`result: "committed_persistence_pending"`, the committed index and term, and `degraded: true`.
Internal auto-join uses `accepted_persistence_pending`. The node adopts the higher term in memory,
fences leadership, and retries durable term publication. The command is committed and must not be
retried as a rejection.

If leader establishment or repair encounters an existing fence before the requested membership,
handoff, DR, or auto-join mutation runs, that request returns
`503 control_persistence_indeterminate`; the degraded HTTP 200 applies only when the requested
mutation itself committed and then left its mirror checkpoint, post-publication cleanup, or
higher-term persistence pending.

Control-log schema v1 is read and upgraded to schema v2 on first open. This is a downgrade
boundary: a v1-only binary rejects the rewritten log. Preserve a compatible pre-upgrade copy of
the data directory before deployment if binary rollback may be required; there is no automatic
in-place downgrade. Schema v2 also persists `steppedDownTerm`, so a higher observed term continues
to revoke local leadership after restart.

### Snapshots

Snapshot and restore the control-plane state:

```bash
# Snapshot this node's control plane
curl -X POST http://node-1:9201/api/v1/admin/cluster/control/snapshot

# Cluster-wide coordinated snapshot
curl -X POST http://node-1:9201/api/v1/admin/cluster/snapshot

# Restore from a cluster-wide snapshot
curl -X POST http://node-1:9201/api/v1/admin/cluster/restore
```

A control restore whose log replacement and parent-directory sync succeed but whose state mirror is
still pending returns success with `checkpointPending: true`, `degraded: true`, and
`checkpointDetail`. A cluster-wide restore reports the analogous `controlCheckpointPending`,
`degraded`, and `controlCheckpointDetail` fields. If the pair is durable and only finalization or
cleanup remains, control restore instead reports `cleanupDebt: true` and `cleanupDetail`; cluster
restore uses `controlCleanupDebt` and `controlCleanupDetail`.

Creating either a control recovery snapshot or a cluster snapshot returns HTTP 503
`control_persistence_indeterminate` while control authority is fenced, a durable candidate is
pending, or a mirror checkpoint needs repair. Cleanup-only debt does not block export because the
durable log and mirror already agree on the authoritative checkpoint.

Recovery snapshots carry `steppedDownTerm`; bundles written before that field existed default it to
zero, and normal restore merges the live and restored revocation floors. Set
`forceLocalLeader: true` only for an intentional recovery that must clear that floor, advance the
term, and assign leadership to the local node.

---

## Internal security

All inter-node communication runs on the internal RPC port (`--cluster-bind`) under `/internal/v1/*` endpoints. Two mutually-exclusive authentication mechanisms are available.

### Shared-secret token (simpler)

Provide a token at startup. All nodes in the cluster must use the same token.

```bash
tsink-server \
  --cluster-internal-auth-token "s3cr3t-tok3n" \
  ...
```

Or load it from a file (useful for secret management):

```bash
tsink-server \
  --cluster-internal-auth-token-file /run/secrets/cluster-token \
  ...
```

The token is sent on every internal RPC call via the `x-tsink-internal-auth` header. Tokens can be rotated at runtime without restart — see the [secret rotation guide](secret-rotation.md).

### mTLS (recommended)

Enable mTLS to authenticate and encrypt all peer-to-peer traffic using certificates. When mTLS is enabled, shared-secret token auth is disabled.

All three paths are required when `--cluster-internal-mtls-enabled` is set:

```bash
tsink-server \
  --cluster-internal-mtls-enabled \
  --cluster-internal-mtls-ca-cert /etc/tsink/ca.pem \
  --cluster-internal-mtls-cert    /etc/tsink/node.crt \
  --cluster-internal-mtls-key     /etc/tsink/node.key \
  ...
```

| Flag | Description |
|---|---|
| `--cluster-internal-mtls-ca-cert` | PEM CA bundle. Used to verify incoming peer connections and outbound TLS. |
| `--cluster-internal-mtls-cert` | PEM certificate presented by this node on all outbound RPC calls. |
| `--cluster-internal-mtls-key` | PEM private key for the certificate above. |

mTLS certificates can be rotated at runtime without restart. See the [secret rotation guide](secret-rotation.md).

---

## RPC tuning

The following environment variables control internal RPC behavior and resource limits:

| Environment variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_FANOUT_CONCURRENCY` | `16` | Maximum concurrent fanout RPCs per request. |
| `TSINK_CLUSTER_RPC_TIMEOUT_MS` | `2000` | Timeout per individual RPC call (ms). |
| `TSINK_CLUSTER_RPC_MAX_RETRIES` | `2` | Maximum retries before falling back to hinted handoff (writes) or error (reads). |
| `TSINK_CLUSTER_WRITE_MAX_BATCH_ROWS` | `1024` | Maximum rows per write batch sent to a peer. |
| `TSINK_CLUSTER_WRITE_MAX_INFLIGHT_BATCHES` | `32` | Maximum concurrent write batches in flight to all peers. |
| `TSINK_CLUSTER_READ_MAX_MERGED_SERIES` | `250000` | Maximum number of series that can be merged in a single distributed read. |
| `TSINK_CLUSTER_READ_MAX_MERGED_POINTS_PER_SERIES` | `1000000` | Maximum data points per series in a distributed read result. |
| `TSINK_CLUSTER_READ_MAX_MERGED_POINTS_TOTAL` | `5000000` | Maximum total data points in a distributed read result. |
| `TSINK_CLUSTER_READ_MAX_INFLIGHT_QUERIES` | `64` | Maximum concurrent distributed read queries. |
| `TSINK_CLUSTER_READ_MAX_INFLIGHT_MERGED_POINTS` | `20000000` | Maximum total in-flight merged points across all concurrent reads. |
| `TSINK_CLUSTER_READ_RESOURCE_ACQUIRE_TIMEOUT_MS` | `25` | Timeout acquiring read concurrency slots (ms). |

---

## Write deduplication

To suppress duplicate internal retries, each receiving node tracks idempotency keys in a bounded
sliding window. Current-format completions replay the original indexed result, counts, and
acknowledgement; legacy markers that lack a stored result return
`409 idempotency_result_unavailable` rather than inventing success. A completion-marker persistence
failure returns `503 dedupe_persistence_failed` and discloses any already accepted components;
shared disk quota or headroom rejection instead returns partial
`413 write_disk_quota_exceeded` with the same progress disclosure.
This is bounded duplicate suppression, not an end-to-end exactly-once guarantee; see
[clustering internals](clustering-internals.md#idempotency-and-deduplication).

| Environment variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_DEDUPE_WINDOW_SECS` | `900` | Deduplication window (15 minutes). |
| `TSINK_CLUSTER_DEDUPE_MAX_ENTRIES` | `250000` | Maximum tracked idempotency keys. |
| `TSINK_CLUSTER_DEDUPE_MAX_LOG_BYTES` | `67108864` (64 MiB) | Maximum WAL size for the dedupe log. |
| `TSINK_CLUSTER_DEDUPE_CLEANUP_INTERVAL_SECS` | `30` | Cleanup interval for expired entries. |

---

## Cluster audit log

The server attempts to record every control-plane operation outcome (joins, leaves, ring changes,
handoffs) in an audit log queryable via the admin API. Audit persistence happens after the operation;
failure is logged but cannot roll back a mutation that already completed. The JSONL log is durable
but is not cryptographically tamper-evident.
When the server shared local-disk budget is configured, audit appends reserve `Cluster` bytes and
retain typed quota/headroom classification in the server log. Expired/size-pruned records are
rewritten through an exact bounded atomic compaction; failure after an already durable append is
cleanup debt and does not turn that append into a false rejection. The TSDB status payload and
Prometheus metrics expose cleanup debt and persistence fencing. An indeterminate append fences
later audit appends so they cannot extend an uncertain JSONL tail; strict restart replay rejects an
incomplete tail rather than silently truncating it.

| Environment variable | Default | Description |
|---|---|---|
| `TSINK_CLUSTER_AUDIT_RETENTION_SECS` | `2592000` (30 days) | How long audit records are retained. |
| `TSINK_CLUSTER_AUDIT_MAX_LOG_BYTES` | `134217728` (128 MiB) | Maximum size of the audit log. |
| `TSINK_CLUSTER_AUDIT_MAX_QUERY_LIMIT` | `1000` | Maximum records returned per audit query. |

```bash
# Query recent audit records
curl http://node-1:9201/api/v1/admin/cluster/audit

# Export full audit log as NDJSON
curl http://node-1:9201/api/v1/admin/cluster/audit/export
```

---

## Configuration checklist

Before evaluating an experimental cluster deployment:

- [ ] Every node has a unique, stable `--cluster-node-id`.
- [ ] `--cluster-bind` is reachable by all peers on the network.
- [ ] `--cluster-shards` is consistent across all nodes in the cluster.
- [ ] `--cluster-replication-factor` ≤ number of storage-capable nodes.
- [ ] Internal auth is configured: either `--cluster-internal-auth-token[|-file]` or `--cluster-internal-mtls-*`.
- [ ] `--cluster-seeds` lists enough existing nodes for bootstrap (any subset works).
- [ ] Firewalls allow traffic on both the public `--listen` port and the internal `--cluster-bind` port.
