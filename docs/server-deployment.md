# Server deployment

Run tsink as an optional standalone adapter around the embedded engine. The server exposes the
Prometheus, OTLP, and legacy ingest/query endpoints listed in this guide; their presence is not a
claim of complete upstream compatibility. TLS uses rustls rather than OpenSSL, and the protobuf
compiler needed at build time is vendored.

```bash
cargo build -p tsink-server --release

./target/release/tsink-server \
  --listen 127.0.0.1:9201 \
  --data-path ./var/tsink
```

---

## Quick start

```bash
# Start a minimal single-node server.
tsink-server --listen 0.0.0.0:9201 --data-path /var/lib/tsink

# Write via Prometheus text exposition.
curl -X POST http://127.0.0.1:9201/api/v1/import/prometheus \
  -H 'Content-Type: text/plain' \
  -d 'http_requests_total{method="GET"} 1027 1700000000000'

# Instant PromQL query.
curl 'http://127.0.0.1:9201/api/v1/query?query=http_requests_total'

# Health probe.
curl http://127.0.0.1:9201/healthz
```

---

## Building from source

```bash
# Release build (recommended for production).
cargo build -p tsink-server --release
# Binary path: target/release/tsink-server

# Development build.
cargo build -p tsink-server
```

`protoc` is vendored at build time — no installation required.

---

## CLI flags

All flags can be passed directly on the command line.
Use `--help` to print the full listing with types and defaults.

### Network

| Flag | Default | Description |
|---|---|---|
| `--listen` | `127.0.0.1:9201` | TCP address for the HTTP/HTTPS listener. |
| `--statsd-listen` | *disabled* | UDP address for the StatsD listener. |
| `--statsd-tenant` | `default` | Tenant ID for StatsD writes. |
| `--graphite-listen` | *disabled* | TCP address for the Graphite plaintext listener. |
| `--graphite-tenant` | `default` | Tenant ID for Graphite writes. |

### Storage

| Flag | Default | Description |
|---|---|---|
| `--resource-profile PROFILE` | `server` | Core resource base: `test`, `embedded`, `edge`, `server`, or explicit migration profile `expert-unlimited`. |
| `--data-path PATH` | *none* | Directory for WAL, segments, metadata, and rules. Required for persistent storage. |
| `--object-store-path PATH` | *none* | Shared local, FUSE, or network-filesystem mount for warm/cold tier segments. Native object-store URIs are not supported; the path must not overlap `--data-path`. |
| `--local-disk-limit BYTES` | `256 GiB` (`Server`, when persistent) | Shared logical byte limit for budget-integrated writers under `--data-path`. Must be greater than zero when set. |
| `--filesystem-free-headroom BYTES` | `2 GiB` (`Server`, when persistent) | Filesystem free space that budget-integrated writes must leave available. |
| `--maintenance-temp-reserve BYTES` | `16 GiB` (`Server`, when persistent) | Capacity withheld from normal growth for maintenance temporary output. Must be smaller than `--local-disk-limit` when that limit is set. |
| `--offline-restore-root PATH` | *none* | Dedicated parent for bounded offline restore targets. Requires a finite `--offline-restore-disk-limit`; must be isolated from live/object-store roots. |
| `--offline-restore-disk-limit BYTES` | *none* | Finite logical limit for staging, restored targets, and restore reports beneath the offline root. |
| `--offline-restore-filesystem-free-headroom BYTES` | `0` | Free-space floor for offline restore work. Requires the paired root and limit. |
| `--wal-enabled BOOL` | `true` | Enable or disable the write-ahead log. |
| `--wal-sync-mode MODE` | `per-append` | WAL durability policy: `per-append` synchronizes each non-empty write; `periodic` uses an append-driven interval for higher throughput. |
| `--timestamp-precision PRECISION` | `ms` | Interpret ingested timestamps as `s`, `ms`, `us`, or `ns`. |
| `--retention DURATION` | 14 days | Global data retention window (e.g. `30d`, `720h`). |
| `--hot-tier-retention DURATION` | same as effective global retention | Age threshold before segments move from hot to warm. |
| `--warm-tier-retention DURATION` | same as effective global retention | Age threshold before segments move from warm to cold. |
| `--storage-mode MODE` | `read-write` | `read-write` for full local persistence, `compute-only` for query-only nodes backed by object store. |
| `--remote-segment-refresh-interval DURATION` | *default* | Metadata refresh TTL for `compute-only` nodes. |
| `--mirror-hot-segments-to-object-store BOOL` | `false` | Copy hot-tier segments to the object store for DR. Requires `--object-store-path`. |

Disk-size flags accept integer bytes or a case-insensitive binary `K`, `M`, `G`, or `T` suffix
(for example, `512M` or `1.5G`). Setting `--local-disk-limit`, a non-zero
`--filesystem-free-headroom`, or a non-zero `--maintenance-temp-reserve` requires `--data-path`.
The reserve remains unavailable to normal growth but may be consumed by maintenance temporary
output; maintenance must still leave the filesystem headroom. The headroom plus reserve must fit in
the supported 64-bit byte range.

The shared budget's quota-aware writers are core storage, metric metadata, exemplars, rules, the
usage ledger, managed control-plane state, the experimental hinted-handoff outbox, cluster audit
log, cluster deduplication markers, the paired cluster control state and consensus log, and both
the edge source queue and standalone edge-accept deduplication markers. External snapshot
destinations remain outside that live budget.

Offline restore uses a second, independently leased coordinator. The root/limit pair is mandatory
for every server restore; it governs staging, standalone and internal targets, local cluster-node
targets, and the cluster restore report. The root must not overlap live data or object storage and,
when `--admin-path-prefix` is set, must be its strict descendant. Snapshot destinations beneath the
offline root are rejected. A restore target may neither equal the offline root nor overlap the live
`--data-path`; cluster report paths are preflight-checked against the source manifest and every
local snapshot source and target before any node is restored. Files beneath `--data-path` can still
be included when usage is reconciled, but that does not provide admission guarantees for excluded
writers.

### Memory and performance

| Flag | Default | Description |
|---|---|---|
| `--memory-limit BYTES` | `2 GiB` (`Server`) | Accounted storage-memory budget (e.g. `1G` or `1073741824`). Triggers admission backpressure when exceeded; it is not a process-RSS cap. |
| `--maintenance-max-items-per-pass N` | `500,000` (`Server`) | Maximum logical items selected by one bounded maintenance pass. A finite value must cover the effective write-batch row limit. |
| `--maintenance-max-bytes-per-pass BYTES` | `2 GiB` (`Server`) | Maximum modeled bytes selected by one bounded maintenance pass. A finite value must cover the effective memory limit. |
| `--cardinality-limit N` | `10,000,000` (`Server`) | Maximum number of unique series. New series are rejected at the limit. |
| `--chunk-points N` | *engine default* | Target number of data points per in-memory chunk before sealing. |
| `--max-writers N` | `16` (`Server`) | Parallel writer permits for ingestion. |

### TLS

| Flag | Default | Description |
|---|---|---|
| `--tls-cert PATH` | *none* | PEM TLS certificate. Must be paired with `--tls-key`. |
| `--tls-key PATH` | *none* | PEM TLS private key. Must be paired with `--tls-cert`. |

TLS uses rustls — no OpenSSL is required.

### Authentication

| Flag | Default | Description |
|---|---|---|
| `--auth-token TOKEN` | *none* | Bearer token required on all public requests. |
| `--auth-token-file PATH` | *none* | Load the public Bearer token from a file or exec manifest. Mutually exclusive with `--auth-token`. |
| `--admin-auth-token TOKEN` | *none* | Bearer token accepted for authenticated `/api/v1/admin/*` endpoints. |
| `--admin-auth-token-file PATH` | *none* | Load the admin Bearer token from a file or exec manifest. Mutually exclusive with `--admin-auth-token`. |
| `--rbac-config PATH` | *none* | RBAC roles, service accounts, and OIDC mappings (JSON). Supersedes legacy token auth when present. |
| `--tenant-config PATH` | *none* | Per-tenant authorization quotas and admission policies (JSON). |

`--auth-token` and `--auth-token-file` are mutually exclusive.
`--admin-auth-token` and `--admin-auth-token-file` are mutually exclusive.

### Admin API

| Flag | Default | Description |
|---|---|---|
| `--enable-admin-api` | *disabled* | Enable snapshot, restore, rollup, RBAC, and experimental cluster admin endpoints. Requires at least one auth option. |
| `--admin-path-prefix PATH` | *none* | Restrict admin file-system operations (snapshot/restore) to paths under PATH. Requires `--enable-admin-api`. |

### Edge sync

Edge sync lets an edge node queue accepted row batches locally and replay them to a central server,
tolerating network partitions. Metadata and exemplar sidecars are not included in the source
queue. Queue Put, Ack, and expiry records are synchronized before their in-memory state changes, so
a successful enqueue is crash-durable local queue state. It is not a durable-upload guarantee:
configured pre-ack expiry may remove pending rows, and the source currently accepts any valid
upstream acknowledgement, including `Volatile`.

| Flag | Default | Description |
|---|---|---|
| `--edge-sync-upstream HOST:PORT` | *disabled* | Upstream tsink-server to replay writes to. Requires `--edge-sync-auth-token` and `--data-path`. |
| `--edge-sync-auth-token TOKEN` | *none* | Shared token for edge sync replay and accept-side ingest. |
| `--edge-sync-source-id ID` | *listen address* | Stable source identifier for idempotency keys. |
| `--edge-sync-static-tenant ID` | *preserve* | Rewrite all replayed rows into a single upstream tenant. |

Edge sync and cluster mode are mutually exclusive.

On successful replay, the source removes a queued entry only after the upstream returns a complete,
validated canonical atomic result for every row and after the local queue acknowledgement record is
successfully appended and synchronized. Pending entries can also be expired without an upstream
acknowledgement after `TSINK_EDGE_SYNC_PRE_ACK_RETENTION_SECS`; retention drops are exposed in
status and metrics. The admin status snapshot exposes the last successful result as
`lastUpstreamAcknowledgement`. It also reports `persistenceFenced` when an indeterminate local
append has stopped further queue mutation, and `cleanupPending` plus `lastCleanupError` when a
durable record succeeded but log compaction must be retried. Those states set `degraded` and the
corresponding `tsink_edge_sync_queue_health{state}` gauges. Any valid upstream acknowledgement,
including `Volatile`, currently completes replay; deployments that require crash-durable upstream
acceptance must configure the upstream for durable WAL acknowledgement. There is not yet a
source-side minimum-acknowledgement policy, and edge sync is not an end-to-end exactly-once
protocol.

### Cluster (experimental)

Cluster mode is an experimental advanced capability, not part of tsink's primary embedded, single-node product. See the [cluster setup guide](cluster-setup.md) for full cluster documentation.

| Flag | Default | Description |
|---|---|---|
| `--cluster-enabled BOOL` | `false` | Enable experimental cluster mode. |
| `--cluster-node-id ID` | *none* | Stable identifier for this node; required when cluster mode is enabled. |
| `--cluster-bind HOST:PORT` | *none* | Internal RPC bind/advertise address; required when cluster mode is enabled. |
| `--cluster-node-role ROLE` | `hybrid` | Node role: `storage`, `query`, or `hybrid`. |
| `--cluster-seeds HOST:PORT,...` | *none* | Comma-separated seed peers for bootstrap. |
| `--cluster-shards N` | `128` | Logical shard count for the consistent hash ring. |
| `--cluster-replication-factor N` | `1` | Replicas per shard. |
| `--cluster-write-consistency MODE` | `quorum` | Write consistency: `one`, `quorum`, or `all`. |
| `--cluster-read-consistency MODE` | `eventual` | Read consistency: `eventual`, `quorum`, or `strict`. |
| `--cluster-read-partial-response MODE` | `allow` | Partial read policy: `allow` or `deny`. |
| `--cluster-internal-auth-token TOKEN` | *none* | Shared secret for internal RPC when mTLS is disabled. |
| `--cluster-internal-auth-token-file PATH` | *none* | Load the internal RPC token from a file or exec manifest. |
| `--cluster-internal-mtls-enabled BOOL` | `false` | Enable mTLS for peer-to-peer RPC. |
| `--cluster-internal-mtls-ca-cert PATH` | *none* | PEM CA bundle for peer certificate verification. |
| `--cluster-internal-mtls-cert PATH` | *none* | PEM client certificate for outbound RPC. |
| `--cluster-internal-mtls-key PATH` | *none* | PEM client private key for outbound RPC. |

---

## Environment variables

Admission control, rules evaluation, and edge sync behaviour are tuned through environment variables rather than CLI flags.

### Write admission

| Variable | Default | Description |
|---|---|---|
| `TSINK_SERVER_WRITE_MAX_INFLIGHT_REQUESTS` | `64` | Maximum concurrent write requests. |
| `TSINK_SERVER_WRITE_MAX_INFLIGHT_ROWS` | `200000` | Maximum total rows allowed in flight across all concurrent write requests. |
| `TSINK_SERVER_WRITE_RESOURCE_ACQUIRE_TIMEOUT_MS` | `25` | Milliseconds to wait for an admission slot before returning 429. |

### Read admission

| Variable | Default | Description |
|---|---|---|
| `TSINK_SERVER_READ_MAX_INFLIGHT_REQUESTS` | `64` | Maximum concurrent read requests. |
| `TSINK_SERVER_READ_MAX_INFLIGHT_QUERIES` | `128` | Maximum total PromQL queries allowed in flight across all concurrent read requests. |
| `TSINK_SERVER_READ_RESOURCE_ACQUIRE_TIMEOUT_MS` | `25` | Milliseconds to wait for an admission slot before returning 429. |

### Remote write feature flags

| Variable | Default | Description |
|---|---|---|
| `TSINK_REMOTE_WRITE_METADATA_ENABLED` | `true` | Accept metric metadata in Prometheus remote write payloads. |
| `TSINK_REMOTE_WRITE_MAX_METADATA_UPDATES` | `512` | Maximum metadata updates accepted per remote write request. |
| `TSINK_REMOTE_WRITE_EXEMPLARS_ENABLED` | `true` | Accept exemplars in Prometheus remote write payloads. |
| `TSINK_REMOTE_WRITE_HISTOGRAMS_ENABLED` | `true` | Accept native histograms in Prometheus remote write payloads. |
| `TSINK_REMOTE_WRITE_MAX_HISTOGRAM_BUCKET_ENTRIES` | `16384` | Maximum total native-histogram bucket entries per remote write request. |
| `TSINK_OTLP_METRICS_ENABLED` | `true` | Enable the OTLP `/v1/metrics` endpoint. |

### Rules engine

| Variable | Default | Description |
|---|---|---|
| `TSINK_RULES_SCHEDULER_TICK_MS` | `1000` | Evaluation interval in milliseconds. |
| `TSINK_RULES_MAX_RECORDING_ROWS_PER_EVAL` | `10000` | Maximum rows a recording rule may write per evaluation tick. |
| `TSINK_RULES_MAX_ALERT_INSTANCES_PER_RULE` | `10000` | Maximum active alert instances per rule. |

### Edge sync tuning

| Variable | Default | Description |
|---|---|---|
| `TSINK_EDGE_SYNC_MAX_ENTRIES` | `100000` | Maximum entries in the edge sync queue. |
| `TSINK_EDGE_SYNC_MAX_BYTES` | `536870912` (512 MiB) | Maximum queue size in bytes. |
| `TSINK_EDGE_SYNC_MAX_LOG_BYTES` | `2147483648` (2 GiB) | Maximum total log file size. |
| `TSINK_EDGE_SYNC_MAX_RECORD_BYTES` | `2097152` (2 MiB) | Maximum size of a single queued record. |
| `TSINK_EDGE_SYNC_REPLAY_INTERVAL_SECS` | `2` | Seconds between upstream replay attempts. |
| `TSINK_EDGE_SYNC_REPLAY_BATCH_SIZE` | `256` | Maximum queued entries considered per replay pass. |
| `TSINK_EDGE_SYNC_MAX_BACKOFF_SECS` | `30` | Maximum retry back-off on upstream failures. |
| `TSINK_EDGE_SYNC_CLEANUP_INTERVAL_SECS` | `30` | Seconds between queue cleanup passes. |
| `TSINK_EDGE_SYNC_PRE_ACK_RETENTION_SECS` | `86400` (24 h) | How long to retain entries pending upstream acknowledgment. |
| `TSINK_EDGE_SYNC_DEDUPE_WINDOW_SECS` | `86400` (24 h) | Deduplication window for replayed writes. |
| `TSINK_EDGE_SYNC_DEDUPE_MAX_ENTRIES` | *default* | Maximum entries in the deduplication store. |
| `TSINK_EDGE_SYNC_DEDUPE_MAX_LOG_BYTES` | *default* | Maximum size of the deduplication log. |
| `TSINK_EDGE_SYNC_DEDUPE_CLEANUP_INTERVAL_SECS` | *default* | Seconds between deduplication cleanup passes. |

---

## HTTP endpoints

All endpoints are on the same `--listen` address.
`/healthz` and `/ready` require no authentication.
All other endpoints require a valid `Authorization: Bearer <token>` header when `--auth-token` or `--rbac-config` is set.

### Health and observability

| Method | Path | Description |
|---|---|---|
| `GET` | `/healthz` | Liveness probe — returns 200 when the process is running. |
| `GET` | `/ready` | Readiness probe — returns 200 once the storage engine is ready to serve traffic. |
| `GET` | `/metrics` | Self-instrumentation in Prometheus text format. |

### PromQL queries

| Method | Path | Description |
|---|---|---|
| `GET`, `POST` | `/api/v1/query` | Instant query. |
| `GET`, `POST` | `/api/v1/query_range` | Range query. |
| `GET` | `/api/v1/series` | Series metadata matching `match[]` selector. |
| `GET` | `/api/v1/labels` | All label names. |
| `GET` | `/api/v1/label/<name>/values` | Values for a label name. |
| `GET` | `/api/v1/metadata` | Metric metadata. |
| `GET` | `/api/v1/query_exemplars` | Query exemplars by selector and time range. |
| `GET` | `/api/v1/status/tsdb` | TSDB-level stats (series count, cardinality). |

### Ingestion

| Method | Path | Protocol |
|---|---|---|
| `POST` | `/api/v1/write` | Prometheus remote write (Snappy block-compressed protobuf). |
| `POST` | `/api/v1/read` | Prometheus remote read. |
| `POST` | `/api/v1/import/prometheus` | Prometheus text exposition bulk import. |
| `POST` | `/write` | InfluxDB line protocol (v1 path). |
| `POST` | `/api/v2/write` | InfluxDB line protocol (v2 path). |
| `POST` | `/v1/metrics` | OTLP HTTP/protobuf metrics. Requires `TSINK_OTLP_METRICS_ENABLED=true`. |

StatsD and Graphite use separate UDP/TCP listeners configured via `--statsd-listen` and `--graphite-listen`.

### Admin endpoints

Admin endpoints are only served when `--enable-admin-api` is set and require the admin Bearer token.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/admin/snapshot` | Create an atomic snapshot at an external destination; managed-root destinations are rejected. |
| `POST` | `/api/v1/admin/restore` | Restore a snapshot beneath the configured, finitely bounded offline restore root; live-root overlap is rejected. |
| `POST` | `/api/v1/admin/rollups/apply` | Replace persisted rollup policies. |
| `POST` | `/api/v1/admin/rollups/run` | Run one synchronous rollup materialization pass. |
| `GET` | `/api/v1/admin/rollups/status` | Rollup policy freshness and coverage. |
| `POST` | `/api/v1/admin/delete_series` | Delete series by `match[]` selector with optional time range. |
| `GET` | `/api/v1/admin/rbac/state` | Inspect live RBAC roles, service accounts, and OIDC mappings. |
| `GET` | `/api/v1/admin/rbac/audit` | Inspect recent RBAC decision and reload audit entries. |
| `POST` | `/api/v1/admin/rbac/reload` | Reload RBAC config from disk without restarting. |
| `POST` | `/api/v1/admin/rbac/service_accounts/create` | Create a scoped service account and return its token. |
| `POST` | `/api/v1/admin/rbac/service_accounts/update` | Update service-account bindings or metadata. |
| `POST` | `/api/v1/admin/rbac/service_accounts/rotate` | Rotate a service-account token. |
| `POST` | `/api/v1/admin/rbac/service_accounts/disable` | Disable a service account. |
| `POST` | `/api/v1/admin/rbac/service_accounts/enable` | Re-enable a disabled service account. |
| `GET` | `/api/v1/admin/support_bundle` | Download a bounded JSON diagnostic snapshot for one tenant. |

Experimental cluster admin endpoints (`/api/v1/admin/cluster/*`) are documented in the [cluster setup guide](cluster-setup.md). They are not part of tsink's primary product.

---

## Authentication

### Bearer token

Pass a static shared token with `--auth-token`:

```bash
tsink-server \
  --listen 0.0.0.0:9201 \
  --data-path /var/lib/tsink \
  --auth-token "my-secret-token"
```

Clients must include `Authorization: Bearer my-secret-token` on every request (except `/healthz` and `/ready`).

### Token from a file

`--auth-token-file` reads the token from a plain text file (leading/trailing whitespace is trimmed):

```bash
echo "my-secret-token" > /run/secrets/tsink-token
tsink-server \
  --auth-token-file /run/secrets/tsink-token \
  --data-path /var/lib/tsink
```

The file can also be an exec manifest — a JSON object with a `cmd` array that is executed to produce the token, enabling dynamic credential retrieval.

### Separate admin token

Use `--admin-auth-token` to require a distinct token for admin endpoints while keeping a different token (or no token) for data endpoints:

```bash
tsink-server \
  --auth-token "reader-token" \
  --enable-admin-api \
  --admin-auth-token "admin-only-token" \
  --data-path /var/lib/tsink
```

### RBAC

Pass `--rbac-config` for role-based access with OIDC JWT or service account authentication.
Full RBAC reference is in the [security model guide](security.md).

---

## TLS

Provide a certificate and key pair to enable HTTPS:

```bash
tsink-server \
  --listen 0.0.0.0:9201 \
  --data-path /var/lib/tsink \
  --tls-cert /etc/tsink/tls/server.pem \
  --tls-key  /etc/tsink/tls/server.key \
  --auth-token "my-secret-token"
```

`--tls-cert` and `--tls-key` must both be present or both absent.
TLS is implemented with rustls — no OpenSSL dependency.

Certificates are hot-reloadable via the secret rotation API without restarting the process.
See the [secret rotation guide](secret-rotation.md) for details.

---

## Data directory layout

```
<data-path>/
├── wal/                        # Write-ahead log segments
├── index/                      # Series index structures
├── segments/                   # Persisted data chunks
│   ├── hot/                    # Hot-tier segments
│   ├── warm/                   # Warm-tier segments (when tiering is configured)
│   └── cold/                   # Cold-tier segments (when tiering is configured)
├── metadata/                   # Metric metadata and exemplar stores
├── rules-store.json            # Persisted recording and alerting rules
└── edge_sync/                  # Edge sync queue (when edge sync is configured)
    ├── queue.log               # Pending write entries
    └── dedupe.log              # Deduplication window
```

When cluster mode is enabled, additional directories are created under `<data-path>` for control-plane state, consensus log, audit log, and the hinted-handoff outbox.
Exact paths are printed to stderr during startup.

---

## Startup behaviour

The server prints diagnostic lines to stderr during bootstrap that confirm path locations and configuration parameters:

```
cluster control-state store initialized at /var/lib/tsink/cluster/control/node-a.control-state.json (schema v1)
cluster control-log consensus initialized at /var/lib/tsink/cluster/control/node-a.control-log.json
cluster audit log initialized at /var/lib/tsink/cluster/audit/node-a.audit.log
cluster dedupe marker store initialized at /var/lib/tsink/cluster/dedupe/node-a.markers.log
cluster hinted-handoff outbox initialized at /var/lib/tsink/cluster/outbox/node-a.outbox.log
```

Before any listener binds, the server holds the canonical data-path process lease. Its
budget-integrated stores durably link newly created nested directories into their parents and reject
owned file paths whose final entry is a symlink or another non-regular file. These static checks do
not close a hostile concurrent namespace-swap race. The metadata, exemplar, rules, and managed-state
stores remove orphan atomic-replacement files only when they match that
store's generated `.<target>.tmp-<pid>-<nonce>` shape, using a canonical decimal `u32` PID and
exactly 16 lowercase hexadecimal nonce digits; an ambiguous matching directory makes startup fail
instead of being deleted. A successful removal is synchronized and followed by accounting
reconciliation, while a no-op orphan pass skips that rescan. The hinted-handoff outbox applies the
same generated-temporary rule and also recognizes its one exact legacy `<outbox>.compact.tmp` path;
the dedupe marker store recognizes only its exact legacy `<dedupe>.tmp` path in addition to the
generated rule. Neither store treats lookalike operator files as owned cleanup candidates.

The server performs one final idle-coordinator reconciliation after all persistent stores open. The
TCP listener binds last. Once bound, status and metrics include files created during bootstrap and
the server is ready to accept connections.

---

## Graceful shutdown

The server handles `SIGTERM` and `SIGINT` (Ctrl-C).
On receipt, it:

1. Stops accepting new connections.
2. Signals idle HTTP and Graphite connections to close and waits for in-flight request work.
3. Logs a warning after **10 seconds** if connections remain, but continues draining them instead of
   detaching work that may still mutate persistent state.
4. Stops owned background workers, flushes and closes the storage runtime, and only then releases
   the data-path process lease.

---

## Kubernetes deployment

### Deployment example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tsink
spec:
  replicas: 1
  selector:
    matchLabels:
      app: tsink
  template:
    metadata:
      labels:
        app: tsink
    spec:
      containers:
        - name: tsink
          image: your-registry/tsink-server:0.10.2
          args:
            - --listen=0.0.0.0:9201
            - --data-path=/data
            - --auth-token=$(TSINK_AUTH_TOKEN)
          env:
            - name: TSINK_AUTH_TOKEN
              valueFrom:
                secretKeyRef:
                  name: tsink-secrets
                  key: auth-token
          ports:
            - containerPort: 9201
          livenessProbe:
            httpGet:
              path: /healthz
              port: 9201
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /ready
              port: 9201
            initialDelaySeconds: 5
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: tsink-data
```

### Prometheus scrape config

```yaml
scrape_configs:
  - job_name: tsink
    static_configs:
      - targets: ["tsink:9201"]
    authorization:
      credentials: "my-secret-token"
```

---

## Example configurations

### Minimal persistent single node

```bash
tsink-server \
  --listen 0.0.0.0:9201 \
  --data-path /var/lib/tsink
```

### Production single node with auth, TLS, and retention

```bash
tsink-server \
  --listen 0.0.0.0:9201 \
  --data-path /var/lib/tsink \
  --retention 30d \
  --memory-limit 4G \
  --tls-cert /etc/tsink/server.pem \
  --tls-key  /etc/tsink/server.key \
  --auth-token-file /run/secrets/tsink-token \
  --enable-admin-api \
  --admin-auth-token-file /run/secrets/tsink-admin-token
```

### Tiered storage with object store

```bash
tsink-server \
  --listen 0.0.0.0:9201 \
  --data-path /var/lib/tsink \
  --object-store-path /mnt/object-store/tsink \
  --hot-tier-retention 7d \
  --warm-tier-retention 30d \
  --retention 365d
```

### Query-only (compute-only) node

```bash
tsink-server \
  --listen 0.0.0.0:9201 \
  --storage-mode compute-only \
  --object-store-path /mnt/object-store/tsink
```

### Edge node forwarding to central server

```bash
# Edge node — buffers writes locally and replays to upstream.
tsink-server \
  --listen 127.0.0.1:9201 \
  --data-path /var/lib/tsink-edge \
  --edge-sync-upstream central.example.com:9201 \
  --edge-sync-auth-token "shared-edge-token" \
  --edge-sync-source-id "edge-node-1"

# Central server — accepts replayed writes.
tsink-server \
  --listen 0.0.0.0:9201 \
  --data-path /var/lib/tsink \
  --edge-sync-auth-token "shared-edge-token"
```

### Single node with StatsD and Graphite

```bash
tsink-server \
  --listen 0.0.0.0:9201 \
  --data-path /var/lib/tsink \
  --statsd-listen 0.0.0.0:8125 \
  --graphite-listen 0.0.0.0:2003
```

---

## Connection limits

| Limit | Value |
|---|---|
| Maximum concurrent HTTP connections | 1024 |
| TCP keep-alive interval | 60 s |
| Keep-alive idle timeout | 30 s |
| TLS handshake timeout | 10 s |
| Graceful shutdown warning threshold | 10 s; active request work continues draining before persistence teardown |

Admission control for concurrent write and read requests is tunable via environment variables — see [Write admission](#write-admission) and [Read admission](#read-admission) above.
