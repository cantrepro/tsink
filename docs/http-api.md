# HTTP API reference

This document covers every HTTP endpoint exposed by the tsink server, including request formats, query parameters, request/response headers, and error codes.

---

## Base URL

All paths are relative to the server listen address (e.g. `http://127.0.0.1:9201`).

---

## Authentication

### Scopes

| Scope | Paths | Notes |
|---|---|---|
| **Probe** | `/healthz`, `/ready` | Always unauthenticated. |
| **Internal** | `/internal/v1/*` | mTLS peer-to-peer traffic; validated separately. |
| **Public** | All other non-admin paths | Bearer token or RBAC. |
| **Admin** | `/api/v1/admin/*` | Requires admin token or elevated RBAC role; must be enabled with `--admin-api-enabled`. |

### Bearer token

Pass credentials in the `Authorization` header:

```
Authorization: Bearer <token>
```

For public endpoints a static token can be configured via `--auth-token`. For admin endpoints a separate token can be set with `--admin-auth-token`; if only a public token is configured it is accepted for admin too.

### RBAC / OIDC

When RBAC is enabled the bearer token is validated as a service-account token or an OIDC JWT (RS256/HS256). The server attaches the resolved principal and role to the request internally; see the [Security model](security.md) for role definitions.

### Authentication error headers

All 401/403 responses include:

| Header | Values | Meaning |
|---|---|---|
| `WWW-Authenticate` | `Bearer` | Signals that a bearer token is expected (401 only). |
| `X-Tsink-Auth-Error-Code` | `auth_token_missing` | No token provided for a protected endpoint. |
| | `auth_token_invalid` | Token provided but did not match. |
| | `admin_auth_token_missing` | No token provided for an admin endpoint. |
| | `admin_auth_token_invalid` | Admin token provided but did not match. |
| | `auth_scope_denied` | Public token used on an admin-only endpoint (403). |

---

## Multi-tenancy

Pass the tenant identifier on every request using the request header:

```
x-tsink-tenant: <tenant-id>
```

If omitted, the request is attributed to the `default` tenant. Tenant IDs must be non-empty strings that do not start with `__`. See [Multi-tenancy](multi-tenancy.md) for quota and isolation details.

---

## Common response envelope

JSON responses from query and admin endpoints use one of two shapes:

**Success**

```json
{
  "status": "success",
  "data": <result>
}
```

**PromQL error**

```json
{
  "status": "error",
  "errorType": "<type>",
  "error": "<message>"
}
```

`errorType` is one of `bad_data` (request validation) or `execution` (query evaluation).

---

## Cluster response headers

When clustering is active, query responses include additional metadata headers:

| Header | Description |
|---|---|
| `X-Tsink-Read-Consistency` | Effective read consistency level used (`one`, `quorum`, `all`). |
| `X-Tsink-Read-Partial-Policy` | Active partial-response policy (`allow`, `deny`). |
| `X-Tsink-Read-Partial-Response` | `true` if the response is partial (some shards unavailable). |
| `X-Tsink-Read-Partial-Warnings` | Number of partial-response warning messages. |

The JSON body of cluster query responses also contains a `partialResponse` object:

```json
{
  "status": "success",
  "data": ...,
  "partialResponse": {
    "enabled": true,
    "policy": "allow",
    "consistency": "quorum",
    "warningCount": 1
  },
  "warnings": ["shard 3: node node-2 unreachable"]
}
```

Write responses set per-request consistency headers when clustering is active:

| Header | Description |
|---|---|
| `X-Tsink-Write-Consistency` | Effective write consistency mode used. |
| `X-Tsink-Write-Required-Acks` | Number of replica acknowledgements required. |
| `X-Tsink-Write-Acknowledged-Replicas` | Minimum replica acknowledgements actually received. |

All successful non-empty HTTP ingestion responses report
`X-Tsink-Write-Acknowledgement: volatile|appended|durable`. When a failure follows a proven row
commit, `X-Tsink-Write-Partial: true`, `X-Tsink-Rows-Accepted`, and component counts disclose the
effect. If the backend or experimental cluster cannot determine whether rows committed, the
response instead uses `X-Tsink-Write-Partial: possible` and
`X-Tsink-Write-Outcome: indeterminate_backend|indeterminate_cluster`.

To override the write consistency on a per-request basis, set:

```
x-tsink-write-consistency: one|quorum|all
```

To override the partial-response behaviour on reads, set:

```
x-tsink-read-partial-response: allow|deny
```

---

## Health & readiness probes

### `GET /healthz`

Liveness probe. Always returns `200 ok` as plain text.

### `GET /ready`

Readiness probe. Returns `200 ready` as plain text when the server is ready to serve traffic.

Both probes are unauthenticated and are suitable for Kubernetes `livenessProbe` / `readinessProbe` configuration.

---

## Self-instrumentation

### `GET /metrics`

Returns self-instrumentation counters and gauges in Prometheus text exposition format.

**Authentication:** public scope (bearer token if configured).

**Response:** `200 text/plain` — Prometheus text exposition.

Key exported metric families include `tsink_memory_*`, `tsink_series_total`,
`tsink_uptime_seconds`, `tsink_wal_*`, `tsink_compaction_*`, `tsink_cluster_*`,
`tsink_ingest_*`, `tsink_query_*`, `tsink_query_budget_*`,
`tsink_metric_metadata_store_*`, `tsink_exemplar_*`, and `tsink_rules_store_*`.

---

## PromQL query

### `GET|POST /api/v1/query`

Instant PromQL evaluation.

**Query parameters / POST form fields:**

| Parameter | Required | Description |
|---|---|---|
| `query` | Yes | PromQL expression string. |
| `time` | No | Evaluation timestamp. Numeric values are Unix seconds, matching the Prometheus HTTP API; RFC 3339 is also accepted. Defaults to current server time. |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {"__name__": "cpu_usage", "host": "web-1"},
        "value": [1700000000.000, "42"]
      }
    ]
  }
}
```

---

### `GET|POST /api/v1/query_range`

Range PromQL evaluation.

**Query parameters / POST form fields:**

| Parameter | Required | Description |
|---|---|---|
| `query` | Yes | PromQL expression string. |
| `start` | Yes | Range start. Numeric values are Unix seconds, matching the Prometheus HTTP API; RFC 3339 is also accepted. |
| `end` | Yes | Range end. Must be ≥ `start`. |
| `step` | Yes | Step duration as seconds when numeric, or as a Go-style duration string (e.g. `15s`, `1m`, `1500ms`). |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {"__name__": "cpu_usage", "host": "web-1"},
        "values": [[1700000000.000, "42"], [1700000015.000, "43"]]
      }
    ]
  }
}
```

**Error codes:** `400` invalid parameters, `413` per-tenant range-points quota exceeded.

### PromQL query-budget errors

Instant and range evaluation each use one core query execution, including selector prefetch,
subqueries, evaluation steps, and final result accounting. If a configured core query limit is
reached, the response remains a Prometheus-style error envelope and includes a stable
`X-Tsink-Read-Error-Code` header:

| HTTP status | `errorType` and `X-Tsink-Read-Error-Code` | Meaning |
|---:|---|---|
| 400 | `invalid_query_limits` | Invalid request-specific or backend query-limit configuration. |
| 429 | `query_limit_concurrent_queries` | All configured core query permits are in use. |
| 429 | `query_limit_shared_memory_bytes` | The shared modeled query-memory budget is in use. |
| 413 | `query_limit_<reason>` | A non-retryable per-query work or memory limit was exceeded. |
| 503 | `query_cancelled` header, `canceled` error type | Cooperative cancellation was observed. |
| 503 | `query_deadline_exceeded` header, `timeout` error type | The effective query deadline expired. |

The two 429 responses include `Retry-After: 1`. Stable non-retryable reason suffixes are
`per_query_memory_bytes`, `series_matched`, `samples_scanned`, `samples_returned`,
`returned_bytes`, `pattern_expansion`, `steps`, and `intermediate_vector_size`. Limit failures do
not return truncated success data.

---

## Metadata queries

### `GET /api/v1/series`

Returns all time series matching one or more label selectors.

**Query parameters:**

| Parameter | Required | Description |
|---|---|---|
| `match[]` | Yes | Repeated. PromQL vector selector, e.g. `up{job="prometheus"}`. |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": [
    {"__name__": "cpu_usage", "host": "web-1"},
    {"__name__": "cpu_usage", "host": "web-2"}
  ]
}
```

---

### `GET /api/v1/labels`

Returns a sorted list of all label names present in the store.

**Response:** `200 application/json`

```json
{"status": "success", "data": ["__name__", "host", "region"]}
```

---

### `GET /api/v1/label/{name}/values`

Returns the set of values for a single label name.

**Path parameters:**

| Parameter | Description |
|---|---|
| `{name}` | URL-encoded label name, e.g. `host`. |

**Response:** `200 application/json`

```json
{"status": "success", "data": ["web-1", "web-2", "db-1"]}
```

---

### `GET /api/v1/metadata`

Returns metric metadata (type, help, unit).

**Query parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `metric` | No | — | Filter by metric name. |
| `limit` | No | `1000` | Maximum number of results. Values above the hard maximum of `10000` are rejected; they are not clamped. |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "http_requests_total": [
      {"type": "counter", "help": "Total HTTP requests", "unit": ""}
    ]
  }
}
```

The request is tenant-scoped and runs under one core query execution. The store result, JSON body,
and response-header allocation remain charged until response handoff. The returned-byte limit is
applied to the exact encoded JSON body, and results are never silently truncated. Query-budget and
metadata-store failures use a Prometheus error envelope plus a stable
`X-Tsink-Read-Error-Code`; retryable admission or store-unavailable responses include
`Retry-After: 1`.

---

### `GET|POST /api/v1/query_exemplars`

Returns exemplars for the series matched by a PromQL expression.

**Query parameters / POST form fields:**

| Parameter | Required | Description |
|---|---|---|
| `query` | Yes | PromQL expression (vector or matrix selector). |
| `start` | Yes | Start timestamp. |
| `end` | Yes | End timestamp; must not precede `start`. |
| `limit` | No | Positive maximum exemplar count. Defaults to the configured store query maximum; larger values are rejected. |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": [
    {
      "seriesLabels": {"__name__": "http_request_duration_seconds"},
      "exemplars": [
        {
          "labels": {"traceID": "abc123"},
          "value": "0.042",
          "timestamp": 1700000000.000
        }
      ]
    }
  ]
}
```

The response includes `X-Tsink-Exemplar-Limit` with the effective result limit. Local and
distributed execution share one tenant-scoped query budget across parsing, selector expansion,
store/RPC results, deduplication, and exact response encoding. Peers are queried sequentially so
transport and decoded-result envelopes do not multiply with cluster size. Invalid, over-limit,
cancelled, deadline-exceeded, unavailable, or accounting-invalid outcomes return stable
`X-Tsink-Read-Error-Code` values and never return a silently partial exemplar set.

---

## Status

### `GET /api/v1/status/tsdb`

Returns a comprehensive JSON status snapshot covering effective storage and query limits, memory
usage, WAL state, compaction levels, cluster topology, admission guardrails, ingestion protocol
status, metric-metadata and exemplar sidecar metrics, rules and rollup state, edge-sync state, and
tenant policy.

`data.effectiveStorageLimits` reports the controls enforced by the built storage backend. Optional
fields are JSON `null` when the built-in backend has no finite limit; when
`reportedByBackend` is `false`, they are unknown. `writeTimeoutNanos` preserves the configured
duration without millisecond rounding. These are storage-side controls, not a complete process
memory or local-disk envelope; see [Resource limits and profiles](resource-limits.md).
`data.queryBudget` separately reports the core query limits, active and peak query permits, current
and peak modeled query memory, lifecycle totals, fixed-reason rejection totals, cancellations,
deadlines, and accounting-invariant violations. A `null` query-limit field means no finite value is
configured for that dimension. The server selects the finite `Server` profile by default; null
profile dimensions require explicit `ExpertUnlimited` or a deliberate low-level override.
The background fields report the fixed per-instance thread/concurrency bounds and effective
cadences. `data.backgroundWork` reports the four worker slots' installed/running state, notifications,
idle parks, passes, exits, and shutdown joins. `persistedRefresh` also owns retention/tiering and
remote catalog refresh; those activities are not hidden extra threads.

When cluster consensus is active, `data.cluster.control.persistence` reports `fenced`,
`pendingCheckpoint` (`{index, term}` or `null`), `cleanupDebt`, `detail`, and `degraded`. A pending
checkpoint implies `fenced: true`: the schema-v2 log is durably authoritative but its control-state
mirror still needs repair. `degraded` is true when persistence is fenced or cleanup debt is
present. A fence can also represent a consensus-required candidate or post-commit higher term still
awaiting durable log publication; in that case `pendingCheckpoint` is `null` and `detail` describes
the indeterminate publication. `cleanupDebt` without a fence means the log and mirror are both
durable but grouped finalization, owned-temp cleanup, or accounting reconciliation remains to be
retried; cleanup is attempted before any separate fence repair.

**Authentication:** public scope, read permission.

**Response:** `200 application/json` — Large nested object; contents vary by configuration.

---

## Ingestion

### `POST /api/v1/write`

Prometheus Remote Write. Accepts snappy-compressed protobuf (`WriteRequest`).

**Request headers:**

| Header | Value |
|---|---|
| `Content-Type` | `application/x-protobuf` |
| `Content-Encoding` | `snappy` |
| `X-Prometheus-Remote-Write-Version` | `0.1.0` (optional) |

**Response:** `200` on success (empty body).

Optional response headers set when relevant features are active:

| Header | Description |
|---|---|
| `X-Tsink-Metadata-Applied` | Number of metadata updates applied. |
| `X-Tsink-Histograms-Accepted` | Number of native histogram samples accepted. |
| `X-Tsink-Exemplars-Accepted` | Number of exemplars accepted. |
| `X-Tsink-Exemplars-Dropped` | Number of exemplars dropped (over per-series limit). |

Feature flags (environment variables):

| Variable | Default | Description |
|---|---|---|
| `TSINK_REMOTE_WRITE_METADATA_ENABLED` | `true` | Accept metric metadata updates. |
| `TSINK_REMOTE_WRITE_EXEMPLARS_ENABLED` | `true` | Accept exemplars. |
| `TSINK_REMOTE_WRITE_HISTOGRAMS_ENABLED` | `true` | Accept native Prometheus histograms. |
| `TSINK_REMOTE_WRITE_MAX_METADATA_UPDATES` | `512` | Max metadata updates per request. |
| `TSINK_REMOTE_WRITE_MAX_HISTOGRAM_BUCKET_ENTRIES` | `16384` | Max total histogram bucket entries per request. |

**Error codes:** `400` invalid protobuf, `413` write quota exceeded, `422` disabled feature, `503` admission unavailable.

---

### `POST /api/v1/read`

Prometheus Remote Read. Accepts snappy-compressed protobuf (`ReadRequest`); responds with snappy-compressed protobuf (`ReadResponse`).

**Request headers:**

| Header | Value |
|---|---|
| `Content-Type` | `application/x-protobuf` |
| `Content-Encoding` | `snappy` |

**Response headers:**

| Header | Value |
|---|---|
| `Content-Type` | `application/x-protobuf` |
| `Content-Encoding` | `snappy` |
| `X-Prometheus-Remote-Read-Version` | `0.1.0` |

Only `Samples` response type is supported. Chunked streaming is not yet available. Each completed
query result is encoded immediately instead of retaining every result object in the request. The
complete uncompressed protobuf response has a hard 64 MiB ceiling, and Snappy output allocation is
preflighted from that bounded size. Exceeding the envelope returns `413` with
`X-Tsink-Read-Error-Code: query_limit_returned_bytes`; no truncated success response is returned.
Local reads also charge each encoded query-result frame to that query's core returned-byte budget.

**Error codes:** `400` invalid request, `413` queries-per-request or returned-byte limit exceeded,
`429` query concurrency/shared-memory admission unavailable, `500` internal storage or encoding
failure, `503` closed/shutting-down storage, deadline/cancellation, or admission unavailable.

---

### `POST /api/v1/import/prometheus`

Prometheus text exposition bulk import.

**Request headers:**

| Header | Value |
|---|---|
| `Content-Type` | `text/plain` |

**Request body:** Prometheus text exposition format, one sample per line:

```
http_requests_total{method="GET"} 1027 1700000000000
```

**Response:** `200` on success (empty body).

---

### `POST /v1/metrics`

OTLP HTTP metrics ingest. Accepts protobuf-encoded `ExportMetricsServiceRequest`.

**Request headers:**

| Header | Value |
|---|---|
| `Content-Type` | `application/x-protobuf` or `application/protobuf` |

Supported metric kinds: gauges, monotonic sums, histograms, and summaries. Exponential histograms
are rejected explicitly. Exemplars within supported OTLP payloads are forwarded to the exemplar
store.

Feature flag: `TSINK_OTLP_METRICS_ENABLED` (default `true`).

**Response:** `200 application/x-protobuf` — protobuf-encoded OTLP
`ExportMetricsServiceResponse`.

**Error codes:** `415` unsupported content type, `422` OTLP ingest disabled.

---

### `POST /write` and `POST /api/v2/write`

InfluxDB line protocol (v1 and v2 API paths). Accepts plain-text line protocol.

**Request body:** InfluxDB line protocol, e.g.:

```
cpu_usage,host=web-1 value=42.0 1700000000000000000
```

**Response:** `204` on success (no body), matching the InfluxDB convention.

---

## Admin API

Admin endpoints require `--admin-api-enabled` on the server. All admin requests require the admin bearer token (or a public token if no dedicated admin token is configured).

After a mutating admin operation produces its response, the server attempts to record the outcome in
the cluster audit log with the actor identity derived from the `Authorization` header. The actor ID
can be overridden by passing `x-tsink-actor-id`. Audit persistence failure is logged but does not
roll back or reclassify an operation that already completed.

---

### Data management

#### `POST /api/v1/admin/snapshot`

Take a local data snapshot.

**Request (query param or JSON body):**

| Field | Required | Description |
|---|---|---|
| `path` | Yes | Filesystem path to write the snapshot to. |

**Response:** `200 application/json`

```json
{"status": "success", "data": {"path": "/var/snapshots/snap-1"}}
```

---

#### `POST /api/v1/admin/restore`

Restore a local data snapshot. The server must be started with a paired
`--offline-restore-root` and finite `--offline-restore-disk-limit`; otherwise this endpoint fails
closed with `503 offline_restore_unconfigured`. The destination must be a strict descendant of
that root and must not overlap the live data directory. Snapshot source/target overlap and static
link-like entries are rejected by the core restore preflight.

**Request (query params or JSON body):**

| Field | Required | Aliases | Description |
|---|---|---|---|
| `snapshot_path` | Yes | `snapshotPath` | Path to an existing snapshot directory. |
| `data_path` | Yes | `dataPath` | Destination data directory to restore into. |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {"snapshotPath": "...", "dataPath": "..."}
}
```

A logical quota or filesystem-headroom rejection returns HTTP 413 with
`write_disk_quota_exceeded` and the matching `X-Tsink-Write-Error-Code` header. Invalid targets or
snapshots return HTTP 422; other failures whose publication outcome cannot be proven return HTTP
503. Snapshot creation rejects destinations beneath the offline restore root so exports cannot
consume its reserved capacity.

---

#### `POST /api/v1/admin/delete_series`

Tombstone-delete series matching label selectors within an optional time range.

**Request (query params or JSON body):**

| Field | Required | Description |
|---|---|---|
| `match[]` | Yes | Repeated PromQL vector selectors. |
| `start` | No | Delete range start (Unix ms or RFC 3339). Defaults to `i64::MIN`. |
| `end` | No | Delete range end. Must be > `start`. Defaults to `i64::MAX`. |

The JSON body equivalent uses `selectors` (array of strings), `start`, and `end`.

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "tenantId": "default",
    "matchersProcessed": 1,
    "matchedSeries": 3,
    "tombstonesApplied": 3,
    "start": -9223372036854775808,
    "end": 9223372036854775807
  }
}
```

Malformed bodies, selectors, or time ranges return `400`; a storage mode that cannot durably delete
returns `409`. Before any selector commits, a logical quota, filesystem-headroom, or maintenance-
reserve rejection returns structured `413 write_disk_quota_exceeded` without partial-outcome
headers. If an earlier selector, or an earlier series expanded from the same tenant-scoped selector,
already committed tombstones, a later quota rejection remains `413` but returns
`X-Tsink-Write-Partial: true`, `X-Tsink-Write-Outcome: partial`, and the proven
`X-Tsink-Delete-Matchers-Processed`, `X-Tsink-Delete-Matched-Series`, and
`X-Tsink-Delete-Tombstones-Applied` counts; the JSON error body carries the same counts. Other
tombstone persistence failures return an indeterminate `500` with `possible`/
`indeterminate_backend` headers; when prior committed progress exists, the response also carries
the proven delete counts.

---

### Rules & rollups

#### `POST /api/v1/admin/rules/apply`

Apply a recording/alerting rules configuration.

**Request body (JSON):**

```json
{
  "groups": [
    {
      "name": "recording",
      "tenantId": "default",
      "interval": "1m",
      "rules": [
        {
          "kind": "recording",
          "record": "job:http_requests:rate5m",
          "expr": "rate(http_requests_total[5m])"
        }
      ]
    }
  ]
}
```

**Response:** `200 application/json` — rules snapshot.

Invalid rule definitions return `400`. A local-disk quota, filesystem-headroom, or
maintenance-reserve rejection returns structured `413 write_disk_quota_exceeded`; other rules-store
persistence failures return an indeterminate `500`.

---

#### `POST /api/v1/admin/rules/run`

Trigger an immediate rules evaluation cycle.

**Response:** `200 application/json` — rules snapshot. `409` if already running.

---

#### `GET /api/v1/admin/rules/status`

Return current rules scheduler state and last evaluation results.

**Response:** `200 application/json` — rules snapshot including `metrics.configuredGroups`, per-group rule results, and last-run timestamps.

---

#### `POST /api/v1/admin/rollups/apply`

Apply rollup downsampling policies.

**Request body (JSON):**

```json
{
  "policies": [
    {
      "metric": "cpu_usage",
      "resolution": "5m",
      "retention": "90d",
      "aggregations": ["avg", "max"]
    }
  ]
}
```

**Response:** `200 application/json` — rollup policies snapshot. Policy persistence commits before
the initial materialization attempt. If that first materialization fails, the apply still returns
the committed `200` snapshot with the per-policy `lastError` and incremented `workerErrorsTotal`;
use `/api/v1/admin/rollups/run` to retry materialization rather than treating the policy update as
uncommitted.

Invalid or unsupported policies return `400`. A local-disk quota, filesystem-headroom, or
maintenance-reserve rejection returns structured `413 write_disk_quota_exceeded`; other rollup
persistence failures return an indeterminate `500`.

---

#### `POST /api/v1/admin/rollups/run`

Trigger an immediate rollup materialization run.

**Response:** `200 application/json` — rollup snapshot.

A local-disk quota, filesystem-headroom, or maintenance-reserve rejection returns structured
`413 write_disk_quota_exceeded`; other materialization persistence failures return an indeterminate
`500`.

---

#### `GET /api/v1/admin/rollups/status`

Return current rollup scheduler state.

**Response:** `200 application/json` — rollup snapshot from storage observability.

---

### RBAC

#### `GET /api/v1/admin/rbac/state`

Return a full RBAC configuration snapshot (roles, service accounts, OIDC providers).

**Response:** `200 application/json`

```json
{"status": "success", "data": { ... }}
```

---

#### `GET /api/v1/admin/rbac/audit`

Query RBAC authorization audit log.

**Query parameters:**

| Parameter | Default | Description |
|---|---|---|
| `limit` | `100` | Maximum number of entries to return. |

**Response:** `200 application/json` — `{entries: [...]}`.

---

#### `POST /api/v1/admin/rbac/reload`

Hot-reload the RBAC configuration from disk.

**Response:** `200 application/json` — updated RBAC state snapshot.

---

#### `POST /api/v1/admin/rbac/service_accounts/create`

Create a new service account and return its bearer token.

**Request body (JSON):** `ServiceAccountSpec`

```json
{
  "id": "my-writer",
  "role": "writer",
  "description": "CI pipeline ingest account"
}
```

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "serviceAccount": { "id": "my-writer", "role": "writer", ... },
    "token": "<bearer-token>"
  }
}
```

---

#### `POST /api/v1/admin/rbac/service_accounts/update`

Update an existing service account (role, description).

**Request body (JSON):** `ServiceAccountSpec` with the target account `id`.

**Response:** `200 application/json` — updated service account.

---

#### `POST /api/v1/admin/rbac/service_accounts/rotate`

Rotate the bearer token of a service account.

**Request (query param or JSON body):**

| Field | Required | Description |
|---|---|---|
| `id` | Yes | Service account identifier. |

**Response:** `200 application/json` — service account and new token (same shape as create).

---

#### `POST /api/v1/admin/rbac/service_accounts/disable`

Disable a service account (token will be rejected).

**Request (query param or JSON body):** `id`

**Response:** `200 application/json`.

---

#### `POST /api/v1/admin/rbac/service_accounts/enable`

Re-enable a previously disabled service account.

**Request (query param or JSON body):** `id`

**Response:** `200 application/json`.

---

### Secrets & TLS rotation

#### `GET /api/v1/admin/secrets/state`

Return current security and secret state (TLS certificate expiry, token rotation status, mTLS materials).

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": { ... }
}
```

---

#### `POST /api/v1/admin/secrets/rotate`

Rotate a TLS certificate, bearer token, or mTLS material.

**Request body (JSON):**

| Field | Required | Description |
|---|---|---|
| `target` | Yes | Secret target identifier (e.g. `tls`, `admin_token`, `mtls_ca`). |
| `mode` | Yes | Rotation mode (`replace` or `overlap`). |
| `new_value` | No | New credential value (if not auto-generated). |
| `overlap_seconds` | No | Grace period during which the old credential remains valid. |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "target": "admin_token",
    "mode": "overlap",
    "issuedCredential": "<new-token>",
    "state": { ... }
  }
}
```

---

### Usage accounting

#### `GET /api/v1/admin/usage/report`

Return aggregated usage report for one or all tenants.

**Query parameters:**

| Parameter | Default | Description |
|---|---|---|
| `tenant` | — | Tenant ID to filter. Omit for all tenants. |
| `start` | — | Unix milliseconds start (optional). |
| `end` | — | Unix milliseconds end (optional). |
| `bucket` | `hour` | Time bucket granularity (`none`, `hour`, `day`). |
| `reconcile` | `false` | If `true`, reconciles counters against storage before reporting. |
| `limit` | `1000` | Maximum matching recent records aggregated in this page; server maximum defaults to `4096`. Applies to time-filtered, bucketed, or cursor-based reports. |
| `afterSequence` | — | Exclusive sequence cursor returned as `page.nextAfterSequence`. |
| `snapshotSequence` | current last sequence | Pins continuation pages to the first page's `page.snapshotSequence`. |
| `maxBytes` | `4194304` | Requested encoded response ceiling, no larger than the configured server maximum. |

**Response:** `200 application/json` — `{report, reconciliation, reconciledStorageSnapshots}`.
`report.page` states the snapshot, earliest retained sequence, number of records aggregated,
continuation cursor, whether all raw history remains retained, and whether the result used the exact all-time aggregate. An unfiltered
`bucket=none` request uses exact all-time per-tenant aggregates. Time-filtered and bucketed reports
operate on the bounded recent-record window and must follow `nextAfterSequence` with the same
`snapshotSequence` while `hasMore` is true.
When `reconcile=true` completes, the response carries
`X-Tsink-Usage-Reconciliation: completed`, including a structured response-size rejection after
the reconciliation itself completed.

---

#### `GET /api/v1/admin/usage/export`

Stream raw per-request usage records as newline-delimited JSON.

**Query parameters:** `tenant`, `start`, and `end` have the same meaning as for reports. `limit`
defaults to `1000` (configured maximum `4096`), `maxBytes` defaults to `4194304`, and
`afterSequence`/`snapshotSequence` continue a stable page.

**Response:** `200 application/x-ndjson` — one JSON record per line.

Every response includes `X-Tsink-Usage-Snapshot-Sequence`,
`X-Tsink-Usage-Earliest-Available-Sequence`, `X-Tsink-Usage-Records-Returned`, and
`X-Tsink-Usage-Response-Bytes`, `X-Tsink-Usage-Raw-History-Complete`, and
`X-Tsink-Usage-Has-More`. When more records match, it also includes
`X-Tsink-Usage-Next-After-Sequence`; pass that value as `afterSequence` and preserve the snapshot
header as `snapshotSequence`. New appends are excluded from that pinned traversal. A cursor older
than the retained window returns structured HTTP 410 `usage_cursor_expired` rather than silently
skipping records. Invalid record/byte limits return structured HTTP 400, a single record that cannot
fit the requested byte page returns structured HTTP 413, and a report whose aggregate encoding
exceeds its response limit returns structured HTTP 413 `usage_report_response_too_large`.

---

#### `POST /api/v1/admin/usage/reconcile`

Force immediate usage ledger reconciliation against live storage.

**Response:** `200 application/json` — `{journal, storageSnapshots}`.

If the shared local-disk quota, filesystem headroom, or maintenance reserve rejects the
reconciliation frame, the endpoint returns structured `413 write_disk_quota_exceeded` without
publishing a tenant prefix. Other ledger persistence failures return an indeterminate
`500 usage_ledger_persistence_failed` response.

---

### Support bundle

#### `GET /api/v1/admin/support_bundle`

Download a bounded JSON diagnostic snapshot for a tenant. Includes status, usage, RBAC state, RBAC audit, security state, cluster audit, handoff/repair/rebalance status, rules, and rollup state.

**Query parameters:**

| Parameter | Default | Description |
|---|---|---|
| `tenant` | `default` | Tenant to scope the bundle to. |

**Response:** `200 application/json` — downloaded as `tsink-support-bundle-<tenant>-<timestamp>.json`.

---

### Cluster management

All cluster endpoints require an active cluster runtime (`--cluster-enabled`), which in turn
requires an explicit `--data-path`. Parameters can be supplied as query params or in a JSON request
body using either `snake_case` or `camelCase` field names.

Membership and handoff mutations can have five ordinary outcomes. A normal commit returns
`result: "committed"`; a quorum that is not yet complete returns HTTP 202 with `result: "pending"`;
and an authoritative log whose replacement and parent-directory sync succeeded but whose state
mirror could not be published returns HTTP 200 with
`result: "committed_checkpoint_pending"`, `degraded: true`, `committedLogIndex`, and
`committedLogTerm`. If the complete pair is durable but finalization, owned-temp cleanup, or
accounting reconciliation remains, HTTP 200 instead returns
`result: "committed_cleanup_pending"` with the same committed position and `degraded: true`. If a
post-commit notice reveals a higher term that cannot yet be persisted, HTTP 200 returns
`result: "committed_persistence_pending"`, the committed position, and `degraded: true`; auto-join
uses `accepted_persistence_pending`. All three degraded outcomes are already committed and must not
be retried as ordinary rejections. Before consensus requires a candidate, a shared-disk quota,
physical-headroom, or maintenance-reserve rejection returns HTTP 413 with
`write_disk_quota_exceeded`. If quorum or a
leader commit already makes that candidate required, a failure before durable log publication is
instead fenced and returned as HTTP 503 `control_persistence_indeterminate`; it is not a definitive
quota rejection. Both responses set `X-Tsink-Write-Error-Code` to the stable code. Internal control
RPC JSON uses the same codes with `retryable: false`: 413 is reserved for definitive typed disk
resource failures, while fenced or indeterminate persistence uses 503.

The degraded HTTP 200 applies only when the requested mutation itself reached the durable-log
checkpoint-pending, durable-pair cleanup-pending, or quorum-committed higher-term-persistence-
pending boundary. The last outcome adopts the higher term in memory, fences leadership, and
retries its required log-only publication. If leader establishment or repair leaves a pre-existing
persistence fence and the requested mutation has not run, membership, handoff, DR, and auto-join
surfaces return 503 `control_persistence_indeterminate`, not a generic conflict or a committed
result.

#### `POST /api/v1/admin/cluster/join`

Add a node to the cluster ring.

**Parameters:** `node_id` (or `nodeId`), `endpoint`

**Response:** `200 application/json` — membership operation result.

---

#### `POST /api/v1/admin/cluster/leave`

Remove a node from the cluster ring.

**Parameters:** `node_id`

**Response:** `200 application/json`.

The current Active leader cannot remove itself through this endpoint. Transfer leadership to
another Active voter first, then submit the former leader's leave through the new leader.

---

#### `POST /api/v1/admin/cluster/recommission`

Re-add a previously removed node.

**Parameters:** `node_id`, `endpoint` (optional)

**Response:** `200 application/json`.

---

#### Shard handoff

Shard handoff is the mechanism for migrating a shard between nodes.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/admin/cluster/handoff/begin` | Start a handoff. Parameters: `shard`, `from_node_id`, `to_node_id`, `activation_ring_version` (optional). |
| `POST` | `/api/v1/admin/cluster/handoff/progress` | Report handoff progress. Parameters: `shard`, `phase`, `copied_rows`, `pending_rows`, `last_error`. |
| `POST` | `/api/v1/admin/cluster/handoff/complete` | Mark a handoff as complete. Parameters: `shard`, `from_node_id`, `to_node_id`. |
| `GET` | `/api/v1/admin/cluster/handoff/status` | Return handoff scheduler state. |

---

#### Digest-based repair

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/admin/cluster/repair/run` | Trigger an immediate repair cycle. |
| `POST` | `/api/v1/admin/cluster/repair/pause` | Pause the repair scheduler. |
| `POST` | `/api/v1/admin/cluster/repair/resume` | Resume the repair scheduler. |
| `POST` | `/api/v1/admin/cluster/repair/cancel` | Cancel an in-progress repair. |
| `GET` | `/api/v1/admin/cluster/repair/status` | Return repair scheduler state. |

---

#### Rebalance

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/admin/cluster/rebalance/run` | Trigger an immediate rebalance. |
| `POST` | `/api/v1/admin/cluster/rebalance/pause` | Pause rebalance. |
| `POST` | `/api/v1/admin/cluster/rebalance/resume` | Resume rebalance. |
| `GET` | `/api/v1/admin/cluster/rebalance/status` | Return rebalance state. |

---

#### Cluster audit log

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/admin/cluster/audit` | Query audit entries. Query params: `limit` (default 100), `operation`, `target_kind`, `target_id`. |
| `GET` | `/api/v1/admin/cluster/audit/export` | Download audit log as newline-delimited JSON. Query params: `limit`, `format` (`jsonl`/`ndjson`). |

---

#### Cluster snapshots

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/admin/cluster/snapshot` | Coordinate a cluster-wide data snapshot. Parameter: `path`. |
| `POST` | `/api/v1/admin/cluster/restore` | Coordinate a cluster-wide restore. Parameters: `snapshot_path`, `restore_root`, optional `report_path`, optional per-node `dataPaths`, and `force_local_leader` (default `false`). |
| `POST` | `/api/v1/admin/cluster/control/snapshot` | Snapshot the Raft control-plane log. Parameter: `path`. |
| `POST` | `/api/v1/admin/cluster/control/restore` | Restore the Raft control-plane log from snapshot. Parameters: `snapshot_path` and `force_local_leader` (default `false`). |

Cluster data restore uses only the capability-gated
`POST /internal/v1/restore_data_budgeted` peer route. A peer without
`budgeted_restore_v1` is rejected as `503 remote_restore_incompatible`; the coordinator never
falls back to the legacy route. The legacy internal alias is retained for compatibility but also
fails closed without the peer's configured offline envelope and uses the same budgeted core API.
An HTTP 413 from a peer retains its stable error code and write-error header.

`restore_root`, every local node target, and `report_path` must be strict descendants of the
coordinator's offline restore root. Before the first node restore, the coordinator rejects a report
path that overlaps the source manifest, a local snapshot source, or a local restored target. The
report itself is written through the same finite coordinator. A report failure after data and
control publication therefore returns HTTP 200 with `reportPending: true`, `degraded: true`, and a
bounded `reportDetail`; it does not misclassify the already-committed restore as rejected.

A control restore whose log replacement and parent-directory sync succeed but whose mirror repair
remains pending still returns success with `checkpointPending: true`, `degraded: true`, and
`checkpointDetail`. Cluster-wide restore uses `controlCheckpointPending`, `degraded`, and
`controlCheckpointDetail`. Typed disk resource rejection before authoritative publication returns
HTTP 413 with `write_disk_quota_exceeded`. If only post-publication finalization or cleanup remains,
control restore returns `cleanupDebt: true` and `cleanupDetail`; cluster-wide restore uses
`controlCleanupDebt` and `controlCleanupDetail`.

Creating either snapshot returns HTTP 503 `control_persistence_indeterminate` while control
authority is fenced, a durable candidate is pending, or a mirror checkpoint needs repair.
Cleanup-only debt remains exportable because both persistent files already describe the same
authoritative checkpoint.

Control recovery snapshots carry `steppedDownTerm`; older bundles without the field decode it as
zero. A normal restore merges the live and restored step-down floors.
`force_local_leader=true` (or `forceLocalLeader`) explicitly clears that floor, advances the term,
and assigns leadership to the local node, and should be used only for intentional recovery.

---

### Managed control plane

These endpoints manage deployment records in the embedded control-plane store. They are only available when the managed control-plane feature is configured.

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/admin/control-plane/state` | Return full control-plane state snapshot. |
| `GET` | `/api/v1/admin/control-plane/audit` | Query control-plane audit log. |
| `POST` | `/api/v1/admin/control-plane/deployments/provision` | Provision or update a managed deployment record. |
| `POST` | `/api/v1/admin/control-plane/deployments/backup-policy` | Apply a backup policy to a managed deployment. |
| `POST` | `/api/v1/admin/control-plane/deployments/backup-run` | Record a completed backup run. |
| `POST` | `/api/v1/admin/control-plane/deployments/maintenance` | Apply a maintenance window to a deployment. |
| `POST` | `/api/v1/admin/control-plane/deployments/upgrade` | Apply an upgrade intent to a deployment. |
| `POST` | `/api/v1/admin/control-plane/tenants/apply` | Create or update a managed tenant record. |
| `POST` | `/api/v1/admin/control-plane/tenants/lifecycle` | Apply a lifecycle transition to a managed tenant. |

All control-plane mutation requests require a JSON body. Responses follow `{"status":"success","data":{"deployment"|"tenant": ...}}`.

---

## Error codes summary

| HTTP status | Meaning |
|---|---|
| `200` | Success. |
| `204` | Success with no body (InfluxDB write). |
| `400` | Invalid request parameters or body. |
| `401` | Missing or invalid authentication token. |
| `403` | Token has insufficient scope. |
| `404` | Path not found. |
| `409` | Conflict (e.g. scheduler already running, duplicate provisioning). |
| `413` | Request or definitive write admission exceeds a configured quota, including local-disk quota/headroom. |
| `415` | Unsupported `Content-Type`. |
| `422` | Semantically unsupported or policy-rejected data, including retention/future-skew bounds, or a disabled payload feature. |
| `429` | Retryable admission pressure or write timeout. |
| `500` | Internal server error. |
| `503` | Required subsystem unavailable, or cluster control persistence is fenced/indeterminate. |
| `507` | A persistent queue or local storage resource could not accept more data. |

On write and read admission errors the response also sets:

| Header | Description |
|---|---|
| `X-Tsink-Write-Error-Code` | Machine-readable write rejection code. |
| `X-Tsink-Read-Error-Code` | Machine-readable read rejection code. |

Write diagnostics are bounded. Expected canonical row rejections use stable reason-specific codes;
malformed backend results use `write_invalid_outcome`, and post-dispatch failures with unknown
commit state are explicitly marked indeterminate rather than reported as a definite rejection.
