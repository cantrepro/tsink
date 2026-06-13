# Multi-tenancy

tsink's multi-tenancy model gives every tenant a fully isolated data namespace with independent quotas, admission budgets, authentication tokens, and lifecycle state — all sharing a single storage engine with zero cross-tenant data leakage.

---

## How tenants are identified

Every HTTP request carries the tenant ID in a header:

```
x-tsink-tenant: <tenant-id>
```

If the header is absent the request is attributed to the built-in `"default"` tenant. Tenant IDs are validated on each request: they must be non-empty, not exceed the maximum label value length, and contain no control characters.

### Protocol-specific identification

Sources that do not use HTTP headers resolve a tenant statically:

| Source | How the tenant is set |
|---|---|
| Prometheus Remote Write / Remote Read | `X-Scope-OrgID` or `x-tsink-tenant` header |
| StatsD (UDP) | `--statsd-tenant <id>` server flag (default: `"default"`) |
| Graphite (TCP) | `--graphite-tenant <id>` server flag (default: `"default"`) |
| Edge-sync upstream | `--edge-sync-static-tenant <id>` rewrites all tenant labels before forwarding |

---

## Storage isolation

Isolation is enforced at the storage layer through the `TenantScopedStorage` wrapper, which sits between the HTTP handlers and the core storage engine.

**On write** — the reserved label `__tsink_tenant__` is automatically appended to every written row. Clients cannot set this label directly; any inbound write or query that contains `__tsink_tenant__` is rejected immediately.

**On read** — an equality matcher for `__tsink_tenant__ = <tenant-id>` is automatically injected into every query, series listing, and label enumeration. A tenant can only ever see data it owns.

**Default tenant compatibility** — the `"default"` tenant also performs an unlabeled fallback query so that pre-tenancy series (written without the label) remain accessible.

---

## Tenant configuration file

Pass a JSON policy file at startup:

```bash
tsink-server --tenant-config /etc/tsink/tenants.json
```

The file has a `defaults` block whose values are inherited by every tenant, and a `tenants` map of per-tenant overrides. Any tenant not listed in the file automatically gets the `defaults` policy on its first request.

### Full example

```jsonc
{
  "defaults": {
    "quotas": {
      "maxWriteRowsPerRequest": 50000,
      "maxReadQueriesPerRequest": 10,
      "maxMetadataMatchersPerRequest": 100,
      "maxQueryLengthBytes": 8192,
      "maxRangePointsPerQuery": 5000000
    },
    "admission": {
      "maxInflightReads": 32,
      "maxInflightWrites": 32,
      "ingest":    { "maxInflightRequests": 64, "maxInflightUnits": 200000 },
      "query":     { "maxInflightRequests": 32 },
      "metadata":  { "maxInflightRequests": 16 },
      "retention": { "maxInflightRequests": 4  }
    }
  },
  "tenants": {
    "acme": {
      "auth": {
        "tokens": [
          { "token": "acme-write-secret", "scopes": ["write"] },
          { "token": "acme-read-secret",  "scopes": ["read"]  }
        ]
      },
      "quotas": {
        "maxWriteRowsPerRequest": 10000,
        "maxRangePointsPerQuery": 1000000
      },
      "admission": {
        "ingest":    { "maxInflightRequests": 50, "maxInflightUnits": 100000 },
        "query":     { "maxInflightRequests": 20 },
        "metadata":  { "maxInflightRequests": 10 },
        "retention": { "maxInflightRequests": 5  }
      },
      "cluster": {
        "writeConsistency":    "all",
        "readConsistency":     "strict",
        "readPartialResponse": "deny"
      }
    }
  }
}
```

### Quota fields

All quota fields are optional. Unset fields fall back to the `defaults` block, then to the server's built-in defaults (unlimited if not specified).

| Field | Description |
|---|---|
| `maxWriteRowsPerRequest` | Maximum rows accepted in a single write request |
| `maxReadQueriesPerRequest` | Maximum queries in a single remote-read batch |
| `maxMetadataMatchersPerRequest` | Maximum label matchers in a single metadata request |
| `maxQueryLengthBytes` | Maximum byte length of a PromQL query string |
| `maxRangePointsPerQuery` | Maximum time-series data points returned by a range query |

Requests that exceed a quota are rejected with `HTTP 400` before any storage work is done.

### Admission budget fields

Admission limits cap concurrent in-flight work. All limits are enforced with non-blocking semaphores — a request that cannot acquire a permit is rejected immediately with `HTTP 429 Too Many Requests` and a `Retry-After: 1` header.

| Field | Level | Description |
|---|---|---|
| `maxInflightReads` | Tenant | Total concurrent read requests across all read surfaces |
| `maxInflightWrites` | Tenant | Total concurrent write requests across all write surfaces |
| `ingest.maxInflightRequests` | Surface | Concurrent ingest HTTP requests |
| `ingest.maxInflightUnits` | Surface | Concurrent ingest rows (units = row count) |
| `query.maxInflightRequests` | Surface | Concurrent query requests |
| `metadata.maxInflightRequests` | Surface | Concurrent metadata/series/label requests |
| `retention.maxInflightRequests` | Surface | Concurrent retention/deletion operations |

### Per-tenant cluster consistency

The `cluster` block overrides the server-wide consistency defaults for a specific tenant:

| Field | Values | Description |
|---|---|---|
| `writeConsistency` | `"one"`, `"quorum"`, `"all"` | Replication acknowledgement requirement for writes |
| `readConsistency` | `"one"`, `"quorum"`, `"strict"` | Quorum requirement for reads |
| `readPartialResponse` | `"allow"`, `"deny"` | Whether to return partial results when some shards are unavailable |

---

## Global admission limits

In addition to per-tenant budgets, server-wide admission guards apply across all tenants. These are controlled by environment variables:

| Environment variable | Default | Description |
|---|---|---|
| `TSINK_SERVER_WRITE_MAX_INFLIGHT_REQUESTS` | 64 | Global max concurrent write requests |
| `TSINK_SERVER_WRITE_MAX_INFLIGHT_ROWS` | 200,000 | Global max in-flight write rows |
| `TSINK_SERVER_WRITE_RESOURCE_ACQUIRE_TIMEOUT_MS` | 25 | Timeout (ms) waiting for the write semaphore |
| `TSINK_SERVER_READ_MAX_INFLIGHT_REQUESTS` | 64 | Global max concurrent read requests |
| `TSINK_SERVER_READ_MAX_INFLIGHT_QUERIES` | 128 | Global max in-flight query slots |
| `TSINK_SERVER_READ_RESOURCE_ACQUIRE_TIMEOUT_MS` | 25 | Timeout (ms) waiting for the read semaphore |

A request must pass both the global guard and the per-tenant budget before being admitted.

---

## Per-tenant authentication tokens

The `auth.tokens` list in the tenant config file defines bearer tokens scoped to that tenant:

```jsonc
"auth": {
  "tokens": [
    { "token": "acme-write-secret", "scopes": ["write"] },
    { "token": "acme-read-secret",  "scopes": ["read"]  }
  ]
}
```

A `write`-scoped token grants write access to that tenant only. A `read`-scoped token grants read access to that tenant only. Tokens cannot cross tenant boundaries.

These per-tenant tokens are evaluated before the global security manager token. See the [security model](security.md) for OIDC and RBAC configuration.

### RBAC tenant resources

RBAC roles use the `Tenant` resource kind to restrict access by tenant name:

```jsonc
{
  "permissions": [
    { "action": "Write", "resource": { "kind": "Tenant", "name": "*" } },
    { "action": "Read",  "resource": { "kind": "Tenant", "name": "ops" } }
  ]
}
```

A `Tenant / *` wildcard grants access to all tenants; a named entry restricts to a single tenant.

---

## Managed tenants (control plane)

For deployments that need programmatic tenant provisioning, tsink includes a lightweight control-plane store. Managed tenant records carry lifecycle state, storage quotas, and ingest-rate limits that the runtime enforces alongside the static policy file.

### Tenant lifecycle states

| State | Description |
|---|---|
| `provisioning` | Tenant is being set up; not yet accepting traffic |
| `active` | Fully operational |
| `suspended` | Writes and queries are blocked; data is retained |
| `deleting` | Triggered deletion is in progress |
| `deleted` | All data has been removed |

### Provisioning a managed tenant

```bash
curl -X POST http://127.0.0.1:9201/api/v1/admin/control-plane/tenants/apply \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <admin-token>' \
  -d '{
    "tenantId": "acme",
    "deploymentId": "prod-eu",
    "displayName": "ACME Corp",
    "lifecycle": "active",
    "retentionDays": 30,
    "storageLimitBytes": 107374182400,
    "ingestRateLimitPerSec": 100000,
    "queryConcurrencyLimit": 20,
    "labels": { "team": "platform" }
  }'
```

| Field | Type | Description |
|---|---|---|
| `tenantId` | string | Unique, immutable identifier |
| `deploymentId` | string | Logical deployment group this tenant belongs to |
| `displayName` | string | Human-readable name |
| `lifecycle` | string | Target lifecycle state |
| `retentionDays` | integer | Data retention window in days |
| `storageLimitBytes` | integer | Hard storage cap in bytes |
| `ingestRateLimitPerSec` | integer | Maximum ingested rows per second |
| `queryConcurrencyLimit` | integer | Maximum concurrent queries |
| `labels` | object | Arbitrary key-value metadata |

### Transitioning lifecycle state

```bash
curl -X POST http://127.0.0.1:9201/api/v1/admin/control-plane/tenants/lifecycle \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <admin-token>' \
  -d '{
    "tenantId": "acme",
    "lifecycle": "suspended",
    "note": "billing issue"
  }'
```

Both endpoints are idempotent — applying the same state twice is safe.

---

## Usage accounting

tsink records per-tenant resource consumption to an append-only NDJSON ledger. Each record captures:

- **Category** — `ingest`, `query`, `retention`, `background`, or `storage`
- **Operation** — the specific operation (e.g., `prometheus_remote_write`, `promql_range_query`)
- **Counters** — `rows`, `matchedSeries`, `requestBytes`, `logicalStorageBytes`, `durationNanos`

Records are aggregated into per-tenant summaries broken down by category. A storage snapshot is periodically reconciled against live index state and attached to the summary.

### Retrieving usage data

```bash
# Aggregated per-tenant summary
curl 'http://127.0.0.1:9201/api/v1/admin/usage/report?tenant=acme' \
  -H 'Authorization: Bearer <admin-token>'

# Time-bucketed breakdown (hour | day)
curl 'http://127.0.0.1:9201/api/v1/admin/usage/report?tenant=acme&bucket=day' \
  -H 'Authorization: Bearer <admin-token>'

# All tenants
curl 'http://127.0.0.1:9201/api/v1/admin/usage/report' \
  -H 'Authorization: Bearer <admin-token>'

# Raw NDJSON ledger records
curl 'http://127.0.0.1:9201/api/v1/admin/usage/export?tenant=acme' \
  -H 'Authorization: Bearer <admin-token>'

# Force reconcile storage counters
curl -X POST 'http://127.0.0.1:9201/api/v1/admin/usage/reconcile' \
  -H 'Authorization: Bearer <admin-token>'
```

---

## Admin API reference

All admin endpoints require a token with `Admin` or `System` RBAC scope.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/admin/control-plane/tenants/apply` | Create or update a managed tenant record |
| `POST` | `/api/v1/admin/control-plane/tenants/lifecycle` | Transition a tenant's lifecycle state |
| `GET` | `/api/v1/admin/control-plane/state` | Retrieve full control-plane state including all tenant records |
| `GET` | `/api/v1/admin/usage/report` | Aggregated usage summary (accepts `?tenant=` and `?bucket=`) |
| `GET` | `/api/v1/admin/usage/export` | Stream raw usage ledger records (accepts `?tenant=`) |
| `POST` | `/api/v1/admin/usage/reconcile` | Reconcile usage counters against live storage state |
| `GET` | `/api/v1/admin/support_bundle` | Download diagnostic JSON bundle (accepts `?tenant=`) |
| `POST` | `/api/v1/admin/delete_series` | Tombstone-delete series for the requesting tenant |

---

## Data-plane usage

Tenants use all standard data-plane endpoints. The only requirement is the `x-tsink-tenant` header.

```bash
# Write — Prometheus remote write
curl -X POST http://127.0.0.1:9201/api/v1/write \
  -H 'x-tsink-tenant: acme' \
  -H 'Authorization: Bearer acme-write-secret' \
  --data-binary @payload.pb

# Write — Prometheus text exposition
curl -X POST http://127.0.0.1:9201/api/v1/import/prometheus \
  -H 'x-tsink-tenant: acme' \
  -H 'Authorization: Bearer acme-write-secret' \
  -H 'Content-Type: text/plain' \
  -d 'http_requests_total{method="GET"} 1027 1700000000000'

# Query
curl 'http://127.0.0.1:9201/api/v1/query?query=http_requests_total' \
  -H 'x-tsink-tenant: acme' \
  -H 'Authorization: Bearer acme-read-secret'

# Series listing
curl 'http://127.0.0.1:9201/api/v1/series?match[]=http_requests_total' \
  -H 'x-tsink-tenant: acme' \
  -H 'Authorization: Bearer acme-read-secret'
```

---

## Clustering considerations

In a cluster deployment, tenant-scoped queries are fanned out to the relevant shards with the `__tsink_tenant__` matcher injected automatically. The `"default"` tenant skips the matcher injection during fanout to avoid double-scoping unlabeled legacy series.

The hotspot tracker accumulates per-tenant write skew counters. Tenants with a write skew factor exceeding 4× the cluster average are flagged in the cluster snapshot, which can inform rebalancing decisions. See the [clustering internals](clustering-internals.md) guide for details.
