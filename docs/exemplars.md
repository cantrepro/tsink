# Exemplars

Exemplars are high-cardinality annotations attached to metric samples. Each exemplar links a measured value to an external trace, request ID, or other context label set. tsink stores exemplars in a dedicated sidecar store that is separate from the main time-series engine, exposes them through a Prometheus-compatible query endpoint, and fans out reads across cluster nodes transparently.

---

## Table of Contents

1. [Data Model](#data-model)
2. [Ingestion](#ingestion)
   - [Prometheus Remote Write](#prometheus-remote-write)
   - [Prometheus Text Exposition import](#prometheus-text-exposition-import)
   - [OTLP](#otlp)
3. [Querying](#querying)
4. [Cardinality Limits](#cardinality-limits)
5. [Eviction Policy](#eviction-policy)
6. [Persistence](#persistence)
7. [Cluster behaviour](#cluster-behaviour)
8. [Observability](#observability)
9. [Multi-tenancy](#multi-tenancy)
10. [Snapshots](#snapshots)

---

## Data Model

An exemplar record is composed of five fields:

| Field | Type | Description |
|---|---|---|
| `metric` | string | Name of the parent metric (e.g. `http_request_duration_seconds`). |
| `series_labels` | label set | Key-value labels that identify the series the exemplar belongs to (e.g. `{job="api", instance="host-1"}`). |
| `exemplar_labels` | label set | Annotations carried by the exemplar itself (e.g. `{traceID="abc123"}`). |
| `timestamp` | i64 | Sample timestamp in the server's configured precision. |
| `value` | f64 | The measured value (e.g. a latency observation `0.042`). |

The combination of `metric` + `series_labels` forms a **series key**. Exemplars for the same series key are stored together and ordered by timestamp. Each timestamp within a series is unique; writing a new exemplar with an existing timestamp for the same series replaces the previous record.

---

## Ingestion

Exemplars arrive through three ingestion paths.

### Prometheus Remote Write

**Endpoint:** `POST /api/v1/write`

Exemplars are read from the `exemplars` field of each `TimeSeries` entry in the protobuf `WriteRequest`. They are normalised alongside the scalar samples, then written to the exemplar store.

**Feature flag:**

| Variable | Default | Description |
|---|---|---|
| `TSINK_REMOTE_WRITE_EXEMPLARS_ENABLED` | `true` | Accept exemplars in Prometheus remote-write payloads. Set to `false` to silently discard all exemplars (returns `422` instead). |

**Per-request limit:** requests containing more than `max_exemplars_per_request` exemplars are rejected with `413`.

**Response headers** (present when the request contains at least one exemplar):

| Header | Description |
|---|---|
| `X-Tsink-Exemplars-Accepted` | Number of exemplars written to the store. |
| `X-Tsink-Exemplars-Dropped` | Number of exemplars evicted during the write due to per-series or global cardinality limits. |

### Prometheus Text Exposition import

**Endpoint:** `POST /api/v1/import/prometheus`

The text-format parser extracts exemplar annotations (the `# {<labels>} <value> <timestamp>` suffix allowed on histogram and summary observations) alongside the metric rows. All samples and exemplars in the body are processed as a single atomic batch. The same `max_exemplars_per_request` limit and `TSINK_REMOTE_WRITE_EXEMPLARS_ENABLED` flag apply.

### OTLP

**Endpoint:** `POST /v1/metrics`

Exemplar fields embedded in OTLP gauge, sum, histogram, summary, and exponential-histogram data points are extracted during normalisation and forwarded to the exemplar store. OTLP exemplars are subject to the same per-request limit.

---

## Querying

**Endpoint:** `GET|POST /api/v1/query_exemplars`

The exemplar query endpoint takes a PromQL expression and a time range, extracts all vector and matrix selectors from the expression, resolves them against the exemplar store, and returns every matching exemplar within the requested window.

**Parameters:**

| Parameter | Required | Description |
|---|---|---|
| `query` | Yes | PromQL expression. All vector (`metric{...}`) and matrix (`metric{...}[d]`) selectors are used as lookups. |
| `start` | Yes | Start of the query range. Numeric values are Unix seconds, matching the Prometheus HTTP API; RFC 3339 is also accepted. |
| `end` | Yes | End of the query range. Must not be before `start`. Numeric values use the same format as `start`. |
| `limit` | No | Maximum total exemplars to return. Defaults to `max_query_results`. Must be > 0 and ≤ `max_query_results`. |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": [
    {
      "seriesLabels": {
        "__name__": "http_request_duration_seconds",
        "job": "api",
        "instance": "host-1"
      },
      "exemplars": [
        {
          "labels": {"traceID": "abc123def456"},
          "value": "0.042",
          "timestamp": 1700000000.000
        }
      ]
    }
  ]
}
```

The `X-Tsink-Exemplar-Limit` response header carries the effective `limit` value that was applied.

**Selector extraction rules:** tsink walks the full PromQL AST and collects every `VectorSelector` and the vector component of every `MatrixSelector`. Subqueries, binary expressions, aggregations, and function calls are all traversed recursively. The query is rejected with `bad_data` if no selectors are found (e.g. the expression is a bare number literal).

**Selector count limit:** queries that expand to more selectors than `max_query_selectors` (default 32) are rejected with `bad_data`.

---

## Cardinality Limits

The exemplar store enforces two independent bounds. All five parameters are fixed at startup with no hot-reload; they must be set before the store is opened.

Currently the limits are compiled-in defaults. Override them by building a custom server binary and passing an `ExemplarStoreConfig` to `ExemplarStore::open_with_config`.

| Config field | Default | Description |
|---|---|---|
| `max_total_exemplars` | 50,000 | Hard ceiling on the total number of exemplars held in memory across all series. |
| `max_exemplars_per_series` | 128 | Maximum exemplars retained per individual series (metric + label set). |
| `max_exemplars_per_request` | 512 | Maximum exemplars accepted in a single ingestion request. Exceeding this returns `413`. |
| `max_query_results` | 1,000 | Maximum total exemplars returned by a single `query_exemplars` call. |
| `max_query_selectors` | 32 | Maximum number of PromQL selectors extracted from a single `query_exemplars` expression. |

All five values must be greater than zero; the store refuses to open if any of them is zero.

---

## Eviction Policy

Eviction is triggered synchronously at write time whenever a limit is exceeded. There is no background eviction loop.

**Per-series eviction:** after inserting an exemplar into a series, if that series now holds more than `max_exemplars_per_series` entries, the oldest exemplar (smallest timestamp) is removed. This is applied once per new exemplar written.

**Global eviction:** after processing all exemplars in a batch, if the total count across all series exceeds `max_total_exemplars`, the globally oldest exemplar (smallest timestamp, with series identity as a tiebreaker) is removed repeatedly until the total falls back to the limit.

Exemplars removed by either rule are counted in `dropped_total` (visible on `/metrics` as `tsink_exemplars_dropped_total`) and reported in the `X-Tsink-Exemplars-Dropped` response header.

---

## Persistence

The exemplar store is persisted to a single JSON file named `exemplar-store.json` inside the server's data directory. The file uses a versioned format:

```json
{
  "magic": "tsink-exemplar-store",
  "schema_version": 1,
  "entries": [
    {
      "metric": "http_request_duration_seconds",
      "series_labels": [{"name": "job", "value": "api"}],
      "exemplar_labels": [{"name": "traceID", "value": "abc123"}],
      "timestamp": 1700000000000,
      "value": 0.042
    }
  ]
}
```

Writes are atomic: the new content is written to `exemplar-store.tmp`, `fsync`ed, then renamed over `exemplar-store.json`. This prevents partial writes from corrupting the store across crashes.

On startup the file is loaded back in full and reconstructed into the in-memory BTree index. If the file does not exist the store starts empty. A magic-string or schema-version mismatch causes the server to refuse to start with an error.

When no data path is configured (e.g. in unit tests or certain embedded contexts) the store operates entirely in memory and the file is never written.

---

## Cluster behaviour

In a multi-node cluster, exemplars are written to the local node that receives the ingest request. There is no replication of the exemplar store across nodes; each node holds the exemplars it ingested.

Query fan-out is performed transparently at query time:

1. The receiving node executes the query against its local exemplar store.
2. For each peer node in the cluster membership it issues an internal `POST /internal/v1/query_exemplars` RPC.
3. All results are merged and deduplicated. Records for the same series (identified by metric name + sorted labels) and the same timestamp are deduplicated; the first occurrence wins.
4. The merged result is truncated to `limit` before being returned to the client.

Two cluster capability flags gate the internal protocol:

| Capability | Description |
|---|---|
| `exemplar_ingest_v1` | Node accepts the internal exemplar ingest protocol. |
| `exemplar_query_v1` | Node accepts the internal exemplar query protocol. |

Before routing a batch containing exemplars to a peer, the write path issues a zero-row preflight capability check. If the peer does not advertise `exemplar_ingest_v1`, the request is rejected with `409`.

---

## Observability

### `/metrics` (Prometheus exposition)

All exemplar metrics are in the `tsink_exemplar*` family:

| Metric | Type | Description |
|---|---|---|
| `tsink_exemplars_accepted_total` | counter | Exemplars successfully written to the store. |
| `tsink_exemplars_rejected_total` | counter | Exemplars rejected before reaching the store (disabled feature flag, quota exceeded). |
| `tsink_exemplars_dropped_total` | counter | Exemplars evicted from the store by the per-series or global cardinality limit. |
| `tsink_exemplars_query_requests_total` | counter | `query_exemplars` calls served by the local store. |
| `tsink_exemplars_query_series_total` | counter | Total series rows returned by exemplar queries. |
| `tsink_exemplars_query_results_total` | counter | Total individual exemplar records returned by exemplar queries. |
| `tsink_exemplars_stored{kind="series"}` | gauge | Series currently present in the store. |
| `tsink_exemplars_stored{kind="exemplars"}` | gauge | Individual exemplars currently held in memory. |
| `tsink_exemplar_limits{kind="max_total"}` | gauge | Configured `max_total_exemplars`. |
| `tsink_exemplar_limits{kind="max_per_series"}` | gauge | Configured `max_exemplars_per_series`. |
| `tsink_exemplar_limits{kind="max_per_request"}` | gauge | Configured `max_exemplars_per_request`. |
| `tsink_exemplar_limits{kind="max_query_results"}` | gauge | Configured `max_query_results`. |
| `tsink_exemplar_limits{kind="max_query_selectors"}` | gauge | Configured `max_query_selectors`. |

### `/api/v1/status/tsdb`

The status endpoint includes an `exemplarStore` sub-object with `maxPerRequest`, `acceptedTotal`, and `rejectedTotal` counters.

---

## Multi-tenancy

When multi-tenancy is enabled, each ingest request is associated with a tenant ID. The tenant ID is appended as the reserved label `__tsink_tenant__` on every series key stored in the exemplar store. Queries are automatically scoped to the requesting tenant's series; cross-tenant exemplar results are never visible to another tenant's queries.

---

## Snapshots

When an admin snapshot is triggered via `POST /admin/v1/snapshot`, the exemplar store is included in the snapshot output. The snapshot operation calls `snapshot_into` which copies the current in-memory state (serialised in the same versioned JSON format) into the snapshot directory as `exemplar-store.json`. The snapshot is taken under the same read lock used for queries, so it is consistent with the in-memory state at the time of the snapshot.
