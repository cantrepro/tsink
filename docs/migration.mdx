# Migration guide

`tsink-migrate` is the official tool for importing historical data into tsink from an existing metrics system. It handles the full migration lifecycle: backfilling raw data, verifying correctness, and performing a final readiness check before you cut traffic over.

---

## Contents

1. [Overview](#1-overview)
2. [Supported sources](#2-supported-sources)
3. [Installation & invocation](#3-installation--invocation)
4. [Migration plan file](#4-migration-plan-file)
   - [Source configuration](#41-source-configuration)
   - [Target configuration](#42-target-configuration)
   - [Selectors](#43-selectors)
   - [Optional fields](#44-optional-fields)
   - [Batch tuning](#45-batch-tuning)
   - [Comparison tolerances](#46-comparison-tolerances)
5. [Commands](#5-commands)
   - [backfill](#51-backfill)
   - [verify](#52-verify)
   - [cutover-check](#53-cutover-check)
6. [Capture manifests](#6-capture-manifests)
7. [Artifacts & reports](#7-artifacts--reports)
8. [Per-source examples](#8-per-source-examples)
   - [Prometheus](#81-prometheus)
   - [VictoriaMetrics](#82-victoriametrics)
   - [OTLP](#83-otlp)
   - [InfluxDB line protocol](#84-influxdb-line-protocol)
   - [StatsD](#85-statsd)
   - [Graphite](#86-graphite)
9. [Recommended workflow](#9-recommended-workflow)

---

## 1. Overview

Migration happens in three sequential steps:

1. **Backfill** — pull historical data from the source and write it into tsink via Prometheus remote write.
2. **Verify** — compare series counts, sample counts, metadata, and exemplars between source and destination over the same time window.
3. **Cutover check** — re-run verification plus validate that tsink's ingest surface supports every payload type required by the source.

All three steps are driven by a single JSON plan file that describes the source, the target, and what to check.

---

## 2. Supported sources

| Source kind | Plan `kind` value | Data access model |
|---|---|---|
| Prometheus | `prometheus` | Live — Prometheus remote read API |
| VictoriaMetrics | `victoriametrics` | Live — `/api/v1/export` NDJSON endpoint |
| OTLP | `otlp` | Capture manifest — pre-recorded protobuf payloads |
| InfluxDB line protocol | `influx_line_protocol` | Capture manifest — pre-recorded text payloads |
| StatsD | `statsd` | Capture manifest — pre-recorded UDP packet text |
| Graphite plaintext | `graphite_plaintext` | Capture manifest — pre-recorded plaintext lines |

Prometheus and VictoriaMetrics pull data directly from the live source system over HTTP. OTLP, InfluxDB, StatsD, and Graphite require you to capture a representative sample of payloads ahead of time and point the plan at that capture manifest file.

---

## 3. Installation & invocation

The tool ships as a separate binary in the `tsink-server` crate. Build it alongside the server:

```bash
cargo build -p tsink-server --bin tsink-migrate --release
```

A convenience wrapper is provided at [`scripts/tsink_migrate.sh`](../scripts/tsink_migrate.sh) that proxies arguments directly:

```bash
./scripts/tsink_migrate.sh <command> --config plan.json --start-ms <ms> --end-ms <ms>
```

Or invoke the binary directly:

```
tsink-migrate <backfill|verify|cutover-check>
  --config     <plan.json>        required  path to the migration plan
  --start-ms   <unix_ms>          required  start of the time window (milliseconds)
  --end-ms     <unix_ms>          required  end of the time window (milliseconds)
  --artifact-dir <dir>            optional  directory to write JSON + Markdown reports
```

`--end-ms` must be greater than or equal to `--start-ms`. On success the tool exits with code `0`; on failure (verify issues, cutover issues, or an unrecoverable error) it exits with code `1`.

---

## 4. Migration plan file

The plan is a JSON object. All paths in the plan are interpreted relative to the plan file's directory unless they are absolute.

### 4.1 Source configuration

```json
"source": {
  "kind": "<source_kind>",
  "headers": { "Authorization": "Bearer <token>" },
  "remote_read_url":   "http://prometheus:9090/api/v1/read",
  "export_url":        "http://victoria:8428/api/v1/export",
  "query_range_url":   "http://prometheus:9090/api/v1/query_range",
  "metadata_url":      "http://prometheus:9090/api/v1/metadata",
  "exemplar_url":      "http://prometheus:9090/api/v1/query_exemplars",
  "capture_manifest_path": "capture.json"
}
```

| Field | Required by | Description |
|---|---|---|
| `kind` | all | Source system identifier. See [Supported sources](#2-supported-sources). |
| `headers` | optional | HTTP headers added to every request to the source. Use for authentication tokens. |
| `remote_read_url` | `prometheus` | Prometheus remote read endpoint. |
| `export_url` | `victoriametrics` | VictoriaMetrics `/api/v1/export` endpoint (NDJSON format). |
| `query_range_url` | optional | Used by `cutover-check` PromQL parity checks when the source is Prometheus or VictoriaMetrics. |
| `metadata_url` | optional | Source metadata endpoint (`/api/v1/metadata`). If omitted, metadata backfill and verification are skipped. |
| `exemplar_url` | optional (Prometheus only) | Source exemplar query endpoint. If omitted, exemplar backfill is skipped. |
| `capture_manifest_path` | `otlp`, `influx_line_protocol`, `statsd`, `graphite_plaintext` | Path to the capture manifest JSON file. See [Capture manifests](#6-capture-manifests). |

### 4.2 Target configuration

```json
"target": {
  "write_url":       "http://tsink:9201/api/v1/write",
  "read_url":        "http://tsink:9201/api/v1/read",
  "query_range_url": "http://tsink:9201/api/v1/query_range",
  "metadata_url":    "http://tsink:9201/api/v1/metadata",
  "exemplar_url":    "http://tsink:9201/api/v1/query_exemplars",
  "status_url":      "http://tsink:9201/api/v1/status/tsdb",
  "tenant":          "default",
  "headers":         { "Authorization": "Bearer <token>" }
}
```

| Field | Required | Description |
|---|---|---|
| `write_url` | yes | tsink Prometheus remote write endpoint. Used by `backfill`. |
| `read_url` | yes | tsink Prometheus remote read endpoint. Used by `verify` and `cutover-check`. |
| `query_range_url` | optional | Used by `cutover-check` PromQL parity checks. |
| `metadata_url` | optional | Used when metadata verification or backfill is enabled. |
| `exemplar_url` | optional | Used when exemplar verification is configured. |
| `status_url` | optional | tsink `/api/v1/status/tsdb` endpoint. When provided, `cutover-check` probes whether required ingest payload types (metadata, exemplars, histograms, OTLP, InfluxDB, StatsD, Graphite) are enabled on the target. |
| `tenant` | optional | Target tenant name. Sent as `X-Tsink-Tenant` on every request. Defaults to `default`. |
| `headers` | optional | Additional HTTP headers for every request to tsink (for per-tenant auth tokens, etc.). |

### 4.3 Selectors

```json
"selectors": [
  "http_requests_total{job=\"api\"}",
  "node_cpu_seconds_total{mode=~\"idle|iowait\"}"
]
```

A list of one or more PromQL-style label selectors. At least one selector is required. During backfill the tool fetches all series matching any selector. During verify each selector is checked independently. Selectors support `=`, `!=`, `=~`, `!~` matcher operators.

### 4.4 Optional fields

```json
"metadata_metrics": ["http_requests_total", "node_cpu_seconds_total"],
"exemplar_checks": [
  { "query": "http_request_duration_seconds_bucket{job=\"api\"}", "limit": 200 }
],
"promql_checks": [
  { "query": "sum(rate(http_requests_total[5m])) by (job)", "step": "30s" }
]
```

**`metadata_metrics`** — metric names whose type/help/unit metadata should be backfilled and verified. When omitted the tool derives a list automatically from metric names it can extract from the `selectors`.

**`exemplar_checks`** — exemplar queries to run during verify and cutover-check. Each entry has:
- `query` — a PromQL selector to identify the set of series (required)
- `limit` — maximum number of exemplars to retrieve per series (default: `200`)

When no `exemplar_checks` are specified the tool falls back to running one check per selector.

**`promql_checks`** — PromQL expressions compared between source and tsink during `cutover-check`. Each entry has:
- `query` — any PromQL expression (required)
- `step` — query range step (default: `"30s"`)

PromQL checks require `query_range_url` to be set on both source and target, and are only meaningful for Prometheus and VictoriaMetrics sources.

### 4.5 Batch tuning

```json
"batch": {
  "max_series_per_write": 250,
  "max_points_per_write": 25000,
  "http_timeout_secs": 30
}
```

Controls how data is batched during backfill writes.

| Field | Default | Description |
|---|---|---|
| `max_series_per_write` | `250` | Maximum number of series per remote write request. |
| `max_points_per_write` | `25000` | Maximum total data points across all series per request. |
| `http_timeout_secs` | `30` | HTTP request timeout for all source and target calls. |

### 4.6 Comparison tolerances

```json
"compare": {
  "max_absolute_value_delta": 1e-12,
  "max_relative_value_delta": 1e-9
}
```

Sample values are compared within these tolerances during `verify` and `cutover-check`.

| Field | Default | Description |
|---|---|---|
| `max_absolute_value_delta` | `1e-12` | Maximum allowed absolute difference between source and target sample values. |
| `max_relative_value_delta` | `1e-9` | Maximum allowed relative difference (as a fraction of the source value). |

---

## 5. Commands

### 5.1 backfill

```bash
tsink-migrate backfill --config plan.json --start-ms 1700000000000 --end-ms 1700086400000
```

Fetches all series matching the plan selectors from the source within `[start-ms, end-ms]` and writes them to tsink in batched Prometheus remote write requests. Additionally backfills metric metadata (if `metadata_url` is configured on the source) and exemplars (if `exemplar_url` is configured and the source is Prometheus).

**Exit code:** `0` on success, `1` on any transport or write error.

**Console output on success:**
```
tsink-migrate backfill: pass
  source_kind: Prometheus
  selectors: 2
  series: 18
  samples: 43200
  histograms: 0
  exemplars: 0
  metadata: 2
  write_batches: 3
```

### 5.2 verify

```bash
tsink-migrate verify --config plan.json --start-ms 1700000000000 --end-ms 1700086400000
```

Compares source and target data for the specified window. The check has three layers:

1. **Raw checks** — per selector: series count, row count, sample count, histogram count, missing/extra series, and per-sample value mismatch within the configured tolerances.
2. **Metadata checks** — per metric: whether the type/help/unit metadata entries match.
3. **Exemplar checks** — per query: series count and per-exemplar value comparison.

Any discrepancy is recorded as an issue. The command exits with `1` if any issues are found.

**Console output on pass:**
```
tsink-migrate verify: pass
  raw_checks: 2
  metadata_checks: 2
  exemplar_checks: 2
  issues: 0
```

### 5.3 cutover-check

```bash
tsink-migrate cutover-check --config plan.json --start-ms 1700000000000 --end-ms 1700086400000
```

Runs the full `verify` check and then two additional layers:

1. **Target payload capability probes** — when `status_url` is configured, queries tsink's status endpoint to confirm that the ingest features required by the source are enabled: metadata, exemplars, histograms, OTLP, InfluxDB line protocol, StatsD, or Graphite (depending on the plan's source kind and payload usage). Issues are raised for any feature that is disabled.

2. **PromQL parity checks** — for each entry in `promql_checks`, executes the query range against both source and target and compares the result sets. Mismatches, partial responses, or target warnings are all recorded as issues.

The command exits with `1` if any verify issues, capability issues, or PromQL parity mismatches are found.

---

## 6. Capture manifests

OTLP, InfluxDB line protocol, StatsD, and Graphite sources cannot be queried retroactively via a read API. Instead you record the original inbound payloads to a capture manifest ahead of the migration and replay them locally.

A capture manifest is a JSON array. Each entry describes one payload:

```json
[
  {
    "path": "payloads/batch-001.bin",
    "received_at_ms": 1700000000000,
    "query_params": {}
  },
  {
    "body": "cpu,host=node-a value=1.5 1700000000000",
    "received_at_ms": 1700000000000,
    "query_params": { "db": "telegraf", "precision": "ms" }
  },
  {
    "body_base64": "<base64-encoded protobuf>",
    "received_at_ms": 1700000000000,
    "query_params": {}
  }
]
```

Each entry must have exactly one of:

| Field | Description |
|---|---|
| `path` | Path to the raw payload file (relative to the plan file, or absolute). |
| `body` | Inline payload text (UTF-8). |
| `body_base64` | Base64-encoded payload bytes. Useful for binary formats like OTLP protobuf. |

Additional fields:

| Field | Required for | Description |
|---|---|---|
| `received_at_ms` | `influx_line_protocol`, `statsd`, `graphite_plaintext` | Wall-clock time when the payload arrived. Used as the fallback timestamp for lines that carry no timestamp of their own. |
| `query_params` | InfluxDB only | Query parameters from the original HTTP request. Recognized keys: `db`, `rp`, `bucket`, `org`, `precision`. The `db`, `rp`, `bucket`, and `org` values are promoted to labels (`influx_db`, `influx_rp`, `influx_bucket`, `influx_org`) on ingested series. |

Capture manifests are processed locally — the tool normalizes each payload using the same logic as the live ingest path and then writes the resulting series to tsink via remote write. The `backfill` command imports from the capture manifest; `verify` and `cutover-check` use the same normalized data as the source-side reference.

---

## 7. Artifacts & reports

Pass `--artifact-dir <dir>` to any command to write structured output files into that directory. The directory is created if it does not exist.

```bash
tsink-migrate backfill \
  --config plan.json \
  --start-ms 1700000000000 \
  --end-ms 1700086400000 \
  --artifact-dir ./migration-reports
```

Two files are written per run:

| File | Format | Contents |
|---|---|---|
| `report.json` | JSON | Full structured report with all check details, counters, and issue lists. |
| `report.md` | Markdown | Human-readable summary suitable for attaching to a PR or ticket. |

The JSON report can be used for programmatic validation in CI pipelines. Exit code `1` is raised before any artifact files are written if the command itself fails (e.g. a transport error); artifact files are written only after the command completes.

---

## 8. Per-source examples

### 8.1 Prometheus

Prometheus uses its remote read API for backfill and verification, and optionally its metadata and exemplar APIs.

```json
{
  "source": {
    "kind": "prometheus",
    "remote_read_url": "http://prometheus:9090/api/v1/read",
    "query_range_url": "http://prometheus:9090/api/v1/query_range",
    "metadata_url":    "http://prometheus:9090/api/v1/metadata",
    "exemplar_url":    "http://prometheus:9090/api/v1/query_exemplars",
    "headers": {
      "Authorization": "Bearer <source-token>"
    }
  },
  "target": {
    "write_url":       "http://tsink:9201/api/v1/write",
    "read_url":        "http://tsink:9201/api/v1/read",
    "query_range_url": "http://tsink:9201/api/v1/query_range",
    "metadata_url":    "http://tsink:9201/api/v1/metadata",
    "exemplar_url":    "http://tsink:9201/api/v1/query_exemplars",
    "status_url":      "http://tsink:9201/api/v1/status/tsdb",
    "tenant": "default",
    "headers": {
      "Authorization": "Bearer <target-token>"
    }
  },
  "selectors": [
    "up{job=\"node\"}",
    "http_requests_total{job=~\"api|worker\"}"
  ],
  "metadata_metrics": ["up", "http_requests_total"],
  "exemplar_checks": [
    { "query": "http_request_duration_seconds_bucket{job=\"api\"}", "limit": 200 }
  ],
  "promql_checks": [
    { "query": "sum(rate(http_requests_total[5m])) by (job)", "step": "30s" },
    {
      "query": "histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))",
      "step": "30s"
    }
  ]
}
```

### 8.2 VictoriaMetrics

VictoriaMetrics uses the `/api/v1/export` endpoint (NDJSON format) for backfill and verification. The Prometheus-compatible `/prometheus/api/v1/query_range` and `/prometheus/api/v1/metadata` endpoints are used for PromQL parity and metadata checks.

```json
{
  "source": {
    "kind": "victoriametrics",
    "export_url":      "http://victoria:8428/api/v1/export",
    "query_range_url": "http://victoria:8428/prometheus/api/v1/query_range",
    "metadata_url":    "http://victoria:8428/prometheus/api/v1/metadata"
  },
  "target": {
    "write_url":       "http://tsink:9201/api/v1/write",
    "read_url":        "http://tsink:9201/api/v1/read",
    "query_range_url": "http://tsink:9201/api/v1/query_range",
    "metadata_url":    "http://tsink:9201/api/v1/metadata",
    "status_url":      "http://tsink:9201/api/v1/status/tsdb",
    "tenant": "default"
  },
  "selectors": [
    "node_cpu_seconds_total{mode=\"idle\"}",
    "http_requests_total{job=\"edge\"}"
  ],
  "metadata_metrics": ["node_cpu_seconds_total", "http_requests_total"],
  "promql_checks": [
    { "query": "sum(rate(node_cpu_seconds_total[5m])) by (instance)", "step": "30s" }
  ]
}
```

### 8.3 OTLP

OTLP sources require a capture manifest containing the raw protobuf `ExportMetricsServiceRequest` payloads. Payloads are typically base64-encoded since they are binary. The `cutover-check` command additionally verifies that the tsink OTLP ingest endpoint is enabled and that it supports the OTLP metric shapes present in the captured payloads (gauge, sum, histogram, summary).

**`otlp-capture.json`:**
```json
[
  {
    "body_base64": "<base64-encoded ExportMetricsServiceRequest>"
  }
]
```

**`otlp-plan.json`:**
```json
{
  "source": {
    "kind": "otlp",
    "capture_manifest_path": "otlp-capture.json"
  },
  "target": {
    "write_url":    "http://tsink:9201/api/v1/write",
    "read_url":     "http://tsink:9201/api/v1/read",
    "metadata_url": "http://tsink:9201/api/v1/metadata",
    "status_url":   "http://tsink:9201/api/v1/status/tsdb",
    "tenant": "default"
  },
  "selectors": ["system_x2e_cpu_x2e_time"],
  "metadata_metrics": ["system_x2e_cpu_x2e_time"]
}
```

Note: OTLP metric names that contain `.` or other characters unsafe in Prometheus labels are percent-encoded (`.` → `_x2e_`) by the normalizer. Use the encoded form in selectors and `metadata_metrics`.

### 8.4 InfluxDB line protocol

InfluxDB sources require a capture manifest of raw line protocol HTTP request bodies. The `db`, `rp`, `bucket`, and `org` values from `query_params` are promoted to labels so selectors can filter by database or bucket.

**`influx-capture.json`:**
```json
[
  {
    "received_at_ms": 1700000000000,
    "query_params": { "db": "telegraf", "precision": "ms" },
    "body": "cpu,host=node-a value=1.5,temp=3.0 1700000000000\nmem,host=node-a used=42 1700000005000"
  }
]
```

**`influx-plan.json`:**
```json
{
  "source": {
    "kind": "influx_line_protocol",
    "capture_manifest_path": "influx-capture.json"
  },
  "target": {
    "write_url":    "http://tsink:9201/api/v1/write",
    "read_url":     "http://tsink:9201/api/v1/read",
    "metadata_url": "http://tsink:9201/api/v1/metadata",
    "status_url":   "http://tsink:9201/api/v1/status/tsdb",
    "tenant": "default"
  },
  "selectors": [
    "cpu{host=\"node-a\",influx_db=\"telegraf\"}",
    "cpu_temp{host=\"node-a\",influx_db=\"telegraf\"}"
  ],
  "metadata_metrics": ["cpu", "cpu_temp"]
}
```

### 8.5 StatsD

StatsD sources require a capture manifest of UDP packet text (one or more `metric:value|type` lines per entry). StatsD has no native timestamps; every entry must supply `received_at_ms`.

**`statsd-capture.json`:**
```json
[
  {
    "received_at_ms": 1700000000000,
    "body": "jobs.completed:4|c|#env:prod\nworkers.active:2|g|#env:prod"
  }
]
```

**`statsd-plan.json`:**
```json
{
  "source": {
    "kind": "statsd",
    "capture_manifest_path": "statsd-capture.json"
  },
  "target": {
    "write_url":    "http://tsink:9201/api/v1/write",
    "read_url":     "http://tsink:9201/api/v1/read",
    "metadata_url": "http://tsink:9201/api/v1/metadata",
    "status_url":   "http://tsink:9201/api/v1/status/tsdb",
    "tenant": "default"
  },
  "selectors": [
    "jobs_completed{env=\"prod\"}",
    "workers_active{env=\"prod\"}"
  ],
  "metadata_metrics": ["jobs_completed", "workers_active"]
}
```

### 8.6 Graphite

Graphite sources require a capture manifest of TCP plaintext lines. Graphite timestamps are in Unix seconds; `received_at_ms` is used as a fallback only for lines that omit the timestamp field.

**`graphite-capture.json`:**
```json
[
  {
    "received_at_ms": 1700000000999,
    "body": "servers.api.latency;env=prod;region=us-west 42.5 1700000000\nservers.api.errors 1 1700000001"
  }
]
```

**`graphite-plan.json`:**
```json
{
  "source": {
    "kind": "graphite_plaintext",
    "capture_manifest_path": "graphite-capture.json"
  },
  "target": {
    "write_url":    "http://tsink:9201/api/v1/write",
    "read_url":     "http://tsink:9201/api/v1/read",
    "metadata_url": "http://tsink:9201/api/v1/metadata",
    "status_url":   "http://tsink:9201/api/v1/status/tsdb",
    "tenant": "default"
  },
  "selectors": [
    "servers_api_latency{env=\"prod\",region=\"us-west\"}",
    "servers_api_errors"
  ],
  "metadata_metrics": ["servers_api_latency", "servers_api_errors"]
}
```

---

## 9. Recommended workflow

A safe migration sequence from any supported source:

**Step 1 — Prepare the plan.** Write a plan JSON for your source and target. For live sources (Prometheus, VictoriaMetrics) verify the API URLs are reachable. For capture-manifest sources, record a representative set of payloads.

**Step 2 — Backfill historical data.** Choose a time window that covers the retention you want to import. For large windows run backfill in overlapping slices if memory or network constraints require it.

```bash
tsink-migrate backfill \
  --config plan.json \
  --start-ms 1696118400000 \
  --end-ms   1700000000000 \
  --artifact-dir ./artifacts/backfill
```

**Step 3 — Verify a recent window.** Pick a short window near the present where the source still has fresh data and tsink has just received the backfill. Check that series counts and sample values agree.

```bash
tsink-migrate verify \
  --config plan.json \
  --start-ms 1699913600000 \
  --end-ms   1700000000000 \
  --artifact-dir ./artifacts/verify
```

Review any issues in the console output or in `artifacts/verify/report.md`. Common causes:
- **Missing series on target** — the series was created during the backfill window but not matched by the selector; broaden the selector or re-run backfill.
- **Sample count mismatch** — the source had data outside the window used for backfill; adjust `--start-ms` / `--end-ms`.
- **Metadata mismatch** — the source metadata endpoint was unreachable; check `source.metadata_url`.

**Step 4 — Run cutover-check.** Before switching write traffic, confirm that tsink is ready to accept every payload type the source uses.

```bash
tsink-migrate cutover-check \
  --config plan.json \
  --start-ms 1699913600000 \
  --end-ms   1700000000000 \
  --artifact-dir ./artifacts/cutover
```

A `pass` result means:
- Data verification passed.
- All required tsink ingest features (metadata, exemplars, histograms, protocol-specific endpoints) are enabled.
- PromQL query results match between source and tsink (when `promql_checks` are configured).

**Step 5 — Switch write traffic.** Reconfigure your instrumentation, scrape targets, or upstream forwarders to send new data directly to tsink. The source can be kept running in read-only mode for a grace period while operators confirm the cutover is clean.
