# Ingestion Protocols

tsink exposes four HTTP ingestion endpoints and two optional side-channel listeners (UDP for
StatsD and TCP for Graphite). Prometheus remote read is listed for completeness, but it is a read
protocol rather than an ingestion path.

---

## Protocol overview

| Protocol | Transport | Endpoint(s) | Notes |
|---|---|---|---|
| Prometheus Remote Write | HTTP POST | `POST /api/v1/write` | Snappy block-compressed protobuf |
| Prometheus Remote Read | HTTP POST | `POST /api/v1/read` | Snappy block-compressed protobuf |
| Prometheus Text Exposition | HTTP POST | `POST /api/v1/import/prometheus` | Bulk plain-text import |
| InfluxDB Line Protocol | HTTP POST | `POST /write`, `POST /api/v2/write` | v1 and v2 compatible |
| OTLP HTTP | HTTP POST | `POST /v1/metrics` | Protobuf only |
| StatsD | UDP | `--statsd-listen ADDR` | Disabled by default |
| Graphite | TCP | `--graphite-listen ADDR` | Disabled by default |

---

## Common write results

Successful non-empty HTTP writes report `X-Tsink-Write-Acknowledgement` as `volatile`,
`appended`, or `durable`. The value is the weakest guarantee for every accepted component. A
metadata or exemplar sidecar is outside the core row WAL transaction and therefore weakens the
complete response to `volatile`. A successful empty no-op has no acknowledgement.

Canonical storage rejections and mapped admission failures include a stable
`X-Tsink-Write-Error-Code`. Protocol framing, disabled-feature, decode, and some sidecar
persistence failures are instead identified by their HTTP status and bounded response body. If
rows committed before a later component failed, the response includes `X-Tsink-Write-Partial: true`,
`X-Tsink-Rows-Accepted`, the acknowledgement for those rows, and relevant metadata/exemplar
counts. A failure whose commit state cannot be proven uses `X-Tsink-Write-Partial: possible` and
`X-Tsink-Write-Outcome: indeterminate_backend` or `indeterminate_cluster`.

Diagnostics are bounded and do not echo metric, label, attribute, tag, or field values. The UDP
StatsD and TCP Graphite protocols have no response frame that can carry these headers; their
acknowledgements, outcomes, and fixed-cardinality error reasons are exported as server metrics.

---

## Prometheus Remote Write

**Endpoint:** `POST /api/v1/write`

The standard [Prometheus Remote Write](https://prometheus.io/docs/specs/remote_write_spec/)
endpoint normally receives a Snappy block-compressed protobuf `WriteRequest`. For compatibility,
tsink also accepts raw protobuf when `Content-Encoding` is omitted or set to `identity`.

### Headers

Standard Prometheus clients send the headers below. The server uses `Content-Encoding` when it is
present, but currently accepts a missing content type and version header.

| Header | Value |
|---|---|
| `Content-Encoding` | `snappy` |
| `Content-Type` | `application/x-protobuf` |
| `X-Prometheus-Remote-Write-Version` | `0.1.0` (optional) |

### Supported payload features

Beyond plain float samples, tsink supports three optional payload extensions. Each extension can be individually disabled at runtime.

| Feature | Env var to disable | Default limit |
|---|---|---|
| Metric metadata | `TSINK_REMOTE_WRITE_METADATA_ENABLED=false` | 512 updates per request |
| Exemplars | `TSINK_REMOTE_WRITE_EXEMPLARS_ENABLED=false` | Configured on exemplar store |
| Native histograms | `TSINK_REMOTE_WRITE_HISTOGRAMS_ENABLED=false` | 16,384 bucket entries per request |

Requests that include a disabled feature type are rejected with `422`. Requests that exceed a per-request limit are rejected with `413`.

### Cluster capability negotiation

In cluster mode, tsink uses the Prometheus remote write capability header mechanism. The server announces the capabilities it supports for metadata, exemplar, and native histogram payloads in its `/api/v1/status/tsdb` response (`prometheusPayloads.localCapabilities`). Before routing a payload batch to a peer node, the write path issues a zero-row preflight request to ensure the peer supports the required capabilities. If the peer does not advertise them, the request is rejected with `409`.

### Response

A successful write returns `200` with an empty body.

Every successful non-empty response includes `X-Tsink-Write-Acknowledgement`. Optional payloads
also return accepted/applied or dropped component counts.

In cluster mode the response also includes:

| Header | Description |
|---|---|
| `X-Tsink-Write-Consistency` | Consistency mode that was applied (`one`, `quorum`, or `all`) |
| `X-Tsink-Write-Required-Acks` | Number of replica acknowledgements required |
| `X-Tsink-Write-Acknowledged-Replicas` | Minimum complete replica acknowledgements received across routed shards |

### Example

```bash
# Using prometheus-remote-write-compatible client or curl (requires manual protobuf encoding)
curl -X POST http://127.0.0.1:9201/api/v1/write \
  -H 'Content-Encoding: snappy' \
  -H 'Content-Type: application/x-protobuf' \
  -H 'X-Prometheus-Remote-Write-Version: 0.1.0' \
  --data-binary @payload.snappy
```

---

## Prometheus Remote Read

**Endpoint:** `POST /api/v1/read`

The standard Prometheus Remote Read endpoint normally receives a Snappy block-compressed protobuf
`ReadRequest`. tsink also accepts raw protobuf when `Content-Encoding` is omitted or set to
`identity`.

### Standard request headers

These are sent by standard clients; the server currently accepts either header being absent.

| Header | Value |
|---|---|
| `Content-Encoding` | `snappy` |
| `Content-Type` | `application/x-protobuf` |

Only the `SAMPLES` response type is supported. Requests that include other `accepted_response_types` values are rejected with `400`.

### Response

Returns `200` with a Snappy-compressed protobuf `ReadResponse`.

| Header | Value |
|---|---|
| `Content-Type` | `application/x-protobuf` |
| `Content-Encoding` | `snappy` |
| `X-Prometheus-Remote-Read-Version` | `0.1.0` |

---

## Prometheus Text Exposition (Bulk Import)

**Endpoint:** `POST /api/v1/import/prometheus`

Accepts a body in the [Prometheus text exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/). This is intended for bulk historical imports or one-shot pushes from scripts.

All sample rows in the body are processed as a single atomic batch. Embedded exemplars are parsed
and applied as a separate atomic exemplar-store update; a failure in that later component reports
the already accepted row effect rather than claiming one cross-component transaction.

### Content type

`Content-Type: text/plain` (optional; body is always decoded as UTF-8)

### Timestamp handling

If a sample line does not include a timestamp, the server's current time in the configured `--timestamp-precision` is used.

### Limits

Exemplar limits from the exemplar store configuration apply; requests exceeding them are rejected with `413`.

### Example

```bash
curl -X POST http://127.0.0.1:9201/api/v1/import/prometheus \
  -H 'Content-Type: text/plain' \
  -d 'http_requests_total{method="GET"} 1027 1700000000000
http_requests_total{method="POST"} 42 1700000000000'
```

---

## InfluxDB Line Protocol

**Endpoints:** `POST /write`, `POST /api/v2/write`

Accepts the [InfluxDB line protocol](https://docs.influxdata.com/influxdb/v1/write_data/developer_tools/line_protocol/) format. Both the v1 path (`/write`) and the v2 path (`/api/v2/write`) are handled identically.

### Enable / disable

Enabled by default. Set `TSINK_INFLUX_LINE_PROTOCOL_ENABLED=false` to disable.

### Wire format

```
measurement[,tag_key=tag_value]... field_key=field_value[,field_key=field_value]... [unix_timestamp]
```

Only numeric float, signed-integer, and unsigned-integer fields are supported. String or boolean
fields reject the request explicitly. Each field in an accepted measurement becomes its own metric
series:
- A field named `value` maps to `measurement`.
- Any other field named `field_name` maps to `measurement_field_name`.

Tag key-value pairs are stored as series labels.

### Precision

The optional `precision` query parameter controls the input timestamp unit. Accepted values are
`ns`, `u`/`us`, `ms`, `s`, `m`, and `h`. If omitted, input timestamps are interpreted as
nanoseconds. Samples without a timestamp use the current server time, converted to the configured
storage precision.

```
POST /write?precision=s
POST /api/v2/write?precision=ms
```

### Labels from query parameters

The recognized compatibility parameters `db`, `rp`, `bucket`, and `org` become the labels
`influx_db`, `influx_rp`, `influx_bucket`, and `influx_org`, respectively. Other query parameters
are not copied into series labels.

### Limits

| Env var | Default | Description |
|---|---|---|
| `TSINK_INFLUX_LINE_PROTOCOL_MAX_LINES_PER_REQUEST` | `4096` | Maximum lines per HTTP request |

Requests exceeding the line limit are rejected with `413`.

### Example

```bash
curl -X POST 'http://127.0.0.1:9201/write?precision=ms' \
  -d 'cpu_usage,host=web-01 user=42.5,system=1.2 1700000000000'
```

---

## OTLP HTTP

**Endpoint:** `POST /v1/metrics`

Accepts [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) metrics via HTTP protobuf encoding (`ExportMetricsServiceRequest`).

### Enable / disable

Enabled by default. Set `TSINK_OTLP_METRICS_ENABLED=false` to disable. Requests to a disabled endpoint return `422`.

### Content type

When present, `Content-Type` must be `application/x-protobuf` or `application/protobuf`. An omitted
content type is accepted; another explicit content type is rejected with `415`.

### Supported metric shapes

| OTLP shape | Stored as |
|---|---|
| `Gauge` | `gauge` series (float64 samples) |
| `Sum` (monotonic) | `counter` series |
| `Sum` (non-monotonic) | `gauge` series |
| `Histogram` (explicit buckets, cumulative) | Flat bucket series with `le` label per boundary |
| `Summary` | Quantile series with `quantile` label |
| `ExponentialHistogram` | **Not supported** — requests containing these are rejected with `400` |

### Label mapping

- Resource attributes become series labels.
- Instrumentation scope name and version become `otel_scope_name` and `otel_scope_version` labels.
- Aggregation temporality is recorded in an `otel_temporality` label where applicable.
- Exemplars attached to data points are extracted and stored in the exemplar store.

### Timestamps

OTLP uses nanosecond Unix timestamps. tsink converts them to the server's configured
`--timestamp-precision` before storage. Data points with `NO_RECORDED_VALUE`, unknown flags, or
unsupported shapes reject the atomic request with `400`; they are not silently skipped.

### Example

```bash
# Using otelcol or grpcurl; raw curl requires a valid binary ExportMetricsServiceRequest body
curl -X POST http://127.0.0.1:9201/v1/metrics \
  -H 'Content-Type: application/x-protobuf' \
  --data-binary @export_metrics.pb
```

---

## StatsD (UDP)

StatsD is an optional side-channel listener on a separate UDP socket. It is **disabled by default** and must be explicitly enabled at startup.

### Enabling

```bash
tsink-server --listen 0.0.0.0:9201 --data-path ./data \
  --statsd-listen 0.0.0.0:8125 \
  --statsd-tenant default
```

| Flag | Description |
|---|---|
| `--statsd-listen ADDR` | UDP address to bind. Omitting this flag disables the listener. |
| `--statsd-tenant ID` | Tenant ID to attribute all StatsD writes to. Defaults to `default`. |

### Wire format

Standard StatsD line format:

```
metric_name:value|type[|@sample_rate][|#tag1:val1,tag2:val2]
```

### Supported metric types

| Type code | Stored as | Notes |
|---|---|---|
| `c` | Counter (float64) | Value is divided by the sample rate |
| `g` | Gauge (float64) | Supports `+N` / `-N` relative updates on the running value |
| `ms` | Summary (float64) | Timer in milliseconds; sample rate recorded as `statsd_sample_rate` label |
| `h` | Histogram (float64) | Sample rate recorded as `statsd_sample_rate` label |
| `d` | Distribution (histogram float64) | Sample rate recorded as `statsd_sample_rate` label |
| `s` | Stateset | Set membership is encoded as a `statsd_set_member` label; value is always `1.0` |

All other type codes are rejected.

Tags in `key:value` format are stored as series labels. The metric name is normalized to a valid tsink metric name.

### Limits

| Env var | Default | Description |
|---|---|---|
| `TSINK_STATSD_MAX_PACKET_BYTES` | `8192` | Maximum UDP packet size in bytes |
| `TSINK_STATSD_MAX_EVENTS_PER_PACKET` | `1024` | Maximum events per packet |

Packets exceeding either limit, failing parsing, or receiving a row rejection are dropped as a
request. A later metadata-sidecar failure can follow proven row acceptance and is recorded as a
partial outcome; relative-gauge state advances only when every packet row is proven accepted.
Because UDP has no reply channel, the sender receives no per-packet acknowledgement; inspect
`tsink_legacy_ingest_*` metrics for the result.

---

## Graphite (TCP)

Graphite is an optional side-channel listener on a separate TCP port. It uses the [Graphite plaintext protocol](https://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol). It is **disabled by default** and must be explicitly enabled at startup.

### Enabling

```bash
tsink-server --listen 0.0.0.0:9201 --data-path ./data \
  --graphite-listen 0.0.0.0:2003 \
  --graphite-tenant default
```

| Flag | Description |
|---|---|
| `--graphite-listen ADDR` | TCP address to bind. Omitting this flag disables the listener. |
| `--graphite-tenant ID` | Tenant ID to attribute all Graphite writes to. Defaults to `default`. |

### Wire format

Each connection sends newline-delimited lines:

```
metric.path[;tag1=val1;tag2=val2] value [unix_timestamp]
```

- The metric path is normalized to a valid tsink metric name (dots are replaced with underscores).
- Tags in the `key=value` semicolon-separated format are stored as series labels.
- The timestamp must be a Unix timestamp in seconds (will be converted to the server's precision); if omitted, the current server time is used.
- All metrics are stored as gauge series.

### Limits

| Env var | Default | Description |
|---|---|---|
| `TSINK_GRAPHITE_MAX_LINE_BYTES` | `8192` | Maximum bytes per line |

An oversized, malformed, admission-rejected, or storage-rejected line closes the connection. The
plaintext protocol has no per-line error frame, so connection closure is the transport-visible
failure signal and `tsink_legacy_ingest_*` metrics retain the structured outcome.

---

## Authentication

All HTTP-based ingest endpoints respect the server's configured authentication:

- **Bearer token auth** — set `--auth-token` or `--auth-token-file`. Requests must include `Authorization: Bearer <token>`.
- **RBAC** — set `--rbac-config`. Requests must carry a valid RBAC bearer token or OIDC JWT.
- **Multi-tenancy** — set a `--tenant-config` file and pass the `X-Scope-OrgID` (or `x-tsink-tenant`) header to route writes to a specific tenant. If both headers are present they must match.

StatsD and Graphite side-channel listeners write all data to a single tenant specified by `--statsd-tenant` and `--graphite-tenant` respectively. They do not support per-connection authentication.

---

## Observability

All ingest paths expose counters via:

- **`/metrics`** — Prometheus text format self-instrumentation endpoint.
- **`/api/v1/status/tsdb`** — JSON snapshot including per-protocol acceptance, rejection, and throttle counters under `prometheusPayloads`, `otlpMetrics`, and `legacyIngest`.

Example fields in `/api/v1/status/tsdb`:

```json
{
  "prometheusPayloads": {
    "metadata": { "enabled": true, "acceptedTotal": 0, "rejectedTotal": 0 },
    "exemplars": { "enabled": true, "acceptedTotal": 0, "rejectedTotal": 0 },
    "histograms": { "enabled": true, "acceptedTotal": 0, "rejectedTotal": 0 }
  },
  "otlpMetrics": {
    "enabled": true,
    "acceptedRequestsTotal": 0,
    "rejectedRequestsTotal": 0,
    "gauges": { "acceptedTotal": 0 },
    "sums": { "acceptedTotal": 0 },
    "histograms": { "acceptedTotal": 0 },
    "summaries": { "acceptedTotal": 0 }
  },
  "legacyIngest": {
    "influxLineProtocol": { "enabled": true, "acceptedRequestsTotal": 0, "acceptedSamplesTotal": 0 },
    "statsd": { "enabled": false, "acceptedRequestsTotal": 0 },
    "graphite": { "enabled": false, "acceptedRequestsTotal": 0 }
  }
}
```

---

## Admission control

All ingest paths are subject to global and per-tenant admission control:

- **Global write admission** — controls the maximum number of in-flight write requests and total in-flight row count. Configurable via environment variables; tuned at startup.
- **Tenant quotas** — maximum rows per request per tenant, maximum in-flight write requests and units per tenant, configurable in the tenant config JSON.

HTTP admission failures return a non-success response with a stable write error code (normally
`413` for a non-retryable request limit or `429` for timed admission pressure). StatsD drops the
datagram and Graphite closes the connection; both increment bounded rejection/throttle metrics.
