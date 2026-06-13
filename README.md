<p align="center">
  <img src="https://raw.githubusercontent.com/h2337/tsink/refs/heads/master/logo.svg" width="220" height="100" alt="tsink logo"><br>
  A lightweight time-series database written in Rust.<br>
  Embed it, run it as a server, or scale it as a cluster.
</p>

<p align="center">
  <a href="https://crates.io/crates/tsink"><img src="https://img.shields.io/crates/v/tsink.svg" alt="crates.io"></a>
  <a href="https://docs.rs/tsink/latest/tsink"><img src="https://img.shields.io/docsrs/tsink.svg" alt="docs.rs"></a>
  <a href="https://pypi.org/project/tsink"><img src="https://img.shields.io/pypi/v/tsink.svg" alt="pypi.org"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License"></a>
</p>

---

## Why tsink?

- **Three deployment modes** — embed the library directly in your Rust or Python application, run a standalone server binary, or form a replicated cluster. Same engine everywhere.
- **Robust engine** — segmented WAL with crash-safe sync, LSM-style leveled compaction, adaptive delta/XOR/zstd encoding, mmap zero-copy reads, and configurable memory backpressure.
- **Tiered storage** — hot, warm, and cold tiers with automatic lifecycle management and optional object-store backing.
- **Drop-in protocol support** — accepts Prometheus remote write/read, InfluxDB line protocol, OTLP, StatsD, and Graphite out of the box.
- **Built-in PromQL** — query your data with a native PromQL parser and evaluator. No external query layer needed.
- **Secure by default** — TLS (rustls, no OpenSSL), RBAC with OIDC, multi-tenant isolation, and mTLS between cluster nodes.
- **Zero external dependencies at runtime** — single static binary for the server; `protoc` is vendored at build time.

---

## Deployment modes

### Embedded library

Add `tsink` as a dependency and get a full time-series engine in-process — WAL durability, compaction, retention, and queries included.

```rust
use tsink::{DataPoint, Row, StorageBuilder, TimestampPrecision};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = StorageBuilder::new()
        .with_data_path("./tsink-data")
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()?;

    storage.insert_rows(&[
        Row::new("cpu_usage", DataPoint::new(1_700_000_000_000_i64, 42.0)),
    ])?;

    let points = storage.select("cpu_usage", &[], 1_700_000_000_000, 1_700_000_000_001)?;
    println!("{points:?}");

    storage.close()?;
    Ok(())
}
```

UniFFI bindings expose the core API as a native Python module:

```python
from tsink import TsinkStorageBuilder, DataPoint, Row, Value

builder = TsinkStorageBuilder()
builder.with_data_path("./tsink-data")
db = builder.build()

db.insert_rows([
    Row(
        metric="cpu_usage",
        labels=[],
        data_point=DataPoint(timestamp=1_700_000_000_000, value=Value.F64(v=42.0)),
    )
])
print(db.select("cpu_usage", [], 0, 2_000_000_000_000))
```

### Server mode

A single binary that speaks every major metrics protocol.

```bash
cargo run -p tsink-server --bin tsink-server --release -- \
  --listen 127.0.0.1:9201 \
  --data-path ./var/tsink
```

Write data with any client you already have:

```bash
# Prometheus text exposition
curl -X POST http://127.0.0.1:9201/api/v1/import/prometheus \
  -H 'Content-Type: text/plain' \
  -d 'http_requests_total{method="GET"} 1027 1700000000000'

# PromQL query
curl 'http://127.0.0.1:9201/api/v1/query?query=http_requests_total'
```

### Cluster mode

Enable clustering with a flag and scale horizontally. tsink handles shard routing, replication, consistency, hinted handoff, repair, and rebalance automatically.

```bash
tsink-server \
  --listen 0.0.0.0:9201 \
  --data-path ./var/tsink \
  --cluster-enabled \
  --cluster-replication-factor 3 \
  --cluster-peers node-2:9202,node-3:9203
```

---

## Storage engine

| Capability | Details |
|---|---|
| **Durability** | Segmented WAL with configurable sync — per-append (crash-safe) or periodic (throughput-optimized). Strict or salvage replay on recovery. |
| **Compaction** | LSM-style leveled compaction (L0 → L1 → L2) with tombstone-aware merging and atomic segment replacement. |
| **Tiered storage** | Automatic hot → warm → cold lifecycle with configurable retention windows. Object-store backing for warm/cold tiers. |
| **Encoding** | Adaptive timestamp codecs (fixed-step, delta-varint, delta-of-delta), Gorilla XOR float compression, and zstd for persisted segments. |
| **Data types** | float64, bytes, and native Prometheus histograms. |
| **Memory control** | Configurable memory budget with admission-based backpressure. Cardinality limits on unique series. |
| **Reads** | mmap-based zero-copy segment reads. Downsampling, aggregation, and regex-capable label matchers built in. |

---

## Ingestion protocols

| Protocol | Endpoint | Notes |
|---|---|---|
| Prometheus Remote Write | `POST /api/v1/write` | Snappy-framed protobuf |
| Prometheus Remote Read | `POST /api/v1/read` | |
| Prometheus Text Exposition | `POST /api/v1/import/prometheus` | Bulk import |
| InfluxDB Line Protocol | `POST /write`, `POST /api/v2/write` | v1 and v2 compatible |
| OTLP HTTP | `POST /v1/metrics` | Protobuf; gauges, sums, histograms, summaries |
| StatsD | UDP (`--statsd-listen`) | Counter, gauge, timer, set |
| Graphite | TCP (`--graphite-listen`) | Plaintext protocol |

---

## Clustering & replication

- **Consistent hash-ring sharding** with configurable shard count
- **Tunable replication factor** and consistency levels (One / Quorum / All) for writes and reads
- **Node roles** — dedicated Storage, Query, or Hybrid nodes
- **Hinted handoff** — queues writes for temporarily unavailable replicas
- **Digest-based repair** — fingerprint exchange detects and resolves inconsistencies
- **Online rebalance** — pause, resume, and monitor shard migration
- **Distributed query fan-out** — concurrent shard-aware reads with merge limits
- **Cluster-wide snapshots** — coordinated data + control-plane backup and restore
- **Internal mTLS** — dedicated CA for peer-to-peer traffic

---

## Security & multi-tenancy

- **TLS** — rustls-based with hot-reloadable certificates
- **Authentication** — bearer tokens (file or exec-based loading), OIDC JWT validation (RS256, HS256)
- **RBAC** — roles, service accounts with rotation, and live audit logging
- **Multi-tenant isolation** — per-tenant policies for write rate, query concurrency, admission budgets, and retention
- **Secret rotation** — runtime rotation of auth tokens, TLS certs, and mTLS materials with overlap grace periods

---

## Operations

- **`/healthz`** and **`/ready`** — Kubernetes-compatible probes
- **`/metrics`** — Prometheus-format self-instrumentation
- **Recording & alerting rules** — built-in rules engine with configurable evaluation intervals
- **Rollup policies** — persistent downsampled materialization with automated scheduling
- **Migration tooling** — backfill, verify, and cutover from Prometheus, VictoriaMetrics, InfluxDB, OTLP, StatsD, and Graphite
- **Support bundles** — bounded JSON diagnostic snapshots per tenant

---

## Documentation

### Getting started

- [Embedded library guide](docs/embedded-library.mdx) — using tsink as a Rust dependency, `StorageBuilder` configuration, sync and async APIs, snapshots
- [Python bindings guide](docs/python-bindings.mdx) — UniFFI setup, `TsinkStorageBuilder`, type mappings, error handling
- [Server deployment](docs/server-deployment.mdx) — running the single-node server binary, CLI flags, environment variables
- [Cluster setup](docs/cluster-setup.mdx) — multi-node deployment, peer discovery, shard count, replication factor, consistency levels, node roles

### Architecture & design

- [Architecture overview](docs/architecture.mdx) — high-level system design, component interactions, data flow
- [Storage engine internals](docs/storage-engine.mdx) — WAL, segments, LSM-style compaction, encoding codecs, mmap reads, write buffer
- [PromQL implementation](docs/promql.mdx) — lexer, parser, evaluator, supported functions, aggregations, subqueries
- [Clustering internals](docs/clustering-internals.mdx) — consistent hash ring, replication protocol, hinted handoff, digest repair, rebalance, distributed queries

### API & protocol reference

- [HTTP API reference](docs/http-api.mdx) — all endpoints, request/response formats, authentication headers, error codes
- [PromQL reference](docs/promql-reference.mdx) — function catalogue, operators, vector matching, type coercion rules
- [Ingestion protocols](docs/ingestion-protocols.mdx) — Prometheus remote write, InfluxDB line protocol, OTLP, StatsD, Graphite wire formats and endpoints
- [Configuration reference](docs/configuration.mdx) — complete list of server, engine, cluster, and security options with defaults

### Features

- [Tiered storage](docs/tiered-storage.mdx) — hot/warm/cold lifecycle, retention windows, object-store backing
- [Compaction](docs/compaction.mdx) — L0/L1/L2 levels, merge strategies, tombstone handling, tuning
- [Rollups & downsampling](docs/rollups.mdx) — rollup policies, materialization scheduling, query integration
- [Data types & native histograms](docs/data-types.mdx) — float64, bytes, native histograms, timestamp precision modes
- [Exemplars](docs/exemplars.mdx) — exemplar storage, querying, cardinality limits

### Security & operations

- [Security model](docs/security.mdx) — TLS/mTLS setup, RBAC roles, OIDC authentication, audit logging
- [Multi-tenancy](docs/multi-tenancy.mdx) — tenant isolation, per-tenant quotas, admission budgets, usage accounting
- [Secret rotation](docs/secret-rotation.mdx) — rotating auth tokens, TLS certificates, mTLS materials, grace periods
- [Monitoring & observability](docs/monitoring.mdx) — `/metrics` endpoint, self-instrumentation, health probes, support bundles
- [Recording & alerting rules](docs/rules.mdx) — rule definitions, evaluation intervals, recording rule output
- [Performance tuning](docs/performance-tuning.mdx) — memory budgets, compaction tuning, write pipelining, cgroup-aware scheduling
- [Migration guide](docs/migration.mdx) — migrating from Prometheus, VictoriaMetrics, InfluxDB; backfill, verify, cutover

---

## License

MIT — see [LICENSE](LICENSE).
