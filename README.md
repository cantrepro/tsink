<p align="center">
  <img src="https://raw.githubusercontent.com/cantrepro/tsink/refs/heads/master/logo.svg" width="220" height="100" alt="tsink logo"><br>
  <strong>Durable local Prometheus-style metrics and PromQL, embedded in your application.</strong>
</p>

<p align="center">
  <a href="https://crates.io/crates/tsink"><img src="https://img.shields.io/crates/v/tsink.svg" alt="crates.io"></a>
  <a href="https://docs.rs/tsink/latest/tsink"><img src="https://img.shields.io/docsrs/tsink.svg" alt="docs.rs"></a>
  <a href="https://pypi.org/project/tsink"><img src="https://img.shields.io/pypi/v/tsink.svg" alt="PyPI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License"></a>
</p>

tsink is an embeddable metrics database for applications that need local history and
PromQL without operating a separate database service. Think of it as a local database
component for Prometheus-style metrics: one library, one data directory, and an
optional server adapter when network protocols are useful.

The current Rust crate provides:

- an in-process synchronous API plus a runtime-independent async facade;
- labeled metric storage backed by a write-ahead log and persisted segments;
- direct reads and an embedded PromQL parser and evaluator;
- write acknowledgements that distinguish volatile, WAL-appended, and durable results;
- configurable memory, cardinality, WAL, retention, and concurrency controls;
- snapshots and native Python bindings.

## Embedded Rust quick start

Add the library:

```toml
[dependencies]
tsink = "0.10"
```

The current crate metadata declares Rust 1.89 as the minimum supported Rust version.

Then open a local data directory, write a metric, and query it in-process:

```rust
use tsink::promql::Engine;
use tsink::{DataPoint, Label, Row, StorageBuilder, TimestampPrecision};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = StorageBuilder::new()
        .with_data_path("./tsink-data")
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        // Today's builder exposes individual limits; set the ones your host requires.
        .with_memory_limit(64 * 1024 * 1024)
        .with_cardinality_limit(50_000)
        .with_wal_size_limit(64 * 1024 * 1024)
        .build()?;

    let timestamp = 1_700_000_000_000_i64;
    let write = db.insert_rows_with_result(&[Row::with_labels(
        "http_requests_total",
        vec![Label::new("method", "GET")],
        DataPoint::new(timestamp, 1.0),
    )])?;
    println!("write acknowledgement: {}", write.acknowledgement.as_str());

    let promql = Engine::with_precision(db.clone(), TimestampPrecision::Milliseconds);
    let result = promql.instant_query(
        r#"http_requests_total{method="GET"}"#,
        timestamp,
    )?;
    println!("{result:?}");

    db.close()?;
    Ok(())
}
```

No daemon, listener, or async runtime is required by this path. See the
[embedded library guide](docs/embedded-library.md) for labels, direct queries, WAL
configuration, snapshots, and the async API.

## Where tsink fits

tsink is aimed at applications where a separate TSDB would be disproportionate:

- self-hosted applications and developer tools with a built-in diagnostics history;
- agents, gateways, and appliances that retain metrics locally;
- Rust or Python applications that need direct metric queries or PromQL;
- protocol integration tests that need a small local metrics backend.

The intended first adoption beachhead is integration testing, but the dedicated
deterministic test fixture described below is still roadmap work.

## Maturity and compatibility

tsink is a pre-1.0 project with a broad implementation surface. Evaluate the exact
paths you depend on and pin versions. In particular:

| Area | Current status |
|---|---|
| Resource bounds | Individual builder knobs and [effective-limit inspection](docs/resource-limits.md) exist, but finite `Test`, `Embedded`, `Edge`, and `Server` profiles are **not shipped**. Memory, cardinality, and WAL quotas default to no explicit limit, and a complete hard disk/query/resource envelope is roadmap work. |
| Test support | A dedicated `tsink-test` crate, public manual clock, and deterministic maintenance loop are **not shipped**. |
| Prometheus and OTLP compatibility | Implementations exist, but generated capability matrices and a differential compatibility suite are **not shipped**. Do not interpret “PromQL” or a protocol endpoint as a claim of complete upstream compatibility. |
| Write results | `write_batch` reports indexed structured outcomes with explicit `Atomic` or `BestEffort` policy; `insert_rows_with_result` retains the compatibility batch acknowledgement. Principal HTTP adapters expose acknowledgement and known partial/indeterminate effects. Server sidecars and experimental cluster routing are not one cross-component transaction. |
| API and storage stability | Public APIs and on-disk upgrade guarantees are still being hardened for 1.0. Test recovery and upgrades against the versions you deploy. |
| Clustering | Cluster mode is **experimental**; it is not the primary product path or a production-readiness claim. |

### Metrics integration tests without Docker — roadmap

The roadmap calls for a `tsink-test` library that can run protocol endpoints on
ephemeral loopback ports, advance a manual clock, drive maintenance explicitly, and
assert PromQL results without Docker or arbitrary sleeps. That package and API do not
exist in the current workspace, so there is no testkit quick start to copy yet.

If this is your use case, the
[design-partner guide](docs/design-partner-guide.md) explains how to record the
constraints that should shape it.

## Optional server adapter

`tsink-server` wraps the same engine in network and operational adapters. It is
optional; embedded applications do not need it.

```bash
cargo run -p tsink-server --release -- \
  --listen 127.0.0.1:9201 \
  --data-path ./var/tsink
```

The server currently includes Prometheus remote write/read, Prometheus instant and
range query endpoints, Prometheus text import, OTLP HTTP metrics ingestion, and
Influx line-protocol ingestion. These endpoints inherit the compatibility caveat
above. See [server deployment](docs/server-deployment.md),
[HTTP API](docs/http-api.md), and
[ingestion protocols](docs/ingestion-protocols.md).

## Advanced and experimental capabilities

These surfaces remain available, but they are secondary to the embedded local-engine
contract:

- native [Python bindings](docs/python-bindings.md);
- [tiered storage](docs/tiered-storage.md), rollups, snapshots, exemplars, and
  non-Prometheus value types;
- server-side TLS, authentication, RBAC, multi-tenancy, rules, monitoring, and
  administrative APIs;
- optional StatsD and Graphite listeners and the existing edge-sync machinery.

Consult the linked documentation and test the exact configuration before relying on
an advanced capability in production.

> **Experimental cluster mode:** the server contains sharding, replication, repair,
> and rebalance code, but clustering is not the reason to adopt tsink and is not
> presented as production-ready. Prefer the single-instance embedded or server path
> unless you are explicitly evaluating experimental cluster behavior. See
> [cluster setup](docs/cluster-setup.md) and
> [clustering internals](docs/clustering-internals.md).

## Documentation

- [Product positioning](docs/product-positioning.md)
- [Embedded library](docs/embedded-library.md)
- [Storage engine](docs/storage-engine.md), [WAL and compaction](docs/compaction.md),
  and [architecture](docs/architecture.md)
- [PromQL overview](docs/promql.md) and [current reference](docs/promql-reference.md)
- [Server deployment](docs/server-deployment.md), [HTTP API](docs/http-api.md), and
  [configuration](docs/configuration.md)
- [Python bindings](docs/python-bindings.md)
- [Design-partner guide](docs/design-partner-guide.md)

## License

MIT — see [LICENSE](LICENSE).
