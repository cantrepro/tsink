# tsink-test

`tsink-test` provides small, synchronous, in-process fixtures for testing code against the
[`tsink`](https://crates.io/crates/tsink) embedded metrics engine.

The initial API supports:

- isolated temporary databases;
- pure in-memory databases;
- caller-owned persistent directories for restart tests;
- atomic writes that return the core `BatchWriteResult` unchanged;
- focused label, series, arbitrary sample, gauge, counter, checked sequence, classic-histogram, and
  native-histogram fixture constructors;
- direct PromQL instant and range evaluation at explicit timestamps;
- error-returning PromQL assertions for normalized values, approximate scalars, empty/non-empty
  vectors and matrices, and structured parser/storage/query-limit failures;
- explicit, error-returning close and same-directory restart;
- a bounded per-fixture diagnostic ring.

The assertion entry points are `assert_promql_instant_eq`, `assert_promql_range_eq`,
`assert_promql_scalar`, the instant/range `*_empty` and `*_nonempty` helpers, and the typed
`assert_promql_instant_error` and `assert_promql_range_error` helpers. Successful value assertions
return the normalized actual `PromqlValue` for further inspection. Duplicate series identities and
duplicate or mixed float/histogram timestamps are rejected explicitly instead of making equality
depend on input order.

Enable the optional `prometheus` feature for an endpoint-ready remote-write payload builder backed
by the same generated protobuf model as `tsink-server`:

```rust
use tsink_test::{
    label, prometheus_remote_write_payload, PrometheusRemoteWriteSample,
    PrometheusRemoteWriteSeries,
};

let payload = prometheus_remote_write_payload(&[PrometheusRemoteWriteSeries::new(
    "requests_total",
    vec![label("method", "GET")],
    vec![PrometheusRemoteWriteSample::new(1_000, 3.0)],
)])?;
assert_eq!(payload.content_encoding(), "snappy");
# Ok::<(), tsink::TsinkError>(())
```

This feature generates a real Snappy-compressed protobuf request body but does not start a
listener. The default feature set does not depend on protobuf, Snappy, the server, or an async
runtime.

It starts no protocol listener, shells out to no binary, downloads nothing, and requires no
Docker. A manual clock, deterministic maintenance controls, and optional Prometheus or OTLP
endpoints are later testkit work and are not part of this package yet. Because there is no manual
clock yet, PromQL queries and assertions require explicit timestamps; the fixture does not invent a
"current" time.

```rust
use tsink::TimestampPrecision;
use tsink_test::{counter_sequence, labels, TsinkTestDb};

let mut db = TsinkTestDb::builder()
    .temporary()
    .timestamp_precision(TimestampPrecision::Seconds)
    .start()?;

let rows = counter_sequence(
    "requests_total",
    labels([("method", "GET")]),
    10,
    10,
    3.0,
    2.0,
    2,
)?;
assert_eq!(db.write_atomic(&rows)?.accepted, 2);

let value = db.promql_instant("requests_total", 20).unwrap();
assert_eq!(value.as_instant_vector().unwrap()[0].value, 5.0);
db.assert_promql_scalar("scalar(sum(requests_total))", 20, 5.0, 0.0)?;

db.close()?;
# Ok::<(), tsink::TsinkError>(())
```
