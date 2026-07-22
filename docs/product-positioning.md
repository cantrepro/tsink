# Product positioning

> **Status:** Product direction for the releases leading to 1.0. This document
> describes the problem tsink is choosing to solve; it is not a compatibility
> matrix or a claim that every part of the target contract ships today.

## Thesis

tsink is an embeddable metrics database that gives applications durable local
retention and PromQL without requiring a separate database service.

The useful mental model is “SQLite or DuckDB for Prometheus-style metrics,” not
“a smaller replacement for every distributed TSDB.” The product should behave
politely as a component inside another application:

- one library and one data directory;
- no mandatory daemon, listener, async runtime, or external service;
- explicit write and durability results;
- predictable resource use and deterministic lifecycle behavior;
- Prometheus-style data and query semantics;
- optional protocol endpoints implemented as adapters;
- safe restart, snapshot, restore, inspection, and upgrade paths.

The north-star positioning is an OEM-grade, embedded, resource-bounded,
Prometheus-native metrics engine with deterministic testing support. “OEM-grade,”
“resource-bounded,” and “deterministic” are acceptance targets, not blanket claims
about the current release.

Standalone server mode is an optional adapter around the engine. Cluster mode is
experimental and must not drive the architecture or headline.

## Who it is for

### Test authors

Teams whose integration tests currently start Prometheus or another TSDB in
Docker should eventually be able to use an in-process or ephemeral-loopback
fixture, send real Prometheus or OTLP traffic, control time and maintenance, run
PromQL assertions, and tear down without fixed ports or arbitrary sleeps.

This is the intended adoption beachhead. The dedicated `tsink-test` fixture is
roadmap work and is not present in the current workspace.

### Application and appliance developers

Self-hosted applications, desktop tools, commercial appliances, and other
products may need several hours or days of built-in diagnostic history without
asking their users to operate Prometheus. These developers need a small embedded
API, understandable background work, explicit pressure and failure signals,
snapshots, and a clean shutdown path.

### Agent, gateway, and edge developers

Agents and constrained gateways need bounded local retention while disconnected
and visible, resumable synchronization when connectivity returns. Local
retention and upload progress should remain independent; the primary design is
one-way store-and-forward, not distributed membership.

### Rust and Python integrators

Rust is the primary embedded API. Python bindings should expose the same stable
lifecycle, write outcomes, and structured errors rather than creating a separate
behavioral model.

## Primary uses

1. **Local application metrics:** write labeled metrics in-process, retain them
   on local storage, and query through direct APIs or PromQL.
2. **Metrics integration tests:** replace a Prometheus/TSDB container with a
   deterministic fixture that uses real Prometheus and OTLP adapters. This is the
   first product-expansion priority, but its dedicated testkit is not shipped.
3. **Built-in diagnostics:** give a self-hosted application, developer tool, or
   appliance local performance history without a required sidecar.
4. **Disconnected collection:** retain data under finite limits and later send
   it to a standard Prometheus remote-write destination. The complete
   resource-and-synchronization contract remains a roadmap gate.

The intended adoption path is:

```text
integration tests
    -> developer tools
    -> self-hosted applications
    -> agents and appliances
    -> offline and edge telemetry with remote synchronization
```

Product investment is prioritized accordingly: the direct Rust API, Python
bindings, Prometheus remote write/read and query APIs, OTLP HTTP metrics, the
testkit, lifecycle and snapshot tooling, then one-way synchronization. The
single-node server remains useful but secondary.

## Product principles

- **Data safety before throughput.** Acknowledged, rejected, partial, volatile,
  or failed writes must never be confused.
- **Bounds are a contract.** Memory, disk, WAL, cardinality, query, concurrency,
  threads, and shutdown behavior must have finite, tested profiles.
- **Protocols are adapters.** HTTP, authentication, tenant routing, and
  Kubernetes concerns stay outside the core engine.
- **Local first, synchronization second.** Edge support should use a durable
  checkpointed export path, not require consensus or a custom distributed
  database.
- **Compatibility is evidence.** PromQL and protocol claims should point to
  reproducible differential tests and capability matrices.
- **Correctness before catalogue growth.** Existing breadth may remain, but new
  major subsystems wait until a real integration is blocked without them.

## Non-goals

For the current roadmap, tsink is not pursuing:

- SQL or a general analytical query language;
- logs or traces as first-class queryable datasets;
- dashboards or a visualization UI;
- a full alert manager;
- additional ingestion protocols;
- new authentication systems or enterprise administration layers;
- arbitrary user-defined server-side code;
- broader object-storage architecture;
- more distributed query, replication, or cluster features;
- many additional language bindings;
- benchmark marketing that claims universal superiority over another TSDB.

Existing implementations can be maintained for correctness and security, but
they do not take priority over the embedded trust, resource, testkit, and
compatibility gates.

## Current maturity caveats

- tsink is pre-1.0; public APIs and on-disk upgrade guarantees are still being
  hardened.
- The builder has individual memory, cardinality, WAL, retention, and
  concurrency controls. Finite named resource profiles and a comprehensive
  tested resource envelope are not shipped, and several important quotas
  default to no explicit limit.
- `write_batch` reports indexed structured outcomes with explicit atomic or
  best-effort policy, and the principal ingest adapters preserve durability and
  partial/indeterminate effects. Cross-component and cross-node atomicity are
  deliberately not claimed.
- The repository does not yet contain a `tsink-test` crate, public controllable
  clock, or deterministic maintenance fixture.
- PromQL, Prometheus protocol, and OTLP implementations exist, but generated
  compatibility matrices and a pinned differential test program do not.
- Existing tiering, security, rules, synchronization, and server operations are
  advanced surfaces that require configuration-specific evaluation.
- Cluster mode is experimental and carries no implied production-grade
  consensus or availability guarantee.

Use the [README](../README.md) for a current quick start and
[design-partner guide](design-partner-guide.md) to record evidence from real
integrations.
