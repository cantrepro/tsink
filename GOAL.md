# tsink: Product and Engineering Goal

> **Repository:** <https://github.com/cantrepro/tsink>
>
> **Baseline observed while drafting:** `master` at `98d2a5f`, package version `0.10.2`, on 2026-07-22. This is context, not a pinned requirement: the coding agent must inspect the checked-out revision and adapt this goal to the actual code before changing behavior.

> **Purpose:** This file is an execution charter for an autonomous coding agent working in the `tsink` repository. Treat it as the product direction, engineering priority order, and definition of done for the next several releases.
>
> **Primary objective:** Turn tsink into the most trustworthy and convenient **embedded, resource-bounded, Prometheus-native metrics engine** for applications, automated tests, agents, and edge devices.
>
> **Working product description:**
>
> **tsink is an embeddable metrics database that gives applications durable local retention and PromQL without requiring a separate database service.**
>
> A useful mental model is **“SQLite or DuckDB for Prometheus-style metrics,”** not “a smaller replacement for every distributed TSDB.”

---

## 1. Instructions to the coding agent

This document describes a multi-release direction. Do not attempt to implement every item in one giant change. Work continuously in the priority order defined below, keep the repository buildable, and finish each hard gate before moving to the next dependent phase.

### 1.1 First actions

Before changing code:

1. Read the entire repository, especially:
   - `README.md`
   - root `Cargo.toml`
   - every workspace member’s `Cargo.toml`
   - `src/storage.rs`, `src/async.rs`, `src/wal.rs`, `src/error.rs`, and `src/engine/`
   - `src/promql/`
   - `crates/tsink-server/`
   - `crates/tsink-uniffi/`
   - `docs/architecture.md`, storage/WAL/PromQL/protocol/configuration docs
   - all tests, benches, scripts, and GitHub Actions workflows
2. Verify the current branch, version, feature flags, CI commands, and public APIs. This goal was written against a repository state around `0.10.2`; the code may have advanced.
3. Run the existing test and lint suite before making changes. Record any pre-existing failures rather than hiding them.
4. Create or update `docs/goal-progress.md` with:
   - current revision and date;
   - baseline test results;
   - a checklist mirroring the phases in this document;
   - decisions made;
   - links to relevant code, tests, and documentation;
   - remaining risks or blockers.
5. For any substantial or irreversible design change, add a short ADR under `docs/adr/` before implementation.

If the current code conflicts with an API name or implementation detail in this document, preserve the **intent** and adapt to the actual code. Do not blindly create duplicate abstractions.

### 1.2 Operating rules

Follow these rules throughout the work:

- **Data safety outranks throughput, convenience, and backward compatibility.** Never preserve an API behavior that can silently lose acknowledged data merely to avoid a breaking change.
- **Do not silently drop writes, truncate query results, skip corrupt data, or delete unexpired data.** Every such outcome must be explicit, observable, and documented.
- **Do not claim compatibility that is not backed by tests.** Use precise phrases such as “supports the tested subset listed in `docs/promql-compatibility.md`.”
- **Keep the core embeddable.** The root engine crate must not require a daemon, network listener, Tokio, Kubernetes, object storage, authentication service, or another process.
- **Keep optional surfaces optional.** Protocol servers, Python bindings, object storage, and experimental clustering must be separable from the core through crate boundaries and/or feature flags.
- **Do not expand the feature catalogue while core trust work is unfinished.** In particular, do not add SQL, logs, traces, more ingestion protocols, dashboards, alert managers, new cluster features, or additional language bindings unless this document explicitly reaches that phase.
- **Prefer small, reviewable changes.** Avoid a full rewrite. Preserve well-tested internals unless a focused redesign is necessary for correctness or the embedded contract.
- **Keep every commit compiling whenever practical.** At minimum, do not leave the branch in a knowingly broken state at the end of a work session.
- **No placeholder implementations.** Do not satisfy interfaces with `todo!()`, unconditional success, ignored errors, fake durability, or no-op quota checks.
- **No hidden global runtime.** Background threads must be owned by a storage instance, named, bounded, observable, and cleanly stoppable.
- **No unbounded work from untrusted input.** Parsing, regex execution, protocol decompression, query fan-out, allocations, and cardinality creation require limits.
- **Preserve cross-platform support.** Linux and Windows are first-class. Keep filesystem and process-kill tests portable or clearly separate platform-specific portions.
- **Use stable Rust.** Establish and document an MSRV rather than accidentally relying on the newest compiler.
- **Document public APIs.** New public items require useful rustdoc, examples where appropriate, and migration notes when replacing older APIs.
- **Tests are part of the feature.** A behavior is not complete until success, failure, restart, and boundary cases are tested.

### 1.3 Autonomy and decision-making

The agent is expected to continue autonomously for as long as useful work remains. Apply these decision rules:

- Start with the first unfinished hard gate and continue to the next unblocked item after completing it.
- When details are ambiguous, choose the safest reversible design that advances the product thesis, document the choice, and proceed.
- Ask for human input only when a decision is genuinely irreversible, requires credentials, changes licensing/ownership, publishes externally, or is explicitly marked `HUMAN GATE`. Continue with other unblocked work instead of stopping.
- Do not publish crates, wheels, releases, images, or packages without configured credentials and explicit authorization. Preparing and dry-running release automation is allowed.
- Do not force-push, rewrite shared history, delete user branches, or discard unrelated working-tree changes.
- Do not add secrets, tokens, personal data, generated credentials, or machine-specific paths to the repository.
- If an external compatibility service or network download is unavailable, keep the local deterministic suite working, document the missing external verification, and proceed with other tasks.
- If a broad refactor becomes necessary, first land characterization tests and split the refactor into behavior-preserving steps.
- If a benchmark conflicts with a correctness invariant, retain correctness and record the performance cost.
- If this document contains more work than can fit in one execution window, leave the current hard gate complete and the next task precisely described rather than starting several unfinished subsystems.

### 1.4 Progress discipline

Use these labels in `docs/goal-progress.md`:

- `DONE` — implemented, documented, and verified by named tests.
- `IN PROGRESS` — actively being changed; repository remains buildable.
- `BLOCKED` — cannot proceed without a human decision or credential; document exactly why.
- `DEFERRED` — intentionally outside the current release gate.
- `HUMAN GATE` — requires maintainer or real-user validation and cannot be truthfully completed by code alone.

Do not mark an item `DONE` without evidence. Evidence should include the relevant test name or command and, where useful, benchmark output or a documentation path.

### 1.5 Standard verification commands

Adapt these to the repository’s actual features and CI, but keep equivalent coverage:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
```

Also add and run, as applicable:

```bash
cargo test -p tsink
cargo test -p tsink-test
cargo test -p tsink-server
cargo test -p tsink-uniffi
cargo test --workspace --no-default-features
cargo package -p tsink --list
```

The final branch must pass the repository’s complete CI matrix. New expensive compatibility, fault-injection, fuzz, or soak suites may be scheduled separately, but they must be runnable locally with documented commands and must have at least an appropriate CI cadence.

---

## 2. Product thesis

### 2.1 The problem to solve

Many applications need several hours or days of queryable metrics, but operating a separate TSDB is disproportionate to the problem. Common examples include:

- integration tests that currently start Prometheus or another TSDB in Docker;
- self-hosted applications that want a built-in diagnostics or performance page;
- desktop developer tools that need local performance history;
- infrastructure agents that must retain metrics during network outages;
- industrial, home, or network gateways with limited memory and disk;
- commercial appliances whose customers should not need to install Prometheus;
- exporters and SDKs that need deterministic tests for Prometheus remote write, remote read, OTLP metrics, and PromQL behavior.

These users do not primarily need horizontal scaling. They need a database component that behaves politely inside another product:

- one library and one data directory;
- no mandatory sidecar or daemon;
- predictable memory, disk, CPU, thread, and query usage;
- explicit write and durability results;
- safe process restart and software upgrade;
- Prometheus-compatible data and query semantics;
- optional local HTTP protocol endpoints;
- straightforward snapshot, restore, inspection, and export;
- eventual synchronization to a central backend when connectivity exists.

### 2.2 Positioning

The primary positioning is:

> **An OEM-grade, embedded Prometheus metrics engine with bounded resources and deterministic testing support.**

The first sentence of project documentation should describe the embedded use case. Standalone server mode is an adapter around the same engine. Cluster mode is experimental and must not be presented as the reason to adopt the project.

### 2.3 Differentiation

Do not try to differentiate with generic claims such as:

- “written in Rust”;
- “lightweight”;
- “single binary”;
- “supports PromQL”;
- “supports many protocols”;
- “runs at the edge.”

Established systems already make most of those claims. Differentiate through the complete embedded contract:

1. **No separate service required.**
2. **Hard, tested resource bounds.**
3. **Explicit write admission and durability semantics.**
4. **Deterministic test mode with a controllable clock and maintenance loop.**
5. **Prometheus protocol and PromQL behavior measured by a differential compatibility suite.**
6. **Versioned, safely upgradable on-disk storage.**
7. **Local-first store-and-forward synchronization instead of a custom distributed database.**
8. **Polished Rust and Python integration.**

### 2.4 Beachhead and expansion path

The first adoption beachhead is:

> **A no-Docker Prometheus and OTLP test database for integration tests.**

This is deliberately lower-risk than asking users to trust a young engine with production data. It provides immediate value, forces protocol and query correctness, and introduces teams to the embedded engine.

The intended expansion path is:

```text
integration tests
    -> developer tools
    -> self-hosted applications
    -> agents and appliances
    -> offline and edge telemetry with remote synchronization
```

---

## 3. Target users and jobs to be done

### 3.1 Test authors

A test author should be able to start a temporary tsink instance in-process or on an ephemeral loopback port, write through a real protocol, advance virtual time, run maintenance deterministically, evaluate PromQL, assert results, and tear it down without Docker, fixed ports, arbitrary sleeps, or leaked processes.

Success looks like this conceptually:

```rust
let db = TsinkTestDb::builder()
    .temporary()
    .manual_clock()
    .prometheus_api()
    .otlp_http()
    .start()?;

application.configure_remote_write(db.remote_write_url());
application.emit_test_metrics()?;

db.advance(Duration::from_secs(60))?;
db.run_maintenance_until_idle()?;
db.assert_promql_scalar("sum(rate(http_requests_total[1m]))", 12.0)?;
```

The actual API may differ, but it should be similarly direct and deterministic.

### 3.2 Application and appliance developers

An application developer should be able to add durable local metric history with a small, stable API. They must be able to choose a resource profile, understand every background thread, receive explicit pressure and failure signals, perform snapshots and upgrades, and expose an optional Prometheus-compatible query endpoint.

### 3.3 Edge and agent developers

An edge developer should be able to retain data locally under hard limits while disconnected, then synchronize it to a standard remote-write destination after reconnecting. Local retention, upload retention, bandwidth, retry behavior, and failure visibility must be independently configurable.

---

## 4. North-star outcomes

The project is moving in the correct direction when all of the following are true:

1. A Rust user can add tsink, insert metrics, query them, and close the database from a concise example without starting another process.
2. A test suite can replace a Prometheus container with `tsink-test` and use no arbitrary sleeps.
3. Every successful write response truthfully identifies the durability level achieved.
4. A partial batch is never reported as a full success.
5. Default user-facing profiles have finite memory, disk, WAL, cardinality, query, and concurrency limits.
6. The engine behaves predictably when each limit is reached and reports a structured reason.
7. Every supported on-disk format has an explicit version and tested upgrade path.
8. Durable acknowledgements survive abrupt process termination in the crash test harness.
9. PromQL and protocol support are described by a generated or test-backed capability matrix.
10. README claims are reproducible from public tests, examples, and benchmark scripts.
11. Rust and Python package metadata, documentation, and releases point to the current repository and version.
12. At least several external projects use tsink as an embedded engine or test database before `1.0`.

### 4.1 Adoption-oriented metrics

Code cannot create adoption by itself, but prepare the project to measure it. Useful indicators include:

- external repositories depending on the `tsink` crate;
- test suites removing a Prometheus/TSDB container in favor of `tsink-test`;
- applications successfully opening data created by an older tsink release;
- edge deployments completing disconnect/reconnect synchronization cycles;
- issue reports about real integrations rather than only feature requests;
- time-to-first-query from a clean machine;
- package download counts and release artifact usage.

Create a lightweight `docs/design-partner-guide.md` with integration questions and a template for recording findings. Recruiting users is a `HUMAN GATE`, not something the coding agent may pretend to complete. The guide must explicitly seek three design-partner archetypes:

1. a project that currently starts Prometheus or another TSDB in integration tests;
2. a self-hosted application or developer tool that wants built-in local metrics;
3. an agent, gateway, or appliance that needs bounded offline retention.

For each integration, record:

- where installation, compilation, packaging, or startup fails;
- which APIs are confusing or force knowledge of engine internals;
- whether the user needs direct query APIs, PromQL, or both;
- actual RAM, disk, retention, cardinality, startup, and shutdown constraints;
- whether store-and-forward synchronization is required;
- what would prevent the user from shipping tsink;
- which data-loss, corruption, recovery, or upgrade scenario concerns them most;
- which requested subsystem is backed by an immediate real-world blocker rather than hypothetical appeal.

Apply this product rule: **do not add another major subsystem until at least one external user is concretely blocked without it.** This rule does not prevent correctness, security, compatibility, documentation, or packaging work.

---

## 5. Scope, priority, and non-goals

### 5.1 Primary product surfaces

Invest in these surfaces first:

1. Direct embedded Rust API.
2. Python bindings for the stable embedded API.
3. Prometheus remote write.
4. Prometheus remote read where useful for compatibility.
5. Prometheus HTTP query endpoints, including tested PromQL behavior.
6. OTLP HTTP metrics ingest.
7. The `tsink-test` library and pytest integration.
8. Snapshots, restore, inspection, and portable export.
9. One-way store-and-forward remote-write synchronization.

### 5.2 Secondary surfaces

Maintain, but do not let these drive the architecture:

- standalone single-node server mode;
- Influx line protocol if already working and inexpensive to preserve;
- object-backed warm/cold tiers;
- non-Prometheus internal value types;
- existing administrative and security capabilities.

### 5.3 Experimental surfaces

Treat existing cluster functionality as experimental:

- do not advertise it in the headline or primary quick start;
- isolate it behind an explicit `experimental-cluster` feature and/or clearly separated crate boundary where feasible;
- keep compile and regression coverage so existing code does not silently rot;
- do not add cluster features during the trust and testkit milestones;
- do not call a custom control plane “production-grade consensus” without substantial formal and failure testing;
- if production clustering is revisited, prefer a mature consensus implementation and a dedicated design review.

### 5.4 Explicit non-goals for the current roadmap

Do not add the following before the hard gates in this document are complete:

- SQL or a general analytical query language;
- logs and traces as first-class queryable datasets;
- dashboards or a visualization UI;
- a full alert manager;
- more ingestion protocols;
- new authentication systems or enterprise administration layers;
- arbitrary user-defined server-side code;
- more general object-storage architecture;
- additional distributed query or replication features;
- many new language bindings;
- benchmark marketing intended to prove universal superiority over another TSDB.

Existing implementations may remain, but they must not consume roadmap priority unless a correctness or security issue requires work.

---

## 6. Baseline assumptions to verify

The repository state used to write this goal appears to have the following characteristics. Verify each one against the current branch and correct this section in `docs/goal-progress.md`; do not assume it remains exact:

- The root package is around version `0.10.2`.
- The workspace includes the root `tsink` engine, `tsink-server`, and `tsink-uniffi`.
- The root crate describes itself as an embedded TSDB, but the README gives equal prominence to embedded, server, and cluster modes.
- The engine has a runtime-independent asynchronous façade and does not require an async runtime in the core.
- The public storage API includes write acknowledgement concepts such as volatile, WAL-appended, and durable completion.
- The builder exposes memory, cardinality, WAL size, replay, and other controls, but several limits default to effectively unlimited values.
- The architecture documentation and actual write API may disagree about per-row outcomes or silent rejection behavior. Audit code and tests; resolve the inconsistency in favor of explicit, truthful results.
- Snapshots and restore support already exist and should be hardened rather than recreated.
- PromQL, remote write/read, OTLP, multiple extra protocols, tiered storage, rollups, security, and cluster functionality already create a very broad maintenance surface.
- CI already covers substantial linting and cross-platform tests.
- A publish workflow exists, while GitHub release packaging and project metadata may still be incomplete or stale.
- The builder has test-only time overrides and background-thread controls that can inform, but should not substitute for, a proper public test clock and deterministic runtime design.

---

## 7. Architectural principles

### 7.1 The embedded contract

“Embedded” is a product contract, not merely the ability to import a crate. The engine must guarantee:

- no mandatory network listener;
- no mandatory async runtime;
- no mandatory external process or service;
- no hidden global state;
- instance-owned, bounded, named background threads;
- deterministic startup and shutdown;
- explicit flush, persistence, compaction, and snapshot controls;
- finite resource profiles;
- structured backpressure and errors;
- a documented durability contract;
- a versioned storage format and upgrade policy;
- a stable, intentionally small public API;
- optional protocol endpoints implemented as adapters.

### 7.2 Protocols are adapters

The storage engine must not know about HTTP servers, authentication, Kubernetes, or tenant routing. Protocol crates translate external representations into a small core write/query API and map structured errors back into protocol-appropriate responses.

Avoid duplicating ingest semantics in every adapter. Validation, admission, durability outcomes, and query budgets should have a shared core representation.

### 7.3 Local first, synchronization second

For edge deployments, prefer this architecture:

```text
embedded local store
    -> durable export queue or checkpointed scan
    -> one-way remote-write synchronization
    -> central Prometheus-compatible backend
```

Do not require membership, leader election, split-brain handling, distributed snapshots, or cross-node consistency for the primary edge use case.

### 7.4 Correctness before micro-optimization

Preserve useful performance work, but prioritize these invariants:

- acknowledged data is not silently lost;
- the same logical series has one canonical identity;
- visibility across active, sealed, WAL, persisted, compacted, and tiered data is consistent;
- deletion and retention do not resurrect data;
- compaction replacement is atomic;
- readers do not see corrupt or half-published files;
- resource-limit failures leave state internally consistent;
- shutdown does not return before the promised durability level is satisfied;
- upgrade and recovery either succeed completely or fail with actionable diagnostics.

---

# PART I — v0.11: Focus and Trust

## 8. Phase 0: Repository baseline, product focus, and hygiene

**Priority:** P0

**Goal:** Make the project’s identity and current maturity clear before adding another subsystem.

### 8.1 Deliverables

- [ ] Create `docs/goal-progress.md` and establish the baseline.
- [ ] Fix stale `repository`, `homepage`, author/contact, package links, badges, and documentation URLs across all manifests and package metadata.
- [ ] Audit crate descriptions and keywords so they consistently describe an embedded Prometheus metrics engine.
- [ ] Rewrite the top of `README.md` around the embedded/test/local-retention use case.
- [ ] Put an embedded Rust quick start first.
- [ ] Add a “metrics integration tests without Docker” quick start as soon as the testkit exists; until then, add a clearly marked roadmap section rather than pretending the package exists.
- [ ] Move server, cluster, object storage, RBAC, and the full protocol catalogue into “Advanced and experimental capabilities.”
- [ ] Label cluster mode experimental in README, configuration docs, CLI help, and cluster docs.
- [ ] Add `docs/product-positioning.md` containing the product thesis, target users, primary use cases, and non-goals from this file.
- [ ] Add `CHANGELOG.md` if absent and record user-visible compatibility changes from this effort.
- [ ] Establish an MSRV in `Cargo.toml`, CI, and documentation after verifying dependencies.
- [ ] Ensure `cargo package` includes all required docs and excludes accidental development artifacts.
- [ ] Raise public API documentation quality. Do not optimize solely for a coverage percentage; document the APIs users must understand to embed and operate the engine.

### 8.2 README target structure

Use roughly this information hierarchy:

1. One-sentence embedded product promise.
2. Four to six concrete benefits:
   - embedded Rust/Python;
   - hard resource limits;
   - explicit durability;
   - Prometheus/OTLP compatibility;
   - deterministic test mode;
   - optional remote synchronization.
3. Minimal Rust example.
4. Testkit example.
5. Typical uses.
6. Stability and compatibility status.
7. Server mode.
8. Advanced/experimental features.
9. Links to detailed docs.

Avoid a first screen dominated by a huge feature inventory.

### 8.3 Acceptance criteria

- A new visitor can identify the intended user and primary job in the first paragraph.
- No package or documentation link points to an old repository owner.
- Cluster mode is not presented as production-ready by implication.
- All existing functionality remains discoverable, but advanced features no longer obscure the main product.
- Workspace formatting, linting, tests, docs, and package dry runs pass.

---

## 9. Phase 1: Write safety and truthful admission semantics

**Priority:** P0 and a hard gate

**Goal:** No caller can mistake a partial, rejected, volatile, or failed write for a complete durable success.

### 9.1 Audit before design

Trace every write path from all public entry points through validation, series creation, admission, WAL append, memory mutation, visibility publication, protocol response mapping, and language bindings.

Document:

- whether batches can partially succeed;
- the exact ordering of series-definition WAL records and sample records;
- whether state can be mutated before an error is returned;
- how memory, cardinality, WAL, timestamp, retention, and validation rejections are represented;
- whether protocol adapters currently discard row-level details;
- whether retries can duplicate samples;
- the semantics for duplicate timestamps and out-of-order samples;
- what shutdown or background degradation does to new writes.

Add an ADR for the canonical write contract.

### 9.2 Canonical API requirements

Introduce or refine a canonical batch API. Adapt names to the existing code, but provide these semantics:

```rust
pub enum WriteMode {
    Atomic,
    BestEffort,
}

pub struct BatchWriteResult {
    pub submitted: usize,
    pub accepted: usize,
    pub rejected: usize,
    pub acknowledgement: WriteAcknowledgement,
    pub outcomes: Vec<RowWriteOutcome>,
}

pub struct RowWriteOutcome {
    pub index: usize,
    pub status: RowWriteStatus,
}

pub enum RowWriteStatus {
    Accepted,
    Rejected(WriteRejection),
}
```

This is illustrative, not a mandate to duplicate existing types. Required behavior:

- The canonical API returns enough information to identify all rejected rows.
- Rejections use structured enums rather than string matching.
- Expected rejection categories include invalid metric/labels, unsupported value, timestamp bounds, retention floor, future skew, cardinality, cardinality creation rate, memory pressure, disk quota, WAL quota, query/tenant policy where applicable, write timeout, closed database, degraded/fenced database, and internal I/O failure.
- A batch-level acknowledgement is the weakest guarantee that applies to its accepted rows, unless acknowledgements are recorded per row.
- Empty batches have explicitly documented behavior.
- `Atomic` mode either admits and commits the whole batch or commits none of it. If true atomicity cannot be implemented safely in the current milestone, do not fake it; omit or mark the mode unsupported and make best-effort partial behavior explicit.
- Best-effort behavior must be named explicitly.
- A compatibility `insert_rows` method may remain temporarily, but it must return an error when any row is rejected. It may not report `Ok(())` for a partial batch.
- Deprecate ambiguous APIs with migration documentation rather than removing them without guidance.
- Rust, async, Python, and HTTP surfaces must preserve the same truth. No adapter may collapse partial success into unconditional success.

### 9.3 Durability acknowledgement requirements

Keep or refine explicit levels such as:

- `Volatile` — visible in memory but not protected by a crash-recovery log;
- `Appended` — encoded and appended to the WAL, but not yet known to be durable after sudden power/process loss;
- `Durable` — all required WAL/file and directory synchronization for the documented platform contract completed before return.

For each WAL sync mode, document exactly which acknowledgement a successful write may return. Do not use `Durable` merely because data was handed to the operating system.

### 9.4 Protocol response mapping

For remote write, OTLP, text import, Influx, StatsD, and Graphite adapters:

- map total failure to an appropriate non-success response or observable transport error;
- do not send a success status for a partially rejected request unless the protocol has no way to represent partial success and the rejection is still explicitly observable and documented;
- include bounded diagnostic detail without echoing enormous payloads;
- increment rejection counters by reason;
- avoid leaking sensitive label values in logs by default;
- test decompression, parser, and batch limits.

For lossy protocols such as UDP, document the transport limitation and expose counters. Do not allow their semantics to weaken the direct embedded API.

### 9.5 Tests

Add tests for at least:

- all rows accepted;
- one invalid row in the middle of a best-effort batch;
- one invalid row in an atomic batch;
- cardinality limit reached;
- memory admission limit reached;
- WAL quota reached;
- disk quota reached once implemented;
- timestamp below retention floor;
- excessive future timestamp;
- write timeout;
- I/O failure before and after WAL append using fault injection;
- shutdown racing a write;
- background worker degradation fencing writes;
- async façade preserving the result;
- Python binding preserving the result;
- each principal HTTP ingest adapter mapping the result correctly.

### 9.6 Acceptance criteria

- No public write path silently loses rejected rows.
- The architecture document and code agree.
- Every successful response identifies its durability guarantee.
- Partial writes are visible to the caller and metrics.
- Failure tests prove that unsuccessful operations do not corrupt series identity, visibility, WAL replay, or memory accounting.

**Do not begin major testkit or synchronization work until this gate is complete.**

---

## 10. Phase 2: Resource-bounded profiles and admission control

**Priority:** P0 and a hard gate

**Goal:** Make “resource-bounded” a tested product property rather than an optional set of low-level knobs.

### 10.1 Introduce explicit profiles

Add a public profile abstraction, for example:

```rust
pub enum ResourceProfile {
    Test,
    Embedded,
    Edge,
    Server,
    Custom(ResourceLimits),
}
```

Avoid forcing the exact shape above if the builder already has a better abstraction. Requirements:

- User-facing profiles have finite defaults.
- Unlimited values are available only through an explicit expert configuration, not accidental `usize::MAX` defaults.
- The selected profile and effective limits are inspectable after build.
- Builder overrides are deterministic and documented.
- Invalid combinations fail during build with actionable errors.

Proposed starting profiles may be conservative, but determine final values with tests and resource measurements. A reasonable initial design target is:

| Profile | Intended use | Memory | Data/WAL disk | Cardinality | Background behavior |
|---|---|---:|---:|---:|---|
| `Test` | CI and unit/integration tests | small and fixed | small temporary quota | low | manual/deterministic available |
| `Embedded` | self-hosted apps and desktop tools | moderate, cgroup-aware cap | finite local quota | moderate | bounded automatic maintenance |
| `Edge` | agents/gateways | lower cap | finite, retention-driven | lower | low idle CPU and bandwidth |
| `Server` | dedicated process | larger but finite unless explicitly overridden | explicit operator quota | explicit | higher concurrency |

Do not publish arbitrary numbers without measuring them. It is acceptable to revise proposed values during implementation, but it is not acceptable for all standard profiles to remain unlimited.

### 10.2 Memory accounting

The limit must cover more than active chunk payloads. Inventory and account for:

- active and sealed chunks;
- series registry and interned strings;
- label/postings indexes;
- metadata caches;
- tombstones and rollup metadata;
- query working sets;
- decompression buffers;
- pending write batches;
- WAL buffers;
- remote/tier metadata;
- memory-mapped regions, clearly distinguishing virtual mapping from resident/accounted memory.

Where exact accounting is impossible, expose `accounted`, `estimated`, and `excluded` categories. Never advertise a hard total-process memory limit if only one subset is bounded.

Add pressure levels and observability:

- normal;
- approaching limit;
- backpressured;
- rejecting;
- degraded.

### 10.3 Disk quota

Implement a real local disk budget that includes, as appropriate:

- WAL files;
- persisted segments and indexes;
- tombstones/catalog metadata;
- temporary compaction output;
- migration staging files;
- local remote-write queue/checkpoints when that feature exists.

Requirements:

- reserve configurable free-space headroom;
- estimate compaction/migration temporary space before starting;
- clean expired data first when permitted;
- never delete unexpired data merely to make a write appear successful;
- reject writes with a structured `DiskQuotaExceeded` or `InsufficientCompactionHeadroom` result;
- recover accounting after restart and orphan cleanup;
- tolerate external files in the directory without deleting them;
- expose current usage, effective quota, reserved headroom, and major categories;
- test filesystem-full and injected short-write behavior.

A snapshot written outside the data directory should not silently consume the database quota, but its failure must be explicit. A snapshot inside the data directory should be rejected or accounted for consistently.

### 10.4 Cardinality and label controls

Add or verify:

- maximum total active series;
- maximum new series per time window;
- maximum labels per series;
- maximum metric and label name/value lengths;
- maximum cumulative identity bytes per series;
- duplicate-label validation;
- canonical label ordering;
- optional per-metric or per-tenant budgets in server mode without complicating the core API.

Cardinality rejection must occur before expensive allocation and before a series identity is durably published unless the entire write commits.

### 10.5 Query budgets

Add a shared query budget abstraction with finite defaults for user-facing profiles:

- maximum series matched;
- maximum raw samples scanned;
- maximum samples or bytes returned;
- maximum query wall-clock time or cancellation deadline;
- maximum concurrent queries;
- maximum regex/pattern complexity or candidate expansion;
- maximum range/subquery steps;
- maximum intermediate vector size;
- maximum memory reserved per query.

Return structured limit errors. HTTP adapters should map them to stable error responses. Direct APIs should not silently paginate or truncate unless pagination was explicitly requested.

### 10.6 Background work and CPU

Bound and expose:

- writer permits;
- read worker count;
- compaction concurrency;
- remote/tier fetch concurrency;
- maintenance cadence;
- rollup concurrency;
- synchronization bandwidth and workers later.

Background threads must sleep efficiently when idle. Add a test or benchmark for idle CPU and clean shutdown.

### 10.7 Tests and invariants

Use tiny limits to exercise boundaries quickly. Add property or model tests for:

- memory accounting never underflows;
- rejected writes do not increase cardinality;
- failed compaction does not exceed quota permanently or publish partial output;
- disk quota survives restart;
- concurrent writers cannot collectively bypass a limit;
- query cancellation releases permits and memory reservations;
- retention and quota cleanup do not resurrect tombstoned data;
- observability snapshots remain internally consistent.

### 10.8 Acceptance criteria

- Standard profiles are finite and documented.
- Hitting any limit produces predictable, structured behavior.
- A reproducible test demonstrates operation under small memory and disk budgets.
- No benchmark or README claim says “hard memory limit” unless the measured scope is accurately described.
- Resource failures do not corrupt the database or silently discard accepted data.

---

## 11. Phase 3: Durability contract, storage format, upgrades, and crash recovery

**Priority:** P0 and a hard gate

**Goal:** Make restart and upgrade behavior boring, explicit, and testable.

### 11.1 Durability matrix

Create `docs/durability.md` with a table covering every storage/WAL mode. For each mode specify:

- when writes become visible to current readers;
- whether data survives clean close;
- whether data survives process termination;
- whether data is intended to survive power loss;
- what `Volatile`, `Appended`, and `Durable` mean;
- which files and directories are synchronized;
- how periodic sync intervals affect loss windows;
- behavior when fsync fails;
- behavior when a background durability worker fails;
- differences across supported operating systems/filesystems, where known.

Avoid absolute hardware guarantees that software cannot provide. State the exact operations the engine performs.

### 11.2 Versioned data-directory manifest

Add a small, atomically written manifest with:

- magic identifier;
- storage format version;
- minimum reader version if needed;
- creating tsink version;
- last successfully opened tsink version;
- enabled format-affecting features;
- timestamp precision and immutable storage parameters;
- checksum/version for the manifest itself.

Opening behavior must be explicit:

- current supported format: open normally;
- older supported format: migrate or open through a tested compatibility path;
- newer unknown format: refuse without modification;
- corrupt manifest: refuse or enter an explicit inspection/recovery flow;
- missing manifest in a legacy directory: detect only through a safe, tested legacy path;
- empty directory: initialize atomically.

Never guess a format from arbitrary files and then mutate the directory.

### 11.3 Migration policy

Create `docs/storage-format.md` and define:

- which prior versions are supported;
- whether upgrades are in-place, copy-on-write, or export/import;
- whether downgrade is supported;
- required free-space headroom;
- backup/snapshot requirements;
- failure and rollback behavior;
- how long a migration may block access;
- how applications receive progress and errors.

Prefer copy-on-write or staged atomic replacement for risky migrations. If an in-place migration is unavoidable, make it resumable and journaled.

### 11.4 Golden compatibility fixtures

Commit small data-directory fixtures produced by supported older releases or by a versioned fixture generator. Tests must:

1. open the old fixture;
2. query known data;
3. perform the supported upgrade;
4. write more data;
5. close and reopen;
6. verify all old and new data;
7. verify tombstones, retention metadata, native histogram data, and WAL recovery where applicable.

Keep fixtures small and document how they were generated. Never regenerate old fixtures silently with the new code.

### 11.5 Crash harness

Build a process-based crash test harness. It should:

- spawn a child process that opens a temporary database;
- write uniquely numbered samples;
- communicate each returned acknowledgement to the parent only after the call returns;
- terminate the child abruptly at controlled or randomized points;
- reopen the database in a new process;
- assert that every `Durable` acknowledged sample exists;
- treat `Appended` and `Volatile` samples according to their documented guarantees;
- assert that recovery never produces invalid series identities or corrupt values;
- repeat enough seeds to catch ordering bugs.

Add test-only failpoints around:

- series definition WAL append;
- sample WAL append;
- WAL flush and sync;
- chunk sealing;
- segment file creation;
- index writing;
- file sync;
- directory sync;
- catalog/manifest replacement;
- compaction publication;
- WAL truncation/reset;
- snapshot copy and final rename.

Failpoints must be compiled out of normal builds or safely inert.

### 11.6 Corruption and recovery tools

Provide an inspection path that can:

- identify the data-directory format;
- list WAL segments and persisted segments;
- verify checksums and catalog references;
- report orphan or missing files;
- distinguish a corrupt tail from mid-log corruption;
- run read-only by default;
- produce a bounded machine-readable report.

Strict recovery remains the safe default. Salvage must be explicit, produce a report of discarded ranges, and never overwrite the original without a backup or destination directory.

### 11.7 Snapshot and restore hardening

Since snapshot/restore functionality already exists, audit and test:

- consistency with concurrent writes;
- whether acknowledged writes included in the snapshot match the documented fence;
- atomic destination publication;
- behavior when the destination exists;
- full-disk and permission failures;
- manifest/version preservation;
- restore into non-empty directories;
- cleanup after interrupted snapshot or restore;
- Windows file-handle and rename behavior.

### 11.8 Acceptance criteria

- Every data directory has an explicit format identity.
- Opening an unsupported newer format cannot mutate it.
- At least one previous-version fixture is tested.
- The crash harness proves the durable acknowledgement contract.
- Strict and salvage recovery are unambiguous and tested.
- Snapshot and restore are included in compatibility testing.

---

# PART II — v0.12: The Testkit Release

## 12. Phase 4: Create `tsink-test`

**Priority:** P1 after the trust gates

**Goal:** Make tsink the easiest way to test Prometheus- and OTLP-based metric integrations without Docker.

### 12.1 Crate and dependency design

Create a dedicated workspace crate, preferably `crates/tsink-test`, with a package name such as `tsink-test` if available and appropriate.

Requirements:

- It depends on the core engine and reusable protocol-library code, not on shelling out to the server binary.
- Refactor `tsink-server` so reusable protocol handlers can be instantiated from a library target if necessary.
- Do not move HTTP/auth/cluster dependencies into the core engine.
- Default testkit features should stay reasonably small.
- Optional features may enable Prometheus HTTP, remote write/read, OTLP, and advanced protocols.
- It must work without Docker and without downloading binaries at test runtime.

### 12.2 Core `TsinkTestDb` behavior

Provide a high-level test fixture with:

- temporary-directory mode;
- pure in-memory mode if the engine genuinely supports equivalent semantics;
- explicit persistent-directory mode for restart tests;
- automatic cleanup with an explicit `close()` that reports errors;
- loopback listeners bound to port `0`, never fixed ports;
- methods returning endpoint URLs;
- direct access to a deliberately small safe subset of the underlying storage API;
- a unique instance identifier for diagnostics;
- bounded logs and diagnostic dump on assertion failure;
- safe parallel use from multiple test processes.

Do not hide teardown errors exclusively in `Drop`. `Drop` may perform best-effort cleanup, while explicit close must be available for tests that need guarantees.

### 12.3 Clock abstraction

Promote the current test-only time overrides into a coherent internal `Clock` abstraction shared by retention, rollups, rules if retained, synchronization, and maintenance scheduling.

Provide:

- `SystemClock` for production;
- `ManualClock` or `TestClock` for deterministic tests;
- monotonic advancement by default;
- explicit errors for accidental backwards movement;
- an expert-only way to test clock rewind if a real behavior requires it;
- timestamp precision-aware conversion;
- no global clock singleton;
- no arbitrary sleeps in testkit examples.

All components that make time-based decisions must use the injected clock rather than calling wall-clock APIs directly.

### 12.4 Deterministic maintenance mode

Allow background threads to be disabled and maintenance to be driven explicitly:

- flush active chunks;
- persist sealed chunks;
- compact eligible levels;
- enforce retention;
- run rollup materialization;
- refresh catalogs/tier metadata where relevant;
- process synchronization retries later.

Expose high-level methods such as:

```rust
db.flush()?;
db.compact()?;
db.run_retention()?;
db.run_maintenance_once()?;
db.run_maintenance_until_idle()?;
```

`run_maintenance_until_idle` must have a bounded iteration/time limit and return a diagnostic error if work never converges.

### 12.5 Protocol endpoints

Support optional ephemeral endpoints for:

- Prometheus remote write;
- Prometheus remote read if useful;
- Prometheus instant and range query APIs;
- Prometheus text import;
- OTLP HTTP metrics.

Requirements:

- startup returns only after the listener is ready;
- shutdown joins listener tasks/threads;
- no port polling or arbitrary sleeps;
- request body, decompression, label, sample, and timeout limits are active even in test mode;
- endpoint helpers return typed URLs where practical;
- testkit error messages include bounded response bodies.

### 12.6 PromQL assertion API

Provide ergonomic helpers without hiding actual results:

- execute instant query at an explicit or current manual timestamp;
- execute range query with explicit start/end/step;
- assert scalar value with tolerance;
- assert vector/matrix series and labels;
- assert no data;
- assert a query fails with a specific unsupported/limit/parse error;
- normalize output ordering deterministically;
- include the query, evaluation time, actual result, and nearby stored series in failure output.

Do not implement a separate PromQL semantics layer in the assertion helper. It must exercise the same evaluator and API path being tested.

### 12.7 Fixture and data helpers

Provide concise helpers for:

- metric/label construction;
- counters, gauges, classic histograms, and native histograms where supported;
- sequences and evenly spaced samples;
- direct fixture insertion with explicit acknowledgements;
- remote-write payload generation through the real protobuf model;
- restart with the same data directory;
- corruption/fault fixtures for internal tests.

Keep helpers focused on testing metrics systems rather than becoming a general synthetic-data framework.

### 12.8 Python/pytest integration

Once the Rust testkit is stable enough, expose a Python fixture or small package that supports:

```python
def test_metrics(tsink_db):
    app = start_app(remote_write_url=tsink_db.remote_write_url)
    app.emit_metrics()
    tsink_db.advance(seconds=60)
    tsink_db.assert_promql_scalar(
        "sum(rate(http_requests_total[1m]))",
        expected=12.0,
    )
```

Requirements:

- context-manager support;
- pytest fixture example;
- temporary cleanup;
- explicit close errors;
- typed/structured write results;
- platform wheel coverage aligned with existing Python support;
- no requirement to invoke Cargo during a normal installed-package test.

Do not rush a second independent implementation. Reuse the same engine and testkit semantics through bindings or a small native wrapper.

### 12.9 Testkit acceptance suite

The testkit milestone is complete when tests prove:

- a remote-write client can send data and query it through PromQL;
- an OTLP client can export metrics and query the mapped series;
- virtual time drives range functions and retention without sleeping;
- maintenance can be run deterministically;
- a database can close, restart, and preserve expected data;
- parallel tests use independent ports and directories;
- teardown leaves no listener or worker threads;
- failures produce useful diagnostics;
- the basic path runs on Linux and Windows;
- the example suite uses no Docker.

---

## 13. Phase 5: Prometheus compatibility program

**Priority:** P1

**Goal:** Replace vague “PromQL compatible” claims with measured, reproducible compatibility.

### 13.1 Capability documents

Create and maintain:

- `docs/promql-compatibility.md`;
- `docs/prometheus-api-compatibility.md`;
- `docs/otlp-mapping.md`;
- `docs/protocol-limits.md`.

Each matrix entry should be one of:

- supported and differentially tested;
- supported with a documented difference;
- partially supported with exact constraints;
- unsupported and rejected explicitly;
- not yet tested.

Generate the matrix from test metadata if practical so documentation cannot easily drift.

### 13.2 Differential harness

Build a compatibility harness that executes the same fixture and queries against tsink and a pinned official Prometheus release, then normalizes and compares results.

The harness may use Docker or a downloaded official binary in a dedicated compatibility environment; this does not weaken the no-Docker promise of `tsink-test` itself.

Requirements:

- pin Prometheus version and verify artifact checksum;
- record version in test output;
- isolate data and ports;
- normalize label and series ordering only, not semantic differences;
- compare successful values, timestamps, labels, result types, and errors;
- preserve mismatches as readable fixtures;
- make adding a regression case straightforward;
- run a useful subset on every change and the full matrix on a scheduled or release workflow.

### 13.3 PromQL coverage priorities

Prioritize the semantics most useful for application metrics and test assertions:

1. selectors and label matchers;
2. instant and range vectors;
3. scalar/string/vector type checking;
4. arithmetic, comparison, and set operators;
5. vector matching and grouping;
6. aggregation operators;
7. `rate`, `irate`, `increase`, `delta`, `idelta`, and reset behavior;
8. common over-time functions;
9. histogram functions and classic/native histogram behavior;
10. subqueries, offsets, and `@` modifiers if claimed;
11. absent/staleness behavior;
12. NaN, infinities, empty vectors, duplicate timestamps, and boundary inclusivity;
13. parser limits and invalid input.

Do not add obscure functions merely to increase a count while common semantics remain incorrect.

### 13.4 Protocol compatibility

Test:

- remote-write content encoding and protobuf limits;
- metadata and exemplar handling if claimed;
- native histograms;
- out-of-order and duplicate samples;
- remote-read request/response shapes;
- query and query-range API envelopes;
- error status and payload format;
- timestamp units and rounding;
- OTLP resource/scope/attribute mapping;
- counter temporality and monotonicity;
- histogram and summary mapping;
- invalid labels and high-cardinality protection.

### 13.5 Fuzzing and property tests

Add fuzz targets or equivalent randomized tests for:

- PromQL lexer/parser;
- protobuf/snappy remote-write decoding;
- OTLP decoding and mapping;
- text and line protocol parsers retained by the project;
- WAL frame parsing;
- segment/index parsing;
- manifest parsing;
- label canonicalization;
- timestamp codecs;
- value codecs.

Untrusted input must not panic, hang, allocate without bound, or read out of bounds.

### 13.6 Acceptance criteria

- README compatibility language links to the matrices.
- Every fixed semantic bug receives a regression case.
- Unsupported syntax/functions fail explicitly rather than producing plausible but incorrect data.
- A documented command reproduces differential results.
- The project can state exactly which Prometheus release was used as the reference.

---

# PART III — v0.13: OEM and Edge Readiness

## 14. Phase 6: Stable lifecycle and OEM integration

**Priority:** P2 after testkit and compatibility foundations

**Goal:** Make tsink safe to bundle inside long-lived third-party applications.

### 14.1 Public lifecycle API

Audit and stabilize:

- builder validation;
- open/create/open-read-only distinctions;
- database lock acquisition;
- health/degraded state;
- write and query APIs;
- flush/persist/compact/maintenance controls;
- snapshot and restore;
- close and shutdown timeout;
- observability snapshot;
- version and effective-configuration inspection.

Avoid exposing a large number of engine-internal types. Add an intentional prelude or small primary API if that improves usability.

### 14.2 Thread and shutdown contract

Document:

- every thread an instance may create;
- how many are created under each profile;
- when they start;
- whether they may call user code;
- shutdown order;
- timeout behavior;
- whether `close()` flushes, persists, syncs, or compacts;
- what happens if the host process exits without close.

Tests must detect leaked threads and deadlocks. Do not block forever during shutdown.

### 14.3 Health and host integration

Expose structured health rather than only logs:

- healthy;
- pressure/backpressure;
- degraded but queryable;
- write-fenced;
- read-only recovery;
- closing/closed.

Allow a host application to subscribe to or poll important state changes without coupling the core to a specific async runtime. Keep callbacks bounded and do not invoke them while holding internal locks.

### 14.4 Read-only and inspection modes

Support opening a data directory read-only for:

- diagnostics;
- backup verification;
- export;
- safe inspection after a failed upgrade;
- serving historical queries when writes are intentionally disabled.

Read-only mode must not create, truncate, migrate, compact, enforce retention, or rewrite metadata.

### 14.5 Portable export

Add a portable export path, preferably behind an optional feature, using a standard format such as Parquet and/or Arrow.

Requirements:

- preserve metric name, canonical labels, timestamp, value type, and histogram representation;
- support bounded streaming rather than loading the whole database;
- expose time and series selectors;
- write to a temporary destination and publish atomically where applicable;
- document schema and version it;
- test round-trip or reference-reader interoperability;
- do not add SQL merely to expose export.

### 14.6 Language bindings

Make Rust and Python excellent before adding more languages:

- preserve structured errors and write outcomes;
- expose context-manager/lifecycle APIs in Python;
- avoid copies where practical but prefer safety over cleverness;
- document thread-safety and blocking behavior;
- ship type hints and examples;
- test wheel installation, not only source-tree imports.

A small stable C ABI may be considered only after the Rust API and storage format stop moving rapidly. Do not build many bindings directly against unstable internals.

### 14.7 Acceptance criteria

- A host can enumerate effective limits, health, and durability mode.
- Clean close is deterministic and tested under concurrent activity.
- Read-only open provably performs no mutation.
- Export handles large ranges through bounded streaming.
- Python examples mirror the truth of the Rust API.

---

## 15. Phase 7: Store-and-forward remote-write synchronization

**Priority:** P2; do not start before write safety, resource bounds, durability, and deterministic testing are complete

**Goal:** Support offline-first edge telemetry without building another distributed database.

### 15.1 Product semantics

Synchronization is one-way export from local tsink storage to a configured Prometheus remote-write destination. It is not membership, replication, consensus, or distributed query.

Local storage remains authoritative for its configured retention window. Upload progress is tracked independently.

### 15.2 Architecture requirements

Design a checkpointed exporter with:

- one or more named destinations only if the state model remains clear; start with one destination if necessary;
- persistent per-destination checkpoints;
- deterministic scan ordering by series identity and timestamp/window;
- bounded batches by sample count and encoded bytes;
- retry with exponential backoff and jitter;
- explicit authentication/TLS configuration in the adapter layer;
- bandwidth and request-concurrency limits;
- pause/resume;
- graceful shutdown and restart;
- metrics for lag, oldest unsent timestamp, queue/checkpoint size, retries, failures, bytes, samples, and last success;
- an explicit poison-data policy that never silently skips data;
- independent local retention and post-upload retention policies;
- no requirement for object storage.

Avoid copying all metric data into a second unbounded queue if a checkpointed scan of immutable local segments can provide equivalent safety. If a queue is necessary for correctness, include it in disk quotas and recovery tests.

### 15.3 Delivery semantics

Prometheus remote write is generally retryable but not a transactional exactly-once protocol. Document realistic semantics such as at-least-once delivery within a time range. Design for harmless retransmission of identical samples where the destination permits it.

A checkpoint may advance only after a successful destination response for the complete batch. Partial or ambiguous responses must not be treated as success.

If local retention is about to delete unsent data:

- expose an urgent health condition;
- optionally block deletion when configured;
- optionally allow explicit `drop_unsent_on_retention` behavior;
- never make the destructive choice silently.

### 15.4 Tests

Use a local mock remote-write destination to test:

- normal upload;
- connection refusal and recovery;
- timeouts;
- HTTP retryable and non-retryable status codes;
- connection reset after receiving part or all of a request;
- restart between send and checkpoint persistence;
- duplicate resend after ambiguous failure;
- batch size limits;
- bandwidth limiting;
- auth rotation if supported;
- pause/resume;
- retention collision with unsent data;
- disk quota including exporter state;
- multiple days of virtual clock advancement without wall-clock sleeps.

Add an optional interoperability test against a real compatible receiver.

### 15.5 Acceptance criteria

- A disconnected instance can continue ingesting under configured local limits.
- Reconnection resumes from a persistent checkpoint after process restart.
- No unsent range is skipped silently.
- Lag and destructive-retention risk are visible through API and metrics.
- The feature adds no distributed membership or consensus dependency.

---

# PART IV — Evidence, Packaging, and 1.0

## 16. Phase 8: Resource envelope and reproducible benchmarks

**Priority:** P1 throughout; publication after core semantics stabilize

**Goal:** Demonstrate predictable behavior under constraints instead of chasing a universal throughput trophy.

### 16.1 Benchmark principles

Do not lead with “tsink is N times faster than X.” Measure questions embedded users actually ask. Every published result must include:

- exact commit and version;
- hardware and operating system;
- filesystem;
- Rust version and build profile;
- configuration and resource profile;
- dataset generator and seed;
- raw results;
- commands to reproduce;
- known limitations.

### 16.2 Required scenarios

Measure at least:

- RSS and internal accounting at 10k, 100k, and 1M active series where feasible;
- bytes per sample for representative counters, gauges, sparse series, and histograms;
- startup time with increasing series/segment counts;
- crash recovery time with increasing WAL sizes;
- write throughput and latency under fixed small memory budgets;
- query latency during flush and compaction;
- behavior at cardinality, memory, WAL, and disk limits;
- idle CPU and resident memory;
- shutdown duration;
- snapshot and restore throughput;
- testkit startup and teardown time;
- binary/library size and dependency footprint;
- synchronization catch-up under bandwidth limits later.

### 16.3 Regression policy

Keep benchmark thresholds conservative enough to avoid noisy failures. Gate severe regressions in:

- bytes per sample;
- startup/recovery time;
- idle resource use;
- testkit startup;
- common query latency;
- memory accounting.

Performance improvements must not weaken durability, validation, or resource checks.

### 16.4 Acceptance criteria

- `docs/resource-envelope.md` reports measured ranges, not adjectives.
- Public “bounded” claims link to tests and measurements.
- Benchmark scripts are versioned and runnable.
- Raw results can be distinguished from interpretation.

---

## 17. Phase 9: Release engineering and distribution

**Priority:** P1

**Goal:** Make releases trustworthy and easy to consume.

### 17.1 Release artifacts

Prepare automation for tagged releases that can produce:

- crates.io packages for appropriate crates;
- Python wheels for supported platforms;
- standalone server binaries for supported targets;
- checksums;
- SBOMs where practical;
- release notes generated from a curated changelog;
- source archive references;
- signatures or attestations where the project’s infrastructure supports them.

The agent may configure and test workflows but must not fabricate credentials or claim a publish occurred when it did not.

### 17.2 Release checks

Before a release workflow can publish:

- all workspace tests pass;
- package versions agree where required;
- changelog contains the version;
- repository metadata is correct;
- docs build without warnings;
- package dry runs succeed;
- storage-format compatibility suite passes;
- crash/durability suite passes at its release cadence;
- compatibility matrix is updated;
- binaries start and report the correct version;
- Python wheels install and execute a smoke test;
- checksums/attestations are generated from final artifacts.

### 17.3 Semver and support policy

Document:

- public API semver policy;
- storage-format compatibility policy, which is separate from Rust API semver;
- MSRV policy;
- supported operating systems and architectures;
- support window for older storage formats;
- security reporting process;
- deprecation timeline for ambiguous write APIs;
- experimental feature policy.

### 17.4 Acceptance criteria

- A dry-run release can be produced from a clean checkout.
- Release artifacts have reproducible names and checksums.
- Package metadata points to the current repository.
- The release process cannot publish a version that failed compatibility gates.

---

## 18. Phase 10: v1.0 readiness

**Priority:** P3 and subject to human gates

**Goal:** Make `1.0` represent compatibility and trust, not another wave of features.

### 18.1 Technical gates

Before `1.0`, require:

- a stable primary Rust API;
- a stable Python API for the supported subset;
- a versioned on-disk format;
- tested upgrades from every promised supported release;
- documented durability modes verified by crash tests;
- finite and tested standard resource profiles;
- a production-quality testkit;
- a published PromQL/protocol compatibility matrix;
- fuzzing for parsers and storage formats;
- long-running ingest/query/retention/compaction tests;
- Windows and Linux coverage;
- signed or attested release artifacts if infrastructure allows;
- a backup, restore, inspection, and recovery story;
- no known silent-data-loss issue;
- no known data-corruption issue without a documented mitigation.

### 18.2 Human gates

The following cannot be completed honestly by an autonomous agent alone:

- several independent external applications use the embedded engine;
- at least one project uses `tsink-test` instead of a TSDB container;
- at least one real upgrade occurs across released versions without re-ingestion;
- at least one constrained/edge deployment completes long disconnect and catch-up cycles;
- maintainers review and approve the public API and support policy;
- maintainers decide whether experimental cluster functionality remains in the workspace, moves to a separate repository/crate, or receives renewed investment.

Record these as `HUMAN GATE`, not `DONE`.

### 18.3 Long-running validation

Add scheduled or manually triggered jobs for:

- hours-long mixed ingest and query load;
- repeated crash/restart cycles;
- retention across many windows;
- compaction with concurrent reads and deletes;
- snapshot/restore loops;
- format upgrade fixtures;
- memory and file-descriptor leak detection;
- testkit parallelism;
- synchronization disconnect/reconnect loops.

Preserve seeds and artifacts for any failure.

---

## 19. Cross-cutting engineering requirements

These requirements apply to every phase.

### 19.1 Error design

- Use stable, structured error variants for programmatic decisions.
- Preserve the underlying I/O source where useful.
- Include actionable context such as path, operation, configured limit, and observed usage.
- Do not include unbounded user data in error messages.
- Do not make callers parse strings to determine admission or compatibility failures.
- Map core errors consistently into Python exceptions and HTTP error payloads.

### 19.2 API design

- Prefer configuration structs and builders that validate at construction.
- Use newtypes for bytes, sample counts, timestamps, and durations where confusion is likely.
- Mark important return values `#[must_use]`.
- Keep the primary API small; put expert tuning behind explicit advanced configuration.
- Avoid exposing internal IDs or file-format structures as stable API.
- Avoid runtime-specific futures in the core.
- Document thread safety and blocking behavior.

### 19.3 Filesystem correctness

- Write new files to unique temporary paths.
- Flush and synchronize according to the durability contract.
- Publish through atomic rename where supported.
- Synchronize parent directories where required by the declared guarantee.
- Validate checksums before publication or use.
- Clean stale temporary files safely after restart.
- Never follow unsafe symlinks or delete unknown files during cleanup.
- Keep exclusive data-directory locking reliable across supported platforms.

### 19.4 Concurrency

- Define lock ordering and preserve it.
- Do not call external/user callbacks while holding engine locks.
- Use bounded channels.
- Test close, snapshot, retention, compaction, query, and writes in concurrent combinations.
- Ensure permits and reservations are released on cancellation and panic boundaries.
- Avoid detached threads.

### 19.5 Security and untrusted input

Even though the product is embedded-first, protocol endpoints may process hostile input:

- bound request sizes before decompression where possible and after decompression always;
- defend against compression bombs;
- limit protobuf repeated fields and string sizes;
- limit regex complexity/candidate expansion;
- reject invalid UTF-8 or labels consistently;
- avoid panics from malformed WAL, segments, snapshots, or manifests;
- keep secrets out of logs and support bundles;
- use safe defaults for listeners, preferably loopback unless explicitly configured;
- maintain dependency auditing in CI.

### 19.6 Observability

Expose instance-level metrics and structured snapshots for:

- accepted/rejected writes by reason;
- durability acknowledgements by level;
- memory and disk usage by category;
- cardinality and series creation rate;
- WAL size, sync, replay, and errors;
- flush/compaction/retention progress and failures;
- query counts, latency, limits, and cancellation;
- background health and last error;
- snapshot/restore/migration progress;
- testkit listener and maintenance state where useful;
- synchronization lag and failures later.

Metric collection itself must be bounded and must not create unbounded dynamic labels.

### 19.7 Documentation synchronization

Every semantic change must update all relevant places:

- rustdoc;
- README quick start;
- architecture docs;
- configuration reference;
- durability/resource/compatibility matrices;
- Python documentation;
- HTTP API docs;
- changelog and migration guide.

Prefer tests that verify documented examples compile and run.

---

## 20. Concrete first execution sequence

Unless the current repository reveals a critical corruption or security issue, execute work in this order:

1. **Baseline and progress file**
   - run existing checks;
   - inventory APIs and features;
   - record contradictions between docs and code.
2. **Metadata and positioning cleanup**
   - fix repository links and package descriptions;
   - rewrite README first screen;
   - label cluster experimental.
3. **Write-contract ADR and tests**
   - reproduce any silent or ambiguous partial-write behavior;
   - write failing tests before changing semantics;
   - implement structured batch outcomes and adapter mapping.
4. **Durability documentation and acknowledgement tests**
   - verify every WAL mode;
   - ensure returned acknowledgement is truthful.
5. **Resource profile scaffold**
   - introduce finite profiles without yet claiming complete process memory control;
   - implement disk quota and query budgets incrementally;
   - add tiny-limit tests.
6. **Format manifest and upgrade fixture**
   - add non-destructive detection first;
   - then migration/fixture tests.
7. **Crash harness and failpoints**
   - prove the durable contract.
8. **`tsink-test` core**
   - temporary DB, manual clock, deterministic maintenance, direct query assertions.
9. **Ephemeral Prometheus/OTLP endpoints**
   - refactor reusable protocol code out of the server binary as necessary.
10. **Differential compatibility suite**
    - start with common selectors, `rate`, aggregation, and HTTP envelopes.
11. **Release and documentation polish**
    - package dry runs, examples, changelog, artifact workflow.
12. **OEM lifecycle, export, and read-only mode.**
13. **Store-and-forward synchronization only after the preceding gates pass.**

Do not jump to item 13 because it appears novel. The project differentiates through trust and integration quality, not the number of subsystems.

---

## 21. Definition of done for each change

A change is complete only when:

1. Its behavior is described in code comments or an ADR where design intent is non-obvious.
2. Public APIs have rustdoc and examples where appropriate.
3. Success, boundary, and failure cases have tests.
4. Restart behavior is tested if persistent state changed.
5. Resource-limit behavior is tested if allocations or disk use changed.
6. Direct, async, Python, and protocol surfaces are updated when they expose the behavior.
7. Observability is updated where operators or host applications need to detect the behavior.
8. Relevant documentation and changelog entries are updated.
9. Formatting, linting, tests, and docs pass.
10. No README claim exceeds what the implementation and tests demonstrate.

---

## 22. Final completion report expected from the agent

When the agent reaches the end of its available execution window, it must leave the repository in a buildable state and produce a concise report in `docs/goal-progress.md` containing:

- revision/branch worked on;
- phases and checklist items completed;
- user-visible API or behavior changes;
- storage-format changes and migration impact;
- exact test, lint, compatibility, fuzz, and benchmark commands run;
- pass/fail results and links to generated artifacts;
- remaining known correctness risks;
- deferred work and why it was deferred;
- `HUMAN GATE` items;
- recommended next three tasks in dependency order.

Do not conceal incomplete work. A smaller, fully tested trust improvement is preferable to a broad unfinished implementation.

---

## 23. Product copy to converge toward

The README and package descriptions should converge toward wording like:

> **tsink is an embeddable, Prometheus-native metrics database for applications, agents, and tests. It provides durable local history and PromQL without requiring a separate database process.**
>
> - Embed directly in Rust or Python.
> - Set hard memory, disk, cardinality, and query limits.
> - Know whether each accepted write is volatile, WAL-appended, or durable.
> - Test remote write, OTLP, and PromQL without Docker or arbitrary sleeps.
> - Retain metrics locally and optionally synchronize them to a central backend.

Typical uses:

- add a local diagnostics page to a self-hosted application;
- retain metrics on a disconnected edge device;
- test Prometheus and OpenTelemetry integrations without a container;
- buffer metrics locally before remote-write synchronization;
- embed metric history in a commercial appliance.

Do not use this copy until each claim is true or clearly labeled as planned.

---

# Final strategic directive

Build tsink in this sequence:

```text
trustworthy embedded core
    -> deterministic metrics testkit
    -> measured Prometheus compatibility
    -> OEM/local application readiness
    -> offline edge store-and-forward
    -> 1.0 compatibility commitment
```

The moat is not another protocol, another dashboard, or another cluster feature. The moat is the complete embedded experience:

- no separate service;
- predictable resource use;
- explicit failure and durability semantics;
- Prometheus-native behavior;
- safe upgrades and recovery;
- deterministic testing;
- local-first synchronization;
- polished packaging and documentation.

When forced to choose, remove ambiguity before adding capability, add a test before adding a claim, and make the embedded user successful before expanding the server or cluster story.
