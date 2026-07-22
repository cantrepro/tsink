# Goal progress

This ledger tracks work against the execution charter in `GOAL.md`. Status labels have the
meanings defined there: `DONE`, `IN PROGRESS`, `BLOCKED`, `DEFERRED`, and `HUMAN GATE`.

## Baseline

- Date: 2026-07-22
- Branch: `master`
- Baseline revision: `277e7449a1df65e2291ecb49820918f7d36ba5d6`
- Workspace version: `0.10.2`
- Workspace members: `tsink`, `tsink-server`, and `tsink-uniffi`
- Declared Cargo features at baseline: none
- Baseline toolchain: Rust/Cargo 1.97.1; no MSRV was declared before this change
- Initial working tree: clean except for the user-supplied, untracked `GOAL.md`

### Baseline verification

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | `PASS` |
| `cargo check --workspace --all-targets` | `PASS` |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | `PASS` |
| `cargo doc --workspace --all-features --no-deps` | `PASS` |
| `cargo test --workspace --all-features` | Core and integration suites passed; 32 server tests could not bind loopback sockets inside the sandbox (`EPERM`) |
| `cargo test -p tsink-server --bin tsink-server` with host loopback permission | `PASS` — 476 passed, 1 ignored; confirms the 32 failures above were environmental |
| `cargo test -p tsink-uniffi` | `PASS` — 19 unit, 9 integration, and 1 configuration test |

The quick BPP and Criterion regression jobs in CI were inspected but were not part of the initial
baseline run.

## Verified baseline inventory and contradictions

The statements in this section describe the unmodified baseline. The roadmap ledger below records
the changes made since that snapshot.

- The core is an embeddable Rust crate and its asynchronous facade does not require Tokio. The
  optional server and UniFFI packages wrap the same [`Storage`](../src/storage.rs) API.
- [`WriteAcknowledgement`](../src/storage.rs) distinguishes volatile, WAL-appended, and durable
  completion, and the synchronous, async, and UniFFI surfaces preserve the batch acknowledgement.
- At baseline, the core ingest pipeline was all-or-error and had rollback tests. However,
  [`docs/architecture.md`](architecture.md) incorrectly describes per-row `WriteResult` values and
  silent rejection by `insert_rows`. The public result currently contains only a batch durability
  acknowledgement, not indexed row outcomes.
- At baseline, protocol handlers called the compatibility `insert_rows` surface, so successful HTTP responses did
  not expose the local durability acknowledgement. A later metadata or exemplar failure can also
  follow an already-committed row batch without disclosing that partial cross-component effect.
- Memory, cardinality, and WAL limits exist, but their builder defaults are effectively unlimited.
  There is no standard finite resource profile, complete disk quota, or shared query budget yet.
- Segment files have versioned manifests, but the data directory itself has no versioned format
  manifest or tested cross-release upgrade fixture.
- Cluster and edge-sync implementations already exist in the server, but they are not separated by
  Cargo features and have not passed the trust gates required for primary-product positioning.
- The pre-change root package included the untracked execution charter, CI configuration, and
  repository maintenance scripts; these are not runtime crate contents.

## Roadmap checklist

### Phase 0 — repository baseline, product focus, and hygiene: `DONE`

- `DONE` Establish this progress ledger and record the repository/test baseline.
- `DONE` Correct repository ownership, links, descriptions, and Python/package metadata.
- `DONE` Reposition the README around embedded local metrics and label unshipped resource-profile
  and testkit work as roadmap.
- `DONE` Label cluster mode experimental in README, configuration, CLI help, and cluster docs.
- `DONE` Add product-positioning, design-partner, durability, changelog, and
  [`ADR 0001`](adr/0001-write-contract.md) documentation.
- `DONE` Declare Rust 1.89 as the MSRV, add CI coverage, and pass the locked all-target workspace
  check on that toolchain.
- `DONE` Verify crate contents and the package dry run. The current package contains 249 files and excludes
  `GOAL.md`, `docs/goal-progress.md`, `.github`, and `scripts`.
- `DONE` Improve documentation of the primary embedded lifecycle and canonical write APIs.
- `DONE` Pass the complete post-change formatting, lint, test, documentation, no-default-feature,
  MSRV, and package verification matrix recorded below.

### Phase 1 — write safety and truthful admission: `DONE`

- `DONE` Define the canonical contract in [`ADR 0001`](adr/0001-write-contract.md) and synchronize
  the architecture, storage-engine, embedded, Python, HTTP, protocol, monitoring, and durability
  documentation.
- `DONE` Add [`Storage::write_batch`](../src/storage.rs), explicit `Atomic` and `BestEffort` modes,
  one indexed outcome per submitted row, bounded structured rejection details, and
  weakest-acknowledgement aggregation. Compatibility write methods remain all-or-error; third-party
  `Storage` implementations receive an explicit unsupported default rather than fabricated
  outcomes.
- `DONE` Preserve canonical results through the runtime-independent async facade and UniFFI/Python,
  including indexed rejection and cancellation tests.
- `DONE` Make atomic application stage all fallible active-state changes before publication and
  make active-head finalization exception-safe. WAL-enabled, WAL-disabled, reopen, new-series,
  later-shard codec-failure, and memory-accounting regressions are covered by
  `later_shard_codec_failure_does_not_partially_apply_batch`,
  `later_shard_codec_failure_without_wal_is_atomic`,
  `apply_failure_rolls_back_new_series_visibility_and_timestamp_bounds`,
  `flush_codec_failure_preserves_active_points_and_memory_accounting`, and
  `partial_flush_error_accounts_chunks_completed_before_failure`.
- `DONE` Cover admission and durability failure boundaries, including cardinality, memory, WAL
  quota, retention floor, opt-in future skew, timeout, WAL append/sync/post-append failure,
  lifecycle close, and fail-fast degraded fencing. Relevant tests include
  `opt_in_future_skew_limit_accepts_boundary_and_rejects_before_publication`,
  `write_limiter_respects_configured_timeout`,
  `wal_append_failure_does_not_ingest_points_or_survive_reopen`,
  `wal_sync_failure_does_not_ingest_points_or_survive_reopen`,
  `post_wal_append_failure_aborts_staged_frames_and_does_not_survive_reopen`, and
  `canonical_writes_report_degraded_without_committing_after_fail_fast_fence`.
- `DONE` Route Prometheus remote write, OTLP metrics, Prometheus text import, and Influx HTTP ingest
  through canonical atomic writes. Successful non-empty responses expose the established
  acknowledgement; rejected, partial, malformed-backend, and indeterminate outcomes use explicit
  statuses, bounded diagnostics, and fixed-cardinality metrics.
- `DONE` Make metadata and exemplar stores stage, persist, and then publish their own updates.
  Cross-component atomicity is not claimed: later sidecar failures disclose proven committed
  components and the weakest established acknowledgement.
- `DONE` Make lossy protocol behavior truthful: StatsD relative-gauge state advances only after
  proven row acceptance, oversized datagrams are rejected without truncation, and Graphite closes
  a connection after parse or storage failure. Bounded counters expose outcomes where the protocol
  has no reply channel.
- `DONE` Bound and redact public diagnostics so metric, label, attribute, tag, and field values are
  not reflected in write errors.
- `DONE` Harden the retained experimental cluster path: validate local and remote atomic results,
  count one acknowledgement per replica/shard only after every fragment succeeds, preserve known
  physical commits in metrics, and expose the weakest successful acknowledgement.
- `DONE` Persist exact cluster dedupe completions for retry replay, report append/flush/fsync
  failures as `dedupe_persistence_failed`, reject malformed or torn marker logs, and retain
  backward-readable legacy markers without inventing unavailable results.
- `DONE` Make hinted-handoff and edge replay remove entries after a valid complete atomic result;
  edge queue Ack records are successfully appended and flushed before pending in-memory state is
  removed. Retention expiry remains a separate, explicit edge-queue removal path.

Phase 1 does not claim a cross-component HTTP transaction, cross-node atomicity, or exactly-once
delivery. Those boundaries are explicit and observable. Comprehensive disk quota enforcement
remains Phase 2 work, as permitted by the Phase 1 charter's “once implemented” condition.

### Phase 2 — finite resource profiles and admission control: `IN PROGRESS`

- `DONE` Record the staged resource-contract decision in
  [`ADR 0002`](adr/0002-resource-profiles-and-budgets.md) and inventory the effective, estimated,
  excluded, and still-unbounded controls in [`resource-limits.md`](resource-limits.md). Finite named
  profiles remain deliberately unpublished until their values are measured and every advertised
  resource class is actually enforced.
- `DONE` Expose backend-qualified effective storage limits through the synchronous, async,
  UniFFI/Python, tenant, distributed, and HTTP status surfaces. Write timeouts retain nanosecond
  precision rather than fabricating millisecond equivalence.
- `DONE` Make current memory observability explicit about estimated accounted bytes, mmap virtual
  extent, unknown excluded bytes, and named excluded work classes. Publish deterministic
  `Normal`, `ApproachingLimit`, `Backpressured`, `Rejecting`, and instance-level `Degraded`
  pressure states plus memory-specific waiter, event, and rejection counters.
- `DONE` Close the concurrent WAL-quota preflight race by performing the definitive size check
  while holding the WAL writer lock. A deterministic competing-writer test proves that only one
  frame can consume the final available quota and that runtime accounting matches disk.
- `IN PROGRESS` The persistent core now has a shared local-disk coordinator with checked atomic
  reservations, configurable physical headroom and maintenance reserve, exact restart/cleanup
  reconciliation, category accounting, over-limit recovery admission, structured failures, and
  observability across sync, async, UniFFI/Python, and server status/metrics surfaces. Core WAL,
  segment/compaction, registry/catalog, tombstone, rollup, retention, and temporary publication
  paths are integrated. The built-in server now opens the coordinator before its persistent stores,
  shares it with metadata, exemplars, rules, usage accounting, and managed control-plane state,
  holds the canonical data-path lease in non-read-write modes, reconciles again before serving, and
  drains listener work before releasing storage or that lease. Managed sidecars clean owned orphan
  temporaries at startup, reject final symlinks, durably create nested directories, and roll back
  post-publication replacement failures. Core startup also rejects symlinked owned namespaces and
  removes only exact current-format atomic-write and segment-staging orphans before enforcing future
  growth; lookalikes, unknown operator files, and pending compaction markers are preserved. Snapshot
  sidecars use collision-safe temporary files and parent-directory synchronization. Rules persist a
  recording-evaluation attempt before external row/usage effects. Usage accounting runs ordinary
  durable appends on one bounded blocking lane and storage reconciliation on a distinct one-permit
  scan lane, exposes failed appends, frames multi-tenant reconciliation atomically, and treats an
  append-task join failure as indeterminate persistence. Tombstone publication returns a definitive
  pre-commit failure only after a clean rollback; an indeterminate manifest rollback retains the
  pending repair marker and any candidate shard that might still be referenced. Rollup policy/state
  publication uses a conservative invalidating-state order and retains that state when policy
  rollback cannot be proven. Their admin endpoints preserve typed quota rejection, including proven
  counts when an earlier selector or an earlier series within one tenant-scoped selector already
  committed. A failed initial
  rollup materialization remains a committed success whose snapshot exposes the degradation;
  repairable post-commit tombstone cache cleanup logs a warning and likewise does not fabricate a
  mutation rejection. Tiny-limit, rollback, restart,
  interruption-point, concurrent append, compute-only observability, typed direct/internal and
  clustered HTTP error, support-bundle status, live-root restore rejection, and data-before-control
  cluster restore tests cover that slice.
  Experimental cluster control/consensus/audit/dedupe/outbox writers, edge-sync queues, and
  external restore staging remain outside the complete boundary. Cleanup-before-growth-rejection,
  whole-operation compaction preflight, and a crash-durable coordinator for multi-file tombstone and
  rollup publication also remain incomplete, so the disk item is not `DONE`.
- `DEFERRED` Add shared query work/memory/concurrency budgets and cooperative cancellation across
  direct, async, PromQL, and HTTP entry points.
- `DEFERRED` Complete byte reservations for transient and non-write memory growth, cardinality
  reservations, bounded background queues/work, measurements, finite named profiles, and override
  precedence tests.

Phase 2 remains incomplete. Current memory totals are modeled estimates rather than RSS, several
important allocation classes are named but unaccounted, the local-disk boundary does not yet cover
cluster and edge persistence or external restore staging, static path validation does not protect
against a hostile concurrent namespace swap, and no shared query budget is enforced yet. Usage
metering is awaited and may add response latency, while status, report, support-bundle, and export
reads still scan an unbounded in-memory ledger.

### Phase 3 — durability, format upgrades, and crash recovery: `DEFERRED`

WAL and segment recovery tests exist, but the durability matrix, data-directory manifest, golden
upgrade fixtures, and process crash harness are not complete.

### Phase 4 — `tsink-test`: `DEFERRED`

No dedicated testkit crate, public manual clock, or deterministic maintenance fixture exists.

### Phase 5 — Prometheus compatibility program: `DEFERRED`

PromQL/protocol implementations and tests exist, but there is no differential harness or test-backed
compatibility matrix.

### Phase 6 — stable lifecycle and OEM integration: `DEFERRED`

Builder, close, snapshot, health, and observability primitives exist; the stable lifecycle contract,
read-only open, and portable export gates remain incomplete.

### Phase 7 — store-and-forward synchronization: `DEFERRED`

An existing server edge-sync subsystem is retained but receives no expansion before the trust,
resource, durability, and deterministic-test gates are complete.

### Phase 8 — resource envelope and reproducible benchmarks: `DEFERRED`

Benchmark and measurement scripts exist, but no complete measured resource-envelope document does.

### Phase 9 — release engineering and distribution: `DEFERRED`

A publish workflow exists, but tagged release gating, artifacts, checksums, SBOMs/attestations, and
compatibility gates remain incomplete.

### Phase 10 — v1.0 readiness: `DEFERRED`

The technical gates depend on Phases 1–9.

## Decisions

1. **Embedded first.** The core library and local data directory are the primary product. Server,
   cluster, and other broad operational surfaces remain discoverable as adapters or advanced work.
2. **No aspirational claims.** Finite standard profiles, deterministic testkit APIs, measured
   compatibility, and remote synchronization are described as roadmap items until their gates pass.
3. **MSRV 1.89.** The locked dependency graph requires at least Rust 1.88, but tsink's
   `std::fs::File` locking calls are still unstable on 1.88. A full all-target workspace check
   passes on 1.89, so CI pins 1.89 to prevent accidental drift.
4. **Characterize before redesign.** ADR 0001 and sync/async WAL-reopen tests pinned the
   all-or-error contract before canonical indexed outcomes changed the public surface.
5. **Preserve existing advanced code without centering it.** Experimental cluster and edge-sync code
   remains compiled and tested while trust work has priority.
6. **Publish profiles only after enforcement and measurement.** [`ADR 0002`](adr/0002-resource-profiles-and-budgets.md)
   makes limits backend-qualified and inspectable, uses shared reservations for concurrent work,
   and prohibits named finite profiles whose advertised dimensions are no-ops.

## Post-change verification

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | `PASS` |
| `git diff --check` | `PASS` |
| `cargo check --workspace --all-targets` | `PASS` |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | `PASS` |
| `cargo test -p tsink --all-features` | `PASS` — 521 core unit, 15 async, and 51 integration tests; remaining core suites and doc tests also passed |
| `cargo test -p tsink-server cluster::replication::tests` | `PASS` — 28 passed |
| `cargo test -p tsink-server cluster::dedupe::tests` | `PASS` — 9 passed |
| `cargo test -p tsink-server handlers::internal_api::tests` | `PASS` — 12 passed |
| `cargo test -p tsink-server edge_sync::tests` | `PASS` — 8 passed |
| `cargo test -p tsink-uniffi` | `PASS` — 19 unit, 11 integration, and 1 configuration test |
| `cargo test --workspace --all-features` | `PASS` — core counts above; server 539 passed and 1 ignored; UniFFI 19 unit, 11 integration, and 1 configuration test; remaining suites and doc tests passed |
| `cargo doc --workspace --all-features --no-deps` | `PASS` |
| `cargo test --workspace --no-default-features` | `PASS` |
| `cargo +1.89.0 check --workspace --all-targets --locked` | `PASS` |
| `cargo package -p tsink --list --allow-dirty` | `PASS` — required package contents present; repository-only artifacts excluded |
| `cargo package -p tsink --allow-dirty` | `PASS` — 246 files, 3.5 MiB unpacked, 633.1 KiB compressed; package verification succeeded |

Phase 2 targeted verification completed since that full matrix:

- `cargo check --workspace --all-features` — `PASS` after effective-limit, WAL-quota, and memory
  pressure changes.
- Core tests for effective limits, exact write-timeout reporting, all five memory pressure states,
  immediate memory rejection accounting, active admission backpressure, and concurrent WAL quota
  serialization — `PASS`.
- UniFFI effective-limit and memory-observability conversion tests — `PASS`.
- Server default/configured status and Prometheus memory-metric tests — `PASS`.
- `cargo check --workspace --all-targets` and
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` — `PASS` after the
  shared core/server local-disk slice.
- `cargo +1.89.0 check --workspace --all-targets --locked` — `PASS` for the declared MSRV after
  the shared core/server local-disk slice.
- `cargo test -p tsink --all-features` — `PASS`: 586 core unit, 17 async, 4 concurrency, 59
  integration, and all remaining core suites; 0 failures.
- `cargo test -p tsink-uniffi` — `PASS`: 19 unit, 13 integration, and 1 configuration test.
- `cargo test -p tsink-server status_and_metrics_report_core_local_disk_scope_when_persistent` —
  `PASS`; `cargo doc --workspace --all-features --no-deps` — `PASS` after the shared server slice.
- `disk_budget::tests` — `PASS` (24 tests), including concurrent final-byte admission, logical and
  physical headroom, Recovery admission, exclusive reconciliation, category/total overflow and
  underflow invariants, unknown files, and symlink-safe containment.
- `engine::fs_utils::tests` plus focused segment tests — `PASS`, including exact over-limit cleanup,
  atomic final-symlink replacement, rollback after injected parent-directory sync failure,
  injected partial filesystem-full cleanup, and quota-preflight staging cleanup.
- `engine::wal::tests` — `PASS` (42 tests), including quota serialization, logical-write rollback,
  over-limit reset reconciliation, and physical-headroom rejection before destructive reset.
- `engine::storage_engine::tests::persistence_recovery` — `PASS` (31 tests), including malformed,
  wrong-version, symlink, missing, and already-over-limit registry-catalog repair.
- `canonical_atomic_batch_reports_disk_quota_after_over_limit_reopen` and
  `persistent_reopen_reconciles_disk_categories_and_unknown_files` — `PASS`.
- Server metadata, exemplar, rules, usage-ledger, and managed-control-plane tiny-quota,
  persist-before-publish, exact-accounting, restart, concurrent-ordering, and torn-tail tests —
  `PASS`.
- Direct and internal sidecar disk-quota response tests — `PASS`, including metadata-only,
  exemplar-only, and rows-committed partial-progress cases; overlapping live-root admin restore is
  rejected without replacing the live tree.
- Clustered local sidecar quota tests — `PASS` for metadata and exemplars with no publication,
  preserved HTTP 413/error code, and existing indeterminate-cluster headers; RPC tests — `PASS`
  (22 tests), including bounded propagation of a peer's structured disk-quota code.
- Cluster recovery regression tests — `PASS`: node data restores finish before control consensus is
  committed, and a failed data restore leaves the existing control state unchanged. HTTP and
  Graphite shutdown tests also prove active request work is drained beyond the warning threshold.
- `cargo test -p tsink-server edge_sync::tests` — `PASS` (20 tests) after the mock HTTP fixtures
  were changed to consume complete bounded requests; the previously timing-sensitive source-runtime
  replay regression also passed 50 consecutive focused runs.
- `cargo test --workspace --no-default-features` — `PASS` after that edge-sync fixture hardening.
- `cargo test -p tsink-server --bin tsink-server --all-features` — `PASS` with loopback permission:
  599 passed, 1 ignored; the initial sandboxed run was unable to bind its TCP/UDP fixtures.
- `cargo package -p tsink --list --allow-dirty` and `cargo package -p tsink --allow-dirty` —
  `PASS`: 249 files, 3.9 MiB unpacked, 702.9 KiB compressed; `GOAL.md`, this progress ledger,
  `.github`, and `scripts` remain excluded.

The complete matrix will be rerun after the remaining Phase 2 implementation slices. These targeted
results do not make the phase complete.

The first sandboxed package dry run could not resolve the registry host. Re-running with host
network access succeeded; this was environmental rather than a package failure.

No differential compatibility, fuzz, soak, or benchmark suite was run in this phase; those remain
assigned to later roadmap phases.

## Storage-format and compatibility impact

- No core segment, WAL wire format, or data-directory format version changed.
- The experimental cluster dedupe log gained optional canonical completion data. Current code reads
  legacy markers, but a duplicate whose legacy marker lacks the original result returns
  `409 idempotency_result_unavailable` rather than fabricating success.
- The server usage ledger gained an additive batch-line format for atomic multi-tenant
  reconciliation. It still reads legacy single-record lines and accepts legacy sequence gaps, while
  unterminated lines, empty batches, and zero or duplicate record sequences fail closed.
- Canonical Rust and UniFFI/Python write APIs are additive. Existing compatibility write methods
  retain their signatures and all-or-error behavior.

## Current risks and blockers

- Atomic results identify every rejected row, but the compatibility ingest pipeline cannot always
  identify one causal input; `WriteRejection::cause_index` can therefore be `None`.
- Standard profiles still default several limits to effectively unlimited values. The effective
  limits are now inspectable, and shared core plus integrated server-side disk accounting/free-space
  headroom are enforceable, but cluster/edge writers, query budgets, atomic transient-memory
  reservations, and complete memory accounting are not implemented.
- Memory pressure is pressure on estimated accounted storage state, not process RSS. Excluded byte
  totals remain honestly unknown; query working sets, pending/staged writes, WAL buffers and replay,
  rollup work, remote refresh staging, thread stacks, allocator/runtime overhead, and adapter/server
  state are named but not yet charged.
- Atomic active-state staging and metadata/exemplar replacement can temporarily clone state; that
  transient memory amplification is not yet measured or governed by a complete resource profile.
- Exact disk cleanup that removes an owned entry performs a full-tree reconciliation and relies on
  an internal no-nested-reservation invariant; a no-op orphan pass skips that rescan. This is
  correctness-first but can make repeated effective cleanup expensive, so batching or safe deferred
  reconciliation is still needed before claiming high cleanup throughput.
- Usage metering does not change the response classification of already-completed primary work,
  but handlers await its bounded blocking append and can therefore add ledger-I/O latency. Usage
  status, reporting, support-bundle, and export reads still scan the unbounded in-memory ledger; an
  incremental aggregate plus bounded pagination/streaming remains necessary.
- The disk coordinator counts arbitrary external files at reconciliation and never deletes them,
  but it cannot atomically govern concurrent writes by another process. Cluster control,
  consensus, audit, dedupe, outbox, and edge-queue writers do not yet share its reservations, and
  their runtime growth can temporarily make accounting stale until reconciliation.
- There is no data-directory manifest, previous-release golden fixture, or process-kill crash
  harness. `Durable` currently describes the documented synchronization operations, not a
  cross-platform hardware guarantee.
- Rows, metadata, and exemplars remain separate transactions, so a later sidecar failure can report
  partial progress after rows commit. Sidecar replacement now synchronizes the file and parent
  directory, but response acknowledgements remain conservatively `Volatile` because the envelope
  and clustered sidecar protocols do not encode an atomic cross-component durability result.
- Experimental cluster writes are not cross-node transactions. Dedupe keys are not bound to a
  receiver-verified payload fingerprint, and bounded expiry/eviction means dedupe is not
  exactly-once delivery.
- Edge replay currently treats any valid upstream acknowledgement, including `Volatile`, as
  complete. There is no configurable source-side minimum acknowledgement.
- Edge queue records are flushed but not synchronized with `sync_data`/`sync_all`, so a reported
  queue acceptance is not a crash-durable upload guarantee.
- The hinted-handoff outbox synchronizes Put and Ack records, but compaction replacement does not
  yet sync the parent directory after rename.
- Cluster functionality is not yet isolated behind an explicit experimental Cargo feature or crate
  boundary.
- `HUMAN GATE`: external design partners, real cross-release upgrades, constrained edge validation,
  external testkit adoption, and maintainer approval of stable APIs remain unresolved.

## Recommended next three tasks

1. Extend the shared local-disk coordinator through experimental cluster control/consensus/audit,
   dedupe and hinted-handoff outbox writers plus edge queues; add restart/concurrency/failure tests,
   and batch exact cleanup reconciliation without weakening the no-undercount invariant.
2. Add a shared query-budget and cancellation abstraction for matched
   series, scanned/returned samples and bytes, intermediate memory, concurrency, regex expansion,
   steps, and wall time. Prove that cancellation and failure release permits and reservations, and
   map limits consistently through direct and HTTP APIs.
3. Measure constrained test, embedded, edge, and server workloads; then publish finite named
   profiles and an explicit expert-only unlimited configuration with deterministic base-plus-
   override precedence tests. Do not assign values before the covered dimensions are enforced.
