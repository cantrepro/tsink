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

### Continuation audit

- Date: 2026-07-26
- Audited revision: `4b54297e94ca1c86b4416c9bdc4f161e41e67629`
- Branch: `master`, four commits ahead of `origin/master`
- Toolchain: Rust/Cargo 1.97.1
- Initial working tree: clean

The continuation audit re-read the implementation, public documentation, adapters, package
manifests, and workflows rather than treating this ledger as authority. It found that the
cross-lane/tiered tombstone coordinator described by
[`ADR 0005`](adr/0005-cross-filesystem-tombstone-transactions.md) is implemented and tested, while
this ledger still called it incomplete. It also found stale finite-profile defaults, stale on-disk
layout names, invalid Python and cluster quick-start forms, incomplete downstream package metadata,
and a publication workflow that could publish on every push to `master`.

At the audited revision, formatting and the all-target workspace check passed. The all-features
clippy run found one pre-existing `field_reassign_with_default` test lint, and the all-features
workspace test reached 892 passing core unit tests before four core integration regressions failed:
one canonical memory-admission test received an outer error instead of indexed rejections, and
three tests still assumed legacy unlimited builder defaults. The continuation reconciles the
canonical case by separately admitting the bounded rejection-result envelope: indexed outcomes are
returned when that response fits, while an outer memory error remains when even the result cannot
be admitted. It also makes the three intentionally unbounded test configurations explicit. The
complete rerun is recorded below; the initial failures are not hidden as environmental.

### 2026-07-29 continuation audit

- Branch: `pivot`
- Continuation base revision: `ae480bc873dc4885cd9744164d644686e73276e9`
- Workspace version: `0.10.2`
- Working tree: retained the existing multi-phase goal changes; no unrelated edits were discarded

This continuation resumed the first unfinished Phase 2 accounting gate from that exact base. It
re-audited direct status, metrics, internal-read, support-bundle, activation, and background
recovery boundaries rather than treating earlier progress prose as proof. The evidence and
remaining gaps below describe the resulting working tree; they are not release or publication
claims.

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
- `DONE` Verify crate contents and package dry runs. The current root package contains 324 entries
  and excludes `GOAL.md`, `docs/goal-progress.md`, `.github`, `scripts`, and benchmark results.
  The exact five-crate workspace set shares one version; protocol and core archives receive Cargo's
  full package verification, and the exact server, testkit, and UniFFI archives compile offline
  against those packaged foundations.
- `DONE` Improve documentation of the primary embedded lifecycle and canonical write APIs.
- `DONE` Re-audit public examples, finite-profile defaults, on-disk layout, and tuning guidance
  against the implementation. Correct Python naming/restore usage, boolean CLI examples, real
  segment/tombstone/catalog paths, and the direct server invocation shown by `--help`.
- `DONE` Add pull-request, rustdoc, no-default-feature, and package-dry-run CI coverage and remove
  automatic publication from `master`. Published GitHub Releases and manual tag dispatches now
  require a real version tag at the exact checked-out revision, a promoted and empty-Unreleased
  changelog, the release workflow's Ubuntu verification matrix, MSRV validation, a server-version
  smoke test, exact five-crate version agreement, and package/archive verification in dependency
  order. It does not currently depend on the separate Windows CI or benchmark jobs. Actual registry
  publication remains fail closed until every mandatory GOAL release gate exists.
- `DONE` Complete crates.io/PyPI metadata for the server and Python binding packages while keeping
  repository-only measurement and migration helper scripts out of runtime package contents.
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
  edge queue Ack records are successfully appended and synchronized before pending in-memory state
  is removed. Retention expiry remains a separate, explicit edge-queue removal path.
- `DONE` Make `tsink-migrate backfill` retain and validate remote-write response evidence instead
  of accepting any 2xx. Every non-empty batch requires one canonical acknowledgement and no
  partial, indeterminate, or error evidence; metadata counts must be consistent, all submitted
  native histograms and exemplars must be accepted, and any dropped exemplar fails the migration.
  The default acknowledgement floor is `durable`; explicitly weaker floors and the weakest observed
  result are reported. The point limit remains hard when one source series must be split into
  label-preserving sample, histogram, and exemplar fragments.

Phase 1 does not claim a cross-component HTTP transaction, cross-node atomicity, or exactly-once
delivery. Those boundaries are explicit and observable. Comprehensive disk quota enforcement
remains Phase 2 work, as permitted by the Phase 1 charter's “once implemented” condition.

### Phase 2 — finite resource profiles and admission control: `IN PROGRESS`

- `DONE` Record the staged resource-contract decision in
  [`ADR 0002`](adr/0002-resource-profiles-and-budgets.md) and inventory the effective, estimated,
  excluded, and still-unbounded controls in [`resource-limits.md`](resource-limits.md). The named
  profiles are now available with explicitly provisional constants; the inventory continues to
  distinguish enforced dimensions from residual or excluded resource classes.
- `DONE` Expose backend-qualified effective storage limits through the synchronous, async,
  UniFFI/Python, tenant, distributed, and HTTP status surfaces. Write timeouts retain nanosecond
  precision rather than fabricating millisecond equivalence.
- `DONE` Make current memory observability explicit about estimated accounted bytes, mmap virtual
  extent, unknown excluded bytes, and named excluded work classes. Publish deterministic
  `Normal`, `ApproachingLimit`, `Backpressured`, `Rejecting`, and instance-level `Degraded`
  pressure states plus memory-specific waiter, event, and rejection counters. The live WAL writer
  buffer is charged at its actual retained capacity and exposed as a separate memory component;
  finite persistent configurations smaller than that indivisible allocation fail before opening
  the data path.
- `DONE` Admit atomic active-state clone/finalization/codec staging through the foreground
  `write_transient_bytes` lease. The conservative model charges each pre-existing state and the
  prepared retained-growth allowance before any clone, applies to startup WAL replay, releases on
  every outcome, and has an exact-fit/one-byte-short no-publication regression.
- `DONE` Move initial local-disk reconciliation, exact owned-orphan planning, tombstone
  recovery/cleanup preflight, and post-flush replacement-marker recovery out of the unbounded
  startup gap. These operations now admit conservative transient peaks against the configured
  memory ceiling before durable mutation. They are released before build returns and therefore do
  not inflate the retained `accounted_bytes` snapshot; structured startup errors report the
  modeled `required` peak.
- `DONE` Bound submitted series identities before registry allocation with finite defaults of 128
  labels and 64 KiB of cumulative metric-and-label UTF-8 bytes, while retaining the storage-format
  name/value maxima, duplicate-label validation, and canonical ordering. Add optional fixed-window
  new-series admission that counts concurrent reservations, commits only published identities,
  releases failures and registry-race excess, reports a structured creation-rate rejection, and is
  inspectable through sync, async, UniFFI, server status, and Prometheus observability. Exact N/N+1,
  rollback, clock-window, and 32-writer contention tests cover the core invariants.
- `DONE` Close the concurrent WAL-quota preflight race by performing the definitive size check
  while holding the WAL writer lock. A deterministic competing-writer test proves that only one
  frame can consume the final available quota and that runtime accounting matches disk.
- `DONE` The persistent core now has a shared local-disk coordinator with checked atomic
  reservations, configurable physical headroom and maintenance reserve, exact restart/cleanup
  reconciliation, category accounting, over-limit recovery admission, structured failures, and
  observability across sync, async, UniFFI/Python, and server status/metrics surfaces. Core WAL,
  segment/compaction, registry/catalog, tombstone, rollup, retention, and temporary publication
  paths are integrated. The built-in server now opens the coordinator before its persistent stores,
  shares it with metadata, exemplars, rules, usage accounting, and managed control-plane state,
  requires an explicit data path for cluster mode instead of using an unleased/unbudgeted temporary
  root, holds the canonical data-path lease in non-read-write modes, reconciles again before serving,
  and drains listener work before releasing storage or that lease. Managed sidecars clean owned orphan
  temporaries at startup, reject final symlinks, durably create nested directories, and roll back
  post-publication replacement failures. Core startup also rejects symlinked owned namespaces and
  removes only exact current-format atomic-write and segment-staging orphans before enforcing future
  growth; lookalikes, unknown operator files, and pending compaction markers are preserved. The
  configured memory ceiling is now derived before the initial local-disk reconciliation. That scan
  is depth-first and streaming with fixed category state, a 16,384-entry global namespace cap, and
  depth 128 rather than retaining directory-wide entry vectors or sibling path stacks. Startup
  registry recovery accepts legacy checkpoints/deltas plus checksummed `RJNL` generations. Small
  incremental deltas merge into `journal-active.bin` with a 1,024-series / 4 MiB stored-and-decoded
  cap; rename-first rollover seals the active generation before publishing its replacement, and
  exact oversized retries are idempotent. Startup admission covers the aggregate namespace and
  cleanup preserves unknown/lookalike files. Legacy deltas are readable but not incrementally
  migrated, and sealed journal generations still accumulate until an explicit checkpoint, so the
  namespace guard remains the final safety boundary. Startup blocker cleanup preflights every
  atomic temporary and recursive stage across configured roots
  before the first deletion, and generic owned-orphan cleanup preflights all categories and all
  tombstone lanes before mutation. Post-flush replacement recovery admits all marker paths, the
  bounded 4 MiB read/decode peak, at most 16,384 records per marker, and every collected
  rollback/commit state before its first rename or removal. Structured `MemoryBudgetExceeded`
  rejection therefore preserves all markers, stages, temporaries, and segment names for retry;
  exact-threshold, many-unknown-entry, cross-parent global-cap, and full `StorageBuilder` regressions
  cover these invariants. Snapshot
  sidecars use collision-safe temporary files and parent-directory synchronization. Rules persist a
  recording-evaluation attempt before external row/usage effects. Usage accounting runs ordinary
  durable appends on one bounded blocking lane and storage reconciliation on a distinct one-permit
  scan lane, exposes failed appends, frames multi-tenant reconciliation atomically, and treats an
  append-task join failure as indeterminate persistence. Tombstone publication returns a definitive
  pre-commit failure only after a clean rollback; an indeterminate manifest rollback retains the
  pending repair marker and any candidate shard that might still be referenced. Rollup policy/state
  publication now admits and stages the complete pair, including filesystem-entry allowances, then
  publishes conservative invalidating state before policies. Partial or ambiguous publication
  fences policy changes, checkpoint writes, delete invalidation, and materialization until reopen;
  a proven complete pair with cleanup debt remains committed. Their admin endpoints preserve typed
  quota rejection, including proven
  counts when an earlier selector or an earlier series within one tenant-scoped selector already
  committed. A failed initial
  rollup materialization remains a committed success whose snapshot exposes the degradation;
  repairable post-commit tombstone cache cleanup logs a warning and likewise does not fabricate a
  mutation rejection. Tiny-limit, rollback, restart,
  interruption-point, concurrent append, compute-only observability, typed direct/internal and
  clustered HTTP error, support-bundle status, live-root restore rejection, and data-before-control
  cluster restore tests cover that slice. The experimental hinted-handoff outbox now reserves Put
  growth in the shared `Cluster` category, reports structured quota/headroom failures through HTTP
  413, reconciles exact restart bytes, and uses Recovery admission for Ack appends plus durable,
  bounded cleanup attempts at a quota-full log. Post-record compaction failure remains retryable
  cleanup debt without changing a durable Ack or reschedule. Cluster dedupe markers now reserve
  normal growth in the same `Cluster` category, while standalone edge-accept dedupe markers use
  `EdgeSync`; quota/headroom failures map to HTTP 413, disclosing partial progress when the primary
  write already committed, and exact, bounded atomic compaction can recover non-growing state at the
  logical quota. The cluster audit log now reserves `Cluster` growth and keeps typed resource
  failures, while edge source Put growth reserves `EdgeSync` capacity and maps quota rejection to a
  partial HTTP 413. Edge Ack and batched expiry records can use Recovery admission, and audit/edge
  post-record compaction failure remains cleanup debt rather than reversing a durable result. Audit
  and edge-source status and metrics expose both that cleanup debt and persistence fencing after an
  indeterminate append, and either condition marks the corresponding subsystem degraded. The
  experimental cluster control-state/consensus-log pair now stages both complete replacements under
  one shared `Cluster` reservation and publishes the schema-v2 log, with its required authoritative
  `checkpointState` and restart-durable `steppedDownTerm`, before the state mirror. Before consensus
  requires a candidate, a pre-log resource failure remains typed and leaves live state unpublished.
  Once quorum or a leader commit makes the candidate required, pre-log failure retains it in memory
  as pending durability and fences mutation with `503 control_persistence_indeterminate`; a durable
  log followed by mirror failure installs the committed candidate and reports checkpoint pending.
  Authoritative Recovery may recreate or grow a missing/stale mirror at the logical quota because
  the log already holds authority, while still reserving the full temporary peak against physical
  headroom. If both pair members are durable but finalization, owned-temp cleanup, or accounting
  reconciliation fails, `cleanupDebt` reports a non-fencing committed cleanup-pending outcome and
  cleanup is retried before any separate fence repair. Only Active members vote or assert control
  leadership, and a no-longer-Active recorded leader is excluded from deterministic failover.
  An Active leader must transfer leadership before its own leave can be committed. If a command is
  already quorum-committed but a higher-term commit-notice response cannot yet be persisted, the
  successful degraded `committed_persistence_pending` outcome fences leadership and retains the
  required log-only candidate for repair rather than misreporting the command as rejected.
  Fenced control and cluster recovery-snapshot exports return
  `503 control_persistence_indeterminate`; cleanup-only debt remains exportable. Status, metrics,
  membership/handoff results, and restore responses expose the applicable degraded outcome.
  The public `StorageBuilder::restore_from_snapshot_with_disk_budget` API now places external
  staging and a strict-descendant target beneath a caller-owned offline envelope. Preflight rejects
  resolved overlap and static link-like entries, limits the trusted immutable source to 100,000
  entries and depth 128, and reserves logical bytes plus the greater of a 4 KiB floor and the
  destination allocation unit for every entry and missing target ancestor. Successful
  reconciliation installs exact accounting; reconciliation failure is explicit and retains the
  full conservative charge. The legacy restore remains caller-unbudgeted but shares the overlap,
  entry/depth, static-link, bounded-copy, durable-ancestry, and rollback-aware activation
  hardening. A typed capacity rejection during flush staging or foreground WAL Growth admission now
  gets one fully-expired-owned-segment-only cleanup pass and one retry; mixed-age rewrites, tier
  moves, and external-file deletion are excluded, and an unreclaimed capacity failure preserves the
  original typed error. Multi-output compaction now deterministically measures every output file,
  simultaneous Preparing/Ready marker bytes, allocation-unit and missing-ancestry allowances, and
  source-retirement entries, then acquires one shared Maintenance reservation before output-ID or
  filesystem mutation. Exact N succeeds; N+1 from either the configured limit or a concurrent
  reservation returns structured `InsufficientCompactionHeadroom` with every source and ID intact.
  Preparing recovery rolls outputs back, Ready recovery finishes validated retirement, ordinary
  faults reconcile exact per-file/category accounting, and unwind RAII leaves no active reservation
  while retaining a conservative peak charge for restart reconciliation. Explicit
  `ExpertUnlimited` removes the logical disk ceiling but retains actual-free-space/headroom
  preflight. The server now requires a distinct finite, cross-process-leased offline
  root; standalone and both internal restore routes use the budgeted core API, cluster peers must
  advertise `budgeted_restore_v1`, and local cluster targets plus the post-restore report share that
  coordinator without an unbudgeted fallback. Report/source/target overlap is rejected before
  restore mutation, while a bounded post-commit report failure is explicit degraded success.
  The cross-lane and tiered tombstone transaction described by
  [`ADR 0005`](adr/0005-cross-filesystem-tombstone-transactions.md) is also present: the data path
  has a crash-recoverable transaction record, one read-write owner holds the shared object-store
  writer lease, remote visibility is durably anchored before delete acknowledgement, and startup
  converges interrupted publication. The continuation audit passed all 31 focused deletion tests
  and all 5 shared-object-store writer-lease tests. Expensive full reconciliation after effective
  cleanup remains a throughput risk recorded below, but it is no longer a missing disk-consistency
  mechanism or a reason to leave this acceptance item open.
- `IN PROGRESS` Make core background ownership explicit and observable. A built instance now reports
  its fixed flush, compaction, persisted-refresh/retention/tiering, and rollup thread/concurrency
  bounds plus effective cadences. Every worker records starts, exits, coalesced notifications, idle
  parks, passes, and shutdown joins through sync, UniFFI, server status, and fixed-cardinality
  Prometheus surfaces. Intervals have a 1 ms idle floor, and shutdown attempts every join even when
  an earlier worker panicked. Deterministic supervisor tests cover non-spinning park behavior and
  clean/error shutdown. Active-head flush selection now resumes from a cursor, obeys the shared
  item/modeled-byte pass ceilings, and stops after one catalog cycle instead of wrapping to inspect
  the same series again. Healthy WAL-backed non-tiered timed passes require a current head to fill
  half its initial point block; no-WAL/tiered durability and memory/WAL pressure override that rule.
  Ordinary non-tiered catalog publication accounts only touched roots and posting keys rather than
  rebuilding the complete segment inventory. Background retention/tiering now visits one
  root-ordered persisted-inventory page per wake, charges every inspected descriptor plus exact
  manifest-declared source bytes for every selected action, and publishes only that page's root delta. Its cursor
  advances after durable replacement finalization, stays put across publication failure/recovery,
  and uses an empty terminal page for exact-multiple cycles so a partial scan cannot claim a full
  no-op. Foreground startup, close, and capacity-reclamation sweeps retain complete strict scans.
  The background rollup worker visits one policy and
  one seeked metric-postings page per wake, never peeks past the configured posting limit, continues
  later page members after a source-local failure while retaining the cursor on global persistence
  or write failure, and reports policy coverage only after every matched source has a checkpoint.
  It drains all writer permits across source read, internal
  materialization, and checkpoint publication so a concurrent historical write cannot be omitted;
  finite internal output batches are chunked by row and modeled-byte limits. Every raw or
  existing-materialized source read now admits an internal `QueryExecution`, inherits all finite
  instance query limits and deadlines, and is further tightened by the finite maintenance ceiling
  for modeled memory, scanned/returned samples, returned bytes, and intermediate length. Snapshot
  and append-sort preflights reject an oversized source without partial results or false checkpoint
  progress; that structured error remains source-local in a background page. The same execution
  now remains admitted across the raw and existing-materialized reads, reserves retained point
  vectors, downsample output/value payload, numeric scratch, and every cloned row identity before
  allocation, and releases all charges through RAII. A finite transform/row rejection occurs
  before pending state, output, or checkpoint publication, while an exact-boundary retry commits
  completely; `ExpertUnlimited` remains explicitly unbounded. Per-source checkpoint/pending state
  is now a checksummed create-only replacement journal with 1,024-event/4 MiB batch directories,
  equal same-generation packing shadows, bounded adjacent-generation compaction, a 1,024-generation
  and 16,384-observed-entry recovery ceiling, replay-inert detached cleanup, and restart-safe epoch
  rebasing into full policy/delete snapshots. Those full snapshots reject more
  than 65,536 logical items or 64 MiB of conservative modeled JSON before live-map cloning; the
  corresponding structured ceiling is still a Server-cardinality strand point rather than hidden
  unbounded work. Ordinary registry-catalog root changes now use a per-segment store with one
  crash-replayable bounded manifest intent: pages touch only their named remove/upsert entries,
  invalidate the aggregate series fingerprint before mutation, cap the live namespace at 16,382
  entries, the intent at 16 MiB, native manifest/entry decodes at 17 MiB/16 KiB, and retain
  64 MiB-bounded legacy-v2 startup migration. Finite non-tiered unknown-dirty refresh now charges
  fixed lane/level scan continuations and every raw namespace entry, retains its deduplicated
  snapshot within the maintenance byte ceiling and 16,384-entry namespace, then publishes stable
  add/prune root deltas with exact-page retry and visibility-generation invalidation. It never
  publishes a partial scan as complete; restart falls back to strict startup hydration.
  Finite compute-only segment refresh now requires the side-by-side v3 pointer and immutable framed
  generation described by
  [`ADR 0006`](adr/0006-framed-tiered-segment-catalog-generations.md). It validates the complete
  checksummed generation in item/byte-bounded pages before live mutation, applies bounded additions
  before removals, requires a distinct terminal pointer probe, and never falls back to a tier scan
  for missing or corrupt v3. Pointer changes restart the process-local cycle; without a shared
  stale-reader lease, continuous writer publication can delay convergence. The v2 JSON snapshot
  remains readable for old binaries, startup, and `ExpertUnlimited`. Finite read-write publication
  now scans persisted roots and streams local v2, immutable v3, and shared v2 fragments through a
  process-local item/byte-bounded continuation. It commits the authoritative pointer last, restarts
  on visibility-generation churn, accounts retained state as `remote_catalog_staging_bytes`, and
  keeps post-flush source roots behind a durable Committing marker until the pointer is published.
  Exact owned startup cleanup removes deterministic crash-orphan stages without globbing the
  shared namespace; `ExpertUnlimited` retains the complete one-shot path.
  Finite compute-only add/remove pages now reserve their complete one-root load and publication
  peak against both the maintenance byte ceiling and shared storage-memory budget before source
  validation, runtime-index loading, or visibility mutation. The model includes root/vector clones,
  registry-catalog and inventory deltas, scoped visibility/accounting scratch, and eventual live
  index/registry/postings growth, then reconciles actual collection/string/postings capacities
  before publication and restores the cursor-only lease on every outcome. Exact N/N-1 byte and
  memory tests, plus a missing-source preflight failure, prove no under-budget publication and zero
  residual reservation after terminal/error cleanup. `ExpertUnlimited` retains its complete legacy
  path.
  Finite compute-only tombstone refresh now has its separate process-local continuation: it probes
  only six exact shared manifests, charges each existing manifest and immutable referenced shard,
  retains admitted decoded fragments across wakes, and terminally revalidates every manifest.
  It then builds a fixed 256-shard immutable remote overlay, sharing unchanged shard `Arc`s and
  copy-on-writing only affected shards, before an atomic visibility-fenced pointer exchange.
  Queries union the local base with the requested remote shard. Manifest/pointer/visibility changes
  restart without exposing the candidate, and no tombstone root scan is used. Per-series visibility
  cache payloads are logically invalidated by a checked remote epoch rather than being globally
  cleared; lazy reads retag only the requested series. A semantic no-op preserves the overlay
  pointer, epoch, cache tags, and generations, while epoch exhaustion rejects before any
  publication state changes. Candidate construction, predecessor/candidate overlap, and terminal
  revalidation are admitted to the finite maintenance and modeled-memory envelopes. Finite
  read-write publication likewise preflights the complete catalog transition plus any committed
  tombstone-recovery dependency window before mutation. Finite explicit/manual
  rollup calls now advance the same shared cursor by
  one policy and one item/byte-bounded source-postings page, return structured continuation state,
  retain the cursor on global failure, and require an empty terminal page for an exact multiple.
  Rollup status uses traversal counters rather than re-enumerating every source. Explicit
  `ExpertUnlimited` alone drains the complete policy/source cycle in one manual call.
  Finite engine-background compaction now retains a clone-shared replacement-marker directory
  cursor and lets one raw namespace entry or one admitted marker own the wake before ordinary
  planning. Marker input has fixed 4 MiB and 16,384-record caps, conservative decoded
  String/Vec/path memory is admitted before decode, and the pathname, opened handle, and post-read
  entry must retain one file identity. Exact N/N+1 record and decoded-byte tests, cursor
  continuation/restart, same-size replacement-race, oversized-input, and no-mutation failure tests
  cover the boundary. Startup, close, and standalone compaction retain exhaustive recovery. The
  production background pre-compaction post-flush clean fence now separately retains a
  shared-memory-accounted `ReadDir` cursor, consumes one admitted raw namespace entry per wake, and
  requires a marker-generation-stable terminal probe before planning. Its portable model charges
  twice each simultaneously owned path/name payload plus a fixed 64 KiB directory-stream and
  entry-scratch allowance; the long-valid-path boundary verifies six-times data-path growth for the
  two marker-directory owners and recognized entry path. Marker publication invalidates the cursor;
  terminal, reset, and error release its reservation. Foreground, flush, and catalog callers remain
  exhaustive. Close now caps maintenance-gate,
  writer-drain, and compaction-drain waits with the
  configured lifecycle timeout, caps settling at 128 compaction passes, and reports close outcomes,
  waits, timeouts, passes, total duration, and join duration. Blocking filesystem calls cannot be
  portably preempted after entry without weakening the durability result, so that platform boundary
  remains explicit. This item is therefore not `DONE`.
- `IN PROGRESS` Enforce shared query work/memory/concurrency budgets and cooperative cancellation
  across direct, async, PromQL, and HTTP entry points. Core storage reads, metadata scans, and
  custom aggregation now admit one `QueryExecution`; nested work shares that execution, charges
  exact matched/scanned/returned work where available, reserves modeled engine-owned query memory,
  and releases permits and reservations on success, failure, cancellation, and deadline expiry.
  PromQL instant/range evaluation carries the same execution through selector prefetch, subqueries,
  steps, binary operations, and aggregations. Each async read carries a request-owned cancellation
  token into that shared execution, so dropping its future cooperatively stops running built-in
  selector/scan work and releases query resources. Built-in `list_metrics` now admits one execution
  for direct calls and reuses the async worker's execution; its fixed registry-page scratch,
  accumulated identity result, cold visibility-summary rebuild, dead-series IDs, and pruning
  companion vector are reserved before allocation and charged to the same
  series/result/intermediate limits. Cold repair re-admits actual range growth while holding the
  active/sealed read guards, so a concurrent post-estimate write cannot bypass the memory envelope.
  Ordinary and shard-scoped metadata selection now carries that execution through missing-summary
  repair, live/dead retention partitioning, and time-range summary repair. ID vectors are admitted
  before collection, long partitions checkpoint cooperatively, and exact/one-under memory and
  vector tests release every reservation.
  The default-tenant server wrapper shares that execution across scoped and legacy selections and
  reserves its combined in-place merge. Execution-aware point and metadata operations now return
  detailed results whose capacity-based modeled-memory guards remain live with the vectors;
  point batches also carry exact selector-existence bits. Guarded paged row scans likewise retain
  their result allocation and pre-admit the complete cloned identity-resolution vector before
  allocation. Built-in local storage,
  tenant/default-tenant wrappers, and distributed storage propagate or replace those guards around
  their final retained results. Third-party compatibility backends still default to
  `QueryExecutionAccounting::Unaccounted`, and bounded PromQL/internal/distributed paths that need
  complete accounting reject that contract instead of accepting an unguarded result.
  PromQL multi-series fetch, range-prefetch, and `info()` data paths now use both detailed metadata
  and detailed point results. They validate batch identities/existence evidence, pre-admit their
  label/point row transform, resize and transfer the point reservation to the actual
  capacity-based rows, and retain it through consumption or cache ownership. Bounded point
  backends fail closed before selection when they report `Unaccounted`. The exact-label selector
  fast path now uses that same guarded point contract instead of the compatibility `Vec` handoff,
  and retains its first result guard across later expression reads. `info()` pre-admits its keyed
  series map before cloning keys or labels, drops replaced-entry reservations, reserves
  map-to-vector conversion, and accounts both scratch and retained label-merge growth. Exact/one-
  under memory and false-`Complete` tests cover these paths.
  `max_returned_bytes` now uses a canonical logical model based on fixed slots and content lengths,
  including byte/string and native-histogram contents, while retained memory continues to use
  capacities and allocation allowances. PromQL `info()` metric discovery and series selection
  reuse the same execution; focused tests at a one-query concurrency ceiling prove there is no
  nested permit acquisition, request-tightened series accounting is retained, and all query
  resources return to zero. Prometheus remote-read now admits one execution per HTTP request and
  reuses it across every protobuf query, candidate discovery, guarded batch point read, protobuf
  transform, aggregate response encoding, and Snappy compression. Its allocation-free protobuf
  wire preflight admits the decoded body and decoded request heap before those allocations.
  Exact-two and one-over returned-sample tests prove cumulative accounting at a one-query
  concurrency ceiling, stable 413 error codes, and complete permit/memory release without loopback
  I/O. Exact/one-under aggregate byte and full-request memory tests, malformed/oversized input
  tests, and false/missing detailed-result guards cover non-truncating failure and release paths.
  The aggregate protobuf remains capped at 64 MiB and an over-limit result never returns a partial
  response. Raw and compressed buffers overlap under query-memory reservations through successful
  compression and usage recording; the compressed body becomes caller/transport-owned only at the
  explicit `HttpResponse` handoff, where that query reservation is released.
  Bounded distributed metadata and point reads execute peers sequentially, forwarding residual
  cumulative scan/pattern/step/deadline work, reserving planning and merge state, validating peer
  counters/existence evidence, and charging the deduplicated final logical union. Exactly exhausted
  scan work intentionally rejects before another peer, even if that peer might add no scan work.
  Final logical result limits are not divided into physical transport shares: each peer retains the
  original finite result limits, and every raw response is independently constrained by the fixed
  HTTP header/body cap. Accounted RPC calls preflight exact request JSON and headers, reserve raw
  response growth, and retain a conservative decode envelope until merge consumption.
  Compatibility/caller-owned vectors after their detailed guard is consumed, caller/backend
  internals, and external allocator/runtime/kernel/TLS buffers remain outside the portable
  shared-memory model. The embedded PromQL parser also rejects more than 64 KiB of input,
  16,384 non-EOF tokens, or 64 nested expression levels; unary, binary,
  parenthesis, and repeated subquery chains have exact-boundary tests and cannot recurse or
  construct an arbitrarily deep AST. A no-loopback acceptance matrix now selects the finite `Test`
  profile and tightens only `max_samples_returned` to two. Direct, async, PromQL range, and HTTP
  range entrypoints all accept exact N, reject N+1 structurally without truncation, and release
  every permit and shared-memory reservation; direct and PromQL expired deadlines also reject
  before admission, the async dropped-future characterization now uses the same finite `Test`
  query limits and releases its permit/queue bytes, and HTTP preserves the stable 413 error code.
  This closes baseline standard-profile propagation evidence, not large-shape or profile-constant
  calibration.
  A three-run Server pressure row filled all 32 query slots,
  rejected N+1 structurally, completed 32/32 range reads plus a concurrent
  100,000-point writer each run, and released active/shared memory accounting to zero; high-scale
  async, PromQL, HTTP, distributed, and broader query-shape calibration remain open, so this item
  is not `DONE`.
- `DONE` Bound the runtime-independent async facade's owned command inputs. The read and write
  channels retain their finite command counts and now have independent finite modeled-byte caps,
  atomic RAII admission across concurrent and channel-blocked producers, structured rejections,
  and current/peak/rejection observability. Exact N/N+1, concurrent admission, canceled-read drain,
  close drain, and accounting-underflow tests pin the boundary. The model explicitly excludes
  allocator slack, channel internals, results, and caller-owned custom-aggregation pointees.
- `DONE` Bound server usage-ledger state and its status, report, support-bundle, and raw-export
  readers. Exact all-time counters, category totals, latest storage snapshots, and at most 4,096
  tenant summaries are maintained incrementally; an N+1 tenant is rejected before publication.
  Raw and time-bucket reads retain at most 8,192 sequence-ordered records and use snapshot-pinned
  pages with independent record and encoded-byte ceilings. Stable tenant/sequence ordering,
  exact-N/N+1 continuation, concurrent append isolation, expired-cursor 410s, and oversized
  record/response 413s are covered without silent truncation. Status and the tenant-scoped support
  bundle use the incremental summaries rather than scanning raw history. Startup replay remains
  proportional to the durable ledger's finite standard-profile disk envelope, and explicit
  `ExpertUnlimited` can still remove that outer disk bound.
- `IN PROGRESS` Ship finite `Test`, `Embedded`, `Edge`, and `Server` profiles, fully specified Rust
  `Custom(ResourceLimits)`, and explicit `ExpertUnlimited` migration behavior. Core and async
  builders select `Embedded`; the server selects `Server`. Sparse low-level overrides win
  independently of call order and can be cleared, while a versioned resolved snapshot crosses
  sync, async, UniFFI/Python, observability, tenant/distributed, and server status surfaces.
  Standard/custom validation rejects incomplete or inconsistent finite fields, including a
  maintenance byte cap that could strand an admitted sealed chunk. Reproducible rows selected
  provisional 64 MiB `Test`, 256 MiB `Edge`, and 512 MiB `Embedded` memory/maintenance ceilings
  after each completed three runs without late rejection. Earlier direct Edge probes at 256, 384,
  and 512 MiB exposed a full-capacity head-growth projection plus 250 ms current-head
  fragmentation. Exact growth accounting and fill-aware timed flushing removed that cap-chasing:
  the stock Test row peaked at 5,756,707 modeled bytes, the stock Edge row at 90,011,984 bytes, and
  the 512 MiB Embedded row at 221,164,931 bytes. The Server base row passed 3/3 with a 441,129,372-
  byte p95 modeled peak, while its query-pressure row passed 3/3 with a 6,092,048-byte p95 peak
  shared query reservation. The 16-writer row admitted 400,000 new series in each of three runs.
  A 2026-07-26 modified-tree continuation repeated every named row: Test, Edge, Embedded, and Server
  base peaked at 5,963,340, 90,134,415, 221,100,680, and 441,075,551 modeled bytes respectively,
  while their process-wide RSS high-waters were 46,104,576, 429,703,168, 1,018,019,840, and
  1,995,440,128 bytes. The writer row admitted all 1.2 million submitted series but reached
  4,083,548,160 bytes of process RSS; the query row retained exact N/N+1 and release invariants with
  a 6,666,297-byte peak shared reservation. Every mixed row still missed the separate persisted
  B/point target. The clean/multi-host RSS matrix, modeled-to-process reconciliation, high-scale
  and larger-shape query breadth, and non-memory calibration remain open, so exact profile
  constants are provisional.
- `IN PROGRESS` Complete byte reservations for remaining transient and non-write memory growth,
  the residual background pass integrations, and final profile measurements. This is required
  Phase 2 work: only allocator/runtime/kernel/TLS behavior outside tsink's portable ownership model
  remains an accepted documented exclusion.

Phase 2 remains incomplete. Enforced memory totals are modeled estimates rather than RSS; the
measurement harness now reports an OS process-wide RSS high-water separately, and several
important allocation classes are named but unaccounted. Snapshot restore requires a trusted
immutable source because static path validation does not protect against a hostile concurrent
namespace swap. Shared query budgets are enforceable and observable; standard profiles now install
finite values, while explicit `ExpertUnlimited` preserves the legacy all-`None` query controls.
Detailed built-in point/metadata/row-page results and bounded tenant/distributed propagation retain
their modeled-memory guards, but compatibility caller-owned vectors, caller/backend internals, and
external allocator/runtime/kernel buffers remain outside that contract. The named constants and
broader adapter/query shapes are not yet calibrated. Usage metering is awaited and may add response
latency. Its in-memory state and administrative readers are finite. Complete storage reconciliation
uses guarded bounded pages, one whole-operation execution, two-pass fingerprints, a final manifest,
and finite row/sample/byte/memory/page/attempt/time ceilings on its separately serialized lane; it
is still an optimistic retry protocol rather than a durable linearizable storage generation.

### Phase 3 — durability, format upgrades, and crash recovery: `IN PROGRESS`

The durability matrix and WAL/segment recovery tests now have an initial public-API, cross-process
crash harness. For three deterministic seeds in each of `PerAppend` and one-hour `Periodic` mode,
the parent starts a child on a fresh temporary database, receives one uniquely numbered
acknowledgement only after the write returns and the pipe record is flushed, then kills the child
while its storage remains live and blocked before the next write. The parent reopens the database,
requires every `Durable` sample to have survived with its exact identity and value, and permits an
`Appended` or `Volatile` sample to be absent or present but never corrupt. Six child executions
finish in roughly two seconds on the current development host.

Two frozen compatibility fixtures now cover the supported pre-manifest storage-format-v2 layout.
The independently produced fixture was written by tsink package 0.10.1 from local release commit
`00cc627df7b36ae1838f68da273c42949f0a5d52` in an isolated `/tmp` `git archive` export, using
locked dependencies and a frozen external public-API driver that rejects other package versions.
That historical release predates `tsink-manifest.json`, so the directory was naturally
manifestless and no identity file was removed. The original format-focused fixture remains
unchanged; it was produced by the current v2 writer before its manual generator removed only the
manifest. The local checkout contains no tag ref for 0.10.1, so the independent provenance claims
the exact release commit and package version rather than a signed or annotated artifact.

Both fixtures' checked-in provenance and every data file's content-hash/size inventory are verified
before every test copy. The public legacy-open path installs the current manifest; each test then
queries numeric, blob, native-histogram, retained, tombstoned, and genuinely WAL-recovered records,
writes a new typed record, closes, reopens, and verifies the old and new state plus the current
manifest. Each upgraded fixture is then snapshotted and restored through the public APIs; the
restored manifest is checked byte-for-byte before strict reopen, followed by the same complete old,
tombstoned, histogram, WAL-recovered, and new-data assertions. Generators are explicit and manual,
refuse to overwrite existing fixtures, and are never invoked by tests. Retention-policy metadata is
not applicable because the builder policy is runtime configuration rather than persisted v2 state;
the named retained samples prove only that ordinary non-tombstoned data remains visible. The known
zero-byte `.tsink.lock` entries are intentionally frozen. Fresh generation is not promised to be
byte-identical because segment creation timestamps and concurrent series registration can vary;
inventories freeze reviewed bytes rather than authorizing automatic hash replacement.

This evidence establishes independent previous-release compatibility for the exact supported
format-2 path, but does not test migration from a storage format older than 2. The crash harness
covers abrupt user-process termination, not kernel/power loss. The deterministic in-process
failpoint matrix listed in the charter is now implemented and indexed in
[`durability-failpoints.md`](durability-failpoints.md): definition/sample WAL append, flush, sync,
chunk sealing, segment creation, index writing, file/directory sync, catalog/manifest replacement,
compaction publication, WAL reset, snapshot copy, and snapshot final publication all have named
failure/retry evidence. Those hooks prove software ordering and recovery at their injected
boundaries; they do not simulate torn sectors, kernel or power failure, lying devices, controller
caches, or every supported filesystem.

Snapshot/restore now enforce aggregate 100,000-entry/depth-128 staging limits, reuse exact measured
copy ceilings, create staging exclusively, publish through platform no-replace primitives, and
preserve raced names. Cleanup never expands from the precomputed operation namespace; completed
trees require captured descendant identities, unknown or replaced entries observed at a cleanup
boundary are retained, and partial copies without complete identity evidence are retained. Windows
holds the verified identity through disposition; portable Unix restore cleanup requires the public
offline target-containing-namespace contract through its final unlink window. A visible snapshot
whose final parent sync fails is reported with indeterminate durability rather than recursively
deleting possible consumer data. The final inspection/salvage acceptance audit is complete. Cross-platform
handle-relative traversal/copy qualification remains open, so Phase 3 and the full crash-recovery
gate remain in progress.

### Phase 4 — `tsink-test`: `IN PROGRESS`

The workspace now includes the foundational `tsink-test` crate. `TsinkTestDb` supports isolated
`TempDir`, pure in-memory, and caller-owned persistent-directory modes with
`ResourceProfile::Test` by default. Its deliberately narrow public surface preserves canonical
atomic `BatchWriteResult`, evaluates instant and range PromQL at caller-supplied timestamps,
restarts temporary or persistent storage on the same directory, exposes a process-safe diagnostic
identifier and bounded operation ring, and provides explicit idempotent close with best-effort
`Drop`. Its PromQL assertion layer calls those same direct instant/range methods and adds
error-returning normalized full-value comparison, approximate scalar checks with explicit
tolerance, empty/non-empty vector or matrix checks, and structural parser, unsupported-operation,
and query-limit expectations. Assertion failures hard-cap the query, expected/actual summaries,
total message, and a finite-profile nearby-series snapshot drawn from at most 4 of 16 bounded
accepted identities and 3 points per identity. No separate PromQL semantics layer or implicit
wall-clock time is introduced.

The focused public-API suite proves close/restart persistence, persistent reopen across fixture
values, parallel independent temporary roots and IDs, direct explicit-time PromQL, deterministic
vector/matrix normalization, exact scalar-tolerance boundaries, typed expected errors, bounded
nearby-series diagnostics, diagnostic eviction, and configuration rejection with
`cargo test -p tsink-test --all-targets` (18 passed). The fixture-data slice proves canonical
atomic ingestion for label/series/sample helpers, checked evenly spaced counter and gauge
sequences, PromQL-queryable classic histogram expansion, malformed histogram rejection, and
first-class native histogram round-trip. The package also passes 2 doctests, warning-free
package-local clippy, and formatting verification.

Generated Prometheus remote read/write and OTLP metric models now live in the small
`tsink-protocol` workspace crate rather than the server binary package. `tsink-server` consumes
that shared crate, while the default `tsink-test` dependency set remains engine-plus-`tempfile`
only. Enabling `tsink-test/prometheus` adds the shared protobuf model and Snappy encoder and exposes
a deterministic endpoint-ready remote-write payload builder plus a lower-level encoder for
intentional negative fixtures. Three feature tests decode the body through the real model and
prove canonical labels, exact samples, structured ambiguity rejection, and malformed-request
preservation. The extracted server schema suites remain green (4 Prometheus fixture executions and
6 OTLP normalization executions across the server and migration binaries), and
`tsink-protocol` passes warning-free clippy and package-content inspection. The complete
`tsink-test --all-features --all-targets` run passes 21 tests plus 2 doctests; the default
dependency tree remains exactly `tsink`, `tempfile`, and their transitive dependencies.

This is not the complete Phase 4 milestone. The crate starts no protocol listener and contains no
Prometheus remote-write/read or OTLP adapter, public manual clock, deterministic maintenance
driver, current-time assertion mode, remote-read and OTLP payload helpers, corruption/fault
fixtures, or Python/pytest fixture yet. It shells out to no binary, downloads nothing, and requires
no Docker for its current direct-core tests.

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

Published GitHub Releases and manual tag dispatches verify the real tag, version, revision, a
promoted changelog with no pending Unreleased entries, formatting, lint, all-feature and
no-default-feature tests, docs, MSRV, server version, and package assembly. Native wheels executable
on their builders are installed and exercised before upload, but the current matrix cannot execute
every cross-compiled target. PyPI publication is therefore intentionally disabled while wheel and
sdist build artifacts remain available. crates.io publication is also intentionally disabled
because the required storage-format compatibility suite, process-crash durability suite,
compatibility matrix, final artifact checksums, and attestations do not yet exist. Full cross-target
runtime coverage, standalone release artifacts, checksums, SBOMs/attestations, and broader
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

- `cargo test -p tsink --lib manual_rollup -- --test-threads=1` — `PASS` (3 tests), covering the
  finite N-1/exact-N page boundary and terminal continuation, same-cursor retry after a global
  persistence failure, and multi-policy `ExpertUnlimited` full-cycle drain.
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
- `cargo test -p tsink --test query_budget_surface_acceptance_test -- --nocapture --test-threads=1`
  — `PASS` (3 tests): finite `Test`-profile direct, async, and PromQL exact-N/N+1 returned-sample
  admission, structured rejection, direct/PromQL deadline rejection, and zero permit/memory
  residue.
- `cargo test -p tsink --test async_storage_test dropped_read_futures_cancel_running_work_and_release_queued_bytes -- --nocapture --test-threads=1`
  — `PASS` (1 test; 20 filtered): a dropped async read cancels work under the finite `Test` query
  budget and releases its query permit, shared-memory gauge, and queued input-byte reservation.
- `cargo test -p tsink-server range_http_test_profile_accepts_exact_n_rejects_n_plus_one_and_releases -- --nocapture --test-threads=1`
  — `PASS` (1 matching test; 861 filtered): no-loopback finite `Test`-profile HTTP range admission,
  stable 413/error-code mapping, and zero permit/memory residue.
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
- `cargo test -p tsink-server --all-features exemplar_store::tests -- --test-threads=1` —
  `PASS` (19 tests) after the exemplar lifecycle envelope: exact/N-1 shape, series, batch,
  retained, replacement, transient, serialization, durable, startup, and snapshot limits;
  streamed/normalized restart with stable retained accounting; actual disk-budget buffer
  reconciliation before publication; a valid 64 MiB maximum-file reopen under the 160 MiB default
  startup formula; atomic failure paths; and concurrent durable accounting with zero transient
  residue. The focused exemplar resource-metric rendering test also passes.
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
- `cargo test -p tsink disk_budget::tests` — `PASS` (29 tests), including Recovery append/rewrite
  accounting, exact-length bounded streaming, physical maintenance-reserve use, and rejection of a
  streamed replacement that exceeds its declared length.
- `cargo test -p tsink-server cluster::outbox::tests` — `PASS` (14 tests) with loopback permission;
  the initial sandboxed run could not bind the two replay fixtures. New coverage proves tiny-quota
  no-publication, exact restart reconciliation, concurrent final-byte admission, legacy temporary
  cleanup, legacy default-field growth admission, quota-full Ack recovery, and nonfatal cleanup debt
  after durable Ack and reschedule records.
- `cargo test -p tsink-server budgeted_` — `PASS` (8 tests), and
  `write_routing_error_response_preserves_outbox_disk_quota_category` — `PASS`.
- `cargo test -p tsink-server cluster::dedupe::tests` — `PASS` (16 tests), including typed logical
  quota and physical-headroom classification, exact same-process replay after failed marker growth,
  new-key fencing, exact `Cluster`/`EdgeSync` restart accounting, concurrent final-byte admission,
  quota-full Recovery compaction, append after nonempty atomic replacement, malformed/torn-log
  rejection, and exact owned-temporary cleanup with lookalike preservation.
- `cargo test -p tsink-server handlers::internal_api::tests` — `PASS` (14 tests), including a
  completed row write followed by dedupe quota rejection as partial
  `413 write_disk_quota_exceeded`, exact retry replay, and pre-write fencing of a new key.
- `cargo test -p tsink-server cluster::audit::tests` — `PASS` (16 tests), including typed
  quota/headroom rejection without publication, exact restart/category accounting, concurrent
  final-byte admission, quota-full Recovery compaction, cleanup debt, torn-tail rejection,
  monotonic IDs after expired-log cleanup, owned-temp cleanup, retention-only cleanup retry, and
  fencing after an indeterminate append.
- `cargo test -p tsink-server edge_sync::tests` — `PASS` with loopback permission (16 tests),
  including typed quota rejection without publication, exact restart/category accounting,
  Recovery Ack at the growth limit, all-or-nothing expiry append failure, post-Put cleanup debt,
  invalid/torn-log and exhausted-ID handling, no ID consumption on quota rejection, observable
  cleanup/fence health, and owned-temp cleanup.
- `budgeted_edge_source_preserves_typed_quota_through_partial_http_response` — `PASS`, proving a
  real tiny-budget source queue carries the typed resource failure into a non-retryable partial
  `413 write_disk_quota_exceeded` and releases its reservation.
- `indeterminate_edge_append_fences_queue_without_retry_advice` — `PASS`, proving a real injected
  append failure fences the source queue, reports degraded status, and returns partial HTTP 503
  without misleading `Retry-After` advice.
- `cargo test -p tsink-server cluster::consensus::tests -- --test-threads=1` — `PASS` (43 tests),
  including log-first v2 missing/invalid-mirror repair, v1 and corrupt-log fail-closed behavior,
  refusal to promote a non-bootstrap index-zero mirror, term validation, malformed follower input,
  restart/restore step-down persistence, Active-only quorum and inbound leader assertions,
  inactive-leader failover, leader self-leave rejection, pre- versus post-quorum quota
  classification, post-commit higher-term persistence fencing, grouped log-first checkpoint
  publication, cleanup-only snapshot export, and authoritative repair at the logical quota.
- Seven focused `cargo test -p tsink-server --bin tsink-server <test-name>` control-persistence
  cases — `PASS` (each 1 passed, 650 filtered), including
  `control_persistence_metrics_render_fixed_cardinality_health`,
  `control_persistence_resource_and_fence_contracts_are_nonretryable`, and
  `control_checkpoint_pending_status_and_admin_response_are_degraded_successes`, plus auto-join,
  membership, handoff, and TSDB status regressions. The contract case also proves a persistence
  fence takes precedence over overlapping resource classification and returns indeterminate 503.
- `cargo check -p tsink-server --all-targets --all-features` — `PASS` after the control-persistence
  HTTP/status/metrics contract changes.
- `cargo test -p tsink-server --bin tsink-server --all-features` — `PASS` with loopback permission:
  641 passed and 1 ignored after the completed audit/edge source slice.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`,
  `cargo +1.89.0 check --workspace --all-targets --locked`, and
  `cargo doc --workspace --all-features --no-deps` — `PASS` after the audit/edge source slice.
- `cargo test --workspace --all-features --quiet` — `PASS` with loopback permission after the
  completed audit/edge source slice: 591 core unit tests, 641 server tests with 1 ignored fixture,
  and all core integration, UniFFI, migration, and documentation suites passed.
- `cargo test --workspace --no-default-features --quiet -- --test-threads=1` — `PASS` with the same
  counts and remaining suites. The first parallel no-default-features run stalled in an unrelated
  core rollup test; that exact test passed immediately in isolation before the complete serialized
  matrix passed.
- `cargo test -p tsink-server --bin tsink-server --all-features` — `PASS` with loopback permission:
  616 passed and 1 ignored after the dedupe slice. The sandboxed attempt had 49 loopback-bind
  `EPERM` failures; the host-permitted rerun passed all of those fixtures.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — `PASS` after the dedupe
  slice.
- `cargo +1.89.0 check --workspace --all-targets --locked` and
  `cargo doc --workspace --all-features --no-deps` — `PASS` after the dedupe slice.
- `cargo test --workspace --all-features --quiet` and
  `cargo test --workspace --no-default-features --quiet` — `PASS` with loopback permission after
  the dedupe slice: 591 core unit tests, 616 server tests with 1 ignored fixture, and all core
  integration, UniFFI, migration, and documentation suites passed.
- `cargo test --workspace --all-features` — `PASS` with loopback permission after the
  hinted-handoff slice: 591 core unit tests, 608 server tests with 1 ignored fixture, and all core
  integration, UniFFI, migration, and documentation suites passed. The first final run hit an
  unrelated temporary data-path lock in one core retention test; that exact test passed in isolation
  and the complete rerun passed.
- `cargo test --workspace --no-default-features` — `PASS` after the hinted-handoff slice with the
  same core/server counts and all remaining suites green.
- Current finite-maintenance/query slice: `cargo test -p tsink --lib query_budget --
  --test-threads=1` — `PASS` (34 tests); `list_metrics_` — `PASS` (13 tests); compactor — `PASS`
  (38 tests); serialized persistence background — `PASS` (53 tests); unknown-dirty catalog —
  `PASS` (4 tests); async storage — `PASS` (21 tests); and default-tenant server regressions —
  `PASS` (8 tests). The persistence rerun first exposed one stale single-pass test assumption; the
  test now drives the intentionally bounded catalog continuation and the isolated plus complete
  reruns pass.
- The rollup filter passes all 50 tests both serialized and in the final default-parallel rerun.
  One earlier parallel run failed a durable-rollup assertion that passed immediately in isolation,
  in the complete serialized run, and in the final parallel rerun.
- `cargo check -p tsink-server -p tsink-uniffi`, core library clippy, core test-target clippy,
  server/UniFFI clippy, and benchmark-workload clippy all pass with warnings denied. The stronger
  test-target lint also moved three mid-file test modules to their file ends and removed three
  mechanical test-only warnings without runtime changes.
- 2026-07-26 continuation audit: `cargo check --workspace --all-targets` passed at the audited
  revision. The initial full clippy run exposed one server test-only
  `field_reassign_with_default` warning, and the initial all-features workspace test exposed four
  integration failures after 892 core unit tests passed; those findings are recorded in
  the continuation baseline above rather than relabeled as passes.
- `cargo test -p tsink --test integration_test --test promql_query_budget_test` — `PASS`: all 70
  core integration tests and all 11 PromQL budget tests passed after admitting bounded canonical
  rejection results separately, preserving an outer error when the result itself cannot fit,
  making the three legacy-unlimited test configurations explicit, and threading `info()` reads
  through the caller's execution.
- `cargo test -p tsink-server handlers::public_api::remote_read::tests -- --test-threads=1` —
  `PASS`: 7 deterministic handler/unit tests cover one shared execution across two returned
  series, cumulative `max_samples_returned`, exact and one-under encoded-byte boundaries,
  multi-query sequential slot release, stable 400/413/429/500/503 mappings, and zero active/shared
  resources after both success and failure.
- Canonical detailed-result and distributed query-accounting verification — `PASS`:
  `cargo test -p tsink --lib engine::storage_engine::tests::query_budget --all-features`
  passed all 19 tests; `cargo test -p tsink --test promql_query_budget_test --all-features`
  passed all 17 tests; and the all-feature `cluster::query::tests` and
  `cluster::query_merge::tests` server filters passed 26 and 4 tests respectively. Focused tests
  pin capacity-independent logical bytes for byte strings, UTF-8 strings, and native histograms;
  exact/one-under point, metadata-result, and planning-memory limits; detailed-result guard
  lifetime/release; PromQL multi-series guard adoption, fail-closed point accounting, and exact
  prefetch-memory boundaries; bounded remote deduplication; and the intentional
  zero-scan-residual rejection.
- `cargo test -p tsink-server --bin tsink-migrate` — `PASS`: 46 tests passed and one live
  interoperability test remained explicitly ignored. Focused response tests cover canonical
  acknowledgements including the default durable floor and explicit weaker opt-in, metadata-only
  idempotent replay, partial/indeterminate evidence, exact exemplar and native-histogram counts,
  rejection of any dropped exemplar, and hard batching for one oversized mixed-payload series.
- PromQL parser safety verification — `PASS`: 82 extended lexer/parser tests, 43 PromQL integration
  tests, and 12 library PromQL tests cover exact/one-over byte, token, and depth ceilings plus
  4,096-level adversarial parenthesis, unary, and right-associative shapes without stack overflow.
- Focused deletion and shared-object-store lease verification — `PASS`: 31 tombstone/deletion
  tests and 5 writer-lease tests, confirming the ADR 0005 coordinator recorded above is present.
- Public-API process crash durability verification — `PASS`: `cargo check -p tsink --test
  crash_durability_test`, `cargo clippy -p tsink --test crash_durability_test -- -D warnings`, and
  `cargo test -p tsink --test crash_durability_test -- --nocapture` passed. One parent test
  completed six abrupt child kills in 1.96 seconds: three deterministic crash points each for
  `PerAppend` and one-hour `Periodic`, with flushed acknowledgement handshakes, no child
  `close`/Drop path, exact durable-sample recovery, non-guaranteed appended/volatile handling, and
  recovered identity/value validation.
- Frozen pre-manifest storage-format-v2 compatibility verification — `PASS`: the current
  compatibility test contains two passing cases. One keeps the format-focused fixture generated by
  the current v2 writer. The second opens bytes independently written by tsink 0.10.1 from release
  commit `00cc627df7b36ae1838f68da273c42949f0a5d52`, built offline from an isolated `git archive`.
  The historical `cargo run --offline --locked` generation completed against package 0.10.1;
  targeted rustfmt and diff checks passed; `cargo check -p tsink --test
  storage_format_compatibility_test` and warning-denied clippy over that target passed; and `cargo
  test -p tsink --test storage_format_compatibility_test -- --nocapture` passed 2 tests.
  Each case verifies checked-in provenance and every regular data file's xxHash64/size inventory
  before copying it, exercises the strict legacy-open path and installed current manifest, checks
  numeric, blob, native-histogram, retained, tombstoned, and WAL-recovered data, then writes,
  closes, reopens, and verifies both old and new records. Each upgraded fixture then snapshots,
  restores through the public API, verifies byte-for-byte manifest preservation, strict-opens the
  restored directory, and re-verifies all applicable old and new state. Both drivers remain
  manual-only and refuse destructive replacement of an existing fixture.
- Finite remote segment-catalog v3 verification — `PASS`: `cargo fmt --all -- --check`,
  `cargo check -p tsink --all-targets`, and `cargo clippy -p tsink --lib --tests -- -D warnings`
  passed. All 58 persistence-background, 31 retention-policy, and 35 deletion-filter tests passed,
  followed by all 910 core library tests. Focused coverage includes exact frame/item/byte and
  namespace limits, v2 compatibility, ordered publication, symlink/corruption refusal, v2-only
  finite backoff without a scan, full validation before visibility mutation, exact-page terminal
  probes, pointer changes during validation/application, and repeated pointer churn delaying
  success until publication becomes quiet.
- Finite remote catalog apply-envelope verification — `PASS`: `cargo fmt --all -- --check`,
  `cargo check -p tsink --all-targets`, `cargo clippy -p tsink --lib --tests -- -D warnings`, all 8
  `finite_remote_catalog` tests, and all 71 persistence-background tests passed. The focused
  additions cover exact N/N-1 maintenance bytes for both add and removal pages (including an 8 KiB
  series identity), exact N/N-1 shared-memory admission for the complete add peak, no visibility
  mutation on rejection, source disappearance before load, capacity reconciliation before the
  visibility fence, and zero residual catalog staging after failure or terminal completion.
- Immutable remote tombstone overlay and bounded recovery verification — `PASS`: focused core
  checks cover the 256-shard copy-on-write candidate, shared unchanged shards, local-plus-remote
  query union, one-item manifest/shard/revalidation/publication wakes, no root scan, exact
  terminal admission, retained-charge release, and clean restart on manifest replacement. They
  also pin logical per-series epoch invalidation, lazy retagging without whole-cache enumeration,
  no-op pointer/epoch/generation preservation, fail-closed epoch exhaustion, and committed
  recovery/catalog-transition preflight before mutation.
- Incremental WAL and finite-flush accounting verification — `PASS`: successful ordinary finite
  persistence and finite close no longer invoke a whole-engine memory recount. WAL series-definition
  cache growth and reset observations are serialized by the cache mutex; a successful physical
  reset publishes the exact post-clear retained capacity, while a skipped or failed physical reset
  publishes no decrement. Full active-head flushing now accounts an empty-head removal and
  continues to later nonempty heads instead of leaving them stranded. Focused validation passed
  all 50 WAL tests, 5 write-buffer tests, 17 write-transient-memory tests, 7 shutdown tests, and
  the large-backlog, mixed numeric/blob restart, and reset-after-truncate failpoint cases. The
  touched Rust files pass standalone rustfmt checking and the core library passes `cargo check`.
  This closes the recount subtask, not Phase 2.
- Finite catalog, retention, post-flush, and snapshot acceptance stage — `PASS` on 2026-07-27:
  finite compute-only and read-write dirty reconciliation no longer enter a complete physical
  inventory path; bounded flush rollback retains one exact retry intent and reverses its exact
  tier-counter delta; active, sealed, and retention lookahead work shares one wake; and finite
  post-flush marker recovery is paged, namespace-bounded, aggregate-memory-admitted, and leaves
  only the true publication remainder. Snapshot/restore now re-attests requested namespaces under
  the shared operation cap and uses the documented platform-safe cleanup/rename classification.
  Verification passed `cargo fmt --all -- --check`, `git diff --check`,
  `cargo check --workspace --all-targets --locked`, and warning-denied all-feature workspace
  clippy. Serial focused results were 90/90 persistence-background, 38/38 post-flush, 19/19
  bounded-tombstone, 30/30 secure-snapshot, 10/10 finite-remote-catalog, and 5/5
  background-retention tests. A final baseline-excluded core sweep passed 1,175/1,175.
- Clean-revision acceptance debt — `RESOLVED` on 2026-07-28: all 11 failures reproduced from
  `ab10341` were reconciled while preserving finite rejection and data-safety behavior. The four
  memory-pressure cases now assert structured rejection, rollback, queryability, and an exact
  admitted retry; the three deletion/tombstone-transition cases use an explicit unbounded
  maintenance profile where they intentionally exercise complete-snapshot semantics; and the four
  runtime-refresh/flush/WAL cases use bounded convergence and distinguish logical open-handle
  salvage from persistent strict reopen validation. Focused serial results were 12/12 admission
  control, 36/36 capacity, 35/35 deletion, and 33/33 persistence-recovery tests.
- Workflow and package hygiene validation — `PASS`: both GitHub workflow files parse as YAML,
  their embedded release shell fragments pass syntax checks, manual branch dispatch is rejected,
  artifact checkouts resolve to the verified commit, and both crates.io and PyPI publication remain
  fail closed behind their documented missing gates. Locked Cargo metadata resolves, both
  downstream package lists contain their README, and the root package contains 264 files.
- Final 2026-07-26 workspace matrix — `PASS`: `cargo fmt --all -- --check`,
  `cargo check --workspace --all-targets --locked`, and all-feature workspace clippy with warnings
  denied passed. The loopback-permitted `cargo test --workspace --all-features --locked --quiet`
  passed 893 core unit tests, all core integration suites, 715 server tests with one ignored
  fixture-regeneration test, 46 migration tests with one ignored fixture-regeneration test, and all
  async, UniFFI, documentation, and binary suites. The serialized no-default-feature workspace
  check/test matrix passed the same counts.
- Final documentation/toolchain/package matrix — `PASS`: rustdoc built the all-feature workspace
  with warnings denied; Rust 1.89 checked every workspace target; `tsink-server --version` reported
  `0.10.2`; local Markdown targets and heading anchors passed across 36 files; workflow YAML and
  embedded shell parsed; and package dry runs produced 264-file core, 91-file server, and 18-file
  UniFFI archives. The core archive also compiled from its packaged source.
- The first final all-feature attempt found five missing UniFFI Python renames for newly exported
  resource-profile types; the focused contract and final matrix now pass. A later parallel attempt
  exposed a real rollup-response race after 892 core tests passed: explicit runs released their
  serialization lock before taking the returned status snapshot. Snapshotting now occurs inside
  that lock, the 30-test rollup filter passed 20 consecutive repetitions, and the final default-
  parallel workspace rerun passed.
- The exact named Test resource-profile row was repeated after that matrix: all 3 runs retained
  251,000 points with zero late rejections and zero suite failures. Peak post-write modeled memory
  was 5,963,340 bytes against the 67,108,864-byte profile ceiling, and the process-wide cumulative
  RSS high-water was 46,104,576 bytes. Effective persisted size remained 2.332267 B/point at p50
  and 2.339904 B/point at p95, so the separate 0.75/1.0 target still fails. The run is recorded
  with its base revision and pre-documentation working-diff digest in
  [`resource-profile-measurements.md`](resource-profile-measurements.md); it does not claim the
  still-open clean-revision gate.
- The exact named Edge row then passed 3/3 with 1,020,000 retained points per run, no late
  rejections, and no suite failures. Its largest post-write modeled sample was 90,134,415 bytes
  against the 268,435,456-byte ceiling, while cumulative process RSS reached 429,703,168 bytes.
  That separation confirms that the enforced modeled ceiling is not a total-process RSS claim.
  Persisted size remained 4.357009 B/point at p50 and 4.366931 at p95, so this row also leaves the
  separate B/point and final clean-revision gates open.
- The exact named Embedded row also passed 3/3 with 2,050,000 retained points per run, no late
  rejections, and no suite failures. Its largest post-write modeled sample was 221,100,680 bytes
  against the 536,870,912-byte ceiling, while cumulative process RSS reached 1,018,019,840 bytes.
  Persisted size was 5.023767 B/point at p50 and 5.025235 at p95. The result is recorded with exact
  working-diff provenance and leaves the same total-process, B/point, and clean-revision gates open.
- The Server base continuation row passed 3/3 with 4,100,000 retained points per run, no late
  rejections, and no suite failures. Its largest post-write modeled sample was 441,075,551 bytes
  against the 2,147,483,648-byte ceiling; cumulative process RSS reached 1,995,440,128 bytes.
  Persisted size was 4.889525 B/point at p50 and 4.893182 at p95. Exact provenance is recorded, and
  the writer/query-pressure reruns plus the broader clean qualification matrix remain open.
- The Server 16-writer continuation row admitted all 400,000 new series in each of 3 runs with no
  failures. Aggregate throughput was 114,842.262 series/s at p50 and 115,986.291 at p95, while
  cumulative process RSS reached 4,083,548,160 bytes—about 1.90 times the Server profile's 2 GiB
  modeled-memory ceiling. The specialized row exposes no modeled retained-state sample, so that
  gap remains a concrete total-process calibration blocker rather than a qualified capacity claim.
- The Server query-pressure continuation row passed 3/3: all 32 admitted queries per run returned
  all 262,144 points, each N+1 admission rejected structurally, every overlapping 100,000-point
  writer completed, and active/shared query reservations ended at zero. Query-p95 latency was
  6.944 ms at p50 and 8.415 ms at p95; peak shared reservation was 6,666,297 bytes and cumulative
  process RSS was 168,181,760 bytes. The small deterministic acceptance matrix now covers direct,
  async, PromQL, and HTTP propagation; high-scale adapter, distributed, and larger-shape
  calibration remain open.
- Independent final review found and closed six release-workflow gaps: checkout and tag
  verification now bind to the immutable event SHA; `[Unreleased]` must be the first changelog
  section; the ledger no longer implies that the Ubuntu release job depends on separate Windows
  and benchmark CI; Python wheels/sdist build unconditionally while publication is disabled;
  release/manual runs for one tag share a concurrency group; and missing artifact globs fail
  instead of warning. YAML/duplicate-key checks, all 18 shell blocks, all 3 embedded Python
  snippets, positive and adversarial tag/changelog simulations, job-graph inspection, and a
  publication-command/token scan pass on the final workflow. The same review found one extra owned
  bounded rejection message in the atomic error path.
  Cloning only N-1 row rejections and moving the original into the final outcome restores the
  modeled N-message peak; the focused 11-test transient-memory and 7-test canonical-atomic filters
  pass.
- A post-review loopback-enabled workspace run passed all 893 core unit tests but then exposed one
  intermittent `test_concurrent_different_metrics` failure when a background fail-fast fence caused
  writers to see `StorageShuttingDown`. The case passed immediately in isolation, the complete
  four-test concurrency binary passed 5/5 repetitions, and the isolated case passed 20/20 further
  repetitions. Its assertion now includes the storage health snapshot if the failure recurs. A
  subsequent complete default-parallel workspace run passed the full matrix, including all 893 core
  unit tests, the concurrency binary, 715 server tests with one ignored fixture test, and every
  remaining suite. The original intermittent signal remains recorded rather than being treated as
  a diagnosed fix.

The complete current workspace matrix was repeated for this slice. These results do not make
Phase 2 complete: final clean resource measurements, higher-scale query-shape calibration, and the
residual boundaries recorded above remain open.

The first sandboxed package dry run could not resolve the registry host. Re-running with host
network access succeeded; this was environmental rather than a package failure.

No differential compatibility, fuzz, soak, or benchmark suite was run in this phase; those remain
assigned to later roadmap phases.

## Storage-format and compatibility impact

- The core segment and data-directory format versions did not change. WAL publication markers now
  have an additive 40-byte `TSH2` form that records a checksummed reset-through floor `R` alongside
  the published high-water boundary `H`; current code still reads legacy 24-byte `TSHW` markers.
  Reset writes `TSH2`, so rollback to a binary that only accepts `TSHW` requires a compatible
  pre-upgrade data copy.
- The experimental cluster dedupe log gained optional canonical completion data. Current code reads
  legacy markers, but a duplicate whose legacy marker lacks the original result returns
  `409 idempotency_result_unavailable` rather than fabricating success. This disk-budget slice did
  not change that record format; compaction now uses bounded atomic replacement, and managed startup
  removes only generated replacement temporaries plus the exact legacy `.tmp` path.
- The hinted-handoff record format did not change. Its compaction replacement now synchronizes the
  parent directory, and startup removes only the exact legacy `.compact.tmp` path plus generated
  current-format atomic-write temporaries.
- The experimental control-log format advances from schema v1 to schema v2 and requires an embedded
  `checkpointState` whose applied index and term match the committed log position, plus a
  `steppedDownTerm` no greater than the current term. Current code reads v1 as migration input and
  republishes v2; v1-only binaries reject that file, so this is a downgrade boundary requiring a
  compatible pre-upgrade copy. The separate control-state mirror remains at schema v1. Startup uses
  a valid v2 log to attempt repair of a stale, missing, or invalid mirror and opens fenced with the
  checkpoint pending if repair fails; v1 migration still requires a valid mirror. Recovery
  snapshots default a missing step-down field to zero, merge it with the live revocation floor on
  normal restore, and clear it only for explicit `forceLocalLeader` recovery. Recovery-snapshot
  export fails closed while durable authority is fenced or its mirror checkpoint is pending, but
  remains available for cleanup-only debt.
- The server usage ledger gained an additive batch-line format for atomic multi-tenant
  reconciliation. It still reads legacy single-record lines and accepts legacy sequence gaps, while
  unterminated lines, empty batches, and zero or duplicate record sequences fail closed.
- Canonical Rust and UniFFI/Python write APIs are additive. Existing compatibility write methods
  retain their signatures and all-or-error behavior.

## Current risks and blockers

- Atomic results identify every rejected row, but the compatibility ingest pipeline cannot always
  identify one causal input; `WriteRejection::cause_index` can therefore be `None`.
- Standard profiles now populate finite storage, disk/WAL, cardinality, query, concurrency, async,
  and maintenance controls, and expose their resolved values and override provenance. Their
  constants are provisional rather than calibrated capacity promises. Shared core plus integrated
  server-side disk accounting/free-space headroom are enforceable, and core embedders and the
  server can give restore staging, targets, and reports a separate finite offline envelope. Atomic
  transient-memory reservations outside the covered query/storage paths and complete process-memory
  accounting are not implemented.
- Memory pressure is pressure on estimated accounted storage state, not process RSS. Query working
  sets now have a separate modeled budget covering tsink-owned decode buffers, snapshots, candidate
  sets, built-in aggregation state, PromQL intermediates, guarded detailed results, bounded
  distributed merge state, and accounted internal-RPC request/header/raw/decode buffers when
  configured. Excluded byte totals remain honestly unknown; caller-provided aggregator/backend
  internals, compatibility results after transfer to caller ownership, public-adapter buffers not
  explicitly reserved, caller-owned write inputs, rollup work, `ExpertUnlimited` and explicit
  compatibility remote-refresh complete-inventory input materialization, thread stacks,
  allocator/runtime/kernel/TLS overhead, and other server state are not charged to a complete
  process envelope. Atomic active-state staging is admitted through `write_transient_bytes`;
  metadata, exemplar, and rules replacement have separate finite store envelopes. Finite
  compute-only v3 generation reading, decoded page/path retention, one-root
  application/publication, retained cursors, and staged root maps are admitted separately and
  exposed as `remote_catalog_staging_bytes`.
- Exact disk cleanup that removes an owned entry performs a full-tree reconciliation and relies on
  an internal no-nested-reservation invariant; a proven absent no-op skips that rescan. Strict
  barriers whose requests predate the same terminal scan now coalesce only while the checked disk
  reservation generation remains unchanged. Post-flush staged cleanup batches all governed roots
  under one Recovery reservation and one scan while retaining best-effort cleanup after a malformed
  path. These changes remove avoidable and multiplied scans, but an effective batch still performs
  complete-root work, so broader batching or a separately specified audit/debt protocol remains
  necessary before claiming high cleanup throughput.
- Usage metering does not change the response classification of already-completed primary work,
  but handlers await its bounded blocking append and can therefore add ledger-I/O latency. Usage
  status, reporting, support-bundle, and export reads now use bounded incremental summaries or
  snapshot-pinned recent-record pages. Complete storage reconciliation and durable-ledger replay
  remain proportional to the database and on-disk ledger respectively; the former is serialized
  away from ordinary appends, while the latter is bounded by standard profiles' finite disk
  envelope but can be unbounded after an explicit `ExpertUnlimited` selection.
- The disk coordinator counts arbitrary external files at reconciliation and never deletes them,
  but it cannot atomically govern concurrent writes by another process. The cluster control pair,
  hinted-handoff outbox, cluster audit, cluster dedupe markers, edge source queue, and standalone
  edge-accept dedupe markers now share live-root reservations. External restore can use a separate
  caller-owned core coordinator; the server now opens and leases an independently finite instance
  for standalone/internal/cluster targets and report persistence.
- The public-API process-kill harness covers deterministic `PerAppend` and `Periodic` user-process
  termination and validates all recovered identities and values. It does not simulate kernel or
  power loss or qualify every supported platform/filesystem. A separate deterministic in-process
  matrix exercises the charter's complete named failpoint list, but those injected Rust failures
  are not hardware-crash evidence. Two content-hashed golden fixtures cover the supported
  pre-manifest storage-format-v2 layout, including legacy-open upgrade, mixed typed data,
  tombstones, and WAL recovery. One is format-focused and current-writer-produced; the other was
  independently written by tsink 0.10.1 from exact local release commit
  `00cc627df7b36ae1838f68da273c42949f0a5d52` before a root manifest existed. This does not cover
  migration from a storage format older than 2. `Durable` continues to describe the documented
  synchronization operations, not a cross-platform hardware guarantee.
- Rows, metadata, and exemplars remain separate transactions, so a later sidecar failure can report
  partial progress after rows commit. Sidecar replacement now synchronizes the file and parent
  directory, but response acknowledgements remain conservatively `Volatile` because the envelope
  and clustered sidecar protocols do not encode an atomic cross-component durability result.
- Experimental cluster writes are not cross-node transactions. Dedupe keys are not bound to a
  receiver-verified payload fingerprint, and bounded expiry/eviction means dedupe is not
  exactly-once delivery.
- Edge replay currently treats any valid upstream acknowledgement, including `Volatile`, as
  complete. There is no configurable source-side minimum acknowledgement.
- Edge queue records are synchronized before publication, so reported queue acceptance is
  crash-durable local state. It is not a durable-upload guarantee because configured pre-ack expiry
  may remove pending rows and the source accepts `Volatile` upstream acknowledgement.
- Cluster functionality is not yet isolated behind an explicit experimental Cargo feature or crate
  boundary.
- Strict Active-only inbound leader eligibility relies on activation and leadership transfer not
  outrunning control-log catch-up. A membership certificate or joint-configuration proof that a
  newly activated leader can present to a lagging voter is not implemented, so that convergence
  guarantee remains incomplete Phase 2 work.
- `HUMAN GATE`: external design partners, real cross-release upgrades, constrained edge validation,
  external testkit adoption, maintainer approval of stable APIs, and selection of a monitored
  security-disclosure contact/channel remain unresolved. A security address is not fabricated in
  repository metadata.

## Current continuation and next work

Phase 2 remains `IN PROGRESS`. The 2026-07-28 continuation closed the inherited 11-test acceptance
debt and additional WAL and query-accounting boundaries:

- Strict WAL opening now validates a complete markerless stream before publishing a boundary,
  rejects duplicate logical segment aliases and recognized symlink/non-regular entries without
  mutation, and proves contiguous coverage only across the authoritative persisted replay floor
  through the published boundary. Legal checkpointed gaps below that floor and unpublished gaps
  above the boundary remain supported. A reset now publishes `TSH2` with
  `H = R = max(last appended high-water mark, (active segment, 0))` before removing WAL bytes;
  future commits preserve `R` while advancing `H`, and replay starts at the greater of the clean
  persisted floor and `R`.
- Core, tenant, UniFFI, and distributed metadata-list paths now share one admitted execution
  through shard selection, WAL-definition merging, fanout, deduplication, tenant-label stripping,
  and retained result ownership. Exact-boundary, one-under, empty-scope/error-precedence,
  held-concurrency, no-partial-result, and release assertions pass in 35/35 core query-budget,
  24/24 tenant, 13/13 distributed-storage, and 15/15 UniFFI integration tests.
- Built-in core metric-name row scans now use the same detailed `QueryRowsExecutionResult` contract
  as explicit-series scans. Metric-postings count and identity materialization are admitted while
  the postings read guard prevents concurrent growth. Both async row-scan commands carry the
  detailed guard through the worker reply and reject an unaccounted finite backend before
  invocation. The later continuation below closes the non-default tenant metric-name adapter;
  default-tenant and distributed metric-name adapters remain explicitly `Unaccounted`.
- Async `list_metrics` and `select_series` now carry their operation-specific detailed metadata
  guards through the worker reply instead of consuming them in the worker. Finite calls require
  `Complete` accounting and reject missing or undersized false-`Complete` guards; unlimited calls
  preserve the exact compatibility operation. Core, tenant, and distributed list adapters expose
  guarded detailed results. A non-default tenant series-row scan replaces the scoped inner guard
  around its visible page, while default-tenant and distributed series-row scans remain explicitly
  `Unaccounted` because their compound paths can fetch and charge more points than the page reports.
- Raw point pages retain their working reservation through row materialization and coalesce it with
  the final row guard without a cancellation or error-path accounting gap. Snapshot vectors are
  preflighted before allocation; persisted and sealed cursor scratch is charged simultaneously;
  compressed chunks include declared output, a decoded-window allowance, and a 1 MiB zstd
  workspace; decoder timestamp/value/point vectors and nested value-capacity growth are included.
  Pagination now reports continuation only after observing a real later row. Focused verification
  currently passes 56/56 core query-budget tests, 9/9 query-read tests, 43/43 async tests, the
  metric-postings concurrency regression, 3/3 server series-row accounting tests, and 2/2
  tenant/distributed detailed-list guard tests.
- Public Prometheus metadata and finite-limit internal metadata already retain their source,
  projection, body, and header guards through `HttpResponse` construction. The audited remaining
  HTTP boundaries at that checkpoint were legacy internal metadata without `query_limits`,
  best-effort `/metrics`, rebalance reporting, and the support bundle; these were explicitly
  documented rather than being grouped into an ambiguous “metadata HTTP” gap. The adapter-owned
  support-bundle slice is closed below, while its operational child producers remain separate
  boundaries.
- `/api/v1/status/tsdb` now requires one admitted execution and complete detailed metric-list
  accounting, propagates enumeration failures through stable error envelopes, reserves the full
  hotspot tracker clone/map/union/top-N transformation under that execution, checkpoints and
  observes intermediate hotspot work, and guards the measured retained JSON tree plus exact
  encoded body/header allocations through `HttpResponse` construction. The independent 1 MiB
  encoded ceiling no longer tightens `ExpertUnlimited` enumeration. This is partial hardening:
  named operational snapshot clones and the JSON projection/tree-construction peak remain outside
  reserve-before-allocation query accounting and are documented as an explicit adapter boundary.
  Focused exact-minus-one/exact returned-byte and memory tests accompany execution/guard-release,
  large-identity unlimited enumeration, hotspot cancellation/intermediate accounting,
  unaccounted-backend, false-`Complete` missing/undersized guard, and storage-error regressions.
- Empty-policy shared background rollup passes now complete without draining writer permits, so
  sustained ordinary writes cannot turn idle rollup maintenance into a write-timeout fail-fast.
  The fast path remains serialized with policy publication, preserves worker/cursor observability,
  honors the indeterminate-publication fence, and keeps a pending cursor retry unchanged on error.
- The current tree passes `RUSTFLAGS="-D warnings" cargo test --workspace --all-features --locked`:
  1,235/1,235 core library tests, 42/42 async integration tests, the 4/4 concurrent-write
  integration binary (including the previously load-sensitive different-metrics case), 887/887
  server tests with one intentional fixture-regeneration ignore, and every remaining workspace
  integration and doc test passed. Warning-denied default and no-default all-target checks and the
  Rust 1.89 MSRV all-target check also pass. Focused idle-rollup tests pass 3/3, and the complete
  server binary had already passed independently. The full no-default test run, final package
  verification, CI benchmark smokes, and complete constrained profile matrix remain before any
  constant is qualified.

The 2026-07-29 continuation re-audited those remaining gates and closed another bounded HTTP slice:

- The package jobs had fallen behind the five-crate workspace. CI and release verification now
  require exactly `tsink`, `tsink-protocol`, `tsink-server`, `tsink-test`, and `tsink-uniffi` at one
  version. Protocol and core receive Cargo's full offline package verification; each exact
  downstream archive is then extracted and checked offline against those packaged foundations.
  The reproduced archives contain 324 core, 14 protocol, 84 server, 12 testkit, and 18 UniFFI
  files. A bare downstream package command correctly cannot resolve the unpublished protocol
  crate; the verified workflow supplies the same-version source patch during assembly and replaces
  it with the extracted protocol archive for the final check.
- The best-effort `/metrics` metric-list and hotspot path now requires complete detailed accounting,
  admits one execution, validates and retains the listing result guard through the accounted
  hotspot transform, and holds the hotspot reservation and execution through exposition
  construction. Admission, storage, missing/undersized false-`Complete`, and hotspot-budget
  failures preserve the existing `200` collector-error contract and never call the uncontrolled
  listing or transform. Default and no-default focused suites pass 11/11 each. Other operational
  snapshot clones, the cloned hotspot control-state input, and the uncapped growable exposition
  body remain explicit boundaries.
- `GET /api/v1/admin/support_bundle` now bounds its adapter-owned response accumulation,
  composition, and encoding. A fixed 16 MiB aggregate cap bounds retained child bodies and headers;
  the parent reserves root strings, 256 KiB of bounded serialization scratch, and response-header
  preflight before combined output allocation. Valid child JSON is serialized from borrowed raw
  bodies rather than duplicated into a `serde_json::Value` tree; non-JSON fallback text is limited
  to 8,192 decoded characters. Two-pass pretty encoding enforces a separate fixed 16 MiB response
  ceiling, charges exact returned bytes, pre-admits exact body capacity, and keeps the modeled
  body/header reservation live through `HttpResponse` construction. The initial focused default
  and no-default suites passed 9/9 each, covering raw JSON and Unicode/invalid-UTF-8 fallback,
  exact/one-under returned-byte and memory admission, both fixed ceilings, cancellation and
  structured pressure/error mapping, Test/Edge/custom one-query profiles, compatibility headers
  and schema, and zero residual query resources. At that checkpoint, completed child responses
  still crossed an unreserved handoff; the later continuation below closes it. Tenant/actor and
  synthetic-request/header preparation before the parent reservation remained outside that claim.
- The quick B/point job is now an execution/health smoke instead of enforcing the unqualified
  0.75/1.0 target. Its one modified-tree run retained 4,096,000 points with zero workload failures
  and measured 2.047495 effective B/point, so the target report was false while the health smoke
  correctly remained green. This is not a qualified capacity or compression row.
- The Criterion smoke exposed and closed four harness defects rather than weakening finite
  production limits: empty Bash arrays now work on Bash 3.2; the million-point fixture seeds in
  10,000-row batches; that explicit scaling case uses a finite Embedded-derived query envelope with
  a one-million-element intermediate limit and 64 MiB per-query memory under the unchanged 128 MiB
  shared cap; and an explicit suite selector prevents filtered-out comprehensive fixtures from
  running eagerly. Quick mode now executes eight intended cases, including the million-point select
  and 64-segment refresh, while full mode retains the 64/256/1,024 refresh matrix. The final quick
  run passed; the million-point select measured 34.542–35.022 ms and the 64-segment refresh measured
  3.0542 s. The cached comparison is restricted to those eight current IDs and passed with a worst
  observed change of +8.3% against its restored, unpinned cache. These modified-tree, cache-relative
  observations are smoke evidence, not a stable regression baseline.
- A proposed compound tenant/default-tenant row-accounting shortcut was reviewed and removed before
  publication. It materialized and charged offset-skipped/off-page points as returned work, could
  double-charge metadata plus point composition, and moved budget admission ahead of compatibility
  validation. The rejected shortcut remains absent. The matcher-aware primitive described below
  later closes the non-default tenant metric-name case; default-tenant and distributed row adapters
  remain honestly `Unaccounted` until a scoped-versus-legacy or per-peer paged contract can preserve
  final-logical-result semantics.
- Before the support-bundle slice, the settled 12-file working diff passed formatting and diff
  checks, warning-denied all-feature workspace Clippy, warning-denied no-default all-target
  checking, the Rust 1.89 all-target check, warning-denied all-feature rustdoc, and the final exact
  archive flow described above. The complete warning-denied no-default workspace test command also
  passes: 1,235 core tests in 443.65 s,
  42 async tests, 4 concurrency tests, 2 crash-durability tests, 70 core integration tests,
  893 server tests with one intentional fixture-regeneration ignore, and all remaining testkit,
  UniFFI, integration, and doc tests completed with zero failures.
- After the support-bundle slice, the current 15-file working diff passes formatting and diff
  checks, warning-denied all-feature server Clippy, and warning-denied no-default server all-target
  checking. The full server package suite with localhost socket access passes all 901 active server
  tests with one intentional fixture-regeneration ignore, plus all 46 active migration-binary tests
  with its matching fixture-regeneration ignore.
- A later 2026-07-29 continuation closes the remaining `/metrics` scrape-owned projection and
  exposition boundary. One admitted `QueryExecution` now covers complete metric enumeration,
  accounted hotspot transformation, built-in storage observability, local/offline disk and rollup
  labels, rules, and the cluster control, write, fanout, outbox, digest, and rebalance projections.
  Allocation-bearing projections reserve before cloning or materialization; scalar and
  fixed-cardinality projections are copied or borrowed. A cancellation-aware counting pass
  enforces a fixed 1 MiB normal exposition ceiling and exact returned-byte admission, followed by
  an exact-capacity controlled pass whose body/header reservation remains live through
  `HttpResponse` construction. Collection or body failures preserve the HTTP 200 contract with a
  Prometheus-parseable fallback bounded to 4 KiB and never invoke an uncontrolled fallback.
  Environment-derived protocol configuration is initialized before listener binding, so a first
  scrape cannot perform that lazy initialization outside the request model.
- Exact/one-under and cancellation coverage includes
  `metrics_endpoint_enforces_exact_returned_byte_boundary`,
  `metrics_response_reservation_enforces_exact_memory_boundary`,
  `metrics_body_replay_honors_cancellation_and_releases_accounting`,
  `metrics_fallback_is_fixed_parseable_and_bounded`,
  `metrics_observability_projection_has_exact_memory_boundary_and_retains_guard`,
  `control_metrics_projection_reserves_exact_output_before_materialization`, and the write,
  fanout, outbox, digest, and rebalance projection boundary tests. The network-enabled server
  package suite passes 936/936 active library tests and 48/48 active migration-binary tests, with
  one intentional fixture-regeneration ignore in each binary. Workspace all-target checking,
  warning-denied all-feature Clippy, and all-feature rustdoc pass.
- This closes scrape-time projection and body work only. The process-global hotspot tracker still
  retains uncapped shard and tenant maps between scrapes. A safe bound requires an explicit
  product policy because eviction or approximation changes published per-identity totals and
  automatic rebalance ordering; scrape accounting does not solve that retained-state boundary.
- Legacy `/internal/v1/select_series` and `/internal/v1/list_metrics` calls that omit the additive
  `query_limits` field now inherit a finite Server per-query ceiling tightened by the storage
  instance. They require complete accounting and admission, keep local result, handoff bridge,
  encoding, and response reservations under one execution, and preserve the legacy wire shape by
  omitting the additive accounting field. Exact-boundary, cancellation, false-accounting,
  handoff, and zero-residual tests close this former compatibility exception.
- The older single-series `/internal/v1/select` request now uses the same finite Server ceiling and
  complete one-selector batch implementation internally. Local result, cutover handoff merge,
  exact two-pass JSON encoding, and response memory remain under one cancellation-aware execution,
  while the request and exact `{"points": ...}` response shape stay unchanged. Exact
  sample/returned-byte/memory N/N-1, false-accounting, cancellation, bridge, and zero-residual tests
  close the last unaccounted internal read compatibility route.
- Three characterization tests now pin the unresolved activation-convergence boundary without
  changing production behavior:
  `divergent_membership_view_rejects_proofless_newly_active_leader_repair`,
  `balanced_joiner_can_be_planned_for_activation_without_control_catchup_evidence`, and
  `removed_recommission_target_enters_postcommit_fanout_without_proposal_entry`. They prove that
  activation planning has no catch-up evidence, a lagging voter correctly rejects a proof-less
  newly Active leader before log repair, and a Removed recommission target can miss its own
  proposal entry. A volatile optimistic `peer_next_index` gate would not close this; durable joint
  configuration and receiver-verifiable membership proof remain required.
- Full corrected workspace matrices pass with localhost socket access. The all-feature run passes
  1,241/1,241 core tests, every async/integration/testkit/UniFFI/doc-test binary, and 941/941 active
  server tests with one intentional fixture-regeneration ignore. The no-default run passes the
  same 1,241 core tests and every remaining workspace binary, including 937/937 active server tests
  with the same ignore. Those runs exposed and then verified two deterministic test/contract
  fixes: plain async results now release their query execution before reply publication, and
  process-global directory-sync failpoints can be scoped so concurrent snapshots cannot replace a
  test's observed staging identity.

The 2026-07-29 continuation completed two more producer slices without completing either the
direct TSDB-status adapter or Phase 2:

- Core storage now exposes a schema-complete
  `status_observability_snapshot_with_execution` projection. The built-in engine measures and
  reserves the full retained clone while source guards are held, allocates only after admission,
  and returns a wrapper that retains the reservation. The `Storage` default fails closed instead
  of calling `observability_snapshot`; tenant-scoped and distributed adapters forward the
  contract. Evidence includes `default_storage_refuses_unaccounted_status_projection`,
  `status_projection_is_schema_complete_and_field_equivalent`,
  `status_projection_has_exact_memory_boundary_and_retains_guard`,
  `cancelled_status_projection_stops_before_policy_copy`, and
  `status_observability_delegates_schema_and_releases_reservation`. This closes the core producer,
  and the direct `/api/v1/status/tsdb` consumer now uses it.
- Direct `/api/v1/status/tsdb` now admits one root execution before external-disk and storage
  observability production and retains it across complete metric enumeration plus its
  allocation-bearing write/fanout, outbox, consensus/persistence/handoff, digest,
  hotspot/rebalance, tenant, audit, security/RBAC, usage, managed-control-plane, and edge-sync
  projections, plus its allocation-free fixed planner projection. Each dynamic producer measures
  and reserves its complete dynamic output before materialization; private wrappers prevent
  extraction and remain live while response fields are borrowed. Usage replaces three separately
  sampled journal/tenant/report generations with one
  state-to-health generation and allocation-free reconciliation scalars. Managed control plane
  replaces three state-lock generations with one coherent projection, and edge sync accounts both
  populated source diagnostics and the two strings owned by its disabled default. Source guards
  forbid the legacy calls in this handler. Evidence includes
  `status_tsdb_cluster_path_uses_only_accounted_single_generation_producers`,
  `status_tsdb_usage_projection_preserves_tenant_filtered_schema_values`,
  `usage_status_projection_enforces_exact_peak_before_output_clones`,
  `status_projection_enforces_exact_peak_before_any_output_clone` in the managed-control-plane
  module, and edge sync's
  `status_projection_enforces_exact_peak_before_output_clones`. This closes the named dynamic
  source-producer slice, not the adapter's final JSON-tree construction peak. The support-bundle
  handoff is closed separately below.
- Focused verification after direct integration passes all 18 `status_tsdb` tests, all three
  usage-status projection tests, all three managed-control-plane projection tests, and all three
  edge-sync projection tests. `cargo check -p tsink-server --bin tsink-server --locked` is
  warning-free, and
  `cargo clippy -p tsink-server --all-targets --all-features --locked -- -D warnings` passes.
- The integrated working tree also passes `cargo fmt --all -- --check`, `git diff --check`, both
  locked workspace all-target check matrices, warning-denied locked all-feature workspace Clippy,
  and warning-denied locked all-feature workspace rustdoc. Full test matrices remain to be rerun
  after the next adapter slice rather than being claimed from the earlier pre-integration run.
- Admin rebalance status, pause, resume, and run now share one admitted root execution across
  complete metric enumeration, the minimal live control projection, one accounted hotspot
  tracker/projection, the full scheduler status, and exact cancellation-aware response encoding.
  Producer guards remain live while their projections are serialized, and the exact response
  reservation remains live through `HttpResponse` construction; the established successful
  response shape is unchanged. Mutations reserve a bounded fallback before applying their effect;
  a later projection or serialization failure truthfully reports `effectApplied: true` and the
  resulting state. Evidence includes
  `admin_rebalance_live_control_and_full_status_producers_enforce_exact_peak_limits`,
  `admin_rebalance_json_enforces_exact_memory_returned_and_cancellation_boundaries`,
  `admin_cluster_rebalance_status_uses_one_execution_and_releases_all_reservations`,
  `admin_cluster_rebalance_cancellation_releases_the_root_execution`, and
  `admin_cluster_rebalance_pause_reports_post_effect_memory_failure_truthfully`. This closes the
  direct admin-rebalance request path, not the process-global retained hotspot policy. The
  support-bundle handoff is closed separately below.
- The support bundle now admits one root execution before collecting children. TSDB status and
  rebalance reuse it without self-admission or child returned-byte charging. Each of the eleven
  support-specific child APIs establishes an exact same-execution reservation around its completed
  response before returning to the orchestrator; response-first wrapper layout releases the
  allocation before its guard, and every wrapper remains live through final composition. The
  16 MiB aggregate cap remains, the parent base excludes already-guarded child bytes, and the
  final bundle charges HTTP response-body bytes exactly once; child sources still charge canonical
  logical returned work. Tenant override resolution now rejects decoded IDs above 16 KiB before
  decoding, constructs only the two-header compatibility view, and never clones the potentially
  64 MiB HTTP request body. Focused verification passes 14/14 support-bundle tests, 18/18 direct
  TSDB-status tests, and 7/7 admin-rebalance tests. The cluster-enabled one-query regression
  returns `200` for both TSDB status and rebalance with exactly one admitted execution. This closes
  the completed child-response handoff. Tenant/actor and synthetic-request/header preparation
  before the parent reservation plus legacy child snapshot/serialization transients before the
  response guard remain open adapter work.
- Production background compaction now replaces its exhaustive post-flush clean fence with a
  retained, shared-memory-accounted cursor that consumes one admitted raw namespace entry per wake.
  It cannot enter lane planning or mutation until an empty terminal probe observes the same
  marker-publication generation that began the cycle. Publication resets the cursor before placing
  a marker behind it; terminal, error, reset, close, and drop release the reservation before the
  data-path lease. The path model covers both retained marker-directory copies, the recognized
  full marker path, file-name scratch, and 64 KiB of platform directory-stream scratch. The
  16,384-entry namespace cap, marker deferral, marker-shaped corruption checks, and exhaustive
  foreground, flush, and catalog fences remain unchanged. The portable capacity model charges
  twice each simultaneously owned path/name payload plus the fixed 64 KiB directory-stream
  allowance; the long-path regression pins six-times data-path growth for the two directory owners
  and recognized entry path. Verification passes the 25/25 recovery, 9/9 shutdown, 7/7 context,
  and 47/47 post-flush focused suites, the lifecycle no-planning and real staged-publication
  ordering regressions, both all-target check feature matrices, and warning-denied all-feature
  workspace Clippy. The all-feature workspace run passed all 1,261 core tests and every non-server
  suite; its sandboxed server phase was rerun with loopback permission and passed 999 tests with
  one intentional fixture-regeneration ignore. The complete no-default workspace matrix then
  passed end to end with the same core and server counts. Two unrelated fixed-delay/live-sample
  tests exposed by the full parallel matrix were made deterministic: remote-refresh backoff now
  uses bounded condition polling, and independently sampled filesystem-availability values are
  compared for JSON shape rather than unstable byte-for-byte equality. Final closure also passes
  `cargo fmt --all -- --check`, `git diff --check`, both locked all-target check feature matrices,
  warning-denied all-feature workspace rustdoc, package content listing, and full core package
  verification (326 files, 8.4 MiB unpacked, 1.5 MiB compressed). Incremental integration for
  finite background flush and catalog refresh was deferred at that checkpoint; the later
  continuation below closes it without retaining staged output between wakes.
- `HUMAN GATE`: the retained hotspot policy cannot be completed truthfully without a maintainer
  compatibility decision. The current process-global tracker never removes shard or normalized
  tenant identities, publishes saturating process-lifetime totals, and resets only on restart.
  Tenant scoping happens after global totals and denominators are computed, so even an identity
  omitted from a tenant-scoped response can change the returned score. Shard pressure also feeds
  automatic rebalance ordering. No finite profile-sized bound can preserve all of those exact
  semantics for arbitrarily many accepted identities.
  The maintainer must choose among: (a) exact per-runtime tracking with finite-ring shard slots and
  a configured lifetime tenant capacity that atomically rejects otherwise-valid ingest, query, or
  repair work before a new identity exceeds the cap; (b) a bounded rolling or epoch tenant window
  with explicit completeness/overflow metadata and versioned non-lifetime semantics, while
  retaining exact finite-ring shard counters for rebalance; or (c) deprecating tenant identity
  telemetry and retaining only exact bounded per-runtime shard tracking. The same decision must
  approve replacing accidental process-global aggregation with per-server or cluster-runtime
  ownership and must say whether any non-exact shard signal may influence automatic rebalance.
  No cap, eviction, reset, or approximation is being inferred before that decision.
- Unblocked hotspot characterization now pins cross-instance mixing, lifetime retention,
  tenant-scope/global-denominator behavior, top-eight truncation versus full aggregate values, and
  shard removal/merge effects on candidate ordering. The focused hotspot suite passes 9/9, the new
  repair-ordering test passes, and the existing hotter-mismatch ranking regression still passes.
  Remaining safe work is to expose fixed-label retained identity counts, modeled identity
  bytes/high-water, and reset/generation observability. Current ingest hotspot counters are
  recorded before later
  row-admission/write outcome, which must also remain documented or be changed as part of the
  selected contract.
- Direct `/api/v1/status/tsdb` response composition now streams its exact public schema twice from
  borrowed/accounted snapshots: a cancellation-aware counting pass enforces the fixed 1 MiB
  encoded ceiling, then the body and conservative response-header model are reserved before the
  body allocator runs. The adapter no longer builds a retained `serde_json::Value` tree, mapped
  `Vec<Value>` arrays, or per-pass string clones; optional object-or-null sections and dynamic
  arrays serialize directly while their producer reservations remain live. Focused regressions pin
  the exact 1 MiB acceptance and one-byte rejection boundaries, exact returned- and retained-byte
  admission, second-pass cancellation cleanup, the absence of legacy tree producers in the
  handler, the broad status schema, and a nonempty `final_sync` rebalance job with its complete
  fixed field set. This closes the direct-status serialization subtask, not Phase 2.
  Focused verification passes `cargo test -p tsink-server status_tsdb` (19 tests), the borrowed
  local-disk schema regression, and `cargo test -p tsink-server support_bundle` (19 tests), plus
  `cargo fmt --all -- --check`, `cargo check -p tsink-server --all-targets --all-features`, and
  warning-denied all-target/all-feature server Clippy. The full clean workspace matrix has not yet
  been rerun for this continuation.
- Support-bundle setup now admits the root query before tenant decoding, actor construction, or
  synthetic-request allocation. A borrowed-input preflight reserves their conservative peak, then
  reconciles to the retained root strings and one reusable request containing only verified-auth
  and tenant headers. The previous three full header-map copies are gone, so authorization,
  cookies, tracing headers, and the potentially 64 MiB body never enter child requests. Exact
  N/N-1, cancellation/release, admission-precedence, schema, and sensitive-header/body regressions
  pass in the 19-test `support_bundle` slice. This closes setup accounting, while legacy
  operational child snapshot/serialization transients before their completed-response guards
  remain open.

The 2026-08-05 continuation closes that remaining support-bundle producer gate without completing
Phase 2:

- All eleven support sections now have reserve-before-materialization source producers and
  measured, reserve-before-allocation serializers under the one forwarded execution. Usage, RBAC
  state/audit, security state, cluster audit/handoff/repair, rules, and rollups no longer call their
  legacy owned response/tree producers. TSDB status and cluster rebalance retain their already
  accounted success paths. Static source guards forbid the legacy snapshot/handler, `json!`,
  `serde_json::Value`, collection, and post-hoc response-accounting paths at each focused boundary.
- Rules status was the last large dynamic child. Its private accounted projection reserves the
  complete four-or-more-rule tree, maps, diagnostics, and alert instances before cloning. Separate
  legacy-limit and query-memory models preserve the established rules-store limit semantics while
  charging conservative B-tree nodes. The support serializer preserves declaration/key order,
  optional-field behavior, `status`-before-`data`, and the legacy eight-attempt self-observing peak
  fixed point across counting, exact body allocation, and final-capacity enforcement. Typed
  allocation/measurement/serialization failures retain the direct endpoint's exact text response.
  Producer and adapter regressions cover rich raw-byte parity, exact N/N-1 source and combined
  source/body peaks, cancellation, poisoned-lock release, unavailable/limit errors, and legacy
  source exclusion.
- The two remaining fixed compatibility-error paths no longer install a generic response guard
  after leaving their wrappers. In the support path, TSDB status and rebalance return the untouched
  raw error to the root. Before either call, an input-derived contract partitions the already
  admitted 256 KiB scratch between the capped 16 KiB tenant/raw response and its fixed budget-error
  mapper. The root then transfers the completed response to an exact same-execution guard before
  retention. Transfer failure preserves the distinct TSDB versus rebalance status/header/body
  mapping. Direct-versus-transferred regressions pin a TSDB early accounting error and both
  rebalance-unavailable shapes; worst-tenant/every-fixed-rebalance-shape tests reconcile below the
  preflight. This closes only the support shared-execution adapters; equivalent direct endpoint
  error construction after internal admission remains separate work.
- Persistent tenant runtime initialization is intentionally moved ahead of support root admission
  after the allocation-free malformed-tenant pass. It performs no authorization or permit
  acquisition, so the synthetic verified child still has its historical token behavior. When a
  runtime already exists or an unreserved slot is available, root concurrency retains precedence.
  Ordinary admission decisions now enter a preallocated compact ring without retaining new
  strings; exact legacy reason strings are reconstructed only inside the query-accounted
  tenant-status projection. Tests pin invalid-cache noncreation, idempotent warmup,
  configured-token/no-bearer behavior, blocked-root raw parity, all compact reason variants, stable
  ring heap/capacity, and status projection parity. This removes support-induced post-admission
  resident allocation.
- The registry's process-lifetime tenant runtime map is now capped by the optional top-level
  `maxRuntimeTenants` policy field, with a finite default of 4,096. Configured tenants and `default`
  own reserved slots, an undersized policy fails startup, unconfigured tenants use only the
  remainder, and one mutex makes concurrent check/construction/insertion atomic. The map never
  evicts: existing plans keep one semaphore generation and admission counters/decision history are
  not silently reset. Public template and managed-policy authorization runs before insertion, so
  established `401`/`403` results do not consume capacity. A full unconfigured pool returns stable
  `503`/`tenant_runtime_cache_limit_exceeded` without `Retry-After`; support prewarm returns that
  result before root admission. Four focused tests pin default/config validation, reserved-slot and
  exact wire behavior, authorization-before-insertion with duplicate token-scope parity, and a
  32-thread exact boundary. TSDB status now emits that scalar snapshot under
  `data.admission.tenant.runtimeCache` and emits `null` without a registry. `/metrics` always emits
  seven fixed unlabeled configured/count/limit/reservation/rejection series; an absent registry
  produces `configured = 0` and zeros for the other six. Focused unit and end-to-end regressions pin
  configured and absent behavior on both surfaces. This closes the former tenant-runtime cache
  policy-and-observability next-work item.
- Direct TSDB status and admin rebalance now reserve their compatibility-error construction
  envelopes immediately after their own query admission. TSDB status and rebalance status use a
  fixed 96 KiB fallback; pause/resume/run add a worst-case node-ID allowance derived from the
  admitted operation input. The guard remains live while legacy errors are built, partial success
  responses are dropped before mapping, and the success serializer reuses the same reservation so
  peak memory is `max(fallback, response)`. A rejected initial reservation drops the execution
  before its budget error is constructed. Raw status/header/body compatibility is unchanged.
  `direct_tsdb_error_scratch_enforces_exact_boundary_and_reuses_response_guard` and
  `direct_admin_rebalance_scratch_is_input_derived_exact_and_reused`, plus the 19-test
  `status_tsdb_`, seven-test `admin_cluster_rebalance`, and three-test `admin_rebalance_` slices in
  both feature modes, pin exact N/N-1, hostile input, post-effect, reuse, and cleanup behavior.
  This closes the former direct compatibility-error next-work item.
- Core storage now exposes an additive matcher-aware metric-name row primitive. It validates the
  request, matcher shape, and any output projection before cancellation or memory admission;
  resolves the exact metric-plus-matcher candidate set once; charges that selected set once; and
  applies an optional projection before row materialization and logical returned-byte charging.
  Projection is accepted only for a non-`__name__` label bound by a non-empty exact-equality
  matcher, preserving one-to-one visible identities. The non-default `TenantScopedStorage` path
  uses this primitive with an exact tenant matcher, requires inner `Complete` accounting, verifies
  the hidden label and result guard, and transfers/resizes that guard without a second page clone.
  Default-tenant fallback remains `Unaccounted` because it can merge scoped and legacy-unlabeled
  rows; distributed metric-name scans remain `Unaccounted` because the RPC has no canonical
  per-peer row cursor. Async result validation now also destroys a falsely guarded payload before
  releasing its detached reservation. Focused locked all-feature and no-default verification passes
  56/56 core query-budget tests, 48/48 tenant tests, 43/43 async-storage tests, ten synchronous and
  two async tenant metric-row contract tests, seven core matcher-aware contract tests, and the
  distributed fail-closed capability regression.
- Finite background flush and open-state catalog refresh now share the generation-stable,
  shared-memory-accounted post-flush clean-fence cursor previously used only by compaction. One raw
  marker-namespace entry consumes a wake; a stable terminal probe must complete under the
  compaction gate before flush stages a discoverable root or catalog refresh constructs inventory
  or publication state. Marker publication invalidates the cursor before writing its marker, so a
  marker inserted between wakes restarts the proof. Allocation-free minimum-work preflights retain
  the existing sealed-chunk and unknown-catalog error precedence. Foreground/lifecycle operations,
  manual compaction, and both-limits-unlimited background calls retain exhaustive behavior; flush
  now completes that strict proof before staging too, so a pending-marker or namespace error cannot
  strand newly published roots. Registry-catalog source failures after staging now also enter the
  existing root-rollback path. Catalog refresh reloads lifecycle under the compaction gate, so a
  refresh that waited behind close cannot recreate a retained cursor after the close-side reset.
  The 29-test `fence` slice and complete 96-test
  persistence-background module pass in both locked feature modes.
- Strict local-disk reconciliation now assigns scan-start request tickets and checked reservation
  generations. Concurrent idle barriers and reconciled-operation finishers can share one exact
  terminal scan, while a request registered during traversal or any later admitted reservation
  forces a follow-up scan. Reuse also requires the waiting caller's finite memory limit to admit the
  completed scan's recorded modeled peak; an overlapping unlimited scan cannot waive a smaller
  caller's bound. Ticket saturation disables reuse without wrapping; generation overflow rejects
  admission before changing reservation state. Governed removal skips a scan only for a
  definite absent target plus successful zero-byte settlement. Best-effort post-flush failure
  cleanup lazily admits one aggregate Recovery reservation, removes every classifiable path, and
  performs at most one strict scan for the governed batch; governance or reservation errors no
  longer prevent later external/valid cleanup. Tombstone recovery now batches every preflighted
  owned atomic temporary under one lazy Recovery reservation and one terminal scan, while each
  lane's already-preflighted orphan-shard sweep does the same for all recognized candidates.
  Unknown names remain untouched, definite missing targets avoid a scan, and a post-unlink
  parent-sync failure reconciles exact accounting before returning its native error. Obsolete
  immutable segment-catalog generations now
  retain the current and nearest predecessor while batching every governed deletion under one
  Recovery reservation and one memory-limited strict scan. Pending registry-catalog root deltas
  similarly batch every governed removed-entry unlink under one Recovery reservation and one
  terminal scan while retaining first per-entry cleanup-error ordering; an unlink or parent-sync
  ambiguity reconciles exact accounting before returning its native cleanup error. Incremental
  series-registry checkpoint cleanup preserves its bounded all-entry preflight and unknown/link
  retention while batching recognized governed generations under one lazy Recovery reservation and
  one terminal scan; it stops at the first per-entry cleanup error and reconciles post-unlink
  ambiguity before returning the native error. Full-snapshot rollup state-journal cleanup likewise
  removes every recognized sealed/active generation under one Recovery reservation and one terminal
  scan. Canonical v3 pointer/generation files also reconcile into Registry rather than Unknown;
  strict filename/layout lookalikes remain Unknown.
- `IN PROGRESS` The next reconciliation/pressure batch has focused two-mode evidence but awaits its
  complete integrated matrix. Preparing-compaction rollback and rollback of newly published flush
  segment roots now retain one aggregate Recovery reservation through all exact removals and one
  terminal scan, including post-unlink parent-sync failure. Read-write startup preflights every
  post-flush marker temporary and rewrite/copy staging tree before mutation under one shared
  16,384-entry, depth, and modeled-startup-memory envelope, then uses one zero-byte Recovery
  reservation and one bounded scan for all governed roots; empty, lookalike-only, and external-only
  plans do not scan. Reconciliation-waiter saturation releases the consumed reservation and keeps
  the full admitted peak as conservative accounting. Native registry-catalog deltas admit the
  checked sum of intent, added entries, and final manifest under one strict aggregate. Incremental
  series journals derive restart nonces from their own directory without advancing the global
  empty-namespace seed. Memory/WAL pressure now retries one positively delayed, serialized bounded
  relief pass on every still-blocked poll, allowing multiple old heads to be reclaimed while the
  current head remains intact.
- `DONE` Close the bounded rollup source-state journal and JSON-startup slice. Create-only batches
  now use exact namespace slots, require equality with a same-generation packing shadow before
  cleanup/replay, detach live batches before child deletion, serialize governed mutations once at
  their outer boundary, and distinguish definite pre-publication rejection from the canonical
  rename fence. Policies, checkpoints, pending state, invalidations, generations, and journal
  replacements share one retained envelope. Policy/state JSON loading now performs a
  schema-equivalent streaming physical-record pass, admits raw predecessor/successor buffers,
  trace growth, typed DTOs, and destination maps, preserves format/magic error precedence, and
  moves decoded state into the runtime without full install clones. A bounded materialization page
  reads only each selected source's checkpoint/pending entry rather than cloning the policy's
  complete maps, then computes coverage from installed results.
- `DONE` Aggregate core startup's remaining exact owned-orphan classes. Fixed atomic targets,
  registry deltas, the tombstone coordinator, per-lane manifest/shard temporaries and unreferenced
  final shards, compaction markers, and segment staging trees now share one mutation lock, global
  namespace counter, retained-memory plan, lazy Recovery reservation, and terminal reconciliation.
  Directory identities and tombstone-manifest fingerprints are rechecked before exact deletion;
  execution preserves source order and first-error stopping. The terminal scan retains a bounded
  native-error allowance, including a dynamically larger post-discovery namespace, while empty and
  external-only plans avoid local reservation/reconciliation work.
- Focused verification passes 74/74 `support_bundle_` tests in both locked all-feature and
  no-default-feature modes, 48/48 tenant tests, 23/23 tombstone tests, 9/9 registry-catalog tests,
  3/3 incremental-registry checkpoint-cleanup tests, 51/51 rollup state-journal tests, four rich
  rules producer tests in both feature modes, and six rules child tests in both modes. Both
  warning-denied workspace all-target Clippy matrices pass, as do formatting, warning-denied
  workspace checks, the warning-denied all-feature workspace documentation build, and diff checks.
  The complete locked server package matrices
  also pass with loopback access and `RUSTFLAGS='-D warnings'`: each feature mode passes 48/48
  active auxiliary tests and 1,103/1,103 active server tests, with the two established fixture
  ignores. The complete locked all-feature and no-default-feature workspace matrices also pass
  with loopback access and warning denial: each passes 1,297/1,297 core tests, 43/43 async-storage
  tests, and every remaining workspace binary, including those same server counts and established
  ignores.

The 2026-08-06 wrap boundary additionally passes, in both all-feature and no-default-feature modes,
11/11 rollup JSON-preflight tests, 9/9 rollup transformation/page-state tests, 13/13 aggregate
startup orphan-cleanup tests, the active/reused policy-id startup boundary, and the 51/51 journal
slice. Warning-denied core library checks and library-test Clippy pass in both modes, as do scoped
formatting and diff checks. The complete workspace/package counts immediately above predate this
latest boundary and were not rerun before the requested stop.

The next required Phase 2 work is:

1. Resolve the retained-hotspot `HUMAN GATE` above. After a maintainer selects the compatibility
   contract, implement instance ownership, shard capacity tied to a finite ring, the selected
   tenant capacity/window/removal behavior, overflow/rejection observability, and explicit
   rebalance semantics. Until then, continue only the non-semantic characterization and
   observability work listed above.
2. Close the default-tenant series-row and metric-name row gaps, plus the distributed series-row
   and metric-name row gaps, only after exact paged-work reconciliation can preserve canonical
   logical result charging.
3. Finish the remaining background pass-budget integrations: full-root disk reconciliation after
   ordinary governed mutations; registry-journal discovery/merge peaks; one shared residual
   allowance across every rollup source selected in a wake; whole-policy state-snapshot encoding;
   and complete pressure-path reclamation/flush fallbacks.
4. Implement the cluster activation membership-certificate/joint-configuration convergence proof;
   characterization now pins the gap, but it has not been reclassified as an accepted exclusion.
5. Calibrate the shared query envelope under constrained direct, async, PromQL, HTTP, and
   distributed workloads, keeping logical-versus-physical byte evidence separate from
   process-memory measurements.
6. Run the complete clean constrained Test, Embedded, Edge, and Server workload matrix; qualify or
   revise the shipped provisional constants and record the final evidence. Preserve the explicit
   expert-only unlimited migration path and deterministic base-plus-override contract.
