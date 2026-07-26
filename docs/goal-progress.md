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
- `DONE` Verify crate contents and the package dry run. The current package contains 264 files and excludes
  `GOAL.md`, `docs/goal-progress.md`, `.github`, and `scripts`.
- `DONE` Improve documentation of the primary embedded lifecycle and canonical write APIs.
- `DONE` Re-audit public examples, finite-profile defaults, on-disk layout, and tuning guidance
  against the implementation. Correct Python naming/restore usage, boolean CLI examples, real
  segment/tombstone/catalog paths, and the direct server invocation shown by `--help`.
- `DONE` Add pull-request, rustdoc, no-default-feature, and package-dry-run CI coverage and remove
  automatic publication from `master`. Published GitHub Releases and manual tag dispatches now
  require a real version tag at the exact checked-out revision, a promoted and empty-Unreleased
  changelog, the release workflow's Ubuntu verification matrix, MSRV validation, a server-version
  smoke test, and package dry runs. It does not currently depend on the separate Windows CI or
  benchmark jobs. Actual registry publication remains fail closed until every mandatory GOAL
  release gate exists.
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
  completely; `ExpertUnlimited` remains explicitly unbounded. Per-source
  checkpoint/pending state is now a checksummed replacement journal with 1,024-record/4 MiB active
  generations, bounded adjacent-generation compaction, a 1,024-generation namespace ceiling, and
  restart-safe epoch rebasing into full policy/delete snapshots. Those full snapshots reject more
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
  retains admitted decoded fragments across wakes, terminally revalidates every manifest, and
  publishes their monotonic union with the old live map under one visibility fence before segment
  additions. Manifest/pointer/visibility changes restart without exposing the staged map, and no
  tombstone root scan is used. The final mutable `HashMap` swap plus visibility-cache rebuild
  remains one hard-bounded item; `MaintenanceWorkItemTooLarge` preserves old visibility when it
  cannot fit. Replacing that structured strand point with an immutable sharded live snapshot and
  epoch-tagged cache is still open. Finite explicit/manual
  rollup calls now advance the same shared cursor by
  one policy and one item/byte-bounded source-postings page, return structured continuation state,
  retain the cursor on global failure, and require an empty terminal page for an exact multiple.
  Rollup status uses traversal counters rather than re-enumerating every source. Explicit
  `ExpertUnlimited` alone drains the complete policy/source cycle in one manual call. Close now
  caps maintenance-gate,
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
- `DEFERRED` Complete byte reservations for remaining transient and non-write memory growth,
  the residual background pass integrations, and final profile measurements.

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
trees require captured descendant identities, unknown or replaced entries are retained, partial
copies without complete identity evidence are retained, and a visible snapshot whose final parent
sync fails is reported with indeterminate durability rather than recursively deleting possible
consumer data. The final inspection/salvage acceptance audit is complete. Cross-platform
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
- Finite remote tombstone continuation verification — `PASS`: `cargo check --lib`; all 3
  `maintenance::catalog_refresh::bounded_tombstones::tests`; all 54 tombstone-filtered core tests;
  the 3 finite-remote-catalog tests; and focused pointer-churn, corrupt-v3, and
  ExpertUnlimited-compatibility tests. Deterministic coverage pins one-item manifest/shard/
  revalidation/publication wakes, no root scan, unchanged live visibility before the terminal
  swap, exact terminal byte admission versus N+1 structured rejection, retained-charge release,
  and manifest replacement causing a clean restart before a complete retry.
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

- No core segment, WAL wire format, or data-directory format version changed.
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
  explicitly reserved, caller-owned write inputs, rollup work, remote
  refresh complete-inventory input materialization, thread stacks,
  allocator/runtime/kernel/TLS overhead, and other server state are not charged to a complete
  process envelope. Atomic active-state staging is admitted through `write_transient_bytes`;
  metadata, exemplar, and rules replacement have separate finite store envelopes. Finite
  compute-only v3 generation reading, decoded page/path retention, one-root
  application/publication, retained cursors, and staged root maps are admitted separately and
  exposed as `remote_catalog_staging_bytes`.
- Exact disk cleanup that removes an owned entry performs a full-tree reconciliation and relies on
  an internal no-nested-reservation invariant; a no-op orphan pass skips that rescan. This is
  correctness-first but can make repeated effective cleanup expensive, so batching or safe deferred
  reconciliation is still needed before claiming high cleanup throughput.
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

## Recommended next three tasks

1. Calibrate the implemented query envelope under constrained direct, async, PromQL, HTTP, and
   distributed workloads, and decide explicit contracts for remaining compatibility adapters and
   caller-owned result vectors. Keep cancellation/error release and logical-versus-physical byte
   evidence separate from process-memory measurements.
2. Replace the hard-bounded monolithic remote-tombstone map/cache publication with an immutable
   sharded live snapshot and epoch-tagged cache, and measure catalog/tombstone
   cleanup/reconciliation throughput under a large managed namespace. Finite compute-only catalog,
   tombstone-file staging, and read-write tiered catalog publication are now paged.
3. Run the complete clean constrained Test, Embedded, Edge, and Server workload matrix; qualify or
   revise the shipped provisional constants and record the final evidence. Preserve the explicit
   expert-only unlimited migration path and deterministic base-plus-override contract.
