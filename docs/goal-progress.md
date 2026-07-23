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
  edge queue Ack records are successfully appended and synchronized before pending in-memory state
  is removed. Retention expiry remains a separate, explicit edge-queue removal path.

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
  pressure states plus memory-specific waiter, event, and rejection counters.
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
- `IN PROGRESS` The persistent core now has a shared local-disk coordinator with checked atomic
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
  A stable leased coordinator for cross-lane, potentially cross-filesystem tombstone manifest
  publication remains incomplete, so the disk item is not `DONE`.
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
  `ExpertUnlimited`, lifecycle startup/close, and tiered segment-catalog publication retain
  complete snapshots. Finite explicit/manual rollup calls now advance the same shared cursor by
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
  The default-tenant server wrapper shares that execution across scoped and legacy selections and
  reserves its combined in-place merge. A three-run Server pressure row filled all 32 query slots,
  rejected N+1 structurally, completed 32/32 range reads plus a concurrent
  100,000-point writer each run, and released active/shared memory accounting to zero; async,
  PromQL, HTTP, and broader query-shape calibration remain open, so this item is not `DONE`.
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
  The harness now records a separately labeled process-wide RSS high-water, but the clean RSS
  matrix, query entry-point breadth, and non-memory calibration remain open,
  so exact profile constants are still provisional pending the final clean matrix.
- `DEFERRED` Complete byte reservations for remaining transient and non-write memory growth,
  the residual background pass integrations, and final profile measurements.

Phase 2 remains incomplete. Enforced memory totals are modeled estimates rather than RSS; the
measurement harness now reports an OS process-wide RSS high-water separately, and several
important allocation classes are named but unaccounted. Snapshot restore requires a trusted
immutable source because static path validation does not protect against a hostile concurrent
namespace swap. Shared query budgets are enforceable and observable; standard profiles now install
finite values, while explicit `ExpertUnlimited` preserves the legacy all-`None` query controls.
The named constants are not yet calibrated, and caller-provided aggregator internals remain outside
the modeled query-memory boundary. Usage metering is awaited and may add response latency. Its
in-memory state and administrative readers are now finite; a complete storage reconciliation still
performs a full database scan on its separately serialized blocking lane.

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

The affected matrix above was rerun for this slice. The complete workspace matrix will be repeated
after the remaining Phase 2 implementation. These results do not make the phase complete.

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
  sets, built-in aggregation state, and PromQL intermediates when configured. Excluded byte totals
  remain honestly unknown; caller-provided aggregator internals, pending/staged writes, WAL buffers
  and replay, rollup work, remote refresh staging, thread stacks, allocator/runtime overhead, and
  adapter/server state are named but not yet charged to a complete process envelope.
- Atomic active-state staging and metadata/exemplar replacement can temporarily clone state; that
  transient memory amplification is not yet measured or governed by a complete resource profile.
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
  external testkit adoption, and maintainer approval of stable APIs remain unresolved.

## Recommended next three tasks

1. Design the stable leased coordinator for cross-filesystem tombstone publication, then measure
   cleanup-reconciliation throughput.
2. Calibrate the implemented query-budget dimensions under constrained direct, async, PromQL, and
   HTTP workloads, and revise the provisional profile values from those measurements.
3. Run the complete clean constrained test, embedded, edge, and server workload matrix; qualify or
   revise the shipped provisional constants and record the final evidence. Preserve the explicit
   expert-only unlimited migration path and deterministic base-plus-override contract.
