# Changelog

All notable user-visible changes to tsink are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases follow Semantic
Versioning for the public API. Storage-format compatibility is tracked separately from Rust API
compatibility and is not yet covered by a stable support promise.

## [Unreleased]

### Added

- A repository progress ledger and product-positioning documentation for the embedded metrics
  engine roadmap.
- A declared Rust 1.89 minimum supported Rust version (MSRV) and an MSRV CI check.
- A design-partner interview guide for validating embedded, test-database, and edge use cases.
- Canonical `Storage::write_batch` and async/UniFFI equivalents with explicit `Atomic` and
  `BestEffort` modes, indexed outcomes, structured rejection categories, and bounded diagnostics.
- An opt-in `with_max_future_skew` admission cutoff; the default future-timestamp behavior remains
  unchanged.
- Finite default series-shape limits (128 labels and 64 KiB of cumulative metric/label UTF-8
  identity bytes), optional fixed-window new-series creation admission, structured rate-limit
  rejections, effective-limit inspection, and cardinality observability across core, async,
  UniFFI, and server surfaces.
- Optional pre-clone write row/input bounds and atomic modeled-memory reservations for foreground
  preparation, WAL encoding, retries, rollup writes, and streamed startup replay. Core, async,
  UniFFI, effective-limit, and observability surfaces expose the controls and current/peak/counter
  state; the finite WAL writer buffer is inspected explicitly.
- A durability contract covering core WAL modes, lifecycle behavior, server sidecars, cluster
  acknowledgements, and platform limits.
- Structured write-rejection, acknowledgement, partial-effect, and indeterminate-outcome metrics
  and HTTP response headers across the principal ingest adapters.
- Shared local-disk quota admission and exact restart/category accounting for the experimental
  hinted-handoff outbox, including structured HTTP 413 responses when enqueue growth is rejected.
- Shared local-disk quota admission for cluster and standalone edge-accept deduplication markers,
  with exact restart accounting and structured partial HTTP 413 responses when a completed write's
  marker cannot reserve disk growth.
- Shared local-disk admission for the experimental cluster audit log and edge source queue. Audit
  appends retain typed quota/headroom failures; edge queue quota rejection maps to a non-retryable
  partial HTTP 413 after the primary row write has committed.
- Paired persistence for the experimental cluster control state and consensus log. Control-log
  schema v2 embeds the authoritative `checkpointState` and restart-durable `steppedDownTerm`; both
  replacements are staged under one shared `Cluster` disk reservation and the log is published
  before its repairable state mirror.
- `StorageBuilder::restore_from_snapshot_with_disk_budget`, an additive offline restore API for a
  caller-owned finite disk envelope rooted above the target. Admission covers logical snapshot
  bytes, a destination-allocation-unit-aware allowance for every bounded snapshot entry, and
  missing target ancestry; exact accounting replaces that reservation after a successful scan.
- A dedicated server offline-restore envelope configured by `--offline-restore-root`,
  `--offline-restore-disk-limit`, and optional filesystem headroom. One cross-process-leased
  coordinator governs standalone and internal restore staging/targets, local cluster restore
  targets, and post-restore reports, with independent status and Prometheus metrics.
- Explicit per-instance background-worker bounds and lifecycle observability across core, UniFFI,
  server status, and fixed-cardinality Prometheus metrics. The four named slots report effective
  cadence, idle parks, passes, notifications, exits, and shutdown joins.
- Bounded background-persistence progress and limit-hit counters across core, UniFFI, server
  status, and fixed-cardinality Prometheus metrics.
- Finite `Test`, `Embedded`, `Edge`, and `Server` resource profiles, a fully specified Rust
  `Custom(ResourceLimits)` profile, and explicit `ExpertUnlimited` migration behavior. The core and
  async builders now default to `Embedded`, the server defaults to `Server`, sparse low-level
  overrides win independently of call order, and a versioned resolved snapshot is exposed through
  sync, async, UniFFI/Python, observability, and server status surfaces. Initial constants remain
  provisional pending the final clean resource-measurement matrix.

### Changed

- Reproducible named-profile measurements retain the original 64 MiB Test, 256 MiB Edge, and
  512 MiB Embedded accounted-memory and maintenance-pass ceilings. After exact write-head growth
  accounting and fill-aware timed flushes removed the prior cap-chasing behavior, all three named
  rows completed three repetitions without late rejection; the temporary Test and Embedded ceiling
  increases were therefore reverted.
- The reproducible `server-queries` row now saturates all 32 Server query permits, checks the
  structured N+1 rejection, and overlaps the admitted reads with a 100,000-point writer. Three
  repetitions completed without query-resource leaks.
- The 100,000-series Server base row completed three 4.1-million-point repetitions without late
  rejection and used a 441,129,372-byte p95 modeled peak under the 2 GiB ceiling. The 16-writer row
  also admitted all 400,000 new series per repetition across three runs. RSS, query entry-point
  breadth, and non-memory calibration remain provisional work.
- The resource-measurement harness now emits a separately labeled operating-system process RSS
  high-water for mixed, writer-saturation, and query-pressure rows. The value is cumulative within
  a benchmark process and includes allocator, runtime, and harness memory; it is evidence for the
  remaining clean RSS matrix, not an engine-only accounting or cap.
- Built-in `list_metrics` now admits the shared query budget for direct calls and reuses the async
  worker's execution. Fixed registry-page scratch, accumulated returned identities, cold
  visibility-summary repair, retained dead-series IDs, and the pruning companion vector are
  preflighted against series, returned-byte, intermediate-vector, and per-query/shared-memory
  limits before growth. Cold repair re-admits actual active/sealed counts under their read guards,
  so concurrent ingest cannot invalidate the estimate. The default-tenant server wrapper also
  shares one execution across scoped and legacy selections and reserves its in-place merge before
  growth.
- Timed non-tiered flushes now defer a WAL-backed current head until it fills at least half of its
  allocated point block. No-WAL and tiered durability, retained-memory pressure, finite-WAL
  pressure, explicit flush, and close still admit young heads. This prevents high-cardinality
  ingest from turning every 250 ms pass into thousands of two-point persisted chunks.
- Project and package descriptions now lead with the embeddable, Prometheus-native use case.
- Repository and package links now point to `github.com/cantrepro/tsink`.
- Cluster mode is documented as experimental and is no longer presented as the primary adoption
  path.
- The root crate package excludes repository-only CI, progress, roadmap, and maintenance-script
  files.
- Multi-shard writes stage every fallible active-state mutation before publication, and failed
  active-head finalization preserves query visibility and correct memory accounting.
- Principal HTTP and internal cluster ingest paths use canonical atomic writes and validate backend
  result invariants before reporting success.
- Background intervals now have a nonzero idle floor, and shutdown continues joining later owned
  workers after reporting an earlier worker panic.
- Metadata and exemplar sidecar stores publish staged state only after persistence succeeds; a
  later sidecar failure discloses already accepted components.
- StatsD relative-gauge state advances only after proven row acceptance, oversized UDP datagrams
  are rejected without truncation, and Graphite closes a connection when a line cannot be parsed or
  stored.
- Cluster write consistency now counts one acknowledgement per replica and shard only after all of
  that replica's bounded transport fragments succeed; successful fragments still contribute to
  physical commit accounting when a later fragment fails.
- Cluster dedupe completion persistence failures are retryable and disclose established row and
  sidecar effects instead of inventing success. Edge-sync and hinted-handoff replay retain entries
  until a complete canonical atomic result is validated, and edge-sync synchronizes its queue
  acknowledgement record before removing an entry from memory.
- Hinted-handoff acknowledgements can append with Recovery admission when the shared disk quota is
  exhausted, then attempt bounded streaming compaction. A failed post-record compaction remains
  retryable cleanup debt without changing the already-durable Ack or reschedule outcome.
- Dedupe marker compaction uses bounded atomic replacement and Recovery admission for non-growing
  cleanup. The marker store no longer retains an append descriptor across atomic replacement, and
  startup removes only generated replacement temporaries plus the exact legacy `.tmp` path.
- Edge source Put, Ack, and expiry records are synchronized before in-memory publication. Ack and
  expiry cleanup can use Recovery admission at the growth limit, and both edge and audit logs use
  exact-length atomic compaction whose post-record failure remains cleanup debt. Edge source status
  and metrics now distinguish deferred cleanup from a persistence-fenced queue and mark either
  condition degraded.
- Cluster control mutations now distinguish a typed pre-consensus persistence rejection, a
  consensus-required candidate still pending durable log publication, and a durable log commit
  whose state-mirror checkpoint is pending. The latter two fence further mutation until repair.
  A fourth outcome, durable pair publication followed by finalization, owned-temp cleanup, or
  accounting-reconciliation debt, returns a degraded `committed_cleanup_pending` result without
  fencing authority; cleanup runs before any separate fence repair. TSDB status and fixed-
  cardinality Prometheus health flags expose both conditions, while HTTP distinguishes stable
  `write_disk_quota_exceeded` and `control_persistence_indeterminate` codes.
- Another successful degraded outcome covers a quorum-committed command whose commit-notice
  response reveals a higher term that cannot yet be persisted:
  `committed_persistence_pending`, or `accepted_persistence_pending` for internal auto-join. The
  node adopts the term in memory, fences leadership, and retries the required log-only publication
  instead of misreporting the command as rejected.
- Authoritative control-mirror recovery can recreate or grow a missing/stale mirror at the logical
  quota because the durable schema-v2 log already holds authority; physical-space and headroom
  checks still cover the full temporary peak. Only Active members count toward control quorum or
  may assert leadership, and failover excludes a recorded leader that is no longer Active. Control
  and cluster recovery-snapshot exports return 503 `control_persistence_indeterminate` while
  authority is fenced, but cleanup-only debt remains exportable. Activation and leadership transfer
  must follow control-log catch-up; proof of newly activated leader eligibility to a lagging voter
  remains incomplete Phase 2 work. As a crash-safe near-term rule, an Active leader must transfer
  leadership before its own leave can be committed.
- Enabling experimental cluster mode now requires an explicit `--data-path`. Cluster sidecars and
  consensus state no longer fall back to an unleased, unbudgeted temporary root; data-path-free
  operation remains available only to non-cluster in-memory servers.
- A disk-capacity rejection during segment-flush staging or foreground WAL Growth admission now
  triggers one fully-expired-segment-only cleanup pass and one retry. This path neither rewrites
  mixed-age segments nor removes unknown or host-created files, and preserves the original typed
  capacity error when no eligible cleanup reclaims space or cleanup itself reaches capacity.
- Bounded segment persistence now publishes only replay-closed sealed-chunk prefixes. Older active
  WAL state defers publication, and a dependency group larger than the configured pass returns
  `MaintenanceDependencyWindowExceeded`; neither case can lower a checkpoint below selected data
  or advance it through an unpersisted suffix. WAL durability advances only after verified catalog
  visibility commits, so a failed publication remains replayable.
- Incremental registry persistence now returns structured
  `MaintenanceNamespaceLimitExceeded` before a new delta would exceed the bounded startup
  namespace, preventing a running instance from creating a directory its own recovery scan must
  reject.
- Repeated incremental registry writes now compact into a checksummed, bounded active generation
  and roll over by sealing that generation before publishing its replacement. Startup remains
  compatible with legacy deltas, interrupted rollover retains exact replay state, and checkpoint
  cleanup no longer removes unknown files from the registry-delta directory.
- Clean no-op retention/tiering wakes no longer rebuild and rewrite the complete persisted segment
  and registry catalogs after an already-published flush.
- Ordinary registry-catalog root changes now publish through a per-segment
  `series_index.catalog.d/` store. A bounded, crash-replayable manifest intent applies only the
  page's named removes/upserts, invalidates the aggregate series fingerprint before mutation, caps
  the live namespace, intent payload, and every sidecar decode, reconciles managed-disk accounting,
  and migrates legacy v2 JSON catalogs during a complete startup checkpoint.
- Finite non-tiered unknown-dirty refresh now scans one charged continuation page at a time, retains
  its deduplicated snapshot within the maintenance byte ceiling, and publishes stable add/remove
  root deltas only after scan completion. Every directory open, raw entry (including unknown
  names), manifest inspection, source load, and removal consumes the item/byte envelope; the fixed
  namespace ceiling returns `MaintenanceNamespaceLimitExceeded`. A failed publication retries its
  exact page, a newer visibility generation discards the stale cycle, and restart falls back to
  startup's strict inventory. Dirty state clears only after terminal add and prune probes.
  `ExpertUnlimited`, lifecycle startup/close, and tiered local/shared segment catalogs retain their
  explicit complete-inventory behavior.
- Background retention/tiering now seeks through one deterministic persisted-root page per wake.
  Every inspected descriptor and every selected source's manifest-declared logical bytes consume
  the configured maintenance item/byte envelope; the cursor advances only after the page's durable
  replacement transaction finishes. Exact-multiple scans require an empty terminal page, and a
  failed publication retains its cursor so recovery retries the same page before later roots.
  Foreground startup, close, and capacity-reclamation sweeps retain their complete strict-scan
  behavior.
- The background compaction worker alternates numeric and blob lanes, so a dual-lane instance no
  longer consumes the configured per-pass work allowance twice in one wake.
- Multi-output compaction and retention rewrites now measure their complete staged disk peak and
  acquire one shared Maintenance reservation before the first output mutation. Admission covers
  all output files, simultaneous Preparing/Ready replacement markers, allocation-unit and
  missing-ancestry allowances, and source-retirement entries. Exact N/N+1 and concurrent
  reservation tests prove structured rejection without consuming output IDs or publishing partial
  state; Preparing/Ready restart replay, later-output faults, and unwind RAII preserve exact
  accounting and release every active reservation. `ExpertUnlimited` retains no logical disk
  ceiling while still checking actual free space and configured filesystem headroom.
- The background rollup worker now seeks through one bounded metric-postings page for one policy
  per wake, using the maintenance item and modeled identity-byte ceilings without inspecting an
  extra posting. Source-local read/downsample failures do not starve later series in the same page;
  global persistence/write failures retain the cursor for retry. Finite explicit/manual runs share
  that cursor and process one item/byte-bounded page per call. Status uses accumulated traversal
  counters instead of a fresh full source enumeration and returns additive completion,
  continuation-policy, and continuation-series fields across Rust, JSON, UniFFI/Python, and
  Prometheus. `ExpertUnlimited` retains the legacy complete-cycle manual behavior.
- Rollup source reads, internal writes, and checkpoint publication now drain every writer permit,
  excluding concurrent raw commits from the checkpoint window; a later historical commit safely
  invalidates the completed generation. Materialized output is split into finite row/byte write
  batches under that held fence. Each raw or existing-materialized source read also owns an
  internal `QueryExecution`: it preserves every finite instance work/deadline limit, and a finite
  maintenance byte cap further tightens memory, scan/result work, and intermediate length.
  Oversized append-sort sources fail before allocation without truncating their checkpoint window,
  and the background page continues at later sources. One source-scoped execution now remains
  admitted across raw and existing-materialized reads, conservatively reserves both retained point
  vectors, downsample output capacity/value payload, numeric aggregation scratch, and every
  per-bucket row/identity clone before allocation, then releases each charge through RAII. A
  finite transform/row-admission rejection is source-local and occurs before
  pending/output/checkpoint publication; an exact-boundary retry completes atomically.
  `ExpertUnlimited` retains its explicit unbounded query/maintenance behavior.
- Per-source rollup checkpoint and pending-dedup publication no longer clones and rewrites the
  complete state JSON after each source/page. Checksummed source-state replacements compact in a
  1,024-record/4 MiB active generation, roll into at most 1,024 files, and replay idempotently after
  an ambiguous parent sync. Bounded adjacent-generation compaction reclaims superseded sources;
  full state-first policy/delete snapshots advance a journal epoch, absorb and clean older
  generations, and preflight a 65,536-item/64 MiB modeled envelope before cloning or JSON encoding.
  Crossing either the full-snapshot or journal-namespace ceiling returns a structured maintenance
  limit error without publishing partial state.
- Server usage status, reports, support bundles, and raw exports no longer scan or clone an
  ever-growing in-memory ledger. Exact all-time per-tenant summaries have a finite tenant namespace;
  raw/time-bucket reads use a finite recent-record window and stable snapshot-pinned pages with
  record/response-byte ceilings, explicit continuation metadata, structured expired-cursor and
  oversize errors, and fixed-cardinality retained-window observability. These server-side limits
  remain finite even when the core profile is `ExpertUnlimited`.
- Core close now retains the data-path process lease until all owned background workers have
  joined, preventing a second opener from overlapping a worker's final filesystem I/O.
- Core close now applies the configured lifecycle timeout to maintenance-gate, writer-drain, and
  compaction-drain waits before durability work can block behind an owned worker. Contention
  restores the open lifecycle with a retryable structured timeout; successful close still drains
  every accepted head, caps settling at 128 compaction passes, and retains the process lease
  through worker joins. Close outcomes, coordination waits/timeouts, compaction passes, whole-call
  duration, and join duration are observable in core, UniFFI, server status, and fixed-cardinality
  Prometheus metrics. Blocking write/fsync/rename/remove calls remain intentionally
  non-preemptible because returning early would make the durability outcome unknowable.
- Snapshot restore now rejects resolved source/target overlap, including existing path aliases,
  bounds trees to 100,000 entries and directory depth 128, rejects static link-like entries, and
  durably links missing target ancestry. The legacy restore API remains caller-unbudgeted; both
  restore APIs require a trusted, immutable source because portable traversal is not race-free
  against concurrent namespace replacement.
- Server restore now fails closed unless its separate finite envelope is configured. The legacy
  internal alias also uses the budgeted core API; cluster coordination requires the new
  `budgeted_restore_v1` peer capability and never falls back to an unbudgeted route. Cluster report
  destinations are checked against local sources and targets before restore begins, and a bounded
  post-commit report failure is returned as degraded success rather than a false rejection.
- Rollup policy and state snapshots now stage both complete files under one combined disk
  admission, including allocation-unit and missing-parent allowances, then publish the
  conservative invalidating state before the policy set. Any partial or ambiguous publication
  fences later in-process rollup mutations until reopen; a proven complete pair with cleanup debt
  remains a committed result instead of being misreported as rejected.

### Compatibility

- Rollup policy/state JSON remains schema v1, with `journal_epoch` optional and defaulting to zero
  when older snapshots are loaded. Older binaries ignore that field and the new journal files, so
  they conservatively observe the last full snapshot; startup now rejects policy/state JSON files
  above 64 MiB, and administrative snapshots above 65,536 logical items or 64 MiB modeled size.
- Existing compatibility write methods remain available and retain their all-or-error signatures.
  The canonical methods and result types are additive; third-party `Storage` implementations get an
  explicit unsupported default rather than fabricated indexed outcomes.
- Successful HTTP writes now include durability/component headers. Rejected or partially applied
  envelopes can return more specific non-success statuses and error codes than before.
- Graphite clients now observe a failed line as connection closure. StatsD remains response-less by
  design, with failures exposed through bounded counters.
- The cluster dedupe log stores optional completion data for exact retry replay and remains able to
  read older markers without that field; retries of legacy markers now return an explicit conflict
  because their original result cannot be reconstructed.
- The hinted-handoff log record format is unchanged. Startup additionally removes only the exact
  legacy compaction temporary path and generated current-format atomic-write temporaries.
- The dedupe marker format is unchanged; cluster markers are charged to the shared `Cluster`
  category and standalone edge-accept markers to `EdgeSync`.
- Cluster audit JSONL remains charged to `Cluster`; the edge queue JSONL remains charged to
  `EdgeSync`. Their record formats are unchanged, while an unterminated current log now fails
  closed during startup.
- Opening a legacy cluster control-log schema v1 file migrates and republishes it as schema v2 with
  required `checkpointState` and `steppedDownTerm`. Schema v2 is a downgrade boundary: older
  binaries that only understand v1 reject the rewritten log, so preserve a pre-upgrade data copy
  before rollback. The separate control-state mirror remains at its existing schema version.
  Startup attempts to rebuild a missing or invalid mirror from a valid v2 log and opens fenced with
  the checkpoint pending if repair fails; v1 migration still requires a valid mirror. Recovery
  snapshots carry `steppedDownTerm`, default older bundles to zero, merge the live/restored floor on
  normal restore, and clear it only for an explicit `forceLocalLeader` restore.
- No core segment or WAL wire-format version changed in this work.
- The budgeted restore method and its public entry/depth/admission constants are additive. Existing
  callers of `restore_from_snapshot` retain an unbudgeted API, with stricter invalid-snapshot and
  overlapping-path rejection before destination mutation.
- Existing internal restore routing remains accepted by upgraded servers but is now fail-closed and
  budgeted. Coordinated cluster restore requires upgraded peers advertising
  `budgeted_restore_v1`; an older peer returns `remote_restore_incompatible` instead of receiving a
  legacy fallback request.
- Rust versions older than 1.89 are not supported beginning with the next release.

## 0.10.2 - 2026-06-13

- Baseline release predating this changelog. Consult Git history for earlier changes.

[Unreleased]: https://github.com/cantrepro/tsink/commits/master
