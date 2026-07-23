# Durability contract

This document describes what a successful tsink write means at the API boundary. It is deliberately
conservative: an acknowledgement names the weakest guarantee established before the call returns;
it is not a promise about hardware or filesystem behavior beyond the operations described here.

The canonical API is `Storage::write_batch`. `Storage::insert_rows_with_result` exposes the same
batch-level durability for the compatibility write path. See
[`ADR 0001`](adr/0001-write-contract.md) for acceptance, rollback, and indexed-outcome semantics.

## Acknowledgement levels

| Level | Meaning when returned |
|---|---|
| `Volatile` | The accepted rows are visible to current readers. No crash-recovery log guarantee applies to the complete write. |
| `Appended` | The logical write is committed in the WAL, but the WAL has not necessarily been synchronized since that append. A process, kernel, power, or storage failure may lose it. |
| `Durable` | The write call completed the synchronization required by the configured core WAL path before returning. This is the strongest software guarantee tsink exposes, but it cannot override storage hardware, mount-option, controller-cache, or filesystem guarantees. |

For a best-effort batch, the result reports the weakest level among accepted rows. A result with no
accepted rows has no canonical acknowledgement.

## Core storage matrix

| Storage configuration | Normal non-empty acknowledgement | Clean close | Abrupt process termination | Power or host loss |
|---|---|---|---|---|
| In-memory; no data path | `Volatile` | No persistent copy exists | Accepted rows are lost | Accepted rows are lost |
| Data path; WAL disabled | `Volatile` | `close()` flushes pending state and returns any flush error | Rows not already persisted may be lost | No write-time persistence guarantee |
| WAL `Periodic(interval)` | Usually `Appended`; the append that performs an elapsed-interval sync may return `Durable` | `close()` synchronizes and flushes or returns an error | Recovery replays WAL data that reached stable storage; the unsynchronized interval is a loss window | Same loss window, subject to filesystem and device semantics |
| WAL `PerAppend` | `Durable` after the logical WAL write and its publication are synchronized | `close()` also flushes segment state and reports failure | WAL recovery is intended to restore the acknowledged logical batch | Intended to survive when the platform honors the synchronization operations; no stronger hardware claim is made |
| Compute-only runtime | Writes are rejected | No writable local state | Not applicable | Not applicable |

`Periodic` does not currently own an autonomous timer that fsyncs an idle WAL. The interval is
checked by later appends and lifecycle work, so an idle process can retain an `Appended` high-water
mark until another write or close.

An empty compatibility write is a durable no-op because it creates no state to lose. A canonical
empty batch instead returns zero outcomes and no acknowledgement. Lifecycle and compute-only checks
still run in both cases.

## Visibility and ordering

The single-node atomic pipeline validates and stages a complete batch before publishing it to live
read state. With the WAL enabled, series definitions and samples are staged as one unpublished
logical WAL write, applied to staged in-memory state, and then published. A rejected atomic batch
has no accepted rows, no acknowledgement, and is not replayable.

`BestEffort` deliberately creates one such boundary per input row. Its indexed outcomes are the
source of truth for which rows committed. The complete submission is nevertheless checked and
admitted once before row-wise execution, so a configured row/input bound also caps the outcome
allocation and cannot be bypassed by selecting this mode.

Crash recovery walks committed WAL frames in order under the same modeled-memory budget. It admits
a frame before payload allocation, registers one series definition at a time, and decodes/applies
one sample batch at a time. A configured write-batch limit or the intrinsic decoded-batch safety
ceiling fails open with a structured error instead of partially publishing a later frame.

If logical WAL publication fails after in-memory application, tsink cannot truthfully claim WAL
recovery for that batch. The current core returns a successful `Volatile` acknowledgement and marks
the durability path degraded for observability; callers must not reinterpret it as `Appended` or
`Durable`.

## Background and close failures

The built-in engine fences or reports background persistence failure according to
`with_background_fail_fast`. A successful `close()` waits for owned lifecycle work and returns only
after its required flush/synchronization work succeeds. A failed close is not a durability
acknowledgement and must be handled by the embedder.

Close unparks all four possible instance-owned workers and attempts every join in fixed order. If a
worker panicked, the returned error names it, but that first error does not leave later worker handles
unjoined. Close still has no timeout and cannot interrupt a worker blocked inside a filesystem call;
embedders must not interpret this lifecycle contract as a bounded shutdown-duration guarantee.

A fail-fast fence remains `TsinkError::StorageShuttingDown` on the compatibility write API and is
reported as `WriteRejectionCategory::StorageDegraded` by the canonical API. Lifecycle close is
reported separately as `StorageClosed` in both APIs.

Write-time admission errors, I/O errors, closed storage, and safe atomic rollback are represented as
structured canonical rejections. An outer API error is reserved for a backend or failure that cannot
provide trustworthy indexed outcomes.

## Server sidecars and experimental cluster mode

Metric metadata and exemplars are maintained by server-side stores outside the core row/WAL
transaction. They stage a full replacement, synchronize the replacement file, rename it,
synchronize the parent directory, and only then publish the staged in-memory state. A failure
reported after rename restores the preceding file before returning when rollback succeeds. An HTTP
envelope containing either sidecar is still conservatively reported as `Volatile`, even when its
core rows received a stronger WAL acknowledgement, because the sidecars do not participate in the
core WAL transaction or have a WAL-backed acknowledgement contract of their own.

At startup, the budget-integrated metadata, exemplar, rules, and managed-state stores remove only
temporary entries matching the generated `.<target>.tmp-<pid>-<nonce>` shape while the server holds
the data-path process lease; `pid` is canonical decimal `u32` text and `nonce` is exactly 16
lowercase hexadecimal digits. A matching directory is ambiguous and fails startup instead of being
deleted. A successful removal is synchronized and followed by accounting reconciliation; a no-op
orphan pass does not rescan the tree. The server independently performs one final reconciliation
after all persistent stores open. Managed stores durably link newly created nested directories into
their parents and reject an owned file whose final directory entry is a symlink or another
non-regular file, including a dangling symlink. These checks are not descriptor-relative and do not
close the remaining hostile concurrent namespace-swap race.

Rows, metadata, and exemplars do not yet share one cross-component transaction. If a later sidecar
fails after earlier components commit, HTTP returns a non-success response with
`X-Tsink-Write-Partial: true`, component counts, and the weakest acknowledgement for the committed
part. It must not be retried as though nothing committed without an idempotency strategy.

Experimental cluster routing does not provide cross-node atomicity. For row-only writes, a
successful response reports the weakest acknowledgement returned by every replica counted toward
the requested consistency; that aggregate says what those acknowledging replicas established, not
that all replicas committed as one transaction. Metadata or exemplar sidecars weaken the complete
envelope acknowledgement to `Volatile`. A failed route that may have committed on a replica is labeled
`X-Tsink-Write-Outcome: indeterminate_cluster` and `X-Tsink-Write-Partial: possible`.
If a storage implementation returns a malformed canonical result, the server cannot safely infer
whether rows committed and returns `X-Tsink-Write-Outcome: indeterminate_backend` with the same
`possible` partial marker.

Hinted-handoff and edge-sync queues are asynchronous delivery mechanisms, not stronger core row
acknowledgements. Hinted-handoff Put and Ack records are flushed and synchronized before their
in-memory state changes. Compaction synchronizes its replacement file, atomically renames it, and
synchronizes the parent directory; when a shared logical disk quota is full, the Ack append can use
Recovery admission and is followed by a shrinking-compaction attempt. Compaction failure leaves
retryable cleanup debt without changing an already-durable Ack. Edge-sync Put, Ack, and batched
expiry records are likewise flushed and synchronized before their corresponding in-memory state
changes. Under the shared budget, Ack and expiry records may use Recovery admission and are followed
by an exact, bounded shrinking-compaction attempt. Successful queue acceptance is therefore
crash-durable local queue state, but it is not a durable-upload guarantee: a replayed entry is
removed after any valid upstream acknowledgement, including `Volatile`, and a still-pending entry
can expire under the configured pre-ack retention.

Cluster and standalone edge-accept dedupe completion markers are synchronized before a marker
commit succeeds. Under a shared local-disk budget the append also synchronizes the parent directory,
and compaction publishes an exactly bounded, synchronized atomic replacement. A marker failure does
not undo primary rows or sidecars that already committed; the internal response discloses that
partial progress and keeps the completion in memory for same-process replay.

Rollup policies and their checkpoint/invalidation state form an ordered two-file persistence
boundary. Both complete replacements are admitted and synchronized before publication. The
invalidating state is published first, so a crash can expose the predecessor pair, predecessor
policies with more-conservative candidate state, or the complete candidate pair; it cannot expose a
new policy with reusable predecessor checkpoints. A partial or ambiguous publication fences later
policy changes, checkpoint writes, delete-invalidation updates, and materialization until storage is
reopened and the durable pair is reloaded. If both files are proven durable but final cleanup or
accounting reconciliation fails, the policy change remains committed and the failure is recorded as
cleanup debt rather than returned as a false rejection.

Post-flush retention and tiering use a leased two-phase replacement marker under
`.post-flush-replacements/` ([ADR 0004](adr/0004-post-flush-segment-replacement.md)). A durable
`Prepared` marker keeps exact sources authoritative and makes published outputs rollback-owned. A
durable `Committing` marker is the commit point: outputs are never rolled back, the catalog is
converged idempotently, and exact sources are retired before the marker is removed. Startup finishes
this protocol before inventory discovery. Ordinary compaction, snapshot export, dirty inventory
scan, and flush recovery metadata scan are fenced while a marker remains. Marker absence is not
reported as finalized until its parent is synchronized.

Tombstone manifests can span the numeric and blob lanes plus configured tier roots on unrelated
filesystems. A parent-synchronized local coordinator records the exact lane identities, complete
previous/candidate manifest images, and candidate shard fingerprints. `Prepared` recovery rolls
back only exact transaction-owned shards; `Committing` recovery always rolls every lane forward.
After the commit decision, a complete shared manifest is published first as the compute-only
visibility anchor. An interruption before that anchor is indeterminate; after it is durable, a
later failure is committed recovery debt and cannot be returned as a false rejection. Startup and
catalog refresh recover before loading manifests, while compute-only readers remain read-only and
consume the remote anchor. One read-write process holds and identity-revalidates
`<object-store-root>/.tsink-writer.lock`; other read-write opens on that root are rejected. See
[ADR 0005](adr/0005-cross-filesystem-tombstone-transactions.md).

The experimental cluster control plane has a paired persistence boundary. A checkpoint candidate
contains both a schema-v2 consensus log with its required authoritative `checkpointState` and
restart-durable `steppedDownTerm`, and the separate control-state mirror. Both complete replacements
are staged under one shared `Cluster` disk reservation, then the log file and parent directory are
synchronized before the mirror is published and synchronized. Before consensus requires a
candidate, a typed quota, headroom, or maintenance-reserve admission failure leaves it unpublished
and can be returned as a definitive resource rejection. Other encode, staging, or publication
failures use the ordinary persistence-error contract (HTTP 503 on the control surfaces) rather than
being mislabeled as quota. After quorum or a leader commit makes the candidate required, any
pre-log persistence failure installs it in memory as pending durability and fences mutation; the
response is indeterminate rather than a misleading definitive rejection.

The two renames are ordered, not a single filesystem transaction. Only after the log replacement
and its parent-directory synchronization succeed is its embedded checkpoint authoritative. A later
mirror failure cannot truthfully be reported as an uncommitted mutation: the runtime installs that
candidate, reports a committed checkpoint pending, and fences subsequent control mutations until
the mirror can be rebuilt from the log using Recovery admission. An ambiguous rename or failed
parent synchronization is indeterminate and fenced, never classified as committed. A required
higher consensus term is likewise adopted in memory and fenced if its log publication fails. On
reopen, a valid v2 log repairs a stale, missing, or invalid mirror; legacy v1 logs have no embedded
checkpoint and still require a valid mirror for migration. If that mirror repair cannot complete,
startup keeps the v2 checkpoint live but opens the control runtime fenced with its checkpoint
pending. Recovery snapshots default a missing step-down term to zero and normal restore merges the
live and restored revocation floors; only an explicit `forceLocalLeader` restore clears it.

Authoritative mirror repair may recreate a missing mirror or grow a stale mirror at the logical
quota because the durable v2 checkpoint already accounts for the logical state being materialized.
It still reserves the complete temporary peak against physical free space and filesystem
headroom. If both files are durable but grouped finalization, owned-temp cleanup, or accounting
reconciliation fails, `cleanupDebt` records that post-commit work without fencing the pair. Cleanup
is retried before any separate fence repair. Control and cluster recovery-snapshot exports return
HTTP 503 `control_persistence_indeterminate` while authority is fenced or a mirror checkpoint is
pending, but cleanup-only debt remains exportable.

If a command is already quorum-committed but a commit-notice response reveals a higher term that
cannot yet be written to the log, the command returns successful degraded
`committed_persistence_pending`. The higher term and step-down floor take effect in memory,
leadership is fenced, and the required log-only candidate is retried. This is post-commit
persistence debt, not permission to retry the command. As a related crash-safe membership rule, an
Active leader must transfer leadership before another voter can commit that leader's leave.

## Offline snapshot restore

Restore is an offline operation: complete it before opening storage at the target. Both restore APIs
measure and validate the snapshot before destination mutation, reject resolved source/target overlap
in either direction, and cap the trusted source at 100,000 entries and descendant-directory depth
128. Static symlinks, Windows reparse points, and other non-file entries are rejected during
measurement and checked again during bounded copy. These are path-based checks, not a descriptor-
relative traversal guarantee; the caller must keep the source trusted and immutable so another
process cannot replace a validated namespace entry before it is opened.

`StorageBuilder::restore_from_snapshot_with_disk_budget` uses a caller-owned offline
`LocalDiskBudget` rooted above the strict-descendant target. Before creating target ancestry or a
staging tree, it reserves the measured logical file bytes plus one per-entry allowance for every
snapshot entry and missing target-parent directory. The allowance is the greater of the 4 KiB
policy floor and the destination filesystem's reported allocation unit. This is deliberately
conservative admission, not an exact physical-allocation assertion.

The staged copy synchronizes its files and directory before activation. Missing target ancestry is
created with its new parent links synchronized. If a target already exists, activation first moves
it to a distinct backup, synchronizes the parent, publishes the staged tree, and synchronizes the
parent again. A publication failure attempts to restore and synchronize the preceding target rather
than claiming that the replacement never became visible. After the managed operation returns, an
exclusive tree scan installs exact logical accounting before new admission resumes. A scan failure
is explicit and conservatively retains the full reservation; if activation had committed, the
result says that restore committed but accounting reconciliation failed.

Server restore requires a separate offline root and finite limit and retains that root's process
lease until listener drain and storage shutdown complete. Standalone restore, the compatibility
and capability-gated internal routes, local cluster-node targets, and the cluster restore report
share one coordinator. Cluster restore validates all local report/source/target overlaps before
the first data mutation. A report failure after data and control publication is explicit degraded
success (`reportPending`) because rolling back the already-restored cluster would be dishonest.
Remote peers must advertise `budgeted_restore_v1`; there is no legacy unbudgeted fallback.

The legacy `StorageBuilder::restore_from_snapshot` uses the same validation, finite traversal,
bounded copy, durable ancestry creation, and rollback-aware activation, but it remains
caller-unbudgeted. Its caller is responsible for providing enough logical and physical capacity.

## Capacity cleanup before write rejection

When rollback-safe flush staging or foreground WAL Growth admission receives a typed disk-capacity
rejection, the engine makes at most one reclaim-and-retry attempt. That reclaim plan can retire only
fully expired owned segment roots: it disables mixed-age rewrites and tier moves, and it never
selects unknown or host-created files. Finding no eligible reclaim, or receiving another typed
capacity rejection during cleanup, preserves the original rejection; an independent non-capacity
cleanup failure is reported in its own right. No WAL or new flush visibility is published merely
because cleanup ran; the normal publication and acknowledgement rules still apply to the single
retry.

## Platform boundary

`sync_all`/filesystem synchronization is only as strong as the operating system, filesystem, mount
configuration, virtualized storage stack, and device implementation beneath it. tsink does not
claim protection from faulty hardware, disabled barriers, volatile controller caches that ignore
flushes, or corruption outside the files covered by its checksums and recovery logic.

Crash-process and cross-filesystem durability verification remains a release-gate item. Until those
fixtures cover a platform, interpret `Durable` as the documented software operation contract rather
than an absolute hardware guarantee.
