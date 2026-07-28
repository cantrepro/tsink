# Durability contract

This document describes what a successful tsink write means at the API boundary. It is deliberately
conservative: an acknowledgement names the weakest guarantee established before the call returns;
it is not a promise about hardware or filesystem behavior beyond the operations described here.

The canonical API is `Storage::write_batch`. `Storage::insert_rows_with_result` exposes the same
batch-level durability for the compatibility write path. See
[`ADR 0001`](adr/0001-write-contract.md) for acceptance, rollback, and indexed-outcome semantics.
The deterministic injection coverage for each storage boundary is listed in
[the durability failpoint matrix](durability-failpoints.md).

## Acknowledgement levels

| Level | Meaning when returned |
|---|---|
| `Volatile` | The accepted rows are visible to current readers. No crash-recovery log guarantee applies to the complete write. |
| `Appended` | The logical write is committed in the WAL, but the WAL has not necessarily been synchronized since that append. A process, kernel, power, or storage failure may lose it. |
| `Durable` | The write call completed the synchronization required by the configured core WAL path before returning. This is the strongest software guarantee tsink exposes, but it cannot override storage hardware, mount-option, controller-cache, or filesystem guarantees. |

For a best-effort batch, the result reports the weakest level among accepted rows. A result with no
accepted rows has no canonical acknowledgement.

## Core storage matrix

| Storage configuration | Visibility to current readers | Normal non-empty acknowledgement | Clean close | Abrupt process termination | Power or host loss |
|---|---|---|---|---|---|
| In-memory; no data path | After the complete atomic in-memory publication, before return | `Volatile` | No persistent copy exists | Accepted rows are lost | Accepted rows are lost |
| Data path; WAL disabled | After the complete atomic in-memory publication, before return | `Volatile` | `close()` publishes pending rows as synchronized segment state or returns the failure | Only rows already published as segments are intended to survive | No write-time persistence guarantee; already-published segments have the platform-qualified file/directory guarantee below |
| WAL `Periodic(interval)` | After in-memory application and logical WAL publication, before return | Usually `Appended`; it is `Durable` if that append performs the elapsed-interval sync or synchronized segment publication has already advanced through its high-water mark | `close()` publishes pending rows as synchronized segment state, checkpoints recovery metadata, and returns any failure | Every returned `Durable` write is intended to replay; an `Appended` write may or may not replay | `Durable` writes have the platform-qualified guarantee below; `Appended` writes remain inside a loss window |
| WAL `PerAppend` | After in-memory application and logical WAL publication, before return | `Durable` after the WAL frames and publish boundary are synchronized | `close()` also publishes pending segment state, checkpoints recovery metadata, and returns any failure | Recovery is intended to restore every acknowledged non-empty logical batch | Intended to survive when the platform honors the synchronization operations; no stronger hardware claim is made |
| Compute-only runtime | Writes are rejected; queries observe the last successfully installed catalog generation | Not applicable | No writable local state | Locally cached process state is lost and the remote catalog is loaded again | Not applicable to local writes |

`Periodic` does not currently own an autonomous timer that fsyncs an idle WAL. The interval is
checked by later appends and lifecycle work, so an idle process can retain an `Appended` high-water
mark until another write or close.

An empty compatibility write is a durable no-op because it creates no state to lose. A canonical
empty batch instead returns zero outcomes and no acknowledgement. Lifecycle and compute-only checks
still run in both cases.

## Exact core synchronization operations

For a WAL-backed write, tsink performs these operations in order:

1. Encode the series-definition and sample frames into the active WAL segment and flush the
   `BufWriter`, making the bytes available to the operating system.
2. In `PerAppend`, or when a `Periodic` append observes that its interval has elapsed, call
   `sync_data` on the active WAL segment. An append that does not take this step cannot be
   acknowledged as `Durable`.
3. Write the logical commit boundary to `wal/wal.published.tmp`. On the durable path, call
   `sync_data` on that temporary file. Rename it over `wal/wal.published`, then synchronize the
   `wal/` directory on platforms where directory-handle synchronization is supported.
4. Only after the boundary publication is attempted does the write expose the corresponding WAL
   high-water mark and return its acknowledgement. Recovery discards a syntactically valid WAL
   suffix beyond the last published boundary rather than treating an uncommitted frame as a later
   write.

Creating or rotating a WAL segment synchronizes the `wal/` directory, and rotation synchronizes the
previous active segment before switching files. `Periodic(interval)` is append-driven: no timer
wakes solely to synchronize an idle WAL. Its loss window can therefore exceed `interval` when no
later append occurs. A successful segment flush or clean close can make earlier data durable without
changing the acknowledgement already returned to its caller.

Segment publication writes and calls `sync_all` on temporary
`chunks.bin`, `chunk_index.bin`, `series.bin`, `postings.bin`, and `manifest.bin` files. It renames
the completed staging directory into the lane's `segments/L*/` namespace, then synchronizes the
published segment directory and its level directory. Recovery metadata such as
`series_index.bin`, incremental registry state, registry catalogs, tombstones, and rollup state
uses synchronized temporary-file replacement followed by parent-directory synchronization.
The manifest is published last within each segment so recovery never treats a partially encoded
staging directory as a complete segment.

WAL reset first flushes and synchronizes the active WAL file, installs and synchronizes its empty
replacement, removes older WAL segment files, synchronizes `wal/`, and atomically replaces the
published-boundary marker with its file and directory synchronization enabled. It occurs only after
the corresponding segment and recovery-metadata publication has committed.

On Linux, macOS, and other non-Windows targets, directory synchronization opens the directory and
calls `sync_all`. The current Windows implementation synchronizes regular files but treats
directory-handle synchronization as a no-op because the Rust file API used here does not support
flushing directory handles. Consequently, the strongest Windows claim excludes a guarantee that a
just-created or just-renamed directory entry survives sudden power loss.

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
the durability path degraded for observability. It retains the complete WAL prefix instead of
truncating against already-advanced sequence state: the preceding durable marker may ignore that
prefix, a marker replacement whose outcome was ambiguous may expose it, or a later successful
marker may include it. All are valid outcomes for `Volatile`; callers must not reinterpret it as
`Appended` or `Durable`.

## Background and close failures

A WAL append, flush, or required `sync_data` failure occurs before the staged rows are published to
readers. The write returns an error and truncates the attempted WAL suffix back to its preceding
boundary; the old publish marker remains authoritative on restart. If rollback itself fails, the
combined error is surfaced and the old publish boundary still prevents the attempted suffix from
being intentionally replayed. A failure while publishing the boundary after in-memory application
is different: the rows are already visible, so the core returns successful `Volatile`, records a
maintenance error, and makes no crash-recovery claim for that batch.

A segment-file, segment-directory, or recovery-metadata synchronization failure prevents that
flush from advancing the durable WAL high-water mark or trimming the protected WAL prefix. The
existing in-memory/WAL state remains the recovery source where possible, and the failure is exposed
through maintenance health and the lifecycle result rather than being converted into a stronger
write acknowledgement.

The built-in engine handles a background persistence failure according to
`with_background_fail_fast`:

- when enabled, it records the worker and error, sets the fail-fast health fence, stops that worker,
  and rejects subsequent writes as degraded;
- when disabled, it records degraded health but leaves writes enabled and lets the worker retry on
  later wakes. Each foreground write still receives only the acknowledgement established by its own
  WAL path.

A successful `close()` waits for owned lifecycle work and returns only after its required
flush/synchronization work succeeds. A failed close is not a durability acknowledgement and must be
handled by the embedder.

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

## Online snapshot export

`Storage::snapshot` is an online, point-in-time export. It first fences background maintenance,
drains every write permit, and takes the rollup, compaction, and visibility publication fences
before copying. The copied registry is generated from the fenced in-memory registry rather than
from a possibly stale on-disk compatibility snapshot, and WAL copying occurs under the same fence,
so an acknowledged write is represented by either the copied persisted state or its copied WAL
prefix. The validated data-directory manifest is copied byte for byte.

The destination must not already exist and must resolve outside the managed data directory.
Before destination ancestry is created, every managed source subtree is opened component by
component without following links, measured through directory handles, and retained as a closed
identity manifest. Unix classifies entries with `fstatat(AT_SYMLINK_NOFOLLOW)` before opening only
directories or regular files. Windows holds ancestor/component handles without delete sharing.
The combined staged namespace, including generated manifest/registry files, is admitted against
restore's 100,000-entry limit; descendant depth is at most 128.

Each secure session is capped at 64 MiB of modeled retained state. Every simultaneously live source
session, staging manifest, verification manifest, requested-path re-attestation anchor, and
generated buffer shares one 128 MiB operation cap, admitted incrementally as manifests grow.
Registry snapshot encoding admits its output and cloned-series scratch before either proportional
allocation. The exact source identities are reused by the copy, and source and complete staged
trees are remeasured through their anchors before publication. Copying uses length-bounded streams
into a uniquely named sibling staging directory
created relative to a retained parent handle. Permission changes apply to the already-open
destination file, never its pathname. Every copied regular file is flushed and synchronized, every
copied directory is synchronized, and source growth, shrinkage, change-time changes, type changes,
or path replacement fail the operation.

Publication uses an atomic no-replace rename, so a destination created by another actor after
preflight is preserved rather than overwritten. Linux and Android use
`renameat2(RENAME_NOREPLACE)`, Apple platforms use `renamex_np(RENAME_EXCL)`, and Windows uses
`MoveFileExW` without replacement. Other Unix targets fail explicitly when that safe primitive is
not configured instead of using a racy check followed by an overwriting rename. A failure before
the rename never rescans the staging pathname to “bless” a replacement for cleanup. Cleanup first
verifies the complete captured tree and admits bounded scratch. Windows dispositions the same
exclusive `DELETE` handle whose identity was verified, closing the check/delete pathname window.
Portable Unix has no identity-conditioned unlink primitive, so it reports that limitation and
retains even a fully verified tree. Publication is relative to the retained parent and re-attests
the caller-requested parent pathname afterward. If the rename succeeds but
post-publication attestation or final parent synchronization fails, the visible destination is
retained and the error says its durability or requested-path reachability is indeterminate. It is
not recursively removed because another actor may already have created files below it. As
elsewhere, Windows currently lacks
directory-handle synchronization, so its claim is atomic visibility and regular-file
synchronization, not power-loss persistence of the new directory entry.

The staging namespace is private to the operation: callers and other same-identity processes must
not enumerate, rewrite, rename, or inject entries below `.tmp-tsink-snapshot-*` while a snapshot is
running. Closed file identities, no-follow checks, and whole-tree verification detect ordinary
pre-boundary replacements, but portable filesystems do not expose an atomic conditional rename by
source identity. On Windows, `MoveFileExW` also requires a narrow release of the staging-root
no-delete handle immediately before the move. A reported rename failure probes both the old and new
names for the expected root identity: visibility at the destination is classified as committed and
retained, while an exact source identity can carry cleanup ownership. Retained parent/ancestor
handles and post-move identity verification bound but do not eliminate a hostile same-UID race in
that interval. Unix canonicalization of an existing alias (for example `/var` to `/private/var`) is
compatibility normalization before the anchor is acquired, not a claim that a hostile actor cannot
mutate the alias during that initial resolution.

## Offline snapshot restore

Restore is an offline operation: complete it before opening storage at the target. Both restore APIs
perform retained-handle manifest and compatibility preflight before creating destination ancestry,
reject resolved source/target overlap in either direction, and cap the trusted source at 100,000
entries and descendant-directory depth 128. Static symlinks, Windows reparse points, and other
non-file entries are rejected during handle-anchored measurement and checked again during bounded
copy. No descriptor/handle is retained per entry: the finite manifest stores device/volume,
inode/file-index, change/last-write time, length, and type, then reopens each parent and child
relative to an anchor and compares that closed identity. Secure traversal manifests, staging
manifests, verification state, and generated copy buffers share the 128 MiB aggregate secure-copy
operation cap.

Before the existing target is inspected or moved, restore copies the snapshot into a private
validation sibling and opens that copy through the normal strict discovery, segment validation,
registry recovery, tombstone hydration, WAL replay, rollup loading, and catalog-loading paths.
Validation derives timestamp precision, chunk capacity, and partition duration from the snapshot
manifest and enables WAL only when the measured snapshot contains the canonical WAL directory. It
uses the finite `Server` profile (2 GiB accounted memory, 10 million series, 8 GiB WAL, and 256 GiB
local disk), with filesystem headroom and maintenance reserve set to zero. These production-open
limits are separate from the 128 MiB secure-copy cap and can intentionally reject a snapshot made
under larger custom or `ExpertUnlimited` limits. Validation requires non-degraded health and ends
through a non-persisting lifecycle: it does not run normal close/flush, checkpoint persistence,
retention deletion, or background workers.

`StorageBuilder::restore_from_snapshot_with_disk_budget` uses a caller-owned offline
`LocalDiskBudget` rooted above the strict-descendant target. Before creating target ancestry or a
staging tree, its staging term reserves
`2 * logical_file_bytes + (snapshot_entries + 2) * entry_allowance`: one copied tree, one
source-logical recovery scratch envelope, the snapshot entries, the validation lock, and one
possible atomic recovery scratch entry. The coordinator separately adds one allowance for every
missing target-parent directory. The allowance is the greater of the 4 KiB policy floor and the
destination filesystem's reported allocation unit. The validation and publication copies reuse
the same staging reservation sequentially. This is deliberately conservative admission, not an
exact physical-allocation assertion.

The staged copy synchronizes its files and directory before activation. Missing target ancestry is
created with its new parent links synchronized. The staging directory itself is create-exclusive.
If a target already exists, activation identity-checks and moves it to a distinct absent backup
relative to the retained parent, synchronizes that parent, publishes the staged tree into an absent
target, and synchronizes the parent again. Both renames use atomic no-replace primitives, so a
concurrently installed backup or target is preserved and makes restore fail rather than being
overwritten. On Windows, a reported failure from either replacement rename probes both names for
the exact expected identity, so a move that committed before the error is classified from the
observed namespace rather than retried blindly. A failure before staging publication attempts an
identity-attested backup-to-target rollback through the same parent anchor. Once staging is visible
as the target, failure never rolls it back or deletes it; the visible target and backup are reported
and retained.

After successful replacement publication, restore verifies the original target against its
retained pre-move manifest and removes that exact backup with handle-relative cleanup. A normal
populated-target restore therefore leaves no backup debt. If the backup name, identity, type, or
descendant set changed, exact cleanup refuses to adopt or delete the replacement; the visible
target and any surviving backup are retained and reported. A copy or attestation failure likewise
retains staging without path-only cleanup. After the managed operation returns, an exclusive tree
scan installs exact logical accounting before new admission resumes. A scan failure is explicit
and conservatively retains the full reservation; if activation had committed, the result says that
restore committed but accounting reconciliation failed.

Restore is offline for the containing namespace as well as the target: callers must exclude
same-identity processes that mutate the target, backup, or `.tmp-tsink-restore-*` entries during
the operation. Windows cleanup holds each exact `DELETE` handle through disposition. Unknown
entries observed at a cleanup boundary are retained, but portable Unix backup/validation cleanup
has no identity-conditioned unlink and is not safe against an actor racing inside the last
identity-check syscall window. Its pathname deletion therefore relies on the caller-enforced
offline namespace exclusion; retained handles and manifest verification do not replace that
precondition.

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
