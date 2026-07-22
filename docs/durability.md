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
source of truth for which rows committed.

If logical WAL publication fails after in-memory application, tsink cannot truthfully claim WAL
recovery for that batch. The current core returns a successful `Volatile` acknowledgement and marks
the durability path degraded for observability; callers must not reinterpret it as `Appended` or
`Durable`.

## Background and close failures

The built-in engine fences or reports background persistence failure according to
`with_background_fail_fast`. A successful `close()` waits for owned lifecycle work and returns only
after its required flush/synchronization work succeeds. A failed close is not a durability
acknowledgement and must be handled by the embedder.

A fail-fast fence remains `TsinkError::StorageShuttingDown` on the compatibility write API and is
reported as `WriteRejectionCategory::StorageDegraded` by the canonical API. Lifecycle close is
reported separately as `StorageClosed` in both APIs.

Write-time admission errors, I/O errors, closed storage, and safe atomic rollback are represented as
structured canonical rejections. An outer API error is reserved for a backend or failure that cannot
provide trustworthy indexed outcomes.

## Server sidecars and experimental cluster mode

Metric metadata and exemplars are maintained by server-side stores outside the core row/WAL
transaction. They stage a full replacement, synchronize the replacement file, rename it, and only
then publish the staged in-memory state. Their install path does not yet synchronize the parent
directory, so an HTTP envelope containing either sidecar is conservatively reported as `Volatile`,
even when its core rows received a stronger WAL acknowledgement.

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
in-memory state changes, although compaction does not yet synchronize the parent directory after
rename. Edge-sync queue records are flushed but are not currently synchronized with
`sync_data`/`sync_all`; queue acceptance therefore is not a crash-durable upload guarantee. A
successfully replayed edge entry is removed after any valid upstream acknowledgement, including
`Volatile`, and a still-pending entry can also expire under the configured pre-ack retention.

## Platform boundary

`sync_all`/filesystem synchronization is only as strong as the operating system, filesystem, mount
configuration, virtualized storage stack, and device implementation beneath it. tsink does not
claim protection from faulty hardware, disabled barriers, volatile controller caches that ignore
flushes, or corruption outside the files covered by its checksums and recovery logic.

Crash-process and cross-filesystem durability verification remains a release-gate item. Until those
fixtures cover a platform, interpret `Durable` as the documented software operation contract rather
than an absolute hardware guarantee.
