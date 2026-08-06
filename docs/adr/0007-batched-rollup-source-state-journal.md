# ADR 0007: Batched rollup source-state journal

- Status: Accepted
- Date: 2026-08-06

## Context

Background rollup materialization must make a source's pending-dedup marker durable before
publishing materialized rows and make its replacement checkpoint durable before changing the
in-memory checkpoint. Rewriting the complete rollup state snapshot after every source made one
bounded page perform work proportional to all policies and sources. The first incremental journal
removed that full-state rewrite, but appending or replacing a shared active file still required
whole-root disk-accounting reconciliation after each durable event.

The replacement must preserve crash ordering and legacy replay while keeping event publication,
recovery discovery, compaction, cleanup, namespace work, and memory finite. Cleanup must own only
canonical journal names and must never recursively remove an unvalidated directory.

## Decision

New source-state events are stored beside the rollup snapshot in create-only batch generations:

```text
state-journal-batch-<generation-as-16-lowercase-hex>.d/
  event-<sequence-as-16-lowercase-hex>.bin
```

Each event file uses the existing checksummed version-1 journal frame and contains exactly one
source-state replacement. A materialization page opens a writer lazily on its first changed source
and reuses one batch for the page. Event publication writes and synchronizes an exact temporary,
renames it without replacement, and synchronizes the parent. Successful governed publication
settles its exact reservation without a full-root reconciliation. Failure or ambiguous settlement
performs one terminal reconciliation before returning.

The write order is:

1. Publish the pending-dedup event.
2. Update the in-memory pending state.
3. Publish materialized rows in bounded write batches.
4. Publish the completion/checkpoint event.
5. Update the in-memory checkpoint and remove the pending state.

Consequently, an interrupted write either has no output, has a replayable pending marker covering
possibly committed output, or has a replayable completion checkpoint. Page cursor state advances
only after the corresponding source replacements are durable.

One batch contains at most 1,024 events and 4 MiB of encoded payload. Replay accepts at most 1,024
logical generations. Discovery shares one 16,384-entry ceiling across the rollup directory and all
recognized batch children; unknown top-level entries count toward that ceiling but are not owned.
Inside a recognized batch, every child must be a contiguous canonical lowercase-hex event file.
Unknown, non-UTF-8, link-like, wrong-type, missing-sequence, and over-limit children fail closed
before cleanup or mutation.

The reader continues to accept the legacy `state-journal-active.bin` file and immutable
`state-journal-<generation>.bin` files. On rotation, a legacy active file is sealed before a new
batch is created. Generation allocation is monotonic and preflights all nonces needed for that
transition; exhaustion returns a typed unsupported-operation error before a rename.

Under namespace or generation pressure, a batch is packed into a same-generation immutable shadow
using Maintenance admission. The shadow is durable before batch cleanup. A batch-plus-shadow pair
is a recovery state, not two replay inputs: startup decodes both, requires exact event equality, and
only then removes the batch before applying journal state. A retry accepts an existing shadow only
when its decoded events are identical. Stale regular generations may be pruned at the exact
namespace ceiling, but a regular-generation atomic rewrite requires one free temporary-entry slot.
Once batches are packed, adjacent immutable generations retain the existing
publish-newer-before-remove-older compaction ordering.

Full snapshot publication absorbs the current journal state. Cleanup first preflights every
recognized regular generation and every batch child under the global namespace bound. A live batch
is durably renamed to the replay-inert
`.state-journal-cleanup-<generation-as-16-lowercase-hex>.d` name before any child is unlinked.
Execution holds one directory identity handle at a time, verifies the exact admitted children,
removes only those children, and removes the directory only when empty. Restart recognizes and
finishes an interrupted detached cleanup. All governed removals share one lazy Recovery
reservation and at most one terminal full-root reconciliation; definite no-op cleanup does not
scan.

Policy, checkpoint, pending-materialization, invalidation, and generation entries share the same
65,536-item / 64 MiB modeled state envelope. Startup seeds it from policies, admits each decoded
base-state item and journal replacement before growing the destination maps, and admits a missing
policy generation before insertion. Live journal publication serializes the visibility lock,
envelope counter, and affected maps; replacement accounting uses the larger of the predecessor and
candidate retained states until the event is durable. Journal discovery, frame decode, encoding,
publication, packing, cleanup planning, and terminal reconciliation also carry the configured
memory ceiling plus the retained state baseline. The policy/state JSON files retain their existing
64 MiB stored-file ceiling and now use a schema-equivalent streaming preflight before typed serde
materialization. Raw-buffer growth admits predecessor and successor allocations, decoded physical
records are charged in deterministic checkpoint/generation/pending/invalidation order, and typed
DTO plus destination-map growth is admitted before allocation. Duplicate-heavy payloads therefore
cannot hide behind later map replacement, while malformed JSON and magic/version errors retain
their existing precedence. Startup also charges its temporary active-policy-id tree, reuses that
role for delete-repair membership, drops it before repair persistence, and moves the policy vector
into the runtime without a second full clone.

For a new event, definite validation, memory, namespace, or quota rejection occurs before the
canonical no-replace rename and leaves the runtime retryable. Once that rename succeeds, a parent
sync or settlement failure is publication-ambiguous and fences further in-process rollup mutation
until reopen. Governed journal recovery, writer initialization, each event/compaction mutation, and
full cleanup take the shared managed-file mutation lock once at their outer boundary so retained
`LocalDiskBudget` handles cannot race the same namespace.

## Consequences

- Per-source success is proportional to the event being published and does not scan the complete
  data root. Directory discovery and pressure compaction remain bounded maintenance work.
- A crash can leave an empty batch, a batch plus identical shadow, an old immutable generation, or
  cleanup debt. Each state is replayable or retryable without treating an incomplete file as
  committed.
- Current binaries replay both legacy and batched journals. Older binaries do not understand batch
  directories; a downgrade can therefore resume from the last full snapshot and redo later rollup
  work. This ADR does not claim forward compatibility for the new journal layout.
- The configured journal-memory envelope is additive to retained rollup state. Policy/state JSON
  raw bytes, streaming charge traces, typed candidates, and destination maps use that same finite
  startup limit without changing the persisted schema or format-error precedence.
- Exact lowercase names define the owned namespace. Lookalike names are preserved, while malformed
  contents under an exact batch name require operator inspection rather than recursive deletion.
- Batch directories add small-file and parent-sync overhead. The tradeoff is deliberate: create-only
  publication gives a precise durability point and exact disk-budget settlement without a scan on
  every source event.
