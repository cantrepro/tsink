# ADR 0005: Cross-filesystem tombstone transactions

- Status: Accepted
- Date: 2026-07-22

## Context

One logical delete can update tombstone manifests in the numeric lane, blob lane, and configured
hot, warm, or cold tier roots. Those roots may be on unrelated filesystems, so ordered manifest
renames cannot form one atomic filesystem transaction. A crash between renames could previously
leave different lanes at different delete histories. Loading and merging that state can expose a
delete in only part of the database, while rolling an uncertain candidate back can resurrect data
that was already durably deleted.

The protocol also has to coexist with finite local-disk and modeled-memory budgets, immutable
sharded tombstone files, startup orphan cleanup, snapshots, close, compaction, and compute-only
readers. Recovery must never infer ownership from a broad filename prefix or delete an unknown
host-created entry.

## Decision

Read-write storage uses one leased coordinator at
`<data_path>/.tombstone-transactions/active.bin`. The framed, checksummed record contains the exact
normalized lane set and, for every lane, its stable namespace identity, manifest identity,
previous manifest image, candidate manifest image, and length plus xxHash64 fingerprint for every
candidate shard. Paths, counts, encoded files, decoded allocations, and recovery-directory scans
have explicit bounds.

Only one tombstone transaction may be active for a storage instance. Callers serialize recovery
and publication with the storage writer, rollup, and visibility fences before deriving a
read-modify-write update.

One read-write process may own a configured shared object-store root. It holds an advisory lock on
`<object_store_root>/.tsink-writer.lock` for its complete lifetime, in addition to the local
data-path lease. Lock acquisition rejects a link-like configured root or lock entry, canonicalizes
legitimate platform ancestor aliases, and uses a no-follow open for the lock file. Before each
engine operation that can recover or mutate shared tombstones, segments,
or catalogs, the holder reopens the lock pathname without following links and proves that it still
names the originally locked file identity. This fences a stale holder if the pathname was renamed,
unlinked, or replaced. Compute-only processes do not acquire this lease and remain read-only.

The protocol has two durable phases:

1. Write every new immutable shard with create-new semantics, synchronize each file and its parent,
   then atomically publish and parent-synchronize a `Prepared` coordinator record. While this phase
   is authoritative, every lane must still match its recorded previous manifest. Recovery rolls
   the candidate back and removes only shards whose exact names and fingerprints belong to the
   record.
2. Revalidate the complete lane set, previous manifests, and candidate shards. Atomically rewrite
   and parent-synchronize the coordinator as `Committing`. This rewrite is the sole commit point.
   After it succeeds, candidate manifests are published and parent-synchronized lane by lane.
   Recovery always rolls a `Committing` transaction forward; it never removes a candidate merely
   because some lane still exposes the predecessor.

The coordinator is local and therefore invisible to compute-only readers. After the `Committing`
decision, the writer publishes one shared remote lane manifest before any local or secondary lane.
That manifest is the shared visibility anchor. Candidate tombstones are the monotonic union of the
predecessor and new ranges, so a compute-only reader that merges the tier manifests hides every
newly acknowledged delete as soon as the anchor is durable. An interruption before the anchor is
durable is indeterminate and is never acknowledged as committed. An interruption after the anchor
is durable is committed recovery debt even when local or other remote manifests still lag.

Finalization first proves that every lane durably exposes its exact candidate image. It then
settles disk accounting, removes and synchronizes the coordinator, and reclaims exact superseded
shards. A failure after the commit point is committed recovery or cleanup debt, not a definitive
delete rejection. A failure whose coordinator rename or parent synchronization is ambiguous is
reported as indeterminate and leaves enough state for restart recovery.

Startup synchronizes the coordinator parent before interpreting a visible phase, recovers the
transaction before orphan-shard cleanup or tombstone loading, and reloads the authoritative merged
index after a committed roll-forward. Runtime recovery uses the same path before a new delete or
recovery snapshot. Snapshot, close, and other tombstone writers share the serialization boundary.
Compute-only storage never creates or repairs coordinator state.

Catalog refresh recovers a pending read-write transaction before it reads tombstone manifests.
Publication swaps the authoritative tombstone map before it installs segment additions or removals;
if later catalog work fails, readers retain the conservative delete state. A failed visibility-
summary rebuild clears affected summaries instead of restoring stale cache entries. Compaction
reads the live authoritative tombstone map when planning and executing work, so reclamation cannot
silently use a startup-era snapshot.

All coordinator, manifest, and shard reads reject link-like or special entries, use no-follow
opens where supported, and read an admitted exact length with a fixed one-byte growth probe. Manual
preflight validates allocation-driving lengths and counts before bincode decoding; legacy JSON is
byte-bounded and receives its conservative decode admission before serde runs. Tombstone decode,
merge, publication, visibility-summary rebuild, and recovery scratch are charged through one RAII
staging reservation. Recovery-owned namespaces are fully enumerated only up to a fixed count-all
entry limit before any discovered entry is deleted; unknown and lookalike names count toward the
work bound but are never selected for removal.

## Consequences

- A crash can expose a durable `Prepared` rollback decision or a durable `Committing` roll-forward
  decision. It cannot require guessing which lane manifest should win.
- A shared object-store root supports any number of compute-only readers but at most one
  cooperating read-write owner. Deployments that need multiple writers must add an external
  consensus/fencing protocol; pointing distinct local data paths at one root is rejected.
- A successful tiered delete is already visible through a durable remote anchor. Writer-local
  recovery debt cannot make a compute-only reader resurrect that acknowledged delete.
- The coordinator is a small write-amplification and synchronization cost on durable deletes. Data
  safety and an honest result take priority over delete throughput.
- Cross-filesystem atomicity is implemented as logged recovery, not claimed as a multi-filesystem
  rename guarantee. It remains subject to the platform synchronization limits documented in
  `docs/durability.md`.
- The coordinator is transitional state, but it is a downgrade boundary while present. An older
  binary that does not understand this protocol must not open the directory until an upgraded
  binary has completed recovery and removed `active.bin`; otherwise it could read a partially
  published lane set.
- Legacy tombstone manifests remain readable as recorded predecessor images. New candidates use
  the validated sharded format, and recovery never silently converts corrupt or unrecognized
  bytes.
