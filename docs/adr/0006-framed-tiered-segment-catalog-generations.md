# ADR 0006: Framed tiered segment-catalog generations

- Status: Accepted
- Date: 2026-07-26

## Context

The shared tiered segment catalog was a complete v2 JSON file. A periodic compute-only refresh had
to read, decode, plan, and publish that complete snapshot in one operation. Falling back from a
missing or invalid catalog to a physical tier scan was also unsafe for finite runtime maintenance:
the scan had no stable commit point and could rediscover an orphan or retired segment.

The replacement must give finite readers explicit item, byte, path, and namespace bounds without
making an older binary silently consume a stale file. Existing deployments and startup recovery
still need the v2 file during the compatibility interval.

## Decision

The object-store root has three side-by-side catalog entries:

```text
segment_catalog.json
segment_catalog.current
segment_catalog.d/
  catalog-<generation-as-16-lowercase-hex>.bin
```

`segment_catalog.json` remains the version-2 JSON compatibility snapshot. This release does not
repurpose or remove it. `segment_catalog.current` is the sole v3 commit record. It is an exact
44-byte binary pointer containing magic, version, generation, declared entry count, generation
length, generation xxHash64, and CRC32. A reader never guesses the current generation from a
directory listing.

Each immutable generation has an exact 28-byte checksummed header followed by one checksummed frame
per segment. A frame records the lane, tier, level, segment identity, manifest counts and timestamp
bounds, WAL high-water mark, and canonical relative path. Entries are strictly ordered by
lane/level/segment identity. Duplicate identities, unknown enum or flag values, unsupported levels,
noncanonical paths, invalid timestamp ranges, truncation, trailing bytes, count mismatches, and
checksum/hash mismatches are corruption.

The format admits at most 16,384 entries, a 256-byte relative path, a 336-byte frame, and a
5,505,052-byte generation. Pointer and file lengths are validated before allocation. Pointer and
generation files must be regular no-follow entries, and `segment_catalog.d` must be a regular
no-follow directory. Full reconstructed roots have a separate 256 KiB ceiling. Unknown generation
namespace entries count toward the 16,384-entry cleanup bound but are never deleted.

The leased read-write owner publishes in this order:

1. Stream and synchronize the optional local v2 compatibility snapshot to its exact stage, then
   atomically publish it.
2. Stream and synchronize a new immutable v3 generation.
3. Stream and synchronize the shared v2 JSON compatibility snapshot to its exact stage, then
   atomically publish it.
4. Atomically replace and synchronize the v3 pointer.
5. Retry hard-bounded cleanup of recognized noncurrent generation files.

The pointer rename is the v3 commit point. A failure before it leaves an unreferenced generation or
a newer v2 compatibility image but cannot make a finite reader consume an incomplete v3 image.
Post-flush replacement markers already keep source roots until catalog publication succeeds; source
retirement therefore remains after the v3 commit point. Cleanup failure is observable retryable
debt and never rolls the pointer back.

With finite maintenance limits, the writer retains a process-local ordered persisted-root cursor
and advances the scan and three encoders in item/byte-charged passes. It holds no file handle across
wakes. Retained entries, paths, and scratch are admitted to the global storage-memory budget and
reported as `remote_catalog_staging_bytes`. A visibility-generation change removes unpublished
exact stages/generation and restarts from the latest snapshot. Preparation admits its complete
hard-bounded generation-namespace dependency before creating the namespace or cleaning crash
orphans. Startup one-shot recovery removes only the two deterministic owned stage names.
`ExpertUnlimited` keeps the complete one-shot path.

Finite compute-only refresh requires the v3 pointer. A missing pointer, including a valid v2-only
store, returns a structured unsupported-operation error, records the failed attempt and backoff,
keeps the last visible catalog, and never scans tier roots. A corrupt pointer or generation also
fails closed. Startup and explicit `ExpertUnlimited` runtime behavior retain the v2/physical-scan
compatibility path.

A finite reader pins the exact pointer tuple, opens/seeks/reads/closes the generation on every pass,
and stages no live mutation until every frame and the complete file hash validate. It then applies
bounded additions before bounded removals under the visibility-generation fence. Success is
recorded only after a distinct terminal removal probe and a terminal pointer reread. Segment
catalog v3 does not invoke the legacy whole-map tombstone refresh; finite remote tombstone
publication is a separate protocol.

If the pointer changes during validation or application, the reader discards its process-local
cursor and reconciles the newer pointer. Applied additions may remain as a conservative union until
the newer cycle removes them. The reader does **not** promise to finish an older pinned generation:
there is no shared reader lease keeping that generation's source roots alive after a newer commit.
Old generation files may therefore be reclaimed after the pointer swap. Continuous writer
publication can continuously restart a slower finite reader and delay convergence. Deployments
must set a refresh interval and maintenance envelope that allow progress between publications; a
future stale-reader lease/epoch protocol is required to guarantee convergence under unbounded
publication churn.

## Consequences

- Older binaries continue reading the atomically refreshed v2 JSON file. New finite readers use
  only v3 and cannot silently fall back to an unsafe scan.
- A compute-only process holds no generation file handle between passes, so Windows sharing
  violations during cleanup are temporary and retried by later publications.
- The finite compute-only reader admits its generation path, one-frame page/decode peak, retained
  cursor, and staged root map to the global storage-memory budget. The public
  `remote_catalog_staging_bytes` component remains live across wakes and returns to zero on pointer
  replacement, error/invalidation, completion, reset, and close.
- Finite read-write publication pages its persisted-root scan and v3/v2/pointer construction,
  retaining only a hard-capped ordered snapshot plus page scratch in
  `remote_catalog_staging_bytes` through pointer handoff. Finite compute-only application also
  preadmits and capacity-reconciles each one-root load/transition/visibility peak through that
  component. Complete-inventory compatibility transitions and `ExpertUnlimited` retain their
  caller-materialized input.
- Generation cleanup removes only exact canonical regular filenames. It leaves unknown entries,
  symlinks, directories, and over-limit namespaces untouched except for reporting the bounded
  error.
- Snapshot/export of the external object-store root is outside the local data snapshot. Any future
  object-store snapshot protocol must copy the referenced generation before copying the pointer
  last.
