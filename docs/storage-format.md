# Storage Format and Upgrade Policy

This document defines the compatibility contract for a read-write tsink data directory. It covers
the root format identity record, the one supported legacy migration path, and the behavior callers
can expect when an open or migration fails.

## Root manifest

Every data directory successfully opened by the current engine has a
`tsink-manifest.json` file at its root. The file is small (at most 16 KiB), versioned, checksummed,
and replaced atomically through a temporary file plus rename and parent-directory synchronization.

Manifest schema 1 records:

- magic `TSINK_DATA_DIRECTORY`;
- manifest schema version and CRC-32 over the canonical payload;
- current and minimum-reader storage format versions;
- the creating tsink version;
- the last tsink version that completed startup successfully;
- enabled format-affecting features;
- timestamp precision;
- immutable chunk point capacity; and
- immutable partition-window width in timestamp units.

For a newly initialized directory, `creating_tsink_version` is the package version that created the
manifest. For a migrated pre-manifest directory it is JSON `null`: the creating binary is
unknowable, so tsink does not invent a version. The field remains present. The
`last_successfully_opened_tsink_version` field is updated only after recovery, registry
persistence, resource reconciliation, and startup finalization all succeed.

The manifest applies to the core read-write `StorageBuilder` data path. In-memory and read-only
runtime modes do not create or update it.

## Supported formats

The current compatibility window is intentionally narrow:

| On-disk state | Support |
|---|---|
| Manifest schema 1, storage format 2 | Open normally after checksum, version, feature, and immutable-configuration validation. |
| Exact pre-manifest storage-format-2 directory | Supported through the read-only legacy identity checks below, then upgraded by installing schema 1 atomically. This path is tested with a directory written by the historical tsink 0.10.1 release commit. |
| Older manifest schema or older declared storage format | Not supported in place; export/import with a compatible older release is required. |
| Newer manifest schema or newer declared storage format | Refused without acquiring the data-path lock or modifying the directory. |
| Corrupt manifest | Refused without recovery mutation. |
| Missing manifest with unknown files or no definitive legacy identity | Refused; tsink does not guess from arbitrary names. |
| Empty or not-yet-created directory | Initialized atomically. |

The tested pre-manifest path first requires the exact bounded set of legacy root names and regular
file/directory types. It then requires at least one definitive, read-only format identity:

1. a valid bounded series-registry checkpoint or journal;
2. a framed WAL whose canonical first frame passes header, checksum, and codec validation, or its
   exact empty bootstrap state paired with a checksummed publication marker; or
3. a fully published v2 immutable segment with a canonical path and exact five regular files,
   whose manifest CRC/version/path identity, declared lengths, streaming file hashes, and bounded
   `series.bin` structural walk all validate. That walk materializes no registry strings, labels,
   or series: it admits the exact raw representation plus any declared decompressed
   representation and checks every dictionary/reference/table invariant in place.

All namespace scans and parser allocations have fixed format or startup-memory limits. Links,
non-canonical names, unknown legacy root entries, corrupt known records, and unsupported versions
fail before the process lock or manifest installation.

## Frozen compatibility evidence

Two separately named, immutable fixtures exercise the supported pre-manifest format-2 path:

- `tests/fixtures/storage-format-v2-tsink-0.10.1` was produced by package version 0.10.1 from
  local Git commit `00cc627df7b36ae1838f68da273c42949f0a5d52`, whose commit subject is
  `release version 0.10.1`. The exact commit was exported beneath `/tmp` with `git archive`, and
  the frozen external driver was compiled against that export with its locked dependencies. The
  historical writer never created a root manifest, so no identity file was removed. Local history
  contains no tag ref for this release; the evidence therefore claims the exact commit and package
  version, not a signed or annotated release artifact.
- `tests/fixtures/storage-format-v2-pre-manifest-v1` remains the format-focused fixture produced
  by the current v2 writer and made manifestless by its versioned manual generator. It is useful
  independent coverage of the current writer's pre-manifest layout, but is not cross-release
  evidence.

Both fixtures have checked-in provenance and an xxHash64/size inventory over every regular data
file. Tests verify the inventory before copying a fixture, never invoke a generator, and never
rewrite checked-in fixture bytes. Each copied database is opened through the public legacy path,
queried for its old numeric/blob/native-histogram data and WAL-only suffix, checked for persisted
tombstone visibility, written after manifest installation, closed, reopened, and checked for all
old and new data. The upgraded copy is then snapshotted and restored through the public APIs; the
test verifies byte-for-byte manifest preservation before strict-opening the restored directory and
checking the same old, tombstoned, histogram, WAL-recovered, and newly written data again.

Retention-policy metadata is not applicable to storage format 2: retention is runtime builder
configuration rather than persisted directory state. The fixtures disable enforcement and include
a named ordinary retained sample, but do not claim to test nonexistent policy metadata.
Reproduction details and the precise evidence boundary are in each fixture's `PROVENANCE.md`; the
historical driver is
`tests/fixture-generators/generate_storage_format_v2_tsink_0_10_1.rs`.

## Upgrade behavior

The only in-place upgrade currently supported is installing the root manifest for a validated
pre-manifest format-2 directory. It does not rewrite WAL frames, segments, registry state, or user
data. The manifest is staged and atomically renamed before normal recovery can mutate legacy
state.

If writing fails before rename, the old directory remains manifestless and can be retried. If
rename succeeds but synchronizing the parent directory reports an error, the open reports that
error even though the new manifest may be visible; a later open revalidates the complete
checksummed file. If later WAL, segment, or registry recovery fails, the already installed format
identity remains, but `last_successfully_opened_tsink_version` stays `null`; a retry validates the
same manifest and repeats strict recovery. Recovery never treats a partial temporary manifest as a
current format identity.

A supported legacy WAL that lacks `wal.published` has no smaller durable boundary to trust. Normal
recovery therefore treats every existing segment byte as published and streams the complete frame
prefix through header, sequence, size, checksum, and codec validation before creating the marker.
This check is independent of `WalReplayMode`; failure leaves the WAL namespace byte-for-byte
unchanged, although the already installed root identity may remain with its successful-open field
unset as described above.

`wal.published` has two accepted checksummed encodings:

| Encoding | Bytes | Fields |
|---|---:|---|
| Legacy `TSHW` | 24 | Magic, published high-water mark `H = (segment, frame)`, CRC-32. It carries no reset floor. |
| V2 `TSH2` | 40 | Magic, published high-water mark `H`, reset-through mark `R`, CRC-32. The record is invalid unless `R <= H`. |

All integer fields are little-endian. The CRC-32 covers every preceding byte in the record.
Ordinary publication uses `TSHW` until a reset floor exists. A reset first durably publishes
`TSH2` with
`H = R = max(last appended high-water mark, (active segment, 0))`; only then may it truncate the
active segment or remove older segments. Later commits keep the same `R`, advance `H`, and remain
encoded as `TSH2`.

Recovery derives an effective floor `F = max(clean persisted replay floor, R)`, with absent `R`
contributing no reset authorization. When `H > F`, logical segment IDs from `F.segment` through
`H.segment` must be contiguous and validation must reach the exact `H` frame in the boundary
segment. An empty or short boundary segment, or one whose first frame is above `H`, fails before
any suffix is truncated. The exact frame may be absent only when `F >= H`: either clean persisted
state or a valid checksummed `TSH2` reset floor then proves that history is no longer required.
Duplicate aliases and recognized link-like or non-regular segment paths also fail before recovery
mutation. Gaps below `F` are already checkpointed or reset, while an empty segment strictly above
`H` is outside the replay interval. After validation, recovery restores the runtime append and
durable high-water floors through `H`, preventing frame numbering or durability state from moving
backward.

`wal.published` must be a regular non-link file of exactly 24 or 40 bytes. The reader opens it
without following links where the platform supports that flag, verifies the opened file's identity
before and after its fixed-size read, and rejects oversized markers before allocation. Publication
removes only the stale `wal.published.tmp` directory entry without following a target, rejects a
directory at that name, creates the replacement exclusively with no-follow protection, checks its
identity before rename, and semantically rereads the installed marker after rename.

Migration can block while it performs bounded validation. In particular, segment-only identity
validation streams the declared immutable files to verify their hashes. There is currently no
progress callback: applications receive completion or a typed `StorageBuilder::build` error.

## Downgrades

Downgrade is not supported. Do not open a migrated or current directory with an older tsink binary:
older binaries do not participate in this manifest contract and may not understand current
format-affecting features.

## Space and backups

Manifest installation needs temporary headroom for one small staged copy (no more than 16 KiB),
plus filesystem metadata and the existing local-disk recovery reserve. It does not require a
second copy of the database.

Because the supported migration only adds an atomic identity record, an offline backup is
recommended but not required by the engine. Operators should take a verified filesystem backup
before any upgrade when rollback to an older binary is an operational requirement. Restore into a
different directory; downgrade-in-place is not a rollback mechanism.

## Snapshots and restore

A successful snapshot copies the already validated root manifest byte-for-byte into the snapshot
staging directory. Restore preserves that file byte-for-byte. Opening the restored directory then
runs the same checksum, version, feature, and immutable-configuration checks as any other open.

Snapshot creation does not rewrite `creating_tsink_version` or
`last_successfully_opened_tsink_version`. A later successful open of the restored directory may
atomically update only the last-successful-open field.

## Offline inspection and explicit salvage

`tsink-inspect` provides the bounded machine-readable path for diagnosing a directory without
opening `Storage`, acquiring `.tsink.lock`, replaying WAL, migrating, compacting, enforcing
retention, or rewriting metadata:

```console
tsink-inspect inspect /srv/tsink/data > inspection.json
```

The JSON report identifies the root format, manifest fields and checksum state, every admitted
canonical WAL and persisted segment, WAL publication metadata, segment-file hashes, local catalog
references, the bounded series-registry snapshot decode, orphan or missing segments, stable
findings, discarded byte/frame ranges, exact work counters, and any bound that prevented
completion. `health: "clean"` is trustworthy only when `completeness.complete` is also `true`.
Exit status is zero only for a clean inspection; findings, an incomplete report, and invocation
failures return status 2 and still emit JSON.

Every inspector limit is finite and configurable on the command line. The defaults bound namespace
entries/depth and modeled retained namespace bytes, bytes read and hashed, retained report/path
data, findings, WAL frames and modeled per-frame decode memory, WAL files, persisted segments, and
catalog JSON bytes. Report paths reversibly percent-encode opaque and JSON-reserved bytes; a
truncated path carries an explicit `%TRUNCATED` marker and makes the report incomplete. On Unix,
inspection pins the root and opens every requested-root, directory, and regular-file component
handle-relative with no-follow semantics; a requested path containing a symbolic-link component is
refused, and discovered links are reported without traversal. Inspection is conservatively refused
on other platforms until equivalent primitives are implemented. This strict recovery-tool
contract also rejects macOS `/var/...` and `/tmp/...` spellings; pass their physical
`/private/var/...` or `/private/tmp/...` spelling instead. Registry decoding has a
separate modeled-memory bound covering stored and decoded bytes plus the production retention
factor. Files are read only to their admitted length, probed for concurrent growth, and revalidated
for length and identity. Run the tool against an offline directory or a storage-consistent
snapshot: it detects many concurrent changes and fails closed, but it is not an atomic snapshot
mechanism.

Salvage is a separate, deliberately narrow destination-only operation:

```console
tsink-inspect salvage /srv/tsink/damaged /srv/tsink/recovered \
  --source-is-offline-and-immutable \
  --max-copy-entries 100000 \
  --max-copy-bytes 8589934592 > salvage.json
```

It is accepted only when a complete inspection proves all of the following:

- the manifest is the current supported format and its checksum is valid;
- the WAL publication marker is present and valid;
- the production-decoded registry snapshot is empty;
- there are no persisted segments, catalogs, tombstones, recovery state, or server/cluster
  auxiliary objects;
- no recognized recovery-critical auxiliary namespace lacks a bounded format validator;
- every finding is one of the recognized WAL corrupt-tail, mid-log-corruption, or missing
  publication-boundary findings; and
- at least one exact corrupt WAL range was identified.

WAL frame sequences must be globally consecutive across canonical segment files. A nonzero
publication boundary `H` must be observed at a samples-frame logical commit boundary unless the
effective floor covers it. A verified `TSH2` marker supplies its own authoritative reset
evidence through `R`; a later commit with `H > R` still requires the exact `H` frame unless clean
persisted state covers it. Legacy `TSHW` supplies no reset evidence, so an absent legacy boundary
is covered only when a clean persisted segment manifest reports an equal or later WAL high-water
mark. A segment contributes that floor only after its manifest checksum, identity, complete
referenced-file set, lengths, and hashes all verify; corrupt, missing, or incomplete segments
cannot mask absent WAL history. Inspection reports independently decoded nonempty WAL as
replay-semantics-unverified until production replay is modeled.

Salvage v1 intentionally retains **no WAL frames**. It rewrites the first canonical WAL file to
zero bytes, publishes `TSH2` with `H = R = (0, 0)`, omits every later WAL file, and emits
`wal.salvage_v1_full_reset` ranges covering every source WAL byte. This is explicit data loss, not
a best-effort attempt to preserve a structurally plausible prefix. The empty decoded registry and
empty retained WAL make cross-artifact series identity consistency provable.

The destination must be absent, have a real existing parent, and not overlap the source. The caller
must pass `--source-is-offline-and-immutable`, attesting that every writer is stopped for the whole
operation. Salvage is currently implemented only on Unix, where source traversal, staging writes,
and publication use handle-relative no-follow operations. Other platforms refuse the action.
Every ordinary file copy enforces its inspected length and applies permissions through the opened
file descriptor. The staging tree is synchronized and inspected. After the pre-publication hook,
every retained file is compared byte-for-byte with its source or generated plan, the complete tree
is synchronized again, and its namespace and exact bytes are reverified immediately before an
atomic handle-relative no-replace rename and parent synchronization. Publication then reattests
the retained parent and requested destination identities, rediscovers the full tree through the
retained destination handle, and repeats the namespace-generation and exact-byte checks. A late
parent displacement or content change is therefore reported as a visible retained publication,
not as success.

As with other same-user filesystem transactions, these checks do not turn an actively hostile
same-UID namespace into a kernel transaction: another process with permission to rename the parent
or modify the staging tree could race after any final check. The destination namespace must remain
under the operator's exclusive control for the operation, in addition to the required immutable
source attestation.

The successful JSON report records copied entries/bytes, the zero-frame WAL high-water mark, every
discarded range, wholly omitted WAL paths, operational path dispositions, bounded outer-plus-
embedded report retention, and the clean pre-publication inspection. The same recovery evidence is
persisted as `tsink-salvage-report.json` inside the atomic destination, so a broken stdout pipe
does not lose the success record. The inspector structurally cross-checks this self-reported
evidence against the empty WAL and rewritten marker, including canonical WAL paths, contained
ranges, a unique full-reset record for each represented segment, the exact unique omitted-path
set, and unique canonical path dispositions; it is not a cryptographic authenticity record.
Failure never overwrites a raced or existing destination.
Populated failed staging trees—and a destination already made visible if parent synchronization
fails—are retained for manual inspection rather than recursively deleted by pathname. The error
includes the retained path.

The current narrow salvage policy also refuses nonempty registry snapshots, every persisted
segment, incremental registry journals and every catalog, tombstone state and transactions,
post-flush replacement state, rollup state, stale WAL marker temporaries, and server/cluster-owned
auxiliary stores. The tool never copies unvalidated recovery-critical bytes and calls the result
clean.

There is no in-place salvage mode. `StorageBuilder::with_wal_replay_mode(WalReplayMode::Salvage)`
does not bypass the strict published-prefix validation performed by persistent open. The damaged
source is never opened for writing and remains the operator's backup/evidence. Verify the report
and the recovered destination before redirecting an application to it; discarded ranges represent
explicit data loss and cannot be reconstructed by this tool.
