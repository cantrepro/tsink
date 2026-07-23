# ADR 0004: Crash-safe post-flush segment replacement

- Status: Accepted
- Date: 2026-07-22

## Context

Post-flush retention and tiering can replace several immutable segment roots across the local hot
lane and configured warm or cold filesystems. The operation may publish copied or rewritten
outputs, update the live catalog, and retire multiple sources. Those renames cannot be one
filesystem transaction. Without a durable coordinator, interruption can expose both source and
output roots, lose an output after the catalog adopts it, or let compaction and inventory scans
interpret a half-finished replacement.

The local disk budget adds another indivisible boundary: a marker, every governed promotion, and
recovery rename entries need admission before their first mutation, while external tier roots are
outside the local byte quota. Recovery must still reconcile local accounting when an external
promotion failure removes a governed marker.

## Decision

### Coordinator and schema

The leased data-path root owns `.post-flush-replacements/`. Each transaction is one bounded JSON
marker with schema version, fixed-width identifier, phase, exact source records, exact output
records, and a tier-move count. A segment record contains only its lane, tier, and canonical
`segments/L[0-2]/seg-<16 lowercase hex>` relative path. Sources additionally record whether their
retirement contributes to the expired-segment metric.

Markers, source roots, staged outputs, final outputs, rollback roots, and retirement roots are
validated without following links. A complete segment has exactly the five format files, whose
full decode, checksum, manifest level, and segment identifier must agree. Unknown entries,
ambiguous configured roots, links or reparse points, noncanonical paths, oversized markers, excess
records, duplicate paths, and source/output overlap fail closed.

### Two phases

`Prepared` is durable before the first staging-to-final rename. It means sources remain
authoritative. Recovery validates every source, moves each visible marker-owned final to its
deterministic rollback sibling, validates all rollback roots before deleting them, and removes the
marker only after its absence is parent-synchronized.

After every final exists and validates, the marker is atomically rewritten as `Committing` and its
parent is synchronized. This phase change is the commit point. A failed or ambiguous rewrite is
resolved only by synchronizing and parsing the visible marker; no catalog or source mutation may
continue unless the intended Committing marker is proven durable.

`Committing` never rolls back an output. Runtime recovery idempotently converges the live catalog
to the marker's exact source/output delta, then retires visible sources to deterministic sibling
names. No recursive source deletion begins until every loader-visible source name is absent.
Startup has no live catalog, so it finishes source retirement before inventory discovery. The
marker is removed last, and a failed proof of durable absence remains recovery debt rather than a
successful finalization.

### Fencing and startup order

While any exact marker remains, ordinary compaction, snapshot creation, and dirty-catalog
inventory scans are fenced under the shared compaction gate. A flush holds that gate for its full
segment-publication transaction: before its final roots become discoverable, through verification
and recovery-metadata persistence, until the catalog visibility swap commits. The fence
synchronizes the marker directory before deciding it is empty, including after an unlink whose
earlier parent synchronization failed, and prevents compaction from retiring a staged flush root.

Only read-write startup holding the data-path process lease may recover or clean these
transactions. Before phase recovery it removes exact atomic-marker temporaries and exact rewrite
or copy staging directories, because markers never reference raw staging paths and this debris can
consume physical headroom required by recovery renames. Lookalikes and unknown entries are left
untouched. Phase recovery runs before generic orphan cleanup and before inventory loading.

### Capacity and accounting

Initial marker publication reserves its encoded payload, one marker entry allowance, and every
missing marker-parent allowance before creating the marker directory. Governed promotions and
missing final parents share one aggregate Maintenance reservation. Recovery source/output renames
share an aggregate Recovery reservation, bypassing logical quota but still enforcing physical
headroom.

Even when every promotion is external, the promotion operation keeps a zero-byte reconciled local
reservation so failure cleanup of the governed marker settles exact local accounting. Operations
inside aggregate reservations use unbudgeted filesystem primitives; the outer guard reconciles on
both success and error.

## Consequences

- A Prepared interruption preserves sources; a Committing interruption preserves outputs and
  converges source retirement.
- Segment readers never intentionally scan a transaction's mixed path set.
- Startup can safely recover cross-filesystem tier moves without treating cross-filesystem rename
  as atomic.
- Recovery may be delayed by physical headroom, corruption, or an unknown namespace entry, but it
  fails closed without deleting unproven data.
- Marker and per-entry allowances add temporary capacity cost to post-flush maintenance.
- The protocol coordinates filesystems through one leased local marker; it does not claim a
  hardware-atomic multi-filesystem transaction.
