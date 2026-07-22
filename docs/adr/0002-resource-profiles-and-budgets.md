# ADR 0002: Resource profiles and shared budget model

- Status: Accepted for staged implementation
- Date: 2026-07-22

## Context

`StorageBuilder` currently exposes individual write-side controls for accounted storage memory,
cardinality, WAL bytes, writer concurrency, write timeout, and active partition heads. Several of
those controls default to `usize::MAX`, and the built-in engine has no complete local-disk budget or
shared embedded-query budget. Server adapters add their own finite request and fan-out guardrails,
but those do not make a direct embedded query bounded.

Publishing named `Test`, `Embedded`, `Edge`, or `Server` profiles on top of only the existing knobs
would be misleading. A standard profile must not imply a hard process-memory, disk, or query bound
until the corresponding work is actually admitted and accounted. At the same time, profile work
needs a stable configuration and observability model so disk and query enforcement do not become
unrelated one-off limits.

## Decision

### Profile shape

The completed public design will expose these named profiles:

- `Test` — small deterministic budgets suitable for temporary databases and CI;
- `Embedded` — the default for an in-process application;
- `Edge` — lower memory, disk, concurrency, and idle-work budgets;
- `Server` — larger but still finite budgets for a dedicated process;
- `Custom(ResourceLimits)` — a fully specified set of limits;
- an explicitly named expert-only unlimited configuration.

Named profiles will not be added as nominal labels with unenforced fields. They become public only
when their memory, local-disk, WAL, cardinality, query, and concurrency values are finite and the
engine can report the effective values after build. Until then the existing default behavior stays
documented as legacy/unbounded rather than being relabeled as `Embedded`.

Profile numbers will be selected from reproducible measurements and tiny-limit boundary tests, not
from unreferenced estimates. The measurements, platform, workload, and accounted scope will be
recorded alongside the chosen values.

### Limit representation

Configuration distinguishes a finite value from explicit unlimited operation. Unlimited is not
represented by an accidental `usize::MAX` in the final public profile model. Conversion to legacy
internal sentinels happens only at the engine boundary while those sentinels remain in use.

The final `ResourceLimits` groups limits by ownership:

- storage memory and its accounted/estimated/excluded scope;
- local disk budget and reserved free-space/temporary-output headroom;
- WAL bytes and userspace buffer bytes;
- total and creation-rate cardinality plus identity-shape limits;
- query series, scanned samples, returned bytes/samples, intermediate memory, steps, pattern
  expansion, wall time, and concurrency;
- write/read workers, queues, maintenance, compaction, tier fetch, and rollup concurrency.

Server-only request-envelope limits remain server configuration, but the server must derive finite
defaults from its selected profile and may only tighten the shared core budget for a request.

### Deterministic overrides

The builder stores a selected base profile separately from field overrides. Explicit low-level
builder methods are overrides and win over the base profile regardless of call order. Selecting a
different profile changes only the base and does not erase explicit overrides. A dedicated method
is required to clear overrides and return to pure profile values.

`Custom(ResourceLimits)` is already a complete base and is validated like a named profile. Invalid
relationships fail during `build()` with actionable errors, including:

- reserved disk headroom greater than or equal to the disk budget;
- WAL budget greater than the local disk budget after reserved headroom;
- zero concurrency or zero query work limits;
- per-query memory greater than the shared query-memory budget;
- tier cutoffs outside global retention;
- a finite persistent-storage profile without a data path, unless the selected profile explicitly
  supports an in-memory-only mode.

### Inspectability

The built-in backend exposes one effective-limit snapshot after build. During staged implementation
the snapshot reports every already enforced control and uses `None` for a category with no enforced
limit. It must never substitute a profile target for an unimplemented check. As disk and query
budgets land, they join the same snapshot and observability surface.

The snapshot is preserved through the async facade and UniFFI/Python bindings. Third-party
`Storage` backends get a conservative default with unknown/unlimited optional fields rather than
fabricated finite values.

### Memory scope

The engine will continue to report memory by category and will classify each category as:

- **accounted** — measured and admitted against the storage or query budget;
- **estimated** — modeled from owned allocations where exact allocator capacity is unavailable;
- **excluded** — visible but not admitted, such as mapped virtual bytes or host/runtime overhead.

The profile memory value is not described as a hard RSS cap. Pending batches, WAL buffers,
decompression, query working sets, rollup state, and remote metadata must either become accounted
or remain explicitly listed as estimated/excluded.

### Disk and query ownership

A shared local-disk budget is owned by the storage instance. Writers reserve bytes before creating
or extending WAL, segment, index, tombstone, catalog, metadata, queue, or temporary-output files.
Reservations are released or converted to used bytes on every success and failure path. Startup
reconciles actual files without deleting unknown external files. Snapshot destinations outside the
data directory remain outside the database budget; destinations inside it are rejected until they
can be accounted consistently.

The embedded query budget is runtime-independent. It uses a synchronous cancellation/deadline token
and RAII reservations for query slots and intermediate memory; the async facade and HTTP server
adapt their cancellation mechanisms to the same core contract rather than introducing Tokio into
the root crate. Explicit pagination is distinct from a resource rejection: a direct query never
silently truncates because it reached a profile budget.

## Implementation sequence

1. Publish an inventory document and an effective snapshot for controls that are already enforced.
2. Complete memory-category classification and pressure-state observability.
3. Add shared local-disk accounting, reservations, headroom, startup reconciliation, and failure
   tests.
4. Add the shared query budget, deadline/cancellation token, reservations, and direct/HTTP mapping.
5. Measure and publish finite profile values, switch new builders to `Embedded`, add explicit expert
   unlimited configuration, and expose equivalent async/Python surfaces.

This sequence is an implementation dependency graph, not permission to mark Phase 2 complete
piecemeal. Phase 2 remains incomplete until every named profile is finite and the acceptance tests
in `GOAL.md` pass.

## Consequences

- Existing knobs remain usable while the complete profile model is built.
- No interim API claims disk, query, or total-process memory enforcement that does not exist.
- Adding a real finite default is an intentional behavioral change and will receive migration notes
  and measured values before release.
- The same effective snapshot becomes the source for Rust, async, Python, server status, and support
  tooling, avoiding divergent claims about active limits.
