# ADR 0001: Core batch write contract

- Status: Accepted
- Date: 2026-07-22

## Context

The built-in storage engine exposes the compatibility methods `Storage::insert_rows` and
`Storage::insert_rows_with_result` plus the canonical `Storage::write_batch` API. `AsyncStorage`
delegates each complete call to the same core methods on its write worker, and UniFFI mirrors the
canonical result for Python and other generated bindings. Older documentation described per-row
outcomes and silent rejection behavior before an indexed result actually existed.

This decision records the existing single-node core contract. Protocol adapters, routing, and
replication layers may add their own request-level behavior around a core write.

## Decision

### Intended acceptance boundary

A non-empty compatibility call or `WriteMode::Atomic` call is one all-or-error acceptance unit:

- `Ok` means every row in the batch was accepted and committed by the core engine.
- `Err(TsinkError)` should mean no row in that batch was committed. `insert_rows` propagates
  rejections; it does not intentionally or silently discard an invalid row while accepting the
  rest.
- WAL-backed recovery should observe the same logical boundary: the whole published batch or none
  of an unpublished/aborted batch.

Here, atomicity describes the completed call's acceptance and recovery boundary. It does not add a
new snapshot-isolation guarantee for reads racing a write that is still in progress.

### Pipeline and rollback boundary

The live write pipeline is intended to preserve that boundary and runs in this order:

1. **Resolve** validates metric and label syntax and resolves or provisionally creates series IDs.
2. **Prepare** validates per-series lane and value-family compatibility, retention and partition
   constraints, admission limits, and prepares encoded WAL payloads.
3. **Stage** persists the series-definition and samples payloads as an unpublished logical WAL
   write when the WAL is enabled.
4. **Apply** installs every point in the in-memory write buffers.
5. **Publish** commits the logical WAL boundary when present and establishes the acknowledgement
   returned to the caller.

Before **Apply**, errors roll back newly created series entries and any unpublished staged WAL
write. Validation, admission, and WAL-stage rejections return a batch error with none of the rows
installed or replayable; callers do not perform rollback themselves. Apply-time cleanup also
reverses provisional value-family and empty lane reservations.

During **Apply**, the engine locks every affected active shard in ascending order and performs all
fallible partition rotation and chunk encoding against staged active-series copies. It installs the
staged states and publishes their sealed chunks only after every affected shard succeeds. An
apply-time error therefore leaves the live active and sealed state unchanged before the outer
cleanup aborts the staged WAL write and provisional metadata. Regression coverage exercises a
later-shard codec failure both with and without the WAL and verifies in-memory, accounting, and
reopen state.

Active-head finalization used by explicit, background, and close-time flush is also exception-safe:
the live head is removed or reset only after encoding succeeds. A flush may make safe progress on
earlier heads before a later head fails, but accepted points remain query-visible and the completed
movement is reflected in memory accounting and flush metrics.

### Durability acknowledgement is batch-level

`insert_rows_with_result` returns one `WriteResult` for the entire successful batch, not one result
per row. Its `acknowledgement` records the guarantee established when the call returns:

- `Durable`: the configured core WAL synchronization contract completed before return; platform and
  hardware guarantees still bound crash survival.
- `Appended`: the batch is committed to the WAL but may still be lost before a later sync.
- `Volatile`: the batch is visible in memory without a complete WAL recovery guarantee.

For a non-empty built-in-engine write, the configured WAL mode influences but does not by itself
uniquely determine the result:

- With the WAL disabled, the normal acknowledgement is `Volatile`.
- `PerAppend` returns `Durable` when both the batch sync and logical WAL publication succeed.
- `Periodic(interval)` checks whether the interval has elapsed during an append. The append that
  performs a sync can return `Durable`; another append normally returns `Appended`. There is no
  autonomous timer that guarantees a sync in the absence of another write or lifecycle action.
- If logical WAL publication fails after in-memory apply, the engine records the degradation and
  returns a successful `Volatile` acknowledgement because it cannot claim WAL recovery for that
  batch. It preserves the complete appended prefix after the in-process sequence has advanced
  instead of truncating it from a destructor: the preceding marker may ignore the prefix, an
  ambiguously replaced marker may expose it, or a later marker may include it. Those are all
  permitted `Volatile` outcomes and none may be upgraded in the original response.

`insert_rows` has the same acceptance and error behavior but intentionally discards this durability
metadata after a successful write.

### Canonical indexed outcomes

`Storage::write_batch(rows, mode)` returns a `BatchWriteResult` with exactly one ordered
`RowWriteOutcome` for each input index. Expected validation, admission, and safe apply failures are
returned as structured `WriteRejectionCategory` values inside `Ok(BatchWriteResult)`. Configured
top-level row/input bounds and checked input-size overflow are the deliberate exception: they return
an outer `TsinkError` before allocating the very outcome vector the bound is meant to constrain,
and commit no rows. When pre-commit admission fails before the full write scratch lease exists, the
built-in engine separately admits the bounded rejection-result envelope before constructing
indexed outcomes. If even that response envelope cannot fit the configured memory budget, the
engine returns the outer memory error and commits no rows instead of allocating an unaccounted
result. The outer error also remains available when a backend cannot provide trustworthy canonical
outcomes, such as the default implementation on a legacy third-party backend.

- `WriteMode::Atomic` invokes the all-or-error pipeline once. Success marks every row accepted and
  returns one batch acknowledgement. Failure marks every index rejected, commits no input row, and
  returns no acknowledgement. The current compatibility error does not carry its causal input
  index, so `cause_index` is `None` for an atomic rejection.
- `WriteMode::BestEffort` deliberately creates one atomic write boundary per row in input order.
  Valid rows may commit around rejected rows, every rejection carries its input index, and the
  result acknowledgement is the weakest guarantee among accepted rows.
- Rejection messages are bounded diagnostics. Callers use `WriteRejectionCategory`, not message
  matching, for control flow.

Third-party `Storage` implementations must opt in to `write_batch`; its default returns
`UnsupportedOperation` rather than pretending a legacy write method has indexed or atomic
semantics. The built-in async facade queues the whole canonical call as one worker command. UniFFI
uses `u64` counts and indices while preserving modes, categories, outcomes, and the optional
acknowledgement.

### Empty batches

On an open read-write engine, an empty batch is a successful no-op. `insert_rows(&[])` returns
`Ok(())`, and `insert_rows_with_result(&[])` returns `WriteResult::durable()` regardless of WAL
configuration. The `Durable` acknowledgement means that there is no state to lose; an empty call
does not append a WAL record or mutate the write buffers. Normal lifecycle and runtime-mode checks
still apply before this no-op result.

On an open read-write engine, canonical empty calls in either mode return zero counts, no outcomes,
and `acknowledgement: None` because no row was accepted. Closed and compute-only lifecycle checks
still run. This intentionally leaves the compatibility empty acknowledgement unchanged.

## Known gaps

Atomic rejections report every rejected row but do not yet identify the one causal input index.
Principal HTTP adapters use canonical atomic writes, validate indexed results, expose the weakest
acknowledgement, and disclose known cross-component partial effects. Server metadata and exemplar
stores remain separate transactions. Experimental cluster routing validates each replica's atomic
result and aggregates acknowledgements, but it does not provide a cross-node all-or-none boundary;
failures that may have committed are explicitly indeterminate.

## Consequences

- Callers can treat a returned core write error as rejection of the entire batch.
- Callers that need indexed outcomes use `write_batch`; compatibility callers that only need
  durability metadata can continue using `insert_rows_with_result`.
- Synchronous, async, and UniFFI surfaces retain equivalent modes, categories, acknowledgement,
  rollback, and recovery behavior.
