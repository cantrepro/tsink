# Rollups & downsampling

Rollups let you define **persistent downsampling policies** that continuously materialise pre-aggregated copies of your raw metrics. Instead of aggregating millions of raw points on every query, queries that match an active policy read the pre-computed buckets directly — with a live tail computed on-the-fly for any data that has not yet been materialised.

---

## Contents

- [Concepts](#concepts)
- [Defining a policy](#defining-a-policy)
- [Applying policies at runtime](#applying-policies-at-runtime)
- [Triggering and scheduling](#triggering-and-scheduling)
- [Query-time substitution](#query-time-substitution)
- [Observability](#observability)
- [Crash safety and durability](#crash-safety-and-durability)
- [Invalidation](#invalidation)
- [HTTP API reference](#http-api-reference)
- [Rust embedded API](#rust-embedded-api)
- [Python bindings](#python-bindings)
- [Constraints and requirements](#constraints-and-requirements)

---

## Concepts

### Policies

A **rollup policy** describes how one source metric should be downsampled:

| Field | Type | Description |
|---|---|---|
| `id` | `string` | Unique identifier for the policy. Used as a persistent key — renaming an `id` creates a new policy and discards the old materialisation. |
| `metric` | `string` | Source metric name to downsample. |
| `matchLabels` | `Label[]` | Optional label filter. The policy applies only to source series that carry **all** of these labels. An empty list matches every series for the metric. |
| `interval` | `i64` | Bucket width in the same units as your timestamp precision (milliseconds, nanoseconds, etc.). Must be greater than zero. |
| `aggregation` | `Aggregation` | Aggregation function applied within each bucket. Must not be `None`. |
| `bucketOrigin` | `i64` | Alignment origin for bucket boundaries. Bucket edges are computed as `origin + N × interval`. Defaults to `0` for power-of-two-aligned intervals. |

### Bucket alignment

Bucket boundaries are computed with Euclidean floor division:

```
bucket_start(ts, origin, interval) = origin + ⌊(ts − origin) / interval⌋ × interval
```

This ensures stable bucket edges for both positive and negative timestamps. All timestamps within the same bucket collapse to the bucket's start timestamp in the materialised output.

### Synthetic metric storage

Materialised data is stored internally under synthetic metric names that are never visible through the metadata APIs (`list_metrics`, label enumeration, etc.):

```
__tsink_rollup__:<policy_id>:<source_metric>
```

When a policy is modified (any field change), a **generation counter** is incremented and the new data is stored under:

```
__tsink_rollup__:<policy_id>:g<N>:<source_metric>
```

This lets the old materialisation become unreachable while the policy rebuilds from scratch, without any blocking rename or delete step.

### Checkpoints

The engine tracks a **checkpoint** for every `(policy, source series)` pair — the timestamp through which that series has been fully materialised. On each worker run, only data between the checkpoint and the current stable boundary is processed, making runs incremental.

The **stable boundary** is the latest bucket *start* before the most-recently-observed timestamp. The in-progress bucket is deliberately excluded so that partial-bucket data is never committed to the materialised output.

---

## Defining a policy

### JSON representation

```json
{
  "id": "cpu-1min",
  "metric": "cpu_usage",
  "matchLabels": [
    { "name": "env", "value": "prod" }
  ],
  "interval": 60000,
  "aggregation": "Avg",
  "bucketOrigin": 0
}
```

### Available aggregations

| Value | Description |
|---|---|
| `Sum` | Sum of all raw values in the bucket |
| `Avg` | Arithmetic mean |
| `Min` | Minimum value |
| `Max` | Maximum value |
| `Count` | Number of raw points |
| `Last` | Last value by timestamp |

`None` is not permitted in a rollup policy.

---

## Applying policies at runtime

Policies are managed as an **atomic set** — each `apply_rollup_policies` call replaces the entire active policy set. There is no add or remove operation; submit the complete desired set every time.

When a new set is applied:

1. Every submitted policy is validated and normalised.
2. The engine acquires the run lock and waits for any in-flight worker to finish.
3. Policies whose definitions are unchanged retain their existing checkpoints and materialised data.
4. A modified policy (any field change) receives a new generation, clearing its checkpoint so it rematerialises from scratch.
5. Policies that were removed have their checkpoints and pending state discarded; the synthetic materialised series are no longer queried. The underlying stored data is garbage-collected during subsequent compaction.
6. The new policy set and updated state are atomically persisted to disk.
7. A synchronous materialization pass runs immediately before the call returns.
8. The current `RollupObservabilitySnapshot` is returned.

**Idempotency**: submitting an identical policy set is a no-op beyond the disk persist and sync run.

---

## Triggering and scheduling

### Background worker

A background thread named `tsink-rollups` wakes every **5 seconds** and runs a full materialization pass across all active policies. The worker is also unparked immediately after:

- Every committed write batch (to keep materialisation lag low).
- Every committed tombstone (delete operation).
- An explicit `trigger_rollup_run` call.

The worker is co-ordinated with the background maintenance gate shared by compaction and flush, so these operations do not contend with each other mid-run.

### Forced run

Call `trigger_rollup_run` (or `POST /api/v1/admin/rollups/run`) to block until a full pass completes and return the resulting snapshot. Useful after bulk imports or in CI.

---

## Query-time substitution

When a `select` call requests downsampling (`downsample` option with an interval and aggregation), the engine checks whether a rollup candidate can satisfy the request before falling back to on-the-fly downsampling.

A candidate is accepted when all of the following hold:

- `policy.interval` equals the requested interval.
- `policy.aggregation` equals the requested aggregation.
- The query's `start` timestamp is exactly aligned to a bucket boundary (`(start − bucketOrigin) % interval == 0`).
- The policy matches the requested metric and all requested labels (`matchLabels` is a subset of the series' label set).
- A checkpoint exists for the source series and `materializedThrough > start`.
- No pending delete invalidation overlaps the query window.

When multiple candidates qualify, the engine prefers the most specific one (most `matchLabels`), then the one with the greatest `materializedThrough`, and finally the lexicographically smallest `id` as a tiebreak.

### Partial coverage

If the query window extends beyond `materializedThrough`, the engine:

1. Reads the materialised buckets for `[start, materializedThrough]`.
2. Reads raw points for `[materializedThrough, end]` and downsamples them on-the-fly using the same bucket alignment origin as the matching policy.
3. Merges the two result sets and deduplicates by timestamp.

This guarantees up-to-the-second results without waiting for the background worker.

If no candidate is found, the full query window is downsampled on-the-fly — identical behaviour to a storage instance with no policies.

---

## Observability

Every policy-management call returns a `RollupObservabilitySnapshot`:

```rust
pub struct RollupObservabilitySnapshot {
    pub worker_runs_total: u64,
    pub worker_success_total: u64,
    pub worker_errors_total: u64,
    pub policy_runs_total: u64,
    pub buckets_materialized_total: u64,
    pub points_materialized_total: u64,
    pub last_run_duration_nanos: u64,
    pub policies: Vec<RollupPolicyStatus>,
}
```

Per-policy status:

```rust
pub struct RollupPolicyStatus {
    pub policy: RollupPolicy,
    pub matched_series: u64,          // live source series matching the policy
    pub materialized_series: u64,     // source series with at least one checkpoint
    pub materialized_through: Option<i64>, // min checkpoint across all source series
    pub lag: Option<i64>,             // most-recent-point − materialized_through
    pub last_run_started_at_ms: Option<u64>,
    pub last_run_completed_at_ms: Option<u64>,
    pub last_run_duration_nanos: u64,
    pub last_error: Option<String>,
}
```

`lag` is `None` until the policy has processed at least one series. A lag of zero means every committed point is covered by the materialisation. Lag grows when the worker has not yet processed recent writes (typically less than 5 seconds under normal operation).

A `rollups` field with the same shape is included in the engine's `observability_snapshot()` output, which is served at `/metrics` in Prometheus format as part of the server's self-instrumentation.

---

## Crash safety and durability

Rollup state is persisted in two files under `<data_path>/.rollups/`:

| File | Contents |
|---|---|
| `policies.json` | Active policy set |
| `state.json` | Checkpoints, generation counters, pending materializations, pending delete invalidations |

Both files are written atomically (write to a temp file, `rename`, fsync of the parent directory).

### Pending materializations

Before writing any materialised rows to the storage engine, the engine records a **pending materialization** entry in `state.json` containing:

- The checkpoint advance it is about to make.
- The current generation for the policy.

On restart, the engine inspects each pending entry:

- If the materialised rows are already present (checkpoint already advanced ahead of the pending range), the entry is dropped as a no-op.
- If the stored checkpoint matches the pending entry's generation and checkpoint, the materialization window is retried without writing duplicate buckets.
- If no checkpoint exists for the series (crash before any rows were written), the entry is dropped and the policy re-materialises from scratch.

This two-phase approach ensures that a crash at any point — between the state write and the row write, or between the row write and the checkpoint update — results in at-most-once bucket duplication with full recovery.

### Pending delete invalidations

Before a tombstone is committed, the engine records a **pending delete invalidation** listing the affected series IDs and policy IDs. After the tombstone becomes visible, the invalidation finalises: it bumps the generation and clears the checkpoint for the affected policies. If a crash occurs between staging and finalisation, the invalidation is re-applied automatically on startup.

### Recovery sequence

On startup, after WAL replay:

1. `policies.json` and `state.json` are loaded.
2. Stale state for removed policies is discarded.
3. Generation counters and checkpoints are reconciled.
4. Any `pending_delete_invalidations` whose tombstones are already committed are immediately finalised.
5. The background worker thread is started.

The first worker run after startup picks up from the persisted checkpoints and fills in any gap left by the crash.

---

## Invalidation

Materialised data is invalidated (checkpoint cleared, generation bumped) whenever the source data changes in a way that would corrupt pre-computed buckets:

### Out-of-order writes

If a new point arrives with a timestamp **earlier than** the existing checkpoint for its source series, the affected policies are invalidated. The next worker run rematerialises from scratch under a new generation.

Writes with timestamps **at or above** the checkpoint do not trigger invalidation — they extend the materialization window on the next run.

### Deletes

A range delete on a source series invalidates any policy whose materialised data overlaps the deleted time range. The invalidation is staged durably before the tombstone commits, then finalised after it does. The rollup worker is immediately unparked to begin rebuilding.

---

## HTTP API reference

### `POST /api/v1/admin/rollups/apply`

Atomically replace the active policy set with the submitted list. Runs a synchronous materialization pass before returning.

**Request body**: JSON array of policy objects.

```json
[
  {
    "id": "cpu-5min",
    "metric": "cpu_usage",
    "matchLabels": [],
    "interval": 300000,
    "aggregation": "Avg",
    "bucketOrigin": 0
  }
]
```

**Response**: `RollupObservabilitySnapshot` (JSON).

**Notes**:
- Pass an empty array `[]` to remove all policies.
- Duplicate `id` values in the submitted list are rejected.
- Policy `metric` must not begin with `__tsink_rollup__:`.

---

### `POST /api/v1/admin/rollups/run`

Trigger an immediate, synchronous materialization pass. Blocks until the pass completes.

**Request body**: empty.

**Response**: `RollupObservabilitySnapshot` (JSON).

---

### `GET /api/v1/admin/rollups/status`

Return the current `RollupObservabilitySnapshot` without running a materialization pass.

**Response**: `RollupObservabilitySnapshot` (JSON).

---

## Rust embedded API

```rust
use tsink::{
    Aggregation, DataPoint, Label, RollupPolicy, Row,
    StorageBuilder, TimestampPrecision,
};

let storage = StorageBuilder::new()
    .with_data_path("./tsink-data")
    .with_timestamp_precision(TimestampPrecision::Milliseconds)
    .build()?;

// Insert some raw data.
storage.insert_rows(&[
    Row::new("cpu_usage", DataPoint::new(1_700_000_060_000_i64, 45.0)),
    Row::new("cpu_usage", DataPoint::new(1_700_000_120_000_i64, 50.0)),
    Row::new("cpu_usage", DataPoint::new(1_700_000_180_000_i64, 55.0)),
])?;

// Apply a 1-minute average policy.
let snapshot = storage.apply_rollup_policies(vec![
    RollupPolicy {
        id: "cpu-1min".to_string(),
        metric: "cpu_usage".to_string(),
        match_labels: vec![],
        interval: 60_000,
        aggregation: Aggregation::Avg,
        bucket_origin: 0,
    },
])?;

println!("materialized {} buckets", snapshot.buckets_materialized_total);

// Force a full materialization pass.
let snapshot = storage.trigger_rollup_run()?;

// A downsampled select query will automatically use the materialised rollup.
let points = storage.select_with_options(
    "cpu_usage",
    &[],
    1_700_000_000_000,
    1_700_000_300_000,
    SelectOptions::default().with_downsample(60_000, Aggregation::Avg),
)?;

storage.close()?;
```

---

## Python bindings

```python
from tsink import (
    TsinkStorageBuilder, DataPoint, RollupPolicy, Row, Value, Aggregation
)

builder = TsinkStorageBuilder()
builder.with_data_path("./tsink-data")
db = builder.build()

db.insert_rows([
    Row(metric="cpu_usage", labels=[], data_point=DataPoint(timestamp=1_700_000_060_000, value=Value.F64(v=45.0))),
    Row(metric="cpu_usage", labels=[], data_point=DataPoint(timestamp=1_700_000_120_000, value=Value.F64(v=50.0))),
])

snapshot = db.apply_rollup_policies([
    RollupPolicy(
        id="cpu-1min",
        metric="cpu_usage",
        match_labels=[],
        interval=60_000,
        aggregation=Aggregation.AVG,
        bucket_origin=0,
    )
])
print(f"materialized {snapshot.buckets_materialized_total} buckets")

snapshot = db.trigger_rollup_run()
print(snapshot)
```

---

## Constraints and requirements

| Constraint | Details |
|---|---|
| **Requires `data_path`** | Rollups are not available for in-memory-only storage instances. `apply_rollup_policies` returns `TsinkError::InvalidConfiguration` if no data path was configured. |
| **Non-empty `id`** | Policy identifiers must be non-empty strings. |
| **Positive `interval`** | `interval` must be greater than zero. |
| **Aggregation must not be `None`** | The `None` aggregation is rejected during policy validation. |
| **No internal metric names** | The source `metric` must not begin with `__tsink_rollup__:`. |
| **Atomic set semantics** | `apply_rollup_policies` replaces the entire policy set. To add a policy, submit the existing policies plus the new one. |
| **Bucket-aligned query start** | Query-time substitution only activates when the query's `start` is exactly aligned to a bucket boundary relative to `bucketOrigin`. Mis-aligned queries fall back to on-the-fly downsampling. |
| **Generation rebuilds on modification** | Any field change to an existing policy triggers a full rematerialisation. Modifying `interval` or `aggregation` of a high-cardinality policy temporarily increases storage usage until old-generation data is compacted away. |
