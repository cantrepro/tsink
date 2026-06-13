# Tiered storage

tsink supports automatic hot → warm → cold tiered storage backed by an object store (or any locally mounted volume). Segments are moved between tiers by the post-flush maintenance pipeline based on configurable age windows, and reads are automatically routed to the correct tier at query time.

---

## Overview

Without tiered storage, all persisted segments live on the local data volume. Tiered storage extends that with a second volume — the **object-store root** — that holds three subdirectories:

| Tier | Location | Data age |
|------|----------|----------|
| **Hot** | `{object_store_root}/hot/` | Within `hot_retention_window` of the ingestion frontier |
| **Warm** | `{object_store_root}/warm/` | Older than `hot_retention_window`, within `warm_retention_window` |
| **Cold** | `{object_store_root}/cold/` | Older than `warm_retention_window`, within the global retention window |

Segments past the global retention window are deleted.

Tiering is **optional** and **disabled by default**. When disabled, all segments remain in the local `data_path` and no warm/cold movement ever occurs.

---

## Enabling tiered storage

### Rust `StorageBuilder`

```rust
use std::time::Duration;
use tsink::{StorageBuilder, TimestampPrecision};

let storage = StorageBuilder::new()
    .with_data_path("./local-data")
    .with_timestamp_precision(TimestampPrecision::Milliseconds)
    .with_object_store_path("./object-store")          // enables tiering
    .with_tiered_retention_policy(
        Duration::from_secs(2  * 24 * 3600),           // hot → warm after 2 days
        Duration::from_secs(14 * 24 * 3600),           // warm → cold after 14 days
    )
    // overall expiry — data older than this is deleted
    .with_retention(Duration::from_secs(90 * 24 * 3600))
    .build()?;
```

`with_tiered_retention_policy` implicitly enables retention enforcement.

### Server binary

```bash
tsink-server \
  --data-path ./local-data \
  --object-store-path ./object-store \
  --hot-tier-retention 2d \
  --warm-tier-retention 14d \
  --retention 90d
```

---

## Configuration reference

### `StorageBuilder` methods

| Method | Default | Description |
|--------|---------|-------------|
| `with_object_store_path(path)` | `None` (no tiering) | Sets the root path for warm/cold segment storage. Typically a path on an object-store-backed volume separate from `data_path`. Setting this enables tiering. |
| `with_tiered_retention_policy(hot, warm)` | — | Sets hot and warm cutoff windows and enables retention enforcement. |
| `with_retention(duration)` | 14 days | Global data expiry. Also used as the fallback value for unconfigured tier windows. |
| `with_runtime_mode(mode)` | `ReadWrite` | `ComputeOnly` for query-only nodes — see [Compute-only mode](#compute-only-mode). |
| `with_remote_segment_refresh_interval(duration)` | ~5 s | How often the segment catalog is re-read from the object store. |
| `with_mirror_hot_segments_to_object_store(bool)` | `false` | Copy hot segments into `{object_store_root}/hot/` as they are flushed — see [Hot segment mirroring](#hot-segment-mirroring). |
| `with_remote_segment_cache_policy(policy)` | `MetadataOnly` | Controls remote chunk prefetching. `MetadataOnly` prefetches chunk index metadata only; payload bytes are mmap'd on demand. |

### Server CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--object-store-path PATH` | unset | Object-store root — enables tiering |
| `--hot-tier-retention DURATION` | falls back to `--retention` | Age cutoff for hot→warm migration |
| `--warm-tier-retention DURATION` | falls back to `--retention` | Age cutoff for warm→cold migration |
| `--retention DURATION` | `14d` | Global expiry |
| `--storage-mode MODE` | `read-write` | `read-write` or `compute-only` |
| `--remote-segment-refresh-interval DURATION` | ~5 s | Catalog refresh TTL |
| `--mirror-hot-segments-to-object-store BOOL` | `false` | Mirror hot segments on flush |

Duration values accept a number followed by a unit suffix: `s`, `m`, `h`, `d` (e.g. `7d`, `48h`).

---

## Directory layout

When tiered storage is configured, the object-store root adopts this layout:

```
{object_store_root}/
  segment_catalog.json          ← shared inventory file
  hot/
    lane_numeric/
      segments/
        L0/seg-<id>/
        L1/seg-<id>/
        ...
    lane_blob/
      segments/
        ...
  warm/
    lane_numeric/ ...
    lane_blob/    ...
  cold/
    lane_numeric/ ...
    lane_blob/    ...
```

Each `seg-<id>` directory contains the segment's data files and a `manifest.json`. The segment catalog at the root provides a fast, authoritative index of all segments and their tiers without walking the full directory tree.

---

## Tier lifecycle

### Ingestion and flush

New data is always written to the local write buffer and WAL. When the flush pipeline seals a memory chunk, it writes a new persisted segment to the local hot storage. If `mirror_hot_segments_to_object_store` is enabled, an additional copy is placed under `{object_store_root}/hot/`.

### Post-flush maintenance

After every flush, the maintenance pipeline computes a `RetentionTierPolicy` using:

- **`retention_cutoff`** — timestamps older than this are expired.
- **`hot_cutoff`** — `now − hot_retention_window`; segments with `max_ts < hot_cutoff` move to warm.
- **`warm_cutoff`** — `now − warm_retention_window`; segments with `max_ts < warm_cutoff` move to cold.

For each segment in the inventory, the policy produces one of four outcomes:

| Condition | Action |
|-----------|--------|
| `max_ts < retention_cutoff` | Delete segment |
| Segment spans the retention boundary (`min_ts < retention_cutoff ≤ max_ts`) | Rewrite segment to strip expired data, then move |
| `max_ts < warm_cutoff` and tiering enabled | Move segment to cold tier |
| `max_ts < hot_cutoff` and tiering enabled | Move segment to warm tier |
| Otherwise | Leave in current tier |

### Move semantics

Tier moves are **copy-then-delete**:

1. The segment directory is copied to the destination path under a staging name.
2. Once the copy is verified (fingerprint checked), the staged copy is atomically promoted.
3. The segment catalog is updated and swapped into the visible persisted index.
4. Only after the new location is visible to queries is the source directory retired.

This guarantees that no query ever sees a gap: either the old location or the new location is always visible, never neither.

Moves are also **idempotent**: if a destination already exists with matching content, the move is a no-op.

---

## Segment catalog

The catalog (`segment_catalog.json` in the object-store root) is a JSON snapshot of the full `SegmentInventory`. It records each segment's lane, tier, level, ID, timestamp bounds, point count, and relative path.

- **ReadWrite nodes** write the catalog atomically after each maintenance pass.
- **Compute-only nodes** read the catalog periodically (controlled by `remote_segment_refresh_interval`) and never write it.
- The catalog is version-stamped (current version: 2) and validated on load. Entries with path traversal sequences (`..`, absolute paths) are rejected.

If the catalog is absent or stale, the engine falls back to a full directory scan.

---

## Query routing

Each query carries a `TieredQueryPlan` that specifies which tiers to include based on the query's time range and the current tier cutoffs:

| Query time range | Tiers scanned |
|------------------|---------------|
| Entirely within hot window (`start ≥ hot_cutoff`) | Hot only |
| Overlaps warm window (`start < hot_cutoff`) | Hot + Warm |
| Overlaps cold window (`start < warm_cutoff`) | Hot + Warm + Cold |

Chunk-level reads in the read path skip any chunk whose tier is not included in the plan, avoiding unnecessary I/O against remote tiers for recent-data queries.

---

## Hot segment mirroring

Setting `with_mirror_hot_segments_to_object_store(true)` (or `--mirror-hot-segments-to-object-store true`) copies each newly flushed segment into `{object_store_root}/hot/` immediately, before the normal post-flush age-based movement occurs.

Use this when:

- You want compute-only query nodes to have immediate access to fresh data (avoiding the refresh interval lag).
- Hot data durability beyond the local disk is required.
- You are running a disaggregated storage/compute architecture where all I/O should go through the object store.

When mirroring is off, hot segments stay on local disk and are only moved to the object store once they age past `hot_retention_window`.

---

## Compute-only mode

A node in `ComputeOnly` mode:

- Does **not** accept writes or run the WAL.
- Does **not** hold a local segment catalog path (no writes to the catalog).
- Reads the segment catalog from the object-store root on a periodic refresh cycle.
- Serves queries by reading segments directly from the object-store tiers.

```rust
use tsink::{StorageBuilder, StorageRuntimeMode};

let storage = StorageBuilder::new()
    .with_object_store_path("./shared-object-store")
    .with_runtime_mode(StorageRuntimeMode::ComputeOnly)
    .with_remote_segment_refresh_interval(Duration::from_secs(5))
    .build()?;
```

```bash
tsink-server \
  --object-store-path ./shared-object-store \
  --storage-mode compute-only \
  --remote-segment-refresh-interval 5s
```

Compute-only nodes require `object_store_path` to be set and `mirror_hot_segments_to_object_store` to be enabled on the writer node so that they can see fresh data promptly.

---

## Observability

### Flush metrics (`FlushObservabilitySnapshot`)

| Field | Description |
|-------|-------------|
| `tier_moves_total` | Successful tier move operations since startup |
| `tier_move_errors_total` | Failed tier move operations |
| `expired_segments_total` | Segments deleted by retention enforcement |
| `hot_segments_visible` | Current count of hot-tier segments in the index |
| `warm_segments_visible` | Current count of warm-tier segments |
| `cold_segments_visible` | Current count of cold-tier segments |

### Query metrics (`QueryObservabilitySnapshot`)

| Field | Description |
|-------|-------------|
| `hot_only_query_plans_total` | Queries that scanned the hot tier only |
| `warm_tier_query_plans_total` | Queries that included the warm tier |
| `cold_tier_query_plans_total` | Queries that included the cold tier |
| `hot_tier_persisted_chunks_read_total` | Chunk reads from the hot tier |
| `warm_tier_persisted_chunks_read_total` | Chunk reads from the warm tier |
| `cold_tier_persisted_chunks_read_total` | Chunk reads from the cold tier |
| `warm_tier_fetch_duration_nanos_total` | Cumulative fetch time for warm tier chunks |
| `cold_tier_fetch_duration_nanos_total` | Cumulative fetch time for cold tier chunks |

### Remote storage metrics (`RemoteStorageObservabilitySnapshot`)

| Field | Description |
|-------|-------------|
| `enabled` | Whether tiered storage is configured |
| `runtime_mode` | `ReadWrite` or `ComputeOnly` |
| `mirror_hot_segments` | Whether hot segment mirroring is active |
| `catalog_refreshes_total` | Total catalog refresh attempts |
| `catalog_refresh_errors_total` | Failed catalog refreshes |
| `accessible` | Whether the object store was reachable on last check |
| `last_successful_refresh_unix_ms` | Unix timestamp of last successful catalog read |
| `consecutive_refresh_failures` | Number of consecutive failures (used for backoff) |
| `backoff_active` | Whether exponential backoff is in effect |

These are exposed under the `/metrics` endpoint in the server.

---

## Python bindings

The tiering configuration is available through the UniFFI Python bindings:

```python
from tsink import TsinkStorageBuilder

builder = TsinkStorageBuilder()
builder.with_data_path("./local-data")
builder.with_object_store_path("./object-store")
# Tier retention and full retention are set via with_tiered_retention_policy /
# with_retention if you need them; the hot-mirror flag is available too:
builder.with_mirror_hot_segments_to_object_store(True)
db = builder.build()
```

See the [Python bindings guide](python-bindings.md) for complete API details.

---

## Operational notes

- **Object-store root can be any path** — in production this is typically a FUSE mount or network filesystem. tsink itself uses standard filesystem calls and has no direct S3/GCS SDK dependency.
- **Tier moves are not reversible automatically** — once a segment is in the cold tier there is no built-in promotion back to warm or hot. Adjust `hot_retention_window` / `warm_retention_window` to control placement.
- **Concurrent access** — multiple ReadWrite nodes pointing at the same `object_store_root` are not supported. Use the cluster mode (which distributes shards) instead of sharing a single tier root.
- **Recovery at startup** — on startup, the engine reads the catalog (if present) or scans all tier directories. Corrupt or unreadable segments are quarantined rather than causing a startup failure. Quarantined paths are logged.
- **Capacity planning** — each tier directory grows monotonically until the post-flush sweep runs. Retention enforcement and compaction both reduce segment count; ensure the object-store volume has sufficient capacity for `warm_retention_window + cold_retention_window` worth of data.
