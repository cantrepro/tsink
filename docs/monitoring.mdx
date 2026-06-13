# Monitoring & Observability

tsink exposes three built-in observability surfaces: health probes for Kubernetes liveness/readiness checks, a Prometheus-format self-instrumentation endpoint, and support bundles for ad-hoc diagnostics. All three are available without any extra configuration.

---

## Health probes

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness probe — returns `ok` with HTTP 200 if the server process is running |
| `GET /ready` | Readiness probe — returns `ready` with HTTP 200 when the server is ready to serve traffic |

Both endpoints bypass authentication, respond with `Content-Type: text/plain`, and are safe to poll from infrastructure tools without bearer tokens.

**Kubernetes example**

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 9201
  initialDelaySeconds: 5
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /ready
    port: 9201
  initialDelaySeconds: 5
  periodSeconds: 10
```

---

## Self-instrumentation endpoint

```
GET /metrics
```

Returns all internal metrics in [Prometheus text exposition format 0.0.4](https://prometheus.io/docs/instrumenting/exposition_formats/). The response is suitable for direct Prometheus scraping.

```bash
curl http://127.0.0.1:9201/metrics
```

When RBAC is enabled, this endpoint requires a token with the `metrics:read` permission. In unauthenticated deployments it is open.

### Scraping with Prometheus

```yaml
scrape_configs:
  - job_name: tsink
    static_configs:
      - targets: ['127.0.0.1:9201']
    # If RBAC is enabled:
    # authorization:
    #   credentials: <service-account-token>
```

---

## Metric reference

All metrics use the `tsink_` prefix. The sections below enumerate every metric group emitted by the server.

### General

| Metric | Type | Description |
|---|---|---|
| `tsink_uptime_seconds` | gauge | Server uptime in seconds |
| `tsink_series_total` | gauge | Number of known metric series |

### Memory

| Metric | Type | Description |
|---|---|---|
| `tsink_memory_used_bytes` | gauge | Bytes counted against the configured memory budget |
| `tsink_memory_budget_bytes` | gauge | Configured memory budget |
| `tsink_memory_excluded_bytes` | gauge | Memory intentionally excluded from the budget |
| `tsink_memory_registry_bytes` | gauge | Budget bytes used by the in-memory series registry |
| `tsink_memory_metadata_cache_bytes` | gauge | Budget bytes used by metadata caches and indexes |
| `tsink_memory_persisted_index_bytes` | gauge | Budget bytes used by persisted chunk refs and timestamp indexes |
| `tsink_memory_persisted_mmap_bytes` | gauge | Budget bytes used by mmap-backed segment payloads |
| `tsink_memory_tombstone_bytes` | gauge | Budget bytes used by tombstone state |

### Write-Ahead Log (WAL)

| Metric | Type | Description |
|---|---|---|
| `tsink_wal_enabled` | gauge | WAL enabled (1) or disabled (0) |
| `tsink_wal_size_bytes` | gauge | WAL size on disk |
| `tsink_wal_segments` | gauge | WAL segment files present |
| `tsink_wal_active_segment` | gauge | Current WAL segment id |
| `tsink_wal_acknowledged_writes_durable` | gauge | Whether acknowledged writes are fsync-durable (1) or append-only (0) |
| `tsink_wal_highwater_segment` | gauge | Last appended WAL highwater segment |
| `tsink_wal_highwater_frame` | gauge | Last appended WAL highwater frame |
| `tsink_wal_durable_highwater_segment` | gauge | Last durable WAL highwater segment |
| `tsink_wal_durable_highwater_frame` | gauge | Last durable WAL highwater frame |
| `tsink_wal_replay_runs_total` | counter | WAL replay runs |
| `tsink_wal_replay_frames_total` | counter | WAL replayed frames |
| `tsink_wal_replay_series_definitions_total` | counter | WAL replayed series definitions |
| `tsink_wal_replay_sample_batches_total` | counter | WAL replayed sample batches |
| `tsink_wal_replay_points_total` | counter | WAL replayed points |
| `tsink_wal_replay_errors_total` | counter | WAL replay errors |
| `tsink_wal_replay_duration_nanoseconds_total` | counter | WAL replay runtime |
| `tsink_wal_append_series_definitions_total` | counter | WAL appended series definitions |
| `tsink_wal_append_sample_batches_total` | counter | WAL appended sample batches |
| `tsink_wal_append_points_total` | counter | WAL appended points |
| `tsink_wal_append_bytes_total` | counter | WAL appended bytes |
| `tsink_wal_append_errors_total` | counter | WAL append errors |
| `tsink_wal_resets_total` | counter | WAL resets |
| `tsink_wal_reset_errors_total` | counter | WAL reset errors |

### Flush pipeline

The flush pipeline moves active (in-memory) chunks into persisted segments and manages tier lifecycle.

| Metric | Type | Description |
|---|---|---|
| `tsink_flush_pipeline_runs_total` | counter | Flush pipeline runs |
| `tsink_flush_pipeline_success_total` | counter | Successful flush pipeline runs |
| `tsink_flush_pipeline_timeout_total` | counter | Flush pipeline write-timeout skips |
| `tsink_flush_pipeline_errors_total` | counter | Flush pipeline errors |
| `tsink_flush_pipeline_duration_nanoseconds_total` | counter | Flush pipeline runtime |
| `tsink_flush_active_runs_total` | counter | Active chunk flush runs |
| `tsink_flush_active_errors_total` | counter | Active chunk flush errors |
| `tsink_flush_active_series_total` | counter | Active series flushed into sealed chunks |
| `tsink_flush_active_chunks_total` | counter | Active chunks flushed |
| `tsink_flush_active_points_total` | counter | Active points flushed |
| `tsink_flush_persist_runs_total` | counter | Persist attempts |
| `tsink_flush_persist_success_total` | counter | Successful persist runs |
| `tsink_flush_persist_noop_total` | counter | Persist runs with no new chunks |
| `tsink_flush_persist_errors_total` | counter | Persist errors |
| `tsink_flush_persisted_series_total` | counter | Series persisted |
| `tsink_flush_persisted_chunks_total` | counter | Chunks persisted |
| `tsink_flush_persisted_points_total` | counter | Points persisted |
| `tsink_flush_persisted_segments_total` | counter | Segments emitted by persist |
| `tsink_flush_persist_duration_nanoseconds_total` | counter | Persist runtime |
| `tsink_flush_evicted_sealed_chunks_total` | counter | Sealed chunks evicted after persistence |
| `tsink_flush_tier_moves_total` | counter | Persisted segments moved across tiers |
| `tsink_flush_tier_move_errors_total` | counter | Tier-move errors |
| `tsink_flush_expired_segments_total` | counter | Segments expired by retention |
| `tsink_flush_hot_segments_visible` | gauge | Hot-tier persisted segments visible to queries |
| `tsink_flush_warm_segments_visible` | gauge | Warm-tier persisted segments visible to queries |
| `tsink_flush_cold_segments_visible` | gauge | Cold-tier persisted segments visible to queries |

### Compaction

| Metric | Type | Description |
|---|---|---|
| `tsink_compaction_runs_total` | counter | Compaction invocations |
| `tsink_compaction_success_total` | counter | Compaction runs that rewrote segments |
| `tsink_compaction_noop_total` | counter | Compaction runs with no rewrite |
| `tsink_compaction_errors_total` | counter | Compaction errors |
| `tsink_compaction_source_segments_total` | counter | Source segments considered |
| `tsink_compaction_output_segments_total` | counter | Output segments emitted |
| `tsink_compaction_source_chunks_total` | counter | Source chunks considered |
| `tsink_compaction_output_chunks_total` | counter | Output chunks emitted |
| `tsink_compaction_source_points_total` | counter | Source points considered |
| `tsink_compaction_output_points_total` | counter | Output points emitted |
| `tsink_compaction_duration_nanoseconds_total` | counter | Compaction runtime |

### Query

| Metric | Type | Description |
|---|---|---|
| `tsink_query_select_calls_total` | counter | `select` calls |
| `tsink_query_select_errors_total` | counter | `select` errors |
| `tsink_query_select_duration_nanoseconds_total` | counter | `select` runtime |
| `tsink_query_select_points_returned_total` | counter | Points returned by `select` |
| `tsink_query_select_with_options_calls_total` | counter | `select_with_options` calls |
| `tsink_query_select_with_options_errors_total` | counter | `select_with_options` errors |
| `tsink_query_select_with_options_duration_nanoseconds_total` | counter | `select_with_options` runtime |
| `tsink_query_select_with_options_points_returned_total` | counter | Points returned |
| `tsink_query_select_all_calls_total` | counter | `select_all` calls |
| `tsink_query_select_all_errors_total` | counter | `select_all` errors |
| `tsink_query_select_all_duration_nanoseconds_total` | counter | `select_all` runtime |
| `tsink_query_select_all_series_returned_total` | counter | Series returned |
| `tsink_query_select_all_points_returned_total` | counter | Points returned |
| `tsink_query_select_series_calls_total` | counter | `select_series` calls |
| `tsink_query_select_series_errors_total` | counter | `select_series` errors |
| `tsink_query_select_series_duration_nanoseconds_total` | counter | `select_series` runtime |
| `tsink_query_select_series_returned_total` | counter | Series returned |
| `tsink_query_merge_path_queries_total` | counter | Series collections using merge path |
| `tsink_query_merge_path_shard_snapshots_total` | counter | Merge-path shard snapshots taken |
| `tsink_query_merge_path_shard_snapshot_wait_nanoseconds_total` | counter | Merge-path time waiting for shard read locks |
| `tsink_query_merge_path_shard_snapshot_hold_nanoseconds_total` | counter | Merge-path time holding shard read locks |
| `tsink_query_append_sort_path_queries_total` | counter | Series collections using append/sort path |
| `tsink_query_hot_only_plans_total` | counter | Query plans satisfied from the hot tier only |
| `tsink_query_warm_tier_plans_total` | counter | Query plans that include the warm tier |
| `tsink_query_cold_tier_plans_total` | counter | Query plans that include the cold tier |
| `tsink_query_hot_tier_persisted_chunks_read_total` | counter | Hot-tier persisted chunks decoded |
| `tsink_query_warm_tier_persisted_chunks_read_total` | counter | Warm-tier persisted chunks decoded |
| `tsink_query_cold_tier_persisted_chunks_read_total` | counter | Cold-tier persisted chunks decoded |
| `tsink_query_warm_tier_fetch_duration_nanoseconds_total` | counter | Warm-tier chunk fetch and decode time |
| `tsink_query_cold_tier_fetch_duration_nanoseconds_total` | counter | Cold-tier chunk fetch and decode time |
| `tsink_query_rollup_plans_total` | counter | Queries that used persisted rollup artifacts |
| `tsink_query_partial_rollup_plans_total` | counter | Queries that mixed rollups with raw tail reads |
| `tsink_query_rollup_points_read_total` | counter | Persisted rollup points read |

### Remote (object-store) storage

| Metric | Type | Description |
|---|---|---|
| `tsink_remote_storage_accessible` | gauge | `1` when object-store access is healthy |
| `tsink_remote_storage_compute_only` | gauge | `1` when running in compute-only mode |
| `tsink_remote_storage_mirror_hot_segments` | gauge | `1` when hot segments are mirrored to object store |
| `tsink_remote_storage_catalog_refreshes_total` | counter | Remote catalog refreshes attempted |
| `tsink_remote_storage_catalog_refresh_errors_total` | counter | Remote catalog refresh errors |
| `tsink_remote_storage_catalog_refresh_consecutive_failures` | gauge | Consecutive catalog refresh failures |
| `tsink_remote_storage_catalog_refresh_backoff_active` | gauge | `1` when retry backoff is active |

### Rollups

| Metric | Type | Description |
|---|---|---|
| `tsink_rollup_worker_runs_total` | counter | Rollup maintenance passes attempted |
| `tsink_rollup_worker_success_total` | counter | Successful maintenance passes |
| `tsink_rollup_worker_errors_total` | counter | Maintenance passes that errored |
| `tsink_rollup_policy_runs_total` | counter | Individual rollup policy evaluations |
| `tsink_rollup_buckets_materialized_total` | counter | Rollup buckets materialized |
| `tsink_rollup_points_materialized_total` | counter | Rollup points materialized |
| `tsink_rollup_last_run_duration_nanoseconds` | gauge | Duration of the most recent maintenance pass |
| `tsink_rollup_policy_status{policy,metric,aggregation,kind}` | gauge | Per-policy coverage, lag, and timing |

`tsink_rollup_policy_status` is emitted once per configured rollup policy with a `kind` label for each dimension:

| `kind` value | Meaning |
|---|---|
| `matched_series` | Series matched by the policy selector |
| `materialized_series` | Series with persisted rollup artifacts |
| `interval` | Configured rollup interval in milliseconds |
| `materialized_through` | Latest materialized timestamp (unix ms) |
| `lag` | Materialization lag in milliseconds |
| `last_run_duration_nanos` | Duration of the last policy run |
| `last_run_started_at_ms` | Start time of the last policy run |
| `last_run_completed_at_ms` | Completion time of the last policy run |

### Rules engine

| Metric | Type | Description |
|---|---|---|
| `tsink_rules_scheduler_runs_total` | counter | Rules scheduler ticks attempted |
| `tsink_rules_scheduler_skipped_not_leader_total` | counter | Ticks skipped — not cluster leader |
| `tsink_rules_scheduler_skipped_inflight_total` | counter | Ticks skipped — previous run still in flight |
| `tsink_rules_evaluated_total` | counter | Rules evaluated |
| `tsink_rules_evaluation_failures_total` | counter | Rules evaluations that errored |
| `tsink_rules_recording_rows_written_total` | counter | Samples written by recording rules |
| `tsink_rules_scheduler_active` | gauge | `1` when this node is the active rules scheduler |
| `tsink_rules_configured{kind}` | gauge | Configured groups, rules, pending alerts, firing alerts |
| `tsink_rules_runtime_limits{kind}` | gauge | Scheduler tick interval and per-evaluation limits |

### Exemplars

| Metric | Type | Description |
|---|---|---|
| `tsink_exemplars_accepted_total` | counter | Exemplars accepted |
| `tsink_exemplars_rejected_total` | counter | Exemplars rejected |
| `tsink_exemplars_dropped_total` | counter | Exemplars dropped due to retention guardrails |
| `tsink_exemplars_query_requests_total` | counter | Exemplar query requests served |
| `tsink_exemplars_query_series_total` | counter | Exemplar series returned by queries |
| `tsink_exemplars_query_results_total` | counter | Exemplars returned by queries |
| `tsink_exemplars_stored{kind}` | gauge | Currently stored series and exemplars |
| `tsink_exemplar_limits{kind}` | gauge | Configured exemlar quotas and guardrails |

### Ingest protocols

#### Prometheus remote write

| Metric | Type | Description |
|---|---|---|
| `tsink_prometheus_payload_feature_enabled{payload}` | gauge | Feature flag per payload kind (metadata, exemplar, histogram) |
| `tsink_prometheus_payload_accepted_total{payload}` | counter | Payloads accepted per kind |
| `tsink_prometheus_payload_rejected_total{payload}` | counter | Payloads rejected per kind |

#### OTLP

| Metric | Type | Description |
|---|---|---|
| `tsink_otlp_metrics_enabled` | gauge | OTLP metrics ingest enabled (1) or not (0) |
| `tsink_otlp_requests_total{outcome}` | counter | OTLP `/v1/metrics` requests, labeled `accepted` or `rejected` |
| `tsink_otlp_data_points_total{kind,outcome}` | counter | OTLP data points by metric kind and outcome |
| `tsink_otlp_exemplars_total{outcome}` | counter | OTLP exemplars by outcome |
| `tsink_otlp_supported_shape{shape}` | gauge | `1` for each supported OTLP metric shape |

#### Legacy ingest (StatsD, Graphite, InfluxDB)

| Metric | Type | Description |
|---|---|---|
| `tsink_legacy_ingest_enabled{adapter}` | gauge | `1` for each enabled legacy adapter |

### Admission control

Write and read admission are tracked independently.

#### Write admission

| Metric | Type | Description |
|---|---|---|
| `tsink_write_admission_rejections_total` | counter | Total public write admission rejections |
| `tsink_write_admission_request_slot_rejections_total` | counter | Rejections due to concurrency saturation |
| `tsink_write_admission_row_budget_rejections_total` | counter | Rejections due to in-flight row saturation |
| `tsink_write_admission_oversize_rows_rejections_total` | counter | Rejections for requests exceeding the row budget |
| `tsink_write_admission_acquire_wait_nanoseconds_total` | counter | Wait time acquiring admission permits |
| `tsink_write_admission_active_requests` | gauge | Active requests holding admission slots |
| `tsink_write_admission_active_rows` | gauge | Active rows reserved against admission budget |

#### Read admission

| Metric | Type | Description |
|---|---|---|
| `tsink_read_admission_rejections_total` | counter | Total public read admission rejections |
| `tsink_read_admission_request_slot_rejections_total` | counter | Rejections due to concurrency saturation |
| `tsink_read_admission_query_budget_rejections_total` | counter | Rejections due to in-flight query saturation |
| `tsink_read_admission_oversize_queries_rejections_total` | counter | Rejections for requests exceeding the query budget |
| `tsink_read_admission_acquire_wait_nanoseconds_total` | counter | Wait time acquiring admission permits |
| `tsink_read_admission_active_requests` | gauge | Active requests holding admission slots |

#### Per-tenant admission

Tenant admission metrics carry a `tenant` label when multi-tenancy is enabled.

### Edge sync

Edge sync ships writes queued on edge/source nodes upstream. Metrics are emitted per role.

| Metric | Type | Description |
|---|---|---|
| `tsink_edge_sync_enabled{role}` | gauge | Source and accept mode enablement |
| `tsink_edge_sync_queue{kind}` | gauge | Backlog entries, bytes, log size, oldest age, and retention window |
| `tsink_edge_sync_events_total{event}` | counter | Enqueue, replay, and retention-drop events |
| `tsink_edge_sync_replayed_rows_total` | counter | Rows replayed upstream |
| `tsink_edge_sync_accept_dedupe{...}` | gauge/counter | Accept-side idempotency window state |

### Cluster — write routing

| Metric | Type | Description |
|---|---|---|
| `tsink_cluster_write_requests_total` | counter | Write requests routed through the coordinator |
| `tsink_cluster_write_local_rows_total` | counter | Rows inserted locally |
| `tsink_cluster_write_routed_rows_total` | counter | Rows forwarded to remote owners |
| `tsink_cluster_write_routed_batches_total` | counter | Remote write batches sent |
| `tsink_cluster_write_failures_total` | counter | Write routing failures |
| `tsink_cluster_write_shard_rows_total{shard}` | counter | Rows routed per shard |
| `tsink_cluster_write_peer_routed_rows_total{node_id}` | counter | Rows routed per peer |
| `tsink_cluster_write_peer_routed_batches_total{node_id}` | counter | Batches routed per peer |
| `tsink_cluster_write_remote_requests_total{node_id}` | counter | Remote write RPC requests per peer |
| `tsink_cluster_write_remote_failures_total{node_id}` | counter | Remote write RPC failures per peer |
| `tsink_cluster_write_remote_request_duration_seconds{node_id,le}` | histogram | Remote write RPC latency per peer |

### Cluster — deduplication

| Metric | Type | Description |
|---|---|---|
| `tsink_cluster_dedupe_requests_total` | counter | Idempotency key checks |
| `tsink_cluster_dedupe_accepted_total` | counter | Requests accepted for dedupe tracking |
| `tsink_cluster_dedupe_duplicates_total` | counter | Requests deduplicated |
| `tsink_cluster_dedupe_inflight_rejections_total` | counter | Conflicts while key is in-flight |
| `tsink_cluster_dedupe_commits_total` | counter | Dedupe marker commits |
| `tsink_cluster_dedupe_aborts_total` | counter | Dedupe marker aborts |
| `tsink_cluster_dedupe_cleanup_runs_total` | counter | Cleanup runs |
| `tsink_cluster_dedupe_expired_keys_total` | counter | Keys expired by TTL |
| `tsink_cluster_dedupe_evicted_keys_total` | counter | Keys evicted by size bound |
| `tsink_cluster_dedupe_persistence_failures_total` | counter | Dedupe marker persistence failures |
| `tsink_cluster_dedupe_active_keys` | gauge | Active dedupe keys in window |
| `tsink_cluster_dedupe_inflight_keys` | gauge | In-flight dedupe keys |
| `tsink_cluster_dedupe_log_bytes` | gauge | Durable dedupe marker log size on disk |

### Cluster — read fanout

| Metric | Type | Description |
|---|---|---|
| `tsink_cluster_fanout_requests_total` | counter | Read fanout requests |
| `tsink_cluster_fanout_failures_total` | counter | Read fanout failures |
| `tsink_cluster_fanout_duration_nanoseconds_total` | counter | Fanout execution time |
| `tsink_cluster_fanout_remote_requests_total` | counter | Remote RPC requests |
| `tsink_cluster_fanout_remote_failures_total` | counter | Remote RPC failures |
| `tsink_cluster_fanout_resource_rejections_total` | counter | Guardrail rejections |
| `tsink_cluster_fanout_resource_acquire_wait_nanoseconds_total` | counter | Wait time for query permits |
| `tsink_cluster_fanout_resource_active_queries` | gauge | Active distributed reads holding permits |
| `tsink_cluster_fanout_resource_active_merged_points` | gauge | Merged-point budget in use |
| `tsink_cluster_fanout_operation_requests_total{operation}` | counter | Fanout requests per operation |
| `tsink_cluster_fanout_operation_failures_total{operation}` | counter | Fanout failures per operation |
| `tsink_cluster_fanout_remote_requests_by_peer_total{node_id,operation}` | counter | RPC requests per peer and operation |
| `tsink_cluster_fanout_remote_failures_by_peer_total{node_id,operation}` | counter | RPC failures per peer and operation |
| `tsink_cluster_fanout_remote_request_duration_seconds{node_id,operation,le}` | histogram | RPC latency per peer and operation |

### Cluster — read planner

| Metric | Type | Description |
|---|---|---|
| `tsink_cluster_read_planner_requests_total` | counter | Read planner requests |
| `tsink_cluster_read_planner_candidate_shards_total` | counter | Candidate shards evaluated |
| `tsink_cluster_read_planner_pruned_shards_total` | counter | Shards pruned |
| `tsink_cluster_read_planner_local_shards_total` | counter | Local shards selected |
| `tsink_cluster_read_planner_remote_targets_total` | counter | Remote peer targets selected |
| `tsink_cluster_read_planner_remote_shards_total` | counter | Remote shard assignments |
| `tsink_cluster_read_planner_operation_requests_total{operation}` | counter | Planner requests per operation |
| `tsink_cluster_read_planner_operation_candidate_shards_total{operation}` | counter | Candidate shards per operation |
| `tsink_cluster_read_planner_operation_pruned_shards_total{operation}` | counter | Pruned shards per operation |
| `tsink_cluster_read_planner_operation_remote_targets_total{operation}` | counter | Remote targets per operation |

### Cluster — hinted handoff (outbox)

| Metric | Type | Description |
|---|---|---|
| `tsink_cluster_outbox_enqueued_total` | counter | Replica batches enqueued |
| `tsink_cluster_outbox_enqueue_rejected_total` | counter | Enqueue rejections due to quota limits |
| `tsink_cluster_outbox_persistence_failures_total` | counter | Outbox persistence failures |
| `tsink_cluster_outbox_replay_attempts_total` | counter | Outbox replay attempts |
| `tsink_cluster_outbox_replay_success_total` | counter | Successful replays |
| `tsink_cluster_outbox_replay_failures_total` | counter | Failed replays |
| `tsink_cluster_outbox_queued_entries` | gauge | Pending outbox entries |
| `tsink_cluster_outbox_queued_bytes` | gauge | Pending outbox bytes |
| `tsink_cluster_outbox_log_bytes` | gauge | Outbox log file size |
| `tsink_cluster_outbox_stale_records` | gauge | Stale log records pending cleanup |
| `tsink_cluster_outbox_stalled_peers` | gauge | Peers currently stalled |
| `tsink_cluster_outbox_stalled_oldest_age_milliseconds` | gauge | Oldest stalled peer backlog age |
| `tsink_cluster_outbox_cleanup_runs_total` | counter | Cleanup worker iterations |
| `tsink_cluster_outbox_cleanup_compactions_total` | counter | Cleanup-triggered compactions |
| `tsink_cluster_outbox_cleanup_reclaimed_bytes_total` | counter | Bytes reclaimed by cleanup |
| `tsink_cluster_outbox_cleanup_failures_total` | counter | Cleanup failures |
| `tsink_cluster_outbox_stalled_alerts_total` | counter | Stalled-peer alert transitions |
| `tsink_cluster_outbox_peer_queued_entries{node_id}` | gauge | Pending entries per peer |
| `tsink_cluster_outbox_peer_queued_bytes{node_id}` | gauge | Pending bytes per peer |
| `tsink_cluster_outbox_peer_stalled{node_id}` | gauge | `1` when a peer is stalled |

### Cluster — control plane

| Metric | Type | Description |
|---|---|---|
| `tsink_cluster_control_current_term` | gauge | Current consensus term |
| `tsink_cluster_control_commit_index` | gauge | Current committed control-log index |
| `tsink_cluster_control_leader_stale` | gauge | `1` if the current leader is considered stale |
| `tsink_cluster_control_leader_contact_age_ms` | gauge | Milliseconds since last leader heartbeat |
| `tsink_cluster_control_suspect_peers` | gauge | Peers currently marked suspect |
| `tsink_cluster_control_dead_peers` | gauge | Peers currently marked dead |
| `tsink_cluster_control_peer_status{node_id,status}` | gauge | One-hot liveness status per peer (`unknown`, `healthy`, `suspect`, `dead`) |
| `tsink_cluster_control_peer_last_success_unix_ms{node_id}` | gauge | Last successful heartbeat per peer |
| `tsink_cluster_control_peer_last_failure_unix_ms{node_id}` | gauge | Last failed attempt per peer |
| `tsink_cluster_control_peer_consecutive_failures{node_id}` | gauge | Consecutive failures per peer |

### Security & RBAC

| Metric | Type | Description |
|---|---|---|
| `tsink_secret_rotation_generation{target}` | gauge | Current rotation generation per secret target |
| `tsink_secret_rotation_reload_total{target}` | counter | Secret reload operations per target |
| `tsink_secret_rotation_total{target}` | counter | Secret rotation operations per target |
| `tsink_secret_rotation_failures_total{target}` | counter | Failures per target |
| `tsink_secret_rotation_last_success_unix_ms{target}` | gauge | Last successful reload or rotation (unix ms) |
| `tsink_secret_rotation_last_failure_unix_ms{target}` | gauge | Last failure (unix ms) |
| `tsink_secret_rotation_previous_credential_active{target}` | gauge | `1` during overlap grace window |
| `tsink_rbac_service_accounts_total` | gauge | Configured RBAC service accounts |
| `tsink_rbac_service_accounts_disabled` | gauge | Disabled RBAC service accounts |
| `tsink_rbac_service_accounts_last_rotated_unix_ms` | gauge | Latest service-account rotation timestamp |

### Usage accounting

| Metric | Type | Description |
|---|---|---|
| `tsink_usage_ledger_records_total` | gauge | Durable or in-memory tenant usage ledger records |
| `tsink_usage_ledger_tenants_total` | gauge | Distinct tenants in the usage ledger |
| `tsink_usage_ledger_storage_reconciliations_total` | counter | Storage reconciliation snapshots recorded |
| `tsink_usage_ledger_durable` | gauge | `1` when the ledger is backed by a durable on-disk store |

---

## Alert recommendations

The following metrics are good starting points for alerts:

| Concern | Metric | Threshold guidance |
|---|---|---|
| WAL errors | `tsink_wal_append_errors_total` | Rate > 0 for 1 minute |
| WAL replay errors | `tsink_wal_replay_errors_total` | Any increase |
| Flush errors | `tsink_flush_pipeline_errors_total` | Rate > 0 for 2 minutes |
| Persist errors | `tsink_flush_persist_errors_total` | Rate > 0 |
| Compaction errors | `tsink_compaction_errors_total` | Rate > 0 |
| Memory pressure | `tsink_memory_used_bytes / tsink_memory_budget_bytes` | > 0.90 |
| Write admission rejections | `tsink_write_admission_rejections_total` | Rate sustained > 0 |
| Read admission rejections | `tsink_read_admission_rejections_total` | Rate sustained > 0 |
| Object-store inaccessible | `tsink_remote_storage_accessible` | == 0 for 2 minutes |
| Remote catalog backoff | `tsink_remote_storage_catalog_refresh_consecutive_failures` | > 3 |
| Dead cluster peers | `tsink_cluster_control_dead_peers` | > 0 |
| Leader stale | `tsink_cluster_control_leader_stale` | == 1 for 1 minute |
| Hinted handoff stalled | `tsink_cluster_outbox_stalled_peers` | > 0 |
| Secret rotation failure | `tsink_secret_rotation_failures_total` | Any increase |
| Rollup lag | `tsink_rollup_policy_status{kind="lag"}` | > acceptable lag threshold |

---

## Support bundles

```
GET /api/v1/admin/support_bundle?tenant=<id>
```

Downloads a bounded JSON diagnostic snapshot for a single tenant. Requires the `admin:read` RBAC permission. The response is returned as a downloadable `.json` file with a `Content-Disposition: attachment` header.

The bundle includes:

| Section | Contents |
|---|---|
| `statusTsdb` | TSDB status endpoint snapshot |
| `usage` | Tenant usage accounting summary |
| `rbacState` | Live RBAC roles, service accounts, and OIDC mappings |
| `rbacAudit` | Last 50 RBAC decision and reload audit entries |
| `securityState` | Secret rotation and TLS state |
| `clusterAudit` | Last 50 cluster admin mutation log entries |
| `clusterHandoff` | Cluster handoff progress |
| `clusterRepair` | Cluster repair progress |
| `clusterRebalance` | Shard rebalance progress |
| `rules` | Rules engine status |
| `rollups` | Rollup policy freshness and coverage |

```bash
curl -H "Authorization: Bearer $TOKEN" \
  'http://127.0.0.1:9201/api/v1/admin/support_bundle?tenant=default' \
  -o tsink-support-bundle.json
```
