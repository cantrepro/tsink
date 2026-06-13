use super::*;

mod cluster;

use self::cluster::ClusterMetrics;

#[allow(clippy::too_many_arguments)]
pub(super) fn render_metrics(
    storage: &Arc<dyn Storage>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    server_start: Instant,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    rbac_registry: Option<&RbacRegistry>,
    security_manager: Option<&SecurityManager>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let memory_used = storage.memory_used();
    let memory_budget = storage.memory_budget();
    let metrics_list = storage.list_metrics().unwrap_or_default();
    let series_count = metrics_list.len();
    let uptime = server_start.elapsed().as_secs();
    let obs = storage.observability_snapshot();
    let memory_obs = &obs.memory;
    let wal_enabled = u8::from(obs.wal.enabled);
    let cluster_write_metrics = write_routing_metrics_snapshot();
    let cluster_write_labeled_metrics = write_routing_labeled_metrics_snapshot();
    let cluster_fanout_metrics = read_fanout_metrics_snapshot();
    let cluster_fanout_labeled_metrics = read_fanout_labeled_metrics_snapshot();
    let cluster_read_planner_metrics = read_planner_metrics_snapshot();
    let cluster_read_planner_labeled_metrics = read_planner_labeled_metrics_snapshot();
    let cluster_dedupe_metrics = dedupe_metrics_snapshot();
    let cluster_outbox_metrics = outbox_metrics_snapshot();
    let read_admission_metrics = admission::read_admission_metrics_snapshot();
    let write_admission_metrics = admission::write_admission_metrics_snapshot();
    let tenant_admission_metrics = tenant::tenant_admission_metrics_snapshot();
    let exemplar_metrics =
        exemplar_store
            .metrics_snapshot()
            .unwrap_or(ExemplarStoreMetricsSnapshot {
                accepted_total: 0,
                rejected_total: 0,
                dropped_total: 0,
                query_requests_total: 0,
                query_series_total: 0,
                query_exemplars_total: 0,
                stored_series: 0,
                stored_exemplars: 0,
            });
    let payload_status = payload_status_snapshot(cluster_context);
    let otlp_status = otlp_metrics_status_snapshot();
    let legacy_ingest_status = legacy_ingest::status_snapshot();
    let edge_sync_source_status = edge_sync_context
        .map(|context| context.source_status_snapshot())
        .unwrap_or_default();
    let edge_sync_accept_status = edge_sync_context
        .map(|context| context.accept_status_snapshot())
        .unwrap_or_default();
    let cluster_outbox_peers = cluster_context
        .and_then(|context| context.outbox.as_ref())
        .map(|outbox| outbox.peer_backlog_snapshot())
        .unwrap_or_default();
    let cluster_outbox_stalled_peers = cluster_context
        .and_then(|context| context.outbox.as_ref())
        .map(|outbox| outbox.stalled_peer_snapshot())
        .unwrap_or_default();
    let cluster_control_liveness = cluster_control_liveness_snapshot(cluster_context);
    let cluster_handoff = cluster_handoff_snapshot(cluster_context);
    let cluster_digest = cluster_digest_snapshot(cluster_context);
    let cluster_rebalance = cluster_rebalance_snapshot(cluster_context);
    let cluster_hotspot =
        cluster_hotspot_snapshot_for_request(&metrics_list, cluster_context, None);
    let rules_snapshot = rules_runtime
        .and_then(|runtime| runtime.snapshot().ok())
        .unwrap_or_else(rules::empty_rules_snapshot);
    let usage_status = usage_accounting
        .map(UsageAccounting::ledger_status)
        .unwrap_or_default();

    let mut body = format!(
        "# HELP tsink_memory_used_bytes Estimated bytes counted against the configured memory budget\n\
         # TYPE tsink_memory_used_bytes gauge\n\
         tsink_memory_used_bytes {memory_used}\n\
         # HELP tsink_memory_budget_bytes Configured memory budget\n\
         # TYPE tsink_memory_budget_bytes gauge\n\
         tsink_memory_budget_bytes {memory_budget}\n\
         # HELP tsink_memory_excluded_bytes Estimated memory intentionally excluded from the configured memory budget\n\
         # TYPE tsink_memory_excluded_bytes gauge\n\
         tsink_memory_excluded_bytes {memory_excluded}\n\
         # HELP tsink_memory_registry_bytes Estimated budgeted bytes used by the in-memory series registry\n\
         # TYPE tsink_memory_registry_bytes gauge\n\
         tsink_memory_registry_bytes {memory_registry}\n\
         # HELP tsink_memory_metadata_cache_bytes Estimated budgeted bytes used by metadata caches and indexes\n\
         # TYPE tsink_memory_metadata_cache_bytes gauge\n\
         tsink_memory_metadata_cache_bytes {memory_metadata}\n\
         # HELP tsink_memory_persisted_index_bytes Estimated budgeted bytes used by persisted chunk refs and timestamp indexes\n\
         # TYPE tsink_memory_persisted_index_bytes gauge\n\
         tsink_memory_persisted_index_bytes {memory_persisted_index}\n\
         # HELP tsink_memory_persisted_mmap_bytes Estimated budgeted bytes used by persisted mmap-backed segment payloads\n\
         # TYPE tsink_memory_persisted_mmap_bytes gauge\n\
         tsink_memory_persisted_mmap_bytes {memory_persisted_mmap}\n\
         # HELP tsink_memory_tombstone_bytes Estimated budgeted bytes used by tombstone state\n\
         # TYPE tsink_memory_tombstone_bytes gauge\n\
         tsink_memory_tombstone_bytes {memory_tombstones}\n\
         # HELP tsink_series_total Number of known metric series\n\
         # TYPE tsink_series_total gauge\n\
         tsink_series_total {series_count}\n\
         # HELP tsink_uptime_seconds Server uptime in seconds\n\
         # TYPE tsink_uptime_seconds gauge\n\
         tsink_uptime_seconds {uptime}\n\
         # HELP tsink_wal_enabled WAL enabled state (1 enabled, 0 disabled)\n\
         # TYPE tsink_wal_enabled gauge\n\
         tsink_wal_enabled {wal_enabled}\n\
         # HELP tsink_wal_size_bytes WAL size on disk in bytes\n\
         # TYPE tsink_wal_size_bytes gauge\n\
         tsink_wal_size_bytes {wal_size_bytes}\n\
         # HELP tsink_wal_segments WAL segment files present\n\
         # TYPE tsink_wal_segments gauge\n\
         tsink_wal_segments {wal_segments}\n\
         # HELP tsink_wal_active_segment Current WAL segment id\n\
         # TYPE tsink_wal_active_segment gauge\n\
         tsink_wal_active_segment {wal_active_segment}\n\
         # HELP tsink_wal_acknowledged_writes_durable Whether successful writes are fsync-durable at acknowledgement time (1 durable, 0 append-only)\n\
         # TYPE tsink_wal_acknowledged_writes_durable gauge\n\
         tsink_wal_acknowledged_writes_durable {wal_acknowledged_writes_durable}\n\
         # HELP tsink_wal_highwater_segment Last appended WAL highwater segment\n\
         # TYPE tsink_wal_highwater_segment gauge\n\
         tsink_wal_highwater_segment {wal_highwater_segment}\n\
         # HELP tsink_wal_highwater_frame Last appended WAL highwater frame\n\
         # TYPE tsink_wal_highwater_frame gauge\n\
         tsink_wal_highwater_frame {wal_highwater_frame}\n\
         # HELP tsink_wal_durable_highwater_segment Last durable WAL highwater segment\n\
         # TYPE tsink_wal_durable_highwater_segment gauge\n\
         tsink_wal_durable_highwater_segment {wal_durable_highwater_segment}\n\
         # HELP tsink_wal_durable_highwater_frame Last durable WAL highwater frame\n\
         # TYPE tsink_wal_durable_highwater_frame gauge\n\
         tsink_wal_durable_highwater_frame {wal_durable_highwater_frame}\n\
         # HELP tsink_wal_replay_runs_total WAL replay runs\n\
         # TYPE tsink_wal_replay_runs_total counter\n\
         tsink_wal_replay_runs_total {wal_replay_runs_total}\n\
         # HELP tsink_wal_replay_frames_total WAL replayed frames\n\
         # TYPE tsink_wal_replay_frames_total counter\n\
         tsink_wal_replay_frames_total {wal_replay_frames_total}\n\
         # HELP tsink_wal_replay_series_definitions_total WAL replayed series definitions\n\
         # TYPE tsink_wal_replay_series_definitions_total counter\n\
         tsink_wal_replay_series_definitions_total {wal_replay_series_definitions_total}\n\
         # HELP tsink_wal_replay_sample_batches_total WAL replayed sample batches\n\
         # TYPE tsink_wal_replay_sample_batches_total counter\n\
         tsink_wal_replay_sample_batches_total {wal_replay_sample_batches_total}\n\
         # HELP tsink_wal_replay_points_total WAL replayed points\n\
         # TYPE tsink_wal_replay_points_total counter\n\
         tsink_wal_replay_points_total {wal_replay_points_total}\n\
         # HELP tsink_wal_replay_errors_total WAL replay errors\n\
         # TYPE tsink_wal_replay_errors_total counter\n\
         tsink_wal_replay_errors_total {wal_replay_errors_total}\n\
         # HELP tsink_wal_replay_duration_nanoseconds_total WAL replay runtime\n\
         # TYPE tsink_wal_replay_duration_nanoseconds_total counter\n\
         tsink_wal_replay_duration_nanoseconds_total {wal_replay_duration_nanos_total}\n\
         # HELP tsink_wal_append_series_definitions_total WAL appended series definitions\n\
         # TYPE tsink_wal_append_series_definitions_total counter\n\
         tsink_wal_append_series_definitions_total {wal_append_series_definitions_total}\n\
         # HELP tsink_wal_append_sample_batches_total WAL appended sample batches\n\
         # TYPE tsink_wal_append_sample_batches_total counter\n\
         tsink_wal_append_sample_batches_total {wal_append_sample_batches_total}\n\
         # HELP tsink_wal_append_points_total WAL appended points\n\
         # TYPE tsink_wal_append_points_total counter\n\
         tsink_wal_append_points_total {wal_append_points_total}\n\
         # HELP tsink_wal_append_bytes_total WAL appended bytes\n\
         # TYPE tsink_wal_append_bytes_total counter\n\
         tsink_wal_append_bytes_total {wal_append_bytes_total}\n\
         # HELP tsink_wal_append_errors_total WAL append errors\n\
         # TYPE tsink_wal_append_errors_total counter\n\
         tsink_wal_append_errors_total {wal_append_errors_total}\n\
         # HELP tsink_wal_resets_total WAL resets\n\
         # TYPE tsink_wal_resets_total counter\n\
         tsink_wal_resets_total {wal_resets_total}\n\
         # HELP tsink_wal_reset_errors_total WAL reset errors\n\
         # TYPE tsink_wal_reset_errors_total counter\n\
         tsink_wal_reset_errors_total {wal_reset_errors_total}\n\
         # HELP tsink_flush_pipeline_runs_total Flush pipeline runs\n\
         # TYPE tsink_flush_pipeline_runs_total counter\n\
         tsink_flush_pipeline_runs_total {flush_pipeline_runs_total}\n\
         # HELP tsink_flush_pipeline_success_total Successful flush pipeline runs\n\
         # TYPE tsink_flush_pipeline_success_total counter\n\
         tsink_flush_pipeline_success_total {flush_pipeline_success_total}\n\
         # HELP tsink_flush_pipeline_timeout_total Flush pipeline write-timeout skips\n\
         # TYPE tsink_flush_pipeline_timeout_total counter\n\
         tsink_flush_pipeline_timeout_total {flush_pipeline_timeout_total}\n\
         # HELP tsink_flush_pipeline_errors_total Flush pipeline errors\n\
         # TYPE tsink_flush_pipeline_errors_total counter\n\
         tsink_flush_pipeline_errors_total {flush_pipeline_errors_total}\n\
         # HELP tsink_flush_pipeline_duration_nanoseconds_total Flush pipeline runtime\n\
         # TYPE tsink_flush_pipeline_duration_nanoseconds_total counter\n\
         tsink_flush_pipeline_duration_nanoseconds_total {flush_pipeline_duration_nanos_total}\n\
         # HELP tsink_flush_active_runs_total Active chunk flush runs\n\
         # TYPE tsink_flush_active_runs_total counter\n\
         tsink_flush_active_runs_total {active_flush_runs_total}\n\
         # HELP tsink_flush_active_errors_total Active chunk flush errors\n\
         # TYPE tsink_flush_active_errors_total counter\n\
         tsink_flush_active_errors_total {active_flush_errors_total}\n\
         # HELP tsink_flush_active_series_total Active series flushed into sealed chunks\n\
         # TYPE tsink_flush_active_series_total counter\n\
         tsink_flush_active_series_total {active_flushed_series_total}\n\
         # HELP tsink_flush_active_chunks_total Active chunks flushed\n\
         # TYPE tsink_flush_active_chunks_total counter\n\
         tsink_flush_active_chunks_total {active_flushed_chunks_total}\n\
         # HELP tsink_flush_active_points_total Active points flushed\n\
         # TYPE tsink_flush_active_points_total counter\n\
         tsink_flush_active_points_total {active_flushed_points_total}\n\
         # HELP tsink_flush_persist_runs_total Persist attempts\n\
         # TYPE tsink_flush_persist_runs_total counter\n\
         tsink_flush_persist_runs_total {persist_runs_total}\n\
         # HELP tsink_flush_persist_success_total Successful persist runs\n\
         # TYPE tsink_flush_persist_success_total counter\n\
         tsink_flush_persist_success_total {persist_success_total}\n\
         # HELP tsink_flush_persist_noop_total Persist runs with no new chunks\n\
         # TYPE tsink_flush_persist_noop_total counter\n\
         tsink_flush_persist_noop_total {persist_noop_total}\n\
         # HELP tsink_flush_persist_errors_total Persist errors\n\
         # TYPE tsink_flush_persist_errors_total counter\n\
         tsink_flush_persist_errors_total {persist_errors_total}\n\
         # HELP tsink_flush_persisted_series_total Series persisted\n\
         # TYPE tsink_flush_persisted_series_total counter\n\
         tsink_flush_persisted_series_total {persisted_series_total}\n\
         # HELP tsink_flush_persisted_chunks_total Chunks persisted\n\
         # TYPE tsink_flush_persisted_chunks_total counter\n\
         tsink_flush_persisted_chunks_total {persisted_chunks_total}\n\
         # HELP tsink_flush_persisted_points_total Points persisted\n\
         # TYPE tsink_flush_persisted_points_total counter\n\
         tsink_flush_persisted_points_total {persisted_points_total}\n\
         # HELP tsink_flush_persisted_segments_total Segments emitted by persist\n\
         # TYPE tsink_flush_persisted_segments_total counter\n\
         tsink_flush_persisted_segments_total {persisted_segments_total}\n\
         # HELP tsink_flush_persist_duration_nanoseconds_total Persist runtime\n\
         # TYPE tsink_flush_persist_duration_nanoseconds_total counter\n\
         tsink_flush_persist_duration_nanoseconds_total {persist_duration_nanos_total}\n\
         # HELP tsink_flush_evicted_sealed_chunks_total Sealed chunks evicted after persistence\n\
         # TYPE tsink_flush_evicted_sealed_chunks_total counter\n\
         tsink_flush_evicted_sealed_chunks_total {evicted_sealed_chunks_total}\n\
         # HELP tsink_flush_tier_moves_total Persisted segments moved across storage tiers\n\
         # TYPE tsink_flush_tier_moves_total counter\n\
         tsink_flush_tier_moves_total {flush_tier_moves_total}\n\
         # HELP tsink_flush_tier_move_errors_total Persisted segment tier-move errors\n\
         # TYPE tsink_flush_tier_move_errors_total counter\n\
         tsink_flush_tier_move_errors_total {flush_tier_move_errors_total}\n\
         # HELP tsink_flush_expired_segments_total Persisted segments expired by retention\n\
         # TYPE tsink_flush_expired_segments_total counter\n\
         tsink_flush_expired_segments_total {flush_expired_segments_total}\n\
         # HELP tsink_flush_hot_segments_visible Hot-tier persisted segments visible to queries\n\
         # TYPE tsink_flush_hot_segments_visible gauge\n\
         tsink_flush_hot_segments_visible {flush_hot_segments_visible}\n\
         # HELP tsink_flush_warm_segments_visible Warm-tier persisted segments visible to queries\n\
         # TYPE tsink_flush_warm_segments_visible gauge\n\
         tsink_flush_warm_segments_visible {flush_warm_segments_visible}\n\
         # HELP tsink_flush_cold_segments_visible Cold-tier persisted segments visible to queries\n\
         # TYPE tsink_flush_cold_segments_visible gauge\n\
         tsink_flush_cold_segments_visible {flush_cold_segments_visible}\n\
         # HELP tsink_compaction_runs_total Compaction invocations\n\
         # TYPE tsink_compaction_runs_total counter\n\
         tsink_compaction_runs_total {compaction_runs_total}\n\
         # HELP tsink_compaction_success_total Compaction runs that rewrote segments\n\
         # TYPE tsink_compaction_success_total counter\n\
         tsink_compaction_success_total {compaction_success_total}\n\
         # HELP tsink_compaction_noop_total Compaction runs with no rewrite\n\
         # TYPE tsink_compaction_noop_total counter\n\
         tsink_compaction_noop_total {compaction_noop_total}\n\
         # HELP tsink_compaction_errors_total Compaction errors\n\
         # TYPE tsink_compaction_errors_total counter\n\
         tsink_compaction_errors_total {compaction_errors_total}\n\
         # HELP tsink_compaction_source_segments_total Source segments considered by compaction\n\
         # TYPE tsink_compaction_source_segments_total counter\n\
         tsink_compaction_source_segments_total {compaction_source_segments_total}\n\
         # HELP tsink_compaction_output_segments_total Output segments emitted by compaction\n\
         # TYPE tsink_compaction_output_segments_total counter\n\
         tsink_compaction_output_segments_total {compaction_output_segments_total}\n\
         # HELP tsink_compaction_source_chunks_total Source chunks considered by compaction\n\
         # TYPE tsink_compaction_source_chunks_total counter\n\
         tsink_compaction_source_chunks_total {compaction_source_chunks_total}\n\
         # HELP tsink_compaction_output_chunks_total Output chunks emitted by compaction\n\
         # TYPE tsink_compaction_output_chunks_total counter\n\
         tsink_compaction_output_chunks_total {compaction_output_chunks_total}\n\
         # HELP tsink_compaction_source_points_total Source points considered by compaction\n\
         # TYPE tsink_compaction_source_points_total counter\n\
         tsink_compaction_source_points_total {compaction_source_points_total}\n\
         # HELP tsink_compaction_output_points_total Output points emitted by compaction\n\
         # TYPE tsink_compaction_output_points_total counter\n\
         tsink_compaction_output_points_total {compaction_output_points_total}\n\
         # HELP tsink_compaction_duration_nanoseconds_total Compaction runtime\n\
         # TYPE tsink_compaction_duration_nanoseconds_total counter\n\
         tsink_compaction_duration_nanoseconds_total {compaction_duration_nanos_total}\n\
         # HELP tsink_query_select_calls_total Storage select calls\n\
         # TYPE tsink_query_select_calls_total counter\n\
         tsink_query_select_calls_total {query_select_calls_total}\n\
         # HELP tsink_query_select_errors_total Storage select errors\n\
         # TYPE tsink_query_select_errors_total counter\n\
         tsink_query_select_errors_total {query_select_errors_total}\n\
         # HELP tsink_query_select_duration_nanoseconds_total Storage select runtime\n\
         # TYPE tsink_query_select_duration_nanoseconds_total counter\n\
         tsink_query_select_duration_nanoseconds_total {query_select_duration_nanos_total}\n\
         # HELP tsink_query_select_points_returned_total Points returned by select\n\
         # TYPE tsink_query_select_points_returned_total counter\n\
         tsink_query_select_points_returned_total {query_select_points_returned_total}\n\
         # HELP tsink_query_select_with_options_calls_total Storage select_with_options calls\n\
         # TYPE tsink_query_select_with_options_calls_total counter\n\
         tsink_query_select_with_options_calls_total {query_select_with_options_calls_total}\n\
         # HELP tsink_query_select_with_options_errors_total Storage select_with_options errors\n\
         # TYPE tsink_query_select_with_options_errors_total counter\n\
         tsink_query_select_with_options_errors_total {query_select_with_options_errors_total}\n\
         # HELP tsink_query_select_with_options_duration_nanoseconds_total Storage select_with_options runtime\n\
         # TYPE tsink_query_select_with_options_duration_nanoseconds_total counter\n\
         tsink_query_select_with_options_duration_nanoseconds_total {query_select_with_options_duration_nanos_total}\n\
         # HELP tsink_query_select_with_options_points_returned_total Points returned by select_with_options\n\
         # TYPE tsink_query_select_with_options_points_returned_total counter\n\
         tsink_query_select_with_options_points_returned_total {query_select_with_options_points_returned_total}\n\
         # HELP tsink_query_select_all_calls_total Storage select_all calls\n\
         # TYPE tsink_query_select_all_calls_total counter\n\
         tsink_query_select_all_calls_total {query_select_all_calls_total}\n\
         # HELP tsink_query_select_all_errors_total Storage select_all errors\n\
         # TYPE tsink_query_select_all_errors_total counter\n\
         tsink_query_select_all_errors_total {query_select_all_errors_total}\n\
         # HELP tsink_query_select_all_duration_nanoseconds_total Storage select_all runtime\n\
         # TYPE tsink_query_select_all_duration_nanoseconds_total counter\n\
         tsink_query_select_all_duration_nanoseconds_total {query_select_all_duration_nanos_total}\n\
         # HELP tsink_query_select_all_series_returned_total Series returned by select_all\n\
         # TYPE tsink_query_select_all_series_returned_total counter\n\
         tsink_query_select_all_series_returned_total {query_select_all_series_returned_total}\n\
         # HELP tsink_query_select_all_points_returned_total Points returned by select_all\n\
         # TYPE tsink_query_select_all_points_returned_total counter\n\
         tsink_query_select_all_points_returned_total {query_select_all_points_returned_total}\n\
         # HELP tsink_query_select_series_calls_total Storage select_series calls\n\
         # TYPE tsink_query_select_series_calls_total counter\n\
         tsink_query_select_series_calls_total {query_select_series_calls_total}\n\
         # HELP tsink_query_select_series_errors_total Storage select_series errors\n\
         # TYPE tsink_query_select_series_errors_total counter\n\
         tsink_query_select_series_errors_total {query_select_series_errors_total}\n\
         # HELP tsink_query_select_series_duration_nanoseconds_total Storage select_series runtime\n\
         # TYPE tsink_query_select_series_duration_nanoseconds_total counter\n\
         tsink_query_select_series_duration_nanoseconds_total {query_select_series_duration_nanos_total}\n\
         # HELP tsink_query_select_series_returned_total Series returned by select_series\n\
         # TYPE tsink_query_select_series_returned_total counter\n\
         tsink_query_select_series_returned_total {query_select_series_returned_total}\n\
         # HELP tsink_query_merge_path_queries_total Query series collections using merge path\n\
         # TYPE tsink_query_merge_path_queries_total counter\n\
         tsink_query_merge_path_queries_total {query_merge_path_queries_total}\n\
         # HELP tsink_query_merge_path_shard_snapshots_total Merge-path shard snapshots taken before persisted decode\n\
         # TYPE tsink_query_merge_path_shard_snapshots_total counter\n\
         tsink_query_merge_path_shard_snapshots_total {query_merge_path_shard_snapshots_total}\n\
         # HELP tsink_query_merge_path_shard_snapshot_wait_nanoseconds_total Merge-path runtime waiting to acquire shard read locks for an in-memory snapshot\n\
         # TYPE tsink_query_merge_path_shard_snapshot_wait_nanoseconds_total counter\n\
         tsink_query_merge_path_shard_snapshot_wait_nanoseconds_total {query_merge_path_shard_snapshot_wait_nanos_total}\n\
         # HELP tsink_query_merge_path_shard_snapshot_hold_nanoseconds_total Merge-path runtime holding shard read locks while copying the in-memory snapshot\n\
         # TYPE tsink_query_merge_path_shard_snapshot_hold_nanoseconds_total counter\n\
         tsink_query_merge_path_shard_snapshot_hold_nanoseconds_total {query_merge_path_shard_snapshot_hold_nanos_total}\n\
         # HELP tsink_query_append_sort_path_queries_total Query series collections using append/sort path\n\
         # TYPE tsink_query_append_sort_path_queries_total counter\n\
         tsink_query_append_sort_path_queries_total {query_append_sort_path_queries_total}\n\
         # HELP tsink_query_hot_only_plans_total Query plans satisfied from the hot tier only\n\
         # TYPE tsink_query_hot_only_plans_total counter\n\
         tsink_query_hot_only_plans_total {query_hot_only_plans_total}\n\
         # HELP tsink_query_warm_tier_plans_total Query plans that include the warm tier\n\
         # TYPE tsink_query_warm_tier_plans_total counter\n\
         tsink_query_warm_tier_plans_total {query_warm_tier_plans_total}\n\
         # HELP tsink_query_cold_tier_plans_total Query plans that include the cold tier\n\
         # TYPE tsink_query_cold_tier_plans_total counter\n\
         tsink_query_cold_tier_plans_total {query_cold_tier_plans_total}\n\
         # HELP tsink_query_hot_tier_persisted_chunks_read_total Hot-tier persisted chunks decoded by queries\n\
         # TYPE tsink_query_hot_tier_persisted_chunks_read_total counter\n\
         tsink_query_hot_tier_persisted_chunks_read_total {query_hot_tier_persisted_chunks_read_total}\n\
         # HELP tsink_query_warm_tier_persisted_chunks_read_total Warm-tier persisted chunks decoded by queries\n\
         # TYPE tsink_query_warm_tier_persisted_chunks_read_total counter\n\
         tsink_query_warm_tier_persisted_chunks_read_total {query_warm_tier_persisted_chunks_read_total}\n\
         # HELP tsink_query_cold_tier_persisted_chunks_read_total Cold-tier persisted chunks decoded by queries\n\
         # TYPE tsink_query_cold_tier_persisted_chunks_read_total counter\n\
         tsink_query_cold_tier_persisted_chunks_read_total {query_cold_tier_persisted_chunks_read_total}\n\
         # HELP tsink_query_warm_tier_fetch_duration_nanoseconds_total Warm-tier persisted chunk fetch and decode time\n\
         # TYPE tsink_query_warm_tier_fetch_duration_nanoseconds_total counter\n\
         tsink_query_warm_tier_fetch_duration_nanoseconds_total {query_warm_tier_fetch_duration_nanos_total}\n\
         # HELP tsink_query_cold_tier_fetch_duration_nanoseconds_total Cold-tier persisted chunk fetch and decode time\n\
         # TYPE tsink_query_cold_tier_fetch_duration_nanoseconds_total counter\n\
         tsink_query_cold_tier_fetch_duration_nanoseconds_total {query_cold_tier_fetch_duration_nanos_total}\n\
         # HELP tsink_remote_storage_catalog_refreshes_total Remote object-store catalog refreshes\n\
         # TYPE tsink_remote_storage_catalog_refreshes_total counter\n\
         tsink_remote_storage_catalog_refreshes_total {remote_storage_catalog_refreshes_total}\n\
         # HELP tsink_remote_storage_catalog_refresh_errors_total Remote object-store catalog refresh errors\n\
         # TYPE tsink_remote_storage_catalog_refresh_errors_total counter\n\
         tsink_remote_storage_catalog_refresh_errors_total {remote_storage_catalog_refresh_errors_total}\n\
         # HELP tsink_remote_storage_catalog_refresh_consecutive_failures Consecutive remote object-store catalog refresh failures\n\
         # TYPE tsink_remote_storage_catalog_refresh_consecutive_failures gauge\n\
         tsink_remote_storage_catalog_refresh_consecutive_failures {remote_storage_catalog_refresh_consecutive_failures}\n\
         # HELP tsink_remote_storage_catalog_refresh_backoff_active Whether remote catalog refresh retry backoff is currently active\n\
         # TYPE tsink_remote_storage_catalog_refresh_backoff_active gauge\n\
         tsink_remote_storage_catalog_refresh_backoff_active {remote_storage_catalog_refresh_backoff_active}\n\
         # HELP tsink_remote_storage_accessible Whether remote object-store access is currently healthy\n\
         # TYPE tsink_remote_storage_accessible gauge\n\
         tsink_remote_storage_accessible {remote_storage_accessible}\n\
         # HELP tsink_remote_storage_mirror_hot_segments Whether hot segments are mirrored into object-store hot/\n\
         # TYPE tsink_remote_storage_mirror_hot_segments gauge\n\
         tsink_remote_storage_mirror_hot_segments {remote_storage_mirror_hot_segments}\n\
         # HELP tsink_remote_storage_compute_only Whether the local storage runtime is compute-only\n\
         # TYPE tsink_remote_storage_compute_only gauge\n\
         tsink_remote_storage_compute_only {remote_storage_compute_only}\n\
         # HELP tsink_cluster_write_requests_total Cluster write requests routed through the coordinator\n\
         # TYPE tsink_cluster_write_requests_total counter\n\
         tsink_cluster_write_requests_total {cluster_write_requests_total}\n\
         # HELP tsink_cluster_write_local_rows_total Rows inserted on local owner by write router\n\
         # TYPE tsink_cluster_write_local_rows_total counter\n\
         tsink_cluster_write_local_rows_total {cluster_write_local_rows_total}\n\
         # HELP tsink_cluster_write_routed_rows_total Rows forwarded to remote owners by write router\n\
         # TYPE tsink_cluster_write_routed_rows_total counter\n\
         tsink_cluster_write_routed_rows_total {cluster_write_routed_rows_total}\n\
         # HELP tsink_cluster_write_routed_batches_total Remote write batches sent by write router\n\
         # TYPE tsink_cluster_write_routed_batches_total counter\n\
         tsink_cluster_write_routed_batches_total {cluster_write_routed_batches_total}\n\
         # HELP tsink_cluster_write_failures_total Cluster write routing failures\n\
         # TYPE tsink_cluster_write_failures_total counter\n\
         tsink_cluster_write_failures_total {cluster_write_failures_total}\n\
         # HELP tsink_cluster_dedupe_requests_total Internal ingest idempotency key checks\n\
         # TYPE tsink_cluster_dedupe_requests_total counter\n\
         tsink_cluster_dedupe_requests_total {cluster_dedupe_requests_total}\n\
         # HELP tsink_cluster_dedupe_accepted_total Internal ingest requests accepted for dedupe tracking\n\
         # TYPE tsink_cluster_dedupe_accepted_total counter\n\
         tsink_cluster_dedupe_accepted_total {cluster_dedupe_accepted_total}\n\
         # HELP tsink_cluster_dedupe_duplicates_total Internal ingest requests deduplicated by idempotency key\n\
         # TYPE tsink_cluster_dedupe_duplicates_total counter\n\
         tsink_cluster_dedupe_duplicates_total {cluster_dedupe_duplicates_total}\n\
         # HELP tsink_cluster_dedupe_inflight_rejections_total Internal ingest idempotency conflicts while key is in-flight\n\
         # TYPE tsink_cluster_dedupe_inflight_rejections_total counter\n\
         tsink_cluster_dedupe_inflight_rejections_total {cluster_dedupe_inflight_rejections_total}\n\
         # HELP tsink_cluster_dedupe_commits_total Internal ingest dedupe marker commits\n\
         # TYPE tsink_cluster_dedupe_commits_total counter\n\
         tsink_cluster_dedupe_commits_total {cluster_dedupe_commits_total}\n\
         # HELP tsink_cluster_dedupe_aborts_total Internal ingest dedupe marker aborts after failed writes\n\
         # TYPE tsink_cluster_dedupe_aborts_total counter\n\
         tsink_cluster_dedupe_aborts_total {cluster_dedupe_aborts_total}\n\
         # HELP tsink_cluster_dedupe_cleanup_runs_total Internal ingest dedupe cleanup runs\n\
         # TYPE tsink_cluster_dedupe_cleanup_runs_total counter\n\
         tsink_cluster_dedupe_cleanup_runs_total {cluster_dedupe_cleanup_runs_total}\n\
         # HELP tsink_cluster_dedupe_expired_keys_total Internal ingest dedupe markers expired by TTL\n\
         # TYPE tsink_cluster_dedupe_expired_keys_total counter\n\
         tsink_cluster_dedupe_expired_keys_total {cluster_dedupe_expired_keys_total}\n\
         # HELP tsink_cluster_dedupe_evicted_keys_total Internal ingest dedupe markers evicted by size bound\n\
         # TYPE tsink_cluster_dedupe_evicted_keys_total counter\n\
         tsink_cluster_dedupe_evicted_keys_total {cluster_dedupe_evicted_keys_total}\n\
         # HELP tsink_cluster_dedupe_persistence_failures_total Internal ingest dedupe marker persistence failures\n\
         # TYPE tsink_cluster_dedupe_persistence_failures_total counter\n\
         tsink_cluster_dedupe_persistence_failures_total {cluster_dedupe_persistence_failures_total}\n\
         # HELP tsink_cluster_dedupe_active_keys Active internal ingest dedupe keys in window\n\
         # TYPE tsink_cluster_dedupe_active_keys gauge\n\
         tsink_cluster_dedupe_active_keys {cluster_dedupe_active_keys}\n\
         # HELP tsink_cluster_dedupe_inflight_keys Internal ingest dedupe keys currently in-flight\n\
         # TYPE tsink_cluster_dedupe_inflight_keys gauge\n\
         tsink_cluster_dedupe_inflight_keys {cluster_dedupe_inflight_keys}\n\
         # HELP tsink_cluster_dedupe_log_bytes Durable internal ingest dedupe marker log bytes on disk\n\
         # TYPE tsink_cluster_dedupe_log_bytes gauge\n\
         tsink_cluster_dedupe_log_bytes {cluster_dedupe_log_bytes}\n",
        memory_excluded = memory_obs.excluded_bytes,
        memory_registry = memory_obs.registry_bytes,
        memory_metadata = memory_obs.metadata_cache_bytes,
        memory_persisted_index = memory_obs.persisted_index_bytes,
        memory_persisted_mmap = memory_obs.persisted_mmap_bytes,
        memory_tombstones = memory_obs.tombstone_bytes,
        wal_size_bytes = obs.wal.size_bytes,
        wal_segments = obs.wal.segment_count,
        wal_active_segment = obs.wal.active_segment,
        wal_acknowledged_writes_durable = u8::from(obs.wal.acknowledged_writes_durable),
        wal_highwater_segment = obs.wal.highwater_segment,
        wal_highwater_frame = obs.wal.highwater_frame,
        wal_durable_highwater_segment = obs.wal.durable_highwater_segment,
        wal_durable_highwater_frame = obs.wal.durable_highwater_frame,
        wal_replay_runs_total = obs.wal.replay_runs_total,
        wal_replay_frames_total = obs.wal.replay_frames_total,
        wal_replay_series_definitions_total = obs.wal.replay_series_definitions_total,
        wal_replay_sample_batches_total = obs.wal.replay_sample_batches_total,
        wal_replay_points_total = obs.wal.replay_points_total,
        wal_replay_errors_total = obs.wal.replay_errors_total,
        wal_replay_duration_nanos_total = obs.wal.replay_duration_nanos_total,
        wal_append_series_definitions_total = obs.wal.append_series_definitions_total,
        wal_append_sample_batches_total = obs.wal.append_sample_batches_total,
        wal_append_points_total = obs.wal.append_points_total,
        wal_append_bytes_total = obs.wal.append_bytes_total,
        wal_append_errors_total = obs.wal.append_errors_total,
        wal_resets_total = obs.wal.resets_total,
        wal_reset_errors_total = obs.wal.reset_errors_total,
        flush_pipeline_runs_total = obs.flush.pipeline_runs_total,
        flush_pipeline_success_total = obs.flush.pipeline_success_total,
        flush_pipeline_timeout_total = obs.flush.pipeline_timeout_total,
        flush_pipeline_errors_total = obs.flush.pipeline_errors_total,
        flush_pipeline_duration_nanos_total = obs.flush.pipeline_duration_nanos_total,
        active_flush_runs_total = obs.flush.active_flush_runs_total,
        active_flush_errors_total = obs.flush.active_flush_errors_total,
        active_flushed_series_total = obs.flush.active_flushed_series_total,
        active_flushed_chunks_total = obs.flush.active_flushed_chunks_total,
        active_flushed_points_total = obs.flush.active_flushed_points_total,
        persist_runs_total = obs.flush.persist_runs_total,
        persist_success_total = obs.flush.persist_success_total,
        persist_noop_total = obs.flush.persist_noop_total,
        persist_errors_total = obs.flush.persist_errors_total,
        persisted_series_total = obs.flush.persisted_series_total,
        persisted_chunks_total = obs.flush.persisted_chunks_total,
        persisted_points_total = obs.flush.persisted_points_total,
        persisted_segments_total = obs.flush.persisted_segments_total,
        persist_duration_nanos_total = obs.flush.persist_duration_nanos_total,
        evicted_sealed_chunks_total = obs.flush.evicted_sealed_chunks_total,
        flush_tier_moves_total = obs.flush.tier_moves_total,
        flush_tier_move_errors_total = obs.flush.tier_move_errors_total,
        flush_expired_segments_total = obs.flush.expired_segments_total,
        flush_hot_segments_visible = obs.flush.hot_segments_visible,
        flush_warm_segments_visible = obs.flush.warm_segments_visible,
        flush_cold_segments_visible = obs.flush.cold_segments_visible,
        compaction_runs_total = obs.compaction.runs_total,
        compaction_success_total = obs.compaction.success_total,
        compaction_noop_total = obs.compaction.noop_total,
        compaction_errors_total = obs.compaction.errors_total,
        compaction_source_segments_total = obs.compaction.source_segments_total,
        compaction_output_segments_total = obs.compaction.output_segments_total,
        compaction_source_chunks_total = obs.compaction.source_chunks_total,
        compaction_output_chunks_total = obs.compaction.output_chunks_total,
        compaction_source_points_total = obs.compaction.source_points_total,
        compaction_output_points_total = obs.compaction.output_points_total,
        compaction_duration_nanos_total = obs.compaction.duration_nanos_total,
        query_select_calls_total = obs.query.select_calls_total,
        query_select_errors_total = obs.query.select_errors_total,
        query_select_duration_nanos_total = obs.query.select_duration_nanos_total,
        query_select_points_returned_total = obs.query.select_points_returned_total,
        query_select_with_options_calls_total = obs.query.select_with_options_calls_total,
        query_select_with_options_errors_total = obs.query.select_with_options_errors_total,
        query_select_with_options_duration_nanos_total =
            obs.query.select_with_options_duration_nanos_total,
        query_select_with_options_points_returned_total =
            obs.query.select_with_options_points_returned_total,
        query_select_all_calls_total = obs.query.select_all_calls_total,
        query_select_all_errors_total = obs.query.select_all_errors_total,
        query_select_all_duration_nanos_total = obs.query.select_all_duration_nanos_total,
        query_select_all_series_returned_total = obs.query.select_all_series_returned_total,
        query_select_all_points_returned_total = obs.query.select_all_points_returned_total,
        query_select_series_calls_total = obs.query.select_series_calls_total,
        query_select_series_errors_total = obs.query.select_series_errors_total,
        query_select_series_duration_nanos_total = obs.query.select_series_duration_nanos_total,
        query_select_series_returned_total = obs.query.select_series_returned_total,
        query_merge_path_queries_total = obs.query.merge_path_queries_total,
        query_merge_path_shard_snapshots_total = obs.query.merge_path_shard_snapshots_total,
        query_merge_path_shard_snapshot_wait_nanos_total =
            obs.query.merge_path_shard_snapshot_wait_nanos_total,
        query_merge_path_shard_snapshot_hold_nanos_total =
            obs.query.merge_path_shard_snapshot_hold_nanos_total,
        query_append_sort_path_queries_total = obs.query.append_sort_path_queries_total,
        query_hot_only_plans_total = obs.query.hot_only_query_plans_total,
        query_warm_tier_plans_total = obs.query.warm_tier_query_plans_total,
        query_cold_tier_plans_total = obs.query.cold_tier_query_plans_total,
        query_hot_tier_persisted_chunks_read_total =
            obs.query.hot_tier_persisted_chunks_read_total,
        query_warm_tier_persisted_chunks_read_total =
            obs.query.warm_tier_persisted_chunks_read_total,
        query_cold_tier_persisted_chunks_read_total =
            obs.query.cold_tier_persisted_chunks_read_total,
        query_warm_tier_fetch_duration_nanos_total =
            obs.query.warm_tier_fetch_duration_nanos_total,
        query_cold_tier_fetch_duration_nanos_total =
            obs.query.cold_tier_fetch_duration_nanos_total,
        remote_storage_catalog_refreshes_total = obs.remote.catalog_refreshes_total,
        remote_storage_catalog_refresh_errors_total = obs.remote.catalog_refresh_errors_total,
        remote_storage_catalog_refresh_consecutive_failures =
            obs.remote.consecutive_refresh_failures,
        remote_storage_catalog_refresh_backoff_active = u8::from(obs.remote.backoff_active),
        remote_storage_accessible = u8::from(obs.remote.accessible),
        remote_storage_mirror_hot_segments = u8::from(obs.remote.mirror_hot_segments),
        remote_storage_compute_only =
            u8::from(obs.remote.runtime_mode == tsink::StorageRuntimeMode::ComputeOnly),
        cluster_write_requests_total = cluster_write_metrics.requests_total,
        cluster_write_local_rows_total = cluster_write_metrics.local_rows_total,
        cluster_write_routed_rows_total = cluster_write_metrics.routed_rows_total,
        cluster_write_routed_batches_total = cluster_write_metrics.routed_batches_total,
        cluster_write_failures_total = cluster_write_metrics.failures_total,
        cluster_dedupe_requests_total = cluster_dedupe_metrics.requests_total,
        cluster_dedupe_accepted_total = cluster_dedupe_metrics.accepted_total,
        cluster_dedupe_duplicates_total = cluster_dedupe_metrics.duplicates_total,
        cluster_dedupe_inflight_rejections_total =
            cluster_dedupe_metrics.inflight_rejections_total,
        cluster_dedupe_commits_total = cluster_dedupe_metrics.commits_total,
        cluster_dedupe_aborts_total = cluster_dedupe_metrics.aborts_total,
        cluster_dedupe_cleanup_runs_total = cluster_dedupe_metrics.cleanup_runs_total,
        cluster_dedupe_expired_keys_total = cluster_dedupe_metrics.expired_keys_total,
        cluster_dedupe_evicted_keys_total = cluster_dedupe_metrics.evicted_keys_total,
        cluster_dedupe_persistence_failures_total =
            cluster_dedupe_metrics.persistence_failures_total,
        cluster_dedupe_active_keys = cluster_dedupe_metrics.active_keys,
        cluster_dedupe_inflight_keys = cluster_dedupe_metrics.inflight_keys,
        cluster_dedupe_log_bytes = cluster_dedupe_metrics.log_bytes,
    );

    cluster::append_metrics(
        &mut body,
        ClusterMetrics {
            write_labeled: &cluster_write_labeled_metrics,
            fanout: cluster_fanout_metrics,
            fanout_labeled: &cluster_fanout_labeled_metrics,
            read_planner: cluster_read_planner_metrics,
            read_planner_labeled: &cluster_read_planner_labeled_metrics,
            outbox: cluster_outbox_metrics,
            outbox_peers: &cluster_outbox_peers,
            outbox_stalled_peers: &cluster_outbox_stalled_peers,
            control: &cluster_control_liveness,
            handoff: &cluster_handoff,
            digest: &cluster_digest,
            rebalance: &cluster_rebalance,
            hotspot: &cluster_hotspot,
        },
    );
    append_exemplar_metrics(&mut body, &exemplar_metrics, exemplar_store.config());
    append_rules_metrics(&mut body, &rules_snapshot);
    append_rollup_metrics(&mut body, &obs.rollups, &obs.query);
    append_prometheus_payload_metrics(&mut body, &payload_status);
    append_otlp_metrics(&mut body, &otlp_status);
    append_legacy_ingest_metrics(&mut body, &legacy_ingest_status);
    append_edge_sync_metrics(
        &mut body,
        &edge_sync_source_status,
        &edge_sync_accept_status,
    );
    append_read_admission_metrics(&mut body, read_admission_metrics);
    append_write_admission_metrics(&mut body, write_admission_metrics);
    append_tenant_admission_metrics(&mut body, tenant_admission_metrics);
    append_security_metrics(&mut body, security_manager, rbac_registry);
    append_usage_metrics(&mut body, &usage_status);

    HttpResponse::new(200, body.into_bytes())
        .with_header("Content-Type", "text/plain; version=0.0.4")
}

fn append_security_metrics(
    body: &mut String,
    security_manager: Option<&SecurityManager>,
    rbac_registry: Option<&RbacRegistry>,
) {
    if let Some(security_manager) = security_manager {
        let snapshot = security_manager.state_snapshot(rbac_registry);
        if !snapshot.targets.is_empty() {
            body.push_str(
                "# HELP tsink_secret_rotation_generation Current secret rotation generation\n\
                 # TYPE tsink_secret_rotation_generation gauge\n\
                 # HELP tsink_secret_rotation_reload_total Secret reload operations attempted\n\
                 # TYPE tsink_secret_rotation_reload_total counter\n\
                 # HELP tsink_secret_rotation_total Secret rotation operations attempted\n\
                 # TYPE tsink_secret_rotation_total counter\n\
                 # HELP tsink_secret_rotation_failures_total Secret reload or rotation failures\n\
                 # TYPE tsink_secret_rotation_failures_total counter\n\
                 # HELP tsink_secret_rotation_last_success_unix_ms Last successful secret reload or rotation\n\
                 # TYPE tsink_secret_rotation_last_success_unix_ms gauge\n\
                 # HELP tsink_secret_rotation_last_failure_unix_ms Last failed secret reload or rotation\n\
                 # TYPE tsink_secret_rotation_last_failure_unix_ms gauge\n\
                 # HELP tsink_secret_rotation_previous_credential_active Previous credential overlap window active (1 yes, 0 no)\n\
                 # TYPE tsink_secret_rotation_previous_credential_active gauge\n",
            );
        }
        for target in &snapshot.targets {
            match target {
                crate::security::SecurityTargetSnapshot::Token(secret) => {
                    let target = secret.target.as_str();
                    body.push_str(&format!(
                        "tsink_secret_rotation_generation{{target=\"{target}\"}} {}\n\
                         tsink_secret_rotation_reload_total{{target=\"{target}\"}} {}\n\
                         tsink_secret_rotation_total{{target=\"{target}\"}} {}\n\
                         tsink_secret_rotation_failures_total{{target=\"{target}\"}} {}\n\
                         tsink_secret_rotation_last_success_unix_ms{{target=\"{target}\"}} {}\n\
                         tsink_secret_rotation_last_failure_unix_ms{{target=\"{target}\"}} {}\n\
                         tsink_secret_rotation_previous_credential_active{{target=\"{target}\"}} {}\n",
                        secret.generation,
                        secret.reloads_total,
                        secret.rotations_total,
                        secret.failures_total,
                        secret.last_success_unix_ms,
                        secret.last_failure_unix_ms,
                        u8::from(secret.accepts_previous_credential),
                    ));
                }
                crate::security::SecurityTargetSnapshot::Tls(secret) => {
                    let target = secret.target.as_str();
                    body.push_str(&format!(
                        "tsink_secret_rotation_generation{{target=\"{target}\"}} {}\n\
                         tsink_secret_rotation_reload_total{{target=\"{target}\"}} {}\n\
                         tsink_secret_rotation_total{{target=\"{target}\"}} {}\n\
                         tsink_secret_rotation_failures_total{{target=\"{target}\"}} {}\n\
                         tsink_secret_rotation_last_success_unix_ms{{target=\"{target}\"}} {}\n\
                         tsink_secret_rotation_last_failure_unix_ms{{target=\"{target}\"}} {}\n\
                         tsink_secret_rotation_previous_credential_active{{target=\"{target}\"}} 0\n",
                        secret.generation,
                        secret.reloads_total,
                        secret.rotations_total,
                        secret.failures_total,
                        secret.last_success_unix_ms,
                        secret.last_failure_unix_ms,
                    ));
                }
            }
        }
    }

    if let Some(summary) = rbac_service_account_summary(rbac_registry) {
        body.push_str(
            "# HELP tsink_rbac_service_accounts_total Configured RBAC service accounts\n\
             # TYPE tsink_rbac_service_accounts_total gauge\n",
        );
        body.push_str(&format!(
            "tsink_rbac_service_accounts_total {}\n",
            summary.total
        ));
        body.push_str(
            "# HELP tsink_rbac_service_accounts_disabled Total disabled RBAC service accounts\n\
             # TYPE tsink_rbac_service_accounts_disabled gauge\n",
        );
        body.push_str(&format!(
            "tsink_rbac_service_accounts_disabled {}\n",
            summary.disabled
        ));
        body.push_str(
            "# HELP tsink_rbac_service_accounts_last_rotated_unix_ms Latest RBAC service-account rotation timestamp\n\
             # TYPE tsink_rbac_service_accounts_last_rotated_unix_ms gauge\n",
        );
        body.push_str(&format!(
            "tsink_rbac_service_accounts_last_rotated_unix_ms {}\n",
            summary.last_rotated_unix_ms
        ));
    }
}

fn append_usage_metrics(body: &mut String, snapshot: &crate::usage::UsageLedgerStatus) {
    body.push_str(
        "# HELP tsink_usage_ledger_records_total Durable or in-memory tenant usage ledger records\n\
         # TYPE tsink_usage_ledger_records_total gauge\n",
    );
    body.push_str(&format!(
        "tsink_usage_ledger_records_total {}\n",
        snapshot.records_total
    ));
    body.push_str(
        "# HELP tsink_usage_ledger_tenants_total Distinct tenants observed in the usage ledger\n\
         # TYPE tsink_usage_ledger_tenants_total gauge\n",
    );
    body.push_str(&format!(
        "tsink_usage_ledger_tenants_total {}\n",
        snapshot.tenant_count
    ));
    body.push_str(
        "# HELP tsink_usage_ledger_storage_reconciliations_total Storage reconciliation snapshots recorded in the usage ledger\n\
         # TYPE tsink_usage_ledger_storage_reconciliations_total counter\n",
    );
    body.push_str(&format!(
        "tsink_usage_ledger_storage_reconciliations_total {}\n",
        snapshot.storage_reconciliations_total
    ));
    body.push_str(
        "# HELP tsink_usage_ledger_durable Whether usage accounting is backed by a durable on-disk ledger\n\
         # TYPE tsink_usage_ledger_durable gauge\n",
    );
    body.push_str(&format!(
        "tsink_usage_ledger_durable {}\n",
        u8::from(snapshot.durable)
    ));
}

fn append_rules_metrics(body: &mut String, snapshot: &rules::RulesStatusSnapshot) {
    body.push_str(
        "# HELP tsink_rules_scheduler_runs_total Rules scheduler ticks attempted on this node\n\
         # TYPE tsink_rules_scheduler_runs_total counter\n",
    );
    body.push_str(&format!(
        "tsink_rules_scheduler_runs_total {}\n",
        snapshot.metrics.scheduler_runs_total
    ));
    body.push_str(
        "# HELP tsink_rules_scheduler_skipped_not_leader_total Rules scheduler ticks skipped because this node is not the active cluster leader\n\
         # TYPE tsink_rules_scheduler_skipped_not_leader_total counter\n",
    );
    body.push_str(&format!(
        "tsink_rules_scheduler_skipped_not_leader_total {}\n",
        snapshot.metrics.scheduler_skipped_not_leader_total
    ));
    body.push_str(
        "# HELP tsink_rules_scheduler_skipped_inflight_total Rules scheduler ticks skipped because a previous evaluation run was still in flight\n\
         # TYPE tsink_rules_scheduler_skipped_inflight_total counter\n",
    );
    body.push_str(&format!(
        "tsink_rules_scheduler_skipped_inflight_total {}\n",
        snapshot.metrics.scheduler_skipped_inflight_total
    ));
    body.push_str(
        "# HELP tsink_rules_evaluated_total Rules evaluated by the built-in rules engine\n\
         # TYPE tsink_rules_evaluated_total counter\n",
    );
    body.push_str(&format!(
        "tsink_rules_evaluated_total {}\n",
        snapshot.metrics.evaluated_rules_total
    ));
    body.push_str(
        "# HELP tsink_rules_evaluation_failures_total Rules evaluations that ended in an error\n\
         # TYPE tsink_rules_evaluation_failures_total counter\n",
    );
    body.push_str(&format!(
        "tsink_rules_evaluation_failures_total {}\n",
        snapshot.metrics.evaluation_failures_total
    ));
    body.push_str(
        "# HELP tsink_rules_recording_rows_written_total Samples written by recording rules\n\
         # TYPE tsink_rules_recording_rows_written_total counter\n",
    );
    body.push_str(&format!(
        "tsink_rules_recording_rows_written_total {}\n",
        snapshot.metrics.recording_rows_written_total
    ));
    body.push_str(
        "# HELP tsink_rules_configured Configured rules-engine inventory and alert state gauges\n\
         # TYPE tsink_rules_configured gauge\n",
    );
    body.push_str(&format!(
        "tsink_rules_configured{{kind=\"groups\"}} {}\n",
        snapshot.metrics.configured_groups
    ));
    body.push_str(&format!(
        "tsink_rules_configured{{kind=\"rules\"}} {}\n",
        snapshot.metrics.configured_rules
    ));
    body.push_str(&format!(
        "tsink_rules_configured{{kind=\"pending_alerts\"}} {}\n",
        snapshot.metrics.pending_alerts
    ));
    body.push_str(&format!(
        "tsink_rules_configured{{kind=\"firing_alerts\"}} {}\n",
        snapshot.metrics.firing_alerts
    ));
    body.push_str(
        "# HELP tsink_rules_scheduler_active Whether this node is currently allowed to execute scheduled rule evaluations (1=yes,0=no)\n\
         # TYPE tsink_rules_scheduler_active gauge\n",
    );
    body.push_str(&format!(
        "tsink_rules_scheduler_active {}\n",
        u8::from(snapshot.metrics.local_scheduler_active)
    ));
    body.push_str(
        "# HELP tsink_rules_runtime_limits Rules runtime configuration guardrails\n\
         # TYPE tsink_rules_runtime_limits gauge\n",
    );
    body.push_str(&format!(
        "tsink_rules_runtime_limits{{kind=\"scheduler_tick_ms\"}} {}\n",
        snapshot.scheduler_tick_ms
    ));
    body.push_str(&format!(
        "tsink_rules_runtime_limits{{kind=\"max_recording_rows_per_eval\"}} {}\n",
        snapshot.max_recording_rows_per_eval
    ));
    body.push_str(&format!(
        "tsink_rules_runtime_limits{{kind=\"max_alert_instances_per_rule\"}} {}\n",
        snapshot.max_alert_instances_per_rule
    ));
}

fn append_rollup_metrics(
    body: &mut String,
    snapshot: &tsink::RollupObservabilitySnapshot,
    query: &tsink::QueryObservabilitySnapshot,
) {
    body.push_str(
        "# HELP tsink_rollup_worker_runs_total Rollup maintenance passes attempted by the background worker or admin trigger\n\
         # TYPE tsink_rollup_worker_runs_total counter\n",
    );
    body.push_str(&format!(
        "tsink_rollup_worker_runs_total {}\n",
        snapshot.worker_runs_total
    ));
    body.push_str(
        "# HELP tsink_rollup_worker_success_total Successful rollup maintenance passes\n\
         # TYPE tsink_rollup_worker_success_total counter\n",
    );
    body.push_str(&format!(
        "tsink_rollup_worker_success_total {}\n",
        snapshot.worker_success_total
    ));
    body.push_str(
        "# HELP tsink_rollup_worker_errors_total Rollup maintenance passes that ended in an error\n\
         # TYPE tsink_rollup_worker_errors_total counter\n",
    );
    body.push_str(&format!(
        "tsink_rollup_worker_errors_total {}\n",
        snapshot.worker_errors_total
    ));
    body.push_str(
        "# HELP tsink_rollup_policy_runs_total Individual rollup policy evaluations attempted\n\
         # TYPE tsink_rollup_policy_runs_total counter\n",
    );
    body.push_str(&format!(
        "tsink_rollup_policy_runs_total {}\n",
        snapshot.policy_runs_total
    ));
    body.push_str(
        "# HELP tsink_rollup_buckets_materialized_total Materialized rollup buckets written as persisted artifacts\n\
         # TYPE tsink_rollup_buckets_materialized_total counter\n",
    );
    body.push_str(&format!(
        "tsink_rollup_buckets_materialized_total {}\n",
        snapshot.buckets_materialized_total
    ));
    body.push_str(
        "# HELP tsink_rollup_points_materialized_total Materialized rollup points written as persisted artifacts\n\
         # TYPE tsink_rollup_points_materialized_total counter\n",
    );
    body.push_str(&format!(
        "tsink_rollup_points_materialized_total {}\n",
        snapshot.points_materialized_total
    ));
    body.push_str(
        "# HELP tsink_rollup_last_run_duration_nanoseconds Duration of the most recent rollup maintenance pass\n\
         # TYPE tsink_rollup_last_run_duration_nanoseconds gauge\n",
    );
    body.push_str(&format!(
        "tsink_rollup_last_run_duration_nanoseconds {}\n",
        snapshot.last_run_duration_nanos
    ));
    body.push_str(
        "# HELP tsink_query_rollup_plans_total Queries that used persisted rollup artifacts\n\
         # TYPE tsink_query_rollup_plans_total counter\n",
    );
    body.push_str(&format!(
        "tsink_query_rollup_plans_total {}\n",
        query.rollup_query_plans_total
    ));
    body.push_str(
        "# HELP tsink_query_partial_rollup_plans_total Queries that mixed persisted rollups with raw tail reads\n\
         # TYPE tsink_query_partial_rollup_plans_total counter\n",
    );
    body.push_str(&format!(
        "tsink_query_partial_rollup_plans_total {}\n",
        query.partial_rollup_query_plans_total
    ));
    body.push_str(
        "# HELP tsink_query_rollup_points_read_total Persisted rollup points read by query planning\n\
         # TYPE tsink_query_rollup_points_read_total counter\n",
    );
    body.push_str(&format!(
        "tsink_query_rollup_points_read_total {}\n",
        query.rollup_points_read_total
    ));
    body.push_str(
        "# HELP tsink_rollup_policy_status Rollup policy coverage, freshness, and runtime state\n\
         # TYPE tsink_rollup_policy_status gauge\n",
    );
    for policy in &snapshot.policies {
        let policy_id = prometheus_escape_label_value(&policy.policy.id);
        let metric = prometheus_escape_label_value(&policy.policy.metric);
        let aggregation =
            prometheus_escape_label_value(&format!("{:?}", policy.policy.aggregation));
        body.push_str(&format!(
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{}\",kind=\"matched_series\"}} {}\n",
            policy_id,
            metric,
            aggregation,
            policy.matched_series
        ));
        body.push_str(&format!(
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{}\",kind=\"materialized_series\"}} {}\n",
            policy_id,
            metric,
            aggregation,
            policy.materialized_series
        ));
        body.push_str(&format!(
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{}\",kind=\"interval\"}} {}\n",
            policy_id,
            metric,
            aggregation,
            policy.policy.interval
        ));
        body.push_str(&format!(
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{}\",kind=\"materialized_through\"}} {}\n",
            policy_id,
            metric,
            aggregation,
            policy.materialized_through.unwrap_or(0)
        ));
        body.push_str(&format!(
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{}\",kind=\"lag\"}} {}\n",
            policy_id,
            metric,
            aggregation,
            policy.lag.unwrap_or(0)
        ));
        body.push_str(&format!(
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{}\",kind=\"last_run_duration_nanos\"}} {}\n",
            policy_id,
            metric,
            aggregation,
            policy.last_run_duration_nanos
        ));
        body.push_str(&format!(
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{}\",kind=\"last_run_started_at_ms\"}} {}\n",
            policy_id,
            metric,
            aggregation,
            policy.last_run_started_at_ms.unwrap_or(0)
        ));
        body.push_str(&format!(
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{}\",kind=\"last_run_completed_at_ms\"}} {}\n",
            policy_id,
            metric,
            aggregation,
            policy.last_run_completed_at_ms.unwrap_or(0)
        ));
    }
}

fn append_exemplar_metrics(
    body: &mut String,
    snapshot: &ExemplarStoreMetricsSnapshot,
    config: ExemplarStoreConfig,
) {
    body.push_str(
        "# HELP tsink_exemplars_accepted_total Exemplars accepted into the bounded exemplar store\n\
         # TYPE tsink_exemplars_accepted_total counter\n",
    );
    body.push_str(&format!(
        "tsink_exemplars_accepted_total {}\n",
        snapshot.accepted_total
    ));
    body.push_str(
        "# HELP tsink_exemplars_rejected_total Exemplars rejected before storage due to unsupported payloads or configured quotas\n\
         # TYPE tsink_exemplars_rejected_total counter\n",
    );
    body.push_str(&format!(
        "tsink_exemplars_rejected_total {}\n",
        snapshot.rejected_total
    ));
    body.push_str(
        "# HELP tsink_exemplars_dropped_total Exemplars dropped from the bounded exemplar store due to retention guardrails\n\
         # TYPE tsink_exemplars_dropped_total counter\n",
    );
    body.push_str(&format!(
        "tsink_exemplars_dropped_total {}\n",
        snapshot.dropped_total
    ));
    body.push_str(
        "# HELP tsink_exemplars_query_requests_total Exemplar query requests served by the exemplar store\n\
         # TYPE tsink_exemplars_query_requests_total counter\n",
    );
    body.push_str(&format!(
        "tsink_exemplars_query_requests_total {}\n",
        snapshot.query_requests_total
    ));
    body.push_str(
        "# HELP tsink_exemplars_query_series_total Exemplar series returned by exemplar queries\n\
         # TYPE tsink_exemplars_query_series_total counter\n",
    );
    body.push_str(&format!(
        "tsink_exemplars_query_series_total {}\n",
        snapshot.query_series_total
    ));
    body.push_str(
        "# HELP tsink_exemplars_query_results_total Exemplars returned by exemplar queries\n\
         # TYPE tsink_exemplars_query_results_total counter\n",
    );
    body.push_str(&format!(
        "tsink_exemplars_query_results_total {}\n",
        snapshot.query_exemplars_total
    ));
    body.push_str(
        "# HELP tsink_exemplars_stored Series currently present in the exemplar sidecar store\n\
         # TYPE tsink_exemplars_stored gauge\n",
    );
    body.push_str(&format!(
        "tsink_exemplars_stored{{kind=\"series\"}} {}\n",
        snapshot.stored_series
    ));
    body.push_str(&format!(
        "tsink_exemplars_stored{{kind=\"exemplars\"}} {}\n",
        snapshot.stored_exemplars
    ));
    body.push_str(
        "# HELP tsink_exemplar_limits Configured exemplar request, query, and storage guardrails\n\
         # TYPE tsink_exemplar_limits gauge\n",
    );
    body.push_str(&format!(
        "tsink_exemplar_limits{{kind=\"max_total\"}} {}\n",
        config.max_total_exemplars
    ));
    body.push_str(&format!(
        "tsink_exemplar_limits{{kind=\"max_per_series\"}} {}\n",
        config.max_exemplars_per_series
    ));
    body.push_str(&format!(
        "tsink_exemplar_limits{{kind=\"max_per_request\"}} {}\n",
        config.max_exemplars_per_request
    ));
    body.push_str(&format!(
        "tsink_exemplar_limits{{kind=\"max_query_results\"}} {}\n",
        config.max_query_results
    ));
    body.push_str(&format!(
        "tsink_exemplar_limits{{kind=\"max_query_selectors\"}} {}\n",
        config.max_query_selectors
    ));
}

fn append_prometheus_payload_metrics(
    body: &mut String,
    snapshot: &PrometheusPayloadStatusSnapshot,
) {
    body.push_str(
        "# HELP tsink_prometheus_payload_feature_enabled Expanded Prometheus payload feature flags (1 enabled, 0 disabled)\n\
         # TYPE tsink_prometheus_payload_feature_enabled gauge\n",
    );
    for (payload, enabled) in [
        ("metadata", snapshot.metadata.enabled),
        ("exemplar", snapshot.exemplars.enabled),
        ("histogram", snapshot.histograms.enabled),
    ] {
        body.push_str(&format!(
            "tsink_prometheus_payload_feature_enabled{{payload=\"{payload}\"}} {}\n",
            u8::from(enabled)
        ));
    }

    body.push_str(
        "# HELP tsink_prometheus_payload_accepted_total Expanded Prometheus payload items accepted by this node\n\
         # TYPE tsink_prometheus_payload_accepted_total counter\n",
    );
    for (payload, counters) in [
        ("metadata", &snapshot.metadata),
        ("exemplar", &snapshot.exemplars),
        ("histogram", &snapshot.histograms),
    ] {
        body.push_str(&format!(
            "tsink_prometheus_payload_accepted_total{{payload=\"{payload}\"}} {}\n",
            counters.accepted_total
        ));
    }

    body.push_str(
        "# HELP tsink_prometheus_payload_rejected_total Expanded Prometheus payload items rejected by this node\n\
         # TYPE tsink_prometheus_payload_rejected_total counter\n",
    );
    for (payload, counters) in [
        ("metadata", &snapshot.metadata),
        ("exemplar", &snapshot.exemplars),
        ("histogram", &snapshot.histograms),
    ] {
        body.push_str(&format!(
            "tsink_prometheus_payload_rejected_total{{payload=\"{payload}\"}} {}\n",
            counters.rejected_total
        ));
    }

    body.push_str(
        "# HELP tsink_prometheus_payload_throttled_total Expanded Prometheus payload items throttled by request guardrails\n\
         # TYPE tsink_prometheus_payload_throttled_total counter\n",
    );
    for (payload, counters) in [
        ("metadata", &snapshot.metadata),
        ("exemplar", &snapshot.exemplars),
        ("histogram", &snapshot.histograms),
    ] {
        body.push_str(&format!(
            "tsink_prometheus_payload_throttled_total{{payload=\"{payload}\"}} {}\n",
            counters.throttled_total
        ));
    }

    body.push_str(
        "# HELP tsink_prometheus_payload_capability_required Required compatibility capability markers for expanded Prometheus payloads\n\
         # TYPE tsink_prometheus_payload_capability_required gauge\n",
    );
    for (payload, capabilities) in [
        ("metadata", &snapshot.metadata.required_capabilities),
        ("exemplar", &snapshot.exemplars.required_capabilities),
        ("histogram", &snapshot.histograms.required_capabilities),
    ] {
        for capability in capabilities {
            let capability = prometheus_escape_label_value(capability);
            body.push_str(&format!(
                "tsink_prometheus_payload_capability_required{{payload=\"{payload}\",capability=\"{capability}\"}} 1\n"
            ));
        }
    }

    body.push_str(
        "# HELP tsink_cluster_capability_enabled Local cluster capability markers exported by this node\n\
         # TYPE tsink_cluster_capability_enabled gauge\n",
    );
    for capability in &snapshot.local_capabilities {
        let capability = prometheus_escape_label_value(capability);
        body.push_str(&format!(
            "tsink_cluster_capability_enabled{{capability=\"{capability}\"}} 1\n"
        ));
    }

    body.push_str(
        "# HELP tsink_prometheus_payload_limits Expanded Prometheus payload request guardrails\n\
         # TYPE tsink_prometheus_payload_limits gauge\n",
    );
    if let Some(limit) = snapshot.metadata.max_per_request {
        body.push_str(&format!(
            "tsink_prometheus_payload_limits{{payload=\"metadata\",kind=\"max_per_request\"}} {}\n",
            limit
        ));
    }
    if let Some(limit) = snapshot.histograms.max_bucket_entries_per_request {
        body.push_str(&format!(
            "tsink_prometheus_payload_limits{{payload=\"histogram\",kind=\"max_bucket_entries_per_request\"}} {}\n",
            limit
        ));
    }
}

fn append_otlp_metrics(body: &mut String, snapshot: &OtlpMetricsStatusSnapshot) {
    body.push_str(
        "# HELP tsink_otlp_metrics_enabled OTLP metrics ingest feature flag (1 enabled, 0 disabled)\n\
         # TYPE tsink_otlp_metrics_enabled gauge\n",
    );
    body.push_str(&format!(
        "tsink_otlp_metrics_enabled {}\n",
        u8::from(snapshot.enabled)
    ));

    body.push_str(
        "# HELP tsink_otlp_requests_total OTLP /v1/metrics requests accepted or rejected by this node\n\
         # TYPE tsink_otlp_requests_total counter\n",
    );
    body.push_str(&format!(
        "tsink_otlp_requests_total{{outcome=\"accepted\"}} {}\n",
        snapshot.accepted_requests_total
    ));
    body.push_str(&format!(
        "tsink_otlp_requests_total{{outcome=\"rejected\"}} {}\n",
        snapshot.rejected_requests_total
    ));

    body.push_str(
        "# HELP tsink_otlp_data_points_total OTLP data points accepted or rejected by metric kind\n\
         # TYPE tsink_otlp_data_points_total counter\n",
    );
    for (kind, counters) in [
        ("gauge", snapshot.gauges),
        ("sum", snapshot.sums),
        ("histogram", snapshot.histograms),
        ("summary", snapshot.summaries),
        ("exponential_histogram", snapshot.exponential_histograms),
    ] {
        body.push_str(&format!(
            "tsink_otlp_data_points_total{{kind=\"{kind}\",outcome=\"accepted\"}} {}\n",
            counters.accepted_total
        ));
        body.push_str(&format!(
            "tsink_otlp_data_points_total{{kind=\"{kind}\",outcome=\"rejected\"}} {}\n",
            counters.rejected_total
        ));
    }

    body.push_str(
        "# HELP tsink_otlp_exemplars_total OTLP exemplars accepted or rejected by this node\n\
         # TYPE tsink_otlp_exemplars_total counter\n",
    );
    body.push_str(&format!(
        "tsink_otlp_exemplars_total{{outcome=\"accepted\"}} {}\n",
        snapshot.accepted_exemplars_total
    ));
    body.push_str(&format!(
        "tsink_otlp_exemplars_total{{outcome=\"rejected\"}} {}\n",
        snapshot.rejected_exemplars_total
    ));

    body.push_str(
        "# HELP tsink_otlp_supported_shape OTLP metric shapes supported by /v1/metrics\n\
         # TYPE tsink_otlp_supported_shape gauge\n",
    );
    for shape in &snapshot.supported_shapes {
        let shape = prometheus_escape_label_value(shape);
        body.push_str(&format!(
            "tsink_otlp_supported_shape{{shape=\"{shape}\"}} 1\n"
        ));
    }
}

fn append_legacy_ingest_metrics(body: &mut String, snapshot: &LegacyIngestStatusSnapshot) {
    body.push_str(
        "# HELP tsink_legacy_ingest_enabled Legacy protocol adapter availability on this node (1 enabled, 0 disabled)\n\
         # TYPE tsink_legacy_ingest_enabled gauge\n",
    );
    for (adapter, enabled) in [
        ("influx_line_protocol", snapshot.influx.enabled),
        ("statsd", snapshot.statsd.enabled),
        ("graphite", snapshot.graphite.enabled),
    ] {
        body.push_str(&format!(
            "tsink_legacy_ingest_enabled{{adapter=\"{adapter}\"}} {}\n",
            u8::from(enabled)
        ));
    }

    body.push_str(
        "# HELP tsink_legacy_ingest_requests_total Legacy protocol adapter requests accepted, rejected, or throttled by this node\n\
         # TYPE tsink_legacy_ingest_requests_total counter\n",
    );
    append_legacy_request_metrics(body, "influx_line_protocol", snapshot.influx.counters);
    append_legacy_request_metrics(body, "statsd", snapshot.statsd.counters);
    append_legacy_request_metrics(body, "graphite", snapshot.graphite.counters);

    body.push_str(
        "# HELP tsink_legacy_ingest_samples_total Legacy protocol adapter samples accepted or rejected by this node\n\
         # TYPE tsink_legacy_ingest_samples_total counter\n",
    );
    append_legacy_sample_metrics(body, "influx_line_protocol", snapshot.influx.counters);
    append_legacy_sample_metrics(body, "statsd", snapshot.statsd.counters);
    append_legacy_sample_metrics(body, "graphite", snapshot.graphite.counters);

    body.push_str(
        "# HELP tsink_legacy_ingest_limits Legacy protocol adapter request and listener guardrails\n\
         # TYPE tsink_legacy_ingest_limits gauge\n",
    );
    body.push_str(&format!(
        "tsink_legacy_ingest_limits{{adapter=\"influx_line_protocol\",kind=\"max_lines_per_request\"}} {}\n",
        snapshot.influx.max_lines_per_request
    ));
    body.push_str(&format!(
        "tsink_legacy_ingest_limits{{adapter=\"statsd\",kind=\"max_packet_bytes\"}} {}\n",
        snapshot.statsd.max_packet_bytes
    ));
    body.push_str(&format!(
        "tsink_legacy_ingest_limits{{adapter=\"statsd\",kind=\"max_events_per_packet\"}} {}\n",
        snapshot.statsd.max_events_per_packet
    ));
    body.push_str(&format!(
        "tsink_legacy_ingest_limits{{adapter=\"graphite\",kind=\"max_line_bytes\"}} {}\n",
        snapshot.graphite.max_line_bytes
    ));
}

fn append_edge_sync_metrics(
    body: &mut String,
    source: &edge_sync::EdgeSyncSourceStatusSnapshot,
    accept: &edge_sync::EdgeSyncAcceptStatusSnapshot,
) {
    body.push_str(
        "# HELP tsink_edge_sync_enabled Edge sync source and accept mode enablement on this node\n\
         # TYPE tsink_edge_sync_enabled gauge\n",
    );
    body.push_str(&format!(
        "tsink_edge_sync_enabled{{role=\"source\"}} {}\n",
        u8::from(source.enabled)
    ));
    body.push_str(&format!(
        "tsink_edge_sync_enabled{{role=\"accept\"}} {}\n",
        u8::from(accept.enabled)
    ));

    body.push_str(
        "# HELP tsink_edge_sync_queue Gauge view of the local edge sync backlog and retention window\n\
         # TYPE tsink_edge_sync_queue gauge\n",
    );
    body.push_str(&format!(
        "tsink_edge_sync_queue{{kind=\"queued_entries\"}} {}\n",
        source.queued_entries
    ));
    body.push_str(&format!(
        "tsink_edge_sync_queue{{kind=\"queued_bytes\"}} {}\n",
        source.queued_bytes
    ));
    body.push_str(&format!(
        "tsink_edge_sync_queue{{kind=\"log_bytes\"}} {}\n",
        source.log_bytes
    ));
    body.push_str(&format!(
        "tsink_edge_sync_queue{{kind=\"oldest_queued_age_ms\"}} {}\n",
        source.oldest_queued_age_ms.unwrap_or(0)
    ));
    body.push_str(&format!(
        "tsink_edge_sync_queue{{kind=\"pre_ack_retention_secs\"}} {}\n",
        source.pre_ack_retention_secs
    ));

    body.push_str(
        "# HELP tsink_edge_sync_events_total Edge sync enqueue, replay, and retention-drop counters\n\
         # TYPE tsink_edge_sync_events_total counter\n",
    );
    for (event, value) in [
        ("enqueued", source.enqueued_total),
        ("enqueue_rejected", source.enqueue_rejected_total),
        ("replay_attempt", source.replay_attempts_total),
        ("replay_success", source.replay_success_total),
        ("replay_failure", source.replay_failures_total),
        ("expired_entry", source.expired_entries_total),
    ] {
        body.push_str(&format!(
            "tsink_edge_sync_events_total{{event=\"{event}\"}} {value}\n"
        ));
    }

    body.push_str(
        "# HELP tsink_edge_sync_replayed_rows_total Rows successfully replayed upstream by edge sync\n\
         # TYPE tsink_edge_sync_replayed_rows_total counter\n",
    );
    body.push_str(&format!(
        "tsink_edge_sync_replayed_rows_total {}\n",
        source.replayed_rows_total
    ));

    body.push_str(
        "# HELP tsink_edge_sync_accept_dedupe Edge sync accept-side idempotency window gauges and counters\n\
         # TYPE tsink_edge_sync_accept_dedupe gauge\n",
    );
    for (kind, value) in [
        ("window_secs", accept.dedupe_window_secs),
        ("max_entries", accept.max_entries as u64),
        ("max_log_bytes", accept.max_log_bytes),
        ("cleanup_interval_secs", accept.cleanup_interval_secs),
        ("active_keys", accept.active_keys),
        ("inflight_keys", accept.inflight_keys),
        ("log_bytes", accept.log_bytes),
    ] {
        body.push_str(&format!(
            "tsink_edge_sync_accept_dedupe{{kind=\"{kind}\"}} {value}\n"
        ));
    }
}

fn append_legacy_request_metrics(
    body: &mut String,
    adapter: &str,
    counters: AdapterCounterSnapshot,
) {
    body.push_str(&format!(
        "tsink_legacy_ingest_requests_total{{adapter=\"{adapter}\",outcome=\"accepted\"}} {}\n",
        counters.accepted_requests_total
    ));
    body.push_str(&format!(
        "tsink_legacy_ingest_requests_total{{adapter=\"{adapter}\",outcome=\"rejected\"}} {}\n",
        counters.rejected_requests_total
    ));
    body.push_str(&format!(
        "tsink_legacy_ingest_requests_total{{adapter=\"{adapter}\",outcome=\"throttled\"}} {}\n",
        counters.throttled_requests_total
    ));
}

fn append_legacy_sample_metrics(
    body: &mut String,
    adapter: &str,
    counters: AdapterCounterSnapshot,
) {
    body.push_str(&format!(
        "tsink_legacy_ingest_samples_total{{adapter=\"{adapter}\",outcome=\"accepted\"}} {}\n",
        counters.accepted_samples_total
    ));
    body.push_str(&format!(
        "tsink_legacy_ingest_samples_total{{adapter=\"{adapter}\",outcome=\"rejected\"}} {}\n",
        counters.rejected_samples_total
    ));
}

fn append_write_admission_metrics(body: &mut String, snapshot: WriteAdmissionMetricsSnapshot) {
    body.push_str(
        "# HELP tsink_write_admission_rejections_total Public write admission rejections across request-slot, row-budget, and oversize guardrails\n\
         # TYPE tsink_write_admission_rejections_total counter\n",
    );
    body.push_str(&format!(
        "tsink_write_admission_rejections_total {}\n",
        snapshot.rejections_total
    ));
    body.push_str(
        "# HELP tsink_write_admission_request_slot_rejections_total Public write request-slot admission rejections due to global concurrency saturation\n\
         # TYPE tsink_write_admission_request_slot_rejections_total counter\n",
    );
    body.push_str(&format!(
        "tsink_write_admission_request_slot_rejections_total {}\n",
        snapshot.request_slot_rejections_total
    ));
    body.push_str(
        "# HELP tsink_write_admission_row_budget_rejections_total Public write row-budget admission rejections due to global in-flight row saturation\n\
         # TYPE tsink_write_admission_row_budget_rejections_total counter\n",
    );
    body.push_str(&format!(
        "tsink_write_admission_row_budget_rejections_total {}\n",
        snapshot.row_budget_rejections_total
    ));
    body.push_str(
        "# HELP tsink_write_admission_oversize_rows_rejections_total Public write rejections for single requests that exceed the global row budget\n\
         # TYPE tsink_write_admission_oversize_rows_rejections_total counter\n",
    );
    body.push_str(&format!(
        "tsink_write_admission_oversize_rows_rejections_total {}\n",
        snapshot.oversize_rows_rejections_total
    ));
    body.push_str(
        "# HELP tsink_write_admission_acquire_wait_nanoseconds_total Total wait time for admitted public writes to acquire request-slot and row-budget permits\n\
         # TYPE tsink_write_admission_acquire_wait_nanoseconds_total counter\n",
    );
    body.push_str(&format!(
        "tsink_write_admission_acquire_wait_nanoseconds_total {}\n",
        snapshot.acquire_wait_nanos_total
    ));
    body.push_str(
        "# HELP tsink_write_admission_active_requests Active public write requests currently holding global admission request slots\n\
         # TYPE tsink_write_admission_active_requests gauge\n",
    );
    body.push_str(&format!(
        "tsink_write_admission_active_requests {}\n",
        snapshot.active_requests
    ));
    body.push_str(
        "# HELP tsink_write_admission_active_rows Active public write rows currently reserved against the global admission budget\n\
         # TYPE tsink_write_admission_active_rows gauge\n",
    );
    body.push_str(&format!(
        "tsink_write_admission_active_rows {}\n",
        snapshot.active_rows
    ));
}

fn append_read_admission_metrics(body: &mut String, snapshot: ReadAdmissionMetricsSnapshot) {
    body.push_str(
        "# HELP tsink_read_admission_rejections_total Public read admission rejections across request-slot, query-budget, and oversize guardrails\n\
         # TYPE tsink_read_admission_rejections_total counter\n",
    );
    body.push_str(&format!(
        "tsink_read_admission_rejections_total {}\n",
        snapshot.rejections_total
    ));
    body.push_str(
        "# HELP tsink_read_admission_request_slot_rejections_total Public read request-slot admission rejections due to global concurrency saturation\n\
         # TYPE tsink_read_admission_request_slot_rejections_total counter\n",
    );
    body.push_str(&format!(
        "tsink_read_admission_request_slot_rejections_total {}\n",
        snapshot.request_slot_rejections_total
    ));
    body.push_str(
        "# HELP tsink_read_admission_query_budget_rejections_total Public read query-budget admission rejections due to global in-flight query saturation\n\
         # TYPE tsink_read_admission_query_budget_rejections_total counter\n",
    );
    body.push_str(&format!(
        "tsink_read_admission_query_budget_rejections_total {}\n",
        snapshot.query_budget_rejections_total
    ));
    body.push_str(
        "# HELP tsink_read_admission_oversize_queries_rejections_total Public read rejections for single requests that exceed the global query budget\n\
         # TYPE tsink_read_admission_oversize_queries_rejections_total counter\n",
    );
    body.push_str(&format!(
        "tsink_read_admission_oversize_queries_rejections_total {}\n",
        snapshot.oversize_queries_rejections_total
    ));
    body.push_str(
        "# HELP tsink_read_admission_acquire_wait_nanoseconds_total Total wait time for admitted public reads to acquire request-slot and query-budget permits\n\
         # TYPE tsink_read_admission_acquire_wait_nanoseconds_total counter\n",
    );
    body.push_str(&format!(
        "tsink_read_admission_acquire_wait_nanoseconds_total {}\n",
        snapshot.acquire_wait_nanos_total
    ));
    body.push_str(
        "# HELP tsink_read_admission_active_requests Active public read requests currently holding global admission request slots\n\
         # TYPE tsink_read_admission_active_requests gauge\n",
    );
    body.push_str(&format!(
        "tsink_read_admission_active_requests {}\n",
        snapshot.active_requests
    ));
    body.push_str(
        "# HELP tsink_read_admission_active_queries Active public read query units currently reserved against the global admission budget\n\
         # TYPE tsink_read_admission_active_queries gauge\n",
    );
    body.push_str(&format!(
        "tsink_read_admission_active_queries {}\n",
        snapshot.active_queries
    ));
}

fn append_tenant_admission_metrics(
    body: &mut String,
    snapshot: tenant::TenantAdmissionMetricsSnapshot,
) {
    body.push_str(
        "# HELP tsink_tenant_admission_read_rejections_total Tenant-scoped read admission rejections from per-tenant in-flight limits\n\
         # TYPE tsink_tenant_admission_read_rejections_total counter\n",
    );
    body.push_str(&format!(
        "tsink_tenant_admission_read_rejections_total {}\n",
        snapshot.read_rejections_total
    ));
    body.push_str(
        "# HELP tsink_tenant_admission_write_rejections_total Tenant-scoped write admission rejections from per-tenant in-flight limits\n\
         # TYPE tsink_tenant_admission_write_rejections_total counter\n",
    );
    body.push_str(&format!(
        "tsink_tenant_admission_write_rejections_total {}\n",
        snapshot.write_rejections_total
    ));
    body.push_str(
        "# HELP tsink_tenant_admission_active_reads Active tenant-scoped read requests currently holding per-tenant admission permits\n\
         # TYPE tsink_tenant_admission_active_reads gauge\n",
    );
    body.push_str(&format!(
        "tsink_tenant_admission_active_reads {}\n",
        snapshot.active_reads
    ));
    body.push_str(
        "# HELP tsink_tenant_admission_active_writes Active tenant-scoped write requests currently holding per-tenant admission permits\n\
         # TYPE tsink_tenant_admission_active_writes gauge\n",
    );
    body.push_str(&format!(
        "tsink_tenant_admission_active_writes {}\n",
        snapshot.active_writes
    ));
    body.push_str(
        "# HELP tsink_tenant_admission_surface_rejections_total Tenant-scoped admission rejections by protected surface\n\
         # TYPE tsink_tenant_admission_surface_rejections_total counter\n",
    );
    for (surface, value) in [
        ("ingest", snapshot.ingest_rejections_total),
        ("query", snapshot.query_rejections_total),
        ("metadata", snapshot.metadata_rejections_total),
        ("retention", snapshot.retention_rejections_total),
    ] {
        body.push_str(&format!(
            "tsink_tenant_admission_surface_rejections_total{{surface=\"{surface}\"}} {value}\n"
        ));
    }
    body.push_str(
        "# HELP tsink_tenant_admission_surface_active_requests Active tenant-scoped requests by protected surface\n\
         # TYPE tsink_tenant_admission_surface_active_requests gauge\n",
    );
    for (surface, value) in [
        ("ingest", snapshot.ingest_active_requests),
        ("query", snapshot.query_active_requests),
        ("metadata", snapshot.metadata_active_requests),
        ("retention", snapshot.retention_active_requests),
    ] {
        body.push_str(&format!(
            "tsink_tenant_admission_surface_active_requests{{surface=\"{surface}\"}} {value}\n"
        ));
    }
    body.push_str(
        "# HELP tsink_tenant_admission_surface_active_units Active tenant-scoped resource units reserved by protected surface\n\
         # TYPE tsink_tenant_admission_surface_active_units gauge\n",
    );
    for (surface, value) in [
        ("ingest", snapshot.ingest_active_units),
        ("query", snapshot.query_active_units),
        ("metadata", snapshot.metadata_active_units),
        ("retention", snapshot.retention_active_units),
    ] {
        body.push_str(&format!(
            "tsink_tenant_admission_surface_active_units{{surface=\"{surface}\"}} {value}\n"
        ));
    }
}

fn prometheus_escape_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}
