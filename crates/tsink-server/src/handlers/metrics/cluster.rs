use super::*;

pub(super) struct ClusterMetrics<'a> {
    pub write_labeled: &'a WriteRoutingLabeledMetricsSnapshot,
    pub fanout: ReadFanoutMetricsSnapshot,
    pub fanout_labeled: &'a ReadFanoutLabeledMetricsSnapshot,
    pub read_planner: ReadPlannerMetricsSnapshot,
    pub read_planner_labeled: &'a ReadPlannerLabeledMetricsSnapshot,
    pub outbox: OutboxMetricsSnapshot,
    pub outbox_peers: &'a [crate::cluster::outbox::OutboxPeerBacklogSnapshot],
    pub outbox_stalled_peers: &'a [OutboxStalledPeerSnapshot],
    pub control: &'a ControlLivenessSnapshot,
    pub handoff: &'a ClusterHandoffSnapshot,
    pub digest: &'a DigestExchangeSnapshot,
    pub rebalance: &'a RebalanceSchedulerSnapshot,
    pub hotspot: &'a ClusterHotspotSnapshot,
}

pub(super) fn append_metrics(body: &mut String, metrics: ClusterMetrics<'_>) {
    append_cluster_write_labeled_metrics(body, metrics.write_labeled);
    append_cluster_fanout_metrics(body, metrics.fanout, metrics.fanout_labeled);
    append_cluster_read_planner_metrics(body, metrics.read_planner, metrics.read_planner_labeled);
    append_cluster_outbox_metrics(
        body,
        metrics.outbox,
        metrics.outbox_peers,
        metrics.outbox_stalled_peers,
    );
    append_cluster_control_metrics(body, metrics.control);
    append_cluster_handoff_metrics(body, metrics.handoff);
    append_cluster_digest_metrics(body, metrics.digest);
    append_cluster_rebalance_metrics(body, metrics.rebalance);
    append_cluster_hotspot_metrics(body, metrics.hotspot);
}

fn append_cluster_write_labeled_metrics(
    body: &mut String,
    snapshot: &WriteRoutingLabeledMetricsSnapshot,
) {
    body.push_str(
        "# HELP tsink_cluster_write_shard_rows_total Rows routed by shard id\n\
         # TYPE tsink_cluster_write_shard_rows_total counter\n",
    );
    for shard in &snapshot.shards {
        body.push_str(&format!(
            "tsink_cluster_write_shard_rows_total{{shard=\"{}\"}} {}\n",
            shard.shard, shard.rows_total
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_write_peer_routed_rows_total Rows routed to remote owner by peer\n\
         # TYPE tsink_cluster_write_peer_routed_rows_total counter\n",
    );
    for peer in &snapshot.peers {
        let node_id = prometheus_escape_label_value(&peer.node_id);
        body.push_str(&format!(
            "tsink_cluster_write_peer_routed_rows_total{{node_id=\"{node_id}\"}} {}\n",
            peer.routed_rows_total
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_write_peer_routed_batches_total Batches routed to remote owner by peer\n\
         # TYPE tsink_cluster_write_peer_routed_batches_total counter\n",
    );
    for peer in &snapshot.peers {
        let node_id = prometheus_escape_label_value(&peer.node_id);
        body.push_str(&format!(
            "tsink_cluster_write_peer_routed_batches_total{{node_id=\"{node_id}\"}} {}\n",
            peer.routed_batches_total
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_write_remote_requests_total Remote write RPC requests by peer\n\
         # TYPE tsink_cluster_write_remote_requests_total counter\n",
    );
    for peer in &snapshot.peers {
        let node_id = prometheus_escape_label_value(&peer.node_id);
        body.push_str(&format!(
            "tsink_cluster_write_remote_requests_total{{node_id=\"{node_id}\"}} {}\n",
            peer.remote_requests_total
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_write_remote_failures_total Remote write RPC failures by peer\n\
         # TYPE tsink_cluster_write_remote_failures_total counter\n",
    );
    for peer in &snapshot.peers {
        let node_id = prometheus_escape_label_value(&peer.node_id);
        body.push_str(&format!(
            "tsink_cluster_write_remote_failures_total{{node_id=\"{node_id}\"}} {}\n",
            peer.remote_failures_total
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_write_remote_request_duration_seconds Remote write RPC latency histogram by peer\n\
         # TYPE tsink_cluster_write_remote_request_duration_seconds histogram\n",
    );
    for peer in &snapshot.peers {
        let node_id = prometheus_escape_label_value(&peer.node_id);
        for (idx, le) in WRITE_REMOTE_REQUEST_LATENCY_BUCKETS_SECONDS
            .iter()
            .enumerate()
        {
            let bucket_value = peer
                .remote_request_duration_buckets
                .get(idx)
                .copied()
                .unwrap_or(0);
            body.push_str(&format!(
                "tsink_cluster_write_remote_request_duration_seconds_bucket{{node_id=\"{node_id}\",le=\"{le}\"}} {bucket_value}\n"
            ));
        }
        body.push_str(&format!(
            "tsink_cluster_write_remote_request_duration_seconds_bucket{{node_id=\"{node_id}\",le=\"+Inf\"}} {}\n",
            peer.remote_request_duration_count
        ));
        body.push_str(&format!(
            "tsink_cluster_write_remote_request_duration_seconds_sum{{node_id=\"{node_id}\"}} {:.9}\n",
            peer.remote_request_duration_nanos_total as f64 / 1_000_000_000.0
        ));
        body.push_str(&format!(
            "tsink_cluster_write_remote_request_duration_seconds_count{{node_id=\"{node_id}\"}} {}\n",
            peer.remote_request_duration_count
        ));
    }
}

fn append_cluster_fanout_metrics(
    body: &mut String,
    snapshot: ReadFanoutMetricsSnapshot,
    labeled: &ReadFanoutLabeledMetricsSnapshot,
) {
    body.push_str(
        "# HELP tsink_cluster_fanout_requests_total Cluster read fanout requests\n\
         # TYPE tsink_cluster_fanout_requests_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_fanout_requests_total {}\n",
        snapshot.requests_total
    ));
    body.push_str(
        "# HELP tsink_cluster_fanout_failures_total Cluster read fanout request failures\n\
         # TYPE tsink_cluster_fanout_failures_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_fanout_failures_total {}\n",
        snapshot.failures_total
    ));
    body.push_str(
        "# HELP tsink_cluster_fanout_duration_nanoseconds_total Cluster read fanout execution time\n\
         # TYPE tsink_cluster_fanout_duration_nanoseconds_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_fanout_duration_nanoseconds_total {}\n",
        snapshot.duration_nanos_total
    ));
    body.push_str(
        "# HELP tsink_cluster_fanout_remote_requests_total Cluster read fanout remote RPC requests\n\
         # TYPE tsink_cluster_fanout_remote_requests_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_fanout_remote_requests_total {}\n",
        snapshot.remote_requests_total
    ));
    body.push_str(
        "# HELP tsink_cluster_fanout_remote_failures_total Cluster read fanout remote RPC failures\n\
         # TYPE tsink_cluster_fanout_remote_failures_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_fanout_remote_failures_total {}\n",
        snapshot.remote_failures_total
    ));
    body.push_str(
        "# HELP tsink_cluster_fanout_resource_rejections_total Cluster read fanout resource guardrail rejections\n\
         # TYPE tsink_cluster_fanout_resource_rejections_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_fanout_resource_rejections_total {}\n",
        snapshot.resource_rejections_total
    ));
    body.push_str(
        "# HELP tsink_cluster_fanout_resource_acquire_wait_nanoseconds_total Total wait time to acquire read fanout resource permits\n\
         # TYPE tsink_cluster_fanout_resource_acquire_wait_nanoseconds_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_fanout_resource_acquire_wait_nanoseconds_total {}\n",
        snapshot.resource_acquire_wait_nanos_total
    ));
    body.push_str(
        "# HELP tsink_cluster_fanout_resource_active_queries Active distributed read operations holding global query-slot permits\n\
         # TYPE tsink_cluster_fanout_resource_active_queries gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_fanout_resource_active_queries {}\n",
        snapshot.resource_active_queries
    ));
    body.push_str(
        "# HELP tsink_cluster_fanout_resource_active_merged_points Active estimated merged-point budget reserved by in-flight distributed reads\n\
         # TYPE tsink_cluster_fanout_resource_active_merged_points gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_fanout_resource_active_merged_points {}\n",
        snapshot.resource_active_merged_points
    ));

    body.push_str(
        "# HELP tsink_cluster_fanout_operation_requests_total Fanout requests by operation\n\
         # TYPE tsink_cluster_fanout_operation_requests_total counter\n",
    );
    for operation in &labeled.operations {
        let op = prometheus_escape_label_value(&operation.operation);
        body.push_str(&format!(
            "tsink_cluster_fanout_operation_requests_total{{operation=\"{op}\"}} {}\n",
            operation.requests_total
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_fanout_operation_failures_total Fanout failures by operation\n\
         # TYPE tsink_cluster_fanout_operation_failures_total counter\n",
    );
    for operation in &labeled.operations {
        let op = prometheus_escape_label_value(&operation.operation);
        body.push_str(&format!(
            "tsink_cluster_fanout_operation_failures_total{{operation=\"{op}\"}} {}\n",
            operation.failures_total
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_fanout_remote_requests_by_peer_total Fanout remote RPC requests by peer and operation\n\
         # TYPE tsink_cluster_fanout_remote_requests_by_peer_total counter\n",
    );
    for peer in &labeled.peers {
        let node_id = prometheus_escape_label_value(&peer.node_id);
        let operation = prometheus_escape_label_value(&peer.operation);
        body.push_str(&format!(
            "tsink_cluster_fanout_remote_requests_by_peer_total{{node_id=\"{node_id}\",operation=\"{operation}\"}} {}\n",
            peer.remote_requests_total
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_fanout_remote_failures_by_peer_total Fanout remote RPC failures by peer and operation\n\
         # TYPE tsink_cluster_fanout_remote_failures_by_peer_total counter\n",
    );
    for peer in &labeled.peers {
        let node_id = prometheus_escape_label_value(&peer.node_id);
        let operation = prometheus_escape_label_value(&peer.operation);
        body.push_str(&format!(
            "tsink_cluster_fanout_remote_failures_by_peer_total{{node_id=\"{node_id}\",operation=\"{operation}\"}} {}\n",
            peer.remote_failures_total
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_fanout_remote_request_duration_seconds Fanout remote RPC latency histogram by peer and operation\n\
         # TYPE tsink_cluster_fanout_remote_request_duration_seconds histogram\n",
    );
    for peer in &labeled.peers {
        let node_id = prometheus_escape_label_value(&peer.node_id);
        let operation = prometheus_escape_label_value(&peer.operation);
        for (idx, le) in FANOUT_REMOTE_REQUEST_LATENCY_BUCKETS_SECONDS
            .iter()
            .enumerate()
        {
            let bucket_value = peer
                .remote_request_duration_buckets
                .get(idx)
                .copied()
                .unwrap_or(0);
            body.push_str(&format!(
                "tsink_cluster_fanout_remote_request_duration_seconds_bucket{{node_id=\"{node_id}\",operation=\"{operation}\",le=\"{le}\"}} {bucket_value}\n"
            ));
        }
        body.push_str(&format!(
            "tsink_cluster_fanout_remote_request_duration_seconds_bucket{{node_id=\"{node_id}\",operation=\"{operation}\",le=\"+Inf\"}} {}\n",
            peer.remote_request_duration_count
        ));
        body.push_str(&format!(
            "tsink_cluster_fanout_remote_request_duration_seconds_sum{{node_id=\"{node_id}\",operation=\"{operation}\"}} {:.9}\n",
            peer.remote_request_duration_nanos_total as f64 / 1_000_000_000.0
        ));
        body.push_str(&format!(
            "tsink_cluster_fanout_remote_request_duration_seconds_count{{node_id=\"{node_id}\",operation=\"{operation}\"}} {}\n",
            peer.remote_request_duration_count
        ));
    }
}

fn append_cluster_read_planner_metrics(
    body: &mut String,
    snapshot: ReadPlannerMetricsSnapshot,
    labeled: &ReadPlannerLabeledMetricsSnapshot,
) {
    body.push_str(
        "# HELP tsink_cluster_read_planner_requests_total Cluster read planner requests\n\
         # TYPE tsink_cluster_read_planner_requests_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_read_planner_requests_total {}\n",
        snapshot.requests_total
    ));

    body.push_str(
        "# HELP tsink_cluster_read_planner_candidate_shards_total Candidate shards evaluated by planner\n\
         # TYPE tsink_cluster_read_planner_candidate_shards_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_read_planner_candidate_shards_total {}\n",
        snapshot.candidate_shards_total
    ));

    body.push_str(
        "# HELP tsink_cluster_read_planner_pruned_shards_total Shards pruned by planner\n\
         # TYPE tsink_cluster_read_planner_pruned_shards_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_read_planner_pruned_shards_total {}\n",
        snapshot.pruned_shards_total
    ));

    body.push_str(
        "# HELP tsink_cluster_read_planner_local_shards_total Local shards selected by planner\n\
         # TYPE tsink_cluster_read_planner_local_shards_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_read_planner_local_shards_total {}\n",
        snapshot.local_shards_total
    ));

    body.push_str(
        "# HELP tsink_cluster_read_planner_remote_targets_total Remote peer targets selected by planner\n\
         # TYPE tsink_cluster_read_planner_remote_targets_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_read_planner_remote_targets_total {}\n",
        snapshot.remote_targets_total
    ));

    body.push_str(
        "# HELP tsink_cluster_read_planner_remote_shards_total Remote shard assignments selected by planner\n\
         # TYPE tsink_cluster_read_planner_remote_shards_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_read_planner_remote_shards_total {}\n",
        snapshot.remote_shards_total
    ));

    body.push_str(
        "# HELP tsink_cluster_read_planner_operation_requests_total Read planner requests by operation\n\
         # TYPE tsink_cluster_read_planner_operation_requests_total counter\n",
    );
    for operation in &labeled.operations {
        let op = prometheus_escape_label_value(&operation.operation);
        body.push_str(&format!(
            "tsink_cluster_read_planner_operation_requests_total{{operation=\"{op}\"}} {}\n",
            operation.requests_total
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_read_planner_operation_candidate_shards_total Candidate shards by operation\n\
         # TYPE tsink_cluster_read_planner_operation_candidate_shards_total counter\n",
    );
    for operation in &labeled.operations {
        let op = prometheus_escape_label_value(&operation.operation);
        body.push_str(&format!(
            "tsink_cluster_read_planner_operation_candidate_shards_total{{operation=\"{op}\"}} {}\n",
            operation.candidate_shards_total
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_read_planner_operation_pruned_shards_total Pruned shards by operation\n\
         # TYPE tsink_cluster_read_planner_operation_pruned_shards_total counter\n",
    );
    for operation in &labeled.operations {
        let op = prometheus_escape_label_value(&operation.operation);
        body.push_str(&format!(
            "tsink_cluster_read_planner_operation_pruned_shards_total{{operation=\"{op}\"}} {}\n",
            operation.pruned_shards_total
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_read_planner_operation_remote_targets_total Remote targets by operation\n\
         # TYPE tsink_cluster_read_planner_operation_remote_targets_total counter\n",
    );
    for operation in &labeled.operations {
        let op = prometheus_escape_label_value(&operation.operation);
        body.push_str(&format!(
            "tsink_cluster_read_planner_operation_remote_targets_total{{operation=\"{op}\"}} {}\n",
            operation.remote_targets_total
        ));
    }
}

fn append_cluster_outbox_metrics(
    body: &mut String,
    snapshot: OutboxMetricsSnapshot,
    peers: &[crate::cluster::outbox::OutboxPeerBacklogSnapshot],
    stalled_peers: &[OutboxStalledPeerSnapshot],
) {
    body.push_str(
        "# HELP tsink_cluster_outbox_enqueued_total Replica batches enqueued for hinted handoff\n\
         # TYPE tsink_cluster_outbox_enqueued_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_enqueued_total {}\n",
        snapshot.enqueued_total
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_enqueue_rejected_total Hinted handoff outbox enqueue rejections due to quota limits\n\
         # TYPE tsink_cluster_outbox_enqueue_rejected_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_enqueue_rejected_total {}\n",
        snapshot.enqueue_rejected_total
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_persistence_failures_total Hinted handoff outbox persistence failures\n\
         # TYPE tsink_cluster_outbox_persistence_failures_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_persistence_failures_total {}\n",
        snapshot.persistence_failures_total
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_replay_attempts_total Hinted handoff outbox replay attempts\n\
         # TYPE tsink_cluster_outbox_replay_attempts_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_replay_attempts_total {}\n",
        snapshot.replay_attempts_total
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_replay_success_total Hinted handoff outbox replay successes\n\
         # TYPE tsink_cluster_outbox_replay_success_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_replay_success_total {}\n",
        snapshot.replay_success_total
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_replay_failures_total Hinted handoff outbox replay failures\n\
         # TYPE tsink_cluster_outbox_replay_failures_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_replay_failures_total {}\n",
        snapshot.replay_failures_total
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_queued_entries Pending hinted handoff outbox entries\n\
         # TYPE tsink_cluster_outbox_queued_entries gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_queued_entries {}\n",
        snapshot.queued_entries
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_queued_bytes Pending hinted handoff outbox bytes\n\
         # TYPE tsink_cluster_outbox_queued_bytes gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_queued_bytes {}\n",
        snapshot.queued_bytes
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_log_bytes Hinted handoff outbox log file size in bytes\n\
         # TYPE tsink_cluster_outbox_log_bytes gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_log_bytes {}\n",
        snapshot.log_bytes
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_stale_records Stale hinted handoff outbox log records pending cleanup compaction\n\
         # TYPE tsink_cluster_outbox_stale_records gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_stale_records {}\n",
        snapshot.stale_records
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_cleanup_runs_total Hinted handoff outbox cleanup worker iterations\n\
         # TYPE tsink_cluster_outbox_cleanup_runs_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_cleanup_runs_total {}\n",
        snapshot.cleanup_runs_total
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_cleanup_compactions_total Hinted handoff outbox cleanup-triggered compactions\n\
         # TYPE tsink_cluster_outbox_cleanup_compactions_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_cleanup_compactions_total {}\n",
        snapshot.cleanup_compactions_total
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_cleanup_reclaimed_bytes_total Hinted handoff outbox bytes reclaimed by cleanup compactions\n\
         # TYPE tsink_cluster_outbox_cleanup_reclaimed_bytes_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_cleanup_reclaimed_bytes_total {}\n",
        snapshot.cleanup_reclaimed_bytes_total
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_cleanup_failures_total Hinted handoff outbox cleanup failures\n\
         # TYPE tsink_cluster_outbox_cleanup_failures_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_cleanup_failures_total {}\n",
        snapshot.cleanup_failures_total
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_stalled_alerts_total Hinted handoff stalled-peer alert transitions\n\
         # TYPE tsink_cluster_outbox_stalled_alerts_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_stalled_alerts_total {}\n",
        snapshot.stalled_alerts_total
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_stalled_peers Peers currently considered stalled by outbox backlog alert thresholds\n\
         # TYPE tsink_cluster_outbox_stalled_peers gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_stalled_peers {}\n",
        snapshot.stalled_peers
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_stalled_oldest_age_milliseconds Oldest stalled peer backlog age in milliseconds\n\
         # TYPE tsink_cluster_outbox_stalled_oldest_age_milliseconds gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_outbox_stalled_oldest_age_milliseconds {}\n",
        snapshot.stalled_oldest_age_ms
    ));

    body.push_str(
        "# HELP tsink_cluster_outbox_peer_queued_entries Pending hinted handoff entries by peer node\n\
         # TYPE tsink_cluster_outbox_peer_queued_entries gauge\n",
    );
    for peer in peers {
        let node_id = prometheus_escape_label_value(&peer.node_id);
        body.push_str(&format!(
            "tsink_cluster_outbox_peer_queued_entries{{node_id=\"{node_id}\"}} {}\n",
            peer.queued_entries
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_outbox_peer_queued_bytes Pending hinted handoff bytes by peer node\n\
         # TYPE tsink_cluster_outbox_peer_queued_bytes gauge\n",
    );
    for peer in peers {
        let node_id = prometheus_escape_label_value(&peer.node_id);
        body.push_str(&format!(
            "tsink_cluster_outbox_peer_queued_bytes{{node_id=\"{node_id}\"}} {}\n",
            peer.queued_bytes
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_outbox_peer_stalled Backlog stall alert by peer node (1=stalled)\n\
         # TYPE tsink_cluster_outbox_peer_stalled gauge\n",
    );
    for peer in stalled_peers {
        let node_id = prometheus_escape_label_value(&peer.node_id);
        body.push_str(&format!(
            "tsink_cluster_outbox_peer_stalled{{node_id=\"{node_id}\"}} 1\n"
        ));
    }
}

fn append_cluster_control_metrics(body: &mut String, snapshot: &ControlLivenessSnapshot) {
    body.push_str(
        "# HELP tsink_cluster_control_current_term Current control-plane consensus term\n\
         # TYPE tsink_cluster_control_current_term gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_control_current_term {}\n",
        snapshot.current_term
    ));

    body.push_str(
        "# HELP tsink_cluster_control_commit_index Current committed control-log index\n\
         # TYPE tsink_cluster_control_commit_index gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_control_commit_index {}\n",
        snapshot.commit_index
    ));

    body.push_str(
        "# HELP tsink_cluster_control_leader_stale Whether the current control leader is stale (1=yes, 0=no)\n\
         # TYPE tsink_cluster_control_leader_stale gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_control_leader_stale {}\n",
        u8::from(snapshot.leader_stale)
    ));

    body.push_str(
        "# HELP tsink_cluster_control_leader_contact_age_ms Milliseconds since last leader heartbeat\n\
         # TYPE tsink_cluster_control_leader_contact_age_ms gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_control_leader_contact_age_ms {}\n",
        snapshot.leader_contact_age_ms.unwrap_or(0)
    ));

    body.push_str(
        "# HELP tsink_cluster_control_suspect_peers Control-plane peers currently marked suspect\n\
         # TYPE tsink_cluster_control_suspect_peers gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_control_suspect_peers {}\n",
        snapshot.suspect_peers
    ));

    body.push_str(
        "# HELP tsink_cluster_control_dead_peers Control-plane peers currently marked dead\n\
         # TYPE tsink_cluster_control_dead_peers gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_control_dead_peers {}\n",
        snapshot.dead_peers
    ));

    body.push_str(
        "# HELP tsink_cluster_control_peer_status Control-plane peer liveness status (one-hot by status label)\n\
         # TYPE tsink_cluster_control_peer_status gauge\n",
    );
    body.push_str(
        "# HELP tsink_cluster_control_peer_last_success_unix_ms Last successful control heartbeat/RPC response per peer\n\
         # TYPE tsink_cluster_control_peer_last_success_unix_ms gauge\n",
    );
    body.push_str(
        "# HELP tsink_cluster_control_peer_last_failure_unix_ms Last failed control heartbeat/RPC attempt per peer\n\
         # TYPE tsink_cluster_control_peer_last_failure_unix_ms gauge\n",
    );
    body.push_str(
        "# HELP tsink_cluster_control_peer_consecutive_failures Consecutive failed control heartbeat/RPC attempts per peer\n\
         # TYPE tsink_cluster_control_peer_consecutive_failures gauge\n",
    );
    for peer in &snapshot.peers {
        let node_id = prometheus_escape_label_value(&peer.node_id);
        for status in [
            ControlPeerLivenessStatus::Unknown,
            ControlPeerLivenessStatus::Healthy,
            ControlPeerLivenessStatus::Suspect,
            ControlPeerLivenessStatus::Dead,
        ] {
            body.push_str(&format!(
                "tsink_cluster_control_peer_status{{node_id=\"{node_id}\",status=\"{}\"}} {}\n",
                status.as_str(),
                u8::from(peer.status == status)
            ));
        }
        body.push_str(&format!(
            "tsink_cluster_control_peer_last_success_unix_ms{{node_id=\"{node_id}\"}} {}\n",
            peer.last_success_unix_ms.unwrap_or(0)
        ));
        body.push_str(&format!(
            "tsink_cluster_control_peer_last_failure_unix_ms{{node_id=\"{node_id}\"}} {}\n",
            peer.last_failure_unix_ms.unwrap_or(0)
        ));
        body.push_str(&format!(
            "tsink_cluster_control_peer_consecutive_failures{{node_id=\"{node_id}\"}} {}\n",
            peer.consecutive_failures
        ));
    }
}

fn append_cluster_handoff_metrics(body: &mut String, snapshot: &ClusterHandoffSnapshot) {
    body.push_str(
        "# HELP tsink_cluster_handoff_total Total shard handoff transitions tracked in control state\n\
         # TYPE tsink_cluster_handoff_total gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_handoff_total {}\n",
        snapshot.total_shards
    ));

    body.push_str(
        "# HELP tsink_cluster_handoff_in_progress Active shard handoff transitions (warmup/cutover/final_sync)\n\
         # TYPE tsink_cluster_handoff_in_progress gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_handoff_in_progress {}\n",
        snapshot.in_progress_shards
    ));

    body.push_str(
        "# HELP tsink_cluster_handoff_warmup Shard handoffs in warmup phase\n\
         # TYPE tsink_cluster_handoff_warmup gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_handoff_warmup {}\n",
        snapshot.warmup_shards
    ));

    body.push_str(
        "# HELP tsink_cluster_handoff_cutover Shard handoffs in cutover phase\n\
         # TYPE tsink_cluster_handoff_cutover gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_handoff_cutover {}\n",
        snapshot.cutover_shards
    ));

    body.push_str(
        "# HELP tsink_cluster_handoff_final_sync Shard handoffs in final_sync phase\n\
         # TYPE tsink_cluster_handoff_final_sync gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_handoff_final_sync {}\n",
        snapshot.final_sync_shards
    ));

    body.push_str(
        "# HELP tsink_cluster_handoff_completed Shard handoffs marked completed\n\
         # TYPE tsink_cluster_handoff_completed gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_handoff_completed {}\n",
        snapshot.completed_shards
    ));

    body.push_str(
        "# HELP tsink_cluster_handoff_failed Shard handoffs currently failed\n\
         # TYPE tsink_cluster_handoff_failed gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_handoff_failed {}\n",
        snapshot.failed_shards
    ));

    body.push_str(
        "# HELP tsink_cluster_handoff_resumed Shard handoffs that have resumed after at least one failure\n\
         # TYPE tsink_cluster_handoff_resumed gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_handoff_resumed {}\n",
        snapshot.resumed_shards
    ));

    body.push_str(
        "# HELP tsink_cluster_handoff_copied_rows_total Rows copied during shard handoff warmup/final-sync\n\
         # TYPE tsink_cluster_handoff_copied_rows_total gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_handoff_copied_rows_total {}\n",
        snapshot.copied_rows_total
    ));

    body.push_str(
        "# HELP tsink_cluster_handoff_pending_rows_total Estimated rows remaining across shard handoffs\n\
         # TYPE tsink_cluster_handoff_pending_rows_total gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_handoff_pending_rows_total {}\n",
        snapshot.pending_rows_total
    ));

    body.push_str(
        "# HELP tsink_cluster_handoff_shard_phase One-hot shard handoff phase by shard and owner pair\n\
         # TYPE tsink_cluster_handoff_shard_phase gauge\n",
    );
    body.push_str(
        "# HELP tsink_cluster_handoff_shard_copied_rows Rows copied for a shard handoff transition\n\
         # TYPE tsink_cluster_handoff_shard_copied_rows gauge\n",
    );
    body.push_str(
        "# HELP tsink_cluster_handoff_shard_pending_rows Estimated pending rows for a shard handoff transition\n\
         # TYPE tsink_cluster_handoff_shard_pending_rows gauge\n",
    );
    body.push_str(
        "# HELP tsink_cluster_handoff_shard_resumed_count Resume attempts for a shard handoff transition\n\
         # TYPE tsink_cluster_handoff_shard_resumed_count gauge\n",
    );
    body.push_str(
        "# HELP tsink_cluster_handoff_shard_updated_unix_ms Last update timestamp per shard handoff transition\n\
         # TYPE tsink_cluster_handoff_shard_updated_unix_ms gauge\n",
    );
    for shard in &snapshot.shards {
        let from_node_id = prometheus_escape_label_value(&shard.from_node_id);
        let to_node_id = prometheus_escape_label_value(&shard.to_node_id);
        for phase in [
            ShardHandoffPhase::Warmup,
            ShardHandoffPhase::Cutover,
            ShardHandoffPhase::FinalSync,
            ShardHandoffPhase::Completed,
            ShardHandoffPhase::Failed,
        ] {
            body.push_str(&format!(
                "tsink_cluster_handoff_shard_phase{{shard=\"{}\",from_node_id=\"{from_node_id}\",to_node_id=\"{to_node_id}\",phase=\"{}\"}} {}\n",
                shard.shard,
                phase.as_str(),
                u8::from(shard.phase == phase)
            ));
        }
        body.push_str(&format!(
            "tsink_cluster_handoff_shard_copied_rows{{shard=\"{}\",from_node_id=\"{from_node_id}\",to_node_id=\"{to_node_id}\"}} {}\n",
            shard.shard, shard.copied_rows
        ));
        body.push_str(&format!(
            "tsink_cluster_handoff_shard_pending_rows{{shard=\"{}\",from_node_id=\"{from_node_id}\",to_node_id=\"{to_node_id}\"}} {}\n",
            shard.shard, shard.pending_rows
        ));
        body.push_str(&format!(
            "tsink_cluster_handoff_shard_resumed_count{{shard=\"{}\",from_node_id=\"{from_node_id}\",to_node_id=\"{to_node_id}\"}} {}\n",
            shard.shard, shard.resumed_count
        ));
        body.push_str(&format!(
            "tsink_cluster_handoff_shard_updated_unix_ms{{shard=\"{}\",from_node_id=\"{from_node_id}\",to_node_id=\"{to_node_id}\"}} {}\n",
            shard.shard, shard.updated_unix_ms
        ));
    }
}

fn append_cluster_digest_metrics(body: &mut String, snapshot: &DigestExchangeSnapshot) {
    body.push_str(
        "# HELP tsink_cluster_repair_digest_runs_total Total digest-exchange worker runs\n\
         # TYPE tsink_cluster_repair_digest_runs_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_runs_total {}\n",
        snapshot.runs_total
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_windows_compared_total Total shard-window digest comparisons attempted\n\
         # TYPE tsink_cluster_repair_digest_windows_compared_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_windows_compared_total {}\n",
        snapshot.windows_compared_total
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_windows_success_total Successful shard-window digest comparisons\n\
         # TYPE tsink_cluster_repair_digest_windows_success_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_windows_success_total {}\n",
        snapshot.windows_success_total
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_windows_failed_total Failed shard-window digest comparisons\n\
         # TYPE tsink_cluster_repair_digest_windows_failed_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_windows_failed_total {}\n",
        snapshot.windows_failed_total
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_local_compute_failures_total Local shard digest compute failures\n\
         # TYPE tsink_cluster_repair_digest_local_compute_failures_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_local_compute_failures_total {}\n",
        snapshot.local_compute_failures_total
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_mismatches_total Detected shard-window digest mismatches\n\
         # TYPE tsink_cluster_repair_digest_mismatches_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_mismatches_total {}\n",
        snapshot.mismatches_total
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_bytes_exchanged_total Estimated digest exchange bytes sent/received\n\
         # TYPE tsink_cluster_repair_digest_bytes_exchanged_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_bytes_exchanged_total {}\n",
        snapshot.bytes_exchanged_total
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_bytes_exchanged_last_run Estimated digest exchange bytes sent/received in the latest run\n\
         # TYPE tsink_cluster_repair_digest_bytes_exchanged_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_bytes_exchanged_last_run {}\n",
        snapshot.bytes_exchanged_last_run
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_budget_exhaustions_total Digest runs that exhausted the configured byte budget\n\
         # TYPE tsink_cluster_repair_digest_budget_exhaustions_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_budget_exhaustions_total {}\n",
        snapshot.budget_exhaustions_total
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_windows_skipped_budget_total Digest shard-window comparisons skipped due to byte budget exhaustion\n\
         # TYPE tsink_cluster_repair_digest_windows_skipped_budget_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_windows_skipped_budget_total {}\n",
        snapshot.windows_skipped_budget_total
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_budget_exhausted_last_run Whether the latest digest run exhausted byte budget (1=yes,0=no)\n\
         # TYPE tsink_cluster_repair_digest_budget_exhausted_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_budget_exhausted_last_run {}\n",
        u8::from(snapshot.budget_exhausted_last_run)
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_last_run_unix_ms Last digest-exchange run timestamp\n\
         # TYPE tsink_cluster_repair_digest_last_run_unix_ms gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_last_run_unix_ms {}\n",
        snapshot.last_run_unix_ms
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_last_success_unix_ms Last successful digest-exchange comparison timestamp\n\
         # TYPE tsink_cluster_repair_digest_last_success_unix_ms gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_last_success_unix_ms {}\n",
        snapshot.last_success_unix_ms
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_last_ring_version Ring version used by the latest digest-exchange run\n\
         # TYPE tsink_cluster_repair_digest_last_ring_version gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_last_ring_version {}\n",
        snapshot.last_ring_version
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_compared_shards_last_run Shards sampled in the latest digest-exchange run\n\
         # TYPE tsink_cluster_repair_digest_compared_shards_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_compared_shards_last_run {}\n",
        snapshot.compared_shards_last_run
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_digest_prioritized_shards_last_run Handoff-priority shards sampled in the latest digest-exchange run\n\
         # TYPE tsink_cluster_repair_digest_prioritized_shards_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_prioritized_shards_last_run {}\n",
        snapshot.prioritized_shards_last_run
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_compared_peers_last_run Peer comparisons attempted in the latest digest-exchange run\n\
         # TYPE tsink_cluster_repair_digest_compared_peers_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_compared_peers_last_run {}\n",
        snapshot.compared_peers_last_run
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_backfill_attempts_total Targeted backfill attempts triggered from digest mismatches\n\
         # TYPE tsink_cluster_repair_backfill_attempts_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_attempts_total {}\n",
        snapshot.repairs_attempted_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_success_total Successful targeted backfill attempts\n\
         # TYPE tsink_cluster_repair_backfill_success_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_success_total {}\n",
        snapshot.repairs_succeeded_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_failures_total Failed targeted backfill attempts\n\
         # TYPE tsink_cluster_repair_backfill_failures_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_failures_total {}\n",
        snapshot.repairs_failed_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_skipped_budget_total Digest mismatches skipped for backfill due to per-tick repair budgets\n\
         # TYPE tsink_cluster_repair_backfill_skipped_budget_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_skipped_budget_total {}\n",
        snapshot.repairs_skipped_budget_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_skipped_non_additive_total Digest mismatches skipped for backfill because remote digest was not additive\n\
         # TYPE tsink_cluster_repair_backfill_skipped_non_additive_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_skipped_non_additive_total {}\n",
        snapshot.repairs_skipped_non_additive_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_skipped_backoff_total Digest mismatches skipped for backfill while a failure backoff cooldown is active\n\
         # TYPE tsink_cluster_repair_backfill_skipped_backoff_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_skipped_backoff_total {}\n",
        snapshot.repairs_skipped_backoff_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_skipped_paused_total Digest mismatches skipped for backfill while repair is paused\n\
         # TYPE tsink_cluster_repair_backfill_skipped_paused_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_skipped_paused_total {}\n",
        snapshot.repairs_skipped_paused_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_skipped_time_budget_total Digest mismatches skipped for backfill because repair runtime budget was exhausted\n\
         # TYPE tsink_cluster_repair_backfill_skipped_time_budget_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_skipped_time_budget_total {}\n",
        snapshot.repairs_skipped_time_budget_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_cancelled_total Backfill attempts cancelled via operator control\n\
         # TYPE tsink_cluster_repair_backfill_cancelled_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_cancelled_total {}\n",
        snapshot.repairs_cancelled_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_time_budget_exhaustions_total Digest runs where repair runtime budget was exhausted\n\
         # TYPE tsink_cluster_repair_backfill_time_budget_exhaustions_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_time_budget_exhaustions_total {}\n",
        snapshot.repair_time_budget_exhaustions_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_series_scanned_total Series scanned during targeted backfill attempts\n\
         # TYPE tsink_cluster_repair_backfill_series_scanned_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_series_scanned_total {}\n",
        snapshot.repair_series_scanned_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_rows_scanned_total Remote points scanned during targeted backfill attempts\n\
         # TYPE tsink_cluster_repair_backfill_rows_scanned_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_rows_scanned_total {}\n",
        snapshot.repair_rows_scanned_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_rows_inserted_total Local points inserted by targeted backfill attempts\n\
         # TYPE tsink_cluster_repair_backfill_rows_inserted_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_rows_inserted_total {}\n",
        snapshot.repair_rows_inserted_total
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_attempts_last_run Targeted backfill attempts in the latest digest run\n\
         # TYPE tsink_cluster_repair_backfill_attempts_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_attempts_last_run {}\n",
        snapshot.repairs_attempted_last_run
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_success_last_run Successful targeted backfill attempts in the latest digest run\n\
         # TYPE tsink_cluster_repair_backfill_success_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_success_last_run {}\n",
        snapshot.repairs_succeeded_last_run
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_failures_last_run Failed targeted backfill attempts in the latest digest run\n\
         # TYPE tsink_cluster_repair_backfill_failures_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_failures_last_run {}\n",
        snapshot.repairs_failed_last_run
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_cancelled_last_run Backfill attempts cancelled in the latest digest run\n\
         # TYPE tsink_cluster_repair_backfill_cancelled_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_cancelled_last_run {}\n",
        snapshot.repairs_cancelled_last_run
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_skipped_backoff_last_run Digest mismatches skipped for backfill due to active failure cooldown in the latest digest run\n\
         # TYPE tsink_cluster_repair_backfill_skipped_backoff_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_skipped_backoff_last_run {}\n",
        snapshot.repairs_skipped_backoff_last_run
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_time_budget_exhausted_last_run Whether targeted backfill exhausted runtime budget in the latest digest run (1=yes,0=no)\n\
         # TYPE tsink_cluster_repair_backfill_time_budget_exhausted_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_time_budget_exhausted_last_run {}\n",
        u8::from(snapshot.repair_time_budget_exhausted_last_run)
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_backfill_rows_inserted_last_run Local points inserted by targeted backfill in the latest digest run\n\
         # TYPE tsink_cluster_repair_backfill_rows_inserted_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_backfill_rows_inserted_last_run {}\n",
        snapshot.repair_rows_inserted_last_run
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_config_interval_seconds Digest exchange schedule interval in seconds\n\
         # TYPE tsink_cluster_repair_digest_config_interval_seconds gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_config_interval_seconds {}\n",
        snapshot.interval_secs
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_digest_config_window_seconds Digest exchange window size in seconds\n\
         # TYPE tsink_cluster_repair_digest_config_window_seconds gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_config_window_seconds {}\n",
        snapshot.window_secs
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_digest_config_max_shards_per_tick Maximum shards sampled per digest-exchange run\n\
         # TYPE tsink_cluster_repair_digest_config_max_shards_per_tick gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_config_max_shards_per_tick {}\n",
        snapshot.max_shards_per_tick
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_digest_config_max_mismatch_reports Maximum retained digest mismatch reports\n\
         # TYPE tsink_cluster_repair_digest_config_max_mismatch_reports gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_config_max_mismatch_reports {}\n",
        snapshot.max_mismatch_reports
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_digest_config_max_bytes_per_tick Maximum digest exchange bytes allowed per run\n\
         # TYPE tsink_cluster_repair_digest_config_max_bytes_per_tick gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_digest_config_max_bytes_per_tick {}\n",
        snapshot.max_bytes_per_tick
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_config_max_mismatches_per_tick Maximum digest mismatches that may trigger backfill per run\n\
         # TYPE tsink_cluster_repair_config_max_mismatches_per_tick gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_config_max_mismatches_per_tick {}\n",
        snapshot.max_repair_mismatches_per_tick
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_config_max_series_per_tick Maximum series scanned by targeted backfill per run\n\
         # TYPE tsink_cluster_repair_config_max_series_per_tick gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_config_max_series_per_tick {}\n",
        snapshot.max_repair_series_per_tick
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_config_max_rows_per_tick Maximum rows inserted by targeted backfill per run\n\
         # TYPE tsink_cluster_repair_config_max_rows_per_tick gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_config_max_rows_per_tick {}\n",
        snapshot.max_repair_rows_per_tick
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_config_max_runtime_ms_per_tick Maximum wall-clock runtime budget for targeted backfill per digest run\n\
         # TYPE tsink_cluster_repair_config_max_runtime_ms_per_tick gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_config_max_runtime_ms_per_tick {}\n",
        snapshot.max_repair_runtime_ms_per_tick
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_config_failure_backoff_seconds Targeted backfill failure cooldown duration in seconds\n\
         # TYPE tsink_cluster_repair_config_failure_backoff_seconds gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_config_failure_backoff_seconds {}\n",
        snapshot.repair_failure_backoff_secs
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_control_paused Whether targeted backfill repair is currently paused (1=yes,0=no)\n\
         # TYPE tsink_cluster_repair_control_paused gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_control_paused {}\n",
        u8::from(snapshot.repair_paused)
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_control_cancel_generation Monotonic repair cancellation generation\n\
         # TYPE tsink_cluster_repair_control_cancel_generation gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_control_cancel_generation {}\n",
        snapshot.repair_cancel_generation
    ));
    body.push_str(
        "# HELP tsink_cluster_repair_control_cancellations_total Total operator-issued repair cancellations\n\
         # TYPE tsink_cluster_repair_control_cancellations_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_repair_control_cancellations_total {}\n",
        snapshot.repair_cancellations_total
    ));

    body.push_str(
        "# HELP tsink_cluster_repair_digest_mismatch_report Current retained digest mismatch reports (one sample per report)\n\
         # TYPE tsink_cluster_repair_digest_mismatch_report gauge\n",
    );
    for report in &snapshot.mismatches {
        let peer_node_id = prometheus_escape_label_value(&report.peer_node_id);
        let peer_endpoint = prometheus_escape_label_value(&report.peer_endpoint);
        body.push_str(&format!(
            "tsink_cluster_repair_digest_mismatch_report{{shard=\"{}\",peer_node_id=\"{peer_node_id}\",peer_endpoint=\"{peer_endpoint}\",ring_version=\"{}\",window_start=\"{}\",window_end=\"{}\",local_fingerprint=\"{}\",remote_fingerprint=\"{}\"}} 1\n",
            report.shard,
            report.ring_version,
            report.window_start,
            report.window_end,
            report.local_fingerprint,
            report.remote_fingerprint
        ));
    }
}

fn append_cluster_rebalance_metrics(body: &mut String, snapshot: &RebalanceSchedulerSnapshot) {
    body.push_str(
        "# HELP tsink_cluster_rebalance_runs_total Total rebalance scheduler runs\n\
         # TYPE tsink_cluster_rebalance_runs_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_runs_total {}\n",
        snapshot.runs_total
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_jobs_considered_last_run Rebalance jobs considered in the latest scheduler run\n\
         # TYPE tsink_cluster_rebalance_jobs_considered_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_jobs_considered_last_run {}\n",
        snapshot.jobs_considered_last_run
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_jobs_advanced_total Rebalance jobs advanced by scheduler proposals\n\
         # TYPE tsink_cluster_rebalance_jobs_advanced_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_jobs_advanced_total {}\n",
        snapshot.jobs_advanced_total
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_jobs_completed_total Rebalance jobs completed by scheduler proposals\n\
         # TYPE tsink_cluster_rebalance_jobs_completed_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_jobs_completed_total {}\n",
        snapshot.jobs_completed_total
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_rows_scheduled_total Total rebalance rows scheduled under throughput ceilings\n\
         # TYPE tsink_cluster_rebalance_rows_scheduled_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_rows_scheduled_total {}\n",
        snapshot.rows_scheduled_total
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_rows_scheduled_last_run Rebalance rows scheduled in the latest run\n\
         # TYPE tsink_cluster_rebalance_rows_scheduled_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_rows_scheduled_last_run {}\n",
        snapshot.rows_scheduled_last_run
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_proposals_committed_total Rebalance scheduler proposals committed to control quorum\n\
         # TYPE tsink_cluster_rebalance_proposals_committed_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_proposals_committed_total {}\n",
        snapshot.proposals_committed_total
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_proposals_pending_total Rebalance scheduler proposals pending quorum acknowledgement\n\
         # TYPE tsink_cluster_rebalance_proposals_pending_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_proposals_pending_total {}\n",
        snapshot.proposals_pending_total
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_proposal_failures_total Rebalance scheduler proposal failures\n\
         # TYPE tsink_cluster_rebalance_proposal_failures_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_proposal_failures_total {}\n",
        snapshot.proposal_failures_total
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_moves_blocked_by_slo_total Rebalance move proposals blocked by SLO guardrails\n\
         # TYPE tsink_cluster_rebalance_moves_blocked_by_slo_total counter\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_moves_blocked_by_slo_total {}\n",
        snapshot.moves_blocked_by_slo_total
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_active_jobs Active rebalance jobs in warmup/cutover/final_sync\n\
         # TYPE tsink_cluster_rebalance_active_jobs gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_active_jobs {}\n",
        snapshot.active_jobs
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_control_paused Whether rebalance scheduler is paused (1=yes,0=no)\n\
         # TYPE tsink_cluster_rebalance_control_paused gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_control_paused {}\n",
        u8::from(snapshot.paused)
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_is_local_control_leader Whether local node is active control leader (1=yes,0=no)\n\
         # TYPE tsink_cluster_rebalance_is_local_control_leader gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_is_local_control_leader {}\n",
        u8::from(snapshot.is_local_control_leader)
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_last_run_unix_ms Last rebalance scheduler run timestamp\n\
         # TYPE tsink_cluster_rebalance_last_run_unix_ms gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_last_run_unix_ms {}\n",
        snapshot.last_run_unix_ms
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_last_success_unix_ms Last successful rebalance scheduler run timestamp\n\
         # TYPE tsink_cluster_rebalance_last_success_unix_ms gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_last_success_unix_ms {}\n",
        snapshot.last_success_unix_ms
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_config_interval_seconds Rebalance scheduler interval in seconds\n\
         # TYPE tsink_cluster_rebalance_config_interval_seconds gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_config_interval_seconds {}\n",
        snapshot.interval_secs
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_config_max_rows_per_tick Rebalance scheduler max rows per run\n\
         # TYPE tsink_cluster_rebalance_config_max_rows_per_tick gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_config_max_rows_per_tick {}\n",
        snapshot.max_rows_per_tick
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_config_max_shards_per_tick Rebalance scheduler max jobs advanced per run\n\
         # TYPE tsink_cluster_rebalance_config_max_shards_per_tick gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_config_max_shards_per_tick {}\n",
        snapshot.max_shards_per_tick
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_effective_max_rows_per_tick_last_run Effective rebalance row budget applied in the latest run after SLO guardrails\n\
         # TYPE tsink_cluster_rebalance_effective_max_rows_per_tick_last_run gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_effective_max_rows_per_tick_last_run {}\n",
        snapshot.effective_max_rows_per_tick_last_run
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_slo_write_pressure_ratio Current public write pressure ratio used by rebalance guardrails\n\
         # TYPE tsink_cluster_rebalance_slo_write_pressure_ratio gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_slo_write_pressure_ratio {}\n",
        snapshot.slo_guard.write_pressure_ratio
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_slo_query_pressure_ratio Current public read pressure ratio used by rebalance guardrails\n\
         # TYPE tsink_cluster_rebalance_slo_query_pressure_ratio gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_slo_query_pressure_ratio {}\n",
        snapshot.slo_guard.query_pressure_ratio
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_slo_cluster_query_pressure_ratio Current cluster query fanout pressure ratio used by rebalance guardrails\n\
         # TYPE tsink_cluster_rebalance_slo_cluster_query_pressure_ratio gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_slo_cluster_query_pressure_ratio {}\n",
        snapshot.slo_guard.cluster_query_pressure_ratio
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_slo_guard_block_new_handoffs Whether rebalance SLO guardrails currently block new handoffs (1=yes,0=no)\n\
         # TYPE tsink_cluster_rebalance_slo_guard_block_new_handoffs gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_rebalance_slo_guard_block_new_handoffs {}\n",
        u8::from(snapshot.slo_guard.block_new_handoffs)
    ));

    body.push_str(
        "# HELP tsink_cluster_rebalance_job_phase One-hot rebalance job phase by shard and owner pair\n\
         # TYPE tsink_cluster_rebalance_job_phase gauge\n",
    );
    body.push_str(
        "# HELP tsink_cluster_rebalance_job_pending_rows Pending rows reported per rebalance job\n\
         # TYPE tsink_cluster_rebalance_job_pending_rows gauge\n",
    );
    body.push_str(
        "# HELP tsink_cluster_rebalance_job_copied_rows Copied rows reported per rebalance job\n\
         # TYPE tsink_cluster_rebalance_job_copied_rows gauge\n",
    );
    for job in &snapshot.jobs {
        let from_node_id = prometheus_escape_label_value(&job.from_node_id);
        let to_node_id = prometheus_escape_label_value(&job.to_node_id);
        for phase in [
            ShardHandoffPhase::Warmup,
            ShardHandoffPhase::Cutover,
            ShardHandoffPhase::FinalSync,
            ShardHandoffPhase::Completed,
            ShardHandoffPhase::Failed,
        ] {
            body.push_str(&format!(
                "tsink_cluster_rebalance_job_phase{{shard=\"{}\",from_node_id=\"{from_node_id}\",to_node_id=\"{to_node_id}\",phase=\"{}\"}} {}\n",
                job.shard,
                phase.as_str(),
                u8::from(job.phase == phase)
            ));
        }
        body.push_str(&format!(
            "tsink_cluster_rebalance_job_pending_rows{{shard=\"{}\",from_node_id=\"{from_node_id}\",to_node_id=\"{to_node_id}\"}} {}\n",
            job.shard, job.pending_rows
        ));
        body.push_str(&format!(
            "tsink_cluster_rebalance_job_copied_rows{{shard=\"{}\",from_node_id=\"{from_node_id}\",to_node_id=\"{to_node_id}\"}} {}\n",
            job.shard, job.copied_rows
        ));
    }

    body.push_str(
        "# HELP tsink_cluster_rebalance_candidate_decision_score Decision score for top candidate rebalance moves\n\
         # TYPE tsink_cluster_rebalance_candidate_decision_score gauge\n",
    );
    for candidate in &snapshot.candidate_moves {
        let from_node_id = prometheus_escape_label_value(&candidate.from_node_id);
        let to_node_id = prometheus_escape_label_value(&candidate.to_node_id);
        body.push_str(&format!(
            "tsink_cluster_rebalance_candidate_decision_score{{shard=\"{}\",from_node_id=\"{from_node_id}\",to_node_id=\"{to_node_id}\"}} {}\n",
            candidate.shard, candidate.decision_score
        ));
    }
}

fn append_cluster_hotspot_metrics(body: &mut String, snapshot: &ClusterHotspotSnapshot) {
    body.push_str(
        "# HELP tsink_cluster_hotspot_skewed_shards Number of shards currently above hotspot skew threshold\n\
         # TYPE tsink_cluster_hotspot_skewed_shards gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_hotspot_skewed_shards {}\n",
        snapshot.skewed_shards
    ));

    body.push_str(
        "# HELP tsink_cluster_hotspot_skewed_tenants Number of tenants currently above hotspot skew threshold\n\
         # TYPE tsink_cluster_hotspot_skewed_tenants gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_hotspot_skewed_tenants {}\n",
        snapshot.skewed_tenants
    ));

    body.push_str(
        "# HELP tsink_cluster_hotspot_max_shard_score Highest hotspot pressure score among tracked shards\n\
         # TYPE tsink_cluster_hotspot_max_shard_score gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_hotspot_max_shard_score {}\n",
        snapshot.max_shard_score
    ));

    body.push_str(
        "# HELP tsink_cluster_hotspot_max_tenant_score Highest hotspot pressure score among tracked tenants\n\
         # TYPE tsink_cluster_hotspot_max_tenant_score gauge\n",
    );
    body.push_str(&format!(
        "tsink_cluster_hotspot_max_tenant_score {}\n",
        snapshot.max_tenant_score
    ));

    body.push_str(
        "# HELP tsink_cluster_hotspot_shard_pressure_score Hotspot pressure score for top tracked shards\n\
         # TYPE tsink_cluster_hotspot_shard_pressure_score gauge\n",
    );
    body.push_str(
        "# HELP tsink_cluster_hotspot_shard_movement_cost_score Estimated movement cost score for top tracked shards\n\
         # TYPE tsink_cluster_hotspot_shard_movement_cost_score gauge\n",
    );
    body.push_str(
        "# HELP tsink_cluster_hotspot_shard_storage_series Live series footprint for top tracked shards\n\
         # TYPE tsink_cluster_hotspot_shard_storage_series gauge\n",
    );
    body.push_str(
        "# HELP tsink_cluster_hotspot_shard_handoff_pending_rows Pending handoff rows for top tracked shards\n\
         # TYPE tsink_cluster_hotspot_shard_handoff_pending_rows gauge\n",
    );
    for shard in &snapshot.hot_shards {
        body.push_str(&format!(
            "tsink_cluster_hotspot_shard_pressure_score{{shard=\"{}\"}} {}\n",
            shard.shard, shard.pressure_score
        ));
        body.push_str(&format!(
            "tsink_cluster_hotspot_shard_movement_cost_score{{shard=\"{}\"}} {}\n",
            shard.shard, shard.movement_cost_score
        ));
        body.push_str(&format!(
            "tsink_cluster_hotspot_shard_storage_series{{shard=\"{}\"}} {}\n",
            shard.shard, shard.storage_series
        ));
        body.push_str(&format!(
            "tsink_cluster_hotspot_shard_handoff_pending_rows{{shard=\"{}\"}} {}\n",
            shard.shard, shard.handoff_pending_rows
        ));
    }
}
