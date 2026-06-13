use super::*;

pub(crate) fn handle_admin_cluster_audit_query(
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let Some(audit_log) = cluster_context.and_then(|context| context.audit_log.as_ref()) else {
        return admin_audit_error_response(
            503,
            "audit_log_unavailable",
            "cluster audit log is not available",
        );
    };
    let query = match parse_admin_audit_query(request) {
        Ok(query) => query,
        Err(err) => return admin_audit_error_response(400, "invalid_request", err),
    };
    let entries = audit_log.query(&query);
    json_response(
        200,
        &json!({
            "status": "success",
            "data": {
                "operation": "audit_query",
                "count": entries.len(),
                "entries": entries
            }
        }),
    )
}

pub(crate) fn handle_admin_cluster_audit_export(
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let Some(audit_log) = cluster_context.and_then(|context| context.audit_log.as_ref()) else {
        return admin_audit_error_response(
            503,
            "audit_log_unavailable",
            "cluster audit log is not available",
        );
    };
    let query = match parse_admin_audit_query(request) {
        Ok(query) => query,
        Err(err) => return admin_audit_error_response(400, "invalid_request", err),
    };
    let format = non_empty_param(request.param("format")).unwrap_or_else(|| "jsonl".to_string());
    if !format.eq_ignore_ascii_case("jsonl") && !format.eq_ignore_ascii_case("ndjson") {
        return admin_audit_error_response(
            400,
            "invalid_request",
            "invalid format: expected jsonl (or ndjson)",
        );
    }
    let exported = match audit_log.export_jsonl(&query) {
        Ok(exported) => exported,
        Err(err) => {
            return admin_audit_error_response(500, "audit_export_failed", err);
        }
    };
    HttpResponse::new(200, exported)
        .with_header("Content-Type", "application/x-ndjson")
        .with_header(
            "Content-Disposition",
            "attachment; filename=\"tsink-cluster-audit.ndjson\"",
        )
}

pub(crate) async fn handle_admin_cluster_join(
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let payload = match parse_optional_json_body::<ClusterJoinAdminPayload>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => return admin_membership_error_response(400, "invalid_request", err),
    };
    let node_id = non_empty_param(
        request
            .param("node_id")
            .or_else(|| request.param("nodeId"))
            .or(payload.node_id),
    );
    let endpoint = non_empty_param(request.param("endpoint").or(payload.endpoint));

    let Some(node_id) = node_id else {
        return admin_membership_error_response(
            400,
            "invalid_request",
            "missing required parameter 'node_id' (or 'nodeId')",
        );
    };
    let Some(endpoint) = endpoint else {
        return admin_membership_error_response(
            400,
            "invalid_request",
            "missing required parameter 'endpoint'",
        );
    };
    let Some(cluster_context) = cluster_context else {
        return admin_membership_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };
    execute_admin_membership_operation(
        cluster_context,
        AdminMembershipOperation::Join,
        InternalControlCommand::JoinNode { node_id, endpoint },
    )
    .await
}

pub(crate) async fn handle_admin_cluster_leave(
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let payload = match parse_optional_json_body::<ClusterLeaveAdminPayload>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => return admin_membership_error_response(400, "invalid_request", err),
    };
    let node_id = non_empty_param(
        request
            .param("node_id")
            .or_else(|| request.param("nodeId"))
            .or(payload.node_id),
    );
    let Some(node_id) = node_id else {
        return admin_membership_error_response(
            400,
            "invalid_request",
            "missing required parameter 'node_id' (or 'nodeId')",
        );
    };
    let Some(cluster_context) = cluster_context else {
        return admin_membership_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };
    execute_admin_membership_operation(
        cluster_context,
        AdminMembershipOperation::Leave,
        InternalControlCommand::LeaveNode { node_id },
    )
    .await
}

pub(crate) async fn handle_admin_cluster_recommission(
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let payload = match parse_optional_json_body::<ClusterRecommissionAdminPayload>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => return admin_membership_error_response(400, "invalid_request", err),
    };
    let node_id = non_empty_param(
        request
            .param("node_id")
            .or_else(|| request.param("nodeId"))
            .or(payload.node_id),
    );
    let endpoint = non_empty_param(request.param("endpoint").or(payload.endpoint));

    let Some(node_id) = node_id else {
        return admin_membership_error_response(
            400,
            "invalid_request",
            "missing required parameter 'node_id' (or 'nodeId')",
        );
    };
    let Some(cluster_context) = cluster_context else {
        return admin_membership_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };
    execute_admin_membership_operation(
        cluster_context,
        AdminMembershipOperation::Recommission,
        InternalControlCommand::RecommissionNode { node_id, endpoint },
    )
    .await
}

pub(crate) async fn handle_admin_cluster_handoff_begin(
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let payload = match parse_optional_json_body::<ClusterHandoffBeginAdminPayload>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => return admin_handoff_error_response(400, "invalid_request", err),
    };

    let shard = match request.param("shard") {
        Some(value) => match parse_admin_u32(&value, "shard") {
            Ok(value) => Some(value),
            Err(err) => return admin_handoff_error_response(400, "invalid_request", err),
        },
        None => payload.shard,
    };
    let from_node_id = non_empty_param(
        request
            .param("from_node_id")
            .or_else(|| request.param("fromNodeId"))
            .or(payload.from_node_id),
    );
    let to_node_id = non_empty_param(
        request
            .param("to_node_id")
            .or_else(|| request.param("toNodeId"))
            .or(payload.to_node_id),
    );
    let activation_ring_version = match request
        .param("activation_ring_version")
        .or_else(|| request.param("activationRingVersion"))
    {
        Some(value) => match parse_admin_u64(&value, "activation_ring_version") {
            Ok(value) => Some(value),
            Err(err) => return admin_handoff_error_response(400, "invalid_request", err),
        },
        None => payload.activation_ring_version,
    };

    let Some(shard) = shard else {
        return admin_handoff_error_response(
            400,
            "invalid_request",
            "missing required parameter 'shard'",
        );
    };
    let Some(from_node_id) = from_node_id else {
        return admin_handoff_error_response(
            400,
            "invalid_request",
            "missing required parameter 'from_node_id' (or 'fromNodeId')",
        );
    };
    let Some(to_node_id) = to_node_id else {
        return admin_handoff_error_response(
            400,
            "invalid_request",
            "missing required parameter 'to_node_id' (or 'toNodeId')",
        );
    };
    let Some(cluster_context) = cluster_context else {
        return admin_handoff_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };
    let activation_ring_version = activation_ring_version.unwrap_or_else(|| {
        cluster_context
            .control_consensus
            .as_ref()
            .map(|consensus| consensus.current_state().ring_version.saturating_add(1))
            .unwrap_or(1)
    });

    execute_admin_handoff_operation(
        cluster_context,
        AdminHandoffOperation::Begin,
        InternalControlCommand::BeginShardHandoff {
            shard,
            from_node_id,
            to_node_id,
            activation_ring_version,
        },
    )
    .await
}

pub(crate) async fn handle_admin_cluster_handoff_progress(
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let payload = match parse_optional_json_body::<ClusterHandoffProgressAdminPayload>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => return admin_handoff_error_response(400, "invalid_request", err),
    };

    let shard = match request.param("shard") {
        Some(value) => match parse_admin_u32(&value, "shard") {
            Ok(value) => Some(value),
            Err(err) => return admin_handoff_error_response(400, "invalid_request", err),
        },
        None => payload.shard,
    };
    let phase = match request.param("phase").or(payload.phase) {
        Some(value) => match parse_handoff_phase(&value) {
            Ok(value) => Some(value),
            Err(err) => return admin_handoff_error_response(400, "invalid_request", err),
        },
        None => None,
    };
    let copied_rows = match request
        .param("copied_rows")
        .or_else(|| request.param("copiedRows"))
    {
        Some(value) => match parse_admin_u64(&value, "copied_rows") {
            Ok(value) => Some(value),
            Err(err) => return admin_handoff_error_response(400, "invalid_request", err),
        },
        None => payload.copied_rows,
    };
    let pending_rows = match request
        .param("pending_rows")
        .or_else(|| request.param("pendingRows"))
    {
        Some(value) => match parse_admin_u64(&value, "pending_rows") {
            Ok(value) => Some(value),
            Err(err) => return admin_handoff_error_response(400, "invalid_request", err),
        },
        None => payload.pending_rows,
    };
    let last_error = non_empty_param(
        request
            .param("last_error")
            .or_else(|| request.param("lastError"))
            .or(payload.last_error),
    );

    let Some(shard) = shard else {
        return admin_handoff_error_response(
            400,
            "invalid_request",
            "missing required parameter 'shard'",
        );
    };
    let Some(phase) = phase else {
        return admin_handoff_error_response(
            400,
            "invalid_request",
            "missing required parameter 'phase'",
        );
    };
    let Some(cluster_context) = cluster_context else {
        return admin_handoff_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };

    execute_admin_handoff_operation(
        cluster_context,
        AdminHandoffOperation::Progress,
        InternalControlCommand::UpdateShardHandoff {
            shard,
            phase,
            copied_rows,
            pending_rows,
            last_error,
        },
    )
    .await
}

pub(crate) async fn handle_admin_cluster_handoff_complete(
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let payload = match parse_optional_json_body::<ClusterHandoffCompleteAdminPayload>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => return admin_handoff_error_response(400, "invalid_request", err),
    };

    let shard = match request.param("shard") {
        Some(value) => match parse_admin_u32(&value, "shard") {
            Ok(value) => Some(value),
            Err(err) => return admin_handoff_error_response(400, "invalid_request", err),
        },
        None => payload.shard,
    };
    let Some(shard) = shard else {
        return admin_handoff_error_response(
            400,
            "invalid_request",
            "missing required parameter 'shard'",
        );
    };
    let Some(cluster_context) = cluster_context else {
        return admin_handoff_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };

    execute_admin_handoff_operation(
        cluster_context,
        AdminHandoffOperation::Complete,
        InternalControlCommand::CompleteShardHandoff { shard },
    )
    .await
}

pub(crate) async fn handle_admin_cluster_handoff_status(
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let Some(cluster_context) = cluster_context else {
        return admin_handoff_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };
    let Some(consensus) = cluster_context.control_consensus.as_ref() else {
        return admin_handoff_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };
    let state = consensus.current_state();
    let handoff = state.handoff_snapshot();
    let rebalance = cluster_context
        .digest_runtime
        .as_ref()
        .map(|runtime| runtime.rebalance_snapshot())
        .unwrap_or_else(RebalanceSchedulerSnapshot::empty);
    admin_handoff_status_response(
        &cluster_context.runtime.membership.local_node_id,
        &state,
        &handoff,
        &rebalance,
    )
}

pub(crate) async fn handle_admin_cluster_repair_pause(
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let Some(cluster_context) = cluster_context else {
        return admin_repair_error_response(
            503,
            "repair_runtime_unavailable",
            "cluster digest repair runtime is not available",
        );
    };
    let Some(digest_runtime) = cluster_context.digest_runtime.as_ref() else {
        return admin_repair_error_response(
            503,
            "repair_runtime_unavailable",
            "cluster digest repair runtime is not available",
        );
    };
    let snapshot = digest_runtime.pause_repairs();
    admin_repair_success_response(
        200,
        AdminRepairOperation::Pause,
        &cluster_context.runtime.membership.local_node_id,
        snapshot,
        "cluster digest-triggered backfill is paused",
    )
}

pub(crate) async fn handle_admin_cluster_repair_resume(
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let Some(cluster_context) = cluster_context else {
        return admin_repair_error_response(
            503,
            "repair_runtime_unavailable",
            "cluster digest repair runtime is not available",
        );
    };
    let Some(digest_runtime) = cluster_context.digest_runtime.as_ref() else {
        return admin_repair_error_response(
            503,
            "repair_runtime_unavailable",
            "cluster digest repair runtime is not available",
        );
    };
    let snapshot = digest_runtime.resume_repairs();
    admin_repair_success_response(
        200,
        AdminRepairOperation::Resume,
        &cluster_context.runtime.membership.local_node_id,
        snapshot,
        "cluster digest-triggered backfill is resumed",
    )
}

async fn admin_cluster_hotspot_snapshot(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
) -> ClusterHotspotSnapshot {
    let storage = Arc::clone(storage);
    let metrics = tokio::task::spawn_blocking(move || storage.list_metrics().unwrap_or_default())
        .await
        .unwrap_or_default();
    cluster_hotspot_snapshot_for_request(&metrics, cluster_context, None)
}

pub(crate) async fn handle_admin_cluster_repair_cancel(
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let Some(cluster_context) = cluster_context else {
        return admin_repair_error_response(
            503,
            "repair_runtime_unavailable",
            "cluster digest repair runtime is not available",
        );
    };
    let Some(digest_runtime) = cluster_context.digest_runtime.as_ref() else {
        return admin_repair_error_response(
            503,
            "repair_runtime_unavailable",
            "cluster digest repair runtime is not available",
        );
    };
    let snapshot = digest_runtime.cancel_repairs();
    admin_repair_success_response(
        200,
        AdminRepairOperation::Cancel,
        &cluster_context.runtime.membership.local_node_id,
        snapshot,
        "cluster digest-triggered backfill cancellation requested",
    )
}

pub(crate) async fn handle_admin_cluster_repair_status(
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let Some(cluster_context) = cluster_context else {
        return admin_repair_error_response(
            503,
            "repair_runtime_unavailable",
            "cluster digest repair runtime is not available",
        );
    };
    let Some(digest_runtime) = cluster_context.digest_runtime.as_ref() else {
        return admin_repair_error_response(
            503,
            "repair_runtime_unavailable",
            "cluster digest repair runtime is not available",
        );
    };
    let control = digest_runtime.repair_control_snapshot();
    let snapshot = digest_runtime.snapshot();
    let run_inflight = digest_runtime.is_repair_run_inflight();
    admin_repair_status_response(
        200,
        AdminRepairOperation::Status,
        &cluster_context.runtime.membership.local_node_id,
        control,
        snapshot,
        run_inflight,
        "cluster digest repair runtime status",
    )
}

pub(crate) async fn handle_admin_cluster_repair_run(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let Some(cluster_context) = cluster_context else {
        return admin_repair_error_response(
            503,
            "repair_runtime_unavailable",
            "cluster digest repair runtime is not available",
        );
    };
    let Some(digest_runtime) = cluster_context.digest_runtime.as_ref() else {
        return admin_repair_error_response(
            503,
            "repair_runtime_unavailable",
            "cluster digest repair runtime is not available",
        );
    };
    let control = digest_runtime.repair_control_snapshot();
    let snapshot = match digest_runtime.trigger_repair_run(Arc::clone(storage)).await {
        Ok(snapshot) => snapshot,
        Err(RepairRunTriggerError::AlreadyRunning) => {
            return admin_repair_error_response(
                409,
                "repair_run_in_progress",
                "a repair run is already in progress",
            );
        }
    };
    admin_repair_status_response(
        200,
        AdminRepairOperation::Run,
        &cluster_context.runtime.membership.local_node_id,
        control,
        snapshot,
        digest_runtime.is_repair_run_inflight(),
        "cluster digest repair run completed",
    )
}

pub(crate) async fn handle_admin_cluster_rebalance_pause(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let Some(cluster_context) = cluster_context else {
        return admin_rebalance_error_response(
            503,
            "rebalance_runtime_unavailable",
            "cluster rebalance scheduler runtime is not available",
        );
    };
    let Some(digest_runtime) = cluster_context.digest_runtime.as_ref() else {
        return admin_rebalance_error_response(
            503,
            "rebalance_runtime_unavailable",
            "cluster rebalance scheduler runtime is not available",
        );
    };
    let control = digest_runtime.pause_rebalance();
    let snapshot = digest_runtime.rebalance_snapshot();
    let hotspot_snapshot = admin_cluster_hotspot_snapshot(storage, Some(cluster_context)).await;
    admin_rebalance_success_response(
        200,
        AdminRebalanceOperation::Pause,
        &cluster_context.runtime.membership.local_node_id,
        control,
        snapshot,
        hotspot_snapshot,
        digest_runtime.is_rebalance_run_inflight(),
        "cluster rebalance scheduler is paused",
    )
}

pub(crate) async fn handle_admin_cluster_rebalance_resume(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let Some(cluster_context) = cluster_context else {
        return admin_rebalance_error_response(
            503,
            "rebalance_runtime_unavailable",
            "cluster rebalance scheduler runtime is not available",
        );
    };
    let Some(digest_runtime) = cluster_context.digest_runtime.as_ref() else {
        return admin_rebalance_error_response(
            503,
            "rebalance_runtime_unavailable",
            "cluster rebalance scheduler runtime is not available",
        );
    };
    let control = digest_runtime.resume_rebalance();
    let snapshot = digest_runtime.rebalance_snapshot();
    let hotspot_snapshot = admin_cluster_hotspot_snapshot(storage, Some(cluster_context)).await;
    admin_rebalance_success_response(
        200,
        AdminRebalanceOperation::Resume,
        &cluster_context.runtime.membership.local_node_id,
        control,
        snapshot,
        hotspot_snapshot,
        digest_runtime.is_rebalance_run_inflight(),
        "cluster rebalance scheduler is resumed",
    )
}

pub(crate) async fn handle_admin_cluster_rebalance_run(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let Some(cluster_context) = cluster_context else {
        return admin_rebalance_error_response(
            503,
            "rebalance_runtime_unavailable",
            "cluster rebalance scheduler runtime is not available",
        );
    };
    let Some(digest_runtime) = cluster_context.digest_runtime.as_ref() else {
        return admin_rebalance_error_response(
            503,
            "rebalance_runtime_unavailable",
            "cluster rebalance scheduler runtime is not available",
        );
    };
    let control = digest_runtime.rebalance_control_snapshot();
    let snapshot = match digest_runtime.trigger_rebalance_run().await {
        Ok(snapshot) => snapshot,
        Err(RebalanceRunTriggerError::AlreadyRunning) => {
            return admin_rebalance_error_response(
                409,
                "rebalance_run_in_progress",
                "a rebalance run is already in progress",
            );
        }
    };
    let hotspot_snapshot = admin_cluster_hotspot_snapshot(storage, Some(cluster_context)).await;
    admin_rebalance_success_response(
        200,
        AdminRebalanceOperation::Run,
        &cluster_context.runtime.membership.local_node_id,
        control,
        snapshot,
        hotspot_snapshot,
        digest_runtime.is_rebalance_run_inflight(),
        "cluster rebalance scheduler run completed",
    )
}

pub(crate) async fn handle_admin_cluster_rebalance_status(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let Some(cluster_context) = cluster_context else {
        return admin_rebalance_error_response(
            503,
            "rebalance_runtime_unavailable",
            "cluster rebalance scheduler runtime is not available",
        );
    };
    let Some(digest_runtime) = cluster_context.digest_runtime.as_ref() else {
        return admin_rebalance_error_response(
            503,
            "rebalance_runtime_unavailable",
            "cluster rebalance scheduler runtime is not available",
        );
    };
    let control = digest_runtime.rebalance_control_snapshot();
    let snapshot = digest_runtime.rebalance_snapshot();
    let hotspot_snapshot = admin_cluster_hotspot_snapshot(storage, Some(cluster_context)).await;
    admin_rebalance_success_response(
        200,
        AdminRebalanceOperation::Status,
        &cluster_context.runtime.membership.local_node_id,
        control,
        snapshot,
        hotspot_snapshot,
        digest_runtime.is_rebalance_run_inflight(),
        "cluster rebalance scheduler status",
    )
}
