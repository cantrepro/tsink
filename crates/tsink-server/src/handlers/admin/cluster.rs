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
    execute_admin_cluster_rebalance(storage, cluster_context, AdminRebalanceOperation::Pause).await
}

pub(crate) async fn handle_admin_cluster_rebalance_resume(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    execute_admin_cluster_rebalance(storage, cluster_context, AdminRebalanceOperation::Resume).await
}

pub(crate) async fn handle_admin_cluster_rebalance_run(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    execute_admin_cluster_rebalance(storage, cluster_context, AdminRebalanceOperation::Run).await
}

pub(crate) async fn handle_admin_cluster_rebalance_status(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    execute_admin_cluster_rebalance(storage, cluster_context, AdminRebalanceOperation::Status).await
}

pub(crate) async fn handle_admin_cluster_rebalance_status_with_execution(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
    execution: &tsink::QueryExecution,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let mut response_reservation = None;
    let response = execute_admin_cluster_rebalance_impl(
        storage,
        cluster_context,
        AdminRebalanceOperation::Status,
        Some(execution),
        Some(&mut response_reservation),
        false,
    )
    .await;
    match response_reservation {
        Some(reservation) => Ok(AccountedHttpResponse {
            response,
            reservation,
        }),
        // The only caller is the support-bundle adapter. Its admitted root scratch covers these
        // bounded compatibility errors until it transfers them to an exact response guard.
        None => Err(response),
    }
}

async fn execute_admin_cluster_rebalance(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
    operation: AdminRebalanceOperation,
) -> HttpResponse {
    execute_admin_cluster_rebalance_impl(storage, cluster_context, operation, None, None, true)
        .await
}

async fn execute_admin_cluster_rebalance_impl(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
    operation: AdminRebalanceOperation,
    shared_execution: Option<&tsink::QueryExecution>,
    retained_response_reservation: Option<&mut Option<tsink::QueryMemoryReservation>>,
    charge_http_returned_bytes: bool,
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
    if operation == AdminRebalanceOperation::Run && digest_runtime.is_rebalance_run_inflight() {
        return admin_rebalance_error_response(
            409,
            "rebalance_run_in_progress",
            "a rebalance run is already in progress",
        );
    }
    if storage.list_metrics_execution_accounting() != tsink::QueryExecutionAccounting::Complete {
        return admin_rebalance_error_response(
            500,
            "rebalance_query_accounting_unavailable",
            "admin rebalance status requires complete metric-enumeration accounting",
        );
    }
    let node_id = cluster_context.runtime.membership.local_node_id.as_str();
    let (execution, cancellation_guard, mut direct_error_scratch) = match shared_execution {
        Some(execution) => (execution.clone(), None, None),
        None => {
            let cancellation = tsink::QueryCancellationToken::new();
            let cancellation_guard = TsdbStatusCancellationGuard {
                token: cancellation.clone(),
            };
            let execution = match storage
                .begin_query_execution(tsink::QueryWorkLimits::default(), cancellation)
            {
                Ok(Some(execution)) => execution,
                Ok(None) => {
                    return admin_rebalance_error_response(
                        500,
                        "rebalance_query_accounting_unavailable",
                        "admin rebalance status requires query execution admission",
                    )
                }
                Err(tsink::TsinkError::QueryBudget(error)) => {
                    return admin_rebalance_response_error_response(
                        AdminRebalanceResponseError::Budget(error),
                        None,
                    )
                }
                Err(_) => {
                    return admin_rebalance_error_response(
                        500,
                        "rebalance_query_admission_failed",
                        "admin rebalance query admission failed",
                    )
                }
            };
            let error_scratch = match reserve_direct_admin_rebalance_error_scratch(
                &execution, operation, node_id,
            ) {
                Ok(reservation) => reservation,
                Err(error) => {
                    drop(execution);
                    drop(cancellation_guard);
                    return admin_rebalance_response_error_response(
                        AdminRebalanceResponseError::Budget(error),
                        None,
                    );
                }
            };
            (execution, Some(cancellation_guard), Some(error_scratch))
        }
    };
    let worker_storage = Arc::clone(storage);
    let worker_execution = execution.clone();
    let metrics_result = tokio::task::spawn_blocking(move || {
        worker_storage.list_metrics_with_execution_result(&worker_execution)
    })
    .await;
    let metrics_result = match metrics_result {
        Ok(Ok(result)) => result,
        Ok(Err(tsink::TsinkError::QueryBudget(error))) => {
            return admin_rebalance_response_error_response(
                AdminRebalanceResponseError::Budget(error),
                None,
            )
        }
        Ok(Err(_)) => {
            return admin_rebalance_error_response(
                500,
                "rebalance_storage_list_failed",
                "admin rebalance metric enumeration failed",
            )
        }
        Err(_) => {
            return admin_rebalance_error_response(
                500,
                "rebalance_task_failed",
                "admin rebalance metric-enumeration task failed",
            )
        }
    };
    let guarded_metrics = match validate_complete_metric_enumeration_result(metrics_result) {
        Ok(guarded) => guarded,
        Err(error) => {
            return admin_rebalance_error_response(
                500,
                "rebalance_query_accounting_invalid",
                error
                    .status_message()
                    .replace("TSDB status", "admin rebalance"),
            )
        }
    };
    if let Err(error) = execution.checkpoint() {
        return admin_rebalance_response_error_response(
            AdminRebalanceResponseError::Budget(error),
            None,
        );
    }

    let effect = match operation {
        AdminRebalanceOperation::Pause => {
            let control = digest_runtime.pause_rebalance();
            Some(AdminRebalanceAppliedEffect {
                operation,
                node_id,
                rebalance_paused: control.paused,
                rebalance_run_completed: false,
            })
        }
        AdminRebalanceOperation::Resume => {
            let control = digest_runtime.resume_rebalance();
            Some(AdminRebalanceAppliedEffect {
                operation,
                node_id,
                rebalance_paused: control.paused,
                rebalance_run_completed: false,
            })
        }
        AdminRebalanceOperation::Run => {
            if let Err(RebalanceRunTriggerError::AlreadyRunning) =
                digest_runtime.trigger_rebalance_run().await
            {
                return admin_rebalance_error_response(
                    409,
                    "rebalance_run_in_progress",
                    "a rebalance run is already in progress",
                );
            }
            Some(AdminRebalanceAppliedEffect {
                operation,
                node_id,
                rebalance_paused: digest_runtime.rebalance_control_snapshot().paused,
                rebalance_run_completed: true,
            })
        }
        AdminRebalanceOperation::Status => None,
    };

    let control_projection =
        match digest_runtime.rebalance_control_projection_with_execution(&execution) {
            Ok(projection) => projection,
            Err(error) => {
                return admin_rebalance_response_error_response(
                    AdminRebalanceResponseError::Budget(error),
                    effect,
                )
            }
        };
    let hotspot_snapshot =
        match hotspot::build_rebalance_hotspot_snapshot_with_control_metrics_execution(
            &guarded_metrics.series,
            Some(&cluster_context.runtime.ring),
            Some(&control_projection.hotspot),
            &execution,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return admin_rebalance_response_error_response(
                    AdminRebalanceResponseError::Budget(error),
                    effect,
                )
            }
        };
    let snapshot = match digest_runtime.rebalance_status_snapshot_with_execution(
        &control_projection,
        &hotspot_snapshot.tracker,
        &execution,
    ) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return admin_rebalance_response_error_response(
                AdminRebalanceResponseError::Budget(error),
                effect,
            )
        }
    };
    let message = match operation {
        AdminRebalanceOperation::Pause => "cluster rebalance scheduler is paused",
        AdminRebalanceOperation::Resume => "cluster rebalance scheduler is resumed",
        AdminRebalanceOperation::Run => "cluster rebalance scheduler run completed",
        AdminRebalanceOperation::Status => "cluster rebalance scheduler status",
    };
    let accounted_response = admin_rebalance_success_response(
        200,
        operation,
        node_id,
        &snapshot,
        &hotspot_snapshot.snapshot,
        digest_runtime.is_rebalance_run_inflight(),
        message,
        &execution,
        charge_http_returned_bytes,
        direct_error_scratch.take(),
    );
    let (response, response_reservation) = match accounted_response {
        Ok(AccountedHttpResponse {
            response,
            reservation,
        }) => (response, Some(reservation)),
        Err(AdminRebalanceResponseFailure { error, reservation }) => {
            let response = admin_rebalance_response_error_response(error, effect);
            drop(reservation);
            (response, None)
        }
    };
    drop(snapshot);
    drop(hotspot_snapshot);
    drop(control_projection);
    let GuardedMetricEnumeration {
        series,
        reservation,
    } = guarded_metrics;
    drop(series);
    drop(reservation);
    if let Some(response_reservation) = response_reservation {
        if let Some(retained_response_reservation) = retained_response_reservation {
            *retained_response_reservation = Some(response_reservation);
        } else {
            drop(response_reservation);
        }
    }
    drop(execution);
    drop(cancellation_guard);
    response
}
