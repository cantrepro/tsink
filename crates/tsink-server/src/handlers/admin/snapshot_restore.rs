use super::*;

pub(crate) async fn handle_admin_snapshot(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    request: &HttpRequest,
    admin_path_prefix: Option<&Path>,
) -> HttpResponse {
    let path = match non_empty_param(request.param("path")) {
        Some(path) => path,
        None => {
            let payload = match parse_optional_json_body::<SnapshotAdminPayload>(request) {
                Ok(payload) => payload.unwrap_or_default(),
                Err(err) => return text_response(400, &err),
            };
            match non_empty_param(payload.path) {
                Some(path) => path,
                None => {
                    return text_response(
                        400,
                        "missing required parameter 'path' (query/form or JSON body)",
                    )
                }
            }
        }
    };

    let path_buf = match resolve_admin_path(Path::new(&path), admin_path_prefix, false) {
        Ok(path_buf) => path_buf,
        Err(err) => return text_response(400, &err),
    };

    let response_path = path.clone();
    match perform_local_data_snapshot(
        storage,
        metadata_store,
        exemplar_store,
        rules_runtime,
        &path_buf,
        None,
    )
    .await
    {
        Ok(_) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "path": response_path
                }
            }),
        ),
        Err(err) => text_response(500, &format!("snapshot failed: {err}")),
    }
}

pub(crate) async fn handle_admin_restore(
    request: &HttpRequest,
    admin_path_prefix: Option<&Path>,
) -> HttpResponse {
    let payload = match parse_optional_json_body::<RestoreAdminPayload>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => return text_response(400, &err),
    };

    let snapshot_path = non_empty_param(
        request
            .param("snapshot_path")
            .or_else(|| request.param("snapshotPath")),
    )
    .or_else(|| non_empty_param(payload.snapshot_path));
    let data_path = non_empty_param(
        request
            .param("data_path")
            .or_else(|| request.param("dataPath")),
    )
    .or_else(|| non_empty_param(payload.data_path));

    let Some(snapshot_path) = snapshot_path else {
        return text_response(
            400,
            "missing required parameter 'snapshot_path' (or 'snapshotPath')",
        );
    };
    let Some(data_path) = data_path else {
        return text_response(
            400,
            "missing required parameter 'data_path' (or 'dataPath')",
        );
    };

    let snapshot_path_buf =
        match resolve_admin_path(Path::new(&snapshot_path), admin_path_prefix, true) {
            Ok(path_buf) => path_buf,
            Err(err) => return text_response(400, &err),
        };
    let data_path_buf = match resolve_admin_path(Path::new(&data_path), admin_path_prefix, false) {
        Ok(path_buf) => path_buf,
        Err(err) => return text_response(400, &err),
    };

    let response_snapshot_path = snapshot_path_buf.display().to_string();
    let response_data_path = data_path_buf.display().to_string();
    let result = tokio::task::spawn_blocking(move || {
        StorageBuilder::restore_from_snapshot(&snapshot_path_buf, &data_path_buf)
    })
    .await;

    match result {
        Ok(Ok(())) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "snapshotPath": response_snapshot_path,
                    "dataPath": response_data_path
                }
            }),
        ),
        Ok(Err(err)) => text_response(500, &format!("restore failed: {err}")),
        Err(err) => text_response(500, &format!("restore task failed: {err}")),
    }
}

pub(crate) async fn handle_admin_delete_series(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    tenant_registry: Option<&tenant::TenantRegistry>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let started = Instant::now();
    let tenant_id = match tenant_id_for_text_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        tenant_registry,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Write,
    ) {
        Ok(plan) => plan,
        Err(response) => return response,
    };
    let payload = match parse_optional_json_body::<DeleteSeriesAdminPayload>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => return text_response(400, &err),
    };
    let DeleteSeriesAdminPayload {
        selectors: payload_selectors,
        start: payload_start_raw,
        end: payload_end_raw,
    } = payload;

    let mut selectors = request.param_all("match[]");
    if selectors.is_empty() {
        selectors = payload_selectors
            .into_iter()
            .filter_map(|selector| non_empty_param(Some(selector)))
            .collect();
    }
    if selectors.is_empty() {
        return text_response(
            400,
            "missing required parameter 'match[]' (query/form or JSON body field 'match')",
        );
    }

    let payload_start = match json_scalar_param(payload_start_raw, "start") {
        Ok(value) => value,
        Err(err) => return text_response(400, &err),
    };
    let payload_end = match json_scalar_param(payload_end_raw, "end") {
        Ok(value) => value,
        Err(err) => return text_response(400, &err),
    };
    let start = match request.param("start").or(payload_start) {
        Some(value) => match parse_timestamp(&value, precision) {
            Ok(timestamp) => timestamp,
            Err(err) => return text_response(400, &format!("invalid 'start': {err}")),
        },
        None => i64::MIN,
    };
    let end = match request.param("end").or(payload_end) {
        Some(value) => match parse_timestamp(&value, precision) {
            Ok(timestamp) => timestamp,
            Err(err) => return text_response(400, &format!("invalid 'end': {err}")),
        },
        None => i64::MAX,
    };
    if start >= end {
        return text_response(
            400,
            "invalid time range: 'start' must be strictly less than 'end'",
        );
    }

    let mut selections = Vec::with_capacity(selectors.len());
    for selector in selectors {
        let expr = match tsink::promql::parse(&selector) {
            Ok(expr) => expr,
            Err(err) => return text_response(400, &format!("invalid match expression: {err}")),
        };
        let Some(selection) = expr_to_selection(&expr) else {
            return text_response(
                400,
                &format!("match expression must be a vector selector: '{selector}'"),
            );
        };
        selections.push(selection.with_time_range(start, end));
    }
    let matcher_count = selections.len();
    let _tenant_request = match tenant_plan.admit(
        tenant::TenantAdmissionSurface::Retention,
        matcher_count.max(1),
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };

    let storage = tenant::scoped_storage(Arc::clone(storage), tenant_id.clone());
    let matcher_count = matcher_count.min(u64::MAX as usize) as u64;
    let result = tokio::task::spawn_blocking(move || {
        let mut matched_series = 0u64;
        let mut tombstones_applied = 0u64;
        for selection in selections {
            let outcome = storage.delete_series(&selection)?;
            matched_series = matched_series.saturating_add(outcome.matched_series);
            tombstones_applied = tombstones_applied.saturating_add(outcome.tombstones_applied);
        }
        Ok::<_, tsink::TsinkError>((matched_series, tombstones_applied))
    })
    .await;

    match result {
        Ok(Ok((matched_series, tombstones_applied))) => {
            record_retention_usage(
                usage_accounting,
                &tenant_id,
                matched_series,
                tombstones_applied,
                matcher_count,
                elapsed_nanos_since(started),
            );
            json_response(
                200,
                &json!({
                    "status": "success",
                    "data": {
                        "tenantId": tenant_id,
                        "matchersProcessed": matcher_count,
                        "matchedSeries": matched_series,
                        "tombstonesApplied": tombstones_applied,
                        "start": start,
                        "end": end
                    }
                }),
            )
        }
        Ok(Err(err)) => delete_series_error_response(&err),
        Err(err) => text_response(500, &format!("delete_series task failed: {err}")),
    }
}

pub(crate) async fn handle_admin_cluster_control_snapshot(
    request: &HttpRequest,
    admin_path_prefix: Option<&Path>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let path = match non_empty_param(request.param("path")) {
        Some(path) => path,
        None => {
            let payload =
                match parse_optional_json_body::<ClusterControlSnapshotAdminPayload>(request) {
                    Ok(payload) => payload.unwrap_or_default(),
                    Err(err) => {
                        return admin_control_recovery_error_response(400, "invalid_request", err);
                    }
                };
            match non_empty_param(payload.path) {
                Some(path) => path,
                None => {
                    return admin_control_recovery_error_response(
                        400,
                        "invalid_request",
                        "missing required parameter 'path' (query/form or JSON body)",
                    );
                }
            }
        }
    };

    let snapshot_path = match resolve_admin_path(Path::new(&path), admin_path_prefix, false) {
        Ok(path) => path,
        Err(err) => return admin_control_recovery_error_response(400, "invalid_path", err),
    };

    let Some(cluster_context) = cluster_context else {
        return admin_control_recovery_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };
    let Some(consensus) = cluster_context.control_consensus.as_ref() else {
        return admin_control_recovery_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };
    let Some(control_state_store) = cluster_context.control_state_store.as_ref() else {
        return admin_control_recovery_error_response(
            503,
            "control_plane_unavailable",
            "cluster control state store is not available",
        );
    };

    let (control_state, log_snapshot) = consensus.recovery_snapshot_bundle();
    if let Err(err) = control_state_store.persist(&control_state) {
        return admin_control_recovery_error_response(
            500,
            "control_snapshot_failed",
            format!("failed to persist control state before snapshot: {err}"),
        );
    }

    let snapshot = ControlRecoverySnapshotFileV1 {
        magic: CONTROL_RECOVERY_SNAPSHOT_MAGIC.to_string(),
        schema_version: CONTROL_RECOVERY_SNAPSHOT_SCHEMA_VERSION,
        created_unix_ms: unix_timestamp_millis(),
        source_node_id: cluster_context.runtime.membership.local_node_id.clone(),
        source_control_state_path: control_state_store.path().display().to_string(),
        source_control_log_path: consensus.log_path().display().to_string(),
        control_state: control_state.clone(),
        control_log: log_snapshot.clone(),
    };
    let snapshot_created_unix_ms = snapshot.created_unix_ms;

    let snapshot_path_clone = snapshot_path.clone();
    let snapshot_for_write = snapshot.clone();
    let snapshot_write = tokio::task::spawn_blocking(move || {
        write_control_recovery_snapshot_file(&snapshot_path_clone, &snapshot_for_write)
    })
    .await;
    match snapshot_write {
        Ok(Ok(())) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "path": snapshot_path.display().to_string(),
                    "createdUnixMs": snapshot_created_unix_ms,
                    "membershipEpoch": control_state.membership_epoch,
                    "ringVersion": control_state.ring_version,
                    "appliedLogIndex": control_state.applied_log_index,
                    "appliedLogTerm": control_state.applied_log_term,
                    "commitIndex": log_snapshot.commit_index,
                    "currentTerm": log_snapshot.current_term
                }
            }),
        ),
        Ok(Err(err)) => admin_control_recovery_error_response(
            500,
            "control_snapshot_failed",
            format!("failed to write control recovery snapshot: {err}"),
        ),
        Err(err) => admin_control_recovery_error_response(
            500,
            "control_snapshot_failed",
            format!("control recovery snapshot task failed: {err}"),
        ),
    }
}

pub(crate) async fn handle_admin_cluster_control_restore(
    request: &HttpRequest,
    admin_path_prefix: Option<&Path>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let payload = match parse_optional_json_body::<ClusterControlRestoreAdminPayload>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => return admin_control_recovery_error_response(400, "invalid_request", err),
    };

    let snapshot_path = non_empty_param(
        request
            .param("snapshot_path")
            .or_else(|| request.param("snapshotPath")),
    )
    .or_else(|| non_empty_param(payload.snapshot_path));

    let force_local_leader = match request
        .param("force_local_leader")
        .or_else(|| request.param("forceLocalLeader"))
    {
        Some(value) => match parse_admin_bool(&value) {
            Ok(value) => value,
            Err(err) => {
                return admin_control_recovery_error_response(400, "invalid_request", err);
            }
        },
        None => payload.force_local_leader.unwrap_or(false),
    };

    let Some(snapshot_path) = snapshot_path else {
        return admin_control_recovery_error_response(
            400,
            "invalid_request",
            "missing required parameter 'snapshot_path' (or 'snapshotPath')",
        );
    };

    let snapshot_path = match resolve_admin_path(Path::new(&snapshot_path), admin_path_prefix, true)
    {
        Ok(path) => path,
        Err(err) => return admin_control_recovery_error_response(400, "invalid_path", err),
    };

    let Some(cluster_context) = cluster_context else {
        return admin_control_recovery_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };
    let Some(consensus) = cluster_context.control_consensus.as_ref() else {
        return admin_control_recovery_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };

    let snapshot_path_clone = snapshot_path.clone();
    let loaded_snapshot = match tokio::task::spawn_blocking(move || {
        load_control_recovery_snapshot_file(&snapshot_path_clone)
    })
    .await
    {
        Ok(Ok(snapshot)) => snapshot,
        Ok(Err(err)) => {
            return admin_control_recovery_error_response(400, "invalid_control_snapshot", err);
        }
        Err(err) => {
            return admin_control_recovery_error_response(
                500,
                "control_restore_failed",
                format!("control recovery snapshot load task failed: {err}"),
            );
        }
    };

    let restored_state = match consensus.restore_recovery_snapshot(
        loaded_snapshot.control_state,
        loaded_snapshot.control_log.clone(),
        force_local_leader,
    ) {
        Ok(state) => state,
        Err(err) => {
            return admin_control_recovery_error_response(
                409,
                "control_restore_rejected",
                format!("control recovery restore rejected: {err}"),
            );
        }
    };

    json_response(
        200,
        &json!({
            "status": "success",
            "data": {
                "snapshotPath": snapshot_path.display().to_string(),
                "sourceNodeId": loaded_snapshot.source_node_id,
                "forceLocalLeader": force_local_leader,
                "membershipEpoch": restored_state.membership_epoch,
                "ringVersion": restored_state.ring_version,
                "leaderNodeId": restored_state.leader_node_id,
                "appliedLogIndex": restored_state.applied_log_index,
                "appliedLogTerm": restored_state.applied_log_term,
                "commitIndex": loaded_snapshot.control_log.commit_index,
                "currentTerm": loaded_snapshot.control_log.current_term
            }
        }),
    )
}

pub(crate) async fn handle_admin_cluster_snapshot(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    request: &HttpRequest,
    admin_path_prefix: Option<&Path>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let payload = match parse_optional_json_body::<ClusterSnapshotAdminPayload>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => return admin_cluster_snapshot_error_response(400, "invalid_request", err),
    };
    let root_path =
        non_empty_param(request.param("path")).or_else(|| non_empty_param(payload.path));
    let manifest_path_override = non_empty_param(
        request
            .param("manifest_path")
            .or_else(|| request.param("manifestPath")),
    )
    .or_else(|| non_empty_param(payload.manifest_path));
    let control_snapshot_path_override = non_empty_param(
        request
            .param("control_snapshot_path")
            .or_else(|| request.param("controlSnapshotPath")),
    )
    .or_else(|| non_empty_param(payload.control_snapshot_path));
    let node_path_overrides = match normalize_named_paths(payload.node_paths, "nodePaths") {
        Ok(paths) => paths,
        Err(err) => return admin_cluster_snapshot_error_response(400, "invalid_request", err),
    };

    let Some(root_path) = root_path else {
        return admin_cluster_snapshot_error_response(
            400,
            "invalid_request",
            "missing required parameter 'path'",
        );
    };
    let root_path = match resolve_admin_path(Path::new(&root_path), admin_path_prefix, false) {
        Ok(path) => path,
        Err(err) => return admin_cluster_snapshot_error_response(400, "invalid_path", err),
    };
    let manifest_path = match manifest_path_override {
        Some(path) => match resolve_admin_path(Path::new(&path), admin_path_prefix, false) {
            Ok(path) => path,
            Err(err) => return admin_cluster_snapshot_error_response(400, "invalid_path", err),
        },
        None => root_path.join("cluster-snapshot-manifest.json"),
    };
    let control_snapshot_path = match control_snapshot_path_override {
        Some(path) => match resolve_admin_path(Path::new(&path), admin_path_prefix, false) {
            Ok(path) => path,
            Err(err) => return admin_cluster_snapshot_error_response(400, "invalid_path", err),
        },
        None => root_path.join("control-recovery.json"),
    };

    let Some(cluster_context) = cluster_context else {
        return admin_cluster_snapshot_error_response(
            503,
            "control_plane_unavailable",
            "cluster runtime is not available",
        );
    };
    if let Err(response) = ensure_local_control_leader(cluster_context).await {
        return response;
    }
    let Some(consensus) = cluster_context.control_consensus.as_ref() else {
        return admin_cluster_snapshot_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };

    let control_snapshot = match build_control_recovery_snapshot_file(cluster_context) {
        Ok(snapshot) => snapshot,
        Err(err) => {
            return admin_cluster_snapshot_error_response(500, "control_snapshot_failed", err);
        }
    };
    let control_snapshot_path_clone = control_snapshot_path.clone();
    let control_snapshot_for_write = control_snapshot.clone();
    let control_snapshot_write = tokio::task::spawn_blocking(move || {
        write_control_recovery_snapshot_file(
            &control_snapshot_path_clone,
            &control_snapshot_for_write,
        )
    })
    .await;
    match control_snapshot_write {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            return admin_cluster_snapshot_error_response(500, "control_snapshot_failed", err);
        }
        Err(err) => {
            return admin_cluster_snapshot_error_response(
                500,
                "control_snapshot_failed",
                format!("control recovery snapshot task failed: {err}"),
            );
        }
    }

    let nodes = cluster_snapshot_nodes(&control_snapshot.control_state);
    let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
    let mut cluster_nodes = Vec::with_capacity(nodes.len());
    for node in nodes {
        let requested_snapshot_path = node_path_overrides
            .get(node.id.as_str())
            .cloned()
            .unwrap_or_else(|| {
                root_path
                    .join("nodes")
                    .join(node.id.as_str())
                    .join("data.snapshot")
                    .display()
                    .to_string()
            });
        let snapshot = if node.id == local_node_id {
            let resolved = match resolve_admin_path(
                Path::new(&requested_snapshot_path),
                admin_path_prefix,
                false,
            ) {
                Ok(path) => path,
                Err(err) => {
                    return admin_cluster_snapshot_error_response(400, "invalid_path", err);
                }
            };
            match perform_local_data_snapshot(
                storage,
                metadata_store,
                exemplar_store,
                rules_runtime,
                &resolved,
                Some(cluster_context),
            )
            .await
            {
                Ok(response) => response,
                Err(err) => {
                    return admin_cluster_snapshot_error_response(503, "snapshot_failed", err);
                }
            }
        } else {
            match cluster_context
                .rpc_client
                .data_snapshot(
                    node.endpoint.as_str(),
                    &InternalDataSnapshotRequest {
                        path: requested_snapshot_path,
                    },
                )
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    return admin_cluster_snapshot_error_response(
                        503,
                        "snapshot_failed",
                        format!(
                            "remote snapshot failed for node '{}' via {}: {err}",
                            node.id, node.endpoint
                        ),
                    );
                }
            }
        };
        cluster_nodes.push(ClusterSnapshotNodeArtifactV1 {
            node_id: node.id,
            endpoint: node.endpoint,
            status: node.status,
            snapshot_path: snapshot.path,
            snapshot_created_unix_ms: snapshot.created_unix_ms,
            snapshot_duration_ms: snapshot.duration_ms,
            snapshot_size_bytes: snapshot.size_bytes,
        });
    }
    cluster_nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    let rpo_estimate_ms = cluster_nodes
        .iter()
        .map(|node| node.snapshot_created_unix_ms)
        .max()
        .unwrap_or(control_snapshot.created_unix_ms)
        .saturating_sub(control_snapshot.created_unix_ms);
    let snapshot_id = format!(
        "tsink-cluster-{}-e{}-r{}",
        control_snapshot.created_unix_ms,
        control_snapshot.control_state.membership_epoch,
        control_snapshot.control_state.ring_version
    );
    let manifest = ClusterSnapshotManifestFileV1 {
        magic: CLUSTER_SNAPSHOT_MANIFEST_MAGIC.to_string(),
        schema_version: CLUSTER_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
        snapshot_id: snapshot_id.clone(),
        created_unix_ms: control_snapshot.created_unix_ms,
        coordinator_node_id: cluster_context.runtime.membership.local_node_id.clone(),
        manifest_path: manifest_path.display().to_string(),
        control_snapshot_path: control_snapshot_path.display().to_string(),
        membership_epoch: control_snapshot.control_state.membership_epoch,
        ring_version: control_snapshot.control_state.ring_version,
        leader_node_id: control_snapshot.control_state.leader_node_id.clone(),
        applied_log_index: control_snapshot.control_state.applied_log_index,
        applied_log_term: control_snapshot.control_state.applied_log_term,
        current_term: control_snapshot.control_log.current_term,
        commit_index: control_snapshot.control_log.commit_index,
        rpo_estimate_ms,
        cluster_nodes,
        control_snapshot,
    };
    let manifest_path_clone = manifest_path.clone();
    let manifest_for_write = manifest.clone();
    let manifest_write = tokio::task::spawn_blocking(move || {
        write_cluster_snapshot_manifest_file(&manifest_path_clone, &manifest_for_write)
    })
    .await;
    match manifest_write {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            return admin_cluster_snapshot_error_response(500, "snapshot_manifest_failed", err);
        }
        Err(err) => {
            return admin_cluster_snapshot_error_response(
                500,
                "snapshot_manifest_failed",
                format!("cluster snapshot manifest task failed: {err}"),
            );
        }
    }

    let state = consensus.current_state();
    json_response(
        200,
        &json!({
            "status": "success",
            "data": {
                "snapshotId": snapshot_id,
                "manifestPath": manifest_path.display().to_string(),
                "controlSnapshotPath": control_snapshot_path.display().to_string(),
                "coordinatorNodeId": cluster_context.runtime.membership.local_node_id.clone(),
                "membershipEpoch": state.membership_epoch,
                "ringVersion": state.ring_version,
                "leaderNodeId": state.leader_node_id,
                "appliedLogIndex": state.applied_log_index,
                "appliedLogTerm": state.applied_log_term,
                "commitIndex": manifest.commit_index,
                "currentTerm": manifest.current_term,
                "createdUnixMs": manifest.created_unix_ms,
                "rpoEstimateMs": manifest.rpo_estimate_ms,
                "clusterNodes": manifest.cluster_nodes.iter().map(|node| {
                    json!({
                        "nodeId": node.node_id,
                        "endpoint": node.endpoint,
                        "status": node.status,
                        "snapshotPath": node.snapshot_path,
                        "snapshotCreatedUnixMs": node.snapshot_created_unix_ms,
                        "snapshotDurationMs": node.snapshot_duration_ms,
                        "snapshotSizeBytes": node.snapshot_size_bytes
                    })
                }).collect::<Vec<_>>()
            }
        }),
    )
}

pub(crate) async fn handle_admin_cluster_restore(
    request: &HttpRequest,
    admin_path_prefix: Option<&Path>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let payload = match parse_optional_json_body::<ClusterRestoreAdminPayload>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => return admin_cluster_snapshot_error_response(400, "invalid_request", err),
    };
    let snapshot_path = non_empty_param(
        request
            .param("snapshot_path")
            .or_else(|| request.param("snapshotPath")),
    )
    .or_else(|| non_empty_param(payload.snapshot_path));
    let restore_root = non_empty_param(
        request
            .param("restore_root")
            .or_else(|| request.param("restoreRoot")),
    )
    .or_else(|| non_empty_param(payload.restore_root));
    let report_path_override = non_empty_param(
        request
            .param("report_path")
            .or_else(|| request.param("reportPath")),
    )
    .or_else(|| non_empty_param(payload.report_path));
    let force_local_leader = match request
        .param("force_local_leader")
        .or_else(|| request.param("forceLocalLeader"))
    {
        Some(value) => match parse_admin_bool(&value) {
            Ok(value) => value,
            Err(err) => {
                return admin_cluster_snapshot_error_response(400, "invalid_request", err);
            }
        },
        None => payload.force_local_leader.unwrap_or(false),
    };
    let data_path_overrides = match normalize_named_paths(payload.data_paths, "dataPaths") {
        Ok(paths) => paths,
        Err(err) => return admin_cluster_snapshot_error_response(400, "invalid_request", err),
    };

    let Some(snapshot_path) = snapshot_path else {
        return admin_cluster_snapshot_error_response(
            400,
            "invalid_request",
            "missing required parameter 'snapshot_path' (or 'snapshotPath')",
        );
    };
    let Some(restore_root) = restore_root else {
        return admin_cluster_snapshot_error_response(
            400,
            "invalid_request",
            "missing required parameter 'restore_root' (or 'restoreRoot')",
        );
    };
    let snapshot_path = match resolve_admin_path(Path::new(&snapshot_path), admin_path_prefix, true)
    {
        Ok(path) => path,
        Err(err) => return admin_cluster_snapshot_error_response(400, "invalid_path", err),
    };
    let restore_root = match resolve_admin_path(Path::new(&restore_root), admin_path_prefix, false)
    {
        Ok(path) => path,
        Err(err) => return admin_cluster_snapshot_error_response(400, "invalid_path", err),
    };
    let report_path = match report_path_override {
        Some(path) => match resolve_admin_path(Path::new(&path), admin_path_prefix, false) {
            Ok(path) => path,
            Err(err) => return admin_cluster_snapshot_error_response(400, "invalid_path", err),
        },
        None => restore_root.join("cluster-restore-report.json"),
    };

    let Some(cluster_context) = cluster_context else {
        return admin_cluster_snapshot_error_response(
            503,
            "control_plane_unavailable",
            "cluster runtime is not available",
        );
    };
    let Some(consensus) = cluster_context.control_consensus.as_ref() else {
        return admin_cluster_snapshot_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };
    if !force_local_leader {
        if let Err(response) = ensure_local_control_leader(cluster_context).await {
            return response;
        }
    }

    let snapshot_path_clone = snapshot_path.clone();
    let manifest = match tokio::task::spawn_blocking(move || {
        load_cluster_snapshot_manifest_file(&snapshot_path_clone)
    })
    .await
    {
        Ok(Ok(manifest)) => manifest,
        Ok(Err(err)) => {
            return admin_cluster_snapshot_error_response(400, "invalid_cluster_snapshot", err);
        }
        Err(err) => {
            return admin_cluster_snapshot_error_response(
                500,
                "cluster_restore_failed",
                format!("cluster snapshot manifest load task failed: {err}"),
            );
        }
    };

    let restored_state = match consensus.restore_recovery_snapshot(
        manifest.control_snapshot.control_state.clone(),
        manifest.control_snapshot.control_log.clone(),
        force_local_leader,
    ) {
        Ok(state) => state,
        Err(err) => {
            return admin_cluster_snapshot_error_response(
                409,
                "control_restore_rejected",
                format!("cluster control restore rejected: {err}"),
            );
        }
    };

    let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
    let restore_started = Instant::now();
    let mut cluster_nodes = Vec::with_capacity(manifest.cluster_nodes.len());
    for node in &manifest.cluster_nodes {
        let requested_data_path = data_path_overrides
            .get(node.node_id.as_str())
            .cloned()
            .unwrap_or_else(|| {
                restore_root
                    .join("nodes")
                    .join(node.node_id.as_str())
                    .join("data")
                    .display()
                    .to_string()
            });
        let restored = if node.node_id == local_node_id {
            let resolved =
                match resolve_admin_path(Path::new(&requested_data_path), admin_path_prefix, false)
                {
                    Ok(path) => path,
                    Err(err) => {
                        return admin_cluster_snapshot_error_response(400, "invalid_path", err);
                    }
                };
            let snapshot_path = match resolve_admin_path(
                Path::new(node.snapshot_path.as_str()),
                admin_path_prefix,
                true,
            ) {
                Ok(path) => path,
                Err(err) => {
                    return admin_cluster_snapshot_error_response(400, "invalid_path", err);
                }
            };
            match perform_local_data_restore(&snapshot_path, &resolved, Some(cluster_context)).await
            {
                Ok(response) => response,
                Err(err) => {
                    return admin_cluster_snapshot_error_response(503, "restore_failed", err);
                }
            }
        } else {
            match cluster_context
                .rpc_client
                .data_restore(
                    node.endpoint.as_str(),
                    &InternalDataRestoreRequest {
                        snapshot_path: node.snapshot_path.clone(),
                        data_path: requested_data_path,
                    },
                )
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    return admin_cluster_snapshot_error_response(
                        503,
                        "restore_failed",
                        format!(
                            "remote restore failed for node '{}' via {}: {err}",
                            node.node_id, node.endpoint
                        ),
                    );
                }
            }
        };
        cluster_nodes.push(ClusterRestoreNodeArtifactV1 {
            node_id: node.node_id.clone(),
            endpoint: node.endpoint.clone(),
            snapshot_path: restored.snapshot_path,
            data_path: restored.data_path,
            restored_unix_ms: restored.restored_unix_ms,
            restore_duration_ms: restored.duration_ms,
        });
    }
    cluster_nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    let rto_ms = u64::try_from(restore_started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let report = ClusterRestoreReportFileV1 {
        magic: CLUSTER_RESTORE_REPORT_MAGIC.to_string(),
        schema_version: CLUSTER_RESTORE_REPORT_SCHEMA_VERSION,
        restored_unix_ms: unix_timestamp_millis(),
        coordinator_node_id: cluster_context.runtime.membership.local_node_id.clone(),
        source_snapshot_path: snapshot_path.display().to_string(),
        source_snapshot_id: manifest.snapshot_id.clone(),
        report_path: report_path.display().to_string(),
        restore_root: restore_root.display().to_string(),
        force_local_leader,
        restored_membership_epoch: restored_state.membership_epoch,
        restored_ring_version: restored_state.ring_version,
        restored_leader_node_id: restored_state.leader_node_id.clone(),
        rpo_estimate_ms: manifest.rpo_estimate_ms,
        rto_ms,
        cluster_nodes,
    };
    let report_path_clone = report_path.clone();
    let report_for_write = report.clone();
    let report_write = tokio::task::spawn_blocking(move || {
        write_cluster_restore_report_file(&report_path_clone, &report_for_write)
    })
    .await;
    match report_write {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            return admin_cluster_snapshot_error_response(500, "cluster_restore_failed", err);
        }
        Err(err) => {
            return admin_cluster_snapshot_error_response(
                500,
                "cluster_restore_failed",
                format!("cluster restore report task failed: {err}"),
            );
        }
    }

    json_response(
        200,
        &json!({
            "status": "success",
            "data": {
                "snapshotPath": snapshot_path.display().to_string(),
                "snapshotId": manifest.snapshot_id,
                "reportPath": report_path.display().to_string(),
                "restoreRoot": restore_root.display().to_string(),
                "forceLocalLeader": force_local_leader,
                "membershipEpoch": restored_state.membership_epoch,
                "ringVersion": restored_state.ring_version,
                "leaderNodeId": restored_state.leader_node_id,
                "rpoEstimateMs": report.rpo_estimate_ms,
                "rtoMs": report.rto_ms,
                "clusterNodes": report.cluster_nodes.iter().map(|node| {
                    json!({
                        "nodeId": node.node_id,
                        "endpoint": node.endpoint,
                        "snapshotPath": node.snapshot_path,
                        "dataPath": node.data_path,
                        "restoredUnixMs": node.restored_unix_ms,
                        "restoreDurationMs": node.restore_duration_ms
                    })
                }).collect::<Vec<_>>(),
                "nextStep": "restart recovery-cluster nodes using the restored data paths, then run query validation"
            }
        }),
    )
}
