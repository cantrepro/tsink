use super::*;

#[derive(Debug, Clone, Copy, Default)]
struct AdminDeleteSeriesProgress {
    matchers_processed: u64,
    matched_series: u64,
    tombstones_applied: u64,
}

impl AdminDeleteSeriesProgress {
    fn add_outcome(&mut self, matched_series: u64, tombstones_applied: u64) {
        self.matched_series = self.matched_series.saturating_add(matched_series);
        self.tombstones_applied = self.tombstones_applied.saturating_add(tombstones_applied);
    }

    fn has_committed_tombstones(self) -> bool {
        self.tombstones_applied > 0
    }
}

#[derive(Debug)]
enum AdminDeleteSeriesExecutionError {
    Storage(tsink::TsinkError),
    Partial {
        source: tsink::TsinkError,
        progress: AdminDeleteSeriesProgress,
    },
}

impl AdminDeleteSeriesExecutionError {
    fn retention_usage_status(&self) -> RetentionUsageStatus {
        match self {
            Self::Partial { source, .. }
                if server_persistence_error_category(source)
                    == WriteRejectionCategory::DiskQuotaExceeded =>
            {
                RetentionUsageStatus::Partial
            }
            Self::Partial { .. } => RetentionUsageStatus::Indeterminate,
            Self::Storage(_) => RetentionUsageStatus::Indeterminate,
        }
    }
}

fn partial_delete_series_error_response(
    source: &tsink::TsinkError,
    matchers_processed: u64,
    matched_series: u64,
    tombstones_applied: u64,
) -> HttpResponse {
    let category = server_persistence_error_category(source);
    let (status, error_code, retry_after) = write_rejection_http_mapping(category);
    let outcome = if category == WriteRejectionCategory::DiskQuotaExceeded {
        "partial"
    } else {
        "indeterminate_backend"
    };
    let diagnostic = if category == WriteRejectionCategory::DiskQuotaExceeded {
        format!("delete_series stopped after committed partial progress: {source}")
    } else {
        "delete_series failed after committed partial progress; the remaining durable outcome is indeterminate"
            .to_string()
    };
    let diagnostic = bounded_write_rejection_diagnostic(&diagnostic).to_string();
    let mut response = json_response(
        status,
        &json!({
            "status": "error",
            "errorType": "partial_delete",
            "code": error_code,
            "error": diagnostic,
            "data": {
                "outcome": outcome,
                "matchersProcessed": matchers_processed,
                "matchedSeries": matched_series,
                "tombstonesApplied": tombstones_applied,
            }
        }),
    )
    .with_header(WRITE_ERROR_CODE_HEADER, error_code)
    .with_header(
        DELETE_MATCHERS_PROCESSED_HEADER,
        matchers_processed.to_string(),
    )
    .with_header(DELETE_MATCHED_SERIES_HEADER, matched_series.to_string())
    .with_header(
        DELETE_TOMBSTONES_APPLIED_HEADER,
        tombstones_applied.to_string(),
    );
    if category == WriteRejectionCategory::DiskQuotaExceeded {
        response = response
            .with_header(WRITE_PARTIAL_HEADER, "true")
            .with_header(WRITE_OUTCOME_HEADER, "partial");
    } else {
        response = with_indeterminate_backend_headers(response);
    }
    if let Some(retry_after) = retry_after {
        response = response.with_header("Retry-After", retry_after);
    }
    response
}

fn admin_delete_series_execution_error_response(
    err: &AdminDeleteSeriesExecutionError,
) -> HttpResponse {
    match err {
        AdminDeleteSeriesExecutionError::Storage(source) => delete_series_error_response(source),
        AdminDeleteSeriesExecutionError::Partial { source, progress } => {
            partial_delete_series_error_response(
                source,
                progress.matchers_processed,
                progress.matched_series,
                progress.tombstones_applied,
            )
        }
    }
}

fn admin_delete_series_task_failure_response(progress: AdminDeleteSeriesProgress) -> HttpResponse {
    const DIAGNOSTIC: &str =
        "delete_series task did not complete; the durable outcome is indeterminate";
    if !progress.has_committed_tombstones() {
        return indeterminate_backend_write_error_response(500, "write_internal", DIAGNOSTIC);
    }

    with_indeterminate_backend_headers(
        json_response(
            500,
            &json!({
                "status": "error",
                "errorType": "partial_delete",
                "code": "write_internal",
                "error": DIAGNOSTIC,
                "data": {
                    "outcome": "indeterminate_backend",
                    "matchersProcessed": progress.matchers_processed,
                    "matchedSeries": progress.matched_series,
                    "tombstonesApplied": progress.tombstones_applied,
                }
            }),
        )
        .with_header(WRITE_ERROR_CODE_HEADER, "write_internal")
        .with_header(
            DELETE_MATCHERS_PROCESSED_HEADER,
            progress.matchers_processed.to_string(),
        )
        .with_header(
            DELETE_MATCHED_SERIES_HEADER,
            progress.matched_series.to_string(),
        )
        .with_header(
            DELETE_TOMBSTONES_APPLIED_HEADER,
            progress.tombstones_applied.to_string(),
        ),
    )
}

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
        Ok(snapshot) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "path": response_path,
                    "sizeBytes": snapshot.size_bytes
                }
            }),
        ),
        Err(err) => text_response(500, &format!("snapshot failed: {err}")),
    }
}

pub(crate) async fn handle_admin_restore(
    request: &HttpRequest,
    admin_path_prefix: Option<&Path>,
    local_disk_budget: Option<&tsink::LocalDiskBudget>,
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
    if let Err(err) = validate_restore_target_outside_live_root(&data_path_buf, local_disk_budget) {
        return text_response(409, &err);
    }

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
        None,
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
        Some(value) => match parse_storage_timestamp(&value, precision) {
            Ok(timestamp) => timestamp,
            Err(err) => return text_response(400, &format!("invalid 'start': {err}")),
        },
        None => i64::MIN,
    };
    let end = match request.param("end").or(payload_end) {
        Some(value) => match parse_storage_timestamp(&value, precision) {
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

    let storage = Arc::clone(storage);
    let delete_tenant_id = tenant_id.clone();
    let matcher_count = matcher_count.min(u64::MAX as usize) as u64;
    let known_progress = Arc::new(std::sync::Mutex::new(AdminDeleteSeriesProgress::default()));
    let task_progress = Arc::clone(&known_progress);
    let result = tokio::task::spawn_blocking(move || {
        let mut progress = AdminDeleteSeriesProgress::default();
        for selection in selections {
            let completed_progress = progress;
            let mut selector_progress = AdminDeleteSeriesProgress::default();
            let mut observe_inner_delete = |outcome: tsink::DeleteSeriesResult| {
                selector_progress.add_outcome(outcome.matched_series, outcome.tombstones_applied);
                let mut observed_progress = completed_progress;
                observed_progress.matchers_processed =
                    observed_progress.matchers_processed.saturating_add(1);
                observed_progress.add_outcome(
                    selector_progress.matched_series,
                    selector_progress.tombstones_applied,
                );
                *task_progress
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = observed_progress;
            };
            match tenant::delete_series_with_progress(
                &storage,
                &delete_tenant_id,
                &selection,
                &mut observe_inner_delete,
            ) {
                Ok(outcome) => {
                    progress.matchers_processed = progress.matchers_processed.saturating_add(1);
                    progress.add_outcome(outcome.matched_series, outcome.tombstones_applied);
                    *task_progress
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = progress;
                }
                Err(tenant::TenantDeleteSeriesError::Partial {
                    source,
                    matched_series,
                    tombstones_applied,
                }) => {
                    progress.matchers_processed = progress.matchers_processed.saturating_add(1);
                    progress.add_outcome(matched_series, tombstones_applied);
                    *task_progress
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = progress;
                    return Err(AdminDeleteSeriesExecutionError::Partial { source, progress });
                }
                Err(tenant::TenantDeleteSeriesError::Storage(source))
                    if progress.has_committed_tombstones() =>
                {
                    return Err(AdminDeleteSeriesExecutionError::Partial { source, progress });
                }
                Err(tenant::TenantDeleteSeriesError::Storage(source)) => {
                    return Err(AdminDeleteSeriesExecutionError::Storage(source));
                }
            }
        }
        Ok::<_, AdminDeleteSeriesExecutionError>(progress)
    })
    .await;

    match result {
        Ok(Ok(progress)) => {
            record_retention_usage(
                usage_accounting,
                &tenant_id,
                progress.matched_series,
                progress.tombstones_applied,
                matcher_count,
                elapsed_nanos_since(started),
                RetentionUsageStatus::Success,
            )
            .await;
            json_response(
                200,
                &json!({
                    "status": "success",
                    "data": {
                        "tenantId": tenant_id,
                        "matchersProcessed": matcher_count,
                        "matchedSeries": progress.matched_series,
                        "tombstonesApplied": progress.tombstones_applied,
                        "start": start,
                        "end": end
                    }
                }),
            )
        }
        Ok(Err(err @ AdminDeleteSeriesExecutionError::Partial { progress, .. })) => {
            let usage_status = err.retention_usage_status();
            record_retention_usage(
                usage_accounting,
                &tenant_id,
                progress.matched_series,
                progress.tombstones_applied,
                progress.matchers_processed,
                elapsed_nanos_since(started),
                usage_status,
            )
            .await;
            admin_delete_series_execution_error_response(&err)
        }
        Ok(Err(err)) => admin_delete_series_execution_error_response(&err),
        Err(_) => {
            let progress = *known_progress
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            record_retention_usage(
                usage_accounting,
                &tenant_id,
                progress.matched_series,
                progress.tombstones_applied,
                progress.matchers_processed,
                elapsed_nanos_since(started),
                RetentionUsageStatus::Indeterminate,
            )
            .await;
            admin_delete_series_task_failure_response(progress)
        }
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
    local_disk_budget: Option<&tsink::LocalDiskBudget>,
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

    let preflight_control_state = match consensus.preflight_recovery_snapshot(
        manifest.control_snapshot.control_state.clone(),
        &manifest.control_snapshot.control_log,
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

    enum RestoreTarget {
        Local {
            snapshot_path: PathBuf,
            data_path: PathBuf,
        },
        Remote {
            snapshot_path: String,
            data_path: String,
        },
    }

    struct RestorePlan {
        node_id: String,
        endpoint: String,
        target: RestoreTarget,
    }

    let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
    let mut restore_plan = Vec::with_capacity(manifest.cluster_nodes.len());
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
        let target = if node.node_id == local_node_id {
            let data_path =
                match resolve_admin_path(Path::new(&requested_data_path), admin_path_prefix, false)
                {
                    Ok(path) => path,
                    Err(err) => {
                        return admin_cluster_snapshot_error_response(400, "invalid_path", err);
                    }
                };
            if let Err(err) =
                validate_restore_target_outside_live_root(&data_path, local_disk_budget)
            {
                return admin_cluster_snapshot_error_response(
                    409,
                    "live_data_path_restore_rejected",
                    err,
                );
            }
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
            RestoreTarget::Local {
                snapshot_path,
                data_path,
            }
        } else {
            RestoreTarget::Remote {
                snapshot_path: node.snapshot_path.clone(),
                data_path: requested_data_path,
            }
        };
        restore_plan.push(RestorePlan {
            node_id: node.node_id.clone(),
            endpoint: node.endpoint.clone(),
            target,
        });
    }

    let restore_started = Instant::now();
    let mut cluster_nodes = Vec::with_capacity(restore_plan.len());
    for node in restore_plan {
        let restored = match node.target {
            RestoreTarget::Local {
                snapshot_path,
                data_path,
            } => {
                match perform_local_data_restore(&snapshot_path, &data_path, Some(cluster_context))
                    .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        return admin_cluster_snapshot_error_response(503, "restore_failed", err);
                    }
                }
            }
            RestoreTarget::Remote {
                snapshot_path,
                data_path,
            } => match cluster_context
                .rpc_client
                .data_restore(
                    node.endpoint.as_str(),
                    &InternalDataRestoreRequest {
                        snapshot_path,
                        data_path,
                    },
                )
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    if let RpcError::HttpStatus {
                        status,
                        error_code,
                        message,
                        ..
                    } = &err
                    {
                        if (400..500).contains(status) {
                            return admin_cluster_snapshot_error_response(
                                *status,
                                error_code.as_deref().unwrap_or("remote_restore_rejected"),
                                format!(
                                    "remote restore rejected for node '{}' via {}: {message}",
                                    node.node_id, node.endpoint
                                ),
                            );
                        }
                    }
                    return admin_cluster_snapshot_error_response(
                        503,
                        "restore_failed",
                        format!(
                            "remote restore failed for node '{}' via {}: {err}",
                            node.node_id, node.endpoint
                        ),
                    );
                }
            },
        };
        cluster_nodes.push(ClusterRestoreNodeArtifactV1 {
            node_id: node.node_id,
            endpoint: node.endpoint,
            snapshot_path: restored.snapshot_path,
            data_path: restored.data_path,
            restored_unix_ms: restored.restored_unix_ms,
            restore_duration_ms: restored.duration_ms,
        });
    }

    let restored_state = match consensus.restore_recovery_snapshot(
        preflight_control_state,
        manifest.control_snapshot.control_log.clone(),
        false,
    ) {
        Ok(state) => state,
        Err(err) => {
            return admin_cluster_snapshot_error_response(
                409,
                "control_restore_rejected",
                format!("cluster control restore rejected after data restore: {err}"),
            );
        }
    };

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
