use super::*;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_admin_support_bundle(
    storage: &Arc<dyn Storage>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    rbac_registry: Option<&RbacRegistry>,
    security_manager: Option<&SecurityManager>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let tenant_id = match support_bundle_tenant_id(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let actor = derive_audit_actor(request);
    let status_request =
        support_bundle_request_with_path(request, "/api/v1/status/tsdb", &tenant_id);
    let rbac_audit_request =
        support_bundle_request_with_path(request, "/api/v1/admin/rbac/audit?limit=50", &tenant_id);
    let cluster_audit_request = support_bundle_request_with_path(
        request,
        "/api/v1/admin/cluster/audit?limit=50",
        &tenant_id,
    );

    let bundle = json!({
        "generatedUnixMs": unix_timestamp_millis(),
        "tenantId": tenant_id,
        "requestedBy": {
            "id": actor.id,
            "authScope": actor.auth_scope,
        },
        "serverVersion": env!("CARGO_PKG_VERSION"),
        "sections": {
            "statusTsdb": support_bundle_section_from_response(
                handle_tsdb_status(
                    storage,
                    exemplar_store,
                    &status_request,
                    cluster_context,
                    edge_sync_context,
                    tenant_registry,
                    rbac_registry,
                    security_manager,
                    usage_accounting,
                    None,
                )
                .await
            ),
            "usage": support_bundle_usage_section(storage, usage_accounting, &tenant_id),
            "rbacState": support_bundle_section_from_response(handle_admin_rbac_state(rbac_registry)),
            "rbacAudit": support_bundle_section_from_response(
                handle_admin_rbac_audit(&rbac_audit_request, rbac_registry)
            ),
            "securityState": support_bundle_section_from_response(
                handle_admin_secrets_state(rbac_registry, security_manager)
            ),
            "clusterAudit": support_bundle_section_from_response(
                handle_admin_cluster_audit_query(&cluster_audit_request, cluster_context)
            ),
            "clusterHandoff": support_bundle_section_from_response(
                handle_admin_cluster_handoff_status(cluster_context).await
            ),
            "clusterRepair": support_bundle_section_from_response(
                handle_admin_cluster_repair_status(cluster_context).await
            ),
            "clusterRebalance": support_bundle_section_from_response(
                handle_admin_cluster_rebalance_status(storage, cluster_context).await
            ),
            "rules": support_bundle_section_from_response(
                handle_admin_rules_status(rules_runtime).await
            ),
            "rollups": support_bundle_section_from_response(
                handle_admin_rollups_status(storage).await
            )
        }
    });

    match serde_json::to_vec_pretty(&bundle) {
        Ok(body) => HttpResponse::new(200, body)
            .with_header("Content-Type", "application/json")
            .with_header("Cache-Control", "no-store")
            .with_header(
                "Content-Disposition",
                format!(
                    "attachment; filename=\"tsink-support-bundle-{}-{}.json\"",
                    support_bundle_filename_component(
                        bundle["tenantId"]
                            .as_str()
                            .unwrap_or(tenant::DEFAULT_TENANT_ID)
                    ),
                    bundle["generatedUnixMs"].as_u64().unwrap_or(0)
                ),
            ),
        Err(err) => text_response(500, &format!("failed to encode support bundle: {err}")),
    }
}

pub(crate) fn handle_admin_usage_report(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let Some(usage_accounting) = usage_accounting else {
        return text_response(503, "usage accounting is unavailable");
    };
    let (tenant_id, start_unix_ms, end_unix_ms, bucket_width, reconcile) =
        match parse_usage_report_filter(request) {
            Ok(filter) => filter,
            Err(response) => return response,
        };
    let reconciled_storage_snapshots = if reconcile {
        match usage_accounting.reconcile_storage(storage) {
            Ok(snapshots) => snapshots,
            Err(err) => {
                return text_response(500, &format!("usage storage reconciliation failed: {err}"))
            }
        }
    } else {
        Vec::new()
    };
    let report = usage_accounting.report(
        tenant_id.as_deref(),
        start_unix_ms,
        end_unix_ms,
        bucket_width,
    );
    json_response(
        200,
        &json!({
            "status": "success",
            "data": {
                "report": report,
                "reconciliation": usage_reconciliation_json(&report, &storage.observability_snapshot()),
                "reconciledStorageSnapshots": reconciled_storage_snapshots,
            }
        }),
    )
}

pub(crate) fn handle_admin_usage_export(
    request: &HttpRequest,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let Some(usage_accounting) = usage_accounting else {
        return text_response(503, "usage accounting is unavailable");
    };
    let (tenant_id, start_unix_ms, end_unix_ms, _, _) = match parse_usage_report_filter(request) {
        Ok(filter) => filter,
        Err(response) => return response,
    };
    let records = usage_accounting.export_records(tenant_id.as_deref(), start_unix_ms, end_unix_ms);
    let mut body = String::new();
    for record in records {
        match serde_json::to_string(&record) {
            Ok(line) => {
                body.push_str(&line);
                body.push('\n');
            }
            Err(err) => {
                return text_response(500, &format!("failed to encode usage export: {err}"))
            }
        }
    }
    HttpResponse::new(200, body).with_header("Content-Type", "application/x-ndjson")
}

pub(crate) async fn handle_admin_usage_reconcile(
    storage: &Arc<dyn Storage>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let Some(usage_accounting) = usage_accounting else {
        return text_response(503, "usage accounting is unavailable");
    };
    match usage_accounting.reconcile_storage(storage) {
        Ok(snapshots) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "journal": usage_accounting.ledger_status(),
                    "storageSnapshots": snapshots,
                }
            }),
        ),
        Err(err) => text_response(500, &format!("usage storage reconciliation failed: {err}")),
    }
}
