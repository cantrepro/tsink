use super::*;

pub(super) async fn route_request(
    app_context: AppContext<'_>,
    request_context: RequestContext<'_>,
) -> HttpResponse {
    let AppContext {
        storage,
        metadata_store,
        exemplar_store,
        rules_runtime,
        engine,
        cluster_context,
        tenant_registry,
        rbac_registry,
        edge_sync_context,
        security_manager,
        usage_accounting,
        managed_control_plane,
    } = app_context;
    let RequestContext {
        request,
        server_start,
        timestamp_precision,
        admin_api_enabled,
        admin_path_prefix,
        internal_api,
    } = request_context;
    let path = request.path_without_query().to_owned();
    let public_read_context = PublicReadContext::new(
        cluster_context,
        tenant_registry,
        managed_control_plane,
        usage_accounting,
    );

    if request.method == "GET" {
        if let Some(label_name) = label_values_path(path.as_str()) {
            return handle_label_values(storage, &request, label_name, public_read_context).await;
        }
    }

    match (request.method.as_str(), path.as_str()) {
        ("POST", "/internal/v1/ingest_write") => {
            handle_internal_ingest_write(
                storage,
                metadata_store,
                exemplar_store,
                &request,
                internal_api,
                cluster_context,
                edge_sync_context,
            )
            .await
        }
        ("POST", "/internal/v1/ingest_rows") => {
            handle_internal_ingest_rows(
                storage,
                &request,
                internal_api,
                cluster_context,
                edge_sync_context,
            )
            .await
        }
        ("POST", "/internal/v1/select") => {
            handle_internal_select(storage, &request, internal_api, cluster_context).await
        }
        ("POST", "/internal/v1/select_batch") => {
            handle_internal_select_batch(storage, &request, internal_api, cluster_context).await
        }
        ("POST", "/internal/v1/select_series") => {
            handle_internal_select_series(storage, &request, internal_api, cluster_context).await
        }
        ("POST", "/internal/v1/query_exemplars") => {
            handle_internal_query_exemplars(exemplar_store, &request, internal_api, cluster_context)
                .await
        }
        ("GET" | "POST", "/internal/v1/list_metrics") => {
            handle_internal_list_metrics(storage, &request, internal_api, cluster_context).await
        }
        ("POST", "/internal/v1/digest_window") => {
            handle_internal_digest_window(storage, &request, internal_api, cluster_context).await
        }
        ("POST", "/internal/v1/snapshot_data") => {
            handle_internal_snapshot_data(
                storage,
                metadata_store,
                exemplar_store,
                rules_runtime,
                &request,
                internal_api,
                cluster_context,
                admin_path_prefix,
            )
            .await
        }
        ("POST", "/internal/v1/restore_data") => {
            handle_internal_restore_data(&request, internal_api, cluster_context, admin_path_prefix)
                .await
        }
        ("POST", "/internal/v1/repair_backfill") => {
            handle_internal_repair_backfill(storage, &request, internal_api, cluster_context).await
        }
        ("POST", "/internal/v1/control/append") => {
            handle_internal_control_append(&request, internal_api, cluster_context).await
        }
        ("POST", "/internal/v1/control/install_snapshot") => {
            handle_internal_control_install_snapshot(&request, internal_api, cluster_context).await
        }
        ("POST", "/internal/v1/control/auto_join") => {
            handle_internal_control_auto_join(&request, internal_api, cluster_context).await
        }
        ("GET", "/healthz") => {
            HttpResponse::new(200, "ok\n").with_header("Content-Type", "text/plain")
        }
        ("GET", "/ready") => {
            HttpResponse::new(200, "ready\n").with_header("Content-Type", "text/plain")
        }
        ("GET", "/metrics") => handle_metrics(
            storage,
            exemplar_store,
            rules_runtime,
            server_start,
            cluster_context,
            edge_sync_context,
            rbac_registry,
            security_manager,
            usage_accounting,
        ),
        ("GET" | "POST", "/api/v1/query") => {
            handle_instant_query(
                storage,
                engine,
                &request,
                timestamp_precision,
                public_read_context,
            )
            .await
        }
        ("GET" | "POST", "/api/v1/query_range") => {
            handle_range_query(
                storage,
                engine,
                &request,
                timestamp_precision,
                public_read_context,
            )
            .await
        }
        ("GET", "/api/v1/series") => {
            handle_series(
                storage,
                &request,
                cluster_context,
                tenant_registry,
                managed_control_plane,
                usage_accounting,
            )
            .await
        }
        ("GET", "/api/v1/labels") => {
            handle_labels(
                storage,
                &request,
                cluster_context,
                tenant_registry,
                managed_control_plane,
                usage_accounting,
            )
            .await
        }
        ("GET", "/api/v1/metadata") => {
            handle_metadata(
                metadata_store,
                &request,
                tenant_registry,
                managed_control_plane,
                usage_accounting,
            )
            .await
        }
        ("GET" | "POST", "/api/v1/query_exemplars") => {
            handle_query_exemplars(
                exemplar_store,
                &request,
                timestamp_precision,
                public_read_context,
            )
            .await
        }
        ("POST", "/api/v1/write") => {
            handle_remote_write(
                storage,
                metadata_store,
                exemplar_store,
                &request,
                cluster_context,
                edge_sync_context,
                tenant_registry,
                managed_control_plane,
                usage_accounting,
            )
            .await
        }
        ("POST", "/v1/metrics") => {
            handle_otlp_metrics(
                storage,
                metadata_store,
                exemplar_store,
                &request,
                timestamp_precision,
                cluster_context,
                edge_sync_context,
                tenant_registry,
                managed_control_plane,
                usage_accounting,
            )
            .await
        }
        ("POST", "/write") | ("POST", "/api/v2/write") => {
            handle_influx_line_protocol(
                storage,
                metadata_store,
                exemplar_store,
                &request,
                timestamp_precision,
                cluster_context,
                edge_sync_context,
                tenant_registry,
                managed_control_plane,
                usage_accounting,
            )
            .await
        }
        ("POST", "/api/v1/read") => {
            handle_remote_read(
                storage,
                &request,
                cluster_context,
                tenant_registry,
                managed_control_plane,
                usage_accounting,
            )
            .await
        }
        ("POST", "/api/v1/import/prometheus") => {
            handle_prometheus_import(
                storage,
                exemplar_store,
                &request,
                timestamp_precision,
                cluster_context,
                edge_sync_context,
                tenant_registry,
                managed_control_plane,
                usage_accounting,
            )
            .await
        }
        ("GET", "/api/v1/status/tsdb") => {
            handle_tsdb_status(
                storage,
                exemplar_store,
                &request,
                cluster_context,
                edge_sync_context,
                tenant_registry,
                rbac_registry,
                security_manager,
                usage_accounting,
                managed_control_plane,
            )
            .await
        }
        ("GET", "/api/v1/admin/control-plane/state") if admin_api_enabled => {
            handle_admin_control_plane_state(managed_control_plane)
        }
        ("GET", "/api/v1/admin/control-plane/audit") if admin_api_enabled => {
            handle_admin_control_plane_audit(&request, managed_control_plane)
        }
        ("GET", "/api/v1/admin/usage/report") if admin_api_enabled => {
            handle_admin_usage_report(storage, &request, usage_accounting)
        }
        ("GET", "/api/v1/admin/usage/export") if admin_api_enabled => {
            handle_admin_usage_export(&request, usage_accounting)
        }
        ("GET", "/api/v1/admin/support_bundle") if admin_api_enabled => {
            handle_admin_support_bundle(
                storage,
                exemplar_store,
                rules_runtime,
                &request,
                cluster_context,
                edge_sync_context,
                tenant_registry,
                rbac_registry,
                security_manager,
                usage_accounting,
            )
            .await
        }
        ("POST", "/api/v1/admin/usage/reconcile") if admin_api_enabled => {
            let response = handle_admin_usage_reconcile(storage, usage_accounting).await;
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "reconcile_usage",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/control-plane/deployments/provision") if admin_api_enabled => {
            let response =
                handle_admin_control_plane_deployment_provision(&request, managed_control_plane);
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "control_plane_provision_deployment",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/control-plane/deployments/backup-policy") if admin_api_enabled => {
            let response =
                handle_admin_control_plane_backup_policy(&request, managed_control_plane);
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "control_plane_apply_backup_policy",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/control-plane/deployments/backup-run") if admin_api_enabled => {
            let response = handle_admin_control_plane_backup_run(&request, managed_control_plane);
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "control_plane_record_backup_run",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/control-plane/deployments/maintenance") if admin_api_enabled => {
            let response = handle_admin_control_plane_maintenance(&request, managed_control_plane);
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "control_plane_apply_maintenance",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/control-plane/deployments/upgrade") if admin_api_enabled => {
            let response = handle_admin_control_plane_upgrade(&request, managed_control_plane);
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "control_plane_apply_upgrade",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/control-plane/tenants/apply") if admin_api_enabled => {
            let response = handle_admin_control_plane_tenant_apply(&request, managed_control_plane);
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "control_plane_apply_tenant",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/control-plane/tenants/lifecycle") if admin_api_enabled => {
            let response =
                handle_admin_control_plane_tenant_lifecycle(&request, managed_control_plane);
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "control_plane_apply_tenant_lifecycle",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/snapshot") if admin_api_enabled => {
            let response = handle_admin_snapshot(
                storage,
                metadata_store,
                exemplar_store,
                rules_runtime,
                &request,
                admin_path_prefix,
            )
            .await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "snapshot", &response);
            response
        }
        ("POST", "/api/v1/admin/restore") if admin_api_enabled => {
            let response = handle_admin_restore(&request, admin_path_prefix).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "restore", &response);
            response
        }
        ("GET", "/api/v1/admin/rbac/state") if admin_api_enabled => {
            handle_admin_rbac_state(rbac_registry)
        }
        ("GET", "/api/v1/admin/rbac/audit") if admin_api_enabled => {
            handle_admin_rbac_audit(&request, rbac_registry)
        }
        ("GET", "/api/v1/admin/secrets/state") if admin_api_enabled => {
            handle_admin_secrets_state(rbac_registry, security_manager)
        }
        ("POST", "/api/v1/admin/secrets/rotate") if admin_api_enabled => {
            let response = handle_admin_secrets_rotate(&request, security_manager);
            emit_mutating_admin_audit_entry(cluster_context, &request, "rotate_secret", &response);
            response
        }
        ("POST", "/api/v1/admin/rbac/reload") if admin_api_enabled => {
            let response = handle_admin_rbac_reload(rbac_registry);
            emit_mutating_admin_audit_entry(cluster_context, &request, "reload_rbac", &response);
            response
        }
        ("POST", "/api/v1/admin/rbac/service_accounts/create") if admin_api_enabled => {
            let response = handle_admin_rbac_service_account_create(&request, rbac_registry);
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "create_service_account",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/rbac/service_accounts/update") if admin_api_enabled => {
            let response = handle_admin_rbac_service_account_update(&request, rbac_registry);
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "update_service_account",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/rbac/service_accounts/rotate") if admin_api_enabled => {
            let response = handle_admin_rbac_service_account_rotate(&request, rbac_registry);
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "rotate_service_account",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/rbac/service_accounts/disable") if admin_api_enabled => {
            let response = handle_admin_rbac_service_account_disable(&request, rbac_registry);
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "disable_service_account",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/rbac/service_accounts/enable") if admin_api_enabled => {
            let response = handle_admin_rbac_service_account_enable(&request, rbac_registry);
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "enable_service_account",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/rules/apply") if admin_api_enabled => {
            let response = handle_admin_rules_apply(&request, rules_runtime).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "apply_rules", &response);
            response
        }
        ("POST", "/api/v1/admin/rules/run") if admin_api_enabled => {
            let response = handle_admin_rules_run(rules_runtime).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "run_rules", &response);
            response
        }
        ("GET", "/api/v1/admin/rules/status") if admin_api_enabled => {
            handle_admin_rules_status(rules_runtime).await
        }
        ("POST", "/api/v1/admin/rollups/apply") if admin_api_enabled => {
            let response = handle_admin_rollups_apply(storage, &request).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "apply_rollups", &response);
            response
        }
        ("POST", "/api/v1/admin/rollups/run") if admin_api_enabled => {
            let response = handle_admin_rollups_run(storage).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "run_rollups", &response);
            response
        }
        ("GET", "/api/v1/admin/rollups/status") if admin_api_enabled => {
            handle_admin_rollups_status(storage).await
        }
        ("POST", "/api/v1/admin/cluster/join") if admin_api_enabled => {
            let response = handle_admin_cluster_join(&request, cluster_context).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "join", &response);
            response
        }
        ("POST", "/api/v1/admin/cluster/leave") if admin_api_enabled => {
            let response = handle_admin_cluster_leave(&request, cluster_context).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "leave", &response);
            response
        }
        ("POST", "/api/v1/admin/cluster/recommission") if admin_api_enabled => {
            let response = handle_admin_cluster_recommission(&request, cluster_context).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "recommission", &response);
            response
        }
        ("POST", "/api/v1/admin/cluster/handoff/begin") if admin_api_enabled => {
            let response = handle_admin_cluster_handoff_begin(&request, cluster_context).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "begin_handoff", &response);
            response
        }
        ("POST", "/api/v1/admin/cluster/handoff/progress") if admin_api_enabled => {
            let response = handle_admin_cluster_handoff_progress(&request, cluster_context).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "update_handoff", &response);
            response
        }
        ("POST", "/api/v1/admin/cluster/handoff/complete") if admin_api_enabled => {
            let response = handle_admin_cluster_handoff_complete(&request, cluster_context).await;
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "complete_handoff",
                &response,
            );
            response
        }
        ("GET", "/api/v1/admin/cluster/audit") if admin_api_enabled => {
            handle_admin_cluster_audit_query(&request, cluster_context)
        }
        ("GET", "/api/v1/admin/cluster/audit/export") if admin_api_enabled => {
            handle_admin_cluster_audit_export(&request, cluster_context)
        }
        ("GET", "/api/v1/admin/cluster/handoff/status") if admin_api_enabled => {
            handle_admin_cluster_handoff_status(cluster_context).await
        }
        ("POST", "/api/v1/admin/cluster/repair/pause") if admin_api_enabled => {
            let response = handle_admin_cluster_repair_pause(cluster_context).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "pause_repair", &response);
            response
        }
        ("POST", "/api/v1/admin/cluster/repair/resume") if admin_api_enabled => {
            let response = handle_admin_cluster_repair_resume(cluster_context).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "resume_repair", &response);
            response
        }
        ("POST", "/api/v1/admin/cluster/repair/cancel") if admin_api_enabled => {
            let response = handle_admin_cluster_repair_cancel(cluster_context).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "cancel_repair", &response);
            response
        }
        ("GET", "/api/v1/admin/cluster/repair/status") if admin_api_enabled => {
            handle_admin_cluster_repair_status(cluster_context).await
        }
        ("POST", "/api/v1/admin/cluster/repair/run") if admin_api_enabled => {
            let response = handle_admin_cluster_repair_run(storage, cluster_context).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "run_repair", &response);
            response
        }
        ("POST", "/api/v1/admin/cluster/rebalance/pause") if admin_api_enabled => {
            let response = handle_admin_cluster_rebalance_pause(storage, cluster_context).await;
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "pause_rebalance",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/cluster/rebalance/resume") if admin_api_enabled => {
            let response = handle_admin_cluster_rebalance_resume(storage, cluster_context).await;
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "resume_rebalance",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/cluster/rebalance/run") if admin_api_enabled => {
            let response = handle_admin_cluster_rebalance_run(storage, cluster_context).await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "run_rebalance", &response);
            response
        }
        ("GET", "/api/v1/admin/cluster/rebalance/status") if admin_api_enabled => {
            handle_admin_cluster_rebalance_status(storage, cluster_context).await
        }
        ("POST", "/api/v1/admin/cluster/control/snapshot") if admin_api_enabled => {
            let response =
                handle_admin_cluster_control_snapshot(&request, admin_path_prefix, cluster_context)
                    .await;
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "control_snapshot",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/cluster/control/restore") if admin_api_enabled => {
            let response =
                handle_admin_cluster_control_restore(&request, admin_path_prefix, cluster_context)
                    .await;
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "control_restore",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/cluster/snapshot") if admin_api_enabled => {
            let response = handle_admin_cluster_snapshot(
                storage,
                metadata_store,
                exemplar_store,
                rules_runtime,
                &request,
                admin_path_prefix,
                cluster_context,
            )
            .await;
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "cluster_snapshot",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/cluster/restore") if admin_api_enabled => {
            let response =
                handle_admin_cluster_restore(&request, admin_path_prefix, cluster_context).await;
            emit_mutating_admin_audit_entry(
                cluster_context,
                &request,
                "cluster_restore",
                &response,
            );
            response
        }
        ("POST", "/api/v1/admin/delete_series") if admin_api_enabled => {
            let response = handle_admin_delete_series(
                storage,
                &request,
                timestamp_precision,
                tenant_registry,
                usage_accounting,
            )
            .await;
            emit_mutating_admin_audit_entry(cluster_context, &request, "delete_series", &response);
            response
        }
        _ => text_response(404, "not found"),
    }
}

fn label_values_path(path: &str) -> Option<&str> {
    path.strip_prefix("/api/v1/label/")?.strip_suffix("/values")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build")
    }

    fn make_engine(storage: &Arc<dyn Storage>) -> Engine {
        Engine::with_precision(Arc::clone(storage), TimestampPrecision::Milliseconds)
    }

    fn app_context<'a>(
        storage: &'a Arc<dyn Storage>,
        metadata_store: &'a Arc<MetricMetadataStore>,
        exemplar_store: &'a Arc<ExemplarStore>,
        engine: &'a Engine,
    ) -> AppContext<'a> {
        AppContext {
            storage,
            metadata_store,
            exemplar_store,
            rules_runtime: None,
            engine,
            cluster_context: None,
            tenant_registry: None,
            rbac_registry: None,
            edge_sync_context: None,
            security_manager: None,
            usage_accounting: None,
            managed_control_plane: None,
        }
    }

    fn request_context<'a>(request: HttpRequest, admin_api_enabled: bool) -> RequestContext<'a> {
        RequestContext {
            request,
            server_start: Instant::now(),
            timestamp_precision: TimestampPrecision::Milliseconds,
            admin_api_enabled,
            admin_path_prefix: None,
            internal_api: None,
        }
    }

    #[tokio::test]
    async fn route_request_handles_probe_and_removed_admin_route() {
        let storage = make_storage();
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        let engine = make_engine(&storage);

        let ready = route_request(
            app_context(&storage, &metadata_store, &exemplar_store, &engine),
            request_context(
                HttpRequest {
                    method: "GET".to_string(),
                    path: "/ready".to_string(),
                    headers: HashMap::new(),
                    body: Vec::new(),
                },
                false,
            ),
        )
        .await;
        assert_eq!(ready.status, 200);
        assert_eq!(std::str::from_utf8(&ready.body).unwrap(), "ready\n");

        let missing = route_request(
            app_context(&storage, &metadata_store, &exemplar_store, &engine),
            request_context(
                HttpRequest {
                    method: "GET".to_string(),
                    path: "/admin".to_string(),
                    headers: HashMap::new(),
                    body: Vec::new(),
                },
                false,
            ),
        )
        .await;
        assert_eq!(missing.status, 404);
    }

    #[tokio::test]
    async fn route_request_dispatches_label_values_before_static_fallthrough() {
        let storage = make_storage();
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        let engine = make_engine(&storage);

        tenant::scoped_storage(Arc::clone(&storage), tenant::DEFAULT_TENANT_ID)
            .insert_rows(&[Row::with_labels(
                "cpu_usage",
                vec![Label::new("host", "router-a")],
                DataPoint::new(1_700_000_000_000, 1.0),
            )])
            .expect("seed write should succeed");

        let response = route_request(
            app_context(&storage, &metadata_store, &exemplar_store, &engine),
            request_context(
                HttpRequest {
                    method: "GET".to_string(),
                    path: "/api/v1/label/host/values".to_string(),
                    headers: HashMap::new(),
                    body: Vec::new(),
                },
                false,
            ),
        )
        .await;
        assert_eq!(response.status, 200);

        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("response body should be valid JSON");
        assert_eq!(body["status"], "success");
        assert!(body["data"]
            .as_array()
            .is_some_and(|values| values.iter().any(|value| value == "router-a")));
    }

    #[tokio::test]
    async fn route_request_keeps_admin_paths_behind_the_feature_flag() {
        let storage = make_storage();
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        let engine = make_engine(&storage);

        let response = route_request(
            app_context(&storage, &metadata_store, &exemplar_store, &engine),
            request_context(
                HttpRequest {
                    method: "GET".to_string(),
                    path: "/api/v1/admin/rules/status".to_string(),
                    headers: HashMap::new(),
                    body: Vec::new(),
                },
                false,
            ),
        )
        .await;

        assert_eq!(response.status, 404);
    }
}
