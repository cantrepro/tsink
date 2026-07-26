use super::*;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_admin_support_bundle(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    rbac_registry: Option<&RbacRegistry>,
    security_manager: Option<&SecurityManager>,
    usage_accounting: Option<&UsageAccounting>,
    local_disk_budget: Option<&tsink::LocalDiskBudget>,
    offline_restore_disk_budget: Option<&tsink::LocalDiskBudget>,
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
                    metadata_store,
                    exemplar_store,
                    &status_request,
                    cluster_context,
                    edge_sync_context,
                    tenant_registry,
                    rbac_registry,
                    security_manager,
                    usage_accounting,
                    None,
                    local_disk_budget,
                    offline_restore_disk_budget,
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

pub(crate) async fn handle_admin_usage_report(
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
    let (read_options, max_response_bytes) =
        match parse_usage_read_page(request, usage_accounting, UsageReadKind::Report) {
            Ok(options) => options,
            Err(response) => return response,
        };
    let reconciled_storage_snapshots = if reconcile {
        match usage_accounting
            .reconcile_storage_async(Arc::clone(storage))
            .await
        {
            Ok(snapshots) => snapshots,
            Err(err) => {
                return usage_accounting_error_response("usage storage reconciliation", &err)
            }
        }
    } else {
        Vec::new()
    };
    let report = match usage_accounting.report_page(
        tenant_id.as_deref(),
        start_unix_ms,
        end_unix_ms,
        bucket_width,
        read_options,
    ) {
        Ok(report) => report,
        Err(err) => return usage_read_error_response(&err),
    };
    let reconciliation = usage_reconciliation_json(&report, &storage.observability_snapshot());
    let payload = AdminUsageReportResponse {
        status: "success",
        data: AdminUsageReportResponseData {
            report: &report,
            reconciliation: &reconciliation,
            reconciled_storage_snapshots: &reconciled_storage_snapshots,
        },
    };
    let response = bounded_usage_json_response(
        200,
        &payload,
        max_response_bytes,
        "usage_report_response_too_large",
    );
    if reconcile {
        response.with_header("X-Tsink-Usage-Reconciliation", "completed")
    } else {
        response
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminUsageReportResponse<'a> {
    status: &'static str,
    data: AdminUsageReportResponseData<'a>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminUsageReportResponseData<'a> {
    report: &'a crate::usage::UsageReport,
    reconciliation: &'a JsonValue,
    reconciled_storage_snapshots: &'a [crate::usage::UsageStorageSnapshot],
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
    let (read_options, max_response_bytes) =
        match parse_usage_read_page(request, usage_accounting, UsageReadKind::Export) {
            Ok(options) => options,
            Err(response) => return response,
        };
    let page = match usage_accounting.export_page(
        tenant_id.as_deref(),
        start_unix_ms,
        end_unix_ms,
        read_options,
        max_response_bytes,
    ) {
        Ok(page) => page,
        Err(err) => return usage_read_error_response(&err),
    };
    let mut body = Vec::with_capacity(page.response_bytes);
    for record in &page.records {
        if let Err(err) = serde_json::to_writer(&mut body, record) {
            return usage_read_error_response(&crate::usage::UsageReadError::Encoding(format!(
                "failed to encode usage export: {err}"
            )));
        }
        body.push(b'\n');
    }
    debug_assert_eq!(body.len(), page.response_bytes);
    let mut response = HttpResponse::new(200, body)
        .with_header("Content-Type", "application/x-ndjson")
        .with_header("Cache-Control", "no-store")
        .with_header(
            "X-Tsink-Usage-Snapshot-Sequence",
            page.snapshot_sequence.to_string(),
        )
        .with_header(
            "X-Tsink-Usage-Records-Returned",
            page.records_returned.to_string(),
        )
        .with_header(
            "X-Tsink-Usage-Response-Bytes",
            page.response_bytes.to_string(),
        )
        .with_header(
            "X-Tsink-Usage-Raw-History-Complete",
            page.raw_history_complete.to_string(),
        )
        .with_header("X-Tsink-Usage-Has-More", page.has_more.to_string());
    if let Some(sequence) = page.earliest_available_sequence {
        response = response.with_header(
            "X-Tsink-Usage-Earliest-Available-Sequence",
            sequence.to_string(),
        );
    }
    if let Some(sequence) = page.next_after_sequence {
        response = response.with_header("X-Tsink-Usage-Next-After-Sequence", sequence.to_string());
    }
    response
}

#[derive(Debug, Clone, Copy)]
enum UsageReadKind {
    Report,
    Export,
}

fn parse_usage_read_page(
    request: &HttpRequest,
    accounting: &UsageAccounting,
    kind: UsageReadKind,
) -> Result<(crate::usage::UsageReadOptions, usize), HttpResponse> {
    let limits = accounting.limits();
    let (default_records, maximum_records, maximum_response_bytes) = match kind {
        UsageReadKind::Report => (
            limits.report_default_records,
            limits.report_max_records,
            limits.report_max_response_bytes,
        ),
        UsageReadKind::Export => (
            limits.export_default_records,
            limits.export_max_records,
            limits.export_max_response_bytes,
        ),
    };
    let limit = match request.param("limit") {
        Some(value) => parse_usage_usize(&value, "limit")?,
        None => default_records,
    };
    if limit == 0 || limit > maximum_records {
        return Err(usage_read_error_response(
            &crate::usage::UsageReadError::InvalidLimit {
                requested: limit,
                maximum: maximum_records,
            },
        ));
    }
    let max_response_bytes = match request
        .param("maxBytes")
        .or_else(|| request.param("maxResponseBytes"))
    {
        Some(value) => parse_usage_usize(&value, "maxBytes")?,
        None => maximum_response_bytes,
    };
    if max_response_bytes == 0 || max_response_bytes > maximum_response_bytes {
        return Err(usage_read_error_response(
            &crate::usage::UsageReadError::InvalidResponseBytes {
                requested: max_response_bytes,
                maximum: maximum_response_bytes,
            },
        ));
    }
    let after_sequence = parse_optional_usage_u64(
        request
            .param("afterSequence")
            .or_else(|| request.param("after_sequence")),
        "afterSequence",
    )?;
    let snapshot_sequence = parse_optional_usage_u64(
        request
            .param("snapshotSequence")
            .or_else(|| request.param("snapshot_sequence")),
        "snapshotSequence",
    )?;
    Ok((
        crate::usage::UsageReadOptions {
            after_sequence,
            snapshot_sequence,
            limit,
        },
        max_response_bytes,
    ))
}

fn parse_usage_usize(value: &str, name: &str) -> Result<usize, HttpResponse> {
    let value =
        parse_admin_u64(value, name).map_err(|err| usage_parameter_error(name, value, &err))?;
    usize::try_from(value).map_err(|_| {
        usage_parameter_error(
            name,
            &value.to_string(),
            &format!("invalid '{name}': value exceeds this platform's range"),
        )
    })
}

fn parse_optional_usage_u64(
    value: Option<String>,
    name: &str,
) -> Result<Option<u64>, HttpResponse> {
    value
        .map(|value| {
            parse_admin_u64(&value, name).map_err(|err| usage_parameter_error(name, &value, &err))
        })
        .transpose()
}

fn usage_parameter_error(name: &str, value: &str, message: &str) -> HttpResponse {
    json_response(
        400,
        &json!({
            "status": "error",
            "error": {
                "code": "usage_page_parameter_invalid",
                "message": message,
                "details": {
                    "parameter": name,
                    "value": value,
                }
            }
        }),
    )
}

fn usage_read_error_response(err: &crate::usage::UsageReadError) -> HttpResponse {
    let details = match err {
        crate::usage::UsageReadError::InvalidLimit { requested, maximum } => json!({
            "requested": requested,
            "maximum": maximum,
        }),
        crate::usage::UsageReadError::InvalidResponseBytes { requested, maximum } => json!({
            "requestedBytes": requested,
            "maximumBytes": maximum,
        }),
        crate::usage::UsageReadError::InvalidSnapshot { requested, latest } => json!({
            "requestedSnapshotSequence": requested,
            "latestSequence": latest,
        }),
        crate::usage::UsageReadError::CursorExpired {
            requested_after,
            earliest_available,
        } => json!({
            "requestedAfterSequence": requested_after,
            "earliestAvailableSequence": earliest_available,
        }),
        crate::usage::UsageReadError::RecordExceedsResponseLimit {
            sequence,
            required_bytes,
            maximum_bytes,
        } => json!({
            "sequence": sequence,
            "requiredBytes": required_bytes,
            "maximumBytes": maximum_bytes,
        }),
        crate::usage::UsageReadError::Encoding(_) => JsonValue::Null,
    };
    json_response(
        err.http_status(),
        &json!({
            "status": "error",
            "error": {
                "code": err.code(),
                "message": err.to_string(),
                "details": details,
            }
        }),
    )
}

#[derive(Debug)]
struct BoundedUsageResponseWriter {
    body: Vec<u8>,
    maximum: usize,
    exceeded: bool,
}

impl std::io::Write for BoundedUsageResponseWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.body.len()) {
            self.exceeded = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "usage response exceeds configured byte limit",
            ));
        }
        self.body.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn bounded_usage_json_response(
    status: u16,
    value: &impl Serialize,
    maximum: usize,
    error_code: &str,
) -> HttpResponse {
    let mut writer = BoundedUsageResponseWriter {
        body: Vec::with_capacity(maximum.min(8 * 1024)),
        maximum,
        exceeded: false,
    };
    if let Err(err) = serde_json::to_writer(&mut writer, value) {
        if writer.exceeded {
            return json_response(
                413,
                &json!({
                    "status": "error",
                    "error": {
                        "code": error_code,
                        "message": format!("usage response exceeds the configured maximum {maximum} bytes"),
                        "maximumBytes": maximum,
                    }
                }),
            );
        }
        return text_response(500, &format!("failed to encode usage response: {err}"));
    }
    HttpResponse::new(status, writer.body).with_header("Content-Type", "application/json")
}

pub(crate) async fn handle_admin_usage_reconcile(
    storage: &Arc<dyn Storage>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let Some(usage_accounting) = usage_accounting else {
        return text_response(503, "usage accounting is unavailable");
    };
    match usage_accounting
        .reconcile_storage_async(Arc::clone(storage))
        .await
    {
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
        Err(err) => usage_accounting_error_response("usage storage reconciliation", &err),
    }
}

fn usage_accounting_error_response(
    action: &str,
    err: &crate::usage::UsageAccountingError,
) -> HttpResponse {
    if matches!(err, crate::usage::UsageAccountingError::Limit(_)) {
        return json_response(
            413,
            &json!({
                "status": "error",
                "error": {
                    "code": "usage_ledger_limit_exceeded",
                    "message": format!("{action} failed: {err}"),
                }
            }),
        );
    }
    if let Some(
        disk_error @ (tsink::TsinkError::DiskQuotaExceeded { .. }
        | tsink::TsinkError::InsufficientDiskSpace { .. }
        | tsink::TsinkError::InsufficientCompactionHeadroom { .. }),
    ) = err.disk_error()
    {
        return server_persistence_error_response(action, disk_error);
    }
    if err.is_persistence_failure() {
        return indeterminate_backend_write_error_response(
            500,
            "usage_ledger_persistence_failed",
            "usage ledger persistence failed; the durable outcome is indeterminate",
        );
    }
    text_response(500, &format!("{action} failed: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage_request(path: &str) -> HttpRequest {
        HttpRequest {
            method: "GET".to_string(),
            path: path.to_string(),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        }
    }

    fn header<'a>(response: &'a HttpResponse, name: &str) -> Option<&'a str> {
        response
            .headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn non_quota_budget_failure_uses_usage_specific_indeterminate_response() {
        let err = crate::usage::UsageAccountingError::Disk(tsink::TsinkError::Io(
            std::io::Error::other("injected usage ledger failure"),
        ));

        let response = usage_accounting_error_response("usage reconciliation", &err);

        assert_eq!(response.status, 500);
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case(WRITE_ERROR_CODE_HEADER)
                && value == "usage_ledger_persistence_failed"
        }));
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case(WRITE_PARTIAL_HEADER) && value == "possible"
        }));
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case(WRITE_OUTCOME_HEADER) && value == "indeterminate_backend"
        }));
    }

    #[test]
    fn export_handler_returns_stable_continuation_headers() {
        let accounting = UsageAccounting::open(None).expect("usage accounting should open");
        for _ in 0..3 {
            accounting
                .record(UsageRecordInput::success(
                    "team-a",
                    UsageCategory::Query,
                    "query",
                    "test",
                ))
                .expect("usage record should append");
        }

        let first = handle_admin_usage_export(
            &usage_request("/api/v1/admin/usage/export?tenant=team-a&limit=2"),
            Some(&accounting),
        );
        assert_eq!(first.status, 200);
        assert_eq!(header(&first, "X-Tsink-Usage-Records-Returned"), Some("2"));
        assert_eq!(header(&first, "X-Tsink-Usage-Has-More"), Some("true"));
        assert_eq!(
            header(&first, "X-Tsink-Usage-Raw-History-Complete"),
            Some("true")
        );
        assert_eq!(
            header(&first, "X-Tsink-Usage-Next-After-Sequence"),
            Some("2")
        );
        assert_eq!(header(&first, "X-Tsink-Usage-Snapshot-Sequence"), Some("3"));

        let second = handle_admin_usage_export(
            &usage_request(
                "/api/v1/admin/usage/export?tenant=team-a&limit=2&afterSequence=2&snapshotSequence=3",
            ),
            Some(&accounting),
        );
        assert_eq!(second.status, 200);
        assert_eq!(header(&second, "X-Tsink-Usage-Records-Returned"), Some("1"));
        assert_eq!(header(&second, "X-Tsink-Usage-Has-More"), Some("false"));
        let records = String::from_utf8(second.body)
            .expect("export must be utf-8")
            .lines()
            .map(|line| {
                serde_json::from_str::<crate::usage::UsageLedgerRecord>(line)
                    .expect("export line should decode")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            records.iter().map(|record| record.seq).collect::<Vec<_>>(),
            vec![3]
        );
    }

    #[test]
    fn export_handler_rejects_invalid_limit_structurally() {
        let accounting = UsageAccounting::open(None).expect("usage accounting should open");
        let response = handle_admin_usage_export(
            &usage_request("/api/v1/admin/usage/export?limit=0"),
            Some(&accounting),
        );
        assert_eq!(response.status, 400);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("error should be JSON");
        assert_eq!(body["error"]["code"], "usage_page_limit_invalid");
    }

    #[tokio::test]
    async fn report_handler_enforces_encoded_response_bytes() {
        let accounting = UsageAccounting::open(None).expect("usage accounting should open");
        accounting
            .record(UsageRecordInput::success(
                "team-a",
                UsageCategory::Query,
                "query",
                "test",
            ))
            .expect("usage record should append");
        let storage: Arc<dyn Storage> = tsink::StorageBuilder::new()
            .build()
            .expect("storage should build");
        let response = handle_admin_usage_report(
            &storage,
            &usage_request("/api/v1/admin/usage/report?bucket=none&maxBytes=1"),
            Some(&accounting),
        )
        .await;
        assert_eq!(response.status, 413);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("error should be JSON");
        assert_eq!(body["error"]["code"], "usage_report_response_too_large");
    }
}
