use super::*;
use serde::ser::SerializeMap;
use serde_json::value::RawValue;

pub(crate) const SUPPORT_BUNDLE_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES: u64 = 16 * 1024 * 1024;
// Covers the simultaneous worst-case lossy UTF-8 prefix (allocator-rounded to under 129 KiB), the
// truncated 8,192-scalar output (allocator-rounded to 32 KiB), and serializer bookkeeping.
const SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES: u64 = 256 * 1024;
const SUPPORT_BUNDLE_TEXT_MAX_CHARS: usize = 8 * 1024;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleRequestedBy<'a> {
    id: &'a str,
    auth_scope: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleSections<'a> {
    status_tsdb: SupportBundleResponseSection<'a>,
    usage: SupportBundleResponseSection<'a>,
    rbac_state: SupportBundleResponseSection<'a>,
    rbac_audit: SupportBundleResponseSection<'a>,
    security_state: SupportBundleResponseSection<'a>,
    cluster_audit: SupportBundleResponseSection<'a>,
    cluster_handoff: SupportBundleResponseSection<'a>,
    cluster_repair: SupportBundleResponseSection<'a>,
    cluster_rebalance: SupportBundleResponseSection<'a>,
    rules: SupportBundleResponseSection<'a>,
    rollups: SupportBundleResponseSection<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleEnvelope<'a> {
    generated_unix_ms: u64,
    tenant_id: &'a str,
    requested_by: SupportBundleRequestedBy<'a>,
    server_version: &'static str,
    sections: SupportBundleSections<'a>,
}

#[derive(Clone, Copy)]
struct SupportBundleResponseSection<'a> {
    response: &'a HttpResponse,
}

impl Serialize for SupportBundleResponseSection<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let content_type = self
            .response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
            .map(|(_, value)| value.as_str());
        let raw_json = serde_json::from_slice::<&RawValue>(&self.response.body).ok();
        let mut section = serializer.serialize_map(Some(
            2usize.saturating_add(usize::from(content_type.is_some())),
        ))?;
        section.serialize_entry("httpStatus", &self.response.status)?;
        if let Some(content_type) = content_type {
            section.serialize_entry("contentType", content_type)?;
        }
        if let Some(raw_json) = raw_json {
            section.serialize_entry("body", raw_json)?;
        } else {
            let body_text = bounded_support_bundle_text(&self.response.body);
            section.serialize_entry("bodyText", &body_text)?;
        }
        section.end()
    }
}

fn bounded_support_bundle_text(body: &[u8]) -> String {
    // Four bytes per Unicode scalar plus a small split-codepoint allowance is enough to inspect
    // the first configured number of lossy-decoded characters without ever allocating in
    // proportion to an oversized non-JSON child body.
    let inspected_bytes = SUPPORT_BUNDLE_TEXT_MAX_CHARS
        .saturating_mul(4)
        .saturating_add(4)
        .min(body.len());
    let body_prefix = String::from_utf8_lossy(&body[..inspected_bytes]);
    let mut chars = body_prefix.chars();
    let mut text = String::with_capacity(inspected_bytes.min(SUPPORT_BUNDLE_TEXT_MAX_CHARS));
    for _ in 0..SUPPORT_BUNDLE_TEXT_MAX_CHARS {
        let Some(ch) = chars.next() else {
            break;
        };
        text.push(ch);
    }
    if chars.next().is_some() || inspected_bytes < body.len() {
        text.push_str("...[truncated]");
    }
    text
}

struct SupportBundleLengthCounter<'a> {
    bytes: usize,
    execution: &'a tsink::QueryExecution,
    control_error: Option<tsink::QueryBudgetError>,
    fixed_limit_exceeded: bool,
}

impl IoWrite for SupportBundleLengthCounter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.execution.checkpoint() {
            self.control_error = Some(error);
            return Err(io::Error::other(
                "support-bundle JSON measurement was canceled",
            ));
        }
        let next_bytes = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("support-bundle JSON length overflowed usize"))?;
        if next_bytes > SUPPORT_BUNDLE_MAX_RESPONSE_BYTES {
            self.fixed_limit_exceeded = true;
            return Err(io::Error::other(
                "support-bundle JSON exceeded its fixed encoded-byte limit",
            ));
        }
        self.bytes = next_bytes;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct SupportBundleControlledWriter<'a, W> {
    inner: W,
    execution: &'a tsink::QueryExecution,
    control_error: Option<tsink::QueryBudgetError>,
}

impl<W: IoWrite> IoWrite for SupportBundleControlledWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.execution.checkpoint() {
            self.control_error = Some(error);
            return Err(io::Error::other(
                "support-bundle JSON serialization was canceled",
            ));
        }
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct PreparedSupportBundleResponse {
    response: HttpResponse,
    retained_bytes: u64,
}

fn support_bundle_error_response(
    status: u16,
    error_type: &str,
    error_code: &str,
    message: &str,
    retry_after: Option<&str>,
) -> HttpResponse {
    let mut response = json_response(
        status,
        &json!({
            "status": "error",
            "errorType": error_type,
            "error": message,
        }),
    )
    .with_header(READ_ERROR_CODE_HEADER, error_code);
    if let Some(retry_after) = retry_after {
        response = response.with_header("Retry-After", retry_after);
    }
    response
}

fn support_bundle_query_budget_error_response(error: &tsink::QueryBudgetError) -> HttpResponse {
    match error {
        tsink::QueryBudgetError::InvalidLimits(_) => support_bundle_error_response(
            400,
            "invalid_query_limits",
            "invalid_query_limits",
            "invalid support-bundle query limits",
            None,
        ),
        tsink::QueryBudgetError::LimitExceeded(exceeded) => {
            let error_code = format!("query_limit_{}", exceeded.reason.as_str());
            let retryable = matches!(
                exceeded.reason,
                tsink::QueryLimitReason::ConcurrentQueries
                    | tsink::QueryLimitReason::SharedMemoryBytes
            );
            support_bundle_error_response(
                if retryable { 429 } else { 413 },
                &error_code,
                &error_code,
                &format!(
                    "support-bundle composition exceeded the {} limit",
                    exceeded.reason.as_str()
                ),
                retryable.then_some("1"),
            )
        }
        tsink::QueryBudgetError::Cancelled => support_bundle_error_response(
            503,
            "canceled",
            "query_cancelled",
            "support-bundle composition was canceled",
            None,
        ),
        tsink::QueryBudgetError::DeadlineExceeded => support_bundle_error_response(
            503,
            "timeout",
            "query_deadline_exceeded",
            "support-bundle composition exceeded its deadline",
            None,
        ),
        _ => support_bundle_error_response(
            500,
            "execution",
            "support_bundle_query_budget_failed",
            "support-bundle query accounting failed",
            None,
        ),
    }
}

fn begin_support_bundle_execution(
    storage: &Arc<dyn Storage>,
    cancellation: tsink::QueryCancellationToken,
) -> Result<tsink::QueryExecution, HttpResponse> {
    match storage.begin_query_execution(tsink::QueryWorkLimits::default(), cancellation) {
        Ok(Some(execution)) => Ok(execution),
        Ok(None) => Err(support_bundle_error_response(
            500,
            "execution",
            "support_bundle_query_accounting_unavailable",
            "support-bundle composition requires query execution admission",
            None,
        )),
        Err(tsink::TsinkError::QueryBudget(error)) => {
            Err(support_bundle_query_budget_error_response(&error))
        }
        Err(_) => Err(support_bundle_error_response(
            500,
            "execution",
            "support_bundle_query_admission_failed",
            "support-bundle query admission failed",
            None,
        )),
    }
}

fn modeled_support_bundle_header_preflight_bytes(tenant_id: &str) -> u64 {
    let content_disposition_bytes = "attachment; filename=\"tsink-support-bundle--.json\""
        .len()
        .saturating_add(tenant_id.len().max("default".len()))
        .saturating_add(20);
    modeled_tsdb_status_vec_capacity_bytes::<(String, String)>(4).saturating_add(
        tsdb_status_saturating_u64_from_usize(
            "Content-Type".len()
                + "application/json".len()
                + "Cache-Control".len()
                + "no-store".len()
                + "Content-Disposition".len()
                + content_disposition_bytes,
        )
        .saturating_add(TSDB_STATUS_COLLECTION_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(6)),
    )
}

fn modeled_support_bundle_root_retained_bytes(
    tenant_id: &String,
    actor: &ClusterAuditActor,
) -> u64 {
    modeled_tsdb_status_string_capacity_bytes(tenant_id)
        .saturating_add(modeled_tsdb_status_string_capacity_bytes(&actor.id))
        .saturating_add(modeled_tsdb_status_string_capacity_bytes(&actor.auth_scope))
}

fn account_support_bundle_child(
    response: HttpResponse,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response_retained_bytes = modeled_tsdb_status_response_retained_bytes(&response);
    let next = retained_bytes
        .checked_add(response_retained_bytes)
        .ok_or_else(|| {
            support_bundle_error_response(
                413,
                "support_bundle_child_responses_too_large",
                "support_bundle_child_responses_too_large",
                "support-bundle child responses exceed the fixed retained-byte envelope",
                None,
            )
        })?;
    if next > SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES {
        drop(response);
        return Err(support_bundle_error_response(
            413,
            "support_bundle_child_responses_too_large",
            "support_bundle_child_responses_too_large",
            "support-bundle child responses exceed the fixed retained-byte envelope",
            None,
        ));
    }
    let accounted = account_completed_http_response(response, execution)
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    *retained_bytes = next;
    debug_assert_eq!(accounted.reservation.bytes(), response_retained_bytes);
    Ok(accounted)
}

fn admit_accounted_support_bundle_child(
    accounted: AccountedHttpResponse,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response_retained_bytes = modeled_tsdb_status_response_retained_bytes(&accounted.response);
    if accounted.reservation.bytes() != response_retained_bytes {
        drop(accounted);
        return Err(support_bundle_error_response(
            500,
            "execution",
            "support_bundle_child_response_accounting_invalid",
            "support-bundle child response returned a non-exact reservation",
            None,
        ));
    }
    let next = retained_bytes
        .checked_add(response_retained_bytes)
        .filter(|next| *next <= SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES)
        .ok_or_else(|| {
            support_bundle_error_response(
                413,
                "support_bundle_child_responses_too_large",
                "support_bundle_child_responses_too_large",
                "support-bundle child responses exceed the fixed retained-byte envelope",
                None,
            )
        })?;
    *retained_bytes = next;
    Ok(accounted)
}

fn prepare_support_bundle_response(
    bundle: &SupportBundleEnvelope<'_>,
    execution: &tsink::QueryExecution,
    reservation: &mut tsink::QueryMemoryReservation,
    base_reserved_bytes: u64,
) -> Result<PreparedSupportBundleResponse, HttpResponse> {
    let mut counter = SupportBundleLengthCounter {
        bytes: 0,
        execution,
        control_error: None,
        fixed_limit_exceeded: false,
    };
    if serde_json::to_writer_pretty(&mut counter, bundle).is_err() {
        return Err(match counter.control_error {
            Some(error) => support_bundle_query_budget_error_response(&error),
            None if counter.fixed_limit_exceeded => support_bundle_error_response(
                413,
                "query_limit_returned_bytes",
                "query_limit_returned_bytes",
                "support-bundle response exceeds the fixed encoded-byte limit",
                None,
            ),
            None => support_bundle_error_response(
                500,
                "execution",
                "support_bundle_json_measurement_failed",
                "support-bundle JSON measurement failed",
                None,
            ),
        });
    }
    let body_len = counter.bytes;
    execution
        .charge_returned_bytes(tsdb_status_saturating_u64_from_usize(body_len))
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    reservation
        .resize(
            base_reserved_bytes
                .saturating_add(modeled_tsdb_status_vec_capacity_bytes::<u8>(body_len)),
        )
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;

    let mut body = Vec::new();
    body.try_reserve_exact(body_len).map_err(|_| {
        support_bundle_error_response(
            500,
            "execution",
            "support_bundle_json_allocation_failed",
            "support-bundle JSON allocation failed",
            None,
        )
    })?;
    reservation
        .resize(
            base_reserved_bytes.saturating_add(modeled_tsdb_status_vec_capacity_bytes::<u8>(
                body.capacity(),
            )),
        )
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    body.resize(body_len, 0);
    let written = {
        let cursor = io::Cursor::new(body.as_mut_slice());
        let mut writer = SupportBundleControlledWriter {
            inner: cursor,
            execution,
            control_error: None,
        };
        if serde_json::to_writer_pretty(&mut writer, bundle).is_err() {
            return Err(match writer.control_error {
                Some(error) => support_bundle_query_budget_error_response(&error),
                None => support_bundle_error_response(
                    500,
                    "execution",
                    "support_bundle_json_serialization_failed",
                    "support-bundle JSON serialization failed",
                    None,
                ),
            });
        }
        usize::try_from(writer.inner.position()).unwrap_or(usize::MAX)
    };
    if written != body_len {
        return Err(support_bundle_error_response(
            500,
            "execution",
            "support_bundle_json_length_changed",
            "support-bundle JSON length changed after admission",
            None,
        ));
    }

    let filename_tenant = support_bundle_filename_component(bundle.tenant_id);
    let response = HttpResponse::new(200, body)
        .with_header("Content-Type", "application/json")
        .with_header("Cache-Control", "no-store")
        .with_header(
            "Content-Disposition",
            format!(
                "attachment; filename=\"tsink-support-bundle-{filename_tenant}-{}.json\"",
                bundle.generated_unix_ms
            ),
        );
    let retained_bytes = modeled_tsdb_status_response_retained_bytes(&response);
    reservation
        .resize(base_reserved_bytes.saturating_add(retained_bytes))
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    Ok(PreparedSupportBundleResponse {
        response,
        retained_bytes,
    })
}

#[derive(Serialize)]
struct SupportBundleUsagePayload<'a> {
    status: &'static str,
    data: SupportBundleUsageData<'a>,
}

#[derive(Serialize)]
struct SupportBundleUsageData<'a> {
    report: &'a crate::usage::UsageReport,
    journal: &'a crate::usage::UsageLedgerStatus,
    reconciliation: &'a JsonValue,
}

fn support_bundle_usage_response(
    storage: &Arc<dyn Storage>,
    usage_accounting: Option<&UsageAccounting>,
    tenant_id: &str,
) -> HttpResponse {
    let Some(usage_accounting) = usage_accounting else {
        return HttpResponse::new(503, "usage accounting is unavailable");
    };
    let report = usage_accounting.report(Some(tenant_id), None, None, UsageBucketWidth::None);
    let journal = usage_accounting.ledger_status();
    let reconciliation = usage_reconciliation_json(&report, &storage.observability_snapshot());
    let payload = SupportBundleUsagePayload {
        status: "success",
        data: SupportBundleUsageData {
            report: &report,
            journal: &journal,
            reconciliation: &reconciliation,
        },
    };
    let mut response = bounded_usage_json_response(
        200,
        &payload,
        usage_accounting.limits().report_max_response_bytes,
        "usage_report_response_too_large",
    );
    // The historical synthetic support-bundle section did not expose a contentType member for
    // usage. Keep that successful schema while still composing every section from encoded bytes.
    response
        .headers
        .retain(|(name, _)| !name.eq_ignore_ascii_case("content-type"));
    response
}

fn support_bundle_usage_child(
    storage: &Arc<dyn Storage>,
    usage_accounting: Option<&UsageAccounting>,
    tenant_id: &str,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response = support_bundle_usage_response(storage, usage_accounting, tenant_id);
    account_support_bundle_child(response, execution, retained_bytes)
}

fn support_bundle_rbac_state_child(
    rbac_registry: Option<&RbacRegistry>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response = handle_admin_rbac_state(rbac_registry);
    account_support_bundle_child(response, execution, retained_bytes)
}

fn support_bundle_rbac_audit_child(
    request: &HttpRequest,
    rbac_registry: Option<&RbacRegistry>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response = handle_admin_rbac_audit(request, rbac_registry);
    account_support_bundle_child(response, execution, retained_bytes)
}

fn support_bundle_security_state_child(
    rbac_registry: Option<&RbacRegistry>,
    security_manager: Option<&SecurityManager>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response = handle_admin_secrets_state(rbac_registry, security_manager);
    account_support_bundle_child(response, execution, retained_bytes)
}

fn support_bundle_cluster_audit_child(
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response = handle_admin_cluster_audit_query(request, cluster_context);
    account_support_bundle_child(response, execution, retained_bytes)
}

async fn support_bundle_cluster_handoff_child(
    cluster_context: Option<&ClusterRequestContext>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response = handle_admin_cluster_handoff_status(cluster_context).await;
    account_support_bundle_child(response, execution, retained_bytes)
}

async fn support_bundle_cluster_repair_child(
    cluster_context: Option<&ClusterRequestContext>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response = handle_admin_cluster_repair_status(cluster_context).await;
    account_support_bundle_child(response, execution, retained_bytes)
}

async fn support_bundle_rules_child(
    rules_runtime: Option<&RulesRuntime>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response = handle_admin_rules_status(rules_runtime).await;
    account_support_bundle_child(response, execution, retained_bytes)
}

async fn support_bundle_rollups_child(
    storage: &Arc<dyn Storage>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response = handle_admin_rollups_status(storage).await;
    account_support_bundle_child(response, execution, retained_bytes)
}

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

    // Admit the complete support-bundle request exactly once. TSDB status and rebalance reuse this
    // execution, while every completed legacy child response receives its own exact same-lease
    // guard before the orchestrator retains it. Legacy child snapshot/encoding transients remain a
    // separate producer boundary; this closes the completed-response handoff into composition.
    let cancellation = tsink::QueryCancellationToken::new();
    let cancellation_guard = TsdbStatusCancellationGuard {
        token: cancellation.clone(),
    };
    let execution = match begin_support_bundle_execution(storage, cancellation) {
        Ok(execution) => execution,
        Err(response) => return response,
    };
    let header_preflight = modeled_support_bundle_header_preflight_bytes(&tenant_id);
    let root_retained_bytes = modeled_support_bundle_root_retained_bytes(&tenant_id, &actor);
    let base_reserved_bytes = root_retained_bytes
        .saturating_add(SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES)
        .saturating_add(header_preflight);
    let mut reservation = match execution.reserve_memory(base_reserved_bytes) {
        Ok(reservation) => reservation,
        Err(error) => return support_bundle_query_budget_error_response(&error),
    };
    let mut child_retained_bytes = 0u64;

    let status_tsdb = match handle_tsdb_status_with_execution(
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
        &execution,
    )
    .await
    {
        Ok(response) => {
            match admit_accounted_support_bundle_child(response, &mut child_retained_bytes) {
                Ok(response) => response,
                Err(response) => return response,
            }
        }
        Err(response) => return response,
    };

    let usage = match support_bundle_usage_child(
        storage,
        usage_accounting,
        &tenant_id,
        &execution,
        &mut child_retained_bytes,
    ) {
        Ok(response) => response,
        Err(response) => return response,
    };
    let rbac_state =
        match support_bundle_rbac_state_child(rbac_registry, &execution, &mut child_retained_bytes)
        {
            Ok(response) => response,
            Err(response) => return response,
        };
    let rbac_audit = match support_bundle_rbac_audit_child(
        &rbac_audit_request,
        rbac_registry,
        &execution,
        &mut child_retained_bytes,
    ) {
        Ok(response) => response,
        Err(response) => return response,
    };
    let security_state = match support_bundle_security_state_child(
        rbac_registry,
        security_manager,
        &execution,
        &mut child_retained_bytes,
    ) {
        Ok(response) => response,
        Err(response) => return response,
    };
    let cluster_audit = match support_bundle_cluster_audit_child(
        &cluster_audit_request,
        cluster_context,
        &execution,
        &mut child_retained_bytes,
    ) {
        Ok(response) => response,
        Err(response) => return response,
    };
    let cluster_handoff = match support_bundle_cluster_handoff_child(
        cluster_context,
        &execution,
        &mut child_retained_bytes,
    )
    .await
    {
        Ok(response) => response,
        Err(response) => return response,
    };
    let cluster_repair = match support_bundle_cluster_repair_child(
        cluster_context,
        &execution,
        &mut child_retained_bytes,
    )
    .await
    {
        Ok(response) => response,
        Err(response) => return response,
    };
    let cluster_rebalance = match handle_admin_cluster_rebalance_status_with_execution(
        storage,
        cluster_context,
        &execution,
    )
    .await
    {
        Ok(response) => {
            match admit_accounted_support_bundle_child(response, &mut child_retained_bytes) {
                Ok(response) => response,
                Err(response) => return response,
            }
        }
        Err(response) => return response,
    };
    let rules = match support_bundle_rules_child(
        rules_runtime,
        &execution,
        &mut child_retained_bytes,
    )
    .await
    {
        Ok(response) => response,
        Err(response) => return response,
    };
    let rollups =
        match support_bundle_rollups_child(storage, &execution, &mut child_retained_bytes).await {
            Ok(response) => response,
            Err(response) => return response,
        };

    drop(cluster_audit_request);
    drop(rbac_audit_request);
    drop(status_request);

    let prepared_result = {
        let bundle = SupportBundleEnvelope {
            generated_unix_ms: unix_timestamp_millis(),
            tenant_id: &tenant_id,
            requested_by: SupportBundleRequestedBy {
                id: &actor.id,
                auth_scope: &actor.auth_scope,
            },
            server_version: env!("CARGO_PKG_VERSION"),
            sections: SupportBundleSections {
                status_tsdb: SupportBundleResponseSection {
                    response: &status_tsdb.response,
                },
                usage: SupportBundleResponseSection {
                    response: &usage.response,
                },
                rbac_state: SupportBundleResponseSection {
                    response: &rbac_state.response,
                },
                rbac_audit: SupportBundleResponseSection {
                    response: &rbac_audit.response,
                },
                security_state: SupportBundleResponseSection {
                    response: &security_state.response,
                },
                cluster_audit: SupportBundleResponseSection {
                    response: &cluster_audit.response,
                },
                cluster_handoff: SupportBundleResponseSection {
                    response: &cluster_handoff.response,
                },
                cluster_repair: SupportBundleResponseSection {
                    response: &cluster_repair.response,
                },
                cluster_rebalance: SupportBundleResponseSection {
                    response: &cluster_rebalance.response,
                },
                rules: SupportBundleResponseSection {
                    response: &rules.response,
                },
                rollups: SupportBundleResponseSection {
                    response: &rollups.response,
                },
            },
        };
        prepare_support_bundle_response(&bundle, &execution, &mut reservation, base_reserved_bytes)
    };
    let prepared = match prepared_result {
        Ok(prepared) => prepared,
        Err(response) => {
            // Each wrapper destroys its response before releasing the corresponding child guard.
            drop(rollups);
            drop(rules);
            drop(cluster_repair);
            drop(cluster_handoff);
            drop(cluster_audit);
            drop(security_state);
            drop(rbac_audit);
            drop(rbac_state);
            drop(usage);
            drop(cluster_rebalance);
            drop(status_tsdb);
            drop(actor);
            drop(tenant_id);
            drop(reservation);
            drop(execution);
            drop(cancellation_guard);
            return response;
        }
    };
    let PreparedSupportBundleResponse {
        response,
        retained_bytes,
    } = prepared;
    drop(rollups);
    drop(rules);
    drop(cluster_repair);
    drop(cluster_handoff);
    drop(cluster_audit);
    drop(security_state);
    drop(rbac_audit);
    drop(rbac_state);
    drop(usage);
    drop(cluster_rebalance);
    drop(status_tsdb);
    drop(actor);
    drop(tenant_id);
    reservation
        .resize(retained_bytes)
        .expect("shrinking a support-bundle response reservation cannot fail");
    drop(reservation);
    drop(execution);
    drop(cancellation_guard);
    response
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

    fn support_bundle_test_response(body: impl Into<Vec<u8>>) -> HttpResponse {
        HttpResponse::new(200, body).with_header("Content-Type", "application/json")
    }

    fn support_bundle_test_envelope<'a>(
        json_response: &'a HttpResponse,
        text_response: &'a HttpResponse,
    ) -> SupportBundleEnvelope<'a> {
        SupportBundleEnvelope {
            generated_unix_ms: 123,
            tenant_id: "team-a",
            requested_by: SupportBundleRequestedBy {
                id: "test-actor",
                auth_scope: "admin",
            },
            server_version: "test",
            sections: SupportBundleSections {
                status_tsdb: SupportBundleResponseSection {
                    response: json_response,
                },
                usage: SupportBundleResponseSection {
                    response: json_response,
                },
                rbac_state: SupportBundleResponseSection {
                    response: json_response,
                },
                rbac_audit: SupportBundleResponseSection {
                    response: json_response,
                },
                security_state: SupportBundleResponseSection {
                    response: json_response,
                },
                cluster_audit: SupportBundleResponseSection {
                    response: json_response,
                },
                cluster_handoff: SupportBundleResponseSection {
                    response: json_response,
                },
                cluster_repair: SupportBundleResponseSection {
                    response: json_response,
                },
                cluster_rebalance: SupportBundleResponseSection {
                    response: json_response,
                },
                rules: SupportBundleResponseSection {
                    response: json_response,
                },
                rollups: SupportBundleResponseSection {
                    response: text_response,
                },
            },
        }
    }

    fn support_bundle_test_budget(
        memory_bytes: Option<u64>,
        returned_bytes: Option<u64>,
    ) -> tsink::QueryBudget {
        tsink::QueryBudget::new(tsink::QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: memory_bytes,
            per_query: tsink::QueryWorkLimits {
                max_memory_bytes: memory_bytes,
                max_returned_bytes: returned_bytes,
                ..tsink::QueryWorkLimits::default()
            },
        })
        .expect("support-bundle test budget should build")
    }

    fn prepare_support_bundle_for_test(
        budget: &tsink::QueryBudget,
        json_response: &HttpResponse,
        text_response: &HttpResponse,
    ) -> Result<(HttpResponse, u64), HttpResponse> {
        let execution = budget
            .begin_query()
            .expect("support-bundle test query should admit");
        let root_tenant = "team-a".to_string();
        let root_actor = ClusterAuditActor {
            id: "test-actor".to_string(),
            auth_scope: "admin".to_string(),
        };
        let child_retained_bytes = modeled_tsdb_status_response_retained_bytes(json_response)
            .saturating_add(modeled_tsdb_status_response_retained_bytes(text_response));
        let base_reserved_bytes = child_retained_bytes
            .saturating_add(modeled_support_bundle_root_retained_bytes(
                &root_tenant,
                &root_actor,
            ))
            .saturating_add(SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES)
            .saturating_add(modeled_support_bundle_header_preflight_bytes("team-a"));
        let mut reservation = execution
            .reserve_memory(base_reserved_bytes)
            .map_err(|error| support_bundle_query_budget_error_response(&error))?;
        let PreparedSupportBundleResponse {
            response,
            retained_bytes,
        } = {
            let bundle = support_bundle_test_envelope(json_response, text_response);
            prepare_support_bundle_response(
                &bundle,
                &execution,
                &mut reservation,
                base_reserved_bytes,
            )?
        };
        reservation
            .resize(retained_bytes)
            .expect("support-bundle test reservation shrink should succeed");
        drop(root_actor);
        drop(root_tenant);
        drop(reservation);
        drop(execution);
        Ok((response, retained_bytes))
    }

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
    fn support_bundle_sections_embed_raw_json_and_bound_non_json_text() {
        let json_response =
            support_bundle_test_response(br#"{"status":"success","data":{"value":7}}"#.to_vec());
        let text_response = HttpResponse::new(
            503,
            "é".repeat(SUPPORT_BUNDLE_TEXT_MAX_CHARS.saturating_add(1)),
        );

        let json_section = serde_json::to_value(SupportBundleResponseSection {
            response: &json_response,
        })
        .expect("JSON section should encode");
        assert_eq!(json_section["httpStatus"], 200);
        assert_eq!(json_section["contentType"], "application/json");
        assert_eq!(json_section["body"]["data"]["value"], 7);
        assert!(json_section.get("bodyText").is_none());

        let text_section = serde_json::to_value(SupportBundleResponseSection {
            response: &text_response,
        })
        .expect("text section should encode");
        let body_text = text_section["bodyText"]
            .as_str()
            .expect("text section should contain bodyText");
        assert_eq!(
            body_text
                .chars()
                .take(SUPPORT_BUNDLE_TEXT_MAX_CHARS)
                .count(),
            SUPPORT_BUNDLE_TEXT_MAX_CHARS
        );
        assert!(body_text.ends_with("...[truncated]"));
        assert!(text_section.get("body").is_none());

        let invalid = vec![
            0xff;
            SUPPORT_BUNDLE_TEXT_MAX_CHARS
                .saturating_mul(4)
                .saturating_add(8)
        ];
        let invalid_text = bounded_support_bundle_text(&invalid);
        assert_eq!(
            invalid_text
                .trim_end_matches("...[truncated]")
                .chars()
                .count(),
            SUPPORT_BUNDLE_TEXT_MAX_CHARS
        );
        assert!(invalid_text.ends_with("...[truncated]"));
    }

    #[test]
    fn support_bundle_child_retained_envelope_has_an_exact_boundary() {
        let response = support_bundle_test_response(br#"{"status":"success"}"#.to_vec());
        let response_bytes = modeled_tsdb_status_response_retained_bytes(&response);
        assert!(response_bytes < SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES);
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("child boundary query should admit");

        let mut exact = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES - response_bytes;
        let accounted = account_support_bundle_child(response, &execution, &mut exact)
            .expect("exact child retained-byte boundary should pass");
        assert_eq!(exact, SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES);
        assert_eq!(execution.snapshot().memory_reserved_bytes, response_bytes);

        let mut one_over = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES - response_bytes + 1;
        let error = account_support_bundle_child(
            support_bundle_test_response(br#"{"status":"success"}"#.to_vec()),
            &execution,
            &mut one_over,
        )
        .expect_err("one byte beyond the child envelope should fail");
        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("support_bundle_child_responses_too_large")
        );
        drop(accounted);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn support_bundle_child_reservations_remain_cumulative_until_composition_finishes() {
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("cumulative child query should admit");
        let composition_bytes = 17;
        let composition = execution
            .reserve_memory(composition_bytes)
            .expect("composition reservation should admit");
        let mut retained = 0;

        let first = account_support_bundle_child(
            support_bundle_test_response(br#"{"child":1}"#.to_vec()),
            &execution,
            &mut retained,
        )
        .expect("first child should admit");
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            composition_bytes + retained
        );
        let first_retained = retained;

        let second = account_support_bundle_child(
            HttpResponse::new(503, "component unavailable"),
            &execution,
            &mut retained,
        )
        .expect("second child should admit");
        assert!(retained > first_retained);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            composition_bytes + retained
        );

        drop(second);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            composition_bytes + first_retained
        );
        drop(first);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            composition_bytes
        );
        drop(composition);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_child_cancellation_releases_response_and_query_reservations() {
        let budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(token.clone())
            .expect("cancellation child query should admit");
        token.cancel();
        let mut retained = 0;

        let error = account_support_bundle_child(
            support_bundle_test_response(br#"{"child":"cancelled"}"#.to_vec()),
            &execution,
            &mut retained,
        )
        .expect_err("cancelled child accounting should fail");
        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        assert_eq!(retained, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_response_enforces_exact_returned_and_memory_boundaries() {
        let json_response =
            support_bundle_test_response(br#"{"status":"success","data":{"value":7}}"#.to_vec());
        let text_response = HttpResponse::new(503, "component unavailable");

        let calibration_budget = support_bundle_test_budget(None, None);
        let (calibration_response, _) =
            prepare_support_bundle_for_test(&calibration_budget, &json_response, &text_response)
                .expect("calibration response should encode");
        let response_bytes =
            u64::try_from(calibration_response.body.len()).expect("response length should fit u64");
        let peak_memory = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(response_bytes > 0);
        assert!(
            usize::try_from(response_bytes).unwrap_or(usize::MAX)
                <= SUPPORT_BUNDLE_MAX_RESPONSE_BYTES
        );
        assert!(peak_memory > SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES);

        let exact_returned_budget = support_bundle_test_budget(None, Some(response_bytes));
        let (exact_returned, _) =
            prepare_support_bundle_for_test(&exact_returned_budget, &json_response, &text_response)
                .expect("exact returned-byte boundary should pass");
        assert_eq!(
            u64::try_from(exact_returned.body.len()).unwrap_or(u64::MAX),
            response_bytes
        );
        let below_returned_budget =
            support_bundle_test_budget(None, Some(response_bytes.saturating_sub(1)));
        let below_returned =
            prepare_support_bundle_for_test(&below_returned_budget, &json_response, &text_response)
                .expect_err("one byte below the returned-byte requirement should fail");
        assert_eq!(below_returned.status, 413);
        assert_eq!(
            header(&below_returned, READ_ERROR_CODE_HEADER),
            Some("query_limit_returned_bytes")
        );

        let exact_memory_budget = support_bundle_test_budget(Some(peak_memory), None);
        prepare_support_bundle_for_test(&exact_memory_budget, &json_response, &text_response)
            .expect("exact memory boundary should pass");
        let below_memory_budget =
            support_bundle_test_budget(Some(peak_memory.saturating_sub(1)), None);
        let below_memory =
            prepare_support_bundle_for_test(&below_memory_budget, &json_response, &text_response)
                .expect_err("one byte below the simultaneous memory requirement should fail");
        assert_eq!(below_memory.status, 413);
        assert_eq!(
            header(&below_memory, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );

        for budget in [
            calibration_budget,
            exact_returned_budget,
            below_returned_budget,
            exact_memory_budget,
            below_memory_budget,
        ] {
            let snapshot = budget.snapshot();
            assert_eq!(snapshot.active_queries, 0);
            assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
            assert_eq!(snapshot.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_fixed_encoded_ceiling_rejects_before_body_allocation() {
        let mut oversized_json = Vec::with_capacity(SUPPORT_BUNDLE_MAX_RESPONSE_BYTES + 3);
        oversized_json.push(b'"');
        oversized_json.resize(SUPPORT_BUNDLE_MAX_RESPONSE_BYTES + 2, b'x');
        oversized_json.push(b'"');
        let json_response = support_bundle_test_response(oversized_json);
        let text_response = HttpResponse::new(503, "component unavailable");
        let budget = support_bundle_test_budget(None, None);

        let error = prepare_support_bundle_for_test(&budget, &json_response, &text_response)
            .expect_err("fixed encoded ceiling should reject the oversized envelope");

        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_limit_returned_bytes")
        );
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn support_bundle_serialization_cancellation_releases_all_resources() {
        let json_response =
            support_bundle_test_response(br#"{"status":"success","data":{"value":7}}"#.to_vec());
        let text_response = HttpResponse::new(503, "component unavailable");
        let budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(token.clone())
            .expect("support-bundle cancellation query should admit");
        let root_tenant = "team-a".to_string();
        let root_actor = ClusterAuditActor {
            id: "test-actor".to_string(),
            auth_scope: "admin".to_string(),
        };
        let base_reserved_bytes = modeled_tsdb_status_response_retained_bytes(&json_response)
            .saturating_add(modeled_tsdb_status_response_retained_bytes(&text_response))
            .saturating_add(modeled_support_bundle_root_retained_bytes(
                &root_tenant,
                &root_actor,
            ))
            .saturating_add(SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES)
            .saturating_add(modeled_support_bundle_header_preflight_bytes(&root_tenant));
        let mut reservation = execution
            .reserve_memory(base_reserved_bytes)
            .expect("support-bundle cancellation reservation should admit");
        token.cancel();

        let error = {
            let bundle = support_bundle_test_envelope(&json_response, &text_response);
            prepare_support_bundle_response(
                &bundle,
                &execution,
                &mut reservation,
                base_reserved_bytes,
            )
            .expect_err("canceled support-bundle serialization should fail")
        };

        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        drop(root_actor);
        drop(root_tenant);
        drop(reservation);
        drop(execution);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
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
