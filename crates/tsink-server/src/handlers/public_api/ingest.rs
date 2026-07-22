use super::super::*;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_remote_write(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let write_admission = match admission::global_public_write_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("write admission unavailable: {err}")),
    };
    handle_remote_write_with_admission(
        storage,
        metadata_store,
        exemplar_store,
        request,
        precision,
        cluster_context,
        edge_sync_context,
        tenant_registry,
        managed_control_plane,
        usage_accounting,
        write_admission,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_remote_write_with_admission(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    usage_accounting: Option<&UsageAccounting>,
    write_admission: &WriteAdmissionController,
) -> HttpResponse {
    let started = Instant::now();
    let tenant_id = match tenant_id_for_text_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        tenant_registry,
        managed_control_plane,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Write,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let decoded = match decode_body(request) {
        Ok(body) => body,
        Err(err) => return text_response(400, &err),
    };

    let write_req = match WriteRequest::decode(decoded.as_slice()) {
        Ok(req) => req,
        Err(err) => return text_response(400, &format!("invalid protobuf body: {err}")),
    };
    let envelope = match normalize_remote_write_request(write_req, &tenant_id, precision) {
        Ok(envelope) => envelope,
        Err(err) => {
            return text_response(400, bounded_write_rejection_diagnostic(&err))
                .with_header(WRITE_ERROR_CODE_HEADER, "remote_write_invalid_payload")
        }
    };
    let payload_config = prometheus_payload_config();
    let metadata_count = envelope.metadata_updates.len();
    let exemplar_count = envelope.exemplars.len();
    let histogram_count = envelope.histogram_samples.len();
    let histogram_bucket_entries = histogram_bucket_entries_total(&envelope.histogram_samples);
    if let Err((kind, message)) = validate_payload_feature_flags(
        payload_config,
        metadata_count,
        exemplar_count,
        histogram_count,
    ) {
        let rejected_count =
            payload_item_count(kind, metadata_count, exemplar_count, histogram_count);
        record_payload_rejected(kind, rejected_count);
        if matches!(kind, PrometheusPayloadKind::Exemplar) {
            exemplar_store.record_rejected(exemplar_count);
        }
        return text_response(422, &message);
    }
    let exemplar_request_limit = exemplar_store.config().max_exemplars_per_request;
    if let Err((kind, message)) = validate_payload_quotas(
        payload_config,
        metadata_count,
        exemplar_count,
        exemplar_request_limit,
        histogram_bucket_entries,
    ) {
        let throttled_count =
            payload_item_count(kind, metadata_count, exemplar_count, histogram_count);
        record_payload_throttled(kind, throttled_count);
        if matches!(kind, PrometheusPayloadKind::Exemplar) {
            exemplar_store.record_rejected(exemplar_count);
        }
        return HttpResponse::new(413, message).with_header("Content-Type", "text/plain");
    }

    let ingest_units = envelope_row_units(&envelope);
    if let Err(err) = tenant::enforce_write_rows_quota(tenant_plan.policy(), ingest_units) {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Ingest,
            ingest_units,
            err.clone(),
        );
        return HttpResponse::new(413, err).with_header("Content-Type", "text/plain");
    }
    let tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Ingest,
        ingest_units,
        usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let request_slot = match write_admission.acquire_request_slot().await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Ingest,
                ingest_units,
                err.to_string(),
            );
            return write_admission_error_response(err);
        }
    };

    let apply_result = match apply_normalized_write_envelope(
        storage,
        metadata_store,
        exemplar_store,
        request,
        cluster_context,
        edge_sync_context,
        &tenant_request,
        &tenant_id,
        envelope,
        write_admission,
        request_slot,
        histogram_count > 0,
    )
    .await
    {
        Ok(result) => result,
        Err(response) => {
            let accepted_rows = response_header_count(&response, WRITE_ROWS_ACCEPTED_HEADER);
            let accepted_metadata = response_header_count(&response, "X-Tsink-Metadata-Accepted");
            let applied_metadata = response_header_count(&response, "X-Tsink-Metadata-Applied");
            let accepted_exemplars = response_header_count(&response, "X-Tsink-Exemplars-Accepted");
            let indeterminate = response_write_outcome_is_indeterminate(&response);

            if histogram_count > 0 {
                if accepted_rows > 0 {
                    record_payload_accepted(PrometheusPayloadKind::Histogram, histogram_count);
                } else if !indeterminate {
                    record_payload_rejected(PrometheusPayloadKind::Histogram, histogram_count);
                }
            }
            if accepted_metadata > 0 {
                record_payload_accepted(PrometheusPayloadKind::Metadata, accepted_metadata);
            }
            if !indeterminate && metadata_count > accepted_metadata {
                record_payload_rejected(
                    PrometheusPayloadKind::Metadata,
                    metadata_count - accepted_metadata,
                );
            }
            if accepted_exemplars > 0 {
                record_payload_accepted(PrometheusPayloadKind::Exemplar, accepted_exemplars);
            }
            if !indeterminate && exemplar_count > accepted_exemplars {
                record_payload_rejected(
                    PrometheusPayloadKind::Exemplar,
                    exemplar_count - accepted_exemplars,
                );
            }
            if accepted_rows > 0 || accepted_metadata > 0 || accepted_exemplars > 0 {
                record_ingest_usage(
                    usage_accounting,
                    &tenant_id,
                    "remote_write",
                    request.path_without_query(),
                    IngestUsageMetrics::new(
                        accepted_rows as u64,
                        applied_metadata as u64,
                        accepted_exemplars as u64,
                        0,
                        if accepted_rows > 0 {
                            histogram_count as u64
                        } else {
                            0
                        },
                        elapsed_nanos_since(started),
                        request.body.len() as u64,
                    ),
                );
            }
            return response;
        }
    };

    if histogram_count > 0 {
        record_payload_accepted(PrometheusPayloadKind::Histogram, histogram_count);
    }
    if metadata_count > 0 {
        record_payload_accepted(
            PrometheusPayloadKind::Metadata,
            apply_result.accepted_metadata_updates,
        );
    }
    if exemplar_count > 0 {
        record_payload_accepted(
            PrometheusPayloadKind::Exemplar,
            apply_result.accepted_exemplars,
        );
    }
    record_ingest_usage(
        usage_accounting,
        &tenant_id,
        "remote_write",
        request.path_without_query(),
        IngestUsageMetrics::new(
            ingest_units as u64,
            apply_result.applied_metadata_updates as u64,
            apply_result.accepted_exemplars as u64,
            apply_result.dropped_exemplars as u64,
            histogram_count as u64,
            elapsed_nanos_since(started),
            request.body.len() as u64,
        ),
    );

    let mut response = HttpResponse::new(200, Vec::<u8>::new());
    if let Some(consistency) = apply_result.consistency {
        response = response
            .with_header("X-Tsink-Write-Consistency", consistency.mode.to_string())
            .with_header(
                "X-Tsink-Write-Required-Acks",
                consistency.required_acks.to_string(),
            )
            .with_header(
                "X-Tsink-Write-Acknowledged-Replicas",
                consistency.acknowledged_replicas_min.to_string(),
            );
    }
    if let Some(acknowledgement) = apply_result.acknowledgement {
        response = response.with_header(WRITE_ACKNOWLEDGEMENT_HEADER, acknowledgement.as_str());
    }
    if apply_result.applied_metadata_updates > 0 || metadata_count > 0 {
        response = response
            .with_header(
                "X-Tsink-Metadata-Accepted",
                apply_result.accepted_metadata_updates.to_string(),
            )
            .with_header(
                "X-Tsink-Metadata-Applied",
                apply_result.applied_metadata_updates.to_string(),
            );
    }
    if histogram_count > 0 {
        response = response.with_header("X-Tsink-Histograms-Accepted", histogram_count.to_string());
    }
    if exemplar_count > 0 {
        response = response
            .with_header(
                "X-Tsink-Exemplars-Accepted",
                apply_result.accepted_exemplars.to_string(),
            )
            .with_header(
                "X-Tsink-Exemplars-Dropped",
                apply_result.dropped_exemplars.to_string(),
            );
    }
    response
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_otlp_metrics(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let write_admission = match admission::global_public_write_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("write admission unavailable: {err}")),
    };
    handle_otlp_metrics_with_admission(
        storage,
        metadata_store,
        exemplar_store,
        request,
        precision,
        cluster_context,
        edge_sync_context,
        tenant_registry,
        managed_control_plane,
        usage_accounting,
        write_admission,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_otlp_metrics_with_admission(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    usage_accounting: Option<&UsageAccounting>,
    write_admission: &WriteAdmissionController,
) -> HttpResponse {
    let started = Instant::now();
    if !otlp_metrics_config().enabled {
        record_otlp_request_rejected();
        return text_response(422, "OTLP metrics ingest is disabled on this node");
    }
    if let Some(content_type) = request.header("content-type") {
        let media_type = content_type.split(';').next().unwrap_or("").trim();
        if !media_type.is_empty()
            && media_type != "application/x-protobuf"
            && media_type != "application/protobuf"
        {
            record_otlp_request_rejected();
            return HttpResponse::new(415, "unsupported content-type for /v1/metrics")
                .with_header("Content-Type", "text/plain");
        }
    }

    let tenant_id = match tenant_id_for_text_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        tenant_registry,
        managed_control_plane,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Write,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let decoded = match decode_body(request) {
        Ok(body) => body,
        Err(err) => {
            record_otlp_request_rejected();
            return text_response(400, &err);
        }
    };

    let export_request = match ExportMetricsServiceRequest::decode(decoded.as_slice()) {
        Ok(req) => req,
        Err(err) => {
            record_otlp_request_rejected();
            return text_response(400, &format!("invalid OTLP protobuf body: {err}"));
        }
    };

    let (envelope, stats) =
        match normalize_metrics_export_request(export_request, &tenant_id, precision) {
            Ok(result) => result,
            Err(err) => {
                record_otlp_request_rejected();
                if let Some(kind) = err.stats.rejected_kind {
                    record_otlp_rejected_kind(kind);
                }
                return text_response(400, bounded_write_rejection_diagnostic(&err.to_string()))
                    .with_header(WRITE_ERROR_CODE_HEADER, "otlp_invalid_metrics_payload");
            }
        };

    let exemplar_count = envelope.exemplars.len();
    let payload_config = prometheus_payload_config();
    if exemplar_count > 0 && !payload_config.exemplars_enabled {
        record_otlp_request_rejected();
        OTLP_EXEMPLAR_REJECTED_TOTAL.fetch_add(exemplar_count as u64, Ordering::Relaxed);
        exemplar_store.record_rejected(exemplar_count);
        return text_response(
            422,
            "OTLP exemplars are disabled because exemplar ingest is disabled on this node",
        );
    }
    if exemplar_count > exemplar_store.config().max_exemplars_per_request {
        record_otlp_request_rejected();
        OTLP_EXEMPLAR_REJECTED_TOTAL.fetch_add(exemplar_count as u64, Ordering::Relaxed);
        exemplar_store.record_rejected(exemplar_count);
        return HttpResponse::new(
            413,
            format!(
                "OTLP exemplar payload exceeds limit: {} > {}",
                exemplar_count,
                exemplar_store.config().max_exemplars_per_request
            ),
        )
        .with_header("Content-Type", "text/plain");
    }

    let total_points = stats
        .gauges
        .saturating_add(stats.sums)
        .saturating_add(stats.histograms)
        .saturating_add(stats.summaries);
    let metadata_count = envelope.metadata_updates.len();
    let ingest_units = envelope_row_units(&envelope);
    if let Err(err) = tenant::enforce_write_rows_quota(tenant_plan.policy(), ingest_units) {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Ingest,
            ingest_units,
            err.clone(),
        );
        return HttpResponse::new(413, err).with_header("Content-Type", "text/plain");
    }
    let tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Ingest,
        ingest_units,
        usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let request_slot = match write_admission.acquire_request_slot().await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Ingest,
                ingest_units,
                err.to_string(),
            );
            return write_admission_error_response(err);
        }
    };
    let apply_result = match apply_normalized_write_envelope(
        storage,
        metadata_store,
        exemplar_store,
        request,
        cluster_context,
        edge_sync_context,
        &tenant_request,
        &tenant_id,
        envelope,
        write_admission,
        request_slot,
        false,
    )
    .await
    {
        Ok(result) => result,
        Err(response) => {
            record_otlp_request_rejected();
            let accepted_rows = response_header_count(&response, WRITE_ROWS_ACCEPTED_HEADER);
            let accepted_metadata = response_header_count(&response, "X-Tsink-Metadata-Accepted");
            let applied_metadata = response_header_count(&response, "X-Tsink-Metadata-Applied");
            let accepted_exemplars = response_header_count(&response, "X-Tsink-Exemplars-Accepted");
            let indeterminate = response_write_outcome_is_indeterminate(&response);

            if accepted_rows > 0 {
                record_otlp_points_accepted(&stats);
            } else if !indeterminate {
                record_otlp_points_rejected(&stats);
            }
            if accepted_metadata > 0 {
                record_payload_accepted(PrometheusPayloadKind::Metadata, accepted_metadata);
            }
            if !indeterminate && metadata_count > accepted_metadata {
                record_payload_rejected(
                    PrometheusPayloadKind::Metadata,
                    metadata_count - accepted_metadata,
                );
            }
            OTLP_EXEMPLAR_ACCEPTED_TOTAL.fetch_add(accepted_exemplars as u64, Ordering::Relaxed);
            if !indeterminate && exemplar_count > accepted_exemplars {
                OTLP_EXEMPLAR_REJECTED_TOTAL.fetch_add(
                    (exemplar_count - accepted_exemplars) as u64,
                    Ordering::Relaxed,
                );
            }
            if accepted_rows > 0 || accepted_metadata > 0 || accepted_exemplars > 0 {
                record_ingest_usage(
                    usage_accounting,
                    &tenant_id,
                    "otlp_metrics",
                    request.path_without_query(),
                    IngestUsageMetrics::new(
                        accepted_rows as u64,
                        applied_metadata as u64,
                        accepted_exemplars as u64,
                        0,
                        0,
                        elapsed_nanos_since(started),
                        request.body.len() as u64,
                    ),
                );
            }
            return response;
        }
    };

    record_otlp_request_accepted();
    record_otlp_points_accepted(&stats);
    if apply_result.accepted_metadata_updates > 0 {
        record_payload_accepted(
            PrometheusPayloadKind::Metadata,
            apply_result.accepted_metadata_updates,
        );
    }
    OTLP_EXEMPLAR_ACCEPTED_TOTAL
        .fetch_add(apply_result.accepted_exemplars as u64, Ordering::Relaxed);
    record_ingest_usage(
        usage_accounting,
        &tenant_id,
        "otlp_metrics",
        request.path_without_query(),
        IngestUsageMetrics::new(
            ingest_units as u64,
            apply_result.applied_metadata_updates as u64,
            apply_result.accepted_exemplars as u64,
            apply_result.dropped_exemplars as u64,
            0,
            elapsed_nanos_since(started),
            request.body.len() as u64,
        ),
    );

    let mut encoded = Vec::new();
    if let Err(err) = (ExportMetricsServiceResponse {
        partial_success: None,
    })
    .encode(&mut encoded)
    {
        return text_response(500, &format!("failed to encode OTLP response: {err}"));
    }

    let mut response =
        HttpResponse::new(200, encoded).with_header("Content-Type", "application/x-protobuf");
    if let Some(consistency) = apply_result.consistency {
        response = response
            .with_header("X-Tsink-Write-Consistency", consistency.mode.to_string())
            .with_header(
                "X-Tsink-Write-Required-Acks",
                consistency.required_acks.to_string(),
            )
            .with_header(
                "X-Tsink-Write-Acknowledged-Replicas",
                consistency.acknowledged_replicas_min.to_string(),
            );
    }
    if let Some(acknowledgement) = apply_result.acknowledgement {
        response = response.with_header(WRITE_ACKNOWLEDGEMENT_HEADER, acknowledgement.as_str());
    }
    response = response.with_header(
        "X-Tsink-OTLP-Data-Points-Accepted",
        total_points.to_string(),
    );
    if metadata_count > 0 || apply_result.applied_metadata_updates > 0 {
        response = response
            .with_header(
                "X-Tsink-Metadata-Accepted",
                apply_result.accepted_metadata_updates.to_string(),
            )
            .with_header(
                "X-Tsink-Metadata-Applied",
                apply_result.applied_metadata_updates.to_string(),
            );
    }
    if exemplar_count > 0 {
        response = response
            .with_header(
                "X-Tsink-Exemplars-Accepted",
                apply_result.accepted_exemplars.to_string(),
            )
            .with_header(
                "X-Tsink-Exemplars-Dropped",
                apply_result.dropped_exemplars.to_string(),
            );
    }
    response
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_prometheus_import(
    storage: &Arc<dyn Storage>,
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let write_admission = match admission::global_public_write_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("write admission unavailable: {err}")),
    };
    handle_prometheus_import_with_admission(
        storage,
        exemplar_store,
        request,
        precision,
        cluster_context,
        edge_sync_context,
        tenant_registry,
        managed_control_plane,
        usage_accounting,
        write_admission,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_prometheus_import_with_admission(
    storage: &Arc<dyn Storage>,
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    usage_accounting: Option<&UsageAccounting>,
    write_admission: &WriteAdmissionController,
) -> HttpResponse {
    let started = Instant::now();
    let tenant_id = match tenant_id_for_text_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        tenant_registry,
        managed_control_plane,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Write,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let body_str = match std::str::from_utf8(&request.body) {
        Ok(s) => s.to_string(),
        Err(_) => return text_response(400, "body must be valid UTF-8"),
    };

    let now = current_timestamp(precision);
    let parsed = match parse_prometheus_text_with_exemplars(&body_str, now) {
        Ok(parsed) => parsed,
        Err(err) => return prometheus_import_parse_error_response(&err),
    };
    let payload_config = prometheus_payload_config();
    if !payload_config.exemplars_enabled && !parsed.exemplars.is_empty() {
        record_payload_rejected(PrometheusPayloadKind::Exemplar, parsed.exemplars.len());
        exemplar_store.record_rejected(parsed.exemplars.len());
        return text_response(
            422,
            "prometheus import exemplar payloads are disabled on this node",
        );
    }
    if parsed.exemplars.len() > exemplar_store.config().max_exemplars_per_request {
        record_payload_throttled(PrometheusPayloadKind::Exemplar, parsed.exemplars.len());
        exemplar_store.record_rejected(parsed.exemplars.len());
        return HttpResponse::new(
            413,
            format!(
                "prometheus import exemplar payload exceeds limit: {} > {}",
                parsed.exemplars.len(),
                exemplar_store.config().max_exemplars_per_request
            ),
        )
        .with_header("Content-Type", "text/plain");
    }
    let rows = parsed.rows;
    let exemplars = match scope_exemplars_for_tenant(parsed.exemplars, &tenant_id) {
        Ok(exemplars) => exemplars,
        Err(err) => return text_response(400, &err),
    };
    let exemplar_count = exemplars.len();
    let row_count = rows.len();

    if rows.is_empty() && exemplars.is_empty() {
        return HttpResponse::new(200, Vec::<u8>::new());
    }
    if let Err(err) = tenant::enforce_write_rows_quota(tenant_plan.policy(), rows.len()) {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Ingest,
            rows.len(),
            err.clone(),
        );
        return HttpResponse::new(413, err).with_header("Content-Type", "text/plain");
    }
    let tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Ingest,
        rows.len(),
        usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let request_slot = match write_admission.acquire_request_slot().await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Ingest,
                rows.len(),
                err.to_string(),
            );
            return write_admission_error_response(err);
        }
    };
    let rows = match tenant::scope_rows_for_tenant(rows, &tenant_id) {
        Ok(rows) => rows,
        Err(err) => return text_response(400, &err),
    };
    hotspot::record_ingest_rows(cluster_context.map(|context| &context.runtime.ring), &rows);
    let _write_admission = if rows.is_empty() {
        None
    } else {
        match write_admission.reserve_rows(request_slot, rows.len()).await {
            Ok(lease) => Some(lease),
            Err(err) => return write_admission_error_response(err),
        }
    };

    if let Some(cluster_context) = cluster_context {
        let ring_version = cluster_ring_version(Some(cluster_context));
        let write_router = match effective_write_router(cluster_context) {
            Ok(router) => router,
            Err(err) => {
                return text_response(
                    503,
                    &format!("cluster write routing topology unavailable: {err}"),
                )
            }
        };
        let write_router = if let Some(mode) = tenant_request.policy().write_consistency {
            write_router.with_default_write_consistency(mode)
        } else {
            write_router
        };
        let requested_consistency =
            match resolve_request_write_consistency(request, tenant_request.policy()) {
                Ok(mode) => mode,
                Err(err) => return text_response(400, &err),
            };
        let row_stats = if rows.is_empty() {
            None
        } else {
            match write_router
                .route_and_write_with_consistency_and_ring_version(
                    storage,
                    &cluster_context.rpc_client,
                    rows,
                    requested_consistency,
                    ring_version,
                )
                .await
            {
                Ok(stats) => Some(stats),
                Err(err) => {
                    return indeterminate_cluster_write_response(write_routing_error_response(err))
                }
            }
        };
        let exemplar_stats = if exemplars.is_empty() {
            None
        } else {
            match route_exemplars_with_consistency_and_ring_version(
                exemplar_store,
                cluster_context,
                exemplars,
                requested_consistency.unwrap_or(cluster_context.runtime.write_consistency),
                ring_version,
            )
            .await
            {
                Ok(stats) => {
                    record_payload_accepted(
                        PrometheusPayloadKind::Exemplar,
                        stats.accepted_exemplars,
                    );
                    Some(stats)
                }
                Err(err) => {
                    return indeterminate_cluster_write_response(partial_write_error_response(
                        text_response(409, &err),
                        row_count,
                        (row_count > 0).then_some(WriteAcknowledgement::Volatile),
                        0,
                        0,
                        0,
                    ));
                }
            }
        };
        let accepted_exemplars = exemplar_stats
            .as_ref()
            .map(|stats| stats.accepted_exemplars as u64)
            .unwrap_or(0);
        let dropped_exemplars = exemplar_stats
            .as_ref()
            .map(|stats| stats.dropped_exemplars as u64)
            .unwrap_or(0);
        record_ingest_usage(
            usage_accounting,
            &tenant_id,
            "prometheus_import",
            request.path_without_query(),
            IngestUsageMetrics::new(
                row_count as u64,
                0,
                accepted_exemplars,
                dropped_exemplars,
                0,
                elapsed_nanos_since(started),
                request.body.len() as u64,
            ),
        );

        let mut response = HttpResponse::new(200, Vec::<u8>::new());
        if let Some(consistency) = weakest_write_consistency(
            row_stats.as_ref().and_then(|stats| stats.consistency),
            exemplar_stats.as_ref().and_then(|stats| stats.consistency),
        ) {
            response = response
                .with_header("X-Tsink-Write-Consistency", consistency.mode.to_string())
                .with_header(
                    "X-Tsink-Write-Required-Acks",
                    consistency.required_acks.to_string(),
                )
                .with_header(
                    "X-Tsink-Write-Acknowledged-Replicas",
                    consistency.acknowledged_replicas_min.to_string(),
                );
        }
        if let Some(stats) = exemplar_stats {
            response = response
                .with_header(
                    "X-Tsink-Exemplars-Accepted",
                    stats.accepted_exemplars.to_string(),
                )
                .with_header(
                    "X-Tsink-Exemplars-Dropped",
                    stats.dropped_exemplars.to_string(),
                );
        }
        let acknowledgement = if exemplar_count > 0 {
            // The exemplar sidecar has no WAL-backed durability contract, so it weakens the
            // complete import even when every routed row replica reported a stronger result.
            Some(WriteAcknowledgement::Volatile)
        } else {
            row_stats.as_ref().and_then(|stats| stats.acknowledgement)
        };
        if let Some(acknowledgement) = acknowledgement {
            response = response.with_header(WRITE_ACKNOWLEDGEMENT_HEADER, acknowledgement.as_str());
        }
        return response;
    }

    let mut acknowledgement = if rows.is_empty() {
        None
    } else {
        let edge_rows = rows.clone();
        let storage = Arc::clone(storage);
        let result =
            tokio::task::spawn_blocking(move || storage.write_batch(&rows, WriteMode::Atomic))
                .await;
        match result {
            Ok(Ok(result)) => {
                let acknowledgement =
                    match validate_atomic_write_result("import", edge_rows.len(), &result) {
                        Ok(acknowledgement) => acknowledgement,
                        Err(response) => return response,
                    };
                if let Err(error) = maybe_enqueue_edge_sync_rows(edge_sync_context, &edge_rows) {
                    return edge_sync_enqueue_error_response(
                        "import",
                        &error,
                        edge_rows.len(),
                        acknowledgement,
                    );
                }
                Some(acknowledgement)
            }
            Ok(Err(err)) => return storage_write_error_response("import", &err),
            Err(_) => return backend_write_task_failure_response("import"),
        }
    };

    if exemplars.is_empty() {
        record_ingest_usage(
            usage_accounting,
            &tenant_id,
            "prometheus_import",
            request.path_without_query(),
            IngestUsageMetrics::new(
                row_count as u64,
                0,
                0,
                0,
                0,
                elapsed_nanos_since(started),
                request.body.len() as u64,
            ),
        );
        let mut response = HttpResponse::new(200, Vec::<u8>::new());
        if let Some(acknowledgement) = acknowledgement {
            response = response.with_header(WRITE_ACKNOWLEDGEMENT_HEADER, acknowledgement.as_str());
        }
        return response;
    }
    match exemplar_store.apply_writes(
        &exemplars
            .into_iter()
            .map(normalized_exemplar_to_store_write)
            .collect::<Vec<_>>(),
    ) {
        Ok(outcome) => {
            acknowledgement =
                weakest_write_acknowledgement(acknowledgement, WriteAcknowledgement::Volatile);
            record_payload_accepted(PrometheusPayloadKind::Exemplar, outcome.accepted);
            record_ingest_usage(
                usage_accounting,
                &tenant_id,
                "prometheus_import",
                request.path_without_query(),
                IngestUsageMetrics::new(
                    row_count as u64,
                    0,
                    outcome.accepted as u64,
                    outcome.dropped as u64,
                    0,
                    elapsed_nanos_since(started),
                    request.body.len() as u64,
                ),
            );
            let mut response = HttpResponse::new(200, Vec::<u8>::new())
                .with_header("X-Tsink-Exemplars-Accepted", outcome.accepted.to_string())
                .with_header("X-Tsink-Exemplars-Dropped", outcome.dropped.to_string());
            if let Some(acknowledgement) = acknowledgement {
                response =
                    response.with_header(WRITE_ACKNOWLEDGEMENT_HEADER, acknowledgement.as_str());
            }
            response
        }
        Err(err) => {
            record_payload_rejected(PrometheusPayloadKind::Exemplar, exemplar_count);
            if row_count > 0 {
                record_ingest_usage(
                    usage_accounting,
                    &tenant_id,
                    "prometheus_import",
                    request.path_without_query(),
                    IngestUsageMetrics::new(
                        row_count as u64,
                        0,
                        0,
                        0,
                        0,
                        elapsed_nanos_since(started),
                        request.body.len() as u64,
                    ),
                );
            }
            partial_write_error_response(
                text_response(500, &format!("exemplar import failed: {err}")),
                row_count,
                acknowledgement,
                0,
                0,
                0,
            )
        }
    }
}

#[derive(Debug, Default)]
struct ParsedPrometheusImport {
    rows: Vec<Row>,
    exemplars: Vec<NormalizedExemplar>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrometheusImportParseErrorClass {
    UnclosedLabelBlock,
    InvalidLabelName,
    MissingLabelEquals,
    UnquotedLabelValue,
    UnterminatedLabelEscape,
    UnterminatedLabelValue,
    MissingLabelSeparator,
    MissingSampleValue,
    InvalidSampleValue,
    InvalidSampleTimestamp,
    InvalidExemplarFragment,
    MissingExemplarValue,
    InvalidExemplarValue,
    InvalidExemplarTimestamp,
}

impl PrometheusImportParseErrorClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::UnclosedLabelBlock => "unclosed_label_block",
            Self::InvalidLabelName => "invalid_label_name",
            Self::MissingLabelEquals => "missing_label_equals",
            Self::UnquotedLabelValue => "unquoted_label_value",
            Self::UnterminatedLabelEscape => "unterminated_label_escape",
            Self::UnterminatedLabelValue => "unterminated_label_value",
            Self::MissingLabelSeparator => "missing_label_separator",
            Self::MissingSampleValue => "missing_sample_value",
            Self::InvalidSampleValue => "invalid_sample_value",
            Self::InvalidSampleTimestamp => "invalid_sample_timestamp",
            Self::InvalidExemplarFragment => "invalid_exemplar_fragment",
            Self::MissingExemplarValue => "missing_exemplar_value",
            Self::InvalidExemplarValue => "invalid_exemplar_value",
            Self::InvalidExemplarTimestamp => "invalid_exemplar_timestamp",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::UnclosedLabelBlock => "label block is not closed",
            Self::InvalidLabelName => "label name is invalid",
            Self::MissingLabelEquals => "label assignment is missing '='",
            Self::UnquotedLabelValue => "label value must be quoted",
            Self::UnterminatedLabelEscape => "label escape sequence is not terminated",
            Self::UnterminatedLabelValue => "label value is not terminated",
            Self::MissingLabelSeparator => "labels must be separated by ','",
            Self::MissingSampleValue => "sample value is missing",
            Self::InvalidSampleValue => "sample value is not a number",
            Self::InvalidSampleTimestamp => "sample timestamp is not an integer",
            Self::InvalidExemplarFragment => "exemplar fragment is malformed",
            Self::MissingExemplarValue => "exemplar value is missing",
            Self::InvalidExemplarValue => "exemplar value is not a number",
            Self::InvalidExemplarTimestamp => "exemplar timestamp is not an integer",
        }
    }

    fn http_error_code(self) -> &'static str {
        match self {
            Self::UnclosedLabelBlock
            | Self::InvalidLabelName
            | Self::MissingLabelEquals
            | Self::UnquotedLabelValue
            | Self::UnterminatedLabelEscape
            | Self::UnterminatedLabelValue
            | Self::MissingLabelSeparator => "prometheus_import_invalid_labels",
            Self::MissingSampleValue => "prometheus_import_missing_value",
            Self::InvalidSampleValue => "prometheus_import_invalid_value",
            Self::InvalidSampleTimestamp => "prometheus_import_invalid_timestamp",
            Self::InvalidExemplarFragment => "prometheus_import_invalid_exemplar",
            Self::MissingExemplarValue => "prometheus_import_missing_exemplar_value",
            Self::InvalidExemplarValue => "prometheus_import_invalid_exemplar_value",
            Self::InvalidExemplarTimestamp => "prometheus_import_invalid_exemplar_timestamp",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PrometheusImportParseError {
    class: PrometheusImportParseErrorClass,
    line_number: Option<usize>,
    label_index: Option<usize>,
}

impl PrometheusImportParseError {
    fn new(class: PrometheusImportParseErrorClass) -> Self {
        Self {
            class,
            line_number: None,
            label_index: None,
        }
    }

    fn at_line(mut self, line_number: usize) -> Self {
        self.line_number = Some(line_number);
        self
    }

    fn at_label(mut self, label_index: usize) -> Self {
        self.label_index = Some(label_index);
        self
    }
}

impl std::fmt::Display for PrometheusImportParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("prometheus import parse error")?;
        if let Some(line_number) = self.line_number {
            write!(formatter, " at line {line_number}")?;
        }
        if let Some(label_index) = self.label_index {
            write!(formatter, ", label {label_index}")?;
        }
        write!(
            formatter,
            ": {} ({})",
            self.class.as_str(),
            self.class.description()
        )
    }
}

fn prometheus_import_parse_error_response(error: &PrometheusImportParseError) -> HttpResponse {
    let diagnostic = error.to_string();
    text_response(400, bounded_write_rejection_diagnostic(&diagnostic))
        .with_header(WRITE_ERROR_CODE_HEADER, error.class.http_error_code())
}

fn scope_exemplars_for_tenant(
    exemplars: Vec<NormalizedExemplar>,
    tenant_id: &str,
) -> Result<Vec<NormalizedExemplar>, String> {
    exemplars
        .into_iter()
        .map(|mut exemplar| {
            if exemplar
                .series
                .labels
                .iter()
                .any(|label| label.name == tenant::TENANT_LABEL)
            {
                return Err(format!(
                    "label '{}' is reserved for server-managed tenant isolation",
                    tenant::TENANT_LABEL
                ));
            }
            exemplar
                .series
                .labels
                .push(Label::new(tenant::TENANT_LABEL, tenant_id));
            exemplar.series.labels.sort();
            Ok(exemplar)
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn parse_prometheus_text(
    text: &str,
    default_timestamp: i64,
) -> Result<Vec<Row>, String> {
    parse_prometheus_text_with_exemplars(text, default_timestamp)
        .map(|parsed| parsed.rows)
        .map_err(|err| err.to_string())
}

fn parse_prometheus_text_with_exemplars(
    text: &str,
    default_timestamp: i64,
) -> Result<ParsedPrometheusImport, PrometheusImportParseError> {
    let mut parsed = ParsedPrometheusImport::default();

    let mut rows = Vec::new();

    for (line_index, line) in text.lines().enumerate() {
        let line_number = line_index + 1;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (sample_text, exemplar_text) = split_openmetrics_exemplar(line);

        let (metric_and_labels, rest) = if let Some(brace_start) = sample_text.find('{') {
            let brace_end = find_label_block_end(sample_text, brace_start).ok_or_else(|| {
                PrometheusImportParseError::new(PrometheusImportParseErrorClass::UnclosedLabelBlock)
                    .at_line(line_number)
            })?;
            let metric_name = &sample_text[..brace_start];
            let labels_str = &sample_text[brace_start + 1..brace_end];
            let rest = sample_text[brace_end + 1..].trim();
            let labels =
                parse_prom_labels_diagnostic(labels_str).map_err(|err| err.at_line(line_number))?;
            ((metric_name.to_string(), labels), rest)
        } else {
            let mut parts = sample_text.splitn(2, |c: char| c.is_whitespace());
            let metric_name = parts.next().unwrap_or("");
            let rest = parts.next().unwrap_or("");
            ((metric_name.to_string(), Vec::new()), rest)
        };

        let (metric_name, labels) = metric_and_labels;
        if metric_name.is_empty() {
            continue;
        }

        let mut value_parts = rest.split_whitespace();
        let value_str = value_parts.next().ok_or_else(|| {
            PrometheusImportParseError::new(PrometheusImportParseErrorClass::MissingSampleValue)
                .at_line(line_number)
        })?;
        let value: f64 = value_str.parse().map_err(|_| {
            PrometheusImportParseError::new(PrometheusImportParseErrorClass::InvalidSampleValue)
                .at_line(line_number)
        })?;

        let timestamp = if let Some(ts_str) = value_parts.next() {
            ts_str.parse::<i64>().map_err(|_| {
                PrometheusImportParseError::new(
                    PrometheusImportParseErrorClass::InvalidSampleTimestamp,
                )
                .at_line(line_number)
            })?
        } else {
            default_timestamp
        };

        rows.push(Row::with_labels(
            metric_name.clone(),
            labels.clone(),
            DataPoint::new(timestamp, value),
        ));
        if let Some(exemplar_text) = exemplar_text {
            parsed.exemplars.push(
                parse_openmetrics_exemplar(exemplar_text, &metric_name, &labels, timestamp)
                    .map_err(|err| err.at_line(line_number))?,
            );
        }
    }

    parsed.rows = rows;
    Ok(parsed)
}

fn split_openmetrics_exemplar(line: &str) -> (&str, Option<&str>) {
    match line.split_once(" # ") {
        Some((sample, exemplar)) => (sample.trim(), Some(exemplar.trim())),
        None => (line, None),
    }
}

fn parse_openmetrics_exemplar(
    text: &str,
    metric: &str,
    labels: &[Label],
    sample_timestamp: i64,
) -> Result<NormalizedExemplar, PrometheusImportParseError> {
    if !text.starts_with('{') {
        return Err(PrometheusImportParseError::new(
            PrometheusImportParseErrorClass::InvalidExemplarFragment,
        ));
    }
    let brace_end = find_label_block_end(text, 0).ok_or_else(|| {
        PrometheusImportParseError::new(PrometheusImportParseErrorClass::InvalidExemplarFragment)
    })?;
    let exemplar_labels = parse_prom_labels_diagnostic(&text[1..brace_end])?;
    let rest = text[brace_end + 1..].trim();
    let mut parts = rest.split_whitespace();
    let value_str = parts.next().ok_or_else(|| {
        PrometheusImportParseError::new(PrometheusImportParseErrorClass::MissingExemplarValue)
    })?;
    let value = value_str.parse::<f64>().map_err(|_| {
        PrometheusImportParseError::new(PrometheusImportParseErrorClass::InvalidExemplarValue)
    })?;
    let timestamp = match parts.next() {
        Some(value) => value.parse::<i64>().map_err(|_| {
            PrometheusImportParseError::new(
                PrometheusImportParseErrorClass::InvalidExemplarTimestamp,
            )
        })?,
        None => sample_timestamp,
    };
    let mut series_labels = labels.to_vec();
    series_labels.sort();
    Ok(NormalizedExemplar {
        series: NormalizedSeriesIdentity {
            metric: metric.to_string(),
            labels: series_labels,
        },
        labels: exemplar_labels,
        timestamp,
        value,
    })
}

fn find_label_block_end(line: &str, open_brace: usize) -> Option<usize> {
    let mut in_quotes = false;
    let mut escaped = false;

    for (offset, ch) in line[open_brace + 1..].char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quotes => escaped = true,
            '"' => in_quotes = !in_quotes,
            '}' if !in_quotes => return Some(open_brace + 1 + offset),
            _ => {}
        }
    }
    None
}

#[cfg(test)]
pub(crate) fn parse_prom_labels(labels_str: &str) -> Result<Vec<Label>, String> {
    parse_prom_labels_diagnostic(labels_str).map_err(|err| err.to_string())
}

fn parse_prom_labels_diagnostic(
    labels_str: &str,
) -> Result<Vec<Label>, PrometheusImportParseError> {
    let mut labels = Vec::new();
    let mut chars = labels_str.chars().peekable();

    loop {
        while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
            chars.next();
        }

        if chars.peek().is_none() {
            break;
        }
        let label_index = labels.len() + 1;

        let mut name = String::new();
        while let Some(&ch) = chars.peek() {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                name.push(ch);
                chars.next();
            } else {
                break;
            }
        }

        if name.is_empty() {
            return Err(PrometheusImportParseError::new(
                PrometheusImportParseErrorClass::InvalidLabelName,
            )
            .at_label(label_index));
        }

        while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
            chars.next();
        }

        if chars.next() != Some('=') {
            return Err(PrometheusImportParseError::new(
                PrometheusImportParseErrorClass::MissingLabelEquals,
            )
            .at_label(label_index));
        }

        while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
            chars.next();
        }

        if chars.next() != Some('"') {
            return Err(PrometheusImportParseError::new(
                PrometheusImportParseErrorClass::UnquotedLabelValue,
            )
            .at_label(label_index));
        }

        let mut value = String::new();
        loop {
            match chars.next() {
                Some('"') => break,
                Some('\\') => match chars.next() {
                    Some('\\') => value.push('\\'),
                    Some('"') => value.push('"'),
                    Some('n') => value.push('\n'),
                    Some('t') => value.push('\t'),
                    Some('r') => value.push('\r'),
                    Some(other) => value.push(other),
                    None => {
                        return Err(PrometheusImportParseError::new(
                            PrometheusImportParseErrorClass::UnterminatedLabelEscape,
                        )
                        .at_label(label_index));
                    }
                },
                Some(ch) => value.push(ch),
                None => {
                    return Err(PrometheusImportParseError::new(
                        PrometheusImportParseErrorClass::UnterminatedLabelValue,
                    )
                    .at_label(label_index));
                }
            }
        }

        labels.push(Label::new(name, value));

        while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
            chars.next();
        }

        match chars.peek() {
            Some(',') => {
                chars.next();
            }
            Some(_) => {
                return Err(PrometheusImportParseError::new(
                    PrometheusImportParseErrorClass::MissingLabelSeparator,
                )
                .at_label(label_index));
            }
            None => break,
        }
    }

    Ok(labels)
}

#[cfg(test)]
mod import_diagnostic_tests {
    use super::*;
    use std::collections::HashMap;

    fn response_header<'a>(response: &'a HttpResponse, name: &str) -> Option<&'a str> {
        response
            .headers
            .iter()
            .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn long_sensitive_sample_and_label_values_are_not_echoed_in_import_diagnostics() {
        let secret = format!("sensitive-token-{}", "x".repeat(8_192));
        let text = format!("valid_metric 1\nprivate_metric{{token=\"{secret}\"}} {secret}\n");

        let error = parse_prometheus_text_with_exemplars(&text, 0)
            .expect_err("invalid sample value should be rejected");
        let response = prometheus_import_parse_error_response(&error);

        assert_eq!(response.status, 400);
        assert_eq!(
            response_header(&response, WRITE_ERROR_CODE_HEADER),
            Some("prometheus_import_invalid_value")
        );
        let body = String::from_utf8(response.body).expect("diagnostic should be UTF-8");
        assert!(body.contains("line 2"));
        assert!(body.contains("invalid_sample_value"));
        assert!(body.len() <= MAX_WRITE_REJECTION_MESSAGE_BYTES);
        assert!(!body.contains("sensitive-token"));
        assert!(!body.contains("private_metric"));
    }

    #[test]
    fn malformed_label_and_exemplar_diagnostics_use_positions_and_fixed_classes() {
        let secret = format!("private-label-value-{}", "y".repeat(8_192));
        let label_text = format!("metric{{token={secret}}} 1\n");
        let label_error = parse_prometheus_text_with_exemplars(&label_text, 0)
            .expect_err("unquoted label should be rejected");
        let label_response = prometheus_import_parse_error_response(&label_error);
        assert_eq!(label_response.status, 400);
        assert_eq!(
            response_header(&label_response, WRITE_ERROR_CODE_HEADER),
            Some("prometheus_import_invalid_labels")
        );
        let label_body =
            String::from_utf8(label_response.body).expect("diagnostic should be UTF-8");
        assert!(label_body.contains("line 1, label 1"));
        assert!(label_body.contains("unquoted_label_value"));
        assert!(label_body.len() <= MAX_WRITE_REJECTION_MESSAGE_BYTES);
        assert!(!label_body.contains("private-label-value"));

        let exemplar_text = format!("metric 1 # {{trace_id=\"{secret}\"}} not-a-number-{secret}\n");
        let exemplar_error = parse_prometheus_text_with_exemplars(&exemplar_text, 0)
            .expect_err("invalid exemplar value should be rejected");
        let exemplar_response = prometheus_import_parse_error_response(&exemplar_error);
        assert_eq!(exemplar_response.status, 400);
        assert_eq!(
            response_header(&exemplar_response, WRITE_ERROR_CODE_HEADER),
            Some("prometheus_import_invalid_exemplar_value")
        );
        let exemplar_body =
            String::from_utf8(exemplar_response.body).expect("diagnostic should be UTF-8");
        assert!(exemplar_body.contains("line 1"));
        assert!(exemplar_body.contains("invalid_exemplar_value"));
        assert!(exemplar_body.len() <= MAX_WRITE_REJECTION_MESSAGE_BYTES);
        assert!(!exemplar_body.contains("private-label-value"));
    }

    #[tokio::test]
    async fn prometheus_import_http_response_does_not_reflect_sensitive_payload_text() {
        let storage: Arc<dyn Storage> =
            StorageBuilder::new().build().expect("storage should build");
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        let write_admission =
            WriteAdmissionController::new(admission::WriteAdmissionGuardrails::default())
                .expect("write admission should build");
        let secret = format!("http-sensitive-token-{}", "z".repeat(8_192));
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/import/prometheus".to_string(),
            headers: HashMap::from([("content-type".to_string(), "text/plain".to_string())]),
            body: format!("metric{{credential=\"{secret}\"}} {secret}\n").into_bytes(),
        };

        let response = handle_prometheus_import_with_admission(
            &storage,
            &exemplar_store,
            &request,
            TimestampPrecision::Milliseconds,
            None,
            None,
            None,
            None,
            None,
            &write_admission,
        )
        .await;

        assert_eq!(response.status, 400);
        assert_eq!(
            response_header(&response, WRITE_ERROR_CODE_HEADER),
            Some("prometheus_import_invalid_value")
        );
        let body = std::str::from_utf8(&response.body).expect("diagnostic should be UTF-8");
        assert!(body.contains("line 1"));
        assert!(body.contains("invalid_sample_value"));
        assert!(body.len() <= MAX_WRITE_REJECTION_MESSAGE_BYTES);
        assert!(!body.contains("http-sensitive-token"));
    }
}
