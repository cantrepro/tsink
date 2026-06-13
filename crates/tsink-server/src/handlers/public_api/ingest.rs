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
        Err(err) => return text_response(400, &err),
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
            if histogram_count > 0 && response.status == 409 {
                record_payload_rejected(PrometheusPayloadKind::Histogram, histogram_count);
            }
            if metadata_count > 0 && response.status >= 409 {
                record_payload_rejected(PrometheusPayloadKind::Metadata, metadata_count);
            }
            if exemplar_count > 0 && response.status >= 409 {
                record_payload_rejected(PrometheusPayloadKind::Exemplar, exemplar_count);
            }
            return response;
        }
    };

    if histogram_count > 0 {
        record_payload_accepted(PrometheusPayloadKind::Histogram, histogram_count);
    }
    if metadata_count > 0 {
        record_payload_accepted(PrometheusPayloadKind::Metadata, metadata_count);
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
    if apply_result.applied_metadata_updates > 0 || metadata_count > 0 {
        response = response.with_header(
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
            return HttpResponse::new(
                415,
                format!("unsupported content-type for /v1/metrics: {content_type}"),
            )
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
                return text_response(400, &err.to_string());
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
            OTLP_EXEMPLAR_REJECTED_TOTAL.fetch_add(exemplar_count as u64, Ordering::Relaxed);
            return response;
        }
    };

    record_otlp_request_accepted();
    record_otlp_points_accepted(&stats);
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
    response = response.with_header(
        "X-Tsink-OTLP-Data-Points-Accepted",
        total_points.to_string(),
    );
    if metadata_count > 0 || apply_result.applied_metadata_updates > 0 {
        response = response.with_header(
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
        Err(err) => return text_response(400, &err),
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
                Err(err) => return write_routing_error_response(err),
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
                    record_payload_rejected(PrometheusPayloadKind::Exemplar, exemplar_count);
                    return text_response(409, &err);
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
        if let Some(consistency) = row_stats
            .as_ref()
            .and_then(|stats| stats.consistency)
            .or_else(|| exemplar_stats.as_ref().and_then(|stats| stats.consistency))
        {
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
        return response;
    }

    if !rows.is_empty() {
        let edge_rows = rows.clone();
        let storage = Arc::clone(storage);
        let result = tokio::task::spawn_blocking(move || storage.insert_rows(&rows)).await;
        match result {
            Ok(Ok(())) => maybe_enqueue_edge_sync_rows(edge_sync_context, &edge_rows),
            Ok(Err(err)) => return storage_write_error_response("import", &err),
            Err(err) => return text_response(500, &format!("import task failed: {err}")),
        }
    }

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
        return HttpResponse::new(200, Vec::<u8>::new());
    }
    match exemplar_store.apply_writes(
        &exemplars
            .into_iter()
            .map(normalized_exemplar_to_store_write)
            .collect::<Vec<_>>(),
    ) {
        Ok(outcome) => {
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
            HttpResponse::new(200, Vec::<u8>::new())
                .with_header("X-Tsink-Exemplars-Accepted", outcome.accepted.to_string())
                .with_header("X-Tsink-Exemplars-Dropped", outcome.dropped.to_string())
        }
        Err(err) => {
            record_payload_rejected(PrometheusPayloadKind::Exemplar, exemplar_count);
            text_response(500, &format!("exemplar import failed: {err}"))
        }
    }
}

#[derive(Debug, Default)]
struct ParsedPrometheusImport {
    rows: Vec<Row>,
    exemplars: Vec<NormalizedExemplar>,
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
    parse_prometheus_text_with_exemplars(text, default_timestamp).map(|parsed| parsed.rows)
}

fn parse_prometheus_text_with_exemplars(
    text: &str,
    default_timestamp: i64,
) -> Result<ParsedPrometheusImport, String> {
    let mut parsed = ParsedPrometheusImport::default();

    let mut rows = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (sample_text, exemplar_text) = split_openmetrics_exemplar(line);

        let (metric_and_labels, rest) = if let Some(brace_start) = sample_text.find('{') {
            let brace_end = find_label_block_end(sample_text, brace_start)
                .ok_or_else(|| format!("unclosed brace in line: {sample_text}"))?;
            let metric_name = &sample_text[..brace_start];
            let labels_str = &sample_text[brace_start + 1..brace_end];
            let rest = sample_text[brace_end + 1..].trim();
            let labels = parse_prom_labels(labels_str)?;
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
        let value_str = value_parts
            .next()
            .ok_or_else(|| format!("missing value in line: {line}"))?;
        let value: f64 = value_str
            .parse()
            .map_err(|_| format!("invalid value '{value_str}' in line: {line}"))?;

        let timestamp = if let Some(ts_str) = value_parts.next() {
            ts_str
                .parse::<i64>()
                .map_err(|_| format!("invalid timestamp '{ts_str}' in line: {line}"))?
        } else {
            default_timestamp
        };

        rows.push(Row::with_labels(
            metric_name.clone(),
            labels.clone(),
            DataPoint::new(timestamp, value),
        ));
        if let Some(exemplar_text) = exemplar_text {
            parsed.exemplars.push(parse_openmetrics_exemplar(
                exemplar_text,
                &metric_name,
                &labels,
                timestamp,
            )?);
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
) -> Result<NormalizedExemplar, String> {
    if !text.starts_with('{') {
        return Err(format!("invalid exemplar fragment: {text}"));
    }
    let brace_end = find_label_block_end(text, 0)
        .ok_or_else(|| format!("invalid exemplar fragment: {text}"))?;
    let exemplar_labels = parse_prom_labels(&text[1..brace_end])?;
    let rest = text[brace_end + 1..].trim();
    let mut parts = rest.split_whitespace();
    let value_str = parts
        .next()
        .ok_or_else(|| format!("missing exemplar value in '{text}'"))?;
    let value = value_str
        .parse::<f64>()
        .map_err(|_| format!("invalid exemplar value '{value_str}' in '{text}'"))?;
    let timestamp = match parts.next() {
        Some(value) => value
            .parse::<i64>()
            .map_err(|_| format!("invalid exemplar timestamp '{value}' in '{text}'"))?,
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

pub(crate) fn parse_prom_labels(labels_str: &str) -> Result<Vec<Label>, String> {
    let mut labels = Vec::new();
    let mut chars = labels_str.chars().peekable();

    loop {
        while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
            chars.next();
        }

        if chars.peek().is_none() {
            break;
        }

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
            return Err(format!("invalid label in '{labels_str}'"));
        }

        while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
            chars.next();
        }

        if chars.next() != Some('=') {
            return Err(format!("invalid label pair for '{name}' in '{labels_str}'"));
        }

        while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
            chars.next();
        }

        if chars.next() != Some('"') {
            return Err(format!(
                "label '{name}' value must be quoted in '{labels_str}'"
            ));
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
                    None => return Err(format!("unterminated escape sequence in '{labels_str}'")),
                },
                Some(ch) => value.push(ch),
                None => return Err(format!("unterminated label value in '{labels_str}'")),
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
            Some(_) => return Err(format!("expected ',' separator in '{labels_str}'")),
            None => break,
        }
    }

    Ok(labels)
}
