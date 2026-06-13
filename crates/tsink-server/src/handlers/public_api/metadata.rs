use super::super::*;

pub(crate) async fn handle_series(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("read admission unavailable: {err}")),
    };
    handle_series_with_admission(
        storage,
        request,
        cluster_context,
        tenant_registry,
        usage_accounting,
        read_admission,
    )
    .await
}

pub(crate) async fn handle_series_with_admission(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    usage_accounting: Option<&UsageAccounting>,
    read_admission: &ReadAdmissionController,
) -> HttpResponse {
    let started = Instant::now();
    let tenant_id = match tenant_id_for_promql_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        tenant_registry,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let matchers = request.param_all("match[]");
    let matcher_count = matchers.len();
    if matchers.is_empty() {
        return promql_error_response("bad_data", "missing required parameter 'match[]'");
    }
    if let Err(err) = tenant::enforce_metadata_matchers_quota(tenant_plan.policy(), matchers.len())
    {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Metadata,
            matcher_count.max(1),
            err.clone(),
        );
        return promql_error_response("bad_data", &err);
    }
    let _tenant_request = match tenant_plan.admit(
        tenant::TenantAdmissionSurface::Metadata,
        matcher_count.max(1),
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(matchers.len()).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Metadata,
                matcher_count.max(1),
                err.to_string(),
            );
            return read_admission_error_response(err);
        }
    };

    if let Some(cluster_context) =
        cluster_context.filter(|context| !context.runtime.local_reads_serve_global_queries)
    {
        let ring_version = cluster_ring_version(Some(cluster_context));
        let read_fanout = match effective_read_fanout(cluster_context) {
            Ok(fanout) => fanout,
            Err(err) => {
                return promql_error_response(
                    "execution",
                    &format!("cluster read fanout topology unavailable: {err}"),
                )
            }
        };
        let read_fanout = match apply_request_read_policies(
            request,
            cluster_context,
            read_fanout,
            tenant_plan.policy(),
        ) {
            Ok(fanout) => fanout,
            Err(err) => return promql_error_response("bad_data", &err),
        };
        let mut read_metadata = default_read_response_metadata(&read_fanout);
        let mut all_series = Vec::new();
        let mut seen = BTreeSet::<(String, Vec<(String, String)>)>::new();
        for match_str in &matchers {
            let expr = match tsink::promql::parse(match_str) {
                Ok(expr) => expr,
                Err(err) => return promql_error_response("bad_data", &err.to_string()),
            };
            let selection = match expr_to_selection(&expr) {
                Some(selection) => selection,
                None => {
                    return promql_error_response(
                        "bad_data",
                        &format!("match expression must be a vector selector: '{match_str}'"),
                    )
                }
            };
            let selection = match tenant::fanout_selection_for_tenant(&selection, &tenant_id) {
                Ok(selection) => selection,
                Err(err) => return promql_error_response("bad_data", &err),
            };

            let series_response = match read_fanout
                .select_series_with_ring_version_detailed(
                    storage,
                    &cluster_context.rpc_client,
                    &selection,
                    ring_version,
                )
                .await
            {
                Ok(response) => response,
                Err(err) => return fanout_error_response(err),
            };
            merge_read_response_metadata(&mut read_metadata, &series_response.metadata);
            for s in series_response
                .value
                .into_iter()
                .filter_map(|series| tenant::visible_metric_series(series, &tenant_id))
            {
                let mut labels: Vec<(String, String)> = s
                    .labels
                    .into_iter()
                    .map(|label| (label.name, label.value))
                    .collect();
                labels.sort();
                if !seen.insert((s.name.clone(), labels.clone())) {
                    continue;
                }
                let mut labels_map = serde_json::Map::new();
                labels_map.insert("__name__".to_string(), JsonValue::String(s.name));
                for (name, value) in labels {
                    labels_map.insert(name, JsonValue::String(value));
                }
                all_series.push(JsonValue::Object(labels_map));
            }
        }
        let result_units = all_series.len();
        record_query_pressure(&tenant_id, matcher_count, result_units);
        record_query_usage(
            usage_accounting,
            &tenant_id,
            "series",
            request.path_without_query(),
            QueryUsageMetrics::new(
                matcher_count.max(1) as u64,
                result_units as u64,
                elapsed_nanos_since(started),
                request.body.len() as u64,
            ),
        );
        return cluster_json_success_response(JsonValue::Array(all_series), &read_metadata);
    }

    let storage = tenant::scoped_storage(Arc::clone(storage), tenant_id.clone());
    let result = tokio::task::spawn_blocking(move || {
        let mut all_series = Vec::new();
        let mut seen = BTreeSet::<(String, Vec<(String, String)>)>::new();
        for match_str in &matchers {
            let expr = match tsink::promql::parse(match_str) {
                Ok(expr) => expr,
                Err(err) => return Err(format!("invalid match expression '{match_str}': {err}")),
            };
            let selection = match expr_to_selection(&expr) {
                Some(s) => s,
                None => {
                    return Err(format!(
                        "match expression must be a vector selector: '{match_str}'"
                    ))
                }
            };
            let series = storage
                .select_series(&selection)
                .map_err(|err| format!("series selection failed: {err}"))?;
            for s in series {
                let mut labels: Vec<(String, String)> = s
                    .labels
                    .into_iter()
                    .map(|label| (label.name, label.value))
                    .collect();
                labels.sort();
                if !seen.insert((s.name.clone(), labels.clone())) {
                    continue;
                }
                let mut labels_map = serde_json::Map::new();
                labels_map.insert("__name__".to_string(), JsonValue::String(s.name));
                for (name, value) in labels {
                    labels_map.insert(name, JsonValue::String(value));
                }
                all_series.push(JsonValue::Object(labels_map));
            }
        }
        Ok(all_series)
    })
    .await;

    match result {
        Ok(Ok(data)) => {
            let result_units = data.len();
            record_query_pressure(&tenant_id, matcher_count, result_units);
            record_query_usage(
                usage_accounting,
                &tenant_id,
                "series",
                request.path_without_query(),
                QueryUsageMetrics::new(
                    matcher_count.max(1) as u64,
                    result_units as u64,
                    elapsed_nanos_since(started),
                    request.body.len() as u64,
                ),
            );
            json_response(200, &json!({"status": "success", "data": data}))
        }
        Ok(Err(err)) => promql_error_response("bad_data", &err),
        Err(err) => promql_error_response("execution", &format!("task failed: {err}")),
    }
}

pub(crate) async fn handle_labels(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("read admission unavailable: {err}")),
    };
    handle_labels_with_admission(
        storage,
        request,
        cluster_context,
        tenant_registry,
        usage_accounting,
        read_admission,
    )
    .await
}

pub(crate) async fn handle_labels_with_admission(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    usage_accounting: Option<&UsageAccounting>,
    read_admission: &ReadAdmissionController,
) -> HttpResponse {
    let started = Instant::now();
    let tenant_id = match tenant_id_for_promql_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        tenant_registry,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let _tenant_request = match tenant_plan.admit(tenant::TenantAdmissionSurface::Metadata, 1) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(1).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Metadata,
                1,
                err.to_string(),
            );
            return read_admission_error_response(err);
        }
    };
    if let Some(cluster_context) =
        cluster_context.filter(|context| !context.runtime.local_reads_serve_global_queries)
    {
        let ring_version = cluster_ring_version(Some(cluster_context));
        let read_fanout = match effective_read_fanout(cluster_context) {
            Ok(fanout) => fanout,
            Err(err) => {
                return promql_error_response(
                    "execution",
                    &format!("cluster read fanout topology unavailable: {err}"),
                )
            }
        };
        let read_fanout = match apply_request_read_policies(
            request,
            cluster_context,
            read_fanout,
            tenant_plan.policy(),
        ) {
            Ok(fanout) => fanout,
            Err(err) => return promql_error_response("bad_data", &err),
        };
        let selection =
            match tenant::fanout_selection_for_tenant(&SeriesSelection::new(), &tenant_id) {
                Ok(selection) => selection,
                Err(err) => return promql_error_response("bad_data", &err),
            };
        let series_response = match read_fanout
            .select_series_with_ring_version_detailed(
                storage,
                &cluster_context.rpc_client,
                &selection,
                ring_version,
            )
            .await
        {
            Ok(series) => series,
            Err(err) => return fanout_error_response(err),
        };
        let metadata = series_response.metadata.clone();
        let mut names = BTreeSet::new();
        names.insert("__name__".to_string());
        for series in series_response
            .value
            .into_iter()
            .filter_map(|series| tenant::visible_metric_series(series, &tenant_id))
        {
            for label in &series.labels {
                names.insert(label.name.clone());
            }
        }
        let result_units = names.len();
        record_query_pressure(&tenant_id, 1, result_units);
        record_query_usage(
            usage_accounting,
            &tenant_id,
            "labels",
            request.path_without_query(),
            QueryUsageMetrics::new(
                1,
                result_units as u64,
                elapsed_nanos_since(started),
                request.body.len() as u64,
            ),
        );
        return cluster_json_success_response(
            json!(names.into_iter().collect::<Vec<_>>()),
            &metadata,
        );
    }

    let storage = tenant::scoped_storage(Arc::clone(storage), tenant_id.clone());
    let result = tokio::task::spawn_blocking(move || {
        let metrics = storage.list_metrics()?;
        let mut names = BTreeSet::new();
        names.insert("__name__".to_string());
        for series in &metrics {
            for label in &series.labels {
                names.insert(label.name.clone());
            }
        }
        Ok::<_, tsink::TsinkError>(names.into_iter().collect::<Vec<_>>())
    })
    .await;

    match result {
        Ok(Ok(data)) => {
            let result_units = data.len();
            record_query_pressure(&tenant_id, 1, result_units);
            record_query_usage(
                usage_accounting,
                &tenant_id,
                "labels",
                request.path_without_query(),
                QueryUsageMetrics::new(
                    1,
                    result_units as u64,
                    elapsed_nanos_since(started),
                    request.body.len() as u64,
                ),
            );
            json_response(200, &json!({"status": "success", "data": data}))
        }
        Ok(Err(err)) => promql_error_response("execution", &format!("label query failed: {err}")),
        Err(err) => promql_error_response("execution", &format!("task failed: {err}")),
    }
}

pub(crate) async fn handle_metadata(
    metadata_store: &Arc<MetricMetadataStore>,
    request: &HttpRequest,
    tenant_registry: Option<&tenant::TenantRegistry>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("read admission unavailable: {err}")),
    };
    handle_metadata_with_admission(
        metadata_store,
        request,
        tenant_registry,
        usage_accounting,
        read_admission,
    )
    .await
}

pub(crate) async fn handle_metadata_with_admission(
    metadata_store: &Arc<MetricMetadataStore>,
    request: &HttpRequest,
    tenant_registry: Option<&tenant::TenantRegistry>,
    usage_accounting: Option<&UsageAccounting>,
    read_admission: &ReadAdmissionController,
) -> HttpResponse {
    let started = Instant::now();
    let tenant_id = match tenant_id_for_promql_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        tenant_registry,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };

    let metric = match request.param("metric") {
        Some(metric) => match non_empty_param(Some(metric)) {
            Some(metric) => Some(metric),
            None => {
                return promql_error_response("bad_data", "parameter 'metric' must not be empty")
            }
        },
        None => None,
    };
    let limit = match metadata_query_limit(request) {
        Ok(limit) => limit,
        Err(response) => return response,
    };
    let _tenant_request =
        match tenant_plan.admit(tenant::TenantAdmissionSurface::Metadata, limit.max(1)) {
            Ok(guard) => guard,
            Err(err) => return err.to_http_response(),
        };
    let _read_admission = match read_admission.admit_request(1).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Metadata,
                limit.max(1),
                err.to_string(),
            );
            return read_admission_error_response(err);
        }
    };
    let records = match metadata_store.query(&tenant_id, metric.as_deref(), limit) {
        Ok(records) => records,
        Err(err) => return promql_error_response("execution", &err),
    };
    let result_units = records.len();
    record_query_pressure(&tenant_id, 1, result_units);
    record_query_usage(
        usage_accounting,
        &tenant_id,
        "metadata",
        request.path_without_query(),
        QueryUsageMetrics::new(
            1,
            result_units as u64,
            elapsed_nanos_since(started),
            request.body.len() as u64,
        ),
    );

    json_response(
        200,
        &json!({
            "status": "success",
            "data": metadata_response_payload(records),
        }),
    )
}

pub(crate) async fn handle_label_values(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    label_name: &str,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("read admission unavailable: {err}")),
    };
    handle_label_values_with_admission(
        storage,
        request,
        label_name,
        cluster_context,
        tenant_registry,
        usage_accounting,
        read_admission,
    )
    .await
}

pub(crate) async fn handle_label_values_with_admission(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    label_name: &str,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    usage_accounting: Option<&UsageAccounting>,
    read_admission: &ReadAdmissionController,
) -> HttpResponse {
    let started = Instant::now();
    let tenant_id = match tenant_id_for_promql_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        tenant_registry,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let _tenant_request = match tenant_plan.admit(tenant::TenantAdmissionSurface::Metadata, 1) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(1).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Metadata,
                1,
                err.to_string(),
            );
            return read_admission_error_response(err);
        }
    };
    if let Some(cluster_context) =
        cluster_context.filter(|context| !context.runtime.local_reads_serve_global_queries)
    {
        let ring_version = cluster_ring_version(Some(cluster_context));
        let read_fanout = match effective_read_fanout(cluster_context) {
            Ok(fanout) => fanout,
            Err(err) => {
                return promql_error_response(
                    "execution",
                    &format!("cluster read fanout topology unavailable: {err}"),
                )
            }
        };
        let read_fanout = match apply_request_read_policies(
            request,
            cluster_context,
            read_fanout,
            tenant_plan.policy(),
        ) {
            Ok(fanout) => fanout,
            Err(err) => return promql_error_response("bad_data", &err),
        };
        let selection =
            match tenant::fanout_selection_for_tenant(&SeriesSelection::new(), &tenant_id) {
                Ok(selection) => selection,
                Err(err) => return promql_error_response("bad_data", &err),
            };
        let series_response = match read_fanout
            .select_series_with_ring_version_detailed(
                storage,
                &cluster_context.rpc_client,
                &selection,
                ring_version,
            )
            .await
        {
            Ok(series) => series,
            Err(err) => return fanout_error_response(err),
        };
        let metadata = series_response.metadata.clone();
        let mut values = BTreeSet::new();
        for series in series_response
            .value
            .into_iter()
            .filter_map(|series| tenant::visible_metric_series(series, &tenant_id))
        {
            if label_name == "__name__" {
                values.insert(series.name.clone());
            } else {
                for label in &series.labels {
                    if label.name == label_name {
                        values.insert(label.value.clone());
                    }
                }
            }
        }
        let result_units = values.len();
        record_query_pressure(&tenant_id, 1, result_units);
        record_query_usage(
            usage_accounting,
            &tenant_id,
            "label_values",
            request.path_without_query(),
            QueryUsageMetrics::new(
                1,
                result_units as u64,
                elapsed_nanos_since(started),
                request.body.len() as u64,
            ),
        );
        return cluster_json_success_response(
            json!(values.into_iter().collect::<Vec<_>>()),
            &metadata,
        );
    }
    let label_name = label_name.to_string();
    let storage = tenant::scoped_storage(Arc::clone(storage), tenant_id.clone());
    let result = tokio::task::spawn_blocking(move || {
        let metrics = storage.list_metrics()?;
        let mut values = BTreeSet::new();
        for series in &metrics {
            if label_name == "__name__" {
                values.insert(series.name.clone());
            } else {
                for label in &series.labels {
                    if label.name == label_name {
                        values.insert(label.value.clone());
                    }
                }
            }
        }
        Ok::<_, tsink::TsinkError>(values.into_iter().collect::<Vec<_>>())
    })
    .await;

    match result {
        Ok(Ok(data)) => {
            let result_units = data.len();
            record_query_pressure(&tenant_id, 1, result_units);
            record_query_usage(
                usage_accounting,
                &tenant_id,
                "label_values",
                request.path_without_query(),
                QueryUsageMetrics::new(
                    1,
                    result_units as u64,
                    elapsed_nanos_since(started),
                    request.body.len() as u64,
                ),
            );
            json_response(200, &json!({"status": "success", "data": data}))
        }
        Ok(Err(err)) => {
            promql_error_response("execution", &format!("label values query failed: {err}"))
        }
        Err(err) => promql_error_response("execution", &format!("task failed: {err}")),
    }
}
