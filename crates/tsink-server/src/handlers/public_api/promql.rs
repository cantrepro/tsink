use super::super::*;

pub(crate) async fn handle_instant_query(
    storage: &Arc<dyn Storage>,
    _engine: &Engine,
    request: &HttpRequest,
    precision: TimestampPrecision,
    context: PublicReadContext<'_>,
) -> HttpResponse {
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("read admission unavailable: {err}")),
    };
    handle_instant_query_with_admission(storage, request, precision, context, read_admission).await
}

pub(crate) async fn handle_instant_query_with_admission(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    context: PublicReadContext<'_>,
    read_admission: &ReadAdmissionController,
) -> HttpResponse {
    let started = Instant::now();
    let tenant_id = match tenant_id_for_promql_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        context.tenant_registry,
        context.managed_control_plane,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let Some(query) = request.param("query") else {
        return promql_error_response("bad_data", "missing required parameter 'query'");
    };
    if let Err(err) = tenant::enforce_query_length_quota(tenant_plan.policy(), &query) {
        tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, err.clone());
        return promql_error_response("bad_data", &err);
    }
    let _tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Query,
        1,
        context.usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(1).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(tenant::TenantAdmissionSurface::Query, 1, err.to_string());
            return read_admission_error_response(err);
        }
    };

    let time = match request.param("time") {
        Some(t) => match parse_timestamp(&t, precision) {
            Ok(ts) => ts,
            Err(err) => return promql_error_response("bad_data", &err),
        },
        None => current_timestamp(precision),
    };

    let (storage, distributed_storage) = match storage_for_promql_request(
        storage,
        request,
        context.cluster_context,
        &tenant_id,
        tenant_plan.policy(),
    ) {
        Ok(result) => result,
        Err(response) => return response,
    };
    let engine = Engine::with_precision(Arc::clone(&storage), precision);
    let result = tokio::task::spawn_blocking(move || engine.instant_query(&query, time)).await;

    match result {
        Ok(Ok(value)) => {
            let result_units = promql_value_units(&value);
            record_query_pressure(&tenant_id, 1, result_units);
            record_query_usage(
                context.usage_accounting,
                &tenant_id,
                "instant_query",
                request.path_without_query(),
                QueryUsageMetrics::new(
                    1,
                    result_units as u64,
                    elapsed_nanos_since(started),
                    request.body.len() as u64,
                ),
            );
            let mut response = promql_success_response(&value, precision);
            if let Some(distributed_storage) = distributed_storage.as_ref() {
                let metadata = distributed_storage.read_metadata_snapshot();
                response = with_read_metadata_headers(response, &metadata);
            }
            response
        }
        Ok(Err(err)) => promql_error_response("execution", &err.to_string()),
        Err(err) => promql_error_response("execution", &format!("query task failed: {err}")),
    }
}

pub(crate) async fn handle_range_query(
    storage: &Arc<dyn Storage>,
    _engine: &Engine,
    request: &HttpRequest,
    precision: TimestampPrecision,
    context: PublicReadContext<'_>,
) -> HttpResponse {
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("read admission unavailable: {err}")),
    };
    handle_range_query_with_admission(storage, request, precision, context, read_admission).await
}

pub(crate) async fn handle_range_query_with_admission(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    context: PublicReadContext<'_>,
    read_admission: &ReadAdmissionController,
) -> HttpResponse {
    let started = Instant::now();
    let tenant_id = match tenant_id_for_promql_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        context.tenant_registry,
        context.managed_control_plane,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let Some(query) = request.param("query") else {
        return promql_error_response("bad_data", "missing required parameter 'query'");
    };
    if let Err(err) = tenant::enforce_query_length_quota(tenant_plan.policy(), &query) {
        tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, err.clone());
        return promql_error_response("bad_data", &err);
    }
    let Some(start_str) = request.param("start") else {
        return promql_error_response("bad_data", "missing required parameter 'start'");
    };
    let Some(end_str) = request.param("end") else {
        return promql_error_response("bad_data", "missing required parameter 'end'");
    };
    let Some(step_str) = request.param("step") else {
        return promql_error_response("bad_data", "missing required parameter 'step'");
    };

    let start = match parse_timestamp(&start_str, precision) {
        Ok(ts) => ts,
        Err(err) => return promql_error_response("bad_data", &err),
    };
    let end = match parse_timestamp(&end_str, precision) {
        Ok(ts) => ts,
        Err(err) => return promql_error_response("bad_data", &err),
    };
    if end < start {
        return promql_error_response("bad_data", "end timestamp must not be before start time");
    }
    let step = match parse_step(&step_str, precision) {
        Ok(s) => s,
        Err(err) => return promql_error_response("bad_data", &err),
    };
    if let Err(err) = tenant::enforce_range_points_quota(tenant_plan.policy(), start, end, step) {
        tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, err.clone());
        return promql_error_response("bad_data", &err);
    }
    let _tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Query,
        1,
        context.usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(1).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(tenant::TenantAdmissionSurface::Query, 1, err.to_string());
            return read_admission_error_response(err);
        }
    };

    let (storage, distributed_storage) = match storage_for_promql_request(
        storage,
        request,
        context.cluster_context,
        &tenant_id,
        tenant_plan.policy(),
    ) {
        Ok(result) => result,
        Err(response) => return response,
    };
    let engine = Engine::with_precision(Arc::clone(&storage), precision);
    let result =
        tokio::task::spawn_blocking(move || engine.range_query(&query, start, end, step)).await;

    match result {
        Ok(Ok(value)) => {
            let result_units = promql_value_units(&value);
            record_query_pressure(&tenant_id, 1, result_units);
            record_query_usage(
                context.usage_accounting,
                &tenant_id,
                "range_query",
                request.path_without_query(),
                QueryUsageMetrics::new(
                    1,
                    result_units as u64,
                    elapsed_nanos_since(started),
                    request.body.len() as u64,
                ),
            );
            let mut response = promql_success_response(&value, precision);
            if let Some(distributed_storage) = distributed_storage.as_ref() {
                let metadata = distributed_storage.read_metadata_snapshot();
                response = with_read_metadata_headers(response, &metadata);
            }
            response
        }
        Ok(Err(err)) => promql_error_response("execution", &err.to_string()),
        Err(err) => promql_error_response("execution", &format!("query task failed: {err}")),
    }
}

fn collect_exemplar_selections(expr: &Expr, selections: &mut Vec<SeriesSelection>) {
    match expr {
        Expr::VectorSelector(_) => {
            if let Some(selection) = expr_to_selection(expr) {
                selections.push(selection);
            }
        }
        Expr::MatrixSelector(selector) => {
            let vector = Expr::VectorSelector(selector.vector.clone());
            if let Some(selection) = expr_to_selection(&vector) {
                selections.push(selection);
            }
        }
        Expr::Subquery(inner) => collect_exemplar_selections(&inner.expr, selections),
        Expr::Unary(inner) => collect_exemplar_selections(&inner.expr, selections),
        Expr::Binary(inner) => {
            collect_exemplar_selections(&inner.lhs, selections);
            collect_exemplar_selections(&inner.rhs, selections);
        }
        Expr::Aggregation(inner) => {
            collect_exemplar_selections(&inner.expr, selections);
            if let Some(param) = inner.param.as_ref() {
                collect_exemplar_selections(param, selections);
            }
        }
        Expr::Call(inner) => {
            for arg in &inner.args {
                collect_exemplar_selections(arg, selections);
            }
        }
        Expr::Paren(inner) => collect_exemplar_selections(inner, selections),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
    }
}

fn dedupe_and_limit_exemplar_series(
    series: Vec<ExemplarSeries>,
    limit: usize,
) -> Vec<ExemplarSeries> {
    let mut merged = BTreeMap::<
        String,
        (
            String,
            Vec<Label>,
            BTreeMap<i64, crate::exemplar_store::ExemplarSample>,
        ),
    >::new();
    for item in series {
        let key = metric_series_identity_key(&item.metric, &item.labels);
        let entry = merged
            .entry(key)
            .or_insert_with(|| (item.metric.clone(), item.labels.clone(), BTreeMap::new()));
        for exemplar in item.exemplars {
            entry.2.entry(exemplar.timestamp).or_insert(exemplar);
        }
    }

    let mut out = Vec::new();
    let mut remaining = limit.max(1);
    for (_, (metric, labels, exemplars)) in merged {
        if remaining == 0 {
            break;
        }
        let series_exemplars = exemplars.into_values().take(remaining).collect::<Vec<_>>();
        if series_exemplars.is_empty() {
            continue;
        }
        remaining = remaining.saturating_sub(series_exemplars.len());
        out.push(ExemplarSeries {
            metric,
            labels,
            exemplars: series_exemplars,
        });
    }
    out
}

fn exemplar_series_json(
    series: ExemplarSeries,
    tenant_id: &str,
    precision: TimestampPrecision,
) -> Option<JsonValue> {
    let visible = tenant::visible_metric_series(
        MetricSeries {
            name: series.metric,
            labels: series.labels,
        },
        tenant_id,
    )?;
    let mut series_labels = serde_json::Map::new();
    series_labels.insert("__name__".to_string(), JsonValue::String(visible.name));
    for label in visible.labels {
        series_labels.insert(label.name, JsonValue::String(label.value));
    }

    Some(json!({
        "seriesLabels": series_labels,
        "exemplars": series.exemplars.into_iter().map(|exemplar| {
            let mut labels = serde_json::Map::new();
            for label in exemplar.labels {
                labels.insert(label.name, JsonValue::String(label.value));
            }
            json!({
                "labels": labels,
                "value": format_value(exemplar.value),
                "timestamp": timestamp_to_f64(exemplar.timestamp, precision),
            })
        }).collect::<Vec<_>>(),
    }))
}

async fn query_exemplars_across_cluster(
    exemplar_store: &Arc<ExemplarStore>,
    cluster_context: &ClusterRequestContext,
    selectors: &[SeriesSelection],
    start: i64,
    end: i64,
    limit: usize,
) -> Result<Vec<ExemplarSeries>, String> {
    let (membership, _) = effective_write_topology(cluster_context)?;
    let ring_version = cluster_ring_version(Some(cluster_context));
    let local_node_id = membership.local_node_id.clone();
    let mut merged = exemplar_store.query(selectors, start, end, limit)?;

    for node in membership.nodes {
        if node.id == local_node_id {
            continue;
        }
        let response = cluster_context
            .rpc_client
            .query_exemplars(
                node.endpoint.as_str(),
                &InternalQueryExemplarsRequest {
                    ring_version,
                    selectors: selectors.to_vec(),
                    start,
                    end,
                    limit,
                },
            )
            .await
            .map_err(|err| {
                format!(
                    "remote exemplar query failed for node '{}' ({}): {err}",
                    node.id, node.endpoint
                )
            })?;
        merged.extend(response.series.into_iter().map(|series| {
            ExemplarSeries {
                metric: series.metric,
                labels: series.labels,
                exemplars: series
                    .exemplars
                    .into_iter()
                    .map(|exemplar| crate::exemplar_store::ExemplarSample {
                        labels: exemplar.labels,
                        value: exemplar.value,
                        timestamp: exemplar.timestamp,
                    })
                    .collect(),
            }
        }));
    }

    Ok(dedupe_and_limit_exemplar_series(merged, limit))
}

pub(crate) async fn handle_query_exemplars(
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    context: PublicReadContext<'_>,
) -> HttpResponse {
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("read admission unavailable: {err}")),
    };
    handle_query_exemplars_with_admission(
        exemplar_store,
        request,
        precision,
        context,
        read_admission,
    )
    .await
}

pub(crate) async fn handle_query_exemplars_with_admission(
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    context: PublicReadContext<'_>,
    read_admission: &ReadAdmissionController,
) -> HttpResponse {
    let started = Instant::now();
    let tenant_id = match tenant_id_for_promql_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        context.tenant_registry,
        context.managed_control_plane,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let Some(query) = request.param("query") else {
        return promql_error_response("bad_data", "missing required parameter 'query'");
    };
    if let Err(err) = tenant::enforce_query_length_quota(tenant_plan.policy(), &query) {
        tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, err.clone());
        return promql_error_response("bad_data", &err);
    }
    let Some(start_str) = request.param("start") else {
        return promql_error_response("bad_data", "missing required parameter 'start'");
    };
    let Some(end_str) = request.param("end") else {
        return promql_error_response("bad_data", "missing required parameter 'end'");
    };
    let start = match parse_timestamp(&start_str, precision) {
        Ok(ts) => ts,
        Err(err) => return promql_error_response("bad_data", &format!("invalid 'start': {err}")),
    };
    let end = match parse_timestamp(&end_str, precision) {
        Ok(ts) => ts,
        Err(err) => return promql_error_response("bad_data", &format!("invalid 'end': {err}")),
    };
    if end < start {
        return promql_error_response("bad_data", "end timestamp must not be before start time");
    }
    let limit = match request.param("limit") {
        Some(raw) => match raw.parse::<usize>() {
            Ok(limit) if limit > 0 && limit <= exemplar_store.config().max_query_results => limit,
            Ok(limit) if limit > exemplar_store.config().max_query_results => {
                return promql_error_response(
                    "bad_data",
                    &format!(
                        "parameter 'limit' exceeds maximum {}: {limit}",
                        exemplar_store.config().max_query_results
                    ),
                )
            }
            _ => {
                return promql_error_response(
                    "bad_data",
                    "parameter 'limit' must be a positive integer",
                )
            }
        },
        None => exemplar_store.config().max_query_results,
    };

    let expr = match tsink::promql::parse(&query) {
        Ok(expr) => expr,
        Err(err) => return promql_error_response("bad_data", &err.to_string()),
    };
    let mut selections = Vec::new();
    collect_exemplar_selections(&expr, &mut selections);
    if selections.is_empty() {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Query,
            1,
            "query_exemplars requires at least one vector or matrix selector",
        );
        return promql_error_response(
            "bad_data",
            "query_exemplars requires at least one vector or matrix selector",
        );
    }
    if selections.len() > exemplar_store.config().max_query_selectors {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Query,
            selections.len(),
            format!(
                "query_exemplars selector limit exceeded: {} > {}",
                selections.len(),
                exemplar_store.config().max_query_selectors
            ),
        );
        return promql_error_response(
            "bad_data",
            &format!(
                "query_exemplars selector limit exceeded: {} > {}",
                selections.len(),
                exemplar_store.config().max_query_selectors
            ),
        );
    }
    let scoped = match selections
        .into_iter()
        .map(|selection| tenant::selection_for_tenant(&selection, &tenant_id))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(scoped) => scoped,
        Err(err) => return promql_error_response("bad_data", &err),
    };

    let _tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Query,
        scoped.len().max(1),
        context.usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(scoped.len()).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Query,
                scoped.len().max(1),
                err.to_string(),
            );
            return read_admission_error_response(err);
        }
    };

    let series = if let Some(cluster_context) = context.cluster_context {
        match query_exemplars_across_cluster(
            exemplar_store,
            cluster_context,
            &scoped,
            start,
            end,
            limit,
        )
        .await
        {
            Ok(series) => series,
            Err(err) => return promql_error_response("execution", &err),
        }
    } else {
        match exemplar_store.query(&scoped, start, end, limit) {
            Ok(series) => dedupe_and_limit_exemplar_series(series, limit),
            Err(err) => return promql_error_response("execution", &err),
        }
    };
    let result_units = exemplar_query_units(&series);
    record_query_pressure(&tenant_id, scoped.len(), result_units);
    record_query_usage(
        context.usage_accounting,
        &tenant_id,
        "query_exemplars",
        request.path_without_query(),
        QueryUsageMetrics::new(
            scoped.len().max(1) as u64,
            result_units as u64,
            elapsed_nanos_since(started),
            request.body.len() as u64,
        ),
    );

    let data = JsonValue::Array(
        series
            .into_iter()
            .filter_map(|series| exemplar_series_json(series, &tenant_id, precision))
            .collect(),
    );

    if let Some(cluster_context) = context.cluster_context {
        let metadata = ReadFanoutResponseMetadata {
            consistency: cluster_context.runtime.read_consistency,
            partial_response_policy: cluster_context.runtime.read_partial_response,
            partial_response: false,
            warnings: Vec::new(),
        };
        return cluster_json_success_response(data, &metadata)
            .with_header("X-Tsink-Exemplar-Limit", limit.to_string());
    }

    json_response(200, &json!({"status": "success", "data": data}))
        .with_header("X-Tsink-Exemplar-Limit", limit.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exemplar_store::{ExemplarSample, ExemplarSeries};
    use tsink::Label;

    type LabelPair<'a> = (&'a str, &'a str);
    type ExemplarInput<'a> = (i64, f64, &'a [LabelPair<'a>]);

    fn exemplar_series(
        metric: &str,
        labels: &[LabelPair<'_>],
        exemplars: &[ExemplarInput<'_>],
    ) -> ExemplarSeries {
        ExemplarSeries {
            metric: metric.to_string(),
            labels: labels
                .iter()
                .map(|(name, value)| Label::new(*name, *value))
                .collect(),
            exemplars: exemplars
                .iter()
                .map(|(timestamp, value, labels)| ExemplarSample {
                    labels: labels
                        .iter()
                        .map(|(name, label_value)| Label::new(*name, *label_value))
                        .collect(),
                    value: *value,
                    timestamp: *timestamp,
                })
                .collect(),
        }
    }

    #[test]
    fn dedupe_and_limit_exemplar_series_distinguishes_delimiter_collision_series() {
        let left = exemplar_series(
            "cpu",
            &[("job", "api,zone=west|prod\u{1f}blue")],
            &[(10, 1.0, &[("trace_id", "left")])],
        );
        let right = exemplar_series(
            "cpu",
            &[("job", "api"), ("zone", "west|prod\u{1f}blue")],
            &[(20, 2.0, &[("trace_id", "right")])],
        );

        let merged = dedupe_and_limit_exemplar_series(vec![left.clone(), right.clone()], 8);

        assert_eq!(merged.len(), 2);
        assert!(merged.iter().any(|series| series == &left));
        assert!(merged.iter().any(|series| series == &right));
    }

    #[test]
    fn dedupe_and_limit_exemplar_series_dedupes_reordered_labels() {
        let merged = dedupe_and_limit_exemplar_series(
            vec![
                exemplar_series(
                    "cpu",
                    &[("instance", "a"), ("job", "api")],
                    &[(10, 1.0, &[("trace_id", "left")])],
                ),
                exemplar_series(
                    "cpu",
                    &[("job", "api"), ("instance", "a")],
                    &[(20, 2.0, &[("trace_id", "right")])],
                ),
            ],
            8,
        );

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].metric, "cpu");
        assert_eq!(merged[0].exemplars.len(), 2);
        assert_eq!(merged[0].exemplars[0].timestamp, 10);
        assert_eq!(merged[0].exemplars[1].timestamp, 20);
    }
}
