use super::super::*;
use crate::cluster::query::ReadFanoutResponse;

async fn fanout_select_series_union(
    storage: &Arc<dyn Storage>,
    cluster_context: &ClusterRequestContext,
    read_fanout: &ReadFanoutExecutor,
    selections: &[SeriesSelection],
    ring_version: u64,
) -> Result<ReadFanoutResponse<Vec<MetricSeries>>, ReadFanoutError> {
    let mut metadata = default_read_response_metadata(read_fanout);
    let mut merged = BTreeSet::new();

    for selection in selections {
        let response = read_fanout
            .select_series_with_ring_version_detailed(
                storage,
                &cluster_context.rpc_client,
                selection,
                ring_version,
            )
            .await?;
        merge_read_response_metadata(&mut metadata, &response.metadata);
        merged.extend(response.value);
    }

    Ok(ReadFanoutResponse {
        value: merged.into_iter().collect(),
        metadata,
    })
}

pub(crate) async fn handle_remote_read(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("read admission unavailable: {err}")),
    };
    handle_remote_read_with_admission(
        storage,
        request,
        cluster_context,
        tenant_registry,
        managed_control_plane,
        usage_accounting,
        read_admission,
    )
    .await
}

pub(crate) async fn handle_remote_read_with_admission(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    usage_accounting: Option<&UsageAccounting>,
    read_admission: &ReadAdmissionController,
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
        tenant::TenantAccessScope::Read,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let decoded = match decode_body(request) {
        Ok(body) => body,
        Err(err) => return text_response(400, &err),
    };

    let read_req = match ReadRequest::decode(decoded.as_slice()) {
        Ok(req) => req,
        Err(err) => return text_response(400, &format!("invalid protobuf body: {err}")),
    };
    if let Err(err) = validate_remote_read_request(&read_req) {
        return text_response(400, &err);
    }
    let read_query_count = read_req.queries.len();
    if let Err(err) =
        tenant::enforce_read_queries_quota(tenant_plan.policy(), read_req.queries.len())
    {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Query,
            read_query_count.max(1),
            err.clone(),
        );
        return HttpResponse::new(413, err).with_header("Content-Type", "text/plain");
    }
    let _tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Query,
        read_query_count.max(1),
        usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(read_req.queries.len()).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Query,
                read_query_count.max(1),
                err.to_string(),
            );
            return read_admission_error_response(err);
        }
    };

    let mut distributed_metadata: Option<ReadFanoutResponseMetadata> = None;
    let results = if let Some(cluster_context) =
        cluster_context.filter(|context| !context.runtime.local_reads_serve_global_queries)
    {
        let read_fanout = match effective_read_fanout(cluster_context) {
            Ok(fanout) => fanout,
            Err(err) => {
                return text_response(
                    503,
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
            Err(err) => return text_response(400, &err),
        };
        let mut read_metadata = default_read_response_metadata(&read_fanout);
        let mut out = Vec::with_capacity(read_req.queries.len());
        for query in &read_req.queries {
            match execute_query_distributed(
                storage,
                cluster_context,
                &read_fanout,
                query,
                &tenant_id,
            )
            .await
            {
                Ok((result, metadata)) => {
                    merge_read_response_metadata(&mut read_metadata, &metadata);
                    out.push(result);
                }
                Err(err) => return fanout_error_response(err),
            }
        }
        distributed_metadata = Some(read_metadata);
        out
    } else {
        let storage = tenant::scoped_storage(Arc::clone(storage), tenant_id.clone());
        let result = tokio::task::spawn_blocking(move || {
            let mut results = Vec::with_capacity(read_req.queries.len());
            for query in &read_req.queries {
                results.push(execute_query(&storage, query)?);
            }
            Ok::<_, String>(results)
        })
        .await;

        match result {
            Ok(Ok(r)) => r,
            Ok(Err(err)) => return text_response(400, &err),
            Err(err) => return text_response(500, &format!("read task failed: {err}")),
        }
    };

    let response = ReadResponse { results };
    let result_units = remote_read_query_units(&response.results);
    record_query_pressure(&tenant_id, read_query_count, result_units);
    record_query_usage(
        usage_accounting,
        &tenant_id,
        "remote_read",
        request.path_without_query(),
        QueryUsageMetrics::new(
            read_query_count.max(1) as u64,
            result_units as u64,
            elapsed_nanos_since(started),
            request.body.len() as u64,
        ),
    );
    let mut raw = Vec::new();
    if let Err(err) = response.encode(&mut raw) {
        return text_response(500, &format!("failed to encode response: {err}"));
    }

    let compressed = match SnappyEncoder::new().compress_vec(&raw) {
        Ok(bytes) => bytes,
        Err(err) => return text_response(500, &format!("snappy encode failed: {err}")),
    };

    let mut response = HttpResponse::new(200, compressed)
        .with_header("Content-Type", "application/x-protobuf")
        .with_header("Content-Encoding", "snappy")
        .with_header("X-Prometheus-Remote-Read-Version", "0.1.0");
    if let Some(metadata) = distributed_metadata.as_ref() {
        response = with_read_metadata_headers(response, metadata);
    }
    response
}

fn validate_remote_read_request(read_req: &ReadRequest) -> Result<(), String> {
    for response_type in &read_req.accepted_response_types {
        let response_type = ReadResponseType::try_from(*response_type)
            .map_err(|_| format!("unsupported remote read response type: {response_type}"))?;
        if response_type != ReadResponseType::Samples {
            return Err(format!(
                "remote read response type {:?} is not supported yet",
                response_type
            ));
        }
    }

    Ok(())
}

fn execute_query(storage: &Arc<dyn Storage>, query: &Query) -> Result<QueryResult, String> {
    let selection = series_selection_from_remote_matchers(&query.matchers)?;
    let series_list = storage
        .select_series(&selection)
        .map_err(|err| format!("failed to resolve candidate series: {err}"))?;

    let mut out_series = Vec::new();
    let end = query.end_timestamp_ms.saturating_add(1);
    for series in series_list {
        let points =
            match storage.select(&series.name, &series.labels, query.start_timestamp_ms, end) {
                Ok(points) => points,
                Err(tsink::TsinkError::NoDataPoints { .. }) => continue,
                Err(err) => return Err(format!("query failed for {}: {err}", series.name)),
            };

        let Some(series) = prom_time_series_from_points(series, points) else {
            continue;
        };
        out_series.push(series);
    }

    Ok(QueryResult {
        timeseries: out_series,
    })
}

async fn execute_query_distributed(
    storage: &Arc<dyn Storage>,
    cluster_context: &ClusterRequestContext,
    read_fanout: &ReadFanoutExecutor,
    query: &Query,
    tenant_id: &str,
) -> Result<(QueryResult, ReadFanoutResponseMetadata), ReadFanoutError> {
    let ring_version = cluster_ring_version(Some(cluster_context));
    let selections = series_selection_from_remote_matchers(&query.matchers)
        .map_err(|err| ReadFanoutError::InvalidRequest {
            message: format!("invalid remote-read matchers: {err}"),
        })
        .and_then(|selection| {
            tenant::read_selections_for_tenant(&selection, tenant_id).map_err(|err| {
                ReadFanoutError::InvalidRequest {
                    message: format!("invalid remote-read matchers: {err}"),
                }
            })
        })?;
    let series_response = fanout_select_series_union(
        storage,
        cluster_context,
        read_fanout,
        &selections,
        ring_version,
    )
    .await?;
    let mut read_metadata = series_response.metadata.clone();

    let end = query.end_timestamp_ms.saturating_add(1);
    let points_response = read_fanout
        .select_points_for_series_with_ring_version_detailed(
            storage,
            &cluster_context.rpc_client,
            &series_response.value,
            query.start_timestamp_ms,
            end,
            ring_version,
        )
        .await?;
    merge_read_response_metadata(&mut read_metadata, &points_response.metadata);

    let mut out_series = Vec::new();
    for SeriesPoints { series, points } in points_response.value {
        let Some(series) = tenant::visible_metric_series(series, tenant_id) else {
            continue;
        };
        let Some(series) = prom_time_series_from_points(series, points) else {
            continue;
        };
        out_series.push(series);
    }
    out_series.sort_by(compare_prom_series);

    Ok((
        QueryResult {
            timeseries: out_series,
        },
        read_metadata,
    ))
}

fn compare_prom_series(left: &TimeSeries, right: &TimeSeries) -> std::cmp::Ordering {
    let mut idx = 0usize;
    loop {
        let left_label = left.labels.get(idx);
        let right_label = right.labels.get(idx);
        match (left_label, right_label) {
            (Some(left_label), Some(right_label)) => {
                let order = left_label
                    .name
                    .cmp(&right_label.name)
                    .then(left_label.value.cmp(&right_label.value));
                if order != std::cmp::Ordering::Equal {
                    return order;
                }
            }
            (None, Some(_)) => return std::cmp::Ordering::Less,
            (Some(_), None) => return std::cmp::Ordering::Greater,
            (None, None) => break,
        }
        idx += 1;
    }

    prom_series_point_count(left)
        .cmp(&prom_series_point_count(right))
        .then_with(|| prom_series_first_timestamp(left).cmp(&prom_series_first_timestamp(right)))
}

fn prom_time_series_from_points(
    series: MetricSeries,
    points: Vec<DataPoint>,
) -> Option<TimeSeries> {
    let mut samples = Vec::new();
    let mut histograms = Vec::new();
    for point in points {
        if let Some(histogram) = point.value.as_histogram() {
            histograms.push(tsink_histogram_to_prom(histogram, point.timestamp));
        } else if let Some(value) = point.value.as_f64() {
            samples.push(PromSample {
                value,
                timestamp: point.timestamp,
            });
        }
    }
    if samples.is_empty() && histograms.is_empty() {
        return None;
    }

    let mut labels = Vec::with_capacity(series.labels.len() + 1);
    labels.push(PromLabel {
        name: "__name__".to_string(),
        value: series.name,
    });
    labels.extend(series.labels.into_iter().map(|label| PromLabel {
        name: label.name,
        value: label.value,
    }));
    labels.sort_by(|a, b| a.name.cmp(&b.name).then(a.value.cmp(&b.value)));

    Some(TimeSeries {
        labels,
        samples,
        histograms,
        ..Default::default()
    })
}

fn tsink_histogram_to_prom(histogram: &tsink::NativeHistogram, timestamp: i64) -> PromHistogram {
    PromHistogram {
        count: histogram.count.as_ref().map(|count| match count {
            tsink::HistogramCount::Int(value) => histogram::Count::CountInt(*value),
            tsink::HistogramCount::Float(value) => histogram::Count::CountFloat(*value),
        }),
        sum: histogram.sum,
        schema: histogram.schema,
        zero_threshold: histogram.zero_threshold,
        zero_count: histogram.zero_count.as_ref().map(|count| match count {
            tsink::HistogramCount::Int(value) => histogram::ZeroCount::ZeroCountInt(*value),
            tsink::HistogramCount::Float(value) => histogram::ZeroCount::ZeroCountFloat(*value),
        }),
        negative_spans: histogram
            .negative_spans
            .iter()
            .map(|span| BucketSpan {
                offset: span.offset,
                length: span.length,
            })
            .collect(),
        negative_deltas: histogram.negative_deltas.clone(),
        negative_counts: histogram.negative_counts.clone(),
        positive_spans: histogram
            .positive_spans
            .iter()
            .map(|span| BucketSpan {
                offset: span.offset,
                length: span.length,
            })
            .collect(),
        positive_deltas: histogram.positive_deltas.clone(),
        positive_counts: histogram.positive_counts.clone(),
        reset_hint: match histogram.reset_hint {
            tsink::HistogramResetHint::Unknown => PromHistogramResetHint::Unknown as i32,
            tsink::HistogramResetHint::Yes => PromHistogramResetHint::Yes as i32,
            tsink::HistogramResetHint::No => PromHistogramResetHint::No as i32,
            tsink::HistogramResetHint::Gauge => PromHistogramResetHint::Gauge as i32,
        },
        timestamp,
        custom_values: histogram.custom_values.clone(),
    }
}

fn prom_series_point_count(series: &TimeSeries) -> usize {
    series.samples.len() + series.histograms.len()
}

fn prom_series_first_timestamp(series: &TimeSeries) -> Option<i64> {
    series
        .samples
        .first()
        .map(|sample| sample.timestamp)
        .into_iter()
        .chain(
            series
                .histograms
                .first()
                .map(|histogram| histogram.timestamp),
        )
        .min()
}
