use crate::http::{json_response, text_response, HttpRequest, HttpResponse};
use crate::prom_remote::{
    Label as PromLabel, LabelMatcher, MatcherType, Query, QueryResult, ReadRequest, ReadResponse,
    Sample as PromSample, TimeSeries, WriteRequest,
};
use prost::Message;
use serde_json::{json, Value as JsonValue};
use snap::raw::{Decoder as SnappyDecoder, Encoder as SnappyEncoder};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;
use tsink::promql::ast::{Expr, MatchOp};
use tsink::promql::types::PromqlValue;
use tsink::promql::Engine;
use tsink::{
    DataPoint, Label, Row, SeriesMatcher, SeriesMatcherOp, SeriesSelection, Storage,
    TimestampPrecision,
};

pub async fn handle_request(
    storage: &Arc<dyn Storage>,
    engine: &Engine,
    request: HttpRequest,
    server_start: Instant,
    timestamp_precision: TimestampPrecision,
) -> HttpResponse {
    let path = request.path_without_query().to_owned();
    match (request.method.as_str(), path.as_str()) {
        ("GET", "/healthz") => {
            HttpResponse::new(200, "ok\n").with_header("Content-Type", "text/plain")
        }
        ("GET", "/ready") => {
            HttpResponse::new(200, "ready\n").with_header("Content-Type", "text/plain")
        }
        ("GET", "/metrics") => handle_metrics(storage, server_start),
        ("GET" | "POST", "/api/v1/query") => {
            handle_instant_query(storage, engine, &request, timestamp_precision).await
        }
        ("GET" | "POST", "/api/v1/query_range") => {
            handle_range_query(storage, engine, &request, timestamp_precision).await
        }
        ("GET", "/api/v1/series") => handle_series(storage, &request).await,
        ("GET", "/api/v1/labels") => handle_labels(storage).await,
        ("GET", p) if p.starts_with("/api/v1/label/") && p.ends_with("/values") => {
            let label_name = &p["/api/v1/label/".len()..p.len() - "/values".len()];
            handle_label_values(storage, label_name).await
        }
        ("POST", "/api/v1/write") => handle_remote_write(storage, &request).await,
        ("POST", "/api/v1/read") => handle_remote_read(storage, &request).await,
        ("POST", "/api/v1/import/prometheus") => {
            handle_prometheus_import(storage, &request, timestamp_precision).await
        }
        ("GET", "/api/v1/status/tsdb") => handle_tsdb_status(storage).await,
        ("POST", "/api/v1/admin/delete_series") => {
            text_response(501, "series deletion is not yet supported")
        }
        _ => text_response(404, "not found"),
    }
}

async fn handle_instant_query(
    storage: &Arc<dyn Storage>,
    _engine: &Engine,
    request: &HttpRequest,
    precision: TimestampPrecision,
) -> HttpResponse {
    let Some(query) = request.param("query") else {
        return promql_error_response("bad_data", "missing required parameter 'query'");
    };

    let time = match request.param("time") {
        Some(t) => match parse_timestamp(&t, precision) {
            Ok(ts) => ts,
            Err(err) => return promql_error_response("bad_data", &err),
        },
        None => current_timestamp(precision),
    };

    let storage = Arc::clone(storage);
    let engine = Engine::with_precision(Arc::clone(&storage), precision);
    let result = tokio::task::spawn_blocking(move || engine.instant_query(&query, time)).await;

    match result {
        Ok(Ok(value)) => promql_success_response(&value, precision),
        Ok(Err(err)) => promql_error_response("execution", &err.to_string()),
        Err(err) => promql_error_response("execution", &format!("query task failed: {err}")),
    }
}

async fn handle_range_query(
    storage: &Arc<dyn Storage>,
    _engine: &Engine,
    request: &HttpRequest,
    precision: TimestampPrecision,
) -> HttpResponse {
    let Some(query) = request.param("query") else {
        return promql_error_response("bad_data", "missing required parameter 'query'");
    };
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

    let storage = Arc::clone(storage);
    let engine = Engine::with_precision(Arc::clone(&storage), precision);
    let result =
        tokio::task::spawn_blocking(move || engine.range_query(&query, start, end, step)).await;

    match result {
        Ok(Ok(value)) => promql_success_response(&value, precision),
        Ok(Err(err)) => promql_error_response("execution", &err.to_string()),
        Err(err) => promql_error_response("execution", &format!("query task failed: {err}")),
    }
}

async fn handle_series(storage: &Arc<dyn Storage>, request: &HttpRequest) -> HttpResponse {
    let matchers = request.param_all("match[]");
    if matchers.is_empty() {
        return promql_error_response("bad_data", "missing required parameter 'match[]'");
    }

    let storage = Arc::clone(storage);
    let result = tokio::task::spawn_blocking(move || {
        let mut all_series = Vec::new();
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
                let mut labels_map = serde_json::Map::new();
                labels_map.insert("__name__".to_string(), JsonValue::String(s.name.clone()));
                for label in &s.labels {
                    labels_map.insert(label.name.clone(), JsonValue::String(label.value.clone()));
                }
                all_series.push(JsonValue::Object(labels_map));
            }
        }
        Ok(all_series)
    })
    .await;

    match result {
        Ok(Ok(data)) => json_response(200, &json!({"status": "success", "data": data})),
        Ok(Err(err)) => promql_error_response("bad_data", &err),
        Err(err) => promql_error_response("execution", &format!("task failed: {err}")),
    }
}

async fn handle_labels(storage: &Arc<dyn Storage>) -> HttpResponse {
    let storage = Arc::clone(storage);
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
        Ok(Ok(data)) => json_response(200, &json!({"status": "success", "data": data})),
        Ok(Err(err)) => promql_error_response("execution", &format!("label query failed: {err}")),
        Err(err) => promql_error_response("execution", &format!("task failed: {err}")),
    }
}

async fn handle_label_values(storage: &Arc<dyn Storage>, label_name: &str) -> HttpResponse {
    let label_name = label_name.to_string();
    let storage = Arc::clone(storage);
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
        Ok(Ok(data)) => json_response(200, &json!({"status": "success", "data": data})),
        Ok(Err(err)) => {
            promql_error_response("execution", &format!("label values query failed: {err}"))
        }
        Err(err) => promql_error_response("execution", &format!("task failed: {err}")),
    }
}

fn handle_metrics(storage: &Arc<dyn Storage>, server_start: Instant) -> HttpResponse {
    let memory_used = storage.memory_used();
    let memory_budget = storage.memory_budget();
    let series_count = storage.list_metrics().map(|m| m.len()).unwrap_or(0);
    let uptime = server_start.elapsed().as_secs();
    let obs = storage.observability_snapshot();
    let wal_enabled = u8::from(obs.wal.enabled);

    let body = format!(
        "# HELP tsink_memory_used_bytes Estimated in-memory bytes\n\
         # TYPE tsink_memory_used_bytes gauge\n\
         tsink_memory_used_bytes {memory_used}\n\
         # HELP tsink_memory_budget_bytes Configured memory budget\n\
         # TYPE tsink_memory_budget_bytes gauge\n\
         tsink_memory_budget_bytes {memory_budget}\n\
         # HELP tsink_series_total Number of known metric series\n\
         # TYPE tsink_series_total gauge\n\
         tsink_series_total {series_count}\n\
         # HELP tsink_uptime_seconds Server uptime in seconds\n\
         # TYPE tsink_uptime_seconds gauge\n\
         tsink_uptime_seconds {uptime}\n\
         # HELP tsink_wal_enabled WAL enabled state (1 enabled, 0 disabled)\n\
         # TYPE tsink_wal_enabled gauge\n\
         tsink_wal_enabled {wal_enabled}\n\
         # HELP tsink_wal_size_bytes WAL size on disk in bytes\n\
         # TYPE tsink_wal_size_bytes gauge\n\
         tsink_wal_size_bytes {wal_size_bytes}\n\
         # HELP tsink_wal_segments WAL segment files present\n\
         # TYPE tsink_wal_segments gauge\n\
         tsink_wal_segments {wal_segments}\n\
         # HELP tsink_wal_active_segment Current WAL segment id\n\
         # TYPE tsink_wal_active_segment gauge\n\
         tsink_wal_active_segment {wal_active_segment}\n\
         # HELP tsink_wal_highwater_segment Last WAL replay highwater segment\n\
         # TYPE tsink_wal_highwater_segment gauge\n\
         tsink_wal_highwater_segment {wal_highwater_segment}\n\
         # HELP tsink_wal_highwater_frame Last WAL replay highwater frame\n\
         # TYPE tsink_wal_highwater_frame gauge\n\
         tsink_wal_highwater_frame {wal_highwater_frame}\n\
         # HELP tsink_wal_replay_runs_total WAL replay runs\n\
         # TYPE tsink_wal_replay_runs_total counter\n\
         tsink_wal_replay_runs_total {wal_replay_runs_total}\n\
         # HELP tsink_wal_replay_frames_total WAL replayed frames\n\
         # TYPE tsink_wal_replay_frames_total counter\n\
         tsink_wal_replay_frames_total {wal_replay_frames_total}\n\
         # HELP tsink_wal_replay_series_definitions_total WAL replayed series definitions\n\
         # TYPE tsink_wal_replay_series_definitions_total counter\n\
         tsink_wal_replay_series_definitions_total {wal_replay_series_definitions_total}\n\
         # HELP tsink_wal_replay_sample_batches_total WAL replayed sample batches\n\
         # TYPE tsink_wal_replay_sample_batches_total counter\n\
         tsink_wal_replay_sample_batches_total {wal_replay_sample_batches_total}\n\
         # HELP tsink_wal_replay_points_total WAL replayed points\n\
         # TYPE tsink_wal_replay_points_total counter\n\
         tsink_wal_replay_points_total {wal_replay_points_total}\n\
         # HELP tsink_wal_replay_errors_total WAL replay errors\n\
         # TYPE tsink_wal_replay_errors_total counter\n\
         tsink_wal_replay_errors_total {wal_replay_errors_total}\n\
         # HELP tsink_wal_replay_duration_nanoseconds_total WAL replay runtime\n\
         # TYPE tsink_wal_replay_duration_nanoseconds_total counter\n\
         tsink_wal_replay_duration_nanoseconds_total {wal_replay_duration_nanos_total}\n\
         # HELP tsink_wal_append_series_definitions_total WAL appended series definitions\n\
         # TYPE tsink_wal_append_series_definitions_total counter\n\
         tsink_wal_append_series_definitions_total {wal_append_series_definitions_total}\n\
         # HELP tsink_wal_append_sample_batches_total WAL appended sample batches\n\
         # TYPE tsink_wal_append_sample_batches_total counter\n\
         tsink_wal_append_sample_batches_total {wal_append_sample_batches_total}\n\
         # HELP tsink_wal_append_points_total WAL appended points\n\
         # TYPE tsink_wal_append_points_total counter\n\
         tsink_wal_append_points_total {wal_append_points_total}\n\
         # HELP tsink_wal_append_bytes_total WAL appended bytes\n\
         # TYPE tsink_wal_append_bytes_total counter\n\
         tsink_wal_append_bytes_total {wal_append_bytes_total}\n\
         # HELP tsink_wal_append_errors_total WAL append errors\n\
         # TYPE tsink_wal_append_errors_total counter\n\
         tsink_wal_append_errors_total {wal_append_errors_total}\n\
         # HELP tsink_wal_resets_total WAL resets\n\
         # TYPE tsink_wal_resets_total counter\n\
         tsink_wal_resets_total {wal_resets_total}\n\
         # HELP tsink_wal_reset_errors_total WAL reset errors\n\
         # TYPE tsink_wal_reset_errors_total counter\n\
         tsink_wal_reset_errors_total {wal_reset_errors_total}\n\
         # HELP tsink_flush_pipeline_runs_total Flush pipeline runs\n\
         # TYPE tsink_flush_pipeline_runs_total counter\n\
         tsink_flush_pipeline_runs_total {flush_pipeline_runs_total}\n\
         # HELP tsink_flush_pipeline_success_total Successful flush pipeline runs\n\
         # TYPE tsink_flush_pipeline_success_total counter\n\
         tsink_flush_pipeline_success_total {flush_pipeline_success_total}\n\
         # HELP tsink_flush_pipeline_timeout_total Flush pipeline write-timeout skips\n\
         # TYPE tsink_flush_pipeline_timeout_total counter\n\
         tsink_flush_pipeline_timeout_total {flush_pipeline_timeout_total}\n\
         # HELP tsink_flush_pipeline_errors_total Flush pipeline errors\n\
         # TYPE tsink_flush_pipeline_errors_total counter\n\
         tsink_flush_pipeline_errors_total {flush_pipeline_errors_total}\n\
         # HELP tsink_flush_pipeline_duration_nanoseconds_total Flush pipeline runtime\n\
         # TYPE tsink_flush_pipeline_duration_nanoseconds_total counter\n\
         tsink_flush_pipeline_duration_nanoseconds_total {flush_pipeline_duration_nanos_total}\n\
         # HELP tsink_flush_active_runs_total Active chunk flush runs\n\
         # TYPE tsink_flush_active_runs_total counter\n\
         tsink_flush_active_runs_total {active_flush_runs_total}\n\
         # HELP tsink_flush_active_errors_total Active chunk flush errors\n\
         # TYPE tsink_flush_active_errors_total counter\n\
         tsink_flush_active_errors_total {active_flush_errors_total}\n\
         # HELP tsink_flush_active_series_total Active series flushed into sealed chunks\n\
         # TYPE tsink_flush_active_series_total counter\n\
         tsink_flush_active_series_total {active_flushed_series_total}\n\
         # HELP tsink_flush_active_chunks_total Active chunks flushed\n\
         # TYPE tsink_flush_active_chunks_total counter\n\
         tsink_flush_active_chunks_total {active_flushed_chunks_total}\n\
         # HELP tsink_flush_active_points_total Active points flushed\n\
         # TYPE tsink_flush_active_points_total counter\n\
         tsink_flush_active_points_total {active_flushed_points_total}\n\
         # HELP tsink_flush_persist_runs_total Persist attempts\n\
         # TYPE tsink_flush_persist_runs_total counter\n\
         tsink_flush_persist_runs_total {persist_runs_total}\n\
         # HELP tsink_flush_persist_success_total Successful persist runs\n\
         # TYPE tsink_flush_persist_success_total counter\n\
         tsink_flush_persist_success_total {persist_success_total}\n\
         # HELP tsink_flush_persist_noop_total Persist runs with no new chunks\n\
         # TYPE tsink_flush_persist_noop_total counter\n\
         tsink_flush_persist_noop_total {persist_noop_total}\n\
         # HELP tsink_flush_persist_errors_total Persist errors\n\
         # TYPE tsink_flush_persist_errors_total counter\n\
         tsink_flush_persist_errors_total {persist_errors_total}\n\
         # HELP tsink_flush_persisted_series_total Series persisted\n\
         # TYPE tsink_flush_persisted_series_total counter\n\
         tsink_flush_persisted_series_total {persisted_series_total}\n\
         # HELP tsink_flush_persisted_chunks_total Chunks persisted\n\
         # TYPE tsink_flush_persisted_chunks_total counter\n\
         tsink_flush_persisted_chunks_total {persisted_chunks_total}\n\
         # HELP tsink_flush_persisted_points_total Points persisted\n\
         # TYPE tsink_flush_persisted_points_total counter\n\
         tsink_flush_persisted_points_total {persisted_points_total}\n\
         # HELP tsink_flush_persisted_segments_total Segments emitted by persist\n\
         # TYPE tsink_flush_persisted_segments_total counter\n\
         tsink_flush_persisted_segments_total {persisted_segments_total}\n\
         # HELP tsink_flush_persist_duration_nanoseconds_total Persist runtime\n\
         # TYPE tsink_flush_persist_duration_nanoseconds_total counter\n\
         tsink_flush_persist_duration_nanoseconds_total {persist_duration_nanos_total}\n\
         # HELP tsink_flush_evicted_sealed_chunks_total Sealed chunks evicted after persistence\n\
         # TYPE tsink_flush_evicted_sealed_chunks_total counter\n\
         tsink_flush_evicted_sealed_chunks_total {evicted_sealed_chunks_total}\n\
         # HELP tsink_compaction_runs_total Compaction invocations\n\
         # TYPE tsink_compaction_runs_total counter\n\
         tsink_compaction_runs_total {compaction_runs_total}\n\
         # HELP tsink_compaction_success_total Compaction runs that rewrote segments\n\
         # TYPE tsink_compaction_success_total counter\n\
         tsink_compaction_success_total {compaction_success_total}\n\
         # HELP tsink_compaction_noop_total Compaction runs with no rewrite\n\
         # TYPE tsink_compaction_noop_total counter\n\
         tsink_compaction_noop_total {compaction_noop_total}\n\
         # HELP tsink_compaction_errors_total Compaction errors\n\
         # TYPE tsink_compaction_errors_total counter\n\
         tsink_compaction_errors_total {compaction_errors_total}\n\
         # HELP tsink_compaction_source_segments_total Source segments considered by compaction\n\
         # TYPE tsink_compaction_source_segments_total counter\n\
         tsink_compaction_source_segments_total {compaction_source_segments_total}\n\
         # HELP tsink_compaction_output_segments_total Output segments emitted by compaction\n\
         # TYPE tsink_compaction_output_segments_total counter\n\
         tsink_compaction_output_segments_total {compaction_output_segments_total}\n\
         # HELP tsink_compaction_source_chunks_total Source chunks considered by compaction\n\
         # TYPE tsink_compaction_source_chunks_total counter\n\
         tsink_compaction_source_chunks_total {compaction_source_chunks_total}\n\
         # HELP tsink_compaction_output_chunks_total Output chunks emitted by compaction\n\
         # TYPE tsink_compaction_output_chunks_total counter\n\
         tsink_compaction_output_chunks_total {compaction_output_chunks_total}\n\
         # HELP tsink_compaction_source_points_total Source points considered by compaction\n\
         # TYPE tsink_compaction_source_points_total counter\n\
         tsink_compaction_source_points_total {compaction_source_points_total}\n\
         # HELP tsink_compaction_output_points_total Output points emitted by compaction\n\
         # TYPE tsink_compaction_output_points_total counter\n\
         tsink_compaction_output_points_total {compaction_output_points_total}\n\
         # HELP tsink_compaction_duration_nanoseconds_total Compaction runtime\n\
         # TYPE tsink_compaction_duration_nanoseconds_total counter\n\
         tsink_compaction_duration_nanoseconds_total {compaction_duration_nanos_total}\n\
         # HELP tsink_query_select_calls_total Storage select calls\n\
         # TYPE tsink_query_select_calls_total counter\n\
         tsink_query_select_calls_total {query_select_calls_total}\n\
         # HELP tsink_query_select_errors_total Storage select errors\n\
         # TYPE tsink_query_select_errors_total counter\n\
         tsink_query_select_errors_total {query_select_errors_total}\n\
         # HELP tsink_query_select_duration_nanoseconds_total Storage select runtime\n\
         # TYPE tsink_query_select_duration_nanoseconds_total counter\n\
         tsink_query_select_duration_nanoseconds_total {query_select_duration_nanos_total}\n\
         # HELP tsink_query_select_points_returned_total Points returned by select\n\
         # TYPE tsink_query_select_points_returned_total counter\n\
         tsink_query_select_points_returned_total {query_select_points_returned_total}\n\
         # HELP tsink_query_select_with_options_calls_total Storage select_with_options calls\n\
         # TYPE tsink_query_select_with_options_calls_total counter\n\
         tsink_query_select_with_options_calls_total {query_select_with_options_calls_total}\n\
         # HELP tsink_query_select_with_options_errors_total Storage select_with_options errors\n\
         # TYPE tsink_query_select_with_options_errors_total counter\n\
         tsink_query_select_with_options_errors_total {query_select_with_options_errors_total}\n\
         # HELP tsink_query_select_with_options_duration_nanoseconds_total Storage select_with_options runtime\n\
         # TYPE tsink_query_select_with_options_duration_nanoseconds_total counter\n\
         tsink_query_select_with_options_duration_nanoseconds_total {query_select_with_options_duration_nanos_total}\n\
         # HELP tsink_query_select_with_options_points_returned_total Points returned by select_with_options\n\
         # TYPE tsink_query_select_with_options_points_returned_total counter\n\
         tsink_query_select_with_options_points_returned_total {query_select_with_options_points_returned_total}\n\
         # HELP tsink_query_select_all_calls_total Storage select_all calls\n\
         # TYPE tsink_query_select_all_calls_total counter\n\
         tsink_query_select_all_calls_total {query_select_all_calls_total}\n\
         # HELP tsink_query_select_all_errors_total Storage select_all errors\n\
         # TYPE tsink_query_select_all_errors_total counter\n\
         tsink_query_select_all_errors_total {query_select_all_errors_total}\n\
         # HELP tsink_query_select_all_duration_nanoseconds_total Storage select_all runtime\n\
         # TYPE tsink_query_select_all_duration_nanoseconds_total counter\n\
         tsink_query_select_all_duration_nanoseconds_total {query_select_all_duration_nanos_total}\n\
         # HELP tsink_query_select_all_series_returned_total Series returned by select_all\n\
         # TYPE tsink_query_select_all_series_returned_total counter\n\
         tsink_query_select_all_series_returned_total {query_select_all_series_returned_total}\n\
         # HELP tsink_query_select_all_points_returned_total Points returned by select_all\n\
         # TYPE tsink_query_select_all_points_returned_total counter\n\
         tsink_query_select_all_points_returned_total {query_select_all_points_returned_total}\n\
         # HELP tsink_query_select_series_calls_total Storage select_series calls\n\
         # TYPE tsink_query_select_series_calls_total counter\n\
         tsink_query_select_series_calls_total {query_select_series_calls_total}\n\
         # HELP tsink_query_select_series_errors_total Storage select_series errors\n\
         # TYPE tsink_query_select_series_errors_total counter\n\
         tsink_query_select_series_errors_total {query_select_series_errors_total}\n\
         # HELP tsink_query_select_series_duration_nanoseconds_total Storage select_series runtime\n\
         # TYPE tsink_query_select_series_duration_nanoseconds_total counter\n\
         tsink_query_select_series_duration_nanoseconds_total {query_select_series_duration_nanos_total}\n\
         # HELP tsink_query_select_series_returned_total Series returned by select_series\n\
         # TYPE tsink_query_select_series_returned_total counter\n\
         tsink_query_select_series_returned_total {query_select_series_returned_total}\n\
         # HELP tsink_query_merge_path_queries_total Query series collections using merge path\n\
         # TYPE tsink_query_merge_path_queries_total counter\n\
         tsink_query_merge_path_queries_total {query_merge_path_queries_total}\n\
         # HELP tsink_query_append_sort_path_queries_total Query series collections using append/sort path\n\
         # TYPE tsink_query_append_sort_path_queries_total counter\n\
         tsink_query_append_sort_path_queries_total {query_append_sort_path_queries_total}\n"
        ,
        wal_size_bytes = obs.wal.size_bytes,
        wal_segments = obs.wal.segment_count,
        wal_active_segment = obs.wal.active_segment,
        wal_highwater_segment = obs.wal.highwater_segment,
        wal_highwater_frame = obs.wal.highwater_frame,
        wal_replay_runs_total = obs.wal.replay_runs_total,
        wal_replay_frames_total = obs.wal.replay_frames_total,
        wal_replay_series_definitions_total = obs.wal.replay_series_definitions_total,
        wal_replay_sample_batches_total = obs.wal.replay_sample_batches_total,
        wal_replay_points_total = obs.wal.replay_points_total,
        wal_replay_errors_total = obs.wal.replay_errors_total,
        wal_replay_duration_nanos_total = obs.wal.replay_duration_nanos_total,
        wal_append_series_definitions_total = obs.wal.append_series_definitions_total,
        wal_append_sample_batches_total = obs.wal.append_sample_batches_total,
        wal_append_points_total = obs.wal.append_points_total,
        wal_append_bytes_total = obs.wal.append_bytes_total,
        wal_append_errors_total = obs.wal.append_errors_total,
        wal_resets_total = obs.wal.resets_total,
        wal_reset_errors_total = obs.wal.reset_errors_total,
        flush_pipeline_runs_total = obs.flush.pipeline_runs_total,
        flush_pipeline_success_total = obs.flush.pipeline_success_total,
        flush_pipeline_timeout_total = obs.flush.pipeline_timeout_total,
        flush_pipeline_errors_total = obs.flush.pipeline_errors_total,
        flush_pipeline_duration_nanos_total = obs.flush.pipeline_duration_nanos_total,
        active_flush_runs_total = obs.flush.active_flush_runs_total,
        active_flush_errors_total = obs.flush.active_flush_errors_total,
        active_flushed_series_total = obs.flush.active_flushed_series_total,
        active_flushed_chunks_total = obs.flush.active_flushed_chunks_total,
        active_flushed_points_total = obs.flush.active_flushed_points_total,
        persist_runs_total = obs.flush.persist_runs_total,
        persist_success_total = obs.flush.persist_success_total,
        persist_noop_total = obs.flush.persist_noop_total,
        persist_errors_total = obs.flush.persist_errors_total,
        persisted_series_total = obs.flush.persisted_series_total,
        persisted_chunks_total = obs.flush.persisted_chunks_total,
        persisted_points_total = obs.flush.persisted_points_total,
        persisted_segments_total = obs.flush.persisted_segments_total,
        persist_duration_nanos_total = obs.flush.persist_duration_nanos_total,
        evicted_sealed_chunks_total = obs.flush.evicted_sealed_chunks_total,
        compaction_runs_total = obs.compaction.runs_total,
        compaction_success_total = obs.compaction.success_total,
        compaction_noop_total = obs.compaction.noop_total,
        compaction_errors_total = obs.compaction.errors_total,
        compaction_source_segments_total = obs.compaction.source_segments_total,
        compaction_output_segments_total = obs.compaction.output_segments_total,
        compaction_source_chunks_total = obs.compaction.source_chunks_total,
        compaction_output_chunks_total = obs.compaction.output_chunks_total,
        compaction_source_points_total = obs.compaction.source_points_total,
        compaction_output_points_total = obs.compaction.output_points_total,
        compaction_duration_nanos_total = obs.compaction.duration_nanos_total,
        query_select_calls_total = obs.query.select_calls_total,
        query_select_errors_total = obs.query.select_errors_total,
        query_select_duration_nanos_total = obs.query.select_duration_nanos_total,
        query_select_points_returned_total = obs.query.select_points_returned_total,
        query_select_with_options_calls_total = obs.query.select_with_options_calls_total,
        query_select_with_options_errors_total = obs.query.select_with_options_errors_total,
        query_select_with_options_duration_nanos_total =
            obs.query.select_with_options_duration_nanos_total,
        query_select_with_options_points_returned_total =
            obs.query.select_with_options_points_returned_total,
        query_select_all_calls_total = obs.query.select_all_calls_total,
        query_select_all_errors_total = obs.query.select_all_errors_total,
        query_select_all_duration_nanos_total = obs.query.select_all_duration_nanos_total,
        query_select_all_series_returned_total = obs.query.select_all_series_returned_total,
        query_select_all_points_returned_total = obs.query.select_all_points_returned_total,
        query_select_series_calls_total = obs.query.select_series_calls_total,
        query_select_series_errors_total = obs.query.select_series_errors_total,
        query_select_series_duration_nanos_total = obs.query.select_series_duration_nanos_total,
        query_select_series_returned_total = obs.query.select_series_returned_total,
        query_merge_path_queries_total = obs.query.merge_path_queries_total,
        query_append_sort_path_queries_total = obs.query.append_sort_path_queries_total,
    );

    HttpResponse::new(200, body.into_bytes())
        .with_header("Content-Type", "text/plain; version=0.0.4")
}

async fn handle_tsdb_status(storage: &Arc<dyn Storage>) -> HttpResponse {
    let memory_used = storage.memory_used();
    let memory_budget = storage.memory_budget();
    let storage = Arc::clone(storage);
    let result = tokio::task::spawn_blocking(move || {
        let series_count = storage.list_metrics().map(|m| m.len()).unwrap_or(0);
        let observability = storage.observability_snapshot();
        (series_count, observability)
    })
    .await;

    let (series_count, observability): (usize, tsink::StorageObservabilitySnapshot) =
        result.unwrap_or_default();

    json_response(
        200,
        &json!({
            "status": "success",
            "data": {
                "seriesCount": series_count,
                "memoryUsedBytes": memory_used,
                "memoryBudgetBytes": memory_budget,
                "wal": {
                    "enabled": observability.wal.enabled,
                    "sizeBytes": observability.wal.size_bytes,
                    "segmentCount": observability.wal.segment_count,
                    "activeSegment": observability.wal.active_segment,
                    "highwaterSegment": observability.wal.highwater_segment,
                    "highwaterFrame": observability.wal.highwater_frame,
                    "replayRunsTotal": observability.wal.replay_runs_total,
                    "replayFramesTotal": observability.wal.replay_frames_total,
                    "replaySeriesDefinitionsTotal": observability.wal.replay_series_definitions_total,
                    "replaySampleBatchesTotal": observability.wal.replay_sample_batches_total,
                    "replayPointsTotal": observability.wal.replay_points_total,
                    "replayErrorsTotal": observability.wal.replay_errors_total,
                    "replayDurationNanosTotal": observability.wal.replay_duration_nanos_total,
                    "appendSeriesDefinitionsTotal": observability.wal.append_series_definitions_total,
                    "appendSampleBatchesTotal": observability.wal.append_sample_batches_total,
                    "appendPointsTotal": observability.wal.append_points_total,
                    "appendBytesTotal": observability.wal.append_bytes_total,
                    "appendErrorsTotal": observability.wal.append_errors_total,
                    "resetsTotal": observability.wal.resets_total,
                    "resetErrorsTotal": observability.wal.reset_errors_total
                },
                "flush": {
                    "pipelineRunsTotal": observability.flush.pipeline_runs_total,
                    "pipelineSuccessTotal": observability.flush.pipeline_success_total,
                    "pipelineTimeoutTotal": observability.flush.pipeline_timeout_total,
                    "pipelineErrorsTotal": observability.flush.pipeline_errors_total,
                    "pipelineDurationNanosTotal": observability.flush.pipeline_duration_nanos_total,
                    "activeFlushRunsTotal": observability.flush.active_flush_runs_total,
                    "activeFlushErrorsTotal": observability.flush.active_flush_errors_total,
                    "activeFlushedSeriesTotal": observability.flush.active_flushed_series_total,
                    "activeFlushedChunksTotal": observability.flush.active_flushed_chunks_total,
                    "activeFlushedPointsTotal": observability.flush.active_flushed_points_total,
                    "persistRunsTotal": observability.flush.persist_runs_total,
                    "persistSuccessTotal": observability.flush.persist_success_total,
                    "persistNoopTotal": observability.flush.persist_noop_total,
                    "persistErrorsTotal": observability.flush.persist_errors_total,
                    "persistedSeriesTotal": observability.flush.persisted_series_total,
                    "persistedChunksTotal": observability.flush.persisted_chunks_total,
                    "persistedPointsTotal": observability.flush.persisted_points_total,
                    "persistedSegmentsTotal": observability.flush.persisted_segments_total,
                    "persistDurationNanosTotal": observability.flush.persist_duration_nanos_total,
                    "evictedSealedChunksTotal": observability.flush.evicted_sealed_chunks_total
                },
                "compaction": {
                    "runsTotal": observability.compaction.runs_total,
                    "successTotal": observability.compaction.success_total,
                    "noopTotal": observability.compaction.noop_total,
                    "errorsTotal": observability.compaction.errors_total,
                    "sourceSegmentsTotal": observability.compaction.source_segments_total,
                    "outputSegmentsTotal": observability.compaction.output_segments_total,
                    "sourceChunksTotal": observability.compaction.source_chunks_total,
                    "outputChunksTotal": observability.compaction.output_chunks_total,
                    "sourcePointsTotal": observability.compaction.source_points_total,
                    "outputPointsTotal": observability.compaction.output_points_total,
                    "durationNanosTotal": observability.compaction.duration_nanos_total
                },
                "query": {
                    "selectCallsTotal": observability.query.select_calls_total,
                    "selectErrorsTotal": observability.query.select_errors_total,
                    "selectDurationNanosTotal": observability.query.select_duration_nanos_total,
                    "selectPointsReturnedTotal": observability.query.select_points_returned_total,
                    "selectWithOptionsCallsTotal": observability.query.select_with_options_calls_total,
                    "selectWithOptionsErrorsTotal": observability.query.select_with_options_errors_total,
                    "selectWithOptionsDurationNanosTotal": observability.query.select_with_options_duration_nanos_total,
                    "selectWithOptionsPointsReturnedTotal": observability.query.select_with_options_points_returned_total,
                    "selectAllCallsTotal": observability.query.select_all_calls_total,
                    "selectAllErrorsTotal": observability.query.select_all_errors_total,
                    "selectAllDurationNanosTotal": observability.query.select_all_duration_nanos_total,
                    "selectAllSeriesReturnedTotal": observability.query.select_all_series_returned_total,
                    "selectAllPointsReturnedTotal": observability.query.select_all_points_returned_total,
                    "selectSeriesCallsTotal": observability.query.select_series_calls_total,
                    "selectSeriesErrorsTotal": observability.query.select_series_errors_total,
                    "selectSeriesDurationNanosTotal": observability.query.select_series_duration_nanos_total,
                    "selectSeriesReturnedTotal": observability.query.select_series_returned_total,
                    "mergePathQueriesTotal": observability.query.merge_path_queries_total,
                    "appendSortPathQueriesTotal": observability.query.append_sort_path_queries_total
                }
            }
        }),
    )
}

async fn handle_remote_write(storage: &Arc<dyn Storage>, request: &HttpRequest) -> HttpResponse {
    let decoded = match decode_body(request) {
        Ok(body) => body,
        Err(err) => return text_response(400, &err),
    };

    let write_req = match WriteRequest::decode(decoded.as_slice()) {
        Ok(req) => req,
        Err(err) => return text_response(400, &format!("invalid protobuf body: {err}")),
    };

    let mut rows = Vec::new();
    for series in write_req.timeseries {
        let Some((metric, labels)) = extract_metric_and_labels(series.labels) else {
            continue;
        };
        for sample in series.samples {
            rows.push(Row::with_labels(
                metric.clone(),
                labels.clone(),
                DataPoint::new(sample.timestamp, sample.value),
            ));
        }
    }

    if rows.is_empty() {
        return HttpResponse::new(200, Vec::<u8>::new());
    }

    let storage = Arc::clone(storage);
    let result = tokio::task::spawn_blocking(move || storage.insert_rows(&rows)).await;

    match result {
        Ok(Ok(())) => HttpResponse::new(200, Vec::<u8>::new()),
        Ok(Err(err)) => text_response(500, &format!("insert failed: {err}")),
        Err(err) => text_response(500, &format!("insert task failed: {err}")),
    }
}

async fn handle_remote_read(storage: &Arc<dyn Storage>, request: &HttpRequest) -> HttpResponse {
    let decoded = match decode_body(request) {
        Ok(body) => body,
        Err(err) => return text_response(400, &err),
    };

    let read_req = match ReadRequest::decode(decoded.as_slice()) {
        Ok(req) => req,
        Err(err) => return text_response(400, &format!("invalid protobuf body: {err}")),
    };

    let storage = Arc::clone(storage);
    let result = tokio::task::spawn_blocking(move || {
        let mut results = Vec::with_capacity(read_req.queries.len());
        for query in &read_req.queries {
            results.push(execute_query(&storage, query)?);
        }
        Ok::<_, String>(results)
    })
    .await;

    let results = match result {
        Ok(Ok(r)) => r,
        Ok(Err(err)) => return text_response(400, &err),
        Err(err) => return text_response(500, &format!("read task failed: {err}")),
    };

    let response = ReadResponse { results };
    let mut raw = Vec::new();
    if let Err(err) = response.encode(&mut raw) {
        return text_response(500, &format!("failed to encode response: {err}"));
    }

    let compressed = match SnappyEncoder::new().compress_vec(&raw) {
        Ok(bytes) => bytes,
        Err(err) => return text_response(500, &format!("snappy encode failed: {err}")),
    };

    HttpResponse::new(200, compressed)
        .with_header("Content-Type", "application/x-protobuf")
        .with_header("Content-Encoding", "snappy")
        .with_header("X-Prometheus-Remote-Read-Version", "0.1.0")
}

async fn handle_prometheus_import(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    precision: TimestampPrecision,
) -> HttpResponse {
    let body_str = match std::str::from_utf8(&request.body) {
        Ok(s) => s.to_string(),
        Err(_) => return text_response(400, "body must be valid UTF-8"),
    };

    let now = current_timestamp(precision);
    let rows = match parse_prometheus_text(&body_str, now) {
        Ok(rows) => rows,
        Err(err) => return text_response(400, &err),
    };

    if rows.is_empty() {
        return HttpResponse::new(200, Vec::<u8>::new());
    }

    let storage = Arc::clone(storage);
    let result = tokio::task::spawn_blocking(move || storage.insert_rows(&rows)).await;

    match result {
        Ok(Ok(())) => HttpResponse::new(200, Vec::<u8>::new()),
        Ok(Err(err)) => text_response(500, &format!("import failed: {err}")),
        Err(err) => text_response(500, &format!("import task failed: {err}")),
    }
}

fn parse_prometheus_text(text: &str, default_timestamp: i64) -> Result<Vec<Row>, String> {
    let mut rows = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (metric_and_labels, rest) = if let Some(brace_start) = line.find('{') {
            let brace_end = line[brace_start..]
                .find('}')
                .ok_or_else(|| format!("unclosed brace in line: {line}"))?
                + brace_start;
            let metric_name = &line[..brace_start];
            let labels_str = &line[brace_start + 1..brace_end];
            let rest = line[brace_end + 1..].trim();
            let labels = parse_prom_labels(labels_str)?;
            ((metric_name.to_string(), labels), rest)
        } else {
            let mut parts = line.splitn(2, |c: char| c.is_whitespace());
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
            metric_name,
            labels,
            DataPoint::new(timestamp, value),
        ));
    }

    Ok(rows)
}

fn parse_prom_labels(labels_str: &str) -> Result<Vec<Label>, String> {
    let mut labels = Vec::new();
    if labels_str.is_empty() {
        return Ok(labels);
    }

    for pair in labels_str.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let (name, value) = pair
            .split_once('=')
            .ok_or_else(|| format!("invalid label pair: '{pair}'"))?;
        let name = name.trim();
        let value = value.trim().trim_matches('"');
        labels.push(Label::new(name, value));
    }

    Ok(labels)
}

fn decode_body(request: &HttpRequest) -> Result<Vec<u8>, String> {
    match request.header("content-encoding") {
        None => Ok(request.body.clone()),
        Some(encoding) if encoding.eq_ignore_ascii_case("identity") => Ok(request.body.clone()),
        Some(encoding) if encoding.eq_ignore_ascii_case("snappy") => SnappyDecoder::new()
            .decompress_vec(&request.body)
            .map_err(|err| format!("snappy decode failed: {err}")),
        Some(encoding) => Err(format!("unsupported content-encoding: {encoding}")),
    }
}

fn extract_metric_and_labels(labels: Vec<PromLabel>) -> Option<(String, Vec<Label>)> {
    let mut metric = None;
    let mut out_labels = Vec::new();

    for label in labels {
        if label.name == "__name__" {
            if !label.value.is_empty() {
                metric = Some(label.value);
            }
            continue;
        }
        out_labels.push(Label::new(label.name, label.value));
    }

    metric.map(|name| (name, out_labels))
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

        let samples: Vec<PromSample> = points
            .into_iter()
            .filter_map(|point| {
                point.value.as_f64().map(|value| PromSample {
                    value,
                    timestamp: point.timestamp,
                })
            })
            .collect();

        if samples.is_empty() {
            continue;
        }

        let mut labels = Vec::with_capacity(series.labels.len() + 1);
        labels.push(PromLabel {
            name: "__name__".to_string(),
            value: series.name.clone(),
        });
        labels.extend(series.labels.into_iter().map(|label| PromLabel {
            name: label.name,
            value: label.value,
        }));
        labels.sort_by(|a, b| a.name.cmp(&b.name).then(a.value.cmp(&b.value)));

        out_series.push(TimeSeries { labels, samples });
    }

    Ok(QueryResult {
        timeseries: out_series,
    })
}

pub fn series_selection_from_remote_matchers(
    matchers: &[LabelMatcher],
) -> Result<SeriesSelection, String> {
    let mut selection = SeriesSelection::new();

    for matcher in matchers {
        let matcher_type = MatcherType::try_from(matcher.r#type)
            .map_err(|_| format!("unknown matcher type: {}", matcher.r#type))?;

        if matcher.name == "__name__" && matches!(matcher_type, MatcherType::Eq) {
            selection = selection.with_metric(matcher.value.clone());
        }

        let op = match matcher_type {
            MatcherType::Eq => SeriesMatcherOp::Equal,
            MatcherType::Neq => SeriesMatcherOp::NotEqual,
            MatcherType::Re => SeriesMatcherOp::RegexMatch,
            MatcherType::Nre => SeriesMatcherOp::RegexNoMatch,
        };
        selection = selection.with_matcher(SeriesMatcher::new(
            matcher.name.clone(),
            op,
            matcher.value.clone(),
        ));
    }

    Ok(selection)
}

fn expr_to_selection(expr: &Expr) -> Option<SeriesSelection> {
    match expr {
        Expr::VectorSelector(vs) => {
            let mut selection = SeriesSelection::new();
            if let Some(ref name) = vs.metric_name {
                selection = selection.with_metric(name.clone());
            }
            for m in &vs.matchers {
                let op = match m.op {
                    MatchOp::Equal => SeriesMatcherOp::Equal,
                    MatchOp::NotEqual => SeriesMatcherOp::NotEqual,
                    MatchOp::RegexMatch => SeriesMatcherOp::RegexMatch,
                    MatchOp::RegexNoMatch => SeriesMatcherOp::RegexNoMatch,
                };
                selection =
                    selection.with_matcher(SeriesMatcher::new(m.name.clone(), op, m.value.clone()));
            }
            Some(selection)
        }
        _ => None,
    }
}

fn current_timestamp(precision: TimestampPrecision) -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    match precision {
        TimestampPrecision::Seconds => now.as_secs() as i64,
        TimestampPrecision::Milliseconds => now.as_millis() as i64,
        TimestampPrecision::Microseconds => now.as_micros() as i64,
        TimestampPrecision::Nanoseconds => now.as_nanos() as i64,
    }
}

fn parse_timestamp(s: &str, precision: TimestampPrecision) -> Result<i64, String> {
    if let Ok(ts) = s.parse::<i64>() {
        return Ok(ts);
    }
    if let Ok(secs) = s.parse::<f64>() {
        let ts = match precision {
            TimestampPrecision::Seconds => secs as i64,
            TimestampPrecision::Milliseconds => (secs * 1_000.0) as i64,
            TimestampPrecision::Microseconds => (secs * 1_000_000.0) as i64,
            TimestampPrecision::Nanoseconds => (secs * 1_000_000_000.0) as i64,
        };
        return Ok(ts);
    }
    Err(format!("invalid timestamp: '{s}'"))
}

fn parse_step(s: &str, precision: TimestampPrecision) -> Result<i64, String> {
    if let Ok(secs) = s.parse::<f64>() {
        if secs <= 0.0 {
            return Err("step must be positive".to_string());
        }
        let step = match precision {
            TimestampPrecision::Seconds => secs as i64,
            TimestampPrecision::Milliseconds => (secs * 1_000.0) as i64,
            TimestampPrecision::Microseconds => (secs * 1_000_000.0) as i64,
            TimestampPrecision::Nanoseconds => (secs * 1_000_000_000.0) as i64,
        };
        return Ok(step.max(1));
    }
    let (num_str, unit) = if let Some(stripped) = s.strip_suffix("ms") {
        (stripped, "ms")
    } else if s.len() > 1 {
        (&s[..s.len() - 1], &s[s.len() - 1..])
    } else {
        return Err(format!("invalid step: '{s}'"));
    };

    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid step: '{s}'"))?;
    let secs = match unit {
        "ms" => num / 1_000.0,
        "s" => num,
        "m" => num * 60.0,
        "h" => num * 3_600.0,
        "d" => num * 86_400.0,
        "w" => num * 604_800.0,
        "y" => num * 365.25 * 86_400.0,
        _ => return Err(format!("invalid step unit: '{unit}'")),
    };

    if secs <= 0.0 {
        return Err("step must be positive".to_string());
    }

    let step = match precision {
        TimestampPrecision::Seconds => secs as i64,
        TimestampPrecision::Milliseconds => (secs * 1_000.0) as i64,
        TimestampPrecision::Microseconds => (secs * 1_000_000.0) as i64,
        TimestampPrecision::Nanoseconds => (secs * 1_000_000_000.0) as i64,
    };
    Ok(step.max(1))
}

fn promql_error_response(error_type: &str, error: &str) -> HttpResponse {
    json_response(
        422,
        &json!({
            "status": "error",
            "errorType": error_type,
            "error": error,
        }),
    )
}

fn promql_success_response(value: &PromqlValue, precision: TimestampPrecision) -> HttpResponse {
    let (result_type, result) = match value {
        PromqlValue::Scalar(v, t) => (
            "scalar",
            json!([timestamp_to_f64(*t, precision), format_value(*v)]),
        ),
        PromqlValue::InstantVector(samples) => {
            let items: Vec<JsonValue> = samples
                .iter()
                .map(|s| {
                    let mut metric = serde_json::Map::new();
                    metric.insert("__name__".to_string(), JsonValue::String(s.metric.clone()));
                    for label in &s.labels {
                        metric.insert(label.name.clone(), JsonValue::String(label.value.clone()));
                    }
                    json!({
                        "metric": metric,
                        "value": [timestamp_to_f64(s.timestamp, precision), format_value(s.value)],
                    })
                })
                .collect();
            ("vector", JsonValue::Array(items))
        }
        PromqlValue::RangeVector(series) => {
            let items: Vec<JsonValue> = series
                .iter()
                .map(|s| {
                    let mut metric = serde_json::Map::new();
                    metric.insert("__name__".to_string(), JsonValue::String(s.metric.clone()));
                    for label in &s.labels {
                        metric.insert(label.name.clone(), JsonValue::String(label.value.clone()));
                    }
                    let values: Vec<JsonValue> = s
                        .samples
                        .iter()
                        .map(|(t, v)| json!([timestamp_to_f64(*t, precision), format_value(*v)]))
                        .collect();
                    json!({
                        "metric": metric,
                        "values": values,
                    })
                })
                .collect();
            ("matrix", JsonValue::Array(items))
        }
        PromqlValue::String(s, t) => ("string", json!([timestamp_to_f64(*t, precision), s])),
    };

    json_response(
        200,
        &json!({
            "status": "success",
            "data": {
                "resultType": result_type,
                "result": result,
            }
        }),
    )
}

fn timestamp_to_f64(ts: i64, precision: TimestampPrecision) -> f64 {
    match precision {
        TimestampPrecision::Seconds => ts as f64,
        TimestampPrecision::Milliseconds => ts as f64 / 1_000.0,
        TimestampPrecision::Microseconds => ts as f64 / 1_000_000.0,
        TimestampPrecision::Nanoseconds => ts as f64 / 1_000_000_000.0,
    }
}

fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        v.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::HttpRequest;
    use crate::prom_remote::{
        LabelMatcher, MatcherType, Query, ReadRequest, ReadResponse, WriteRequest,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use tsink::{StorageBuilder, TimestampPrecision};

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build")
    }

    fn make_engine(storage: &Arc<dyn Storage>) -> Engine {
        Engine::with_precision(Arc::clone(storage), TimestampPrecision::Milliseconds)
    }

    fn snappy_encode(data: &[u8]) -> Vec<u8> {
        SnappyEncoder::new()
            .compress_vec(data)
            .expect("snappy encode should succeed")
    }

    fn snappy_decode(data: &[u8]) -> Vec<u8> {
        SnappyDecoder::new()
            .decompress_vec(data)
            .expect("snappy decode should succeed")
    }

    fn start_time() -> Instant {
        Instant::now()
    }

    #[tokio::test]
    async fn remote_write_inserts_points() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    PromLabel {
                        name: "__name__".to_string(),
                        value: "cpu_usage".to_string(),
                    },
                    PromLabel {
                        name: "host".to_string(),
                        value: "server-a".to_string(),
                    },
                ],
                samples: vec![PromSample {
                    value: 11.5,
                    timestamp: 1_700_000_000_000,
                }],
            }],
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");
        let compressed = snappy_encode(&encoded);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([
                ("content-encoding".to_string(), "snappy".to_string()),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
            ]),
            body: compressed,
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);

        let points = storage
            .select(
                "cpu_usage",
                &[Label::new("host", "server-a")],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("point must be persisted");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value.as_f64(), Some(11.5));
    }

    #[tokio::test]
    async fn remote_read_returns_snappy_protobuf() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        storage
            .insert_rows(&[Row::with_labels(
                "http_requests_total",
                vec![Label::new("method", "GET")],
                DataPoint::new(1_700_000_000_100, 42.0),
            )])
            .expect("insert should work");

        let read = ReadRequest {
            queries: vec![Query {
                start_timestamp_ms: 1_700_000_000_000,
                end_timestamp_ms: 1_700_000_000_200,
                matchers: vec![
                    LabelMatcher {
                        r#type: MatcherType::Eq as i32,
                        name: "__name__".to_string(),
                        value: "http_requests_total".to_string(),
                    },
                    LabelMatcher {
                        r#type: MatcherType::Eq as i32,
                        name: "method".to_string(),
                        value: "GET".to_string(),
                    },
                ],
            }],
        };

        let mut encoded = Vec::new();
        read.encode(&mut encoded)
            .expect("protobuf encode should work");
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/read".to_string(),
            headers: HashMap::from([
                ("content-encoding".to_string(), "snappy".to_string()),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
            ]),
            body: snappy_encode(&encoded),
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);
        assert_eq!(
            response
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("content-encoding"))
                .map(|(_, v)| v.as_str()),
            Some("snappy")
        );

        let decoded = snappy_decode(&response.body);
        let read_response =
            ReadResponse::decode(decoded.as_slice()).expect("response should decode");
        assert_eq!(read_response.results.len(), 1);
        assert_eq!(read_response.results[0].timeseries.len(), 1);
        let series = &read_response.results[0].timeseries[0];
        assert_eq!(series.samples.len(), 1);
        assert_eq!(series.samples[0].value, 42.0);
    }

    #[tokio::test]
    async fn promql_instant_query_returns_json() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        storage
            .insert_rows(&[Row::with_labels(
                "up",
                vec![Label::new("job", "prom")],
                DataPoint::new(1_700_000_000_000, 1.0),
            )])
            .expect("insert should work");

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query?query=up&time=1700000000000".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["resultType"], "vector");
    }

    #[tokio::test]
    async fn promql_range_query_returns_matrix() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "up",
                    vec![Label::new("job", "prom")],
                    DataPoint::new(1_700_000_000_000, 1.0),
                ),
                Row::with_labels(
                    "up",
                    vec![Label::new("job", "prom")],
                    DataPoint::new(1_700_000_015_000, 1.0),
                ),
            ])
            .expect("insert should work");

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query_range?query=up&start=1700000000000&end=1700000030000&step=15s"
                .to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["resultType"], "matrix");
    }

    #[tokio::test]
    async fn series_endpoint_returns_matching_series() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        storage
            .insert_rows(&[Row::with_labels(
                "http_requests",
                vec![Label::new("method", "GET")],
                DataPoint::new(1_700_000_000_000, 1.0),
            )])
            .expect("insert should work");

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/series?match[]=http_requests".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        let data = body["data"].as_array().expect("data should be array");
        assert!(!data.is_empty());
        assert_eq!(data[0]["__name__"], "http_requests");
    }

    #[tokio::test]
    async fn labels_endpoint_returns_all_label_names() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        storage
            .insert_rows(&[Row::with_labels(
                "metric",
                vec![
                    Label::new("job", "test"),
                    Label::new("instance", "localhost"),
                ],
                DataPoint::new(1_700_000_000_000, 1.0),
            )])
            .expect("insert should work");

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/labels".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        let data = body["data"].as_array().expect("data should be array");
        let names: Vec<&str> = data.iter().filter_map(|v| v.as_str()).collect();
        assert!(names.contains(&"__name__"));
        assert!(names.contains(&"job"));
        assert!(names.contains(&"instance"));
    }

    #[tokio::test]
    async fn label_values_endpoint() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "metric",
                    vec![Label::new("job", "alpha")],
                    DataPoint::new(1_700_000_000_000, 1.0),
                ),
                Row::with_labels(
                    "metric",
                    vec![Label::new("job", "beta")],
                    DataPoint::new(1_700_000_000_000, 2.0),
                ),
            ])
            .expect("insert should work");

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/label/job/values".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        let data = body["data"].as_array().expect("data should be array");
        let values: Vec<&str> = data.iter().filter_map(|v| v.as_str()).collect();
        assert!(values.contains(&"alpha"));
        assert!(values.contains(&"beta"));
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_exposition_format() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);

        let body = std::str::from_utf8(&response.body).expect("valid utf8");
        assert!(body.contains("tsink_memory_used_bytes"));
        assert!(body.contains("tsink_series_total"));
        assert!(body.contains("tsink_uptime_seconds"));
        assert!(body.contains("tsink_wal_enabled"));
        assert!(body.contains("tsink_flush_pipeline_runs_total"));
        assert!(body.contains("tsink_compaction_runs_total"));
        assert!(body.contains("tsink_query_select_calls_total"));
    }

    #[tokio::test]
    async fn ready_endpoint_returns_200() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/ready".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);
        assert_eq!(std::str::from_utf8(&response.body).unwrap(), "ready\n");
    }

    #[tokio::test]
    async fn status_tsdb_returns_json() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/status/tsdb".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        assert!(body["data"]["seriesCount"].is_number());
        assert!(body["data"]["memoryUsedBytes"].is_number());
        assert!(body["data"]["wal"]["enabled"].is_boolean());
        assert!(body["data"]["flush"]["pipelineRunsTotal"].is_number());
        assert!(body["data"]["compaction"]["runsTotal"].is_number());
        assert!(body["data"]["query"]["selectCallsTotal"].is_number());
    }

    #[tokio::test]
    async fn prometheus_text_import() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let text = r#"# HELP test_metric A test metric
# TYPE test_metric gauge
test_metric{job="test"} 42 1700000000000
test_metric{job="test2"} 99 1700000000000
"#;

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/import/prometheus".to_string(),
            headers: HashMap::from([("content-type".to_string(), "text/plain".to_string())]),
            body: text.as_bytes().to_vec(),
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);

        let points = storage
            .select(
                "test_metric",
                &[Label::new("job", "test")],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("point must be persisted");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value.as_f64(), Some(42.0));
    }

    #[tokio::test]
    async fn unknown_route_returns_404() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/nonexistent".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 404);
    }

    #[tokio::test]
    async fn delete_series_returns_501() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/delete_series".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 501);
    }
}
