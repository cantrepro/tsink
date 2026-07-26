use super::super::*;
use crate::cluster::query::AccountedMetricSeries;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::Serialize;
use std::fmt::Display;
use std::io::{self, Write};

const PUBLIC_METADATA_MAX_RESPONSE_BYTES: usize = MAX_BODY_BYTES;
const PUBLIC_METADATA_COLLECTION_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;
const PUBLIC_METADATA_MAX_DIAGNOSTIC_BYTES: usize = 512;
const PUBLIC_METADATA_DIAGNOSTIC_RESPONSE_ALLOCATION_BYTES: u64 = 16 * 1024;

#[derive(Debug)]
enum MetadataRequestError {
    BadData(&'static str),
    GuardedBadData {
        message: String,
        reservation: tsink::QueryMemoryReservation,
    },
    Execution(&'static str),
    Budget(tsink::QueryBudgetError),
    Fanout(ReadFanoutError),
}

impl MetadataRequestError {
    fn storage(error_type: &'static str, context: &'static str, error: tsink::TsinkError) -> Self {
        match error {
            tsink::TsinkError::QueryBudget(error) => Self::Budget(error),
            _ if error_type == "bad_data" => Self::BadData(context),
            _ => Self::Execution(context),
        }
    }
}

fn metadata_error_response(
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

fn metadata_read_admission_unavailable_response() -> HttpResponse {
    metadata_error_response(
        500,
        "execution",
        "metadata_read_admission_unavailable",
        "public metadata read admission is unavailable",
        None,
    )
}

fn metadata_request_error_response(error: MetadataRequestError) -> HttpResponse {
    match error {
        MetadataRequestError::BadData(message) => {
            metadata_error_response(422, "bad_data", "metadata_invalid_request", message, None)
        }
        MetadataRequestError::GuardedBadData {
            message,
            mut reservation,
        } => {
            let retained_bytes = modeled_string_retained_bytes(&message)
                .saturating_add(PUBLIC_METADATA_DIAGNOSTIC_RESPONSE_ALLOCATION_BYTES);
            if let Err(error) = resize_query_memory(
                &mut reservation,
                retained_bytes,
                "failed to reserve public metadata diagnostic response memory",
            ) {
                drop(reservation);
                return metadata_request_error_response(error);
            }
            let response = metadata_error_response(
                422,
                "bad_data",
                "metadata_invalid_matcher",
                &message,
                None,
            );
            drop(reservation);
            response
        }
        MetadataRequestError::Execution(message) => {
            metadata_error_response(500, "execution", "metadata_execution_failed", message, None)
        }
        MetadataRequestError::Budget(error) => metadata_query_budget_error_response(&error),
        MetadataRequestError::Fanout(ReadFanoutError::QueryBudget { error }) => {
            metadata_query_budget_error_response(&error)
        }
        MetadataRequestError::Fanout(error) => metadata_fanout_error_response(&error),
    }
}

fn metadata_query_budget_error_response(error: &tsink::QueryBudgetError) -> HttpResponse {
    match error {
        tsink::QueryBudgetError::InvalidLimits(_) => metadata_error_response(
            400,
            "invalid_query_limits",
            "invalid_query_limits",
            "invalid public metadata query limits",
            None,
        ),
        tsink::QueryBudgetError::LimitExceeded(exceeded) => {
            let code = format!("query_limit_{}", exceeded.reason.as_str());
            let retryable = matches!(
                exceeded.reason,
                tsink::QueryLimitReason::ConcurrentQueries
                    | tsink::QueryLimitReason::SharedMemoryBytes
            );
            let message = format!(
                "public metadata query exceeded the {} limit",
                exceeded.reason.as_str()
            );
            metadata_error_response(
                if retryable { 429 } else { 413 },
                &code,
                &code,
                &message,
                retryable.then_some("1"),
            )
        }
        tsink::QueryBudgetError::Cancelled => metadata_error_response(
            503,
            "canceled",
            "query_cancelled",
            "public metadata query was canceled",
            None,
        ),
        tsink::QueryBudgetError::DeadlineExceeded => metadata_error_response(
            503,
            "timeout",
            "query_deadline_exceeded",
            "public metadata query deadline exceeded",
            None,
        ),
        _ => metadata_error_response(
            500,
            "execution",
            "metadata_query_budget_failed",
            "public metadata query budget failed",
            None,
        ),
    }
}

fn metadata_fanout_error_response(error: &ReadFanoutError) -> HttpResponse {
    match error {
        ReadFanoutError::InvalidRequest { .. } => metadata_error_response(
            400,
            "bad_data",
            "metadata_invalid_request",
            "invalid distributed public metadata request",
            None,
        ),
        ReadFanoutError::MergeLimitExceeded { .. } => metadata_error_response(
            413,
            "execution",
            "read_merge_limit_exceeded",
            "distributed public metadata merge limit exceeded",
            None,
        ),
        ReadFanoutError::ResourceLimitExceeded {
            retryable: true, ..
        } => metadata_error_response(
            429,
            "execution",
            "read_overloaded",
            "distributed public metadata read capacity is saturated",
            Some("1"),
        ),
        ReadFanoutError::ResourceLimitExceeded { .. } => metadata_error_response(
            413,
            "execution",
            "read_resource_limit_exceeded",
            "distributed public metadata request exceeds a read resource limit",
            None,
        ),
        ReadFanoutError::ConsistencyUnmet {
            mode: ClusterReadConsistency::Strict,
            ..
        } => metadata_error_response(
            409,
            "execution",
            "strict_consistency_unmet",
            "strict public metadata read consistency was not met",
            None,
        ),
        ReadFanoutError::ConsistencyUnmet { .. } => metadata_error_response(
            503,
            "execution",
            "read_consistency_unmet",
            "public metadata read consistency was not met",
            None,
        ),
        _ if error.retryable() => metadata_error_response(
            503,
            "execution",
            "metadata_fanout_unavailable",
            "distributed public metadata read is unavailable",
            Some("1"),
        ),
        _ => metadata_error_response(
            500,
            "execution",
            "metadata_fanout_failed",
            "distributed public metadata read failed",
            None,
        ),
    }
}

struct PublicMetadataCancellationGuard {
    token: tsink::QueryCancellationToken,
}

impl Drop for PublicMetadataCancellationGuard {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

fn begin_public_metadata_query(
    storage: &Arc<dyn Storage>,
) -> Result<(tsink::QueryExecution, PublicMetadataCancellationGuard), MetadataRequestError> {
    let cancellation = tsink::QueryCancellationToken::new();
    let execution = storage
        .begin_query_execution(
            tsink::QueryWorkLimits {
                max_returned_bytes: Some(
                    u64::try_from(PUBLIC_METADATA_MAX_RESPONSE_BYTES).unwrap_or(u64::MAX),
                ),
                ..tsink::QueryWorkLimits::default()
            },
            cancellation.clone(),
        )
        .map_err(|error| {
            MetadataRequestError::storage(
                "execution",
                "failed to begin public metadata query execution",
                error,
            )
        })?
        .ok_or_else(|| {
            MetadataRequestError::Execution(
                "public metadata queries require an execution-accounted storage backend",
            )
        })?;
    Ok((
        execution,
        PublicMetadataCancellationGuard {
            token: cancellation,
        },
    ))
}

fn ensure_local_metadata_accounting(
    storage: &Arc<dyn Storage>,
) -> Result<(), MetadataRequestError> {
    if storage.select_series_execution_accounting() != tsink::QueryExecutionAccounting::Complete {
        return Err(MetadataRequestError::Execution(
            "public metadata queries require complete select_series execution accounting",
        ));
    }
    Ok(())
}

fn ensure_cluster_metadata_accounting(
    storage: &Arc<dyn Storage>,
) -> Result<(), MetadataRequestError> {
    if storage.select_series_in_shards_execution_accounting()
        != tsink::QueryExecutionAccounting::Complete
    {
        return Err(MetadataRequestError::Execution(
            "distributed public metadata queries require complete shard-scoped select_series execution accounting",
        ));
    }
    Ok(())
}

fn saturating_u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn collection_growth_capacity_upper(len: usize) -> usize {
    if len == 0 {
        0
    } else if len <= 4 {
        4
    } else {
        len.checked_next_power_of_two().unwrap_or(usize::MAX)
    }
}

fn vec_growth_capacity_upper(current_capacity: usize, required_len: usize) -> usize {
    if required_len <= current_capacity {
        current_capacity
    } else if current_capacity == 0 {
        collection_growth_capacity_upper(required_len)
    } else {
        current_capacity.saturating_mul(2).max(required_len)
    }
}

fn modeled_vec_capacity_bytes<T>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    saturating_u64_from_usize(capacity)
        .saturating_mul(saturating_u64_from_usize(std::mem::size_of::<T>()))
        .saturating_add(PUBLIC_METADATA_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_string_retained_bytes(value: &String) -> u64 {
    if value.capacity() == 0 {
        0
    } else {
        saturating_u64_from_usize(value.capacity())
            .saturating_add(PUBLIC_METADATA_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_owned_str_retained_bytes(value: &str) -> u64 {
    if value.is_empty() {
        0
    } else {
        saturating_u64_from_usize(value.len())
            .saturating_add(PUBLIC_METADATA_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_string_len_retained_bytes(len: usize) -> u64 {
    if len == 0 {
        0
    } else {
        saturating_u64_from_usize(len)
            .saturating_add(PUBLIC_METADATA_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_percent_decode_peak_bytes(raw_len: usize, decoded_len: usize) -> u64 {
    // `percent_decode` first allocates a byte vector with the raw encoded capacity. Invalid UTF-8
    // then retains that vector while allocating a replacement String whose contents can grow to
    // three bytes for every decoded byte. The replacement String starts at the decoded capacity
    // and can double twice to reach that length; small byte vectors use an eight-byte growth floor.
    let lossy_capacity_upper = if decoded_len == 0 {
        0
    } else {
        decoded_len.saturating_mul(4).max(8)
    };
    modeled_string_len_retained_bytes(raw_len)
        .saturating_add(modeled_string_len_retained_bytes(lossy_capacity_upper))
}

fn modeled_label_heap_retained_bytes(label: &Label) -> u64 {
    modeled_string_retained_bytes(&label.name)
        .saturating_add(modeled_string_retained_bytes(&label.value))
}

fn modeled_metric_series_heap_retained_bytes(series: &MetricSeries) -> u64 {
    modeled_string_retained_bytes(&series.name)
        .saturating_add(modeled_vec_capacity_bytes::<Label>(
            series.labels.capacity(),
        ))
        .saturating_add(series.labels.iter().fold(0u64, |bytes, label| {
            bytes.saturating_add(modeled_label_heap_retained_bytes(label))
        }))
}

fn modeled_metric_series_vec_retained_bytes(series: &Vec<MetricSeries>) -> u64 {
    modeled_vec_capacity_bytes::<MetricSeries>(series.capacity()).saturating_add(
        series.iter().fold(0u64, |bytes, series| {
            bytes.saturating_add(modeled_metric_series_heap_retained_bytes(series))
        }),
    )
}

fn modeled_string_vec_retained_bytes(values: &Vec<String>) -> u64 {
    modeled_vec_capacity_bytes::<String>(values.capacity()).saturating_add(
        values.iter().fold(0u64, |bytes, value| {
            bytes.saturating_add(modeled_string_retained_bytes(value))
        }),
    )
}

fn modeled_series_matcher_heap_retained_bytes(matcher: &SeriesMatcher) -> u64 {
    modeled_string_retained_bytes(&matcher.name)
        .saturating_add(modeled_string_retained_bytes(&matcher.value))
}

fn modeled_series_selection_heap_retained_bytes(selection: &SeriesSelection) -> u64 {
    selection
        .metric
        .as_ref()
        .map(modeled_string_retained_bytes)
        .unwrap_or(0)
        .saturating_add(modeled_vec_capacity_bytes::<SeriesMatcher>(
            selection.matchers.capacity(),
        ))
        .saturating_add(selection.matchers.iter().fold(0u64, |bytes, matcher| {
            bytes.saturating_add(modeled_series_matcher_heap_retained_bytes(matcher))
        }))
}

fn modeled_series_selection_vec_retained_bytes(selections: &Vec<SeriesSelection>) -> u64 {
    modeled_vec_capacity_bytes::<SeriesSelection>(selections.capacity()).saturating_add(
        selections.iter().fold(0u64, |bytes, selection| {
            bytes.saturating_add(modeled_series_selection_heap_retained_bytes(selection))
        }),
    )
}

fn modeled_matcher_decode_preflight_bytes(request: &HttpRequest, matcher_count: usize) -> u64 {
    // The request path/body already belong to the bounded HTTP input envelope. Percent decoding
    // cannot consume more than twice their combined size at once: retained decoded strings plus
    // the current decoder buffer (including invalid-UTF-8 replacement). Reserve that allocation
    // and the exact-capacity String vector before `param_all` creates either.
    let encoded_envelope_bytes =
        saturating_u64_from_usize(request.path.len().saturating_add(request.body.len()));
    // `param_all` can grow a query-parameter vector again when form parameters are appended.
    // Invalid UTF-8 replacement can also hold the raw decoder allocation beside a three-times
    // larger lossy string. Two final slot vectors plus four encoded envelopes cover both peaks.
    modeled_vec_capacity_bytes::<String>(matcher_count)
        .saturating_mul(2)
        .saturating_add(encoded_envelope_bytes.saturating_mul(4))
        .saturating_add(
            saturating_u64_from_usize(matcher_count)
                .saturating_mul(PUBLIC_METADATA_COLLECTION_ALLOCATION_ALLOWANCE_BYTES),
        )
}

fn metadata_matcher_token_count_upper(input: &str) -> usize {
    input
        .len()
        .saturating_add(1)
        .min(tsink::promql::MAX_PARSE_TOKENS.saturating_add(1))
}

fn modeled_matcher_selection_preflight_bytes(input: &str) -> u64 {
    let token_count = metadata_matcher_token_count_upper(input);
    let matcher_capacity = collection_growth_capacity_upper(token_count);
    let string_allocations = saturating_u64_from_usize(token_count)
        .saturating_mul(2)
        .saturating_add(1);
    modeled_vec_capacity_bytes::<SeriesMatcher>(matcher_capacity)
        .saturating_add(saturating_u64_from_usize(input.len()).saturating_mul(2))
        .saturating_add(
            string_allocations
                .saturating_mul(PUBLIC_METADATA_COLLECTION_ALLOCATION_ALLOWANCE_BYTES),
        )
}

fn modeled_matcher_parser_preflight_bytes(input: &str) -> u64 {
    let token_count = metadata_matcher_token_count_upper(input);
    let token_slots = modeled_vec_capacity_bytes::<tsink::promql::lexer::Token>(
        collection_growth_capacity_upper(token_count),
    );
    let per_token_inline = std::mem::size_of::<tsink::promql::ast::Expr>()
        .saturating_add(std::mem::size_of::<tsink::promql::ast::LabelMatcher>());
    let per_token_allocations =
        PUBLIC_METADATA_COLLECTION_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(6);
    token_slots
        .saturating_add(
            saturating_u64_from_usize(token_count)
                .saturating_mul(saturating_u64_from_usize(per_token_inline))
                .saturating_add(
                    saturating_u64_from_usize(token_count).saturating_mul(per_token_allocations),
                ),
        )
        .saturating_add(saturating_u64_from_usize(input.len()).saturating_mul(4))
}

fn resize_query_memory(
    reservation: &mut tsink::QueryMemoryReservation,
    bytes: u64,
    context: &'static str,
) -> Result<(), MetadataRequestError> {
    reservation.resize(bytes).map_err(|error| match error {
        tsink::QueryBudgetError::LimitExceeded(_)
        | tsink::QueryBudgetError::InvalidLimits(_)
        | tsink::QueryBudgetError::Cancelled
        | tsink::QueryBudgetError::DeadlineExceeded => MetadataRequestError::Budget(error),
        _ => MetadataRequestError::Execution(context),
    })
}

#[derive(Debug)]
struct GuardedMetadataSelections {
    values: Vec<SeriesSelection>,
    _reservation: tsink::QueryMemoryReservation,
}

fn bounded_metadata_matcher_parse_diagnostic(matcher_index: usize) -> String {
    let diagnostic = format!("invalid match expression at index {matcher_index}");
    debug_assert!(diagnostic.len() <= PUBLIC_METADATA_MAX_DIAGNOSTIC_BYTES);
    diagnostic
}

fn guarded_bad_data_error(
    message: String,
    reservation: tsink::QueryMemoryReservation,
) -> MetadataRequestError {
    MetadataRequestError::GuardedBadData {
        message,
        reservation,
    }
}

fn guarded_metadata_matcher_selections(
    request: &HttpRequest,
    matcher_count: usize,
    default_if_empty: bool,
    execution: &tsink::QueryExecution,
) -> Result<GuardedMetadataSelections, MetadataRequestError> {
    let mut reservation = execution
        .reserve_memory(0)
        .map_err(MetadataRequestError::Budget)?;
    resize_query_memory(
        &mut reservation,
        modeled_matcher_decode_preflight_bytes(request, matcher_count),
        "failed to reserve public metadata matcher decoding memory",
    )?;
    let matchers = request.param_all("match[]");
    if matchers.len() != matcher_count {
        return Err(MetadataRequestError::Execution(
            "public metadata matcher count changed after admission",
        ));
    }

    let decoded_retained_bytes = modeled_string_vec_retained_bytes(&matchers);
    let selection_count = if matchers.is_empty() {
        usize::from(default_if_empty)
    } else {
        matchers.len()
    };
    let selection_slots = modeled_vec_capacity_bytes::<SeriesSelection>(selection_count);
    let selection_heap_upper = matchers.iter().fold(0u64, |bytes, matcher| {
        bytes.saturating_add(modeled_matcher_selection_preflight_bytes(matcher))
    });
    let parser_transient_upper = matchers
        .iter()
        .map(|matcher| modeled_matcher_parser_preflight_bytes(matcher))
        .max()
        .unwrap_or(0);
    resize_query_memory(
        &mut reservation,
        decoded_retained_bytes
            .saturating_add(selection_slots)
            .saturating_add(selection_heap_upper)
            .saturating_add(parser_transient_upper),
        "failed to reserve public metadata matcher parser and selection memory",
    )?;

    let mut values = Vec::new();
    values.try_reserve_exact(selection_count).map_err(|_| {
        MetadataRequestError::Execution("failed to allocate public metadata matcher selections")
    })?;
    if matchers.is_empty() {
        if default_if_empty {
            values.push(SeriesSelection::new());
        }
    } else {
        for (matcher_index, matcher) in matchers.iter().enumerate() {
            let expression = match tsink::promql::parse(matcher) {
                Ok(expression) => expression,
                Err(_) => {
                    return Err(guarded_bad_data_error(
                        bounded_metadata_matcher_parse_diagnostic(matcher_index),
                        reservation,
                    ))
                }
            };
            let Some(selection) = expr_to_selection(&expression) else {
                return Err(guarded_bad_data_error(
                    format!("match expression at index {matcher_index} must be a vector selector"),
                    reservation,
                ));
            };
            values.push(selection);
        }
    }

    let retained_bytes = modeled_series_selection_vec_retained_bytes(&values);
    drop(matchers);
    resize_query_memory(
        &mut reservation,
        retained_bytes,
        "failed to adopt public metadata matcher selections",
    )?;
    Ok(GuardedMetadataSelections {
        values,
        _reservation: reservation,
    })
}

fn modeled_tenant_selections_upper_bytes(selection: &SeriesSelection, tenant_id: &str) -> u64 {
    let selection_count =
        1usize.saturating_add(usize::from(tenant_id == tenant::DEFAULT_TENANT_ID));
    let metric_bytes = selection
        .metric
        .as_deref()
        .map(modeled_owned_str_retained_bytes)
        .unwrap_or(0);
    let matcher_string_bytes = selection.matchers.iter().fold(0u64, |bytes, matcher| {
        bytes
            .saturating_add(modeled_owned_str_retained_bytes(&matcher.name))
            .saturating_add(modeled_owned_str_retained_bytes(&matcher.value))
    });
    let matcher_slots = modeled_vec_capacity_bytes::<SeriesMatcher>(
        selection
            .matchers
            .len()
            .saturating_mul(2)
            .max(4)
            .max(selection.matchers.len().saturating_add(1)),
    );
    let mut bytes = modeled_vec_capacity_bytes::<SeriesSelection>(selection_count);
    for index in 0..selection_count {
        let tenant_value = if index == 0 { tenant_id } else { ".+" };
        bytes = bytes
            .saturating_add(metric_bytes)
            .saturating_add(matcher_slots)
            .saturating_add(matcher_string_bytes)
            .saturating_add(modeled_owned_str_retained_bytes(tenant::TENANT_LABEL))
            .saturating_add(modeled_owned_str_retained_bytes(tenant_value));
    }
    bytes
}

#[derive(Debug)]
struct GuardedTenantSelections {
    values: Vec<SeriesSelection>,
    reservation: tsink::QueryMemoryReservation,
}

impl GuardedTenantSelections {
    fn new(
        selection: &SeriesSelection,
        tenant_id: &str,
        execution: &tsink::QueryExecution,
    ) -> Result<Self, MetadataRequestError> {
        let mut reservation = execution
            .reserve_memory(0)
            .map_err(MetadataRequestError::Budget)?;
        resize_query_memory(
            &mut reservation,
            modeled_tenant_selections_upper_bytes(selection, tenant_id),
            "failed to reserve public metadata tenant-selection memory",
        )?;
        let values = tenant::read_selections_for_tenant(selection, tenant_id).map_err(|_| {
            MetadataRequestError::BadData("invalid tenant-scoped public metadata selection")
        })?;
        resize_query_memory(
            &mut reservation,
            modeled_series_selection_vec_retained_bytes(&values),
            "failed to adopt public metadata tenant selections",
        )?;
        Ok(Self {
            values,
            reservation,
        })
    }

    fn reconcile_after_selection_drop(&mut self) -> Result<(), MetadataRequestError> {
        if self.values.is_empty() {
            self.values = Vec::new();
        }
        resize_query_memory(
            &mut self.reservation,
            modeled_series_selection_vec_retained_bytes(&self.values),
            "failed to release a public metadata tenant selection after fanout",
        )
    }
}

fn modeled_read_metadata_heap_retained_bytes(metadata: &ReadFanoutResponseMetadata) -> u64 {
    modeled_vec_capacity_bytes::<String>(metadata.warnings.capacity()).saturating_add(
        metadata.warnings.iter().fold(0u64, |bytes, warning| {
            bytes.saturating_add(modeled_string_retained_bytes(warning))
        }),
    )
}

fn modeled_read_metadata_merge_preflight_bytes(
    aggregate: &ReadFanoutResponseMetadata,
    update: &ReadFanoutResponseMetadata,
) -> u64 {
    let desired_len = aggregate
        .warnings
        .len()
        .saturating_add(update.warnings.len());
    let projected_capacity = vec_growth_capacity_upper(aggregate.warnings.capacity(), desired_len);
    let projected_aggregate = modeled_vec_capacity_bytes::<String>(projected_capacity)
        .saturating_add(aggregate.warnings.iter().fold(0u64, |bytes, warning| {
            bytes.saturating_add(modeled_string_retained_bytes(warning))
        }))
        .saturating_add(update.warnings.iter().fold(0u64, |bytes, warning| {
            bytes.saturating_add(modeled_owned_str_retained_bytes(warning))
        }));
    modeled_read_metadata_heap_retained_bytes(update).saturating_add(projected_aggregate)
}

fn modeled_read_metadata_response_headers_bytes() -> u64 {
    let header_names = [
        READ_CONSISTENCY_HEADER,
        READ_PARTIAL_RESPONSE_POLICY_HEADER,
        READ_PARTIAL_RESPONSE_HEADER,
        READ_PARTIAL_WARNINGS_HEADER,
    ];
    let header_value_max_lengths = [32usize, 32, 5, 20];
    // The response already owns its Content-Type entry. Adding four cluster headers can grow the
    // existing four-slot vector to eight slots while the old allocation is still live; the base
    // response reservation accounts that old allocation and this reservation accounts the new.
    modeled_vec_capacity_bytes::<(String, String)>(collection_growth_capacity_upper(
        header_names.len().saturating_add(1),
    ))
    .saturating_add(header_names.iter().fold(0u64, |bytes, name| {
        bytes.saturating_add(modeled_owned_str_retained_bytes(name))
    }))
    .saturating_add(
        header_value_max_lengths
            .iter()
            .fold(0u64, |bytes, value_len| {
                bytes.saturating_add(modeled_string_len_retained_bytes(*value_len))
            }),
    )
}

fn modeled_metadata_base_response_header_bytes() -> u64 {
    modeled_vec_capacity_bytes::<(String, String)>(collection_growth_capacity_upper(1))
        .saturating_add(modeled_owned_str_retained_bytes("Content-Type"))
        .saturating_add(modeled_owned_str_retained_bytes("application/json"))
}

#[derive(Debug)]
struct GuardedReadMetadata {
    value: ReadFanoutResponseMetadata,
    reservation: tsink::QueryMemoryReservation,
}

impl GuardedReadMetadata {
    fn new(
        read_fanout: &ReadFanoutExecutor,
        execution: &tsink::QueryExecution,
    ) -> Result<Self, MetadataRequestError> {
        Ok(Self {
            value: default_read_response_metadata(read_fanout),
            reservation: execution
                .reserve_memory(0)
                .map_err(MetadataRequestError::Budget)?,
        })
    }

    fn merge(&mut self, update: &ReadFanoutResponseMetadata) -> Result<(), MetadataRequestError> {
        resize_query_memory(
            &mut self.reservation,
            modeled_read_metadata_merge_preflight_bytes(&self.value, update),
            "failed to reserve public metadata fanout-warning merge memory",
        )?;
        self.value.consistency = update.consistency;
        self.value.partial_response_policy = update.partial_response_policy;
        self.value.partial_response |= update.partial_response;
        if !update.warnings.is_empty() {
            self.value.warnings.extend(update.warnings.iter().cloned());
            self.value.warnings.sort_unstable();
            self.value.warnings.dedup();
        }
        Ok(())
    }

    fn reconcile_after_update_drop(&mut self) -> Result<(), MetadataRequestError> {
        resize_query_memory(
            &mut self.reservation,
            modeled_read_metadata_heap_retained_bytes(&self.value),
            "failed to adopt public metadata fanout warnings",
        )
    }

    fn reserve_response_headers(&mut self) -> Result<(), MetadataRequestError> {
        resize_query_memory(
            &mut self.reservation,
            modeled_read_metadata_heap_retained_bytes(&self.value)
                .saturating_add(modeled_read_metadata_response_headers_bytes()),
            "failed to reserve public metadata response-header memory",
        )
    }
}

#[derive(Debug)]
struct GuardedOwnedMetadataString {
    value: String,
    _reservation: tsink::QueryMemoryReservation,
}

impl GuardedOwnedMetadataString {
    fn new(value: &str, execution: &tsink::QueryExecution) -> Result<Self, MetadataRequestError> {
        let mut reservation = execution
            .reserve_memory(modeled_owned_str_retained_bytes(value))
            .map_err(MetadataRequestError::Budget)?;
        let value = value.to_string();
        resize_query_memory(
            &mut reservation,
            modeled_string_retained_bytes(&value),
            "failed to adopt public metadata task-owned string",
        )?;
        Ok(Self {
            value,
            _reservation: reservation,
        })
    }
}

#[derive(Debug)]
struct GuardedSeriesSet {
    values: Vec<MetricSeries>,
    reservation: tsink::QueryMemoryReservation,
    heap_retained_bytes: u64,
}

impl GuardedSeriesSet {
    fn new(execution: &tsink::QueryExecution) -> Result<Self, MetadataRequestError> {
        Ok(Self {
            values: Vec::new(),
            reservation: execution
                .reserve_memory(0)
                .map_err(MetadataRequestError::Budget)?,
            heap_retained_bytes: 0,
        })
    }

    fn insert(
        &mut self,
        execution: &tsink::QueryExecution,
        mut series: MetricSeries,
    ) -> Result<(), MetadataRequestError> {
        series.labels.sort_unstable();
        let insertion_index = match self.values.binary_search(&series) {
            Ok(_) => return Ok(()),
            Err(index) => index,
        };
        let requested_len = self.values.len().saturating_add(1);
        execution
            .observe_intermediate_vector_size(saturating_u64_from_usize(requested_len))
            .map_err(MetadataRequestError::Budget)?;
        let entry_heap = modeled_metric_series_heap_retained_bytes(&series);
        let current_slots = modeled_vec_capacity_bytes::<MetricSeries>(self.values.capacity());
        let requested_slots = modeled_vec_capacity_bytes::<MetricSeries>(
            vec_growth_capacity_upper(self.values.capacity(), requested_len),
        );
        let allocation_overlap = usize::from(self.values.len() == self.values.capacity());
        let preflight_slots = current_slots.saturating_add(
            requested_slots.saturating_mul(saturating_u64_from_usize(allocation_overlap)),
        );
        resize_query_memory(
            &mut self.reservation,
            self.heap_retained_bytes
                .saturating_add(entry_heap)
                .saturating_add(preflight_slots),
            "failed to reserve public metadata series dedupe memory",
        )?;
        self.values.try_reserve_exact(1).map_err(|_| {
            MetadataRequestError::Execution("failed to allocate public metadata series dedupe")
        })?;
        let next_heap_retained_bytes = self.heap_retained_bytes.saturating_add(entry_heap);
        resize_query_memory(
            &mut self.reservation,
            next_heap_retained_bytes.saturating_add(modeled_vec_capacity_bytes::<MetricSeries>(
                self.values.capacity(),
            )),
            "failed to adopt public metadata series dedupe memory",
        )?;
        self.values.insert(insertion_index, series);
        self.heap_retained_bytes = next_heap_retained_bytes;
        Ok(())
    }

    fn into_vec(mut self) -> Result<GuardedSeriesVec, MetadataRequestError> {
        let retained_bytes = modeled_metric_series_vec_retained_bytes(&self.values);
        resize_query_memory(
            &mut self.reservation,
            retained_bytes,
            "failed to adopt public metadata series output",
        )?;
        Ok(GuardedSeriesVec {
            values: self.values,
            reservation: self.reservation,
            retained_bytes,
        })
    }
}

#[derive(Debug)]
struct GuardedStringSet {
    values: Vec<String>,
    reservation: tsink::QueryMemoryReservation,
    retained_bytes: u64,
}

impl GuardedStringSet {
    fn new(execution: &tsink::QueryExecution) -> Result<Self, MetadataRequestError> {
        Ok(Self {
            values: Vec::new(),
            reservation: execution
                .reserve_memory(0)
                .map_err(MetadataRequestError::Budget)?,
            retained_bytes: 0,
        })
    }

    fn insert(
        &mut self,
        execution: &tsink::QueryExecution,
        value: &str,
    ) -> Result<(), MetadataRequestError> {
        let insertion_index = match self
            .values
            .binary_search_by(|candidate| candidate.as_str().cmp(value))
        {
            Ok(_) => return Ok(()),
            Err(index) => index,
        };
        let requested_len = self.values.len().saturating_add(1);
        execution
            .observe_intermediate_vector_size(saturating_u64_from_usize(requested_len))
            .map_err(MetadataRequestError::Budget)?;
        let entry_heap = modeled_owned_str_retained_bytes(value);
        let current_slots = modeled_vec_capacity_bytes::<String>(self.values.capacity());
        let requested_slots = modeled_vec_capacity_bytes::<String>(vec_growth_capacity_upper(
            self.values.capacity(),
            requested_len,
        ));
        let allocation_overlap = usize::from(self.values.len() == self.values.capacity());
        let preflight_slots = current_slots.saturating_add(
            requested_slots.saturating_mul(saturating_u64_from_usize(allocation_overlap)),
        );
        resize_query_memory(
            &mut self.reservation,
            self.retained_bytes
                .saturating_add(entry_heap)
                .saturating_add(preflight_slots),
            "failed to reserve public metadata string dedupe memory",
        )?;
        self.values.try_reserve_exact(1).map_err(|_| {
            MetadataRequestError::Execution("failed to allocate public metadata string dedupe")
        })?;
        let owned = value.to_string();
        let next_retained = self
            .retained_bytes
            .saturating_add(modeled_string_retained_bytes(&owned));
        resize_query_memory(
            &mut self.reservation,
            next_retained
                .saturating_add(modeled_vec_capacity_bytes::<String>(self.values.capacity())),
            "failed to adopt public metadata string dedupe memory",
        )?;
        self.values.insert(insertion_index, owned);
        self.retained_bytes = next_retained;
        Ok(())
    }

    fn into_vec(mut self) -> Result<GuardedStringVec, MetadataRequestError> {
        let retained_bytes = modeled_string_vec_retained_bytes(&self.values);
        resize_query_memory(
            &mut self.reservation,
            retained_bytes,
            "failed to adopt public metadata string output",
        )?;
        Ok(GuardedStringVec {
            values: self.values,
            reservation: self.reservation,
            retained_bytes,
        })
    }
}

#[derive(Debug)]
struct GuardedSeriesVec {
    values: Vec<MetricSeries>,
    reservation: tsink::QueryMemoryReservation,
    retained_bytes: u64,
}

#[derive(Debug)]
struct GuardedStringVec {
    values: Vec<String>,
    reservation: tsink::QueryMemoryReservation,
    retained_bytes: u64,
}

enum MetadataProjection {
    Series(GuardedSeriesSet),
    LabelNames(GuardedStringSet),
    LabelValues {
        label_name: String,
        values: GuardedStringSet,
    },
}

impl MetadataProjection {
    fn series(execution: &tsink::QueryExecution) -> Result<Self, MetadataRequestError> {
        Ok(Self::Series(GuardedSeriesSet::new(execution)?))
    }

    fn label_names(execution: &tsink::QueryExecution) -> Result<Self, MetadataRequestError> {
        let mut names = GuardedStringSet::new(execution)?;
        names.insert(execution, "__name__")?;
        Ok(Self::LabelNames(names))
    }

    fn label_values(
        execution: &tsink::QueryExecution,
        label_name: &str,
    ) -> Result<Self, MetadataRequestError> {
        let mut values = GuardedStringSet::new(execution)?;
        let label_name_retained_bytes = modeled_owned_str_retained_bytes(label_name);
        resize_query_memory(
            &mut values.reservation,
            label_name_retained_bytes,
            "failed to reserve public metadata label-name memory",
        )?;
        let label_name = label_name.to_string();
        let label_name_retained_bytes = modeled_string_retained_bytes(&label_name);
        resize_query_memory(
            &mut values.reservation,
            label_name_retained_bytes,
            "failed to adopt public metadata label-name memory",
        )?;
        values.retained_bytes = label_name_retained_bytes;
        Ok(Self::LabelValues { label_name, values })
    }

    fn insert(
        &mut self,
        execution: &tsink::QueryExecution,
        series: MetricSeries,
    ) -> Result<(), MetadataRequestError> {
        match self {
            Self::Series(values) => values.insert(execution, series),
            Self::LabelNames(values) => {
                for label in &series.labels {
                    values.insert(execution, &label.name)?;
                }
                Ok(())
            }
            Self::LabelValues { label_name, values } if label_name == "__name__" => {
                values.insert(execution, &series.name)
            }
            Self::LabelValues { label_name, values } => {
                for label in &series.labels {
                    if label.name == *label_name {
                        values.insert(execution, &label.value)?;
                    }
                }
                Ok(())
            }
        }
    }
}

fn visible_cluster_series(mut series: MetricSeries, tenant_id: &str) -> Option<MetricSeries> {
    let mut matched = false;
    let mut wrong_tenant = false;
    series.labels.retain(|label| {
        if label.name != tenant::TENANT_LABEL {
            return true;
        }
        if label.value == tenant_id {
            matched = true;
        } else {
            wrong_tenant = true;
        }
        false
    });
    (!wrong_tenant && (matched || tenant_id == tenant::DEFAULT_TENANT_ID)).then_some(series)
}

fn ingest_local_series_result(
    execution: &tsink::QueryExecution,
    mut selected: tsink::SelectSeriesExecutionResult,
    projection: &mut MetadataProjection,
) -> Result<(), MetadataRequestError> {
    let reservation = selected.take_memory_reservation().ok_or_else(|| {
        MetadataRequestError::Execution(
            "completely accounted public metadata selection omitted its result reservation",
        )
    })?;
    if !selected.series.is_empty() && reservation.bytes() == 0 {
        return Err(MetadataRequestError::Execution(
            "completely accounted public metadata selection retained zero bytes for a non-empty result",
        ));
    }
    for series in std::mem::take(&mut selected.series) {
        projection.insert(execution, series)?;
    }
    drop(reservation);
    Ok(())
}

fn ingest_cluster_series_result(
    execution: &tsink::QueryExecution,
    mut selected: AccountedMetricSeries,
    tenant_id: &str,
    projection: &mut MetadataProjection,
) -> Result<(), MetadataRequestError> {
    let reservation = selected.take_reservation().ok_or_else(|| {
        MetadataRequestError::Execution(
            "completely accounted distributed public metadata selection omitted its result reservation",
        )
    })?;
    if !selected.series.is_empty() && reservation.bytes() == 0 {
        return Err(MetadataRequestError::Execution(
            "completely accounted distributed public metadata selection retained zero bytes for a non-empty result",
        ));
    }
    for series in std::mem::take(&mut selected.series) {
        if let Some(series) = visible_cluster_series(series, tenant_id) {
            projection.insert(execution, series)?;
        }
    }
    drop(reservation);
    Ok(())
}

struct JsonLengthCounter<'a> {
    bytes: usize,
    execution: &'a tsink::QueryExecution,
    control_error: Option<tsink::QueryBudgetError>,
}

impl Write for JsonLengthCounter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.execution.checkpoint() {
            self.control_error = Some(error);
            return Err(io::Error::other(
                "public metadata JSON measurement was canceled",
            ));
        }
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("JSON response length overflowed usize"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct ControlledJsonWriter<'a, W> {
    inner: W,
    execution: &'a tsink::QueryExecution,
    control_error: Option<tsink::QueryBudgetError>,
}

impl<W: Write> Write for ControlledJsonWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.execution.checkpoint() {
            self.control_error = Some(error);
            return Err(io::Error::other(
                "public metadata JSON serialization was canceled",
            ));
        }
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct DisplayValue<'a, T>(&'a T);

impl<T: Display> Serialize for DisplayValue<'_, T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self.0)
    }
}

struct SeriesResponseData<'a>(&'a [MetricSeries]);

impl Serialize for SeriesResponseData<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for series in self.0 {
            sequence.serialize_element(&SeriesResponseEntry(series))?;
        }
        sequence.end()
    }
}

struct SeriesResponseEntry<'a>(&'a MetricSeries);

impl Serialize for SeriesResponseEntry<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let entry_count = 1usize.saturating_add(
            self.0
                .labels
                .iter()
                .enumerate()
                .filter(|(index, label)| {
                    label.name != "__name__"
                        && !self.0.labels[..*index]
                            .iter()
                            .any(|previous| previous.name == label.name)
                })
                .count(),
        );
        let mut map = serializer.serialize_map(Some(entry_count))?;
        let mut previous_name: Option<&str> = None;
        loop {
            let mut next_name = if previous_name.is_none_or(|previous| "__name__" > previous) {
                Some("__name__")
            } else {
                None
            };
            for label in &self.0.labels {
                let name = label.name.as_str();
                if previous_name.is_some_and(|previous| name <= previous) {
                    continue;
                }
                if next_name.is_none_or(|next| name < next) {
                    next_name = Some(name);
                }
            }
            let Some(name) = next_name else {
                break;
            };
            let mut value = if name == "__name__" {
                Some(self.0.name.as_str())
            } else {
                None
            };
            for label in &self.0.labels {
                if label.name == name {
                    value = Some(label.value.as_str());
                }
            }
            map.serialize_entry(
                name,
                value.expect("selected public metadata series key has a value"),
            )?;
            previous_name = Some(name);
        }
        map.end()
    }
}

#[derive(Serialize)]
struct MetadataSuccessPayload<'a, T: ?Sized> {
    status: &'static str,
    data: &'a T,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterPartialResponsePayload<'a> {
    enabled: bool,
    policy: DisplayValue<'a, ClusterReadPartialResponsePolicy>,
    consistency: DisplayValue<'a, ClusterReadConsistency>,
    warning_count: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterMetadataSuccessPayload<'a, T: ?Sized> {
    status: &'static str,
    data: &'a T,
    partial_response: ClusterPartialResponsePayload<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    warnings: Option<&'a [String]>,
}

fn serialize_metadata_payload<T: Serialize + ?Sized>(
    execution: &tsink::QueryExecution,
    reservation: &mut tsink::QueryMemoryReservation,
    retained_output_bytes: u64,
    data: &T,
    metadata: Option<&ReadFanoutResponseMetadata>,
) -> Result<(Vec<u8>, u64), MetadataRequestError> {
    let serialize = |writer: &mut dyn Write| -> Result<(), serde_json::Error> {
        if let Some(metadata) = metadata {
            let payload = ClusterMetadataSuccessPayload {
                status: "success",
                data,
                partial_response: ClusterPartialResponsePayload {
                    enabled: metadata.partial_response,
                    policy: DisplayValue(&metadata.partial_response_policy),
                    consistency: DisplayValue(&metadata.consistency),
                    warning_count: metadata.warnings.len(),
                },
                warnings: (metadata.partial_response && !metadata.warnings.is_empty())
                    .then_some(metadata.warnings.as_slice()),
            };
            serde_json::to_writer(writer, &payload)
        } else {
            serde_json::to_writer(
                writer,
                &MetadataSuccessPayload {
                    status: "success",
                    data,
                },
            )
        }
    };

    let mut counter = JsonLengthCounter {
        bytes: 0,
        execution,
        control_error: None,
    };
    if serialize(&mut counter).is_err() {
        return Err(match counter.control_error {
            Some(error) => MetadataRequestError::Budget(error),
            None => {
                MetadataRequestError::Execution("failed to measure public metadata JSON response")
            }
        });
    }
    let body_len = counter.bytes;
    execution
        .charge_returned_bytes(saturating_u64_from_usize(body_len))
        .map_err(MetadataRequestError::Budget)?;

    let requested_body_bytes = modeled_vec_capacity_bytes::<u8>(body_len);
    resize_query_memory(
        reservation,
        retained_output_bytes.saturating_add(requested_body_bytes),
        "failed to reserve public metadata JSON response body",
    )?;
    let mut body = Vec::new();
    body.try_reserve_exact(body_len).map_err(|_| {
        MetadataRequestError::Execution("failed to allocate public metadata JSON response body")
    })?;
    let body_retained_bytes = modeled_vec_capacity_bytes::<u8>(body.capacity());
    resize_query_memory(
        reservation,
        retained_output_bytes.saturating_add(body_retained_bytes),
        "failed to adopt public metadata JSON response body",
    )?;
    body.resize(body_len, 0);
    let written = {
        let cursor = io::Cursor::new(body.as_mut_slice());
        let mut writer = ControlledJsonWriter {
            inner: cursor,
            execution,
            control_error: None,
        };
        if serialize(&mut writer).is_err() {
            return Err(match writer.control_error {
                Some(error) => MetadataRequestError::Budget(error),
                None => MetadataRequestError::Execution(
                    "failed to serialize public metadata JSON response",
                ),
            });
        }
        usize::try_from(writer.inner.position()).unwrap_or(usize::MAX)
    };
    if written != body_len {
        return Err(MetadataRequestError::Execution(
            "public metadata JSON response length changed after allocation preflight",
        ));
    }
    Ok((body, body_retained_bytes))
}

struct PreparedMetadataResponse {
    body: Vec<u8>,
    result_units: usize,
    metadata: Option<GuardedReadMetadata>,
    reservation: tsink::QueryMemoryReservation,
}

impl PreparedMetadataResponse {
    fn into_http_response(self) -> HttpResponse {
        let Self {
            body,
            result_units: _,
            metadata,
            reservation,
        } = self;
        let response = HttpResponse::new(200, body).with_header("Content-Type", "application/json");
        let response = match metadata.as_ref() {
            Some(metadata) => with_read_metadata_headers(response, &metadata.value),
            None => response,
        };
        drop(metadata);
        drop(reservation);
        response
    }
}

fn finish_metadata_projection(
    execution: &tsink::QueryExecution,
    projection: MetadataProjection,
    mut metadata: Option<GuardedReadMetadata>,
) -> Result<PreparedMetadataResponse, MetadataRequestError> {
    match projection {
        MetadataProjection::Series(values) => {
            let GuardedSeriesVec {
                values,
                mut reservation,
                retained_bytes,
            } = values.into_vec()?;
            let result_units = values.len();
            let data = SeriesResponseData(&values);
            let (body, body_retained_bytes) = serialize_metadata_payload(
                execution,
                &mut reservation,
                retained_bytes,
                &data,
                metadata.as_ref().map(|metadata| &metadata.value),
            )?;
            drop(values);
            resize_query_memory(
                &mut reservation,
                body_retained_bytes.saturating_add(modeled_metadata_base_response_header_bytes()),
                "failed to release public metadata series output memory",
            )?;
            if let Some(metadata) = &mut metadata {
                metadata.reserve_response_headers()?;
            }
            Ok(PreparedMetadataResponse {
                body,
                result_units,
                metadata,
                reservation,
            })
        }
        MetadataProjection::LabelNames(values) | MetadataProjection::LabelValues { values, .. } => {
            let GuardedStringVec {
                values,
                mut reservation,
                retained_bytes,
            } = values.into_vec()?;
            let result_units = values.len();
            let (body, body_retained_bytes) = serialize_metadata_payload(
                execution,
                &mut reservation,
                retained_bytes,
                values.as_slice(),
                metadata.as_ref().map(|metadata| &metadata.value),
            )?;
            drop(values);
            resize_query_memory(
                &mut reservation,
                body_retained_bytes.saturating_add(modeled_metadata_base_response_header_bytes()),
                "failed to release public metadata string output memory",
            )?;
            if let Some(metadata) = &mut metadata {
                metadata.reserve_response_headers()?;
            }
            Ok(PreparedMetadataResponse {
                body,
                result_units,
                metadata,
                reservation,
            })
        }
    }
}

pub(crate) async fn handle_series(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(_) => return metadata_read_admission_unavailable_response(),
    };
    handle_series_with_admission(
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

pub(crate) async fn handle_series_with_admission(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
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
        managed_control_plane,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let matcher_count = request.param_count("match[]");
    if matcher_count == 0 {
        return metadata_error_response(
            422,
            "bad_data",
            "metadata_missing_matcher",
            "missing required parameter 'match[]'",
            None,
        );
    }
    if let Err(err) = tenant::enforce_metadata_matchers_quota(tenant_plan.policy(), matcher_count) {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Metadata,
            matcher_count.max(1),
            err.clone(),
        );
        return metadata_error_response(
            422,
            "bad_data",
            "metadata_matcher_limit_exceeded",
            "public metadata matcher count exceeds the tenant limit",
            None,
        );
    }
    let _tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Metadata,
        matcher_count.max(1),
        usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(matcher_count).await {
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

    let cluster_context =
        cluster_context.filter(|context| !context.runtime.local_reads_serve_global_queries);
    let local_storage = cluster_context
        .is_none()
        .then(|| tenant::scoped_storage(Arc::clone(storage), tenant_id.clone()));
    let execution_storage = local_storage.as_ref().unwrap_or(storage);
    let accounting = if cluster_context.is_some() {
        ensure_cluster_metadata_accounting(storage)
    } else {
        ensure_local_metadata_accounting(execution_storage)
    };
    if let Err(error) = accounting {
        return metadata_request_error_response(error);
    }
    let (execution, cancellation_guard) = match begin_public_metadata_query(execution_storage) {
        Ok(admission) => admission,
        Err(error) => return metadata_request_error_response(error),
    };
    let selections =
        match guarded_metadata_matcher_selections(request, matcher_count, false, &execution) {
            Ok(selections) => selections,
            Err(error) => return metadata_request_error_response(error),
        };

    let prepared = if let Some(cluster_context) = cluster_context {
        let ring_version = cluster_ring_version(Some(cluster_context));
        let read_fanout = match effective_read_fanout(cluster_context) {
            Ok(fanout) => fanout,
            Err(_) => {
                return metadata_error_response(
                    503,
                    "execution",
                    "metadata_cluster_topology_unavailable",
                    "cluster read fanout topology is unavailable",
                    None,
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
            Err(_) => {
                return metadata_error_response(
                    422,
                    "bad_data",
                    "metadata_invalid_cluster_read_policy",
                    "invalid cluster read policy override",
                    None,
                )
            }
        };
        let mut read_metadata = match GuardedReadMetadata::new(&read_fanout, &execution) {
            Ok(metadata) => metadata,
            Err(error) => return metadata_request_error_response(error),
        };
        let mut projection = match MetadataProjection::series(&execution) {
            Ok(projection) => projection,
            Err(error) => return metadata_request_error_response(error),
        };
        for requested_selection in &selections.values {
            let mut tenant_selections =
                match GuardedTenantSelections::new(requested_selection, &tenant_id, &execution) {
                    Ok(selections) => selections,
                    Err(error) => return metadata_request_error_response(error),
                };
            while let Some(selection) = tenant_selections.values.pop() {
                let response_result = read_fanout
                    .select_series_with_ring_version_detailed_accounted_with_execution(
                        storage,
                        &cluster_context.rpc_client,
                        &selection,
                        ring_version,
                        &execution,
                    )
                    .await;
                drop(selection);
                if let Err(error) = tenant_selections.reconcile_after_selection_drop() {
                    return metadata_request_error_response(error);
                }
                let response = match response_result {
                    Ok(response) => response,
                    Err(error) => {
                        return metadata_request_error_response(MetadataRequestError::Fanout(error))
                    }
                };
                let update_metadata = response.metadata;
                if let Err(error) = read_metadata.merge(&update_metadata) {
                    return metadata_request_error_response(error);
                }
                if let Err(error) = ingest_cluster_series_result(
                    &execution,
                    response.value,
                    &tenant_id,
                    &mut projection,
                ) {
                    return metadata_request_error_response(error);
                }
                drop(update_metadata);
                if let Err(error) = read_metadata.reconcile_after_update_drop() {
                    return metadata_request_error_response(error);
                }
            }
        }
        drop(selections);
        match finish_metadata_projection(&execution, projection, Some(read_metadata)) {
            Ok(prepared) => prepared,
            Err(error) => return metadata_request_error_response(error),
        }
    } else {
        let storage = local_storage.expect("local public metadata backend was selected");
        let task_execution = execution.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut projection = MetadataProjection::series(&task_execution)?;
            for selection in &selections.values {
                let selected = storage
                    .select_series_with_execution_result(selection, &task_execution)
                    .map_err(|error| {
                        MetadataRequestError::storage("execution", "series selection failed", error)
                    })?;
                ingest_local_series_result(&task_execution, selected, &mut projection)?;
            }
            drop(selections);
            finish_metadata_projection(&task_execution, projection, None)
        })
        .await;
        match result {
            Ok(Ok(prepared)) => prepared,
            Ok(Err(error)) => return metadata_request_error_response(error),
            Err(_) => {
                return metadata_error_response(
                    500,
                    "execution",
                    "metadata_task_failed",
                    "public metadata worker task failed",
                    None,
                )
            }
        }
    };

    let result_units = prepared.result_units;
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
    )
    .await;
    let response = prepared.into_http_response();
    drop(execution);
    drop(cancellation_guard);
    response
}

pub(crate) async fn handle_labels(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(_) => return metadata_read_admission_unavailable_response(),
    };
    handle_labels_with_admission(
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

pub(crate) async fn handle_labels_with_admission(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
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
        managed_control_plane,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let requested_matcher_count = request.param_count("match[]");
    let matcher_count = requested_matcher_count.max(1);
    if let Err(err) =
        tenant::enforce_metadata_matchers_quota(tenant_plan.policy(), requested_matcher_count)
    {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Metadata,
            matcher_count,
            err.clone(),
        );
        return metadata_error_response(
            422,
            "bad_data",
            "metadata_matcher_limit_exceeded",
            "public metadata matcher count exceeds the tenant limit",
            None,
        );
    }
    let _tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Metadata,
        matcher_count,
        usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(matcher_count).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Metadata,
                matcher_count,
                err.to_string(),
            );
            return read_admission_error_response(err);
        }
    };
    let cluster_context =
        cluster_context.filter(|context| !context.runtime.local_reads_serve_global_queries);
    let local_storage = cluster_context
        .is_none()
        .then(|| tenant::scoped_storage(Arc::clone(storage), tenant_id.clone()));
    let execution_storage = local_storage.as_ref().unwrap_or(storage);
    let accounting = if cluster_context.is_some() {
        ensure_cluster_metadata_accounting(storage)
    } else {
        ensure_local_metadata_accounting(execution_storage)
    };
    if let Err(error) = accounting {
        return metadata_request_error_response(error);
    }
    let (execution, cancellation_guard) = match begin_public_metadata_query(execution_storage) {
        Ok(admission) => admission,
        Err(error) => return metadata_request_error_response(error),
    };
    let selections = match guarded_metadata_matcher_selections(
        request,
        requested_matcher_count,
        true,
        &execution,
    ) {
        Ok(selections) => selections,
        Err(error) => return metadata_request_error_response(error),
    };
    let prepared = if let Some(cluster_context) = cluster_context {
        let ring_version = cluster_ring_version(Some(cluster_context));
        let read_fanout = match effective_read_fanout(cluster_context) {
            Ok(fanout) => fanout,
            Err(_) => {
                return metadata_error_response(
                    503,
                    "execution",
                    "metadata_cluster_topology_unavailable",
                    "cluster read fanout topology is unavailable",
                    None,
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
            Err(_) => {
                return metadata_error_response(
                    422,
                    "bad_data",
                    "metadata_invalid_cluster_read_policy",
                    "invalid cluster read policy override",
                    None,
                )
            }
        };
        let mut read_metadata = match GuardedReadMetadata::new(&read_fanout, &execution) {
            Ok(metadata) => metadata,
            Err(error) => return metadata_request_error_response(error),
        };
        let mut projection = match MetadataProjection::label_names(&execution) {
            Ok(projection) => projection,
            Err(error) => return metadata_request_error_response(error),
        };
        for requested_selection in &selections.values {
            let mut tenant_selections =
                match GuardedTenantSelections::new(requested_selection, &tenant_id, &execution) {
                    Ok(selections) => selections,
                    Err(error) => return metadata_request_error_response(error),
                };
            while let Some(selection) = tenant_selections.values.pop() {
                let response_result = read_fanout
                    .select_series_with_ring_version_detailed_accounted_with_execution(
                        storage,
                        &cluster_context.rpc_client,
                        &selection,
                        ring_version,
                        &execution,
                    )
                    .await;
                drop(selection);
                if let Err(error) = tenant_selections.reconcile_after_selection_drop() {
                    return metadata_request_error_response(error);
                }
                let response = match response_result {
                    Ok(response) => response,
                    Err(error) => {
                        return metadata_request_error_response(MetadataRequestError::Fanout(error))
                    }
                };
                let update_metadata = response.metadata;
                if let Err(error) = read_metadata.merge(&update_metadata) {
                    return metadata_request_error_response(error);
                }
                if let Err(error) = ingest_cluster_series_result(
                    &execution,
                    response.value,
                    &tenant_id,
                    &mut projection,
                ) {
                    return metadata_request_error_response(error);
                }
                drop(update_metadata);
                if let Err(error) = read_metadata.reconcile_after_update_drop() {
                    return metadata_request_error_response(error);
                }
            }
        }
        drop(selections);
        match finish_metadata_projection(&execution, projection, Some(read_metadata)) {
            Ok(prepared) => prepared,
            Err(error) => return metadata_request_error_response(error),
        }
    } else {
        let storage = local_storage.expect("local public metadata backend was selected");
        let task_execution = execution.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut projection = MetadataProjection::label_names(&task_execution)?;
            for selection in &selections.values {
                let selected = storage
                    .select_series_with_execution_result(selection, &task_execution)
                    .map_err(|error| {
                        MetadataRequestError::storage("execution", "label query failed", error)
                    })?;
                ingest_local_series_result(&task_execution, selected, &mut projection)?;
            }
            drop(selections);
            finish_metadata_projection(&task_execution, projection, None)
        })
        .await;
        match result {
            Ok(Ok(prepared)) => prepared,
            Ok(Err(error)) => return metadata_request_error_response(error),
            Err(_) => {
                return metadata_error_response(
                    500,
                    "execution",
                    "metadata_task_failed",
                    "public metadata worker task failed",
                    None,
                )
            }
        }
    };

    let result_units = prepared.result_units;
    record_query_pressure(&tenant_id, matcher_count, result_units);
    record_query_usage(
        usage_accounting,
        &tenant_id,
        "labels",
        request.path_without_query(),
        QueryUsageMetrics::new(
            matcher_count as u64,
            result_units as u64,
            elapsed_nanos_since(started),
            request.body.len() as u64,
        ),
    )
    .await;
    let response = prepared.into_http_response();
    drop(execution);
    drop(cancellation_guard);
    response
}

pub(crate) async fn handle_metadata(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    request: &HttpRequest,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(_) => return metadata_read_admission_unavailable_response(),
    };
    handle_metadata_with_admission(
        storage,
        metadata_store,
        request,
        tenant_registry,
        managed_control_plane,
        usage_accounting,
        read_admission,
    )
    .await
}

const PUBLIC_METADATA_LIMIT_INPUT_MAX_BYTES: usize = 32;

#[derive(Debug)]
struct GuardedMetricMetadataRequest {
    metric: Option<String>,
    limit: usize,
    _reservation: tsink::QueryMemoryReservation,
}

fn guarded_metric_metadata_request(
    request: &HttpRequest,
    execution: &tsink::QueryExecution,
) -> Result<GuardedMetricMetadataRequest, MetadataRequestError> {
    let raw_metric = request.raw_param("metric");
    let metric_decoded_len = raw_metric.map(percent_decoded_len).unwrap_or(0);
    if metric_decoded_len > tsink::label::MAX_METRIC_NAME_LEN {
        return Err(MetadataRequestError::BadData(
            "parameter 'metric' exceeds the public metadata hard byte limit",
        ));
    }
    let raw_limit = request.raw_param("limit");
    let limit_decoded_len = raw_limit.map(percent_decoded_len).unwrap_or(0);
    if limit_decoded_len > PUBLIC_METADATA_LIMIT_INPUT_MAX_BYTES {
        return Err(MetadataRequestError::BadData(
            "parameter 'limit' exceeds the public metadata hard byte limit",
        ));
    }

    let metric_decode_peak = raw_metric.map_or(0, |raw| {
        modeled_percent_decode_peak_bytes(raw.len(), metric_decoded_len)
    });
    let limit_decode_peak = raw_limit.map_or(0, |raw| {
        modeled_percent_decode_peak_bytes(raw.len(), limit_decoded_len)
    });
    let mut reservation = execution
        .reserve_memory(metric_decode_peak)
        .map_err(MetadataRequestError::Budget)?;
    let metric = request.param("metric");
    let metric_retained_bytes = metric
        .as_ref()
        .map(modeled_string_retained_bytes)
        .unwrap_or(0);
    resize_query_memory(
        &mut reservation,
        metric_retained_bytes.saturating_add(limit_decode_peak),
        "failed to reserve public metric metadata limit decoding memory",
    )?;
    let limit_text = request.param("limit");
    resize_query_memory(
        &mut reservation,
        metric_retained_bytes.saturating_add(
            limit_text
                .as_ref()
                .map(modeled_string_retained_bytes)
                .unwrap_or(0),
        ),
        "failed to adopt public metric metadata request parameters",
    )?;

    if metric.as_deref().is_some_and(str::is_empty) {
        return Err(MetadataRequestError::BadData(
            "parameter 'metric' must not be empty",
        ));
    }
    if metric
        .as_ref()
        .is_some_and(|metric| metric.len() > tsink::label::MAX_METRIC_NAME_LEN)
    {
        return Err(MetadataRequestError::BadData(
            "parameter 'metric' exceeds the public metadata hard byte limit",
        ));
    }
    let limit = match limit_text.as_deref() {
        None => METADATA_API_DEFAULT_LIMIT,
        Some(limit) => {
            let parsed = limit.parse::<usize>().map_err(|_| {
                MetadataRequestError::BadData("parameter 'limit' must be a positive integer")
            })?;
            if parsed == 0 {
                return Err(MetadataRequestError::BadData(
                    "parameter 'limit' must be greater than zero",
                ));
            }
            if parsed > METADATA_API_MAX_LIMIT {
                return Err(MetadataRequestError::BadData(
                    "parameter 'limit' exceeds the public metadata hard limit",
                ));
            }
            parsed
        }
    };
    drop(limit_text);
    resize_query_memory(
        &mut reservation,
        metric_retained_bytes,
        "failed to reconcile public metric metadata request parameters",
    )?;
    Ok(GuardedMetricMetadataRequest {
        metric,
        limit,
        _reservation: reservation,
    })
}

#[derive(Serialize)]
struct MetricMetadataApiEntry<'a> {
    #[serde(rename = "type")]
    metric_type: &'static str,
    help: &'a str,
    unit: &'a str,
}

struct MetricMetadataApiEntries<'a>(&'a MetricMetadataRecord);

impl Serialize for MetricMetadataApiEntries<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let record = self.0;
        let mut entries = serializer.serialize_seq(Some(1))?;
        entries.serialize_element(&MetricMetadataApiEntry {
            metric_type: metric_type_to_api_string(record.metric_type),
            help: &record.help,
            unit: &record.unit,
        })?;
        entries.end()
    }
}

struct MetricMetadataApiData<'a>(&'a [MetricMetadataRecord]);

impl Serialize for MetricMetadataApiData<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut data = serializer.serialize_map(Some(self.0.len()))?;
        let mut previous_name = None;
        for record in self.0 {
            let name = record.metric_family_name.as_str();
            if previous_name.is_some_and(|previous| previous >= name) {
                return Err(<S::Error as serde::ser::Error>::custom(
                    "metric metadata result is not strictly ordered",
                ));
            }
            data.serialize_entry(name, &MetricMetadataApiEntries(record))?;
            previous_name = Some(name);
        }
        data.end()
    }
}

fn metric_metadata_query_error_response(error: MetricMetadataQueryError) -> HttpResponse {
    match error {
        MetricMetadataQueryError::Budget(error) => metadata_query_budget_error_response(&error),
        MetricMetadataQueryError::StoreUnavailable => metadata_error_response(
            503,
            "execution",
            "metadata_store_unavailable",
            "metric metadata store is unavailable",
            Some("1"),
        ),
        MetricMetadataQueryError::RecordLimit => metadata_error_response(
            413,
            "execution",
            "metadata_query_records_limit",
            "metric metadata query record limit exceeded",
            None,
        ),
        MetricMetadataQueryError::ResultLimit => metadata_error_response(
            413,
            "execution",
            "metadata_query_result_bytes_limit",
            "metric metadata query result byte limit exceeded",
            None,
        ),
        MetricMetadataQueryError::Allocation => metadata_error_response(
            500,
            "execution",
            "metadata_query_allocation_failed",
            "metric metadata query allocation failed",
            None,
        ),
    }
}

pub(crate) async fn handle_metadata_with_admission(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    request: &HttpRequest,
    tenant_registry: Option<&tenant::TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
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
        managed_control_plane,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let scoped_storage = tenant::scoped_storage(Arc::clone(storage), tenant_id.clone());
    let (execution, cancellation_guard) = match begin_public_metadata_query(&scoped_storage) {
        Ok(admission) => admission,
        Err(error) => return metadata_request_error_response(error),
    };
    let query = match guarded_metric_metadata_request(request, &execution) {
        Ok(query) => query,
        Err(error) => return metadata_request_error_response(error),
    };
    let limit = query.limit;
    let _tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Metadata,
        limit.max(1),
        usage_accounting,
    ) {
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

    let task_tenant = match GuardedOwnedMetadataString::new(&tenant_id, &execution) {
        Ok(tenant) => tenant,
        Err(error) => return metadata_request_error_response(error),
    };
    let task_store = Arc::clone(metadata_store);
    let task_execution = execution.clone();
    let result = tokio::task::spawn_blocking(move || {
        task_store.query_with_execution_result(
            &task_tenant.value,
            query.metric.as_deref(),
            query.limit,
            &task_execution,
        )
    })
    .await;
    let records = match result {
        Ok(Ok(records)) => records,
        Ok(Err(error)) => return metric_metadata_query_error_response(error),
        Err(_) => {
            return metadata_error_response(
                500,
                "execution",
                "metadata_query_task_failed",
                "metric metadata query worker task failed",
                None,
            )
        }
    };
    let result_units = records.records().len();
    let mut response_reservation = match execution.reserve_memory(0) {
        Ok(reservation) => reservation,
        Err(error) => return metadata_request_error_response(MetadataRequestError::Budget(error)),
    };
    let data = MetricMetadataApiData(records.records());
    let (body, body_retained_bytes) =
        match serialize_metadata_payload(&execution, &mut response_reservation, 0, &data, None) {
            Ok(encoded) => encoded,
            Err(error) => return metadata_request_error_response(error),
        };
    if let Err(error) = resize_query_memory(
        &mut response_reservation,
        body_retained_bytes.saturating_add(modeled_metadata_base_response_header_bytes()),
        "failed to reserve metric metadata response headers",
    ) {
        return metadata_request_error_response(error);
    }
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
    )
    .await;
    if let Err(error) = execution.checkpoint() {
        return metadata_query_budget_error_response(&error);
    }
    let response = HttpResponse::new(200, body).with_header("Content-Type", "application/json");
    drop(records);
    drop(response_reservation);
    drop(execution);
    drop(cancellation_guard);
    response
}

pub(crate) async fn handle_label_values(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    label_name: &str,
    context: PublicReadContext<'_>,
) -> HttpResponse {
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(_) => return metadata_read_admission_unavailable_response(),
    };
    handle_label_values_with_admission(storage, request, label_name, context, read_admission).await
}

pub(crate) async fn handle_label_values_with_admission(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    label_name: &str,
    context: PublicReadContext<'_>,
    read_admission: &ReadAdmissionController,
) -> HttpResponse {
    let started = Instant::now();
    if label_name.is_empty() || label_name.len() > tsink::label::MAX_LABEL_NAME_LEN {
        return metadata_error_response(
            422,
            "bad_data",
            "metadata_invalid_label_name",
            "public metadata label name is empty or exceeds its hard byte limit",
            None,
        );
    }
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
    let requested_matcher_count = request.param_count("match[]");
    let matcher_count = requested_matcher_count.max(1);
    if let Err(err) =
        tenant::enforce_metadata_matchers_quota(tenant_plan.policy(), requested_matcher_count)
    {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Metadata,
            matcher_count,
            err.clone(),
        );
        return metadata_error_response(
            422,
            "bad_data",
            "metadata_matcher_limit_exceeded",
            "public metadata matcher count exceeds the tenant limit",
            None,
        );
    }
    let _tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Metadata,
        matcher_count,
        context.usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(matcher_count).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Metadata,
                matcher_count,
                err.to_string(),
            );
            return read_admission_error_response(err);
        }
    };
    let cluster_context = context
        .cluster_context
        .filter(|context| !context.runtime.local_reads_serve_global_queries);
    let local_storage = cluster_context
        .is_none()
        .then(|| tenant::scoped_storage(Arc::clone(storage), tenant_id.clone()));
    let execution_storage = local_storage.as_ref().unwrap_or(storage);
    let accounting = if cluster_context.is_some() {
        ensure_cluster_metadata_accounting(storage)
    } else {
        ensure_local_metadata_accounting(execution_storage)
    };
    if let Err(error) = accounting {
        return metadata_request_error_response(error);
    }
    let (execution, cancellation_guard) = match begin_public_metadata_query(execution_storage) {
        Ok(admission) => admission,
        Err(error) => return metadata_request_error_response(error),
    };
    let selections = match guarded_metadata_matcher_selections(
        request,
        requested_matcher_count,
        true,
        &execution,
    ) {
        Ok(selections) => selections,
        Err(error) => return metadata_request_error_response(error),
    };
    let prepared = if let Some(cluster_context) = cluster_context {
        let ring_version = cluster_ring_version(Some(cluster_context));
        let read_fanout = match effective_read_fanout(cluster_context) {
            Ok(fanout) => fanout,
            Err(_) => {
                return metadata_error_response(
                    503,
                    "execution",
                    "metadata_cluster_topology_unavailable",
                    "cluster read fanout topology is unavailable",
                    None,
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
            Err(_) => {
                return metadata_error_response(
                    422,
                    "bad_data",
                    "metadata_invalid_cluster_read_policy",
                    "invalid cluster read policy override",
                    None,
                )
            }
        };
        let mut read_metadata = match GuardedReadMetadata::new(&read_fanout, &execution) {
            Ok(metadata) => metadata,
            Err(error) => return metadata_request_error_response(error),
        };
        let mut projection = match MetadataProjection::label_values(&execution, label_name) {
            Ok(projection) => projection,
            Err(error) => return metadata_request_error_response(error),
        };
        for requested_selection in &selections.values {
            let mut tenant_selections =
                match GuardedTenantSelections::new(requested_selection, &tenant_id, &execution) {
                    Ok(selections) => selections,
                    Err(error) => return metadata_request_error_response(error),
                };
            while let Some(selection) = tenant_selections.values.pop() {
                let response_result = read_fanout
                    .select_series_with_ring_version_detailed_accounted_with_execution(
                        storage,
                        &cluster_context.rpc_client,
                        &selection,
                        ring_version,
                        &execution,
                    )
                    .await;
                drop(selection);
                if let Err(error) = tenant_selections.reconcile_after_selection_drop() {
                    return metadata_request_error_response(error);
                }
                let response = match response_result {
                    Ok(response) => response,
                    Err(error) => {
                        return metadata_request_error_response(MetadataRequestError::Fanout(error))
                    }
                };
                let update_metadata = response.metadata;
                if let Err(error) = read_metadata.merge(&update_metadata) {
                    return metadata_request_error_response(error);
                }
                if let Err(error) = ingest_cluster_series_result(
                    &execution,
                    response.value,
                    &tenant_id,
                    &mut projection,
                ) {
                    return metadata_request_error_response(error);
                }
                drop(update_metadata);
                if let Err(error) = read_metadata.reconcile_after_update_drop() {
                    return metadata_request_error_response(error);
                }
            }
        }
        drop(selections);
        match finish_metadata_projection(&execution, projection, Some(read_metadata)) {
            Ok(prepared) => prepared,
            Err(error) => return metadata_request_error_response(error),
        }
    } else {
        let label_name = match GuardedOwnedMetadataString::new(label_name, &execution) {
            Ok(label_name) => label_name,
            Err(error) => return metadata_request_error_response(error),
        };
        let storage = local_storage.expect("local public metadata backend was selected");
        let task_execution = execution.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut projection =
                MetadataProjection::label_values(&task_execution, &label_name.value)?;
            drop(label_name);
            for selection in &selections.values {
                let selected = storage
                    .select_series_with_execution_result(selection, &task_execution)
                    .map_err(|error| {
                        MetadataRequestError::storage(
                            "execution",
                            "label values query failed",
                            error,
                        )
                    })?;
                ingest_local_series_result(&task_execution, selected, &mut projection)?;
            }
            drop(selections);
            finish_metadata_projection(&task_execution, projection, None)
        })
        .await;
        match result {
            Ok(Ok(prepared)) => prepared,
            Ok(Err(error)) => return metadata_request_error_response(error),
            Err(_) => {
                return metadata_error_response(
                    500,
                    "execution",
                    "metadata_task_failed",
                    "public metadata worker task failed",
                    None,
                )
            }
        }
    };

    let result_units = prepared.result_units;
    record_query_pressure(&tenant_id, matcher_count, result_units);
    record_query_usage(
        context.usage_accounting,
        &tenant_id,
        "label_values",
        request.path_without_query(),
        QueryUsageMetrics::new(
            matcher_count as u64,
            result_units as u64,
            elapsed_nanos_since(started),
            request.body.len() as u64,
        ),
    )
    .await;
    let response = prepared.into_http_response();
    drop(execution);
    drop(cancellation_guard);
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::ReadAdmissionGuardrails;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tsink::{DataPoint, QueryBudgetLimits, QueryWorkLimits};

    const TEST_QUERY_MEMORY_BYTES: u64 = 16 * 1024 * 1024;
    const TEST_RETURNED_BYTES: u64 = 4 * 1024 * 1024;

    struct UnaccountedMetadataStorage {
        inner: Arc<dyn Storage>,
    }

    impl Storage for UnaccountedMetadataStorage {
        fn query_budget(&self) -> Option<tsink::QueryBudget> {
            self.inner.query_budget()
        }

        fn insert_rows(&self, rows: &[Row]) -> tsink::Result<()> {
            self.inner.insert_rows(rows)
        }

        fn select(
            &self,
            metric: &str,
            labels: &[Label],
            start: i64,
            end: i64,
        ) -> tsink::Result<Vec<DataPoint>> {
            self.inner.select(metric, labels, start, end)
        }

        fn select_with_options(
            &self,
            metric: &str,
            options: tsink::QueryOptions,
        ) -> tsink::Result<Vec<DataPoint>> {
            self.inner.select_with_options(metric, options)
        }

        fn select_all(
            &self,
            metric: &str,
            start: i64,
            end: i64,
        ) -> tsink::Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            self.inner.select_all(metric, start, end)
        }

        fn close(&self) -> tsink::Result<()> {
            self.inner.close()
        }
    }

    enum ControlledMetadataBehavior {
        BlockUntilCancelled {
            started: Arc<AtomicBool>,
            finished: Arc<AtomicBool>,
        },
        ReturnUnaccounted,
        Fail(String),
    }

    struct ControlledMetadataStorage {
        budget: tsink::QueryBudget,
        behavior: ControlledMetadataBehavior,
    }

    impl ControlledMetadataStorage {
        fn new(behavior: ControlledMetadataBehavior) -> Self {
            Self {
                budget: tsink::QueryBudget::new(finite_query_limits(
                    32,
                    TEST_RETURNED_BYTES,
                    TEST_QUERY_MEMORY_BYTES,
                ))
                .expect("controlled metadata query budget should build"),
                behavior,
            }
        }
    }

    impl Storage for ControlledMetadataStorage {
        fn query_budget(&self) -> Option<tsink::QueryBudget> {
            Some(self.budget.clone())
        }

        fn insert_rows(&self, _rows: &[Row]) -> tsink::Result<()> {
            Ok(())
        }

        fn select(
            &self,
            _metric: &str,
            _labels: &[Label],
            _start: i64,
            _end: i64,
        ) -> tsink::Result<Vec<DataPoint>> {
            Ok(Vec::new())
        }

        fn select_with_options(
            &self,
            _metric: &str,
            _options: tsink::QueryOptions,
        ) -> tsink::Result<Vec<DataPoint>> {
            Ok(Vec::new())
        }

        fn select_all(
            &self,
            _metric: &str,
            _start: i64,
            _end: i64,
        ) -> tsink::Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            Ok(Vec::new())
        }

        fn select_series_with_execution_result(
            &self,
            _selection: &SeriesSelection,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<tsink::SelectSeriesExecutionResult> {
            match &self.behavior {
                ControlledMetadataBehavior::BlockUntilCancelled { started, finished } => {
                    started.store(true, Ordering::Release);
                    loop {
                        match execution.checkpoint() {
                            Ok(()) => std::thread::yield_now(),
                            Err(error) => {
                                finished.store(true, Ordering::Release);
                                return Err(error.into());
                            }
                        }
                    }
                }
                ControlledMetadataBehavior::ReturnUnaccounted => {
                    Ok(tsink::SelectSeriesExecutionResult::unaccounted(vec![
                        MetricSeries {
                            name: "contract_metric".to_string(),
                            labels: Vec::new(),
                        },
                    ]))
                }
                ControlledMetadataBehavior::Fail(diagnostic) => {
                    Err(tsink::TsinkError::Other(diagnostic.clone()))
                }
            }
        }

        fn select_series_execution_accounting(&self) -> tsink::QueryExecutionAccounting {
            tsink::QueryExecutionAccounting::Complete
        }

        fn close(&self) -> tsink::Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum MetadataEndpointKind {
        Series,
        Labels,
        LabelValues,
    }

    fn read_admission() -> ReadAdmissionController {
        ReadAdmissionController::new(ReadAdmissionGuardrails {
            max_inflight_requests: 8,
            max_inflight_queries: 32,
            acquire_timeout: Duration::from_millis(10),
        })
        .expect("read admission should build")
    }

    fn finite_query_limits(
        max_series_matched: u64,
        max_returned_bytes: u64,
        max_memory_bytes: u64,
    ) -> QueryBudgetLimits {
        QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(TEST_QUERY_MEMORY_BYTES.max(max_memory_bytes)),
            per_query: QueryWorkLimits {
                max_series_matched: Some(max_series_matched),
                max_returned_bytes: Some(max_returned_bytes),
                max_intermediate_vector_size: Some(128),
                max_memory_bytes: Some(max_memory_bytes),
                max_wall_time: Some(Duration::from_secs(5)),
                ..QueryWorkLimits::default()
            },
        }
    }

    fn request_decode_budget(max_memory_bytes: u64) -> tsink::QueryBudget {
        tsink::QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(max_memory_bytes),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(max_memory_bytes),
                max_wall_time: Some(Duration::from_secs(5)),
                ..QueryWorkLimits::default()
            },
        })
        .expect("request decode query budget should build")
    }

    fn assert_guarded_metric_request_outcome(
        result: &Result<GuardedMetricMetadataRequest, MetadataRequestError>,
        expected_metric: Option<&str>,
    ) {
        match (result, expected_metric) {
            (Ok(query), Some(expected_metric)) => {
                assert_eq!(query.metric.as_deref(), Some(expected_metric));
                assert_eq!(query.limit, 1);
            }
            (Err(MetadataRequestError::BadData(message)), None) => {
                assert_eq!(*message, "parameter 'limit' must be a positive integer");
            }
            (result, expected) => {
                panic!("unexpected guarded request outcome {result:?} for {expected:?}")
            }
        }
    }

    fn storage_with_limits(limits: QueryBudgetLimits, series_count: usize) -> Arc<dyn Storage> {
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .with_query_budget_limits(limits)
            .build()
            .expect("storage should build");
        let rows = (0..series_count)
            .map(|index| {
                Row::with_labels(
                    "bounded_metric",
                    vec![
                        Label::new("instance", format!("node-{index:02}")),
                        Label::new("job", "metadata"),
                    ],
                    DataPoint::new(1_700_000_000_000, index as f64),
                )
            })
            .collect::<Vec<_>>();
        tenant::scoped_storage(Arc::clone(&storage), tenant::DEFAULT_TENANT_ID)
            .insert_rows(&rows)
            .expect("seed rows should be accepted");
        storage
    }

    fn series_request(matchers: &[&str]) -> HttpRequest {
        HttpRequest {
            method: "GET".to_string(),
            path: format!(
                "/api/v1/series?{}",
                matchers
                    .iter()
                    .map(|matcher| format!("match[]={matcher}"))
                    .collect::<Vec<_>>()
                    .join("&")
            ),
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }

    fn metadata_request(path: &str) -> HttpRequest {
        HttpRequest {
            method: "GET".to_string(),
            path: path.to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }

    fn response_header<'a>(response: &'a HttpResponse, name: &str) -> Option<&'a str> {
        response
            .headers
            .iter()
            .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn assert_query_resources_released(storage: &Arc<dyn Storage>) {
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    async fn run_series(storage: &Arc<dyn Storage>, matchers: &[&str]) -> HttpResponse {
        handle_series_with_admission(
            storage,
            &series_request(matchers),
            None,
            None,
            None,
            None,
            &read_admission(),
        )
        .await
    }

    fn endpoint_request(endpoint: MetadataEndpointKind) -> HttpRequest {
        match endpoint {
            MetadataEndpointKind::Series => series_request(&["bounded_metric"]),
            MetadataEndpointKind::Labels => {
                metadata_request("/api/v1/labels?match[]=bounded_metric")
            }
            MetadataEndpointKind::LabelValues => {
                metadata_request("/api/v1/label/job/values?match[]=bounded_metric")
            }
        }
    }

    async fn run_metadata_endpoint(
        storage: &Arc<dyn Storage>,
        endpoint: MetadataEndpointKind,
    ) -> HttpResponse {
        let request = endpoint_request(endpoint);
        let admission = read_admission();
        match endpoint {
            MetadataEndpointKind::Series => {
                handle_series_with_admission(storage, &request, None, None, None, None, &admission)
                    .await
            }
            MetadataEndpointKind::Labels => {
                handle_labels_with_admission(storage, &request, None, None, None, None, &admission)
                    .await
            }
            MetadataEndpointKind::LabelValues => {
                handle_label_values_with_admission(
                    storage,
                    &request,
                    "job",
                    PublicReadContext::new(None, None, None, None),
                    &admission,
                )
                .await
            }
        }
    }

    fn metric_metadata_store(help_bytes: usize) -> Arc<MetricMetadataStore> {
        let store = Arc::new(MetricMetadataStore::in_memory());
        store
            .apply_updates(
                tenant::DEFAULT_TENANT_ID,
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "bounded_metadata_metric".to_string(),
                    metric_type: MetricType::Counter,
                    help: "h".repeat(help_bytes),
                    unit: "requests".to_string(),
                }],
            )
            .expect("metric metadata fixture should be accepted");
        store
    }

    async fn run_metric_metadata_endpoint(
        storage: &Arc<dyn Storage>,
        metadata_store: &Arc<MetricMetadataStore>,
        path: &str,
    ) -> HttpResponse {
        handle_metadata_with_admission(
            storage,
            metadata_store,
            &metadata_request(path),
            None,
            None,
            None,
            &read_admission(),
        )
        .await
    }

    async fn wait_for_atomic_flag(flag: &AtomicBool, context: &'static str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !flag.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect(context);
    }

    async fn wait_for_query_resources_to_release(storage: &Arc<dyn Storage>) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = storage.query_budget_snapshot();
                if snapshot.active_queries == 0 && snapshot.shared_reserved_memory_bytes == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("canceled metadata query resources should be released");
        assert_query_resources_released(storage);
    }

    fn measure_metadata_envelope(
        storage: &Arc<dyn Storage>,
        endpoint: MetadataEndpointKind,
    ) -> (u64, u64) {
        let scoped =
            tenant::scoped_storage(Arc::clone(storage), tenant::DEFAULT_TENANT_ID.to_string());
        ensure_local_metadata_accounting(&scoped).expect("metadata accounting should be complete");
        let (execution, cancellation_guard) =
            begin_public_metadata_query(&scoped).expect("query execution should be admitted");
        let request = endpoint_request(endpoint);
        let default_if_empty = !matches!(endpoint, MetadataEndpointKind::Series);
        let selections = guarded_metadata_matcher_selections(
            &request,
            request.param_count("match[]"),
            default_if_empty,
            &execution,
        )
        .expect("matcher selection should be guarded");
        let mut projection = match endpoint {
            MetadataEndpointKind::Series => {
                MetadataProjection::series(&execution).expect("series projection should build")
            }
            MetadataEndpointKind::Labels => MetadataProjection::label_names(&execution)
                .expect("label-name projection should build"),
            MetadataEndpointKind::LabelValues => {
                let label_name = GuardedOwnedMetadataString::new("job", &execution)
                    .expect("label name should be guarded");
                let projection = MetadataProjection::label_values(&execution, &label_name.value)
                    .expect("label-value projection should build");
                drop(label_name);
                projection
            }
        };
        for selection in &selections.values {
            let selected = scoped
                .select_series_with_execution_result(selection, &execution)
                .expect("metadata selection should succeed");
            ingest_local_series_result(&execution, selected, &mut projection)
                .expect("metadata selection should be adopted");
        }
        drop(selections);
        let prepared = finish_metadata_projection(&execution, projection, None)
            .expect("response should be prepared");
        let returned_bytes = execution.snapshot().returned_bytes;
        let peak_memory = storage
            .query_budget_snapshot()
            .peak_shared_reserved_memory_bytes;
        drop(prepared);
        drop(execution);
        drop(cancellation_guard);
        assert_query_resources_released(storage);
        (returned_bytes, peak_memory)
    }

    fn measure_series_envelope(storage: &Arc<dyn Storage>) -> (u64, u64) {
        measure_metadata_envelope(storage, MetadataEndpointKind::Series)
    }

    #[tokio::test]
    async fn metric_metadata_endpoint_has_exact_returned_byte_and_memory_boundaries() {
        let metadata_store = metric_metadata_store(4 * 1024);
        let probe = storage_with_limits(
            finite_query_limits(1, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            0,
        );
        let probe_response = run_metric_metadata_endpoint(
            &probe,
            &metadata_store,
            "/api/v1/metadata?metric=bounded_metadata_metric&limit=1",
        )
        .await;
        assert_eq!(probe_response.status, 200);
        let returned_bytes =
            u64::try_from(probe_response.body.len()).expect("response length should fit u64");
        let peak_memory = probe
            .query_budget_snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(returned_bytes > 0);
        assert!(peak_memory > 0);
        assert_query_resources_released(&probe);
        assert_eq!(
            metadata_store
                .metrics_snapshot()
                .expect("metadata metrics should be available")
                .query_result_bytes,
            0
        );

        let exact_returned = storage_with_limits(
            finite_query_limits(1, returned_bytes, TEST_QUERY_MEMORY_BYTES),
            0,
        );
        let response = run_metric_metadata_endpoint(
            &exact_returned,
            &metadata_store,
            "/api/v1/metadata?metric=bounded_metadata_metric&limit=1",
        )
        .await;
        assert_eq!(response.status, 200);
        assert_eq!(
            u64::try_from(response.body.len()).expect("response length should fit u64"),
            returned_bytes
        );
        assert_query_resources_released(&exact_returned);

        let below_returned = storage_with_limits(
            finite_query_limits(1, returned_bytes.saturating_sub(1), TEST_QUERY_MEMORY_BYTES),
            0,
        );
        let response = run_metric_metadata_endpoint(
            &below_returned,
            &metadata_store,
            "/api/v1/metadata?metric=bounded_metadata_metric&limit=1",
        )
        .await;
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("query_limit_returned_bytes")
        );
        assert_query_resources_released(&below_returned);

        let exact_memory =
            storage_with_limits(finite_query_limits(1, returned_bytes, peak_memory), 0);
        let response = run_metric_metadata_endpoint(
            &exact_memory,
            &metadata_store,
            "/api/v1/metadata?metric=bounded_metadata_metric&limit=1",
        )
        .await;
        assert_eq!(response.status, 200);
        assert_query_resources_released(&exact_memory);

        let below_memory = storage_with_limits(
            finite_query_limits(1, returned_bytes, peak_memory.saturating_sub(1)),
            0,
        );
        let response = run_metric_metadata_endpoint(
            &below_memory,
            &metadata_store,
            "/api/v1/metadata?metric=bounded_metadata_metric&limit=1",
        )
        .await;
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_query_resources_released(&below_memory);
        assert_eq!(
            metadata_store
                .metrics_snapshot()
                .expect("metadata metrics should be available")
                .query_result_bytes,
            0
        );
    }

    #[test]
    fn metric_metadata_percent_decode_peaks_have_exact_boundaries_and_zero_residuals() {
        let retained_metric = (0..64).map(|_| "%6d").collect::<String>();
        let cases = [
            (
                "/api/v1/metadata?metric=%62%6f%75%6e%64%65%64&limit=%31".to_string(),
                Some("bounded"),
            ),
            (
                "/api/v1/metadata?metric=%FF%6d&limit=%31".to_string(),
                Some("\u{fffd}m"),
            ),
            (
                format!("/api/v1/metadata?metric={retained_metric}&limit=%FF"),
                None,
            ),
        ];

        for (path, expected_metric) in cases {
            let request = metadata_request(&path);
            let probe_budget = request_decode_budget(TEST_QUERY_MEMORY_BYTES);
            let probe_execution = probe_budget.begin_query().unwrap();
            let probe_result = guarded_metric_metadata_request(&request, &probe_execution);
            assert_guarded_metric_request_outcome(&probe_result, expected_metric);
            let exact_peak = probe_budget.snapshot().peak_shared_reserved_memory_bytes;
            assert!(exact_peak > 1);
            drop(probe_result);
            assert_eq!(probe_execution.snapshot().memory_reserved_bytes, 0);
            drop(probe_execution);
            assert_eq!(probe_budget.snapshot().active_queries, 0);
            assert_eq!(probe_budget.snapshot().shared_reserved_memory_bytes, 0);

            let exact_budget = request_decode_budget(exact_peak);
            let exact_execution = exact_budget.begin_query().unwrap();
            let exact_result = guarded_metric_metadata_request(&request, &exact_execution);
            assert_guarded_metric_request_outcome(&exact_result, expected_metric);
            drop(exact_result);
            assert_eq!(exact_execution.snapshot().memory_reserved_bytes, 0);
            drop(exact_execution);
            assert_eq!(exact_budget.snapshot().active_queries, 0);
            assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

            let below_budget = request_decode_budget(exact_peak - 1);
            let below_execution = below_budget.begin_query().unwrap();
            let below_result = guarded_metric_metadata_request(&request, &below_execution);
            assert!(matches!(
                below_result,
                Err(MetadataRequestError::Budget(
                    tsink::QueryBudgetError::LimitExceeded(_)
                ))
            ));
            assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
            drop(below_execution);
            assert_eq!(below_budget.snapshot().active_queries, 0);
            assert_eq!(below_budget.snapshot().shared_reserved_memory_bytes, 0);
        }
    }

    #[test]
    fn metadata_read_admission_unavailable_response_is_static_and_no_echo() {
        let response = metadata_read_admission_unavailable_response();
        assert_eq!(response.status, 500);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("metadata_read_admission_unavailable")
        );
        let body = String::from_utf8(response.body).expect("error body should be UTF-8 JSON");
        assert!(body.contains("public metadata read admission is unavailable"));
        assert!(!body.contains("SECRET-INVALID-ENV-VALUE"));
        assert!(!body.contains("read admission unavailable:"));
    }

    #[tokio::test]
    async fn metric_metadata_endpoint_rejects_limits_instead_of_clamping_or_echoing() {
        let metadata_store = metric_metadata_store(32);
        let storage = storage_with_limits(
            finite_query_limits(1, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            0,
        );
        let attacker = METADATA_API_MAX_LIMIT.saturating_add(1).to_string();
        let response = run_metric_metadata_endpoint(
            &storage,
            &metadata_store,
            &format!("/api/v1/metadata?limit={attacker}"),
        )
        .await;
        assert_eq!(response.status, 422);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("metadata_invalid_request")
        );
        let rendered =
            String::from_utf8(response.body).expect("metadata error should contain UTF-8 JSON");
        assert!(!rendered.contains(&attacker));
        assert_query_resources_released(&storage);

        let response = run_metric_metadata_endpoint(
            &storage,
            &metadata_store,
            "/api/v1/metadata?metric=&limit=1",
        )
        .await;
        assert_eq!(response.status, 422);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("metadata_invalid_request")
        );
        assert_query_resources_released(&storage);
    }

    #[tokio::test]
    async fn series_limit_is_exact_and_never_truncates_n_plus_one() {
        let exact = storage_with_limits(
            finite_query_limits(3, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            3,
        );
        let response = run_series(&exact, &["bounded_metric"]).await;
        assert_eq!(response.status, 200);
        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("response should be valid JSON");
        assert_eq!(body["data"].as_array().map(Vec::len), Some(3));
        assert_query_resources_released(&exact);

        let one_over = storage_with_limits(
            finite_query_limits(3, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            4,
        );
        let response = run_series(&one_over, &["bounded_metric"]).await;
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("query_limit_series_matched")
        );
        assert_query_resources_released(&one_over);
    }

    #[tokio::test]
    async fn series_matchers_share_one_cumulative_series_budget() {
        let storage = storage_with_limits(
            finite_query_limits(1, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            1,
        );
        let response = run_series(&storage, &["bounded_metric", "bounded_metric"]).await;
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("query_limit_series_matched")
        );
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_query_resources_released(&storage);
    }

    #[tokio::test]
    async fn labels_and_label_values_matchers_share_one_cumulative_budget() {
        for (path, label_name) in [
            (
                "/api/v1/labels?match[]=bounded_metric&match[]=bounded_metric",
                None,
            ),
            (
                "/api/v1/label/job/values?match[]=bounded_metric&match[]=bounded_metric",
                Some("job"),
            ),
        ] {
            let storage = storage_with_limits(
                finite_query_limits(1, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
                1,
            );
            let request = metadata_request(path);
            let response = match label_name {
                None => {
                    handle_labels_with_admission(
                        &storage,
                        &request,
                        None,
                        None,
                        None,
                        None,
                        &read_admission(),
                    )
                    .await
                }
                Some(label_name) => {
                    handle_label_values_with_admission(
                        &storage,
                        &request,
                        label_name,
                        PublicReadContext::new(None, None, None, None),
                        &read_admission(),
                    )
                    .await
                }
            };
            assert_eq!(response.status, 413);
            assert_eq!(
                response_header(&response, READ_ERROR_CODE_HEADER),
                Some("query_limit_series_matched")
            );
            let snapshot = storage.query_budget_snapshot();
            assert_eq!(snapshot.queries_started_total, 1);
            assert_eq!(snapshot.queries_completed_total, 1);
            assert_query_resources_released(&storage);
        }
    }

    #[tokio::test]
    async fn returned_byte_limit_is_exact_for_storage_work_and_final_json() {
        let measured = storage_with_limits(
            finite_query_limits(8, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            3,
        );
        let (exact_returned_bytes, _) = measure_series_envelope(&measured);
        assert!(exact_returned_bytes > 1);

        let exact = storage_with_limits(
            finite_query_limits(8, exact_returned_bytes, TEST_QUERY_MEMORY_BYTES),
            3,
        );
        assert_eq!(run_series(&exact, &["bounded_metric"]).await.status, 200);
        assert_query_resources_released(&exact);

        let one_over = storage_with_limits(
            finite_query_limits(
                8,
                exact_returned_bytes.saturating_sub(1),
                TEST_QUERY_MEMORY_BYTES,
            ),
            3,
        );
        let response = run_series(&one_over, &["bounded_metric"]).await;
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("query_limit_returned_bytes")
        );
        assert_query_resources_released(&one_over);
    }

    #[tokio::test]
    async fn metadata_memory_limit_is_exact_and_releases_shared_memory() {
        let measured = storage_with_limits(
            finite_query_limits(8, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            3,
        );
        let (_, exact_memory_bytes) = measure_series_envelope(&measured);
        assert!(exact_memory_bytes > 1);

        let exact = storage_with_limits(
            finite_query_limits(8, TEST_RETURNED_BYTES, exact_memory_bytes),
            3,
        );
        assert_eq!(run_series(&exact, &["bounded_metric"]).await.status, 200);
        assert_query_resources_released(&exact);

        let one_over = storage_with_limits(
            finite_query_limits(8, TEST_RETURNED_BYTES, exact_memory_bytes.saturating_sub(1)),
            3,
        );
        let response = run_series(&one_over, &["bounded_metric"]).await;
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_query_resources_released(&one_over);
    }

    #[tokio::test]
    async fn labels_and_values_returned_byte_limits_are_exact() {
        for endpoint in [
            MetadataEndpointKind::Labels,
            MetadataEndpointKind::LabelValues,
        ] {
            let measured = storage_with_limits(
                finite_query_limits(8, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
                3,
            );
            let (exact_returned_bytes, _) = measure_metadata_envelope(&measured, endpoint);
            assert!(
                exact_returned_bytes > 1,
                "{endpoint:?} should return a non-empty envelope"
            );

            let exact = storage_with_limits(
                finite_query_limits(8, exact_returned_bytes, TEST_QUERY_MEMORY_BYTES),
                3,
            );
            let response = run_metadata_endpoint(&exact, endpoint).await;
            assert_eq!(response.status, 200, "{endpoint:?} exact boundary");
            assert_query_resources_released(&exact);

            let one_under = storage_with_limits(
                finite_query_limits(
                    8,
                    exact_returned_bytes.saturating_sub(1),
                    TEST_QUERY_MEMORY_BYTES,
                ),
                3,
            );
            let response = run_metadata_endpoint(&one_under, endpoint).await;
            assert_eq!(response.status, 413, "{endpoint:?} one-under boundary");
            assert_eq!(
                response_header(&response, READ_ERROR_CODE_HEADER),
                Some("query_limit_returned_bytes")
            );
            assert_query_resources_released(&one_under);
        }
    }

    #[tokio::test]
    async fn labels_and_values_memory_limits_are_exact() {
        for endpoint in [
            MetadataEndpointKind::Labels,
            MetadataEndpointKind::LabelValues,
        ] {
            let measured = storage_with_limits(
                finite_query_limits(8, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
                3,
            );
            let (_, exact_memory_bytes) = measure_metadata_envelope(&measured, endpoint);
            assert!(
                exact_memory_bytes > 1,
                "{endpoint:?} should reserve query memory"
            );

            let exact = storage_with_limits(
                finite_query_limits(8, TEST_RETURNED_BYTES, exact_memory_bytes),
                3,
            );
            let response = run_metadata_endpoint(&exact, endpoint).await;
            assert_eq!(response.status, 200, "{endpoint:?} exact boundary");
            assert_query_resources_released(&exact);

            let one_under = storage_with_limits(
                finite_query_limits(8, TEST_RETURNED_BYTES, exact_memory_bytes.saturating_sub(1)),
                3,
            );
            let response = run_metadata_endpoint(&one_under, endpoint).await;
            assert_eq!(response.status, 413, "{endpoint:?} one-under boundary");
            assert_eq!(
                response_header(&response, READ_ERROR_CODE_HEADER),
                Some("query_limit_per_query_memory_bytes")
            );
            assert_query_resources_released(&one_under);
        }
    }

    #[tokio::test]
    async fn metadata_endpoints_reuse_one_query_slot_and_release_it() {
        let storage = storage_with_limits(
            finite_query_limits(32, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            3,
        );
        storage
            .insert_rows(&[Row::with_labels(
                "bounded_metric",
                vec![
                    Label::new("instance", "legacy-node"),
                    Label::new("job", "legacy"),
                ],
                DataPoint::new(1_700_000_000_000, 4.0),
            )])
            .expect("legacy default-tenant row should be accepted");
        let admission = read_admission();

        let series = handle_series_with_admission(
            &storage,
            &series_request(&["bounded_metric"]),
            None,
            None,
            None,
            None,
            &admission,
        )
        .await;
        assert_eq!(series.status, 200);
        let body: JsonValue =
            serde_json::from_slice(&series.body).expect("series response should be JSON");
        assert_eq!(body["data"].as_array().map(Vec::len), Some(4));
        assert_query_resources_released(&storage);

        let labels = handle_labels_with_admission(
            &storage,
            &metadata_request("/api/v1/labels"),
            None,
            None,
            None,
            None,
            &admission,
        )
        .await;
        assert_eq!(labels.status, 200);
        let body: JsonValue =
            serde_json::from_slice(&labels.body).expect("labels response should be JSON");
        assert_eq!(
            body["data"],
            json!(["__name__", "instance", "job"]),
            "scoped and legacy fallback branches should deduplicate label names"
        );
        assert_query_resources_released(&storage);

        let values = handle_label_values_with_admission(
            &storage,
            &metadata_request("/api/v1/label/job/values"),
            "job",
            PublicReadContext::new(None, None, None, None),
            &admission,
        )
        .await;
        assert_eq!(values.status, 200);
        let body: JsonValue =
            serde_json::from_slice(&values.body).expect("label-values response should be JSON");
        assert_eq!(body["data"], json!(["legacy", "metadata"]));
        assert_query_resources_released(&storage);

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 3);
        assert_eq!(snapshot.queries_completed_total, 3);
        assert_eq!(snapshot.peak_active_queries, 1);
    }

    #[tokio::test]
    async fn dropping_each_metadata_handler_cancels_work_and_releases_every_resource() {
        for endpoint in [
            MetadataEndpointKind::Series,
            MetadataEndpointKind::Labels,
            MetadataEndpointKind::LabelValues,
        ] {
            let started = Arc::new(AtomicBool::new(false));
            let finished = Arc::new(AtomicBool::new(false));
            let storage: Arc<dyn Storage> = Arc::new(ControlledMetadataStorage::new(
                ControlledMetadataBehavior::BlockUntilCancelled {
                    started: Arc::clone(&started),
                    finished: Arc::clone(&finished),
                },
            ));
            let task_storage = Arc::clone(&storage);
            let task =
                tokio::spawn(async move { run_metadata_endpoint(&task_storage, endpoint).await });

            wait_for_atomic_flag(&started, "metadata worker should enter the backend").await;
            task.abort();
            let task_error = task
                .await
                .expect_err("aborted metadata handler should not return a response");
            assert!(task_error.is_cancelled());
            wait_for_atomic_flag(
                &finished,
                "metadata backend should observe handler cancellation",
            )
            .await;
            wait_for_query_resources_to_release(&storage).await;

            let snapshot = storage.query_budget_snapshot();
            assert_eq!(snapshot.queries_started_total, 1, "{endpoint:?}");
            assert_eq!(snapshot.queries_completed_total, 1, "{endpoint:?}");
            assert_eq!(snapshot.cancellations_total, 1, "{endpoint:?}");
        }
    }

    #[tokio::test]
    async fn metadata_endpoints_fail_closed_for_unaccounted_backends() {
        let inner = storage_with_limits(
            finite_query_limits(8, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            1,
        );
        let storage: Arc<dyn Storage> = Arc::new(UnaccountedMetadataStorage {
            inner: Arc::clone(&inner),
        });
        let response = run_series(&storage, &["bounded_metric"]).await;
        assert_eq!(response.status, 500);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("metadata_execution_failed")
        );
        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("error response should be JSON");
        assert_eq!(body["errorType"], "execution");
        assert_eq!(inner.query_budget_snapshot().queries_started_total, 0);
        assert_query_resources_released(&inner);
    }

    #[tokio::test]
    async fn metadata_endpoints_reject_a_false_complete_result_contract() {
        for endpoint in [
            MetadataEndpointKind::Series,
            MetadataEndpointKind::Labels,
            MetadataEndpointKind::LabelValues,
        ] {
            let storage: Arc<dyn Storage> = Arc::new(ControlledMetadataStorage::new(
                ControlledMetadataBehavior::ReturnUnaccounted,
            ));
            let response = run_metadata_endpoint(&storage, endpoint).await;
            assert_eq!(response.status, 500, "{endpoint:?}");
            assert_eq!(
                response_header(&response, READ_ERROR_CODE_HEADER),
                Some("metadata_execution_failed")
            );
            let body: JsonValue =
                serde_json::from_slice(&response.body).expect("error response should be JSON");
            assert_eq!(body["errorType"], "execution");
            let rendered = String::from_utf8(response.body)
                .expect("metadata error response should contain UTF-8 JSON");
            assert!(!rendered.contains("reservation"));
            assert!(!rendered.contains("contract_metric"));
            assert_query_resources_released(&storage);
        }
    }

    #[tokio::test]
    async fn hostile_backend_diagnostics_are_never_echoed() {
        let attacker = format!("backend-secret-marker-{}", "x".repeat(8 * 1024));
        for endpoint in [
            MetadataEndpointKind::Series,
            MetadataEndpointKind::Labels,
            MetadataEndpointKind::LabelValues,
        ] {
            let storage: Arc<dyn Storage> = Arc::new(ControlledMetadataStorage::new(
                ControlledMetadataBehavior::Fail(attacker.clone()),
            ));
            let response = run_metadata_endpoint(&storage, endpoint).await;
            assert_eq!(response.status, 500, "{endpoint:?}");
            assert_eq!(
                response_header(&response, READ_ERROR_CODE_HEADER),
                Some("metadata_execution_failed")
            );
            let rendered = String::from_utf8(response.body)
                .expect("metadata error response should contain UTF-8 JSON");
            assert!(!rendered.contains("backend-secret-marker"));
            assert!(!rendered.contains(&"x".repeat(128)));
            assert!(rendered.len() <= 1024);
            assert_query_resources_released(&storage);
        }
    }

    #[tokio::test]
    async fn long_invalid_matcher_diagnostic_is_bounded_and_releases_resources() {
        let storage = storage_with_limits(
            finite_query_limits(8, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            1,
        );
        let matcher = format!("1 {}", "a".repeat(8 * 1024));
        let response = run_series(&storage, &[matcher.as_str()]).await;
        assert_eq!(response.status, 422);
        assert!(
            response.body.len() <= 1024,
            "bounded matcher diagnostic unexpectedly retained {} bytes",
            response.body.len()
        );
        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("error response should be JSON");
        let message = body["error"]
            .as_str()
            .expect("error response should include a diagnostic");
        assert_eq!(message, "invalid match expression at index 0");
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("metadata_invalid_matcher")
        );
        assert!(!message.contains(&"a".repeat(32)));
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_query_resources_released(&storage);
    }

    #[tokio::test]
    async fn hostile_label_names_are_rejected_without_echo() {
        let storage = storage_with_limits(
            finite_query_limits(8, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            0,
        );
        let attacker = format!("label-secret-marker-{}", "x".repeat(8 * 1024));
        let response = handle_label_values_with_admission(
            &storage,
            &metadata_request("/api/v1/label/hostile/values"),
            &attacker,
            PublicReadContext::new(None, None, None, None),
            &read_admission(),
        )
        .await;
        assert_eq!(response.status, 422);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("metadata_invalid_label_name")
        );
        let rendered =
            String::from_utf8(response.body).expect("label error response should contain UTF-8");
        assert!(!rendered.contains("label-secret-marker"));
        assert!(!rendered.contains(&"x".repeat(128)));
        assert_eq!(storage.query_budget_snapshot().queries_started_total, 0);
        assert_query_resources_released(&storage);
    }

    #[test]
    fn invalid_matcher_keeps_full_parser_guard_until_response_construction() {
        let storage = storage_with_limits(
            finite_query_limits(8, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            0,
        );
        let (execution, cancellation_guard) =
            begin_public_metadata_query(&storage).expect("query execution should be admitted");
        let invalid = format!("1 {}", "a".repeat(8 * 1024));
        let request = series_request(&["bounded_metric", invalid.as_str()]);
        let error = guarded_metadata_matcher_selections(
            &request,
            request.param_count("match[]"),
            false,
            &execution,
        )
        .expect_err("second matcher should fail parsing");

        let retained_while_error_is_live = execution.snapshot().memory_reserved_bytes;
        let peak_while_parser_locals_unwind = storage
            .query_budget_snapshot()
            .peak_shared_reserved_memory_bytes;
        assert_eq!(
            retained_while_error_is_live, peak_while_parser_locals_unwind,
            "the full parser/selection guard must not shrink before parser locals unwind"
        );
        assert!(
            retained_while_error_is_live > PUBLIC_METADATA_DIAGNOSTIC_RESPONSE_ALLOCATION_BYTES,
            "test matcher should require more than the bounded response-only reservation"
        );

        let response = metadata_request_error_response(error);
        assert_eq!(response.status, 422);
        assert!(response.body.len() <= 1024);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        drop(cancellation_guard);
        assert_query_resources_released(&storage);
    }

    #[test]
    fn prepared_metadata_response_holds_body_memory_until_explicit_http_handoff() {
        let storage = storage_with_limits(
            finite_query_limits(8, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            1,
        );
        let scoped =
            tenant::scoped_storage(Arc::clone(&storage), tenant::DEFAULT_TENANT_ID.to_string());
        ensure_local_metadata_accounting(&scoped).expect("metadata accounting should be complete");
        let (execution, cancellation_guard) =
            begin_public_metadata_query(&scoped).expect("query execution should be admitted");
        let request = series_request(&["bounded_metric"]);
        let selections = guarded_metadata_matcher_selections(
            &request,
            request.param_count("match[]"),
            false,
            &execution,
        )
        .expect("matcher selection should be guarded");
        let mut projection =
            MetadataProjection::series(&execution).expect("series projection should build");
        let selected = scoped
            .select_series_with_execution_result(&selections.values[0], &execution)
            .expect("metadata selection should succeed");
        ingest_local_series_result(&execution, selected, &mut projection)
            .expect("metadata selection should be adopted");
        drop(selections);
        let prepared = finish_metadata_projection(&execution, projection, None)
            .expect("response should be prepared");

        assert!(
            execution.snapshot().memory_reserved_bytes > 0,
            "prepared body and response headers must remain guarded"
        );
        assert_eq!(storage.query_budget_snapshot().active_queries, 1);
        let response = prepared.into_http_response();
        assert_eq!(response.status, 200);
        assert!(
            !response.body.is_empty(),
            "HTTP response should own the handed-off body"
        );
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            0,
            "the explicit handoff is the only point where response memory leaves the query guard"
        );
        assert_eq!(
            storage.query_budget_snapshot().active_queries,
            1,
            "the execution remains live until usage accounting and handoff complete"
        );

        drop(response);
        drop(execution);
        drop(cancellation_guard);
        assert_query_resources_released(&storage);
    }

    #[test]
    fn distributed_tenant_and_warning_guards_are_cumulative_and_release() {
        let storage = storage_with_limits(
            finite_query_limits(8, TEST_RETURNED_BYTES, TEST_QUERY_MEMORY_BYTES),
            0,
        );
        let (execution, cancellation_guard) =
            begin_public_metadata_query(&storage).expect("query execution should be admitted");
        let requested = SeriesSelection::new()
            .with_metric("bounded_metric")
            .with_matcher(SeriesMatcher::equal("job", "metadata"));
        let first = GuardedTenantSelections::new(&requested, tenant::DEFAULT_TENANT_ID, &execution)
            .expect("first tenant expansion should be guarded");
        let second =
            GuardedTenantSelections::new(&requested, tenant::DEFAULT_TENANT_ID, &execution)
                .expect("second tenant expansion should share the execution");
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            first
                .reservation
                .bytes()
                .saturating_add(second.reservation.bytes())
        );

        let mut metadata = GuardedReadMetadata {
            value: ReadFanoutResponseMetadata {
                consistency: ClusterReadConsistency::Quorum,
                partial_response_policy: ClusterReadPartialResponsePolicy::Allow,
                partial_response: false,
                warnings: Vec::new(),
            },
            reservation: execution
                .reserve_memory(0)
                .expect("metadata guard should share the execution"),
        };
        let first_update = ReadFanoutResponseMetadata {
            consistency: ClusterReadConsistency::Quorum,
            partial_response_policy: ClusterReadPartialResponsePolicy::Allow,
            partial_response: true,
            warnings: vec!["first partial-read warning".repeat(8)],
        };
        let second_update = ReadFanoutResponseMetadata {
            consistency: ClusterReadConsistency::Quorum,
            partial_response_policy: ClusterReadPartialResponsePolicy::Allow,
            partial_response: true,
            warnings: vec![
                "first partial-read warning".repeat(8),
                "second partial-read warning".repeat(8),
            ],
        };
        metadata
            .merge(&first_update)
            .expect("first warning merge should be guarded");
        drop(first_update);
        metadata
            .reconcile_after_update_drop()
            .expect("first source warnings should be released");
        metadata
            .merge(&second_update)
            .expect("second warning merge should be cumulative");
        drop(second_update);
        metadata
            .reconcile_after_update_drop()
            .expect("second source warnings should be released");
        assert_eq!(metadata.value.warnings.len(), 2);
        metadata
            .reserve_response_headers()
            .expect("header transfer should be guarded");
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            first
                .reservation
                .bytes()
                .saturating_add(second.reservation.bytes())
                .saturating_add(metadata.reservation.bytes())
        );

        drop(first);
        drop(second);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            metadata.reservation.bytes()
        );
        drop(metadata);
        drop(execution);
        drop(cancellation_guard);
        assert_query_resources_released(&storage);
    }
}
