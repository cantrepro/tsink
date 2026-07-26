mod aggregation;
mod binary;
mod functions;
mod selector;
mod subquery;
pub mod time;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::promql::ast::{AtModifier, Expr, MatrixSelector, SubqueryExpr, UnaryOp, VectorSelector};
use crate::promql::error::{PromqlError, Result};
use crate::promql::types::{PromqlValue, Series};
use crate::{
    DataPoint, Label, NativeHistogram, QueryBudget, QueryBudgetLimits, QueryCancellationToken,
    QueryExecution, QueryMemoryReservation, QueryWorkLimits, Storage, TimestampPrecision, Value,
};

use self::time::{duration_to_units, step_times};

const DEFAULT_LOOKBACK_DELTA_MS: i64 = 5 * 60 * 1_000;
const DEFAULT_SUBQUERY_STEP_MS: i64 = 60 * 1_000;

type LabelPoints = (Vec<Label>, Vec<DataPoint>);
type MetricPrefetchRows = Vec<LabelPoints>;
type RangeSeriesKey = (String, Vec<Label>);
type RangeSeriesAccumulator = Series;

pub struct Engine {
    storage: Arc<dyn Storage>,
    default_lookback_delta: i64,
    timestamp_units_per_second: i64,
}

/// A PromQL value whose retained heap remains charged to its originating query execution.
///
/// Protocol adapters should keep this result (or the reservation returned by [`Self::into_parts`])
/// alive through response encoding and transport handoff.
#[derive(Debug)]
#[must_use = "dropping the result releases its retained query-memory reservation"]
pub struct PromqlExecutionResult {
    value: PromqlValue,
    result_memory_reservation: QueryMemoryReservation,
}

impl PromqlExecutionResult {
    /// Borrows the evaluated PromQL value without releasing its result-memory charge.
    #[must_use]
    pub fn value(&self) -> &PromqlValue {
        &self.value
    }

    /// Returns the bytes retained on behalf of the result.
    #[must_use]
    pub fn reserved_memory_bytes(&self) -> u64 {
        self.result_memory_reservation.bytes()
    }

    /// Consumes the wrapper and returns the value together with its retained-memory reservation.
    pub fn into_parts(self) -> (PromqlValue, QueryMemoryReservation) {
        (self.value, self.result_memory_reservation)
    }

    /// Consumes the wrapper and releases its retained-memory charge.
    ///
    /// This is intended for compatibility callers that do not need accounting through response
    /// serialization.
    #[must_use]
    pub fn into_value(self) -> PromqlValue {
        self.value
    }
}

#[derive(Clone)]
pub(crate) struct QueryParams<'a> {
    pub eval_time: i64,
    pub prefetch: Option<&'a PrefetchCache>,
    pub query_start: i64,
    pub query_end: i64,
    pub query_step: Option<i64>,
    pub execution: &'a QueryExecution,
    pub memory: &'a PromqlMemoryTracker,
}

#[derive(Clone, Default)]
pub(crate) struct PrefetchCache {
    by_metric: HashMap<String, MetricPrefetchRows>,
}

#[derive(Default)]
pub(crate) struct PromqlMemoryTracker {
    reservations: Mutex<Vec<QueryMemoryReservation>>,
}

impl PromqlMemoryTracker {
    pub(crate) fn reserve(&self, execution: &QueryExecution, bytes: u64) -> Result<()> {
        execution.checkpoint().map_err(crate::TsinkError::from)?;
        let reservation = execution
            .reserve_memory(bytes)
            .map_err(crate::TsinkError::from)?;
        self.reservations.lock().push(reservation);
        Ok(())
    }

    pub(crate) fn adopt(&self, reservation: QueryMemoryReservation) {
        self.reservations.lock().push(reservation);
    }

    fn into_result_reservation(
        self,
        execution: &QueryExecution,
        retained_bytes: u64,
    ) -> Result<QueryMemoryReservation> {
        execution
            .coalesce_memory_reservations(self.reservations.into_inner(), retained_bytes)
            .map_err(|error| match error {
                crate::query_budget::QueryMemoryCoalesceError::Budget(error) => {
                    PromqlError::Storage(crate::TsinkError::from(error))
                }
                crate::query_budget::QueryMemoryCoalesceError::InvalidReservations => {
                    PromqlError::Storage(crate::TsinkError::Other(
                        "PromQL memory tracker contained a reservation from another execution"
                            .to_string(),
                    ))
                }
            })
    }
}

const PROMQL_COLLECTION_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;

fn modeled_promql_parse_preparation_bytes(input: &str) -> u64 {
    let token_count_upper = input
        .len()
        .saturating_add(1)
        .min(crate::promql::MAX_PARSE_TOKENS.saturating_add(1));
    let token_count = u64::try_from(token_count_upper).unwrap_or(u64::MAX);
    let token_and_ast_bytes = u64::try_from(
        std::mem::size_of::<crate::promql::lexer::Token>()
            .saturating_add(std::mem::size_of::<Expr>().saturating_mul(4))
            .saturating_add(
                PROMQL_COLLECTION_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(8) as usize,
            ),
    )
    .unwrap_or(u64::MAX);
    // Lexer literals and token kinds can duplicate input text, and parser moves/clones text into
    // the AST while the token vector is still live. Eight input copies plus per-token node,
    // vector-growth, Box, and allocator allowances conservatively cover that overlap.
    token_count
        .saturating_mul(token_and_ast_bytes)
        .saturating_add(
            u64::try_from(input.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(8),
        )
        .saturating_add(PROMQL_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn prepare_promql_parse_with_execution(
    input: &str,
    execution: &QueryExecution,
) -> Result<(Expr, QueryMemoryReservation)> {
    if input.len() > crate::promql::MAX_PARSE_INPUT_BYTES {
        return Err(PromqlError::Parse(format!(
            "PromQL query input exceeds the {}-byte limit (got {} bytes)",
            crate::promql::MAX_PARSE_INPUT_BYTES,
            input.len()
        )));
    }
    execution.checkpoint().map_err(crate::TsinkError::from)?;
    let reservation = execution
        .reserve_memory(modeled_promql_parse_preparation_bytes(input))
        .map_err(crate::TsinkError::from)?;
    execution.checkpoint().map_err(crate::TsinkError::from)?;
    let expr = crate::promql::parse(input)?;
    Ok((expr, reservation))
}

fn modeled_vec_capacity_bytes<T>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    u64::try_from(capacity)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(std::mem::size_of::<T>()).unwrap_or(u64::MAX))
        .saturating_add(PROMQL_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_string_bytes(value: &str) -> u64 {
    if value.is_empty() {
        0
    } else {
        u64::try_from(value.len())
            .unwrap_or(u64::MAX)
            .saturating_add(PROMQL_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_promql_string_value_bytes(value: &str) -> u64 {
    u64::try_from(std::mem::size_of::<PromqlValue>())
        .unwrap_or(u64::MAX)
        .saturating_add(modeled_string_bytes(value))
}

fn modeled_owned_promql_string_value_bytes(value: &String) -> u64 {
    u64::try_from(std::mem::size_of::<PromqlValue>())
        .unwrap_or(u64::MAX)
        .saturating_add(modeled_owned_string_retained_bytes(value))
}

fn modeled_owned_string_retained_bytes(value: &String) -> u64 {
    if value.capacity() == 0 {
        0
    } else {
        u64::try_from(value.capacity())
            .unwrap_or(u64::MAX)
            .saturating_add(PROMQL_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_labels_bytes(labels: &[Label]) -> u64 {
    modeled_vec_capacity_bytes::<Label>(labels.len()).saturating_add(labels.iter().fold(
        0u64,
        |bytes, label| {
            bytes
                .saturating_add(modeled_string_bytes(&label.name))
                .saturating_add(modeled_string_bytes(&label.value))
        },
    ))
}

fn modeled_owned_labels_retained_bytes(labels: &Vec<Label>) -> u64 {
    modeled_vec_capacity_bytes::<Label>(labels.capacity()).saturating_add(labels.iter().fold(
        0u64,
        |bytes, label| {
            bytes
                .saturating_add(modeled_owned_string_retained_bytes(&label.name))
                .saturating_add(modeled_owned_string_retained_bytes(&label.value))
        },
    ))
}

fn modeled_histogram_bytes(histogram: &NativeHistogram) -> u64 {
    u64::try_from(std::mem::size_of::<NativeHistogram>())
        .unwrap_or(u64::MAX)
        .saturating_add(modeled_vec_capacity_bytes::<crate::HistogramBucketSpan>(
            histogram.negative_spans.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<i64>(
            histogram.negative_deltas.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<f64>(
            histogram.negative_counts.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<crate::HistogramBucketSpan>(
            histogram.positive_spans.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<i64>(
            histogram.positive_deltas.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<f64>(
            histogram.positive_counts.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<f64>(
            histogram.custom_values.capacity(),
        ))
}

fn modeled_value_heap_bytes(value: &Value) -> u64 {
    match value {
        Value::Bytes(bytes) => modeled_vec_capacity_bytes::<u8>(bytes.capacity()),
        Value::String(value) => modeled_string_bytes(value),
        Value::Histogram(histogram) => modeled_histogram_bytes(histogram),
        Value::F64(_) | Value::I64(_) | Value::U64(_) | Value::Bool(_) => 0,
    }
}

fn modeled_value_retained_heap_bytes(value: &Value) -> u64 {
    match value {
        Value::Bytes(bytes) => modeled_vec_capacity_bytes::<u8>(bytes.capacity()),
        Value::String(value) => modeled_owned_string_retained_bytes(value),
        Value::Histogram(histogram) => modeled_histogram_bytes(histogram),
        Value::F64(_) | Value::I64(_) | Value::U64(_) | Value::Bool(_) => 0,
    }
}

fn modeled_sample_bytes(
    metric: &str,
    labels: &[Label],
    histogram: Option<&NativeHistogram>,
) -> u64 {
    u64::try_from(std::mem::size_of::<crate::promql::types::Sample>())
        .unwrap_or(u64::MAX)
        .saturating_add(modeled_string_bytes(metric))
        .saturating_add(modeled_labels_bytes(labels))
        .saturating_add(histogram.map(modeled_histogram_bytes).unwrap_or(0))
}

fn modeled_owned_sample_retained_bytes(sample: &crate::promql::types::Sample) -> u64 {
    u64::try_from(std::mem::size_of::<crate::promql::types::Sample>())
        .unwrap_or(u64::MAX)
        .saturating_add(modeled_owned_string_retained_bytes(&sample.metric))
        .saturating_add(modeled_owned_labels_retained_bytes(&sample.labels))
        .saturating_add(
            sample
                .histogram
                .as_deref()
                .map(modeled_histogram_bytes)
                .unwrap_or(0),
        )
}

fn modeled_prefetch_rows_bytes(rows: &MetricPrefetchRows) -> u64 {
    modeled_vec_capacity_bytes::<LabelPoints>(rows.capacity()).saturating_add(rows.iter().fold(
        0u64,
        |bytes, (labels, points)| {
            bytes
                .saturating_add(modeled_owned_labels_retained_bytes(labels))
                .saturating_add(modeled_vec_capacity_bytes::<DataPoint>(points.capacity()))
                .saturating_add(points.iter().fold(0u64, |point_bytes, point| {
                    point_bytes.saturating_add(modeled_value_retained_heap_bytes(&point.value))
                }))
        },
    ))
}

fn promql_value_shape(value: &PromqlValue) -> (u64, u64) {
    match value {
        PromqlValue::Scalar(_, _) => (
            1,
            u64::try_from(std::mem::size_of::<PromqlValue>()).unwrap_or(u64::MAX),
        ),
        PromqlValue::String(value, _) => (1, modeled_owned_promql_string_value_bytes(value)),
        PromqlValue::InstantVector(samples) => (
            u64::try_from(samples.len()).unwrap_or(u64::MAX),
            modeled_vec_capacity_bytes::<crate::promql::types::Sample>(samples.capacity())
                .saturating_add(samples.iter().fold(0u64, |bytes, sample| {
                    bytes.saturating_add(modeled_owned_sample_retained_bytes(sample))
                })),
        ),
        PromqlValue::RangeVector(series) => {
            let samples = series.iter().fold(0u64, |count, series| {
                count
                    .saturating_add(u64::try_from(series.samples.len()).unwrap_or(u64::MAX))
                    .saturating_add(u64::try_from(series.histograms.len()).unwrap_or(u64::MAX))
            });
            let bytes = modeled_vec_capacity_bytes::<Series>(series.capacity()).saturating_add(
                series.iter().fold(0u64, |bytes, series| {
                    bytes
                        .saturating_add(modeled_owned_string_retained_bytes(&series.metric))
                        .saturating_add(modeled_owned_labels_retained_bytes(&series.labels))
                        .saturating_add(modeled_vec_capacity_bytes::<(i64, f64)>(
                            series.samples.capacity(),
                        ))
                        .saturating_add(modeled_vec_capacity_bytes::<(i64, Box<NativeHistogram>)>(
                            series.histograms.capacity(),
                        ))
                        .saturating_add(series.histograms.iter().fold(
                            0u64,
                            |histogram_bytes, (_, histogram)| {
                                histogram_bytes.saturating_add(modeled_histogram_bytes(histogram))
                            },
                        ))
                }),
            );
            (samples, bytes)
        }
    }
}

fn modeled_query_slice_content_bytes<T>(len: usize) -> u64 {
    u64::try_from(len)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(std::mem::size_of::<T>()).unwrap_or(u64::MAX))
}

fn modeled_query_labels_returned_bytes(labels: &[Label]) -> u64 {
    modeled_query_slice_content_bytes::<Label>(labels.len()).saturating_add(labels.iter().fold(
        0u64,
        |bytes, label| {
            bytes
                .saturating_add(u64::try_from(label.name.len()).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(label.value.len()).unwrap_or(u64::MAX))
        },
    ))
}

fn promql_value_returned_shape(value: &PromqlValue) -> (u64, u64) {
    let fixed_value_bytes = u64::try_from(std::mem::size_of::<PromqlValue>()).unwrap_or(u64::MAX);
    match value {
        PromqlValue::Scalar(_, _) => (1, fixed_value_bytes),
        PromqlValue::String(value, _) => (
            1,
            fixed_value_bytes.saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX)),
        ),
        PromqlValue::InstantVector(samples) => {
            let bytes = fixed_value_bytes
                .saturating_add(modeled_query_slice_content_bytes::<
                    crate::promql::types::Sample,
                >(samples.len()))
                .saturating_add(samples.iter().fold(0u64, |bytes, sample| {
                    bytes
                        .saturating_add(u64::try_from(sample.metric.len()).unwrap_or(u64::MAX))
                        .saturating_add(modeled_query_labels_returned_bytes(&sample.labels))
                        .saturating_add(
                            sample
                                .histogram
                                .as_deref()
                                .map(crate::value::modeled_query_histogram_payload_bytes)
                                .unwrap_or(0),
                        )
                }));
            (u64::try_from(samples.len()).unwrap_or(u64::MAX), bytes)
        }
        PromqlValue::RangeVector(series) => {
            let samples = series.iter().fold(0u64, |count, series| {
                count
                    .saturating_add(u64::try_from(series.samples.len()).unwrap_or(u64::MAX))
                    .saturating_add(u64::try_from(series.histograms.len()).unwrap_or(u64::MAX))
            });
            let bytes = fixed_value_bytes
                .saturating_add(modeled_query_slice_content_bytes::<Series>(series.len()))
                .saturating_add(series.iter().fold(0u64, |bytes, series| {
                    bytes
                        .saturating_add(u64::try_from(series.metric.len()).unwrap_or(u64::MAX))
                        .saturating_add(modeled_query_labels_returned_bytes(&series.labels))
                        .saturating_add(modeled_query_slice_content_bytes::<(i64, f64)>(
                            series.samples.len(),
                        ))
                        .saturating_add(modeled_query_slice_content_bytes::<(
                            i64,
                            Box<NativeHistogram>,
                        )>(series.histograms.len()))
                        .saturating_add(series.histograms.iter().fold(
                            0u64,
                            |histogram_bytes, (_, histogram)| {
                                histogram_bytes.saturating_add(
                                    crate::value::modeled_query_histogram_payload_bytes(histogram),
                                )
                            },
                        ))
                }));
            (samples, bytes)
        }
    }
}

impl QueryParams<'_> {
    pub(crate) fn checkpoint(&self) -> Result<()> {
        self.execution
            .checkpoint()
            .map_err(crate::TsinkError::from)
            .map_err(Into::into)
    }

    pub(crate) fn reserve_sample(
        &self,
        metric: &str,
        labels: &[Label],
        histogram: Option<&NativeHistogram>,
    ) -> Result<()> {
        self.checkpoint()?;
        self.execution
            .observe_intermediate_vector_size(1)
            .map_err(crate::TsinkError::from)?;
        self.memory.reserve(
            self.execution,
            modeled_sample_bytes(metric, labels, histogram),
        )
    }

    pub(crate) fn reserve_range_sample(
        &self,
        metric: &str,
        labels: &[Label],
        histogram: bool,
    ) -> Result<()> {
        let histogram_bytes = if histogram {
            u64::try_from(std::mem::size_of::<NativeHistogram>()).unwrap_or(u64::MAX)
        } else {
            0
        };
        self.memory.reserve(
            self.execution,
            u64::try_from(std::mem::size_of::<(i64, f64)>())
                .unwrap_or(u64::MAX)
                .saturating_add(modeled_string_bytes(metric))
                .saturating_add(modeled_labels_bytes(labels))
                .saturating_add(histogram_bytes),
        )
    }

    pub(crate) fn reserve_range_series_upper(
        &self,
        metric: &str,
        labels: &[Label],
        points: &[DataPoint],
    ) -> Result<()> {
        self.checkpoint()?;
        self.execution
            .observe_intermediate_vector_size(u64::try_from(points.len()).unwrap_or(u64::MAX))
            .map_err(crate::TsinkError::from)?;
        let histogram_bytes = points.iter().fold(0u64, |bytes, point| {
            bytes.saturating_add(match &point.value {
                Value::Histogram(histogram) => modeled_histogram_bytes(histogram),
                _ => 0,
            })
        });
        self.memory.reserve(
            self.execution,
            u64::try_from(std::mem::size_of::<Series>())
                .unwrap_or(u64::MAX)
                .saturating_add(modeled_string_bytes(metric))
                .saturating_add(modeled_labels_bytes(labels))
                .saturating_add(modeled_vec_capacity_bytes::<(i64, f64)>(points.len()))
                .saturating_add(modeled_vec_capacity_bytes::<(i64, Box<NativeHistogram>)>(
                    points.len(),
                ))
                .saturating_add(histogram_bytes),
        )
    }

    pub(crate) fn reserve_metric_names<'a>(
        &self,
        metrics: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        let mut count = 0usize;
        let mut bytes = 0u64;
        for metric in metrics {
            count = count.saturating_add(1);
            bytes = bytes.saturating_add(modeled_string_bytes(metric));
        }
        self.execution
            .observe_intermediate_vector_size(u64::try_from(count).unwrap_or(u64::MAX))
            .map_err(crate::TsinkError::from)?;
        self.memory.reserve(
            self.execution,
            modeled_vec_capacity_bytes::<String>(count).saturating_add(bytes),
        )
    }

    pub(crate) fn retain_value(&self, value: &PromqlValue) -> Result<()> {
        let (size, bytes) = promql_value_shape(value);
        self.execution
            .observe_intermediate_vector_size(size)
            .map_err(crate::TsinkError::from)?;
        self.memory.reserve(self.execution, bytes)
    }

    /// Pre-admits a conservative upper bound for an operator that can retain cloned keys,
    /// grouping maps, and an output collection while its input values are still live.
    pub(crate) fn reserve_transform_upper(
        &self,
        values: &[&PromqlValue],
        allocation_copies: u64,
    ) -> Result<()> {
        self.checkpoint()?;
        let (size, bytes) = values.iter().fold((0u64, 0u64), |(size, bytes), value| {
            let (value_size, value_bytes) = promql_value_shape(value);
            (
                size.saturating_add(value_size),
                bytes.saturating_add(value_bytes),
            )
        });
        self.execution
            .observe_intermediate_vector_size(size)
            .map_err(crate::TsinkError::from)?;
        self.memory
            .reserve(self.execution, bytes.saturating_mul(allocation_copies))
    }
}

impl PrefetchCache {
    fn insert(&mut self, metric: String, data: MetricPrefetchRows) {
        self.by_metric.insert(metric, data);
    }

    pub(crate) fn get(&self, metric: &str) -> Option<&MetricPrefetchRows> {
        self.by_metric.get(metric)
    }
}

impl Engine {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self::with_precision(storage, TimestampPrecision::Nanoseconds)
    }

    pub fn with_precision(storage: Arc<dyn Storage>, precision: TimestampPrecision) -> Self {
        let units_per_second = match precision {
            TimestampPrecision::Seconds => 1,
            TimestampPrecision::Milliseconds => 1_000,
            TimestampPrecision::Microseconds => 1_000_000,
            TimestampPrecision::Nanoseconds => 1_000_000_000,
        };

        Self {
            storage,
            default_lookback_delta: duration_to_units(DEFAULT_LOOKBACK_DELTA_MS, units_per_second),
            timestamp_units_per_second: units_per_second,
        }
    }

    fn begin_execution(
        &self,
        request_limits: QueryWorkLimits,
        cancellation: QueryCancellationToken,
    ) -> Result<QueryExecution> {
        if let Some(execution) = self
            .storage
            .begin_query_execution(request_limits, cancellation.clone())?
        {
            return Ok(execution);
        }

        // A compatibility backend with no shared query budget still receives one execution so
        // PromQL cancellation, step, and modeled-memory accounting stay coherent. Its all-None
        // fallback is deliberately unenforced and does not alter the backend's legacy behavior.
        QueryBudget::new(QueryBudgetLimits::default())
            .expect("the all-None fallback query budget is valid")
            .begin_query_with(request_limits, cancellation)
            .map_err(crate::TsinkError::from)
            .map_err(Into::into)
    }

    pub fn instant_query(&self, query_str: &str, time: i64) -> Result<PromqlValue> {
        self.instant_query_with_control(
            query_str,
            time,
            QueryWorkLimits::default(),
            QueryCancellationToken::new(),
        )
    }

    /// Evaluates one instant query under request-specific tightening and cancellation control.
    ///
    /// Exactly one storage query permit is admitted for the complete PromQL request. Backends
    /// without a query budget receive an internal unbounded execution so evaluation checkpoints
    /// remain active while their compatibility storage methods retain legacy behavior.
    pub fn instant_query_with_control(
        &self,
        query_str: &str,
        time: i64,
        request_limits: QueryWorkLimits,
        cancellation: QueryCancellationToken,
    ) -> Result<PromqlValue> {
        let execution = self.begin_execution(request_limits, cancellation)?;
        self.instant_query_with_execution(query_str, time, &execution)
    }

    /// Evaluates one instant query using an already-admitted execution without acquiring another
    /// concurrency permit.
    pub fn instant_query_with_execution(
        &self,
        query_str: &str,
        time: i64,
        execution: &QueryExecution,
    ) -> Result<PromqlValue> {
        self.instant_query_with_execution_internal(query_str, time, execution, false)
            .map(|(value, _)| value)
    }

    /// Evaluates one instant query and retains its result-memory charge for the caller.
    ///
    /// Keep the returned guard alive through serialization. This method reuses the caller's
    /// already-admitted execution and does not acquire another concurrency permit.
    pub fn instant_query_with_execution_result(
        &self,
        query_str: &str,
        time: i64,
        execution: &QueryExecution,
    ) -> Result<PromqlExecutionResult> {
        let (value, result_memory_reservation) =
            self.instant_query_with_execution_internal(query_str, time, execution, true)?;
        Ok(PromqlExecutionResult {
            value,
            result_memory_reservation: result_memory_reservation
                .expect("detailed instant-query execution always retains its result"),
        })
    }

    fn instant_query_with_execution_internal(
        &self,
        query_str: &str,
        time: i64,
        execution: &QueryExecution,
        retain_result: bool,
    ) -> Result<(PromqlValue, Option<QueryMemoryReservation>)> {
        let (expr, parse_reservation) = prepare_promql_parse_with_execution(query_str, execution)?;
        execution.checkpoint().map_err(crate::TsinkError::from)?;
        execution.ensure_steps(1).map_err(crate::TsinkError::from)?;
        execution.charge_steps(1).map_err(crate::TsinkError::from)?;
        let memory = PromqlMemoryTracker::default();
        let value = {
            let params = QueryParams {
                eval_time: time,
                prefetch: None,
                query_start: time,
                query_end: time,
                query_step: None,
                execution,
                memory: &memory,
            };
            self.eval(&expr, &params)?
        };
        charge_promql_result(execution, &value)?;
        drop(expr);
        let result_memory_reservation = if retain_result {
            Some(memory.into_result_reservation(execution, promql_value_shape(&value).1)?)
        } else {
            None
        };
        drop(parse_reservation);
        Ok((value, result_memory_reservation))
    }

    pub fn range_query(
        &self,
        query_str: &str,
        start: i64,
        end: i64,
        step: i64,
    ) -> Result<PromqlValue> {
        self.range_query_with_control(
            query_str,
            start,
            end,
            step,
            QueryWorkLimits::default(),
            QueryCancellationToken::new(),
        )
    }

    /// Evaluates a range query under request-specific tightening and cancellation control.
    pub fn range_query_with_control(
        &self,
        query_str: &str,
        start: i64,
        end: i64,
        step: i64,
        request_limits: QueryWorkLimits,
        cancellation: QueryCancellationToken,
    ) -> Result<PromqlValue> {
        let execution = self.begin_execution(request_limits, cancellation)?;
        self.range_query_with_execution(query_str, start, end, step, &execution)
    }

    /// Evaluates a range query using one caller-owned execution for prefetch and every step.
    pub fn range_query_with_execution(
        &self,
        query_str: &str,
        start: i64,
        end: i64,
        step: i64,
        execution: &QueryExecution,
    ) -> Result<PromqlValue> {
        self.range_query_with_execution_internal(query_str, start, end, step, execution, false)
            .map(|(value, _)| value)
    }

    /// Evaluates one range query and retains its result-memory charge for the caller.
    ///
    /// Keep the returned guard alive through serialization. Prefetch, all steps, and the retained
    /// result share the caller-owned execution.
    pub fn range_query_with_execution_result(
        &self,
        query_str: &str,
        start: i64,
        end: i64,
        step: i64,
        execution: &QueryExecution,
    ) -> Result<PromqlExecutionResult> {
        let (value, result_memory_reservation) =
            self.range_query_with_execution_internal(query_str, start, end, step, execution, true)?;
        Ok(PromqlExecutionResult {
            value,
            result_memory_reservation: result_memory_reservation
                .expect("detailed range-query execution always retains its result"),
        })
    }

    fn range_query_with_execution_internal(
        &self,
        query_str: &str,
        start: i64,
        end: i64,
        step: i64,
        execution: &QueryExecution,
        retain_result: bool,
    ) -> Result<(PromqlValue, Option<QueryMemoryReservation>)> {
        if step <= 0 {
            return Err(PromqlError::Eval("range step must be positive".to_string()));
        }
        if start > end {
            return Err(PromqlError::Eval(format!(
                "invalid range: start ({start}) is greater than end ({end})"
            )));
        }

        let (expr, parse_reservation) = prepare_promql_parse_with_execution(query_str, execution)?;
        let step_count = inclusive_step_count(start, end, step);
        execution
            .ensure_steps(step_count)
            .map_err(crate::TsinkError::from)?;
        let memory = PromqlMemoryTracker::default();
        let prefetch = if self.supports_prefetch(&expr) {
            Some(self.build_prefetch_cache(&expr, start, end, execution, &memory)?)
        } else {
            None
        };

        let mut out: BTreeMap<RangeSeriesKey, RangeSeriesAccumulator> = BTreeMap::new();
        for ts in step_times(start, end, step) {
            execution.charge_steps(1).map_err(crate::TsinkError::from)?;
            let params = QueryParams {
                eval_time: ts,
                prefetch: prefetch.as_ref(),
                query_start: start,
                query_end: end,
                query_step: Some(step),
                execution,
                memory: &memory,
            };
            let val = self.eval(&expr, &params)?;
            Self::append_step_value(&mut out, ts, val, &params)?;
        }

        memory.reserve(execution, modeled_vec_capacity_bytes::<Series>(out.len()))?;
        let series = out.into_values().collect();
        let value = PromqlValue::RangeVector(series);
        charge_promql_result(execution, &value)?;
        drop(prefetch);
        drop(expr);
        let result_memory_reservation = if retain_result {
            Some(memory.into_result_reservation(execution, promql_value_shape(&value).1)?)
        } else {
            None
        };
        drop(parse_reservation);
        Ok((value, result_memory_reservation))
    }

    fn append_step_value(
        out: &mut BTreeMap<RangeSeriesKey, RangeSeriesAccumulator>,
        step_ts: i64,
        value: PromqlValue,
        params: &QueryParams<'_>,
    ) -> Result<()> {
        match value {
            PromqlValue::Scalar(v, _) => {
                params.reserve_range_sample("", &[], false)?;
                out.entry((String::new(), Vec::new()))
                    .or_insert_with(|| Series::new(String::new(), Vec::new()))
                    .samples
                    .push((step_ts, v));
            }
            PromqlValue::InstantVector(samples) => {
                for sample in samples {
                    params.reserve_range_sample(
                        &sample.metric,
                        &sample.labels,
                        sample.histogram.is_some(),
                    )?;
                    let metric = sample.metric;
                    let labels = sample.labels;
                    let histogram = sample.histogram;
                    let value = sample.value;
                    let mut labels = labels;
                    labels.sort();
                    let entry = out
                        .entry((metric.clone(), labels.clone()))
                        .or_insert_with(|| Series::new(metric, labels));
                    if let Some(histogram) = histogram {
                        entry.histograms.push((step_ts, histogram));
                    } else {
                        entry.samples.push((step_ts, value));
                    }
                }
            }
            PromqlValue::RangeVector(series) => {
                for mut series in series {
                    let latest_float = series.samples.last().cloned();
                    let latest_histogram = series.histograms.last().cloned();
                    let Some(latest_point) = latest_series_point(latest_float, latest_histogram)
                    else {
                        continue;
                    };
                    params.reserve_range_sample(
                        &series.metric,
                        &series.labels,
                        matches!(&latest_point, LatestSeriesPoint::Histogram(_)),
                    )?;
                    series.labels.sort();
                    let entry = out
                        .entry((series.metric.clone(), series.labels.clone()))
                        .or_insert_with(|| Series::new(series.metric, series.labels));
                    match latest_point {
                        LatestSeriesPoint::Float(value) => entry.samples.push((step_ts, value)),
                        LatestSeriesPoint::Histogram(histogram) => {
                            entry.histograms.push((step_ts, histogram))
                        }
                    }
                }
            }
            PromqlValue::String(_, _) => {}
        }
        Ok(())
    }

    pub(crate) fn eval(&self, expr: &Expr, params: &QueryParams<'_>) -> Result<PromqlValue> {
        params.checkpoint()?;
        let string_literal_pre_reserved = matches!(expr, Expr::StringLiteral(_));
        let value = match expr {
            Expr::NumberLiteral(v) => Ok(PromqlValue::Scalar(*v, params.eval_time)),
            Expr::StringLiteral(v) => {
                params
                    .memory
                    .reserve(params.execution, modeled_promql_string_value_bytes(v))?;
                Ok(PromqlValue::String(v.clone(), params.eval_time))
            }
            Expr::Paren(expr) => self.eval(expr, params),
            Expr::VectorSelector(selector) => {
                selector::eval_vector_selector(self, selector, params)
            }
            Expr::MatrixSelector(selector) => {
                selector::eval_matrix_selector(self, selector, params)
            }
            Expr::Subquery(subquery) => subquery::eval_subquery(self, subquery, params),
            Expr::Unary(unary) => {
                let value = self.eval(&unary.expr, params)?;
                match unary.op {
                    UnaryOp::Pos => Ok(value),
                    UnaryOp::Neg => negate_value(value),
                }
            }
            Expr::Binary(binary) => binary::eval_binary(self, binary, params),
            Expr::Aggregation(agg) => aggregation::eval_aggregation(self, agg, params),
            Expr::Call(call) => functions::eval_call(self, call, params),
        }?;
        if !string_literal_pre_reserved {
            params.retain_value(&value)?;
        }
        Ok(value)
    }

    pub(crate) fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    pub(crate) fn default_lookback_delta(&self) -> i64 {
        self.default_lookback_delta
    }

    pub(crate) fn timestamp_units_per_second(&self) -> i64 {
        self.timestamp_units_per_second
    }

    pub(crate) fn default_subquery_step(&self) -> i64 {
        duration_to_units(DEFAULT_SUBQUERY_STEP_MS, self.timestamp_units_per_second)
    }

    fn build_prefetch_cache(
        &self,
        expr: &Expr,
        start: i64,
        end: i64,
        execution: &QueryExecution,
        memory: &PromqlMemoryTracker,
    ) -> Result<PrefetchCache> {
        execution.checkpoint().map_err(crate::TsinkError::from)?;
        let mut metrics = BTreeSet::new();
        let mut max_past_window = self.default_lookback_delta;
        let mut max_future_window = 0;
        self.collect_prefetch_requirements(
            expr,
            &mut metrics,
            &mut max_past_window,
            &mut max_future_window,
        );

        let fetch_start = start.saturating_sub(max_past_window);
        let fetch_end = end.saturating_add(max_future_window).saturating_add(1);

        let mut cache = PrefetchCache::default();
        for metric in metrics {
            execution.checkpoint().map_err(crate::TsinkError::from)?;
            let retained = selector::load_metric_rows_with_retained_execution(
                self,
                &metric,
                fetch_start,
                fetch_end,
                execution,
            )?;
            let (data, reservation) = retained.into_parts();
            if let Some(reservation) = reservation {
                memory.adopt(reservation);
            }
            cache.insert(metric, data);
        }

        Ok(cache)
    }

    fn supports_prefetch(&self, expr: &Expr) -> bool {
        !expr_uses_dynamic_time(expr)
    }

    fn collect_prefetch_requirements(
        &self,
        expr: &Expr,
        metrics: &mut BTreeSet<String>,
        max_past_window: &mut i64,
        max_future_window: &mut i64,
    ) {
        match expr {
            Expr::VectorSelector(VectorSelector {
                metric_name,
                offset,
                ..
            }) => {
                if let Some(metric) = metric_name {
                    metrics.insert(metric.clone());
                }
                let offset = duration_to_units(*offset, self.timestamp_units_per_second);
                let past = self.default_lookback_delta.saturating_add(offset.max(0));
                let future = if offset < 0 {
                    offset.saturating_neg()
                } else {
                    0
                };
                *max_past_window = (*max_past_window).max(past);
                *max_future_window = (*max_future_window).max(future);
            }
            Expr::MatrixSelector(MatrixSelector { vector, range }) => {
                if let Some(metric) = &vector.metric_name {
                    metrics.insert(metric.clone());
                }
                let offset = duration_to_units(vector.offset, self.timestamp_units_per_second);
                let range = duration_to_units(*range, self.timestamp_units_per_second);
                let past = range.saturating_add(offset.max(0));
                let future = if offset < 0 {
                    offset.saturating_neg()
                } else {
                    0
                };
                *max_past_window = (*max_past_window).max(past);
                *max_future_window = (*max_future_window).max(future);
            }
            Expr::Unary(u) => self.collect_prefetch_requirements(
                &u.expr,
                metrics,
                max_past_window,
                max_future_window,
            ),
            Expr::Binary(b) => {
                self.collect_prefetch_requirements(
                    &b.lhs,
                    metrics,
                    max_past_window,
                    max_future_window,
                );
                self.collect_prefetch_requirements(
                    &b.rhs,
                    metrics,
                    max_past_window,
                    max_future_window,
                );
            }
            Expr::Aggregation(a) => {
                if let Some(param) = &a.param {
                    self.collect_prefetch_requirements(
                        param,
                        metrics,
                        max_past_window,
                        max_future_window,
                    );
                }
                self.collect_prefetch_requirements(
                    &a.expr,
                    metrics,
                    max_past_window,
                    max_future_window,
                );
            }
            Expr::Call(c) => {
                for arg in &c.args {
                    self.collect_prefetch_requirements(
                        arg,
                        metrics,
                        max_past_window,
                        max_future_window,
                    );
                }
            }
            Expr::Subquery(SubqueryExpr { expr, .. }) => self.collect_prefetch_requirements(
                expr,
                metrics,
                max_past_window,
                max_future_window,
            ),
            Expr::Paren(inner) => self.collect_prefetch_requirements(
                inner,
                metrics,
                max_past_window,
                max_future_window,
            ),
            Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
        }
    }
}

fn expr_uses_dynamic_time(expr: &Expr) -> bool {
    match expr {
        Expr::VectorSelector(selector) => selector.at.is_some(),
        Expr::MatrixSelector(selector) => selector.vector.at.is_some(),
        Expr::Subquery(_) => true,
        Expr::Unary(unary) => expr_uses_dynamic_time(&unary.expr),
        Expr::Binary(binary) => {
            expr_uses_dynamic_time(&binary.lhs) || expr_uses_dynamic_time(&binary.rhs)
        }
        Expr::Aggregation(aggregation) => {
            aggregation
                .param
                .as_ref()
                .is_some_and(|param| expr_uses_dynamic_time(param))
                || expr_uses_dynamic_time(&aggregation.expr)
        }
        Expr::Call(call) => call.args.iter().any(expr_uses_dynamic_time),
        Expr::Paren(inner) => expr_uses_dynamic_time(inner),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => false,
    }
}

pub(crate) fn inclusive_step_count(start: i64, end: i64, step: i64) -> u64 {
    if step <= 0 || start > end {
        return 0;
    }
    let distance = (end as i128).saturating_sub(start as i128);
    let count = distance
        .checked_div(step as i128)
        .unwrap_or(i128::MAX)
        .saturating_add(1);
    u64::try_from(count).unwrap_or(u64::MAX)
}

fn charge_promql_result(execution: &QueryExecution, value: &PromqlValue) -> Result<()> {
    execution.checkpoint().map_err(crate::TsinkError::from)?;
    let (samples, bytes) = promql_value_returned_shape(value);
    execution
        .observe_intermediate_vector_size(samples)
        .map_err(crate::TsinkError::from)?;
    execution
        .charge_samples_returned(samples)
        .map_err(crate::TsinkError::from)?;
    execution
        .charge_returned_bytes(bytes)
        .map_err(crate::TsinkError::from)?;
    Ok(())
}

pub(crate) fn resolve_at_modifier(
    at: Option<&AtModifier>,
    params: &QueryParams<'_>,
    units_per_second: i64,
) -> Result<i64> {
    match at {
        None => Ok(params.eval_time),
        Some(AtModifier::Start) => Ok(params.query_start),
        Some(AtModifier::End) => Ok(params.query_end),
        Some(AtModifier::Timestamp(timestamp)) => timestamp_to_units(*timestamp, units_per_second),
    }
}

pub(crate) fn timestamp_to_units(timestamp: f64, units_per_second: i64) -> Result<i64> {
    if !timestamp.is_finite() {
        return Err(PromqlError::Eval(
            "@ timestamp must be a finite numeric value".to_string(),
        ));
    }

    let scaled = timestamp * units_per_second as f64;
    if !scaled.is_finite() || scaled < i64::MIN as f64 || scaled > i64::MAX as f64 {
        return Err(PromqlError::Eval(
            "@ timestamp is out of supported range".to_string(),
        ));
    }

    Ok(scaled.round() as i64)
}

fn negate_value(value: PromqlValue) -> Result<PromqlValue> {
    match value {
        PromqlValue::Scalar(v, ts) => Ok(PromqlValue::Scalar(-v, ts)),
        PromqlValue::InstantVector(mut samples) => {
            for s in &mut samples {
                if s.histogram.is_some() {
                    return Err(PromqlError::Eval(
                        "unary '-' does not support histogram samples yet".to_string(),
                    ));
                }
                s.value = -s.value;
            }
            Ok(PromqlValue::InstantVector(samples))
        }
        PromqlValue::RangeVector(mut series) => {
            for s in &mut series {
                if !s.histograms.is_empty() {
                    return Err(PromqlError::Eval(
                        "unary '-' does not support histogram samples yet".to_string(),
                    ));
                }
                for (_, v) in &mut s.samples {
                    *v = -*v;
                }
            }
            Ok(PromqlValue::RangeVector(series))
        }
        PromqlValue::String(v, ts) => Ok(PromqlValue::String(v, ts)),
    }
}

enum LatestSeriesPoint {
    Float(f64),
    Histogram(Box<crate::NativeHistogram>),
}

fn latest_series_point(
    latest_float: Option<(i64, f64)>,
    latest_histogram: Option<(i64, Box<crate::NativeHistogram>)>,
) -> Option<LatestSeriesPoint> {
    match (latest_float, latest_histogram) {
        (Some((float_ts, value)), Some((hist_ts, histogram))) => {
            if hist_ts > float_ts {
                Some(LatestSeriesPoint::Histogram(histogram))
            } else {
                Some(LatestSeriesPoint::Float(value))
            }
        }
        (Some((_, value)), None) => Some(LatestSeriesPoint::Float(value)),
        (None, Some((_, histogram))) => Some(LatestSeriesPoint::Histogram(histogram)),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HistogramCount, HistogramResetHint, QueryBudgetError, QueryBudgetLimits, QueryLimitReason,
        QueryWorkLimits,
    };

    fn histogram() -> NativeHistogram {
        NativeHistogram {
            count: Some(HistogramCount::Int(3)),
            sum: 6.0,
            schema: 1,
            zero_threshold: 0.0,
            zero_count: Some(HistogramCount::Int(0)),
            negative_spans: Vec::new(),
            negative_deltas: Vec::new(),
            negative_counts: Vec::new(),
            positive_spans: vec![crate::HistogramBucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_deltas: vec![1, 2],
            positive_counts: vec![1.0, 2.0],
            reset_hint: HistogramResetHint::No,
            custom_values: vec![0.5],
        }
    }

    #[test]
    fn promql_returned_shape_depends_on_content_not_capacity() {
        let compact_histogram = histogram();
        let mut roomy_histogram = compact_histogram.clone();
        roomy_histogram.positive_spans.reserve(32);
        roomy_histogram.positive_deltas.reserve(32);
        roomy_histogram.positive_counts.reserve(32);
        roomy_histogram.custom_values.reserve(32);

        let compact_sample = crate::promql::types::Sample {
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a")],
            timestamp: 10,
            value: 0.0,
            histogram: Some(Box::new(compact_histogram)),
        };
        let mut roomy_labels = Vec::with_capacity(32);
        roomy_labels.push(Label::new("host", "a"));
        let roomy_sample = crate::promql::types::Sample {
            metric: "cpu".to_string(),
            labels: roomy_labels,
            timestamp: 10,
            value: 0.0,
            histogram: Some(Box::new(roomy_histogram)),
        };
        let compact = PromqlValue::InstantVector(vec![compact_sample]);
        let mut roomy_samples = Vec::with_capacity(32);
        roomy_samples.push(roomy_sample);
        let roomy = PromqlValue::InstantVector(roomy_samples);

        assert_eq!(compact, roomy);
        assert_eq!(
            promql_value_returned_shape(&compact),
            promql_value_returned_shape(&roomy)
        );
        assert!(promql_value_shape(&roomy).1 > promql_value_shape(&compact).1);

        let compact_string = PromqlValue::String("x".to_string(), 0);
        let mut roomy_string_value = String::with_capacity(256);
        roomy_string_value.push('x');
        let roomy_string = PromqlValue::String(roomy_string_value, 0);
        assert_eq!(
            promql_value_returned_shape(&compact_string),
            promql_value_returned_shape(&roomy_string)
        );
        assert!(
            promql_value_shape(&roomy_string).1 > promql_value_shape(&compact_string).1,
            "retained-result accounting must use String capacity rather than logical length"
        );
    }

    #[test]
    fn parser_preparation_accepts_its_exact_memory_bound_and_rejects_one_less() {
        let query = format!("metric_name{{label=\"{}\"}}", "value".repeat(128));
        let exact_bytes = modeled_promql_parse_preparation_bytes(&query);
        assert!(exact_bytes > 1);

        for (limit, succeeds) in [(exact_bytes, true), (exact_bytes - 1, false)] {
            let budget = QueryBudget::new(QueryBudgetLimits {
                max_shared_memory_bytes: Some(limit),
                per_query: QueryWorkLimits {
                    max_memory_bytes: Some(limit),
                    ..QueryWorkLimits::default()
                },
                ..QueryBudgetLimits::default()
            })
            .unwrap();
            let execution = budget.begin_query().unwrap();
            let result = prepare_promql_parse_with_execution(&query, &execution);
            if succeeds {
                let (_expr, reservation) = result.unwrap();
                assert_eq!(reservation.bytes(), exact_bytes);
                assert_eq!(execution.snapshot().memory_reserved_bytes, exact_bytes);
                drop(reservation);
            } else {
                assert!(matches!(
                    result,
                    Err(PromqlError::Storage(crate::TsinkError::QueryBudget(
                        QueryBudgetError::LimitExceeded(exceeded)
                    ))) if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
                ));
            }
            assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
            drop(execution);
            let snapshot = budget.snapshot();
            assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
            assert_eq!(snapshot.accounting_invariant_violations_total, 0);
        }
    }
}
