use super::super::*;
use serde::ser::{Error as _, SerializeMap, SerializeSeq, SerializeStruct};

const MAX_PROMQL_SCALAR_PARAMETER_BYTES: usize = 256;
const PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;
const PROMQL_HTTP_DIAGNOSTIC_ENVELOPE_BYTES: u64 = 4 * 1024;
const PROMQL_HTTP_HEADER_ENVELOPE_BYTES: u64 = 2 * 1024;
const PROMQL_JSON_NUMBER_BYTES_UPPER: u64 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromqlParameterError {
    Missing {
        field: &'static str,
    },
    TooLong {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
}

fn preflight_promql_parameter<'a>(
    request: &'a HttpRequest,
    field: &'static str,
    maximum: usize,
    required: bool,
) -> Result<Option<&'a str>, PromqlParameterError> {
    let Some(raw) = request.raw_param(field) else {
        return if required {
            Err(PromqlParameterError::Missing { field })
        } else {
            Ok(None)
        };
    };
    let decoded_len = percent_decoded_len(raw);
    if raw.len() > maximum.saturating_mul(3) || decoded_len > maximum {
        return Err(PromqlParameterError::TooLong {
            field,
            actual: decoded_len,
            maximum,
        });
    }
    Ok(Some(raw))
}

fn promql_parameter_error_response(error: PromqlParameterError) -> HttpResponse {
    match error {
        PromqlParameterError::Missing { field } => {
            promql_error_response("bad_data", &format!("missing required parameter '{field}'"))
        }
        PromqlParameterError::TooLong {
            field,
            actual,
            maximum,
        } => promql_error_response(
            "bad_data",
            &format!("parameter '{field}' is {actual} bytes, exceeding the hard limit {maximum}"),
        ),
    }
}

fn modeled_percent_decode_upper_bytes(raw: &str) -> u64 {
    // `percent_decode` first allocates a raw-length byte vector. Invalid UTF-8 may then allocate
    // up to three replacement bytes per decoded byte while that vector is still live.
    u64::try_from(raw.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(4)
        .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(2))
}

fn modeled_promql_request_upper_bytes(query: &str, scalar_params: &[&str]) -> u64 {
    scalar_params.iter().fold(
        modeled_percent_decode_upper_bytes(query)
            .saturating_add(PROMQL_HTTP_DIAGNOSTIC_ENVELOPE_BYTES),
        |bytes, raw| bytes.saturating_add(modeled_percent_decode_upper_bytes(raw)),
    )
}

fn decode_preflighted_promql_parameter(
    raw: &str,
    field: &'static str,
    maximum: usize,
) -> Result<String, PromqlParameterError> {
    let decoded = percent_decode(raw);
    if decoded.len() > maximum {
        return Err(PromqlParameterError::TooLong {
            field,
            actual: decoded.len(),
            maximum,
        });
    }
    Ok(decoded)
}

struct PromqlHttpCancellationGuard {
    token: tsink::QueryCancellationToken,
}

impl Drop for PromqlHttpCancellationGuard {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

fn promql_internal_error_response(code: &'static str, diagnostic: &'static str) -> HttpResponse {
    json_response(
        500,
        &json!({
            "status": "error",
            "errorType": "execution",
            "error": diagnostic,
        }),
    )
    .with_header(READ_ERROR_CODE_HEADER, code)
}

fn begin_promql_http_execution(
    storage: &Arc<dyn Storage>,
) -> Result<(tsink::QueryExecution, PromqlHttpCancellationGuard), HttpResponse> {
    let limits = tsink::QueryWorkLimits::default();
    let cancellation = tsink::QueryCancellationToken::new();
    match storage.begin_query_execution(limits, cancellation.clone()) {
        Ok(Some(execution)) => Ok((
            execution,
            PromqlHttpCancellationGuard {
                token: cancellation,
            },
        )),
        Ok(None) => Err(promql_internal_error_response(
            "promql_unaccounted_storage_backend",
            "PromQL requires an execution-accounted storage backend",
        )),
        Err(tsink::TsinkError::QueryBudget(error)) => {
            Err(promql_query_budget_error_response(&error))
        }
        Err(error) => Err(promql_storage_error_response(&error)),
    }
}

fn saturating_u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn modeled_promql_json_string_upper_bytes(value: &str) -> u64 {
    // Every input byte can expand to a six-byte JSON `\\u00xx` escape.
    saturating_u64_from_usize(value.len())
        .saturating_mul(6)
        .saturating_add(2)
}

fn modeled_promql_metric_json_upper_bytes(metric: &str, labels: &[Label]) -> u64 {
    labels.iter().fold(
        16u64
            .saturating_add(modeled_promql_json_string_upper_bytes("__name__"))
            .saturating_add(modeled_promql_json_string_upper_bytes(metric)),
        |bytes, label| {
            bytes
                .saturating_add(2)
                .saturating_add(modeled_promql_json_string_upper_bytes(&label.name))
                .saturating_add(modeled_promql_json_string_upper_bytes(&label.value))
        },
    )
}

fn promql_histogram_bucket_count_upper(histogram: &tsink::NativeHistogram) -> usize {
    tsink::promql::types::histogram_bucket_count(histogram)
}

fn modeled_promql_histogram_json_upper_bytes(histogram: &tsink::NativeHistogram) -> u64 {
    let bucket_count = saturating_u64_from_usize(promql_histogram_bucket_count_upper(histogram));
    128u64.saturating_add(
        bucket_count
            .saturating_mul(32u64.saturating_add(PROMQL_JSON_NUMBER_BYTES_UPPER.saturating_mul(4))),
    )
}

fn modeled_promql_histogram_serialization_scratch_bytes(histogram: &tsink::NativeHistogram) -> u64 {
    let negative = histogram.negative_spans.iter().fold(0usize, |count, span| {
        count.saturating_add(span.length as usize)
    });
    let positive = histogram.positive_spans.iter().fold(0usize, |count, span| {
        count.saturating_add(span.length as usize)
    });
    let buckets = promql_histogram_bucket_count_upper(histogram);
    let decoded_side = negative.max(positive);
    saturating_u64_from_usize(decoded_side)
        .saturating_mul(
            saturating_u64_from_usize(std::mem::size_of::<f64>())
                .saturating_add(saturating_u64_from_usize(std::mem::size_of::<(i32, f64)>())),
        )
        .saturating_add(
            saturating_u64_from_usize(buckets)
                .saturating_mul(saturating_u64_from_usize(std::mem::size_of::<
                    tsink::promql::types::HistogramBucket,
                >()))
                // The final bucket vector is exactly preallocated in core. Two copies remain a
                // conservative allowance for allocator rounding and implementation drift.
                .saturating_mul(2),
        )
        .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(4))
}

fn modeled_promql_json_upper_bytes(value: &PromqlValue) -> (u64, u64) {
    let mut scratch = PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(4);
    let body = match value {
        PromqlValue::Scalar(_, _) => 256,
        PromqlValue::String(value, _) => {
            256u64.saturating_add(modeled_promql_json_string_upper_bytes(value))
        }
        PromqlValue::InstantVector(samples) => samples.iter().fold(256u64, |bytes, sample| {
            let item = 128u64
                .saturating_add(modeled_promql_metric_json_upper_bytes(
                    &sample.metric,
                    &sample.labels,
                ))
                .saturating_add(match sample.histogram.as_deref() {
                    Some(histogram) => {
                        scratch = scratch.max(
                            modeled_promql_histogram_serialization_scratch_bytes(histogram),
                        );
                        modeled_promql_histogram_json_upper_bytes(histogram)
                    }
                    None => PROMQL_JSON_NUMBER_BYTES_UPPER.saturating_mul(2),
                });
            bytes.saturating_add(item)
        }),
        PromqlValue::RangeVector(series) => series.iter().fold(256u64, |bytes, series| {
            let sample_bytes = saturating_u64_from_usize(series.samples.len())
                .saturating_mul(32u64.saturating_add(PROMQL_JSON_NUMBER_BYTES_UPPER));
            let histogram_bytes = series
                .histograms
                .iter()
                .fold(0u64, |bytes, (_, histogram)| {
                    scratch = scratch.max(modeled_promql_histogram_serialization_scratch_bytes(
                        histogram.as_ref(),
                    ));
                    bytes
                        .saturating_add(PROMQL_JSON_NUMBER_BYTES_UPPER)
                        .saturating_add(modeled_promql_histogram_json_upper_bytes(
                            histogram.as_ref(),
                        ))
                        .saturating_add(32)
                });
            bytes
                .saturating_add(128)
                .saturating_add(modeled_promql_metric_json_upper_bytes(
                    &series.metric,
                    &series.labels,
                ))
                .saturating_add(sample_bytes)
                .saturating_add(histogram_bytes)
        }),
    };
    (body, scratch)
}

struct PromqlFormattedNumber(f64);

impl Serialize for PromqlFormattedNumber {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if self.0.is_nan() {
            serializer.serialize_str("NaN")
        } else if self.0 == f64::INFINITY {
            serializer.serialize_str("+Inf")
        } else if self.0 == f64::NEG_INFINITY {
            serializer.serialize_str("-Inf")
        } else {
            serializer.collect_str(&self.0)
        }
    }
}

struct PromqlMetricJson<'a> {
    metric: &'a str,
    labels: &'a [Label],
}

impl Serialize for PromqlMetricJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // `serde_json::Map` historically sorted keys lexically and overwrote duplicate keys.
        // Select the next unique key by scanning the bounded label slice so the wire order and
        // overwrite behavior remain identical without allocating a temporary map or sorting copy.
        let entry_count = 1usize.saturating_add(
            self.labels
                .iter()
                .enumerate()
                .filter(|(index, label)| {
                    label.name != "__name__"
                        && !self.labels[..*index]
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
            for label in self.labels {
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
                Some(self.metric)
            } else {
                None
            };
            for label in self.labels {
                if label.name == name {
                    value = Some(label.value.as_str());
                }
            }
            map.serialize_entry(name, value.expect("selected metric key has a value"))?;
            previous_name = Some(name);
        }
        map.end()
    }
}

struct PromqlTimestampedValue {
    timestamp: i64,
    value: f64,
    precision: TimestampPrecision,
}

impl Serialize for PromqlTimestampedValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&timestamp_to_f64(self.timestamp, self.precision))?;
        seq.serialize_element(&PromqlFormattedNumber(self.value))?;
        seq.end()
    }
}

struct PromqlTimestampedString<'a> {
    timestamp: i64,
    value: &'a str,
    precision: TimestampPrecision,
}

impl Serialize for PromqlTimestampedString<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&timestamp_to_f64(self.timestamp, self.precision))?;
        seq.serialize_element(self.value)?;
        seq.end()
    }
}

struct PromqlHistogramJson<'a>(&'a tsink::NativeHistogram);

impl Serialize for PromqlHistogramJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Malformed/unsupported bucket payloads must not become a successful empty-bucket result.
        // Keep the serializer diagnostic static; the public mapper returns a bounded error.
        let buckets = histogram_buckets(self.0)
            .map_err(|_| S::Error::custom("invalid native histogram bucket payload"))?;
        let mut object = serializer.serialize_struct("PromqlHistogram", 3)?;
        object.serialize_field("buckets", &PromqlHistogramBucketsJson(&buckets))?;
        object.serialize_field(
            "count",
            &PromqlFormattedNumber(histogram_count_value(self.0)),
        )?;
        object.serialize_field("sum", &PromqlFormattedNumber(self.0.sum))?;
        object.end()
    }
}

struct PromqlHistogramBucketsJson<'a>(&'a [tsink::promql::types::HistogramBucket]);

impl Serialize for PromqlHistogramBucketsJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buckets = serializer.serialize_seq(Some(self.0.len()))?;
        for bucket in self.0 {
            buckets.serialize_element(&PromqlHistogramBucketJson(bucket))?;
        }
        buckets.end()
    }
}

struct PromqlHistogramBucketJson<'a>(&'a tsink::promql::types::HistogramBucket);

impl Serialize for PromqlHistogramBucketJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut bucket = serializer.serialize_seq(Some(4))?;
        bucket.serialize_element(&histogram_bucket_boundary_code(
            self.0.lower_inclusive,
            self.0.upper_inclusive,
        ))?;
        bucket.serialize_element(&PromqlFormattedNumber(self.0.lower))?;
        bucket.serialize_element(&PromqlFormattedNumber(self.0.upper))?;
        bucket.serialize_element(&PromqlFormattedNumber(self.0.count))?;
        bucket.end()
    }
}

struct PromqlTimestampedHistogram<'a> {
    timestamp: i64,
    histogram: &'a tsink::NativeHistogram,
    precision: TimestampPrecision,
}

impl Serialize for PromqlTimestampedHistogram<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&timestamp_to_f64(self.timestamp, self.precision))?;
        seq.serialize_element(&PromqlHistogramJson(self.histogram))?;
        seq.end()
    }
}

struct PromqlInstantSampleJson<'a> {
    sample: &'a tsink::promql::types::Sample,
    precision: TimestampPrecision,
}

impl Serialize for PromqlInstantSampleJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Some(histogram) = self.sample.histogram.as_deref() {
            let mut object = serializer.serialize_struct("PromqlInstantSample", 2)?;
            object.serialize_field(
                "histogram",
                &PromqlTimestampedHistogram {
                    timestamp: self.sample.timestamp,
                    histogram,
                    precision: self.precision,
                },
            )?;
            object.serialize_field(
                "metric",
                &PromqlMetricJson {
                    metric: &self.sample.metric,
                    labels: &self.sample.labels,
                },
            )?;
            object.end()
        } else {
            let mut object = serializer.serialize_struct("PromqlInstantSample", 2)?;
            object.serialize_field(
                "metric",
                &PromqlMetricJson {
                    metric: &self.sample.metric,
                    labels: &self.sample.labels,
                },
            )?;
            object.serialize_field(
                "value",
                &PromqlTimestampedValue {
                    timestamp: self.sample.timestamp,
                    value: self.sample.value,
                    precision: self.precision,
                },
            )?;
            object.end()
        }
    }
}

struct PromqlSampleValuesJson<'a> {
    samples: &'a [(i64, f64)],
    precision: TimestampPrecision,
}

impl Serialize for PromqlSampleValuesJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut values = serializer.serialize_seq(Some(self.samples.len()))?;
        for (timestamp, value) in self.samples {
            values.serialize_element(&PromqlTimestampedValue {
                timestamp: *timestamp,
                value: *value,
                precision: self.precision,
            })?;
        }
        values.end()
    }
}

struct PromqlHistogramValuesJson<'a> {
    histograms: &'a [(i64, Box<tsink::NativeHistogram>)],
    precision: TimestampPrecision,
}

impl Serialize for PromqlHistogramValuesJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut histograms = serializer.serialize_seq(Some(self.histograms.len()))?;
        for (timestamp, histogram) in self.histograms {
            histograms.serialize_element(&PromqlTimestampedHistogram {
                timestamp: *timestamp,
                histogram,
                precision: self.precision,
            })?;
        }
        histograms.end()
    }
}

struct PromqlRangeSeriesJson<'a> {
    series: &'a tsink::promql::types::Series,
    precision: TimestampPrecision,
}

impl Serialize for PromqlRangeSeriesJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let field_count = 2 + usize::from(!self.series.histograms.is_empty());
        let mut object = serializer.serialize_struct("PromqlRangeSeries", field_count)?;
        if !self.series.histograms.is_empty() {
            object.serialize_field(
                "histograms",
                &PromqlHistogramValuesJson {
                    histograms: &self.series.histograms,
                    precision: self.precision,
                },
            )?;
        }
        object.serialize_field(
            "metric",
            &PromqlMetricJson {
                metric: &self.series.metric,
                labels: &self.series.labels,
            },
        )?;
        object.serialize_field(
            "values",
            &PromqlSampleValuesJson {
                samples: &self.series.samples,
                precision: self.precision,
            },
        )?;
        object.end()
    }
}

struct PromqlResultJson<'a> {
    value: &'a PromqlValue,
    precision: TimestampPrecision,
}

impl Serialize for PromqlResultJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.value {
            PromqlValue::Scalar(value, timestamp) => PromqlTimestampedValue {
                timestamp: *timestamp,
                value: *value,
                precision: self.precision,
            }
            .serialize(serializer),
            PromqlValue::String(value, timestamp) => PromqlTimestampedString {
                timestamp: *timestamp,
                value,
                precision: self.precision,
            }
            .serialize(serializer),
            PromqlValue::InstantVector(samples) => {
                let mut values = serializer.serialize_seq(Some(samples.len()))?;
                for sample in samples {
                    values.serialize_element(&PromqlInstantSampleJson {
                        sample,
                        precision: self.precision,
                    })?;
                }
                values.end()
            }
            PromqlValue::RangeVector(series) => {
                let mut values = serializer.serialize_seq(Some(series.len()))?;
                for item in series {
                    values.serialize_element(&PromqlRangeSeriesJson {
                        series: item,
                        precision: self.precision,
                    })?;
                }
                values.end()
            }
        }
    }
}

#[derive(Serialize)]
struct PromqlSuccessDataJson<'a> {
    result: PromqlResultJson<'a>,
    #[serde(rename = "resultType")]
    result_type: &'static str,
}

#[derive(Serialize)]
struct PromqlSuccessJson<'a> {
    data: PromqlSuccessDataJson<'a>,
    status: &'static str,
}

fn promql_result_type(value: &PromqlValue) -> &'static str {
    match value {
        PromqlValue::Scalar(_, _) => "scalar",
        PromqlValue::InstantVector(_) => "vector",
        PromqlValue::RangeVector(_) => "matrix",
        PromqlValue::String(_, _) => "string",
    }
}

struct FixedCapacityJsonBuffer {
    bytes: Vec<u8>,
    limit: usize,
    limit_exceeded_at: Option<usize>,
}

impl std::io::Write for FixedCapacityJsonBuffer {
    fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(input.len())
            .ok_or_else(|| std::io::Error::other("PromQL JSON response length overflow"))?;
        if next > self.limit {
            self.limit_exceeded_at = Some(next);
            return Err(std::io::Error::other(
                "PromQL JSON response exceeded its preadmitted envelope",
            ));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
enum PromqlResponseEncodingError {
    Budget(tsink::QueryBudgetError),
    TooLarge { requested: u64 },
    Allocation,
    Serialization,
}

struct EncodedPromqlHttpResponse {
    response: HttpResponse,
    reservation: tsink::QueryMemoryReservation,
}

fn encode_promql_success_response(
    value: &PromqlValue,
    precision: TimestampPrecision,
    execution: &tsink::QueryExecution,
) -> Result<EncodedPromqlHttpResponse, PromqlResponseEncodingError> {
    execution
        .checkpoint()
        .map_err(PromqlResponseEncodingError::Budget)?;
    let (body_upper, scratch_upper) = modeled_promql_json_upper_bytes(value);
    let body_capacity_u64 = body_upper.min(saturating_u64_from_usize(MAX_BODY_BYTES));
    let body_capacity =
        usize::try_from(body_capacity_u64).map_err(|_| PromqlResponseEncodingError::TooLarge {
            requested: body_upper,
        })?;
    let reserved_bytes = body_capacity_u64
        .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES)
        .saturating_add(scratch_upper)
        .saturating_add(PROMQL_HTTP_HEADER_ENVELOPE_BYTES);
    let mut reservation = execution
        .reserve_memory(reserved_bytes)
        .map_err(PromqlResponseEncodingError::Budget)?;
    let mut body = Vec::new();
    body.try_reserve_exact(body_capacity)
        .map_err(|_| PromqlResponseEncodingError::Allocation)?;
    let mut writer = FixedCapacityJsonBuffer {
        bytes: body,
        limit: body_capacity,
        limit_exceeded_at: None,
    };
    if serde_json::to_writer(
        &mut writer,
        &PromqlSuccessJson {
            data: PromqlSuccessDataJson {
                result: PromqlResultJson { value, precision },
                result_type: promql_result_type(value),
            },
            status: "success",
        },
    )
    .is_err()
    {
        return Err(match writer.limit_exceeded_at {
            Some(requested) if writer.limit == MAX_BODY_BYTES => {
                PromqlResponseEncodingError::TooLarge {
                    requested: saturating_u64_from_usize(requested),
                }
            }
            _ => PromqlResponseEncodingError::Serialization,
        });
    }
    execution
        .charge_returned_bytes(saturating_u64_from_usize(writer.bytes.len()))
        .map_err(PromqlResponseEncodingError::Budget)?;
    reservation
        .resize(
            saturating_u64_from_usize(writer.bytes.capacity())
                .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES)
                .saturating_add(PROMQL_HTTP_HEADER_ENVELOPE_BYTES),
        )
        .map_err(PromqlResponseEncodingError::Budget)?;
    execution
        .checkpoint()
        .map_err(PromqlResponseEncodingError::Budget)?;
    Ok(EncodedPromqlHttpResponse {
        response: HttpResponse::new(200, writer.bytes)
            .with_header("Content-Type", "application/json"),
        reservation,
    })
}

fn enforce_preflighted_tenant_query_length(
    tenant_plan: &tenant::TenantRequestPlan,
    raw_query: &str,
) -> Result<(), HttpResponse> {
    let Some(limit) = tenant_plan.policy().max_query_length_bytes else {
        return Ok(());
    };
    let actual = percent_decoded_len(raw_query);
    if actual <= limit {
        return Ok(());
    }
    let diagnostic = format!("tenant query length limit exceeded: {actual} > {limit}");
    tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, diagnostic.clone());
    Err(promql_error_response("bad_data", &diagnostic))
}

fn reserve_promql_request_decode(
    execution: &tsink::QueryExecution,
    raw_query: &str,
    scalar_params: &[&str],
) -> Result<tsink::QueryMemoryReservation, HttpResponse> {
    execution
        .checkpoint()
        .map_err(|error| promql_query_budget_error_response(&error))?;
    execution
        .reserve_memory(modeled_promql_request_upper_bytes(raw_query, scalar_params))
        .map_err(|error| promql_query_budget_error_response(&error))
}

struct PromqlSuccessContext<'a> {
    distributed_storage: Option<&'a Arc<DistributedStorageAdapter>>,
    precision: TimestampPrecision,
    tenant_id: &'a str,
    usage_kind: &'static str,
    request_path: &'a str,
    request_body_bytes: u64,
    usage_accounting: Option<&'a UsageAccounting>,
    started: Instant,
}

async fn finish_promql_success(
    result: tsink::promql::PromqlExecutionResult,
    execution: tsink::QueryExecution,
    request_reservation: tsink::QueryMemoryReservation,
    context: PromqlSuccessContext<'_>,
) -> HttpResponse {
    let PromqlSuccessContext {
        distributed_storage,
        precision,
        tenant_id,
        usage_kind,
        request_path,
        request_body_bytes,
        usage_accounting,
        started,
    } = context;
    let result_units = promql_value_units(result.value());
    let EncodedPromqlHttpResponse {
        mut response,
        reservation: response_reservation,
    } = match encode_promql_success_response(result.value(), precision, &execution) {
        Ok(encoded) => encoded,
        Err(error) => return promql_response_encoding_error_response(error),
    };

    // Move the accumulated metadata and its reservation out of the adapter. This avoids cloning
    // warning strings for the response snapshot and keeps their charge alive through headers and
    // usage recording.
    let accounted_metadata = match distributed_storage {
        Some(storage) => match storage.take_accounted_read_metadata() {
            Ok(metadata) => Some(metadata),
            Err(_) => {
                return promql_internal_error_response(
                    "promql_distributed_metadata_failure",
                    "failed to finalize distributed PromQL read metadata",
                )
            }
        },
        None => None,
    };
    if let Some(metadata) = accounted_metadata.as_ref() {
        response = with_read_metadata_headers(response, &metadata.metadata);
    }

    record_query_pressure(tenant_id, 1, result_units);
    record_query_usage(
        usage_accounting,
        tenant_id,
        usage_kind,
        request_path,
        QueryUsageMetrics::new(
            1,
            result_units as u64,
            elapsed_nanos_since(started),
            request_body_bytes,
        ),
    )
    .await;

    // The result, distributed diagnostics, decoded request, encoded body, and header allocations
    // remain charged until `HttpResponse` owns every byte needed by the transport.
    drop(result);
    drop(accounted_metadata);
    drop(response_reservation);
    drop(request_reservation);
    drop(execution);
    response
}

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
    let raw_query = match preflight_promql_parameter(
        request,
        "query",
        tsink::promql::MAX_PARSE_INPUT_BYTES,
        true,
    ) {
        Ok(Some(query)) => query,
        Ok(None) => unreachable!("required PromQL query preflight returned no value"),
        Err(error) => return promql_parameter_error_response(error),
    };
    let raw_time =
        match preflight_promql_parameter(request, "time", MAX_PROMQL_SCALAR_PARAMETER_BYTES, false)
        {
            Ok(time) => time,
            Err(error) => return promql_parameter_error_response(error),
        };
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
    if let Err(response) = enforce_preflighted_tenant_query_length(&tenant_plan, raw_query) {
        return response;
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
    let (execution, _cancellation_guard) = match begin_promql_http_execution(&storage) {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    let request_reservation =
        match reserve_promql_request_decode(&execution, raw_query, raw_time.as_slice()) {
            Ok(reservation) => reservation,
            Err(response) => return response,
        };
    let query = match decode_preflighted_promql_parameter(
        raw_query,
        "query",
        tsink::promql::MAX_PARSE_INPUT_BYTES,
    ) {
        Ok(query) => query,
        Err(error) => return promql_parameter_error_response(error),
    };
    if let Err(err) = tenant::enforce_query_length_quota(tenant_plan.policy(), &query) {
        tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, err.clone());
        return promql_error_response("bad_data", &err);
    }
    let time_text = match raw_time {
        Some(raw) => match decode_preflighted_promql_parameter(
            raw,
            "time",
            MAX_PROMQL_SCALAR_PARAMETER_BYTES,
        ) {
            Ok(value) => Some(value),
            Err(error) => return promql_parameter_error_response(error),
        },
        None => None,
    };
    let time = match time_text.as_deref() {
        Some(value) => match parse_timestamp(value, precision) {
            Ok(ts) => ts,
            Err(_) => return promql_error_response("bad_data", "parameter 'time' is invalid"),
        },
        None => current_timestamp(precision),
    };

    let engine = Engine::with_precision(Arc::clone(&storage), precision);
    let execution_for_task = execution.clone();
    let result = match tokio::task::spawn_blocking(move || {
        engine.instant_query_with_execution_result(&query, time, &execution_for_task)
    })
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => return promql_execution_error_response(&error),
        Err(_) => {
            return promql_internal_error_response(
                "promql_query_task_failed",
                "PromQL query task failed",
            )
        }
    };

    finish_promql_success(
        result,
        execution,
        request_reservation,
        PromqlSuccessContext {
            distributed_storage: distributed_storage.as_ref(),
            precision,
            tenant_id: &tenant_id,
            usage_kind: "instant_query",
            request_path: request.path_without_query(),
            request_body_bytes: request.body.len() as u64,
            usage_accounting: context.usage_accounting,
            started,
        },
    )
    .await
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
    let raw_query = match preflight_promql_parameter(
        request,
        "query",
        tsink::promql::MAX_PARSE_INPUT_BYTES,
        true,
    ) {
        Ok(Some(query)) => query,
        Ok(None) => unreachable!("required PromQL query preflight returned no value"),
        Err(error) => return promql_parameter_error_response(error),
    };
    let raw_start =
        match preflight_promql_parameter(request, "start", MAX_PROMQL_SCALAR_PARAMETER_BYTES, true)
        {
            Ok(Some(value)) => value,
            Ok(None) => unreachable!("required PromQL start preflight returned no value"),
            Err(error) => return promql_parameter_error_response(error),
        };
    let raw_end =
        match preflight_promql_parameter(request, "end", MAX_PROMQL_SCALAR_PARAMETER_BYTES, true) {
            Ok(Some(value)) => value,
            Ok(None) => unreachable!("required PromQL end preflight returned no value"),
            Err(error) => return promql_parameter_error_response(error),
        };
    let raw_step = match preflight_promql_parameter(
        request,
        "step",
        MAX_PROMQL_SCALAR_PARAMETER_BYTES,
        true,
    ) {
        Ok(Some(value)) => value,
        Ok(None) => unreachable!("required PromQL step preflight returned no value"),
        Err(error) => return promql_parameter_error_response(error),
    };
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
    if let Err(response) = enforce_preflighted_tenant_query_length(&tenant_plan, raw_query) {
        return response;
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
    let (execution, _cancellation_guard) = match begin_promql_http_execution(&storage) {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    let request_reservation =
        match reserve_promql_request_decode(&execution, raw_query, &[raw_start, raw_end, raw_step])
        {
            Ok(reservation) => reservation,
            Err(response) => return response,
        };
    let query = match decode_preflighted_promql_parameter(
        raw_query,
        "query",
        tsink::promql::MAX_PARSE_INPUT_BYTES,
    ) {
        Ok(query) => query,
        Err(error) => return promql_parameter_error_response(error),
    };
    if let Err(err) = tenant::enforce_query_length_quota(tenant_plan.policy(), &query) {
        tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, err.clone());
        return promql_error_response("bad_data", &err);
    }
    let start_text = match decode_preflighted_promql_parameter(
        raw_start,
        "start",
        MAX_PROMQL_SCALAR_PARAMETER_BYTES,
    ) {
        Ok(value) => value,
        Err(error) => return promql_parameter_error_response(error),
    };
    let end_text = match decode_preflighted_promql_parameter(
        raw_end,
        "end",
        MAX_PROMQL_SCALAR_PARAMETER_BYTES,
    ) {
        Ok(value) => value,
        Err(error) => return promql_parameter_error_response(error),
    };
    let step_text = match decode_preflighted_promql_parameter(
        raw_step,
        "step",
        MAX_PROMQL_SCALAR_PARAMETER_BYTES,
    ) {
        Ok(value) => value,
        Err(error) => return promql_parameter_error_response(error),
    };
    let start = match parse_timestamp(&start_text, precision) {
        Ok(timestamp) => timestamp,
        Err(_) => return promql_error_response("bad_data", "parameter 'start' is invalid"),
    };
    let end = match parse_timestamp(&end_text, precision) {
        Ok(timestamp) => timestamp,
        Err(_) => return promql_error_response("bad_data", "parameter 'end' is invalid"),
    };
    if end < start {
        return promql_error_response("bad_data", "end timestamp must not be before start time");
    }
    let step = match parse_step(&step_text, precision) {
        Ok(step) => step,
        Err(_) => return promql_error_response("bad_data", "parameter 'step' is invalid"),
    };
    if let Err(err) = tenant::enforce_range_points_quota(tenant_plan.policy(), start, end, step) {
        tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, err.clone());
        return promql_error_response("bad_data", &err);
    }

    let engine = Engine::with_precision(Arc::clone(&storage), precision);
    let execution_for_task = execution.clone();
    let result = match tokio::task::spawn_blocking(move || {
        engine.range_query_with_execution_result(&query, start, end, step, &execution_for_task)
    })
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => return promql_execution_error_response(&error),
        Err(_) => {
            return promql_internal_error_response(
                "promql_query_task_failed",
                "PromQL query task failed",
            )
        }
    };

    finish_promql_success(
        result,
        execution,
        request_reservation,
        PromqlSuccessContext {
            distributed_storage: distributed_storage.as_ref(),
            precision,
            tenant_id: &tenant_id,
            usage_kind: "range_query",
            request_path: request.path_without_query(),
            request_body_bytes: request.body.len() as u64,
            usage_accounting: context.usage_accounting,
            started,
        },
    )
    .await
}

fn promql_query_budget_error_response(error: &tsink::QueryBudgetError) -> HttpResponse {
    let response = |status, error_type: &str, error_code: &str, diagnostic: String| {
        json_response(
            status,
            &json!({
                "status": "error",
                "errorType": error_type,
                "error": diagnostic,
            }),
        )
        .with_header(READ_ERROR_CODE_HEADER, error_code)
    };

    match error {
        tsink::QueryBudgetError::InvalidLimits(_) => response(
            400,
            "invalid_query_limits",
            "invalid_query_limits",
            "invalid PromQL query limits".to_string(),
        ),
        tsink::QueryBudgetError::LimitExceeded(exceeded) => {
            let code = format!("query_limit_{}", exceeded.reason.as_str());
            let retryable = matches!(
                exceeded.reason,
                tsink::QueryLimitReason::ConcurrentQueries
                    | tsink::QueryLimitReason::SharedMemoryBytes
            );
            let mut response = response(
                if retryable { 429 } else { 413 },
                &code,
                &code,
                error.to_string(),
            );
            if retryable {
                response = response.with_header("Retry-After", "1");
            }
            response
        }
        tsink::QueryBudgetError::Cancelled => response(
            503,
            "canceled",
            "query_cancelled",
            "PromQL query was canceled".to_string(),
        ),
        tsink::QueryBudgetError::DeadlineExceeded => response(
            503,
            "timeout",
            "query_deadline_exceeded",
            "PromQL query deadline exceeded".to_string(),
        ),
        _ => promql_error_response("execution", "PromQL query budget failed"),
    }
}

fn promql_response_encoding_error_response(error: PromqlResponseEncodingError) -> HttpResponse {
    match error {
        PromqlResponseEncodingError::Budget(error) => promql_query_budget_error_response(&error),
        PromqlResponseEncodingError::TooLarge { requested } => promql_query_budget_error_response(
            &tsink::QueryBudgetError::LimitExceeded(tsink::QueryLimitExceeded::new(
                tsink::QueryLimitReason::ReturnedBytes,
                saturating_u64_from_usize(MAX_BODY_BYTES),
                0,
                requested,
            )),
        ),
        PromqlResponseEncodingError::Allocation => json_response(
            500,
            &json!({
                "status": "error",
                "errorType": "execution",
                "error": "failed to allocate the bounded PromQL response",
            }),
        )
        .with_header(READ_ERROR_CODE_HEADER, "promql_response_allocation_failed"),
        PromqlResponseEncodingError::Serialization => json_response(
            500,
            &json!({
                "status": "error",
                "errorType": "execution",
                "error": "failed to serialize the PromQL response",
            }),
        )
        .with_header(
            READ_ERROR_CODE_HEADER,
            "promql_response_serialization_failed",
        ),
    }
}

fn promql_storage_error_response(error: &tsink::TsinkError) -> HttpResponse {
    let (status, code, diagnostic) = match error {
        tsink::TsinkError::QueryBudget(error) => return promql_query_budget_error_response(error),
        tsink::TsinkError::InvalidTimeRange { .. }
        | tsink::TsinkError::MetricRequired
        | tsink::TsinkError::InvalidMetricName(_)
        | tsink::TsinkError::InvalidLabel(_)
        | tsink::TsinkError::InvalidConfiguration(_)
        | tsink::TsinkError::UnsupportedOperation { .. }
        | tsink::TsinkError::UnsupportedAggregation { .. }
        | tsink::TsinkError::ValueTypeMismatch { .. } => (
            422,
            "promql_storage_invalid_request",
            "invalid PromQL storage request",
        ),
        tsink::TsinkError::StorageShuttingDown
        | tsink::TsinkError::StorageClosed
        | tsink::TsinkError::ChannelSend { .. }
        | tsink::TsinkError::ChannelReceive { .. }
        | tsink::TsinkError::ChannelTimeout { .. } => (
            503,
            "promql_storage_unavailable",
            "PromQL storage is unavailable",
        ),
        _ => (
            500,
            "promql_storage_failure",
            "PromQL storage evaluation failed",
        ),
    };
    json_response(
        status,
        &json!({
            "status": "error",
            "errorType": "execution",
            "error": diagnostic,
        }),
    )
    .with_header(READ_ERROR_CODE_HEADER, code)
}

fn promql_execution_error_response(error: &tsink::promql::PromqlError) -> HttpResponse {
    match error {
        tsink::promql::PromqlError::Parse(_)
        | tsink::promql::PromqlError::UnexpectedToken { .. } => {
            promql_error_response("bad_data", "invalid PromQL syntax")
                .with_header(READ_ERROR_CODE_HEADER, "promql_invalid_syntax")
        }
        tsink::promql::PromqlError::UnknownFunction(_) => {
            promql_error_response("bad_data", "unknown PromQL function")
                .with_header(READ_ERROR_CODE_HEADER, "promql_unknown_function")
        }
        tsink::promql::PromqlError::ArgumentCount { .. } | tsink::promql::PromqlError::Type(_) => {
            promql_error_response("bad_data", "invalid PromQL expression")
                .with_header(READ_ERROR_CODE_HEADER, "promql_invalid_expression")
        }
        tsink::promql::PromqlError::Regex(_) => {
            promql_error_response("bad_data", "invalid or overly complex PromQL regex")
                .with_header(READ_ERROR_CODE_HEADER, "promql_invalid_regex")
        }
        tsink::promql::PromqlError::Eval(_) => {
            promql_error_response("execution", "PromQL evaluation failed")
                .with_header(READ_ERROR_CODE_HEADER, "promql_evaluation_failed")
        }
        tsink::promql::PromqlError::Storage(error) => promql_storage_error_response(error),
    }
}

const EXEMPLAR_CLUSTER_TOPOLOGY_ENVELOPE_BYTES: u64 = 8 * 1024 * 1024;
const EXEMPLAR_CLUSTER_MAX_NODES: usize = 4_096;
const EXEMPLAR_CLUSTER_MAX_MEMBERSHIP_STRING_BYTES: usize = 1024 * 1024;
const EXEMPLAR_METADATA_ENVELOPE_BYTES: u64 = 512;

#[derive(Debug)]
enum ExemplarEnvelopeError {
    Budget(tsink::QueryBudgetError),
    BadInput {
        code: &'static str,
        diagnostic: &'static str,
    },
    Unavailable {
        code: &'static str,
        diagnostic: &'static str,
    },
    Internal {
        code: &'static str,
        diagnostic: &'static str,
    },
}

impl From<tsink::QueryBudgetError> for ExemplarEnvelopeError {
    fn from(error: tsink::QueryBudgetError) -> Self {
        Self::Budget(error)
    }
}

fn exemplar_envelope_error_response(error: ExemplarEnvelopeError) -> HttpResponse {
    match error {
        ExemplarEnvelopeError::Budget(error) => promql_query_budget_error_response(&error),
        ExemplarEnvelopeError::BadInput { code, diagnostic } => {
            promql_error_response("bad_data", diagnostic).with_header(READ_ERROR_CODE_HEADER, code)
        }
        ExemplarEnvelopeError::Unavailable { code, diagnostic } => json_response(
            503,
            &json!({
                "status": "error",
                "errorType": "execution",
                "error": diagnostic,
            }),
        )
        .with_header(READ_ERROR_CODE_HEADER, code),
        ExemplarEnvelopeError::Internal { code, diagnostic } => {
            promql_internal_error_response(code, diagnostic)
        }
    }
}

fn map_exemplar_store_error(error: ExemplarQueryError) -> ExemplarEnvelopeError {
    match error {
        ExemplarQueryError::Budget(error) => ExemplarEnvelopeError::Budget(error),
        ExemplarQueryError::InvalidSelection => ExemplarEnvelopeError::BadInput {
            code: "query_exemplars_invalid_selector",
            diagnostic: "query_exemplars contains an invalid selector",
        },
        ExemplarQueryError::StoreUnavailable => ExemplarEnvelopeError::Unavailable {
            code: "query_exemplars_store_unavailable",
            diagnostic: "exemplar storage is unavailable",
        },
        ExemplarQueryError::Allocation => ExemplarEnvelopeError::Internal {
            code: "query_exemplars_allocation_failed",
            diagnostic: "exemplar query allocation failed",
        },
    }
}

fn modeled_exemplar_parse_bytes(query: &str) -> u64 {
    let token_count = query
        .len()
        .saturating_add(1)
        .min(tsink::promql::MAX_PARSE_TOKENS.saturating_add(1));
    let bytes_per_token = std::mem::size_of::<Expr>()
        .saturating_mul(4)
        .saturating_add(10 * PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES as usize);
    saturating_u64_from_usize(token_count)
        .saturating_mul(saturating_u64_from_usize(bytes_per_token))
        .saturating_add(saturating_u64_from_usize(query.len()).saturating_mul(8))
        .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES)
}

fn parse_exemplar_query_with_execution(
    query: &str,
    execution: &tsink::QueryExecution,
) -> Result<(Expr, tsink::QueryMemoryReservation), ExemplarEnvelopeError> {
    execution.checkpoint()?;
    let reservation = execution.reserve_memory(modeled_exemplar_parse_bytes(query))?;
    execution.checkpoint()?;
    let expr = tsink::promql::parse(query).map_err(|error| match error {
        tsink::promql::PromqlError::Regex(_) => ExemplarEnvelopeError::BadInput {
            code: "query_exemplars_invalid_regex",
            diagnostic: "query_exemplars contains an invalid or overly complex regex",
        },
        _ => ExemplarEnvelopeError::BadInput {
            code: "query_exemplars_invalid_syntax",
            diagnostic: "invalid query_exemplars PromQL syntax",
        },
    })?;
    Ok((expr, reservation))
}

fn walk_exemplar_vectors(
    expr: &Expr,
    execution: &tsink::QueryExecution,
    visit: &mut impl FnMut(&tsink::promql::ast::VectorSelector) -> Result<(), ExemplarEnvelopeError>,
) -> Result<(), ExemplarEnvelopeError> {
    execution.checkpoint()?;
    match expr {
        Expr::VectorSelector(selector) => visit(selector),
        Expr::MatrixSelector(selector) => visit(&selector.vector),
        Expr::Subquery(inner) => walk_exemplar_vectors(&inner.expr, execution, visit),
        Expr::Unary(inner) => walk_exemplar_vectors(&inner.expr, execution, visit),
        Expr::Binary(inner) => {
            walk_exemplar_vectors(&inner.lhs, execution, visit)?;
            walk_exemplar_vectors(&inner.rhs, execution, visit)
        }
        Expr::Aggregation(inner) => {
            walk_exemplar_vectors(&inner.expr, execution, visit)?;
            if let Some(param) = inner.param.as_ref() {
                walk_exemplar_vectors(param, execution, visit)?;
            }
            Ok(())
        }
        Expr::Call(inner) => {
            for argument in &inner.args {
                walk_exemplar_vectors(argument, execution, visit)?;
            }
            Ok(())
        }
        Expr::Paren(inner) => walk_exemplar_vectors(inner, execution, visit),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => Ok(()),
    }
}

fn modeled_exemplar_vector_selection_bytes(
    selector: &tsink::promql::ast::VectorSelector,
    tenant_id: &str,
) -> u64 {
    selector
        .metric_name
        .as_deref()
        .map_or(0, |metric| {
            saturating_u64_from_usize(metric.len())
                .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES)
        })
        .saturating_add(
            saturating_u64_from_usize(
                selector
                    .matchers
                    .len()
                    .saturating_add(1)
                    .saturating_mul(std::mem::size_of::<SeriesMatcher>()),
            )
            .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
        )
        .saturating_add(selector.matchers.iter().fold(0u64, |bytes, matcher| {
            bytes
                .saturating_add(
                    saturating_u64_from_usize(matcher.name.len())
                        .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
                )
                .saturating_add(
                    saturating_u64_from_usize(matcher.value.len())
                        .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
                )
        }))
        .saturating_add(
            saturating_u64_from_usize(tenant::TENANT_LABEL.len())
                .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
        )
        .saturating_add(
            saturating_u64_from_usize(tenant_id.len())
                .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
        )
}

fn clone_exemplar_string(value: &str) -> Result<String, ExemplarEnvelopeError> {
    let mut cloned = String::new();
    cloned
        .try_reserve_exact(value.len())
        .map_err(|_| ExemplarEnvelopeError::Internal {
            code: "query_exemplars_allocation_failed",
            diagnostic: "exemplar query allocation failed",
        })?;
    cloned.push_str(value);
    Ok(cloned)
}

fn scoped_exemplar_selection(
    selector: &tsink::promql::ast::VectorSelector,
    tenant_id: &str,
) -> Result<SeriesSelection, ExemplarEnvelopeError> {
    if selector
        .matchers
        .iter()
        .any(|matcher| matcher.name == tenant::TENANT_LABEL)
    {
        return Err(ExemplarEnvelopeError::BadInput {
            code: "query_exemplars_reserved_tenant_matcher",
            diagnostic: "query_exemplars contains a reserved tenant matcher",
        });
    }
    let mut matchers = Vec::new();
    matchers
        .try_reserve_exact(selector.matchers.len().saturating_add(1))
        .map_err(|_| ExemplarEnvelopeError::Internal {
            code: "query_exemplars_allocation_failed",
            diagnostic: "exemplar query allocation failed",
        })?;
    for matcher in &selector.matchers {
        matchers.push(SeriesMatcher {
            name: clone_exemplar_string(&matcher.name)?,
            op: match matcher.op {
                MatchOp::Equal => SeriesMatcherOp::Equal,
                MatchOp::NotEqual => SeriesMatcherOp::NotEqual,
                MatchOp::RegexMatch => SeriesMatcherOp::RegexMatch,
                MatchOp::RegexNoMatch => SeriesMatcherOp::RegexNoMatch,
            },
            value: clone_exemplar_string(&matcher.value)?,
        });
    }
    matchers.push(SeriesMatcher::equal(
        clone_exemplar_string(tenant::TENANT_LABEL)?,
        clone_exemplar_string(tenant_id)?,
    ));
    let selection = SeriesSelection {
        metric: selector
            .metric_name
            .as_deref()
            .map(clone_exemplar_string)
            .transpose()?,
        matchers,
        start: None,
        end: None,
    };
    selection
        .validate_shape()
        .map_err(|_| ExemplarEnvelopeError::BadInput {
            code: "query_exemplars_invalid_selector",
            diagnostic: "query_exemplars contains an invalid selector",
        })?;
    Ok(selection)
}

fn modeled_owned_exemplar_selection_bytes(selection: &SeriesSelection) -> u64 {
    selection
        .metric
        .as_ref()
        .map_or(0, |metric| {
            saturating_u64_from_usize(metric.capacity())
                .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES)
        })
        .saturating_add(
            saturating_u64_from_usize(
                selection
                    .matchers
                    .capacity()
                    .saturating_mul(std::mem::size_of::<SeriesMatcher>()),
            )
            .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
        )
        .saturating_add(selection.matchers.iter().fold(0u64, |bytes, matcher| {
            bytes
                .saturating_add(saturating_u64_from_usize(matcher.name.capacity()))
                .saturating_add(saturating_u64_from_usize(matcher.value.capacity()))
                .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(2))
        }))
}

fn clone_exemplar_selection(
    selection: &SeriesSelection,
) -> Result<SeriesSelection, ExemplarEnvelopeError> {
    let mut matchers = Vec::new();
    matchers
        .try_reserve_exact(selection.matchers.len())
        .map_err(|_| ExemplarEnvelopeError::Internal {
            code: "query_exemplars_allocation_failed",
            diagnostic: "exemplar query allocation failed",
        })?;
    for matcher in &selection.matchers {
        matchers.push(SeriesMatcher {
            name: clone_exemplar_string(&matcher.name)?,
            op: matcher.op,
            value: clone_exemplar_string(&matcher.value)?,
        });
    }
    Ok(SeriesSelection {
        metric: selection
            .metric
            .as_deref()
            .map(clone_exemplar_string)
            .transpose()?,
        matchers,
        start: selection.start,
        end: selection.end,
    })
}

fn prepare_scoped_exemplar_selections(
    expr: &Expr,
    tenant_id: &str,
    maximum: usize,
    execution: &tsink::QueryExecution,
) -> Result<(Vec<SeriesSelection>, tsink::QueryMemoryReservation), ExemplarEnvelopeError> {
    let mut count = 0usize;
    let mut modeled_bytes = PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES;
    walk_exemplar_vectors(expr, execution, &mut |selector| {
        count = count.saturating_add(1);
        if count > maximum {
            return Err(ExemplarEnvelopeError::BadInput {
                code: "query_exemplars_selector_limit_exceeded",
                diagnostic: "query_exemplars selector limit exceeded",
            });
        }
        modeled_bytes = modeled_bytes
            .saturating_add(modeled_exemplar_vector_selection_bytes(selector, tenant_id));
        Ok(())
    })?;
    if count == 0 {
        return Err(ExemplarEnvelopeError::BadInput {
            code: "query_exemplars_selector_required",
            diagnostic: "query_exemplars requires at least one vector or matrix selector",
        });
    }
    modeled_bytes = modeled_bytes.saturating_add(
        saturating_u64_from_usize(count.saturating_mul(std::mem::size_of::<SeriesSelection>()))
            .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
    );
    let mut reservation = execution.reserve_memory(modeled_bytes)?;
    let mut selections = Vec::new();
    selections
        .try_reserve_exact(count)
        .map_err(|_| ExemplarEnvelopeError::Internal {
            code: "query_exemplars_allocation_failed",
            diagnostic: "exemplar query allocation failed",
        })?;
    walk_exemplar_vectors(expr, execution, &mut |selector| {
        selections.push(scoped_exemplar_selection(selector, tenant_id)?);
        Ok(())
    })?;
    let retained = saturating_u64_from_usize(
        selections
            .capacity()
            .saturating_mul(std::mem::size_of::<SeriesSelection>()),
    )
    .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES)
    .saturating_add(selections.iter().fold(0u64, |bytes, selection| {
        bytes.saturating_add(modeled_owned_exemplar_selection_bytes(selection))
    }))
    // `Arc::new(selections)` below adds one fixed control-block allocation.
    .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(2));
    reservation.resize(retained)?;
    Ok((selections, reservation))
}

fn compare_exemplar_labels(left: &[Label], right: &[Label]) -> std::cmp::Ordering {
    for (left, right) in left.iter().zip(right) {
        let ordering = left
            .name
            .cmp(&right.name)
            .then_with(|| left.value.cmp(&right.value));
        if !ordering.is_eq() {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

fn exemplar_series_identity_cmp(
    left: &ExemplarSeries,
    right: &ExemplarSeries,
) -> std::cmp::Ordering {
    left.metric
        .cmp(&right.metric)
        .then_with(|| compare_exemplar_labels(&left.labels, &right.labels))
}

fn modeled_sort_scratch_bytes<T>(len: usize) -> u64 {
    saturating_u64_from_usize(len.saturating_add(1).saturating_div(2))
        .saturating_mul(saturating_u64_from_usize(std::mem::size_of::<T>()))
        .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES)
}

fn dedupe_and_limit_exemplar_series_accounted(
    mut series: Vec<ExemplarSeries>,
    limit: usize,
    execution: &tsink::QueryExecution,
) -> Result<(Vec<ExemplarSeries>, tsink::QueryMemoryReservation), ExemplarEnvelopeError> {
    execution.checkpoint()?;
    for item in &mut series {
        item.labels.sort_unstable_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.value.cmp(&right.value))
        });
    }
    let series_sort_reservation =
        execution.reserve_memory(modeled_sort_scratch_bytes::<ExemplarSeries>(series.len()))?;
    series.sort_by(exemplar_series_identity_cmp);
    drop(series_sort_reservation);

    let mut merge_reservation = execution.reserve_memory(0)?;
    let mut write_index = 0usize;
    for read_index in 0..series.len() {
        execution.checkpoint()?;
        if write_index > 0
            && exemplar_series_identity_cmp(&series[write_index - 1], &series[read_index]).is_eq()
        {
            let (left, right) = series.split_at_mut(read_index);
            let target = &mut left[write_index - 1].exemplars;
            let source = &mut right[0].exemplars;
            let requested_capacity = target.len().saturating_add(source.len());
            let upper = saturating_u64_from_usize(
                requested_capacity
                    .saturating_mul(std::mem::size_of::<crate::exemplar_store::ExemplarSample>()),
            )
            .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES);
            // Retain a conservative cumulative charge for every destination reallocation. Source
            // result guards still cover their moved allocations; over-retaining earlier target
            // capacities avoids any gap while `try_reserve_exact` replaces them.
            merge_reservation.resize(merge_reservation.bytes().saturating_add(upper))?;
            target.try_reserve_exact(source.len()).map_err(|_| {
                ExemplarEnvelopeError::Internal {
                    code: "query_exemplars_allocation_failed",
                    diagnostic: "exemplar query allocation failed",
                }
            })?;
            target.append(source);
        } else {
            series.swap(write_index, read_index);
            write_index = write_index.saturating_add(1);
        }
    }
    series.truncate(write_index);

    let largest_exemplar_vector = series
        .iter()
        .map(|item| item.exemplars.len())
        .max()
        .unwrap_or(0);
    let exemplar_sort_reservation = execution.reserve_memory(modeled_sort_scratch_bytes::<
        crate::exemplar_store::ExemplarSample,
    >(largest_exemplar_vector))?;
    let mut remaining = limit;
    let mut retained_series = 0usize;
    for item in &mut series {
        execution.checkpoint()?;
        item.exemplars.sort_by_key(|exemplar| exemplar.timestamp);
        item.exemplars.dedup_by_key(|exemplar| exemplar.timestamp);
        if remaining == 0 {
            item.exemplars.clear();
            continue;
        }
        item.exemplars.truncate(remaining);
        remaining = remaining.saturating_sub(item.exemplars.len());
    }
    drop(exemplar_sort_reservation);
    for read_index in 0..series.len() {
        if !series[read_index].exemplars.is_empty() {
            series.swap(retained_series, read_index);
            retained_series = retained_series.saturating_add(1);
        }
    }
    series.truncate(retained_series);
    execution.checkpoint()?;
    Ok((series, merge_reservation))
}

#[cfg(test)]
fn dedupe_and_limit_exemplar_series(
    series: Vec<ExemplarSeries>,
    limit: usize,
) -> Vec<ExemplarSeries> {
    let budget =
        tsink::QueryBudget::new(tsink::QueryBudgetLimits::default()).expect("test query budget");
    let execution = budget.begin_query().expect("test query execution");
    dedupe_and_limit_exemplar_series_accounted(series, limit, &execution)
        .expect("test exemplar dedupe")
        .0
}

fn exemplar_series_is_visible(series: &ExemplarSeries, tenant_id: &str) -> bool {
    let mut matched = false;
    for label in &series.labels {
        if label.name == tenant::TENANT_LABEL {
            if label.value != tenant_id {
                return false;
            }
            matched = true;
        }
    }
    matched || tenant_id == tenant::DEFAULT_TENANT_ID
}

struct ExemplarLabelsJson<'a>(&'a [Label]);

impl Serialize for ExemplarLabelsJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let entry_count = self
            .0
            .iter()
            .enumerate()
            .filter(|(index, label)| {
                !self.0[..*index]
                    .iter()
                    .any(|previous| previous.name == label.name)
            })
            .count();
        let mut map = serializer.serialize_map(Some(entry_count))?;
        let mut previous_name: Option<&str> = None;
        loop {
            let mut next_name: Option<&str> = None;
            for label in self.0 {
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
            let value = self
                .0
                .iter()
                .rev()
                .find(|label| label.name == name)
                .map(|label| label.value.as_str())
                .expect("selected exemplar label has a value");
            map.serialize_entry(name, value)?;
            previous_name = Some(name);
        }
        map.end()
    }
}

struct ExemplarSeriesLabelsJson<'a> {
    series: &'a ExemplarSeries,
}

impl Serialize for ExemplarSeriesLabelsJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let labels = &self.series.labels;
        let entry_count = 1usize.saturating_add(
            labels
                .iter()
                .enumerate()
                .filter(|(index, label)| {
                    label.name != tenant::TENANT_LABEL
                        && label.name != "__name__"
                        && !labels[..*index]
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
            for label in labels {
                let name = label.name.as_str();
                if name == tenant::TENANT_LABEL
                    || previous_name.is_some_and(|previous| name <= previous)
                {
                    continue;
                }
                if next_name.is_none_or(|next| name < next) {
                    next_name = Some(name);
                }
            }
            let Some(name) = next_name else {
                break;
            };
            let mut value = (name == "__name__").then_some(self.series.metric.as_str());
            for label in labels {
                if label.name == name {
                    value = Some(label.value.as_str());
                }
            }
            map.serialize_entry(name, value.expect("selected series label has a value"))?;
            previous_name = Some(name);
        }
        map.end()
    }
}

struct ExemplarSampleJson<'a> {
    exemplar: &'a crate::exemplar_store::ExemplarSample,
    precision: TimestampPrecision,
}

impl Serialize for ExemplarSampleJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut object = serializer.serialize_struct("ExemplarSample", 3)?;
        object.serialize_field("labels", &ExemplarLabelsJson(&self.exemplar.labels))?;
        object.serialize_field(
            "timestamp",
            &timestamp_to_f64(self.exemplar.timestamp, self.precision),
        )?;
        object.serialize_field("value", &PromqlFormattedNumber(self.exemplar.value))?;
        object.end()
    }
}

struct ExemplarSamplesJson<'a> {
    exemplars: &'a [crate::exemplar_store::ExemplarSample],
    precision: TimestampPrecision,
}

impl Serialize for ExemplarSamplesJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut values = serializer.serialize_seq(Some(self.exemplars.len()))?;
        for exemplar in self.exemplars {
            values.serialize_element(&ExemplarSampleJson {
                exemplar,
                precision: self.precision,
            })?;
        }
        values.end()
    }
}

struct ExemplarSeriesJson<'a> {
    series: &'a ExemplarSeries,
    precision: TimestampPrecision,
}

impl Serialize for ExemplarSeriesJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut object = serializer.serialize_struct("ExemplarSeries", 2)?;
        object.serialize_field(
            "exemplars",
            &ExemplarSamplesJson {
                exemplars: &self.series.exemplars,
                precision: self.precision,
            },
        )?;
        object.serialize_field(
            "seriesLabels",
            &ExemplarSeriesLabelsJson {
                series: self.series,
            },
        )?;
        object.end()
    }
}

struct ExemplarSeriesListJson<'a> {
    series: &'a [ExemplarSeries],
    precision: TimestampPrecision,
}

impl Serialize for ExemplarSeriesListJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut values = serializer.serialize_seq(Some(self.series.len()))?;
        for series in self.series {
            values.serialize_element(&ExemplarSeriesJson {
                series,
                precision: self.precision,
            })?;
        }
        values.end()
    }
}

#[derive(Serialize)]
struct ExemplarPartialResponseJson {
    consistency: &'static str,
    enabled: bool,
    policy: &'static str,
    #[serde(rename = "warningCount")]
    warning_count: usize,
}

#[derive(Serialize)]
struct ExemplarSuccessJson<'a> {
    data: ExemplarSeriesListJson<'a>,
    #[serde(rename = "partialResponse", skip_serializing_if = "Option::is_none")]
    partial_response: Option<ExemplarPartialResponseJson>,
    status: &'static str,
}

fn exemplar_consistency_name(
    consistency: crate::cluster::config::ClusterReadConsistency,
) -> &'static str {
    match consistency {
        crate::cluster::config::ClusterReadConsistency::Eventual => "eventual",
        crate::cluster::config::ClusterReadConsistency::Quorum => "quorum",
        crate::cluster::config::ClusterReadConsistency::Strict => "strict",
    }
}

fn exemplar_partial_response_policy_name(
    policy: crate::cluster::config::ClusterReadPartialResponsePolicy,
) -> &'static str {
    match policy {
        crate::cluster::config::ClusterReadPartialResponsePolicy::Allow => "allow",
        crate::cluster::config::ClusterReadPartialResponsePolicy::Deny => "deny",
    }
}

fn modeled_exemplar_json_upper_bytes(
    series: &[ExemplarSeries],
    metadata: Option<&ReadFanoutResponseMetadata>,
) -> u64 {
    series.iter().fold(
        256u64.saturating_add(if metadata.is_some() { 512 } else { 0 }),
        |bytes, item| {
            let series_labels = item.labels.iter().fold(
                modeled_promql_json_string_upper_bytes("__name__")
                    .saturating_add(modeled_promql_json_string_upper_bytes(&item.metric)),
                |label_bytes, label| {
                    label_bytes
                        .saturating_add(modeled_promql_json_string_upper_bytes(&label.name))
                        .saturating_add(modeled_promql_json_string_upper_bytes(&label.value))
                        .saturating_add(2)
                },
            );
            let exemplars = item
                .exemplars
                .iter()
                .fold(0u64, |exemplar_bytes, exemplar| {
                    exemplar_bytes
                        .saturating_add(PROMQL_JSON_NUMBER_BYTES_UPPER.saturating_mul(2))
                        .saturating_add(exemplar.labels.iter().fold(64u64, |label_bytes, label| {
                            label_bytes
                                .saturating_add(modeled_promql_json_string_upper_bytes(&label.name))
                                .saturating_add(modeled_promql_json_string_upper_bytes(
                                    &label.value,
                                ))
                                .saturating_add(2)
                        }))
                        .saturating_add(64)
                });
            bytes
                .saturating_add(series_labels)
                .saturating_add(exemplars)
                .saturating_add(128)
        },
    )
}

fn encode_exemplar_success_response(
    series: &[ExemplarSeries],
    precision: TimestampPrecision,
    metadata: Option<&ReadFanoutResponseMetadata>,
    execution: &tsink::QueryExecution,
) -> Result<EncodedPromqlHttpResponse, PromqlResponseEncodingError> {
    execution
        .checkpoint()
        .map_err(PromqlResponseEncodingError::Budget)?;
    let body_upper = modeled_exemplar_json_upper_bytes(series, metadata);
    let body_capacity_u64 = body_upper.min(saturating_u64_from_usize(MAX_BODY_BYTES));
    let body_capacity =
        usize::try_from(body_capacity_u64).map_err(|_| PromqlResponseEncodingError::TooLarge {
            requested: body_upper,
        })?;
    let reserved_bytes = body_capacity_u64
        .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES)
        .saturating_add(PROMQL_HTTP_HEADER_ENVELOPE_BYTES);
    let mut reservation = execution
        .reserve_memory(reserved_bytes)
        .map_err(PromqlResponseEncodingError::Budget)?;
    let mut body = Vec::new();
    body.try_reserve_exact(body_capacity)
        .map_err(|_| PromqlResponseEncodingError::Allocation)?;
    let mut writer = FixedCapacityJsonBuffer {
        bytes: body,
        limit: body_capacity,
        limit_exceeded_at: None,
    };
    if serde_json::to_writer(
        &mut writer,
        &ExemplarSuccessJson {
            data: ExemplarSeriesListJson { series, precision },
            partial_response: metadata.map(|metadata| ExemplarPartialResponseJson {
                consistency: exemplar_consistency_name(metadata.consistency),
                enabled: metadata.partial_response,
                policy: exemplar_partial_response_policy_name(metadata.partial_response_policy),
                warning_count: metadata.warnings.len(),
            }),
            status: "success",
        },
    )
    .is_err()
    {
        return Err(match writer.limit_exceeded_at {
            Some(requested) if writer.limit == MAX_BODY_BYTES => {
                PromqlResponseEncodingError::TooLarge {
                    requested: saturating_u64_from_usize(requested),
                }
            }
            _ => PromqlResponseEncodingError::Serialization,
        });
    }
    execution
        .charge_returned_bytes(saturating_u64_from_usize(writer.bytes.len()))
        .map_err(PromqlResponseEncodingError::Budget)?;
    reservation
        .resize(
            saturating_u64_from_usize(writer.bytes.capacity())
                .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES)
                .saturating_add(PROMQL_HTTP_HEADER_ENVELOPE_BYTES),
        )
        .map_err(PromqlResponseEncodingError::Budget)?;
    execution
        .checkpoint()
        .map_err(PromqlResponseEncodingError::Budget)?;
    Ok(EncodedPromqlHttpResponse {
        response: HttpResponse::new(200, writer.bytes)
            .with_header("Content-Type", "application/json"),
        reservation,
    })
}

struct GuardedExemplarSeries {
    series: Vec<ExemplarSeries>,
    _reservations: Vec<tsink::QueryMemoryReservation>,
    _reservation_slots: tsink::QueryMemoryReservation,
}

async fn query_local_exemplars_accounted(
    exemplar_store: &Arc<ExemplarStore>,
    selectors: Arc<Vec<SeriesSelection>>,
    start: i64,
    end: i64,
    limit: usize,
    execution: &tsink::QueryExecution,
) -> Result<AccountedExemplarQueryResult, ExemplarEnvelopeError> {
    let store = Arc::clone(exemplar_store);
    let worker_execution = execution.clone();
    let result = tokio::task::spawn_blocking(move || {
        store.query_with_execution_result(
            selectors.as_slice(),
            start,
            end,
            limit,
            &worker_execution,
        )
    })
    .await
    .map_err(|_| ExemplarEnvelopeError::Internal {
        code: "query_exemplars_task_failed",
        diagnostic: "exemplar query task failed",
    })?
    .map_err(map_exemplar_store_error)?;
    if !result.series().is_empty() && result.reserved_memory_bytes() == 0 {
        return Err(ExemplarEnvelopeError::Internal {
            code: "query_exemplars_result_accounting_missing",
            diagnostic: "accounted exemplar query omitted its result reservation",
        });
    }
    Ok(result)
}

fn modeled_membership_bytes(membership: &MembershipView) -> (usize, u64) {
    let string_bytes =
        membership
            .nodes
            .iter()
            .fold(membership.local_node_id.len(), |bytes, node| {
                bytes
                    .saturating_add(node.id.len())
                    .saturating_add(node.endpoint.len())
            });
    let modeled = saturating_u64_from_usize(
        membership
            .nodes
            .capacity()
            .saturating_mul(std::mem::size_of::<ClusterNode>()),
    )
    .saturating_add(saturating_u64_from_usize(string_bytes))
    .saturating_add(
        saturating_u64_from_usize(membership.nodes.len().saturating_add(1))
            .saturating_mul(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
    );
    (string_bytes, modeled)
}

fn remaining_exemplar_limit(
    limit: Option<u64>,
    current: u64,
    reason: tsink::QueryLimitReason,
) -> Result<Option<u64>, ExemplarEnvelopeError> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let remaining = limit.saturating_sub(current);
    if remaining == 0 {
        return Err(ExemplarEnvelopeError::Budget(
            tsink::QueryLimitExceeded::new(reason, limit, current, 1).into(),
        ));
    }
    Ok(Some(remaining))
}

fn remaining_exemplar_query_limits(
    execution: &tsink::QueryExecution,
) -> Result<tsink::QueryWorkLimits, ExemplarEnvelopeError> {
    execution.checkpoint()?;
    let snapshot = execution.snapshot();
    let mut limits = execution.limits();
    limits.max_series_matched = remaining_exemplar_limit(
        limits.max_series_matched,
        snapshot.series_matched,
        tsink::QueryLimitReason::SeriesMatched,
    )?;
    limits.max_samples_scanned = remaining_exemplar_limit(
        limits.max_samples_scanned,
        snapshot.samples_scanned,
        tsink::QueryLimitReason::SamplesScanned,
    )?;
    limits.max_samples_returned = remaining_exemplar_limit(
        limits.max_samples_returned,
        snapshot.samples_returned,
        tsink::QueryLimitReason::SamplesReturned,
    )?;
    limits.max_returned_bytes = remaining_exemplar_limit(
        limits.max_returned_bytes,
        snapshot.returned_bytes,
        tsink::QueryLimitReason::ReturnedBytes,
    )?;
    limits.max_pattern_expansion = remaining_exemplar_limit(
        limits.max_pattern_expansion,
        snapshot.pattern_expansion,
        tsink::QueryLimitReason::PatternExpansion,
    )?;
    limits.max_steps = remaining_exemplar_limit(
        limits.max_steps,
        snapshot.steps,
        tsink::QueryLimitReason::Steps,
    )?;
    limits.max_intermediate_vector_size = remaining_exemplar_limit(
        limits.max_intermediate_vector_size,
        snapshot.intermediate_vector_size,
        tsink::QueryLimitReason::IntermediateVectorSize,
    )?;
    limits.max_memory_bytes = remaining_exemplar_limit(
        limits.max_memory_bytes,
        snapshot.memory_reserved_bytes,
        tsink::QueryLimitReason::PerQueryMemoryBytes,
    )?;
    if let Some(deadline) = execution.cancellation_token().deadline() {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(ExemplarEnvelopeError::Budget(
                tsink::QueryBudgetError::DeadlineExceeded,
            ));
        };
        if remaining.is_zero() {
            return Err(ExemplarEnvelopeError::Budget(
                tsink::QueryBudgetError::DeadlineExceeded,
            ));
        }
        limits.max_wall_time = Some(remaining);
    }
    Ok(limits)
}

fn modeled_internal_exemplar_logical_bytes(series: &[InternalExemplarSeries]) -> u64 {
    series.iter().fold(0u64, |bytes, item| {
        bytes
            .saturating_add(saturating_u64_from_usize(item.metric.len()))
            .saturating_add(item.labels.iter().fold(0u64, |label_bytes, label| {
                label_bytes
                    .saturating_add(saturating_u64_from_usize(label.name.len()))
                    .saturating_add(saturating_u64_from_usize(label.value.len()))
            }))
            .saturating_add(
                item.exemplars
                    .iter()
                    .fold(0u64, |exemplar_bytes, exemplar| {
                        exemplar_bytes.saturating_add(16).saturating_add(
                            exemplar.labels.iter().fold(0u64, |label_bytes, label| {
                                label_bytes
                                    .saturating_add(saturating_u64_from_usize(label.name.len()))
                                    .saturating_add(saturating_u64_from_usize(label.value.len()))
                            }),
                        )
                    }),
            )
    })
}

fn exemplar_labels_have_valid_shape(labels: &[Label], allow_tenant_label: bool) -> bool {
    if labels.len() > tsink::DEFAULT_MAX_LABELS_PER_SERIES {
        return false;
    }
    let mut bytes = 0usize;
    for (index, label) in labels.iter().enumerate() {
        if label.name.is_empty()
            || label.name.len() > tsink::label::MAX_LABEL_NAME_LEN
            || label.value.len() > tsink::label::MAX_LABEL_VALUE_LEN
            || label.name == "__name__"
            || (!allow_tenant_label && label.name == tenant::TENANT_LABEL)
            || labels[..index]
                .iter()
                .any(|previous| previous.name == label.name)
        {
            return false;
        }
        bytes = bytes
            .saturating_add(label.name.len())
            .saturating_add(label.value.len());
    }
    bytes <= tsink::DEFAULT_MAX_SERIES_IDENTITY_BYTES
}

fn validate_internal_exemplar_response_shape(
    series: &[InternalExemplarSeries],
    tenant_id: &str,
    start: i64,
    end: i64,
    limit: usize,
) -> Result<(), ExemplarEnvelopeError> {
    if series.len() > limit {
        return Err(ExemplarEnvelopeError::Internal {
            code: "query_exemplars_remote_shape_invalid",
            diagnostic: "remote exemplar query returned an invalid response shape",
        });
    }
    let mut total_exemplars = 0usize;
    for item in series {
        if item.metric.is_empty()
            || item.metric.len() > tsink::label::MAX_METRIC_NAME_LEN
            || item.exemplars.is_empty()
            || !exemplar_labels_have_valid_shape(&item.labels, true)
        {
            return Err(ExemplarEnvelopeError::Internal {
                code: "query_exemplars_remote_shape_invalid",
                diagnostic: "remote exemplar query returned an invalid response shape",
            });
        }
        let mut tenant_label_count = 0usize;
        let mut tenant_value_matches = false;
        for label in &item.labels {
            if label.name == tenant::TENANT_LABEL {
                tenant_label_count = tenant_label_count.saturating_add(1);
                tenant_value_matches = label.value == tenant_id;
            }
        }
        if tenant_label_count != 1 || !tenant_value_matches {
            return Err(ExemplarEnvelopeError::Internal {
                code: "query_exemplars_remote_tenant_scope_invalid",
                diagnostic: "remote exemplar query returned data outside the tenant scope",
            });
        }
        let identity_bytes = item.labels.iter().fold(item.metric.len(), |bytes, label| {
            bytes
                .saturating_add(label.name.len())
                .saturating_add(label.value.len())
        });
        if identity_bytes > tsink::DEFAULT_MAX_SERIES_IDENTITY_BYTES {
            return Err(ExemplarEnvelopeError::Internal {
                code: "query_exemplars_remote_shape_invalid",
                diagnostic: "remote exemplar query returned an invalid response shape",
            });
        }
        total_exemplars = total_exemplars.saturating_add(item.exemplars.len());
        if total_exemplars > limit
            || item.exemplars.iter().any(|exemplar| {
                exemplar.timestamp < start
                    || exemplar.timestamp > end
                    || !exemplar_labels_have_valid_shape(&exemplar.labels, false)
            })
        {
            return Err(ExemplarEnvelopeError::Internal {
                code: "query_exemplars_remote_shape_invalid",
                diagnostic: "remote exemplar query returned an invalid response shape",
            });
        }
    }
    Ok(())
}

fn validate_final_exemplar_series_shape(
    series: &[ExemplarSeries],
    tenant_id: &str,
    start: i64,
    end: i64,
    limit: usize,
) -> bool {
    if series.len() > limit {
        return false;
    }
    let mut total_exemplars = 0usize;
    for item in series {
        if item.metric.is_empty()
            || item.metric.len() > tsink::label::MAX_METRIC_NAME_LEN
            || item.exemplars.is_empty()
            || !exemplar_labels_have_valid_shape(&item.labels, true)
            || !exemplar_series_is_visible(item, tenant_id)
        {
            return false;
        }
        let tenant_count = item
            .labels
            .iter()
            .filter(|label| label.name == tenant::TENANT_LABEL)
            .count();
        if tenant_count != 1 {
            return false;
        }
        let identity_bytes = item.labels.iter().fold(item.metric.len(), |bytes, label| {
            bytes
                .saturating_add(label.name.len())
                .saturating_add(label.value.len())
        });
        total_exemplars = total_exemplars.saturating_add(item.exemplars.len());
        if identity_bytes > tsink::DEFAULT_MAX_SERIES_IDENTITY_BYTES
            || total_exemplars > limit
            || item.exemplars.iter().any(|exemplar| {
                exemplar.timestamp < start
                    || exemplar.timestamp > end
                    || !exemplar_labels_have_valid_shape(&exemplar.labels, false)
            })
        {
            return false;
        }
    }
    true
}

fn validate_and_charge_remote_exemplar_accounting(
    execution: &tsink::QueryExecution,
    series: &[InternalExemplarSeries],
    snapshot: tsink::QueryExecutionSnapshot,
) -> Result<(), ExemplarEnvelopeError> {
    let samples = series.iter().fold(0u64, |count, item| {
        count.saturating_add(saturating_u64_from_usize(item.exemplars.len()))
    });
    if snapshot.series_matched < saturating_u64_from_usize(series.len())
        || snapshot.samples_returned < samples
        || snapshot.samples_scanned < snapshot.samples_returned
        || snapshot.returned_bytes < modeled_internal_exemplar_logical_bytes(series)
        || (!series.is_empty() && snapshot.memory_reserved_bytes == 0)
        || snapshot.intermediate_vector_size < saturating_u64_from_usize(series.len())
    {
        return Err(ExemplarEnvelopeError::Internal {
            code: "query_exemplars_remote_accounting_invalid",
            diagnostic: "remote exemplar query returned invalid execution accounting",
        });
    }
    execution.checkpoint()?;
    execution.charge_series_matched(snapshot.series_matched)?;
    execution.charge_samples_scanned(snapshot.samples_scanned)?;
    execution.charge_samples_returned(snapshot.samples_returned)?;
    execution.charge_returned_bytes(snapshot.returned_bytes)?;
    execution.charge_pattern_expansion(snapshot.pattern_expansion)?;
    execution.charge_steps(snapshot.steps)?;
    execution.observe_intermediate_vector_size(snapshot.intermediate_vector_size)?;
    Ok(())
}

fn convert_internal_exemplar_series(
    series: Vec<InternalExemplarSeries>,
    execution: &tsink::QueryExecution,
) -> Result<(Vec<ExemplarSeries>, tsink::QueryMemoryReservation), ExemplarEnvelopeError> {
    let upper = saturating_u64_from_usize(
        series
            .len()
            .saturating_mul(std::mem::size_of::<ExemplarSeries>()),
    )
    .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES)
    .saturating_add(series.iter().fold(0u64, |bytes, item| {
        bytes.saturating_add(
            saturating_u64_from_usize(
                item.exemplars
                    .len()
                    .saturating_mul(std::mem::size_of::<crate::exemplar_store::ExemplarSample>()),
            )
            .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
        )
    }));
    let mut reservation = execution.reserve_memory(upper)?;
    let mut converted = Vec::new();
    converted
        .try_reserve_exact(series.len())
        .map_err(|_| ExemplarEnvelopeError::Internal {
            code: "query_exemplars_allocation_failed",
            diagnostic: "exemplar query allocation failed",
        })?;
    for item in series {
        execution.checkpoint()?;
        let mut exemplars = Vec::new();
        exemplars
            .try_reserve_exact(item.exemplars.len())
            .map_err(|_| ExemplarEnvelopeError::Internal {
                code: "query_exemplars_allocation_failed",
                diagnostic: "exemplar query allocation failed",
            })?;
        for exemplar in item.exemplars {
            exemplars.push(crate::exemplar_store::ExemplarSample {
                labels: exemplar.labels,
                value: exemplar.value,
                timestamp: exemplar.timestamp,
            });
        }
        converted.push(ExemplarSeries {
            metric: item.metric,
            labels: item.labels,
            exemplars,
        });
    }
    let retained = saturating_u64_from_usize(
        converted
            .capacity()
            .saturating_mul(std::mem::size_of::<ExemplarSeries>()),
    )
    .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES)
    .saturating_add(converted.iter().fold(0u64, |bytes, item| {
        bytes.saturating_add(
            saturating_u64_from_usize(
                item.exemplars
                    .capacity()
                    .saturating_mul(std::mem::size_of::<crate::exemplar_store::ExemplarSample>()),
            )
            .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
        )
    }));
    reservation.resize(retained)?;
    Ok((converted, reservation))
}

async fn query_exemplars_across_cluster_accounted(
    exemplar_store: &Arc<ExemplarStore>,
    cluster_context: &ClusterRequestContext,
    selectors: Arc<Vec<SeriesSelection>>,
    start: i64,
    end: i64,
    limit: usize,
    execution: &tsink::QueryExecution,
) -> Result<GuardedExemplarSeries, ExemplarEnvelopeError> {
    let mut topology_reservation =
        execution.reserve_memory(EXEMPLAR_CLUSTER_TOPOLOGY_ENVELOPE_BYTES)?;
    let (membership, ring) = effective_write_topology(cluster_context).map_err(|_| {
        ExemplarEnvelopeError::Unavailable {
            code: "query_exemplars_topology_unavailable",
            diagnostic: "cluster exemplar topology is unavailable",
        }
    })?;
    drop(ring);
    let (membership_string_bytes, membership_bytes) = modeled_membership_bytes(&membership);
    if membership.nodes.len() > EXEMPLAR_CLUSTER_MAX_NODES
        || membership_string_bytes > EXEMPLAR_CLUSTER_MAX_MEMBERSHIP_STRING_BYTES
        || membership_bytes > EXEMPLAR_CLUSTER_TOPOLOGY_ENVELOPE_BYTES
    {
        return Err(ExemplarEnvelopeError::Unavailable {
            code: "query_exemplars_topology_too_large",
            diagnostic: "cluster exemplar topology exceeds its hard envelope",
        });
    }
    topology_reservation.resize(membership_bytes)?;

    let guard_capacity = membership.nodes.len().saturating_mul(2).saturating_add(5);
    let reservation_slots = execution.reserve_memory(
        saturating_u64_from_usize(
            guard_capacity.saturating_mul(std::mem::size_of::<tsink::QueryMemoryReservation>()),
        )
        .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
    )?;
    let mut reservations = Vec::new();
    reservations
        .try_reserve_exact(guard_capacity)
        .map_err(|_| ExemplarEnvelopeError::Internal {
            code: "query_exemplars_allocation_failed",
            diagnostic: "exemplar query allocation failed",
        })?;
    reservations.push(topology_reservation);

    let local = query_local_exemplars_accounted(
        exemplar_store,
        Arc::clone(&selectors),
        start,
        end,
        limit,
        execution,
    )
    .await?;
    let (mut merged, local_reservation) = local.into_parts();
    reservations.push(local_reservation);
    let mut merge_reservation = execution.reserve_memory(0)?;

    let request_clone_upper = selectors.iter().fold(
        saturating_u64_from_usize(
            selectors
                .len()
                .saturating_mul(std::mem::size_of::<SeriesSelection>()),
        )
        .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
        |bytes, selection| bytes.saturating_add(modeled_owned_exemplar_selection_bytes(selection)),
    );
    let _request_clone_reservation = execution.reserve_memory(request_clone_upper)?;
    let mut request_selectors = Vec::new();
    request_selectors
        .try_reserve_exact(selectors.len())
        .map_err(|_| ExemplarEnvelopeError::Internal {
            code: "query_exemplars_allocation_failed",
            diagnostic: "exemplar query allocation failed",
        })?;
    for selection in selectors.iter() {
        request_selectors.push(clone_exemplar_selection(selection)?);
    }
    let mut rpc_request = InternalQueryExemplarsRequest {
        ring_version: cluster_ring_version(Some(cluster_context)),
        selectors: request_selectors,
        start,
        end,
        limit,
        query_limits: None,
    };
    let local_node_id = membership.local_node_id;
    for node in membership.nodes {
        if node.id == local_node_id {
            continue;
        }
        execution.checkpoint()?;
        rpc_request.query_limits = Some(remaining_exemplar_query_limits(execution)?);
        let accounted = cluster_context
            .rpc_client
            .query_exemplars_accounted(node.endpoint.as_str(), &rpc_request, execution)
            .await
            .map_err(|error| match error {
                RpcError::QueryBudget { error } => ExemplarEnvelopeError::Budget(error),
                _ => ExemplarEnvelopeError::Unavailable {
                    code: "query_exemplars_remote_failed",
                    diagnostic: "remote exemplar query failed",
                },
            })?;
        let accounting = accounted
            .response
            .accounting
            .ok_or(ExemplarEnvelopeError::Internal {
                code: "query_exemplars_remote_accounting_missing",
                diagnostic: "remote exemplar query omitted execution accounting",
            })?;
        validate_internal_exemplar_response_shape(
            &accounted.response.series,
            selectors
                .first()
                .and_then(|selection| {
                    selection
                        .matchers
                        .iter()
                        .find(|matcher| matcher.name == tenant::TENANT_LABEL)
                })
                .map(|matcher| matcher.value.as_str())
                .ok_or(ExemplarEnvelopeError::Internal {
                    code: "query_exemplars_tenant_scope_missing",
                    diagnostic: "scoped exemplar query omitted its tenant matcher",
                })?,
            start,
            end,
            limit,
        )?;
        validate_and_charge_remote_exemplar_accounting(
            execution,
            &accounted.response.series,
            accounting,
        )?;
        let (mut remote, conversion_reservation) =
            convert_internal_exemplar_series(accounted.response.series, execution)?;
        let combined_len = merged.len().saturating_add(remote.len());
        execution.observe_intermediate_vector_size(saturating_u64_from_usize(combined_len))?;
        let merge_upper = saturating_u64_from_usize(
            combined_len.saturating_mul(std::mem::size_of::<ExemplarSeries>()),
        )
        .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES);
        if merge_upper > merge_reservation.bytes() {
            merge_reservation.resize(merge_upper)?;
        }
        merged
            .try_reserve_exact(remote.len())
            .map_err(|_| ExemplarEnvelopeError::Internal {
                code: "query_exemplars_allocation_failed",
                diagnostic: "exemplar query allocation failed",
            })?;
        merged.append(&mut remote);
        reservations.push(accounted.reservation);
        reservations.push(conversion_reservation);
    }
    reservations.push(merge_reservation);
    let (series, dedupe_reservation) =
        dedupe_and_limit_exemplar_series_accounted(merged, limit, execution)?;
    reservations.push(dedupe_reservation);
    Ok(GuardedExemplarSeries {
        series,
        _reservations: reservations,
        _reservation_slots: reservation_slots,
    })
}

pub(crate) async fn handle_query_exemplars(
    storage: &Arc<dyn Storage>,
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
        storage,
        exemplar_store,
        request,
        precision,
        context,
        read_admission,
    )
    .await
}

pub(crate) async fn handle_query_exemplars_with_admission(
    storage: &Arc<dyn Storage>,
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    context: PublicReadContext<'_>,
    read_admission: &ReadAdmissionController,
) -> HttpResponse {
    let started = Instant::now();
    let raw_query = match preflight_promql_parameter(
        request,
        "query",
        tsink::promql::MAX_PARSE_INPUT_BYTES,
        true,
    ) {
        Ok(Some(value)) => value,
        Ok(None) => unreachable!("required exemplar query preflight returned no value"),
        Err(error) => return promql_parameter_error_response(error),
    };
    let raw_start =
        match preflight_promql_parameter(request, "start", MAX_PROMQL_SCALAR_PARAMETER_BYTES, true)
        {
            Ok(Some(value)) => value,
            Ok(None) => unreachable!("required exemplar start preflight returned no value"),
            Err(error) => return promql_parameter_error_response(error),
        };
    let raw_end =
        match preflight_promql_parameter(request, "end", MAX_PROMQL_SCALAR_PARAMETER_BYTES, true) {
            Ok(Some(value)) => value,
            Ok(None) => unreachable!("required exemplar end preflight returned no value"),
            Err(error) => return promql_parameter_error_response(error),
        };
    let raw_limit = match preflight_promql_parameter(
        request,
        "limit",
        MAX_PROMQL_SCALAR_PARAMETER_BYTES,
        false,
    ) {
        Ok(value) => value,
        Err(error) => return promql_parameter_error_response(error),
    };
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
    if let Err(response) = enforce_preflighted_tenant_query_length(&tenant_plan, raw_query) {
        return response;
    }
    let (execution_storage, _distributed_storage) = match storage_for_promql_request(
        storage,
        request,
        context.cluster_context,
        &tenant_id,
        tenant_plan.policy(),
    ) {
        Ok(storage) => storage,
        Err(response) => return response,
    };
    let (execution, _cancellation_guard) = match begin_promql_http_execution(&execution_storage) {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    let scalar_params = [raw_start, raw_end, raw_limit.unwrap_or_default()];
    let scalar_param_count = 2usize.saturating_add(usize::from(raw_limit.is_some()));
    let request_reservation = match reserve_promql_request_decode(
        &execution,
        raw_query,
        &scalar_params[..scalar_param_count],
    ) {
        Ok(reservation) => reservation,
        Err(response) => return response,
    };
    let query = match decode_preflighted_promql_parameter(
        raw_query,
        "query",
        tsink::promql::MAX_PARSE_INPUT_BYTES,
    ) {
        Ok(value) => value,
        Err(error) => return promql_parameter_error_response(error),
    };
    if let Err(error) = tenant::enforce_query_length_quota(tenant_plan.policy(), &query) {
        tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, error.clone());
        return promql_error_response("bad_data", &error);
    }
    let start_text = match decode_preflighted_promql_parameter(
        raw_start,
        "start",
        MAX_PROMQL_SCALAR_PARAMETER_BYTES,
    ) {
        Ok(value) => value,
        Err(error) => return promql_parameter_error_response(error),
    };
    let end_text = match decode_preflighted_promql_parameter(
        raw_end,
        "end",
        MAX_PROMQL_SCALAR_PARAMETER_BYTES,
    ) {
        Ok(value) => value,
        Err(error) => return promql_parameter_error_response(error),
    };
    let limit_text = match raw_limit {
        Some(raw) => match decode_preflighted_promql_parameter(
            raw,
            "limit",
            MAX_PROMQL_SCALAR_PARAMETER_BYTES,
        ) {
            Ok(value) => Some(value),
            Err(error) => return promql_parameter_error_response(error),
        },
        None => None,
    };
    let start = match parse_timestamp(&start_text, precision) {
        Ok(timestamp) => timestamp,
        Err(_) => {
            return promql_error_response("bad_data", "parameter 'start' is invalid")
                .with_header(READ_ERROR_CODE_HEADER, "query_exemplars_invalid_start")
        }
    };
    let end = match parse_timestamp(&end_text, precision) {
        Ok(timestamp) => timestamp,
        Err(_) => {
            return promql_error_response("bad_data", "parameter 'end' is invalid")
                .with_header(READ_ERROR_CODE_HEADER, "query_exemplars_invalid_end")
        }
    };
    if end < start {
        return promql_error_response("bad_data", "end timestamp must not be before start time")
            .with_header(READ_ERROR_CODE_HEADER, "query_exemplars_invalid_time_range");
    }
    let limit = match limit_text.as_deref() {
        Some(raw) => match raw.parse::<usize>() {
            Ok(limit) if limit > 0 && limit <= exemplar_store.config().max_query_results => limit,
            Ok(limit) if limit > exemplar_store.config().max_query_results => {
                return promql_error_response(
                    "bad_data",
                    &format!(
                        "parameter 'limit' exceeds the configured maximum {}",
                        exemplar_store.config().max_query_results
                    ),
                )
                .with_header(READ_ERROR_CODE_HEADER, "query_exemplars_limit_exceeded")
            }
            _ => {
                return promql_error_response(
                    "bad_data",
                    "parameter 'limit' must be a positive integer",
                )
                .with_header(READ_ERROR_CODE_HEADER, "query_exemplars_invalid_limit")
            }
        },
        None => exemplar_store.config().max_query_results,
    };

    let (expr, parse_reservation) = match parse_exemplar_query_with_execution(&query, &execution) {
        Ok(parsed) => parsed,
        Err(error) => return exemplar_envelope_error_response(error),
    };
    let (scoped, selection_reservation) = match prepare_scoped_exemplar_selections(
        &expr,
        &tenant_id,
        exemplar_store.config().max_query_selectors,
        &execution,
    ) {
        Ok(prepared) => prepared,
        Err(error) => return exemplar_envelope_error_response(error),
    };
    drop(expr);
    drop(parse_reservation);
    let selector_count = scoped.len();
    let scoped = Arc::new(scoped);

    let _tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Query,
        selector_count.max(1),
        context.usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(selector_count).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Query,
                selector_count.max(1),
                err.to_string(),
            );
            return read_admission_error_response(err);
        }
    };

    let guarded_series = if let Some(cluster_context) = context.cluster_context {
        match query_exemplars_across_cluster_accounted(
            exemplar_store,
            cluster_context,
            Arc::clone(&scoped),
            start,
            end,
            limit,
            &execution,
        )
        .await
        {
            Ok(series) => series,
            Err(error) => return exemplar_envelope_error_response(error),
        }
    } else {
        let local = match query_local_exemplars_accounted(
            exemplar_store,
            Arc::clone(&scoped),
            start,
            end,
            limit,
            &execution,
        )
        .await
        {
            Ok(result) => result,
            Err(error) => return exemplar_envelope_error_response(error),
        };
        let (series, local_reservation) = local.into_parts();
        let (series, dedupe_reservation) =
            match dedupe_and_limit_exemplar_series_accounted(series, limit, &execution) {
                Ok(result) => result,
                Err(error) => return exemplar_envelope_error_response(error),
            };
        let reservation_slots = match execution.reserve_memory(
            saturating_u64_from_usize(
                2usize.saturating_mul(std::mem::size_of::<tsink::QueryMemoryReservation>()),
            )
            .saturating_add(PROMQL_HTTP_ALLOCATION_ALLOWANCE_BYTES),
        ) {
            Ok(reservation) => reservation,
            Err(error) => return promql_query_budget_error_response(&error),
        };
        GuardedExemplarSeries {
            series,
            _reservations: vec![local_reservation, dedupe_reservation],
            _reservation_slots: reservation_slots,
        }
    };
    if !validate_final_exemplar_series_shape(&guarded_series.series, &tenant_id, start, end, limit)
    {
        return promql_internal_error_response(
            "query_exemplars_result_shape_invalid",
            "exemplar query returned an invalid or out-of-scope result",
        );
    }
    let mut metadata_reservation = None;
    let cluster_metadata = if let Some(cluster_context) = context.cluster_context {
        metadata_reservation = match execution.reserve_memory(EXEMPLAR_METADATA_ENVELOPE_BYTES) {
            Ok(reservation) => Some(reservation),
            Err(error) => return promql_query_budget_error_response(&error),
        };
        Some(ReadFanoutResponseMetadata {
            consistency: cluster_context.runtime.read_consistency,
            partial_response_policy: cluster_context.runtime.read_partial_response,
            partial_response: false,
            warnings: Vec::new(),
        })
    } else {
        None
    };
    let EncodedPromqlHttpResponse {
        mut response,
        reservation: response_reservation,
    } = match encode_exemplar_success_response(
        &guarded_series.series,
        precision,
        cluster_metadata.as_ref(),
        &execution,
    ) {
        Ok(encoded) => encoded,
        Err(error) => return promql_response_encoding_error_response(error),
    };
    response = response.with_header("X-Tsink-Exemplar-Limit", limit.to_string());
    if let Some(metadata) = cluster_metadata.as_ref() {
        response = with_read_metadata_headers(response, metadata);
    }

    let result_units = exemplar_query_units(&guarded_series.series);
    record_query_pressure(&tenant_id, selector_count, result_units);
    record_query_usage(
        context.usage_accounting,
        &tenant_id,
        "query_exemplars",
        request.path_without_query(),
        QueryUsageMetrics::new(
            selector_count.max(1) as u64,
            result_units as u64,
            elapsed_nanos_since(started),
            request.body.len() as u64,
        ),
    )
    .await;

    drop(metadata_reservation);
    drop(response_reservation);
    drop(guarded_series);
    drop(scoped);
    drop(selection_reservation);
    drop(request_reservation);
    drop(execution);
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::ReadAdmissionGuardrails;
    use crate::cluster::config::DEFAULT_CLUSTER_SHARDS;
    use crate::exemplar_store::{
        ExemplarSample, ExemplarSeries, ExemplarStoreConfig, ExemplarWrite,
    };
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::sync::Barrier;
    use std::time::Duration;
    use tsink::promql::types::{Sample, Series};
    use tsink::{
        HistogramBucketSpan, HistogramCount, HistogramResetHint, Label, NativeHistogram,
        QueryBudget, QueryBudgetLimits, QueryExecutionAccounting, QueryOptions, ResourceLimits,
        ResourceProfile, ResourceProfileName, SelectManyExecutionResult,
        SelectSeriesExecutionResult,
    };

    type LabelPair<'a> = (&'a str, &'a str);
    type ExemplarInput<'a> = (i64, f64, &'a [LabelPair<'a>]);

    fn query_budget_promql_error(error: tsink::QueryBudgetError) -> tsink::promql::PromqlError {
        tsink::promql::PromqlError::Storage(tsink::TsinkError::QueryBudget(error))
    }

    fn response_header<'a>(response: &'a HttpResponse, name: &str) -> Option<&'a str> {
        response
            .headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn test_read_admission() -> ReadAdmissionController {
        ReadAdmissionController::new(ReadAdmissionGuardrails {
            max_inflight_requests: 4,
            max_inflight_queries: 4,
            acquire_timeout: Duration::from_millis(50),
        })
        .expect("test read admission should build")
    }

    fn test_query_limits(max_returned_bytes: Option<u64>) -> QueryBudgetLimits {
        QueryBudgetLimits {
            max_concurrent_queries: Some(4),
            max_shared_memory_bytes: Some(128 * 1024 * 1024),
            per_query: tsink::QueryWorkLimits {
                max_returned_bytes,
                max_memory_bytes: Some(128 * 1024 * 1024),
                ..tsink::QueryWorkLimits::default()
            },
        }
    }

    fn test_storage(max_returned_bytes: Option<u64>) -> Arc<dyn Storage> {
        test_storage_with_limits(test_query_limits(max_returned_bytes))
    }

    fn test_storage_with_limits(limits: QueryBudgetLimits) -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(DEFAULT_CLUSTER_SHARDS)
            .with_query_budget_limits(limits)
            .build()
            .expect("test storage should build")
    }

    fn finite_test_profile_storage_with_returned_sample_limit(limit: u64) -> Arc<dyn Storage> {
        let mut limits = ResourceLimits::test().query;
        limits.per_query.max_samples_returned = Some(limit);
        StorageBuilder::new()
            .with_resource_profile(ResourceProfile::Test)
            .with_wal_enabled(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_metadata_shard_count(DEFAULT_CLUSTER_SHARDS)
            .with_query_budget_limits(limits)
            .build()
            .expect("finite Test-profile HTTP storage should build")
    }

    fn seeded_exemplar_store(config: ExemplarStoreConfig) -> Arc<ExemplarStore> {
        let store = Arc::new(ExemplarStore::in_memory_with_config(config));
        store
            .apply_writes(&[ExemplarWrite {
                metric: "latency_seconds".to_string(),
                series_labels: vec![
                    Label::new("job", "api"),
                    Label::new(tenant::TENANT_LABEL, tenant::DEFAULT_TENANT_ID),
                ],
                exemplar_labels: vec![Label::new("trace_id", "abc")],
                timestamp: 10,
                value: 1.5,
            }])
            .expect("seed exemplar");
        store
    }

    fn exemplar_request(query: &str, limit: usize) -> HttpRequest {
        HttpRequest {
            method: "GET".to_string(),
            path: format!("/api/v1/query_exemplars?query={query}&start=0&end=20&limit={limit}"),
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }

    fn form_request(path: &str, body: String) -> HttpRequest {
        HttpRequest {
            method: "POST".to_string(),
            path: path.to_string(),
            headers: HashMap::from([(
                "content-type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            )]),
            body: body.into_bytes(),
        }
    }

    fn sample_histogram() -> NativeHistogram {
        NativeHistogram {
            count: Some(HistogramCount::Float(20.0)),
            sum: 15.0,
            schema: 0,
            zero_threshold: 0.5,
            zero_count: Some(HistogramCount::Float(4.0)),
            negative_spans: Vec::new(),
            negative_deltas: Vec::new(),
            negative_counts: Vec::new(),
            positive_spans: vec![HistogramBucketSpan {
                offset: -1,
                length: 2,
            }],
            positive_deltas: Vec::new(),
            positive_counts: vec![6.0, 10.0],
            reset_hint: HistogramResetHint::No,
            custom_values: Vec::new(),
        }
    }

    fn encode_test_value(value: &PromqlValue) -> Result<Vec<u8>, PromqlResponseEncodingError> {
        let budget =
            QueryBudget::new(test_query_limits(None)).expect("test query budget should build");
        let execution = budget
            .begin_query_with(
                tsink::QueryWorkLimits::default(),
                tsink::QueryCancellationToken::new(),
            )
            .expect("test query should admit");
        let encoded =
            encode_promql_success_response(value, TimestampPrecision::Seconds, &execution)?;
        let EncodedPromqlHttpResponse {
            response,
            reservation,
        } = encoded;
        let body = response.body;
        drop(reservation);
        drop(execution);
        assert_eq!(budget.snapshot().active_queries, 0);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
        Ok(body)
    }

    struct UnaccountedStorage {
        select_called: Arc<AtomicBool>,
    }

    impl Storage for UnaccountedStorage {
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
            self.select_called.store(true, AtomicOrdering::SeqCst);
            Ok(Vec::new())
        }

        fn select_with_options(
            &self,
            _metric: &str,
            _opts: QueryOptions,
        ) -> tsink::Result<Vec<DataPoint>> {
            self.select_called.store(true, AtomicOrdering::SeqCst);
            Ok(Vec::new())
        }

        fn select_all(
            &self,
            _metric: &str,
            _start: i64,
            _end: i64,
        ) -> tsink::Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            self.select_called.store(true, AtomicOrdering::SeqCst);
            Ok(Vec::new())
        }

        fn close(&self) -> tsink::Result<()> {
            Ok(())
        }
    }

    struct BlockingAccountedStorage {
        inner: Arc<dyn Storage>,
        metadata_entered: Arc<Barrier>,
        metadata_release: Arc<Barrier>,
    }

    impl Storage for BlockingAccountedStorage {
        fn query_budget(&self) -> Option<QueryBudget> {
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
            opts: QueryOptions,
        ) -> tsink::Result<Vec<DataPoint>> {
            self.inner.select_with_options(metric, opts)
        }

        fn select_all(
            &self,
            metric: &str,
            start: i64,
            end: i64,
        ) -> tsink::Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            self.inner.select_all(metric, start, end)
        }

        fn select_many_with_execution_result(
            &self,
            series: &[MetricSeries],
            start: i64,
            end: i64,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<SelectManyExecutionResult> {
            self.inner
                .select_many_with_execution_result(series, start, end, execution)
        }

        fn select_many_execution_accounting(&self) -> QueryExecutionAccounting {
            self.inner.select_many_execution_accounting()
        }

        fn select_series_with_execution_result(
            &self,
            selection: &SeriesSelection,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<SelectSeriesExecutionResult> {
            self.metadata_entered.wait();
            self.metadata_release.wait();
            self.inner
                .select_series_with_execution_result(selection, execution)
        }

        fn select_series_execution_accounting(&self) -> QueryExecutionAccounting {
            self.inner.select_series_execution_accounting()
        }

        fn close(&self) -> tsink::Result<()> {
            self.inner.close()
        }
    }

    #[test]
    fn query_budget_errors_have_stable_http_mappings() {
        let steps = promql_execution_error_response(&query_budget_promql_error(
            tsink::QueryBudgetError::LimitExceeded(tsink::QueryLimitExceeded::new(
                tsink::QueryLimitReason::Steps,
                2,
                0,
                3,
            )),
        ));
        assert_eq!(steps.status, 413);
        assert_eq!(
            response_header(&steps, READ_ERROR_CODE_HEADER),
            Some("query_limit_steps")
        );
        let body: JsonValue = serde_json::from_slice(&steps.body).unwrap();
        assert_eq!(body["errorType"], "query_limit_steps");

        let concurrency = promql_execution_error_response(&query_budget_promql_error(
            tsink::QueryBudgetError::LimitExceeded(tsink::QueryLimitExceeded::new(
                tsink::QueryLimitReason::ConcurrentQueries,
                1,
                1,
                1,
            )),
        ));
        assert_eq!(concurrency.status, 429);
        assert_eq!(response_header(&concurrency, "Retry-After"), Some("1"));

        let cancelled = promql_execution_error_response(&query_budget_promql_error(
            tsink::QueryBudgetError::Cancelled,
        ));
        assert_eq!(cancelled.status, 503);
        let body: JsonValue = serde_json::from_slice(&cancelled.body).unwrap();
        assert_eq!(body["errorType"], "canceled");

        let deadline = promql_execution_error_response(&query_budget_promql_error(
            tsink::QueryBudgetError::DeadlineExceeded,
        ));
        assert_eq!(deadline.status, 503);
        let body: JsonValue = serde_json::from_slice(&deadline.body).unwrap();
        assert_eq!(body["errorType"], "timeout");
    }

    #[tokio::test]
    async fn range_http_test_profile_accepts_exact_n_rejects_n_plus_one_and_releases() {
        const LIMIT: u64 = 2;
        let storage = finite_test_profile_storage_with_returned_sample_limit(LIMIT);
        let configuration = storage.resource_configuration_snapshot();
        assert_eq!(configuration.selected_profile, ResourceProfileName::Test);
        let limits = configuration.resolved_limits.query;
        limits
            .validate()
            .expect("resolved Test-profile query limits must be valid");
        assert!(limits.max_concurrent_queries.is_some());
        assert!(limits.max_shared_memory_bytes.is_some());
        assert_eq!(limits.per_query.max_samples_returned, Some(LIMIT));
        assert!(limits.per_query.max_wall_time.is_some());

        let read_admission = test_read_admission();
        let exact = handle_range_query_with_admission(
            &storage,
            &form_request(
                "/api/v1/query_range",
                "query=vector(1)&start=0&end=1&step=1".to_string(),
            ),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(exact.status, 200);
        let exact_body: JsonValue =
            serde_json::from_slice(&exact.body).expect("exact response should be JSON");
        assert_eq!(exact_body["status"], "success");
        assert_eq!(
            exact_body["data"]["result"][0]["values"]
                .as_array()
                .expect("range values")
                .len(),
            LIMIT as usize
        );
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);

        let one_over = handle_range_query_with_admission(
            &storage,
            &form_request(
                "/api/v1/query_range",
                "query=vector(1)&start=0&end=2&step=1".to_string(),
            ),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(one_over.status, 413);
        assert_eq!(
            response_header(&one_over, READ_ERROR_CODE_HEADER),
            Some("query_limit_samples_returned")
        );
        let one_over_body: JsonValue =
            serde_json::from_slice(&one_over.body).expect("limit response should be JSON");
        assert_eq!(one_over_body["errorType"], "query_limit_samples_returned");
        assert_eq!(one_over_body["status"], "error");

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.queries_started_total, 2);
        assert_eq!(snapshot.queries_completed_total, 2);
        assert_eq!(snapshot.samples_returned_rejections_total, 1);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn promql_json_serializer_preserves_canonical_golden_bytes_without_cloning() {
        let value = PromqlValue::InstantVector(vec![Sample {
            metric: "original".to_string(),
            labels: vec![
                Label::new("__zzz", "z"),
                Label::new("__aaa", "first"),
                Label::new("__name__", "override"),
                Label::new("__aaa", "last"),
            ],
            timestamp: 1,
            value: 2.5,
            histogram: None,
        }]);
        let body = encode_test_value(&value).expect("scalar-vector JSON should encode");
        assert_eq!(
            body,
            br#"{"data":{"result":[{"metric":{"__aaa":"last","__name__":"override","__zzz":"z"},"value":[1.0,"2.5"]}],"resultType":"vector"},"status":"success"}"#
        );

        let histogram = PromqlValue::InstantVector(vec![Sample {
            metric: "native".to_string(),
            labels: vec![Label::new("job", "api")],
            timestamp: 1,
            value: f64::NAN,
            histogram: Some(Box::new(sample_histogram())),
        }]);
        let body = encode_test_value(&histogram).expect("native histogram JSON should encode");
        assert_eq!(
            body,
            br#"{"data":{"result":[{"histogram":[1.0,{"buckets":[[1,"-0.5","0.5","4"],[1,"0.5","1","6"],[1,"1","2","10"]],"count":"20","sum":"15"}],"metric":{"__name__":"native","job":"api"}}],"resultType":"vector"},"status":"success"}"#
        );

        let matrix = PromqlValue::RangeVector(vec![Series {
            metric: "range".to_string(),
            labels: vec![Label::new("job", "api")],
            samples: vec![(1, 2.0)],
            histograms: Vec::new(),
        }]);
        let body = encode_test_value(&matrix).expect("matrix JSON should encode");
        assert_eq!(
            body,
            br#"{"data":{"result":[{"metric":{"__name__":"range","job":"api"},"values":[[1.0,"2"]]}],"resultType":"matrix"},"status":"success"}"#
        );
    }

    #[test]
    fn malformed_and_custom_histograms_fail_with_bounded_static_http_errors() {
        let mut custom = sample_histogram();
        custom.custom_values = vec![1.0];
        let mut malformed = sample_histogram();
        malformed.positive_spans = vec![HistogramBucketSpan {
            offset: 0,
            length: 32,
        }];
        malformed.positive_counts = vec![1.0];

        for histogram in [custom, malformed] {
            let value = PromqlValue::InstantVector(vec![Sample {
                metric: "private_histogram_metric".to_string(),
                labels: Vec::new(),
                timestamp: 1,
                value: f64::NAN,
                histogram: Some(Box::new(histogram)),
            }]);
            let error = encode_test_value(&value)
                .expect_err("invalid histogram expansion must not return partial success");
            assert!(matches!(error, PromqlResponseEncodingError::Serialization));
            let response = promql_response_encoding_error_response(error);
            assert_eq!(response.status, 500);
            assert_eq!(
                response_header(&response, READ_ERROR_CODE_HEADER),
                Some("promql_response_serialization_failed")
            );
            let body = String::from_utf8(response.body).expect("error body should be utf8");
            assert!(!body.contains("private_histogram_metric"));
            assert!(!body.contains("\"success\""));
            assert!(body.len() <= PROMQL_HTTP_DIAGNOSTIC_ENVELOPE_BYTES as usize);
        }
    }

    #[test]
    fn raw_promql_parameter_caps_accept_exact_n_and_reject_n_plus_one() {
        let exact_query = "q".repeat(tsink::promql::MAX_PARSE_INPUT_BYTES);
        let exact = form_request("/api/v1/query", format!("query={exact_query}"));
        assert_eq!(
            preflight_promql_parameter(&exact, "query", tsink::promql::MAX_PARSE_INPUT_BYTES, true,),
            Ok(Some(exact_query.as_str()))
        );

        let over_query = "q".repeat(tsink::promql::MAX_PARSE_INPUT_BYTES + 1);
        let over = form_request("/api/v1/query", format!("query={over_query}"));
        assert!(matches!(
            preflight_promql_parameter(
                &over,
                "query",
                tsink::promql::MAX_PARSE_INPUT_BYTES,
                true,
            ),
            Err(PromqlParameterError::TooLong {
                actual,
                maximum: tsink::promql::MAX_PARSE_INPUT_BYTES,
                ..
            }) if actual == tsink::promql::MAX_PARSE_INPUT_BYTES + 1
        ));

        let exact_scalar = "%61".repeat(MAX_PROMQL_SCALAR_PARAMETER_BYTES);
        let exact = form_request("/api/v1/query", format!("query=1&time={exact_scalar}"));
        assert_eq!(
            preflight_promql_parameter(&exact, "time", MAX_PROMQL_SCALAR_PARAMETER_BYTES, true,),
            Ok(Some(exact_scalar.as_str()))
        );

        let over_scalar = "%61".repeat(MAX_PROMQL_SCALAR_PARAMETER_BYTES + 1);
        let over = form_request("/api/v1/query", format!("query=1&time={over_scalar}"));
        assert!(matches!(
            preflight_promql_parameter(
                &over,
                "time",
                MAX_PROMQL_SCALAR_PARAMETER_BYTES,
                true,
            ),
            Err(PromqlParameterError::TooLong {
                actual,
                maximum: MAX_PROMQL_SCALAR_PARAMETER_BYTES,
                ..
            }) if actual == MAX_PROMQL_SCALAR_PARAMETER_BYTES + 1
        ));
    }

    #[test]
    fn request_decode_reservation_has_exact_n_and_n_minus_one_boundaries() {
        let raw_query = "%31";
        let raw_time = "%30";
        let required = modeled_promql_request_upper_bytes(raw_query, &[raw_time]);
        assert!(required > 1);

        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(required),
            per_query: tsink::QueryWorkLimits {
                max_memory_bytes: Some(required),
                ..tsink::QueryWorkLimits::default()
            },
        })
        .expect("exact decode budget should build");
        let exact = exact_budget
            .begin_query()
            .expect("exact decode query should admit");
        let reservation = reserve_promql_request_decode(&exact, raw_query, &[raw_time])
            .expect("exact decode reservation should succeed");
        assert_eq!(exact.snapshot().memory_reserved_bytes, required);
        assert_eq!(percent_decode(raw_query), "1");
        assert_eq!(percent_decode(raw_time), "0");
        drop(reservation);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(required - 1),
            per_query: tsink::QueryWorkLimits {
                max_memory_bytes: Some(required - 1),
                ..tsink::QueryWorkLimits::default()
            },
        })
        .expect("one-under decode budget should build");
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under decode query should admit");
        let response = reserve_promql_request_decode(&one_under, raw_query, &[raw_time])
            .expect_err("one-under decode reservation must fail");
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        drop(one_under);
        assert_eq!(one_under_budget.snapshot().shared_reserved_memory_bytes, 0);
    }

    #[tokio::test]
    async fn scalar_parameter_boundary_and_invalid_range_values_never_echo_input() {
        let storage = test_storage(None);
        let read_admission = test_read_admission();
        let exact_sentinel = "X".repeat(MAX_PROMQL_SCALAR_PARAMETER_BYTES);
        let exact = handle_instant_query_with_admission(
            &storage,
            &form_request("/api/v1/query", format!("query=1&time={exact_sentinel}")),
            TimestampPrecision::Milliseconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(exact.status, 422);
        let exact_body = String::from_utf8(exact.body).expect("error body should be utf8");
        assert!(!exact_body.contains(&exact_sentinel));
        assert!(exact_body.len() <= PROMQL_HTTP_DIAGNOSTIC_ENVELOPE_BYTES as usize);
        assert_eq!(storage.query_budget_snapshot().queries_started_total, 1);

        let over_sentinel = "Y".repeat(MAX_PROMQL_SCALAR_PARAMETER_BYTES + 1);
        let over = handle_instant_query_with_admission(
            &storage,
            &form_request("/api/v1/query", format!("query=1&time={over_sentinel}")),
            TimestampPrecision::Milliseconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(over.status, 422);
        let over_body = String::from_utf8(over.body).expect("error body should be utf8");
        assert!(!over_body.contains(&over_sentinel));
        assert_eq!(
            storage.query_budget_snapshot().queries_started_total,
            1,
            "N+1 must reject before query admission"
        );

        let range_sentinel = "PRIVATE_RANGE_SENTINEL".repeat(4);
        for (start, end, step) in [
            (range_sentinel.as_str(), "1", "1"),
            ("0", range_sentinel.as_str(), "1"),
            ("0", "1", range_sentinel.as_str()),
        ] {
            let response = handle_range_query_with_admission(
                &storage,
                &form_request(
                    "/api/v1/query_range",
                    format!("query=1&start={start}&end={end}&step={step}"),
                ),
                TimestampPrecision::Milliseconds,
                PublicReadContext::new(None, None, None, None),
                &read_admission,
            )
            .await;
            assert_eq!(response.status, 422);
            let body = String::from_utf8(response.body).expect("error body should be utf8");
            assert!(!body.contains(&range_sentinel));
            assert!(body.len() <= PROMQL_HTTP_DIAGNOSTIC_ENVELOPE_BYTES as usize);
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    }

    #[tokio::test]
    async fn long_parse_and_regex_sentinels_are_not_reflected_and_release_query_state() {
        let storage = test_storage(None);
        let read_admission = test_read_admission();
        let sentinel = "PRIVATE_PROMQL_SENTINEL_".repeat(128);
        let parse = handle_instant_query_with_admission(
            &storage,
            &form_request("/api/v1/query", format!("query={sentinel}%7B&time=0")),
            TimestampPrecision::Milliseconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(parse.status, 422);
        assert_eq!(
            response_header(&parse, READ_ERROR_CODE_HEADER),
            Some("promql_invalid_syntax")
        );
        let body = String::from_utf8(parse.body).expect("parse body should be utf8");
        assert!(!body.contains("PRIVATE_PROMQL_SENTINEL_"));
        assert!(body.len() <= PROMQL_HTTP_DIAGNOSTIC_ENVELOPE_BYTES as usize);

        let regex = handle_instant_query_with_admission(
            &storage,
            &form_request(
                "/api/v1/query",
                format!("query=up%7Blabel%3D~%22%28{sentinel}%22%7D&time=0"),
            ),
            TimestampPrecision::Milliseconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(regex.status, 422);
        assert_eq!(
            response_header(&regex, READ_ERROR_CODE_HEADER),
            Some("promql_invalid_regex")
        );
        let body = String::from_utf8(regex.body).expect("regex body should be utf8");
        assert!(!body.contains("PRIVATE_PROMQL_SENTINEL_"));
        assert!(body.len() <= PROMQL_HTTP_DIAGNOSTIC_ENVELOPE_BYTES as usize);

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.queries_started_total, 2);
        assert_eq!(snapshot.queries_completed_total, 2);
    }

    #[tokio::test]
    async fn promql_http_charges_exact_json_wire_bytes_and_rejects_n_minus_one() {
        let calibration_storage = test_storage(None);
        let scoped = tenant::scoped_storage(calibration_storage, "default");
        let execution = scoped
            .begin_query_execution(
                tsink::QueryWorkLimits::default(),
                tsink::QueryCancellationToken::new(),
            )
            .expect("calibration execution should begin")
            .expect("calibration storage should be accounted");
        let engine = Engine::with_precision(Arc::clone(&scoped), TimestampPrecision::Milliseconds);
        let result = engine
            .instant_query_with_execution_result("1", 0, &execution)
            .expect("calibration PromQL should evaluate");
        let encoded = encode_promql_success_response(
            result.value(),
            TimestampPrecision::Milliseconds,
            &execution,
        )
        .expect("calibration JSON should encode");
        let exact_bytes = execution.snapshot().returned_bytes;
        let expected_body = encoded.response.body.clone();
        assert!(exact_bytes > expected_body.len() as u64);
        drop(encoded);
        drop(result);
        drop(execution);

        let read_admission = test_read_admission();
        let exact_storage = test_storage(Some(exact_bytes));
        let exact = handle_instant_query_with_admission(
            &exact_storage,
            &HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/query?query=1&time=0".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            TimestampPrecision::Milliseconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(exact.status, 200);
        assert_eq!(exact.body, expected_body);
        let snapshot = exact_storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);

        let one_under_storage = test_storage(Some(exact_bytes - 1));
        let one_under = handle_instant_query_with_admission(
            &one_under_storage,
            &HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/query?query=1&time=0".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            TimestampPrecision::Milliseconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(one_under.status, 413);
        assert_eq!(
            response_header(&one_under, READ_ERROR_CODE_HEADER),
            Some("query_limit_returned_bytes")
        );
        let snapshot = one_under_storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.returned_bytes_rejections_total, 1);
    }

    #[tokio::test]
    async fn public_promql_fails_closed_for_unaccounted_storage_before_evaluation() {
        let select_called = Arc::new(AtomicBool::new(false));
        let storage: Arc<dyn Storage> = Arc::new(UnaccountedStorage {
            select_called: Arc::clone(&select_called),
        });
        let response = handle_instant_query_with_admission(
            &storage,
            &HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/query?query=up&time=0".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            TimestampPrecision::Milliseconds,
            PublicReadContext::new(None, None, None, None),
            &test_read_admission(),
        )
        .await;

        assert_eq!(response.status, 500);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("promql_unaccounted_storage_backend")
        );
        assert!(!select_called.load(AtomicOrdering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_public_promql_future_cancels_blocking_evaluation_and_releases_budget() {
        let inner = test_storage(None);
        inner
            .insert_rows(&[Row::new("up", DataPoint::new(0, 1.0))])
            .expect("test row should insert");
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let storage: Arc<dyn Storage> = Arc::new(BlockingAccountedStorage {
            inner: Arc::clone(&inner),
            metadata_entered: Arc::clone(&entered),
            metadata_release: Arc::clone(&release),
        });
        let read_admission = Arc::new(test_read_admission());
        let storage_for_handler = Arc::clone(&storage);
        let admission_for_handler = Arc::clone(&read_admission);
        let task = tokio::spawn(async move {
            handle_instant_query_with_admission(
                &storage_for_handler,
                &HttpRequest {
                    method: "GET".to_string(),
                    path: "/api/v1/query?query=up&time=0".to_string(),
                    headers: HashMap::new(),
                    body: Vec::new(),
                },
                TimestampPrecision::Milliseconds,
                PublicReadContext::new(None, None, None, None),
                &admission_for_handler,
            )
            .await
        });

        tokio::task::spawn_blocking(move || entered.wait())
            .await
            .expect("metadata-entry waiter should join");
        assert_eq!(inner.query_budget_snapshot().active_queries, 1);
        task.abort();
        let join_error = task.await.expect_err("aborted handler should not complete");
        assert!(join_error.is_cancelled());

        tokio::task::spawn_blocking(move || release.wait())
            .await
            .expect("metadata-release waiter should join");
        for _ in 0..1_000 {
            if inner.query_budget_snapshot().active_queries == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let snapshot = inner.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert!(snapshot.cancellations_total >= 1);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn query_exemplars_streams_canonical_json_and_charges_exact_wire_bytes() {
        let config = ExemplarStoreConfig {
            max_total_exemplars: 8,
            max_exemplars_per_series: 8,
            max_exemplars_per_request: 8,
            max_query_results: 8,
            max_query_selectors: 4,
        };
        let expected = br#"{"data":[{"exemplars":[{"labels":{"trace_id":"abc"},"timestamp":10.0,"value":"1.5"}],"seriesLabels":{"__name__":"latency_seconds","job":"api"}}],"status":"success"}"#;
        let logical_bytes = "latency_seconds".len()
            + "job".len()
            + "api".len()
            + tenant::TENANT_LABEL.len()
            + tenant::DEFAULT_TENANT_ID.len()
            + 16
            + "trace_id".len()
            + "abc".len();
        let exact_returned_bytes = u64::try_from(logical_bytes + expected.len()).unwrap();
        let query = "latency_seconds%7Bjob%3D%22api%22%7D";
        let read_admission = test_read_admission();

        let exact_storage = test_storage(Some(exact_returned_bytes));
        let exact_store = seeded_exemplar_store(config);
        let exact = handle_query_exemplars_with_admission(
            &exact_storage,
            &exact_store,
            &exemplar_request(query, 1),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(exact.status, 200);
        assert_eq!(exact.body, expected);
        assert_eq!(response_header(&exact, "X-Tsink-Exemplar-Limit"), Some("1"));
        let snapshot = exact_storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);

        let under_storage = test_storage(Some(exact_returned_bytes - 1));
        let under_store = seeded_exemplar_store(config);
        let under = handle_query_exemplars_with_admission(
            &under_storage,
            &under_store,
            &exemplar_request(query, 1),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(under.status, 413);
        assert_eq!(
            response_header(&under, READ_ERROR_CODE_HEADER),
            Some("query_limit_returned_bytes")
        );
        let snapshot = under_storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.returned_bytes_rejections_total, 1);
    }

    #[tokio::test]
    async fn query_exemplars_has_exact_end_to_end_memory_envelope() {
        let config = ExemplarStoreConfig {
            max_total_exemplars: 8,
            max_exemplars_per_series: 8,
            max_exemplars_per_request: 8,
            max_query_results: 8,
            max_query_selectors: 4,
        };
        let query = "latency_seconds%7Bjob%3D%22api%22%7D";
        let read_admission = test_read_admission();
        let calibration_storage = test_storage(None);
        let calibration_store = seeded_exemplar_store(config);
        let calibration = handle_query_exemplars_with_admission(
            &calibration_storage,
            &calibration_store,
            &exemplar_request(query, 1),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(calibration.status, 200);
        let exact_memory = calibration_storage
            .query_budget_snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(exact_memory > 0);

        let limits = |memory| QueryBudgetLimits {
            max_concurrent_queries: Some(4),
            max_shared_memory_bytes: Some(memory),
            per_query: tsink::QueryWorkLimits {
                max_memory_bytes: Some(memory),
                ..tsink::QueryWorkLimits::default()
            },
        };
        let exact_storage = test_storage_with_limits(limits(exact_memory));
        let exact_store = seeded_exemplar_store(config);
        let exact = handle_query_exemplars_with_admission(
            &exact_storage,
            &exact_store,
            &exemplar_request(query, 1),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(exact.status, 200);
        assert_eq!(exact.body, calibration.body);
        let snapshot = exact_storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);

        let under_storage = test_storage_with_limits(limits(exact_memory - 1));
        let under_store = seeded_exemplar_store(config);
        let under = handle_query_exemplars_with_admission(
            &under_storage,
            &under_store,
            &exemplar_request(query, 1),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &read_admission,
        )
        .await;
        assert_eq!(under.status, 413);
        assert_eq!(
            response_header(&under, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        let snapshot = under_storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.per_query_memory_rejections_total, 1);
    }

    #[tokio::test]
    async fn query_exemplars_enforces_selector_limit_and_hides_hostile_input() {
        let config = ExemplarStoreConfig {
            max_total_exemplars: 8,
            max_exemplars_per_series: 8,
            max_exemplars_per_request: 8,
            max_query_results: 2,
            max_query_selectors: 2,
        };
        let store = Arc::new(ExemplarStore::in_memory_with_config(config));
        let storage = test_storage(None);
        let admission = test_read_admission();

        let exact = handle_query_exemplars_with_admission(
            &storage,
            &store,
            &exemplar_request("a%2Bb", 2),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &admission,
        )
        .await;
        assert_eq!(exact.status, 200);
        assert_eq!(exact.body, br#"{"data":[],"status":"success"}"#);

        let over = handle_query_exemplars_with_admission(
            &storage,
            &store,
            &exemplar_request("a%2Bb%2Bc", 2),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &admission,
        )
        .await;
        assert_eq!(over.status, 422);
        assert_eq!(
            response_header(&over, READ_ERROR_CODE_HEADER),
            Some("query_exemplars_selector_limit_exceeded")
        );

        let over_limit = handle_query_exemplars_with_admission(
            &storage,
            &store,
            &exemplar_request("a", 3),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &admission,
        )
        .await;
        assert_eq!(over_limit.status, 422);
        assert_eq!(
            response_header(&over_limit, READ_ERROR_CODE_HEADER),
            Some("query_exemplars_limit_exceeded")
        );

        let sentinel = "PRIVATE_EXEMPLAR_SENTINEL_".repeat(64);
        let malformed = handle_query_exemplars_with_admission(
            &storage,
            &store,
            &exemplar_request(&format!("{sentinel}%7B"), 2),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &admission,
        )
        .await;
        assert_eq!(malformed.status, 422);
        let body = String::from_utf8(malformed.body).expect("UTF-8 diagnostic");
        assert!(!body.contains("PRIVATE_EXEMPLAR_SENTINEL_"));
        assert!(body.len() <= PROMQL_HTTP_DIAGNOSTIC_ENVELOPE_BYTES as usize);

        let hostile_regex = handle_query_exemplars_with_admission(
            &storage,
            &store,
            &exemplar_request(&format!("a%7Bjob%3D~%22%28{sentinel}%22%7D"), 2),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &admission,
        )
        .await;
        assert_eq!(hostile_regex.status, 422);
        let body = String::from_utf8(hostile_regex.body).expect("UTF-8 diagnostic");
        assert!(!body.contains("PRIVATE_EXEMPLAR_SENTINEL_"));
        assert!(body.len() <= PROMQL_HTTP_DIAGNOSTIC_ENVELOPE_BYTES as usize);

        let exact_query = "a".repeat(tsink::promql::MAX_PARSE_INPUT_BYTES);
        let exact_query_response = handle_query_exemplars_with_admission(
            &storage,
            &store,
            &exemplar_request(&exact_query, 2),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &admission,
        )
        .await;
        assert_eq!(exact_query_response.status, 422);
        assert_ne!(
            response_header(&exact_query_response, READ_ERROR_CODE_HEADER),
            Some("query_exemplars_parameter_too_long")
        );

        let over_query = "a".repeat(tsink::promql::MAX_PARSE_INPUT_BYTES + 1);
        let over_query_response = handle_query_exemplars_with_admission(
            &storage,
            &store,
            &exemplar_request(&over_query, 2),
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &admission,
        )
        .await;
        assert_eq!(over_query_response.status, 422);

        let exact_scalar = "1".repeat(MAX_PROMQL_SCALAR_PARAMETER_BYTES);
        let exact_scalar_response = handle_query_exemplars_with_admission(
            &storage,
            &store,
            &HttpRequest {
                method: "GET".to_string(),
                path: format!(
                    "/api/v1/query_exemplars?query=a&start={exact_scalar}&end=20&limit=2"
                ),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &admission,
        )
        .await;
        assert_eq!(exact_scalar_response.status, 422);
        assert_eq!(
            response_header(&exact_scalar_response, READ_ERROR_CODE_HEADER),
            Some("query_exemplars_invalid_start")
        );

        let over_scalar = "1".repeat(MAX_PROMQL_SCALAR_PARAMETER_BYTES + 1);
        let over_scalar_response = handle_query_exemplars_with_admission(
            &storage,
            &store,
            &HttpRequest {
                method: "GET".to_string(),
                path: format!("/api/v1/query_exemplars?query=a&start={over_scalar}&end=20&limit=2"),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            TimestampPrecision::Seconds,
            PublicReadContext::new(None, None, None, None),
            &admission,
        )
        .await;
        assert_eq!(over_scalar_response.status, 422);

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_query_exemplars_future_cancels_store_work_and_releases_budget() {
        let config = ExemplarStoreConfig {
            max_total_exemplars: 8,
            max_exemplars_per_series: 8,
            max_exemplars_per_request: 8,
            max_query_results: 8,
            max_query_selectors: 4,
        };
        let store = seeded_exemplar_store(config);
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        store.set_query_test_gate(Arc::clone(&entered), Arc::clone(&release));
        let storage = test_storage(None);
        let admission = Arc::new(test_read_admission());
        let storage_for_handler = Arc::clone(&storage);
        let store_for_handler = Arc::clone(&store);
        let admission_for_handler = Arc::clone(&admission);
        let task = tokio::spawn(async move {
            handle_query_exemplars_with_admission(
                &storage_for_handler,
                &store_for_handler,
                &exemplar_request("latency_seconds", 1),
                TimestampPrecision::Seconds,
                PublicReadContext::new(None, None, None, None),
                &admission_for_handler,
            )
            .await
        });

        tokio::task::spawn_blocking(move || entered.wait())
            .await
            .expect("query-entry waiter should join");
        assert_eq!(storage.query_budget_snapshot().active_queries, 1);
        task.abort();
        assert!(task.await.expect_err("handler should abort").is_cancelled());
        tokio::task::spawn_blocking(move || release.wait())
            .await
            .expect("query-release waiter should join");
        for _ in 0..1_000 {
            if storage.query_budget_snapshot().active_queries == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert!(snapshot.cancellations_total >= 1);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn remote_exemplar_shape_validation_rejects_malformed_or_cross_tenant_data() {
        let valid = InternalExemplarSeries {
            metric: "latency_seconds".to_string(),
            labels: vec![
                Label::new("job", "api"),
                Label::new(tenant::TENANT_LABEL, "tenant-a"),
            ],
            exemplars: vec![InternalExemplar {
                labels: vec![Label::new("trace_id", "abc")],
                value: 1.5,
                timestamp: 10,
            }],
        };
        validate_internal_exemplar_response_shape(
            std::slice::from_ref(&valid),
            "tenant-a",
            0,
            20,
            1,
        )
        .expect("valid remote exemplar response");

        let mut cases = Vec::new();
        let mut empty_samples = valid.clone();
        empty_samples.exemplars.clear();
        cases.push(empty_samples);
        let mut wrong_tenant = valid.clone();
        wrong_tenant
            .labels
            .iter_mut()
            .find(|label| label.name == tenant::TENANT_LABEL)
            .expect("tenant label")
            .value = "PRIVATE_REMOTE_TENANT_SENTINEL".to_string();
        cases.push(wrong_tenant);
        let mut duplicate_label = valid.clone();
        duplicate_label.labels.push(Label::new("job", "duplicate"));
        cases.push(duplicate_label);
        let mut reserved_name = valid.clone();
        reserved_name
            .labels
            .push(Label::new("__name__", "override"));
        cases.push(reserved_name);
        let mut oversized_metric = valid.clone();
        oversized_metric.metric = "m".repeat(tsink::label::MAX_METRIC_NAME_LEN + 1);
        cases.push(oversized_metric);
        let mut oversized_label = valid.clone();
        oversized_label.exemplars[0].labels[0].value =
            "v".repeat(tsink::label::MAX_LABEL_VALUE_LEN + 1);
        cases.push(oversized_label);
        let mut outside_range = valid.clone();
        outside_range.exemplars[0].timestamp = 21;
        cases.push(outside_range);
        let mut before_range = valid.clone();
        before_range.exemplars[0].timestamp = -1;
        cases.push(before_range);
        let mut missing_tenant = valid.clone();
        missing_tenant
            .labels
            .retain(|label| label.name != tenant::TENANT_LABEL);
        cases.push(missing_tenant);
        let mut duplicate_tenant = valid.clone();
        duplicate_tenant
            .labels
            .push(Label::new(tenant::TENANT_LABEL, "tenant-a"));
        cases.push(duplicate_tenant);
        let mut exemplar_tenant = valid.clone();
        exemplar_tenant.exemplars[0]
            .labels
            .push(Label::new(tenant::TENANT_LABEL, "tenant-a"));
        cases.push(exemplar_tenant);
        let mut oversized_identity = valid.clone();
        oversized_identity.metric = "m".repeat(tsink::DEFAULT_MAX_SERIES_IDENTITY_BYTES - 1);
        cases.push(oversized_identity);

        for case in cases {
            let error = validate_internal_exemplar_response_shape(
                std::slice::from_ref(&case),
                "tenant-a",
                0,
                20,
                1,
            )
            .expect_err("malformed remote response must fail closed");
            let response = exemplar_envelope_error_response(error);
            assert_eq!(response.status, 500);
            let body = String::from_utf8(response.body).expect("UTF-8 diagnostic");
            assert!(!body.contains("PRIVATE_REMOTE_TENANT_SENTINEL"));
            assert!(body.len() <= PROMQL_HTTP_DIAGNOSTIC_ENVELOPE_BYTES as usize);
        }

        let too_many_series = vec![valid.clone(), valid.clone()];
        assert!(
            validate_internal_exemplar_response_shape(&too_many_series, "tenant-a", 0, 20, 1,)
                .is_err()
        );
        let mut too_many_samples = valid.clone();
        too_many_samples.exemplars.push(InternalExemplar {
            labels: vec![Label::new("trace_id", "def")],
            value: 2.5,
            timestamp: 11,
        });
        assert!(validate_internal_exemplar_response_shape(
            std::slice::from_ref(&too_many_samples),
            "tenant-a",
            0,
            20,
            1,
        )
        .is_err());
    }

    #[test]
    fn remote_exemplar_accounting_rejects_underreporting_and_has_exact_returned_byte_boundary() {
        let series = vec![InternalExemplarSeries {
            metric: "latency_seconds".to_string(),
            labels: vec![
                Label::new("job", "api"),
                Label::new(tenant::TENANT_LABEL, "tenant-a"),
            ],
            exemplars: vec![InternalExemplar {
                labels: vec![Label::new("trace_id", "abc")],
                value: 1.5,
                timestamp: 10,
            }],
        }];
        let logical_bytes = modeled_internal_exemplar_logical_bytes(&series);
        assert!(logical_bytes > 1);
        let valid = tsink::QueryExecutionSnapshot {
            memory_reserved_bytes: 1,
            series_matched: 1,
            samples_scanned: 1,
            samples_returned: 1,
            returned_bytes: logical_bytes,
            pattern_expansion: 1,
            steps: 0,
            intermediate_vector_size: 1,
        };

        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(1024),
            per_query: tsink::QueryWorkLimits {
                max_returned_bytes: Some(logical_bytes),
                max_memory_bytes: Some(1024),
                ..tsink::QueryWorkLimits::default()
            },
        })
        .expect("exact remote accounting budget");
        let exact = exact_budget
            .begin_query()
            .expect("exact query should admit");
        validate_and_charge_remote_exemplar_accounting(&exact, &series, valid)
            .expect("exact logical returned-byte budget should pass");
        assert_eq!(exact.snapshot().returned_bytes, logical_bytes);
        drop(exact);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let under_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(1024),
            per_query: tsink::QueryWorkLimits {
                max_returned_bytes: Some(logical_bytes - 1),
                max_memory_bytes: Some(1024),
                ..tsink::QueryWorkLimits::default()
            },
        })
        .expect("one-under remote accounting budget");
        let under = under_budget
            .begin_query()
            .expect("one-under query should admit");
        let error = validate_and_charge_remote_exemplar_accounting(&under, &series, valid)
            .expect_err("N-1 logical returned-byte budget must fail");
        assert!(matches!(
            error,
            ExemplarEnvelopeError::Budget(tsink::QueryBudgetError::LimitExceeded(
                tsink::QueryLimitExceeded {
                    reason: tsink::QueryLimitReason::ReturnedBytes,
                    ..
                }
            ))
        ));
        drop(under);
        let under_snapshot = under_budget.snapshot();
        assert_eq!(under_snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(under_snapshot.returned_bytes_rejections_total, 1);

        for invalid in [
            tsink::QueryExecutionSnapshot {
                series_matched: 0,
                ..valid
            },
            tsink::QueryExecutionSnapshot {
                samples_scanned: 0,
                ..valid
            },
            tsink::QueryExecutionSnapshot {
                samples_returned: 0,
                ..valid
            },
            tsink::QueryExecutionSnapshot {
                returned_bytes: logical_bytes - 1,
                ..valid
            },
            tsink::QueryExecutionSnapshot {
                memory_reserved_bytes: 0,
                ..valid
            },
            tsink::QueryExecutionSnapshot {
                intermediate_vector_size: 0,
                ..valid
            },
        ] {
            let budget = QueryBudget::new(QueryBudgetLimits::default())
                .expect("hostile accounting test budget");
            let execution = budget.begin_query().expect("test query should admit");
            let error =
                validate_and_charge_remote_exemplar_accounting(&execution, &series, invalid)
                    .expect_err("under-reported remote accounting must fail closed");
            assert!(matches!(error, ExemplarEnvelopeError::Internal { .. }));
            assert_eq!(execution.snapshot().series_matched, 0);
            drop(execution);
            assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
        }

        let cancellation = tsink::QueryCancellationToken::new();
        let cancellation_budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("cancellation budget");
        let cancelled = cancellation_budget
            .begin_query_with(tsink::QueryWorkLimits::default(), cancellation.clone())
            .expect("cancellation query should admit");
        cancellation.cancel();
        let error = validate_and_charge_remote_exemplar_accounting(&cancelled, &series, valid)
            .expect_err("cancelled coordinator must reject remote accounting");
        assert!(matches!(
            error,
            ExemplarEnvelopeError::Budget(tsink::QueryBudgetError::Cancelled)
        ));
        assert_eq!(cancelled.snapshot().series_matched, 0);
        drop(cancelled);
        let cancelled_snapshot = cancellation_budget.snapshot();
        assert_eq!(cancelled_snapshot.active_queries, 0);
        assert_eq!(cancelled_snapshot.shared_reserved_memory_bytes, 0);
        assert!(cancelled_snapshot.cancellations_total >= 1);
    }

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
