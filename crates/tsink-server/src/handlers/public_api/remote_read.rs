use super::super::*;
use crate::cluster::query::ReadFanoutResponse;
use snap::raw::max_compress_len;

// Bound the complete uncompressed remote-read protobuf independently of the selected storage
// profile. This also gives ExpertUnlimited a finite adapter envelope.
const MAX_REMOTE_READ_RESPONSE_BYTES: usize = MAX_BODY_BYTES;
const REMOTE_READ_COLLECTION_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;
const REMOTE_READ_RESPONSE_DIAGNOSTIC_ENVELOPE_BYTES: u64 = 4 * 1024;
const MAX_REMOTE_READ_DIAGNOSTIC_BYTES: usize = tsink::MAX_QUERY_REGEX_DIAGNOSTIC_BYTES;

fn saturating_u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn modeled_vec_capacity_bytes<T>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    saturating_u64_from_usize(capacity)
        .saturating_mul(saturating_u64_from_usize(std::mem::size_of::<T>()))
        .saturating_add(REMOTE_READ_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_string_capacity_bytes(value: &String) -> u64 {
    if value.capacity() == 0 {
        0
    } else {
        saturating_u64_from_usize(value.capacity())
            .saturating_add(REMOTE_READ_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_owned_str_bytes(value: &str) -> u64 {
    if value.is_empty() {
        0
    } else {
        saturating_u64_from_usize(value.len())
            .saturating_add(REMOTE_READ_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn resize_remote_read_memory(
    reservation: &mut tsink::QueryMemoryReservation,
    bytes: u64,
    context: &'static str,
) -> Result<(), RemoteReadQueryError> {
    reservation.resize(bytes).map_err(|error| match error {
        tsink::QueryBudgetError::LimitExceeded(_)
        | tsink::QueryBudgetError::InvalidLimits(_)
        | tsink::QueryBudgetError::Cancelled
        | tsink::QueryBudgetError::DeadlineExceeded => RemoteReadQueryError::Budget(error),
        _ => RemoteReadQueryError::Internal(format!("{context}: {error}")),
    })
}

struct EncodedRemoteReadResponse {
    raw: Vec<u8>,
    _reservation: Option<tsink::QueryMemoryReservation>,
}

struct CompressedRemoteReadResponse {
    body: Vec<u8>,
    reservation: tsink::QueryMemoryReservation,
}

struct RemoteReadResponseEncoder {
    raw: Vec<u8>,
    result_units: usize,
    max_encoded_bytes: usize,
    reservation: Option<tsink::QueryMemoryReservation>,
}

impl RemoteReadResponseEncoder {
    fn new(execution: &tsink::QueryExecution) -> Result<Self, RemoteReadQueryError> {
        Self::with_execution_limit(MAX_REMOTE_READ_RESPONSE_BYTES, execution)
    }

    #[cfg(test)]
    fn with_limit(max_encoded_bytes: usize) -> Self {
        Self {
            raw: Vec::new(),
            result_units: 0,
            max_encoded_bytes,
            reservation: None,
        }
    }

    fn with_execution_limit(
        max_encoded_bytes: usize,
        execution: &tsink::QueryExecution,
    ) -> Result<Self, RemoteReadQueryError> {
        Ok(Self {
            raw: Vec::new(),
            result_units: 0,
            max_encoded_bytes,
            reservation: Some(
                execution
                    .reserve_memory(0)
                    .map_err(RemoteReadQueryError::Budget)?,
            ),
        })
    }

    fn append(
        &mut self,
        result: QueryResult,
        execution: &tsink::QueryExecution,
    ) -> Result<(), RemoteReadQueryError> {
        execution
            .checkpoint()
            .map_err(RemoteReadQueryError::Budget)?;
        let message_bytes = result.encoded_len();
        let frame_bytes = 1usize
            .checked_add(prost::length_delimiter_len(message_bytes))
            .and_then(|bytes| bytes.checked_add(message_bytes))
            .ok_or_else(|| self.returned_bytes_error(usize::MAX))?;
        let next_bytes = self
            .raw
            .len()
            .checked_add(frame_bytes)
            .ok_or_else(|| self.returned_bytes_error(frame_bytes))?;
        if next_bytes > self.max_encoded_bytes {
            return Err(self.returned_bytes_error(frame_bytes));
        }

        let previous_reservation = self
            .reservation
            .as_ref()
            .map_or(0, tsink::QueryMemoryReservation::bytes);
        if let Some(reservation) = &mut self.reservation {
            resize_remote_read_memory(
                reservation,
                modeled_vec_capacity_bytes::<u8>(next_bytes),
                "failed to reserve remote-read response memory",
            )?;
        }
        if let Err(error) =
            execution.charge_returned_bytes(u64::try_from(frame_bytes).unwrap_or(u64::MAX))
        {
            if let Some(reservation) = &mut self.reservation {
                let _ = reservation.resize(previous_reservation);
            }
            return Err(RemoteReadQueryError::storage(
                "remote-read response exceeded the query budget",
                error.into(),
            ));
        }

        if let Err(error) = self.raw.try_reserve_exact(frame_bytes) {
            if let Some(reservation) = &mut self.reservation {
                let _ = reservation.resize(previous_reservation);
            }
            return Err(RemoteReadQueryError::Internal(format!(
                "failed to reserve remote-read response buffer: {error}"
            )));
        }
        self.raw.push(0x0a);
        if let Err(error) = result.encode_length_delimited(&mut self.raw) {
            return Err(RemoteReadQueryError::Internal(format!(
                "failed to encode remote-read query result: {error}"
            )));
        }
        if let Some(reservation) = &mut self.reservation {
            resize_remote_read_memory(
                reservation,
                modeled_vec_capacity_bytes::<u8>(self.raw.capacity()),
                "failed to reconcile remote-read response memory",
            )?;
        }
        self.result_units = self
            .result_units
            .saturating_add(remote_read_query_units(std::slice::from_ref(&result)));
        Ok(())
    }

    fn returned_bytes_error(&self, requested: usize) -> RemoteReadQueryError {
        RemoteReadQueryError::Budget(tsink::QueryBudgetError::LimitExceeded(
            tsink::QueryLimitExceeded::new(
                tsink::QueryLimitReason::ReturnedBytes,
                u64::try_from(self.max_encoded_bytes).unwrap_or(u64::MAX),
                u64::try_from(self.raw.len()).unwrap_or(u64::MAX),
                u64::try_from(requested).unwrap_or(u64::MAX),
            ),
        ))
    }

    fn finish(self) -> (EncodedRemoteReadResponse, usize) {
        (
            EncodedRemoteReadResponse {
                raw: self.raw,
                _reservation: self.reservation,
            },
            self.result_units,
        )
    }
}

fn remote_read_query_work_limits() -> tsink::QueryWorkLimits {
    tsink::QueryWorkLimits {
        max_returned_bytes: Some(u64::try_from(MAX_REMOTE_READ_RESPONSE_BYTES).unwrap_or(u64::MAX)),
        ..tsink::QueryWorkLimits::default()
    }
}

struct RemoteReadCancellationGuard {
    token: tsink::QueryCancellationToken,
}

impl Drop for RemoteReadCancellationGuard {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

fn begin_remote_read_execution(
    storage: &Arc<dyn Storage>,
) -> Result<(tsink::QueryExecution, RemoteReadCancellationGuard), RemoteReadQueryError> {
    let cancellation = tsink::QueryCancellationToken::new();
    let execution = storage
        .begin_query_execution(remote_read_query_work_limits(), cancellation.clone())
        .map_err(|error| RemoteReadQueryError::storage("failed to begin query execution", error))?
        .ok_or_else(|| {
            RemoteReadQueryError::Internal(
                "remote-read requires an execution-accounted storage backend".to_string(),
            )
        })?;
    Ok((
        execution,
        RemoteReadCancellationGuard {
            token: cancellation,
        },
    ))
}

fn compress_remote_read_response(
    encoded: EncodedRemoteReadResponse,
    execution: &tsink::QueryExecution,
) -> Result<CompressedRemoteReadResponse, RemoteReadQueryError> {
    execution
        .checkpoint()
        .map_err(RemoteReadQueryError::Budget)?;
    let max_compressed_bytes = max_compress_len(encoded.raw.len());
    if max_compressed_bytes == 0 {
        return Err(RemoteReadQueryError::Internal(
            "remote-read response is too large for Snappy".to_string(),
        ));
    }

    let mut compression_reservation = execution
        .reserve_memory(modeled_vec_capacity_bytes::<u8>(max_compressed_bytes))
        .map_err(RemoteReadQueryError::Budget)?;
    let mut compressed = Vec::new();
    compressed
        .try_reserve_exact(max_compressed_bytes)
        .map_err(|error| {
            RemoteReadQueryError::Internal(format!(
                "failed to reserve Snappy response buffer: {error}"
            ))
        })?;
    compressed.resize(max_compressed_bytes, 0);
    let compressed_bytes = SnappyEncoder::new()
        .compress(&encoded.raw, &mut compressed)
        .map_err(|error| {
            RemoteReadQueryError::Internal(format!("snappy encode failed: {error}"))
        })?;
    compressed.truncate(compressed_bytes);
    resize_remote_read_memory(
        &mut compression_reservation,
        modeled_vec_capacity_bytes::<u8>(compressed.capacity()),
        "failed to reconcile Snappy response memory",
    )?;
    execution
        .checkpoint()
        .map_err(RemoteReadQueryError::Budget)?;
    Ok(CompressedRemoteReadResponse {
        body: compressed,
        reservation: compression_reservation,
    })
}

fn protobuf_growth_capacity_upper(len: usize) -> usize {
    if len == 0 {
        0
    } else if len <= 4 {
        4
    } else {
        len.checked_next_power_of_two().unwrap_or(usize::MAX)
    }
}

fn read_protobuf_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let mut value = 0u64;
    for shift in (0..=63).step_by(7) {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| "truncated protobuf varint".to_string())?;
        *cursor = cursor.saturating_add(1);
        if shift == 63 && byte > 1 {
            return Err("protobuf varint overflow".to_string());
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("protobuf varint overflow".to_string())
}

fn read_protobuf_key(bytes: &[u8], cursor: &mut usize) -> Result<(u32, u8), String> {
    let key = read_protobuf_varint(bytes, cursor)?;
    let field = u32::try_from(key >> 3).unwrap_or(u32::MAX);
    if field == 0 {
        return Err("protobuf field number zero is invalid".to_string());
    }
    Ok((field, u8::try_from(key & 0x07).unwrap_or(u8::MAX)))
}

fn read_length_delimited<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], String> {
    let len = usize::try_from(read_protobuf_varint(bytes, cursor)?)
        .map_err(|_| "protobuf length exceeds this platform".to_string())?;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| "protobuf length overflow".to_string())?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| "truncated length-delimited protobuf field".to_string())?;
    *cursor = end;
    Ok(value)
}

fn skip_protobuf_field(bytes: &[u8], cursor: &mut usize, wire_type: u8) -> Result<(), String> {
    match wire_type {
        0 => {
            read_protobuf_varint(bytes, cursor)?;
            Ok(())
        }
        1 => {
            *cursor = cursor
                .checked_add(8)
                .filter(|end| *end <= bytes.len())
                .ok_or_else(|| "truncated fixed64 protobuf field".to_string())?;
            Ok(())
        }
        2 => {
            let _ = read_length_delimited(bytes, cursor)?;
            Ok(())
        }
        5 => {
            *cursor = cursor
                .checked_add(4)
                .filter(|end| *end <= bytes.len())
                .ok_or_else(|| "truncated fixed32 protobuf field".to_string())?;
            Ok(())
        }
        _ => Err(format!("unsupported protobuf wire type {wire_type}")),
    }
}

#[derive(Default)]
struct RemoteReadDecodeModel {
    query_count: usize,
    accepted_response_count: usize,
    query_heap_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteReadMatcherShapeError {
    TooManyMatchers {
        query_index: usize,
        actual: usize,
    },
    NameTooLong {
        query_index: usize,
        matcher_index: usize,
        actual: usize,
    },
    ValueTooLong {
        query_index: usize,
        matcher_index: usize,
        actual: usize,
    },
    TotalTooLong {
        query_index: usize,
        actual: usize,
    },
}

impl std::fmt::Display for RemoteReadMatcherShapeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyMatchers {
                query_index,
                actual,
            } => write!(
                formatter,
                "remote-read query index {query_index} has {actual} matchers, exceeding the hard limit {}",
                tsink::MAX_SERIES_SELECTION_MATCHERS
            ),
            Self::NameTooLong {
                query_index,
                matcher_index,
                actual,
            } => write!(
                formatter,
                "remote-read query index {query_index} matcher index {matcher_index} has a {actual}-byte name, exceeding the hard limit {}",
                tsink::MAX_SERIES_MATCHER_NAME_BYTES
            ),
            Self::ValueTooLong {
                query_index,
                matcher_index,
                actual,
            } => write!(
                formatter,
                "remote-read query index {query_index} matcher index {matcher_index} has a {actual}-byte value, exceeding the hard limit {}",
                tsink::MAX_SERIES_MATCHER_VALUE_BYTES
            ),
            Self::TotalTooLong {
                query_index,
                actual,
            } => write!(
                formatter,
                "remote-read query index {query_index} matcher names and values total {actual} bytes, exceeding the hard limit {}",
                tsink::MAX_SERIES_SELECTION_MATCHER_BYTES
            ),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RemoteReadDecodeScanError {
    Malformed(String),
    MatcherShape(RemoteReadMatcherShapeError),
}

impl From<String> for RemoteReadDecodeScanError {
    fn from(error: String) -> Self {
        Self::Malformed(error)
    }
}

fn scan_label_matcher_heap_bytes(
    bytes: &[u8],
    query_index: usize,
    matcher_index: usize,
    matcher_bytes: &mut usize,
) -> Result<u64, RemoteReadDecodeScanError> {
    let mut cursor = 0usize;
    let mut heap_bytes = 0u64;
    while cursor < bytes.len() {
        let (field, wire_type) = read_protobuf_key(bytes, &mut cursor)?;
        if matches!(field, 2 | 3) && wire_type == 2 {
            let value = read_length_delimited(bytes, &mut cursor)?;
            let value_len = value.len();
            if field == 2 && value_len > tsink::MAX_SERIES_MATCHER_NAME_BYTES {
                return Err(RemoteReadDecodeScanError::MatcherShape(
                    RemoteReadMatcherShapeError::NameTooLong {
                        query_index,
                        matcher_index,
                        actual: value_len,
                    },
                ));
            }
            if field == 3 && value_len > tsink::MAX_SERIES_MATCHER_VALUE_BYTES {
                return Err(RemoteReadDecodeScanError::MatcherShape(
                    RemoteReadMatcherShapeError::ValueTooLong {
                        query_index,
                        matcher_index,
                        actual: value_len,
                    },
                ));
            }
            *matcher_bytes = matcher_bytes.saturating_add(value_len);
            if *matcher_bytes > tsink::MAX_SERIES_SELECTION_MATCHER_BYTES {
                return Err(RemoteReadDecodeScanError::MatcherShape(
                    RemoteReadMatcherShapeError::TotalTooLong {
                        query_index,
                        actual: *matcher_bytes,
                    },
                ));
            }
            heap_bytes = heap_bytes.saturating_add(modeled_owned_str_bytes(
                std::str::from_utf8(value)
                    .map_err(|_| "protobuf matcher string is not valid UTF-8".to_string())?,
            ));
        } else {
            skip_protobuf_field(bytes, &mut cursor, wire_type)?;
        }
    }
    Ok(heap_bytes)
}

fn scan_read_hints_heap_bytes(bytes: &[u8]) -> Result<u64, String> {
    let mut cursor = 0usize;
    let mut heap_bytes = 0u64;
    let mut grouping_count = 0usize;
    while cursor < bytes.len() {
        let (field, wire_type) = read_protobuf_key(bytes, &mut cursor)?;
        if matches!(field, 2 | 5) && wire_type == 2 {
            let value = read_length_delimited(bytes, &mut cursor)?;
            let text = std::str::from_utf8(value)
                .map_err(|_| "protobuf read-hint string is not valid UTF-8".to_string())?;
            heap_bytes = heap_bytes.saturating_add(modeled_owned_str_bytes(text));
            if field == 5 {
                grouping_count = grouping_count.saturating_add(1);
            }
        } else {
            skip_protobuf_field(bytes, &mut cursor, wire_type)?;
        }
    }
    Ok(
        heap_bytes.saturating_add(modeled_vec_capacity_bytes::<String>(
            protobuf_growth_capacity_upper(grouping_count),
        )),
    )
}

fn scan_remote_read_query_heap_bytes(
    bytes: &[u8],
    query_index: usize,
) -> Result<u64, RemoteReadDecodeScanError> {
    let mut cursor = 0usize;
    let mut matcher_count = 0usize;
    let mut matcher_bytes = 0usize;
    let mut heap_bytes = 0u64;
    while cursor < bytes.len() {
        let (field, wire_type) = read_protobuf_key(bytes, &mut cursor)?;
        match (field, wire_type) {
            (3, 2) => {
                let matcher = read_length_delimited(bytes, &mut cursor)?;
                matcher_count = matcher_count.saturating_add(1);
                if matcher_count > tsink::MAX_SERIES_SELECTION_MATCHERS {
                    return Err(RemoteReadDecodeScanError::MatcherShape(
                        RemoteReadMatcherShapeError::TooManyMatchers {
                            query_index,
                            actual: matcher_count,
                        },
                    ));
                }
                heap_bytes = heap_bytes.saturating_add(scan_label_matcher_heap_bytes(
                    matcher,
                    query_index,
                    matcher_count.saturating_sub(1),
                    &mut matcher_bytes,
                )?);
            }
            (4, 2) => {
                let hints = read_length_delimited(bytes, &mut cursor)?;
                heap_bytes = heap_bytes.saturating_add(
                    scan_read_hints_heap_bytes(hints)
                        .map_err(RemoteReadDecodeScanError::Malformed)?,
                );
            }
            _ => skip_protobuf_field(bytes, &mut cursor, wire_type)?,
        }
    }
    Ok(
        heap_bytes.saturating_add(modeled_vec_capacity_bytes::<LabelMatcher>(
            protobuf_growth_capacity_upper(matcher_count),
        )),
    )
}

fn scan_remote_read_decode_model(bytes: &[u8]) -> Result<u64, RemoteReadDecodeScanError> {
    let mut cursor = 0usize;
    let mut model = RemoteReadDecodeModel::default();
    while cursor < bytes.len() {
        let (field, wire_type) = read_protobuf_key(bytes, &mut cursor)?;
        match (field, wire_type) {
            (1, 2) => {
                let query = read_length_delimited(bytes, &mut cursor)?;
                let query_index = model.query_count;
                model.query_count = model.query_count.saturating_add(1);
                model.query_heap_bytes = model
                    .query_heap_bytes
                    .saturating_add(scan_remote_read_query_heap_bytes(query, query_index)?);
            }
            (2, 0) => {
                read_protobuf_varint(bytes, &mut cursor)?;
                model.accepted_response_count = model.accepted_response_count.saturating_add(1);
            }
            (2, 2) => {
                let packed = read_length_delimited(bytes, &mut cursor)?;
                let mut packed_cursor = 0usize;
                while packed_cursor < packed.len() {
                    read_protobuf_varint(packed, &mut packed_cursor)?;
                    model.accepted_response_count = model.accepted_response_count.saturating_add(1);
                }
            }
            _ => skip_protobuf_field(bytes, &mut cursor, wire_type)?,
        }
    }

    Ok(model
        .query_heap_bytes
        .saturating_add(modeled_vec_capacity_bytes::<Query>(
            protobuf_growth_capacity_upper(model.query_count),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<i32>(
            protobuf_growth_capacity_upper(model.accepted_response_count),
        )))
}

fn modeled_label_matcher_heap_bytes(matcher: &LabelMatcher) -> u64 {
    modeled_string_capacity_bytes(&matcher.name)
        .saturating_add(modeled_string_capacity_bytes(&matcher.value))
}

fn modeled_read_hints_heap_bytes(hints: &crate::prom_remote::ReadHints) -> u64 {
    modeled_string_capacity_bytes(&hints.func)
        .saturating_add(modeled_vec_capacity_bytes::<String>(
            hints.grouping.capacity(),
        ))
        .saturating_add(hints.grouping.iter().fold(0u64, |bytes, value| {
            bytes.saturating_add(modeled_string_capacity_bytes(value))
        }))
}

fn modeled_remote_read_request_heap_bytes(request: &ReadRequest) -> u64 {
    modeled_vec_capacity_bytes::<Query>(request.queries.capacity())
        .saturating_add(modeled_vec_capacity_bytes::<i32>(
            request.accepted_response_types.capacity(),
        ))
        .saturating_add(request.queries.iter().fold(0u64, |bytes, query| {
            bytes
                .saturating_add(modeled_vec_capacity_bytes::<LabelMatcher>(
                    query.matchers.capacity(),
                ))
                .saturating_add(query.matchers.iter().fold(0u64, |bytes, matcher| {
                    bytes.saturating_add(modeled_label_matcher_heap_bytes(matcher))
                }))
                .saturating_add(
                    query
                        .hints
                        .as_ref()
                        .map_or(0, modeled_read_hints_heap_bytes),
                )
        }))
}

fn decode_remote_read_request(
    request: &HttpRequest,
    execution: &tsink::QueryExecution,
) -> Result<(ReadRequest, tsink::QueryMemoryReservation), RemoteReadQueryError> {
    execution
        .checkpoint()
        .map_err(RemoteReadQueryError::Budget)?;

    let mut decoded_reservation = None;
    let mut decoded_body = None;
    let bytes = match request.header("content-encoding") {
        None => {
            if request.body.len() > MAX_BODY_BYTES {
                return Err(RemoteReadQueryError::InvalidRequest(format!(
                    "decoded request body too large: {} bytes (max {MAX_BODY_BYTES})",
                    request.body.len()
                )));
            }
            request.body.as_slice()
        }
        Some(encoding) if encoding.eq_ignore_ascii_case("identity") => {
            if request.body.len() > MAX_BODY_BYTES {
                return Err(RemoteReadQueryError::InvalidRequest(format!(
                    "decoded request body too large: {} bytes (max {MAX_BODY_BYTES})",
                    request.body.len()
                )));
            }
            request.body.as_slice()
        }
        Some(encoding) if encoding.eq_ignore_ascii_case("snappy") => {
            let decoded_len = decompress_len(&request.body).map_err(|error| {
                RemoteReadQueryError::InvalidRequest(format!("snappy decode failed: {error}"))
            })?;
            if decoded_len > MAX_BODY_BYTES {
                return Err(RemoteReadQueryError::InvalidRequest(format!(
                    "decoded request body too large: {decoded_len} bytes (max {MAX_BODY_BYTES})"
                )));
            }
            let mut reservation = execution
                .reserve_memory(modeled_vec_capacity_bytes::<u8>(decoded_len))
                .map_err(RemoteReadQueryError::Budget)?;
            let decoded = SnappyDecoder::new()
                .decompress_vec(&request.body)
                .map_err(|error| {
                    RemoteReadQueryError::InvalidRequest(format!("snappy decode failed: {error}"))
                })?;
            if decoded.len() > MAX_BODY_BYTES {
                return Err(RemoteReadQueryError::InvalidRequest(format!(
                    "decoded request body too large: {} bytes (max {MAX_BODY_BYTES})",
                    decoded.len()
                )));
            }
            resize_remote_read_memory(
                &mut reservation,
                modeled_vec_capacity_bytes::<u8>(decoded.capacity()),
                "failed to reconcile decoded remote-read request memory",
            )?;
            decoded_reservation = Some(reservation);
            decoded_body = Some(decoded);
            decoded_body
                .as_deref()
                .expect("decoded Snappy body was just installed")
        }
        Some(encoding) => {
            let _ = encoding;
            return Err(RemoteReadQueryError::InvalidRequest(
                "unsupported remote-read content-encoding".to_string(),
            ));
        }
    };

    let decode_heap_upper = scan_remote_read_decode_model(bytes).map_err(|error| match error {
        RemoteReadDecodeScanError::Malformed(error) => {
            RemoteReadQueryError::InvalidRequest(format!("invalid protobuf body: {error}"))
        }
        RemoteReadDecodeScanError::MatcherShape(error) => RemoteReadQueryError::InvalidRequest(
            format!("invalid remote-read matcher shape: {error}"),
        ),
    })?;
    let mut request_reservation = execution
        .reserve_memory(decode_heap_upper)
        .map_err(RemoteReadQueryError::Budget)?;
    let read_request = ReadRequest::decode(bytes).map_err(|error| {
        RemoteReadQueryError::InvalidRequest(format!("invalid protobuf body: {error}"))
    })?;
    resize_remote_read_memory(
        &mut request_reservation,
        modeled_remote_read_request_heap_bytes(&read_request),
        "failed to reconcile decoded remote-read protobuf memory",
    )?;
    drop(decoded_body);
    drop(decoded_reservation);
    execution
        .checkpoint()
        .map_err(RemoteReadQueryError::Budget)?;
    Ok((read_request, request_reservation))
}

async fn fanout_select_series_union(
    storage: &Arc<dyn Storage>,
    cluster_context: &ClusterRequestContext,
    read_fanout: &ReadFanoutExecutor,
    selections: &[SeriesSelection],
    ring_version: u64,
    execution: &tsink::QueryExecution,
) -> Result<ReadFanoutResponse<GuardedMetricSeries>, ReadFanoutError> {
    let mut metadata = default_read_response_metadata(read_fanout);
    let mut merged = BTreeSet::new();
    let mut retained_bytes = 0u64;
    let mut metadata_bytes = 0u64;
    let mut merged_reservation = execution
        .reserve_memory(0)
        .map_err(|error| ReadFanoutError::QueryBudget { error })?;

    for selection in selections {
        execution
            .checkpoint()
            .map_err(|error| ReadFanoutError::QueryBudget { error })?;
        let mut response = read_fanout
            .select_series_with_ring_version_detailed_accounted_with_execution(
                storage,
                &cluster_context.rpc_client,
                selection,
                ring_version,
                execution,
            )
            .await?;
        merged_reservation
            .resize(
                retained_bytes
                    .saturating_add(metadata_bytes)
                    .saturating_add(modeled_read_metadata_heap_bytes(&response.metadata)),
            )
            .map_err(|error| ReadFanoutError::QueryBudget { error })?;
        merge_read_response_metadata(&mut metadata, &response.metadata);
        metadata_bytes = modeled_read_metadata_heap_bytes(&metadata);
        merged_reservation
            .resize(retained_bytes.saturating_add(metadata_bytes))
            .map_err(|error| ReadFanoutError::QueryBudget { error })?;
        let source_reservation = response.value.take_reservation().ok_or_else(|| {
            ReadFanoutError::InvalidRequest {
                message: "completely accounted distributed remote-read metadata omitted its result reservation"
                    .to_string(),
            }
        })?;
        if !response.value.series.is_empty() && source_reservation.bytes() == 0 {
            return Err(ReadFanoutError::InvalidRequest {
                message: "completely accounted distributed remote-read metadata retained zero bytes for a non-empty result"
                    .to_string(),
            });
        }
        for series in std::mem::take(&mut response.value.series) {
            execution
                .checkpoint()
                .map_err(|error| ReadFanoutError::QueryBudget { error })?;
            if merged.contains(&series) {
                continue;
            }
            let next_bytes =
                retained_bytes.saturating_add(modeled_metric_series_set_entry_bytes(&series));
            merged_reservation
                .resize(next_bytes.saturating_add(metadata_bytes))
                .map_err(|error| ReadFanoutError::QueryBudget { error })?;
            if merged.insert(series) {
                retained_bytes = next_bytes;
            }
        }
        drop(source_reservation);
    }

    let vector_slots = modeled_vec_capacity_bytes::<MetricSeries>(merged.len());
    merged_reservation
        .resize(
            retained_bytes
                .saturating_add(vector_slots)
                .saturating_add(metadata_bytes),
        )
        .map_err(|error| ReadFanoutError::QueryBudget { error })?;
    let series = merged.into_iter().collect::<Vec<_>>();
    merged_reservation
        .resize(modeled_metric_series_vec_retained_bytes(&series).saturating_add(metadata_bytes))
        .map_err(|error| ReadFanoutError::QueryBudget { error })?;
    Ok(ReadFanoutResponse {
        value: GuardedMetricSeries {
            series,
            _reservation: merged_reservation,
        },
        metadata,
        _reservation: None,
    })
}

fn modeled_label_heap_retained_bytes(label: &Label) -> u64 {
    modeled_string_capacity_bytes(&label.name)
        .saturating_add(modeled_string_capacity_bytes(&label.value))
}

fn modeled_metric_series_heap_retained_bytes(series: &MetricSeries) -> u64 {
    modeled_string_capacity_bytes(&series.name)
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

fn modeled_metric_series_set_entry_bytes(series: &MetricSeries) -> u64 {
    saturating_u64_from_usize(std::mem::size_of::<MetricSeries>())
        .saturating_add(REMOTE_READ_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
        .saturating_add(modeled_metric_series_heap_retained_bytes(series))
}

fn modeled_read_metadata_heap_bytes(metadata: &ReadFanoutResponseMetadata) -> u64 {
    modeled_vec_capacity_bytes::<String>(metadata.warnings.capacity()).saturating_add(
        metadata.warnings.iter().fold(0u64, |bytes, warning| {
            bytes.saturating_add(modeled_string_capacity_bytes(warning))
        }),
    )
}

struct GuardedMetricSeries {
    series: Vec<MetricSeries>,
    _reservation: tsink::QueryMemoryReservation,
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
    let (execution, _cancellation_guard) = match begin_remote_read_execution(storage) {
        Ok(admission) => admission,
        Err(error) => return remote_read_query_error_response(error),
    };
    // Keep a small, fixed diagnostic/body/header envelope live for every execution. Any error
    // response is copied while this guard remains held, and the calibrated success envelope also
    // includes the same deterministic reservation.
    let _response_diagnostic_reservation =
        match execution.reserve_memory(REMOTE_READ_RESPONSE_DIAGNOSTIC_ENVELOPE_BYTES) {
            Ok(reservation) => reservation,
            Err(error) => return remote_read_query_budget_error_response(&error),
        };
    let (read_req, _request_reservation) = match decode_remote_read_request(request, &execution) {
        Ok(request) => request,
        Err(error) => return remote_read_query_error_response(error),
    };
    if let Err(error) = execution.checkpoint() {
        return remote_read_query_budget_error_response(&error);
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
    let _read_admission = match read_admission.admit_request(read_query_count.max(1)).await {
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
    let mut _distributed_metadata_reservation: Option<tsink::QueryMemoryReservation> = None;
    let (raw, result_units) = if let Some(cluster_context) =
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
        let mut read_metadata_reservation = match execution.reserve_memory(0) {
            Ok(reservation) => reservation,
            Err(error) => return remote_read_query_budget_error_response(&error),
        };
        let mut response = match RemoteReadResponseEncoder::new(&execution) {
            Ok(response) => response,
            Err(error) => return remote_read_query_error_response(error),
        };
        for query in &read_req.queries {
            match execute_query_distributed(
                storage,
                cluster_context,
                &read_fanout,
                query,
                &tenant_id,
                &execution,
            )
            .await
            {
                Ok(result) => {
                    if let Err(error) = read_metadata_reservation.resize(
                        modeled_read_metadata_heap_bytes(&read_metadata)
                            .saturating_add(modeled_read_metadata_heap_bytes(&result.metadata)),
                    ) {
                        return remote_read_query_budget_error_response(&error);
                    }
                    merge_read_response_metadata(&mut read_metadata, &result.metadata);
                    if let Err(error) = read_metadata_reservation
                        .resize(modeled_read_metadata_heap_bytes(&read_metadata))
                    {
                        return remote_read_query_budget_error_response(&error);
                    }
                    if let Err(error) = result.result.append_to_response(&mut response, &execution)
                    {
                        return remote_read_query_error_response(error);
                    }
                }
                Err(ReadFanoutError::QueryBudget { error }) => {
                    return remote_read_query_budget_error_response(&error)
                }
                Err(err) => return remote_read_fanout_error_response(err),
            }
        }
        distributed_metadata = Some(read_metadata);
        _distributed_metadata_reservation = Some(read_metadata_reservation);
        response.finish()
    } else {
        let storage = tenant::scoped_storage(Arc::clone(storage), tenant_id.clone());
        let execution_for_task = execution.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut response = RemoteReadResponseEncoder::new(&execution_for_task)?;
            for query in &read_req.queries {
                let result = execute_query(&storage, query, &execution_for_task)?;
                result
                    .result
                    .append_to_response(&mut response, &execution_for_task)?;
            }
            Ok::<_, RemoteReadQueryError>(response.finish())
        })
        .await;

        match result {
            Ok(Ok(r)) => r,
            Ok(Err(err)) => return remote_read_query_error_response(err),
            Err(err) => return text_response(500, &format!("read task failed: {err}")),
        }
    };

    let compressed = match compress_remote_read_response(raw, &execution) {
        Ok(compressed) => compressed,
        Err(error) => return remote_read_query_error_response(error),
    };

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
    )
    .await;

    let CompressedRemoteReadResponse {
        body,
        reservation: response_body_reservation,
    } = compressed;
    let mut response = HttpResponse::new(200, body)
        .with_header("Content-Type", "application/x-protobuf")
        .with_header("Content-Encoding", "snappy")
        .with_header("X-Prometheus-Remote-Read-Version", "0.1.0");
    if let Some(metadata) = distributed_metadata.as_ref() {
        response = with_read_metadata_headers(response, metadata);
    }
    // The raw/protobuf and compressed buffers overlap under query-memory reservations until this
    // point. `HttpResponse` now owns the compressed body for transport, so its reservation is
    // released at the explicit adapter-to-transport handoff.
    drop(response_body_reservation);
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

#[derive(Debug)]
enum RemoteReadQueryError {
    InvalidRequest(String),
    Budget(tsink::QueryBudgetError),
    Internal(String),
    Storage {
        context: &'static str,
        error: tsink::TsinkError,
    },
}

impl RemoteReadQueryError {
    fn storage(context: &'static str, error: tsink::TsinkError) -> Self {
        Self::Storage { context, error }
    }
}

fn bounded_remote_read_diagnostic(mut diagnostic: String) -> String {
    if diagnostic.len() <= MAX_REMOTE_READ_DIAGNOSTIC_BYTES {
        return diagnostic;
    }
    let mut end = MAX_REMOTE_READ_DIAGNOSTIC_BYTES;
    while !diagnostic.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    diagnostic.truncate(end);
    diagnostic
}

fn remote_read_query_error_response(error: RemoteReadQueryError) -> HttpResponse {
    match error {
        RemoteReadQueryError::InvalidRequest(message) => {
            let message = bounded_remote_read_diagnostic(message);
            text_response(400, &message)
        }
        RemoteReadQueryError::Budget(error) => remote_read_query_budget_error_response(&error),
        RemoteReadQueryError::Internal(message) => {
            let message = bounded_remote_read_diagnostic(message);
            text_response(500, &message)
        }
        RemoteReadQueryError::Storage {
            error: tsink::TsinkError::QueryBudget(error),
            ..
        } => remote_read_query_budget_error_response(&error),
        RemoteReadQueryError::Storage { context, error } => {
            let status = remote_read_storage_error_status(&error);
            let classification = match status {
                400 => "invalid remote-read query",
                503 => "remote-read storage unavailable",
                _ => "remote-read storage failure",
            };
            text_response(status, &format!("{context}: {classification}"))
        }
    }
}

fn remote_read_fanout_error_response(error: ReadFanoutError) -> HttpResponse {
    let (status, error_code, retry_after_seconds, diagnostic) = match &error {
        ReadFanoutError::InvalidRequest { .. } => (400, None, None, "invalid remote-read query"),
        ReadFanoutError::MergeLimitExceeded { .. } => (
            413,
            Some("read_merge_limit_exceeded"),
            None,
            "remote-read merge limit exceeded",
        ),
        ReadFanoutError::ResourceLimitExceeded {
            retryable: true, ..
        } => (
            429,
            Some("read_overloaded"),
            Some("1"),
            "remote-read service is overloaded",
        ),
        ReadFanoutError::ResourceLimitExceeded { .. } => (
            413,
            Some("read_resource_limit_exceeded"),
            None,
            "remote-read resource limit exceeded",
        ),
        ReadFanoutError::ConsistencyUnmet {
            mode: ClusterReadConsistency::Strict,
            ..
        } => (
            409,
            Some("strict_consistency_unmet"),
            None,
            "strict remote-read consistency was not met",
        ),
        ReadFanoutError::ConsistencyUnmet { .. } => (
            503,
            Some("read_consistency_unmet"),
            None,
            "remote-read consistency was not met",
        ),
        _ if error.retryable() => (503, None, None, "remote-read service is unavailable"),
        _ => (500, None, None, "remote-read failed"),
    };
    let mut response = text_response(status, diagnostic);
    if let Some(error_code) = error_code {
        response = response.with_header(READ_ERROR_CODE_HEADER, error_code);
    }
    if let Some(retry_after) = retry_after_seconds {
        response = response.with_header("Retry-After", retry_after);
    }
    response
}

fn remote_read_storage_error_status(error: &tsink::TsinkError) -> u16 {
    match error {
        tsink::TsinkError::InvalidTimeRange { .. }
        | tsink::TsinkError::MetricRequired
        | tsink::TsinkError::InvalidMetricName(_)
        | tsink::TsinkError::InvalidLabel(_)
        | tsink::TsinkError::InvalidConfiguration(_) => 400,
        tsink::TsinkError::StorageShuttingDown
        | tsink::TsinkError::StorageClosed
        | tsink::TsinkError::LifecycleTimeout { .. }
        | tsink::TsinkError::ChannelTimeout { .. } => 503,
        _ => 500,
    }
}

fn remote_read_query_budget_error_response(error: &tsink::QueryBudgetError) -> HttpResponse {
    let (status, error_code, retryable) = match error {
        tsink::QueryBudgetError::InvalidLimits(_) => {
            (400, "invalid_query_limits".to_string(), false)
        }
        tsink::QueryBudgetError::LimitExceeded(exceeded) => {
            let code = format!("query_limit_{}", exceeded.reason.as_str());
            let retryable = matches!(
                exceeded.reason,
                tsink::QueryLimitReason::ConcurrentQueries
                    | tsink::QueryLimitReason::SharedMemoryBytes
            );
            (if retryable { 429 } else { 413 }, code, retryable)
        }
        tsink::QueryBudgetError::Cancelled => (503, "query_cancelled".to_string(), false),
        tsink::QueryBudgetError::DeadlineExceeded => {
            (503, "query_deadline_exceeded".to_string(), false)
        }
        _ => return text_response(500, "query failed"),
    };

    let diagnostic = bounded_remote_read_diagnostic(error.to_string());
    let mut response =
        text_response(status, &diagnostic).with_header(READ_ERROR_CODE_HEADER, error_code);
    if retryable {
        response = response.with_header("Retry-After", "1");
    }
    response
}

struct LocalRemoteReadQueryResult {
    result: GuardedQueryResult,
}

struct DistributedRemoteReadQueryResult {
    result: GuardedQueryResult,
    metadata: ReadFanoutResponseMetadata,
    _metadata_reservation: tsink::QueryMemoryReservation,
}

fn execute_query(
    storage: &Arc<dyn Storage>,
    query: &Query,
    execution: &tsink::QueryExecution,
) -> Result<LocalRemoteReadQueryResult, RemoteReadQueryError> {
    execution
        .checkpoint()
        .map_err(RemoteReadQueryError::Budget)?;
    let _selection_reservation = execution
        .reserve_memory(modeled_remote_matcher_selection_upper_bytes(
            &query.matchers,
        ))
        .map_err(RemoteReadQueryError::Budget)?;
    let selection = series_selection_from_remote_matchers(&query.matchers)
        .map_err(RemoteReadQueryError::InvalidRequest)?;
    if storage.select_series_execution_accounting() != tsink::QueryExecutionAccounting::Complete {
        return Err(RemoteReadQueryError::storage(
            "failed to resolve candidate series",
            tsink::TsinkError::UnsupportedOperation {
                operation: "bounded local remote-read metadata selection",
                reason: "storage does not provide complete select_series execution accounting"
                    .to_string(),
            },
        ));
    }
    let mut selected = storage
        .select_series_with_execution_result(&selection, execution)
        .map_err(|error| {
            RemoteReadQueryError::storage("failed to resolve candidate series", error)
        })?;
    let series_reservation = selected.take_memory_reservation().ok_or_else(|| {
        RemoteReadQueryError::storage(
            "failed to resolve candidate series",
            tsink::TsinkError::Other(
                "completely accounted local remote-read metadata selection omitted its result reservation"
                    .to_string(),
            ),
        )
    })?;
    if !selected.series.is_empty() && series_reservation.bytes() == 0 {
        return Err(RemoteReadQueryError::storage(
            "failed to resolve candidate series",
            tsink::TsinkError::Other(
                "completely accounted local remote-read metadata selection retained zero bytes for a non-empty result"
                    .to_string(),
            ),
        ));
    }
    if storage.select_many_execution_accounting() != tsink::QueryExecutionAccounting::Complete {
        return Err(RemoteReadQueryError::storage(
            "query failed",
            tsink::TsinkError::UnsupportedOperation {
                operation: "bounded local remote-read point selection",
                reason: "storage does not provide complete select_many execution accounting"
                    .to_string(),
            },
        ));
    }

    let end = query.end_timestamp_ms.saturating_add(1);
    let mut points = storage
        .select_many_with_execution_result(
            &selected.series,
            query.start_timestamp_ms,
            end,
            execution,
        )
        .map_err(|error| RemoteReadQueryError::storage("query failed", error))?;
    if points.series.len() != selected.series.len()
        || points
            .series
            .iter()
            .zip(&selected.series)
            .any(|(item, selector)| item.series != *selector)
    {
        return Err(RemoteReadQueryError::storage(
            "query failed",
            tsink::TsinkError::Other(
                "local remote-read batch result identities or ordering did not match the request"
                    .to_string(),
            ),
        ));
    }
    let matched = points.matched_selectors.take().ok_or_else(|| {
        RemoteReadQueryError::storage(
            "query failed",
            tsink::TsinkError::Other(
                "completely accounted local remote-read point selection omitted selector-existence bits"
                    .to_string(),
            ),
        )
    })?;
    if matched.len() != points.series.len() {
        return Err(RemoteReadQueryError::storage(
            "query failed",
            tsink::TsinkError::Other(format!(
                "local remote-read point selection returned {} existence bits for {} selectors",
                matched.len(),
                points.series.len()
            )),
        ));
    }
    let points_reservation = points.take_memory_reservation().ok_or_else(|| {
        RemoteReadQueryError::storage(
            "query failed",
            tsink::TsinkError::Other(
                "completely accounted local remote-read point selection omitted its result reservation"
                    .to_string(),
            ),
        )
    })?;
    if !points.series.is_empty() && points_reservation.bytes() == 0 {
        return Err(RemoteReadQueryError::storage(
            "query failed",
            tsink::TsinkError::Other(
                "completely accounted local remote-read point selection retained zero bytes for a non-empty result"
                    .to_string(),
            ),
        ));
    }
    drop(series_reservation);
    drop(selected);
    let selected_points = std::mem::take(&mut points.series);
    let result = guarded_prom_query_result(selected_points, Some(matched), None, execution)
        .map_err(RemoteReadQueryError::Budget)?;
    drop(points_reservation);
    Ok(LocalRemoteReadQueryResult { result })
}

async fn execute_query_distributed(
    storage: &Arc<dyn Storage>,
    cluster_context: &ClusterRequestContext,
    read_fanout: &ReadFanoutExecutor,
    query: &Query,
    tenant_id: &str,
    execution: &tsink::QueryExecution,
) -> Result<DistributedRemoteReadQueryResult, ReadFanoutError> {
    execution
        .checkpoint()
        .map_err(|error| ReadFanoutError::QueryBudget { error })?;
    let _selection_reservation = execution
        .reserve_memory(modeled_remote_matcher_selection_upper_bytes(
            &query.matchers,
        ))
        .map_err(|error| ReadFanoutError::QueryBudget { error })?;
    let ring_version = cluster_ring_version(Some(cluster_context));
    let selection = series_selection_from_remote_matchers(&query.matchers).map_err(|err| {
        ReadFanoutError::InvalidRequest {
            message: format!("invalid remote-read matchers: {err}"),
        }
    })?;
    let mut tenant_selections_reservation = execution
        .reserve_memory(modeled_tenant_selections_upper_bytes(&selection, tenant_id))
        .map_err(|error| ReadFanoutError::QueryBudget { error })?;
    let selections = tenant::read_selections_for_tenant(&selection, tenant_id).map_err(|err| {
        ReadFanoutError::InvalidRequest {
            message: format!("invalid remote-read matchers: {err}"),
        }
    })?;
    tenant_selections_reservation
        .resize(modeled_tenant_selections_retained_bytes(&selections))
        .map_err(|error| ReadFanoutError::QueryBudget { error })?;
    let series_response = fanout_select_series_union(
        storage,
        cluster_context,
        read_fanout,
        &selections,
        ring_version,
        execution,
    )
    .await?;
    drop(tenant_selections_reservation);
    let mut metadata_reservation = execution
        .reserve_memory(modeled_read_metadata_heap_bytes(&series_response.metadata))
        .map_err(|error| ReadFanoutError::QueryBudget { error })?;
    let mut read_metadata = series_response.metadata.clone();

    let end = query.end_timestamp_ms.saturating_add(1);
    let mut points_response = read_fanout
        .select_points_for_series_with_ring_version_detailed_accounted_with_execution(
            storage,
            &cluster_context.rpc_client,
            &series_response.value.series,
            query.start_timestamp_ms,
            end,
            ring_version,
            execution,
        )
        .await?;
    metadata_reservation
        .resize(
            modeled_read_metadata_heap_bytes(&read_metadata)
                .saturating_add(modeled_read_metadata_heap_bytes(&points_response.metadata)),
        )
        .map_err(|error| ReadFanoutError::QueryBudget { error })?;
    merge_read_response_metadata(&mut read_metadata, &points_response.metadata);
    metadata_reservation
        .resize(modeled_read_metadata_heap_bytes(&read_metadata))
        .map_err(|error| ReadFanoutError::QueryBudget { error })?;
    if points_response.value.matched.len() != points_response.value.series.len() {
        return Err(ReadFanoutError::InvalidRequest {
            message: format!(
                "distributed remote-read point selection returned {} existence bits for {} selectors",
                points_response.value.matched.len(),
                points_response.value.series.len()
            ),
        });
    }
    let points_reservation = points_response
        .value
        .take_reservation()
        .ok_or_else(|| ReadFanoutError::InvalidRequest {
            message:
                "completely accounted distributed remote-read point selection omitted its result reservation"
                    .to_string(),
        })?;
    if !points_response.value.series.is_empty() && points_reservation.bytes() == 0 {
        return Err(ReadFanoutError::InvalidRequest {
            message:
                "completely accounted distributed remote-read point selection retained zero bytes for a non-empty result"
                    .to_string(),
        });
    }
    let matched = std::mem::take(&mut points_response.value.matched);
    let selected_points = std::mem::take(&mut points_response.value.series);
    let result =
        guarded_prom_query_result(selected_points, Some(matched), Some(tenant_id), execution)
            .map_err(|error| ReadFanoutError::QueryBudget { error })?;
    drop(points_reservation);
    Ok(DistributedRemoteReadQueryResult {
        result,
        metadata: read_metadata,
        _metadata_reservation: metadata_reservation,
    })
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

fn modeled_remote_matcher_selection_upper_bytes(matchers: &[LabelMatcher]) -> u64 {
    let mut bytes =
        modeled_vec_capacity_bytes::<SeriesMatcher>(protobuf_growth_capacity_upper(matchers.len()));
    for matcher in matchers {
        bytes = bytes
            .saturating_add(modeled_owned_str_bytes(&matcher.name))
            .saturating_add(modeled_owned_str_bytes(&matcher.value));
        if matcher.name == "__name__" && matcher.r#type == MatcherType::Eq as i32 {
            bytes = bytes.saturating_add(modeled_owned_str_bytes(&matcher.value));
        }
    }
    bytes
}

fn modeled_series_selection_heap_bytes(selection: &SeriesSelection) -> u64 {
    selection
        .metric
        .as_ref()
        .map_or(0, modeled_string_capacity_bytes)
        .saturating_add(modeled_vec_capacity_bytes::<SeriesMatcher>(
            selection.matchers.capacity(),
        ))
        .saturating_add(selection.matchers.iter().fold(0u64, |bytes, matcher| {
            bytes
                .saturating_add(modeled_string_capacity_bytes(&matcher.name))
                .saturating_add(modeled_string_capacity_bytes(&matcher.value))
        }))
}

fn modeled_tenant_selections_upper_bytes(selection: &SeriesSelection, tenant_id: &str) -> u64 {
    let selection_count = 1 + usize::from(tenant_id == tenant::DEFAULT_TENANT_ID);
    let base_matcher_strings = selection.matchers.iter().fold(0u64, |bytes, matcher| {
        bytes
            .saturating_add(modeled_owned_str_bytes(&matcher.name))
            .saturating_add(modeled_owned_str_bytes(&matcher.value))
    });
    let base_metric = selection
        .metric
        .as_deref()
        .map_or(0, modeled_owned_str_bytes);
    let mut bytes = modeled_vec_capacity_bytes::<SeriesSelection>(selection_count);
    for index in 0..selection_count {
        let tenant_value = if index == 0 { tenant_id } else { ".+" };
        bytes = bytes
            .saturating_add(base_metric)
            .saturating_add(base_matcher_strings)
            .saturating_add(modeled_vec_capacity_bytes::<SeriesMatcher>(
                protobuf_growth_capacity_upper(selection.matchers.len().saturating_add(1)),
            ))
            .saturating_add(modeled_owned_str_bytes(tenant::TENANT_LABEL))
            .saturating_add(modeled_owned_str_bytes(tenant_value));
    }
    bytes
}

fn modeled_tenant_selections_retained_bytes(selections: &Vec<SeriesSelection>) -> u64 {
    modeled_vec_capacity_bytes::<SeriesSelection>(selections.capacity()).saturating_add(
        selections.iter().fold(0u64, |bytes, selection| {
            bytes.saturating_add(modeled_series_selection_heap_bytes(selection))
        }),
    )
}

fn modeled_prom_histogram_transform_upper_bytes(histogram: &tsink::NativeHistogram) -> u64 {
    modeled_vec_capacity_bytes::<BucketSpan>(histogram.negative_spans.len())
        .saturating_add(modeled_vec_capacity_bytes::<i64>(
            histogram.negative_deltas.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<f64>(
            histogram.negative_counts.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<BucketSpan>(
            histogram.positive_spans.len(),
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

fn modeled_prom_transform_upper_bytes(series: &[SeriesPoints]) -> u64 {
    modeled_vec_capacity_bytes::<TimeSeries>(series.len()).saturating_add(series.iter().fold(
        0u64,
        |bytes, item| {
            let sample_count = item
                .points
                .iter()
                .filter(|point| {
                    !matches!(point.value, tsink::Value::Histogram(_))
                        && point.value.as_f64().is_some()
                })
                .count();
            let histogram_count = item
                .points
                .iter()
                .filter(|point| matches!(point.value, tsink::Value::Histogram(_)))
                .count();
            bytes
                .saturating_add(modeled_vec_capacity_bytes::<PromLabel>(
                    item.series.labels.len().saturating_add(1),
                ))
                .saturating_add(modeled_owned_str_bytes("__name__"))
                .saturating_add(modeled_string_capacity_bytes(&item.series.name))
                .saturating_add(item.series.labels.iter().fold(0u64, |bytes, label| {
                    bytes
                        .saturating_add(modeled_string_capacity_bytes(&label.name))
                        .saturating_add(modeled_string_capacity_bytes(&label.value))
                }))
                .saturating_add(modeled_vec_capacity_bytes::<PromSample>(sample_count))
                .saturating_add(modeled_vec_capacity_bytes::<PromHistogram>(histogram_count))
                .saturating_add(item.points.iter().fold(0u64, |bytes, point| {
                    match &point.value {
                        tsink::Value::Histogram(histogram) => bytes.saturating_add(
                            modeled_prom_histogram_transform_upper_bytes(histogram),
                        ),
                        _ => bytes,
                    }
                }))
        },
    ))
}

fn modeled_prom_label_retained_bytes(label: &PromLabel) -> u64 {
    modeled_string_capacity_bytes(&label.name)
        .saturating_add(modeled_string_capacity_bytes(&label.value))
}

fn modeled_prom_histogram_retained_bytes(histogram: &PromHistogram) -> u64 {
    modeled_vec_capacity_bytes::<BucketSpan>(histogram.negative_spans.capacity())
        .saturating_add(modeled_vec_capacity_bytes::<i64>(
            histogram.negative_deltas.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<f64>(
            histogram.negative_counts.capacity(),
        ))
        .saturating_add(modeled_vec_capacity_bytes::<BucketSpan>(
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

fn modeled_prom_query_result_retained_bytes(result: &QueryResult) -> u64 {
    modeled_vec_capacity_bytes::<TimeSeries>(result.timeseries.capacity()).saturating_add(
        result.timeseries.iter().fold(0u64, |bytes, series| {
            bytes
                .saturating_add(modeled_vec_capacity_bytes::<PromLabel>(
                    series.labels.capacity(),
                ))
                .saturating_add(series.labels.iter().fold(0u64, |bytes, label| {
                    bytes.saturating_add(modeled_prom_label_retained_bytes(label))
                }))
                .saturating_add(modeled_vec_capacity_bytes::<PromSample>(
                    series.samples.capacity(),
                ))
                .saturating_add(modeled_vec_capacity_bytes::<PromHistogram>(
                    series.histograms.capacity(),
                ))
                .saturating_add(series.histograms.iter().fold(0u64, |bytes, histogram| {
                    bytes.saturating_add(modeled_prom_histogram_retained_bytes(histogram))
                }))
        }),
    )
}

struct GuardedQueryResult {
    result: QueryResult,
    _reservation: tsink::QueryMemoryReservation,
}

impl GuardedQueryResult {
    fn append_to_response(
        self,
        response: &mut RemoteReadResponseEncoder,
        execution: &tsink::QueryExecution,
    ) -> Result<(), RemoteReadQueryError> {
        let Self {
            result,
            _reservation,
        } = self;
        let append_result = response.append(result, execution);
        drop(_reservation);
        append_result
    }
}

fn guarded_prom_query_result(
    series: Vec<SeriesPoints>,
    matched: Option<Vec<bool>>,
    tenant_id: Option<&str>,
    execution: &tsink::QueryExecution,
) -> Result<GuardedQueryResult, tsink::QueryBudgetError> {
    execution.checkpoint()?;
    let transform_upper = modeled_prom_transform_upper_bytes(&series);
    let mut reservation = execution.reserve_memory(transform_upper)?;
    let mut out_series = Vec::with_capacity(series.len());
    for (index, SeriesPoints { series, points }) in series.into_iter().enumerate() {
        execution.checkpoint()?;
        if matched
            .as_ref()
            .is_some_and(|matched| !matched.get(index).copied().unwrap_or(false))
        {
            continue;
        }
        let series = match tenant_id {
            Some(tenant_id) => match tenant::visible_metric_series(series, tenant_id) {
                Some(series) => series,
                None => continue,
            },
            None => series,
        };
        let Some(series) = prom_time_series_from_points(series, points) else {
            continue;
        };
        execution.observe_intermediate_vector_size(saturating_u64_from_usize(
            series
                .labels
                .len()
                .max(series.samples.len())
                .max(series.histograms.len()),
        ))?;
        out_series.push(series);
    }
    out_series.sort_by(compare_prom_series);
    execution.observe_intermediate_vector_size(saturating_u64_from_usize(out_series.len()))?;
    let result = QueryResult {
        timeseries: out_series,
    };
    reservation.resize(modeled_prom_query_result_retained_bytes(&result))?;
    execution.checkpoint()?;
    Ok(GuardedQueryResult {
        result,
        _reservation: reservation,
    })
}

fn prom_time_series_from_points(
    series: MetricSeries,
    points: Vec<DataPoint>,
) -> Option<TimeSeries> {
    let sample_count = points
        .iter()
        .filter(|point| {
            !matches!(point.value, tsink::Value::Histogram(_)) && point.value.as_f64().is_some()
        })
        .count();
    let histogram_count = points
        .iter()
        .filter(|point| matches!(point.value, tsink::Value::Histogram(_)))
        .count();
    let mut samples = Vec::with_capacity(sample_count);
    let mut histograms = Vec::with_capacity(histogram_count);
    for point in points {
        match point.value {
            tsink::Value::Histogram(histogram) => {
                histograms.push(tsink_histogram_to_prom(histogram, point.timestamp));
            }
            value => {
                if let Some(value) = value.as_f64() {
                    samples.push(PromSample {
                        value,
                        timestamp: point.timestamp,
                    });
                }
            }
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

fn tsink_histogram_to_prom(
    histogram: Box<tsink::NativeHistogram>,
    timestamp: i64,
) -> PromHistogram {
    let tsink::NativeHistogram {
        count,
        sum,
        schema,
        zero_threshold,
        zero_count,
        negative_spans,
        negative_deltas,
        negative_counts,
        positive_spans,
        positive_deltas,
        positive_counts,
        reset_hint,
        custom_values,
    } = *histogram;
    PromHistogram {
        count: count.map(|count| match count {
            tsink::HistogramCount::Int(value) => histogram::Count::CountInt(value),
            tsink::HistogramCount::Float(value) => histogram::Count::CountFloat(value),
        }),
        sum,
        schema,
        zero_threshold,
        zero_count: zero_count.map(|count| match count {
            tsink::HistogramCount::Int(value) => histogram::ZeroCount::ZeroCountInt(value),
            tsink::HistogramCount::Float(value) => histogram::ZeroCount::ZeroCountFloat(value),
        }),
        negative_spans: negative_spans
            .into_iter()
            .map(|span| BucketSpan {
                offset: span.offset,
                length: span.length,
            })
            .collect(),
        negative_deltas,
        negative_counts,
        positive_spans: positive_spans
            .into_iter()
            .map(|span| BucketSpan {
                offset: span.offset,
                length: span.length,
            })
            .collect(),
        positive_deltas,
        positive_counts,
        reset_hint: match reset_hint {
            tsink::HistogramResetHint::Unknown => PromHistogramResetHint::Unknown as i32,
            tsink::HistogramResetHint::Yes => PromHistogramResetHint::Yes as i32,
            tsink::HistogramResetHint::No => PromHistogramResetHint::No as i32,
            tsink::HistogramResetHint::Gauge => PromHistogramResetHint::Gauge as i32,
        },
        timestamp,
        custom_values,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prom_remote::ReadResponse;
    use std::collections::HashMap;
    use std::sync::Barrier;

    struct BlockingRemoteReadStorage {
        budget: tsink::QueryBudget,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    impl Storage for BlockingRemoteReadStorage {
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
            _opts: tsink::QueryOptions,
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

        fn select_series_execution_accounting(&self) -> tsink::QueryExecutionAccounting {
            tsink::QueryExecutionAccounting::Complete
        }

        fn select_many_execution_accounting(&self) -> tsink::QueryExecutionAccounting {
            tsink::QueryExecutionAccounting::Complete
        }

        fn select_series_with_execution_result(
            &self,
            _selection: &SeriesSelection,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<tsink::SelectSeriesExecutionResult> {
            self.entered.wait();
            self.release.wait();
            execution.checkpoint().map_err(tsink::TsinkError::from)?;
            Ok(tsink::SelectSeriesExecutionResult::accounted(
                Vec::new(),
                execution.reserve_memory(0)?,
            ))
        }

        fn close(&self) -> tsink::Result<()> {
            Ok(())
        }
    }

    struct RemoteMetadataContractStorage {
        budget: tsink::QueryBudget,
        accounting: tsink::QueryExecutionAccounting,
        guard_metadata_result: bool,
    }

    impl RemoteMetadataContractStorage {
        fn new(accounting: tsink::QueryExecutionAccounting) -> Self {
            Self {
                budget: tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
                    .expect("query budget should build"),
                accounting,
                guard_metadata_result: false,
            }
        }

        fn with_guarded_metadata(mut self) -> Self {
            self.guard_metadata_result = true;
            self
        }
    }

    impl Storage for RemoteMetadataContractStorage {
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

        fn select_series_with_execution(
            &self,
            _selection: &SeriesSelection,
            _execution: &tsink::QueryExecution,
        ) -> tsink::Result<Vec<MetricSeries>> {
            Ok(vec![MetricSeries {
                name: "contract_metric".to_string(),
                labels: Vec::new(),
            }])
        }

        fn select_series_with_execution_result(
            &self,
            selection: &SeriesSelection,
            execution: &tsink::QueryExecution,
        ) -> tsink::Result<tsink::SelectSeriesExecutionResult> {
            let series = self.select_series_with_execution(selection, execution)?;
            if self.guard_metadata_result {
                Ok(tsink::SelectSeriesExecutionResult::accounted(
                    series,
                    execution.reserve_memory(1)?,
                ))
            } else {
                Ok(tsink::SelectSeriesExecutionResult::unaccounted(series))
            }
        }

        fn select_series_execution_accounting(&self) -> tsink::QueryExecutionAccounting {
            self.accounting
        }

        fn close(&self) -> tsink::Result<()> {
            Ok(())
        }
    }

    fn storage_with_returned_sample_limit(limit: u64) -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_query_budget_limits(tsink::QueryBudgetLimits {
                max_concurrent_queries: Some(1),
                per_query: tsink::QueryWorkLimits {
                    max_samples_returned: Some(limit),
                    ..tsink::QueryWorkLimits::default()
                },
                ..tsink::QueryBudgetLimits::default()
            })
            .build()
            .expect("storage should build")
    }

    fn storage_with_remote_read_memory_limit(limit: u64) -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_query_budget_limits(tsink::QueryBudgetLimits {
                max_concurrent_queries: Some(1),
                max_shared_memory_bytes: Some(limit),
                per_query: tsink::QueryWorkLimits {
                    max_memory_bytes: Some(limit),
                    max_series_matched: Some(1_000),
                    max_samples_scanned: Some(10_000),
                    max_samples_returned: Some(10_000),
                    max_intermediate_vector_size: Some(10_000),
                    ..tsink::QueryWorkLimits::default()
                },
            })
            .build()
            .expect("storage should build")
    }

    fn remote_read_request(metric: &str) -> HttpRequest {
        remote_read_request_for_metrics(&[metric])
    }

    fn remote_read_request_for_metrics(metrics: &[&str]) -> HttpRequest {
        let request = ReadRequest {
            queries: metrics
                .iter()
                .map(|metric| Query {
                    start_timestamp_ms: 0,
                    end_timestamp_ms: 10,
                    matchers: vec![LabelMatcher {
                        r#type: MatcherType::Eq as i32,
                        name: "__name__".to_string(),
                        value: (*metric).to_string(),
                    }],
                    hints: None,
                })
                .collect(),
            accepted_response_types: Vec::new(),
        };
        let mut encoded = Vec::new();
        request
            .encode(&mut encoded)
            .expect("protobuf request should encode");
        HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/read".to_string(),
            headers: HashMap::from([
                ("content-encoding".to_string(), "snappy".to_string()),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
            ]),
            body: SnappyEncoder::new()
                .compress_vec(&encoded)
                .expect("request should compress"),
        }
    }

    fn encoded_remote_read_body(queries: Vec<Query>) -> Vec<u8> {
        let mut encoded = Vec::new();
        ReadRequest {
            queries,
            accepted_response_types: Vec::new(),
        }
        .encode(&mut encoded)
        .expect("remote-read request should encode");
        encoded
    }

    fn matcher(name: String, value: String) -> LabelMatcher {
        LabelMatcher {
            r#type: MatcherType::Eq as i32,
            name,
            value,
        }
    }

    fn query_with_matchers(matchers: Vec<LabelMatcher>) -> Query {
        Query {
            start_timestamp_ms: 0,
            end_timestamp_ms: 10,
            matchers,
            hints: None,
        }
    }

    fn cumulative_matchers(total_bytes: usize) -> Vec<LabelMatcher> {
        let count = tsink::MAX_SERIES_SELECTION_MATCHERS;
        let bytes_per_matcher = total_bytes / count;
        let remainder = total_bytes % count;
        (0..count)
            .map(|index| {
                let matcher_bytes = bytes_per_matcher + usize::from(index < remainder);
                assert!(matcher_bytes >= 1);
                matcher("n".to_string(), "v".repeat(matcher_bytes - 1))
            })
            .collect()
    }

    fn read_admission() -> ReadAdmissionController {
        ReadAdmissionController::new(admission::ReadAdmissionGuardrails {
            max_inflight_requests: 2,
            max_inflight_queries: 2,
            acquire_timeout: std::time::Duration::from_millis(10),
        })
        .expect("read admission should build")
    }

    fn response_header<'a>(response: &'a HttpResponse, name: &str) -> Option<&'a str> {
        response
            .headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn insert_two_series(storage: &Arc<dyn Storage>, metric: &str) {
        tenant::scoped_storage(Arc::clone(storage), tenant::DEFAULT_TENANT_ID)
            .insert_rows(&[
                Row::with_labels(
                    metric,
                    vec![Label::new("host", "a")],
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    metric,
                    vec![Label::new("host", "b")],
                    DataPoint::new(2, 2.0),
                ),
            ])
            .expect("rows should insert");
    }

    fn assert_remote_read_resources_released(storage: &Arc<dyn Storage>) {
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    fn query_result(metric: &str, timestamp: i64) -> QueryResult {
        QueryResult {
            timeseries: vec![TimeSeries {
                labels: vec![PromLabel {
                    name: "__name__".to_string(),
                    value: metric.to_string(),
                }],
                samples: vec![PromSample {
                    value: 1.0,
                    timestamp,
                }],
                ..TimeSeries::default()
            }],
        }
    }

    fn query_budget_with_returned_bytes(limit: u64) -> tsink::QueryBudget {
        tsink::QueryBudget::new(tsink::QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            per_query: tsink::QueryWorkLimits {
                max_returned_bytes: Some(limit),
                ..tsink::QueryWorkLimits::default()
            },
            ..tsink::QueryBudgetLimits::default()
        })
        .expect("query budget should build")
    }

    #[test]
    fn bounded_local_remote_read_rejects_unaccounted_metadata_storage() {
        let storage = Arc::new(RemoteMetadataContractStorage::new(
            tsink::QueryExecutionAccounting::Unaccounted,
        ));
        let storage_trait: Arc<dyn Storage> = storage.clone();
        let query = Query {
            start_timestamp_ms: 0,
            end_timestamp_ms: 1,
            matchers: Vec::new(),
            hints: None,
        };

        let (execution, _cancellation_guard) =
            begin_remote_read_execution(&storage_trait).expect("query execution should begin");
        let Err(error) = execute_query(&storage_trait, &query, &execution) else {
            panic!("bounded local remote-read must reject unaccounted metadata storage");
        };
        assert!(matches!(
            error,
            RemoteReadQueryError::Storage {
                error: tsink::TsinkError::UnsupportedOperation {
                    operation: "bounded local remote-read metadata selection",
                    ..
                },
                ..
            }
        ));
        drop(execution);
        assert_eq!(storage.budget.snapshot().active_queries, 0);
    }

    #[test]
    fn bounded_local_remote_read_rejects_false_complete_metadata_claim() {
        let storage = Arc::new(RemoteMetadataContractStorage::new(
            tsink::QueryExecutionAccounting::Complete,
        ));
        let storage_trait: Arc<dyn Storage> = storage.clone();
        let query = Query {
            start_timestamp_ms: 0,
            end_timestamp_ms: 1,
            matchers: Vec::new(),
            hints: None,
        };

        let (execution, _cancellation_guard) =
            begin_remote_read_execution(&storage_trait).expect("query execution should begin");
        let Err(error) = execute_query(&storage_trait, &query, &execution) else {
            panic!("a false Complete claim must not release an unreserved metadata result");
        };
        assert!(matches!(
            error,
            RemoteReadQueryError::Storage {
                error: tsink::TsinkError::Other(message),
                ..
            } if message.contains("omitted its result reservation")
        ));
        drop(execution);
        assert_eq!(storage.budget.snapshot().active_queries, 0);
    }

    #[test]
    fn bounded_local_remote_read_rejects_unaccounted_point_storage() {
        let storage = Arc::new(
            RemoteMetadataContractStorage::new(tsink::QueryExecutionAccounting::Complete)
                .with_guarded_metadata(),
        );
        let storage_trait: Arc<dyn Storage> = storage.clone();
        let query = Query {
            start_timestamp_ms: 0,
            end_timestamp_ms: 1,
            matchers: Vec::new(),
            hints: None,
        };
        let (execution, _cancellation_guard) =
            begin_remote_read_execution(&storage_trait).expect("query execution should begin");

        let Err(error) = execute_query(&storage_trait, &query, &execution) else {
            panic!("bounded local remote-read must reject unaccounted point storage");
        };
        assert!(matches!(
            error,
            RemoteReadQueryError::Storage {
                error: tsink::TsinkError::UnsupportedOperation {
                    operation: "bounded local remote-read point selection",
                    ..
                },
                ..
            }
        ));
        drop(execution);
        assert_eq!(storage.budget.snapshot().active_queries, 0);
        assert_eq!(storage.budget.snapshot().shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn remote_read_per_query_encoded_bytes_have_an_exact_budget_boundary() {
        let result = query_result("encoded_boundary", 1);
        let encoded_bytes = ReadResponse {
            results: vec![result.clone()],
        }
        .encoded_len();

        let exact_budget = query_budget_with_returned_bytes(u64::try_from(encoded_bytes).unwrap());
        let exact_execution = exact_budget
            .begin_query()
            .expect("query should be admitted");
        let mut exact = RemoteReadResponseEncoder::with_limit(encoded_bytes);
        exact
            .append(result.clone(), &exact_execution)
            .expect("the exact encoded-byte boundary should succeed");
        assert_eq!(exact.raw.len(), encoded_bytes);
        assert_eq!(
            exact_execution.snapshot().returned_bytes,
            u64::try_from(encoded_bytes).unwrap()
        );

        let rejected_budget =
            query_budget_with_returned_bytes(u64::try_from(encoded_bytes - 1).unwrap());
        let rejected_execution = rejected_budget
            .begin_query()
            .expect("query should be admitted");
        let mut rejected = RemoteReadResponseEncoder::with_limit(encoded_bytes);
        let response = remote_read_query_error_response(
            rejected
                .append(result, &rejected_execution)
                .expect_err("one byte below the encoded result must be rejected"),
        );
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("query_limit_returned_bytes")
        );
        assert!(rejected.raw.is_empty());
    }

    #[test]
    fn transformed_result_guard_stays_live_through_response_append() {
        const RESULT_GUARD_BYTES: u64 = 4_096;
        let budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
            .expect("query budget should build");
        let execution = budget.begin_query().expect("query should begin");
        let guarded = GuardedQueryResult {
            result: query_result("guard_overlap", 1),
            _reservation: execution
                .reserve_memory(RESULT_GUARD_BYTES)
                .expect("result memory should reserve"),
        };
        let mut encoder = RemoteReadResponseEncoder::with_execution_limit(
            MAX_REMOTE_READ_RESPONSE_BYTES,
            &execution,
        )
        .expect("encoder should build");

        guarded
            .append_to_response(&mut encoder, &execution)
            .expect("guarded result should append");

        let response_bytes = encoder
            .reservation
            .as_ref()
            .map_or(0, tsink::QueryMemoryReservation::bytes);
        assert!(response_bytes > 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, response_bytes);
        assert_eq!(
            budget.snapshot().peak_shared_reserved_memory_bytes,
            RESULT_GUARD_BYTES.saturating_add(response_bytes)
        );
        drop(encoder);
        drop(execution);
        assert_eq!(budget.snapshot().active_queries, 0);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn remote_read_aggregate_encoded_cap_is_exact_across_queries_without_truncation() {
        let first = query_result("first_metric", 1);
        let second = query_result("second_metric", 2);
        let expected = ReadResponse {
            results: vec![first.clone(), second.clone()],
        };
        let exact_bytes = expected.encoded_len();
        let budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
            .expect("query budget should build");
        let execution = budget.begin_query().expect("query should begin");

        let mut exact = RemoteReadResponseEncoder::with_limit(exact_bytes);
        exact
            .append(first.clone(), &execution)
            .expect("first result should fit");
        exact
            .append(second.clone(), &execution)
            .expect("the exact aggregate boundary should fit");
        let (encoded, units) = exact.finish();
        assert_eq!(encoded.raw.len(), exact_bytes);
        assert_eq!(units, 2);
        assert_eq!(
            ReadResponse::decode(encoded.raw.as_slice()).expect("complete response should decode"),
            expected
        );

        let rejected_execution = budget.begin_query().expect("query should begin");
        let mut rejected = RemoteReadResponseEncoder::with_limit(exact_bytes - 1);
        rejected
            .append(first.clone(), &rejected_execution)
            .expect("first result should still fit");
        let first_only = rejected.raw.clone();
        let response = remote_read_query_error_response(
            rejected
                .append(second, &rejected_execution)
                .expect_err("one byte below the aggregate response must reject the request"),
        );
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("query_limit_returned_bytes")
        );
        assert_eq!(rejected.raw, first_only);
        assert_eq!(
            ReadResponse::decode(rejected.raw.as_slice())
                .expect("the rejected result must not leave a partial frame"),
            ReadResponse {
                results: vec![first],
            }
        );
    }

    #[tokio::test]
    async fn local_remote_read_reuses_one_execution_across_all_series() {
        let storage = storage_with_returned_sample_limit(2);
        insert_two_series(&storage, "remote_budget_exact");

        let response = handle_remote_read_with_admission(
            &storage,
            &remote_read_request("remote_budget_exact"),
            None,
            None,
            None,
            None,
            &read_admission(),
        )
        .await;
        assert_eq!(response.status, 200);

        let decoded = SnappyDecoder::new()
            .decompress_vec(&response.body)
            .expect("response should decompress");
        let read_response =
            ReadResponse::decode(decoded.as_slice()).expect("response should decode");
        assert_eq!(remote_read_query_units(&read_response.results), 2);
        assert_eq!(read_response.results[0].timeseries.len(), 2);

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_eq!(snapshot.peak_active_queries, 1);
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_remote_read_future_cancels_blocking_work_and_releases_query_resources() {
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let blocking = Arc::new(BlockingRemoteReadStorage {
            budget: tsink::QueryBudget::new(tsink::QueryBudgetLimits {
                max_concurrent_queries: Some(1),
                max_shared_memory_bytes: Some(8 * 1024 * 1024),
                per_query: tsink::QueryWorkLimits {
                    max_memory_bytes: Some(8 * 1024 * 1024),
                    ..tsink::QueryWorkLimits::default()
                },
            })
            .expect("query budget should build"),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let storage: Arc<dyn Storage> = blocking.clone();
        let admission = Arc::new(read_admission());
        let storage_for_handler = Arc::clone(&storage);
        let admission_for_handler = Arc::clone(&admission);
        let task = tokio::spawn(async move {
            handle_remote_read_with_admission(
                &storage_for_handler,
                &remote_read_request("remote_read_cancelled"),
                None,
                None,
                None,
                None,
                &admission_for_handler,
            )
            .await
        });

        tokio::task::spawn_blocking(move || entered.wait())
            .await
            .expect("metadata-entry waiter should join");
        assert_eq!(blocking.budget.snapshot().active_queries, 1);

        task.abort();
        assert!(task
            .await
            .expect_err("aborted remote-read handler should not complete")
            .is_cancelled());
        tokio::task::spawn_blocking(move || release.wait())
            .await
            .expect("metadata-release waiter should join");

        for _ in 0..1_000 {
            if blocking.budget.snapshot().active_queries == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let snapshot = blocking.budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert!(snapshot.cancellations_total >= 1);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn local_remote_read_maps_cumulative_sample_limit_and_releases_execution() {
        let storage = storage_with_returned_sample_limit(1);
        insert_two_series(&storage, "remote_budget_rejected");

        let response = handle_remote_read_with_admission(
            &storage,
            &remote_read_request("remote_budget_rejected"),
            None,
            None,
            None,
            None,
            &read_admission(),
        )
        .await;
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("query_limit_samples_returned")
        );

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_eq!(snapshot.samples_returned_rejections_total, 1);
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    }

    #[tokio::test]
    async fn local_remote_read_reuses_one_execution_for_multiple_queries_with_one_slot() {
        let storage = storage_with_returned_sample_limit(2);
        tenant::scoped_storage(Arc::clone(&storage), tenant::DEFAULT_TENANT_ID)
            .insert_rows(&[
                Row::new("remote_multi_first", DataPoint::new(1, 1.0)),
                Row::new("remote_multi_second", DataPoint::new(2, 2.0)),
            ])
            .expect("rows should insert");

        let response = handle_remote_read_with_admission(
            &storage,
            &remote_read_request_for_metrics(&["remote_multi_first", "remote_multi_second"]),
            None,
            None,
            None,
            None,
            &read_admission(),
        )
        .await;
        assert_eq!(response.status, 200);

        let decoded = SnappyDecoder::new()
            .decompress_vec(&response.body)
            .expect("response should decompress");
        let read_response =
            ReadResponse::decode(decoded.as_slice()).expect("response should decode");
        assert_eq!(read_response.results.len(), 2);
        assert_eq!(remote_read_query_units(&read_response.results), 2);

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_eq!(snapshot.peak_active_queries, 1);
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    }

    #[tokio::test]
    async fn local_remote_read_enforces_sample_limits_cumulatively_across_queries() {
        let storage = storage_with_returned_sample_limit(1);
        tenant::scoped_storage(Arc::clone(&storage), tenant::DEFAULT_TENANT_ID)
            .insert_rows(&[
                Row::new("remote_cumulative_first", DataPoint::new(1, 1.0)),
                Row::new("remote_cumulative_second", DataPoint::new(2, 2.0)),
            ])
            .expect("rows should insert");

        let response = handle_remote_read_with_admission(
            &storage,
            &remote_read_request_for_metrics(&[
                "remote_cumulative_first",
                "remote_cumulative_second",
            ]),
            None,
            None,
            None,
            None,
            &read_admission(),
        )
        .await;
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("query_limit_samples_returned")
        );

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_eq!(snapshot.samples_returned_rejections_total, 1);
        assert_eq!(snapshot.peak_active_queries, 1);
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    }

    #[tokio::test]
    async fn empty_remote_read_cannot_bypass_the_query_admission_permit() {
        let storage = storage_with_returned_sample_limit(1);
        let admission = ReadAdmissionController::new(admission::ReadAdmissionGuardrails {
            max_inflight_requests: 2,
            max_inflight_queries: 1,
            acquire_timeout: std::time::Duration::from_millis(1),
        })
        .expect("read admission should build");
        let occupied = admission
            .admit_request(1)
            .await
            .expect("the only query permit should be held");

        let response = handle_remote_read_with_admission(
            &storage,
            &remote_read_request_for_metrics(&[]),
            None,
            None,
            None,
            None,
            &admission,
        )
        .await;
        assert_eq!(response.status, 429);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("read_overloaded")
        );
        drop(occupied);
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_remote_read_resources_released(&storage);
    }

    async fn run_memory_enveloped_remote_read(
        memory_limit: u64,
        metric: &str,
    ) -> (HttpResponse, Arc<dyn Storage>) {
        let storage = storage_with_remote_read_memory_limit(memory_limit);
        insert_two_series(&storage, metric);
        let response = handle_remote_read_with_admission(
            &storage,
            &remote_read_request(metric),
            None,
            None,
            None,
            None,
            &read_admission(),
        )
        .await;
        (response, storage)
    }

    #[tokio::test]
    async fn local_remote_read_memory_envelope_has_an_exact_n_n_plus_one_boundary() {
        let (measured_response, measured) =
            run_memory_enveloped_remote_read(256 * 1024 * 1024, "remote_memory_boundary").await;
        assert_eq!(measured_response.status, 200);
        let exact_memory = measured
            .query_budget_snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(exact_memory > 1);
        assert_remote_read_resources_released(&measured);

        let (exact_response, exact) =
            run_memory_enveloped_remote_read(exact_memory, "remote_memory_boundary").await;
        assert_eq!(exact_response.status, 200);
        assert_remote_read_resources_released(&exact);

        let (rejected_response, rejected) = run_memory_enveloped_remote_read(
            exact_memory.saturating_sub(1),
            "remote_memory_boundary",
        )
        .await;
        assert_eq!(rejected_response.status, 413);
        assert_eq!(
            response_header(&rejected_response, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_eq!(
            rejected
                .query_budget_snapshot()
                .per_query_memory_rejections_total,
            1
        );
        assert_remote_read_resources_released(&rejected);
    }

    #[test]
    fn remote_read_predecode_matcher_count_limit_is_exact() {
        let exact_matchers = (0..tsink::MAX_SERIES_SELECTION_MATCHERS)
            .map(|index| matcher(format!("label_{index}"), String::new()))
            .collect();
        let exact = encoded_remote_read_body(vec![query_with_matchers(exact_matchers)]);
        scan_remote_read_decode_model(&exact)
            .expect("the exact remote-read matcher-count limit should pass predecode");

        let one_over_matchers = (0..=tsink::MAX_SERIES_SELECTION_MATCHERS)
            .map(|index| matcher(format!("label_{index}"), String::new()))
            .collect();
        let one_over = encoded_remote_read_body(vec![query_with_matchers(one_over_matchers)]);
        assert_eq!(
            scan_remote_read_decode_model(&one_over),
            Err(RemoteReadDecodeScanError::MatcherShape(
                RemoteReadMatcherShapeError::TooManyMatchers {
                    query_index: 0,
                    actual: tsink::MAX_SERIES_SELECTION_MATCHERS + 1,
                }
            ))
        );
    }

    #[test]
    fn remote_read_predecode_matcher_name_limit_is_exact() {
        let exact = encoded_remote_read_body(vec![query_with_matchers(vec![matcher(
            "n".repeat(tsink::MAX_SERIES_MATCHER_NAME_BYTES),
            String::new(),
        )])]);
        scan_remote_read_decode_model(&exact)
            .expect("the exact remote-read matcher-name limit should pass predecode");

        let one_over = encoded_remote_read_body(vec![query_with_matchers(vec![matcher(
            "n".repeat(tsink::MAX_SERIES_MATCHER_NAME_BYTES + 1),
            String::new(),
        )])]);
        assert_eq!(
            scan_remote_read_decode_model(&one_over),
            Err(RemoteReadDecodeScanError::MatcherShape(
                RemoteReadMatcherShapeError::NameTooLong {
                    query_index: 0,
                    matcher_index: 0,
                    actual: tsink::MAX_SERIES_MATCHER_NAME_BYTES + 1,
                }
            ))
        );
    }

    #[test]
    fn remote_read_predecode_matcher_value_limit_is_exact() {
        let exact = encoded_remote_read_body(vec![query_with_matchers(vec![matcher(
            "label".to_string(),
            "v".repeat(tsink::MAX_SERIES_MATCHER_VALUE_BYTES),
        )])]);
        scan_remote_read_decode_model(&exact)
            .expect("the exact remote-read matcher-value limit should pass predecode");

        let one_over = encoded_remote_read_body(vec![query_with_matchers(vec![matcher(
            "label".to_string(),
            "v".repeat(tsink::MAX_SERIES_MATCHER_VALUE_BYTES + 1),
        )])]);
        assert_eq!(
            scan_remote_read_decode_model(&one_over),
            Err(RemoteReadDecodeScanError::MatcherShape(
                RemoteReadMatcherShapeError::ValueTooLong {
                    query_index: 0,
                    matcher_index: 0,
                    actual: tsink::MAX_SERIES_MATCHER_VALUE_BYTES + 1,
                }
            ))
        );
    }

    #[test]
    fn remote_read_predecode_cumulative_matcher_bytes_limit_is_exact() {
        let exact = encoded_remote_read_body(vec![query_with_matchers(cumulative_matchers(
            tsink::MAX_SERIES_SELECTION_MATCHER_BYTES,
        ))]);
        scan_remote_read_decode_model(&exact)
            .expect("the exact cumulative remote-read matcher-byte limit should pass predecode");

        let one_over = encoded_remote_read_body(vec![query_with_matchers(cumulative_matchers(
            tsink::MAX_SERIES_SELECTION_MATCHER_BYTES + 1,
        ))]);
        assert_eq!(
            scan_remote_read_decode_model(&one_over),
            Err(RemoteReadDecodeScanError::MatcherShape(
                RemoteReadMatcherShapeError::TotalTooLong {
                    query_index: 0,
                    actual: tsink::MAX_SERIES_SELECTION_MATCHER_BYTES + 1,
                }
            ))
        );
    }

    #[test]
    fn remote_read_matcher_shape_rejection_precedes_prost_allocation_and_is_bounded() {
        const SENTINEL: &str = "REMOTE_READ_MATCHER_SECRET_SENTINEL";
        let storage = storage_with_remote_read_memory_limit(8 * 1024 * 1024);
        let (execution, _cancellation_guard) =
            begin_remote_read_execution(&storage).expect("query execution should begin");
        let oversized_value =
            SENTINEL.repeat((tsink::MAX_SERIES_MATCHER_VALUE_BYTES / SENTINEL.len()) + 2);
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/read".to_string(),
            headers: HashMap::from([(
                "content-type".to_string(),
                "application/x-protobuf".to_string(),
            )]),
            body: encoded_remote_read_body(vec![
                query_with_matchers(Vec::new()),
                query_with_matchers(vec![matcher("label".to_string(), oversized_value)]),
            ]),
        };

        let error = decode_remote_read_request(&request, &execution)
            .expect_err("an oversized matcher must fail during wire predecode");
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(
            storage
                .query_budget_snapshot()
                .peak_shared_reserved_memory_bytes,
            0,
            "prost request-heap reservation must not run after the matcher-shape rejection"
        );
        let response = remote_read_query_error_response(error);
        assert_eq!(response.status, 400);
        let diagnostic = String::from_utf8(response.body).expect("diagnostic should be UTF-8");
        assert!(diagnostic.contains("query index 1"));
        assert!(diagnostic.contains("matcher index 0"));
        assert!(diagnostic.contains("value"));
        assert!(diagnostic.len() <= MAX_REMOTE_READ_DIAGNOSTIC_BYTES);
        assert!(!diagnostic.contains(SENTINEL));
        drop(execution);
        assert_remote_read_resources_released(&storage);
    }

    #[tokio::test]
    async fn malformed_remote_read_protobuf_releases_execution_and_memory() {
        let storage = storage_with_remote_read_memory_limit(8 * 1024 * 1024);
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/read".to_string(),
            headers: HashMap::from([(
                "content-type".to_string(),
                "application/x-protobuf".to_string(),
            )]),
            body: vec![0x0a, 0x05, 0x08],
        };

        let response = handle_remote_read_with_admission(
            &storage,
            &request,
            None,
            None,
            None,
            None,
            &read_admission(),
        )
        .await;
        assert_eq!(response.status, 400);
        assert!(String::from_utf8_lossy(&response.body).contains("invalid protobuf body"));
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_remote_read_resources_released(&storage);
    }

    #[test]
    fn remote_read_decode_drops_snappy_buffer_before_its_reservation() {
        let storage = storage_with_remote_read_memory_limit(8 * 1024 * 1024);
        let (execution, _cancellation_guard) =
            begin_remote_read_execution(&storage).expect("query execution should begin");
        let request = remote_read_request("decode_release");

        let (decoded, request_reservation) =
            decode_remote_read_request(&request, &execution).expect("request should decode");

        assert_eq!(decoded.queries.len(), 1);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            request_reservation.bytes()
        );
        assert!(
            storage
                .query_budget_snapshot()
                .peak_shared_reserved_memory_bytes
                > request_reservation.bytes(),
            "decoded bytes and protobuf heap must overlap under reservations before the decoded buffer is dropped"
        );
        drop(decoded);
        drop(request_reservation);
        drop(execution);
        assert_remote_read_resources_released(&storage);
    }

    #[tokio::test]
    async fn oversized_snappy_remote_read_preflight_releases_execution_without_decompression() {
        let storage = storage_with_remote_read_memory_limit(8 * 1024 * 1024);
        let mut declared_len = u64::try_from(MAX_BODY_BYTES)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let mut body = Vec::new();
        loop {
            let mut byte = u8::try_from(declared_len & 0x7f).unwrap();
            declared_len >>= 7;
            if declared_len != 0 {
                byte |= 0x80;
            }
            body.push(byte);
            if declared_len == 0 {
                break;
            }
        }
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
            body,
        };

        let response = handle_remote_read_with_admission(
            &storage,
            &request,
            None,
            None,
            None,
            None,
            &read_admission(),
        )
        .await;
        assert_eq!(response.status, 400);
        assert!(String::from_utf8_lossy(&response.body).contains("decoded request body too large"));
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_remote_read_resources_released(&storage);
    }

    #[tokio::test]
    async fn unsupported_content_encoding_diagnostic_is_bounded_and_does_not_echo_header() {
        const SENTINEL: &str = "REMOTE_READ_ENCODING_SECRET_SENTINEL";
        let storage = storage_with_remote_read_memory_limit(8 * 1024 * 1024);
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/read".to_string(),
            headers: HashMap::from([
                ("content-encoding".to_string(), SENTINEL.repeat(2 * 1024)),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
            ]),
            body: Vec::new(),
        };

        let response = handle_remote_read_with_admission(
            &storage,
            &request,
            None,
            None,
            None,
            None,
            &read_admission(),
        )
        .await;
        assert_eq!(response.status, 400);
        let diagnostic = String::from_utf8(response.body).expect("diagnostic should be UTF-8");
        assert!(diagnostic.contains("unsupported remote-read content-encoding"));
        assert!(diagnostic.len() <= MAX_REMOTE_READ_DIAGNOSTIC_BYTES);
        assert!(!diagnostic.contains(SENTINEL));
        assert!(
            storage
                .query_budget_snapshot()
                .peak_shared_reserved_memory_bytes
                >= REMOTE_READ_RESPONSE_DIAGNOSTIC_ENVELOPE_BYTES
        );
        assert_remote_read_resources_released(&storage);
    }

    #[tokio::test]
    async fn invalid_regex_diagnostic_is_typed_bounded_and_does_not_echo_pattern() {
        const SENTINEL: &str = "REMOTE_READ_REGEX_SECRET_SENTINEL";
        let storage = storage_with_remote_read_memory_limit(8 * 1024 * 1024);
        let pattern = format!("({SENTINEL}");
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/read".to_string(),
            headers: HashMap::from([(
                "content-type".to_string(),
                "application/x-protobuf".to_string(),
            )]),
            body: encoded_remote_read_body(vec![query_with_matchers(vec![LabelMatcher {
                r#type: MatcherType::Re as i32,
                name: "host".to_string(),
                value: pattern,
            }])]),
        };

        let response = handle_remote_read_with_admission(
            &storage,
            &request,
            None,
            None,
            None,
            None,
            &read_admission(),
        )
        .await;
        assert_eq!(response.status, 400);
        let diagnostic = String::from_utf8(response.body).expect("diagnostic should be UTF-8");
        assert!(diagnostic.contains("invalid remote-read query"));
        assert!(diagnostic.len() <= MAX_REMOTE_READ_DIAGNOSTIC_BYTES);
        assert!(!diagnostic.contains(SENTINEL));
        assert_remote_read_resources_released(&storage);
    }

    #[test]
    fn remote_read_query_errors_have_stable_http_mappings() {
        let invalid = remote_read_query_error_response(RemoteReadQueryError::InvalidRequest(
            "unknown matcher type".to_string(),
        ));
        assert_eq!(invalid.status, 400);
        assert_eq!(response_header(&invalid, READ_ERROR_CODE_HEADER), None);

        let work =
            remote_read_query_budget_error_response(&tsink::QueryBudgetError::LimitExceeded(
                tsink::QueryLimitExceeded::new(tsink::QueryLimitReason::SamplesScanned, 1, 1, 1),
            ));
        assert_eq!(work.status, 413);
        assert_eq!(
            response_header(&work, READ_ERROR_CODE_HEADER),
            Some("query_limit_samples_scanned")
        );
        assert_eq!(response_header(&work, "Retry-After"), None);

        for reason in [
            tsink::QueryLimitReason::ConcurrentQueries,
            tsink::QueryLimitReason::SharedMemoryBytes,
        ] {
            let retryable =
                remote_read_query_budget_error_response(&tsink::QueryBudgetError::LimitExceeded(
                    tsink::QueryLimitExceeded::new(reason, 1, 1, 1),
                ));
            assert_eq!(retryable.status, 429);
            assert_eq!(response_header(&retryable, "Retry-After"), Some("1"));
        }

        for error in [
            tsink::QueryBudgetError::Cancelled,
            tsink::QueryBudgetError::DeadlineExceeded,
        ] {
            let unavailable = remote_read_query_budget_error_response(&error);
            assert_eq!(unavailable.status, 503);
        }
    }

    #[test]
    fn remote_read_storage_errors_distinguish_requests_internal_failures_and_unavailability() {
        let cases = [
            (
                tsink::TsinkError::InvalidTimeRange { start: 2, end: 1 },
                400,
            ),
            (
                tsink::TsinkError::InvalidMetricName("bad metric".to_string()),
                400,
            ),
            (
                tsink::TsinkError::Io(std::io::Error::other("read failed")),
                500,
            ),
            (
                tsink::TsinkError::DataCorruption("invalid segment".to_string()),
                500,
            ),
            (tsink::TsinkError::StorageShuttingDown, 503),
            (tsink::TsinkError::StorageClosed, 503),
            (
                tsink::TsinkError::LifecycleTimeout {
                    operation: "read",
                    timeout_ms: 1,
                },
                503,
            ),
        ];

        for (error, expected_status) in cases {
            let response = remote_read_query_error_response(RemoteReadQueryError::storage(
                "query failed",
                error,
            ));
            assert_eq!(response.status, expected_status);
        }
    }
}
