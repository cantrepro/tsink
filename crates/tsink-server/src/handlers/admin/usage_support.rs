use super::*;
use serde::ser::{SerializeMap, SerializeSeq};
use serde_json::value::RawValue;

pub(crate) const SUPPORT_BUNDLE_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES: u64 = 16 * 1024 * 1024;
const SUPPORT_BUNDLE_TSDB_STATUS_PATH: &str = "/api/v1/status/tsdb";
#[cfg(test)]
const SUPPORT_BUNDLE_RBAC_AUDIT_PATH: &str = "/api/v1/admin/rbac/audit?limit=50";
const SUPPORT_BUNDLE_RBAC_AUDIT_LIMIT: usize = 50;
#[cfg(test)]
const SUPPORT_BUNDLE_CLUSTER_AUDIT_PATH: &str = "/api/v1/admin/cluster/audit?limit=50";
const SUPPORT_BUNDLE_CLUSTER_AUDIT_LIMIT: usize = 50;
// Covers the simultaneous worst-case lossy UTF-8 prefix (allocator-rounded to under 129 KiB), the
// truncated 8,192-scalar output (allocator-rounded to 32 KiB), serializer bookkeeping, and the
// input-derived raw-plus-mapper construction model for the two compatibility error adapters.
const SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES: u64 = 256 * 1024;
const SUPPORT_BUNDLE_COMPATIBILITY_ERROR_FIXED_SCRATCH_BYTES: u64 = 64 * 1024;
const SUPPORT_BUNDLE_COMPATIBILITY_RAW_MAX_RETAINED_BYTES: u64 =
    tsink::label::MAX_LABEL_VALUE_LEN as u64 + 4 * 1024;
const SUPPORT_BUNDLE_COMPATIBILITY_MAPPER_MAX_RETAINED_BYTES: u64 = 4 * 1024;
const SUPPORT_BUNDLE_COMPATIBILITY_MAPPER_SCRATCH_BYTES: u64 =
    SUPPORT_BUNDLE_COMPATIBILITY_MAPPER_MAX_RETAINED_BYTES * 8;
const SUPPORT_BUNDLE_COMPATIBILITY_RAW_SCRATCH_BYTES: u64 =
    SUPPORT_BUNDLE_COMPATIBILITY_RAW_MAX_RETAINED_BYTES * 8
        + SUPPORT_BUNDLE_COMPATIBILITY_ERROR_FIXED_SCRATCH_BYTES;
const _: () = assert!(
    SUPPORT_BUNDLE_COMPATIBILITY_RAW_SCRATCH_BYTES
        + SUPPORT_BUNDLE_COMPATIBILITY_MAPPER_SCRATCH_BYTES
        <= SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES
);
const SUPPORT_BUNDLE_TEXT_MAX_CHARS: usize = 8 * 1024;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleRequestedBy<'a> {
    id: &'a str,
    auth_scope: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleSections<'a> {
    status_tsdb: SupportBundleResponseSection<'a>,
    usage: SupportBundleResponseSection<'a>,
    rbac_state: SupportBundleResponseSection<'a>,
    rbac_audit: SupportBundleResponseSection<'a>,
    security_state: SupportBundleResponseSection<'a>,
    cluster_audit: SupportBundleResponseSection<'a>,
    cluster_handoff: SupportBundleResponseSection<'a>,
    cluster_repair: SupportBundleResponseSection<'a>,
    cluster_rebalance: SupportBundleResponseSection<'a>,
    rules: SupportBundleResponseSection<'a>,
    rollups: SupportBundleResponseSection<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleEnvelope<'a> {
    generated_unix_ms: u64,
    tenant_id: &'a str,
    requested_by: SupportBundleRequestedBy<'a>,
    server_version: &'static str,
    sections: SupportBundleSections<'a>,
}

#[derive(Clone, Copy)]
struct SupportBundleResponseSection<'a> {
    response: &'a HttpResponse,
}

impl Serialize for SupportBundleResponseSection<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let content_type = self
            .response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
            .map(|(_, value)| value.as_str());
        let raw_json = serde_json::from_slice::<&RawValue>(&self.response.body).ok();
        let mut section = serializer.serialize_map(Some(
            2usize.saturating_add(usize::from(content_type.is_some())),
        ))?;
        section.serialize_entry("httpStatus", &self.response.status)?;
        if let Some(content_type) = content_type {
            section.serialize_entry("contentType", content_type)?;
        }
        if let Some(raw_json) = raw_json {
            section.serialize_entry("body", raw_json)?;
        } else {
            let body_text = bounded_support_bundle_text(&self.response.body);
            section.serialize_entry("bodyText", &body_text)?;
        }
        section.end()
    }
}

fn bounded_support_bundle_text(body: &[u8]) -> String {
    // Four bytes per Unicode scalar plus a small split-codepoint allowance is enough to inspect
    // the first configured number of lossy-decoded characters without ever allocating in
    // proportion to an oversized non-JSON child body.
    let inspected_bytes = SUPPORT_BUNDLE_TEXT_MAX_CHARS
        .saturating_mul(4)
        .saturating_add(4)
        .min(body.len());
    let body_prefix = String::from_utf8_lossy(&body[..inspected_bytes]);
    let mut chars = body_prefix.chars();
    let mut text = String::with_capacity(inspected_bytes.min(SUPPORT_BUNDLE_TEXT_MAX_CHARS));
    for _ in 0..SUPPORT_BUNDLE_TEXT_MAX_CHARS {
        let Some(ch) = chars.next() else {
            break;
        };
        text.push(ch);
    }
    if chars.next().is_some() || inspected_bytes < body.len() {
        text.push_str("...[truncated]");
    }
    text
}

struct SupportBundleLengthCounter<'a> {
    bytes: usize,
    execution: &'a tsink::QueryExecution,
    control_error: Option<tsink::QueryBudgetError>,
    fixed_limit_exceeded: bool,
}

impl IoWrite for SupportBundleLengthCounter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.execution.checkpoint() {
            self.control_error = Some(error);
            return Err(io::Error::other(
                "support-bundle JSON measurement was canceled",
            ));
        }
        let next_bytes = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("support-bundle JSON length overflowed usize"))?;
        if next_bytes > SUPPORT_BUNDLE_MAX_RESPONSE_BYTES {
            self.fixed_limit_exceeded = true;
            return Err(io::Error::other(
                "support-bundle JSON exceeded its fixed encoded-byte limit",
            ));
        }
        self.bytes = next_bytes;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct SupportBundleControlledWriter<'a, W> {
    inner: W,
    execution: &'a tsink::QueryExecution,
    control_error: Option<tsink::QueryBudgetError>,
}

impl<W: IoWrite> IoWrite for SupportBundleControlledWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.execution.checkpoint() {
            self.control_error = Some(error);
            return Err(io::Error::other(
                "support-bundle JSON serialization was canceled",
            ));
        }
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct PreparedSupportBundleResponse {
    response: HttpResponse,
    retained_bytes: u64,
}

fn support_bundle_error_response(
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

fn support_bundle_query_budget_error_response(error: &tsink::QueryBudgetError) -> HttpResponse {
    match error {
        tsink::QueryBudgetError::InvalidLimits(_) => support_bundle_error_response(
            400,
            "invalid_query_limits",
            "invalid_query_limits",
            "invalid support-bundle query limits",
            None,
        ),
        tsink::QueryBudgetError::LimitExceeded(exceeded) => {
            let error_code = format!("query_limit_{}", exceeded.reason.as_str());
            let retryable = matches!(
                exceeded.reason,
                tsink::QueryLimitReason::ConcurrentQueries
                    | tsink::QueryLimitReason::SharedMemoryBytes
            );
            support_bundle_error_response(
                if retryable { 429 } else { 413 },
                &error_code,
                &error_code,
                &format!(
                    "support-bundle composition exceeded the {} limit",
                    exceeded.reason.as_str()
                ),
                retryable.then_some("1"),
            )
        }
        tsink::QueryBudgetError::Cancelled => support_bundle_error_response(
            503,
            "canceled",
            "query_cancelled",
            "support-bundle composition was canceled",
            None,
        ),
        tsink::QueryBudgetError::DeadlineExceeded => support_bundle_error_response(
            503,
            "timeout",
            "query_deadline_exceeded",
            "support-bundle composition exceeded its deadline",
            None,
        ),
        _ => support_bundle_error_response(
            500,
            "execution",
            "support_bundle_query_budget_failed",
            "support-bundle query accounting failed",
            None,
        ),
    }
}

fn begin_support_bundle_execution(
    storage: &Arc<dyn Storage>,
    cancellation: tsink::QueryCancellationToken,
) -> Result<tsink::QueryExecution, HttpResponse> {
    match storage.begin_query_execution(tsink::QueryWorkLimits::default(), cancellation) {
        Ok(Some(execution)) => Ok(execution),
        Ok(None) => Err(support_bundle_error_response(
            500,
            "execution",
            "support_bundle_query_accounting_unavailable",
            "support-bundle composition requires query execution admission",
            None,
        )),
        Err(tsink::TsinkError::QueryBudget(error)) => {
            Err(support_bundle_query_budget_error_response(&error))
        }
        Err(_) => Err(support_bundle_error_response(
            500,
            "execution",
            "support_bundle_query_admission_failed",
            "support-bundle query admission failed",
            None,
        )),
    }
}

fn modeled_support_bundle_header_preflight_bytes(tenant_id: &str) -> u64 {
    let content_disposition_bytes = "attachment; filename=\"tsink-support-bundle--.json\""
        .len()
        .saturating_add(tenant_id.len().max("default".len()))
        .saturating_add(20);
    modeled_tsdb_status_vec_capacity_bytes::<(String, String)>(4).saturating_add(
        tsdb_status_saturating_u64_from_usize(
            "Content-Type".len()
                + "application/json".len()
                + "Cache-Control".len()
                + "no-store".len()
                + "Content-Disposition".len()
                + content_disposition_bytes,
        )
        .saturating_add(TSDB_STATUS_COLLECTION_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(6)),
    )
}

fn modeled_support_bundle_root_retained_bytes(
    tenant_id: &String,
    actor: &ClusterAuditActor,
) -> u64 {
    modeled_tsdb_status_string_capacity_bytes(tenant_id)
        .saturating_add(modeled_tsdb_status_string_capacity_bytes(&actor.id))
        .saturating_add(modeled_tsdb_status_string_capacity_bytes(&actor.auth_scope))
}

fn modeled_support_bundle_string_capacity_bytes(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    tsdb_status_saturating_u64_from_usize(capacity)
        .saturating_add(TSDB_STATUS_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_support_bundle_map_capacity_bytes(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    // HashMap capacity omits its control bytes and can be one element below the backing bucket
    // count. One extra tuple plus the standard collection allowance conservatively covers both
    // for this fixed two-entry map.
    modeled_tsdb_status_vec_capacity_bytes::<(String, String)>(capacity.saturating_add(1))
}

fn modeled_support_bundle_request_retained_bytes(request: &HttpRequest) -> u64 {
    modeled_support_bundle_string_capacity_bytes(request.method.capacity())
        .saturating_add(modeled_support_bundle_string_capacity_bytes(
            request.path.capacity(),
        ))
        .saturating_add(modeled_support_bundle_map_capacity_bytes(
            request.headers.capacity(),
        ))
        .saturating_add(request.headers.iter().fold(0u64, |bytes, (name, value)| {
            bytes
                .saturating_add(modeled_tsdb_status_string_capacity_bytes(name))
                .saturating_add(modeled_tsdb_status_string_capacity_bytes(value))
        }))
        .saturating_add(modeled_tsdb_status_vec_capacity_bytes::<u8>(
            request.body.capacity(),
        ))
}

fn support_bundle_raw_tenant_id(request: &HttpRequest) -> Option<&str> {
    request
        .raw_param("tenant")
        .or_else(|| request.raw_param("tenantId"))
        .or_else(|| request.raw_param("tenant_id"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SupportBundleDecodedTenantShape {
    leading_chars: usize,
    trimmed_chars: usize,
    trimmed_bytes: usize,
    trimmed_contains_control: bool,
}

fn support_bundle_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn support_bundle_percent_decode_into<'a>(encoded: &str, output: &'a mut [u8]) -> &'a [u8] {
    let encoded = encoded.as_bytes();
    let mut input_index = 0usize;
    let mut output_index = 0usize;
    while input_index < encoded.len() {
        let (byte, consumed) = if encoded[input_index] == b'+' {
            (b' ', 1)
        } else if encoded[input_index] == b'%' && input_index.saturating_add(2) < encoded.len() {
            match (
                support_bundle_hex_digit(encoded[input_index + 1]),
                support_bundle_hex_digit(encoded[input_index + 2]),
            ) {
                (Some(high), Some(low)) => (high << 4 | low, 3),
                _ => (encoded[input_index], 1),
            }
        } else {
            (encoded[input_index], 1)
        };
        output[output_index] = byte;
        output_index = output_index.saturating_add(1);
        input_index = input_index.saturating_add(consumed);
    }
    &output[..output_index]
}

fn for_each_support_bundle_lossy_char(bytes: &[u8], mut visit: impl FnMut(char)) {
    for chunk in bytes.utf8_chunks() {
        for character in chunk.valid().chars() {
            visit(character);
        }
        if !chunk.invalid().is_empty() {
            visit(char::REPLACEMENT_CHARACTER);
        }
    }
}

fn support_bundle_decoded_tenant_shape(bytes: &[u8]) -> SupportBundleDecodedTenantShape {
    let mut leading_chars = 0usize;
    let mut trimmed_chars = 0usize;
    let mut trimmed_bytes = 0usize;
    let mut trimmed_contains_control = false;
    let mut saw_non_whitespace = false;
    let mut pending_whitespace_chars = 0usize;
    let mut pending_whitespace_bytes = 0usize;
    let mut pending_whitespace_contains_control = false;

    for_each_support_bundle_lossy_char(bytes, |character| {
        if !saw_non_whitespace {
            if character.is_whitespace() {
                leading_chars = leading_chars.saturating_add(1);
                return;
            }
            saw_non_whitespace = true;
        } else if character.is_whitespace() {
            pending_whitespace_chars = pending_whitespace_chars.saturating_add(1);
            pending_whitespace_bytes =
                pending_whitespace_bytes.saturating_add(character.len_utf8());
            pending_whitespace_contains_control |= character.is_control();
            return;
        }

        trimmed_chars = trimmed_chars
            .saturating_add(pending_whitespace_chars)
            .saturating_add(1);
        trimmed_bytes = trimmed_bytes
            .saturating_add(pending_whitespace_bytes)
            .saturating_add(character.len_utf8());
        trimmed_contains_control |= pending_whitespace_contains_control || character.is_control();
        pending_whitespace_chars = 0;
        pending_whitespace_bytes = 0;
        pending_whitespace_contains_control = false;
    });

    SupportBundleDecodedTenantShape {
        leading_chars,
        trimmed_chars,
        trimmed_bytes,
        trimmed_contains_control,
    }
}

fn support_bundle_decoded_tenant_equals(
    bytes: &[u8],
    shape: SupportBundleDecodedTenantShape,
    expected: &str,
) -> bool {
    let mut expected = expected.trim().chars();
    let mut decoded_index = 0usize;
    let mut compared = 0usize;
    let mut equal = true;
    for_each_support_bundle_lossy_char(bytes, |character| {
        if decoded_index >= shape.leading_chars && compared < shape.trimmed_chars {
            equal &= expected.next() == Some(character);
            compared = compared.saturating_add(1);
        }
        decoded_index = decoded_index.saturating_add(1);
    });
    equal && compared == shape.trimmed_chars && expected.next().is_none()
}

fn support_bundle_tenant_length_error() -> HttpResponse {
    text_response(
        400,
        &format!(
            "{} must be <= {} bytes",
            tenant::TENANT_HEADER,
            tsink::label::MAX_LABEL_VALUE_LEN
        ),
    )
}

fn support_bundle_tenant_control_error() -> HttpResponse {
    text_response(
        400,
        &format!(
            "{} must not contain control characters",
            tenant::TENANT_HEADER
        ),
    )
}

fn validate_support_bundle_tenant_before_admission(
    request: &HttpRequest,
) -> Result<(), HttpResponse> {
    let raw_tenant_id = support_bundle_raw_tenant_id(request);
    if let Some(raw_tenant_id) = raw_tenant_id {
        let decoded_len = crate::http::percent_decoded_len(raw_tenant_id);
        if decoded_len > tsink::label::MAX_LABEL_VALUE_LEN {
            return Err(support_bundle_tenant_length_error());
        }

        // A fixed stack buffer preserves `percent_decode` plus `from_utf8_lossy` semantics without
        // creating request-owned heap state before the root query has been admitted. `utf8_chunks`
        // yields the same replacement-character boundaries as `String::from_utf8_lossy`.
        let mut decoded = [0u8; tsink::label::MAX_LABEL_VALUE_LEN];
        let decoded =
            support_bundle_percent_decode_into(raw_tenant_id, &mut decoded[..decoded_len]);
        let shape = support_bundle_decoded_tenant_shape(decoded);
        if shape.trimmed_chars > 0 {
            if let Some(scope_org_id) = request.header(tenant::SCOPE_ORG_ID_HEADER) {
                if !support_bundle_decoded_tenant_equals(decoded, shape, scope_org_id) {
                    return Err(text_response(
                        400,
                        &format!(
                            "{} and {} must match when both headers are set",
                            tenant::TENANT_HEADER,
                            tenant::SCOPE_ORG_ID_HEADER
                        ),
                    ));
                }
            }
            if shape.trimmed_bytes > tsink::label::MAX_LABEL_VALUE_LEN {
                return Err(support_bundle_tenant_length_error());
            }
            if shape.trimmed_contains_control {
                return Err(support_bundle_tenant_control_error());
            }
            return Ok(());
        }
    }

    let tenant_id = request.header(tenant::TENANT_HEADER).map(str::trim);
    let scope_org_id = request.header(tenant::SCOPE_ORG_ID_HEADER).map(str::trim);
    if matches!((tenant_id, scope_org_id), (Some(left), Some(right)) if left != right) {
        return Err(text_response(
            400,
            &format!(
                "{} and {} must match when both headers are set",
                tenant::TENANT_HEADER,
                tenant::SCOPE_ORG_ID_HEADER
            ),
        ));
    }
    let tenant_id = tenant_id
        .or(scope_org_id)
        .unwrap_or(tenant::DEFAULT_TENANT_ID)
        .trim();
    if tenant_id.is_empty() {
        return Err(text_response(
            400,
            &format!("{} must not be empty", tenant::TENANT_HEADER),
        ));
    }
    if tenant_id.len() > tsink::label::MAX_LABEL_VALUE_LEN {
        return Err(support_bundle_tenant_length_error());
    }
    if tenant_id.chars().any(char::is_control) {
        return Err(support_bundle_tenant_control_error());
    }
    Ok(())
}

fn with_validated_support_bundle_tenant_before_admission<T>(
    request: &HttpRequest,
    use_tenant: impl FnOnce(&str) -> T,
) -> Result<T, HttpResponse> {
    if let Some(raw_tenant_id) = support_bundle_raw_tenant_id(request) {
        let decoded_len = crate::http::percent_decoded_len(raw_tenant_id);
        if decoded_len > tsink::label::MAX_LABEL_VALUE_LEN {
            return Err(support_bundle_tenant_length_error());
        }

        let mut decoded = [0u8; tsink::label::MAX_LABEL_VALUE_LEN];
        let decoded =
            support_bundle_percent_decode_into(raw_tenant_id, &mut decoded[..decoded_len]);
        let shape = support_bundle_decoded_tenant_shape(decoded);
        if shape.trimmed_chars > 0 {
            if shape.trimmed_bytes > tsink::label::MAX_LABEL_VALUE_LEN {
                return Err(support_bundle_tenant_length_error());
            }

            let mut normalized = [0u8; tsink::label::MAX_LABEL_VALUE_LEN];
            let mut decoded_index = 0usize;
            let mut output_index = 0usize;
            let trimmed_end = shape.leading_chars.saturating_add(shape.trimmed_chars);
            for_each_support_bundle_lossy_char(decoded, |character| {
                if decoded_index >= shape.leading_chars && decoded_index < trimmed_end {
                    let encoded_len = character.len_utf8();
                    let next_output_index = output_index.saturating_add(encoded_len);
                    character.encode_utf8(&mut normalized[output_index..next_output_index]);
                    output_index = next_output_index;
                }
                decoded_index = decoded_index.saturating_add(1);
            });
            let tenant_id = std::str::from_utf8(&normalized[..output_index])
                .expect("support-bundle tenant normalization only writes valid UTF-8");
            return Ok(use_tenant(tenant_id));
        }
    }

    let tenant_id = request
        .header(tenant::TENANT_HEADER)
        .map(str::trim)
        .or_else(|| request.header(tenant::SCOPE_ORG_ID_HEADER).map(str::trim))
        .unwrap_or(tenant::DEFAULT_TENANT_ID);
    Ok(use_tenant(tenant_id))
}

fn initialize_support_bundle_tenant_runtime_before_admission(
    request: &HttpRequest,
    tenant_registry: Option<&tenant::TenantRegistry>,
) -> Result<(), HttpResponse> {
    let Some(tenant_registry) = tenant_registry else {
        return Ok(());
    };
    // Tenant runtimes own semaphores and a preallocated decision ring that survive the request.
    // Normalize the already-validated tenant in fixed stack buffers, so that persistent runtime
    // initialization is the only heap allocation before the root support-bundle query admits.
    // The ordinary child admission still happens later under that root execution, preserving
    // tenant/global admission ordering and counters.
    with_validated_support_bundle_tenant_before_admission(request, |tenant_id| {
        tenant_registry.initialize_tenant_runtime(tenant_id)
    })?
    .map_err(|error| error.to_http_response())
}

fn support_bundle_actor_output_lengths(request: &HttpRequest) -> (usize, usize) {
    if let Some(principal) = request
        .header(rbac::RBAC_AUTH_PRINCIPAL_ID_HEADER)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let auth_scope_len = request
            .header(rbac::RBAC_AUTH_PROVIDER_HEADER)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| "oidc:".len().saturating_add(value.len()))
            .or_else(|| {
                request
                    .header(rbac::RBAC_AUTH_METHOD_HEADER)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::len)
            })
            .unwrap_or("rbac".len());
        return (principal.len(), auth_scope_len);
    }
    if let Some(actor_id) = request
        .header(AUDIT_ACTOR_ID_HEADER)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return (actor_id.len(), "actor_header".len());
    }
    if let Some(forwarded_user) = request
        .header(AUDIT_FORWARDED_USER_HEADER)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return (forwarded_user.len(), "forwarded_user".len());
    }
    if let Some(node_id) = request
        .header("x-tsink-node-id")
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return (
            "node:".len().saturating_add(node_id.len()),
            "internal_node".len(),
        );
    }
    if extract_bearer_token(request).is_some() {
        return ("bearer:".len().saturating_add(16), "bearer".len());
    }
    if request.header(INTERNAL_RPC_AUTH_HEADER).is_some() {
        return ("internal_auth".len(), "internal_token".len());
    }
    ("unknown".len(), "unknown".len())
}

fn support_bundle_normalized_tenant_len_before_admission(request: &HttpRequest) -> usize {
    if let Some(raw_tenant_id) = support_bundle_raw_tenant_id(request) {
        let decoded_len = crate::http::percent_decoded_len(raw_tenant_id);
        if decoded_len <= tsink::label::MAX_LABEL_VALUE_LEN {
            let mut decoded = [0u8; tsink::label::MAX_LABEL_VALUE_LEN];
            let decoded =
                support_bundle_percent_decode_into(raw_tenant_id, &mut decoded[..decoded_len]);
            let shape = support_bundle_decoded_tenant_shape(decoded);
            if shape.trimmed_chars > 0 {
                return shape.trimmed_bytes;
            }
        }
    }

    request
        .header(tenant::TENANT_HEADER)
        .or_else(|| request.header(tenant::SCOPE_ORG_ID_HEADER))
        .map(str::trim)
        .unwrap_or(tenant::DEFAULT_TENANT_ID)
        .len()
}

fn modeled_support_bundle_setup_preflight_bytes(request: &HttpRequest) -> u64 {
    let final_tenant_len = support_bundle_normalized_tenant_len_before_admission(request);
    let (actor_id_len, actor_scope_len) = support_bundle_actor_output_lengths(request);

    // Tenant normalization uses fixed stack buffers. Actor construction and the selected tenant
    // clone allocate their exact output capacities, so this borrowed-input model only needs the
    // retained root strings plus the one controlled synthetic request.
    let root_strings = modeled_support_bundle_string_capacity_bytes(final_tenant_len)
        .saturating_add(modeled_support_bundle_string_capacity_bytes(actor_id_len))
        .saturating_add(modeled_support_bundle_string_capacity_bytes(
            actor_scope_len,
        ));
    let synthetic_request = modeled_support_bundle_string_capacity_bytes("GET".len())
        .saturating_add(modeled_support_bundle_string_capacity_bytes(
            SUPPORT_BUNDLE_TSDB_STATUS_PATH.len(),
        ))
        .saturating_add(modeled_support_bundle_map_capacity_bytes(3))
        .saturating_add(modeled_support_bundle_string_capacity_bytes(
            rbac::RBAC_AUTH_VERIFIED_HEADER.len(),
        ))
        .saturating_add(modeled_support_bundle_string_capacity_bytes("true".len()))
        .saturating_add(modeled_support_bundle_string_capacity_bytes(
            tenant::TENANT_HEADER.len(),
        ))
        .saturating_add(modeled_support_bundle_string_capacity_bytes(
            final_tenant_len,
        ));

    root_strings.saturating_add(synthetic_request)
}

#[derive(Debug)]
struct PreparedSupportBundleSetup {
    // Field declaration order is intentional: Rust drops struct fields in declaration order, so
    // every owned allocation is destroyed before its accounting guard on all early-return paths.
    tenant_id: String,
    actor: ClusterAuditActor,
    synthetic_request: Option<HttpRequest>,
    reservation: tsink::QueryMemoryReservation,
    root_retained_bytes: u64,
    synthetic_retained_bytes: u64,
}

fn prepare_support_bundle_setup(
    request: &HttpRequest,
    execution: &tsink::QueryExecution,
) -> Result<PreparedSupportBundleSetup, HttpResponse> {
    let setup_preflight_bytes = modeled_support_bundle_setup_preflight_bytes(request);
    let mut reservation = execution
        .reserve_memory(setup_preflight_bytes)
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;

    let tenant_id = with_validated_support_bundle_tenant_before_admission(request, |tenant_id| {
        let mut owned = String::with_capacity(tenant_id.len());
        owned.push_str(tenant_id);
        owned
    })?;
    let actor = derive_audit_actor(request);
    // Build the historical child-request shape from an empty controlled source, without copying
    // arbitrary original headers (Authorization, cookies, tracing, and similar values) or the
    // potentially 64 MiB request body.
    let synthetic_request = support_bundle_request_with_path(
        &HttpRequest {
            method: String::new(),
            path: String::new(),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        },
        SUPPORT_BUNDLE_TSDB_STATUS_PATH,
        &tenant_id,
    );

    let root_retained_bytes = modeled_support_bundle_root_retained_bytes(&tenant_id, &actor);
    let synthetic_retained_bytes =
        modeled_support_bundle_request_retained_bytes(&synthetic_request);
    reservation
        .resize(root_retained_bytes.saturating_add(synthetic_retained_bytes))
        .expect("shrinking a support-bundle setup reservation cannot fail");
    Ok(PreparedSupportBundleSetup {
        tenant_id,
        actor,
        synthetic_request: Some(synthetic_request),
        reservation,
        root_retained_bytes,
        synthetic_retained_bytes,
    })
}

#[cfg(test)]
fn account_support_bundle_child(
    response: HttpResponse,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response_retained_bytes = modeled_tsdb_status_response_retained_bytes(&response);
    let next = retained_bytes
        .checked_add(response_retained_bytes)
        .ok_or_else(|| {
            support_bundle_error_response(
                413,
                "support_bundle_child_responses_too_large",
                "support_bundle_child_responses_too_large",
                "support-bundle child responses exceed the fixed retained-byte envelope",
                None,
            )
        })?;
    if next > SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES {
        drop(response);
        return Err(support_bundle_error_response(
            413,
            "support_bundle_child_responses_too_large",
            "support_bundle_child_responses_too_large",
            "support-bundle child responses exceed the fixed retained-byte envelope",
            None,
        ));
    }
    let accounted = account_completed_http_response(response, execution)
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    *retained_bytes = next;
    debug_assert_eq!(accounted.reservation.bytes(), response_retained_bytes);
    Ok(accounted)
}

fn admit_accounted_support_bundle_child(
    accounted: AccountedHttpResponse,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response_retained_bytes = modeled_tsdb_status_response_retained_bytes(&accounted.response);
    if accounted.reservation.bytes() != response_retained_bytes {
        drop(accounted);
        return Err(support_bundle_error_response(
            500,
            "execution",
            "support_bundle_child_response_accounting_invalid",
            "support-bundle child response returned a non-exact reservation",
            None,
        ));
    }
    let next = retained_bytes
        .checked_add(response_retained_bytes)
        .filter(|next| *next <= SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES)
        .ok_or_else(|| {
            support_bundle_error_response(
                413,
                "support_bundle_child_responses_too_large",
                "support_bundle_child_responses_too_large",
                "support-bundle child responses exceed the fixed retained-byte envelope",
                None,
            )
        })?;
    *retained_bytes = next;
    Ok(accounted)
}

#[derive(Clone, Copy)]
struct AdmittedSupportBundleCompatibilityScratch;

fn admit_support_bundle_compatibility_scratch(
    root_reservation: &tsink::QueryMemoryReservation,
) -> Result<AdmittedSupportBundleCompatibilityScratch, HttpResponse> {
    if root_reservation.bytes() < SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES {
        return Err(support_bundle_error_response(
            500,
            "execution",
            "support_bundle_compatibility_scratch_missing",
            "support-bundle compatibility error scratch was not admitted",
            None,
        ));
    }
    Ok(AdmittedSupportBundleCompatibilityScratch)
}

fn transfer_support_bundle_scratch_child(
    response: HttpResponse,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
    error_surface: SupportBundleScratchErrorSurface,
    _scratch: AdmittedSupportBundleCompatibilityScratch,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let response_retained_bytes = modeled_tsdb_status_response_retained_bytes(&response);
    let construction_upper_bytes =
        modeled_support_bundle_compatibility_error_construction_bytes(&response);
    if response_retained_bytes > SUPPORT_BUNDLE_COMPATIBILITY_RAW_MAX_RETAINED_BYTES
        || construction_upper_bytes > SUPPORT_BUNDLE_COMPATIBILITY_RAW_SCRATCH_BYTES
    {
        drop(response);
        return Err(support_bundle_error_response(
            500,
            "execution",
            "support_bundle_compatibility_error_exceeds_scratch",
            "support-bundle compatibility error exceeds its pre-admitted scratch envelope",
            None,
        ));
    }
    // TSDB-status and rebalance compatibility errors are fixed/bounded: the raw response contract
    // gets the input-derived RAW_SCRATCH share (including the capped 16 KiB tenant identifier),
    // and the fixed budget-error mappers get the remaining MAPPER_SCRATCH share while the raw
    // response is still live. The parent reserves their sum before either wrapper call. Transfer
    // ownership to an exact same-execution guard while that root scratch remains live; later
    // children may then reuse the scratch without leaving retained response bytes uncharged.
    let reservation = execution
        .reserve_memory(response_retained_bytes)
        .map_err(|error| support_bundle_scratch_transfer_budget_error(error_surface, error))?;
    admit_accounted_support_bundle_child(
        AccountedHttpResponse {
            response,
            reservation,
        },
        retained_bytes,
    )
}

fn modeled_support_bundle_compatibility_error_construction_bytes(response: &HttpResponse) -> u64 {
    // The legacy helpers build at most one small serde_json value tree and one response body.
    // Eight times the retained response plus 64 KiB covers the duplicated scalar/string payload,
    // map/vector nodes, allocator rounding, and serializer bookkeeping. The longest reachable
    // input is the already-capped 16 KiB tenant identifier.
    modeled_tsdb_status_response_retained_bytes(response)
        .saturating_mul(8)
        .saturating_add(SUPPORT_BUNDLE_COMPATIBILITY_ERROR_FIXED_SCRATCH_BYTES)
}

#[derive(Clone, Copy)]
enum SupportBundleScratchErrorSurface {
    TsdbStatus,
    ClusterRebalance,
}

fn support_bundle_scratch_transfer_budget_error(
    error_surface: SupportBundleScratchErrorSurface,
    error: tsink::QueryBudgetError,
) -> HttpResponse {
    let response = match error_surface {
        SupportBundleScratchErrorSurface::TsdbStatus => {
            tsdb_status_query_budget_error_response(&error)
        }
        SupportBundleScratchErrorSurface::ClusterRebalance => {
            admin_rebalance_response_error_response(
                AdminRebalanceResponseError::Budget(error),
                None,
            )
        }
    };
    debug_assert!(
        modeled_tsdb_status_response_retained_bytes(&response).saturating_mul(8)
            <= SUPPORT_BUNDLE_COMPATIBILITY_MAPPER_SCRATCH_BYTES,
        "support-bundle compatibility budget mapper exceeded its fixed scratch contract"
    );
    response
}

struct SupportBundleChildJsonLengthCounter<'a> {
    bytes: usize,
    maximum: usize,
    execution: &'a tsink::QueryExecution,
    control_error: Option<tsink::QueryBudgetError>,
    fixed_limit_exceeded: bool,
}

impl IoWrite for SupportBundleChildJsonLengthCounter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.execution.checkpoint() {
            self.control_error = Some(error);
            return Err(io::Error::other(
                "support-bundle child JSON measurement was canceled",
            ));
        }
        let next = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("support-bundle child JSON length overflowed usize"))?;
        if next > self.maximum {
            self.fixed_limit_exceeded = true;
            return Err(io::Error::other(
                "support-bundle child JSON exceeded its retained-byte envelope",
            ));
        }
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum SupportBundleChildJsonMeasurementError {
    QueryBudget(tsink::QueryBudgetError),
    FixedLimitExceeded,
    Serialization,
}

enum SupportBundleChildJsonSerializationError {
    QueryBudget(tsink::QueryBudgetError),
    FixedLimitExceeded,
    Measurement,
    Allocation,
    Serialization,
    LengthChanged,
}

impl SupportBundleChildJsonSerializationError {
    fn into_http_response(self) -> HttpResponse {
        match self {
            Self::QueryBudget(error) => support_bundle_query_budget_error_response(&error),
            Self::FixedLimitExceeded => support_bundle_child_retained_limit_error(),
            Self::Measurement => support_bundle_error_response(
                500,
                "execution",
                "support_bundle_child_json_measurement_failed",
                "support-bundle child JSON measurement failed",
                None,
            ),
            Self::Allocation => support_bundle_error_response(
                500,
                "execution",
                "support_bundle_child_json_allocation_failed",
                "support-bundle child JSON allocation failed",
                None,
            ),
            Self::Serialization => support_bundle_error_response(
                500,
                "execution",
                "support_bundle_child_json_serialization_failed",
                "support-bundle child JSON serialization failed",
                None,
            ),
            Self::LengthChanged => support_bundle_error_response(
                500,
                "execution",
                "support_bundle_child_json_length_changed",
                "support-bundle child JSON length changed after admission",
                None,
            ),
        }
    }
}

fn measure_support_bundle_json_child(
    value: &impl Serialize,
    execution: &tsink::QueryExecution,
    maximum_body_bytes: usize,
) -> Result<usize, SupportBundleChildJsonMeasurementError> {
    let mut counter = SupportBundleChildJsonLengthCounter {
        bytes: 0,
        maximum: maximum_body_bytes,
        execution,
        control_error: None,
        fixed_limit_exceeded: false,
    };
    if serde_json::to_writer(&mut counter, value).is_err() {
        return Err(match counter.control_error {
            Some(error) => SupportBundleChildJsonMeasurementError::QueryBudget(error),
            None if counter.fixed_limit_exceeded => {
                SupportBundleChildJsonMeasurementError::FixedLimitExceeded
            }
            None => SupportBundleChildJsonMeasurementError::Serialization,
        });
    }
    Ok(counter.bytes)
}

struct SupportBundleChildControlledJsonWriter<'a, W> {
    inner: W,
    execution: &'a tsink::QueryExecution,
    control_error: Option<tsink::QueryBudgetError>,
}

impl<W: IoWrite> IoWrite for SupportBundleChildControlledJsonWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.execution.checkpoint() {
            self.control_error = Some(error);
            return Err(io::Error::other(
                "support-bundle child JSON serialization was canceled",
            ));
        }
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn support_bundle_child_retained_limit_error() -> HttpResponse {
    support_bundle_error_response(
        413,
        "support_bundle_child_responses_too_large",
        "support_bundle_child_responses_too_large",
        "support-bundle child responses exceed the fixed retained-byte envelope",
        None,
    )
}

fn serialize_support_bundle_json_child_inner(
    status: u16,
    value: &impl Serialize,
    execution: &tsink::QueryExecution,
    maximum_retained_bytes: u64,
    include_content_type: bool,
) -> Result<AccountedHttpResponse, SupportBundleChildJsonSerializationError> {
    let maximum_body_bytes = usize::try_from(maximum_retained_bytes).unwrap_or(usize::MAX);
    let body_len = measure_support_bundle_json_child(value, execution, maximum_body_bytes)
        .map_err(|error| match error {
            SupportBundleChildJsonMeasurementError::QueryBudget(error) => {
                SupportBundleChildJsonSerializationError::QueryBudget(error)
            }
            SupportBundleChildJsonMeasurementError::FixedLimitExceeded => {
                SupportBundleChildJsonSerializationError::FixedLimitExceeded
            }
            SupportBundleChildJsonMeasurementError::Serialization => {
                SupportBundleChildJsonSerializationError::Measurement
            }
        })?;
    let header_preflight_bytes = if include_content_type {
        modeled_tsdb_status_header_preflight_bytes()
    } else {
        0
    };
    let preflight_bytes = modeled_tsdb_status_vec_capacity_bytes::<u8>(body_len)
        .saturating_add(header_preflight_bytes);
    if preflight_bytes > maximum_retained_bytes {
        return Err(SupportBundleChildJsonSerializationError::FixedLimitExceeded);
    }

    // Source-projection guards owned by the caller remain live throughout both passes. Reserve
    // the complete response allocation before asking the allocator for the body or header vector.
    let mut reservation = execution
        .reserve_memory(preflight_bytes)
        .map_err(SupportBundleChildJsonSerializationError::QueryBudget)?;
    let mut body = Vec::new();
    body.try_reserve_exact(body_len)
        .map_err(|_| SupportBundleChildJsonSerializationError::Allocation)?;
    let allocated_preflight_bytes = modeled_tsdb_status_vec_capacity_bytes::<u8>(body.capacity())
        .saturating_add(header_preflight_bytes);
    if allocated_preflight_bytes > maximum_retained_bytes {
        drop(body);
        return Err(SupportBundleChildJsonSerializationError::FixedLimitExceeded);
    }
    reservation
        .resize(allocated_preflight_bytes)
        .map_err(SupportBundleChildJsonSerializationError::QueryBudget)?;
    body.resize(body_len, 0);
    let written = {
        let cursor = io::Cursor::new(body.as_mut_slice());
        let mut writer = SupportBundleChildControlledJsonWriter {
            inner: cursor,
            execution,
            control_error: None,
        };
        if serde_json::to_writer(&mut writer, value).is_err() {
            return Err(match writer.control_error {
                Some(error) => SupportBundleChildJsonSerializationError::QueryBudget(error),
                None => SupportBundleChildJsonSerializationError::Serialization,
            });
        }
        usize::try_from(writer.inner.position()).unwrap_or(usize::MAX)
    };
    if written != body_len {
        return Err(SupportBundleChildJsonSerializationError::LengthChanged);
    }

    let response = if include_content_type {
        HttpResponse::new(status, body).with_header("Content-Type", "application/json")
    } else {
        HttpResponse::new(status, body)
    };
    let retained_bytes = modeled_tsdb_status_response_retained_bytes(&response);
    if retained_bytes > maximum_retained_bytes {
        drop(response);
        return Err(SupportBundleChildJsonSerializationError::FixedLimitExceeded);
    }
    reservation
        .resize(retained_bytes)
        .map_err(SupportBundleChildJsonSerializationError::QueryBudget)?;
    Ok(AccountedHttpResponse {
        response,
        reservation,
    })
}

fn serialize_support_bundle_json_child(
    status: u16,
    value: &impl Serialize,
    execution: &tsink::QueryExecution,
    maximum_retained_bytes: u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    serialize_support_bundle_json_child_inner(
        status,
        value,
        execution,
        maximum_retained_bytes,
        true,
    )
    .map_err(SupportBundleChildJsonSerializationError::into_http_response)
}

fn serialize_support_bundle_json_child_without_content_type(
    status: u16,
    value: &impl Serialize,
    execution: &tsink::QueryExecution,
    maximum_retained_bytes: u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    serialize_support_bundle_json_child_inner(
        status,
        value,
        execution,
        maximum_retained_bytes,
        false,
    )
    .map_err(SupportBundleChildJsonSerializationError::into_http_response)
}

fn serialize_support_bundle_text_child_inner(
    status: u16,
    value: &str,
    execution: &tsink::QueryExecution,
    maximum_retained_bytes: u64,
    include_content_type: bool,
) -> Result<AccountedHttpResponse, HttpResponse> {
    execution
        .checkpoint()
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    let header_preflight_bytes = if include_content_type {
        modeled_single_header_preflight_bytes("Content-Type", "text/plain")
    } else {
        0
    };
    let requested_bytes = modeled_tsdb_status_vec_capacity_bytes::<u8>(value.len())
        .saturating_add(header_preflight_bytes);
    if requested_bytes > maximum_retained_bytes {
        return Err(support_bundle_child_retained_limit_error());
    }
    let mut reservation = execution
        .reserve_memory(requested_bytes)
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    execution
        .checkpoint()
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;

    let mut body = Vec::new();
    body.try_reserve_exact(value.len()).map_err(|_| {
        support_bundle_error_response(
            500,
            "execution",
            "support_bundle_child_text_allocation_failed",
            "support-bundle child text allocation failed",
            None,
        )
    })?;
    let allocated_bytes = modeled_tsdb_status_vec_capacity_bytes::<u8>(body.capacity())
        .saturating_add(header_preflight_bytes);
    if allocated_bytes > maximum_retained_bytes {
        drop(body);
        return Err(support_bundle_child_retained_limit_error());
    }
    reservation
        .resize(allocated_bytes)
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    execution
        .checkpoint()
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    body.extend_from_slice(value.as_bytes());

    let response = if include_content_type {
        HttpResponse::new(status, body).with_header("Content-Type", "text/plain")
    } else {
        HttpResponse::new(status, body)
    };
    let retained_bytes = modeled_tsdb_status_response_retained_bytes(&response);
    debug_assert_eq!(retained_bytes, allocated_bytes);
    reservation
        .resize(retained_bytes)
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    Ok(AccountedHttpResponse {
        response,
        reservation,
    })
}

fn serialize_support_bundle_text_child(
    status: u16,
    value: &str,
    execution: &tsink::QueryExecution,
    maximum_retained_bytes: u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    serialize_support_bundle_text_child_inner(
        status,
        value,
        execution,
        maximum_retained_bytes,
        false,
    )
}

fn serialize_support_bundle_text_child_with_content_type(
    status: u16,
    value: &str,
    execution: &tsink::QueryExecution,
    maximum_retained_bytes: u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    serialize_support_bundle_text_child_inner(
        status,
        value,
        execution,
        maximum_retained_bytes,
        true,
    )
}

fn prepare_support_bundle_response(
    bundle: &SupportBundleEnvelope<'_>,
    execution: &tsink::QueryExecution,
    reservation: &mut tsink::QueryMemoryReservation,
    base_reserved_bytes: u64,
) -> Result<PreparedSupportBundleResponse, HttpResponse> {
    let mut counter = SupportBundleLengthCounter {
        bytes: 0,
        execution,
        control_error: None,
        fixed_limit_exceeded: false,
    };
    if serde_json::to_writer_pretty(&mut counter, bundle).is_err() {
        return Err(match counter.control_error {
            Some(error) => support_bundle_query_budget_error_response(&error),
            None if counter.fixed_limit_exceeded => support_bundle_error_response(
                413,
                "query_limit_returned_bytes",
                "query_limit_returned_bytes",
                "support-bundle response exceeds the fixed encoded-byte limit",
                None,
            ),
            None => support_bundle_error_response(
                500,
                "execution",
                "support_bundle_json_measurement_failed",
                "support-bundle JSON measurement failed",
                None,
            ),
        });
    }
    let body_len = counter.bytes;
    execution
        .charge_returned_bytes(tsdb_status_saturating_u64_from_usize(body_len))
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    reservation
        .resize(
            base_reserved_bytes
                .saturating_add(modeled_tsdb_status_vec_capacity_bytes::<u8>(body_len)),
        )
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;

    let mut body = Vec::new();
    body.try_reserve_exact(body_len).map_err(|_| {
        support_bundle_error_response(
            500,
            "execution",
            "support_bundle_json_allocation_failed",
            "support-bundle JSON allocation failed",
            None,
        )
    })?;
    reservation
        .resize(
            base_reserved_bytes.saturating_add(modeled_tsdb_status_vec_capacity_bytes::<u8>(
                body.capacity(),
            )),
        )
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    body.resize(body_len, 0);
    let written = {
        let cursor = io::Cursor::new(body.as_mut_slice());
        let mut writer = SupportBundleControlledWriter {
            inner: cursor,
            execution,
            control_error: None,
        };
        if serde_json::to_writer_pretty(&mut writer, bundle).is_err() {
            return Err(match writer.control_error {
                Some(error) => support_bundle_query_budget_error_response(&error),
                None => support_bundle_error_response(
                    500,
                    "execution",
                    "support_bundle_json_serialization_failed",
                    "support-bundle JSON serialization failed",
                    None,
                ),
            });
        }
        usize::try_from(writer.inner.position()).unwrap_or(usize::MAX)
    };
    if written != body_len {
        return Err(support_bundle_error_response(
            500,
            "execution",
            "support_bundle_json_length_changed",
            "support-bundle JSON length changed after admission",
            None,
        ));
    }

    let filename_tenant = support_bundle_filename_component(bundle.tenant_id);
    let response = HttpResponse::new(200, body)
        .with_header("Content-Type", "application/json")
        .with_header("Cache-Control", "no-store")
        .with_header(
            "Content-Disposition",
            format!(
                "attachment; filename=\"tsink-support-bundle-{filename_tenant}-{}.json\"",
                bundle.generated_unix_ms
            ),
        );
    let retained_bytes = modeled_tsdb_status_response_retained_bytes(&response);
    reservation
        .resize(base_reserved_bytes.saturating_add(retained_bytes))
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    Ok(PreparedSupportBundleResponse {
        response,
        retained_bytes,
    })
}

#[derive(Serialize)]
struct SupportBundleUsagePayload<'a> {
    status: &'static str,
    data: SupportBundleUsageData<'a>,
}

#[derive(Serialize)]
struct SupportBundleUsageData<'a> {
    report: SupportBundleUsageReport<'a>,
    journal: &'a crate::usage::UsageLedgerStatus,
    reconciliation: SupportBundleUsageReconciliation,
}

#[derive(Serialize)]
struct SupportBundleUsageReport<'a> {
    filter: SupportBundleUsageReportFilter<'a>,
    journal: &'a crate::usage::UsageLedgerStatus,
    page: SupportBundleUsageReadPage,
    tenants: SupportBundleUsageTenants<'a>,
    buckets: &'static [crate::usage::UsageBucketSummary],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleUsageReportFilter<'a> {
    tenant_id: &'a str,
    bucket_width: UsageBucketWidth,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleUsageReadPage {
    snapshot_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    earliest_available_sequence: Option<u64>,
    records_aggregated: u64,
    limit: usize,
    has_more: bool,
    all_time_exact: bool,
    raw_history_complete: bool,
}

struct SupportBundleUsageTenants<'a> {
    current: Option<&'a crate::usage::UsageTenantSummary>,
}

impl Serialize for SupportBundleUsageTenants<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(usize::from(self.current.is_some())))?;
        if let Some(current) = self.current {
            sequence.serialize_element(current)?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleUsageReconciliation {
    // `usage_reconciliation_json` historically encoded a `serde_json::Map`; keep its sorted key
    // order so the borrowed serializer preserves the exact child body as well as its schema.
    accounted: SupportBundleUsageReconciliationAccounted,
    latest_storage_reconciled_unix_ms: Option<u64>,
    runtime: SupportBundleUsageReconciliationRuntime,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleUsageReconciliationAccounted {
    background_events_total: u64,
    ingest_rows_total: u64,
    latest_storage_logical_bytes: u64,
    query_result_units_total: u64,
    retention_tombstones_applied_total: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleUsageReconciliationRuntime {
    background_errors_total: u64,
    degraded: bool,
    expired_segments_total: u64,
    query_points_returned_total: u64,
    rollup_points_materialized_total: u64,
    wal_append_points_total: u64,
}

#[derive(Serialize)]
struct SupportBundleUsageLimitResponse {
    // `json!` backed the historical response and sorted these map keys lexicographically.
    error: SupportBundleUsageLimitError,
    status: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleUsageLimitError {
    code: &'static str,
    maximum_bytes: usize,
    message: SupportBundleUsageLimitMessage,
}

struct SupportBundleUsageLimitMessage(usize);

impl std::fmt::Display for SupportBundleUsageLimitMessage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "usage response exceeds the configured maximum {} bytes",
            self.0
        )
    }
}

impl Serialize for SupportBundleUsageLimitMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

fn support_bundle_usage_child(
    storage: &Arc<dyn Storage>,
    usage_accounting: Option<&UsageAccounting>,
    tenant_id: &str,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
    let Some(usage_accounting) = usage_accounting else {
        let response = serialize_support_bundle_text_child(
            503,
            "usage accounting is unavailable",
            execution,
            remaining,
        )?;
        return admit_accounted_support_bundle_child(response, retained_bytes);
    };

    let maximum_usage_body_bytes = usage_accounting.limits().report_max_response_bytes;
    let usage = usage_accounting
        .status_snapshot_for_with_execution(tenant_id, execution)
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    let journal = usage_accounting
        .ledger_status_with_execution(execution)
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    let observability = match storage.status_observability_snapshot_with_execution(execution) {
        Ok(observability) => observability,
        Err(tsink::TsinkError::QueryBudget(error)) => {
            return Err(support_bundle_query_budget_error_response(&error))
        }
        Err(tsink::TsinkError::UnsupportedOperation { .. }) => {
            return Err(support_bundle_error_response(
                500,
                "execution",
                "support_bundle_usage_accounting_unavailable",
                "support-bundle usage status requires query-accounted storage observability",
                None,
            ))
        }
        Err(_) => {
            return Err(support_bundle_error_response(
                500,
                "execution",
                "support_bundle_usage_snapshot_failed",
                "support-bundle usage storage snapshot failed",
                None,
            ))
        }
    };
    let current = usage
        .current_tenant_present
        .then_some(&usage.current_tenant);
    let reconciliation = &usage.reconciliation;
    let runtime_query_points_returned_total = observability
        .query
        .select_points_returned_total
        .saturating_add(
            observability
                .query
                .select_with_options_points_returned_total,
        )
        .saturating_add(observability.query.select_all_points_returned_total);
    let reconciliation = SupportBundleUsageReconciliation {
        accounted: SupportBundleUsageReconciliationAccounted {
            background_events_total: reconciliation.background_events_total,
            ingest_rows_total: reconciliation.ingest_rows_total,
            latest_storage_logical_bytes: reconciliation.latest_storage_logical_bytes,
            query_result_units_total: reconciliation.query_result_units_total,
            retention_tombstones_applied_total: reconciliation.retention_tombstones_applied_total,
        },
        latest_storage_reconciled_unix_ms: reconciliation.latest_storage_reconciled_unix_ms,
        runtime: SupportBundleUsageReconciliationRuntime {
            background_errors_total: observability.health.background_errors_total,
            degraded: observability.health.degraded,
            expired_segments_total: observability.flush.expired_segments_total,
            query_points_returned_total: runtime_query_points_returned_total,
            rollup_points_materialized_total: observability.rollups.points_materialized_total,
            wal_append_points_total: observability.wal.append_points_total,
        },
    };
    // The legacy full storage snapshot was a temporary used only to build scalar reconciliation
    // JSON. Release the conservative full-schema projection before any body measurement or
    // allocation; the typed reconciliation above owns every value the response still needs.
    drop(observability);

    let success_response = {
        let payload = SupportBundleUsagePayload {
            status: "success",
            data: SupportBundleUsageData {
                report: SupportBundleUsageReport {
                    filter: SupportBundleUsageReportFilter {
                        tenant_id,
                        bucket_width: UsageBucketWidth::None,
                    },
                    journal: &usage.journal,
                    page: SupportBundleUsageReadPage {
                        snapshot_sequence: usage.journal.last_sequence,
                        earliest_available_sequence: usage.journal.earliest_retained_sequence,
                        records_aggregated: usage.current_tenant_records_total,
                        limit: usage.journal.limits.report_max_records,
                        has_more: false,
                        all_time_exact: true,
                        raw_history_complete: usage.journal.records_total
                            == usage.journal.retained_records,
                    },
                    tenants: SupportBundleUsageTenants { current },
                    buckets: &[],
                },
                journal: &journal,
                reconciliation,
            },
        };

        match measure_support_bundle_json_child(&payload, execution, maximum_usage_body_bytes) {
            Ok(_) => Some(serialize_support_bundle_json_child_without_content_type(
                200, &payload, execution, remaining,
            )?),
            Err(SupportBundleChildJsonMeasurementError::FixedLimitExceeded) => None,
            Err(SupportBundleChildJsonMeasurementError::QueryBudget(error)) => {
                return Err(support_bundle_query_budget_error_response(&error))
            }
            Err(SupportBundleChildJsonMeasurementError::Serialization) => {
                return Err(support_bundle_error_response(
                    500,
                    "execution",
                    "support_bundle_usage_json_measurement_failed",
                    "support-bundle usage JSON measurement failed",
                    None,
                ))
            }
        }
    };
    if let Some(response) = success_response {
        return admit_accounted_support_bundle_child(response, retained_bytes);
    }

    drop(journal);
    drop(usage);
    let error = SupportBundleUsageLimitResponse {
        error: SupportBundleUsageLimitError {
            code: "usage_report_response_too_large",
            maximum_bytes: maximum_usage_body_bytes,
            message: SupportBundleUsageLimitMessage(maximum_usage_body_bytes),
        },
        status: "error",
    };
    let response = serialize_support_bundle_json_child_without_content_type(
        413, &error, execution, remaining,
    )?;
    admit_accounted_support_bundle_child(response, retained_bytes)
}

fn support_bundle_rbac_state_child(
    rbac_registry: Option<&RbacRegistry>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    #[derive(Serialize)]
    struct RbacStateResponse<T> {
        // The legacy `json!` response sorted its object keys.
        data: T,
        status: &'static str,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct DisabledRbacState {
        audit_entries: usize,
        enabled: bool,
        last_loaded_unix_ms: u64,
        oidc_providers: &'static [crate::rbac::RbacOidcProviderSnapshot],
        principals: &'static [crate::rbac::RbacPrincipalSnapshot],
        roles: &'static [crate::rbac::RbacRoleSnapshot],
        service_accounts: &'static [crate::rbac::RbacServiceAccountSnapshot],
        source_path: Option<&'static str>,
    }

    let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
    let response = if let Some(registry) = rbac_registry {
        let snapshot = match registry.borrowed_state_snapshot_with_execution(execution) {
            Ok(snapshot) => snapshot,
            Err(crate::rbac::RbacStateSnapshotError::QueryBudget(error)) => {
                return Err(support_bundle_query_budget_error_response(&error))
            }
            Err(
                crate::rbac::RbacStateSnapshotError::StateLockPoisoned
                | crate::rbac::RbacStateSnapshotError::AuditLockPoisoned,
            ) => {
                return Err(support_bundle_error_response(
                    500,
                    "execution",
                    "support_bundle_rbac_state_snapshot_failed",
                    "support-bundle RBAC state snapshot failed",
                    None,
                ))
            }
        };
        let value = RbacStateResponse {
            data: &snapshot,
            status: "success",
        };
        serialize_support_bundle_json_child(200, &value, execution, remaining)?
    } else {
        let value = RbacStateResponse {
            data: DisabledRbacState {
                audit_entries: 0,
                enabled: false,
                last_loaded_unix_ms: 0,
                oidc_providers: &[],
                principals: &[],
                roles: &[],
                service_accounts: &[],
                source_path: None,
            },
            status: "success",
        };
        serialize_support_bundle_json_child(200, &value, execution, remaining)?
    };
    // The configured branch releases both RBAC locks after the serializer's second pass and
    // before admitting the completed response or collecting later children.
    admit_accounted_support_bundle_child(response, retained_bytes)
}

fn support_bundle_rbac_audit_child(
    rbac_registry: Option<&RbacRegistry>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    #[derive(Serialize)]
    struct RbacAuditResponse<'a> {
        status: &'static str,
        data: RbacAuditData<'a>,
    }

    #[derive(Serialize)]
    struct RbacAuditData<'a> {
        entries: &'a [crate::rbac::RbacAuditEntry],
    }

    let snapshot = match rbac_registry
        .map(|registry| {
            registry.audit_snapshot_with_execution(SUPPORT_BUNDLE_RBAC_AUDIT_LIMIT, execution)
        })
        .transpose()
    {
        Ok(snapshot) => snapshot,
        Err(crate::rbac::RbacAuditSnapshotError::QueryBudget(error)) => {
            return Err(support_bundle_query_budget_error_response(&error))
        }
        Err(crate::rbac::RbacAuditSnapshotError::AuditLockPoisoned) => {
            return Err(support_bundle_error_response(
                500,
                "execution",
                "support_bundle_rbac_audit_snapshot_failed",
                "support-bundle RBAC audit snapshot failed",
                None,
            ))
        }
    };
    let value = RbacAuditResponse {
        status: "success",
        data: RbacAuditData {
            entries: snapshot.as_deref().unwrap_or_default(),
        },
    };
    let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
    let response = serialize_support_bundle_json_child(200, &value, execution, remaining)?;
    admit_accounted_support_bundle_child(response, retained_bytes)
}

fn support_bundle_security_state_child(
    rbac_registry: Option<&RbacRegistry>,
    security_manager: Option<&SecurityManager>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    #[derive(Serialize)]
    struct SecurityResponse<'a> {
        status: &'static str,
        data: SecurityData<'a>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct SecurityData<'a> {
        enabled: bool,
        targets: &'a [crate::security::SecurityTargetSnapshot],
        audit_entries: &'a [crate::security::SecurityAuditEntry],
        // Deliberately do not skip `None`: the established support child schema emits `null`.
        service_accounts: Option<ServiceAccountRotationSummary>,
    }

    let snapshot = match security_manager
        .map(|manager| manager.state_snapshot_with_execution(rbac_registry, execution))
        .transpose()
    {
        Ok(snapshot) => snapshot,
        Err(SecurityStateSnapshotError::QueryBudget(error)) => {
            return Err(support_bundle_query_budget_error_response(&error))
        }
        Err(_) => {
            return Err(support_bundle_error_response(
                500,
                "execution",
                "support_bundle_security_snapshot_failed",
                "support-bundle security snapshot failed",
                None,
            ))
        }
    };
    let rbac_only_service_accounts = if security_manager.is_none() {
        match rbac_registry
            .map(RbacRegistry::service_account_status_summary)
            .transpose()
        {
            Ok(summary) => summary.map(ServiceAccountRotationSummary::from),
            Err(_) => {
                return Err(support_bundle_error_response(
                    500,
                    "execution",
                    "support_bundle_security_snapshot_failed",
                    "support-bundle security snapshot failed",
                    None,
                ))
            }
        }
    } else {
        None
    };
    let snapshot = snapshot.as_deref();
    let value = SecurityResponse {
        status: "success",
        data: SecurityData {
            enabled: security_manager.is_some() || rbac_registry.is_some(),
            targets: snapshot
                .map(|snapshot| snapshot.targets.as_slice())
                .unwrap_or_default(),
            audit_entries: snapshot
                .map(|snapshot| snapshot.audit_entries.as_slice())
                .unwrap_or_default(),
            service_accounts: snapshot
                .and_then(|snapshot| snapshot.service_accounts)
                .or(rbac_only_service_accounts),
        },
    };
    let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
    let response = serialize_support_bundle_json_child(200, &value, execution, remaining)?;
    admit_accounted_support_bundle_child(response, retained_bytes)
}

fn support_bundle_cluster_audit_child(
    cluster_context: Option<&ClusterRequestContext>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    #[derive(Serialize)]
    struct ClusterAuditResponse<'view, 'log> {
        status: &'static str,
        data: ClusterAuditData<'view, 'log>,
    }

    #[derive(Serialize)]
    struct ClusterAuditData<'view, 'log> {
        operation: &'static str,
        count: usize,
        entries: &'view crate::cluster::audit::BorrowedClusterAuditSnapshot<'log>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct UnavailableResponse {
        status: &'static str,
        error_type: &'static str,
        error: &'static str,
    }

    let Some(audit_log) = cluster_context.and_then(|context| context.audit_log.as_deref()) else {
        let value = UnavailableResponse {
            status: "error",
            error_type: "audit_log_unavailable",
            error: "cluster audit log is not available",
        };
        let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
        let response = serialize_support_bundle_json_child(503, &value, execution, remaining)?;
        return admit_accounted_support_bundle_child(response, retained_bytes);
    };
    let response = {
        let snapshot = audit_log
            .latest_snapshot_with_execution(SUPPORT_BUNDLE_CLUSTER_AUDIT_LIMIT, execution)
            .map_err(|error| support_bundle_query_budget_error_response(&error))?;
        let value = ClusterAuditResponse {
            status: "success",
            data: ClusterAuditData {
                operation: "audit_query",
                count: snapshot.record_count(),
                entries: &snapshot,
            },
        };
        let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
        serialize_support_bundle_json_child(200, &value, execution, remaining)?
    };
    // The inner scope releases the audit mutex immediately after both serializer passes, before
    // response admission and collection of the remaining support children.
    admit_accounted_support_bundle_child(response, retained_bytes)
}

// Field declarations in this response family intentionally preserve the lexicographic object-key
// order emitted by the historical dynamically sorted object path. Reordering changes raw bytes.
#[derive(Serialize)]
struct SupportBundleHandoffResponse<'a> {
    data: SupportBundleHandoffData<'a>,
    status: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleHandoffData<'a> {
    copied_rows_total: u64,
    error_summary: SupportBundleHandoffErrorSummary<'a>,
    estimated_eta_seconds: Option<u64>,
    event_unix_ms: u64,
    in_progress_shards: usize,
    jobs: SupportBundleHandoffJobs<'a>,
    leader_node_id: Option<&'a str>,
    message: &'static str,
    node_id: &'a str,
    operation: &'static str,
    pending_rows_total: u64,
    resumed_shards: usize,
    ring_version: u64,
    total_shards: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleHandoffErrorSummary<'a> {
    jobs_with_errors: u64,
    last_error_samples: SupportBundleHandoffErrorSamples<'a>,
    rebalance_last_error: Option<&'a str>,
}

struct SupportBundleHandoffJobs<'a> {
    shards: &'a [ShardHandoffSnapshot],
    rebalance: &'a crate::cluster::repair::HandoffRebalanceStatusSnapshot,
}

impl Serialize for SupportBundleHandoffJobs<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut jobs = serializer.serialize_seq(Some(self.shards.len()))?;
        for shard in self.shards {
            jobs.serialize_element(&SupportBundleHandoffJob {
                activation_ring_version: shard.activation_ring_version,
                copied_rows: shard.copied_rows,
                eta_seconds: support_bundle_handoff_eta_seconds(shard, self.rebalance),
                from_node_id: &shard.from_node_id,
                is_active: shard.phase.is_active(),
                last_error: shard.last_error.as_deref(),
                pending_rows: shard.pending_rows,
                phase: shard.phase.as_str(),
                progress_percent: handoff_progress_percent(shard),
                resumed_count: shard.resumed_count,
                shard: shard.shard,
                started_unix_ms: shard.started_unix_ms,
                to_node_id: &shard.to_node_id,
                updated_unix_ms: shard.updated_unix_ms,
            })?;
        }
        jobs.end()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleHandoffJob<'a> {
    activation_ring_version: u64,
    copied_rows: u64,
    eta_seconds: Option<u64>,
    from_node_id: &'a str,
    is_active: bool,
    last_error: Option<&'a str>,
    pending_rows: u64,
    phase: &'static str,
    progress_percent: f64,
    resumed_count: u64,
    shard: u32,
    started_unix_ms: u64,
    to_node_id: &'a str,
    updated_unix_ms: u64,
}

struct SupportBundleHandoffErrorSamples<'a>(&'a [ShardHandoffSnapshot]);

impl Serialize for SupportBundleHandoffErrorSamples<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let sample_count = self
            .0
            .iter()
            .filter(|shard| shard.last_error.is_some())
            .take(8)
            .count();
        let mut samples = serializer.serialize_seq(Some(sample_count))?;
        for shard in self
            .0
            .iter()
            .filter(|shard| shard.last_error.is_some())
            .take(8)
        {
            samples.serialize_element(&SupportBundleHandoffErrorSample(shard))?;
        }
        samples.end()
    }
}

struct SupportBundleHandoffErrorSample<'a>(&'a ShardHandoffSnapshot);

impl std::fmt::Display for SupportBundleHandoffErrorSample<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "shard {} {}->{}: {}",
            self.0.shard,
            self.0.from_node_id,
            self.0.to_node_id,
            self.0.last_error.as_deref().unwrap_or_default()
        )
    }
}

impl Serialize for SupportBundleHandoffErrorSample<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

fn support_bundle_handoff_eta_seconds(
    shard: &ShardHandoffSnapshot,
    rebalance: &crate::cluster::repair::HandoffRebalanceStatusSnapshot,
) -> Option<u64> {
    if shard.pending_rows == 0 {
        return Some(0);
    }
    if rebalance.paused || rebalance.interval_secs == 0 {
        return None;
    }
    let active_jobs = u64::try_from(rebalance.active_jobs)
        .unwrap_or(u64::MAX)
        .max(1);
    if rebalance.rows_scheduled_last_run == 0 {
        return None;
    }
    let rows_per_job_per_run = ceil_div_u64(rebalance.rows_scheduled_last_run, active_jobs);
    if rows_per_job_per_run == 0 {
        return None;
    }
    let runs_remaining = ceil_div_u64(shard.pending_rows, rows_per_job_per_run);
    Some(runs_remaining.saturating_mul(rebalance.interval_secs))
}

fn support_bundle_handoff_estimated_eta_seconds(
    handoff: &ClusterHandoffSnapshot,
    rebalance: &crate::cluster::repair::HandoffRebalanceStatusSnapshot,
) -> Option<u64> {
    if handoff.in_progress_shards == 0 {
        return Some(0);
    }
    let mut active_jobs = 0usize;
    let mut maximum_eta = 0u64;
    for shard in &handoff.shards {
        if !shard.phase.is_active() {
            continue;
        }
        active_jobs = active_jobs.saturating_add(1);
        let eta = support_bundle_handoff_eta_seconds(shard, rebalance).unwrap_or(u64::MAX);
        if eta == u64::MAX {
            return None;
        }
        maximum_eta = maximum_eta.max(eta);
    }
    (active_jobs > 0).then_some(maximum_eta)
}

fn support_bundle_handoff_response<'a>(
    node_id: &'a str,
    control: &'a crate::cluster::consensus::ControlHandoffStatusSnapshot,
    rebalance: &'a crate::cluster::repair::HandoffRebalanceStatusSnapshot,
    event_unix_ms: u64,
) -> SupportBundleHandoffResponse<'a> {
    let handoff = &control.handoff;
    let jobs_with_errors = handoff.shards.iter().fold(0u64, |count, shard| {
        count.saturating_add(u64::from(shard.last_error.is_some()))
    });
    SupportBundleHandoffResponse {
        data: SupportBundleHandoffData {
            copied_rows_total: handoff.copied_rows_total,
            error_summary: SupportBundleHandoffErrorSummary {
                jobs_with_errors,
                last_error_samples: SupportBundleHandoffErrorSamples(&handoff.shards),
                rebalance_last_error: rebalance.last_error.as_deref(),
            },
            estimated_eta_seconds: support_bundle_handoff_estimated_eta_seconds(handoff, rebalance),
            event_unix_ms,
            in_progress_shards: handoff.in_progress_shards,
            jobs: SupportBundleHandoffJobs {
                shards: &handoff.shards,
                rebalance,
            },
            leader_node_id: control.leader_node_id.as_deref(),
            message: "cluster handoff status",
            node_id,
            operation: AdminHandoffOperation::Status.as_str(),
            pending_rows_total: handoff.pending_rows_total,
            resumed_shards: handoff.resumed_shards,
            ring_version: control.ring_version,
            total_shards: handoff.total_shards,
        },
        status: "success",
    }
}

fn support_bundle_cluster_handoff_child(
    cluster_context: Option<&ClusterRequestContext>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct UnavailableResponse {
        error: &'static str,
        error_type: &'static str,
        status: &'static str,
    }

    let Some(cluster_context) = cluster_context else {
        let value = UnavailableResponse {
            error: "cluster control consensus runtime is not available",
            error_type: "control_plane_unavailable",
            status: "error",
        };
        let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
        let response = serialize_support_bundle_json_child(503, &value, execution, remaining)?;
        return admit_accounted_support_bundle_child(response, retained_bytes);
    };
    let Some(consensus) = cluster_context.control_consensus.as_ref() else {
        let value = UnavailableResponse {
            error: "cluster control consensus runtime is not available",
            error_type: "control_plane_unavailable",
            status: "error",
        };
        let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
        let response = serialize_support_bundle_json_child(503, &value, execution, remaining)?;
        return admit_accounted_support_bundle_child(response, retained_bytes);
    };

    let response = {
        let control = consensus
            .handoff_status_snapshot_with_execution(execution)
            .map_err(|error| support_bundle_query_budget_error_response(&error))?;
        let accounted_rebalance = cluster_context
            .digest_runtime
            .as_ref()
            .map(|runtime| runtime.handoff_status_snapshot_with_execution(execution))
            .transpose()
            .map_err(|error| support_bundle_query_budget_error_response(&error))?;
        let fallback_rebalance = crate::cluster::repair::HandoffRebalanceStatusSnapshot::empty();
        let rebalance = accounted_rebalance
            .as_deref()
            .unwrap_or(&fallback_rebalance);
        let value = support_bundle_handoff_response(
            &cluster_context.runtime.membership.local_node_id,
            &control,
            rebalance,
            unix_timestamp_millis(),
        );
        let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
        serialize_support_bundle_json_child(200, &value, execution, remaining)?
    };
    // Both sampled control generations and the scheduler diagnostics are released immediately
    // after the two serializer passes, before response admission and later support children.
    admit_accounted_support_bundle_child(response, retained_bytes)
}

const SUPPORT_BUNDLE_REPAIR_MISMATCH_SUMMARY_LIMIT: usize = 16;

#[derive(Serialize)]
struct SupportBundleRepairResponse<'a> {
    status: &'static str,
    data: SupportBundleRepairData<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleRepairData<'a> {
    operation: &'static str,
    node_id: &'a str,
    repair_paused: bool,
    repair_run_in_flight: bool,
    repair_cancel_generation: u64,
    repair_cancellations_total: u64,
    interval_secs: u64,
    window_secs: u64,
    runs_total: u64,
    mismatch_reports_retained: u64,
    repairs_attempted_total: u64,
    repairs_succeeded_total: u64,
    repairs_failed_total: u64,
    repairs_cancelled_total: u64,
    repair_rows_inserted_total: u64,
    repair_rows_inserted_last_run: u64,
    progress_percent: f64,
    estimated_eta_seconds: Option<u64>,
    event_unix_ms: u64,
    last_run_unix_ms: u64,
    last_success_unix_ms: u64,
    last_error: Option<&'a str>,
    error_summary: SupportBundleRepairErrorSummary<'a>,
    message: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SupportBundleRepairErrorSummary<'a> {
    failures_last_run: u64,
    cancelled_last_run: u64,
    skipped_backoff_last_run: u64,
    mismatch_backlog: SupportBundleRepairMismatchSummaries<'a>,
}

struct SupportBundleRepairMismatchSummaries<'a>(&'a [crate::cluster::repair::DigestMismatchReport]);

impl Serialize for SupportBundleRepairMismatchSummaries<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Summary<'a> {
            shard: u32,
            peer_node_id: &'a str,
            point_gap: u64,
            series_gap: u64,
            detected_unix_ms: u64,
        }

        let retained = self
            .0
            .len()
            .min(SUPPORT_BUNDLE_REPAIR_MISMATCH_SUMMARY_LIMIT);
        let mut sequence = serializer.serialize_seq(Some(retained))?;
        for mismatch in self
            .0
            .iter()
            .take(SUPPORT_BUNDLE_REPAIR_MISMATCH_SUMMARY_LIMIT)
        {
            sequence.serialize_element(&Summary {
                shard: mismatch.shard,
                peer_node_id: &mismatch.peer_node_id,
                point_gap: mismatch
                    .remote_point_count
                    .saturating_sub(mismatch.local_point_count),
                series_gap: mismatch
                    .remote_series_count
                    .saturating_sub(mismatch.local_series_count),
                detected_unix_ms: mismatch.detected_unix_ms,
            })?;
        }
        sequence.end()
    }
}

fn support_bundle_repair_response<'a>(
    node_id: &'a str,
    control: RepairControlSnapshot,
    snapshot: &'a DigestExchangeSnapshot,
    run_inflight: bool,
    event_unix_ms: u64,
) -> SupportBundleRepairResponse<'a> {
    let mismatch_backlog = u64::try_from(snapshot.mismatches.len()).unwrap_or(u64::MAX);
    let progress_percent = if mismatch_backlog == 0 {
        100.0
    } else {
        let completed = snapshot.repairs_succeeded_total as f64;
        let total = completed + mismatch_backlog as f64;
        if total <= 0.0 {
            0.0
        } else {
            (completed * 100.0) / total
        }
    };
    SupportBundleRepairResponse {
        status: "success",
        data: SupportBundleRepairData {
            operation: AdminRepairOperation::Status.as_str(),
            node_id,
            repair_paused: control.paused,
            repair_run_in_flight: run_inflight,
            repair_cancel_generation: control.cancel_generation,
            repair_cancellations_total: control.cancellations_total,
            interval_secs: snapshot.interval_secs,
            window_secs: snapshot.window_secs,
            runs_total: snapshot.runs_total,
            mismatch_reports_retained: mismatch_backlog,
            repairs_attempted_total: snapshot.repairs_attempted_total,
            repairs_succeeded_total: snapshot.repairs_succeeded_total,
            repairs_failed_total: snapshot.repairs_failed_total,
            repairs_cancelled_total: snapshot.repairs_cancelled_total,
            repair_rows_inserted_total: snapshot.repair_rows_inserted_total,
            repair_rows_inserted_last_run: snapshot.repair_rows_inserted_last_run,
            progress_percent,
            estimated_eta_seconds: estimate_repair_eta_seconds(snapshot),
            event_unix_ms,
            last_run_unix_ms: snapshot.last_run_unix_ms,
            last_success_unix_ms: snapshot.last_success_unix_ms,
            last_error: snapshot.last_error.as_deref(),
            error_summary: SupportBundleRepairErrorSummary {
                failures_last_run: snapshot.repairs_failed_last_run,
                cancelled_last_run: snapshot.repairs_cancelled_last_run,
                skipped_backoff_last_run: snapshot.repairs_skipped_backoff_last_run,
                mismatch_backlog: SupportBundleRepairMismatchSummaries(&snapshot.mismatches),
            },
            message: "cluster digest repair runtime status",
        },
    }
}

fn support_bundle_cluster_repair_child(
    cluster_context: Option<&ClusterRequestContext>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct UnavailableResponse {
        status: &'static str,
        error_type: &'static str,
        error: &'static str,
    }

    let Some((cluster_context, digest_runtime)) = cluster_context.and_then(|context| {
        context
            .digest_runtime
            .as_deref()
            .map(|runtime| (context, runtime))
    }) else {
        let value = UnavailableResponse {
            status: "error",
            error_type: "repair_runtime_unavailable",
            error: "cluster digest repair runtime is not available",
        };
        let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
        let response = serialize_support_bundle_json_child(503, &value, execution, remaining)?;
        return admit_accounted_support_bundle_child(response, retained_bytes);
    };
    // Preserve the direct endpoint's established race semantics: response control fields use a
    // first allocation-free control sample, while ETA uses the later complete digest generation.
    let control = digest_runtime.repair_control_snapshot();
    let snapshot = digest_runtime
        .status_snapshot_with_execution(execution)
        .map_err(|error| support_bundle_query_budget_error_response(&error))?;
    let value = support_bundle_repair_response(
        &cluster_context.runtime.membership.local_node_id,
        control,
        &snapshot,
        digest_runtime.is_repair_run_inflight(),
        unix_timestamp_millis(),
    );
    let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
    let response = serialize_support_bundle_json_child(200, &value, execution, remaining)?;
    admit_accounted_support_bundle_child(response, retained_bytes)
}

#[derive(Serialize)]
struct SupportBundleRulesResponse<'a> {
    status: &'static str,
    data: &'a rules::RulesStatusSnapshot,
}

fn support_bundle_rules_error_text(
    error: &rules::RulesStatusProjectionError,
    serialization: bool,
) -> &'static str {
    match (serialization, error) {
        (false, rules::RulesStatusProjectionError::StoreReadPoisoned) => {
            "rules status failed: rules store read lock poisoned"
        }
        (false, rules::RulesStatusProjectionError::StatusLimit) => {
            "rules status failed: rules snapshot/status exceeds its finite output limit"
        }
        (
            false,
            rules::RulesStatusProjectionError::Serialization(
                "rules snapshot/status accounting did not stabilize",
            ),
        ) => "rules status failed: rules snapshot/status accounting did not stabilize",
        (false, rules::RulesStatusProjectionError::Serialization(_)) => {
            "rules status failed: failed to measure bounded rules JSON"
        }
        (true, rules::RulesStatusProjectionError::StoreReadPoisoned) => {
            "rules status serialization failed: rules store read lock poisoned"
        }
        (true, rules::RulesStatusProjectionError::StatusLimit) => {
            "rules status serialization failed: rules snapshot/status exceeds its finite output limit"
        }
        (
            true,
            rules::RulesStatusProjectionError::Serialization(
                "failed to measure bounded rules JSON",
            ),
        ) => "rules status serialization failed: failed to measure bounded rules JSON",
        (
            true,
            rules::RulesStatusProjectionError::Serialization(
                "failed to allocate bounded rules JSON",
            ),
        ) => "rules status serialization failed: failed to allocate bounded rules JSON",
        (
            true,
            rules::RulesStatusProjectionError::Serialization(
                "failed to encode bounded rules JSON",
            ),
        ) => "rules status serialization failed: failed to encode bounded rules JSON",
        (
            true,
            rules::RulesStatusProjectionError::Serialization(
                "bounded rules JSON length changed during encoding",
            ),
        ) => {
            "rules status serialization failed: bounded rules JSON length changed during encoding"
        }
        (
            true,
            rules::RulesStatusProjectionError::Serialization(
                "rules snapshot/status accounting did not stabilize",
            ),
        ) => {
            "rules status serialization failed: rules snapshot/status accounting did not stabilize"
        }
        (true, rules::RulesStatusProjectionError::Serialization(_)) => {
            "rules status serialization failed: failed to measure bounded rules JSON"
        }
        (_, rules::RulesStatusProjectionError::QueryBudget(_)) => {
            unreachable!("query-budget errors use the structured support-bundle path")
        }
    }
}

fn support_bundle_rules_child(
    rules_runtime: Option<&RulesRuntime>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    let Some(rules_runtime) = rules_runtime else {
        let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
        let response = serialize_support_bundle_text_child_with_content_type(
            503,
            "rules runtime is not available",
            execution,
            remaining,
        )?;
        return admit_accounted_support_bundle_child(response, retained_bytes);
    };

    let response = {
        let mut snapshot = match rules_runtime.status_snapshot_with_execution(execution) {
            Ok(snapshot) => snapshot,
            Err(rules::RulesStatusProjectionError::QueryBudget(error)) => {
                return Err(support_bundle_query_budget_error_response(&error));
            }
            Err(error) => {
                let remaining =
                    SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
                let response = serialize_support_bundle_text_child_with_content_type(
                    500,
                    support_bundle_rules_error_text(&error, false),
                    execution,
                    remaining,
                )?;
                return admit_accounted_support_bundle_child(response, retained_bytes);
            }
        };
        let mut completed_response = None;
        for _ in 0..8 {
            let encoded_len = match rules_runtime
                .prepare_success_snapshot_iteration_with_execution(&mut snapshot, execution)
            {
                Ok(Some(encoded_len)) => encoded_len,
                Ok(None) => continue,
                Err(rules::RulesStatusProjectionError::QueryBudget(error)) => {
                    return Err(support_bundle_query_budget_error_response(&error));
                }
                Err(error) => {
                    let remaining =
                        SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
                    let response = serialize_support_bundle_text_child_with_content_type(
                        500,
                        support_bundle_rules_error_text(&error, true),
                        execution,
                        remaining,
                    )?;
                    return admit_accounted_support_bundle_child(response, retained_bytes);
                }
            };
            let value = SupportBundleRulesResponse {
                status: "success",
                data: &snapshot,
            };
            let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
            let response = match serialize_support_bundle_json_child_inner(
                200, &value, execution, remaining, true,
            ) {
                Ok(response) => response,
                Err(SupportBundleChildJsonSerializationError::QueryBudget(error)) => {
                    return Err(support_bundle_query_budget_error_response(&error));
                }
                Err(SupportBundleChildJsonSerializationError::FixedLimitExceeded) => {
                    return Err(support_bundle_child_retained_limit_error());
                }
                Err(error) => {
                    let message = match error {
                        SupportBundleChildJsonSerializationError::Measurement => {
                            "failed to measure bounded rules JSON"
                        }
                        SupportBundleChildJsonSerializationError::Allocation => {
                            "failed to allocate bounded rules JSON"
                        }
                        SupportBundleChildJsonSerializationError::Serialization => {
                            "failed to encode bounded rules JSON"
                        }
                        SupportBundleChildJsonSerializationError::LengthChanged => {
                            "bounded rules JSON length changed during encoding"
                        }
                        SupportBundleChildJsonSerializationError::QueryBudget(_)
                        | SupportBundleChildJsonSerializationError::FixedLimitExceeded => {
                            unreachable!("handled above")
                        }
                    };
                    let error = rules::RulesStatusProjectionError::Serialization(message);
                    let response = serialize_support_bundle_text_child_with_content_type(
                        500,
                        support_bundle_rules_error_text(&error, true),
                        execution,
                        remaining,
                    )?;
                    return admit_accounted_support_bundle_child(response, retained_bytes);
                }
            };
            debug_assert_eq!(response.response.body.len(), encoded_len);
            let stable = match rules_runtime.finalize_success_snapshot_with_execution(
                &snapshot,
                response.response.body.capacity(),
                execution,
            ) {
                Ok(stable) => stable,
                Err(rules::RulesStatusProjectionError::QueryBudget(error)) => {
                    drop(response);
                    return Err(support_bundle_query_budget_error_response(&error));
                }
                Err(error) => {
                    drop(response);
                    let remaining =
                        SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
                    let response = serialize_support_bundle_text_child_with_content_type(
                        500,
                        support_bundle_rules_error_text(&error, true),
                        execution,
                        remaining,
                    )?;
                    return admit_accounted_support_bundle_child(response, retained_bytes);
                }
            };
            if stable {
                completed_response = Some(response);
                break;
            }
            drop(response);
        }
        match completed_response {
            Some(response) => response,
            None => {
                let error = rules::RulesStatusProjectionError::Serialization(
                    "rules snapshot/status accounting did not stabilize",
                );
                let remaining =
                    SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
                let response = serialize_support_bundle_text_child_with_content_type(
                    500,
                    support_bundle_rules_error_text(&error, true),
                    execution,
                    remaining,
                )?;
                return admit_accounted_support_bundle_child(response, retained_bytes);
            }
        }
    };
    // The complete owned rules projection is dropped after both serializer passes and before
    // response admission, so later support children retain only the exactly charged HTTP body.
    admit_accounted_support_bundle_child(response, retained_bytes)
}

fn support_bundle_rollups_child(
    storage: &Arc<dyn Storage>,
    execution: &tsink::QueryExecution,
    retained_bytes: &mut u64,
) -> Result<AccountedHttpResponse, HttpResponse> {
    #[derive(Serialize)]
    struct RollupsResponse<'a> {
        status: &'static str,
        data: &'a tsink::RollupObservabilitySnapshot,
    }

    let observability = match storage.status_observability_snapshot_with_execution(execution) {
        Ok(observability) => observability,
        Err(tsink::TsinkError::QueryBudget(error)) => {
            return Err(support_bundle_query_budget_error_response(&error))
        }
        Err(tsink::TsinkError::UnsupportedOperation { .. }) => {
            return Err(support_bundle_error_response(
                500,
                "execution",
                "support_bundle_rollups_accounting_unavailable",
                "support-bundle rollup status requires query-accounted storage observability",
                None,
            ))
        }
        Err(_) => {
            return Err(support_bundle_error_response(
                500,
                "execution",
                "support_bundle_rollups_snapshot_failed",
                "support-bundle rollup status snapshot failed",
                None,
            ))
        }
    };
    let value = RollupsResponse {
        status: "success",
        data: &observability.rollups,
    };
    let remaining = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES.saturating_sub(*retained_bytes);
    let response = serialize_support_bundle_json_child(200, &value, execution, remaining)?;
    admit_accounted_support_bundle_child(response, retained_bytes)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_admin_support_bundle(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    rbac_registry: Option<&RbacRegistry>,
    security_manager: Option<&SecurityManager>,
    usage_accounting: Option<&UsageAccounting>,
    local_disk_budget: Option<&tsink::LocalDiskBudget>,
    offline_restore_disk_budget: Option<&tsink::LocalDiskBudget>,
) -> HttpResponse {
    if let Err(response) = validate_support_bundle_tenant_before_admission(request) {
        return response;
    }
    if let Err(response) =
        initialize_support_bundle_tenant_runtime_before_admission(request, tenant_registry)
    {
        return response;
    }
    // Admit the complete support-bundle request exactly once. Every dynamic support source and
    // serializer reuses this execution and retains an exact same-lease guard through composition.
    // The two bounded TSDB/rebalance compatibility errors are constructed under the admitted root
    // scratch and transferred to exact guards before the orchestrator retains them.
    let cancellation = tsink::QueryCancellationToken::new();
    let cancellation_guard = TsdbStatusCancellationGuard {
        token: cancellation.clone(),
    };
    let execution = match begin_support_bundle_execution(storage, cancellation) {
        Ok(execution) => execution,
        Err(response) => return response,
    };
    let mut setup = match prepare_support_bundle_setup(request, &execution) {
        Ok(setup) => setup,
        Err(response) => return response,
    };
    let header_preflight = modeled_support_bundle_header_preflight_bytes(&setup.tenant_id);
    let base_reserved_bytes = setup
        .root_retained_bytes
        .saturating_add(SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES)
        .saturating_add(header_preflight);
    if let Err(error) = setup
        .reservation
        .resize(base_reserved_bytes.saturating_add(setup.synthetic_retained_bytes))
    {
        return support_bundle_query_budget_error_response(&error);
    }
    let compatibility_scratch = match admit_support_bundle_compatibility_scratch(&setup.reservation)
    {
        Ok(scratch) => scratch,
        Err(response) => return response,
    };
    let mut child_retained_bytes = 0u64;

    let status_tsdb = match handle_tsdb_status_with_execution(
        storage,
        metadata_store,
        exemplar_store,
        setup
            .synthetic_request
            .as_ref()
            .expect("support-bundle request remains live through audit collection"),
        cluster_context,
        edge_sync_context,
        tenant_registry,
        rbac_registry,
        security_manager,
        usage_accounting,
        None,
        local_disk_budget,
        offline_restore_disk_budget,
        &execution,
    )
    .await
    {
        Ok(response) => {
            match admit_accounted_support_bundle_child(response, &mut child_retained_bytes) {
                Ok(response) => response,
                Err(response) => return response,
            }
        }
        Err(response) => match transfer_support_bundle_scratch_child(
            response,
            &execution,
            &mut child_retained_bytes,
            SupportBundleScratchErrorSurface::TsdbStatus,
            compatibility_scratch,
        ) {
            Ok(response) => response,
            Err(response) => return response,
        },
    };
    drop(setup.synthetic_request.take());
    setup
        .reservation
        .resize(base_reserved_bytes)
        .expect("shrinking a released support-bundle request reservation cannot fail");

    let usage = match support_bundle_usage_child(
        storage,
        usage_accounting,
        &setup.tenant_id,
        &execution,
        &mut child_retained_bytes,
    ) {
        Ok(response) => response,
        Err(response) => return response,
    };
    let rbac_state =
        match support_bundle_rbac_state_child(rbac_registry, &execution, &mut child_retained_bytes)
        {
            Ok(response) => response,
            Err(response) => return response,
        };
    let rbac_audit =
        match support_bundle_rbac_audit_child(rbac_registry, &execution, &mut child_retained_bytes)
        {
            Ok(response) => response,
            Err(response) => return response,
        };
    let security_state = match support_bundle_security_state_child(
        rbac_registry,
        security_manager,
        &execution,
        &mut child_retained_bytes,
    ) {
        Ok(response) => response,
        Err(response) => return response,
    };
    let cluster_audit = match support_bundle_cluster_audit_child(
        cluster_context,
        &execution,
        &mut child_retained_bytes,
    ) {
        Ok(response) => response,
        Err(response) => return response,
    };
    let cluster_handoff = match support_bundle_cluster_handoff_child(
        cluster_context,
        &execution,
        &mut child_retained_bytes,
    ) {
        Ok(response) => response,
        Err(response) => return response,
    };
    let cluster_repair = match support_bundle_cluster_repair_child(
        cluster_context,
        &execution,
        &mut child_retained_bytes,
    ) {
        Ok(response) => response,
        Err(response) => return response,
    };
    let cluster_rebalance = match handle_admin_cluster_rebalance_status_with_execution(
        storage,
        cluster_context,
        &execution,
    )
    .await
    {
        Ok(response) => {
            match admit_accounted_support_bundle_child(response, &mut child_retained_bytes) {
                Ok(response) => response,
                Err(response) => return response,
            }
        }
        Err(response) => match transfer_support_bundle_scratch_child(
            response,
            &execution,
            &mut child_retained_bytes,
            SupportBundleScratchErrorSurface::ClusterRebalance,
            compatibility_scratch,
        ) {
            Ok(response) => response,
            Err(response) => return response,
        },
    };
    let rules =
        match support_bundle_rules_child(rules_runtime, &execution, &mut child_retained_bytes) {
            Ok(response) => response,
            Err(response) => return response,
        };
    let rollups = match support_bundle_rollups_child(storage, &execution, &mut child_retained_bytes)
    {
        Ok(response) => response,
        Err(response) => return response,
    };

    let prepared_result = {
        let bundle = SupportBundleEnvelope {
            generated_unix_ms: unix_timestamp_millis(),
            tenant_id: &setup.tenant_id,
            requested_by: SupportBundleRequestedBy {
                id: &setup.actor.id,
                auth_scope: &setup.actor.auth_scope,
            },
            server_version: env!("CARGO_PKG_VERSION"),
            sections: SupportBundleSections {
                status_tsdb: SupportBundleResponseSection {
                    response: &status_tsdb.response,
                },
                usage: SupportBundleResponseSection {
                    response: &usage.response,
                },
                rbac_state: SupportBundleResponseSection {
                    response: &rbac_state.response,
                },
                rbac_audit: SupportBundleResponseSection {
                    response: &rbac_audit.response,
                },
                security_state: SupportBundleResponseSection {
                    response: &security_state.response,
                },
                cluster_audit: SupportBundleResponseSection {
                    response: &cluster_audit.response,
                },
                cluster_handoff: SupportBundleResponseSection {
                    response: &cluster_handoff.response,
                },
                cluster_repair: SupportBundleResponseSection {
                    response: &cluster_repair.response,
                },
                cluster_rebalance: SupportBundleResponseSection {
                    response: &cluster_rebalance.response,
                },
                rules: SupportBundleResponseSection {
                    response: &rules.response,
                },
                rollups: SupportBundleResponseSection {
                    response: &rollups.response,
                },
            },
        };
        prepare_support_bundle_response(
            &bundle,
            &execution,
            &mut setup.reservation,
            base_reserved_bytes,
        )
    };
    let prepared = match prepared_result {
        Ok(prepared) => prepared,
        Err(response) => {
            // Each wrapper destroys its response before releasing the corresponding child guard.
            drop(rollups);
            drop(rules);
            drop(cluster_repair);
            drop(cluster_handoff);
            drop(cluster_audit);
            drop(security_state);
            drop(rbac_audit);
            drop(rbac_state);
            drop(usage);
            drop(cluster_rebalance);
            drop(status_tsdb);
            drop(setup);
            drop(execution);
            drop(cancellation_guard);
            return response;
        }
    };
    let PreparedSupportBundleResponse {
        response,
        retained_bytes,
    } = prepared;
    drop(rollups);
    drop(rules);
    drop(cluster_repair);
    drop(cluster_handoff);
    drop(cluster_audit);
    drop(security_state);
    drop(rbac_audit);
    drop(rbac_state);
    drop(usage);
    drop(cluster_rebalance);
    drop(status_tsdb);
    let PreparedSupportBundleSetup {
        tenant_id,
        actor,
        synthetic_request,
        mut reservation,
        ..
    } = setup;
    debug_assert!(synthetic_request.is_none());
    drop(synthetic_request);
    drop(actor);
    drop(tenant_id);
    reservation
        .resize(retained_bytes)
        .expect("shrinking a support-bundle response reservation cannot fail");
    drop(reservation);
    drop(execution);
    drop(cancellation_guard);
    response
}

pub(crate) async fn handle_admin_usage_report(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let Some(usage_accounting) = usage_accounting else {
        return text_response(503, "usage accounting is unavailable");
    };
    let (tenant_id, start_unix_ms, end_unix_ms, bucket_width, reconcile) =
        match parse_usage_report_filter(request) {
            Ok(filter) => filter,
            Err(response) => return response,
        };
    let (read_options, max_response_bytes) =
        match parse_usage_read_page(request, usage_accounting, UsageReadKind::Report) {
            Ok(options) => options,
            Err(response) => return response,
        };
    let reconciled_storage_snapshots = if reconcile {
        match usage_accounting
            .reconcile_storage_async(Arc::clone(storage))
            .await
        {
            Ok(snapshots) => snapshots,
            Err(err) => {
                return usage_accounting_error_response("usage storage reconciliation", &err)
            }
        }
    } else {
        Vec::new()
    };
    let report = match usage_accounting.report_page(
        tenant_id.as_deref(),
        start_unix_ms,
        end_unix_ms,
        bucket_width,
        read_options,
    ) {
        Ok(report) => report,
        Err(err) => return usage_read_error_response(&err),
    };
    let reconciliation = usage_reconciliation_json(&report, &storage.observability_snapshot());
    let payload = AdminUsageReportResponse {
        status: "success",
        data: AdminUsageReportResponseData {
            report: &report,
            reconciliation: &reconciliation,
            reconciled_storage_snapshots: &reconciled_storage_snapshots,
        },
    };
    let response = bounded_usage_json_response(
        200,
        &payload,
        max_response_bytes,
        "usage_report_response_too_large",
    );
    if reconcile {
        response.with_header("X-Tsink-Usage-Reconciliation", "completed")
    } else {
        response
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminUsageReportResponse<'a> {
    status: &'static str,
    data: AdminUsageReportResponseData<'a>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminUsageReportResponseData<'a> {
    report: &'a crate::usage::UsageReport,
    reconciliation: &'a JsonValue,
    reconciled_storage_snapshots: &'a [crate::usage::UsageStorageSnapshot],
}

pub(crate) fn handle_admin_usage_export(
    request: &HttpRequest,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let Some(usage_accounting) = usage_accounting else {
        return text_response(503, "usage accounting is unavailable");
    };
    let (tenant_id, start_unix_ms, end_unix_ms, _, _) = match parse_usage_report_filter(request) {
        Ok(filter) => filter,
        Err(response) => return response,
    };
    let (read_options, max_response_bytes) =
        match parse_usage_read_page(request, usage_accounting, UsageReadKind::Export) {
            Ok(options) => options,
            Err(response) => return response,
        };
    let page = match usage_accounting.export_page(
        tenant_id.as_deref(),
        start_unix_ms,
        end_unix_ms,
        read_options,
        max_response_bytes,
    ) {
        Ok(page) => page,
        Err(err) => return usage_read_error_response(&err),
    };
    let mut body = Vec::with_capacity(page.response_bytes);
    for record in &page.records {
        if let Err(err) = serde_json::to_writer(&mut body, record) {
            return usage_read_error_response(&crate::usage::UsageReadError::Encoding(format!(
                "failed to encode usage export: {err}"
            )));
        }
        body.push(b'\n');
    }
    debug_assert_eq!(body.len(), page.response_bytes);
    let mut response = HttpResponse::new(200, body)
        .with_header("Content-Type", "application/x-ndjson")
        .with_header("Cache-Control", "no-store")
        .with_header(
            "X-Tsink-Usage-Snapshot-Sequence",
            page.snapshot_sequence.to_string(),
        )
        .with_header(
            "X-Tsink-Usage-Records-Returned",
            page.records_returned.to_string(),
        )
        .with_header(
            "X-Tsink-Usage-Response-Bytes",
            page.response_bytes.to_string(),
        )
        .with_header(
            "X-Tsink-Usage-Raw-History-Complete",
            page.raw_history_complete.to_string(),
        )
        .with_header("X-Tsink-Usage-Has-More", page.has_more.to_string());
    if let Some(sequence) = page.earliest_available_sequence {
        response = response.with_header(
            "X-Tsink-Usage-Earliest-Available-Sequence",
            sequence.to_string(),
        );
    }
    if let Some(sequence) = page.next_after_sequence {
        response = response.with_header("X-Tsink-Usage-Next-After-Sequence", sequence.to_string());
    }
    response
}

#[derive(Debug, Clone, Copy)]
enum UsageReadKind {
    Report,
    Export,
}

fn parse_usage_read_page(
    request: &HttpRequest,
    accounting: &UsageAccounting,
    kind: UsageReadKind,
) -> Result<(crate::usage::UsageReadOptions, usize), HttpResponse> {
    let limits = accounting.limits();
    let (default_records, maximum_records, maximum_response_bytes) = match kind {
        UsageReadKind::Report => (
            limits.report_default_records,
            limits.report_max_records,
            limits.report_max_response_bytes,
        ),
        UsageReadKind::Export => (
            limits.export_default_records,
            limits.export_max_records,
            limits.export_max_response_bytes,
        ),
    };
    let limit = match request.param("limit") {
        Some(value) => parse_usage_usize(&value, "limit")?,
        None => default_records,
    };
    if limit == 0 || limit > maximum_records {
        return Err(usage_read_error_response(
            &crate::usage::UsageReadError::InvalidLimit {
                requested: limit,
                maximum: maximum_records,
            },
        ));
    }
    let max_response_bytes = match request
        .param("maxBytes")
        .or_else(|| request.param("maxResponseBytes"))
    {
        Some(value) => parse_usage_usize(&value, "maxBytes")?,
        None => maximum_response_bytes,
    };
    if max_response_bytes == 0 || max_response_bytes > maximum_response_bytes {
        return Err(usage_read_error_response(
            &crate::usage::UsageReadError::InvalidResponseBytes {
                requested: max_response_bytes,
                maximum: maximum_response_bytes,
            },
        ));
    }
    let after_sequence = parse_optional_usage_u64(
        request
            .param("afterSequence")
            .or_else(|| request.param("after_sequence")),
        "afterSequence",
    )?;
    let snapshot_sequence = parse_optional_usage_u64(
        request
            .param("snapshotSequence")
            .or_else(|| request.param("snapshot_sequence")),
        "snapshotSequence",
    )?;
    Ok((
        crate::usage::UsageReadOptions {
            after_sequence,
            snapshot_sequence,
            limit,
        },
        max_response_bytes,
    ))
}

fn parse_usage_usize(value: &str, name: &str) -> Result<usize, HttpResponse> {
    let value =
        parse_admin_u64(value, name).map_err(|err| usage_parameter_error(name, value, &err))?;
    usize::try_from(value).map_err(|_| {
        usage_parameter_error(
            name,
            &value.to_string(),
            &format!("invalid '{name}': value exceeds this platform's range"),
        )
    })
}

fn parse_optional_usage_u64(
    value: Option<String>,
    name: &str,
) -> Result<Option<u64>, HttpResponse> {
    value
        .map(|value| {
            parse_admin_u64(&value, name).map_err(|err| usage_parameter_error(name, &value, &err))
        })
        .transpose()
}

fn usage_parameter_error(name: &str, value: &str, message: &str) -> HttpResponse {
    json_response(
        400,
        &json!({
            "status": "error",
            "error": {
                "code": "usage_page_parameter_invalid",
                "message": message,
                "details": {
                    "parameter": name,
                    "value": value,
                }
            }
        }),
    )
}

fn usage_read_error_response(err: &crate::usage::UsageReadError) -> HttpResponse {
    let details = match err {
        crate::usage::UsageReadError::InvalidLimit { requested, maximum } => json!({
            "requested": requested,
            "maximum": maximum,
        }),
        crate::usage::UsageReadError::InvalidResponseBytes { requested, maximum } => json!({
            "requestedBytes": requested,
            "maximumBytes": maximum,
        }),
        crate::usage::UsageReadError::InvalidSnapshot { requested, latest } => json!({
            "requestedSnapshotSequence": requested,
            "latestSequence": latest,
        }),
        crate::usage::UsageReadError::CursorExpired {
            requested_after,
            earliest_available,
        } => json!({
            "requestedAfterSequence": requested_after,
            "earliestAvailableSequence": earliest_available,
        }),
        crate::usage::UsageReadError::RecordExceedsResponseLimit {
            sequence,
            required_bytes,
            maximum_bytes,
        } => json!({
            "sequence": sequence,
            "requiredBytes": required_bytes,
            "maximumBytes": maximum_bytes,
        }),
        crate::usage::UsageReadError::Encoding(_) => JsonValue::Null,
    };
    json_response(
        err.http_status(),
        &json!({
            "status": "error",
            "error": {
                "code": err.code(),
                "message": err.to_string(),
                "details": details,
            }
        }),
    )
}

#[derive(Debug)]
struct BoundedUsageResponseWriter {
    body: Vec<u8>,
    maximum: usize,
    exceeded: bool,
}

impl std::io::Write for BoundedUsageResponseWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.body.len()) {
            self.exceeded = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "usage response exceeds configured byte limit",
            ));
        }
        self.body.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn bounded_usage_json_response(
    status: u16,
    value: &impl Serialize,
    maximum: usize,
    error_code: &str,
) -> HttpResponse {
    let mut writer = BoundedUsageResponseWriter {
        body: Vec::with_capacity(maximum.min(8 * 1024)),
        maximum,
        exceeded: false,
    };
    if let Err(err) = serde_json::to_writer(&mut writer, value) {
        if writer.exceeded {
            return json_response(
                413,
                &json!({
                    "status": "error",
                    "error": {
                        "code": error_code,
                        "message": format!("usage response exceeds the configured maximum {maximum} bytes"),
                        "maximumBytes": maximum,
                    }
                }),
            );
        }
        return text_response(500, &format!("failed to encode usage response: {err}"));
    }
    HttpResponse::new(status, writer.body).with_header("Content-Type", "application/json")
}

pub(crate) async fn handle_admin_usage_reconcile(
    storage: &Arc<dyn Storage>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let Some(usage_accounting) = usage_accounting else {
        return text_response(503, "usage accounting is unavailable");
    };
    match usage_accounting
        .reconcile_storage_async(Arc::clone(storage))
        .await
    {
        Ok(snapshots) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "journal": usage_accounting.ledger_status(),
                    "storageSnapshots": snapshots,
                }
            }),
        ),
        Err(err) => usage_accounting_error_response("usage storage reconciliation", &err),
    }
}

fn usage_accounting_error_response(
    action: &str,
    err: &crate::usage::UsageAccountingError,
) -> HttpResponse {
    if matches!(err, crate::usage::UsageAccountingError::Limit(_)) {
        return json_response(
            413,
            &json!({
                "status": "error",
                "error": {
                    "code": "usage_ledger_limit_exceeded",
                    "message": format!("{action} failed: {err}"),
                }
            }),
        );
    }
    if let Some(
        disk_error @ (tsink::TsinkError::DiskQuotaExceeded { .. }
        | tsink::TsinkError::InsufficientDiskSpace { .. }
        | tsink::TsinkError::InsufficientCompactionHeadroom { .. }),
    ) = err.disk_error()
    {
        return server_persistence_error_response(action, disk_error);
    }
    if err.is_persistence_failure() {
        return indeterminate_backend_write_error_response(
            500,
            "usage_ledger_persistence_failed",
            "usage ledger persistence failed; the durable outcome is indeterminate",
        );
    }
    text_response(500, &format!("{action} failed: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::audit::{ClusterAuditConfig, ClusterAuditLog};
    use crate::cluster::config::ClusterConfig;
    use crate::cluster::consensus::{ControlConsensusConfig, ControlConsensusRuntime};
    use crate::cluster::control::{
        ControlStateStore, ShardHandoffProgress, ShardOwnershipTransition,
    };
    use crate::cluster::repair::{DigestExchangeConfig, DigestMismatchReport};
    use crate::cluster::ClusterRuntime;
    use crate::rbac::{RbacAction, RbacPermission, RbacResource};

    fn support_bundle_test_response(body: impl Into<Vec<u8>>) -> HttpResponse {
        HttpResponse::new(200, body).with_header("Content-Type", "application/json")
    }

    fn support_bundle_test_envelope<'a>(
        json_response: &'a HttpResponse,
        text_response: &'a HttpResponse,
    ) -> SupportBundleEnvelope<'a> {
        SupportBundleEnvelope {
            generated_unix_ms: 123,
            tenant_id: "team-a",
            requested_by: SupportBundleRequestedBy {
                id: "test-actor",
                auth_scope: "admin",
            },
            server_version: "test",
            sections: SupportBundleSections {
                status_tsdb: SupportBundleResponseSection {
                    response: json_response,
                },
                usage: SupportBundleResponseSection {
                    response: json_response,
                },
                rbac_state: SupportBundleResponseSection {
                    response: json_response,
                },
                rbac_audit: SupportBundleResponseSection {
                    response: json_response,
                },
                security_state: SupportBundleResponseSection {
                    response: json_response,
                },
                cluster_audit: SupportBundleResponseSection {
                    response: json_response,
                },
                cluster_handoff: SupportBundleResponseSection {
                    response: json_response,
                },
                cluster_repair: SupportBundleResponseSection {
                    response: json_response,
                },
                cluster_rebalance: SupportBundleResponseSection {
                    response: json_response,
                },
                rules: SupportBundleResponseSection {
                    response: json_response,
                },
                rollups: SupportBundleResponseSection {
                    response: text_response,
                },
            },
        }
    }

    fn support_bundle_test_budget(
        memory_bytes: Option<u64>,
        returned_bytes: Option<u64>,
    ) -> tsink::QueryBudget {
        tsink::QueryBudget::new(tsink::QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: memory_bytes,
            per_query: tsink::QueryWorkLimits {
                max_memory_bytes: memory_bytes,
                max_returned_bytes: returned_bytes,
                ..tsink::QueryWorkLimits::default()
            },
        })
        .expect("support-bundle test budget should build")
    }

    fn prepare_support_bundle_for_test(
        budget: &tsink::QueryBudget,
        json_response: &HttpResponse,
        text_response: &HttpResponse,
    ) -> Result<(HttpResponse, u64), HttpResponse> {
        let execution = budget
            .begin_query()
            .expect("support-bundle test query should admit");
        let root_tenant = "team-a".to_string();
        let root_actor = ClusterAuditActor {
            id: "test-actor".to_string(),
            auth_scope: "admin".to_string(),
        };
        let child_retained_bytes = modeled_tsdb_status_response_retained_bytes(json_response)
            .saturating_add(modeled_tsdb_status_response_retained_bytes(text_response));
        let base_reserved_bytes = child_retained_bytes
            .saturating_add(modeled_support_bundle_root_retained_bytes(
                &root_tenant,
                &root_actor,
            ))
            .saturating_add(SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES)
            .saturating_add(modeled_support_bundle_header_preflight_bytes("team-a"));
        let mut reservation = execution
            .reserve_memory(base_reserved_bytes)
            .map_err(|error| support_bundle_query_budget_error_response(&error))?;
        let PreparedSupportBundleResponse {
            response,
            retained_bytes,
        } = {
            let bundle = support_bundle_test_envelope(json_response, text_response);
            prepare_support_bundle_response(
                &bundle,
                &execution,
                &mut reservation,
                base_reserved_bytes,
            )?
        };
        reservation
            .resize(retained_bytes)
            .expect("support-bundle test reservation shrink should succeed");
        drop(root_actor);
        drop(root_tenant);
        drop(reservation);
        drop(execution);
        Ok((response, retained_bytes))
    }

    fn usage_request(path: &str) -> HttpRequest {
        HttpRequest {
            method: "GET".to_string(),
            path: path.to_string(),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        }
    }

    fn header<'a>(response: &'a HttpResponse, name: &str) -> Option<&'a str> {
        response
            .headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn support_bundle_setup_request() -> HttpRequest {
        HttpRequest {
            method: "GET".to_string(),
            // A whitespace-only override exercises the longer header/default fallback branch in
            // the allocation-free setup model.
            path: "/api/v1/admin/support_bundle?tenant=%20%20".to_string(),
            headers: std::collections::HashMap::from([
                (
                    tenant::SCOPE_ORG_ID_HEADER.to_string(),
                    "team-a".to_string(),
                ),
                (
                    rbac::RBAC_AUTH_PRINCIPAL_ID_HEADER.to_string(),
                    "operator-7".to_string(),
                ),
                (
                    rbac::RBAC_AUTH_PROVIDER_HEADER.to_string(),
                    "example".to_string(),
                ),
            ]),
            body: Vec::new(),
        }
    }

    #[test]
    fn support_bundle_setup_preflight_has_an_exact_memory_boundary() {
        let request = support_bundle_setup_request();
        let required = modeled_support_bundle_setup_preflight_bytes(&request);
        assert!(required > 0);

        let exact_budget = support_bundle_test_budget(Some(required), None);
        let exact_execution = exact_budget
            .begin_query()
            .expect("exact setup query should admit");
        let setup = prepare_support_bundle_setup(&request, &exact_execution)
            .expect("exact setup preflight should admit before allocation");
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            required
        );
        assert_eq!(setup.tenant_id, "team-a");
        assert_eq!(setup.actor.id, "operator-7");
        assert_eq!(setup.actor.auth_scope, "oidc:example");
        assert_eq!(
            setup.reservation.bytes(),
            setup
                .root_retained_bytes
                .saturating_add(setup.synthetic_retained_bytes)
        );
        drop(setup);
        drop(exact_execution);
        let exact_after = exact_budget.snapshot();
        assert_eq!(exact_after.active_queries, 0);
        assert_eq!(exact_after.shared_reserved_memory_bytes, 0);
        assert_eq!(exact_after.accounting_invariant_violations_total, 0);

        let below_budget = support_bundle_test_budget(Some(required.saturating_sub(1)), None);
        let below_execution = below_budget
            .begin_query()
            .expect("below-boundary setup query should admit its slot");
        let error = prepare_support_bundle_setup(&request, &below_execution)
            .expect_err("one byte below the setup preflight must reject before allocation");
        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
        drop(below_execution);
        let below_after = below_budget.snapshot();
        assert_eq!(below_after.active_queries, 0);
        assert_eq!(below_after.shared_reserved_memory_bytes, 0);
        assert_eq!(below_after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_setup_model_tracks_normalized_tenant_not_input_spelling() {
        let header_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/support_bundle".to_string(),
            headers: std::collections::HashMap::from([(
                tenant::TENANT_HEADER.to_string(),
                "team-a".to_string(),
            )]),
            body: Vec::new(),
        };
        let plain_override = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/support_bundle?tenant=team-a".to_string(),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        };
        let encoded_override = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/support_bundle?tenant=team%2Da".to_string(),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        };
        let expected = modeled_support_bundle_setup_preflight_bytes(&header_request);
        assert_eq!(
            modeled_support_bundle_setup_preflight_bytes(&plain_override),
            expected
        );
        assert_eq!(
            modeled_support_bundle_setup_preflight_bytes(&encoded_override),
            expected
        );

        let lossy_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/support_bundle?tenant=%FFteam".to_string(),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        };
        validate_support_bundle_tenant_before_admission(&lossy_request)
            .expect("lossy tenant should validate after normalization");
        assert_eq!(
            support_bundle_normalized_tenant_len_before_admission(&lossy_request),
            "�team".len()
        );
        let required = modeled_support_bundle_setup_preflight_bytes(&lossy_request);
        let budget = support_bundle_test_budget(Some(required), None);
        let execution = budget
            .begin_query()
            .expect("exact lossy setup query should admit");
        let setup = prepare_support_bundle_setup(&lossy_request, &execution)
            .expect("exact lossy setup preflight should cover normalized output");
        assert_eq!(setup.tenant_id, "�team");
        assert_eq!(
            budget.snapshot().peak_shared_reserved_memory_bytes,
            required
        );
        assert_eq!(
            setup.reservation.bytes(),
            setup
                .root_retained_bytes
                .saturating_add(setup.synthetic_retained_bytes)
        );
        drop(setup);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_header_only_actor_setup_has_an_exact_memory_boundary() {
        let actor_cases = [
            (
                "OIDC provider",
                std::collections::HashMap::from([
                    (tenant::TENANT_HEADER.to_string(), "team-a".to_string()),
                    (
                        rbac::RBAC_AUTH_PRINCIPAL_ID_HEADER.to_string(),
                        "p".to_string(),
                    ),
                    (rbac::RBAC_AUTH_PROVIDER_HEADER.to_string(), "x".to_string()),
                ]),
                "p".to_string(),
                "oidc:x".to_string(),
            ),
            (
                "internal node",
                std::collections::HashMap::from([
                    (tenant::TENANT_HEADER.to_string(), "team-a".to_string()),
                    ("x-tsink-node-id".to_string(), "n".to_string()),
                ]),
                "node:n".to_string(),
                "internal_node".to_string(),
            ),
            (
                "bearer token",
                std::collections::HashMap::from([
                    (tenant::TENANT_HEADER.to_string(), "team-a".to_string()),
                    ("authorization".to_string(), "Bearer secret".to_string()),
                ]),
                bearer_audit_actor_id("secret"),
                "bearer".to_string(),
            ),
        ];

        for (case, headers, expected_id, expected_scope) in actor_cases {
            let request = HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/admin/support_bundle".to_string(),
                headers,
                body: Vec::new(),
            };
            validate_support_bundle_tenant_before_admission(&request)
                .expect("header-only setup tenant should validate");
            let required = modeled_support_bundle_setup_preflight_bytes(&request);
            let budget = support_bundle_test_budget(Some(required), None);
            let execution = budget
                .begin_query()
                .expect("exact header-only setup query should admit");
            let setup = prepare_support_bundle_setup(&request, &execution)
                .unwrap_or_else(|_| panic!("exact header-only {case} setup should admit"));

            assert_eq!(
                budget.snapshot().peak_shared_reserved_memory_bytes,
                required
            );
            assert_eq!(setup.actor.id, expected_id);
            assert_eq!(setup.actor.auth_scope, expected_scope);
            assert_eq!(setup.actor.id.capacity(), setup.actor.id.len());
            assert_eq!(
                setup.actor.auth_scope.capacity(),
                setup.actor.auth_scope.len()
            );
            assert_eq!(
                setup.reservation.bytes(),
                setup
                    .root_retained_bytes
                    .saturating_add(setup.synthetic_retained_bytes)
            );
            drop(setup);
            drop(execution);
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);

            if case == "OIDC provider" {
                let below_budget =
                    support_bundle_test_budget(Some(required.saturating_sub(1)), None);
                let below_execution = below_budget
                    .begin_query()
                    .expect("below-boundary header-only setup query should admit its slot");
                let error = prepare_support_bundle_setup(&request, &below_execution)
                    .expect_err("one byte below header-only setup preflight must reject");
                assert_eq!(error.status, 413);
                assert_eq!(
                    header(&error, READ_ERROR_CODE_HEADER),
                    Some("query_limit_per_query_memory_bytes")
                );
                assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
                drop(below_execution);
                let below_after = below_budget.snapshot();
                assert_eq!(below_after.active_queries, 0);
                assert_eq!(below_after.shared_reserved_memory_bytes, 0);
                assert_eq!(below_after.accounting_invariant_violations_total, 0);
            }
        }
    }

    #[test]
    fn support_bundle_setup_does_not_copy_sensitive_headers_or_body() {
        let sensitive_token = "support-bundle-secret-token";
        let sensitive_cookie = "session=support-bundle-secret-cookie";
        let body = sensitive_token.repeat(4_096).into_bytes();
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/support_bundle?tenant=team-a".to_string(),
            headers: std::collections::HashMap::from([
                (
                    "authorization".to_string(),
                    format!("Bearer {sensitive_token}"),
                ),
                ("cookie".to_string(), sensitive_cookie.to_string()),
                ("x-request-trace".to_string(), "private-trace".to_string()),
            ]),
            body,
        };
        let bodyless_request = HttpRequest {
            method: request.method.clone(),
            path: request.path.clone(),
            headers: request.headers.clone(),
            body: Vec::new(),
        };
        assert_eq!(
            modeled_support_bundle_setup_preflight_bytes(&request),
            modeled_support_bundle_setup_preflight_bytes(&bodyless_request),
            "caller-owned request bodies must not be copied or charged as setup output"
        );

        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("sensitive-header setup query should admit");
        let setup = prepare_support_bundle_setup(&request, &execution)
            .expect("controlled support-bundle setup should succeed");
        let synthetic = setup
            .synthetic_request
            .as_ref()
            .expect("synthetic request should remain live");
        assert_eq!(synthetic.method, "GET");
        assert_eq!(synthetic.path, SUPPORT_BUNDLE_TSDB_STATUS_PATH);
        assert!(synthetic.body.is_empty());
        assert_eq!(synthetic.headers.len(), 2);
        assert_eq!(
            synthetic.header(rbac::RBAC_AUTH_VERIFIED_HEADER),
            Some("true")
        );
        assert_eq!(synthetic.header(tenant::TENANT_HEADER), Some("team-a"));
        assert!(synthetic.header("authorization").is_none());
        assert!(synthetic.header("cookie").is_none());
        assert!(synthetic.header("x-request-trace").is_none());
        assert!(!setup.actor.id.contains(sensitive_token));

        drop(setup);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_setup_cancellation_releases_the_query_and_reservation() {
        let request = support_bundle_setup_request();
        let budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(token.clone())
            .expect("setup cancellation query should admit");
        let mut setup = prepare_support_bundle_setup(&request, &execution)
            .expect("setup should finish before cancellation");
        let reserved = setup.reservation.bytes();
        assert!(reserved > 0);

        token.cancel();
        let error = setup
            .reservation
            .resize(reserved.saturating_add(1))
            .expect_err("cancellation must stop setup reservation growth");
        assert!(matches!(error, tsink::QueryBudgetError::Cancelled));
        assert_eq!(setup.reservation.bytes(), reserved);
        drop(setup);
        drop(execution);

        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_setup_preserves_the_envelope_schema() {
        let request = support_bundle_setup_request();
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("schema setup query should admit");
        let setup = prepare_support_bundle_setup(&request, &execution)
            .expect("schema setup should succeed");
        let json_response =
            support_bundle_test_response(br#"{"status":"success","data":{"value":7}}"#.to_vec());
        let text_response = HttpResponse::new(503, "component unavailable");
        let mut envelope = support_bundle_test_envelope(&json_response, &text_response);
        envelope.tenant_id = &setup.tenant_id;
        envelope.requested_by = SupportBundleRequestedBy {
            id: &setup.actor.id,
            auth_scope: &setup.actor.auth_scope,
        };

        let encoded =
            serde_json::to_value(&envelope).expect("support-bundle envelope should encode");
        assert_eq!(encoded["tenantId"], "team-a");
        assert_eq!(encoded["requestedBy"]["id"], "operator-7");
        assert_eq!(encoded["requestedBy"]["authScope"], "oidc:example");
        assert!(encoded.get("generatedUnixMs").is_some());
        assert!(encoded.get("serverVersion").is_some());
        let sections = encoded["sections"]
            .as_object()
            .expect("sections should remain an object");
        assert_eq!(sections.len(), 11);
        for name in [
            "statusTsdb",
            "usage",
            "rbacState",
            "rbacAudit",
            "securityState",
            "clusterAudit",
            "clusterHandoff",
            "clusterRepair",
            "clusterRebalance",
            "rules",
            "rollups",
        ] {
            assert!(sections.contains_key(name), "missing schema section {name}");
        }

        drop(setup);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn support_bundle_invalid_tenants_precede_blocked_query_admission() {
        let mut query_limits = tsink::ResourceLimits::test().query;
        query_limits.max_concurrent_queries = Some(1);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_resource_profile(tsink::ResourceProfile::Test)
            .with_query_budget_limits(query_limits)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("precedence storage should build");
        let blocker = storage
            .begin_query_execution(
                tsink::QueryWorkLimits::default(),
                tsink::QueryCancellationToken::new(),
            )
            .expect("blocking query admission should execute")
            .expect("blocking query execution should be available");
        let before = storage.query_budget_snapshot();
        let metadata_store = Arc::new(
            MetricMetadataStore::open(None).expect("precedence metadata store should build"),
        );
        let exemplar_store =
            Arc::new(ExemplarStore::open(None).expect("precedence exemplar store should build"));
        let tenant_registry =
            tenant::TenantRegistry::from_json_str(r#"{"defaults":{},"tenants":{}}"#)
                .expect("precedence tenant registry should build");
        let oversized = "x".repeat(tsink::label::MAX_LABEL_VALUE_LEN.saturating_add(1));
        let lossy_expansion = "%FF".repeat(
            tsink::label::MAX_LABEL_VALUE_LEN
                .checked_div(char::REPLACEMENT_CHARACTER.len_utf8())
                .unwrap_or(0)
                .saturating_add(1),
        );
        let requests = [
            (
                "decoded length",
                HttpRequest {
                    method: "GET".to_string(),
                    path: format!("/api/v1/admin/support_bundle?tenant={oversized}"),
                    headers: std::collections::HashMap::new(),
                    body: Vec::new(),
                },
            ),
            (
                "lossy decoded expansion",
                HttpRequest {
                    method: "GET".to_string(),
                    path: format!("/api/v1/admin/support_bundle?tenant={lossy_expansion}"),
                    headers: std::collections::HashMap::new(),
                    body: Vec::new(),
                },
            ),
            (
                "conflicting headers",
                HttpRequest {
                    method: "GET".to_string(),
                    path: "/api/v1/admin/support_bundle".to_string(),
                    headers: std::collections::HashMap::from([
                        (tenant::TENANT_HEADER.to_string(), "team-a".to_string()),
                        (
                            tenant::SCOPE_ORG_ID_HEADER.to_string(),
                            "team-b".to_string(),
                        ),
                    ]),
                    body: Vec::new(),
                },
            ),
            (
                "empty header",
                HttpRequest {
                    method: "GET".to_string(),
                    path: "/api/v1/admin/support_bundle".to_string(),
                    headers: std::collections::HashMap::from([(
                        tenant::TENANT_HEADER.to_string(),
                        " \t ".to_string(),
                    )]),
                    body: Vec::new(),
                },
            ),
            (
                "header control character",
                HttpRequest {
                    method: "GET".to_string(),
                    path: "/api/v1/admin/support_bundle".to_string(),
                    headers: std::collections::HashMap::from([(
                        tenant::TENANT_HEADER.to_string(),
                        "team\0a".to_string(),
                    )]),
                    body: Vec::new(),
                },
            ),
            (
                "decoded control character",
                HttpRequest {
                    method: "GET".to_string(),
                    path: "/api/v1/admin/support_bundle?tenant=team%00a".to_string(),
                    headers: std::collections::HashMap::new(),
                    body: Vec::new(),
                },
            ),
            (
                "decoded override conflict",
                HttpRequest {
                    method: "GET".to_string(),
                    path: "/api/v1/admin/support_bundle?tenant=team-a".to_string(),
                    headers: std::collections::HashMap::from([(
                        tenant::SCOPE_ORG_ID_HEADER.to_string(),
                        "team-b".to_string(),
                    )]),
                    body: Vec::new(),
                },
            ),
        ];

        for (case, request) in requests {
            let response = handle_admin_support_bundle(
                &storage,
                &metadata_store,
                &exemplar_store,
                None,
                &request,
                None,
                None,
                Some(&tenant_registry),
                None,
                None,
                None,
                None,
                None,
            )
            .await;

            assert_eq!(response.status, 400, "case: {case}");
            assert_eq!(storage.query_budget_snapshot(), before, "case: {case}");
            assert_eq!(
                tenant_registry.initialized_runtime_count(),
                0,
                "case: {case}"
            );
        }
        drop(blocker);
        let after = storage.query_budget_snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.concurrency_rejections_total, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn support_bundle_prewarms_token_tenant_before_blocked_root_without_auth_or_parity_drift()
    {
        let mut query_limits = tsink::ResourceLimits::test().query;
        query_limits.max_concurrent_queries = Some(1);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_resource_profile(tsink::ResourceProfile::Test)
            .with_query_budget_limits(query_limits)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("prewarm precedence storage should build");
        let blocker = storage
            .begin_query_execution(
                tsink::QueryWorkLimits::default(),
                tsink::QueryCancellationToken::new(),
            )
            .expect("blocking query admission should execute")
            .expect("blocking query execution should be available");
        let metadata_store =
            Arc::new(MetricMetadataStore::open(None).expect("prewarm metadata store should build"));
        let exemplar_store =
            Arc::new(ExemplarStore::open(None).expect("prewarm exemplar store should build"));
        let tenant_registry = tenant::TenantRegistry::from_json_str(
            r#"{
                "defaults": {},
                "tenants": {
                    "team-a": {
                        "auth": {
                            "tokens": [{ "token": "team-a-read", "scopes": ["read"] }]
                        }
                    }
                }
            }"#,
        )
        .expect("token tenant registry should build");
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/support_bundle?tenant=team-a".to_string(),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        };

        let expected = handle_admin_support_bundle(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &request,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
        let actual = handle_admin_support_bundle(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &request,
            None,
            None,
            Some(&tenant_registry),
            None,
            None,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(actual.status, expected.status);
        assert_eq!(actual.headers, expected.headers);
        assert_eq!(actual.body, expected.body);
        assert_eq!(actual.status, 429);
        assert_eq!(tenant_registry.initialized_runtime_count(), 1);
        initialize_support_bundle_tenant_runtime_before_admission(&request, Some(&tenant_registry))
            .expect("repeated prewarm should remain idempotent");
        assert_eq!(tenant_registry.initialized_runtime_count(), 1);

        drop(blocker);
        let after = storage.query_budget_snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.concurrency_rejections_total, 2);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_prewarm_uses_stack_normalized_lossy_and_fallback_tenants() {
        let tenant_registry =
            tenant::TenantRegistry::from_json_str(r#"{"defaults":{},"tenants":{}}"#)
                .expect("prewarm tenant registry should build");
        let cases = [
            (
                HttpRequest {
                    method: "GET".to_string(),
                    path: "/api/v1/admin/support_bundle?tenant=%FFteam".to_string(),
                    headers: std::collections::HashMap::new(),
                    body: Vec::new(),
                },
                "�team",
            ),
            (
                HttpRequest {
                    method: "GET".to_string(),
                    path: "/api/v1/admin/support_bundle?tenant=%20%20".to_string(),
                    headers: std::collections::HashMap::from([(
                        tenant::SCOPE_ORG_ID_HEADER.to_string(),
                        "team-b".to_string(),
                    )]),
                    body: Vec::new(),
                },
                "team-b",
            ),
        ];

        for (index, (request, expected_tenant)) in cases.into_iter().enumerate() {
            validate_support_bundle_tenant_before_admission(&request)
                .expect("prewarm tenant should validate");
            initialize_support_bundle_tenant_runtime_before_admission(
                &request,
                Some(&tenant_registry),
            )
            .expect("prewarm should initialize the normalized tenant");
            let expected_count = index.saturating_add(1);
            assert_eq!(tenant_registry.initialized_runtime_count(), expected_count);

            tenant_registry
                .initialize_tenant_runtime(expected_tenant)
                .expect("direct initialization should resolve the same tenant");
            assert_eq!(tenant_registry.initialized_runtime_count(), expected_count);
            initialize_support_bundle_tenant_runtime_before_admission(
                &request,
                Some(&tenant_registry),
            )
            .expect("repeated stack-only prewarm should remain idempotent");
            assert_eq!(tenant_registry.initialized_runtime_count(), expected_count);
        }
    }

    #[tokio::test]
    async fn support_bundle_blocked_root_prewarm_stops_at_tenant_runtime_cap() {
        let mut query_limits = tsink::ResourceLimits::test().query;
        query_limits.max_concurrent_queries = Some(1);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_resource_profile(tsink::ResourceProfile::Test)
            .with_query_budget_limits(query_limits)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("bounded-prewarm storage should build");
        let blocker = storage
            .begin_query_execution(
                tsink::QueryWorkLimits::default(),
                tsink::QueryCancellationToken::new(),
            )
            .expect("blocking query admission should execute")
            .expect("blocking query execution should be available");
        let metadata_store = Arc::new(
            MetricMetadataStore::open(None).expect("bounded-prewarm metadata store should build"),
        );
        let exemplar_store = Arc::new(
            ExemplarStore::open(None).expect("bounded-prewarm exemplar store should build"),
        );
        // `default` owns one reserved slot, leaving exactly one slot for arbitrary tenants.
        let tenant_registry = tenant::TenantRegistry::from_json_str(r#"{"maxRuntimeTenants":2}"#)
            .expect("bounded tenant registry should build");

        let request_for = |tenant: Option<&str>| HttpRequest {
            method: "GET".to_string(),
            path: tenant.map_or_else(
                || "/api/v1/admin/support_bundle".to_string(),
                |tenant| format!("/api/v1/admin/support_bundle?tenant={tenant}"),
            ),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        };
        let call = |request: HttpRequest| {
            let storage = &storage;
            let metadata_store = &metadata_store;
            let exemplar_store = &exemplar_store;
            let tenant_registry = &tenant_registry;
            async move {
                handle_admin_support_bundle(
                    storage,
                    metadata_store,
                    exemplar_store,
                    None,
                    &request,
                    None,
                    None,
                    Some(tenant_registry),
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
            }
        };

        let first = call(request_for(Some("team-a"))).await;
        assert_eq!(first.status, 429);
        assert_eq!(tenant_registry.initialized_runtime_count(), 1);

        let overflow = call(request_for(Some("team-b"))).await;
        assert_eq!(overflow.status, 503);
        assert_eq!(
            header(&overflow, "X-Tsink-Tenant-Error-Code"),
            Some("tenant_runtime_cache_limit_exceeded")
        );
        assert!(!String::from_utf8_lossy(&overflow.body).contains("team-b"));
        assert_eq!(tenant_registry.initialized_runtime_count(), 1);

        let default = call(request_for(None)).await;
        assert_eq!(default.status, 429);
        assert_eq!(tenant_registry.initialized_runtime_count(), 2);
        let repeated = call(request_for(Some("team-a"))).await;
        assert_eq!(repeated.status, 429);
        assert_eq!(tenant_registry.initialized_runtime_count(), 2);
        let second_overflow = call(request_for(Some("team-c"))).await;
        assert_eq!(second_overflow.status, 503);
        assert_eq!(tenant_registry.initialized_runtime_count(), 2);

        drop(blocker);
        let after = storage.query_budget_snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.concurrency_rejections_total, 3);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_sections_embed_raw_json_and_bound_non_json_text() {
        let json_response =
            support_bundle_test_response(br#"{"status":"success","data":{"value":7}}"#.to_vec());
        let text_response = HttpResponse::new(
            503,
            "é".repeat(SUPPORT_BUNDLE_TEXT_MAX_CHARS.saturating_add(1)),
        );

        let json_section = serde_json::to_value(SupportBundleResponseSection {
            response: &json_response,
        })
        .expect("JSON section should encode");
        assert_eq!(json_section["httpStatus"], 200);
        assert_eq!(json_section["contentType"], "application/json");
        assert_eq!(json_section["body"]["data"]["value"], 7);
        assert!(json_section.get("bodyText").is_none());

        let text_section = serde_json::to_value(SupportBundleResponseSection {
            response: &text_response,
        })
        .expect("text section should encode");
        let body_text = text_section["bodyText"]
            .as_str()
            .expect("text section should contain bodyText");
        assert_eq!(
            body_text
                .chars()
                .take(SUPPORT_BUNDLE_TEXT_MAX_CHARS)
                .count(),
            SUPPORT_BUNDLE_TEXT_MAX_CHARS
        );
        assert!(body_text.ends_with("...[truncated]"));
        assert!(text_section.get("body").is_none());

        let invalid = vec![
            0xff;
            SUPPORT_BUNDLE_TEXT_MAX_CHARS
                .saturating_mul(4)
                .saturating_add(8)
        ];
        let invalid_text = bounded_support_bundle_text(&invalid);
        assert_eq!(
            invalid_text
                .trim_end_matches("...[truncated]")
                .chars()
                .count(),
            SUPPORT_BUNDLE_TEXT_MAX_CHARS
        );
        assert!(invalid_text.ends_with("...[truncated]"));
    }

    #[test]
    fn support_bundle_child_retained_envelope_has_an_exact_boundary() {
        let response = support_bundle_test_response(br#"{"status":"success"}"#.to_vec());
        let response_bytes = modeled_tsdb_status_response_retained_bytes(&response);
        assert!(response_bytes < SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES);
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("child boundary query should admit");

        let mut exact = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES - response_bytes;
        let accounted = account_support_bundle_child(response, &execution, &mut exact)
            .expect("exact child retained-byte boundary should pass");
        assert_eq!(exact, SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES);
        assert_eq!(execution.snapshot().memory_reserved_bytes, response_bytes);

        let mut one_over = SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES - response_bytes + 1;
        let error = account_support_bundle_child(
            support_bundle_test_response(br#"{"status":"success"}"#.to_vec()),
            &execution,
            &mut one_over,
        )
        .expect_err("one byte beyond the child envelope should fail");
        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("support_bundle_child_responses_too_large")
        );
        drop(accounted);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn support_bundle_child_reservations_remain_cumulative_until_composition_finishes() {
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("cumulative child query should admit");
        let composition_bytes = 17;
        let composition = execution
            .reserve_memory(composition_bytes)
            .expect("composition reservation should admit");
        let mut retained = 0;

        let first = account_support_bundle_child(
            support_bundle_test_response(br#"{"child":1}"#.to_vec()),
            &execution,
            &mut retained,
        )
        .expect("first child should admit");
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            composition_bytes + retained
        );
        let first_retained = retained;

        let second = account_support_bundle_child(
            HttpResponse::new(503, "component unavailable"),
            &execution,
            &mut retained,
        )
        .expect("second child should admit");
        assert!(retained > first_retained);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            composition_bytes + retained
        );

        drop(second);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            composition_bytes + first_retained
        );
        drop(first);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            composition_bytes
        );
        drop(composition);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_child_cancellation_releases_response_and_query_reservations() {
        let budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(token.clone())
            .expect("cancellation child query should admit");
        token.cancel();
        let mut retained = 0;

        let error = account_support_bundle_child(
            support_bundle_test_response(br#"{"child":"cancelled"}"#.to_vec()),
            &execution,
            &mut retained,
        )
        .expect_err("cancelled child accounting should fail");
        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        assert_eq!(retained, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_compatibility_error_transfer_covers_worst_tenant_and_rebalance_shapes() {
        let tenant_id = "t".repeat(tsink::label::MAX_LABEL_VALUE_LEN);
        let tenant_error = tenant::TenantRequestError::TooManyRequests(format!(
            "tenant '{tenant_id}' exceeded max inflight metadata requests (1)"
        ))
        .to_http_response();
        let mut responses = vec![tenant_error];
        responses.push(admin_rebalance_error_response(
            503,
            "rebalance_runtime_unavailable",
            "cluster rebalance scheduler runtime is not available",
        ));
        for error in [
            AdminRebalanceResponseError::EncodedLimit,
            AdminRebalanceResponseError::Measurement,
            AdminRebalanceResponseError::Allocation,
            AdminRebalanceResponseError::Serialization,
            AdminRebalanceResponseError::LengthChanged,
            AdminRebalanceResponseError::Budget(tsink::QueryBudgetError::Cancelled),
            AdminRebalanceResponseError::Budget(tsink::QueryBudgetError::DeadlineExceeded),
        ] {
            responses.push(admin_rebalance_response_error_response(error, None));
        }
        for response in &responses {
            assert!(
                modeled_support_bundle_compatibility_error_construction_bytes(response)
                    <= SUPPORT_BUNDLE_COMPATIBILITY_RAW_SCRATCH_BYTES,
                "compatibility error exceeded the admitted construction model: status {} body {} bytes",
                response.status,
                response.body.len()
            );
        }

        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("compatibility transfer query should admit");
        let scratch = execution
            .reserve_memory(SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES)
            .expect("compatibility scratch should admit");
        let scratch_proof = admit_support_bundle_compatibility_scratch(&scratch)
            .expect("compatibility scratch proof should build");
        for response in responses {
            let expected = modeled_tsdb_status_response_retained_bytes(&response);
            let mut retained = 0;
            let accounted = transfer_support_bundle_scratch_child(
                response,
                &execution,
                &mut retained,
                SupportBundleScratchErrorSurface::ClusterRebalance,
                scratch_proof,
            )
            .expect("bounded compatibility response should transfer to an exact guard");
            assert_eq!(accounted.reservation.bytes(), expected);
            assert_eq!(retained, expected);
            assert_eq!(
                execution.snapshot().memory_reserved_bytes,
                SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES.saturating_add(expected)
            );
            drop(accounted);
            assert_eq!(
                execution.snapshot().memory_reserved_bytes,
                SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES
            );
        }
        drop(scratch);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_compatibility_transfer_preserves_surface_specific_budget_errors() {
        let response = tsdb_status_error_response(
            500,
            "execution",
            "status_task_failed",
            "TSDB status worker task failed",
            None,
        );
        let tsdb_response = response.clone();
        let rebalance_response = response.clone();
        let retained = modeled_tsdb_status_response_retained_bytes(&tsdb_response);
        assert_eq!(
            retained,
            modeled_tsdb_status_response_retained_bytes(&rebalance_response)
        );
        let maximum = SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES
            .saturating_add(retained)
            .saturating_sub(1);

        let tsdb_budget = support_bundle_test_budget(Some(maximum), None);
        let tsdb_execution = tsdb_budget
            .begin_query()
            .expect("TSDB transfer query should admit");
        let tsdb_scratch = tsdb_execution
            .reserve_memory(SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES)
            .expect("TSDB scratch should admit");
        let tsdb_scratch_proof = admit_support_bundle_compatibility_scratch(&tsdb_scratch)
            .expect("TSDB scratch proof should build");
        let mut tsdb_retained = 0;
        let tsdb_error = transfer_support_bundle_scratch_child(
            tsdb_response,
            &tsdb_execution,
            &mut tsdb_retained,
            SupportBundleScratchErrorSurface::TsdbStatus,
            tsdb_scratch_proof,
        )
        .expect_err("one byte below the transfer peak should preserve the TSDB mapper");
        assert_eq!(tsdb_error.status, 413);
        assert_eq!(
            header(&tsdb_error, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        let tsdb_body = serde_json::from_slice::<JsonValue>(&tsdb_error.body)
            .expect("TSDB transfer failure should be JSON");
        assert_eq!(tsdb_body["errorType"], "query_limit_per_query_memory_bytes");
        assert!(tsdb_body["error"]
            .as_str()
            .is_some_and(|message| message.starts_with("TSDB status query exceeded")));
        assert_eq!(tsdb_retained, 0);
        drop(tsdb_scratch);
        drop(tsdb_execution);

        let rebalance_budget = support_bundle_test_budget(Some(maximum), None);
        let rebalance_execution = rebalance_budget
            .begin_query()
            .expect("rebalance transfer query should admit");
        let rebalance_scratch = rebalance_execution
            .reserve_memory(SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES)
            .expect("rebalance scratch should admit");
        let rebalance_scratch_proof =
            admit_support_bundle_compatibility_scratch(&rebalance_scratch)
                .expect("rebalance scratch proof should build");
        let mut rebalance_retained = 0;
        let rebalance_error = transfer_support_bundle_scratch_child(
            rebalance_response,
            &rebalance_execution,
            &mut rebalance_retained,
            SupportBundleScratchErrorSurface::ClusterRebalance,
            rebalance_scratch_proof,
        )
        .expect_err("one byte below the transfer peak should preserve the rebalance mapper");
        assert_eq!(rebalance_error.status, 413);
        assert_eq!(header(&rebalance_error, READ_ERROR_CODE_HEADER), None);
        let rebalance_body = serde_json::from_slice::<JsonValue>(&rebalance_error.body)
            .expect("rebalance transfer failure should be JSON");
        assert_eq!(
            rebalance_body["errorType"],
            "query_limit_per_query_memory_bytes"
        );
        assert!(rebalance_body["error"]
            .as_str()
            .is_some_and(|message| message.starts_with("admin rebalance response exceeded")));
        assert_eq!(rebalance_retained, 0);
        drop(rebalance_scratch);
        drop(rebalance_execution);

        for budget in [tsdb_budget, rebalance_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[tokio::test]
    async fn support_bundle_tsdb_compatibility_transfer_preserves_raw_direct_error() {
        let storage: Arc<dyn Storage> = Arc::new(UnaccountedOnlyRollupStorage {
            legacy_snapshot_calls: AtomicU64::new(0),
        });
        let metadata_store = Arc::new(
            MetricMetadataStore::open(None).expect("TSDB parity metadata store should build"),
        );
        let exemplar_store =
            Arc::new(ExemplarStore::open(None).expect("TSDB parity exemplar store should build"));
        let request = HttpRequest {
            method: "GET".to_string(),
            path: SUPPORT_BUNDLE_TSDB_STATUS_PATH.to_string(),
            headers: std::collections::HashMap::from([
                (
                    rbac::RBAC_AUTH_VERIFIED_HEADER.to_string(),
                    "true".to_string(),
                ),
                (tenant::TENANT_HEADER.to_string(), "team-a".to_string()),
            ]),
            body: Vec::new(),
        };
        let legacy = handle_tsdb_status(
            &storage,
            &metadata_store,
            &exemplar_store,
            &request,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("TSDB parity query should admit");
        let scratch = execution
            .reserve_memory(SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES)
            .expect("TSDB parity scratch should admit");
        let scratch_proof = admit_support_bundle_compatibility_scratch(&scratch)
            .expect("TSDB parity scratch proof should build");
        let raw = handle_tsdb_status_with_execution(
            &storage,
            &metadata_store,
            &exemplar_store,
            &request,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &execution,
        )
        .await
        .expect_err("unaccounted TSDB source should return its raw compatibility child");
        assert_eq!(raw.status, legacy.status);
        assert_eq!(raw.headers, legacy.headers);
        assert_eq!(raw.body, legacy.body);
        let expected_retained = modeled_tsdb_status_response_retained_bytes(&raw);
        let mut retained = 0;
        let accounted = transfer_support_bundle_scratch_child(
            raw,
            &execution,
            &mut retained,
            SupportBundleScratchErrorSurface::TsdbStatus,
            scratch_proof,
        )
        .expect("raw TSDB compatibility child should transfer");
        assert_eq!(accounted.response.status, legacy.status);
        assert_eq!(accounted.response.headers, legacy.headers);
        assert_eq!(accounted.response.body, legacy.body);
        assert_eq!(accounted.reservation.bytes(), expected_retained);
        assert_eq!(retained, expected_retained);
        drop(accounted);
        drop(scratch);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn support_bundle_rebalance_compatibility_transfer_preserves_both_unavailable_shapes() {
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_resource_profile(tsink::ResourceProfile::Test)
            .build()
            .expect("rebalance parity storage should build");
        let temp_dir = tempfile::TempDir::new().expect("rebalance parity tempdir should build");
        let context_without_digest = support_bundle_handoff_context(&temp_dir, false);
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("rebalance parity query should admit");
        let scratch = execution
            .reserve_memory(SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES)
            .expect("rebalance parity scratch should admit");
        let scratch_proof = admit_support_bundle_compatibility_scratch(&scratch)
            .expect("rebalance parity scratch proof should build");

        for context in [None, Some(context_without_digest.as_ref())] {
            let legacy = handle_admin_cluster_rebalance_status(&storage, context).await;
            let raw =
                handle_admin_cluster_rebalance_status_with_execution(&storage, context, &execution)
                    .await
                    .expect_err(
                        "unavailable rebalance runtime should return its raw compatibility child",
                    );
            assert_eq!(raw.status, legacy.status);
            assert_eq!(raw.headers, legacy.headers);
            assert_eq!(raw.body, legacy.body);
            let expected_retained = modeled_tsdb_status_response_retained_bytes(&raw);
            let mut retained = 0;
            let accounted = transfer_support_bundle_scratch_child(
                raw,
                &execution,
                &mut retained,
                SupportBundleScratchErrorSurface::ClusterRebalance,
                scratch_proof,
            )
            .expect("raw rebalance compatibility child should transfer");
            assert_eq!(accounted.response.status, legacy.status);
            assert_eq!(accounted.response.headers, legacy.headers);
            assert_eq!(accounted.response.body, legacy.body);
            assert_eq!(accounted.reservation.bytes(), expected_retained);
            assert_eq!(retained, expected_retained);
            drop(accounted);
            assert_eq!(
                execution.snapshot().memory_reserved_bytes,
                SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES
            );
        }
        drop(scratch);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_shared_wrappers_require_pre_admitted_error_transfer() {
        let handlers = include_str!("../../handlers.rs");
        let tsdb_start = handlers
            .find("async fn handle_tsdb_status_with_execution(")
            .expect("shared TSDB wrapper should exist");
        let tsdb_end = handlers[tsdb_start..]
            .find("\nasync fn handle_tsdb_status_impl(")
            .map(|offset| tsdb_start.saturating_add(offset))
            .expect("shared TSDB wrapper source boundary should exist");
        let tsdb_wrapper = &handlers[tsdb_start..tsdb_end];
        assert!(tsdb_wrapper.contains("None => Err(response)"));
        assert!(!tsdb_wrapper.contains("account_completed_http_response"));

        let cluster = include_str!("cluster.rs");
        let rebalance_start = cluster
            .find("pub(crate) async fn handle_admin_cluster_rebalance_status_with_execution(")
            .expect("shared rebalance wrapper should exist");
        let rebalance_end = cluster[rebalance_start..]
            .find("\nasync fn execute_admin_cluster_rebalance(")
            .map(|offset| rebalance_start.saturating_add(offset))
            .expect("shared rebalance wrapper source boundary should exist");
        let rebalance_wrapper = &cluster[rebalance_start..rebalance_end];
        assert!(rebalance_wrapper.contains("None => Err(response)"));
        assert!(!rebalance_wrapper.contains("account_completed_http_response"));

        let usage_support = include_str!("usage_support.rs");
        let root_start = usage_support
            .find("pub(crate) async fn handle_admin_support_bundle(")
            .expect("support-bundle root should exist");
        let root_end = usage_support[root_start..]
            .find("\npub(crate) async fn handle_admin_usage_report(")
            .map(|offset| root_start.saturating_add(offset))
            .expect("support-bundle root source boundary should exist");
        let root = &usage_support[root_start..root_end];
        assert!(root.contains("admit_support_bundle_compatibility_scratch"));
        assert_eq!(
            root.matches("transfer_support_bundle_scratch_child(")
                .count(),
            2
        );
        assert!(!root.contains("account_completed_http_response("));
    }

    fn support_bundle_usage_test_accounting(
        report_max_response_bytes: Option<usize>,
    ) -> Arc<UsageAccounting> {
        let mut limits = crate::usage::UsageLedgerLimits::default();
        if let Some(maximum) = report_max_response_bytes {
            limits.report_max_response_bytes = maximum;
        }
        let accounting = UsageAccounting::open_with_limits_and_disk_budget(None, limits, None)
            .expect("support-bundle usage accounting should open");

        let mut ingest =
            UsageRecordInput::success("team-a", UsageCategory::Ingest, "write", "support-test");
        ingest.rows = 17;
        accounting
            .record(ingest)
            .expect("support-bundle usage ingest should record");
        let mut query =
            UsageRecordInput::success("team-a", UsageCategory::Query, "query", "support-test");
        query.result_units = 23;
        accounting
            .record(query)
            .expect("support-bundle usage query should record");
        let mut retention = UsageRecordInput::success(
            "team-a",
            UsageCategory::Retention,
            "retention",
            "support-test",
        );
        retention.tombstones_applied = 5;
        accounting
            .record(retention)
            .expect("support-bundle usage retention should record");
        accounting
            .record(UsageRecordInput::success(
                "team-a",
                UsageCategory::Background,
                "background",
                "support-test",
            ))
            .expect("support-bundle usage background should record");
        for logical_storage_bytes in [4_096, 8_192] {
            let mut storage = UsageRecordInput::success(
                "team-a",
                UsageCategory::Storage,
                "reconcile",
                "support-test",
            );
            storage.logical_storage_series = 7;
            storage.logical_storage_samples = 31;
            storage.logical_storage_bytes = logical_storage_bytes;
            accounting
                .record(storage)
                .expect("support-bundle usage storage should record");
        }
        accounting
            .record(UsageRecordInput::success(
                "team-b",
                UsageCategory::Ingest,
                "write",
                "support-test",
            ))
            .expect("support-bundle usage other tenant should record");
        accounting
    }

    fn support_bundle_usage_test_storage() -> (tempfile::TempDir, Arc<dyn Storage>) {
        let temp_dir = tempfile::TempDir::new().expect("support-bundle usage tempdir should build");
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_resource_profile(tsink::ResourceProfile::Test)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .with_data_path(temp_dir.path())
            .build()
            .expect("support-bundle usage storage should build");
        storage
            .insert_rows(&[Row::with_labels(
                "support_usage_metric",
                vec![Label::new("host", "node-\"-α")],
                DataPoint::new(1, 7.0),
            )])
            .expect("support-bundle usage storage row should insert");
        storage
            .select(
                "support_usage_metric",
                &[Label::new("host", "node-\"-α")],
                0,
                2,
            )
            .expect("support-bundle usage storage row should select");
        (temp_dir, storage)
    }

    fn legacy_support_bundle_usage_response_for_test(
        storage: &Arc<dyn Storage>,
        usage_accounting: &UsageAccounting,
        tenant_id: &str,
    ) -> HttpResponse {
        #[derive(Serialize)]
        struct LegacyPayload<'a> {
            status: &'static str,
            data: LegacyData<'a>,
        }

        #[derive(Serialize)]
        struct LegacyData<'a> {
            report: &'a crate::usage::UsageReport,
            journal: &'a crate::usage::UsageLedgerStatus,
            reconciliation: &'a JsonValue,
        }

        let report = usage_accounting.report(Some(tenant_id), None, None, UsageBucketWidth::None);
        let journal = usage_accounting.ledger_status();
        let reconciliation = usage_reconciliation_json(&report, &storage.observability_snapshot());
        let payload = LegacyPayload {
            status: "success",
            data: LegacyData {
                report: &report,
                journal: &journal,
                reconciliation: &reconciliation,
            },
        };
        let mut response = bounded_usage_json_response(
            200,
            &payload,
            usage_accounting.limits().report_max_response_bytes,
            "usage_report_response_too_large",
        );
        response
            .headers
            .retain(|(name, _)| !name.eq_ignore_ascii_case("content-type"));
        response
    }

    fn support_bundle_usage_child_response(
        storage: &Arc<dyn Storage>,
        usage_accounting: Option<&UsageAccounting>,
        tenant_id: &str,
    ) -> HttpResponse {
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("usage child query should admit");
        let mut retained_bytes = 0;
        let response = support_bundle_usage_child(
            storage,
            usage_accounting,
            tenant_id,
            &execution,
            &mut retained_bytes,
        )
        .expect("usage child should encode");
        assert_eq!(
            response.reservation.bytes(),
            modeled_tsdb_status_response_retained_bytes(&response.response)
        );
        assert_eq!(retained_bytes, response.reservation.bytes());
        let cloned = response.response.clone();
        drop(response);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
        cloned
    }

    #[test]
    fn support_bundle_usage_child_preserves_present_absent_and_two_sample_legacy_schema() {
        let (_temp_dir, storage) = support_bundle_usage_test_storage();
        let accounting = support_bundle_usage_test_accounting(None);

        for tenant_id in ["team-a", "missing-tenant"] {
            let current =
                support_bundle_usage_child_response(&storage, Some(accounting.as_ref()), tenant_id);
            let legacy = legacy_support_bundle_usage_response_for_test(
                &storage,
                accounting.as_ref(),
                tenant_id,
            );
            assert_eq!(current.status, legacy.status);
            assert!(current.headers.is_empty());
            assert_eq!(current.body, legacy.body, "tenant: {tenant_id}");
            let body: JsonValue =
                serde_json::from_slice(&current.body).expect("usage child should be JSON");
            assert_eq!(body["data"]["report"]["filter"]["tenantId"], tenant_id);
            assert_eq!(
                body["data"]["report"]["tenants"]
                    .as_array()
                    .expect("usage tenants should be an array")
                    .len(),
                usize::from(tenant_id == "team-a")
            );
            assert_eq!(
                body["data"]["report"]["page"]["recordsAggregated"],
                if tenant_id == "team-a" { 6 } else { 0 }
            );
            assert_eq!(
                body["data"]["reconciliation"]["accounted"]["latestStorageLogicalBytes"],
                if tenant_id == "team-a" { 8_192 } else { 0 }
            );
        }

        accounting.increment_failure_after_next_status_snapshot_for_test();
        let raced =
            support_bundle_usage_child_response(&storage, Some(accounting.as_ref()), "team-a");
        let raced: JsonValue =
            serde_json::from_slice(&raced.body).expect("raced usage child should be JSON");
        assert_eq!(raced["data"]["report"]["journal"]["recordFailuresTotal"], 0);
        assert_eq!(raced["data"]["journal"]["recordFailuresTotal"], 1);
    }

    #[test]
    fn support_bundle_usage_child_preserves_unavailable_and_usage_limit_responses() {
        let (_temp_dir, storage) = support_bundle_usage_test_storage();
        let unavailable = support_bundle_usage_child_response(&storage, None, "team-a");
        assert_eq!(unavailable.status, 503);
        assert!(unavailable.headers.is_empty());
        assert_eq!(unavailable.body, b"usage accounting is unavailable");
        let unavailable_section = serde_json::to_value(SupportBundleResponseSection {
            response: &unavailable,
        })
        .expect("unavailable usage section should encode");
        assert_eq!(unavailable_section["httpStatus"], 503);
        assert_eq!(
            unavailable_section["bodyText"],
            "usage accounting is unavailable"
        );
        assert!(unavailable_section.get("contentType").is_none());
        assert!(unavailable_section.get("body").is_none());

        let accounting = support_bundle_usage_test_accounting(Some(1));
        let current =
            support_bundle_usage_child_response(&storage, Some(accounting.as_ref()), "team-a");
        let legacy =
            legacy_support_bundle_usage_response_for_test(&storage, accounting.as_ref(), "team-a");
        assert_eq!(current.status, 413);
        assert!(current.headers.is_empty());
        assert_eq!(current.body, legacy.body);
        let body: JsonValue =
            serde_json::from_slice(&current.body).expect("usage limit child should be JSON");
        assert_eq!(body["error"]["code"], "usage_report_response_too_large");
        assert_eq!(body["error"]["maximumBytes"], 1);
    }

    #[test]
    fn support_bundle_usage_child_enforces_exact_local_body_limit_boundary() {
        let (_temp_dir, storage) = support_bundle_usage_test_storage();
        let calibration = support_bundle_usage_test_accounting(Some(10_000));
        let calibration_body =
            legacy_support_bundle_usage_response_for_test(&storage, calibration.as_ref(), "team-a");
        assert_eq!(calibration_body.status, 200);

        // The configured maximum is itself reported in the journal. One iteration crosses from
        // the five-digit calibration value to the stable four-digit encoded-size fixed point.
        let first_candidate = calibration_body.body.len();
        let first = support_bundle_usage_test_accounting(Some(first_candidate));
        let first_body =
            legacy_support_bundle_usage_response_for_test(&storage, first.as_ref(), "team-a");
        assert_eq!(first_body.status, 200);
        let exact_limit = first_body.body.len();
        let exact = support_bundle_usage_test_accounting(Some(exact_limit));
        let exact_legacy =
            legacy_support_bundle_usage_response_for_test(&storage, exact.as_ref(), "team-a");
        assert_eq!(exact_legacy.status, 200);
        assert_eq!(exact_legacy.body.len(), exact_limit);
        let exact_current =
            support_bundle_usage_child_response(&storage, Some(exact.as_ref()), "team-a");
        assert_eq!(exact_current.status, 200);
        assert_eq!(exact_current.body, exact_legacy.body);

        let below = support_bundle_usage_test_accounting(Some(exact_limit.saturating_sub(1)));
        let below_current =
            support_bundle_usage_child_response(&storage, Some(below.as_ref()), "team-a");
        let below_legacy =
            legacy_support_bundle_usage_response_for_test(&storage, below.as_ref(), "team-a");
        assert_eq!(below_current.status, 413);
        assert!(below_current.headers.is_empty());
        assert_eq!(below_current.body, below_legacy.body);
        let below_body: JsonValue = serde_json::from_slice(&below_current.body)
            .expect("below-boundary usage child should return JSON");
        assert_eq!(
            below_body["error"]["maximumBytes"],
            exact_limit.saturating_sub(1)
        );
    }

    #[test]
    fn support_bundle_usage_child_enforces_exact_combined_memory_peak() {
        let (_temp_dir, storage) = support_bundle_usage_test_storage();
        let accounting = support_bundle_usage_test_accounting(None);
        let calibration_budget = support_bundle_test_budget(None, None);
        let calibration_execution = calibration_budget
            .begin_query()
            .expect("usage calibration query should admit");
        let mut calibration_retained = 0;
        let calibration_response = support_bundle_usage_child(
            &storage,
            Some(accounting.as_ref()),
            "team-a",
            &calibration_execution,
            &mut calibration_retained,
        )
        .expect("usage calibration child should encode");
        let required = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(required > calibration_response.reservation.bytes());
        drop(calibration_response);
        drop(calibration_execution);

        let exact_budget = support_bundle_test_budget(Some(required), None);
        let exact_execution = exact_budget
            .begin_query()
            .expect("exact usage child query should admit");
        let mut exact_retained = 0;
        let exact_response = support_bundle_usage_child(
            &storage,
            Some(accounting.as_ref()),
            "team-a",
            &exact_execution,
            &mut exact_retained,
        )
        .expect("exact combined usage snapshot/response peak should pass");
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            required
        );
        assert_eq!(exact_retained, exact_response.reservation.bytes());
        drop(exact_response);
        drop(exact_execution);

        let below_budget = support_bundle_test_budget(Some(required.saturating_sub(1)), None);
        let below_execution = below_budget
            .begin_query()
            .expect("below-boundary usage child query should admit its slot");
        let mut below_retained = 0;
        let error = support_bundle_usage_child(
            &storage,
            Some(accounting.as_ref()),
            "team-a",
            &below_execution,
            &mut below_retained,
        )
        .expect_err("one byte below the combined usage peak must reject");
        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_eq!(below_retained, 0);
        assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
        drop(below_execution);

        for budget in [calibration_budget, exact_budget, below_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_usage_child_fails_closed_and_honors_precancellation() {
        let accounting = support_bundle_usage_test_accounting(None);
        let backend = Arc::new(UnaccountedOnlyRollupStorage {
            legacy_snapshot_calls: AtomicU64::new(0),
        });
        let unsupported: Arc<dyn Storage> = backend.clone();
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("unsupported usage query should admit");
        let mut retained_bytes = 0;
        let error = support_bundle_usage_child(
            &unsupported,
            Some(accounting.as_ref()),
            "team-a",
            &execution,
            &mut retained_bytes,
        )
        .expect_err("usage must fail closed without accounted storage observability");
        assert_eq!(error.status, 500);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("support_bundle_usage_accounting_unavailable")
        );
        assert_eq!(backend.legacy_snapshot_calls.load(Ordering::Relaxed), 0);
        assert_eq!(retained_bytes, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);

        let (_temp_dir, storage) = support_bundle_usage_test_storage();
        let cancellation_budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let cancelled = cancellation_budget
            .begin_query_with_token(token.clone())
            .expect("cancelled usage query should admit");
        token.cancel();
        let mut retained_bytes = 0;
        let error = support_bundle_usage_child(
            &storage,
            Some(accounting.as_ref()),
            "team-a",
            &cancelled,
            &mut retained_bytes,
        )
        .expect_err("pre-cancelled usage child must stop at its first source projection");
        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        assert_eq!(retained_bytes, 0);
        assert_eq!(cancelled.snapshot().memory_reserved_bytes, 0);
        drop(cancelled);

        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
        let after = cancellation_budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_usage_child_forbids_legacy_tree_producers() {
        let source = include_str!("usage_support.rs");
        let start = source
            .find("\nfn support_bundle_usage_child(")
            .expect("usage child source should exist");
        let end = source[start..]
            .find("\nfn support_bundle_rbac_state_child(")
            .map(|offset| start.saturating_add(offset))
            .expect("usage child source boundary should exist");
        let child = &source[start..end];
        for required in [
            "status_snapshot_for_with_execution",
            "ledger_status_with_execution",
            "status_observability_snapshot_with_execution",
            "measure_support_bundle_json_child",
            "serialize_support_bundle_json_child_without_content_type",
            "admit_accounted_support_bundle_child",
        ] {
            assert!(
                child.contains(required),
                "missing accounted path {required}"
            );
        }
        let storage_release = child
            .find("drop(observability)")
            .expect("the full storage projection should be released explicitly");
        let response_measurement = child
            .find("measure_support_bundle_json_child")
            .expect("the usage response should be measured before allocation");
        assert!(
            storage_release < response_measurement,
            "the full storage projection must not overlap response measurement/allocation"
        );
        for forbidden in [
            ".report(",
            ".ledger_status()",
            ".observability_snapshot()",
            "usage_reconciliation_json(",
            "JsonValue",
            "json!(",
            "serde_json::to_value",
            "bounded_usage_json_response(",
            "account_support_bundle_child(",
        ] {
            assert!(
                !child.contains(forbidden),
                "support usage child must not use legacy producer {forbidden}"
            );
        }
    }

    fn support_bundle_rbac_state_registry() -> (tempfile::TempDir, RbacRegistry) {
        let temp_dir =
            tempfile::TempDir::new().expect("support-bundle RBAC state tempdir should build");
        let path = temp_dir.path().join("rbac-support-state-α.json");
        std::fs::write(
            &path,
            r#"{
                "roles": {
                    "reader": {
                        "grants": [{
                            "action": "read",
                            "resource": {"kind": "tenant", "name": "team-\"-α"}
                        }]
                    }
                },
                "principals": [{
                    "id": "operator",
                    "token": "operator-token",
                    "bindings": [{
                        "role": "reader",
                        "scopes": [{"kind": "tenant", "name": "team-a"}]
                    }]
                }],
                "serviceAccounts": [{
                    "id": "automation",
                    "token": "automation-token",
                    "description": "automation-\"-α",
                    "createdUnixMs": 11,
                    "updatedUnixMs": 12,
                    "lastRotatedUnixMs": 13,
                    "bindings": [{"role": "reader"}]
                }],
                "oidcProviders": [{
                    "name": "corp",
                    "issuer": "https://issuer.example/α",
                    "audiences": ["tsink", "metrics-\""],
                    "usernameClaim": "email",
                    "jwks": [{
                        "kid": "shared-key",
                        "alg": "HS256",
                        "kty": "oct",
                        "k": "c3VwcG9ydC1idW5kbGUtc2VjcmV0"
                    }],
                    "claimMappings": [{
                        "claim": "groups",
                        "value": "metrics-*",
                        "bindings": [{
                            "role": "reader",
                            "scopes": [{"kind": "tenant", "name": "team-a"}]
                        }]
                    }]
                }]
            }"#,
        )
        .expect("support-bundle RBAC state config should write");
        let registry = RbacRegistry::load_from_path(&path)
            .expect("support-bundle RBAC state registry should load");
        (temp_dir, registry)
    }

    fn support_bundle_rbac_state_child_response(
        rbac_registry: Option<&RbacRegistry>,
    ) -> HttpResponse {
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("RBAC state child query should admit");
        let mut retained_bytes = 0;
        let response =
            support_bundle_rbac_state_child(rbac_registry, &execution, &mut retained_bytes)
                .expect("RBAC state child should encode");
        assert_eq!(response.response.status, 200);
        assert_eq!(
            header(&response.response, "content-type"),
            Some("application/json")
        );
        assert_eq!(
            response.reservation.bytes(),
            modeled_tsdb_status_response_retained_bytes(&response.response)
        );
        assert_eq!(retained_bytes, response.reservation.bytes());
        let cloned = response.response.clone();
        drop(response);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
        cloned
    }

    #[test]
    fn support_bundle_rbac_state_child_preserves_disabled_and_configured_legacy_bytes() {
        let disabled = support_bundle_rbac_state_child_response(None);
        let disabled_legacy = handle_admin_rbac_state(None);
        assert_eq!(disabled.status, disabled_legacy.status);
        assert_eq!(disabled.headers, disabled_legacy.headers);
        assert_eq!(disabled.body, disabled_legacy.body);

        let (_temp_dir, registry) = support_bundle_rbac_state_registry();
        let configured = support_bundle_rbac_state_child_response(Some(&registry));
        let configured_legacy = handle_admin_rbac_state(Some(&registry));
        assert_eq!(configured.status, configured_legacy.status);
        assert_eq!(configured.headers, configured_legacy.headers);
        assert_eq!(configured.body, configured_legacy.body);
        let body: JsonValue =
            serde_json::from_slice(&configured.body).expect("RBAC state child should be JSON");
        assert_eq!(body["data"]["roles"][0]["name"], "reader");
        assert_eq!(
            body["data"]["serviceAccounts"][0]["description"],
            "automation-\"-α"
        );
        assert_eq!(body["data"]["oidcProviders"][0]["keyIds"][0], "shared-key");
        assert_eq!(body["data"]["auditEntries"], 1);
    }

    #[test]
    fn support_bundle_rbac_state_child_enforces_exact_combined_memory_peak() {
        let (_temp_dir, registry) = support_bundle_rbac_state_registry();

        let source_budget = support_bundle_test_budget(None, None);
        let source_execution = source_budget
            .begin_query()
            .expect("RBAC state source query should admit");
        let source = registry
            .borrowed_state_snapshot_with_execution(&source_execution)
            .expect("borrowed RBAC state source should build");
        let source_retained_bytes = source_execution.snapshot().memory_reserved_bytes;
        assert!(source_retained_bytes > 0);
        assert_eq!(
            source_budget.snapshot().peak_shared_reserved_memory_bytes,
            source_retained_bytes,
            "the valid UTF-8 source path should not require a larger transient projection"
        );
        drop(source);
        assert_eq!(source_execution.snapshot().memory_reserved_bytes, 0);
        drop(source_execution);
        let source_after = source_budget.snapshot();
        assert_eq!(source_after.active_queries, 0);
        assert_eq!(source_after.shared_reserved_memory_bytes, 0);
        assert_eq!(source_after.accounting_invariant_violations_total, 0);

        let calibration_budget = support_bundle_test_budget(None, None);
        let calibration_execution = calibration_budget
            .begin_query()
            .expect("RBAC state calibration query should admit");
        let mut calibration_retained = 0;
        let calibration_response = support_bundle_rbac_state_child(
            Some(&registry),
            &calibration_execution,
            &mut calibration_retained,
        )
        .expect("RBAC state calibration child should encode");
        let required = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert_eq!(
            required,
            source_retained_bytes.saturating_add(calibration_response.reservation.bytes()),
            "the borrowed state locks and source path must remain charged through response allocation"
        );
        assert_eq!(
            calibration_retained,
            calibration_response.reservation.bytes()
        );
        drop(calibration_response);
        assert_eq!(calibration_execution.snapshot().memory_reserved_bytes, 0);
        drop(calibration_execution);

        let exact_budget = support_bundle_test_budget(Some(required), None);
        let exact_execution = exact_budget
            .begin_query()
            .expect("exact RBAC state child query should admit");
        let mut exact_retained = 0;
        let exact_response =
            support_bundle_rbac_state_child(Some(&registry), &exact_execution, &mut exact_retained)
                .expect("exact combined RBAC state snapshot/response peak should pass");
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            required
        );
        assert_eq!(exact_retained, exact_response.reservation.bytes());
        drop(exact_response);
        drop(exact_execution);

        let below_budget = support_bundle_test_budget(Some(required.saturating_sub(1)), None);
        let below_execution = below_budget
            .begin_query()
            .expect("below-boundary RBAC state child query should admit its slot");
        let mut below_retained = 0;
        let error =
            support_bundle_rbac_state_child(Some(&registry), &below_execution, &mut below_retained)
                .expect_err("one byte below the combined RBAC state peak must reject");
        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_eq!(below_retained, 0);
        assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
        drop(below_execution);

        for budget in [calibration_budget, exact_budget, below_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_rbac_state_child_honors_precancellation_without_residue() {
        let (_temp_dir, registry) = support_bundle_rbac_state_registry();
        let budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(token.clone())
            .expect("cancelled RBAC state child query should admit");
        token.cancel();
        let mut retained_bytes = 0;

        let error =
            support_bundle_rbac_state_child(Some(&registry), &execution, &mut retained_bytes)
                .expect_err("pre-cancelled RBAC state child must stop at its source projection");
        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        assert_eq!(retained_bytes, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_rbac_state_child_forbids_legacy_tree_producers() {
        let source = include_str!("usage_support.rs");
        let start = source
            .find("\nfn support_bundle_rbac_state_child(")
            .expect("RBAC state child source should exist");
        let end = source[start..]
            .find("\nfn support_bundle_rbac_audit_child(")
            .map(|offset| start.saturating_add(offset))
            .expect("RBAC state child source boundary should exist");
        let child = &source[start..end];
        for required in [
            "borrowed_state_snapshot_with_execution",
            "serialize_support_bundle_json_child",
            "admit_accounted_support_bundle_child",
        ] {
            assert!(
                child.contains(required),
                "missing accounted path {required}"
            );
        }
        for forbidden in [
            "handle_admin_rbac_state(",
            ".state_snapshot()",
            "rbac_disabled_state_snapshot(",
            "JsonValue",
            "json!(",
            "serde_json::to_value",
            "account_support_bundle_child(",
        ] {
            assert!(
                !child.contains(forbidden),
                "support RBAC state child must not use legacy producer {forbidden}"
            );
        }
    }

    fn support_bundle_rbac_audit_registry() -> RbacRegistry {
        let registry = RbacRegistry::from_json_str(
            r#"{
                "roles": {
                    "audit-reader": {
                        "grants": [{
                            "action": "read",
                            "resource": {"kind": "tenant", "name": "*"}
                        }]
                    }
                },
                "principals": [{
                    "id": "audit-operator",
                    "token": "audit-operator-token",
                    "bindings": [{"role": "audit-reader"}]
                }]
            }"#,
        )
        .expect("support-bundle RBAC audit fixture should parse");
        for index in 0..55 {
            registry
                .authorize(
                    Some("audit-operator-token"),
                    &RbacPermission::new(
                        RbacAction::Read,
                        RbacResource::tenant(format!("tenant-{index}-\"-\n")),
                    ),
                )
                .expect("fixture RBAC decision should authorize");
        }
        registry
    }

    fn support_bundle_rbac_audit_child_json(rbac_registry: Option<&RbacRegistry>) -> JsonValue {
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("RBAC audit child query should admit");
        let mut retained_bytes = 0;
        let response =
            support_bundle_rbac_audit_child(rbac_registry, &execution, &mut retained_bytes)
                .expect("RBAC audit child should encode");
        assert_eq!(response.response.status, 200);
        assert_eq!(
            header(&response.response, "content-type"),
            Some("application/json")
        );
        assert_eq!(
            response.reservation.bytes(),
            modeled_tsdb_status_response_retained_bytes(&response.response)
        );
        assert_eq!(retained_bytes, response.reservation.bytes());
        let value = serde_json::from_slice(&response.response.body)
            .expect("RBAC audit child should return JSON");
        drop(response);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
        value
    }

    #[test]
    fn support_bundle_rbac_audit_child_preserves_disabled_and_limited_legacy_schema() {
        let request = usage_request(SUPPORT_BUNDLE_RBAC_AUDIT_PATH);
        let disabled = support_bundle_rbac_audit_child_json(None);
        let disabled_legacy = handle_admin_rbac_audit(&request, None);
        assert_eq!(
            disabled,
            serde_json::from_slice::<JsonValue>(&disabled_legacy.body)
                .expect("disabled legacy RBAC audit response should be JSON")
        );
        assert_eq!(
            disabled["data"]["entries"]
                .as_array()
                .expect("disabled RBAC entries should be an array")
                .len(),
            0
        );

        let registry = support_bundle_rbac_audit_registry();
        let configured = support_bundle_rbac_audit_child_json(Some(&registry));
        let configured_legacy = handle_admin_rbac_audit(&request, Some(&registry));
        assert_eq!(
            configured,
            serde_json::from_slice::<JsonValue>(&configured_legacy.body)
                .expect("configured legacy RBAC audit response should be JSON")
        );
        let entries = configured["data"]["entries"]
            .as_array()
            .expect("configured RBAC entries should be an array");
        assert_eq!(entries.len(), SUPPORT_BUNDLE_RBAC_AUDIT_LIMIT);
        assert!(entries[0]["sequence"].as_u64() > entries[49]["sequence"].as_u64());
        assert_eq!(entries[0]["resource"]["name"], "tenant-54-\"-\n");
    }

    #[test]
    fn support_bundle_rbac_audit_child_enforces_exact_combined_memory_peak() {
        let registry = support_bundle_rbac_audit_registry();
        let calibration_budget = support_bundle_test_budget(None, None);
        let calibration_execution = calibration_budget
            .begin_query()
            .expect("RBAC audit calibration query should admit");
        let mut calibration_retained = 0;
        let calibration_response = support_bundle_rbac_audit_child(
            Some(&registry),
            &calibration_execution,
            &mut calibration_retained,
        )
        .expect("RBAC audit calibration child should encode");
        let required = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(required > calibration_response.reservation.bytes());
        drop(calibration_response);
        drop(calibration_execution);

        let exact_budget = support_bundle_test_budget(Some(required), None);
        let exact_execution = exact_budget
            .begin_query()
            .expect("exact RBAC audit child query should admit");
        let mut exact_retained = 0;
        let exact_response =
            support_bundle_rbac_audit_child(Some(&registry), &exact_execution, &mut exact_retained)
                .expect("exact combined RBAC audit snapshot/response peak should pass");
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            required
        );
        assert_eq!(exact_retained, exact_response.reservation.bytes());
        drop(exact_response);
        drop(exact_execution);

        let below_budget = support_bundle_test_budget(Some(required.saturating_sub(1)), None);
        let below_execution = below_budget
            .begin_query()
            .expect("below-boundary RBAC audit child query should admit its slot");
        let mut below_retained = 0;
        let error = support_bundle_rbac_audit_child(
            Some(&registry),
            &below_execution,
            &mut below_retained,
        )
        .expect_err(
            "one byte below the combined RBAC audit peak must reject before body allocation",
        );
        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_eq!(below_retained, 0);
        assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
        drop(below_execution);

        for budget in [calibration_budget, exact_budget, below_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_rbac_audit_child_honors_precancellation_without_residue() {
        let registry = support_bundle_rbac_audit_registry();
        let budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(token.clone())
            .expect("cancelled RBAC audit child query should admit");
        token.cancel();
        let mut retained_bytes = 0;

        let error =
            support_bundle_rbac_audit_child(Some(&registry), &execution, &mut retained_bytes)
                .expect_err("pre-cancelled RBAC audit child must stop at its source projection");
        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        assert_eq!(retained_bytes, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_rbac_audit_child_forbids_legacy_tree_producers() {
        let source = include_str!("usage_support.rs");
        let start = source
            .find("\nfn support_bundle_rbac_audit_child(")
            .expect("RBAC audit child source should exist");
        let end = source[start..]
            .find("\nfn support_bundle_security_state_child(")
            .map(|offset| start.saturating_add(offset))
            .expect("RBAC audit child source boundary should exist");
        let child = &source[start..end];
        assert!(child.contains("audit_snapshot_with_execution"));
        assert!(child.contains("SUPPORT_BUNDLE_RBAC_AUDIT_LIMIT"));
        assert!(child.contains("serialize_support_bundle_json_child"));
        assert!(child.contains("admit_accounted_support_bundle_child"));
        for forbidden in [
            "handle_admin_rbac_audit(",
            "audit_snapshot(",
            "JsonValue",
            "json!(",
            "serde_json::to_value",
            "query_param(",
            "percent_decode",
            "account_support_bundle_child(",
        ] {
            assert!(
                !child.contains(forbidden),
                "support RBAC audit child must not use legacy producer {forbidden}"
            );
        }
    }

    fn support_bundle_security_child_json(
        rbac_registry: Option<&RbacRegistry>,
        security_manager: Option<&SecurityManager>,
    ) -> JsonValue {
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("security child query should admit");
        let mut retained_bytes = 0;
        let response = support_bundle_security_state_child(
            rbac_registry,
            security_manager,
            &execution,
            &mut retained_bytes,
        )
        .expect("security child should encode");
        assert_eq!(
            response.reservation.bytes(),
            modeled_tsdb_status_response_retained_bytes(&response.response)
        );
        assert_eq!(retained_bytes, response.reservation.bytes());
        let value = serde_json::from_slice(&response.response.body)
            .expect("security child should return JSON");
        drop(response);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
        value
    }

    #[test]
    fn support_bundle_security_child_preserves_configured_rbac_only_and_disabled_schema() {
        let disabled = support_bundle_security_child_json(None, None);
        let disabled_legacy = handle_admin_secrets_state(None, None);
        assert_eq!(
            disabled,
            serde_json::from_slice::<JsonValue>(&disabled_legacy.body)
                .expect("disabled legacy security response should be JSON")
        );
        assert_eq!(disabled["data"]["enabled"], false);
        assert_eq!(disabled["data"]["serviceAccounts"], JsonValue::Null);

        let registry = RbacRegistry::from_json_str(
            r#"{
                "serviceAccounts": [
                    {"id":"active","token":"active-token","lastRotatedUnixMs":41},
                    {"id":"disabled","token":"disabled-token","disabled":true,"lastRotatedUnixMs":73}
                ]
            }"#,
        )
        .expect("RBAC-only security fixture should parse");
        let rbac_only = support_bundle_security_child_json(Some(&registry), None);
        let rbac_only_legacy = handle_admin_secrets_state(Some(&registry), None);
        assert_eq!(
            rbac_only,
            serde_json::from_slice::<JsonValue>(&rbac_only_legacy.body)
                .expect("RBAC-only legacy security response should be JSON")
        );
        assert_eq!(rbac_only["data"]["enabled"], true);
        assert_eq!(rbac_only["data"]["serviceAccounts"]["total"], 2);

        let security_manager = SecurityManager::from_config(&crate::server::ServerConfig {
            auth_token: Some("configured-security-token".to_string()),
            ..crate::server::ServerConfig::default()
        })
        .expect("configured security fixture should build");
        let configured = support_bundle_security_child_json(None, Some(security_manager.as_ref()));
        let configured_legacy = handle_admin_secrets_state(None, Some(security_manager.as_ref()));
        assert_eq!(
            configured,
            serde_json::from_slice::<JsonValue>(&configured_legacy.body)
                .expect("configured legacy security response should be JSON")
        );
        assert_eq!(configured["data"]["enabled"], true);
        assert_eq!(
            configured["data"]["targets"]
                .as_array()
                .expect("configured targets should be an array")
                .len(),
            1
        );
    }

    #[test]
    fn support_bundle_security_child_enforces_exact_combined_memory_peak() {
        let security_manager = SecurityManager::from_config(&crate::server::ServerConfig {
            auth_token: Some("security-boundary-token".to_string()),
            ..crate::server::ServerConfig::default()
        })
        .expect("security boundary fixture should build");

        let calibration_budget = support_bundle_test_budget(None, None);
        let calibration_execution = calibration_budget
            .begin_query()
            .expect("security calibration query should admit");
        let mut calibration_retained = 0;
        let calibration_response = support_bundle_security_state_child(
            None,
            Some(security_manager.as_ref()),
            &calibration_execution,
            &mut calibration_retained,
        )
        .expect("security calibration child should encode");
        let required = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(required > calibration_response.reservation.bytes());
        drop(calibration_response);
        drop(calibration_execution);

        let exact_budget = support_bundle_test_budget(Some(required), None);
        let exact_execution = exact_budget
            .begin_query()
            .expect("exact security child query should admit");
        let mut exact_retained = 0;
        let exact_response = support_bundle_security_state_child(
            None,
            Some(security_manager.as_ref()),
            &exact_execution,
            &mut exact_retained,
        )
        .expect("exact combined security snapshot/response peak should pass");
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            required
        );
        assert_eq!(exact_retained, exact_response.reservation.bytes());
        drop(exact_response);
        drop(exact_execution);

        let below_budget = support_bundle_test_budget(Some(required.saturating_sub(1)), None);
        let below_execution = below_budget
            .begin_query()
            .expect("below-boundary security child query should admit its slot");
        let mut below_retained = 0;
        let error = support_bundle_security_state_child(
            None,
            Some(security_manager.as_ref()),
            &below_execution,
            &mut below_retained,
        )
        .expect_err("one byte below the combined security peak must reject before body allocation");
        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_eq!(below_retained, 0);
        assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
        drop(below_execution);

        for budget in [calibration_budget, exact_budget, below_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_child_second_pass_cancellation_releases_body_reservation() {
        use std::cell::Cell;

        struct CancelOnSecondPass<'a> {
            passes: Cell<u8>,
            token: &'a tsink::QueryCancellationToken,
        }

        impl Serialize for CancelOnSecondPass<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let pass = self.passes.get().saturating_add(1);
                self.passes.set(pass);
                if pass == 2 {
                    self.token.cancel();
                }
                serializer.serialize_str("cancel after measurement")
            }
        }

        let budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(token.clone())
            .expect("second-pass cancellation query should admit");
        let value = CancelOnSecondPass {
            passes: Cell::new(0),
            token: &token,
        };

        let error = serialize_support_bundle_json_child(
            200,
            &value,
            &execution,
            SUPPORT_BUNDLE_MAX_CHILD_RETAINED_BYTES,
        )
        .expect_err("second-pass cancellation should stop child serialization");
        assert_eq!(value.passes.get(), 2);
        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_security_child_forbids_legacy_tree_producers() {
        let source = include_str!("usage_support.rs");
        let start = source
            .find("\nfn support_bundle_security_state_child(")
            .expect("security child source should exist");
        let end = source[start..]
            .find("\nfn support_bundle_cluster_audit_child(")
            .map(|offset| start.saturating_add(offset))
            .expect("security child source boundary should exist");
        let child = &source[start..end];
        assert!(child.contains("state_snapshot_with_execution"));
        assert!(child.contains("serialize_support_bundle_json_child"));
        for forbidden in [
            "handle_admin_secrets_state(",
            "security_status_json(",
            "JsonValue",
            "json!(",
            "account_support_bundle_child(",
        ] {
            assert!(
                !child.contains(forbidden),
                "support security child must not use legacy producer {forbidden}"
            );
        }
    }

    fn support_bundle_cluster_audit_context(
        temp_dir: &tempfile::TempDir,
        record_count: u64,
    ) -> Arc<ClusterRequestContext> {
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: Vec::new(),
            internal_auth_token: Some("support-bundle-audit-token".to_string()),
            ..ClusterConfig::default()
        };
        let runtime = ClusterRuntime::bootstrap(&config)
            .expect("support-bundle audit runtime should build")
            .expect("support-bundle audit runtime should be enabled");
        let mut context =
            ClusterRequestContext::from_runtime(runtime).expect("cluster context should build");
        let audit_log = Arc::new(
            ClusterAuditLog::open(
                temp_dir.path().join("support-cluster-audit.log"),
                ClusterAuditConfig::default(),
            )
            .expect("support-bundle cluster audit log should open"),
        );
        let now_ms = unix_timestamp_millis();
        for index in 0..record_count {
            audit_log
                .append(ClusterAuditEntryInput {
                    timestamp_unix_ms: Some(now_ms.saturating_add(index)),
                    operation: format!("audit-operation-{index}"),
                    actor: ClusterAuditActor {
                        id: format!("audit-actor-{index}-\"-\n"),
                        auth_scope: "admin".to_string(),
                    },
                    target: json!({
                        "nodeId": format!("node-{index}"),
                        "nested": {"labels": ["alpha", {"index": index}]}
                    }),
                    outcome: ClusterAuditOutcome {
                        status: if index.is_multiple_of(2) {
                            "success".to_string()
                        } else {
                            "error".to_string()
                        },
                        http_status: if index.is_multiple_of(2) { 200 } else { 409 },
                        result: Some(format!("result-{index}")),
                        error_type: if index.is_multiple_of(2) {
                            None
                        } else {
                            Some("conflict".to_string())
                        },
                    },
                })
                .expect("support-bundle audit fixture should append");
        }
        context.audit_log = Some(audit_log);
        Arc::new(context)
    }

    #[test]
    fn support_bundle_cluster_audit_child_preserves_unavailable_and_nested_legacy_schema() {
        let request = usage_request(SUPPORT_BUNDLE_CLUSTER_AUDIT_PATH);
        let unavailable_budget = support_bundle_test_budget(None, None);
        let unavailable_execution = unavailable_budget
            .begin_query()
            .expect("unavailable cluster audit query should admit");
        let mut unavailable_retained = 0;
        let unavailable = support_bundle_cluster_audit_child(
            None,
            &unavailable_execution,
            &mut unavailable_retained,
        )
        .expect("unavailable cluster audit child should encode its 503 response");
        let unavailable_legacy = handle_admin_cluster_audit_query(&request, None);
        assert_eq!(unavailable.response.status, unavailable_legacy.status);
        assert_eq!(
            header(&unavailable.response, "content-type"),
            header(&unavailable_legacy, "content-type")
        );
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&unavailable.response.body)
                .expect("accounted unavailable audit response should be JSON"),
            serde_json::from_slice::<JsonValue>(&unavailable_legacy.body)
                .expect("legacy unavailable audit response should be JSON")
        );
        assert_eq!(unavailable_retained, unavailable.reservation.bytes());
        drop(unavailable);
        drop(unavailable_execution);

        let temp_dir = tempfile::TempDir::new().expect("cluster audit tempdir should build");
        let context = support_bundle_cluster_audit_context(&temp_dir, 55);
        let configured_budget = support_bundle_test_budget(None, None);
        let configured_execution = configured_budget
            .begin_query()
            .expect("configured cluster audit query should admit");
        let mut configured_retained = 0;
        let configured = support_bundle_cluster_audit_child(
            Some(context.as_ref()),
            &configured_execution,
            &mut configured_retained,
        )
        .expect("configured cluster audit child should encode");
        let configured_legacy = handle_admin_cluster_audit_query(&request, Some(context.as_ref()));
        assert_eq!(configured.response.status, configured_legacy.status);
        assert_eq!(
            header(&configured.response, "content-type"),
            header(&configured_legacy, "content-type")
        );
        let configured_json = serde_json::from_slice::<JsonValue>(&configured.response.body)
            .expect("accounted configured audit response should be JSON");
        assert_eq!(
            configured_json,
            serde_json::from_slice::<JsonValue>(&configured_legacy.body)
                .expect("legacy configured audit response should be JSON")
        );
        assert_eq!(configured_json["data"]["count"], 50);
        assert_eq!(
            configured_json["data"]["entries"][0]["operation"],
            "audit-operation-54"
        );
        assert_eq!(
            configured_json["data"]["entries"][0]["target"]["nested"]["labels"][1]["index"],
            54
        );
        assert_eq!(configured_retained, configured.reservation.bytes());
        drop(configured);
        drop(configured_execution);

        for budget in [unavailable_budget, configured_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_cluster_audit_child_enforces_exact_streamed_response_peak() {
        let temp_dir = tempfile::TempDir::new().expect("cluster audit tempdir should build");
        let context = support_bundle_cluster_audit_context(&temp_dir, 4);
        let calibration_budget = support_bundle_test_budget(None, None);
        let calibration_execution = calibration_budget
            .begin_query()
            .expect("cluster audit calibration query should admit");
        let mut calibration_retained = 0;
        let calibration_response = support_bundle_cluster_audit_child(
            Some(context.as_ref()),
            &calibration_execution,
            &mut calibration_retained,
        )
        .expect("cluster audit calibration child should encode");
        let required = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert_eq!(required, calibration_response.reservation.bytes());
        drop(calibration_response);
        drop(calibration_execution);

        let exact_budget = support_bundle_test_budget(Some(required), None);
        let exact_execution = exact_budget
            .begin_query()
            .expect("exact cluster audit child query should admit");
        let mut exact_retained = 0;
        let exact_response = support_bundle_cluster_audit_child(
            Some(context.as_ref()),
            &exact_execution,
            &mut exact_retained,
        )
        .expect("exact streamed cluster audit response peak should pass");
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            required
        );
        assert_eq!(exact_retained, exact_response.reservation.bytes());
        drop(exact_response);
        drop(exact_execution);

        let below_budget = support_bundle_test_budget(Some(required.saturating_sub(1)), None);
        let below_execution = below_budget
            .begin_query()
            .expect("below-boundary cluster audit query should admit its slot");
        let mut below_retained = 0;
        let error = support_bundle_cluster_audit_child(
            Some(context.as_ref()),
            &below_execution,
            &mut below_retained,
        )
        .expect_err(
            "one byte below the streamed audit response peak must reject before allocation",
        );
        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_eq!(below_retained, 0);
        assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
        drop(below_execution);

        for budget in [calibration_budget, exact_budget, below_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_cluster_audit_child_honors_precancellation_without_residue() {
        let temp_dir = tempfile::TempDir::new().expect("cluster audit tempdir should build");
        let context = support_bundle_cluster_audit_context(&temp_dir, 1);
        let budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(token.clone())
            .expect("cancelled cluster audit child query should admit");
        token.cancel();
        let mut retained_bytes = 0;

        let error = support_bundle_cluster_audit_child(
            Some(context.as_ref()),
            &execution,
            &mut retained_bytes,
        )
        .expect_err("pre-cancelled cluster audit child must stop before locking");
        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        assert_eq!(retained_bytes, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_cluster_audit_child_forbids_legacy_tree_and_query_parsers() {
        let source = include_str!("usage_support.rs");
        let start = source
            .find("\nfn support_bundle_cluster_audit_child(")
            .expect("cluster audit child source should exist");
        let end = source[start..]
            .find("\n// Field declarations in this response family")
            .map(|offset| start.saturating_add(offset))
            .expect("cluster audit child source boundary should exist");
        let child = &source[start..end];
        for required in [
            "latest_snapshot_with_execution",
            "SUPPORT_BUNDLE_CLUSTER_AUDIT_LIMIT",
            "serialize_support_bundle_json_child",
            "admit_accounted_support_bundle_child",
        ] {
            assert!(
                child.contains(required),
                "support cluster audit child should contain {required}"
            );
        }
        for forbidden in [
            "handle_admin_cluster_audit_query(",
            ".query(",
            "parse_admin_audit_query",
            "JsonValue",
            "json!(",
            "serde_json::to_value",
            "query_param(",
            ".param(",
            "account_support_bundle_child(",
        ] {
            assert!(
                !child.contains(forbidden),
                "support cluster audit child must not use legacy producer {forbidden}"
            );
        }
    }

    #[test]
    fn support_bundle_handoff_typed_response_is_raw_byte_compatible() {
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            shards: 16,
            replication_factor: 1,
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&config).expect("membership should build");
        let ring = ShardRing::build(config.shards, config.replication_factor, &membership)
            .expect("ring should build");
        let mut state = ControlState::from_runtime(&membership, &ring);
        state.leader_node_id = Some("node-b".to_string());
        state.transitions = (0..10u32)
            .map(|shard| {
                let phase = match shard % 5 {
                    0 => ShardHandoffPhase::Warmup,
                    1 => ShardHandoffPhase::Cutover,
                    2 => ShardHandoffPhase::FinalSync,
                    3 => ShardHandoffPhase::Completed,
                    _ => ShardHandoffPhase::Failed,
                };
                ShardOwnershipTransition {
                    shard,
                    from_node_id: "node-a".to_string(),
                    to_node_id: "node-b".to_string(),
                    activation_ring_version: state.ring_version.saturating_add(1),
                    handoff: ShardHandoffProgress {
                        phase,
                        copied_rows: u64::from(shard).saturating_mul(11),
                        pending_rows: if shard == 0 {
                            u64::MAX
                        } else if phase == ShardHandoffPhase::Completed {
                            0
                        } else {
                            u64::from(shard).saturating_add(3)
                        },
                        resumed_count: u64::from(shard % 2),
                        started_unix_ms: 100u64.saturating_add(u64::from(shard)),
                        updated_unix_ms: 200u64.saturating_add(u64::from(shard)),
                        last_error: (shard != 9)
                            .then(|| format!("handoff error {shard} \"quoted\"\n雪")),
                    },
                }
            })
            .collect();
        let handoff = state.handoff_snapshot();
        let mut legacy_rebalance = RebalanceSchedulerSnapshot::empty();
        legacy_rebalance.interval_secs = u64::MAX;
        legacy_rebalance.active_jobs = 3;
        legacy_rebalance.rows_scheduled_last_run = 1;
        legacy_rebalance.last_error = Some("rebalance \"diagnostic\"\n雪".to_string());
        let legacy =
            admin_handoff_status_response("node-local-\"-\n", &state, &handoff, &legacy_rebalance);
        let legacy_json = serde_json::from_slice::<JsonValue>(&legacy.body)
            .expect("legacy handoff response should be JSON");
        let event_unix_ms = legacy_json["data"]["eventUnixMs"]
            .as_u64()
            .expect("legacy handoff response should contain an event timestamp");
        let control = crate::cluster::consensus::ControlHandoffStatusSnapshot {
            ring_version: state.ring_version,
            leader_node_id: state.leader_node_id.clone(),
            handoff,
        };
        let rebalance = crate::cluster::repair::HandoffRebalanceStatusSnapshot {
            interval_secs: legacy_rebalance.interval_secs,
            paused: legacy_rebalance.paused,
            active_jobs: legacy_rebalance.active_jobs,
            rows_scheduled_last_run: legacy_rebalance.rows_scheduled_last_run,
            last_error: legacy_rebalance.last_error.clone(),
        };
        let typed = serde_json::to_vec(&support_bundle_handoff_response(
            "node-local-\"-\n",
            &control,
            &rebalance,
            event_unix_ms,
        ))
        .expect("typed handoff response should encode");

        assert_eq!(
            typed, legacy.body,
            "typed streaming must preserve the historical raw JSON bytes and key order"
        );
        assert_eq!(legacy_json["data"]["operation"], "handoff_status");
        assert_eq!(legacy_json["data"]["errorSummary"]["jobsWithErrors"], 9);
        assert_eq!(
            legacy_json["data"]["errorSummary"]["lastErrorSamples"]
                .as_array()
                .expect("error samples should be an array")
                .len(),
            8
        );
        assert!(legacy_json["data"]["estimatedEtaSeconds"].is_null());
        assert_eq!(
            legacy_json["data"]["jobs"][0]["etaSeconds"],
            JsonValue::from(u64::MAX)
        );
        assert!(legacy_json["data"]["jobs"][9]["lastError"].is_null());
    }

    fn support_bundle_handoff_context(
        temp_dir: &tempfile::TempDir,
        include_digest_runtime: bool,
    ) -> Arc<ClusterRequestContext> {
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            internal_auth_token: Some("support-bundle-handoff-token".to_string()),
            shards: 16,
            replication_factor: 1,
            ..ClusterConfig::default()
        };
        let runtime = ClusterRuntime::bootstrap(&config)
            .expect("support-bundle handoff runtime should build")
            .expect("support-bundle handoff runtime should be enabled");
        let mut context =
            ClusterRequestContext::from_runtime(runtime).expect("cluster context should build");
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state-handoff-support.json"))
                .expect("handoff control state store should open"),
        );
        let mut bootstrap_state =
            ControlState::from_runtime(&context.runtime.membership, &context.runtime.ring);
        bootstrap_state.leader_node_id = Some("node-b".to_string());
        let activation_ring_version = bootstrap_state.ring_version.saturating_add(1);
        bootstrap_state.transitions = vec![
            ShardOwnershipTransition {
                shard: 2,
                from_node_id: "node-a".to_string(),
                to_node_id: "node-b".to_string(),
                activation_ring_version,
                handoff: ShardHandoffProgress {
                    phase: ShardHandoffPhase::Failed,
                    copied_rows: 11,
                    pending_rows: 5,
                    resumed_count: 1,
                    started_unix_ms: 101,
                    updated_unix_ms: 201,
                    last_error: Some("failed \"handoff\"\nline".to_string()),
                },
            },
            ShardOwnershipTransition {
                shard: 1,
                from_node_id: "node-a".to_string(),
                to_node_id: "node-b".to_string(),
                activation_ring_version,
                handoff: ShardHandoffProgress {
                    phase: ShardHandoffPhase::FinalSync,
                    copied_rows: 17,
                    pending_rows: 3,
                    resumed_count: 0,
                    started_unix_ms: 102,
                    updated_unix_ms: 202,
                    last_error: None,
                },
            },
            ShardOwnershipTransition {
                shard: 0,
                from_node_id: "node-a".to_string(),
                to_node_id: "node-b".to_string(),
                activation_ring_version,
                handoff: ShardHandoffProgress {
                    phase: ShardHandoffPhase::Warmup,
                    copied_rows: 23,
                    pending_rows: 7,
                    resumed_count: 2,
                    started_unix_ms: 103,
                    updated_unix_ms: 203,
                    last_error: Some("warmup diagnostic 雪".to_string()),
                },
            },
        ];
        bootstrap_state
            .validate()
            .expect("handoff bootstrap control state should validate");
        state_store
            .persist(&bootstrap_state)
            .expect("handoff bootstrap state should persist");
        let consensus = Arc::new(
            ControlConsensusRuntime::open(
                context.runtime.membership.clone(),
                Arc::clone(&state_store),
                bootstrap_state,
                temp_dir.path().join("control-log-handoff-support.json"),
                ControlConsensusConfig::default(),
            )
            .expect("handoff control consensus should open"),
        );
        context.control_state_store = Some(state_store);
        context.control_consensus = Some(Arc::clone(&consensus));
        if include_digest_runtime {
            let digest_runtime = Arc::new(crate::cluster::repair::DigestExchangeRuntime::new(
                context.runtime.membership.local_node_id.clone(),
                context.rpc_client.clone(),
                consensus,
                DigestExchangeConfig {
                    rebalance_interval: std::time::Duration::from_secs(9),
                    ..DigestExchangeConfig::default()
                },
            ));
            digest_runtime.replace_handoff_status_diagnostics_for_test(
                false,
                5,
                Some("scheduler \"diagnostic\"\n雪".to_string()),
            );
            context.digest_runtime = Some(digest_runtime);
        }
        Arc::new(context)
    }

    fn support_bundle_without_handoff_event(mut value: JsonValue) -> JsonValue {
        value["data"]
            .as_object_mut()
            .expect("handoff response data should be an object")
            .remove("eventUnixMs")
            .expect("handoff response should contain its event timestamp");
        value
    }

    #[tokio::test]
    async fn support_bundle_handoff_child_preserves_unavailable_and_empty_scheduler_paths() {
        let none_budget = support_bundle_test_budget(None, None);
        let none_execution = none_budget
            .begin_query()
            .expect("unavailable handoff query should admit");
        let mut none_retained = 0;
        let none = support_bundle_cluster_handoff_child(None, &none_execution, &mut none_retained)
            .expect("unavailable handoff child should encode");
        let none_legacy = handle_admin_cluster_handoff_status(None).await;
        assert_eq!(none.response.status, none_legacy.status);
        assert_eq!(none.response.headers, none_legacy.headers);
        assert_eq!(none.response.body, none_legacy.body);
        assert_eq!(none_retained, none.reservation.bytes());
        drop(none);
        drop(none_execution);
        assert_eq!(none_budget.snapshot().active_queries, 0);

        let unavailable_temp = tempfile::TempDir::new().expect("tempdir should build");
        let unavailable_context = support_bundle_cluster_audit_context(&unavailable_temp, 0);
        let unavailable_budget = support_bundle_test_budget(None, None);
        let unavailable_execution = unavailable_budget
            .begin_query()
            .expect("missing-consensus handoff query should admit");
        let mut unavailable_retained = 0;
        let unavailable = support_bundle_cluster_handoff_child(
            Some(unavailable_context.as_ref()),
            &unavailable_execution,
            &mut unavailable_retained,
        )
        .expect("missing-consensus handoff child should encode");
        let unavailable_legacy =
            handle_admin_cluster_handoff_status(Some(unavailable_context.as_ref())).await;
        assert_eq!(unavailable.response.status, unavailable_legacy.status);
        assert_eq!(unavailable.response.headers, unavailable_legacy.headers);
        assert_eq!(unavailable.response.body, unavailable_legacy.body);
        assert_eq!(unavailable_retained, unavailable.reservation.bytes());
        drop(unavailable);
        drop(unavailable_execution);
        assert_eq!(unavailable_budget.snapshot().active_queries, 0);

        let fallback_temp = tempfile::TempDir::new().expect("tempdir should build");
        let fallback_context = support_bundle_handoff_context(&fallback_temp, false);
        let fallback_budget = support_bundle_test_budget(None, None);
        let fallback_execution = fallback_budget
            .begin_query()
            .expect("fallback handoff query should admit");
        let mut fallback_retained = 0;
        let fallback = support_bundle_cluster_handoff_child(
            Some(fallback_context.as_ref()),
            &fallback_execution,
            &mut fallback_retained,
        )
        .expect("handoff child should use the empty scheduler fallback");
        let fallback_legacy =
            handle_admin_cluster_handoff_status(Some(fallback_context.as_ref())).await;
        assert_eq!(fallback.response.status, 200);
        assert_eq!(fallback.response.headers, fallback_legacy.headers);
        let fallback_json = serde_json::from_slice::<JsonValue>(&fallback.response.body)
            .expect("fallback handoff response should be JSON");
        let fallback_legacy_json = serde_json::from_slice::<JsonValue>(&fallback_legacy.body)
            .expect("legacy fallback handoff response should be JSON");
        assert_eq!(
            support_bundle_without_handoff_event(fallback_json.clone()),
            support_bundle_without_handoff_event(fallback_legacy_json)
        );
        assert_eq!(fallback_json["data"]["operation"], "handoff_status");
        assert!(fallback_json["data"]["errorSummary"]["rebalanceLastError"].is_null());
        assert!(fallback_json["data"]["estimatedEtaSeconds"].is_null());
        assert_eq!(fallback_retained, fallback.reservation.bytes());
        drop(fallback);
        drop(fallback_execution);
        let fallback_after = fallback_budget.snapshot();
        assert_eq!(fallback_after.active_queries, 0);
        assert_eq!(fallback_after.shared_reserved_memory_bytes, 0);
        assert_eq!(fallback_after.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn support_bundle_handoff_child_preserves_configured_legacy_values() {
        let temp_dir = tempfile::TempDir::new().expect("tempdir should build");
        let context = support_bundle_handoff_context(&temp_dir, true);
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("configured handoff query should admit");
        let mut retained_bytes = 0;
        let response = support_bundle_cluster_handoff_child(
            Some(context.as_ref()),
            &execution,
            &mut retained_bytes,
        )
        .expect("configured handoff child should encode");
        let legacy = handle_admin_cluster_handoff_status(Some(context.as_ref())).await;
        assert_eq!(response.response.status, legacy.status);
        assert_eq!(response.response.headers, legacy.headers);
        let current_json = serde_json::from_slice::<JsonValue>(&response.response.body)
            .expect("configured handoff response should be JSON");
        let legacy_json = serde_json::from_slice::<JsonValue>(&legacy.body)
            .expect("legacy configured handoff response should be JSON");
        assert_eq!(
            support_bundle_without_handoff_event(current_json.clone()),
            support_bundle_without_handoff_event(legacy_json)
        );
        assert_eq!(
            current_json["data"]["errorSummary"]["rebalanceLastError"],
            "scheduler \"diagnostic\"\n雪"
        );
        assert_eq!(current_json["data"]["jobs"][0]["shard"], 0);
        assert_eq!(current_json["data"]["jobs"][1]["shard"], 1);
        assert_eq!(current_json["data"]["jobs"][2]["shard"], 2);
        assert_eq!(retained_bytes, response.reservation.bytes());
        drop(response);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_handoff_child_enforces_exact_combined_peak() {
        let temp_dir = tempfile::TempDir::new().expect("tempdir should build");
        let context = support_bundle_handoff_context(&temp_dir, true);

        let source_budget = support_bundle_test_budget(None, None);
        let source_execution = source_budget
            .begin_query()
            .expect("handoff source query should admit");
        let control = context
            .control_consensus
            .as_ref()
            .expect("handoff context should expose consensus")
            .handoff_status_snapshot_with_execution(&source_execution)
            .expect("control handoff source should build");
        let rebalance = context
            .digest_runtime
            .as_ref()
            .expect("handoff context should expose scheduler")
            .handoff_status_snapshot_with_execution(&source_execution)
            .expect("scheduler handoff source should build");
        let source_retained_bytes = source_execution.snapshot().memory_reserved_bytes;
        let source_peak = source_budget.snapshot().peak_shared_reserved_memory_bytes;
        assert!(source_retained_bytes > 0);
        assert!(source_peak >= source_retained_bytes);
        drop(rebalance);
        drop(control);
        assert_eq!(source_execution.snapshot().memory_reserved_bytes, 0);
        drop(source_execution);
        let source_after = source_budget.snapshot();
        assert_eq!(source_after.active_queries, 0);
        assert_eq!(source_after.shared_reserved_memory_bytes, 0);

        let calibration_budget = support_bundle_test_budget(None, None);
        let calibration_execution = calibration_budget
            .begin_query()
            .expect("handoff calibration query should admit");
        let mut calibration_retained = 0;
        let calibration_response = support_bundle_cluster_handoff_child(
            Some(context.as_ref()),
            &calibration_execution,
            &mut calibration_retained,
        )
        .expect("handoff calibration child should encode");
        let required = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert_eq!(
            required,
            source_retained_bytes.saturating_add(calibration_response.reservation.bytes()),
            "both sampled source generations must remain charged through response allocation"
        );
        assert!(required > source_peak);
        assert_eq!(
            calibration_retained,
            calibration_response.reservation.bytes()
        );
        drop(calibration_response);
        assert_eq!(calibration_execution.snapshot().memory_reserved_bytes, 0);
        drop(calibration_execution);

        let exact_budget = support_bundle_test_budget(Some(required), None);
        let exact_execution = exact_budget
            .begin_query()
            .expect("exact handoff query should admit");
        let mut exact_retained = 0;
        let exact_response = support_bundle_cluster_handoff_child(
            Some(context.as_ref()),
            &exact_execution,
            &mut exact_retained,
        )
        .expect("the exact handoff child peak should pass");
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            required
        );
        assert_eq!(exact_retained, exact_response.reservation.bytes());
        drop(exact_response);
        assert_eq!(exact_execution.snapshot().memory_reserved_bytes, 0);
        drop(exact_execution);

        let below_budget = support_bundle_test_budget(Some(required.saturating_sub(1)), None);
        let below_execution = below_budget
            .begin_query()
            .expect("below-boundary handoff query should admit");
        let mut below_retained = 0;
        let error = support_bundle_cluster_handoff_child(
            Some(context.as_ref()),
            &below_execution,
            &mut below_retained,
        )
        .expect_err("one byte below the combined handoff peak must reject");
        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_eq!(below_retained, 0);
        assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
        drop(below_execution);

        for budget in [calibration_budget, exact_budget, below_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_handoff_child_honors_precancellation_without_residue() {
        let temp_dir = tempfile::TempDir::new().expect("tempdir should build");
        let context = support_bundle_handoff_context(&temp_dir, true);
        let budget = support_bundle_test_budget(None, None);
        let cancellation = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(cancellation.clone())
            .expect("cancelled handoff query should admit");
        cancellation.cancel();
        let mut retained_bytes = 0;

        let error = support_bundle_cluster_handoff_child(
            Some(context.as_ref()),
            &execution,
            &mut retained_bytes,
        )
        .expect_err("pre-cancelled handoff child must stop before locking");
        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        assert_eq!(retained_bytes, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(budget.snapshot().peak_shared_reserved_memory_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_handoff_child_forbids_legacy_trees_and_pins_sample_order() {
        let source = include_str!("usage_support.rs");
        let start = source
            .find("\nstruct SupportBundleHandoffResponse")
            .expect("handoff response source should exist");
        let end = source[start..]
            .find("\nconst SUPPORT_BUNDLE_REPAIR_MISMATCH_SUMMARY_LIMIT")
            .map(|offset| start.saturating_add(offset))
            .expect("handoff child source boundary should exist");
        let child = &source[start..end];
        assert_eq!(
            child
                .matches("handoff_status_snapshot_with_execution(execution)")
                .count(),
            2,
            "the child must sample one first-generation control source and one later scheduler source"
        );
        for required in [
            "HandoffRebalanceStatusSnapshot::empty",
            "serializer.collect_str(self)",
            "serializer.serialize_seq",
            "serialize_support_bundle_json_child",
            "admit_accounted_support_bundle_child",
        ] {
            assert!(
                child.contains(required),
                "support handoff child should contain {required}"
            );
        }
        let first_control = child
            .find("let control = consensus")
            .expect("first control sample should exist");
        let later_scheduler = child
            .find("let accounted_rebalance = cluster_context")
            .expect("later scheduler sample should exist");
        assert!(first_control < later_scheduler);
        for forbidden in [
            "handle_admin_cluster_handoff_status(",
            "admin_handoff_status_response(",
            ".current_state(",
            ".handoff_snapshot(",
            ".rebalance_snapshot(",
            "JsonValue",
            "json!(",
            "format!(",
            "collect::<Vec",
            "account_support_bundle_child(",
        ] {
            assert!(
                !child.contains(forbidden),
                "support handoff child must not use legacy producer {forbidden}"
            );
        }

        let repair_source = include_str!("../../cluster/repair.rs");
        let repair_start = repair_source
            .find("pub(crate) fn handoff_status_snapshot_with_execution(")
            .expect("focused scheduler producer should exist");
        let repair_end = repair_source[repair_start..]
            .find("\n    pub fn is_rebalance_run_inflight")
            .map(|offset| repair_start.saturating_add(offset))
            .expect("focused scheduler producer boundary should exist");
        let producer = &repair_source[repair_start..repair_end];
        let pause_sample = producer
            .find("rebalance_control_snapshot()")
            .expect("pause-control sample should exist");
        let later_control = producer
            .find("handoff_active_jobs_with_execution(execution)")
            .expect("later control sample should exist");
        let metrics_sample = producer
            .find("rebalance_metrics")
            .expect("scheduler metrics sample should exist");
        assert!(pause_sample < later_control && later_control < metrics_sample);
    }

    fn support_bundle_repair_mismatch(index: u32) -> DigestMismatchReport {
        DigestMismatchReport {
            shard: index,
            peer_node_id: format!("peer-{index}-\\\"-\\n"),
            peer_endpoint: format!("127.0.0.1:{}", 9_400u32.saturating_add(index)),
            ring_version: 100u64.saturating_add(u64::from(index)),
            window_start: -500i64.saturating_add(i64::from(index)),
            window_end: 500i64.saturating_add(i64::from(index)),
            local_series_count: if index == 1 { 50 } else { 10 },
            local_point_count: if index == 1 { 70 } else { 20 },
            local_fingerprint: 200u64.saturating_add(u64::from(index)),
            remote_series_count: if index == 1 {
                40
            } else {
                30u64.saturating_add(u64::from(index))
            },
            remote_point_count: if index == 1 {
                60
            } else {
                50u64.saturating_add(u64::from(index))
            },
            remote_fingerprint: 300u64.saturating_add(u64::from(index)),
            detected_unix_ms: 400u64.saturating_add(u64::from(index)),
        }
    }

    fn support_bundle_repair_snapshot(mismatch_count: u32) -> DigestExchangeSnapshot {
        let mut snapshot = DigestExchangeSnapshot::empty();
        snapshot.interval_secs = 9;
        snapshot.window_secs = 91;
        snapshot.repair_paused = false;
        snapshot.runs_total = 7;
        snapshot.repairs_attempted_total = 11;
        snapshot.repairs_succeeded_total = 5;
        snapshot.repairs_failed_total = 3;
        snapshot.repairs_cancelled_total = 2;
        snapshot.repair_rows_inserted_total = 123;
        snapshot.repairs_attempted_last_run = 4;
        snapshot.repairs_succeeded_last_run = 3;
        snapshot.repairs_failed_last_run = 2;
        snapshot.repairs_cancelled_last_run = 1;
        snapshot.repairs_skipped_backoff_last_run = 6;
        snapshot.repair_rows_inserted_last_run = 17;
        snapshot.last_run_unix_ms = 700;
        snapshot.last_success_unix_ms = 650;
        snapshot.last_error = Some("repair \\\"diagnostic\\\"\nline".to_string());
        snapshot.mismatches = (0..mismatch_count)
            .map(support_bundle_repair_mismatch)
            .collect();
        snapshot
    }

    fn support_bundle_without_repair_event(mut value: JsonValue) -> JsonValue {
        value["data"]
            .as_object_mut()
            .expect("repair response data should be an object")
            .remove("eventUnixMs")
            .expect("repair response should contain its event timestamp");
        value
    }

    fn support_bundle_repair_context(temp_dir: &tempfile::TempDir) -> Arc<ClusterRequestContext> {
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: Vec::new(),
            internal_auth_token: Some("support-bundle-cluster-token".to_string()),
            ..ClusterConfig::default()
        };
        let runtime = ClusterRuntime::bootstrap(&config)
            .expect("support-bundle cluster runtime should build")
            .expect("support-bundle cluster runtime should be enabled");
        let mut context =
            ClusterRequestContext::from_runtime(runtime).expect("cluster context should build");
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state-repair-support.json"))
                .expect("repair control state store should open"),
        );
        let bootstrap_state =
            ControlState::from_runtime(&context.runtime.membership, &context.runtime.ring);
        bootstrap_state
            .validate()
            .expect("repair bootstrap control state should validate");
        state_store
            .persist(&bootstrap_state)
            .expect("repair bootstrap control state should persist");
        let consensus = Arc::new(
            ControlConsensusRuntime::open(
                context.runtime.membership.clone(),
                Arc::clone(&state_store),
                bootstrap_state,
                temp_dir.path().join("control-log-repair-support.json"),
                ControlConsensusConfig::default(),
            )
            .expect("repair control consensus should open"),
        );
        context.control_state_store = Some(state_store);
        context.control_consensus = Some(Arc::clone(&consensus));
        context.digest_runtime = Some(Arc::new(
            crate::cluster::repair::DigestExchangeRuntime::new(
                context.runtime.membership.local_node_id.clone(),
                context.rpc_client.clone(),
                consensus,
                DigestExchangeConfig::default(),
            ),
        ));
        Arc::new(context)
    }

    #[test]
    fn support_bundle_repair_typed_response_preserves_nonempty_legacy_schema_and_values() {
        let snapshot = support_bundle_repair_snapshot(18);
        let control = RepairControlSnapshot {
            paused: true,
            cancel_generation: 19,
            cancellations_total: 23,
        };
        let current = serde_json::to_value(support_bundle_repair_response(
            "node-\\\"-\\n",
            control,
            &snapshot,
            true,
            1_234,
        ))
        .expect("typed repair response should serialize");
        assert_eq!(current["data"]["eventUnixMs"], 1_234);
        let legacy = admin_repair_status_response(
            200,
            AdminRepairOperation::Status,
            "node-\\\"-\\n",
            control,
            snapshot,
            true,
            "cluster digest repair runtime status",
        );
        let legacy = serde_json::from_slice::<JsonValue>(&legacy.body)
            .expect("legacy repair response should be JSON");
        assert_eq!(
            support_bundle_without_repair_event(current.clone()),
            support_bundle_without_repair_event(legacy)
        );
        let backlog = current["data"]["errorSummary"]["mismatchBacklog"]
            .as_array()
            .expect("repair mismatch backlog should be an array");
        assert_eq!(backlog.len(), SUPPORT_BUNDLE_REPAIR_MISMATCH_SUMMARY_LIMIT);
        assert_eq!(backlog[1]["pointGap"], 0);
        assert_eq!(backlog[1]["seriesGap"], 0);
        assert_eq!(backlog[15]["shard"], 15);
        assert_eq!(current["data"]["estimatedEtaSeconds"], 45);
    }

    #[tokio::test]
    async fn support_bundle_repair_unavailable_child_preserves_legacy_503_response() {
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("unavailable repair child query should admit");
        let mut retained_bytes = 0;
        let current = support_bundle_cluster_repair_child(None, &execution, &mut retained_bytes)
            .expect("unavailable repair child should encode its 503 response");
        let legacy = handle_admin_cluster_repair_status(None).await;
        assert_eq!(current.response.status, legacy.status);
        assert_eq!(
            header(&current.response, "content-type"),
            header(&legacy, "content-type")
        );
        assert_eq!(
            serde_json::from_slice::<JsonValue>(&current.response.body)
                .expect("accounted unavailable repair response should be JSON"),
            serde_json::from_slice::<JsonValue>(&legacy.body)
                .expect("legacy unavailable repair response should be JSON")
        );
        assert_eq!(
            current.reservation.bytes(),
            modeled_tsdb_status_response_retained_bytes(&current.response)
        );
        assert_eq!(retained_bytes, current.reservation.bytes());
        drop(current);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn support_bundle_repair_configured_child_preserves_legacy_schema() {
        let temp_dir = tempfile::TempDir::new().expect("repair tempdir should build");
        let context = support_bundle_repair_context(&temp_dir);
        let digest_runtime = context
            .digest_runtime
            .as_deref()
            .expect("repair digest runtime should exist");
        digest_runtime.replace_status_diagnostics_for_test(
            Some("configured repair diagnostic".to_string()),
            (0..18).map(support_bundle_repair_mismatch).collect(),
        );
        digest_runtime.pause_repairs();
        digest_runtime.cancel_repairs();
        digest_runtime.set_repair_run_inflight_for_test(true);

        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("configured repair schema query should admit");
        let mut retained_bytes = 0;
        let current = support_bundle_cluster_repair_child(
            Some(context.as_ref()),
            &execution,
            &mut retained_bytes,
        )
        .expect("configured repair child should encode");
        let legacy = handle_admin_cluster_repair_status(Some(context.as_ref())).await;
        assert_eq!(current.response.status, legacy.status);
        assert_eq!(
            header(&current.response, "content-type"),
            header(&legacy, "content-type")
        );
        assert_eq!(
            support_bundle_without_repair_event(
                serde_json::from_slice::<JsonValue>(&current.response.body)
                    .expect("accounted configured repair response should be JSON")
            ),
            support_bundle_without_repair_event(
                serde_json::from_slice::<JsonValue>(&legacy.body)
                    .expect("legacy configured repair response should be JSON")
            )
        );
        assert_eq!(retained_bytes, current.reservation.bytes());
        drop(current);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_repair_child_enforces_exact_combined_memory_peak() {
        let temp_dir = tempfile::TempDir::new().expect("repair tempdir should build");
        let context = support_bundle_repair_context(&temp_dir);
        context
            .digest_runtime
            .as_deref()
            .expect("repair digest runtime should exist")
            .replace_status_diagnostics_for_test(
                Some("combined repair diagnostic".to_string()),
                (0..18).map(support_bundle_repair_mismatch).collect(),
            );

        let calibration_budget = support_bundle_test_budget(None, None);
        let calibration_execution = calibration_budget
            .begin_query()
            .expect("repair calibration query should admit");
        let mut calibration_retained = 0;
        let calibration_response = support_bundle_cluster_repair_child(
            Some(context.as_ref()),
            &calibration_execution,
            &mut calibration_retained,
        )
        .expect("repair calibration child should encode");
        let required = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(required > calibration_response.reservation.bytes());
        drop(calibration_response);
        drop(calibration_execution);

        let exact_budget = support_bundle_test_budget(Some(required), None);
        let exact_execution = exact_budget
            .begin_query()
            .expect("exact repair child query should admit");
        let mut exact_retained = 0;
        let exact_response = support_bundle_cluster_repair_child(
            Some(context.as_ref()),
            &exact_execution,
            &mut exact_retained,
        )
        .expect("exact combined repair snapshot/response peak should pass");
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            required
        );
        assert_eq!(exact_retained, exact_response.reservation.bytes());
        drop(exact_response);
        drop(exact_execution);

        let below_budget = support_bundle_test_budget(Some(required.saturating_sub(1)), None);
        let below_execution = below_budget
            .begin_query()
            .expect("below-boundary repair child query should admit its slot");
        let mut below_retained = 0;
        let error = support_bundle_cluster_repair_child(
            Some(context.as_ref()),
            &below_execution,
            &mut below_retained,
        )
        .expect_err("one byte below the combined repair peak must reject before body allocation");
        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_eq!(below_retained, 0);
        assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
        drop(below_execution);

        for budget in [calibration_budget, exact_budget, below_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_repair_child_honors_precancellation_without_residue() {
        let temp_dir = tempfile::TempDir::new().expect("repair tempdir should build");
        let context = support_bundle_repair_context(&temp_dir);
        context
            .digest_runtime
            .as_deref()
            .expect("repair digest runtime should exist")
            .replace_status_diagnostics_for_test(
                Some("cancelled repair diagnostic".to_string()),
                vec![support_bundle_repair_mismatch(1)],
            );
        let budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(token.clone())
            .expect("cancelled repair child query should admit");
        token.cancel();
        let mut retained_bytes = 0;

        let error = support_bundle_cluster_repair_child(
            Some(context.as_ref()),
            &execution,
            &mut retained_bytes,
        )
        .expect_err("pre-cancelled repair child must stop at its source projection");
        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        assert_eq!(retained_bytes, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_repair_child_forbids_legacy_tree_producers() {
        let source = include_str!("usage_support.rs");
        let start = source
            .find("\nfn support_bundle_cluster_repair_child(")
            .expect("repair child source should exist");
        let end = source[start..]
            .find("\nstruct SupportBundleRulesResponse")
            .map(|offset| start.saturating_add(offset))
            .expect("repair child source boundary should exist");
        let child = &source[start..end];
        for required in [
            "repair_control_snapshot",
            "status_snapshot_with_execution",
            "serialize_support_bundle_json_child",
            "admit_accounted_support_bundle_child",
        ] {
            assert!(
                child.contains(required),
                "support repair child should contain {required}"
            );
        }
        for forbidden in [
            "handle_admin_cluster_repair_status(",
            ".snapshot()",
            "JsonValue",
            "json!(",
            "serde_json::to_value",
            "collect::<Vec<_>>()",
            "account_support_bundle_child(",
        ] {
            assert!(
                !child.contains(forbidden),
                "support repair child must not use legacy producer {forbidden}"
            );
        }
    }

    fn support_bundle_rules_runtime(maximum_status_bytes: Option<usize>) -> Arc<RulesRuntime> {
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_resource_profile(tsink::ResourceProfile::Test)
            .build()
            .expect("rules test storage should build");
        let mut config = rules::RulesRuntimeConfig::default();
        if let Some(maximum_status_bytes) = maximum_status_bytes {
            config.store_limits.max_snapshot_status_bytes = maximum_status_bytes;
        }
        let runtime = RulesRuntime::open_with_config(
            None,
            storage,
            TimestampPrecision::Milliseconds,
            None,
            None,
            None,
            config,
        )
        .expect("rules runtime should open");
        if maximum_status_bytes.is_none() {
            runtime
                .apply_groups(vec![rules::RuleGroupSpec {
                    name: "support-\u{2603}-rules".to_string(),
                    tenant_id: "team-\"quoted\"".to_string(),
                    interval_secs: 30,
                    labels: BTreeMap::from([(
                        "environment".to_string(),
                        "staging\\blue".to_string(),
                    )]),
                    rules: vec![
                        rules::RuleSpec::Recording(rules::RecordingRuleSpec {
                            record: "support_recording_one".to_string(),
                            expr: "sum(source_metric)".to_string(),
                            interval_secs: Some(15),
                            labels: BTreeMap::from([("zone".to_string(), "central".to_string())]),
                        }),
                        rules::RuleSpec::Alert(rules::AlertRuleSpec {
                            alert: "SupportAlertOne".to_string(),
                            expr: "source_metric > 0".to_string(),
                            interval_secs: None,
                            for_secs: 60,
                            labels: BTreeMap::from([(
                                "severity".to_string(),
                                "warning".to_string(),
                            )]),
                            annotations: BTreeMap::from([(
                                "summary".to_string(),
                                "escaped \"status\" \u{2603}".to_string(),
                            )]),
                        }),
                        rules::RuleSpec::Recording(rules::RecordingRuleSpec {
                            record: "support_recording_two".to_string(),
                            expr: "max(secondary_metric)".to_string(),
                            interval_secs: None,
                            labels: BTreeMap::new(),
                        }),
                        rules::RuleSpec::Alert(rules::AlertRuleSpec {
                            alert: "SupportAlertTwo".to_string(),
                            expr: "secondary_metric == 0".to_string(),
                            interval_secs: Some(10),
                            for_secs: 0,
                            labels: BTreeMap::from([(
                                "severity".to_string(),
                                "critical".to_string(),
                            )]),
                            annotations: BTreeMap::from([(
                                "runbook".to_string(),
                                "https://example.invalid/rules".to_string(),
                            )]),
                        }),
                    ],
                }])
                .expect("support rules group should apply");
        }
        runtime
    }

    #[tokio::test]
    async fn support_bundle_rules_child_preserves_raw_legacy_success_response() {
        let runtime = support_bundle_rules_runtime(None);
        let _ = handle_admin_rules_status(Some(runtime.as_ref())).await;
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("rules child query should admit");
        let mut retained_bytes = 0;

        let response =
            support_bundle_rules_child(Some(runtime.as_ref()), &execution, &mut retained_bytes)
                .expect("accounted rules child should encode");
        let legacy = handle_admin_rules_status(Some(runtime.as_ref())).await;
        assert_eq!(response.response.status, legacy.status);
        assert_eq!(response.response.headers, legacy.headers);
        assert_eq!(response.response.body, legacy.body);
        let body = serde_json::from_slice::<JsonValue>(&response.response.body)
            .expect("rules response should be JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(
            body["data"]["groups"][0]["rules"].as_array().map(Vec::len),
            Some(4)
        );
        assert_eq!(
            response.reservation.bytes(),
            modeled_tsdb_status_response_retained_bytes(&response.response)
        );
        assert_eq!(retained_bytes, response.reservation.bytes());
        drop(response);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn support_bundle_rules_child_preserves_raw_legacy_text_responses() {
        let unavailable_budget = support_bundle_test_budget(None, None);
        let unavailable_execution = unavailable_budget
            .begin_query()
            .expect("unavailable rules query should admit");
        let mut unavailable_retained = 0;
        let unavailable =
            support_bundle_rules_child(None, &unavailable_execution, &mut unavailable_retained)
                .expect("unavailable rules response should encode");
        let legacy_unavailable = handle_admin_rules_status(None).await;
        assert_eq!(unavailable.response.status, legacy_unavailable.status);
        assert_eq!(unavailable.response.headers, legacy_unavailable.headers);
        assert_eq!(unavailable.response.body, legacy_unavailable.body);
        assert_eq!(
            header(&unavailable.response, "content-type"),
            Some("text/plain")
        );
        drop(unavailable);
        drop(unavailable_execution);

        let limited_runtime = support_bundle_rules_runtime(Some(1));
        let legacy_limited = handle_admin_rules_status(Some(limited_runtime.as_ref())).await;
        let limited_budget = support_bundle_test_budget(None, None);
        let limited_execution = limited_budget
            .begin_query()
            .expect("limited rules query should admit");
        let mut limited_retained = 0;
        let limited = support_bundle_rules_child(
            Some(limited_runtime.as_ref()),
            &limited_execution,
            &mut limited_retained,
        )
        .expect("limited rules response should remain an embedded child response");
        assert_eq!(limited.response.status, legacy_limited.status);
        assert_eq!(limited.response.headers, legacy_limited.headers);
        assert_eq!(limited.response.body, legacy_limited.body);
        assert_eq!(
            header(&limited.response, "content-type"),
            Some("text/plain")
        );
        drop(limited);
        drop(limited_execution);

        for budget in [unavailable_budget, limited_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_rules_child_maps_typed_serializer_failures_to_legacy_text() {
        for (message, expected) in [
            (
                "failed to measure bounded rules JSON",
                "rules status serialization failed: failed to measure bounded rules JSON",
            ),
            (
                "failed to allocate bounded rules JSON",
                "rules status serialization failed: failed to allocate bounded rules JSON",
            ),
            (
                "failed to encode bounded rules JSON",
                "rules status serialization failed: failed to encode bounded rules JSON",
            ),
            (
                "bounded rules JSON length changed during encoding",
                "rules status serialization failed: bounded rules JSON length changed during encoding",
            ),
        ] {
            assert_eq!(
                support_bundle_rules_error_text(
                    &rules::RulesStatusProjectionError::Serialization(message),
                    true,
                ),
                expected
            );
        }
    }

    #[test]
    fn support_bundle_rules_child_enforces_exact_combined_memory_peak() {
        let runtime = support_bundle_rules_runtime(None);
        let calibration_budget = support_bundle_test_budget(None, None);
        let calibration_execution = calibration_budget
            .begin_query()
            .expect("rules calibration query should admit");
        let mut calibration_retained = 0;
        let calibration_response = support_bundle_rules_child(
            Some(runtime.as_ref()),
            &calibration_execution,
            &mut calibration_retained,
        )
        .expect("rules calibration child should encode");
        let required = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(required > calibration_response.reservation.bytes());
        drop(calibration_response);
        drop(calibration_execution);

        let exact_budget = support_bundle_test_budget(Some(required), None);
        let exact_execution = exact_budget
            .begin_query()
            .expect("exact rules query should admit");
        let mut exact_retained = 0;
        let exact_response = support_bundle_rules_child(
            Some(runtime.as_ref()),
            &exact_execution,
            &mut exact_retained,
        )
        .expect("the exact combined rules peak should pass");
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            required
        );
        assert_eq!(exact_retained, exact_response.reservation.bytes());
        drop(exact_response);
        drop(exact_execution);

        let below_budget = support_bundle_test_budget(Some(required.saturating_sub(1)), None);
        let below_execution = below_budget
            .begin_query()
            .expect("one-under rules query should admit its slot");
        let mut below_retained = 0;
        let error = support_bundle_rules_child(
            Some(runtime.as_ref()),
            &below_execution,
            &mut below_retained,
        )
        .expect_err("one byte below the combined rules peak must reject");
        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_eq!(below_retained, 0);
        assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
        drop(below_execution);

        for budget in [calibration_budget, exact_budget, below_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_rules_child_honors_precancellation_without_residue() {
        let runtime = support_bundle_rules_runtime(None);
        let budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(token.clone())
            .expect("cancelled rules query should admit");
        token.cancel();
        let mut retained_bytes = 0;

        let error =
            support_bundle_rules_child(Some(runtime.as_ref()), &execution, &mut retained_bytes)
                .expect_err("pre-cancelled rules child must stop at its source projection");
        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        assert_eq!(retained_bytes, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_rules_child_forbids_legacy_tree_producers() {
        let source = include_str!("usage_support.rs");
        let start = source
            .find("\nstruct SupportBundleRulesResponse")
            .expect("rules child source should exist");
        let end = source[start..]
            .find("\nfn support_bundle_rollups_child(")
            .map(|offset| start.saturating_add(offset))
            .expect("rules child source boundary should exist");
        let child = &source[start..end];
        for required in [
            "status_snapshot_with_execution",
            "prepare_success_snapshot_iteration_with_execution",
            "finalize_success_snapshot_with_execution",
            "serialize_support_bundle_json_child_inner",
            "admit_accounted_support_bundle_child",
        ] {
            assert!(
                child.contains(required),
                "support rules child should contain {required}"
            );
        }
        for forbidden in [
            "handle_admin_rules_status(",
            ".snapshot()",
            "encode_success_snapshot(",
            "JsonValue",
            "json!(",
            "serde_json::to_value",
            "collect::<Vec<_>>()",
            "account_support_bundle_child(",
        ] {
            assert!(
                !child.contains(forbidden),
                "support rules child must not use legacy producer {forbidden}"
            );
        }
    }

    fn support_bundle_rollup_test_storage() -> (tempfile::TempDir, Arc<dyn Storage>) {
        let temp_dir =
            tempfile::TempDir::new().expect("support-bundle rollup tempdir should build");
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_resource_profile(tsink::ResourceProfile::Test)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .with_data_path(temp_dir.path())
            .build()
            .expect("support-bundle rollup storage should build");
        storage
            .apply_rollup_policies(vec![RollupPolicy {
                id: "support-hourly".to_string(),
                metric: "http_request_duration_seconds".to_string(),
                match_labels: vec![
                    Label::new("environment", "production"),
                    Label::new("region", "central"),
                ],
                interval: 3_600_000,
                aggregation: tsink::Aggregation::Avg,
                bucket_origin: 0,
            }])
            .expect("support-bundle rollup policy should apply");
        (temp_dir, storage)
    }

    struct UnaccountedOnlyRollupStorage {
        legacy_snapshot_calls: AtomicU64,
    }

    impl Storage for UnaccountedOnlyRollupStorage {
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

        fn observability_snapshot(&self) -> tsink::StorageObservabilitySnapshot {
            self.legacy_snapshot_calls.fetch_add(1, Ordering::Relaxed);
            tsink::StorageObservabilitySnapshot::default()
        }

        fn close(&self) -> tsink::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn support_bundle_rollups_child_fails_closed_without_accounted_observability() {
        let backend = Arc::new(UnaccountedOnlyRollupStorage {
            legacy_snapshot_calls: AtomicU64::new(0),
        });
        let storage: Arc<dyn Storage> = backend.clone();
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("unsupported rollup query should admit");
        let mut retained_bytes = 0;

        let error = support_bundle_rollups_child(&storage, &execution, &mut retained_bytes)
            .expect_err("unaccounted backend must fail closed");
        assert_eq!(error.status, 500);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("support_bundle_rollups_accounting_unavailable")
        );
        assert_eq!(backend.legacy_snapshot_calls.load(Ordering::Relaxed), 0);
        assert_eq!(retained_bytes, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_rollups_child_honors_precancellation_without_residue() {
        let (_temp_dir, storage) = support_bundle_rollup_test_storage();
        let budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(token.clone())
            .expect("cancelled rollup query should admit");
        token.cancel();
        let mut retained_bytes = 0;

        let error = support_bundle_rollups_child(&storage, &execution, &mut retained_bytes)
            .expect_err("pre-cancelled rollup child must stop at its source projection");
        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        assert_eq!(retained_bytes, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn support_bundle_rollups_child_preserves_the_legacy_schema() {
        let (_temp_dir, storage) = support_bundle_rollup_test_storage();
        let budget = support_bundle_test_budget(None, None);
        let execution = budget
            .begin_query()
            .expect("rollup schema query should admit");
        let mut retained_bytes = 0;

        let response = support_bundle_rollups_child(&storage, &execution, &mut retained_bytes)
            .expect("accounted rollup child should encode");
        let current = serde_json::from_slice::<JsonValue>(&response.response.body)
            .expect("accounted rollup child should be JSON");
        let legacy = handle_admin_rollups_status(&storage).await;
        let legacy = serde_json::from_slice::<JsonValue>(&legacy.body)
            .expect("legacy rollup child should be JSON");
        assert_eq!(current, legacy);
        assert_eq!(
            current["data"]["policies"][0]["policy"]["matchLabels"][1]["value"],
            "central"
        );
        assert_eq!(
            response.reservation.bytes(),
            modeled_tsdb_status_response_retained_bytes(&response.response)
        );
        assert_eq!(retained_bytes, response.reservation.bytes());
        drop(response);
        drop(execution);
        let after = budget.snapshot();
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn support_bundle_rollups_child_enforces_exact_combined_memory_peak() {
        let (_temp_dir, storage) = support_bundle_rollup_test_storage();
        let calibration_budget = support_bundle_test_budget(None, None);
        let calibration_execution = calibration_budget
            .begin_query()
            .expect("rollup calibration query should admit");
        let mut calibration_retained = 0;
        let calibration_response = support_bundle_rollups_child(
            &storage,
            &calibration_execution,
            &mut calibration_retained,
        )
        .expect("rollup calibration child should encode");
        let required = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(required > calibration_response.reservation.bytes());
        drop(calibration_response);
        drop(calibration_execution);

        let exact_budget = support_bundle_test_budget(Some(required), None);
        let exact_execution = exact_budget
            .begin_query()
            .expect("exact rollup child query should admit");
        let mut exact_retained = 0;
        let exact_response =
            support_bundle_rollups_child(&storage, &exact_execution, &mut exact_retained)
                .expect("exact combined rollup snapshot/response peak should pass");
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            required
        );
        assert_eq!(exact_retained, exact_response.reservation.bytes());
        drop(exact_response);
        drop(exact_execution);

        let below_budget = support_bundle_test_budget(Some(required.saturating_sub(1)), None);
        let below_execution = below_budget
            .begin_query()
            .expect("below-boundary rollup child query should admit its slot");
        let mut below_retained = 0;
        let error = support_bundle_rollups_child(&storage, &below_execution, &mut below_retained)
            .expect_err(
                "one byte below the combined rollup peak must reject before body allocation",
            );
        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );
        assert_eq!(below_retained, 0);
        assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
        drop(below_execution);

        for budget in [calibration_budget, exact_budget, below_budget] {
            let after = budget.snapshot();
            assert_eq!(after.active_queries, 0);
            assert_eq!(after.shared_reserved_memory_bytes, 0);
            assert_eq!(after.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_rollups_child_forbids_legacy_tree_producers() {
        let source = include_str!("usage_support.rs");
        let start = source
            .find("\nfn support_bundle_rollups_child(")
            .expect("rollup child source should exist");
        let end = source[start..]
            .find("\n\n#[allow(clippy::too_many_arguments)]")
            .map(|offset| start.saturating_add(offset))
            .expect("rollup child source boundary should exist");
        let child = &source[start..end];
        assert!(child.contains("status_observability_snapshot_with_execution"));
        assert!(child.contains("serialize_support_bundle_json_child"));
        for forbidden in [
            "handle_admin_rollups_status(",
            "observability_snapshot()",
            "metrics_observability_snapshot_with_execution",
            "JsonValue",
            "json!(",
            "serde_json::to_value",
            "account_support_bundle_child(",
        ] {
            assert!(
                !child.contains(forbidden),
                "support rollup child must not use legacy producer {forbidden}"
            );
        }
    }

    #[test]
    fn support_bundle_response_enforces_exact_returned_and_memory_boundaries() {
        let json_response =
            support_bundle_test_response(br#"{"status":"success","data":{"value":7}}"#.to_vec());
        let text_response = HttpResponse::new(503, "component unavailable");

        let calibration_budget = support_bundle_test_budget(None, None);
        let (calibration_response, _) =
            prepare_support_bundle_for_test(&calibration_budget, &json_response, &text_response)
                .expect("calibration response should encode");
        let response_bytes =
            u64::try_from(calibration_response.body.len()).expect("response length should fit u64");
        let peak_memory = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(response_bytes > 0);
        assert!(
            usize::try_from(response_bytes).unwrap_or(usize::MAX)
                <= SUPPORT_BUNDLE_MAX_RESPONSE_BYTES
        );
        assert!(peak_memory > SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES);

        let exact_returned_budget = support_bundle_test_budget(None, Some(response_bytes));
        let (exact_returned, _) =
            prepare_support_bundle_for_test(&exact_returned_budget, &json_response, &text_response)
                .expect("exact returned-byte boundary should pass");
        assert_eq!(
            u64::try_from(exact_returned.body.len()).unwrap_or(u64::MAX),
            response_bytes
        );
        let below_returned_budget =
            support_bundle_test_budget(None, Some(response_bytes.saturating_sub(1)));
        let below_returned =
            prepare_support_bundle_for_test(&below_returned_budget, &json_response, &text_response)
                .expect_err("one byte below the returned-byte requirement should fail");
        assert_eq!(below_returned.status, 413);
        assert_eq!(
            header(&below_returned, READ_ERROR_CODE_HEADER),
            Some("query_limit_returned_bytes")
        );

        let exact_memory_budget = support_bundle_test_budget(Some(peak_memory), None);
        prepare_support_bundle_for_test(&exact_memory_budget, &json_response, &text_response)
            .expect("exact memory boundary should pass");
        let below_memory_budget =
            support_bundle_test_budget(Some(peak_memory.saturating_sub(1)), None);
        let below_memory =
            prepare_support_bundle_for_test(&below_memory_budget, &json_response, &text_response)
                .expect_err("one byte below the simultaneous memory requirement should fail");
        assert_eq!(below_memory.status, 413);
        assert_eq!(
            header(&below_memory, READ_ERROR_CODE_HEADER),
            Some("query_limit_per_query_memory_bytes")
        );

        for budget in [
            calibration_budget,
            exact_returned_budget,
            below_returned_budget,
            exact_memory_budget,
            below_memory_budget,
        ] {
            let snapshot = budget.snapshot();
            assert_eq!(snapshot.active_queries, 0);
            assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
            assert_eq!(snapshot.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn support_bundle_fixed_encoded_ceiling_rejects_before_body_allocation() {
        let mut oversized_json = Vec::with_capacity(SUPPORT_BUNDLE_MAX_RESPONSE_BYTES + 3);
        oversized_json.push(b'"');
        oversized_json.resize(SUPPORT_BUNDLE_MAX_RESPONSE_BYTES + 2, b'x');
        oversized_json.push(b'"');
        let json_response = support_bundle_test_response(oversized_json);
        let text_response = HttpResponse::new(503, "component unavailable");
        let budget = support_bundle_test_budget(None, None);

        let error = prepare_support_bundle_for_test(&budget, &json_response, &text_response)
            .expect_err("fixed encoded ceiling should reject the oversized envelope");

        assert_eq!(error.status, 413);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_limit_returned_bytes")
        );
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn support_bundle_serialization_cancellation_releases_all_resources() {
        let json_response =
            support_bundle_test_response(br#"{"status":"success","data":{"value":7}}"#.to_vec());
        let text_response = HttpResponse::new(503, "component unavailable");
        let budget = support_bundle_test_budget(None, None);
        let token = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(token.clone())
            .expect("support-bundle cancellation query should admit");
        let root_tenant = "team-a".to_string();
        let root_actor = ClusterAuditActor {
            id: "test-actor".to_string(),
            auth_scope: "admin".to_string(),
        };
        let base_reserved_bytes = modeled_tsdb_status_response_retained_bytes(&json_response)
            .saturating_add(modeled_tsdb_status_response_retained_bytes(&text_response))
            .saturating_add(modeled_support_bundle_root_retained_bytes(
                &root_tenant,
                &root_actor,
            ))
            .saturating_add(SUPPORT_BUNDLE_SERIALIZATION_SCRATCH_BYTES)
            .saturating_add(modeled_support_bundle_header_preflight_bytes(&root_tenant));
        let mut reservation = execution
            .reserve_memory(base_reserved_bytes)
            .expect("support-bundle cancellation reservation should admit");
        token.cancel();

        let error = {
            let bundle = support_bundle_test_envelope(&json_response, &text_response);
            prepare_support_bundle_response(
                &bundle,
                &execution,
                &mut reservation,
                base_reserved_bytes,
            )
            .expect_err("canceled support-bundle serialization should fail")
        };

        assert_eq!(error.status, 503);
        assert_eq!(
            header(&error, READ_ERROR_CODE_HEADER),
            Some("query_cancelled")
        );
        drop(root_actor);
        drop(root_tenant);
        drop(reservation);
        drop(execution);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn non_quota_budget_failure_uses_usage_specific_indeterminate_response() {
        let err = crate::usage::UsageAccountingError::Disk(tsink::TsinkError::Io(
            std::io::Error::other("injected usage ledger failure"),
        ));

        let response = usage_accounting_error_response("usage reconciliation", &err);

        assert_eq!(response.status, 500);
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case(WRITE_ERROR_CODE_HEADER)
                && value == "usage_ledger_persistence_failed"
        }));
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case(WRITE_PARTIAL_HEADER) && value == "possible"
        }));
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case(WRITE_OUTCOME_HEADER) && value == "indeterminate_backend"
        }));
    }

    #[test]
    fn export_handler_returns_stable_continuation_headers() {
        let accounting = UsageAccounting::open(None).expect("usage accounting should open");
        for _ in 0..3 {
            accounting
                .record(UsageRecordInput::success(
                    "team-a",
                    UsageCategory::Query,
                    "query",
                    "test",
                ))
                .expect("usage record should append");
        }

        let first = handle_admin_usage_export(
            &usage_request("/api/v1/admin/usage/export?tenant=team-a&limit=2"),
            Some(&accounting),
        );
        assert_eq!(first.status, 200);
        assert_eq!(header(&first, "X-Tsink-Usage-Records-Returned"), Some("2"));
        assert_eq!(header(&first, "X-Tsink-Usage-Has-More"), Some("true"));
        assert_eq!(
            header(&first, "X-Tsink-Usage-Raw-History-Complete"),
            Some("true")
        );
        assert_eq!(
            header(&first, "X-Tsink-Usage-Next-After-Sequence"),
            Some("2")
        );
        assert_eq!(header(&first, "X-Tsink-Usage-Snapshot-Sequence"), Some("3"));

        let second = handle_admin_usage_export(
            &usage_request(
                "/api/v1/admin/usage/export?tenant=team-a&limit=2&afterSequence=2&snapshotSequence=3",
            ),
            Some(&accounting),
        );
        assert_eq!(second.status, 200);
        assert_eq!(header(&second, "X-Tsink-Usage-Records-Returned"), Some("1"));
        assert_eq!(header(&second, "X-Tsink-Usage-Has-More"), Some("false"));
        let records = String::from_utf8(second.body)
            .expect("export must be utf-8")
            .lines()
            .map(|line| {
                serde_json::from_str::<crate::usage::UsageLedgerRecord>(line)
                    .expect("export line should decode")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            records.iter().map(|record| record.seq).collect::<Vec<_>>(),
            vec![3]
        );
    }

    #[test]
    fn export_handler_rejects_invalid_limit_structurally() {
        let accounting = UsageAccounting::open(None).expect("usage accounting should open");
        let response = handle_admin_usage_export(
            &usage_request("/api/v1/admin/usage/export?limit=0"),
            Some(&accounting),
        );
        assert_eq!(response.status, 400);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("error should be JSON");
        assert_eq!(body["error"]["code"], "usage_page_limit_invalid");
    }

    #[tokio::test]
    async fn report_handler_enforces_encoded_response_bytes() {
        let accounting = UsageAccounting::open(None).expect("usage accounting should open");
        accounting
            .record(UsageRecordInput::success(
                "team-a",
                UsageCategory::Query,
                "query",
                "test",
            ))
            .expect("usage record should append");
        let storage: Arc<dyn Storage> = tsink::StorageBuilder::new()
            .build()
            .expect("storage should build");
        let response = handle_admin_usage_report(
            &storage,
            &usage_request("/api/v1/admin/usage/report?bucket=none&maxBytes=1"),
            Some(&accounting),
        )
        .await;
        assert_eq!(response.status, 413);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("error should be JSON");
        assert_eq!(body["error"]["code"], "usage_report_response_too_large");
    }
}
