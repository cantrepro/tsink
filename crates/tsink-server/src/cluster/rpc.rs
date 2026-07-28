use crate::cluster::control::ShardHandoffPhase;
use crate::cluster::membership::MembershipView;
use crate::http::{
    json_response, text_response, HttpRequest, HttpResponse, MAX_BODY_BYTES, MAX_HEADER_BYTES,
};
use crate::security::ManagedStringSecret;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsConnector;
use tsink::{
    BatchWriteResult, DataPoint, Label, MetadataShardScope, MetricSeries, QueryBudgetError,
    QueryExecution, QueryExecutionSnapshot, QueryMemoryReservation, QueryWorkLimits, Row,
    SeriesPoints, SeriesSelection,
};

pub const INTERNAL_RPC_PROTOCOL_VERSION: &str = "1";
pub const INTERNAL_RPC_VERSION_HEADER: &str = "x-tsink-rpc-version";
pub const INTERNAL_RPC_AUTH_HEADER: &str = "x-tsink-internal-auth";
pub const INTERNAL_RPC_NODE_ID_HEADER: &str = "x-tsink-node-id";
pub const INTERNAL_RPC_VERIFIED_NODE_ID_HEADER: &str = "x-tsink-verified-node-id";
pub const INTERNAL_RPC_CAPABILITIES_HEADER: &str = "x-tsink-peer-capabilities";
pub const MAX_INTERNAL_INGEST_ROWS: usize = 4_096;
pub const DEFAULT_RPC_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_RPC_MAX_RETRIES: usize = 2;
pub const DEFAULT_INTERNAL_RING_VERSION: u64 = 1;
const RETRYABLE_STATUS_CODES: [u16; 4] = [500, 502, 503, 504];
const MAX_INTERNAL_RPC_RESPONSE_BYTES: usize = MAX_HEADER_BYTES + MAX_BODY_BYTES;
const MAX_RPC_ERROR_DIAGNOSTIC_BYTES: usize = 512;
const MAX_RPC_MISSING_CAPABILITIES: usize = 32;
static RUSTLS_CRYPTO_PROVIDER: OnceLock<()> = OnceLock::new();

pub const CLUSTER_CAPABILITY_RPC_V1: &str = "cluster_rpc_v1";
pub const CLUSTER_CAPABILITY_CONTROL_REPLICATION_V1: &str = "control_replication_v1";
pub const CLUSTER_CAPABILITY_CONTROL_SNAPSHOT_RPC_V1: &str = "control_snapshot_rpc_v1";
pub const CLUSTER_CAPABILITY_CONTROL_STATE_V1: &str = "control_state_v1";
pub const CLUSTER_CAPABILITY_CONTROL_LOG_V1: &str = "control_log_v1";
pub const CLUSTER_CAPABILITY_CONTROL_RECOVERY_SNAPSHOT_V1: &str = "control_recovery_snapshot_v1";
pub const CLUSTER_CAPABILITY_CLUSTER_SNAPSHOT_V1: &str = "cluster_snapshot_v1";
pub const CLUSTER_CAPABILITY_BUDGETED_RESTORE_V1: &str = "budgeted_restore_v1";
pub const CLUSTER_CAPABILITY_METADATA_INGEST_V1: &str = "metadata_ingest_v1";
pub const CLUSTER_CAPABILITY_METADATA_STORE_V1: &str = "metadata_store_v1";

fn ensure_rustls_crypto_provider() {
    RUSTLS_CRYPTO_PROVIDER.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
pub const CLUSTER_CAPABILITY_EXEMPLAR_INGEST_V1: &str = "exemplar_ingest_v1";
pub const CLUSTER_CAPABILITY_EXEMPLAR_QUERY_V1: &str = "exemplar_query_v1";
pub const CLUSTER_CAPABILITY_HISTOGRAM_INGEST_V1: &str = "histogram_ingest_v1";
pub const CLUSTER_CAPABILITY_HISTOGRAM_STORAGE_V1: &str = "histogram_storage_v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompatibilityProfile {
    #[serde(default)]
    pub capabilities: Vec<String>,
}

impl Default for CompatibilityProfile {
    fn default() -> Self {
        Self {
            capabilities: normalize_capabilities(default_cluster_capabilities()),
        }
    }
}

impl CompatibilityProfile {
    #[allow(dead_code)]
    pub fn with_capabilities<I, S>(mut self, capabilities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.capabilities = normalize_capabilities(capabilities);
        self
    }
}

pub const EXEMPLAR_PAYLOAD_REQUIRED_CAPABILITIES: [&str; 1] =
    [CLUSTER_CAPABILITY_EXEMPLAR_INGEST_V1];
pub const METADATA_PAYLOAD_REQUIRED_CAPABILITIES: [&str; 2] = [
    CLUSTER_CAPABILITY_METADATA_INGEST_V1,
    CLUSTER_CAPABILITY_METADATA_STORE_V1,
];
pub const HISTOGRAM_PAYLOAD_REQUIRED_CAPABILITIES: [&str; 2] = [
    CLUSTER_CAPABILITY_HISTOGRAM_INGEST_V1,
    CLUSTER_CAPABILITY_HISTOGRAM_STORAGE_V1,
];

pub fn default_cluster_capabilities() -> [&'static str; 14] {
    [
        CLUSTER_CAPABILITY_RPC_V1,
        CLUSTER_CAPABILITY_CONTROL_REPLICATION_V1,
        CLUSTER_CAPABILITY_CONTROL_SNAPSHOT_RPC_V1,
        CLUSTER_CAPABILITY_CONTROL_STATE_V1,
        CLUSTER_CAPABILITY_CONTROL_LOG_V1,
        CLUSTER_CAPABILITY_CONTROL_RECOVERY_SNAPSHOT_V1,
        CLUSTER_CAPABILITY_CLUSTER_SNAPSHOT_V1,
        CLUSTER_CAPABILITY_BUDGETED_RESTORE_V1,
        CLUSTER_CAPABILITY_METADATA_INGEST_V1,
        CLUSTER_CAPABILITY_METADATA_STORE_V1,
        CLUSTER_CAPABILITY_EXEMPLAR_INGEST_V1,
        CLUSTER_CAPABILITY_EXEMPLAR_QUERY_V1,
        CLUSTER_CAPABILITY_HISTOGRAM_INGEST_V1,
        CLUSTER_CAPABILITY_HISTOGRAM_STORAGE_V1,
    ]
}

pub(crate) fn normalize_capabilities<I, S>(capabilities: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut normalized = capabilities
        .into_iter()
        .map(Into::into)
        .map(|capability: String| capability.trim().to_string())
        .filter(|capability| !capability.is_empty())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn parse_capabilities_header(header: Option<&str>) -> Vec<String> {
    header
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|capability| !capability.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>()
}

pub fn required_capabilities_for_rows(rows: &[Row]) -> Vec<String> {
    if rows
        .iter()
        .any(|row| row.data_point().value_as_histogram().is_some())
    {
        return normalize_capabilities(HISTOGRAM_PAYLOAD_REQUIRED_CAPABILITIES);
    }
    Vec::new()
}

pub fn required_capabilities_for_internal_rows(
    rows: &[InternalRow],
    explicit_capabilities: &[String],
) -> Vec<String> {
    let mut required_capabilities = explicit_capabilities.to_vec();
    if rows
        .iter()
        .any(|row| row.data_point.value_as_histogram().is_some())
    {
        required_capabilities.extend(
            HISTOGRAM_PAYLOAD_REQUIRED_CAPABILITIES
                .iter()
                .copied()
                .map(ToString::to_string),
        );
    }
    normalize_capabilities(required_capabilities)
}

pub fn required_capabilities_for_internal_write(
    rows: &[InternalRow],
    exemplars: &[InternalWriteExemplar],
    metadata_updates: &[InternalMetricMetadataUpdate],
    explicit_capabilities: &[String],
) -> Vec<String> {
    let mut required_capabilities =
        required_capabilities_for_internal_rows(rows, explicit_capabilities);
    if !metadata_updates.is_empty() {
        required_capabilities.extend(
            METADATA_PAYLOAD_REQUIRED_CAPABILITIES
                .iter()
                .copied()
                .map(ToString::to_string),
        );
    }
    if !exemplars.is_empty() {
        required_capabilities.extend(
            EXEMPLAR_PAYLOAD_REQUIRED_CAPABILITIES
                .iter()
                .copied()
                .map(ToString::to_string),
        );
    }
    normalize_capabilities(required_capabilities)
}

#[derive(Debug, Clone)]
pub struct InternalApiConfig {
    pub auth_token: String,
    pub protocol_version: String,
    pub require_mtls: bool,
    pub allowed_node_ids: Vec<String>,
    pub compatibility: CompatibilityProfile,
    auth_runtime: Option<Arc<ManagedStringSecret>>,
}

impl InternalApiConfig {
    pub fn new(
        auth_token: String,
        protocol_version: String,
        require_mtls: bool,
        allowed_node_ids: Vec<String>,
    ) -> Self {
        let mut allowed_node_ids = allowed_node_ids
            .into_iter()
            .map(|node_id| node_id.trim().to_string())
            .filter(|node_id| !node_id.is_empty())
            .collect::<Vec<_>>();
        allowed_node_ids.sort();
        allowed_node_ids.dedup();
        Self {
            auth_token,
            protocol_version,
            require_mtls,
            allowed_node_ids,
            compatibility: CompatibilityProfile::default(),
            auth_runtime: None,
        }
    }

    pub fn with_compatibility(mut self, compatibility: CompatibilityProfile) -> Self {
        self.compatibility = compatibility;
        self
    }

    pub fn set_auth_runtime(&mut self, auth_runtime: Arc<ManagedStringSecret>) {
        self.auth_runtime = Some(auth_runtime);
    }

    pub fn auth_token_matches(&self, provided: Option<&str>) -> bool {
        self.auth_runtime
            .as_ref()
            .map(|runtime| runtime.matches(provided))
            .unwrap_or_else(|| provided == Some(self.auth_token.as_str()))
    }

    pub fn from_membership(
        membership: &MembershipView,
        require_mtls: bool,
        auth_token: Option<&str>,
    ) -> Result<Self, String> {
        let allowed_node_ids = membership
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .collect::<Vec<_>>();
        let auth_token = auth_token
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .map(ToString::to_string)
            .or_else(|| require_mtls.then(|| derive_shared_internal_token(membership)))
            .ok_or_else(|| {
                "--cluster-internal-auth-token is required when --cluster-internal-mtls-enabled=false"
                    .to_string()
            })?;
        Ok(Self::new(
            auth_token,
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            require_mtls,
            allowed_node_ids,
        ))
    }
}

impl PartialEq for InternalApiConfig {
    fn eq(&self, other: &Self) -> bool {
        self.auth_token == other.auth_token
            && self.protocol_version == other.protocol_version
            && self.require_mtls == other.require_mtls
            && self.allowed_node_ids == other.allowed_node_ids
            && self.compatibility == other.compatibility
    }
}

impl Eq for InternalApiConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalRow {
    pub metric: String,
    #[serde(default)]
    pub labels: Vec<Label>,
    pub data_point: DataPoint,
}

impl From<&Row> for InternalRow {
    fn from(row: &Row) -> Self {
        Self {
            metric: row.metric().to_string(),
            labels: row.labels().to_vec(),
            data_point: row.data_point().clone(),
        }
    }
}

impl InternalRow {
    pub fn into_row(self) -> Row {
        Row::with_labels(self.metric, self.labels, self.data_point)
    }
}

const REPAIR_QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;

fn repair_query_growth_capacity_upper(elements: usize) -> usize {
    if elements == 0 {
        return 0;
    }
    elements
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .max(4)
}

fn modeled_repair_vec_capacity_bytes<T>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    u64::try_from(capacity)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(std::mem::size_of::<T>()).unwrap_or(u64::MAX))
        .saturating_add(REPAIR_QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_repair_string_capacity_bytes(capacity: usize) -> u64 {
    if capacity == 0 {
        0
    } else {
        u64::try_from(capacity)
            .unwrap_or(u64::MAX)
            .saturating_add(REPAIR_QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_repair_histogram_retained_bytes(histogram: &tsink::NativeHistogram) -> u64 {
    u64::try_from(std::mem::size_of::<tsink::NativeHistogram>())
        .unwrap_or(u64::MAX)
        .saturating_add(modeled_repair_vec_capacity_bytes::<
            tsink::HistogramBucketSpan,
        >(histogram.negative_spans.capacity()))
        .saturating_add(modeled_repair_vec_capacity_bytes::<i64>(
            histogram.negative_deltas.capacity(),
        ))
        .saturating_add(modeled_repair_vec_capacity_bytes::<f64>(
            histogram.negative_counts.capacity(),
        ))
        .saturating_add(modeled_repair_vec_capacity_bytes::<
            tsink::HistogramBucketSpan,
        >(histogram.positive_spans.capacity()))
        .saturating_add(modeled_repair_vec_capacity_bytes::<i64>(
            histogram.positive_deltas.capacity(),
        ))
        .saturating_add(modeled_repair_vec_capacity_bytes::<f64>(
            histogram.positive_counts.capacity(),
        ))
        .saturating_add(modeled_repair_vec_capacity_bytes::<f64>(
            histogram.custom_values.capacity(),
        ))
        .saturating_add(REPAIR_QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_repair_value_retained_bytes(value: &tsink::Value) -> u64 {
    match value {
        tsink::Value::Bytes(bytes) => modeled_repair_vec_capacity_bytes::<u8>(bytes.capacity()),
        tsink::Value::String(text) => modeled_repair_string_capacity_bytes(text.capacity()),
        tsink::Value::Histogram(histogram) => modeled_repair_histogram_retained_bytes(histogram),
        tsink::Value::F64(_)
        | tsink::Value::I64(_)
        | tsink::Value::U64(_)
        | tsink::Value::Bool(_) => 0,
    }
}

fn modeled_repair_histogram_clone_upper_bytes(histogram: &tsink::NativeHistogram) -> u64 {
    u64::try_from(std::mem::size_of::<tsink::NativeHistogram>())
        .unwrap_or(u64::MAX)
        .saturating_add(modeled_repair_vec_capacity_bytes::<
            tsink::HistogramBucketSpan,
        >(repair_query_growth_capacity_upper(
            histogram.negative_spans.len(),
        )))
        .saturating_add(modeled_repair_vec_capacity_bytes::<i64>(
            repair_query_growth_capacity_upper(histogram.negative_deltas.len()),
        ))
        .saturating_add(modeled_repair_vec_capacity_bytes::<f64>(
            repair_query_growth_capacity_upper(histogram.negative_counts.len()),
        ))
        .saturating_add(modeled_repair_vec_capacity_bytes::<
            tsink::HistogramBucketSpan,
        >(repair_query_growth_capacity_upper(
            histogram.positive_spans.len(),
        )))
        .saturating_add(modeled_repair_vec_capacity_bytes::<i64>(
            repair_query_growth_capacity_upper(histogram.positive_deltas.len()),
        ))
        .saturating_add(modeled_repair_vec_capacity_bytes::<f64>(
            repair_query_growth_capacity_upper(histogram.positive_counts.len()),
        ))
        .saturating_add(modeled_repair_vec_capacity_bytes::<f64>(
            repair_query_growth_capacity_upper(histogram.custom_values.len()),
        ))
        .saturating_add(REPAIR_QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_repair_value_clone_upper_bytes(value: &tsink::Value) -> u64 {
    match value {
        tsink::Value::Bytes(bytes) => {
            modeled_repair_vec_capacity_bytes::<u8>(repair_query_growth_capacity_upper(bytes.len()))
        }
        tsink::Value::String(text) => {
            modeled_repair_string_capacity_bytes(repair_query_growth_capacity_upper(text.len()))
        }
        tsink::Value::Histogram(histogram) => modeled_repair_histogram_clone_upper_bytes(histogram),
        tsink::Value::F64(_)
        | tsink::Value::I64(_)
        | tsink::Value::U64(_)
        | tsink::Value::Bool(_) => 0,
    }
}

pub(crate) fn modeled_internal_repair_rows_clone_upper_bytes(rows: &[Row]) -> u64 {
    modeled_repair_vec_capacity_bytes::<InternalRow>(repair_query_growth_capacity_upper(rows.len()))
        .saturating_add(rows.iter().fold(0u64, |bytes, row| {
            bytes
                .saturating_add(modeled_repair_string_capacity_bytes(
                    repair_query_growth_capacity_upper(row.metric().len()),
                ))
                .saturating_add(modeled_repair_vec_capacity_bytes::<Label>(
                    repair_query_growth_capacity_upper(row.labels().len()),
                ))
                .saturating_add(row.labels().iter().fold(0u64, |label_bytes, label| {
                    label_bytes
                        .saturating_add(modeled_repair_string_capacity_bytes(
                            repair_query_growth_capacity_upper(label.name.len()),
                        ))
                        .saturating_add(modeled_repair_string_capacity_bytes(
                            repair_query_growth_capacity_upper(label.value.len()),
                        ))
                }))
                .saturating_add(modeled_repair_value_clone_upper_bytes(
                    &row.data_point().value,
                ))
        }))
}

pub(crate) fn modeled_internal_repair_rows_retained_bytes(rows: &Vec<InternalRow>) -> u64 {
    modeled_repair_vec_capacity_bytes::<InternalRow>(rows.capacity()).saturating_add(
        rows.iter().fold(0u64, |bytes, row| {
            bytes
                .saturating_add(modeled_repair_string_capacity_bytes(row.metric.capacity()))
                .saturating_add(modeled_repair_vec_capacity_bytes::<Label>(
                    row.labels.capacity(),
                ))
                .saturating_add(row.labels.iter().fold(0u64, |label_bytes, label| {
                    label_bytes
                        .saturating_add(modeled_repair_string_capacity_bytes(label.name.capacity()))
                        .saturating_add(modeled_repair_string_capacity_bytes(
                            label.value.capacity(),
                        ))
                }))
                .saturating_add(modeled_repair_value_retained_bytes(&row.data_point.value))
        }),
    )
}

fn modeled_repair_row_returned_bytes(
    metric: &str,
    labels: &[Label],
    data_point: &DataPoint,
) -> u64 {
    u64::try_from(std::mem::size_of::<Row>())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(metric.len()).unwrap_or(u64::MAX))
        .saturating_add(labels.iter().fold(0u64, |bytes, label| {
            bytes
                .saturating_add(u64::try_from(std::mem::size_of::<Label>()).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(label.name.len()).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(label.value.len()).unwrap_or(u64::MAX))
        }))
        // `Row` (and the wire-equivalent `InternalRow`) owns its `DataPoint` inline. Only the
        // value's logical payload is additional to the row envelope.
        .saturating_add(tsink::value::modeled_query_value_payload_bytes(
            &data_point.value,
        ))
}

pub(crate) fn modeled_repair_rows_returned_bytes(rows: &[Row]) -> u64 {
    rows.iter().fold(0u64, |bytes, row| {
        bytes.saturating_add(modeled_repair_row_returned_bytes(
            row.metric(),
            row.labels(),
            row.data_point(),
        ))
    })
}

pub(crate) fn modeled_internal_repair_rows_returned_bytes(rows: &[InternalRow]) -> u64 {
    rows.iter().fold(0u64, |bytes, row| {
        bytes.saturating_add(modeled_repair_row_returned_bytes(
            &row.metric,
            &row.labels,
            &row.data_point,
        ))
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalIngestRowsRequest {
    #[serde(default = "default_internal_ring_version")]
    pub ring_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    pub rows: Vec<InternalRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalIngestRowsResponse {
    /// Compatibility count retained for older internal clients.
    pub inserted_rows: usize,
    /// Canonical per-row outcome and durability established by the receiving replica.
    ///
    /// This is optional on the wire so a rolling-upgrade coordinator can decode an older peer's
    /// response. Write routing treats an absent result as unverified and does not count it as an
    /// acknowledgement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_result: Option<BatchWriteResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalWriteExemplar {
    pub metric: String,
    #[serde(default)]
    pub series_labels: Vec<Label>,
    #[serde(default)]
    pub exemplar_labels: Vec<Label>,
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalMetricMetadataUpdate {
    pub metric_family_name: String,
    pub metric_type: i32,
    #[serde(default)]
    pub help: String,
    #[serde(default)]
    pub unit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalIngestWriteRequest {
    #[serde(default = "default_internal_ring_version")]
    pub ring_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub rows: Vec<InternalRow>,
    #[serde(default)]
    pub metadata_updates: Vec<InternalMetricMetadataUpdate>,
    #[serde(default)]
    pub exemplars: Vec<InternalWriteExemplar>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalIngestWriteResponse {
    pub inserted_rows: usize,
    pub accepted_metadata_updates: usize,
    pub accepted_exemplars: usize,
    pub dropped_exemplars: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalSelectRequest {
    #[serde(default = "default_internal_ring_version")]
    pub ring_version: u64,
    pub metric: String,
    #[serde(default)]
    pub labels: Vec<Label>,
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalSelectResponse {
    pub points: Vec<DataPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalSelectBatchRequest {
    #[serde(default = "default_internal_ring_version")]
    pub ring_version: u64,
    pub selectors: Vec<MetricSeries>,
    pub start: i64,
    pub end: i64,
    /// Optional request-specific limits for one execution on the serving node.
    ///
    /// The omission preserves the legacy unaccounted RPC contract. A node that accepts this field
    /// returns [`InternalSelectBatchAccounting`] or an explicit compatibility error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_limits: Option<QueryWorkLimits>,
}

/// Complete counters charged by a bounded internal `select_batch` execution.
///
/// Presence is an accounting contract: the serving node used one execution for the local batch
/// and aggregated any handoff bridge executions into these counters. Older peers omit this
/// object, which callers must not interpret as zero work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalSelectBatchAccounting {
    pub execution: QueryExecutionSnapshot,
    /// Exact selector-existence bits in request order.
    ///
    /// A selector can be matched even when its requested time range contains no points. The
    /// optional form allows this version to decode the earlier aggregate-only additive response;
    /// current bounded callers require the vector and never infer existence from point counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_selectors: Option<Vec<bool>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalSelectBatchResponse {
    pub series: Vec<SeriesPoints>,
    /// Complete remote execution counters when `query_limits` was supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accounting: Option<InternalSelectBatchAccounting>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalSelectSeriesRequest {
    #[serde(default = "default_internal_ring_version")]
    pub ring_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_scope: Option<MetadataShardScope>,
    pub selection: SeriesSelection,
    /// Optional request-specific limits for one metadata execution on the serving node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_limits: Option<QueryWorkLimits>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalSelectSeriesAccounting {
    pub execution: QueryExecutionSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalSelectSeriesResponse {
    pub series: Vec<MetricSeries>,
    /// Complete serving-node execution counters when `query_limits` was supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accounting: Option<InternalSelectSeriesAccounting>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalExemplar {
    #[serde(default)]
    pub labels: Vec<Label>,
    pub value: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalExemplarSeries {
    pub metric: String,
    #[serde(default)]
    pub labels: Vec<Label>,
    #[serde(default)]
    pub exemplars: Vec<InternalExemplar>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalQueryExemplarsRequest {
    #[serde(default = "default_internal_ring_version")]
    pub ring_version: u64,
    #[serde(default)]
    pub selectors: Vec<SeriesSelection>,
    pub start: i64,
    pub end: i64,
    pub limit: usize,
    /// Optional request-specific limits for one execution on the serving node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_limits: Option<QueryWorkLimits>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalQueryExemplarsResponse {
    #[serde(default)]
    pub series: Vec<InternalExemplarSeries>,
    /// Complete serving-node execution counters when `query_limits` was supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accounting: Option<QueryExecutionSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalListMetricsRequest {
    #[serde(default = "default_internal_ring_version")]
    pub ring_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_scope: Option<MetadataShardScope>,
    /// Optional request-specific limits for one metadata execution on the serving node.
    ///
    /// Omission preserves the legacy unaccounted RPC contract. A node that accepts this field
    /// returns complete serving-node accounting or an explicit compatibility error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_limits: Option<QueryWorkLimits>,
}

impl Default for InternalListMetricsRequest {
    fn default() -> Self {
        Self {
            ring_version: default_internal_ring_version(),
            shard_scope: None,
            query_limits: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalListMetricsResponse {
    pub series: Vec<MetricSeries>,
    /// Complete serving-node execution counters when `query_limits` was supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accounting: Option<InternalSelectSeriesAccounting>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalDataSnapshotRequest {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalDataSnapshotResponse {
    pub node_id: String,
    pub path: String,
    pub created_unix_ms: u64,
    pub duration_ms: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalDataRestoreRequest {
    pub snapshot_path: String,
    pub data_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalDataRestoreResponse {
    pub node_id: String,
    pub snapshot_path: String,
    pub data_path: String,
    pub restored_unix_ms: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalDigestWindowRequest {
    #[serde(default = "default_internal_ring_version")]
    pub ring_version: u64,
    pub shard: u32,
    pub window_start: i64,
    pub window_end: i64,
    /// Required finite limits for the serving node's digest execution.
    ///
    /// The optional wire shape lets upgraded nodes reject older unbounded requests with a stable
    /// compatibility error instead of failing JSON decoding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_limits: Option<QueryWorkLimits>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InternalDigestWindowResponse {
    pub shard: u32,
    pub ring_version: u64,
    pub window_start: i64,
    pub window_end: i64,
    pub series_count: u64,
    pub point_count: u64,
    pub fingerprint: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InternalAccountedDigestWindowResponse {
    #[serde(flatten)]
    pub digest: InternalDigestWindowResponse,
    /// Complete serving-node execution counters for this mandatory bounded read.
    pub accounting: QueryExecutionSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalRepairBackfillRequest {
    #[serde(default = "default_internal_ring_version")]
    pub ring_version: u64,
    pub shard: u32,
    pub window_start: i64,
    pub window_end: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_series: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_offset: Option<u64>,
    /// Required finite limits for the serving node's shard-window scan execution.
    ///
    /// The optional wire shape lets upgraded nodes reject legacy omission with a stable error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_limits: Option<QueryWorkLimits>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalRepairBackfillResponse {
    pub shard: u32,
    pub ring_version: u64,
    pub window_start: i64,
    pub window_end: i64,
    pub series_scanned: u64,
    pub rows_scanned: u64,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_row_offset: Option<u64>,
    pub rows: Vec<InternalRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalAccountedRepairBackfillResponse {
    #[serde(flatten)]
    pub backfill: InternalRepairBackfillResponse,
    /// Complete serving-node execution counters for this mandatory bounded scan.
    pub accounting: QueryExecutionSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InternalControlCommand {
    SetLeader {
        leader_node_id: String,
    },
    JoinNode {
        node_id: String,
        endpoint: String,
    },
    LeaveNode {
        node_id: String,
    },
    RecommissionNode {
        node_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
    },
    ActivateNode {
        node_id: String,
    },
    RemoveNode {
        node_id: String,
    },
    BeginShardHandoff {
        shard: u32,
        from_node_id: String,
        to_node_id: String,
        activation_ring_version: u64,
    },
    UpdateShardHandoff {
        shard: u32,
        phase: ShardHandoffPhase,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        copied_rows: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pending_rows: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_error: Option<String>,
    },
    CompleteShardHandoff {
        shard: u32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InternalControlLogEntry {
    pub index: u64,
    pub term: u64,
    pub command: InternalControlCommand,
    pub created_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalControlAppendRequest {
    pub term: u64,
    pub leader_node_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    #[serde(default)]
    pub entries: Vec<InternalControlLogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalControlAppendResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalControlInstallSnapshotRequest {
    pub term: u64,
    pub leader_node_id: String,
    pub snapshot_last_index: u64,
    pub snapshot_last_term: u64,
    pub state: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalControlInstallSnapshotResponse {
    pub term: u64,
    pub success: bool,
    pub last_index: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalControlAutoJoinRequest {
    pub node_id: String,
    pub endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalControlAutoJoinResponse {
    pub result: String,
    pub membership_epoch: u64,
    pub node_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_node_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InternalErrorResponse {
    pub code: String,
    pub error: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_protocol_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_protocol_version: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub received_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_capabilities: Vec<String>,
}

pub fn internal_error_response(
    status: u16,
    code: impl Into<String>,
    message: impl Into<String>,
    retryable: bool,
) -> HttpResponse {
    let payload = InternalErrorResponse {
        code: code.into(),
        error: message.into(),
        retryable,
        expected_protocol_version: None,
        received_protocol_version: None,
        required_capabilities: Vec::new(),
        received_capabilities: Vec::new(),
        missing_capabilities: Vec::new(),
    };
    json_response(status, &payload)
}

pub fn protocol_mismatch_response(
    expected_protocol_version: &str,
    received_protocol_version: Option<&str>,
) -> HttpResponse {
    let payload = InternalErrorResponse {
        code: "protocol_version_mismatch".to_string(),
        error: format!(
            "protocol version mismatch: expected '{expected_protocol_version}', received '{}'",
            received_protocol_version.unwrap_or("<missing>")
        ),
        retryable: false,
        expected_protocol_version: Some(expected_protocol_version.to_string()),
        received_protocol_version: received_protocol_version.map(ToString::to_string),
        required_capabilities: Vec::new(),
        received_capabilities: Vec::new(),
        missing_capabilities: Vec::new(),
    };
    json_response(409, &payload)
}

pub fn peer_capability_mismatch_response(
    required_capabilities: Vec<String>,
    received_capabilities: Vec<String>,
    missing_capabilities: Vec<String>,
) -> HttpResponse {
    let payload = InternalErrorResponse {
        code: "peer_capability_missing".to_string(),
        error: format!(
            "peer is missing required capabilities: {}",
            missing_capabilities.join(", ")
        ),
        retryable: false,
        expected_protocol_version: None,
        received_protocol_version: None,
        required_capabilities,
        received_capabilities,
        missing_capabilities,
    };
    json_response(409, &payload)
}

pub fn unauthorized_internal_response() -> HttpResponse {
    let mut response = internal_error_response(
        401,
        "internal_auth_failed",
        "internal endpoint authentication failed",
        false,
    );
    response
        .headers
        .push(("WWW-Authenticate".to_string(), "TsinkInternal".to_string()));
    response
}

pub fn unauthorized_internal_mtls_response(message: &str) -> HttpResponse {
    internal_error_response(401, "internal_mtls_auth_failed", message, false)
}

#[allow(dead_code)]
pub fn authorize_internal_request(
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
) -> Result<(), HttpResponse> {
    authorize_internal_request_with_policy(request, internal_api, &[], false, &[])
}

pub fn authorize_internal_request_with_policy(
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    additional_allowed_node_ids: &[String],
    allow_unknown_mtls_node: bool,
    endpoint_required_capabilities: &[&str],
) -> Result<(), HttpResponse> {
    let Some(internal_api) = internal_api else {
        return Err(text_response(404, "not found"));
    };

    let provided_token = request.header(INTERNAL_RPC_AUTH_HEADER);
    if !internal_api.auth_token_matches(provided_token) {
        return Err(unauthorized_internal_response());
    }

    let provided_version = request.header(INTERNAL_RPC_VERSION_HEADER);
    if provided_version != Some(internal_api.protocol_version.as_str()) {
        return Err(protocol_mismatch_response(
            &internal_api.protocol_version,
            provided_version,
        ));
    }

    let required_capabilities =
        normalize_capabilities(endpoint_required_capabilities.iter().copied());
    let received_capabilities = normalize_capabilities(parse_capabilities_header(
        request.header(INTERNAL_RPC_CAPABILITIES_HEADER),
    ));
    let missing_capabilities = required_capabilities
        .iter()
        .filter(|capability| !received_capabilities.contains(capability))
        .cloned()
        .collect::<Vec<_>>();
    if !missing_capabilities.is_empty() {
        return Err(peer_capability_mismatch_response(
            required_capabilities,
            received_capabilities,
            missing_capabilities,
        ));
    }

    if internal_api.require_mtls {
        let claimed_node_id = request.header(INTERNAL_RPC_NODE_ID_HEADER).ok_or_else(|| {
            unauthorized_internal_mtls_response("missing internal node id header")
        })?;
        let verified_node_id = request
            .header(INTERNAL_RPC_VERIFIED_NODE_ID_HEADER)
            .ok_or_else(|| {
                unauthorized_internal_mtls_response(
                    "mTLS-authenticated peer identity is required for internal endpoint",
                )
            })?;
        if claimed_node_id != verified_node_id {
            return Err(unauthorized_internal_mtls_response(
                "internal node id header does not match mTLS peer identity",
            ));
        }
        if !allow_unknown_mtls_node
            && !internal_api
                .allowed_node_ids
                .iter()
                .chain(additional_allowed_node_ids.iter())
                .any(|node_id| node_id == verified_node_id)
        {
            return Err(unauthorized_internal_mtls_response(
                "mTLS peer identity is not part of cluster membership",
            ));
        }
    }

    Ok(())
}

pub fn derive_shared_internal_token(membership: &MembershipView) -> String {
    let mut nodes: Vec<String> = membership
        .nodes
        .iter()
        .map(|node| {
            format!(
                "{}@{}",
                node.id.trim().to_ascii_lowercase(),
                node.endpoint.trim().to_ascii_lowercase()
            )
        })
        .collect();
    nodes.sort();
    let signature = nodes.join(",");
    format!(
        "tsink-cluster-{:#016x}",
        stable_fnv1a64(signature.as_bytes())
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    Timeout {
        endpoint: String,
        path: String,
    },
    Transport {
        endpoint: String,
        path: String,
        message: String,
    },
    ResponseTooLarge {
        endpoint: String,
        path: String,
        limit: usize,
    },
    ProtocolVersionMismatch {
        endpoint: String,
        expected: String,
        received: Option<String>,
    },
    CompatibilityRejected {
        endpoint: String,
        path: String,
        message: String,
        missing_capabilities: Vec<String>,
    },
    HttpStatus {
        endpoint: String,
        path: String,
        status: u16,
        error_code: Option<String>,
        message: String,
        retryable: bool,
    },
    Serialize {
        message: String,
    },
    Deserialize {
        message: String,
    },
    QueryBudget {
        error: QueryBudgetError,
    },
}

impl RpcError {
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Timeout { .. }
                | Self::Transport { .. }
                | Self::HttpStatus {
                    retryable: true,
                    ..
                }
        )
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout { endpoint, path } => {
                write!(f, "RPC timeout calling {endpoint}{path}")
            }
            Self::Transport {
                endpoint,
                path,
                message,
            } => write!(f, "RPC transport error calling {endpoint}{path}: {message}"),
            Self::ResponseTooLarge {
                endpoint,
                path,
                limit,
            } => write!(
                f,
                "RPC response from {endpoint}{path} exceeds the {limit}-byte limit"
            ),
            Self::ProtocolVersionMismatch {
                endpoint,
                expected,
                received,
            } => write!(
                f,
                "RPC protocol mismatch for {endpoint}: expected '{expected}', received '{}'",
                received.as_deref().unwrap_or("<missing>")
            ),
            Self::CompatibilityRejected {
                endpoint,
                path,
                message,
                ..
            } => write!(
                f,
                "RPC compatibility rejection from {endpoint}{path}: {message}"
            ),
            Self::HttpStatus {
                endpoint,
                path,
                status,
                message,
                ..
            } => write!(f, "RPC HTTP {status} from {endpoint}{path}: {message}"),
            Self::Serialize { message } => write!(f, "RPC request serialization failed: {message}"),
            Self::Deserialize { message } => {
                write!(f, "RPC response deserialization failed: {message}")
            }
            Self::QueryBudget { error } => write!(f, "RPC query budget failed: {error}"),
        }
    }
}

impl std::error::Error for RpcError {}

/// Decoded RPC response whose transport and decode allocation remains query-memory-accounted.
#[derive(Debug)]
pub struct AccountedRpcResponse<T> {
    pub response: T,
    pub reservation: QueryMemoryReservation,
}

struct RpcResponseEnvelope<T> {
    response: T,
    reservation: Option<QueryMemoryReservation>,
}

#[derive(Debug, Default)]
struct RpcJsonLengthWriter {
    bytes: usize,
}

impl std::io::Write for RpcJsonLengthWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes = self.bytes.checked_add(buffer.len()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "RPC request JSON length overflow",
            )
        })?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialize_json_request_accounted<Req>(
    request: &Req,
    execution: &QueryExecution,
) -> Result<(Vec<u8>, QueryMemoryReservation), RpcError>
where
    Req: Serialize,
{
    // The first pass performs no request-buffer allocation. Reserve the exact compact-JSON byte
    // count before allocating the fixed-size second-pass buffer so a rejected query never grows
    // an unaccounted request body.
    let mut length_writer = RpcJsonLengthWriter::default();
    serde_json::to_writer(&mut length_writer, request).map_err(|err| RpcError::Serialize {
        message: err.to_string(),
    })?;
    let encoded_len = length_writer.bytes;
    let encoded_bytes = u64::try_from(encoded_len).unwrap_or(u64::MAX);
    let reservation = execution
        .reserve_memory(encoded_bytes)
        .map_err(|error| RpcError::QueryBudget { error })?;

    let mut body = vec![0u8; encoded_len];
    let written = {
        let mut writer = std::io::Cursor::new(body.as_mut_slice());
        serde_json::to_writer(&mut writer, request).map_err(|err| RpcError::Serialize {
            message: err.to_string(),
        })?;
        usize::try_from(writer.position()).unwrap_or(usize::MAX)
    };
    if written != encoded_len {
        return Err(RpcError::Serialize {
            message: format!(
                "RPC request JSON length changed between preflight ({encoded_len} bytes) and \
                 serialization ({written} bytes)"
            ),
        });
    }

    Ok((body, reservation))
}

#[derive(Debug, Clone)]
pub struct RpcClientConfig {
    pub timeout: Duration,
    pub max_retries: usize,
    pub protocol_version: String,
    pub internal_auth_token: String,
    pub internal_auth_runtime: Option<Arc<ManagedStringSecret>>,
    pub local_node_id: String,
    pub compatibility: CompatibilityProfile,
    pub internal_mtls: Option<RpcClientInternalMtlsConfig>,
}

#[derive(Debug, Clone)]
pub struct RpcClientInternalMtlsConfig {
    pub ca_cert: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl Default for RpcClientConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(DEFAULT_RPC_TIMEOUT_MS),
            max_retries: DEFAULT_RPC_MAX_RETRIES,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: String::new(),
            internal_auth_runtime: None,
            local_node_id: String::new(),
            compatibility: CompatibilityProfile::default(),
            internal_mtls: None,
        }
    }
}

#[allow(dead_code)]
impl RpcClientConfig {
    pub fn with_compatibility(mut self, compatibility: CompatibilityProfile) -> Self {
        self.compatibility = compatibility;
        self
    }
}

#[derive(Debug, Clone)]
pub struct RpcClient {
    config: RpcClientConfig,
}

#[allow(dead_code)]
impl RpcClient {
    pub fn new(config: RpcClientConfig) -> Self {
        Self { config }
    }

    pub fn set_internal_auth_runtime(&mut self, auth_runtime: Arc<ManagedStringSecret>) {
        self.config.internal_auth_runtime = Some(auth_runtime);
    }

    pub async fn ingest_rows(
        &self,
        endpoint: &str,
        request: &InternalIngestRowsRequest,
    ) -> Result<InternalIngestRowsResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/ingest_rows", request)
            .await
    }

    pub async fn ingest_write(
        &self,
        endpoint: &str,
        request: &InternalIngestWriteRequest,
    ) -> Result<InternalIngestWriteResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/ingest_write", request)
            .await
    }

    pub async fn select(
        &self,
        endpoint: &str,
        request: &InternalSelectRequest,
    ) -> Result<InternalSelectResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/select", request)
            .await
    }

    pub async fn select_batch(
        &self,
        endpoint: &str,
        request: &InternalSelectBatchRequest,
    ) -> Result<InternalSelectBatchResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/select_batch", request)
            .await
    }

    pub async fn select_batch_accounted(
        &self,
        endpoint: &str,
        request: &InternalSelectBatchRequest,
        execution: &QueryExecution,
    ) -> Result<AccountedRpcResponse<InternalSelectBatchResponse>, RpcError> {
        self.post_json_accounted(endpoint, "/internal/v1/select_batch", request, execution)
            .await
    }

    pub async fn select_series(
        &self,
        endpoint: &str,
        request: &InternalSelectSeriesRequest,
    ) -> Result<InternalSelectSeriesResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/select_series", request)
            .await
    }

    pub async fn select_series_accounted(
        &self,
        endpoint: &str,
        request: &InternalSelectSeriesRequest,
        execution: &QueryExecution,
    ) -> Result<AccountedRpcResponse<InternalSelectSeriesResponse>, RpcError> {
        self.post_json_accounted(endpoint, "/internal/v1/select_series", request, execution)
            .await
    }

    pub async fn query_exemplars(
        &self,
        endpoint: &str,
        request: &InternalQueryExemplarsRequest,
    ) -> Result<InternalQueryExemplarsResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/query_exemplars", request)
            .await
    }

    pub async fn query_exemplars_accounted(
        &self,
        endpoint: &str,
        request: &InternalQueryExemplarsRequest,
        execution: &QueryExecution,
    ) -> Result<AccountedRpcResponse<InternalQueryExemplarsResponse>, RpcError> {
        self.post_json_accounted(endpoint, "/internal/v1/query_exemplars", request, execution)
            .await
    }

    pub async fn list_metrics(
        &self,
        endpoint: &str,
    ) -> Result<InternalListMetricsResponse, RpcError> {
        self.list_metrics_with_request(endpoint, &InternalListMetricsRequest::default())
            .await
    }

    pub async fn list_metrics_with_request(
        &self,
        endpoint: &str,
        request: &InternalListMetricsRequest,
    ) -> Result<InternalListMetricsResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/list_metrics", request)
            .await
    }

    pub async fn list_metrics_accounted(
        &self,
        endpoint: &str,
        request: &InternalListMetricsRequest,
        execution: &QueryExecution,
    ) -> Result<AccountedRpcResponse<InternalListMetricsResponse>, RpcError> {
        self.post_json_accounted(endpoint, "/internal/v1/list_metrics", request, execution)
            .await
    }

    pub async fn digest_window_accounted(
        &self,
        endpoint: &str,
        request: &InternalDigestWindowRequest,
        execution: &QueryExecution,
    ) -> Result<AccountedRpcResponse<InternalAccountedDigestWindowResponse>, RpcError> {
        self.post_json_accounted(endpoint, "/internal/v1/digest_window", request, execution)
            .await
    }

    pub async fn data_snapshot(
        &self,
        endpoint: &str,
        request: &InternalDataSnapshotRequest,
    ) -> Result<InternalDataSnapshotResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/snapshot_data", request)
            .await
    }

    pub async fn data_restore_budgeted(
        &self,
        endpoint: &str,
        request: &InternalDataRestoreRequest,
    ) -> Result<InternalDataRestoreResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/restore_data_budgeted", request)
            .await
    }

    pub async fn repair_backfill_accounted(
        &self,
        endpoint: &str,
        request: &InternalRepairBackfillRequest,
        execution: &QueryExecution,
    ) -> Result<AccountedRpcResponse<InternalAccountedRepairBackfillResponse>, RpcError> {
        self.post_json_accounted(endpoint, "/internal/v1/repair_backfill", request, execution)
            .await
    }

    pub async fn control_append(
        &self,
        endpoint: &str,
        request: &InternalControlAppendRequest,
    ) -> Result<InternalControlAppendResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/control/append", request)
            .await
    }

    pub async fn control_install_snapshot(
        &self,
        endpoint: &str,
        request: &InternalControlInstallSnapshotRequest,
    ) -> Result<InternalControlInstallSnapshotResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/control/install_snapshot", request)
            .await
    }

    pub async fn control_auto_join(
        &self,
        endpoint: &str,
        request: &InternalControlAutoJoinRequest,
    ) -> Result<InternalControlAutoJoinResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/control/auto_join", request)
            .await
    }

    async fn post_json<Req, Resp>(
        &self,
        endpoint: &str,
        path: &str,
        request: &Req,
    ) -> Result<Resp, RpcError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            match self.post_json_once(endpoint, path, request).await {
                Ok(response) => return Ok(response),
                Err(err) => {
                    if attempts > self.config.max_retries + 1 || !err.retryable() {
                        return Err(err);
                    }
                }
            }
        }
    }

    async fn post_json_accounted<Req, Resp>(
        &self,
        endpoint: &str,
        path: &str,
        request: &Req,
        execution: &QueryExecution,
    ) -> Result<AccountedRpcResponse<Resp>, RpcError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let mut attempts = 0usize;
        loop {
            execution
                .checkpoint()
                .map_err(|error| RpcError::QueryBudget { error })?;
            attempts += 1;
            let attempt = self.post_json_once_impl(endpoint, path, request, Some(execution));
            tokio::pin!(attempt);
            let result = tokio::select! {
                result = &mut attempt => result,
                error = wait_for_query_execution_control(execution) => {
                    Err(RpcError::QueryBudget { error })
                }
            };
            match result {
                Ok(mut response) => {
                    execution
                        .checkpoint()
                        .map_err(|error| RpcError::QueryBudget { error })?;
                    let reservation =
                        response
                            .reservation
                            .take()
                            .ok_or_else(|| RpcError::Deserialize {
                                message: "bounded RPC response omitted its memory reservation"
                                    .to_string(),
                            })?;
                    return Ok(AccountedRpcResponse {
                        response: response.response,
                        reservation,
                    });
                }
                Err(err) => {
                    execution
                        .checkpoint()
                        .map_err(|error| RpcError::QueryBudget { error })?;
                    if attempts > self.config.max_retries + 1 || !err.retryable() {
                        return Err(err);
                    }
                }
            }
        }
    }

    async fn post_json_once<Req, Resp>(
        &self,
        endpoint: &str,
        path: &str,
        request: &Req,
    ) -> Result<Resp, RpcError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        self.post_json_once_impl(endpoint, path, request, None)
            .await
            .map(|response| response.response)
    }

    async fn post_json_once_impl<Req, Resp>(
        &self,
        endpoint: &str,
        path: &str,
        request: &Req,
        execution: Option<&QueryExecution>,
    ) -> Result<RpcResponseEnvelope<Resp>, RpcError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let (body, mut reservation) = match execution {
            Some(execution) => {
                let (body, reservation) = serialize_json_request_accounted(request, execution)?;
                (body, Some(reservation))
            }
            None => (
                serde_json::to_vec(request).map_err(|err| RpcError::Serialize {
                    message: err.to_string(),
                })?,
                None,
            ),
        };
        let request_bytes = match reservation.as_mut() {
            Some(reservation) => {
                self.build_http_request_accounted(endpoint, path, body.len(), reservation)?
            }
            None => self.build_http_request(endpoint, path, &body),
        };

        let mut stream = tokio::time::timeout(
            self.config.timeout,
            tokio::net::TcpStream::connect(endpoint),
        )
        .await
        .map_err(|_| RpcError::Timeout {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
        })?
        .map_err(|err| RpcError::Transport {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
            message: err.to_string(),
        })?;

        let raw_response = if let Some(mtls) = &self.config.internal_mtls {
            let tls_config =
                build_client_tls_config(mtls).map_err(|message| RpcError::Transport {
                    endpoint: endpoint.to_string(),
                    path: path.to_string(),
                    message,
                })?;
            let tls_connector = TlsConnector::from(Arc::new(tls_config));
            let server_name =
                tls_server_name_from_endpoint(endpoint).map_err(|message| RpcError::Transport {
                    endpoint: endpoint.to_string(),
                    path: path.to_string(),
                    message,
                })?;
            let mut tls_stream = tokio::time::timeout(
                self.config.timeout,
                tls_connector.connect(server_name, stream),
            )
            .await
            .map_err(|_| RpcError::Timeout {
                endpoint: endpoint.to_string(),
                path: path.to_string(),
            })?
            .map_err(|err| RpcError::Transport {
                endpoint: endpoint.to_string(),
                path: path.to_string(),
                message: format!("TLS handshake failed: {err}"),
            })?;
            write_and_read_response(
                &mut tls_stream,
                &request_bytes,
                &body,
                self.config.timeout,
                endpoint,
                path,
                reservation.as_mut(),
            )
            .await?
        } else {
            write_and_read_response(
                &mut stream,
                &request_bytes,
                &body,
                self.config.timeout,
                endpoint,
                path,
                reservation.as_mut(),
            )
            .await?
        };

        if let Some(reservation) = reservation.as_mut() {
            let decode_envelope = u64::try_from(raw_response.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(128)
                .saturating_add(8 * 1024);
            let total = reservation.bytes().saturating_add(decode_envelope);
            reservation
                .resize(total)
                .map_err(|error| RpcError::QueryBudget { error })?;
        }
        let parsed = parse_http_response(&raw_response).map_err(|err| RpcError::Transport {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
            message: err,
        })?;

        if (200..300).contains(&parsed.status) {
            let response = serde_json::from_slice::<Resp>(&parsed.body).map_err(|err| {
                RpcError::Deserialize {
                    message: err.to_string(),
                }
            })?;
            return Ok(RpcResponseEnvelope {
                response,
                reservation,
            });
        }

        let parsed_error = serde_json::from_slice::<InternalErrorResponse>(&parsed.body).ok();
        if parsed.status == 409
            && parsed_error
                .as_ref()
                .is_some_and(|err| err.code == "protocol_version_mismatch")
        {
            let mismatch = parsed_error.expect("checked is_some");
            return Err(RpcError::ProtocolVersionMismatch {
                endpoint: endpoint.to_string(),
                expected: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
                received: mismatch
                    .received_protocol_version
                    .map(|value| sanitize_rpc_error_diagnostic(&value)),
            });
        }
        if parsed.status == 409
            && parsed_error
                .as_ref()
                .is_some_and(|err| err.code == "peer_capability_missing")
        {
            let mismatch = parsed_error.expect("checked is_some");
            return Err(RpcError::CompatibilityRejected {
                endpoint: endpoint.to_string(),
                path: path.to_string(),
                message: sanitize_rpc_error_diagnostic(&mismatch.error),
                missing_capabilities: mismatch
                    .missing_capabilities
                    .into_iter()
                    .take(MAX_RPC_MISSING_CAPABILITIES)
                    .map(|capability| sanitize_rpc_error_diagnostic(&capability))
                    .collect(),
            });
        }

        let retryable = parsed_error
            .as_ref()
            .map(|err| err.retryable)
            .unwrap_or_else(|| RETRYABLE_STATUS_CODES.contains(&parsed.status));
        let error_code = parsed_error
            .as_ref()
            .and_then(|err| sanitize_rpc_error_code(&err.code));
        let message = parsed_error
            .as_ref()
            .map(|err| sanitize_rpc_error_diagnostic(&err.error))
            .unwrap_or_else(|| "remote peer returned an unstructured error response".to_string());

        Err(RpcError::HttpStatus {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
            status: parsed.status,
            error_code,
            message,
            retryable,
        })
    }

    fn with_internal_auth_token<T>(&self, use_token: impl FnOnce(&str) -> T) -> T {
        if let Some(runtime) = &self.config.internal_auth_runtime {
            runtime.with_current(use_token)
        } else {
            use_token(&self.config.internal_auth_token)
        }
    }

    fn write_http_request_header<W: IoWrite>(
        &self,
        writer: &mut W,
        endpoint: &str,
        path: &str,
        body_len: usize,
        auth_token: &str,
    ) -> std::io::Result<()> {
        writer.write_all(b"POST ")?;
        writer.write_all(path.as_bytes())?;
        writer.write_all(b" HTTP/1.1\r\nHost: ")?;
        writer.write_all(endpoint.as_bytes())?;
        writer.write_all(b"\r\nConnection: close\r\nContent-Type: application/json\r\n")?;
        write!(writer, "Content-Length: {body_len}\r\n")?;
        writer.write_all(INTERNAL_RPC_VERSION_HEADER.as_bytes())?;
        writer.write_all(b": ")?;
        writer.write_all(self.config.protocol_version.as_bytes())?;
        writer.write_all(b"\r\n")?;
        writer.write_all(INTERNAL_RPC_AUTH_HEADER.as_bytes())?;
        writer.write_all(b": ")?;
        writer.write_all(auth_token.as_bytes())?;
        writer.write_all(b"\r\n")?;
        writer.write_all(INTERNAL_RPC_CAPABILITIES_HEADER.as_bytes())?;
        writer.write_all(b": ")?;
        for (index, capability) in self.config.compatibility.capabilities.iter().enumerate() {
            if index != 0 {
                writer.write_all(b",")?;
            }
            writer.write_all(capability.as_bytes())?;
        }
        writer.write_all(b"\r\n")?;
        let local_node_id = self.config.local_node_id.trim();
        if !local_node_id.is_empty() {
            writer.write_all(INTERNAL_RPC_NODE_ID_HEADER.as_bytes())?;
            writer.write_all(b": ")?;
            writer.write_all(local_node_id.as_bytes())?;
            writer.write_all(b"\r\n")?;
        }
        writer.write_all(b"\r\n")
    }

    fn build_http_request(&self, endpoint: &str, path: &str, body: &[u8]) -> Vec<u8> {
        self.with_internal_auth_token(|auth_token| {
            let mut request = Vec::new();
            self.write_http_request_header(&mut request, endpoint, path, body.len(), auth_token)
                .expect("writing an HTTP request header into memory cannot fail");
            request
        })
    }

    fn build_http_request_accounted(
        &self,
        endpoint: &str,
        path: &str,
        body_len: usize,
        reservation: &mut QueryMemoryReservation,
    ) -> Result<Vec<u8>, RpcError> {
        self.with_internal_auth_token(|auth_token| {
            let mut length_writer = RpcJsonLengthWriter::default();
            self.write_http_request_header(
                &mut length_writer,
                endpoint,
                path,
                body_len,
                auth_token,
            )
            .map_err(|err| RpcError::Serialize {
                message: err.to_string(),
            })?;
            let header_len = length_writer.bytes;
            reservation
                .resize(
                    reservation
                        .bytes()
                        .saturating_add(u64::try_from(header_len).unwrap_or(u64::MAX)),
                )
                .map_err(|error| RpcError::QueryBudget { error })?;

            let mut request = vec![0u8; header_len];
            let written = {
                let mut writer = std::io::Cursor::new(request.as_mut_slice());
                self.write_http_request_header(&mut writer, endpoint, path, body_len, auth_token)
                    .map_err(|err| RpcError::Serialize {
                        message: err.to_string(),
                    })?;
                usize::try_from(writer.position()).unwrap_or(usize::MAX)
            };
            if written != header_len {
                return Err(RpcError::Serialize {
                    message: format!(
                        "RPC request header length changed between preflight ({header_len} bytes) \
                         and serialization ({written} bytes)"
                    ),
                });
            }
            Ok(request)
        })
    }
}

async fn wait_for_query_execution_control(execution: &QueryExecution) -> QueryBudgetError {
    const POLL_INTERVAL: Duration = Duration::from_millis(5);
    loop {
        if let Err(error) = execution.checkpoint() {
            return error;
        }
        let delay = execution
            .cancellation_token()
            .deadline()
            .and_then(|deadline| deadline.checked_duration_since(std::time::Instant::now()))
            .map_or(POLL_INTERVAL, |remaining| remaining.min(POLL_INTERVAL));
        if delay.is_zero() {
            tokio::task::yield_now().await;
        } else {
            tokio::time::sleep(delay).await;
        }
    }
}

fn build_client_tls_config(
    mtls: &RpcClientInternalMtlsConfig,
) -> Result<rustls::ClientConfig, String> {
    ensure_rustls_crypto_provider();
    let (ca_certs, _) =
        crate::security::load_pem_certs_from_source(&mtls.ca_cert, "internal mTLS CA cert file")?;

    let mut roots = rustls::RootCertStore::empty();
    for cert in ca_certs {
        roots
            .add(cert)
            .map_err(|err| format!("failed to add internal mTLS CA cert to root store: {err}"))?;
    }

    let (certs, _) =
        crate::security::load_pem_certs_from_source(&mtls.cert, "internal mTLS client cert file")?;
    let (key, _) =
        crate::security::load_private_key_from_source(&mtls.key, "internal mTLS client key file")?;

    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|err| format!("failed to build internal mTLS client config: {err}"))
}

fn tls_server_name_from_endpoint(
    endpoint: &str,
) -> Result<rustls::pki_types::ServerName<'static>, String> {
    let host = if let Some(stripped) = endpoint.strip_prefix('[') {
        let end = stripped
            .find(']')
            .ok_or_else(|| format!("invalid internal RPC endpoint '{endpoint}' for TLS"))?;
        let host = &stripped[..end];
        if host.trim().is_empty() {
            return Err(format!(
                "internal RPC endpoint '{endpoint}' has empty host for TLS"
            ));
        }
        host
    } else {
        let (host, _port) = endpoint.rsplit_once(':').ok_or_else(|| {
            format!("internal RPC endpoint '{endpoint}' must use host:port syntax")
        })?;
        if host.trim().is_empty() {
            return Err(format!(
                "internal RPC endpoint '{endpoint}' has empty host for TLS"
            ));
        }
        host
    };
    rustls::pki_types::ServerName::try_from(host.trim().to_string())
        .map_err(|_| format!("invalid internal RPC TLS server name '{host}'"))
}

async fn write_and_read_response<S>(
    stream: &mut S,
    request_bytes: &[u8],
    body: &[u8],
    timeout: Duration,
    endpoint: &str,
    path: &str,
    reservation: Option<&mut QueryMemoryReservation>,
) -> Result<Vec<u8>, RpcError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::time::timeout(timeout, stream.write_all(request_bytes))
        .await
        .map_err(|_| RpcError::Timeout {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
        })?
        .map_err(|err| RpcError::Transport {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
            message: err.to_string(),
        })?;

    tokio::time::timeout(timeout, stream.write_all(body))
        .await
        .map_err(|_| RpcError::Timeout {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
        })?
        .map_err(|err| RpcError::Transport {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
            message: err.to_string(),
        })?;

    let _ = tokio::time::timeout(timeout, stream.flush()).await;
    let _ = stream.shutdown().await;

    match tokio::time::timeout(timeout, async {
        match reservation {
            Some(reservation) => {
                read_response_bounded_accounted(
                    stream,
                    MAX_INTERNAL_RPC_RESPONSE_BYTES,
                    reservation,
                )
                .await
            }
            None => read_response_bounded(stream, MAX_INTERNAL_RPC_RESPONSE_BYTES).await,
        }
    })
    .await
    {
        Err(_) => Err(RpcError::Timeout {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
        }),
        Ok(Ok(raw_response)) => Ok(raw_response),
        Ok(Err(BoundedResponseReadError::Io(err))) => Err(RpcError::Transport {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
            message: err.to_string(),
        }),
        Ok(Err(BoundedResponseReadError::LimitExceeded)) => Err(RpcError::ResponseTooLarge {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
            limit: MAX_INTERNAL_RPC_RESPONSE_BYTES,
        }),
        Ok(Err(BoundedResponseReadError::QueryBudget(error))) => {
            Err(RpcError::QueryBudget { error })
        }
    }
}

#[derive(Debug)]
enum BoundedResponseReadError {
    Io(std::io::Error),
    LimitExceeded,
    QueryBudget(QueryBudgetError),
}

async fn read_response_bounded_accounted<R>(
    reader: &mut R,
    max_bytes: usize,
    reservation: &mut QueryMemoryReservation,
) -> Result<Vec<u8>, BoundedResponseReadError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let initial_capacity = max_bytes.min(8 * 1024);
    reservation
        .resize(
            reservation
                .bytes()
                .saturating_add(u64::try_from(initial_capacity).unwrap_or(u64::MAX)),
        )
        .map_err(BoundedResponseReadError::QueryBudget)?;
    let mut response = Vec::with_capacity(initial_capacity);
    let mut buffer = [0u8; 8 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(BoundedResponseReadError::Io)?;
        if read == 0 {
            break;
        }
        let desired_len = response
            .len()
            .checked_add(read)
            .ok_or(BoundedResponseReadError::LimitExceeded)?;
        if desired_len > max_bytes {
            return Err(BoundedResponseReadError::LimitExceeded);
        }
        if desired_len > response.capacity() {
            let desired_capacity = desired_len
                .checked_next_power_of_two()
                .unwrap_or(max_bytes)
                .min(max_bytes);
            let additional_capacity = desired_capacity.saturating_sub(response.capacity());
            reservation
                .resize(
                    reservation
                        .bytes()
                        .saturating_add(u64::try_from(additional_capacity).unwrap_or(u64::MAX)),
                )
                .map_err(BoundedResponseReadError::QueryBudget)?;
            response.reserve_exact(additional_capacity);
        }
        response.extend_from_slice(&buffer[..read]);
    }
    Ok(response)
}

async fn read_response_bounded<R>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<Vec<u8>, BoundedResponseReadError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let read_limit = u64::try_from(max_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut limited = reader.take(read_limit);
    let mut response = Vec::with_capacity(max_bytes.min(8 * 1024));
    limited
        .read_to_end(&mut response)
        .await
        .map_err(BoundedResponseReadError::Io)?;
    if response.len() > max_bytes {
        return Err(BoundedResponseReadError::LimitExceeded);
    }
    Ok(response)
}

#[derive(Debug, Clone)]
struct ParsedHttpResponse {
    status: u16,
    #[allow(dead_code)]
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn sanitize_rpc_error_diagnostic(message: &str) -> String {
    let mut sanitized = String::with_capacity(message.len().min(MAX_RPC_ERROR_DIAGNOSTIC_BYTES));
    let mut previous_space = false;
    for character in message.chars() {
        let character = if character.is_control() || character.is_whitespace() {
            ' '
        } else {
            character
        };
        if character == ' ' && previous_space {
            continue;
        }
        if sanitized.len().saturating_add(character.len_utf8()) > MAX_RPC_ERROR_DIAGNOSTIC_BYTES {
            break;
        }
        sanitized.push(character);
        previous_space = character == ' ';
    }
    let sanitized = sanitized.trim();
    if sanitized.is_empty() {
        "remote peer returned an error".to_string()
    } else {
        sanitized.to_string()
    }
}

fn sanitize_rpc_error_code(code: &str) -> Option<String> {
    if code
        .chars()
        .all(|character| character.is_control() || character.is_whitespace())
    {
        return None;
    }
    Some(sanitize_rpc_error_diagnostic(code))
}

fn parse_http_response(raw: &[u8]) -> Result<ParsedHttpResponse, String> {
    let Some(header_end) = raw.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Err("response is missing header terminator".to_string());
    };

    let headers_raw = &raw[..header_end];
    let body_raw = &raw[header_end + 4..];
    let headers_text = std::str::from_utf8(headers_raw)
        .map_err(|_| "response headers are not valid UTF-8".to_string())?;

    let mut lines = headers_text.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| "response is missing status line".to_string())?;
    let mut status_parts = status_line.split_whitespace();
    let version = status_parts
        .next()
        .ok_or_else(|| "response status line is missing HTTP version".to_string())?;
    if version != "HTTP/1.1" && version != "HTTP/1.0" {
        return Err("unsupported HTTP version".to_string());
    }
    let status = status_parts
        .next()
        .ok_or_else(|| "response status line is missing status code".to_string())?
        .parse::<u16>()
        .map_err(|_| "response status code is invalid".to_string())?;

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err("malformed response header line".to_string());
        };
        let name = name.trim();
        if name.is_empty() {
            return Err("malformed response header line".to_string());
        }
        let name = name.to_ascii_lowercase();
        let value = value.trim().to_string();
        match name.as_str() {
            "content-length" if headers.contains_key("content-length") => {
                return Err("duplicate response content-length header".to_string());
            }
            "transfer-encoding" => {
                return Err("response transfer-encoding is not supported".to_string());
            }
            _ => {}
        }
        headers.insert(name, value);
    }

    let body = if let Some(content_length) = headers
        .get("content-length")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| "response content-length is invalid".to_string())?
    {
        if body_raw.len() < content_length {
            return Err(format!(
                "response body truncated: expected {content_length} bytes, got {}",
                body_raw.len()
            ));
        }
        body_raw[..content_length].to_vec()
    } else {
        body_raw.to_vec()
    };

    Ok(ParsedHttpResponse {
        status,
        headers,
        body,
    })
}

fn default_internal_ring_version() -> u64 {
    DEFAULT_INTERNAL_RING_VERSION
}

fn stable_fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::ClusterConfig;
    use crate::cluster::membership::MembershipView;
    use crate::http::{read_http_request, write_http_response, HttpRequest, HttpResponse};
    use std::io::BufReader;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;
    use tsink::{QueryBudget, QueryBudgetLimits, QueryLimitReason};

    const TEST_INTERNAL_MTLS_CA_CERT_PEM: &str = include_str!("testdata/internal-mtls-ca-cert.pem");
    const TEST_INTERNAL_MTLS_CLIENT_A_CERT_PEM: &str =
        include_str!("testdata/internal-mtls-client-a-cert.pem");
    const TEST_INTERNAL_MTLS_CLIENT_A_KEY_PEM: &str =
        include_str!("testdata/internal-mtls-client-a-key.pem");
    const TEST_INTERNAL_MTLS_CLIENT_B_CERT_PEM: &str =
        include_str!("testdata/internal-mtls-client-b-cert.pem");
    const TEST_INTERNAL_MTLS_CLIENT_B_KEY_PEM: &str =
        include_str!("testdata/internal-mtls-client-b-key.pem");
    const TEST_INTERNAL_MTLS_SERVER_CERT_PEM: &str =
        include_str!("testdata/internal-mtls-server-cert.pem");
    const TEST_INTERNAL_MTLS_SERVER_KEY_PEM: &str =
        include_str!("testdata/internal-mtls-server-key.pem");

    fn write_test_file(path: &Path, contents: &str) {
        std::fs::write(path, contents).expect("test fixture file should be writable");
    }

    fn insert_compatibility_headers(
        headers: &mut HashMap<String, String>,
        compatibility: &CompatibilityProfile,
    ) {
        headers.insert(
            INTERNAL_RPC_CAPABILITIES_HEADER.to_string(),
            compatibility.capabilities.join(","),
        );
    }

    fn query_execution_with_memory_limit(memory_limit: u64) -> (QueryBudget, QueryExecution) {
        query_execution_with_memory_limit_and_token(
            memory_limit,
            tsink::QueryCancellationToken::new(),
        )
    }

    fn query_execution_with_memory_limit_and_token(
        memory_limit: u64,
        token: tsink::QueryCancellationToken,
    ) -> (QueryBudget, QueryExecution) {
        let budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(memory_limit),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(memory_limit),
                ..QueryWorkLimits::default()
            },
        })
        .expect("test query budget should be valid");
        let execution = budget
            .begin_query_with_token(token)
            .expect("test query should be admitted");
        (budget, execution)
    }

    async fn spawn_stalled_repair_peer() -> (
        String,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let endpoint = listener
            .local_addr()
            .expect("listener should have local addr")
            .to_string();
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection expected");
            let mut read_buffer = Vec::new();
            let request = read_http_request(&mut stream, &mut read_buffer)
                .await
                .expect("request should parse");
            assert_eq!(request.path_without_query(), "/internal/v1/repair_backfill");
            let _ = seen_tx.send(());
            let _ = release_rx.await;
        });
        (endpoint, seen_rx, release_tx, server)
    }

    struct SerializationCounterRequest<'a> {
        calls: &'a AtomicUsize,
        payload: &'a str,
    }

    impl Serialize for SerializationCounterRequest<'_> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.payload.serialize(serializer)
        }
    }

    #[test]
    fn accounted_rpc_request_serialization_exact_memory_limit_succeeds() {
        let expected = serde_json::to_vec("bounded request").expect("fixture should serialize");
        let encoded_bytes = u64::try_from(expected.len()).expect("fixture length should fit");
        let (budget, execution) = query_execution_with_memory_limit(encoded_bytes);
        let calls = AtomicUsize::new(0);
        let request = SerializationCounterRequest {
            calls: &calls,
            payload: "bounded request",
        };

        let (body, reservation) = serialize_json_request_accounted(&request, &execution)
            .expect("the exact encoded-byte limit should admit the body");

        assert_eq!(body, expected);
        assert_eq!(body.len(), body.capacity());
        assert_eq!(reservation.bytes(), encoded_bytes);
        assert_eq!(execution.snapshot().memory_reserved_bytes, encoded_bytes);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(reservation);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn accounted_rpc_request_serialization_one_byte_under_rejects_before_body_fill() {
        let encoded_bytes = u64::try_from(
            serde_json::to_vec("bounded request")
                .expect("fixture should serialize")
                .len(),
        )
        .expect("fixture length should fit");
        let limit = encoded_bytes
            .checked_sub(1)
            .expect("fixture must encode to more than one byte");
        let (budget, execution) = query_execution_with_memory_limit(limit);
        let calls = AtomicUsize::new(0);
        let request = SerializationCounterRequest {
            calls: &calls,
            payload: "bounded request",
        };

        let error = serialize_json_request_accounted(&request, &execution)
            .expect_err("one byte under the encoded body must be rejected");

        match error {
            RpcError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(exceeded),
            } => {
                assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes);
                assert_eq!(exceeded.limit, limit);
                assert_eq!(exceeded.current, 0);
                assert_eq!(exceeded.requested, encoded_bytes);
            }
            other => panic!("expected query-memory rejection, got {other:?}"),
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "rejection after the allocation-free preflight must skip body serialization"
        );
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn accounted_rpc_header_is_preflighted_at_the_exact_combined_limit() {
        let client = RpcClient::new(RpcClientConfig {
            internal_auth_token: "bounded-token".to_string(),
            local_node_id: "node-a".to_string(),
            ..RpcClientConfig::default()
        });
        let endpoint = "127.0.0.1:19091";
        let path = "/internal/v1/select_series";
        let request = "bounded request";

        let (calibration_budget, calibration_execution) =
            query_execution_with_memory_limit(64 * 1024);
        let (calibration_body, mut calibration_reservation) =
            serialize_json_request_accounted(&request, &calibration_execution)
                .expect("calibration body should serialize");
        let calibration_header = client
            .build_http_request_accounted(
                endpoint,
                path,
                calibration_body.len(),
                &mut calibration_reservation,
            )
            .expect("calibration header should serialize");
        assert_eq!(
            calibration_header,
            client.build_http_request(endpoint, path, &calibration_body)
        );
        assert_eq!(calibration_header.len(), calibration_header.capacity());
        let exact_limit = calibration_reservation.bytes();
        drop(calibration_reservation);
        drop(calibration_execution);
        assert_eq!(
            calibration_budget.snapshot().shared_reserved_memory_bytes,
            0
        );

        let (exact_budget, exact_execution) = query_execution_with_memory_limit(exact_limit);
        let (exact_body, mut exact_reservation) =
            serialize_json_request_accounted(&request, &exact_execution)
                .expect("the body should fit within the combined exact limit");
        let exact_header = client
            .build_http_request_accounted(endpoint, path, exact_body.len(), &mut exact_reservation)
            .expect("the exact combined request limit should pass");
        assert_eq!(exact_header, calibration_header);
        assert_eq!(exact_reservation.bytes(), exact_limit);
        drop(exact_reservation);
        drop(exact_execution);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let one_under_limit = exact_limit
            .checked_sub(1)
            .expect("combined request must use at least one byte");
        let (one_under_budget, one_under_execution) =
            query_execution_with_memory_limit(one_under_limit);
        let (one_under_body, mut one_under_reservation) =
            serialize_json_request_accounted(&request, &one_under_execution)
                .expect("the body alone should fit");
        let error = client
            .build_http_request_accounted(
                endpoint,
                path,
                one_under_body.len(),
                &mut one_under_reservation,
            )
            .expect_err("one byte below the combined request must reject before header allocation");
        assert!(matches!(
            error,
            RpcError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(ref exceeded),
            } if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
                && exceeded.limit == one_under_limit
        ));
        assert_eq!(
            one_under_execution.snapshot().memory_reserved_bytes,
            one_under_reservation.bytes()
        );
        drop(one_under_reservation);
        drop(one_under_execution);
        assert_eq!(one_under_budget.snapshot().shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn ingest_rows_response_decodes_legacy_count_without_claiming_canonical_outcome() {
        let response: InternalIngestRowsResponse = serde_json::from_str(r#"{"inserted_rows":2}"#)
            .expect("legacy response should remain readable");

        assert_eq!(response.inserted_rows, 2);
        assert_eq!(response.write_result, None);
    }

    #[test]
    fn select_batch_wire_accounting_is_optional_for_legacy_peers() {
        let request: InternalSelectBatchRequest =
            serde_json::from_str(r#"{"ring_version":1,"selectors":[],"start":10,"end":20}"#)
                .expect("legacy request should remain readable");
        assert_eq!(request.query_limits, None);
        let request_json = serde_json::to_value(&request).expect("legacy request should serialize");
        assert!(request_json.get("query_limits").is_none());

        let response: InternalSelectBatchResponse = serde_json::from_str(r#"{"series":[]}"#)
            .expect("legacy response should remain readable");
        assert!(response.accounting.is_none());
        let response_json =
            serde_json::to_value(&response).expect("legacy response should serialize");
        assert!(response_json.get("accounting").is_none());
    }

    #[test]
    fn select_batch_wire_round_trips_limits_and_execution_accounting() {
        let request = InternalSelectBatchRequest {
            ring_version: 7,
            selectors: Vec::new(),
            start: 10,
            end: 20,
            query_limits: Some(QueryWorkLimits {
                max_series_matched: Some(3),
                max_samples_scanned: Some(5),
                max_samples_returned: Some(4),
                max_returned_bytes: Some(1_024),
                max_intermediate_vector_size: Some(3),
                ..QueryWorkLimits::default()
            }),
        };
        let request_json = serde_json::to_vec(&request).expect("bounded request should serialize");
        let decoded_request: InternalSelectBatchRequest =
            serde_json::from_slice(&request_json).expect("bounded request should deserialize");
        assert_eq!(decoded_request.query_limits, request.query_limits);

        let snapshot = QueryExecutionSnapshot {
            series_matched: 2,
            samples_scanned: 5,
            samples_returned: 4,
            returned_bytes: 512,
            intermediate_vector_size: 3,
            ..QueryExecutionSnapshot::default()
        };
        let response = InternalSelectBatchResponse {
            series: Vec::new(),
            accounting: Some(InternalSelectBatchAccounting {
                execution: snapshot,
                matched_selectors: Some(vec![true, false]),
            }),
        };
        let response_json =
            serde_json::to_vec(&response).expect("accounted response should serialize");
        let decoded_response: InternalSelectBatchResponse =
            serde_json::from_slice(&response_json).expect("accounted response should deserialize");
        let accounting = decoded_response
            .accounting
            .expect("accounting should be retained");
        assert_eq!(accounting.execution, snapshot);
        assert_eq!(accounting.matched_selectors, Some(vec![true, false]));
    }

    #[test]
    fn rpc_error_diagnostics_are_bounded_and_strip_control_characters() {
        let diagnostic = sanitize_rpc_error_diagnostic(&format!(
            "peer\r\nerror\0{}",
            "é".repeat(MAX_RPC_ERROR_DIAGNOSTIC_BYTES)
        ));

        assert!(diagnostic.len() <= MAX_RPC_ERROR_DIAGNOSTIC_BYTES);
        assert!(!diagnostic.chars().any(char::is_control));
        assert!(diagnostic.starts_with("peer error "));
    }

    #[test]
    fn rpc_error_codes_are_optional_bounded_and_strip_control_characters() {
        let code = sanitize_rpc_error_code(&format!(
            "write_disk_quota_exceeded\r\n{}",
            "x".repeat(MAX_RPC_ERROR_DIAGNOSTIC_BYTES)
        ))
        .expect("non-empty error code should be retained");

        assert!(code.len() <= MAX_RPC_ERROR_DIAGNOSTIC_BYTES);
        assert!(!code.chars().any(char::is_control));
        assert!(code.starts_with("write_disk_quota_exceeded "));
        assert_eq!(sanitize_rpc_error_code(" \r\n\0\t"), None);
    }

    #[test]
    fn parse_http_response_rejects_duplicate_content_length() {
        let err = parse_http_response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nContent-Length: 5\r\n\r\ntest!",
        )
        .expect_err("duplicate content-length should be rejected");

        assert_eq!(err, "duplicate response content-length header");
    }

    #[test]
    fn parse_http_response_rejects_transfer_encoding() {
        let err = parse_http_response(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ntest\r\n0\r\n\r\n",
        )
        .expect_err("chunked response should be rejected");

        assert_eq!(err, "response transfer-encoding is not supported");
    }

    #[test]
    fn parse_http_response_rejects_unsupported_version() {
        let err = parse_http_response(b"HTTP/2 200 OK\r\nContent-Length: 0\r\n\r\n")
            .expect_err("unsupported version should be rejected");

        assert_eq!(err, "unsupported HTTP version");
    }

    #[tokio::test]
    async fn bounded_response_reader_accepts_the_limit_and_rejects_the_next_byte() {
        let (mut writer, mut reader) = tokio::io::duplex(32);
        writer
            .write_all(b"12345678")
            .await
            .expect("test response should write");
        drop(writer);
        let response = read_response_bounded(&mut reader, 8)
            .await
            .expect("response at the limit should be accepted");
        assert_eq!(response, b"12345678");

        let (mut writer, mut reader) = tokio::io::duplex(32);
        writer
            .write_all(b"123456789")
            .await
            .expect("test response should write");
        drop(writer);
        let err = read_response_bounded(&mut reader, 8)
            .await
            .expect_err("response beyond the limit should be rejected");
        assert!(matches!(err, BoundedResponseReadError::LimitExceeded));
    }

    #[test]
    fn oversized_rpc_response_errors_are_not_retried() {
        let err = RpcError::ResponseTooLarge {
            endpoint: "node-b:9201".to_string(),
            path: "/internal/v1/select".to_string(),
            limit: 8,
        };

        assert!(!err.retryable());
    }

    fn build_test_mtls_server_acceptor(
        ca_cert_path: &Path,
        cert_path: &Path,
        key_path: &Path,
    ) -> TlsAcceptor {
        let ca_file = std::fs::File::open(ca_cert_path).expect("CA cert should open");
        let mut ca_reader = BufReader::new(ca_file);
        let ca_certs: Vec<_> = rustls_pemfile::certs(&mut ca_reader)
            .collect::<Result<_, _>>()
            .expect("CA certs should parse");
        assert!(!ca_certs.is_empty(), "CA cert fixture should not be empty");
        let mut roots = rustls::RootCertStore::empty();
        for cert in ca_certs {
            roots.add(cert).expect("CA cert should be loadable");
        }

        let cert_file = std::fs::File::open(cert_path).expect("server cert should open");
        let mut cert_reader = BufReader::new(cert_file);
        let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<_, _>>()
            .expect("server cert should parse");
        assert!(!certs.is_empty(), "server cert fixture should not be empty");

        let key_file = std::fs::File::open(key_path).expect("server key should open");
        let mut key_reader = BufReader::new(key_file);
        let key = rustls_pemfile::private_key(&mut key_reader)
            .expect("server key should parse")
            .expect("server key fixture should contain a key");

        ensure_rustls_crypto_provider();
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .expect("server client verifier should build");
        let tls_config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .expect("server TLS config should build");
        TlsAcceptor::from(Arc::new(tls_config))
    }

    #[test]
    fn shared_internal_token_is_deterministic_and_seed_order_invariant() {
        let cfg_a = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![
                "node-b@127.0.0.1:9302".to_string(),
                "node-c@127.0.0.1:9303".to_string(),
            ],
            ..ClusterConfig::default()
        };
        let cfg_b = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![
                "node-c@127.0.0.1:9303".to_string(),
                "node-b@127.0.0.1:9302".to_string(),
            ],
            ..ClusterConfig::default()
        };

        let membership_a = MembershipView::from_config(&cfg_a).expect("membership should build");
        let membership_b = MembershipView::from_config(&cfg_b).expect("membership should build");

        assert_eq!(
            derive_shared_internal_token(&membership_a),
            derive_shared_internal_token(&membership_b)
        );
    }

    #[test]
    fn internal_api_config_requires_explicit_token_without_mtls() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["127.0.0.1:9302".to_string()],
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&cfg).expect("membership should build");
        let err = InternalApiConfig::from_membership(&membership, false, None)
            .expect_err("plaintext internal RPC should require explicit auth token");
        assert!(err.contains("--cluster-internal-auth-token"));
    }

    #[test]
    fn internal_api_config_allows_mtls_without_explicit_token() {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["127.0.0.1:9302".to_string()],
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&cfg).expect("membership should build");
        let internal_api = InternalApiConfig::from_membership(&membership, true, None)
            .expect("mTLS mode should allow derived fallback token");
        assert!(!internal_api.auth_token.is_empty());
        assert!(internal_api.require_mtls);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rpc_client_succeeds_under_nominal_conditions() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection expected");
            let mut read_buffer = Vec::new();
            let request = read_http_request(&mut stream, &mut read_buffer)
                .await
                .expect("request should parse");

            assert_eq!(request.method, "POST");
            assert_eq!(request.path_without_query(), "/internal/v1/list_metrics");
            assert_eq!(
                request.header(INTERNAL_RPC_AUTH_HEADER),
                Some("cluster-shared-token")
            );
            assert_eq!(
                request.header(INTERNAL_RPC_VERSION_HEADER),
                Some(INTERNAL_RPC_PROTOCOL_VERSION)
            );
            let expected_capabilities =
                normalize_capabilities(default_cluster_capabilities().into_iter()).join(",");
            assert_eq!(
                request.header(INTERNAL_RPC_CAPABILITIES_HEADER),
                Some(expected_capabilities.as_str())
            );
            assert_eq!(request.header(INTERNAL_RPC_NODE_ID_HEADER), Some("node-a"));

            let req: InternalListMetricsRequest =
                serde_json::from_slice(&request.body).expect("request body should decode");
            assert!(req.query_limits.is_none());

            let response = HttpResponse::new(
                200,
                serde_json::to_vec(&InternalListMetricsResponse {
                    series: Vec::new(),
                    accounting: None,
                })
                .expect("response serialization should succeed"),
            )
            .with_header("Content-Type", "application/json");
            write_http_response(&mut stream, &response)
                .await
                .expect("response write should succeed");
        });

        let client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(500),
            max_retries: 0,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "cluster-shared-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: CompatibilityProfile::default(),
            internal_mtls: None,
        });

        let response = client
            .list_metrics(&addr.to_string())
            .await
            .expect("RPC call should succeed");
        assert!(response.series.is_empty());
        assert!(response.accounting.is_none());

        server.await.expect("server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn digest_window_accounted_round_trip_has_exact_transport_memory_boundaries() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let query_limits = tsink::ResourceLimits::test().query.per_query;
        let request = InternalDigestWindowRequest {
            ring_version: 7,
            shard: 3,
            window_start: 10,
            window_end: 20,
            query_limits: Some(query_limits),
        };
        let remote_accounting = QueryExecutionSnapshot {
            memory_reserved_bytes: 4_096,
            series_matched: 2,
            samples_scanned: 5,
            samples_returned: 0,
            returned_bytes: 56,
            pattern_expansion: 2,
            steps: 0,
            intermediate_vector_size: 3,
        };
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().await.expect("connection expected");
                let mut read_buffer = Vec::new();
                let http_request = read_http_request(&mut stream, &mut read_buffer)
                    .await
                    .expect("request should parse");
                assert_eq!(
                    http_request.path_without_query(),
                    "/internal/v1/digest_window"
                );
                let decoded: InternalDigestWindowRequest =
                    serde_json::from_slice(&http_request.body).expect("request should decode");
                assert_eq!(decoded.query_limits, Some(query_limits));
                let response = HttpResponse::new(
                    200,
                    serde_json::to_vec(&InternalAccountedDigestWindowResponse {
                        digest: InternalDigestWindowResponse {
                            shard: decoded.shard,
                            ring_version: decoded.ring_version,
                            window_start: decoded.window_start,
                            window_end: decoded.window_end,
                            series_count: 2,
                            point_count: 5,
                            fingerprint: 9,
                        },
                        accounting: remote_accounting,
                    })
                    .expect("response should serialize"),
                )
                .with_header("Content-Type", "application/json");
                write_http_response(&mut stream, &response)
                    .await
                    .expect("response should write");
            }
        });

        let client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(500),
            max_retries: 0,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "cluster-shared-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: CompatibilityProfile::default(),
            internal_mtls: None,
        });
        let endpoint = addr.to_string();
        let (calibration_budget, calibration_execution) =
            query_execution_with_memory_limit(2 * 1024 * 1024);
        let calibration = client
            .digest_window_accounted(&endpoint, &request, &calibration_execution)
            .await
            .expect("bounded digest RPC should succeed");
        assert_eq!(calibration.response.digest.shard, request.shard);
        assert_eq!(calibration.response.digest.point_count, 5);
        assert_eq!(calibration.response.accounting, remote_accounting);
        let exact_memory = calibration.reservation.bytes();
        assert!(exact_memory > 1);
        assert_eq!(
            calibration_execution.snapshot().memory_reserved_bytes,
            exact_memory
        );
        drop(calibration.reservation);
        assert_eq!(calibration_execution.snapshot().memory_reserved_bytes, 0);
        drop(calibration_execution);
        assert_eq!(
            calibration_budget.snapshot().shared_reserved_memory_bytes,
            0
        );

        let (exact_budget, exact_execution) = query_execution_with_memory_limit(exact_memory);
        let exact = client
            .digest_window_accounted(&endpoint, &request, &exact_execution)
            .await
            .expect("the exact digest transport-memory limit should succeed");
        assert_eq!(exact.reservation.bytes(), exact_memory);
        drop(exact.reservation);
        assert_eq!(exact_execution.snapshot().memory_reserved_bytes, 0);
        drop(exact_execution);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let one_under_memory = exact_memory - 1;
        let (one_under_budget, one_under_execution) =
            query_execution_with_memory_limit(one_under_memory);
        let error = client
            .digest_window_accounted(&endpoint, &request, &one_under_execution)
            .await
            .expect_err("one byte below digest transport memory must reject");
        assert!(matches!(
            error,
            RpcError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(ref exceeded),
            } if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
                && exceeded.limit == one_under_memory
        ));
        assert_eq!(one_under_execution.snapshot().memory_reserved_bytes, 0);
        drop(one_under_execution);
        let snapshot = one_under_budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
        server.await.expect("server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repair_backfill_accounted_round_trip_has_exact_transport_memory_boundaries() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let query_limits = tsink::ResourceLimits::test().query.per_query;
        let request = InternalRepairBackfillRequest {
            ring_version: 7,
            shard: 3,
            window_start: 10,
            window_end: 20,
            max_series: Some(2),
            max_rows: Some(5),
            row_offset: None,
            query_limits: Some(query_limits),
        };
        let remote_accounting = QueryExecutionSnapshot {
            memory_reserved_bytes: 4_096,
            series_matched: 2,
            samples_scanned: 5,
            samples_returned: 1,
            returned_bytes: 256,
            pattern_expansion: 2,
            steps: 0,
            intermediate_vector_size: 2,
        };
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().await.expect("connection expected");
                let mut read_buffer = Vec::new();
                let http_request = read_http_request(&mut stream, &mut read_buffer)
                    .await
                    .expect("request should parse");
                assert_eq!(
                    http_request.path_without_query(),
                    "/internal/v1/repair_backfill"
                );
                let decoded: InternalRepairBackfillRequest =
                    serde_json::from_slice(&http_request.body).expect("request should decode");
                assert_eq!(decoded.query_limits, Some(query_limits));
                let response = HttpResponse::new(
                    200,
                    serde_json::to_vec(&InternalAccountedRepairBackfillResponse {
                        backfill: InternalRepairBackfillResponse {
                            shard: decoded.shard,
                            ring_version: decoded.ring_version,
                            window_start: decoded.window_start,
                            window_end: decoded.window_end,
                            series_scanned: 1,
                            rows_scanned: 1,
                            truncated: false,
                            next_row_offset: None,
                            rows: vec![InternalRow {
                                metric: "bounded_repair_metric".to_string(),
                                labels: vec![Label::new("host", "a")],
                                data_point: DataPoint::new(12, 1.0),
                            }],
                        },
                        accounting: remote_accounting,
                    })
                    .expect("response should serialize"),
                )
                .with_header("Content-Type", "application/json");
                write_http_response(&mut stream, &response)
                    .await
                    .expect("response should write");
            }
        });

        let client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(500),
            max_retries: 0,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "cluster-shared-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: CompatibilityProfile::default(),
            internal_mtls: None,
        });
        let endpoint = addr.to_string();
        let (calibration_budget, calibration_execution) =
            query_execution_with_memory_limit(2 * 1024 * 1024);
        let calibration = client
            .repair_backfill_accounted(&endpoint, &request, &calibration_execution)
            .await
            .expect("bounded repair RPC should succeed");
        assert_eq!(calibration.response.backfill.shard, request.shard);
        assert_eq!(calibration.response.backfill.rows.len(), 1);
        assert_eq!(calibration.response.accounting, remote_accounting);
        let exact_memory = calibration.reservation.bytes();
        assert!(exact_memory > 1);
        assert_eq!(
            calibration_execution.snapshot().memory_reserved_bytes,
            exact_memory
        );
        drop(calibration.reservation);
        assert_eq!(calibration_execution.snapshot().memory_reserved_bytes, 0);
        drop(calibration_execution);
        assert_eq!(
            calibration_budget.snapshot().shared_reserved_memory_bytes,
            0
        );

        let (exact_budget, exact_execution) = query_execution_with_memory_limit(exact_memory);
        let exact = client
            .repair_backfill_accounted(&endpoint, &request, &exact_execution)
            .await
            .expect("the exact repair transport-memory limit should succeed");
        assert_eq!(exact.reservation.bytes(), exact_memory);
        drop(exact.reservation);
        assert_eq!(exact_execution.snapshot().memory_reserved_bytes, 0);
        drop(exact_execution);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let one_under_memory = exact_memory - 1;
        let (one_under_budget, one_under_execution) =
            query_execution_with_memory_limit(one_under_memory);
        let error = client
            .repair_backfill_accounted(&endpoint, &request, &one_under_execution)
            .await
            .expect_err("one byte below repair transport memory must reject");
        assert!(matches!(
            error,
            RpcError::QueryBudget {
                error: QueryBudgetError::LimitExceeded(ref exceeded),
            } if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
                && exceeded.limit == one_under_memory
        ));
        assert_eq!(one_under_execution.snapshot().memory_reserved_bytes, 0);
        drop(one_under_execution);
        let snapshot = one_under_budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
        server.await.expect("server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repair_backfill_accounted_stalled_peer_obeys_control_and_drop_releases_memory() {
        let request = InternalRepairBackfillRequest {
            ring_version: 7,
            shard: 3,
            window_start: 10,
            window_end: 20,
            max_series: Some(2),
            max_rows: Some(5),
            row_offset: None,
            query_limits: Some(tsink::ResourceLimits::test().query.per_query),
        };
        let client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_secs(5),
            max_retries: 0,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "cluster-shared-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: CompatibilityProfile::default(),
            internal_mtls: None,
        });

        let (deadline_endpoint, deadline_seen, deadline_release, deadline_server) =
            spawn_stalled_repair_peer().await;
        let deadline_token =
            tsink::QueryCancellationToken::new().with_timeout(Duration::from_millis(250));
        let (deadline_budget, deadline_execution) =
            query_execution_with_memory_limit_and_token(2 * 1024 * 1024, deadline_token);
        let deadline_client = client.clone();
        let deadline_request = request.clone();
        let deadline_worker_execution = deadline_execution.clone();
        let deadline_call = tokio::spawn(async move {
            deadline_client
                .repair_backfill_accounted(
                    &deadline_endpoint,
                    &deadline_request,
                    &deadline_worker_execution,
                )
                .await
        });
        deadline_seen
            .await
            .expect("deadline request should reach the stalled peer");
        let error = tokio::time::timeout(Duration::from_secs(2), deadline_call)
            .await
            .expect("execution deadline must beat the generic RPC timeout")
            .expect("deadline task should complete")
            .expect_err("stalled RPC must hit the execution deadline");
        assert!(matches!(
            error,
            RpcError::QueryBudget {
                error: QueryBudgetError::DeadlineExceeded
            }
        ));
        assert_eq!(deadline_execution.snapshot().memory_reserved_bytes, 0);
        drop(deadline_execution);
        let snapshot = deadline_budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.deadline_exceeded_total, 1);
        let _ = deadline_release.send(());
        deadline_server
            .await
            .expect("deadline server should stop after release");

        let (cancel_endpoint, cancel_seen, cancel_release, cancel_server) =
            spawn_stalled_repair_peer().await;
        let cancel_token = tsink::QueryCancellationToken::new();
        let (cancel_budget, cancel_execution) =
            query_execution_with_memory_limit_and_token(2 * 1024 * 1024, cancel_token.clone());
        let cancel_client = client.clone();
        let cancel_request = request.clone();
        let cancel_worker_execution = cancel_execution.clone();
        let cancel_call = tokio::spawn(async move {
            cancel_client
                .repair_backfill_accounted(
                    &cancel_endpoint,
                    &cancel_request,
                    &cancel_worker_execution,
                )
                .await
        });
        cancel_seen
            .await
            .expect("cancellation request should reach the stalled peer");
        cancel_token.cancel();
        let error = tokio::time::timeout(Duration::from_secs(1), cancel_call)
            .await
            .expect("execution cancellation must beat the generic RPC timeout")
            .expect("cancellation task should complete")
            .expect_err("stalled RPC must observe execution cancellation");
        assert!(matches!(
            error,
            RpcError::QueryBudget {
                error: QueryBudgetError::Cancelled
            }
        ));
        assert_eq!(cancel_execution.snapshot().memory_reserved_bytes, 0);
        drop(cancel_execution);
        let snapshot = cancel_budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.cancellations_total, 1);
        let _ = cancel_release.send(());
        cancel_server
            .await
            .expect("cancellation server should stop after release");

        let (drop_endpoint, drop_seen, drop_release, drop_server) =
            spawn_stalled_repair_peer().await;
        let (drop_budget, drop_execution) = query_execution_with_memory_limit(2 * 1024 * 1024);
        let drop_client = client;
        let drop_request = request;
        let drop_worker_execution = drop_execution.clone();
        let drop_call = tokio::spawn(async move {
            drop_client
                .repair_backfill_accounted(&drop_endpoint, &drop_request, &drop_worker_execution)
                .await
        });
        drop_seen
            .await
            .expect("dropped request should reach the stalled peer");
        drop_call.abort();
        assert!(drop_call
            .await
            .expect_err("aborted repair RPC should not return")
            .is_cancelled());
        assert_eq!(drop_execution.snapshot().memory_reserved_bytes, 0);
        drop(drop_execution);
        let snapshot = drop_budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
        let _ = drop_release.send(());
        drop_server
            .await
            .expect("dropped-call server should stop after release");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn budgeted_restore_rpc_never_falls_back_to_legacy_restore_path() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection expected");
            let mut read_buffer = Vec::new();
            let request = read_http_request(&mut stream, &mut read_buffer)
                .await
                .expect("request should parse");
            assert_eq!(
                request.path_without_query(),
                "/internal/v1/restore_data_budgeted"
            );
            let payload: InternalDataRestoreRequest =
                serde_json::from_slice(&request.body).expect("request body should decode");
            assert_eq!(payload.snapshot_path, "/snapshots/node-a");
            assert_eq!(payload.data_path, "/offline-restores/node-a");
            write_http_response(&mut stream, &text_response(404, "not found"))
                .await
                .expect("response write should succeed");
        });

        let client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(500),
            max_retries: 3,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "cluster-shared-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: CompatibilityProfile::default(),
            internal_mtls: None,
        });
        let err = client
            .data_restore_budgeted(
                &addr.to_string(),
                &InternalDataRestoreRequest {
                    snapshot_path: "/snapshots/node-a".to_string(),
                    data_path: "/offline-restores/node-a".to_string(),
                },
            )
            .await
            .expect_err("an old peer without the new route must be rejected");
        match err {
            RpcError::HttpStatus { path, status, .. } => {
                assert_eq!(path, "/internal/v1/restore_data_budgeted");
                assert_eq!(status, 404);
            }
            other => panic!("expected HTTP status error, got {other:?}"),
        }
        server.await.expect("server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rpc_client_preserves_structured_disk_quota_error_code() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection expected");
            let mut read_buffer = Vec::new();
            let _ = read_http_request(&mut stream, &mut read_buffer)
                .await
                .expect("request should parse");

            let response = internal_error_response(
                413,
                "write_disk_quota_exceeded",
                "local disk quota exceeded\r\nreserved bytes unavailable",
                false,
            );
            write_http_response(&mut stream, &response)
                .await
                .expect("response write should succeed");
        });

        let client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(500),
            max_retries: 0,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "cluster-shared-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: CompatibilityProfile::default(),
            internal_mtls: None,
        });

        let err = client
            .list_metrics(&addr.to_string())
            .await
            .expect_err("structured disk quota response should fail the RPC");
        match err {
            RpcError::HttpStatus {
                status,
                error_code,
                message,
                retryable,
                ..
            } => {
                assert_eq!(status, 413);
                assert_eq!(error_code.as_deref(), Some("write_disk_quota_exceeded"));
                assert_eq!(
                    message,
                    "local disk quota exceeded reserved bytes unavailable"
                );
                assert!(!retryable);
            }
            other => panic!("expected structured HTTP status error, got {other:?}"),
        }

        server.await.expect("server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rpc_client_preserves_structured_select_batch_query_limit_error() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection expected");
            let mut read_buffer = Vec::new();
            let request = read_http_request(&mut stream, &mut read_buffer)
                .await
                .expect("request should parse");
            assert_eq!(request.path_without_query(), "/internal/v1/select_batch");
            let payload: InternalSelectBatchRequest =
                serde_json::from_slice(&request.body).expect("request should decode");
            assert_eq!(
                payload
                    .query_limits
                    .expect("limits should be present")
                    .max_samples_returned,
                Some(1)
            );

            let response = internal_error_response(
                413,
                "query_limit_samples_returned",
                "query limit 'samples_returned' exceeded",
                false,
            );
            write_http_response(&mut stream, &response)
                .await
                .expect("response write should succeed");
        });

        let client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(500),
            max_retries: 0,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "cluster-shared-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: CompatibilityProfile::default(),
            internal_mtls: None,
        });
        let err = client
            .select_batch(
                &addr.to_string(),
                &InternalSelectBatchRequest {
                    ring_version: DEFAULT_INTERNAL_RING_VERSION,
                    selectors: Vec::new(),
                    start: 10,
                    end: 20,
                    query_limits: Some(QueryWorkLimits {
                        max_samples_returned: Some(1),
                        ..QueryWorkLimits::default()
                    }),
                },
            )
            .await
            .expect_err("query limit response should fail the RPC");
        match err {
            RpcError::HttpStatus {
                status,
                error_code,
                retryable,
                ..
            } => {
                assert_eq!(status, 413);
                assert_eq!(error_code.as_deref(), Some("query_limit_samples_returned"));
                assert!(!retryable);
            }
            other => panic!("expected structured HTTP status error, got {other:?}"),
        }

        server.await.expect("server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rpc_client_enforces_timeout_and_retry_limit() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_server = Arc::clone(&attempts);

        let server = tokio::spawn(async move {
            let mut connection_tasks = Vec::new();
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.expect("connection expected");
                attempts_server.fetch_add(1, Ordering::Relaxed);
                connection_tasks.push(tokio::spawn(async move {
                    let mut read_buffer = Vec::new();
                    let _ = read_http_request(&mut stream, &mut read_buffer).await;
                    tokio::time::sleep(Duration::from_millis(120)).await;
                }));
            }

            for task in connection_tasks {
                let _ = task.await;
            }
        });

        let client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(25),
            max_retries: 1,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "cluster-shared-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: CompatibilityProfile::default(),
            internal_mtls: None,
        });

        let err = client
            .list_metrics(&addr.to_string())
            .await
            .expect_err("RPC call should time out");
        assert!(matches!(err, RpcError::Timeout { .. }));
        assert_eq!(attempts.load(Ordering::Relaxed), 2);

        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server task should complete")
            .expect("server task should not panic");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rpc_client_reports_protocol_mismatch_clearly() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection expected");
            let mut read_buffer = Vec::new();
            let _ = read_http_request(&mut stream, &mut read_buffer)
                .await
                .expect("request should parse");

            let response = protocol_mismatch_response(INTERNAL_RPC_PROTOCOL_VERSION, Some("99"));
            write_http_response(&mut stream, &response)
                .await
                .expect("response write should succeed");
        });

        let client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(500),
            max_retries: 0,
            protocol_version: "99".to_string(),
            internal_auth_token: "cluster-shared-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: CompatibilityProfile::default(),
            internal_mtls: None,
        });

        let err = client
            .list_metrics(&addr.to_string())
            .await
            .expect_err("RPC call should fail on version mismatch");
        assert!(matches!(
            err,
            RpcError::ProtocolVersionMismatch {
                expected,
                received,
                ..
            } if expected == INTERNAL_RPC_PROTOCOL_VERSION && received.as_deref() == Some("99")
        ));

        server.await.expect("server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rpc_client_reports_compatibility_rejection_clearly() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection expected");
            let mut read_buffer = Vec::new();
            let _ = read_http_request(&mut stream, &mut read_buffer)
                .await
                .expect("request should parse");

            let response = peer_capability_mismatch_response(
                vec![CLUSTER_CAPABILITY_CONTROL_SNAPSHOT_RPC_V1.to_string()],
                vec![CLUSTER_CAPABILITY_RPC_V1.to_string()],
                vec![CLUSTER_CAPABILITY_CONTROL_SNAPSHOT_RPC_V1.to_string()],
            );
            write_http_response(&mut stream, &response)
                .await
                .expect("response write should succeed");
        });

        let client = RpcClient::new(
            RpcClientConfig {
                timeout: Duration::from_millis(500),
                max_retries: 0,
                protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
                internal_auth_token: "cluster-shared-token".to_string(),
                internal_auth_runtime: None,
                local_node_id: "node-a".to_string(),
                ..RpcClientConfig::default()
            }
            .with_compatibility(CompatibilityProfile::default()),
        );

        let err = client
            .list_metrics(&addr.to_string())
            .await
            .expect_err("RPC call should fail on compatibility rejection");
        assert!(matches!(
            err,
            RpcError::CompatibilityRejected {
                missing_capabilities,
                ..
            } if missing_capabilities == vec![CLUSTER_CAPABILITY_CONTROL_SNAPSHOT_RPC_V1.to_string()]
        ));

        server.await.expect("server task should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rpc_client_reloads_internal_mtls_client_cert_between_requests() {
        let temp_dir = TempDir::new().expect("tempdir should create");
        let ca_cert_path = temp_dir.path().join("ca-cert.pem");
        let server_cert_path = temp_dir.path().join("server-cert.pem");
        let server_key_path = temp_dir.path().join("server-key.pem");
        let client_cert_path = temp_dir.path().join("client-cert.pem");
        let client_key_path = temp_dir.path().join("client-key.pem");

        write_test_file(&ca_cert_path, TEST_INTERNAL_MTLS_CA_CERT_PEM);
        write_test_file(&server_cert_path, TEST_INTERNAL_MTLS_SERVER_CERT_PEM);
        write_test_file(&server_key_path, TEST_INTERNAL_MTLS_SERVER_KEY_PEM);
        write_test_file(&client_cert_path, TEST_INTERNAL_MTLS_CLIENT_A_CERT_PEM);
        write_test_file(&client_key_path, TEST_INTERNAL_MTLS_CLIENT_A_KEY_PEM);

        let listener = TcpListener::bind("localhost:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let endpoint = format!("localhost:{}", addr.port());
        let acceptor =
            build_test_mtls_server_acceptor(&ca_cert_path, &server_cert_path, &server_key_path);

        let server = tokio::spawn(async move {
            let mut observed_peer_certs = Vec::new();
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.expect("connection expected");
                let mut tls_stream = acceptor
                    .accept(stream)
                    .await
                    .expect("TLS handshake should succeed");

                let (_, connection) = tls_stream.get_ref();
                let peer_certs = connection
                    .peer_certificates()
                    .expect("client cert should be present");
                observed_peer_certs.push(
                    peer_certs
                        .first()
                        .expect("client cert chain should have at least one cert")
                        .as_ref()
                        .to_vec(),
                );

                let mut read_buffer = Vec::new();
                let request = read_http_request(&mut tls_stream, &mut read_buffer)
                    .await
                    .expect("request should parse");
                assert_eq!(
                    request.header(INTERNAL_RPC_NODE_ID_HEADER),
                    Some("node-a"),
                    "rotated mTLS requests should preserve claimed local node id",
                );

                let response = HttpResponse::new(
                    200,
                    serde_json::to_vec(&InternalListMetricsResponse {
                        series: Vec::new(),
                        accounting: None,
                    })
                    .expect("response serialization should succeed"),
                )
                .with_header("Content-Type", "application/json");
                write_http_response(&mut tls_stream, &response)
                    .await
                    .expect("response write should succeed");
                tokio::io::AsyncWriteExt::shutdown(&mut tls_stream)
                    .await
                    .expect("TLS stream shutdown should succeed");
            }
            observed_peer_certs
        });

        let client = RpcClient::new(RpcClientConfig {
            timeout: Duration::from_millis(500),
            max_retries: 0,
            protocol_version: INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            internal_auth_token: "cluster-shared-token".to_string(),
            internal_auth_runtime: None,
            local_node_id: "node-a".to_string(),
            compatibility: CompatibilityProfile::default(),
            internal_mtls: Some(RpcClientInternalMtlsConfig {
                ca_cert: ca_cert_path.clone(),
                cert: client_cert_path.clone(),
                key: client_key_path.clone(),
            }),
        });

        client
            .list_metrics(&endpoint)
            .await
            .expect("initial mTLS RPC call should succeed");

        write_test_file(&client_cert_path, TEST_INTERNAL_MTLS_CLIENT_B_CERT_PEM);
        write_test_file(&client_key_path, TEST_INTERNAL_MTLS_CLIENT_B_KEY_PEM);

        client
            .list_metrics(&endpoint)
            .await
            .expect("rotated mTLS RPC call should succeed");

        let observed_peer_certs = server.await.expect("server task should complete");
        assert_eq!(observed_peer_certs.len(), 2);
        assert_ne!(
            observed_peer_certs[0], observed_peer_certs[1],
            "client cert bytes should change after rotation and be reloaded per request"
        );
    }

    #[test]
    fn authorize_internal_request_requires_mtls_identity_when_enabled() {
        let mut headers = HashMap::new();
        headers.insert(
            INTERNAL_RPC_AUTH_HEADER.to_string(),
            "cluster-shared-token".to_string(),
        );
        headers.insert(
            INTERNAL_RPC_VERSION_HEADER.to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
        );
        insert_compatibility_headers(&mut headers, &CompatibilityProfile::default());
        headers.insert(
            INTERNAL_RPC_NODE_ID_HEADER.to_string(),
            "node-a".to_string(),
        );

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/list_metrics".to_string(),
            headers,
            body: Vec::new(),
        };
        let internal_api = InternalApiConfig::new(
            "cluster-shared-token".to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            true,
            vec!["node-a".to_string()],
        );

        let response = authorize_internal_request(&request, Some(&internal_api))
            .expect_err("request should be rejected without verified mTLS identity");
        assert_eq!(response.status, 401);
        let body: InternalErrorResponse =
            serde_json::from_slice(&response.body).expect("response should decode");
        assert_eq!(body.code, "internal_mtls_auth_failed");
    }

    #[test]
    fn authorize_internal_request_accepts_matching_claimed_and_verified_mtls_identity() {
        let mut headers = HashMap::new();
        headers.insert(
            INTERNAL_RPC_AUTH_HEADER.to_string(),
            "cluster-shared-token".to_string(),
        );
        headers.insert(
            INTERNAL_RPC_VERSION_HEADER.to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
        );
        insert_compatibility_headers(&mut headers, &CompatibilityProfile::default());
        headers.insert(
            INTERNAL_RPC_NODE_ID_HEADER.to_string(),
            "node-a".to_string(),
        );
        headers.insert(
            INTERNAL_RPC_VERIFIED_NODE_ID_HEADER.to_string(),
            "node-a".to_string(),
        );

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/list_metrics".to_string(),
            headers,
            body: Vec::new(),
        };
        let internal_api = InternalApiConfig::new(
            "cluster-shared-token".to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            true,
            vec!["node-a".to_string(), "node-b".to_string()],
        );

        authorize_internal_request(&request, Some(&internal_api))
            .expect("request should be authorized");
    }

    #[test]
    fn authorize_internal_request_accepts_previous_rotated_token_during_overlap_window() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let token_path = temp_dir.path().join("cluster.token");
        std::fs::write(&token_path, "cluster-old\n").expect("token file should write");
        let secret = crate::security::ManagedStringSecret::from_path(
            crate::security::SecretRotationTarget::ClusterInternalAuthToken,
            token_path,
            true,
            false,
        )
        .expect("managed secret should load");
        let mut internal_api = InternalApiConfig::new(
            "cluster-old".to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            false,
            vec!["node-a".to_string()],
        );
        internal_api.set_auth_runtime(secret.clone());
        secret
            .rotate(Some("cluster-new".to_string()), Some(60))
            .expect("cluster token should rotate");

        let mut old_headers = HashMap::new();
        old_headers.insert(
            INTERNAL_RPC_AUTH_HEADER.to_string(),
            "cluster-old".to_string(),
        );
        old_headers.insert(
            INTERNAL_RPC_VERSION_HEADER.to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
        );
        insert_compatibility_headers(&mut old_headers, &CompatibilityProfile::default());
        let old_request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/list_metrics".to_string(),
            headers: old_headers,
            body: Vec::new(),
        };
        authorize_internal_request(&old_request, Some(&internal_api))
            .expect("previous token should remain valid during overlap");

        let mut new_headers = HashMap::new();
        new_headers.insert(
            INTERNAL_RPC_AUTH_HEADER.to_string(),
            "cluster-new".to_string(),
        );
        new_headers.insert(
            INTERNAL_RPC_VERSION_HEADER.to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
        );
        insert_compatibility_headers(&mut new_headers, &CompatibilityProfile::default());
        let new_request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/list_metrics".to_string(),
            headers: new_headers,
            body: Vec::new(),
        };
        authorize_internal_request(&new_request, Some(&internal_api))
            .expect("new token should be accepted immediately");
    }

    #[test]
    fn authorize_internal_request_accepts_required_capability() {
        let compatibility = CompatibilityProfile::default();
        let mut headers = HashMap::new();
        headers.insert(
            INTERNAL_RPC_AUTH_HEADER.to_string(),
            "cluster-shared-token".to_string(),
        );
        headers.insert(
            INTERNAL_RPC_VERSION_HEADER.to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
        );
        insert_compatibility_headers(&mut headers, &compatibility);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/control/append".to_string(),
            headers,
            body: Vec::new(),
        };
        let internal_api = InternalApiConfig::new(
            "cluster-shared-token".to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            false,
            vec!["node-a".to_string()],
        );

        authorize_internal_request_with_policy(
            &request,
            Some(&internal_api),
            &[],
            false,
            &[CLUSTER_CAPABILITY_CONTROL_REPLICATION_V1],
        )
        .expect("request with required capabilities should be accepted");
    }

    #[test]
    fn authorize_internal_request_rejects_missing_required_capability() {
        let compatibility =
            CompatibilityProfile::default().with_capabilities([CLUSTER_CAPABILITY_RPC_V1]);
        let mut headers = HashMap::new();
        headers.insert(
            INTERNAL_RPC_AUTH_HEADER.to_string(),
            "cluster-shared-token".to_string(),
        );
        headers.insert(
            INTERNAL_RPC_VERSION_HEADER.to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
        );
        insert_compatibility_headers(&mut headers, &compatibility);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/control/install_snapshot".to_string(),
            headers,
            body: Vec::new(),
        };
        let internal_api = InternalApiConfig::new(
            "cluster-shared-token".to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            false,
            vec!["node-a".to_string()],
        );

        let response = authorize_internal_request_with_policy(
            &request,
            Some(&internal_api),
            &[],
            false,
            &[CLUSTER_CAPABILITY_CONTROL_SNAPSHOT_RPC_V1],
        )
        .expect_err("missing capability should be rejected");
        assert_eq!(response.status, 409);
        let body: InternalErrorResponse =
            serde_json::from_slice(&response.body).expect("response should decode");
        assert_eq!(body.code, "peer_capability_missing");
        assert_eq!(
            body.missing_capabilities,
            vec![CLUSTER_CAPABILITY_CONTROL_SNAPSHOT_RPC_V1.to_string()]
        );
    }
}
