use crate::cluster::control::ShardHandoffPhase;
use crate::cluster::membership::MembershipView;
use crate::http::{json_response, text_response, HttpRequest, HttpResponse};
use crate::security::ManagedStringSecret;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsConnector;
use tsink::{
    DataPoint, Label, MetadataShardScope, MetricSeries, Row, SeriesPoints, SeriesSelection,
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
static RUSTLS_CRYPTO_PROVIDER: OnceLock<()> = OnceLock::new();

pub const CLUSTER_CAPABILITY_RPC_V1: &str = "cluster_rpc_v1";
pub const CLUSTER_CAPABILITY_CONTROL_REPLICATION_V1: &str = "control_replication_v1";
pub const CLUSTER_CAPABILITY_CONTROL_SNAPSHOT_RPC_V1: &str = "control_snapshot_rpc_v1";
pub const CLUSTER_CAPABILITY_CONTROL_STATE_V1: &str = "control_state_v1";
pub const CLUSTER_CAPABILITY_CONTROL_LOG_V1: &str = "control_log_v1";
pub const CLUSTER_CAPABILITY_CONTROL_RECOVERY_SNAPSHOT_V1: &str = "control_recovery_snapshot_v1";
pub const CLUSTER_CAPABILITY_CLUSTER_SNAPSHOT_V1: &str = "cluster_snapshot_v1";
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

pub fn default_cluster_capabilities() -> [&'static str; 13] {
    [
        CLUSTER_CAPABILITY_RPC_V1,
        CLUSTER_CAPABILITY_CONTROL_REPLICATION_V1,
        CLUSTER_CAPABILITY_CONTROL_SNAPSHOT_RPC_V1,
        CLUSTER_CAPABILITY_CONTROL_STATE_V1,
        CLUSTER_CAPABILITY_CONTROL_LOG_V1,
        CLUSTER_CAPABILITY_CONTROL_RECOVERY_SNAPSHOT_V1,
        CLUSTER_CAPABILITY_CLUSTER_SNAPSHOT_V1,
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
    pub inserted_rows: usize,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalSelectBatchResponse {
    pub series: Vec<SeriesPoints>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalSelectSeriesRequest {
    #[serde(default = "default_internal_ring_version")]
    pub ring_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_scope: Option<MetadataShardScope>,
    pub selection: SeriesSelection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalSelectSeriesResponse {
    pub series: Vec<MetricSeries>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalQueryExemplarsResponse {
    #[serde(default)]
    pub series: Vec<InternalExemplarSeries>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalListMetricsRequest {
    #[serde(default = "default_internal_ring_version")]
    pub ring_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_scope: Option<MetadataShardScope>,
}

impl Default for InternalListMetricsRequest {
    fn default() -> Self {
        Self {
            ring_version: default_internal_ring_version(),
            shard_scope: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalListMetricsResponse {
    pub series: Vec<MetricSeries>,
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
        message: String,
        retryable: bool,
    },
    Serialize {
        message: String,
    },
    Deserialize {
        message: String,
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
        }
    }
}

impl std::error::Error for RpcError {}

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

    pub async fn select_series(
        &self,
        endpoint: &str,
        request: &InternalSelectSeriesRequest,
    ) -> Result<InternalSelectSeriesResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/select_series", request)
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

    pub async fn digest_window(
        &self,
        endpoint: &str,
        request: &InternalDigestWindowRequest,
    ) -> Result<InternalDigestWindowResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/digest_window", request)
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

    pub async fn data_restore(
        &self,
        endpoint: &str,
        request: &InternalDataRestoreRequest,
    ) -> Result<InternalDataRestoreResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/restore_data", request)
            .await
    }

    pub async fn repair_backfill(
        &self,
        endpoint: &str,
        request: &InternalRepairBackfillRequest,
    ) -> Result<InternalRepairBackfillResponse, RpcError> {
        self.post_json(endpoint, "/internal/v1/repair_backfill", request)
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
        let body = serde_json::to_vec(request).map_err(|err| RpcError::Serialize {
            message: err.to_string(),
        })?;
        let request_bytes = self.build_http_request(endpoint, path, &body);

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
            )
            .await?
        };

        let parsed = parse_http_response(&raw_response).map_err(|err| RpcError::Transport {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
            message: err,
        })?;

        if (200..300).contains(&parsed.status) {
            return serde_json::from_slice::<Resp>(&parsed.body).map_err(|err| {
                RpcError::Deserialize {
                    message: err.to_string(),
                }
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
                expected: mismatch
                    .expected_protocol_version
                    .unwrap_or_else(|| INTERNAL_RPC_PROTOCOL_VERSION.to_string()),
                received: mismatch.received_protocol_version,
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
                message: mismatch.error,
                missing_capabilities: mismatch.missing_capabilities,
            });
        }

        let retryable = parsed_error
            .as_ref()
            .map(|err| err.retryable)
            .unwrap_or_else(|| RETRYABLE_STATUS_CODES.contains(&parsed.status));
        let message = parsed_error
            .map(|err| err.error)
            .unwrap_or_else(|| String::from_utf8_lossy(&parsed.body).to_string());

        Err(RpcError::HttpStatus {
            endpoint: endpoint.to_string(),
            path: path.to_string(),
            status: parsed.status,
            message,
            retryable,
        })
    }

    fn build_http_request(&self, endpoint: &str, path: &str, body: &[u8]) -> Vec<u8> {
        let mut request = String::new();
        request.push_str("POST ");
        request.push_str(path);
        request.push_str(" HTTP/1.1\r\n");
        request.push_str("Host: ");
        request.push_str(endpoint);
        request.push_str("\r\n");
        request.push_str("Connection: close\r\n");
        request.push_str("Content-Type: application/json\r\n");
        request.push_str("Content-Length: ");
        request.push_str(&body.len().to_string());
        request.push_str("\r\n");
        request.push_str(INTERNAL_RPC_VERSION_HEADER);
        request.push_str(": ");
        request.push_str(&self.config.protocol_version);
        request.push_str("\r\n");
        request.push_str(INTERNAL_RPC_AUTH_HEADER);
        request.push_str(": ");
        if let Some(runtime) = &self.config.internal_auth_runtime {
            request.push_str(&runtime.current());
        } else {
            request.push_str(&self.config.internal_auth_token);
        }
        request.push_str("\r\n");
        request.push_str(INTERNAL_RPC_CAPABILITIES_HEADER);
        request.push_str(": ");
        request.push_str(&self.config.compatibility.capabilities.join(","));
        request.push_str("\r\n");
        if !self.config.local_node_id.trim().is_empty() {
            request.push_str(INTERNAL_RPC_NODE_ID_HEADER);
            request.push_str(": ");
            request.push_str(self.config.local_node_id.trim());
            request.push_str("\r\n");
        }
        request.push_str("\r\n");
        request.into_bytes()
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

    let mut raw_response = Vec::new();
    tokio::time::timeout(timeout, stream.read_to_end(&mut raw_response))
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
    Ok(raw_response)
}

#[derive(Debug, Clone)]
struct ParsedHttpResponse {
    status: u16,
    #[allow(dead_code)]
    headers: HashMap<String, String>,
    body: Vec<u8>,
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
        return Err(format!("unsupported HTTP version: {version}"));
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
            return Err(format!("malformed response header line: {line}"));
        };
        let name = name.trim();
        if name.is_empty() {
            return Err(format!("malformed response header line: {line}"));
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

        assert_eq!(err, "unsupported HTTP version: HTTP/2");
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

            let _req: InternalListMetricsRequest =
                serde_json::from_slice(&request.body).expect("request body should decode");

            let response = HttpResponse::new(
                200,
                serde_json::to_vec(&InternalListMetricsResponse { series: Vec::new() })
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

        let listener = TcpListener::bind("127.0.0.1:0")
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
                    serde_json::to_vec(&InternalListMetricsResponse { series: Vec::new() })
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
