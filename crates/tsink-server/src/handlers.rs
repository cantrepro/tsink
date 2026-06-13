use crate::admission::{
    self, ReadAdmissionController, ReadAdmissionError, ReadAdmissionMetricsSnapshot,
    WriteAdmissionController, WriteAdmissionError, WriteAdmissionMetricsSnapshot,
};
use crate::cluster::audit::{
    ClusterAuditActor, ClusterAuditEntryInput, ClusterAuditOutcome, ClusterAuditQuery,
};
use crate::cluster::config::{
    ClusterReadConsistency, ClusterReadPartialResponsePolicy, ClusterWriteConsistency,
};
use crate::cluster::consensus::{
    ControlLivenessSnapshot, ControlLogRecoverySnapshot, ControlPeerLivenessStatus, ProposeOutcome,
};
use crate::cluster::control::{
    ClusterHandoffSnapshot, ControlHandoffMutationOutcome, ControlMembershipMutationOutcome,
    ControlNodeStatus, ControlState, ShardHandoffPhase, ShardHandoffSnapshot,
};
use crate::cluster::dedupe::{
    dedupe_metrics_snapshot, validate_idempotency_key, DedupeBeginOutcome, DedupeWindowStore,
};
use crate::cluster::distributed_storage::{DistributedPromqlReadBridge, DistributedStorageAdapter};
use crate::cluster::hotspot::{self, build_cluster_hotspot_snapshot, ClusterHotspotSnapshot};
use crate::cluster::membership::{ClusterNode, MembershipView};
use crate::cluster::outbox::{
    outbox_metrics_snapshot, OutboxMetricsSnapshot, OutboxStalledPeerSnapshot,
};
use crate::cluster::planner::{
    read_planner_labeled_metrics_snapshot, read_planner_last_plans_snapshot,
    read_planner_metrics_snapshot, ReadPlannerLabeledMetricsSnapshot, ReadPlannerMetricsSnapshot,
};
use crate::cluster::query::{
    read_fanout_labeled_metrics_snapshot, read_fanout_metrics_snapshot, ReadFanoutError,
    ReadFanoutExecutor, ReadFanoutLabeledMetricsSnapshot, ReadFanoutMetricsSnapshot,
    ReadFanoutResponseMetadata, SeriesPoints, FANOUT_REMOTE_REQUEST_LATENCY_BUCKETS_SECONDS,
};
use crate::cluster::repair::{
    compute_shard_window_digest, DigestExchangeSnapshot, RebalanceRunTriggerError,
    RebalanceSchedulerControlSnapshot, RebalanceSchedulerSnapshot, RepairControlSnapshot,
    RepairRunTriggerError,
};
use crate::cluster::replication::{
    stable_series_identity_hash, write_routing_labeled_metrics_snapshot,
    write_routing_metrics_snapshot, WriteConsistencyOutcome, WriteRouter, WriteRoutingError,
    WriteRoutingLabeledMetricsSnapshot, WRITE_CONSISTENCY_OVERRIDE_HEADER,
    WRITE_REMOTE_REQUEST_LATENCY_BUCKETS_SECONDS,
};
use crate::cluster::ring::ShardRing;
use crate::cluster::rpc::{
    authorize_internal_request_with_policy, internal_error_response, normalize_capabilities,
    required_capabilities_for_internal_rows, required_capabilities_for_internal_write,
    required_capabilities_for_rows, InternalApiConfig, InternalControlAppendRequest,
    InternalControlAutoJoinRequest, InternalControlAutoJoinResponse, InternalControlCommand,
    InternalControlInstallSnapshotRequest, InternalDataRestoreRequest, InternalDataRestoreResponse,
    InternalDataSnapshotRequest, InternalDataSnapshotResponse, InternalDigestWindowRequest,
    InternalExemplar, InternalExemplarSeries, InternalIngestRowsRequest,
    InternalIngestRowsResponse, InternalIngestWriteRequest, InternalIngestWriteResponse,
    InternalListMetricsRequest, InternalListMetricsResponse, InternalMetricMetadataUpdate,
    InternalQueryExemplarsRequest, InternalQueryExemplarsResponse, InternalRepairBackfillRequest,
    InternalRepairBackfillResponse, InternalRow, InternalSelectBatchRequest,
    InternalSelectBatchResponse, InternalSelectRequest, InternalSelectResponse,
    InternalSelectSeriesRequest, InternalSelectSeriesResponse, InternalWriteExemplar,
    CLUSTER_CAPABILITY_CONTROL_REPLICATION_V1, CLUSTER_CAPABILITY_CONTROL_SNAPSHOT_RPC_V1,
    CLUSTER_CAPABILITY_EXEMPLAR_INGEST_V1, CLUSTER_CAPABILITY_EXEMPLAR_QUERY_V1,
    CLUSTER_CAPABILITY_HISTOGRAM_INGEST_V1, CLUSTER_CAPABILITY_METADATA_INGEST_V1,
    DEFAULT_INTERNAL_RING_VERSION, EXEMPLAR_PAYLOAD_REQUIRED_CAPABILITIES,
    HISTOGRAM_PAYLOAD_REQUIRED_CAPABILITIES, INTERNAL_RPC_AUTH_HEADER, MAX_INTERNAL_INGEST_ROWS,
    METADATA_PAYLOAD_REQUIRED_CAPABILITIES,
};
#[cfg(test)]
use crate::cluster::rpc::{
    CLUSTER_CAPABILITY_HISTOGRAM_STORAGE_V1, CLUSTER_CAPABILITY_RPC_V1,
    INTERNAL_RPC_CAPABILITIES_HEADER, INTERNAL_RPC_PROTOCOL_VERSION, INTERNAL_RPC_VERSION_HEADER,
};
use crate::cluster::ClusterRequestContext;
use crate::edge_sync;
use crate::exemplar_store::{
    ExemplarSeries, ExemplarStore, ExemplarStoreConfig, ExemplarStoreMetricsSnapshot, ExemplarWrite,
};
use crate::http::{json_response, text_response, HttpRequest, HttpResponse, MAX_BODY_BYTES};
use crate::legacy_ingest::{
    self, AdapterCounterSnapshot, LegacyAdapterKind, LegacyIngestStatusSnapshot,
};
use crate::managed_control_plane::{
    ManagedBackupPolicyApplyRequest, ManagedBackupRunRecordRequest, ManagedControlPlane,
    ManagedControlPlaneActor, ManagedControlPlaneAuditFilter, ManagedDeploymentProvisionRequest,
    ManagedMaintenanceApplyRequest, ManagedTenantApplyRequest, ManagedTenantLifecycleRequest,
    ManagedUpgradeApplyRequest,
};
use crate::metadata_store::{metric_type_to_api_string, MetricMetadataRecord, MetricMetadataStore};
use crate::otlp::{
    normalize_metrics_export_request, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    OtlpMetricKind, OtlpNormalizationStats,
};
use crate::prom_remote::{
    histogram, BucketSpan, Histogram as PromHistogram,
    HistogramResetHint as PromHistogramResetHint, Label as PromLabel, LabelMatcher, MatcherType,
    MetricType, Query, QueryResult, ReadRequest, ReadResponse, ReadResponseType,
    Sample as PromSample, TimeSeries, WriteRequest,
};
use crate::prom_write::{
    normalize_remote_write_request, NormalizedExemplar, NormalizedHistogramSample,
    NormalizedMetricMetadataUpdate, NormalizedSeriesIdentity, NormalizedWriteEnvelope,
};
use crate::rbac::{self, RbacRegistry};
use crate::rules::{self, RulesApplyRequest, RulesRunTriggerError, RulesRuntime};
use crate::security::{
    SecretRotationMode, SecretRotationTarget, SecurityManager, SecurityRotateResult,
};
use crate::tenant;
use crate::usage::{UsageAccounting, UsageBucketWidth, UsageCategory, UsageRecordInput};
use prost::Message;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use snap::raw::{decompress_len, Decoder as SnappyDecoder, Encoder as SnappyEncoder};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tsink::promql::ast::{Expr, MatchOp};
use tsink::promql::types::{histogram_buckets, histogram_count_value, PromqlValue};
use tsink::promql::Engine;
use tsink::{
    DataPoint, Label, MetadataShardScope, MetricSeries, RollupPolicy, Row, SeriesMatcher,
    SeriesMatcherOp, SeriesSelection, ShardWindowScanOptions, Storage, StorageBuilder,
    TimestampPrecision,
};

mod admin;
mod internal_api;
mod metrics;
mod public_api;
mod router;

use self::internal_api::{
    cluster_reads_use_local_storage, cluster_ring_version, current_control_state,
    effective_read_fanout, effective_write_router, handle_internal_control_append,
    handle_internal_control_auto_join, handle_internal_control_install_snapshot,
    handle_internal_digest_window, handle_internal_ingest_rows, handle_internal_ingest_write,
    handle_internal_list_metrics, handle_internal_query_exemplars, handle_internal_repair_backfill,
    handle_internal_restore_data, handle_internal_select, handle_internal_select_batch,
    handle_internal_select_series, handle_internal_snapshot_data,
    internal_write_exemplar_to_store_write, membership_from_control_state,
    metric_series_identity_key,
};
#[cfg(test)]
use self::internal_api::{
    collect_internal_repair_backfill_rows, exemplar_series_to_internal,
    owned_metadata_shard_scope_for_local_node,
};
use admin::*;
use public_api::*;

const CONTROL_RECOVERY_SNAPSHOT_MAGIC: &str = "tsink-control-recovery-snapshot";
const CONTROL_RECOVERY_SNAPSHOT_SCHEMA_VERSION: u16 = 1;
const CLUSTER_SNAPSHOT_MANIFEST_MAGIC: &str = "tsink-cluster-snapshot";
const CLUSTER_SNAPSHOT_MANIFEST_SCHEMA_VERSION: u16 = 1;
const CLUSTER_RESTORE_REPORT_MAGIC: &str = "tsink-cluster-restore-report";
const CLUSTER_RESTORE_REPORT_SCHEMA_VERSION: u16 = 1;
const READ_PARTIAL_RESPONSE_OVERRIDE_HEADER: &str = "x-tsink-read-partial-response";
const READ_CONSISTENCY_HEADER: &str = "X-Tsink-Read-Consistency";
const READ_PARTIAL_RESPONSE_POLICY_HEADER: &str = "X-Tsink-Read-Partial-Policy";
const READ_PARTIAL_RESPONSE_HEADER: &str = "X-Tsink-Read-Partial-Response";
const READ_PARTIAL_WARNINGS_HEADER: &str = "X-Tsink-Read-Partial-Warnings";
const READ_ERROR_CODE_HEADER: &str = "X-Tsink-Read-Error-Code";
const WRITE_ERROR_CODE_HEADER: &str = "X-Tsink-Write-Error-Code";
const AUDIT_ACTOR_ID_HEADER: &str = "x-tsink-actor-id";
const AUDIT_FORWARDED_USER_HEADER: &str = "x-forwarded-user";
const METADATA_API_DEFAULT_LIMIT: usize = 1_000;
const METADATA_API_MAX_LIMIT: usize = 10_000;
const REMOTE_WRITE_METADATA_ENABLED_ENV: &str = "TSINK_REMOTE_WRITE_METADATA_ENABLED";
const REMOTE_WRITE_EXEMPLARS_ENABLED_ENV: &str = "TSINK_REMOTE_WRITE_EXEMPLARS_ENABLED";
const REMOTE_WRITE_HISTOGRAMS_ENABLED_ENV: &str = "TSINK_REMOTE_WRITE_HISTOGRAMS_ENABLED";
const REMOTE_WRITE_MAX_METADATA_UPDATES_ENV: &str = "TSINK_REMOTE_WRITE_MAX_METADATA_UPDATES";
const REMOTE_WRITE_MAX_HISTOGRAM_BUCKET_ENTRIES_ENV: &str =
    "TSINK_REMOTE_WRITE_MAX_HISTOGRAM_BUCKET_ENTRIES";
const OTLP_METRICS_ENABLED_ENV: &str = "TSINK_OTLP_METRICS_ENABLED";
const DEFAULT_REMOTE_WRITE_MAX_METADATA_UPDATES: usize = 512;
const DEFAULT_REMOTE_WRITE_MAX_HISTOGRAM_BUCKET_ENTRIES: usize = 16_384;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RollupPoliciesApplyRequest {
    #[serde(default)]
    policies: Vec<RollupPolicy>,
}

static PAYLOAD_METADATA_ACCEPTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_METADATA_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_METADATA_THROTTLED_TOTAL: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_EXEMPLAR_ACCEPTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_EXEMPLAR_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_EXEMPLAR_THROTTLED_TOTAL: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_HISTOGRAM_ACCEPTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_HISTOGRAM_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_HISTOGRAM_THROTTLED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_REQUEST_ACCEPTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_REQUEST_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_GAUGE_ACCEPTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_GAUGE_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_SUM_ACCEPTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_SUM_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_HISTOGRAM_ACCEPTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_HISTOGRAM_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_SUMMARY_ACCEPTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_SUMMARY_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_EXPONENTIAL_HISTOGRAM_ACCEPTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_EXPONENTIAL_HISTOGRAM_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_EXEMPLAR_ACCEPTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static OTLP_EXEMPLAR_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrometheusPayloadKind {
    Metadata,
    Exemplar,
    Histogram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PrometheusPayloadConfig {
    metadata_enabled: bool,
    exemplars_enabled: bool,
    histograms_enabled: bool,
    max_metadata_updates_per_request: usize,
    max_histogram_bucket_entries_per_request: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PayloadCounterSnapshot {
    accepted_total: u64,
    rejected_total: u64,
    throttled_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PayloadStatusSnapshot {
    enabled: bool,
    required_capabilities: Vec<String>,
    accepted_total: u64,
    rejected_total: u64,
    throttled_total: u64,
    max_per_request: Option<usize>,
    max_bucket_entries_per_request: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PrometheusPayloadStatusSnapshot {
    local_capabilities: Vec<String>,
    metadata: PayloadStatusSnapshot,
    exemplars: PayloadStatusSnapshot,
    histograms: PayloadStatusSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OtlpMetricsConfig {
    enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OtlpCounterSnapshot {
    accepted_total: u64,
    rejected_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OtlpMetricsStatusSnapshot {
    enabled: bool,
    accepted_requests_total: u64,
    rejected_requests_total: u64,
    accepted_exemplars_total: u64,
    rejected_exemplars_total: u64,
    supported_shapes: Vec<String>,
    gauges: OtlpCounterSnapshot,
    sums: OtlpCounterSnapshot,
    histograms: OtlpCounterSnapshot,
    summaries: OtlpCounterSnapshot,
    exponential_histograms: OtlpCounterSnapshot,
}

fn prometheus_payload_config() -> PrometheusPayloadConfig {
    PrometheusPayloadConfig {
        metadata_enabled: parse_env_bool(REMOTE_WRITE_METADATA_ENABLED_ENV, true),
        exemplars_enabled: parse_env_bool(REMOTE_WRITE_EXEMPLARS_ENABLED_ENV, true),
        histograms_enabled: parse_env_bool(REMOTE_WRITE_HISTOGRAMS_ENABLED_ENV, true),
        max_metadata_updates_per_request: parse_env_usize(
            REMOTE_WRITE_MAX_METADATA_UPDATES_ENV,
            DEFAULT_REMOTE_WRITE_MAX_METADATA_UPDATES,
        )
        .max(1),
        max_histogram_bucket_entries_per_request: parse_env_usize(
            REMOTE_WRITE_MAX_HISTOGRAM_BUCKET_ENTRIES_ENV,
            DEFAULT_REMOTE_WRITE_MAX_HISTOGRAM_BUCKET_ENTRIES,
        )
        .max(1),
    }
}

fn otlp_metrics_config() -> OtlpMetricsConfig {
    OtlpMetricsConfig {
        enabled: parse_env_bool(OTLP_METRICS_ENABLED_ENV, true),
    }
}

fn parse_env_bool(var: &str, default: bool) -> bool {
    match std::env::var(var) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

fn parse_env_usize(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

#[derive(Clone, Copy)]
pub(crate) struct AppContext<'a> {
    pub storage: &'a Arc<dyn Storage>,
    pub metadata_store: &'a Arc<MetricMetadataStore>,
    pub exemplar_store: &'a Arc<ExemplarStore>,
    pub rules_runtime: Option<&'a RulesRuntime>,
    pub engine: &'a Engine,
    pub cluster_context: Option<&'a ClusterRequestContext>,
    pub tenant_registry: Option<&'a tenant::TenantRegistry>,
    pub rbac_registry: Option<&'a RbacRegistry>,
    pub edge_sync_context: Option<&'a edge_sync::EdgeSyncRuntimeContext>,
    pub security_manager: Option<&'a SecurityManager>,
    pub usage_accounting: Option<&'a UsageAccounting>,
    pub managed_control_plane: Option<&'a ManagedControlPlane>,
}

pub(crate) struct RequestContext<'a> {
    pub request: HttpRequest,
    pub server_start: Instant,
    pub timestamp_precision: TimestampPrecision,
    pub admin_api_enabled: bool,
    pub admin_path_prefix: Option<&'a Path>,
    pub internal_api: Option<&'a InternalApiConfig>,
}

fn payload_required_capabilities(kind: PrometheusPayloadKind) -> Vec<String> {
    match kind {
        PrometheusPayloadKind::Metadata => {
            normalize_capabilities(METADATA_PAYLOAD_REQUIRED_CAPABILITIES)
        }
        PrometheusPayloadKind::Exemplar => {
            normalize_capabilities(EXEMPLAR_PAYLOAD_REQUIRED_CAPABILITIES)
        }
        PrometheusPayloadKind::Histogram => {
            normalize_capabilities(HISTOGRAM_PAYLOAD_REQUIRED_CAPABILITIES)
        }
    }
}

fn required_capability_requested(required_capabilities: &[String], capability: &str) -> bool {
    required_capabilities
        .iter()
        .any(|item| item.as_str() == capability)
}

fn payload_counter_snapshot(kind: PrometheusPayloadKind) -> PayloadCounterSnapshot {
    match kind {
        PrometheusPayloadKind::Metadata => PayloadCounterSnapshot {
            accepted_total: PAYLOAD_METADATA_ACCEPTED_TOTAL.load(Ordering::Relaxed),
            rejected_total: PAYLOAD_METADATA_REJECTED_TOTAL.load(Ordering::Relaxed),
            throttled_total: PAYLOAD_METADATA_THROTTLED_TOTAL.load(Ordering::Relaxed),
        },
        PrometheusPayloadKind::Exemplar => PayloadCounterSnapshot {
            accepted_total: PAYLOAD_EXEMPLAR_ACCEPTED_TOTAL.load(Ordering::Relaxed),
            rejected_total: PAYLOAD_EXEMPLAR_REJECTED_TOTAL.load(Ordering::Relaxed),
            throttled_total: PAYLOAD_EXEMPLAR_THROTTLED_TOTAL.load(Ordering::Relaxed),
        },
        PrometheusPayloadKind::Histogram => PayloadCounterSnapshot {
            accepted_total: PAYLOAD_HISTOGRAM_ACCEPTED_TOTAL.load(Ordering::Relaxed),
            rejected_total: PAYLOAD_HISTOGRAM_REJECTED_TOTAL.load(Ordering::Relaxed),
            throttled_total: PAYLOAD_HISTOGRAM_THROTTLED_TOTAL.load(Ordering::Relaxed),
        },
    }
}

fn otlp_counter_snapshot(kind: OtlpMetricKind) -> OtlpCounterSnapshot {
    match kind {
        OtlpMetricKind::Gauge => OtlpCounterSnapshot {
            accepted_total: OTLP_GAUGE_ACCEPTED_TOTAL.load(Ordering::Relaxed),
            rejected_total: OTLP_GAUGE_REJECTED_TOTAL.load(Ordering::Relaxed),
        },
        OtlpMetricKind::Sum => OtlpCounterSnapshot {
            accepted_total: OTLP_SUM_ACCEPTED_TOTAL.load(Ordering::Relaxed),
            rejected_total: OTLP_SUM_REJECTED_TOTAL.load(Ordering::Relaxed),
        },
        OtlpMetricKind::Histogram => OtlpCounterSnapshot {
            accepted_total: OTLP_HISTOGRAM_ACCEPTED_TOTAL.load(Ordering::Relaxed),
            rejected_total: OTLP_HISTOGRAM_REJECTED_TOTAL.load(Ordering::Relaxed),
        },
        OtlpMetricKind::Summary => OtlpCounterSnapshot {
            accepted_total: OTLP_SUMMARY_ACCEPTED_TOTAL.load(Ordering::Relaxed),
            rejected_total: OTLP_SUMMARY_REJECTED_TOTAL.load(Ordering::Relaxed),
        },
        OtlpMetricKind::ExponentialHistogram => OtlpCounterSnapshot {
            accepted_total: OTLP_EXPONENTIAL_HISTOGRAM_ACCEPTED_TOTAL.load(Ordering::Relaxed),
            rejected_total: OTLP_EXPONENTIAL_HISTOGRAM_REJECTED_TOTAL.load(Ordering::Relaxed),
        },
    }
}

fn record_otlp_request_accepted() {
    OTLP_REQUEST_ACCEPTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

fn record_otlp_request_rejected() {
    OTLP_REQUEST_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

fn record_otlp_points_accepted(stats: &OtlpNormalizationStats) {
    OTLP_GAUGE_ACCEPTED_TOTAL.fetch_add(stats.gauges as u64, Ordering::Relaxed);
    OTLP_SUM_ACCEPTED_TOTAL.fetch_add(stats.sums as u64, Ordering::Relaxed);
    OTLP_HISTOGRAM_ACCEPTED_TOTAL.fetch_add(stats.histograms as u64, Ordering::Relaxed);
    OTLP_SUMMARY_ACCEPTED_TOTAL.fetch_add(stats.summaries as u64, Ordering::Relaxed);
}

fn record_otlp_rejected_kind(kind: OtlpMetricKind) {
    match kind {
        OtlpMetricKind::Gauge => {
            OTLP_GAUGE_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        OtlpMetricKind::Sum => {
            OTLP_SUM_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        OtlpMetricKind::Histogram => {
            OTLP_HISTOGRAM_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        OtlpMetricKind::Summary => {
            OTLP_SUMMARY_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        OtlpMetricKind::ExponentialHistogram => {
            OTLP_EXPONENTIAL_HISTOGRAM_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn payload_item_count(
    kind: PrometheusPayloadKind,
    metadata_updates: usize,
    exemplars: usize,
    histograms: usize,
) -> usize {
    match kind {
        PrometheusPayloadKind::Metadata => metadata_updates,
        PrometheusPayloadKind::Exemplar => exemplars,
        PrometheusPayloadKind::Histogram => histograms,
    }
}

fn record_payload_accepted(kind: PrometheusPayloadKind, count: usize) {
    let count = count as u64;
    match kind {
        PrometheusPayloadKind::Metadata => {
            PAYLOAD_METADATA_ACCEPTED_TOTAL.fetch_add(count, Ordering::Relaxed);
        }
        PrometheusPayloadKind::Exemplar => {
            PAYLOAD_EXEMPLAR_ACCEPTED_TOTAL.fetch_add(count, Ordering::Relaxed);
        }
        PrometheusPayloadKind::Histogram => {
            PAYLOAD_HISTOGRAM_ACCEPTED_TOTAL.fetch_add(count, Ordering::Relaxed);
        }
    }
}

fn record_payload_rejected(kind: PrometheusPayloadKind, count: usize) {
    let count = count as u64;
    match kind {
        PrometheusPayloadKind::Metadata => {
            PAYLOAD_METADATA_REJECTED_TOTAL.fetch_add(count, Ordering::Relaxed);
        }
        PrometheusPayloadKind::Exemplar => {
            PAYLOAD_EXEMPLAR_REJECTED_TOTAL.fetch_add(count, Ordering::Relaxed);
        }
        PrometheusPayloadKind::Histogram => {
            PAYLOAD_HISTOGRAM_REJECTED_TOTAL.fetch_add(count, Ordering::Relaxed);
        }
    }
}

fn record_payload_throttled(kind: PrometheusPayloadKind, count: usize) {
    let count = count as u64;
    match kind {
        PrometheusPayloadKind::Metadata => {
            PAYLOAD_METADATA_THROTTLED_TOTAL.fetch_add(count, Ordering::Relaxed);
        }
        PrometheusPayloadKind::Exemplar => {
            PAYLOAD_EXEMPLAR_THROTTLED_TOTAL.fetch_add(count, Ordering::Relaxed);
        }
        PrometheusPayloadKind::Histogram => {
            PAYLOAD_HISTOGRAM_THROTTLED_TOTAL.fetch_add(count, Ordering::Relaxed);
        }
    }
}

fn record_query_pressure(tenant_id: &str, requests: usize, units: usize) {
    hotspot::record_tenant_query(
        tenant_id,
        u64::try_from(requests).unwrap_or(u64::MAX).max(1),
        u64::try_from(units).unwrap_or(u64::MAX),
    );
}

fn maybe_record_usage(usage_accounting: Option<&UsageAccounting>, record: UsageRecordInput<'_>) {
    if let Some(accounting) = usage_accounting {
        let _ = accounting.record(record);
    }
}

fn elapsed_nanos_since(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Clone, Copy)]
struct QueryUsageMetrics {
    request_units: u64,
    result_units: u64,
    duration_nanos: u64,
    request_bytes: u64,
}

impl QueryUsageMetrics {
    const fn new(
        request_units: u64,
        result_units: u64,
        duration_nanos: u64,
        request_bytes: u64,
    ) -> Self {
        Self {
            request_units,
            result_units,
            duration_nanos,
            request_bytes,
        }
    }
}

fn record_query_usage(
    usage_accounting: Option<&UsageAccounting>,
    tenant_id: &str,
    operation: &str,
    source: &str,
    metrics: QueryUsageMetrics,
) {
    let mut record = UsageRecordInput::success(tenant_id, UsageCategory::Query, operation, source);
    record.request_units = metrics.request_units;
    record.result_units = metrics.result_units;
    record.duration_nanos = metrics.duration_nanos;
    record.request_bytes = metrics.request_bytes;
    maybe_record_usage(usage_accounting, record);
}

#[derive(Clone, Copy)]
struct IngestUsageMetrics {
    rows: u64,
    metadata_updates: u64,
    exemplars_accepted: u64,
    exemplars_dropped: u64,
    histogram_series: u64,
    duration_nanos: u64,
    request_bytes: u64,
}

impl IngestUsageMetrics {
    const fn new(
        rows: u64,
        metadata_updates: u64,
        exemplars_accepted: u64,
        exemplars_dropped: u64,
        histogram_series: u64,
        duration_nanos: u64,
        request_bytes: u64,
    ) -> Self {
        Self {
            rows,
            metadata_updates,
            exemplars_accepted,
            exemplars_dropped,
            histogram_series,
            duration_nanos,
            request_bytes,
        }
    }
}

fn record_ingest_usage(
    usage_accounting: Option<&UsageAccounting>,
    tenant_id: &str,
    operation: &str,
    source: &str,
    metrics: IngestUsageMetrics,
) {
    let mut record = UsageRecordInput::success(tenant_id, UsageCategory::Ingest, operation, source);
    record.request_units = metrics.rows;
    record.rows = metrics.rows;
    record.metadata_updates = metrics.metadata_updates;
    record.exemplars_accepted = metrics.exemplars_accepted;
    record.exemplars_dropped = metrics.exemplars_dropped;
    record.histogram_series = metrics.histogram_series;
    record.duration_nanos = metrics.duration_nanos;
    record.request_bytes = metrics.request_bytes;
    maybe_record_usage(usage_accounting, record);
}

fn record_retention_usage(
    usage_accounting: Option<&UsageAccounting>,
    tenant_id: &str,
    matched_series: u64,
    tombstones_applied: u64,
    request_units: u64,
    duration_nanos: u64,
) {
    let mut record = UsageRecordInput::success(
        tenant_id,
        UsageCategory::Retention,
        "delete_series",
        "/api/v1/admin/delete_series",
    );
    record.request_units = request_units;
    record.matched_series = matched_series;
    record.tombstones_applied = tombstones_applied;
    record.duration_nanos = duration_nanos;
    maybe_record_usage(usage_accounting, record);
}

fn promql_value_units(value: &PromqlValue) -> usize {
    match value {
        PromqlValue::Scalar(_, _) | PromqlValue::String(_, _) => 1,
        PromqlValue::InstantVector(samples) => samples.len(),
        PromqlValue::RangeVector(series) => series
            .iter()
            .map(|item| item.samples.len().saturating_add(item.histograms.len()))
            .sum(),
    }
}

fn remote_read_query_units(results: &[QueryResult]) -> usize {
    results
        .iter()
        .map(|result| {
            result
                .timeseries
                .iter()
                .map(|series| series.samples.len().saturating_add(series.histograms.len()))
                .sum::<usize>()
        })
        .sum()
}

fn exemplar_query_units(series: &[ExemplarSeries]) -> usize {
    series.iter().map(|item| item.exemplars.len()).sum()
}

fn cluster_hotspot_snapshot_for_request(
    metrics: &[MetricSeries],
    cluster_context: Option<&ClusterRequestContext>,
    tenant_scope: Option<&str>,
) -> ClusterHotspotSnapshot {
    let control_state = cluster_context
        .and_then(|context| context.control_consensus.as_ref())
        .map(|consensus| consensus.current_state());
    build_cluster_hotspot_snapshot(
        metrics,
        cluster_context.map(|context| &context.runtime.ring),
        control_state.as_ref(),
        tenant_scope,
    )
}

fn payload_status_snapshot(
    cluster_context: Option<&ClusterRequestContext>,
) -> PrometheusPayloadStatusSnapshot {
    let config = prometheus_payload_config();
    let compatibility = cluster_context
        .map(|context| context.runtime.internal_api.compatibility.clone())
        .unwrap_or_default();
    let metadata_counters = payload_counter_snapshot(PrometheusPayloadKind::Metadata);
    let exemplar_counters = payload_counter_snapshot(PrometheusPayloadKind::Exemplar);
    let histogram_counters = payload_counter_snapshot(PrometheusPayloadKind::Histogram);

    PrometheusPayloadStatusSnapshot {
        local_capabilities: compatibility.capabilities,
        metadata: PayloadStatusSnapshot {
            enabled: config.metadata_enabled,
            required_capabilities: payload_required_capabilities(PrometheusPayloadKind::Metadata),
            accepted_total: metadata_counters.accepted_total,
            rejected_total: metadata_counters.rejected_total,
            throttled_total: metadata_counters.throttled_total,
            max_per_request: Some(config.max_metadata_updates_per_request),
            max_bucket_entries_per_request: None,
        },
        exemplars: PayloadStatusSnapshot {
            enabled: config.exemplars_enabled,
            required_capabilities: payload_required_capabilities(PrometheusPayloadKind::Exemplar),
            accepted_total: exemplar_counters.accepted_total,
            rejected_total: exemplar_counters.rejected_total,
            throttled_total: exemplar_counters.throttled_total,
            max_per_request: None,
            max_bucket_entries_per_request: None,
        },
        histograms: PayloadStatusSnapshot {
            enabled: config.histograms_enabled,
            required_capabilities: payload_required_capabilities(PrometheusPayloadKind::Histogram),
            accepted_total: histogram_counters.accepted_total,
            rejected_total: histogram_counters.rejected_total,
            throttled_total: histogram_counters.throttled_total,
            max_per_request: None,
            max_bucket_entries_per_request: Some(config.max_histogram_bucket_entries_per_request),
        },
    }
}

fn otlp_metrics_status_snapshot() -> OtlpMetricsStatusSnapshot {
    let config = otlp_metrics_config();
    OtlpMetricsStatusSnapshot {
        enabled: config.enabled,
        accepted_requests_total: OTLP_REQUEST_ACCEPTED_TOTAL.load(Ordering::Relaxed),
        rejected_requests_total: OTLP_REQUEST_REJECTED_TOTAL.load(Ordering::Relaxed),
        accepted_exemplars_total: OTLP_EXEMPLAR_ACCEPTED_TOTAL.load(Ordering::Relaxed),
        rejected_exemplars_total: OTLP_EXEMPLAR_REJECTED_TOTAL.load(Ordering::Relaxed),
        supported_shapes: vec![
            "gauge".to_string(),
            "sum:cumulative".to_string(),
            "histogram:cumulative:explicit_buckets".to_string(),
            "summary".to_string(),
        ],
        gauges: otlp_counter_snapshot(OtlpMetricKind::Gauge),
        sums: otlp_counter_snapshot(OtlpMetricKind::Sum),
        histograms: otlp_counter_snapshot(OtlpMetricKind::Histogram),
        summaries: otlp_counter_snapshot(OtlpMetricKind::Summary),
        exponential_histograms: otlp_counter_snapshot(OtlpMetricKind::ExponentialHistogram),
    }
}

fn histogram_bucket_entries_total(samples: &[NormalizedHistogramSample]) -> usize {
    samples
        .iter()
        .map(|sample| {
            sample.negative_spans.len()
                + sample.negative_deltas.len()
                + sample.negative_counts.len()
                + sample.positive_spans.len()
                + sample.positive_deltas.len()
                + sample.positive_counts.len()
                + sample.custom_values.len()
        })
        .sum()
}

fn histogram_bucket_entries_total_for_rows(rows: &[InternalRow]) -> usize {
    rows.iter()
        .filter_map(|row| row.data_point.value_as_histogram())
        .map(|histogram| {
            histogram.negative_spans.len()
                + histogram.negative_deltas.len()
                + histogram.negative_counts.len()
                + histogram.positive_spans.len()
                + histogram.positive_deltas.len()
                + histogram.positive_counts.len()
                + histogram.custom_values.len()
        })
        .sum()
}

fn envelope_row_units(envelope: &NormalizedWriteEnvelope) -> usize {
    envelope
        .scalar_samples
        .len()
        .saturating_add(envelope.histogram_samples.len())
}

fn tenant_id_for_promql_request(request: &HttpRequest) -> Result<String, HttpResponse> {
    tenant::tenant_id_for_request(request).map_err(|err| promql_error_response("bad_data", &err))
}

fn tenant_id_for_text_request(request: &HttpRequest) -> Result<String, HttpResponse> {
    tenant::tenant_id_for_request(request).map_err(|err| text_response(400, &err))
}

fn tenant_policy_json(policy: &tenant::TenantRequestPolicy) -> JsonValue {
    json!({
        "quotas": {
            "maxWriteRowsPerRequest": policy.max_write_rows_per_request,
            "maxReadQueriesPerRequest": policy.max_read_queries_per_request,
            "maxMetadataMatchersPerRequest": policy.max_metadata_matchers_per_request,
            "maxQueryLengthBytes": policy.max_query_length_bytes,
            "maxRangePointsPerQuery": policy.max_range_points_per_query,
        },
        "cluster": {
            "writeConsistency": policy.write_consistency.map(|value| value.to_string()),
            "readConsistency": policy.read_consistency.map(|value| value.to_string()),
            "readPartialResponsePolicy": policy.read_partial_response_policy.map(|value| value.to_string()),
        },
        "admission": {
            "ingest": {
                "maxInflightRequests": policy.admission.ingest.max_inflight_requests,
                "maxInflightUnits": policy.admission.ingest.max_inflight_units,
            },
            "query": {
                "maxInflightRequests": policy.admission.query.max_inflight_requests,
                "maxInflightUnits": policy.admission.query.max_inflight_units,
            },
            "metadata": {
                "maxInflightRequests": policy.admission.metadata.max_inflight_requests,
                "maxInflightUnits": policy.admission.metadata.max_inflight_units,
            },
            "retention": {
                "maxInflightRequests": policy.admission.retention.max_inflight_requests,
                "maxInflightUnits": policy.admission.retention.max_inflight_units,
            }
        }
    })
}

fn tenant_runtime_status_json(snapshot: &tenant::TenantRuntimeStatusSnapshot) -> JsonValue {
    json!({
        "tenantId": snapshot.tenant_id,
        "policy": tenant_policy_json(&snapshot.policy),
        "sharedRead": {
            "maxInflightRequests": snapshot.max_inflight_reads,
            "activeRequests": snapshot.active_reads,
            "rejectionsTotal": snapshot.read_rejections_total
        },
        "sharedWrite": {
            "maxInflightRequests": snapshot.max_inflight_writes,
            "activeRequests": snapshot.active_writes,
            "rejectionsTotal": snapshot.write_rejections_total
        },
        "surfaces": {
            "ingest": {
                "maxInflightRequests": snapshot.ingest.max_inflight_requests,
                "maxInflightUnits": snapshot.ingest.max_inflight_units,
                "activeRequests": snapshot.ingest.active_requests,
                "activeUnits": snapshot.ingest.active_units,
                "rejectionsTotal": snapshot.ingest.rejections_total
            },
            "query": {
                "maxInflightRequests": snapshot.query.max_inflight_requests,
                "maxInflightUnits": snapshot.query.max_inflight_units,
                "activeRequests": snapshot.query.active_requests,
                "activeUnits": snapshot.query.active_units,
                "rejectionsTotal": snapshot.query.rejections_total
            },
            "metadata": {
                "maxInflightRequests": snapshot.metadata.max_inflight_requests,
                "maxInflightUnits": snapshot.metadata.max_inflight_units,
                "activeRequests": snapshot.metadata.active_requests,
                "activeUnits": snapshot.metadata.active_units,
                "rejectionsTotal": snapshot.metadata.rejections_total
            },
            "retention": {
                "maxInflightRequests": snapshot.retention.max_inflight_requests,
                "maxInflightUnits": snapshot.retention.max_inflight_units,
                "activeRequests": snapshot.retention.active_requests,
                "activeUnits": snapshot.retention.active_units,
                "rejectionsTotal": snapshot.retention.rejections_total
            }
        },
        "recentDecisions": snapshot.recent_decisions.iter().map(|decision| {
            json!({
                "unixMs": decision.unix_ms,
                "access": decision.access,
                "surface": decision.surface,
                "outcome": decision.outcome,
                "requestedUnits": decision.requested_units,
                "reason": decision.reason
            })
        }).collect::<Vec<_>>()
    })
}

fn prepare_tenant_request(
    tenant_registry: Option<&tenant::TenantRegistry>,
    request: &HttpRequest,
    tenant_id: &str,
    access: tenant::TenantAccessScope,
) -> Result<tenant::TenantRequestPlan, HttpResponse> {
    tenant::prepare_request_plan(tenant_registry, request, tenant_id, access)
        .map_err(|err| err.to_http_response())
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub struct TestAdminRequestOptions<'a> {
    pub server_start: Instant,
    pub timestamp_precision: TimestampPrecision,
    pub admin_api_enabled: bool,
    pub admin_path_prefix: Option<&'a Path>,
}

#[cfg(test)]
impl<'a> From<TestAdminRequestOptions<'a>> for TestRequestOptions<'a> {
    fn from(options: TestAdminRequestOptions<'a>) -> Self {
        Self {
            server_start: options.server_start,
            timestamp_precision: options.timestamp_precision,
            admin_api_enabled: options.admin_api_enabled,
            admin_path_prefix: options.admin_path_prefix,
            ..Default::default()
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub struct TestRequestOptions<'a> {
    pub metadata_store: Option<&'a Arc<MetricMetadataStore>>,
    pub exemplar_store: Option<&'a Arc<ExemplarStore>>,
    pub rules_runtime: Option<&'a RulesRuntime>,
    pub server_start: Instant,
    pub timestamp_precision: TimestampPrecision,
    pub admin_api_enabled: bool,
    pub admin_path_prefix: Option<&'a Path>,
    pub internal_api: Option<&'a InternalApiConfig>,
    pub cluster_context: Option<&'a ClusterRequestContext>,
    pub tenant_registry: Option<&'a tenant::TenantRegistry>,
    pub rbac_registry: Option<&'a RbacRegistry>,
    pub edge_sync_context: Option<&'a edge_sync::EdgeSyncRuntimeContext>,
    pub security_manager: Option<&'a SecurityManager>,
    pub usage_accounting: Option<&'a UsageAccounting>,
    pub managed_control_plane: Option<&'a ManagedControlPlane>,
}

#[cfg(test)]
impl<'a> Default for TestRequestOptions<'a> {
    fn default() -> Self {
        Self {
            metadata_store: None,
            exemplar_store: None,
            rules_runtime: None,
            server_start: Instant::now(),
            timestamp_precision: TimestampPrecision::Milliseconds,
            admin_api_enabled: false,
            admin_path_prefix: None,
            internal_api: None,
            cluster_context: None,
            tenant_registry: None,
            rbac_registry: None,
            edge_sync_context: None,
            security_manager: None,
            usage_accounting: None,
            managed_control_plane: None,
        }
    }
}

#[cfg(test)]
pub async fn handle_test_request(
    storage: &Arc<dyn Storage>,
    engine: &Engine,
    request: HttpRequest,
    options: TestRequestOptions<'_>,
) -> HttpResponse {
    let default_metadata_store = Arc::new(MetricMetadataStore::in_memory());
    let default_exemplar_store = Arc::new(ExemplarStore::in_memory());
    handle_request_with_context(
        AppContext {
            storage,
            metadata_store: options.metadata_store.unwrap_or(&default_metadata_store),
            exemplar_store: options.exemplar_store.unwrap_or(&default_exemplar_store),
            rules_runtime: options.rules_runtime,
            engine,
            cluster_context: options.cluster_context,
            tenant_registry: options.tenant_registry,
            rbac_registry: options.rbac_registry,
            edge_sync_context: options.edge_sync_context,
            security_manager: options.security_manager,
            usage_accounting: options.usage_accounting,
            managed_control_plane: options.managed_control_plane,
        },
        RequestContext {
            request,
            server_start: options.server_start,
            timestamp_precision: options.timestamp_precision,
            admin_api_enabled: options.admin_api_enabled,
            admin_path_prefix: options.admin_path_prefix,
            internal_api: options.internal_api,
        },
    )
    .await
}

#[cfg(test)]
pub async fn handle_request(
    storage: &Arc<dyn Storage>,
    engine: &Engine,
    request: HttpRequest,
    server_start: Instant,
    timestamp_precision: TimestampPrecision,
) -> HttpResponse {
    handle_test_request(
        storage,
        engine,
        request,
        TestRequestOptions {
            server_start,
            timestamp_precision,
            ..Default::default()
        },
    )
    .await
}

#[cfg(test)]
pub async fn handle_request_with_admin(
    storage: &Arc<dyn Storage>,
    engine: &Engine,
    request: HttpRequest,
    server_start: Instant,
    timestamp_precision: TimestampPrecision,
    admin_api_enabled: bool,
    admin_path_prefix: Option<&Path>,
) -> HttpResponse {
    handle_test_request(
        storage,
        engine,
        request,
        TestRequestOptions {
            server_start,
            timestamp_precision,
            admin_api_enabled,
            admin_path_prefix,
            ..Default::default()
        },
    )
    .await
}

#[cfg(test)]
pub async fn handle_request_with_admin_and_metadata(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    engine: &Engine,
    request: HttpRequest,
    options: TestAdminRequestOptions<'_>,
) -> HttpResponse {
    handle_test_request(
        storage,
        engine,
        request,
        TestRequestOptions {
            metadata_store: Some(metadata_store),
            exemplar_store: Some(exemplar_store),
            ..options.into()
        },
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub async fn handle_request_with_admin_and_cluster(
    storage: &Arc<dyn Storage>,
    engine: &Engine,
    request: HttpRequest,
    server_start: Instant,
    timestamp_precision: TimestampPrecision,
    admin_api_enabled: bool,
    admin_path_prefix: Option<&Path>,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    handle_test_request(
        storage,
        engine,
        request,
        TestRequestOptions {
            server_start,
            timestamp_precision,
            admin_api_enabled,
            admin_path_prefix,
            internal_api,
            cluster_context,
            ..Default::default()
        },
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub async fn handle_request_with_admin_and_cluster_and_metadata(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    engine: &Engine,
    request: HttpRequest,
    server_start: Instant,
    timestamp_precision: TimestampPrecision,
    admin_api_enabled: bool,
    admin_path_prefix: Option<&Path>,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
) -> HttpResponse {
    handle_test_request(
        storage,
        engine,
        request,
        TestRequestOptions {
            metadata_store: Some(metadata_store),
            exemplar_store: Some(exemplar_store),
            rules_runtime,
            server_start,
            timestamp_precision,
            admin_api_enabled,
            admin_path_prefix,
            internal_api,
            cluster_context,
            edge_sync_context,
            ..Default::default()
        },
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub async fn handle_request_with_admin_and_cluster_and_tenant(
    storage: &Arc<dyn Storage>,
    engine: &Engine,
    request: HttpRequest,
    server_start: Instant,
    timestamp_precision: TimestampPrecision,
    admin_api_enabled: bool,
    admin_path_prefix: Option<&Path>,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
) -> HttpResponse {
    handle_test_request(
        storage,
        engine,
        request,
        TestRequestOptions {
            server_start,
            timestamp_precision,
            admin_api_enabled,
            admin_path_prefix,
            internal_api,
            cluster_context,
            tenant_registry,
            ..Default::default()
        },
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub async fn handle_request_with_admin_and_cluster_and_tenant_and_metadata(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    engine: &Engine,
    request: HttpRequest,
    server_start: Instant,
    timestamp_precision: TimestampPrecision,
    admin_api_enabled: bool,
    admin_path_prefix: Option<&Path>,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    rbac_registry: Option<&RbacRegistry>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    handle_test_request(
        storage,
        engine,
        request,
        TestRequestOptions {
            metadata_store: Some(metadata_store),
            exemplar_store: Some(exemplar_store),
            rules_runtime,
            server_start,
            timestamp_precision,
            admin_api_enabled,
            admin_path_prefix,
            internal_api,
            cluster_context,
            tenant_registry,
            rbac_registry,
            edge_sync_context,
            usage_accounting,
            ..Default::default()
        },
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub async fn handle_request_with_admin_and_cluster_and_tenant_and_metadata_and_security(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    engine: &Engine,
    request: HttpRequest,
    server_start: Instant,
    timestamp_precision: TimestampPrecision,
    admin_api_enabled: bool,
    admin_path_prefix: Option<&Path>,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    rbac_registry: Option<&RbacRegistry>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    security_manager: Option<&SecurityManager>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    handle_test_request(
        storage,
        engine,
        request,
        TestRequestOptions {
            metadata_store: Some(metadata_store),
            exemplar_store: Some(exemplar_store),
            rules_runtime,
            server_start,
            timestamp_precision,
            admin_api_enabled,
            admin_path_prefix,
            internal_api,
            cluster_context,
            tenant_registry,
            rbac_registry,
            edge_sync_context,
            security_manager,
            usage_accounting,
            ..Default::default()
        },
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub async fn handle_request_with_admin_and_cluster_and_tenant_and_metadata_and_security_and_managed_control_plane(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    engine: &Engine,
    request: HttpRequest,
    server_start: Instant,
    timestamp_precision: TimestampPrecision,
    admin_api_enabled: bool,
    admin_path_prefix: Option<&Path>,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    rbac_registry: Option<&RbacRegistry>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    security_manager: Option<&SecurityManager>,
    usage_accounting: Option<&UsageAccounting>,
    managed_control_plane: Option<&ManagedControlPlane>,
) -> HttpResponse {
    handle_test_request(
        storage,
        engine,
        request,
        TestRequestOptions {
            metadata_store: Some(metadata_store),
            exemplar_store: Some(exemplar_store),
            rules_runtime,
            server_start,
            timestamp_precision,
            admin_api_enabled,
            admin_path_prefix,
            internal_api,
            cluster_context,
            tenant_registry,
            rbac_registry,
            edge_sync_context,
            security_manager,
            usage_accounting,
            managed_control_plane,
        },
    )
    .await
}

pub(crate) async fn handle_request_with_context(
    app_context: AppContext<'_>,
    request_context: RequestContext<'_>,
) -> HttpResponse {
    router::route_request(app_context, request_context).await
}

#[derive(Debug, Default, Deserialize)]
struct SnapshotAdminPayload {
    path: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct RestoreAdminPayload {
    #[serde(default, alias = "snapshotPath")]
    snapshot_path: Option<String>,
    #[serde(default, alias = "dataPath")]
    data_path: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct DeleteSeriesAdminPayload {
    #[serde(default, alias = "match", alias = "matches", alias = "matchers")]
    selectors: Vec<String>,
    #[serde(default)]
    start: Option<JsonValue>,
    #[serde(default)]
    end: Option<JsonValue>,
}

#[derive(Debug, Default, Deserialize)]
struct ClusterJoinAdminPayload {
    #[serde(default, alias = "nodeId")]
    node_id: Option<String>,
    #[serde(default)]
    endpoint: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ClusterLeaveAdminPayload {
    #[serde(default, alias = "nodeId")]
    node_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ClusterRecommissionAdminPayload {
    #[serde(default, alias = "nodeId")]
    node_id: Option<String>,
    #[serde(default)]
    endpoint: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ClusterHandoffBeginAdminPayload {
    #[serde(default)]
    shard: Option<u32>,
    #[serde(default, alias = "fromNodeId")]
    from_node_id: Option<String>,
    #[serde(default, alias = "toNodeId")]
    to_node_id: Option<String>,
    #[serde(default, alias = "activationRingVersion")]
    activation_ring_version: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct ClusterHandoffProgressAdminPayload {
    #[serde(default)]
    shard: Option<u32>,
    #[serde(default)]
    phase: Option<String>,
    #[serde(default, alias = "copiedRows")]
    copied_rows: Option<u64>,
    #[serde(default, alias = "pendingRows")]
    pending_rows: Option<u64>,
    #[serde(default, alias = "lastError")]
    last_error: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ClusterHandoffCompleteAdminPayload {
    #[serde(default)]
    shard: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
struct ClusterControlSnapshotAdminPayload {
    path: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ClusterControlRestoreAdminPayload {
    #[serde(default, alias = "snapshotPath")]
    snapshot_path: Option<String>,
    #[serde(default, alias = "forceLocalLeader")]
    force_local_leader: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterSnapshotAdminPayload {
    path: Option<String>,
    manifest_path: Option<String>,
    control_snapshot_path: Option<String>,
    #[serde(default)]
    node_paths: BTreeMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterRestoreAdminPayload {
    #[serde(default, alias = "snapshotPath")]
    snapshot_path: Option<String>,
    #[serde(default, alias = "restoreRoot")]
    restore_root: Option<String>,
    #[serde(default, alias = "reportPath")]
    report_path: Option<String>,
    #[serde(default, alias = "forceLocalLeader")]
    force_local_leader: Option<bool>,
    #[serde(default, alias = "dataPaths")]
    data_paths: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlRecoverySnapshotFileV1 {
    magic: String,
    schema_version: u16,
    created_unix_ms: u64,
    source_node_id: String,
    source_control_state_path: String,
    source_control_log_path: String,
    control_state: ControlState,
    control_log: ControlLogRecoverySnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterSnapshotNodeArtifactV1 {
    node_id: String,
    endpoint: String,
    status: String,
    snapshot_path: String,
    snapshot_created_unix_ms: u64,
    snapshot_duration_ms: u64,
    snapshot_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterSnapshotManifestFileV1 {
    magic: String,
    schema_version: u16,
    snapshot_id: String,
    created_unix_ms: u64,
    coordinator_node_id: String,
    manifest_path: String,
    control_snapshot_path: String,
    membership_epoch: u64,
    ring_version: u64,
    leader_node_id: Option<String>,
    applied_log_index: u64,
    applied_log_term: u64,
    current_term: u64,
    commit_index: u64,
    rpo_estimate_ms: u64,
    cluster_nodes: Vec<ClusterSnapshotNodeArtifactV1>,
    control_snapshot: ControlRecoverySnapshotFileV1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterRestoreNodeArtifactV1 {
    node_id: String,
    endpoint: String,
    snapshot_path: String,
    data_path: String,
    restored_unix_ms: u64,
    restore_duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterRestoreReportFileV1 {
    magic: String,
    schema_version: u16,
    restored_unix_ms: u64,
    coordinator_node_id: String,
    source_snapshot_path: String,
    source_snapshot_id: String,
    report_path: String,
    restore_root: String,
    force_local_leader: bool,
    restored_membership_epoch: u64,
    restored_ring_version: u64,
    restored_leader_node_id: Option<String>,
    rpo_estimate_ms: u64,
    rto_ms: u64,
    cluster_nodes: Vec<ClusterRestoreNodeArtifactV1>,
}

#[derive(Debug, Clone)]
struct ClusterSnapshotNodeTarget {
    id: String,
    endpoint: String,
    status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdminMembershipOperation {
    Join,
    Leave,
    Recommission,
}

impl AdminMembershipOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Join => "join",
            Self::Leave => "leave",
            Self::Recommission => "recommission",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdminHandoffOperation {
    Begin,
    Progress,
    Complete,
    Status,
}

impl AdminHandoffOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Begin => "begin_handoff",
            Self::Progress => "update_handoff",
            Self::Complete => "complete_handoff",
            Self::Status => "handoff_status",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdminRepairOperation {
    Pause,
    Resume,
    Cancel,
    Status,
    Run,
}

impl AdminRepairOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "pause_repair",
            Self::Resume => "resume_repair",
            Self::Cancel => "cancel_repair",
            Self::Status => "repair_status",
            Self::Run => "run_repair",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdminRebalanceOperation {
    Pause,
    Resume,
    Run,
    Status,
}

impl AdminRebalanceOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "pause_rebalance",
            Self::Resume => "resume_rebalance",
            Self::Run => "run_rebalance",
            Self::Status => "rebalance_status",
        }
    }
}

fn parse_optional_json_body<T: DeserializeOwned>(
    request: &HttpRequest,
) -> Result<Option<T>, String> {
    if request.body.is_empty() {
        return Ok(None);
    }

    serde_json::from_slice(&request.body)
        .map(Some)
        .map_err(|err| format!("invalid JSON body: {err}"))
}

fn non_empty_param(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn parse_admin_bool(value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(format!("invalid boolean value '{other}'")),
    }
}

fn parse_admin_u32(value: &str, field: &str) -> Result<u32, String> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| format!("invalid {field}: expected unsigned integer"))
}

fn parse_admin_u64(value: &str, field: &str) -> Result<u64, String> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("invalid {field}: expected unsigned integer"))
}

fn parse_handoff_phase(value: &str) -> Result<ShardHandoffPhase, String> {
    match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "warmup" => Ok(ShardHandoffPhase::Warmup),
        "cutover" => Ok(ShardHandoffPhase::Cutover),
        "final_sync" => Ok(ShardHandoffPhase::FinalSync),
        "completed" => Ok(ShardHandoffPhase::Completed),
        "failed" => Ok(ShardHandoffPhase::Failed),
        other => Err(format!(
            "invalid phase '{other}' (expected warmup|cutover|final_sync|completed|failed)"
        )),
    }
}

fn emit_mutating_admin_audit_entry(
    cluster_context: Option<&ClusterRequestContext>,
    request: &HttpRequest,
    operation: &str,
    response: &HttpResponse,
) {
    let Some(audit_log) = cluster_context.and_then(|context| context.audit_log.as_ref()) else {
        return;
    };
    let actor = derive_audit_actor(request);
    let target = derive_audit_target(request);
    let outcome = derive_audit_outcome(response);
    if let Err(err) = audit_log.append(ClusterAuditEntryInput {
        timestamp_unix_ms: None,
        operation: operation.to_string(),
        actor,
        target,
        outcome,
    }) {
        eprintln!("cluster audit append failed for operation '{operation}': {err}");
    }
}

fn parse_admin_audit_query(request: &HttpRequest) -> Result<ClusterAuditQuery, String> {
    let limit = request
        .param("limit")
        .map(|value| {
            parse_admin_u64(&value, "limit").and_then(|parsed| {
                usize::try_from(parsed)
                    .map_err(|_| "invalid limit: value exceeds platform usize".to_string())
            })
        })
        .transpose()?;
    let since_unix_ms = request
        .param("since_unix_ms")
        .or_else(|| request.param("sinceUnixMs"))
        .map(|value| parse_admin_u64(&value, "since_unix_ms"))
        .transpose()?;
    let until_unix_ms = request
        .param("until_unix_ms")
        .or_else(|| request.param("untilUnixMs"))
        .map(|value| parse_admin_u64(&value, "until_unix_ms"))
        .transpose()?;
    Ok(ClusterAuditQuery {
        operation: non_empty_param(request.param("operation")),
        actor_id: non_empty_param(
            request
                .param("actor_id")
                .or_else(|| request.param("actorId")),
        ),
        status: non_empty_param(request.param("status")),
        since_unix_ms,
        until_unix_ms,
        limit,
    })
}

fn admin_audit_error_response(
    status: u16,
    code: &'static str,
    message: impl Into<String>,
) -> HttpResponse {
    json_response(
        status,
        &json!({
            "status": "error",
            "errorType": code,
            "error": message.into(),
        }),
    )
}

fn derive_audit_actor(request: &HttpRequest) -> ClusterAuditActor {
    if let Some(value) = request
        .header(rbac::RBAC_AUTH_PRINCIPAL_ID_HEADER)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let auth_scope = request
            .header(rbac::RBAC_AUTH_PROVIDER_HEADER)
            .map(str::trim)
            .filter(|provider| !provider.is_empty())
            .map(|provider| format!("oidc:{provider}"))
            .or_else(|| {
                request
                    .header(rbac::RBAC_AUTH_METHOD_HEADER)
                    .map(str::trim)
                    .filter(|method| !method.is_empty())
                    .map(ToString::to_string)
            })
            .unwrap_or_else(|| "rbac".to_string());
        return ClusterAuditActor {
            id: value.to_string(),
            auth_scope,
        };
    }
    if let Some(value) = request
        .header(AUDIT_ACTOR_ID_HEADER)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return ClusterAuditActor {
            id: value.to_string(),
            auth_scope: "actor_header".to_string(),
        };
    }
    if let Some(value) = request
        .header(AUDIT_FORWARDED_USER_HEADER)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return ClusterAuditActor {
            id: value.to_string(),
            auth_scope: "forwarded_user".to_string(),
        };
    }
    if let Some(value) = request
        .header("x-tsink-node-id")
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return ClusterAuditActor {
            id: format!("node:{value}"),
            auth_scope: "internal_node".to_string(),
        };
    }
    if let Some(token) = extract_bearer_token(request) {
        return ClusterAuditActor {
            id: format!("bearer:{:016x}", fnv1a64(token.as_bytes())),
            auth_scope: "bearer".to_string(),
        };
    }
    if request.header(INTERNAL_RPC_AUTH_HEADER).is_some() {
        return ClusterAuditActor {
            id: "internal_auth".to_string(),
            auth_scope: "internal_token".to_string(),
        };
    }
    ClusterAuditActor {
        id: "unknown".to_string(),
        auth_scope: "unknown".to_string(),
    }
}

fn derive_audit_target(request: &HttpRequest) -> JsonValue {
    let mut target = serde_json::Map::new();
    target.insert(
        "path".to_string(),
        JsonValue::String(request.path_without_query().to_string()),
    );
    target.insert(
        "method".to_string(),
        JsonValue::String(request.method.clone()),
    );
    for key in [
        "node_id",
        "nodeId",
        "endpoint",
        "shard",
        "from_node_id",
        "fromNodeId",
        "to_node_id",
        "toNodeId",
        "phase",
        "copied_rows",
        "copiedRows",
        "pending_rows",
        "pendingRows",
        "last_error",
        "lastError",
        "activation_ring_version",
        "activationRingVersion",
        "snapshot_path",
        "snapshotPath",
        "manifest_path",
        "manifestPath",
        "data_path",
        "dataPath",
        "restore_root",
        "restoreRoot",
        "report_path",
        "reportPath",
        "path",
        "force_local_leader",
        "forceLocalLeader",
    ] {
        if let Some(value) = request.param(key) {
            target.insert(key.to_string(), JsonValue::String(value));
        }
    }
    if let Ok(body) = serde_json::from_slice::<JsonValue>(&request.body) {
        if let Some(object) = body.as_object() {
            for key in [
                "node_id",
                "nodeId",
                "endpoint",
                "shard",
                "from_node_id",
                "fromNodeId",
                "to_node_id",
                "toNodeId",
                "phase",
                "copied_rows",
                "copiedRows",
                "pending_rows",
                "pendingRows",
                "last_error",
                "lastError",
                "activation_ring_version",
                "activationRingVersion",
                "snapshot_path",
                "snapshotPath",
                "manifest_path",
                "manifestPath",
                "data_path",
                "dataPath",
                "restore_root",
                "restoreRoot",
                "report_path",
                "reportPath",
                "path",
                "force_local_leader",
                "forceLocalLeader",
            ] {
                if let Some(value) = object.get(key) {
                    target
                        .entry(key.to_string())
                        .or_insert_with(|| value.clone());
                }
            }
        }
    }
    JsonValue::Object(target)
}

fn derive_audit_outcome(response: &HttpResponse) -> ClusterAuditOutcome {
    let mut status = if response.status >= 400 {
        "error".to_string()
    } else {
        "success".to_string()
    };
    let mut result = None;
    let mut error_type = None;
    if let Ok(body) = serde_json::from_slice::<JsonValue>(&response.body) {
        if let Some(value) = body.get("status").and_then(JsonValue::as_str) {
            status = value.to_string();
        }
        result = body
            .pointer("/data/result")
            .and_then(JsonValue::as_str)
            .map(ToString::to_string);
        error_type = body
            .get("errorType")
            .and_then(JsonValue::as_str)
            .map(ToString::to_string)
            .or_else(|| {
                body.get("code")
                    .and_then(JsonValue::as_str)
                    .map(ToString::to_string)
            });
    }
    ClusterAuditOutcome {
        status,
        http_status: response.status,
        result,
        error_type,
    }
}

fn extract_bearer_token(request: &HttpRequest) -> Option<&str> {
    let authorization = request.header("authorization")?;
    let (scheme, token) = authorization.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[allow(clippy::too_many_arguments)]
fn handle_metrics(
    storage: &Arc<dyn Storage>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    server_start: Instant,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    rbac_registry: Option<&RbacRegistry>,
    security_manager: Option<&SecurityManager>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    metrics::render_metrics(
        storage,
        exemplar_store,
        rules_runtime,
        server_start,
        cluster_context,
        edge_sync_context,
        rbac_registry,
        security_manager,
        usage_accounting,
    )
}

fn security_status_json(
    security_manager: Option<&SecurityManager>,
    rbac_registry: Option<&RbacRegistry>,
) -> JsonValue {
    let targets = security_manager
        .map(|manager| manager.state_snapshot(rbac_registry).targets)
        .unwrap_or_default();
    let audit_entries = security_manager
        .map(|manager| manager.state_snapshot(rbac_registry).audit_entries)
        .unwrap_or_default();
    json!({
        "enabled": security_manager.is_some() || rbac_registry.is_some(),
        "targets": targets,
        "auditEntries": audit_entries,
        "serviceAccounts": rbac_service_account_summary(rbac_registry),
    })
}

fn rbac_service_account_summary(
    rbac_registry: Option<&RbacRegistry>,
) -> Option<crate::security::ServiceAccountRotationSummary> {
    let registry = rbac_registry?;
    let state = registry.state_snapshot();
    let disabled = state
        .service_accounts
        .iter()
        .filter(|account| account.disabled)
        .count();
    let last_rotated_unix_ms = state
        .service_accounts
        .iter()
        .map(|account| account.last_rotated_unix_ms)
        .max()
        .unwrap_or(0);
    Some(crate::security::ServiceAccountRotationSummary {
        total: state.service_accounts.len(),
        disabled,
        last_rotated_unix_ms,
        audit_entries: state.audit_entries,
    })
}

fn cluster_control_liveness_snapshot(
    cluster_context: Option<&ClusterRequestContext>,
) -> ControlLivenessSnapshot {
    cluster_context
        .and_then(|context| context.control_consensus.as_ref())
        .map(|consensus| consensus.liveness_snapshot())
        .unwrap_or_else(|| {
            let local_node_id = cluster_context
                .map(|context| context.runtime.membership.local_node_id.clone())
                .unwrap_or_else(|| "standalone".to_string());
            ControlLivenessSnapshot::empty(local_node_id)
        })
}

fn cluster_handoff_snapshot(
    cluster_context: Option<&ClusterRequestContext>,
) -> ClusterHandoffSnapshot {
    current_control_state(cluster_context)
        .map(|state| state.handoff_snapshot())
        .unwrap_or_else(ClusterHandoffSnapshot::empty)
}

fn cluster_digest_snapshot(
    cluster_context: Option<&ClusterRequestContext>,
) -> DigestExchangeSnapshot {
    cluster_context
        .and_then(|context| context.digest_runtime.as_ref())
        .map(|runtime| runtime.snapshot())
        .unwrap_or_else(DigestExchangeSnapshot::empty)
}

fn cluster_rebalance_snapshot(
    cluster_context: Option<&ClusterRequestContext>,
) -> RebalanceSchedulerSnapshot {
    cluster_context
        .and_then(|context| context.digest_runtime.as_ref())
        .map(|runtime| runtime.rebalance_snapshot())
        .unwrap_or_else(RebalanceSchedulerSnapshot::empty)
}

#[allow(clippy::too_many_arguments)]
async fn handle_tsdb_status(
    storage: &Arc<dyn Storage>,
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    rbac_registry: Option<&RbacRegistry>,
    security_manager: Option<&SecurityManager>,
    usage_accounting: Option<&UsageAccounting>,
    managed_control_plane: Option<&ManagedControlPlane>,
) -> HttpResponse {
    let tenant_id = match tenant_id_for_text_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        tenant_registry,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let _tenant_request = match tenant_plan.admit(tenant::TenantAdmissionSurface::Metadata, 1) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let storage = tenant::scoped_storage(Arc::clone(storage), tenant_id.clone());
    let memory_used = storage.memory_used();
    let memory_budget = storage.memory_budget();
    let storage = Arc::clone(&storage);
    let result = tokio::task::spawn_blocking(move || {
        let metrics = storage.list_metrics().unwrap_or_default();
        let series_count = metrics.len();
        let observability = storage.observability_snapshot();
        (metrics, series_count, observability)
    })
    .await;

    let (metrics_list, series_count, observability): (
        Vec<MetricSeries>,
        usize,
        tsink::StorageObservabilitySnapshot,
    ) = match result {
        Ok(values) => values,
        Err(err) => return text_response(500, &format!("status task failed: {err}")),
    };
    let cluster_write_metrics = write_routing_metrics_snapshot();
    let cluster_write_labeled_metrics = write_routing_labeled_metrics_snapshot();
    let cluster_fanout_metrics = read_fanout_metrics_snapshot();
    let cluster_fanout_labeled_metrics = read_fanout_labeled_metrics_snapshot();
    let cluster_read_planner_metrics = read_planner_metrics_snapshot();
    let cluster_read_planner_labeled_metrics = read_planner_labeled_metrics_snapshot();
    let mut cluster_read_planner_last_plans = read_planner_last_plans_snapshot();
    let cluster_dedupe_metrics = dedupe_metrics_snapshot();
    let cluster_outbox_metrics = outbox_metrics_snapshot();
    let read_admission_metrics = admission::read_admission_metrics_snapshot();
    let write_admission_metrics = admission::write_admission_metrics_snapshot();
    let tenant_admission_metrics = tenant::tenant_admission_metrics_snapshot();
    let cluster_outbox_peers = cluster_context
        .and_then(|context| context.outbox.as_ref())
        .map(|outbox| outbox.peer_backlog_snapshot())
        .unwrap_or_default();
    let cluster_outbox_stalled_peers = cluster_context
        .and_then(|context| context.outbox.as_ref())
        .map(|outbox| outbox.stalled_peer_snapshot())
        .unwrap_or_default();
    let cluster_outbox_config = cluster_context
        .and_then(|context| context.outbox.as_ref())
        .map(|outbox| outbox.config())
        .unwrap_or_default();
    let cluster_control_liveness = cluster_control_liveness_snapshot(cluster_context);
    let cluster_handoff = cluster_handoff_snapshot(cluster_context);
    let cluster_digest = cluster_digest_snapshot(cluster_context);
    let cluster_rebalance = cluster_rebalance_snapshot(cluster_context);
    let cluster_hotspot =
        cluster_hotspot_snapshot_for_request(&metrics_list, cluster_context, Some(&tenant_id));
    let tenant_runtime_status_json = tenant_registry
        .and_then(|registry| registry.status_snapshot_for(&tenant_id).ok())
        .map(|snapshot| tenant_runtime_status_json(&snapshot))
        .unwrap_or(JsonValue::Null);
    let cluster_read_guardrails =
        cluster_context.map(|context| context.read_fanout.resource_guardrails());
    let read_guardrail_max_queries = cluster_read_guardrails
        .map(|limits| limits.max_inflight_queries)
        .unwrap_or(0);
    let read_guardrail_max_merged_points = cluster_read_guardrails
        .map(|limits| limits.max_inflight_merged_points)
        .unwrap_or(0);
    let read_guardrail_acquire_timeout_ms = cluster_read_guardrails
        .map(|limits| u64::try_from(limits.acquire_timeout.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let read_per_query_fanout_concurrency = cluster_context
        .map(|context| context.runtime.fanout_concurrency)
        .unwrap_or(0);
    let public_read_guardrails = admission::global_public_read_admission()
        .ok()
        .map(|controller| controller.guardrails());
    let public_read_guardrail_max_requests = public_read_guardrails
        .map(|limits| limits.max_inflight_requests)
        .unwrap_or(0);
    let public_read_guardrail_max_queries = public_read_guardrails
        .map(|limits| limits.max_inflight_queries)
        .unwrap_or(0);
    let public_read_guardrail_acquire_timeout_ms = public_read_guardrails
        .map(|limits| u64::try_from(limits.acquire_timeout.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let write_guardrails = admission::global_public_write_admission()
        .ok()
        .map(|controller| controller.guardrails());
    let write_guardrail_max_requests = write_guardrails
        .map(|limits| limits.max_inflight_requests)
        .unwrap_or(0);
    let write_guardrail_max_rows = write_guardrails
        .map(|limits| limits.max_inflight_rows)
        .unwrap_or(0);
    let write_guardrail_acquire_timeout_ms = write_guardrails
        .map(|limits| u64::try_from(limits.acquire_timeout.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let payload_status = payload_status_snapshot(cluster_context);
    let otlp_status = otlp_metrics_status_snapshot();
    let legacy_ingest_status = legacy_ingest::status_snapshot();
    let edge_sync_source_status = edge_sync_context
        .map(|context| context.source_status_snapshot())
        .unwrap_or_default();
    let edge_sync_accept_status = edge_sync_context
        .map(|context| context.accept_status_snapshot())
        .unwrap_or_default();
    let usage_journal = usage_accounting
        .map(UsageAccounting::ledger_status)
        .unwrap_or_default();
    let usage_current_tenant = usage_accounting
        .map(|accounting| accounting.tenant_summary(&tenant_id))
        .map(|summary| {
            json!({
                "tenantId": summary.tenant_id,
                "ingest": summary.ingest,
                "query": summary.query,
                "retention": summary.retention,
                "background": summary.background,
                "latestStorageSnapshot": summary.latest_storage_snapshot,
            })
        })
        .unwrap_or(JsonValue::Null);
    let usage_reconciliation = usage_accounting
        .map(|accounting| {
            let report = accounting.report(Some(&tenant_id), None, None, UsageBucketWidth::None);
            usage_reconciliation_json(&report, &observability)
        })
        .unwrap_or(JsonValue::Null);
    let managed_control_plane_status = managed_control_plane
        .map(ManagedControlPlane::status_snapshot)
        .map(|snapshot| serde_json::to_value(snapshot).unwrap_or(JsonValue::Null))
        .unwrap_or(JsonValue::Null);
    let managed_control_plane_deployments = managed_control_plane
        .map(ManagedControlPlane::deployment_summaries)
        .map(|summaries| serde_json::to_value(summaries).unwrap_or(JsonValue::Null))
        .unwrap_or(JsonValue::Null);
    let managed_control_plane_current_tenant = managed_control_plane
        .and_then(|control_plane| control_plane.tenant_snapshot(&tenant_id))
        .map(|tenant| serde_json::to_value(tenant).unwrap_or(JsonValue::Null))
        .unwrap_or(JsonValue::Null);

    let mut hot_shards = cluster_write_labeled_metrics.shards.clone();
    hot_shards.sort_by(|left, right| {
        right
            .rows_total
            .cmp(&left.rows_total)
            .then_with(|| left.shard.cmp(&right.shard))
    });
    hot_shards.truncate(8);

    let mut write_peers = cluster_write_labeled_metrics.peers.clone();
    write_peers.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    let mut fanout_peers = cluster_fanout_labeled_metrics.peers.clone();
    fanout_peers.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then(left.operation.cmp(&right.operation))
    });
    cluster_read_planner_last_plans.sort_by(|left, right| left.operation.cmp(&right.operation));

    json_response(
        200,
        &json!({
            "status": "success",
            "data": {
                "seriesCount": series_count,
                "memoryUsedBytes": memory_used,
                "memoryBudgetBytes": memory_budget,
                "memory": {
                    "budgetedBytes": observability.memory.budgeted_bytes,
                    "excludedBytes": observability.memory.excluded_bytes,
                    "activeAndSealedBytes": observability.memory.active_and_sealed_bytes,
                    "registryBytes": observability.memory.registry_bytes,
                    "metadataCacheBytes": observability.memory.metadata_cache_bytes,
                    "persistedIndexBytes": observability.memory.persisted_index_bytes,
                    "persistedMmapBytes": observability.memory.persisted_mmap_bytes,
                    "tombstoneBytes": observability.memory.tombstone_bytes,
                    "excludedPersistedMmapBytes": observability.memory.excluded_persisted_mmap_bytes
                },
                "wal": {
                    "enabled": observability.wal.enabled,
                    "syncMode": observability.wal.sync_mode.clone(),
                    "acknowledgedWritesDurable": observability.wal.acknowledged_writes_durable,
                    "sizeBytes": observability.wal.size_bytes,
                    "segmentCount": observability.wal.segment_count,
                    "activeSegment": observability.wal.active_segment,
                    "appendedHighwaterSegment": observability.wal.highwater_segment,
                    "appendedHighwaterFrame": observability.wal.highwater_frame,
                    "highwaterSegment": observability.wal.highwater_segment,
                    "highwaterFrame": observability.wal.highwater_frame,
                    "durableHighwaterSegment": observability.wal.durable_highwater_segment,
                    "durableHighwaterFrame": observability.wal.durable_highwater_frame,
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
                    "evictedSealedChunksTotal": observability.flush.evicted_sealed_chunks_total,
                    "tierMovesTotal": observability.flush.tier_moves_total,
                    "tierMoveErrorsTotal": observability.flush.tier_move_errors_total,
                    "expiredSegmentsTotal": observability.flush.expired_segments_total,
                    "hotSegmentsVisible": observability.flush.hot_segments_visible,
                    "warmSegmentsVisible": observability.flush.warm_segments_visible,
                    "coldSegmentsVisible": observability.flush.cold_segments_visible
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
                    "mergePathShardSnapshotsTotal": observability.query.merge_path_shard_snapshots_total,
                    "mergePathShardSnapshotWaitNanosTotal": observability.query.merge_path_shard_snapshot_wait_nanos_total,
                    "mergePathShardSnapshotHoldNanosTotal": observability.query.merge_path_shard_snapshot_hold_nanos_total,
                    "appendSortPathQueriesTotal": observability.query.append_sort_path_queries_total,
                    "hotOnlyQueryPlansTotal": observability.query.hot_only_query_plans_total,
                    "warmTierQueryPlansTotal": observability.query.warm_tier_query_plans_total,
                    "coldTierQueryPlansTotal": observability.query.cold_tier_query_plans_total,
                    "hotTierPersistedChunksReadTotal": observability.query.hot_tier_persisted_chunks_read_total,
                    "warmTierPersistedChunksReadTotal": observability.query.warm_tier_persisted_chunks_read_total,
                    "coldTierPersistedChunksReadTotal": observability.query.cold_tier_persisted_chunks_read_total,
                    "warmTierFetchDurationNanosTotal": observability.query.warm_tier_fetch_duration_nanos_total,
                    "coldTierFetchDurationNanosTotal": observability.query.cold_tier_fetch_duration_nanos_total,
                    "rollupQueryPlansTotal": observability.query.rollup_query_plans_total,
                    "partialRollupQueryPlansTotal": observability.query.partial_rollup_query_plans_total,
                    "rollupPointsReadTotal": observability.query.rollup_points_read_total
                },
                "rollups": {
                    "workerRunsTotal": observability.rollups.worker_runs_total,
                    "workerSuccessTotal": observability.rollups.worker_success_total,
                    "workerErrorsTotal": observability.rollups.worker_errors_total,
                    "policyRunsTotal": observability.rollups.policy_runs_total,
                    "bucketsMaterializedTotal": observability.rollups.buckets_materialized_total,
                    "pointsMaterializedTotal": observability.rollups.points_materialized_total,
                    "lastRunDurationNanos": observability.rollups.last_run_duration_nanos,
                    "policies": observability.rollups.policies
                },
                "remoteStorage": {
                    "enabled": observability.remote.enabled,
                    "runtimeMode": observability.remote.runtime_mode,
                    "cachePolicy": observability.remote.cache_policy,
                    "metadataRefreshIntervalMs": observability.remote.metadata_refresh_interval_ms,
                    "mirrorHotSegments": observability.remote.mirror_hot_segments,
                    "catalogRefreshesTotal": observability.remote.catalog_refreshes_total,
                    "catalogRefreshErrorsTotal": observability.remote.catalog_refresh_errors_total,
                    "accessible": observability.remote.accessible,
                    "lastRefreshAttemptUnixMs": observability.remote.last_refresh_attempt_unix_ms,
                    "lastSuccessfulRefreshUnixMs": observability.remote.last_successful_refresh_unix_ms,
                    "consecutiveRefreshFailures": observability.remote.consecutive_refresh_failures,
                    "nextRefreshRetryUnixMs": observability.remote.next_refresh_retry_unix_ms,
                    "backoffActive": observability.remote.backoff_active,
                    "lastRefreshError": observability.remote.last_refresh_error
                },
                "prometheusPayloads": {
                    "localCapabilities": payload_status.local_capabilities,
                    "metadata": {
                        "enabled": payload_status.metadata.enabled,
                        "requiredCapabilities": payload_status.metadata.required_capabilities,
                        "acceptedTotal": payload_status.metadata.accepted_total,
                        "rejectedTotal": payload_status.metadata.rejected_total,
                        "throttledTotal": payload_status.metadata.throttled_total,
                        "maxPerRequest": payload_status.metadata.max_per_request
                    },
                    "exemplars": {
                        "enabled": payload_status.exemplars.enabled,
                        "requiredCapabilities": payload_status.exemplars.required_capabilities,
                        "acceptedTotal": payload_status.exemplars.accepted_total,
                        "rejectedTotal": payload_status.exemplars.rejected_total,
                        "throttledTotal": payload_status.exemplars.throttled_total,
                        "maxPerRequest": exemplar_store.config().max_exemplars_per_request
                    },
                    "histograms": {
                        "enabled": payload_status.histograms.enabled,
                        "requiredCapabilities": payload_status.histograms.required_capabilities,
                        "acceptedTotal": payload_status.histograms.accepted_total,
                        "rejectedTotal": payload_status.histograms.rejected_total,
                        "throttledTotal": payload_status.histograms.throttled_total,
                        "maxBucketEntriesPerRequest": payload_status.histograms.max_bucket_entries_per_request
                    }
                },
                "otlpMetrics": {
                    "enabled": otlp_status.enabled,
                    "acceptedRequestsTotal": otlp_status.accepted_requests_total,
                    "rejectedRequestsTotal": otlp_status.rejected_requests_total,
                    "acceptedExemplarsTotal": otlp_status.accepted_exemplars_total,
                    "rejectedExemplarsTotal": otlp_status.rejected_exemplars_total,
                    "supportedShapes": otlp_status.supported_shapes,
                    "gauges": {
                        "acceptedTotal": otlp_status.gauges.accepted_total,
                        "rejectedTotal": otlp_status.gauges.rejected_total
                    },
                    "sums": {
                        "acceptedTotal": otlp_status.sums.accepted_total,
                        "rejectedTotal": otlp_status.sums.rejected_total
                    },
                    "histograms": {
                        "acceptedTotal": otlp_status.histograms.accepted_total,
                        "rejectedTotal": otlp_status.histograms.rejected_total
                    },
                    "summaries": {
                        "acceptedTotal": otlp_status.summaries.accepted_total,
                        "rejectedTotal": otlp_status.summaries.rejected_total
                    },
                    "exponentialHistograms": {
                        "acceptedTotal": otlp_status.exponential_histograms.accepted_total,
                        "rejectedTotal": otlp_status.exponential_histograms.rejected_total
                    }
                },
                "legacyIngest": {
                    "influxLineProtocol": {
                        "enabled": legacy_ingest_status.influx.enabled,
                        "maxLinesPerRequest": legacy_ingest_status.influx.max_lines_per_request,
                        "acceptedRequestsTotal": legacy_ingest_status.influx.counters.accepted_requests_total,
                        "rejectedRequestsTotal": legacy_ingest_status.influx.counters.rejected_requests_total,
                        "throttledRequestsTotal": legacy_ingest_status.influx.counters.throttled_requests_total,
                        "acceptedSamplesTotal": legacy_ingest_status.influx.counters.accepted_samples_total,
                        "rejectedSamplesTotal": legacy_ingest_status.influx.counters.rejected_samples_total
                    },
                    "statsd": {
                        "enabled": legacy_ingest_status.statsd.enabled,
                        "maxPacketBytes": legacy_ingest_status.statsd.max_packet_bytes,
                        "maxEventsPerPacket": legacy_ingest_status.statsd.max_events_per_packet,
                        "acceptedRequestsTotal": legacy_ingest_status.statsd.counters.accepted_requests_total,
                        "rejectedRequestsTotal": legacy_ingest_status.statsd.counters.rejected_requests_total,
                        "throttledRequestsTotal": legacy_ingest_status.statsd.counters.throttled_requests_total,
                        "acceptedSamplesTotal": legacy_ingest_status.statsd.counters.accepted_samples_total,
                        "rejectedSamplesTotal": legacy_ingest_status.statsd.counters.rejected_samples_total
                    },
                    "graphite": {
                        "enabled": legacy_ingest_status.graphite.enabled,
                        "maxLineBytes": legacy_ingest_status.graphite.max_line_bytes,
                        "acceptedRequestsTotal": legacy_ingest_status.graphite.counters.accepted_requests_total,
                        "rejectedRequestsTotal": legacy_ingest_status.graphite.counters.rejected_requests_total,
                        "throttledRequestsTotal": legacy_ingest_status.graphite.counters.throttled_requests_total,
                        "acceptedSamplesTotal": legacy_ingest_status.graphite.counters.accepted_samples_total,
                        "rejectedSamplesTotal": legacy_ingest_status.graphite.counters.rejected_samples_total
                    }
                },
                "edgeSync": {
                    "source": {
                        "enabled": edge_sync_source_status.enabled,
                        "sourceId": edge_sync_source_status.source_id,
                        "upstreamEndpoint": edge_sync_source_status.upstream_endpoint,
                        "tenantMappingMode": edge_sync_source_status.tenant_mapping_mode,
                        "staticTenantId": edge_sync_source_status.static_tenant_id,
                        "conflictSemantics": edge_sync_source_status.conflict_semantics,
                        "queuedEntries": edge_sync_source_status.queued_entries,
                        "queuedBytes": edge_sync_source_status.queued_bytes,
                        "logBytes": edge_sync_source_status.log_bytes,
                        "oldestQueuedAgeMs": edge_sync_source_status.oldest_queued_age_ms,
                        "maxEntries": edge_sync_source_status.max_entries,
                        "maxBytes": edge_sync_source_status.max_bytes,
                        "maxLogBytes": edge_sync_source_status.max_log_bytes,
                        "maxRecordBytes": edge_sync_source_status.max_record_bytes,
                        "replayIntervalSecs": edge_sync_source_status.replay_interval_secs,
                        "replayBatchSize": edge_sync_source_status.replay_batch_size,
                        "maxBackoffSecs": edge_sync_source_status.max_backoff_secs,
                        "cleanupIntervalSecs": edge_sync_source_status.cleanup_interval_secs,
                        "preAckRetentionSecs": edge_sync_source_status.pre_ack_retention_secs,
                        "enqueuedTotal": edge_sync_source_status.enqueued_total,
                        "enqueueRejectedTotal": edge_sync_source_status.enqueue_rejected_total,
                        "replayAttemptsTotal": edge_sync_source_status.replay_attempts_total,
                        "replaySuccessTotal": edge_sync_source_status.replay_success_total,
                        "replayFailuresTotal": edge_sync_source_status.replay_failures_total,
                        "replayedRowsTotal": edge_sync_source_status.replayed_rows_total,
                        "cleanupRunsTotal": edge_sync_source_status.cleanup_runs_total,
                        "expiredEntriesTotal": edge_sync_source_status.expired_entries_total,
                        "expiredBytesTotal": edge_sync_source_status.expired_bytes_total,
                        "lastSuccessfulReplayUnixMs": edge_sync_source_status.last_successful_replay_unix_ms,
                        "lastEnqueueError": edge_sync_source_status.last_enqueue_error,
                        "lastReplayError": edge_sync_source_status.last_replay_error,
                        "degraded": edge_sync_source_status.degraded
                    },
                    "accept": {
                        "enabled": edge_sync_accept_status.enabled,
                        "dedupeWindowSecs": edge_sync_accept_status.dedupe_window_secs,
                        "maxEntries": edge_sync_accept_status.max_entries,
                        "maxLogBytes": edge_sync_accept_status.max_log_bytes,
                        "cleanupIntervalSecs": edge_sync_accept_status.cleanup_interval_secs,
                        "requestsTotal": edge_sync_accept_status.requests_total,
                        "acceptedTotal": edge_sync_accept_status.accepted_total,
                        "duplicatesTotal": edge_sync_accept_status.duplicates_total,
                        "inflightRejectionsTotal": edge_sync_accept_status.inflight_rejections_total,
                        "commitsTotal": edge_sync_accept_status.commits_total,
                        "abortsTotal": edge_sync_accept_status.aborts_total,
                        "cleanupRunsTotal": edge_sync_accept_status.cleanup_runs_total,
                        "expiredKeysTotal": edge_sync_accept_status.expired_keys_total,
                        "evictedKeysTotal": edge_sync_accept_status.evicted_keys_total,
                        "persistenceFailuresTotal": edge_sync_accept_status.persistence_failures_total,
                        "activeKeys": edge_sync_accept_status.active_keys,
                        "inflightKeys": edge_sync_accept_status.inflight_keys,
                        "logBytes": edge_sync_accept_status.log_bytes
                    }
                },
                "usageAccounting": {
                    "journal": usage_journal,
                    "currentTenant": usage_current_tenant,
                    "reconciliation": usage_reconciliation
                },
                "managedControlPlane": {
                    "status": managed_control_plane_status,
                    "deployments": managed_control_plane_deployments,
                    "currentTenant": managed_control_plane_current_tenant
                },
                "admission": {
                    "publicRead": {
                        "rejectionsTotal": read_admission_metrics.rejections_total,
                        "requestSlotRejectionsTotal": read_admission_metrics.request_slot_rejections_total,
                        "queryBudgetRejectionsTotal": read_admission_metrics.query_budget_rejections_total,
                        "oversizeQueriesRejectionsTotal": read_admission_metrics.oversize_queries_rejections_total,
                        "acquireWaitNanosTotal": read_admission_metrics.acquire_wait_nanos_total,
                        "activeRequests": read_admission_metrics.active_requests,
                        "activeQueries": read_admission_metrics.active_queries,
                        "globalMaxInflightRequests": public_read_guardrail_max_requests,
                        "globalMaxInflightQueries": public_read_guardrail_max_queries,
                        "globalAcquireTimeoutMs": public_read_guardrail_acquire_timeout_ms
                    },
                    "publicWrite": {
                        "rejectionsTotal": write_admission_metrics.rejections_total,
                        "requestSlotRejectionsTotal": write_admission_metrics.request_slot_rejections_total,
                        "rowBudgetRejectionsTotal": write_admission_metrics.row_budget_rejections_total,
                        "oversizeRowsRejectionsTotal": write_admission_metrics.oversize_rows_rejections_total,
                        "acquireWaitNanosTotal": write_admission_metrics.acquire_wait_nanos_total,
                        "activeRequests": write_admission_metrics.active_requests,
                        "activeRows": write_admission_metrics.active_rows,
                        "globalMaxInflightRequests": write_guardrail_max_requests,
                        "globalMaxInflightRows": write_guardrail_max_rows,
                        "globalAcquireTimeoutMs": write_guardrail_acquire_timeout_ms
                    },
                    "tenant": {
                        "readRejectionsTotal": tenant_admission_metrics.read_rejections_total,
                        "writeRejectionsTotal": tenant_admission_metrics.write_rejections_total,
                        "activeReads": tenant_admission_metrics.active_reads,
                        "activeWrites": tenant_admission_metrics.active_writes,
                        "surfaceRejectionsTotal": {
                            "ingest": tenant_admission_metrics.ingest_rejections_total,
                            "query": tenant_admission_metrics.query_rejections_total,
                            "metadata": tenant_admission_metrics.metadata_rejections_total,
                            "retention": tenant_admission_metrics.retention_rejections_total
                        },
                        "surfaceActiveRequests": {
                            "ingest": tenant_admission_metrics.ingest_active_requests,
                            "query": tenant_admission_metrics.query_active_requests,
                            "metadata": tenant_admission_metrics.metadata_active_requests,
                            "retention": tenant_admission_metrics.retention_active_requests
                        },
                        "surfaceActiveUnits": {
                            "ingest": tenant_admission_metrics.ingest_active_units,
                            "query": tenant_admission_metrics.query_active_units,
                            "metadata": tenant_admission_metrics.metadata_active_units,
                            "retention": tenant_admission_metrics.retention_active_units
                        },
                        "currentTenant": tenant_runtime_status_json
                    }
                },
                "cluster": {
                    "localNodeRole": cluster_context.map(|context| context.runtime.local_node_role.to_string()),
                    "localReadsServeGlobalQueries": cluster_context
                        .map(|context| context.runtime.local_reads_serve_global_queries)
                        .unwrap_or(false),
                    "writeRouting": {
                        "requestsTotal": cluster_write_metrics.requests_total,
                        "localRowsTotal": cluster_write_metrics.local_rows_total,
                        "routedRowsTotal": cluster_write_metrics.routed_rows_total,
                        "routedBatchesTotal": cluster_write_metrics.routed_batches_total,
                        "failuresTotal": cluster_write_metrics.failures_total,
                        "hotShards": hot_shards.iter().map(|item| {
                            json!({
                                "shard": item.shard,
                                "rowsTotal": item.rows_total
                            })
                        }).collect::<Vec<_>>(),
                        "peers": write_peers.iter().map(|peer| {
                            json!({
                                "nodeId": peer.node_id,
                                "routedRowsTotal": peer.routed_rows_total,
                                "routedBatchesTotal": peer.routed_batches_total,
                                "remoteRequestsTotal": peer.remote_requests_total,
                                "remoteFailuresTotal": peer.remote_failures_total,
                                "remoteRequestDurationNanosTotal": peer.remote_request_duration_nanos_total,
                                "remoteRequestDurationCount": peer.remote_request_duration_count
                            })
                        }).collect::<Vec<_>>()
                    },
                    "readFanout": {
                        "requestsTotal": cluster_fanout_metrics.requests_total,
                        "failuresTotal": cluster_fanout_metrics.failures_total,
                        "durationNanosTotal": cluster_fanout_metrics.duration_nanos_total,
                        "remoteRequestsTotal": cluster_fanout_metrics.remote_requests_total,
                        "remoteFailuresTotal": cluster_fanout_metrics.remote_failures_total,
                        "resourceRejectionsTotal": cluster_fanout_metrics.resource_rejections_total,
                        "resourceAcquireWaitNanosTotal": cluster_fanout_metrics.resource_acquire_wait_nanos_total,
                        "resourceActiveQueries": cluster_fanout_metrics.resource_active_queries,
                        "resourceActiveMergedPoints": cluster_fanout_metrics.resource_active_merged_points,
                        "perQueryFanoutConcurrency": read_per_query_fanout_concurrency,
                        "globalMaxInflightQueries": read_guardrail_max_queries,
                        "globalMaxInflightMergedPoints": read_guardrail_max_merged_points,
                        "globalAcquireTimeoutMs": read_guardrail_acquire_timeout_ms,
                        "operations": cluster_fanout_labeled_metrics.operations.iter().map(|item| {
                            json!({
                                "operation": item.operation,
                                "requestsTotal": item.requests_total,
                                "failuresTotal": item.failures_total
                            })
                        }).collect::<Vec<_>>(),
                        "peers": fanout_peers.iter().map(|peer| {
                            json!({
                                "nodeId": peer.node_id,
                                "operation": peer.operation,
                                "remoteRequestsTotal": peer.remote_requests_total,
                                "remoteFailuresTotal": peer.remote_failures_total,
                                "remoteRequestDurationNanosTotal": peer.remote_request_duration_nanos_total,
                                "remoteRequestDurationCount": peer.remote_request_duration_count
                            })
                        }).collect::<Vec<_>>()
                    },
                    "readPlanning": {
                        "requestsTotal": cluster_read_planner_metrics.requests_total,
                        "candidateShardsTotal": cluster_read_planner_metrics.candidate_shards_total,
                        "prunedShardsTotal": cluster_read_planner_metrics.pruned_shards_total,
                        "localShardsTotal": cluster_read_planner_metrics.local_shards_total,
                        "remoteTargetsTotal": cluster_read_planner_metrics.remote_targets_total,
                        "remoteShardsTotal": cluster_read_planner_metrics.remote_shards_total,
                        "operations": cluster_read_planner_labeled_metrics.operations.iter().map(|item| {
                            json!({
                                "operation": item.operation,
                                "requestsTotal": item.requests_total,
                                "candidateShardsTotal": item.candidate_shards_total,
                                "prunedShardsTotal": item.pruned_shards_total,
                                "remoteTargetsTotal": item.remote_targets_total
                            })
                        }).collect::<Vec<_>>(),
                        "lastPlans": cluster_read_planner_last_plans.iter().map(|item| {
                            json!({
                                "operation": item.operation,
                                "ringVersion": item.ring_version,
                                "timeRange": item.time_range.map(|(start, end)| json!({"start": start, "end": end})),
                                "candidateShards": item.candidate_shards,
                                "prunedShards": item.pruned_shards,
                                "localShards": item.local_shards,
                                "remoteTargets": item.remote_targets,
                                "remoteShards": item.remote_shards
                            })
                        }).collect::<Vec<_>>()
                    },
                    "writeIdempotency": {
                        "requestsTotal": cluster_dedupe_metrics.requests_total,
                        "acceptedTotal": cluster_dedupe_metrics.accepted_total,
                        "duplicatesTotal": cluster_dedupe_metrics.duplicates_total,
                        "inflightRejectionsTotal": cluster_dedupe_metrics.inflight_rejections_total,
                        "commitsTotal": cluster_dedupe_metrics.commits_total,
                        "abortsTotal": cluster_dedupe_metrics.aborts_total,
                        "cleanupRunsTotal": cluster_dedupe_metrics.cleanup_runs_total,
                        "expiredKeysTotal": cluster_dedupe_metrics.expired_keys_total,
                        "evictedKeysTotal": cluster_dedupe_metrics.evicted_keys_total,
                        "persistenceFailuresTotal": cluster_dedupe_metrics.persistence_failures_total,
                        "activeKeys": cluster_dedupe_metrics.active_keys,
                        "inflightKeys": cluster_dedupe_metrics.inflight_keys,
                        "logBytes": cluster_dedupe_metrics.log_bytes
                    },
                    "writeOutbox": {
                        "enqueuedTotal": cluster_outbox_metrics.enqueued_total,
                        "enqueueRejectedTotal": cluster_outbox_metrics.enqueue_rejected_total,
                        "persistenceFailuresTotal": cluster_outbox_metrics.persistence_failures_total,
                        "replayAttemptsTotal": cluster_outbox_metrics.replay_attempts_total,
                        "replaySuccessTotal": cluster_outbox_metrics.replay_success_total,
                        "replayFailuresTotal": cluster_outbox_metrics.replay_failures_total,
                        "queuedEntries": cluster_outbox_metrics.queued_entries,
                        "queuedBytes": cluster_outbox_metrics.queued_bytes,
                        "logBytes": cluster_outbox_metrics.log_bytes,
                        "staleRecords": cluster_outbox_metrics.stale_records,
                        "cleanupRunsTotal": cluster_outbox_metrics.cleanup_runs_total,
                        "cleanupCompactionsTotal": cluster_outbox_metrics.cleanup_compactions_total,
                        "cleanupReclaimedBytesTotal": cluster_outbox_metrics.cleanup_reclaimed_bytes_total,
                        "cleanupFailuresTotal": cluster_outbox_metrics.cleanup_failures_total,
                        "stalledAlertsTotal": cluster_outbox_metrics.stalled_alerts_total,
                        "stalledPeers": cluster_outbox_metrics.stalled_peers,
                        "stalledOldestAgeMs": cluster_outbox_metrics.stalled_oldest_age_ms,
                        "stalledPeerAgeSecs": cluster_outbox_config.stalled_peer_age_secs,
                        "stalledPeerMinEntries": cluster_outbox_config.stalled_peer_min_entries,
                        "stalledPeerMinBytes": cluster_outbox_config.stalled_peer_min_bytes,
                        "peers": cluster_outbox_peers.iter().map(|peer| {
                            json!({
                                "nodeId": peer.node_id,
                                "queuedEntries": peer.queued_entries,
                                "queuedBytes": peer.queued_bytes,
                                "oldestEnqueuedUnixMs": peer.oldest_enqueued_unix_ms
                            })
                        }).collect::<Vec<_>>(),
                        "stalledPeerDetails": cluster_outbox_stalled_peers.iter().map(|peer| {
                            json!({
                                "nodeId": peer.node_id,
                                "queuedEntries": peer.queued_entries,
                                "queuedBytes": peer.queued_bytes,
                                "oldestEnqueuedUnixMs": peer.oldest_enqueued_unix_ms,
                                "oldestAgeMs": peer.oldest_age_ms,
                                "firstStalledUnixMs": peer.first_stalled_unix_ms
                            })
                        }).collect::<Vec<_>>()
                    },
                    "control": {
                        "localNodeId": cluster_control_liveness.local_node_id.clone(),
                        "currentTerm": cluster_control_liveness.current_term,
                        "commitIndex": cluster_control_liveness.commit_index,
                        "leaderNodeId": cluster_control_liveness.leader_node_id.clone(),
                        "leaderStale": cluster_control_liveness.leader_stale,
                        "leaderLastContactUnixMs": cluster_control_liveness.leader_last_contact_unix_ms,
                        "leaderContactAgeMs": cluster_control_liveness.leader_contact_age_ms,
                        "suspectPeers": cluster_control_liveness.suspect_peers,
                        "deadPeers": cluster_control_liveness.dead_peers,
                        "peers": cluster_control_liveness.peers.iter().map(|peer| {
                            json!({
                                "nodeId": peer.node_id,
                                "status": peer.status.as_str(),
                                "lastSuccessUnixMs": peer.last_success_unix_ms,
                                "lastFailureUnixMs": peer.last_failure_unix_ms,
                                "consecutiveFailures": peer.consecutive_failures
                            })
                        }).collect::<Vec<_>>()
                    },
                    "handoff": {
                        "totalShards": cluster_handoff.total_shards,
                        "inProgressShards": cluster_handoff.in_progress_shards,
                        "warmupShards": cluster_handoff.warmup_shards,
                        "cutoverShards": cluster_handoff.cutover_shards,
                        "finalSyncShards": cluster_handoff.final_sync_shards,
                        "completedShards": cluster_handoff.completed_shards,
                        "failedShards": cluster_handoff.failed_shards,
                        "resumedShards": cluster_handoff.resumed_shards,
                        "copiedRowsTotal": cluster_handoff.copied_rows_total,
                        "pendingRowsTotal": cluster_handoff.pending_rows_total,
                        "shards": cluster_handoff.shards.iter().map(|shard| {
                            json!({
                                "shard": shard.shard,
                                "fromNodeId": shard.from_node_id,
                                "toNodeId": shard.to_node_id,
                                "activationRingVersion": shard.activation_ring_version,
                                "phase": shard.phase.as_str(),
                                "copiedRows": shard.copied_rows,
                                "pendingRows": shard.pending_rows,
                                "resumedCount": shard.resumed_count,
                                "startedUnixMs": shard.started_unix_ms,
                                "updatedUnixMs": shard.updated_unix_ms,
                                "lastError": shard.last_error.clone()
                            })
                        }).collect::<Vec<_>>()
                    },
                    "digestExchange": {
                        "intervalSecs": cluster_digest.interval_secs,
                        "windowSecs": cluster_digest.window_secs,
                        "maxShardsPerTick": cluster_digest.max_shards_per_tick,
                        "maxMismatchReports": cluster_digest.max_mismatch_reports,
                        "maxBytesPerTick": cluster_digest.max_bytes_per_tick,
                        "maxRepairMismatchesPerTick": cluster_digest.max_repair_mismatches_per_tick,
                        "maxRepairSeriesPerTick": cluster_digest.max_repair_series_per_tick,
                        "maxRepairRowsPerTick": cluster_digest.max_repair_rows_per_tick,
                        "maxRepairRuntimeMsPerTick": cluster_digest.max_repair_runtime_ms_per_tick,
                        "repairFailureBackoffSecs": cluster_digest.repair_failure_backoff_secs,
                        "repairPaused": cluster_digest.repair_paused,
                        "repairCancelGeneration": cluster_digest.repair_cancel_generation,
                        "repairCancellationsTotal": cluster_digest.repair_cancellations_total,
                        "runsTotal": cluster_digest.runs_total,
                        "windowsComparedTotal": cluster_digest.windows_compared_total,
                        "windowsSuccessTotal": cluster_digest.windows_success_total,
                        "windowsFailedTotal": cluster_digest.windows_failed_total,
                        "localComputeFailuresTotal": cluster_digest.local_compute_failures_total,
                        "mismatchesTotal": cluster_digest.mismatches_total,
                        "bytesExchangedTotal": cluster_digest.bytes_exchanged_total,
                        "bytesExchangedLastRun": cluster_digest.bytes_exchanged_last_run,
                        "budgetExhaustionsTotal": cluster_digest.budget_exhaustions_total,
                        "windowsSkippedBudgetTotal": cluster_digest.windows_skipped_budget_total,
                        "budgetExhaustedLastRun": cluster_digest.budget_exhausted_last_run,
                        "lastRunUnixMs": cluster_digest.last_run_unix_ms,
                        "lastSuccessUnixMs": cluster_digest.last_success_unix_ms,
                        "lastRingVersion": cluster_digest.last_ring_version,
                        "comparedShardsLastRun": cluster_digest.compared_shards_last_run,
                        "prioritizedShardsLastRun": cluster_digest.prioritized_shards_last_run,
                        "comparedPeersLastRun": cluster_digest.compared_peers_last_run,
                        "repairsAttemptedTotal": cluster_digest.repairs_attempted_total,
                        "repairsSucceededTotal": cluster_digest.repairs_succeeded_total,
                        "repairsFailedTotal": cluster_digest.repairs_failed_total,
                        "repairsSkippedBudgetTotal": cluster_digest.repairs_skipped_budget_total,
                        "repairsSkippedNonAdditiveTotal": cluster_digest.repairs_skipped_non_additive_total,
                        "repairsSkippedBackoffTotal": cluster_digest.repairs_skipped_backoff_total,
                        "repairsSkippedPausedTotal": cluster_digest.repairs_skipped_paused_total,
                        "repairsSkippedTimeBudgetTotal": cluster_digest.repairs_skipped_time_budget_total,
                        "repairsCancelledTotal": cluster_digest.repairs_cancelled_total,
                        "repairTimeBudgetExhaustionsTotal": cluster_digest.repair_time_budget_exhaustions_total,
                        "repairTimeBudgetExhaustedLastRun": cluster_digest.repair_time_budget_exhausted_last_run,
                        "repairSeriesScannedTotal": cluster_digest.repair_series_scanned_total,
                        "repairRowsScannedTotal": cluster_digest.repair_rows_scanned_total,
                        "repairRowsInsertedTotal": cluster_digest.repair_rows_inserted_total,
                        "repairsAttemptedLastRun": cluster_digest.repairs_attempted_last_run,
                        "repairsSucceededLastRun": cluster_digest.repairs_succeeded_last_run,
                        "repairsFailedLastRun": cluster_digest.repairs_failed_last_run,
                        "repairsCancelledLastRun": cluster_digest.repairs_cancelled_last_run,
                        "repairsSkippedBackoffLastRun": cluster_digest.repairs_skipped_backoff_last_run,
                        "repairRowsInsertedLastRun": cluster_digest.repair_rows_inserted_last_run,
                        "lastError": cluster_digest.last_error.clone(),
                        "mismatches": cluster_digest
                            .mismatches
                            .iter()
                            .map(|mismatch| serde_json::to_value(mismatch).unwrap_or(JsonValue::Null))
                            .collect::<Vec<_>>()
                    },
                    "rebalance": {
                        "intervalSecs": cluster_rebalance.interval_secs,
                        "maxRowsPerTick": cluster_rebalance.max_rows_per_tick,
                        "maxShardsPerTick": cluster_rebalance.max_shards_per_tick,
                        "effectiveMaxRowsPerTickLastRun": cluster_rebalance.effective_max_rows_per_tick_last_run,
                        "paused": cluster_rebalance.paused,
                        "isLocalControlLeader": cluster_rebalance.is_local_control_leader,
                        "activeJobs": cluster_rebalance.active_jobs,
                        "runsTotal": cluster_rebalance.runs_total,
                        "jobsConsideredLastRun": cluster_rebalance.jobs_considered_last_run,
                        "jobsAdvancedTotal": cluster_rebalance.jobs_advanced_total,
                        "jobsCompletedTotal": cluster_rebalance.jobs_completed_total,
                        "rowsScheduledTotal": cluster_rebalance.rows_scheduled_total,
                        "rowsScheduledLastRun": cluster_rebalance.rows_scheduled_last_run,
                        "proposalsCommittedTotal": cluster_rebalance.proposals_committed_total,
                        "proposalsPendingTotal": cluster_rebalance.proposals_pending_total,
                        "proposalFailuresTotal": cluster_rebalance.proposal_failures_total,
                        "movesBlockedBySloTotal": cluster_rebalance.moves_blocked_by_slo_total,
                        "sloGuard": {
                            "writePressureRatio": cluster_rebalance.slo_guard.write_pressure_ratio,
                            "queryPressureRatio": cluster_rebalance.slo_guard.query_pressure_ratio,
                            "clusterQueryPressureRatio": cluster_rebalance.slo_guard.cluster_query_pressure_ratio,
                            "effectiveMaxRowsPerTick": cluster_rebalance.slo_guard.effective_max_rows_per_tick,
                            "blockNewHandoffs": cluster_rebalance.slo_guard.block_new_handoffs,
                            "reason": cluster_rebalance.slo_guard.reason.clone()
                        },
                        "lastRunUnixMs": cluster_rebalance.last_run_unix_ms,
                        "lastSuccessUnixMs": cluster_rebalance.last_success_unix_ms,
                        "lastError": cluster_rebalance.last_error.clone(),
                        "candidateMoves": cluster_rebalance.candidate_moves.iter().map(|candidate| {
                            json!({
                                "shard": candidate.shard,
                                "fromNodeId": candidate.from_node_id,
                                "toNodeId": candidate.to_node_id,
                                "pressureScore": candidate.pressure_score,
                                "movementCostScore": candidate.movement_cost_score,
                                "imbalanceImprovementScore": candidate.imbalance_improvement_score,
                                "decisionScore": candidate.decision_score,
                                "sourceNodePressure": candidate.source_node_pressure,
                                "targetNodePressure": candidate.target_node_pressure,
                                "reason": candidate.reason
                            })
                        }).collect::<Vec<_>>(),
                        "jobs": cluster_rebalance.jobs.iter().map(|job| {
                            json!({
                                "shard": job.shard,
                                "fromNodeId": job.from_node_id,
                                "toNodeId": job.to_node_id,
                                "activationRingVersion": job.activation_ring_version,
                                "phase": job.phase.as_str(),
                                "copiedRows": job.copied_rows,
                                "pendingRows": job.pending_rows,
                                "updatedUnixMs": job.updated_unix_ms
                            })
                        }).collect::<Vec<_>>()
                    },
                    "hotspot": {
                        "generatedUnixMs": cluster_hotspot.generated_unix_ms,
                        "skewedShards": cluster_hotspot.skewed_shards,
                        "skewedTenants": cluster_hotspot.skewed_tenants,
                        "maxShardScore": cluster_hotspot.max_shard_score,
                        "maxTenantScore": cluster_hotspot.max_tenant_score,
                        "hotShards": cluster_hotspot.hot_shards.iter().map(|item| {
                            json!({
                                "shard": item.shard,
                                "ingestRowsTotal": item.ingest_rows_total,
                                "queryShardHitsTotal": item.query_shard_hits_total,
                                "storageSeries": item.storage_series,
                                "repairMismatchesTotal": item.repair_mismatches_total,
                                "repairSeriesGapTotal": item.repair_series_gap_total,
                                "repairPointGapTotal": item.repair_point_gap_total,
                                "repairRowsInsertedTotal": item.repair_rows_inserted_total,
                                "handoffPendingRows": item.handoff_pending_rows,
                                "pressureScore": item.pressure_score,
                                "movementCostScore": item.movement_cost_score,
                                "skewFactor": item.skew_factor,
                                "recommendMove": item.recommend_move
                            })
                        }).collect::<Vec<_>>(),
                        "tenantPressure": cluster_hotspot.tenant_hotspots.iter().map(|item| {
                            json!({
                                "tenantId": item.tenant_id,
                                "ingestRowsTotal": item.ingest_rows_total,
                                "queryRequestsTotal": item.query_requests_total,
                                "queryUnitsTotal": item.query_units_total,
                                "storageSeries": item.storage_series,
                                "repairRowsInsertedTotal": item.repair_rows_inserted_total,
                                "pressureScore": item.pressure_score,
                                "skewFactor": item.skew_factor
                            })
                        }).collect::<Vec<_>>()
                    },
                    "security": security_status_json(security_manager, rbac_registry)
                }
            }
        }),
    )
}

fn support_bundle_tenant_id(request: &HttpRequest) -> Result<String, HttpResponse> {
    let mut tenant_request = request.clone();
    if let Some(tenant_id) = non_empty_param(
        request
            .param("tenant")
            .or_else(|| request.param("tenantId"))
            .or_else(|| request.param("tenant_id")),
    ) {
        tenant_request
            .headers
            .insert(tenant::TENANT_HEADER.to_string(), tenant_id);
    }
    tenant::tenant_id_for_request(&tenant_request).map_err(|err| text_response(400, &err))
}

fn support_bundle_request_with_path(
    request: &HttpRequest,
    path: impl Into<String>,
    tenant_id: &str,
) -> HttpRequest {
    let mut headers = request.headers.clone();
    headers.insert(
        rbac::RBAC_AUTH_VERIFIED_HEADER.to_string(),
        "true".to_string(),
    );
    headers.insert(tenant::TENANT_HEADER.to_string(), tenant_id.to_string());
    HttpRequest {
        method: "GET".to_string(),
        path: path.into(),
        headers,
        body: Vec::new(),
    }
}

fn support_bundle_filename_component(value: &str) -> String {
    let cleaned = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if cleaned.is_empty() {
        "default".to_string()
    } else {
        cleaned
    }
}

fn support_bundle_text_section(status: u16, message: impl Into<String>) -> JsonValue {
    json!({
        "httpStatus": status,
        "bodyText": message.into(),
    })
}

fn truncate_support_bundle_text(input: &str, max_chars: usize) -> String {
    let mut truncated = input.chars().take(max_chars).collect::<String>();
    if input.chars().count() > max_chars {
        truncated.push_str("...[truncated]");
    }
    truncated
}

fn support_bundle_section_from_response(response: HttpResponse) -> JsonValue {
    let mut section = serde_json::Map::new();
    section.insert("httpStatus".to_string(), json!(response.status));
    if let Some((_, value)) = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
    {
        section.insert("contentType".to_string(), json!(value));
    }
    if let Ok(body) = serde_json::from_slice::<JsonValue>(&response.body) {
        section.insert("body".to_string(), body);
    } else {
        let body_text = String::from_utf8_lossy(&response.body);
        section.insert(
            "bodyText".to_string(),
            JsonValue::String(truncate_support_bundle_text(&body_text, 8 * 1024)),
        );
    }
    JsonValue::Object(section)
}

fn support_bundle_usage_section(
    storage: &Arc<dyn Storage>,
    usage_accounting: Option<&UsageAccounting>,
    tenant_id: &str,
) -> JsonValue {
    let Some(usage_accounting) = usage_accounting else {
        return support_bundle_text_section(503, "usage accounting is unavailable");
    };
    let report = usage_accounting.report(Some(tenant_id), None, None, UsageBucketWidth::None);
    json!({
        "httpStatus": 200,
        "body": {
            "status": "success",
            "data": {
                "report": report,
                "journal": usage_accounting.ledger_status(),
                "reconciliation": usage_reconciliation_json(&report, &storage.observability_snapshot())
            }
        }
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AdminSecretRotateRequest {
    target: SecretRotationTarget,
    #[serde(default)]
    mode: SecretRotationMode,
    #[serde(default)]
    new_value: Option<String>,
    #[serde(default)]
    overlap_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AdminServiceAccountIdRequest {
    id: String,
}

fn handle_admin_rbac_service_account_disablement(
    request: &HttpRequest,
    rbac_registry: Option<&RbacRegistry>,
    disabled: bool,
) -> HttpResponse {
    let Some(rbac_registry) = rbac_registry else {
        return text_response(503, "rbac registry is not configured");
    };
    let id = match parse_admin_service_account_id(request) {
        Ok(id) => id,
        Err(response) => return response,
    };
    match rbac_registry.set_service_account_disabled(&id, disabled) {
        Ok(service_account) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "serviceAccount": service_account,
                }
            }),
        ),
        Err(err) => text_response(400, &err),
    }
}

fn parse_admin_service_account_id(request: &HttpRequest) -> Result<String, HttpResponse> {
    let payload = match parse_optional_json_body::<AdminServiceAccountIdRequest>(request) {
        Ok(payload) => payload,
        Err(err) => return Err(text_response(400, &err)),
    };
    let id = request
        .param("id")
        .or_else(|| payload.map(|payload| payload.id))
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
        .ok_or_else(|| text_response(400, "missing required parameter 'id'"))?;
    Ok(id)
}

fn rbac_disabled_state_snapshot() -> rbac::RbacStateSnapshot {
    rbac::RbacStateSnapshot {
        enabled: false,
        source_path: None,
        last_loaded_unix_ms: 0,
        roles: Vec::new(),
        principals: Vec::new(),
        service_accounts: Vec::new(),
        oidc_providers: Vec::new(),
        audit_entries: 0,
    }
}

fn parse_usage_bucket_width(request: &HttpRequest) -> Result<UsageBucketWidth, HttpResponse> {
    let Some(bucket) = non_empty_param(
        request
            .param("bucket")
            .or_else(|| request.param("bucketWidth")),
    ) else {
        return Ok(UsageBucketWidth::Hour);
    };
    match bucket.to_ascii_lowercase().as_str() {
        "none" => Ok(UsageBucketWidth::None),
        "hour" | "hourly" => Ok(UsageBucketWidth::Hour),
        "day" | "daily" => Ok(UsageBucketWidth::Day),
        _ => Err(text_response(
            400,
            "invalid usage bucket: expected none|hour|day",
        )),
    }
}

type ParsedUsageReportFilter = (
    Option<String>,
    Option<u64>,
    Option<u64>,
    UsageBucketWidth,
    bool,
);

fn parse_usage_report_filter(
    request: &HttpRequest,
) -> Result<ParsedUsageReportFilter, HttpResponse> {
    let tenant_id = non_empty_param(
        request
            .param("tenant")
            .or_else(|| request.param("tenantId"))
            .or_else(|| request.param("tenant_id")),
    );
    let start_unix_ms = match request
        .param("start")
        .or_else(|| request.param("startUnixMs"))
    {
        Some(value) => {
            Some(parse_admin_u64(&value, "start").map_err(|err| text_response(400, &err))?)
        }
        None => None,
    };
    let end_unix_ms = match request.param("end").or_else(|| request.param("endUnixMs")) {
        Some(value) => {
            Some(parse_admin_u64(&value, "end").map_err(|err| text_response(400, &err))?)
        }
        None => None,
    };
    if let (Some(start), Some(end)) = (start_unix_ms, end_unix_ms) {
        if start > end {
            return Err(text_response(
                400,
                "invalid usage time range: 'start' must be <= 'end'",
            ));
        }
    }
    let bucket_width = parse_usage_bucket_width(request)?;
    let reconcile = match request
        .param("reconcile")
        .or_else(|| request.param("refreshStorage"))
    {
        Some(value) => parse_admin_bool(&value).map_err(|err| text_response(400, &err))?,
        None => false,
    };
    Ok((
        tenant_id,
        start_unix_ms,
        end_unix_ms,
        bucket_width,
        reconcile,
    ))
}

fn usage_reconciliation_json(
    report: &crate::usage::UsageReport,
    observability: &tsink::StorageObservabilitySnapshot,
) -> JsonValue {
    let accounted_ingest_rows = report
        .tenants
        .iter()
        .map(|tenant| tenant.ingest.rows)
        .sum::<u64>();
    let accounted_query_units = report
        .tenants
        .iter()
        .map(|tenant| tenant.query.result_units)
        .sum::<u64>();
    let accounted_retention_tombstones = report
        .tenants
        .iter()
        .map(|tenant| tenant.retention.tombstones_applied)
        .sum::<u64>();
    let accounted_background_events = report
        .tenants
        .iter()
        .map(|tenant| tenant.background.events_total)
        .sum::<u64>();
    let accounted_storage_bytes = report
        .tenants
        .iter()
        .filter_map(|tenant| tenant.latest_storage_snapshot.as_ref())
        .map(|snapshot| snapshot.logical_storage_bytes)
        .sum::<u64>();
    let latest_storage_reconciled_unix_ms = report
        .tenants
        .iter()
        .filter_map(|tenant| {
            tenant
                .latest_storage_snapshot
                .as_ref()
                .map(|snapshot| snapshot.reconciled_unix_ms)
        })
        .max();
    let runtime_query_points_returned_total = observability
        .query
        .select_points_returned_total
        .saturating_add(
            observability
                .query
                .select_with_options_points_returned_total,
        )
        .saturating_add(observability.query.select_all_points_returned_total);

    json!({
        "accounted": {
            "ingestRowsTotal": accounted_ingest_rows,
            "queryResultUnitsTotal": accounted_query_units,
            "retentionTombstonesAppliedTotal": accounted_retention_tombstones,
            "backgroundEventsTotal": accounted_background_events,
            "latestStorageLogicalBytes": accounted_storage_bytes,
        },
        "runtime": {
            "walAppendPointsTotal": observability.wal.append_points_total,
            "queryPointsReturnedTotal": runtime_query_points_returned_total,
            "expiredSegmentsTotal": observability.flush.expired_segments_total,
            "rollupPointsMaterializedTotal": observability.rollups.points_materialized_total,
            "backgroundErrorsTotal": observability.health.background_errors_total,
            "degraded": observability.health.degraded,
        },
        "latestStorageReconciledUnixMs": latest_storage_reconciled_unix_ms,
    })
}

fn managed_control_plane_actor(request: &HttpRequest) -> ManagedControlPlaneActor {
    let actor = derive_audit_actor(request);
    ManagedControlPlaneActor {
        id: actor.id,
        scope: actor.auth_scope,
    }
}

fn admin_control_plane_error_response(
    status: u16,
    code: &str,
    detail: impl Into<String>,
) -> HttpResponse {
    json_response(
        status,
        &json!({
            "status": "error",
            "errorType": "control_plane",
            "code": code,
            "error": detail.into(),
        }),
    )
}

fn delete_series_error_response(err: &tsink::TsinkError) -> HttpResponse {
    match err {
        tsink::TsinkError::UnsupportedOperation { operation, .. }
            if *operation == "delete_series" =>
        {
            text_response(409, &format!("delete_series rejected: {err}"))
        }
        _ => text_response(500, &format!("delete_series failed: {err}")),
    }
}

fn json_scalar_param(value: Option<JsonValue>, field: &str) -> Result<Option<String>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) => Ok(non_empty_param(Some(value))),
        Some(JsonValue::Number(value)) => Ok(Some(value.to_string())),
        Some(_) => Err(format!(
            "invalid '{field}' value: expected string or number"
        )),
    }
}

async fn execute_admin_membership_operation(
    cluster_context: &ClusterRequestContext,
    operation: AdminMembershipOperation,
    command: InternalControlCommand,
) -> HttpResponse {
    let Some(consensus) = cluster_context.control_consensus.as_ref() else {
        return admin_membership_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };
    let node_id = membership_command_node_id(&command).to_string();

    if !consensus.is_local_control_leader() {
        if let Err(err) = consensus
            .ensure_leader_established(&cluster_context.rpc_client)
            .await
        {
            return admin_membership_error_response(
                503,
                "control_leader_establish_failed",
                format!("failed to establish control leader before mutation: {err}"),
            );
        }
    }
    if !consensus.is_local_control_leader() {
        let state = consensus.current_state();
        let current_leader = state.leader_node_id.as_deref().unwrap_or("<none>");
        return admin_membership_error_response(
            409,
            "not_control_leader",
            format!(
                "node '{}' is not the active control leader (current leader: '{current_leader}')",
                cluster_context.runtime.membership.local_node_id
            ),
        );
    }

    let current_state = consensus.current_state();
    let preview_outcome = match preview_membership_command(&current_state, &command) {
        Ok(outcome) => outcome,
        Err(err) => {
            return admin_membership_error_response(409, "invalid_membership_mutation", err)
        }
    };
    if preview_outcome == ControlMembershipMutationOutcome::Noop {
        return admin_membership_success_response(
            200,
            operation,
            "noop",
            &node_id,
            &current_state,
            "requested membership mutation is already satisfied",
            None,
            None,
            None,
            None,
        );
    }

    match consensus
        .propose_command(&cluster_context.rpc_client, command)
        .await
    {
        Ok(ProposeOutcome::Committed { index, term }) => {
            let state = consensus.current_state();
            admin_membership_success_response(
                200,
                operation,
                "committed",
                &node_id,
                &state,
                "membership mutation committed to control quorum",
                Some(index),
                Some(term),
                None,
                None,
            )
        }
        Ok(ProposeOutcome::Pending {
            required,
            acknowledged,
        }) => {
            let state = consensus.current_state();
            admin_membership_success_response(
                202,
                operation,
                "pending",
                &node_id,
                &state,
                "membership mutation is pending quorum acknowledgement",
                None,
                None,
                Some(required),
                Some(acknowledged),
            )
        }
        Err(err) => {
            let (status, code) = classify_membership_proposal_error(&err);
            admin_membership_error_response(status, code, err)
        }
    }
}

async fn execute_admin_handoff_operation(
    cluster_context: &ClusterRequestContext,
    operation: AdminHandoffOperation,
    command: InternalControlCommand,
) -> HttpResponse {
    let Some(consensus) = cluster_context.control_consensus.as_ref() else {
        return admin_handoff_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        );
    };
    let shard = handoff_command_shard(&command);
    if cluster_context
        .digest_runtime
        .as_ref()
        .is_some_and(|runtime| runtime.is_rebalance_run_inflight())
    {
        return admin_handoff_error_response(
            409,
            "operation_overlap",
            "rebalance run is in progress; retry handoff mutation when it completes",
        );
    }

    if !consensus.is_local_control_leader() {
        if let Err(err) = consensus
            .ensure_leader_established(&cluster_context.rpc_client)
            .await
        {
            return admin_handoff_error_response(
                503,
                "control_leader_establish_failed",
                format!("failed to establish control leader before mutation: {err}"),
            );
        }
    }
    if !consensus.is_local_control_leader() {
        let state = consensus.current_state();
        let current_leader = state.leader_node_id.as_deref().unwrap_or("<none>");
        return admin_handoff_error_response(
            409,
            "not_control_leader",
            format!(
                "node '{}' is not the active control leader (current leader: '{current_leader}')",
                cluster_context.runtime.membership.local_node_id
            ),
        );
    }

    let current_state = consensus.current_state();
    let preview_outcome = match preview_handoff_command(&current_state, &command) {
        Ok(outcome) => outcome,
        Err(err) => return admin_handoff_error_response(409, "invalid_handoff_mutation", err),
    };
    if preview_outcome == ControlHandoffMutationOutcome::Noop {
        return admin_handoff_success_response(
            200,
            operation,
            "noop",
            shard,
            &current_state,
            "requested shard handoff mutation is already satisfied",
            None,
            None,
            None,
            None,
        );
    }

    match consensus
        .propose_command(&cluster_context.rpc_client, command)
        .await
    {
        Ok(ProposeOutcome::Committed { index, term }) => {
            let state = consensus.current_state();
            admin_handoff_success_response(
                200,
                operation,
                "committed",
                shard,
                &state,
                "shard handoff mutation committed to control quorum",
                Some(index),
                Some(term),
                None,
                None,
            )
        }
        Ok(ProposeOutcome::Pending {
            required,
            acknowledged,
        }) => {
            let state = consensus.current_state();
            admin_handoff_success_response(
                202,
                operation,
                "pending",
                shard,
                &state,
                "shard handoff mutation is pending quorum acknowledgement",
                None,
                None,
                Some(required),
                Some(acknowledged),
            )
        }
        Err(err) => {
            let (status, code) = classify_handoff_proposal_error(&err);
            admin_handoff_error_response(status, code, err)
        }
    }
}

fn membership_command_node_id(command: &InternalControlCommand) -> &str {
    match command {
        InternalControlCommand::JoinNode { node_id, .. }
        | InternalControlCommand::LeaveNode { node_id }
        | InternalControlCommand::RecommissionNode { node_id, .. }
        | InternalControlCommand::ActivateNode { node_id }
        | InternalControlCommand::RemoveNode { node_id } => node_id.as_str(),
        InternalControlCommand::SetLeader { leader_node_id } => leader_node_id.as_str(),
        InternalControlCommand::BeginShardHandoff { .. }
        | InternalControlCommand::UpdateShardHandoff { .. }
        | InternalControlCommand::CompleteShardHandoff { .. } => {
            unreachable!("membership helper received handoff command")
        }
    }
}

fn handoff_command_shard(command: &InternalControlCommand) -> u32 {
    match command {
        InternalControlCommand::BeginShardHandoff { shard, .. }
        | InternalControlCommand::UpdateShardHandoff { shard, .. }
        | InternalControlCommand::CompleteShardHandoff { shard } => *shard,
        InternalControlCommand::SetLeader { .. }
        | InternalControlCommand::JoinNode { .. }
        | InternalControlCommand::LeaveNode { .. }
        | InternalControlCommand::RecommissionNode { .. }
        | InternalControlCommand::ActivateNode { .. }
        | InternalControlCommand::RemoveNode { .. } => {
            unreachable!("handoff helper received non-handoff command")
        }
    }
}

fn preview_membership_command(
    state: &ControlState,
    command: &InternalControlCommand,
) -> Result<ControlMembershipMutationOutcome, String> {
    let mut preview = state.clone();
    let outcome = match command {
        InternalControlCommand::JoinNode { node_id, endpoint } => {
            preview.apply_join_node(node_id, endpoint)?
        }
        InternalControlCommand::LeaveNode { node_id } => preview.apply_leave_node(node_id)?,
        InternalControlCommand::RecommissionNode { node_id, endpoint } => {
            preview.apply_recommission_node(node_id, endpoint.as_deref())?
        }
        InternalControlCommand::ActivateNode { node_id } => preview.apply_activate_node(node_id)?,
        InternalControlCommand::RemoveNode { node_id } => preview.apply_remove_node(node_id)?,
        InternalControlCommand::SetLeader { .. }
        | InternalControlCommand::BeginShardHandoff { .. }
        | InternalControlCommand::UpdateShardHandoff { .. }
        | InternalControlCommand::CompleteShardHandoff { .. } => {
            return Err("membership mutation preview received non-membership command".to_string())
        }
    };
    preview.validate()?;
    Ok(outcome)
}

fn preview_handoff_command(
    state: &ControlState,
    command: &InternalControlCommand,
) -> Result<ControlHandoffMutationOutcome, String> {
    let mut preview = state.clone();
    let outcome = match command {
        InternalControlCommand::BeginShardHandoff {
            shard,
            from_node_id,
            to_node_id,
            activation_ring_version,
        } => preview.apply_begin_shard_handoff(
            *shard,
            from_node_id,
            to_node_id,
            *activation_ring_version,
        )?,
        InternalControlCommand::UpdateShardHandoff {
            shard,
            phase,
            copied_rows,
            pending_rows,
            last_error,
        } => preview.apply_shard_handoff_progress(
            *shard,
            *phase,
            *copied_rows,
            *pending_rows,
            last_error.clone(),
        )?,
        InternalControlCommand::CompleteShardHandoff { shard } => {
            preview.apply_complete_shard_handoff(*shard)?
        }
        InternalControlCommand::SetLeader { .. }
        | InternalControlCommand::JoinNode { .. }
        | InternalControlCommand::LeaveNode { .. }
        | InternalControlCommand::RecommissionNode { .. }
        | InternalControlCommand::ActivateNode { .. }
        | InternalControlCommand::RemoveNode { .. } => {
            return Err("handoff mutation preview received non-handoff command".to_string());
        }
    };
    preview.validate()?;
    Ok(outcome)
}

fn classify_membership_proposal_error(err: &str) -> (u16, &'static str) {
    if err.contains("not the active control leader") || err.contains("not eligible to propose") {
        return (409, "not_control_leader");
    }
    if err.contains("unknown node")
        || err.contains("endpoint mismatch")
        || err.contains("empty node_id")
        || err.contains("empty endpoint")
        || err.contains("activate_node")
        || err.contains("remove_node")
    {
        return (409, "invalid_membership_mutation");
    }
    (503, "control_mutation_failed")
}

fn classify_handoff_proposal_error(err: &str) -> (u16, &'static str) {
    if err.contains("not the active control leader") || err.contains("not eligible to propose") {
        return (409, "not_control_leader");
    }
    if err.contains("unknown shard transition")
        || err.contains("invalid handoff phase transition")
        || err.contains("begin_shard_handoff")
        || err.contains("exceeds ring shard_count")
        || err.contains("unknown from_node_id")
        || err.contains("unknown to_node_id")
        || err.contains("requires distinct owners")
    {
        return (409, "invalid_handoff_mutation");
    }
    (503, "control_mutation_failed")
}

fn ceil_div_u64(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    numerator
        .saturating_add(denominator.saturating_sub(1))
        .saturating_div(denominator)
}

fn handoff_progress_percent(shard: &ShardHandoffSnapshot) -> f64 {
    let total = shard.copied_rows.saturating_add(shard.pending_rows);
    if total == 0 {
        return if shard.phase == ShardHandoffPhase::Completed {
            100.0
        } else {
            0.0
        };
    }
    (shard.copied_rows as f64 * 100.0) / total as f64
}

fn estimate_handoff_eta_seconds(
    shard: &ShardHandoffSnapshot,
    rebalance_snapshot: &RebalanceSchedulerSnapshot,
) -> Option<u64> {
    if shard.pending_rows == 0 {
        return Some(0);
    }
    if rebalance_snapshot.paused || rebalance_snapshot.interval_secs == 0 {
        return None;
    }
    let active_jobs = u64::try_from(rebalance_snapshot.active_jobs)
        .unwrap_or(u64::MAX)
        .max(1);
    let rows_per_run = rebalance_snapshot.rows_scheduled_last_run;
    if rows_per_run == 0 {
        return None;
    }
    let rows_per_job_per_run = ceil_div_u64(rows_per_run, active_jobs);
    if rows_per_job_per_run == 0 {
        return None;
    }
    let runs_remaining = ceil_div_u64(shard.pending_rows, rows_per_job_per_run);
    Some(runs_remaining.saturating_mul(rebalance_snapshot.interval_secs))
}

fn estimate_repair_eta_seconds(snapshot: &DigestExchangeSnapshot) -> Option<u64> {
    let mismatch_backlog = u64::try_from(snapshot.mismatches.len()).unwrap_or(u64::MAX);
    if mismatch_backlog == 0 {
        return Some(0);
    }
    if snapshot.repair_paused || snapshot.interval_secs == 0 {
        return None;
    }
    let repairs_per_run = snapshot
        .repairs_succeeded_last_run
        .max(snapshot.repairs_attempted_last_run)
        .max(snapshot.repairs_failed_last_run)
        .max(snapshot.repairs_cancelled_last_run);
    if repairs_per_run == 0 {
        return None;
    }
    let runs_remaining = ceil_div_u64(mismatch_backlog, repairs_per_run);
    Some(runs_remaining.saturating_mul(snapshot.interval_secs))
}

fn estimate_rebalance_eta_seconds(snapshot: &RebalanceSchedulerSnapshot) -> Option<u64> {
    let total_pending_rows = snapshot
        .jobs
        .iter()
        .fold(0u64, |acc, job| acc.saturating_add(job.pending_rows));
    if total_pending_rows == 0 {
        return Some(0);
    }
    if snapshot.paused || snapshot.interval_secs == 0 || snapshot.rows_scheduled_last_run == 0 {
        return None;
    }
    let runs_remaining = ceil_div_u64(total_pending_rows, snapshot.rows_scheduled_last_run);
    Some(runs_remaining.saturating_mul(snapshot.interval_secs))
}

fn admin_handoff_status_response(
    node_id: &str,
    state: &ControlState,
    handoff: &ClusterHandoffSnapshot,
    rebalance: &RebalanceSchedulerSnapshot,
) -> HttpResponse {
    let event_unix_ms = unix_timestamp_millis();
    let mut error_samples = Vec::new();
    let mut jobs_with_errors = 0u64;
    let mut eta_candidates = Vec::new();
    let jobs = handoff
        .shards
        .iter()
        .map(|shard| {
            if shard.last_error.is_some() {
                jobs_with_errors = jobs_with_errors.saturating_add(1);
                if error_samples.len() < 8 {
                    error_samples.push(format!(
                        "shard {} {}->{}: {}",
                        shard.shard,
                        shard.from_node_id,
                        shard.to_node_id,
                        shard.last_error.as_deref().unwrap_or_default()
                    ));
                }
            }
            let eta_seconds = estimate_handoff_eta_seconds(shard, rebalance);
            if shard.phase.is_active() {
                eta_candidates.push(eta_seconds.unwrap_or(u64::MAX));
            }
            json!({
                "shard": shard.shard,
                "fromNodeId": shard.from_node_id,
                "toNodeId": shard.to_node_id,
                "activationRingVersion": shard.activation_ring_version,
                "phase": shard.phase.as_str(),
                "copiedRows": shard.copied_rows,
                "pendingRows": shard.pending_rows,
                "resumedCount": shard.resumed_count,
                "startedUnixMs": shard.started_unix_ms,
                "updatedUnixMs": shard.updated_unix_ms,
                "lastError": shard.last_error,
                "isActive": shard.phase.is_active(),
                "progressPercent": handoff_progress_percent(shard),
                "etaSeconds": eta_seconds
            })
        })
        .collect::<Vec<_>>();

    let estimated_eta_seconds = if handoff.in_progress_shards == 0 {
        Some(0)
    } else if eta_candidates.contains(&u64::MAX) {
        None
    } else {
        eta_candidates.into_iter().max()
    };
    json_response(
        200,
        &json!({
            "status": "success",
            "data": {
                "operation": AdminHandoffOperation::Status.as_str(),
                "nodeId": node_id,
                "ringVersion": state.ring_version,
                "leaderNodeId": state.leader_node_id,
                "totalShards": handoff.total_shards,
                "inProgressShards": handoff.in_progress_shards,
                "resumedShards": handoff.resumed_shards,
                "copiedRowsTotal": handoff.copied_rows_total,
                "pendingRowsTotal": handoff.pending_rows_total,
                "estimatedEtaSeconds": estimated_eta_seconds,
                "eventUnixMs": event_unix_ms,
                "errorSummary": {
                    "jobsWithErrors": jobs_with_errors,
                    "lastErrorSamples": error_samples,
                    "rebalanceLastError": rebalance.last_error.clone()
                },
                "jobs": jobs,
                "message": "cluster handoff status"
            }
        }),
    )
}

fn admin_repair_status_response(
    status: u16,
    operation: AdminRepairOperation,
    node_id: &str,
    control: RepairControlSnapshot,
    snapshot: DigestExchangeSnapshot,
    run_inflight: bool,
    message: &str,
) -> HttpResponse {
    let event_unix_ms = unix_timestamp_millis();
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
    let mismatch_summary = snapshot
        .mismatches
        .iter()
        .take(16)
        .map(|mismatch| {
            json!({
                "shard": mismatch.shard,
                "peerNodeId": mismatch.peer_node_id,
                "pointGap": mismatch.remote_point_count.saturating_sub(mismatch.local_point_count),
                "seriesGap": mismatch.remote_series_count.saturating_sub(mismatch.local_series_count),
                "detectedUnixMs": mismatch.detected_unix_ms
            })
        })
        .collect::<Vec<_>>();
    let estimated_eta_seconds = estimate_repair_eta_seconds(&snapshot);

    json_response(
        status,
        &json!({
            "status": "success",
            "data": {
                "operation": operation.as_str(),
                "nodeId": node_id,
                "repairPaused": control.paused,
                "repairRunInFlight": run_inflight,
                "repairCancelGeneration": control.cancel_generation,
                "repairCancellationsTotal": control.cancellations_total,
                "intervalSecs": snapshot.interval_secs,
                "windowSecs": snapshot.window_secs,
                "runsTotal": snapshot.runs_total,
                "mismatchReportsRetained": mismatch_backlog,
                "repairsAttemptedTotal": snapshot.repairs_attempted_total,
                "repairsSucceededTotal": snapshot.repairs_succeeded_total,
                "repairsFailedTotal": snapshot.repairs_failed_total,
                "repairsCancelledTotal": snapshot.repairs_cancelled_total,
                "repairRowsInsertedTotal": snapshot.repair_rows_inserted_total,
                "repairRowsInsertedLastRun": snapshot.repair_rows_inserted_last_run,
                "progressPercent": progress_percent,
                "estimatedEtaSeconds": estimated_eta_seconds,
                "eventUnixMs": event_unix_ms,
                "lastRunUnixMs": snapshot.last_run_unix_ms,
                "lastSuccessUnixMs": snapshot.last_success_unix_ms,
                "lastError": snapshot.last_error,
                "errorSummary": {
                    "failuresLastRun": snapshot.repairs_failed_last_run,
                    "cancelledLastRun": snapshot.repairs_cancelled_last_run,
                    "skippedBackoffLastRun": snapshot.repairs_skipped_backoff_last_run,
                    "mismatchBacklog": mismatch_summary
                },
                "message": message
            }
        }),
    )
}

#[allow(clippy::too_many_arguments)]
fn admin_membership_success_response(
    status: u16,
    operation: AdminMembershipOperation,
    result: &str,
    node_id: &str,
    state: &ControlState,
    message: &str,
    committed_index: Option<u64>,
    committed_term: Option<u64>,
    required_acks: Option<usize>,
    acknowledged_acks: Option<usize>,
) -> HttpResponse {
    let event_unix_ms = unix_timestamp_millis();
    let mut data = serde_json::Map::new();
    data.insert("operation".to_string(), json!(operation.as_str()));
    data.insert("result".to_string(), json!(result));
    data.insert("nodeId".to_string(), json!(node_id));
    data.insert("membershipEpoch".to_string(), json!(state.membership_epoch));
    data.insert("ringVersion".to_string(), json!(state.ring_version));
    data.insert("leaderNodeId".to_string(), json!(state.leader_node_id));
    data.insert("message".to_string(), json!(message));
    data.insert("eventUnixMs".to_string(), json!(event_unix_ms));

    if let Some(node) = state.node_record(node_id) {
        data.insert("endpoint".to_string(), json!(node.endpoint));
        data.insert("nodeStatus".to_string(), json!(node.status.as_str()));
        data.insert(
            "membershipGeneration".to_string(),
            json!(node.membership_generation),
        );
    }
    if let Some(index) = committed_index {
        data.insert("committedLogIndex".to_string(), json!(index));
    }
    if let Some(term) = committed_term {
        data.insert("committedLogTerm".to_string(), json!(term));
    }
    if let Some(required) = required_acks {
        data.insert("requiredAcks".to_string(), json!(required));
    }
    if let Some(acknowledged) = acknowledged_acks {
        data.insert("acknowledgedAcks".to_string(), json!(acknowledged));
    }

    json_response(
        status,
        &json!({
            "status": "success",
            "data": JsonValue::Object(data)
        }),
    )
}

fn handoff_snapshot_for_shard(state: &ControlState, shard: u32) -> Option<ShardHandoffSnapshot> {
    state
        .handoff_snapshot()
        .shards
        .into_iter()
        .find(|item| item.shard == shard)
}

#[allow(clippy::too_many_arguments)]
fn admin_handoff_success_response(
    status: u16,
    operation: AdminHandoffOperation,
    result: &str,
    shard: u32,
    state: &ControlState,
    message: &str,
    committed_index: Option<u64>,
    committed_term: Option<u64>,
    required_acks: Option<usize>,
    acknowledged_acks: Option<usize>,
) -> HttpResponse {
    let event_unix_ms = unix_timestamp_millis();
    let mut data = serde_json::Map::new();
    data.insert("operation".to_string(), json!(operation.as_str()));
    data.insert("result".to_string(), json!(result));
    data.insert("shard".to_string(), json!(shard));
    data.insert("ringVersion".to_string(), json!(state.ring_version));
    data.insert("leaderNodeId".to_string(), json!(state.leader_node_id));
    data.insert("message".to_string(), json!(message));
    data.insert("eventUnixMs".to_string(), json!(event_unix_ms));

    if let Some(snapshot) = handoff_snapshot_for_shard(state, shard) {
        data.insert("fromNodeId".to_string(), json!(snapshot.from_node_id));
        data.insert("toNodeId".to_string(), json!(snapshot.to_node_id));
        data.insert(
            "activationRingVersion".to_string(),
            json!(snapshot.activation_ring_version),
        );
        data.insert("phase".to_string(), json!(snapshot.phase.as_str()));
        data.insert("copiedRows".to_string(), json!(snapshot.copied_rows));
        data.insert("pendingRows".to_string(), json!(snapshot.pending_rows));
        data.insert("resumedCount".to_string(), json!(snapshot.resumed_count));
        data.insert("startedUnixMs".to_string(), json!(snapshot.started_unix_ms));
        data.insert("updatedUnixMs".to_string(), json!(snapshot.updated_unix_ms));
        data.insert("lastError".to_string(), json!(snapshot.last_error));
    }

    if let Some(index) = committed_index {
        data.insert("committedLogIndex".to_string(), json!(index));
    }
    if let Some(term) = committed_term {
        data.insert("committedLogTerm".to_string(), json!(term));
    }
    if let Some(required) = required_acks {
        data.insert("requiredAcks".to_string(), json!(required));
    }
    if let Some(acknowledged) = acknowledged_acks {
        data.insert("acknowledgedAcks".to_string(), json!(acknowledged));
    }

    json_response(
        status,
        &json!({
            "status": "success",
            "data": JsonValue::Object(data)
        }),
    )
}

fn admin_membership_error_response(
    status: u16,
    code: &str,
    message: impl Into<String>,
) -> HttpResponse {
    json_response(
        status,
        &json!({
            "status": "error",
            "errorType": code,
            "error": message.into()
        }),
    )
}

fn admin_handoff_error_response(
    status: u16,
    code: &str,
    message: impl Into<String>,
) -> HttpResponse {
    json_response(
        status,
        &json!({
            "status": "error",
            "errorType": code,
            "error": message.into()
        }),
    )
}

fn admin_repair_success_response(
    status: u16,
    operation: AdminRepairOperation,
    node_id: &str,
    snapshot: RepairControlSnapshot,
    message: &str,
) -> HttpResponse {
    let event_unix_ms = unix_timestamp_millis();
    json_response(
        status,
        &json!({
            "status": "success",
            "data": {
                "operation": operation.as_str(),
                "nodeId": node_id,
                "repairPaused": snapshot.paused,
                "repairCancelGeneration": snapshot.cancel_generation,
                "repairCancellationsTotal": snapshot.cancellations_total,
                "eventUnixMs": event_unix_ms,
                "message": message
            }
        }),
    )
}

fn admin_repair_error_response(
    status: u16,
    code: &str,
    message: impl Into<String>,
) -> HttpResponse {
    json_response(
        status,
        &json!({
            "status": "error",
            "errorType": code,
            "error": message.into()
        }),
    )
}

#[allow(clippy::too_many_arguments)]
fn admin_rebalance_success_response(
    status: u16,
    operation: AdminRebalanceOperation,
    node_id: &str,
    control: RebalanceSchedulerControlSnapshot,
    snapshot: RebalanceSchedulerSnapshot,
    hotspot_snapshot: ClusterHotspotSnapshot,
    run_inflight: bool,
    message: &str,
) -> HttpResponse {
    let event_unix_ms = unix_timestamp_millis();
    let estimated_eta_seconds = estimate_rebalance_eta_seconds(&snapshot);
    let total_pending_rows = snapshot
        .jobs
        .iter()
        .fold(0u64, |acc, job| acc.saturating_add(job.pending_rows));
    let total_copied_rows = snapshot
        .jobs
        .iter()
        .fold(0u64, |acc, job| acc.saturating_add(job.copied_rows));
    let progress_percent = if total_pending_rows == 0 && total_copied_rows == 0 {
        100.0
    } else {
        let total = total_pending_rows.saturating_add(total_copied_rows) as f64;
        if total <= 0.0 {
            0.0
        } else {
            (total_copied_rows as f64 * 100.0) / total
        }
    };
    json_response(
        status,
        &json!({
            "status": "success",
            "data": {
                "operation": operation.as_str(),
                "nodeId": node_id,
                "rebalancePaused": control.paused,
                "rebalanceRunInFlight": run_inflight,
                "intervalSecs": snapshot.interval_secs,
                "maxRowsPerTick": snapshot.max_rows_per_tick,
                "maxShardsPerTick": snapshot.max_shards_per_tick,
                "effectiveMaxRowsPerTickLastRun": snapshot.effective_max_rows_per_tick_last_run,
                "isLocalControlLeader": snapshot.is_local_control_leader,
                "activeJobs": snapshot.active_jobs,
                "runsTotal": snapshot.runs_total,
                "jobsConsideredLastRun": snapshot.jobs_considered_last_run,
                "jobsAdvancedTotal": snapshot.jobs_advanced_total,
                "jobsCompletedTotal": snapshot.jobs_completed_total,
                "rowsScheduledTotal": snapshot.rows_scheduled_total,
                "rowsScheduledLastRun": snapshot.rows_scheduled_last_run,
                "proposalsCommittedTotal": snapshot.proposals_committed_total,
                "proposalsPendingTotal": snapshot.proposals_pending_total,
                "proposalFailuresTotal": snapshot.proposal_failures_total,
                "movesBlockedBySloTotal": snapshot.moves_blocked_by_slo_total,
                "sloGuard": {
                    "writePressureRatio": snapshot.slo_guard.write_pressure_ratio,
                    "queryPressureRatio": snapshot.slo_guard.query_pressure_ratio,
                    "clusterQueryPressureRatio": snapshot.slo_guard.cluster_query_pressure_ratio,
                    "effectiveMaxRowsPerTick": snapshot.slo_guard.effective_max_rows_per_tick,
                    "blockNewHandoffs": snapshot.slo_guard.block_new_handoffs,
                    "reason": snapshot.slo_guard.reason.clone()
                },
                "lastRunUnixMs": snapshot.last_run_unix_ms,
                "lastSuccessUnixMs": snapshot.last_success_unix_ms,
                "lastError": snapshot.last_error.clone(),
                "pendingRowsTotal": total_pending_rows,
                "copiedRowsTotal": total_copied_rows,
                "progressPercent": progress_percent,
                "estimatedEtaSeconds": estimated_eta_seconds,
                "eventUnixMs": event_unix_ms,
                "errorSummary": {
                    "proposalFailuresTotal": snapshot.proposal_failures_total,
                    "lastError": snapshot.last_error.clone()
                },
                "candidateMoves": snapshot.candidate_moves.iter().map(|candidate| {
                    json!({
                        "shard": candidate.shard,
                        "fromNodeId": candidate.from_node_id,
                        "toNodeId": candidate.to_node_id,
                        "pressureScore": candidate.pressure_score,
                        "movementCostScore": candidate.movement_cost_score,
                        "imbalanceImprovementScore": candidate.imbalance_improvement_score,
                        "decisionScore": candidate.decision_score,
                        "sourceNodePressure": candidate.source_node_pressure,
                        "targetNodePressure": candidate.target_node_pressure,
                        "reason": candidate.reason
                    })
                }).collect::<Vec<_>>(),
                "hotspot": {
                    "generatedUnixMs": hotspot_snapshot.generated_unix_ms,
                    "skewedShards": hotspot_snapshot.skewed_shards,
                    "skewedTenants": hotspot_snapshot.skewed_tenants,
                    "maxShardScore": hotspot_snapshot.max_shard_score,
                    "maxTenantScore": hotspot_snapshot.max_tenant_score,
                    "hotShards": hotspot_snapshot.hot_shards.iter().map(|item| {
                        json!({
                            "shard": item.shard,
                            "ingestRowsTotal": item.ingest_rows_total,
                            "queryShardHitsTotal": item.query_shard_hits_total,
                            "storageSeries": item.storage_series,
                            "repairMismatchesTotal": item.repair_mismatches_total,
                            "repairSeriesGapTotal": item.repair_series_gap_total,
                            "repairPointGapTotal": item.repair_point_gap_total,
                            "repairRowsInsertedTotal": item.repair_rows_inserted_total,
                            "handoffPendingRows": item.handoff_pending_rows,
                            "pressureScore": item.pressure_score,
                            "movementCostScore": item.movement_cost_score,
                            "skewFactor": item.skew_factor,
                            "recommendMove": item.recommend_move
                        })
                    }).collect::<Vec<_>>(),
                    "tenantHotspots": hotspot_snapshot.tenant_hotspots.iter().map(|item| {
                        json!({
                            "tenantId": item.tenant_id,
                            "ingestRowsTotal": item.ingest_rows_total,
                            "queryRequestsTotal": item.query_requests_total,
                            "queryUnitsTotal": item.query_units_total,
                            "storageSeries": item.storage_series,
                            "repairRowsInsertedTotal": item.repair_rows_inserted_total,
                            "pressureScore": item.pressure_score,
                            "skewFactor": item.skew_factor
                        })
                    }).collect::<Vec<_>>()
                },
                "jobs": snapshot.jobs.iter().map(|job| {
                    let total = job.pending_rows.saturating_add(job.copied_rows);
                    let progress_percent = if total == 0 {
                        if job.phase == ShardHandoffPhase::Completed {
                            100.0
                        } else {
                            0.0
                        }
                    } else {
                        (job.copied_rows as f64 * 100.0) / total as f64
                    };
                    let eta_seconds = if snapshot.paused || snapshot.interval_secs == 0 || snapshot.rows_scheduled_last_run == 0 {
                        if job.pending_rows == 0 { Some(0) } else { None }
                    } else {
                        let active_jobs = u64::try_from(snapshot.active_jobs).unwrap_or(u64::MAX).max(1);
                        let rows_per_job_per_run = ceil_div_u64(snapshot.rows_scheduled_last_run, active_jobs);
                        if rows_per_job_per_run == 0 {
                            None
                        } else {
                            let runs_remaining = ceil_div_u64(job.pending_rows, rows_per_job_per_run);
                            Some(runs_remaining.saturating_mul(snapshot.interval_secs))
                        }
                    };
                    json!({
                        "shard": job.shard,
                        "fromNodeId": job.from_node_id,
                        "toNodeId": job.to_node_id,
                        "activationRingVersion": job.activation_ring_version,
                        "phase": job.phase.as_str(),
                        "copiedRows": job.copied_rows,
                        "pendingRows": job.pending_rows,
                        "updatedUnixMs": job.updated_unix_ms,
                        "progressPercent": progress_percent,
                        "etaSeconds": eta_seconds
                    })
                }).collect::<Vec<_>>(),
                "message": message
            }
        }),
    )
}

fn admin_rebalance_error_response(
    status: u16,
    code: &str,
    message: impl Into<String>,
) -> HttpResponse {
    json_response(
        status,
        &json!({
            "status": "error",
            "errorType": code,
            "error": message.into()
        }),
    )
}

fn admin_control_recovery_error_response(
    status: u16,
    code: &str,
    message: impl Into<String>,
) -> HttpResponse {
    json_response(
        status,
        &json!({
            "status": "error",
            "errorType": code,
            "error": message.into()
        }),
    )
}

fn admin_cluster_snapshot_error_response(
    status: u16,
    code: &str,
    message: impl Into<String>,
) -> HttpResponse {
    json_response(
        status,
        &json!({
            "status": "error",
            "errorType": code,
            "error": message.into()
        }),
    )
}

async fn ensure_local_control_leader(
    cluster_context: &ClusterRequestContext,
) -> Result<(), HttpResponse> {
    let Some(consensus) = cluster_context.control_consensus.as_ref() else {
        return Err(admin_cluster_snapshot_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
        ));
    };
    if !consensus.is_local_control_leader() {
        if let Err(err) = consensus
            .ensure_leader_established(&cluster_context.rpc_client)
            .await
        {
            return Err(admin_cluster_snapshot_error_response(
                503,
                "control_leader_establish_failed",
                format!("failed to establish control leader before cluster DR operation: {err}"),
            ));
        }
    }
    if !consensus.is_local_control_leader() {
        let state = consensus.current_state();
        let current_leader = state.leader_node_id.as_deref().unwrap_or("<none>");
        return Err(admin_cluster_snapshot_error_response(
            409,
            "not_control_leader",
            format!(
                "node '{}' is not the active control leader (current leader: '{current_leader}')",
                cluster_context.runtime.membership.local_node_id
            ),
        ));
    }
    Ok(())
}

fn normalize_named_paths(
    paths: BTreeMap<String, String>,
    field: &str,
) -> Result<BTreeMap<String, String>, String> {
    let mut normalized = BTreeMap::new();
    for (key, value) in paths {
        let Some(key) = non_empty_param(Some(key)) else {
            return Err(format!("{field} contains an empty node id key"));
        };
        let Some(value) = non_empty_param(Some(value)) else {
            return Err(format!("{field} contains an empty path for node '{key}'"));
        };
        normalized.insert(key, value);
    }
    Ok(normalized)
}

fn cluster_snapshot_nodes(control_state: &ControlState) -> Vec<ClusterSnapshotNodeTarget> {
    let mut nodes = control_state
        .nodes
        .iter()
        .filter(|node| node.status != ControlNodeStatus::Removed)
        .map(|node| ClusterSnapshotNodeTarget {
            id: node.id.clone(),
            endpoint: node.endpoint.clone(),
            status: node.status.as_str().to_string(),
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.id.cmp(&right.id));
    nodes
}

fn build_control_recovery_snapshot_file(
    cluster_context: &ClusterRequestContext,
) -> Result<ControlRecoverySnapshotFileV1, String> {
    let consensus = cluster_context
        .control_consensus
        .as_ref()
        .ok_or_else(|| "cluster control consensus runtime is not available".to_string())?;
    let control_state_store = cluster_context
        .control_state_store
        .as_ref()
        .ok_or_else(|| "cluster control state store is not available".to_string())?;
    let (control_state, log_snapshot) = consensus.recovery_snapshot_bundle();
    control_state_store
        .persist(&control_state)
        .map_err(|err| format!("failed to persist control state before snapshot: {err}"))?;
    Ok(ControlRecoverySnapshotFileV1 {
        magic: CONTROL_RECOVERY_SNAPSHOT_MAGIC.to_string(),
        schema_version: CONTROL_RECOVERY_SNAPSHOT_SCHEMA_VERSION,
        created_unix_ms: unix_timestamp_millis(),
        source_node_id: cluster_context.runtime.membership.local_node_id.clone(),
        source_control_state_path: control_state_store.path().display().to_string(),
        source_control_log_path: consensus.log_path().display().to_string(),
        control_state,
        control_log: log_snapshot,
    })
}

async fn perform_local_data_snapshot(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    snapshot_path: &Path,
    cluster_context: Option<&ClusterRequestContext>,
) -> Result<InternalDataSnapshotResponse, String> {
    let node_id = cluster_context
        .map(|context| context.runtime.membership.local_node_id.clone())
        .unwrap_or_else(|| "unknown".to_string());
    let snapshot_path = snapshot_path.to_path_buf();
    let snapshot_path_display = snapshot_path.display().to_string();
    let blocking_snapshot_path = snapshot_path.clone();
    let storage = Arc::clone(storage);
    let metadata_store = Arc::clone(metadata_store);
    let exemplar_store = Arc::clone(exemplar_store);
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        storage.snapshot(&blocking_snapshot_path).map_err(|err| {
            format!(
                "snapshot failed for {}: {err}",
                blocking_snapshot_path.display()
            )
        })?;
        if let Err(err) = metadata_store.snapshot_into(&blocking_snapshot_path) {
            let _ = std::fs::remove_dir_all(&blocking_snapshot_path);
            return Err(format!(
                "metric metadata snapshot failed for {}: {err}",
                blocking_snapshot_path.display()
            ));
        }
        if let Err(err) = exemplar_store.snapshot_into(&blocking_snapshot_path) {
            let _ = std::fs::remove_dir_all(&blocking_snapshot_path);
            return Err(format!(
                "exemplar snapshot failed for {}: {err}",
                blocking_snapshot_path.display()
            ));
        }
        std::fs::metadata(&blocking_snapshot_path)
            .map(|metadata| metadata.len())
            .map_err(|err| {
                format!(
                    "failed to stat snapshot artifact {}: {err}",
                    blocking_snapshot_path.display()
                )
            })
    })
    .await;
    match result {
        Ok(Ok(size_bytes)) => {
            if let Some(rules_runtime) = rules_runtime {
                rules_runtime.snapshot_into(&snapshot_path).map_err(|err| {
                    format!(
                        "rules snapshot failed for {}: {err}",
                        snapshot_path.display()
                    )
                })?;
            }
            Ok(InternalDataSnapshotResponse {
                node_id,
                path: snapshot_path_display,
                created_unix_ms: unix_timestamp_millis(),
                duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                size_bytes,
            })
        }
        Ok(Err(err)) => Err(err),
        Err(err) => Err(format!("snapshot task failed: {err}")),
    }
}

async fn perform_local_data_restore(
    snapshot_path: &Path,
    data_path: &Path,
    cluster_context: Option<&ClusterRequestContext>,
) -> Result<InternalDataRestoreResponse, String> {
    let node_id = cluster_context
        .map(|context| context.runtime.membership.local_node_id.clone())
        .unwrap_or_else(|| "unknown".to_string());
    let snapshot_path = snapshot_path.to_path_buf();
    let data_path = data_path.to_path_buf();
    let snapshot_path_display = snapshot_path.display().to_string();
    let data_path_display = data_path.display().to_string();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        StorageBuilder::restore_from_snapshot(&snapshot_path, &data_path).map_err(|err| {
            format!(
                "restore failed from {} to {}: {err}",
                snapshot_path.display(),
                data_path.display()
            )
        })
    })
    .await;
    match result {
        Ok(Ok(())) => Ok(InternalDataRestoreResponse {
            node_id,
            snapshot_path: snapshot_path_display,
            data_path: data_path_display,
            restored_unix_ms: unix_timestamp_millis(),
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        }),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(format!("restore task failed: {err}")),
    }
}

fn write_json_file<T: Serialize>(path: &Path, value: &T, description: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create {description} directory {}: {err}",
                parent.display()
            )
        })?;
    }

    let mut encoded = serde_json::to_vec_pretty(value)
        .map_err(|err| format!("failed to serialize {description}: {err}"))?;
    encoded.push(b'\n');

    let tmp_path = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .map_err(|err| {
            format!(
                "failed to open temporary {description} file {}: {err}",
                tmp_path.display()
            )
        })?;
    file.write_all(&encoded).map_err(|err| {
        format!(
            "failed to write temporary {description} file {}: {err}",
            tmp_path.display()
        )
    })?;
    file.sync_all().map_err(|err| {
        format!(
            "failed to fsync temporary {description} file {}: {err}",
            tmp_path.display()
        )
    })?;
    std::fs::rename(&tmp_path, path).map_err(|err| {
        format!(
            "failed to replace {description} file {} with {}: {err}",
            path.display(),
            tmp_path.display()
        )
    })
}

fn write_control_recovery_snapshot_file(
    path: &Path,
    snapshot: &ControlRecoverySnapshotFileV1,
) -> Result<(), String> {
    write_json_file(path, snapshot, "control recovery snapshot")
}

fn load_control_recovery_snapshot_file(
    path: &Path,
) -> Result<ControlRecoverySnapshotFileV1, String> {
    let raw = std::fs::read(path).map_err(|err| {
        format!(
            "failed to read control recovery snapshot {}: {err}",
            path.display()
        )
    })?;
    let snapshot: ControlRecoverySnapshotFileV1 = serde_json::from_slice(&raw).map_err(|err| {
        format!(
            "failed to parse control recovery snapshot {}: {err}",
            path.display()
        )
    })?;
    if snapshot.magic != CONTROL_RECOVERY_SNAPSHOT_MAGIC {
        return Err(format!(
            "control recovery snapshot {} has unsupported magic '{}'",
            path.display(),
            snapshot.magic
        ));
    }
    if snapshot.schema_version != CONTROL_RECOVERY_SNAPSHOT_SCHEMA_VERSION {
        return Err(format!(
            "control recovery snapshot {} has unsupported schema version {}",
            path.display(),
            snapshot.schema_version
        ));
    }
    snapshot.control_state.validate().map_err(|err| {
        format!(
            "control recovery snapshot {} has invalid control state: {err}",
            path.display()
        )
    })?;
    Ok(snapshot)
}

fn write_cluster_snapshot_manifest_file(
    path: &Path,
    manifest: &ClusterSnapshotManifestFileV1,
) -> Result<(), String> {
    write_json_file(path, manifest, "cluster snapshot manifest")
}

fn load_cluster_snapshot_manifest_file(
    path: &Path,
) -> Result<ClusterSnapshotManifestFileV1, String> {
    let raw = std::fs::read(path).map_err(|err| {
        format!(
            "failed to read cluster snapshot manifest {}: {err}",
            path.display()
        )
    })?;
    let manifest: ClusterSnapshotManifestFileV1 = serde_json::from_slice(&raw).map_err(|err| {
        format!(
            "failed to parse cluster snapshot manifest {}: {err}",
            path.display()
        )
    })?;
    if manifest.magic != CLUSTER_SNAPSHOT_MANIFEST_MAGIC {
        return Err(format!(
            "cluster snapshot manifest {} has unsupported magic '{}'",
            path.display(),
            manifest.magic
        ));
    }
    if manifest.schema_version != CLUSTER_SNAPSHOT_MANIFEST_SCHEMA_VERSION {
        return Err(format!(
            "cluster snapshot manifest {} has unsupported schema version {}",
            path.display(),
            manifest.schema_version
        ));
    }
    manifest
        .control_snapshot
        .control_state
        .validate()
        .map_err(|err| {
            format!(
                "cluster snapshot manifest {} has invalid control state: {err}",
                path.display()
            )
        })?;
    if manifest.control_snapshot.magic != CONTROL_RECOVERY_SNAPSHOT_MAGIC {
        return Err(format!(
            "cluster snapshot manifest {} embeds unsupported control snapshot magic '{}'",
            path.display(),
            manifest.control_snapshot.magic
        ));
    }
    if manifest.control_snapshot.schema_version != CONTROL_RECOVERY_SNAPSHOT_SCHEMA_VERSION {
        return Err(format!(
            "cluster snapshot manifest {} embeds unsupported control snapshot schema version {}",
            path.display(),
            manifest.control_snapshot.schema_version
        ));
    }
    if manifest.cluster_nodes.is_empty() {
        return Err(format!(
            "cluster snapshot manifest {} does not contain any node snapshots",
            path.display()
        ));
    }
    Ok(manifest)
}

fn write_cluster_restore_report_file(
    path: &Path,
    report: &ClusterRestoreReportFileV1,
) -> Result<(), String> {
    write_json_file(path, report, "cluster restore report")
}

fn unix_timestamp_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn resolve_admin_path(
    requested_path: &Path,
    admin_path_prefix: Option<&Path>,
    must_exist: bool,
) -> Result<PathBuf, String> {
    let resolved = canonicalize_requested_path(requested_path, must_exist)?;
    if let Some(prefix) = admin_path_prefix {
        let canonical_prefix = canonicalize_requested_path(prefix, true)?;
        if !resolved.starts_with(&canonical_prefix) {
            return Err(format!(
                "path '{}' is outside admin path prefix '{}'",
                resolved.display(),
                canonical_prefix.display()
            ));
        }
    }
    Ok(resolved)
}

fn canonicalize_requested_path(path: &Path, must_exist: bool) -> Result<PathBuf, String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|err| format!("failed to resolve current directory: {err}"))?
            .join(path)
    };

    if must_exist {
        return absolute
            .canonicalize()
            .map_err(|err| format!("failed to resolve path {}: {err}", absolute.display()));
    }

    let mut existing = absolute.as_path();
    let mut missing_segments = Vec::new();
    while !existing.exists() {
        let Some(name) = existing.file_name() else {
            return Err(format!(
                "path '{}' does not have an existing parent directory",
                absolute.display()
            ));
        };
        missing_segments.push(name.to_os_string());
        existing = existing.parent().ok_or_else(|| {
            format!(
                "path '{}' does not have an existing parent directory",
                absolute.display()
            )
        })?;
    }

    let mut canonical = existing
        .canonicalize()
        .map_err(|err| format!("failed to resolve path {}: {err}", existing.display()))?;
    let mut appended_segments = 0usize;
    for segment in missing_segments.iter().rev() {
        if segment == std::ffi::OsStr::new(".") {
            continue;
        }
        if segment == std::ffi::OsStr::new("..") {
            if appended_segments == 0 {
                return Err(format!(
                    "path '{}' escapes its resolved parent directory",
                    absolute.display()
                ));
            }
            canonical.pop();
            appended_segments -= 1;
            continue;
        }

        canonical.push(segment);
        appended_segments += 1;
    }
    Ok(canonical)
}

#[derive(Debug, Clone, Default)]
struct ExemplarClusterWriteStats {
    accepted_exemplars: usize,
    dropped_exemplars: usize,
    consistency: Option<WriteConsistencyOutcome>,
}

#[derive(Debug, Clone, Default)]
struct RoutedExemplarBatch {
    owner_node_id: String,
    endpoint: String,
    exemplars: Vec<InternalWriteExemplar>,
    shards: BTreeSet<u32>,
}

#[derive(Debug, Clone, Copy)]
struct ExemplarShardAckState {
    required_acks: u16,
    acknowledged_acks: u16,
}

fn normalized_exemplar_to_store_write(exemplar: NormalizedExemplar) -> ExemplarWrite {
    ExemplarWrite {
        metric: exemplar.series.metric,
        series_labels: exemplar.series.labels,
        exemplar_labels: exemplar.labels,
        timestamp: exemplar.timestamp,
        value: exemplar.value,
    }
}

fn normalized_exemplar_to_internal_write(exemplar: &NormalizedExemplar) -> InternalWriteExemplar {
    InternalWriteExemplar {
        metric: exemplar.series.metric.clone(),
        series_labels: exemplar.series.labels.clone(),
        exemplar_labels: exemplar.labels.clone(),
        timestamp: exemplar.timestamp,
        value: exemplar.value,
    }
}

fn normalized_metadata_to_internal_update(
    update: &NormalizedMetricMetadataUpdate,
) -> InternalMetricMetadataUpdate {
    InternalMetricMetadataUpdate {
        metric_family_name: update.metric_family_name.clone(),
        metric_type: update.metric_type as i32,
        help: update.help.clone(),
        unit: update.unit.clone(),
    }
}

fn internal_metadata_update_to_normalized(
    update: InternalMetricMetadataUpdate,
) -> Result<NormalizedMetricMetadataUpdate, String> {
    let metric_type = MetricType::try_from(update.metric_type).map_err(|_| {
        format!(
            "internal ingest metadata update '{}' has unsupported metric type {}",
            update.metric_family_name, update.metric_type
        )
    })?;
    Ok(NormalizedMetricMetadataUpdate {
        metric_family_name: update.metric_family_name,
        metric_type,
        help: update.help,
        unit: update.unit,
    })
}

fn validate_payload_feature_flags(
    config: PrometheusPayloadConfig,
    metadata_updates: usize,
    exemplars: usize,
    histograms: usize,
) -> Result<(), (PrometheusPayloadKind, String)> {
    if metadata_updates > 0 && !config.metadata_enabled {
        return Err((
            PrometheusPayloadKind::Metadata,
            "remote write metadata payloads are disabled on this node".to_string(),
        ));
    }
    if exemplars > 0 && !config.exemplars_enabled {
        return Err((
            PrometheusPayloadKind::Exemplar,
            "remote write exemplar payloads are disabled on this node".to_string(),
        ));
    }
    if histograms > 0 && !config.histograms_enabled {
        return Err((
            PrometheusPayloadKind::Histogram,
            "remote write histogram payloads are disabled on this node".to_string(),
        ));
    }
    Ok(())
}

fn validate_payload_quotas(
    config: PrometheusPayloadConfig,
    metadata_updates: usize,
    exemplars: usize,
    exemplar_limit: usize,
    histogram_bucket_entries: usize,
) -> Result<(), (PrometheusPayloadKind, String)> {
    if metadata_updates > config.max_metadata_updates_per_request {
        return Err((
            PrometheusPayloadKind::Metadata,
            format!(
                "remote write metadata payload exceeds limit: {} > {}",
                metadata_updates, config.max_metadata_updates_per_request
            ),
        ));
    }
    if exemplars > exemplar_limit {
        return Err((
            PrometheusPayloadKind::Exemplar,
            format!(
                "remote write exemplar payload exceeds limit: {} > {exemplar_limit}",
                exemplars
            ),
        ));
    }
    if histogram_bucket_entries > config.max_histogram_bucket_entries_per_request {
        return Err((
            PrometheusPayloadKind::Histogram,
            format!(
                "remote write histogram bucket payload exceeds limit: {} > {}",
                histogram_bucket_entries, config.max_histogram_bucket_entries_per_request
            ),
        ));
    }
    Ok(())
}

fn internal_payload_disabled_response(
    _kind: PrometheusPayloadKind,
    message: String,
) -> HttpResponse {
    internal_error_response(422, "payload_disabled", message, false)
}

fn internal_payload_too_large_response(
    kind: PrometheusPayloadKind,
    message: String,
) -> HttpResponse {
    let _ = kind;
    internal_error_response(422, "payload_too_large", message, false)
}

async fn preflight_ingest_rows_capabilities(
    cluster_context: &ClusterRequestContext,
    endpoint: &str,
    ring_version: u64,
    required_capabilities: &[String],
) -> Result<(), String> {
    if required_capabilities.is_empty() {
        return Ok(());
    }
    cluster_context
        .rpc_client
        .ingest_rows(
            endpoint,
            &InternalIngestRowsRequest {
                ring_version: ring_version.max(1),
                idempotency_key: None,
                required_capabilities: required_capabilities.to_vec(),
                rows: Vec::new(),
            },
        )
        .await
        .map(|_| ())
        .map_err(|err| format!("{err}"))
}

async fn preflight_ingest_write_capabilities(
    cluster_context: &ClusterRequestContext,
    endpoint: &str,
    ring_version: u64,
    required_capabilities: &[String],
) -> Result<(), String> {
    if required_capabilities.is_empty() {
        return Ok(());
    }
    cluster_context
        .rpc_client
        .ingest_write(
            endpoint,
            &InternalIngestWriteRequest {
                ring_version: ring_version.max(1),
                idempotency_key: None,
                tenant_id: None,
                required_capabilities: required_capabilities.to_vec(),
                rows: Vec::new(),
                metadata_updates: Vec::new(),
                exemplars: Vec::new(),
            },
        )
        .await
        .map(|_| ())
        .map_err(|err| format!("{err}"))
}

async fn preflight_histogram_rows_with_cluster(
    write_router: &WriteRouter,
    cluster_context: &ClusterRequestContext,
    rows: &[Row],
    ring_version: u64,
) -> Result<(), String> {
    let required_capabilities = required_capabilities_for_rows(rows);
    if required_capabilities.is_empty() {
        return Ok(());
    }

    let plan = write_router
        .plan_rows(rows.to_vec())
        .map_err(|err| format!("cluster histogram write planning failed: {err}"))?;
    let mut endpoints = BTreeSet::new();
    for batch in plan.remote_batches {
        endpoints.insert(batch.endpoint);
    }
    for endpoint in endpoints {
        preflight_ingest_rows_capabilities(
            cluster_context,
            &endpoint,
            ring_version,
            &required_capabilities,
        )
        .await
        .map_err(|err| {
            format!("histogram peer capability preflight failed for {endpoint}: {err}")
        })?;
    }

    Ok(())
}

async fn replicate_metadata_updates_with_cluster(
    metadata_store: &Arc<MetricMetadataStore>,
    cluster_context: &ClusterRequestContext,
    tenant_id: &str,
    updates: &[NormalizedMetricMetadataUpdate],
    ring_version: u64,
) -> Result<usize, String> {
    if updates.is_empty() {
        return Ok(0);
    }

    let (membership, _) = effective_write_topology(cluster_context)?;
    let local_node_id = membership.local_node_id.clone();
    let remote_nodes = membership
        .nodes
        .iter()
        .filter(|node| node.id != local_node_id)
        .cloned()
        .collect::<Vec<_>>();
    let required_capabilities = payload_required_capabilities(PrometheusPayloadKind::Metadata);
    for node in &remote_nodes {
        preflight_ingest_write_capabilities(
            cluster_context,
            &node.endpoint,
            ring_version,
            &required_capabilities,
        )
        .await
        .map_err(|err| {
            format!(
                "metadata peer capability preflight failed for {}: {err}",
                node.endpoint
            )
        })?;
    }

    let internal_updates = updates
        .iter()
        .map(normalized_metadata_to_internal_update)
        .collect::<Vec<_>>();
    for node in &remote_nodes {
        cluster_context
            .rpc_client
            .ingest_write(
                &node.endpoint,
                &InternalIngestWriteRequest {
                    ring_version: ring_version.max(1),
                    idempotency_key: Some(build_metadata_batch_idempotency_key(
                        &local_node_id,
                        &node.id,
                        tenant_id,
                        &internal_updates,
                    )),
                    tenant_id: Some(tenant_id.to_string()),
                    required_capabilities: required_capabilities.clone(),
                    rows: Vec::new(),
                    metadata_updates: internal_updates.clone(),
                    exemplars: Vec::new(),
                },
            )
            .await
            .map_err(|err| {
                format!(
                    "remote metadata write to node '{}' ({}) failed: {err}",
                    node.id, node.endpoint
                )
            })?;
    }

    metadata_store.apply_updates(tenant_id, updates)
}

fn stable_fnv1a64_update(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn stable_exemplar_fingerprint(exemplar: &InternalWriteExemplar) -> u64 {
    let mut hash = stable_series_identity_hash(&exemplar.metric, &exemplar.series_labels);
    hash = stable_fnv1a64_update(hash, &exemplar.timestamp.to_le_bytes());
    let value_bits = if exemplar.value.is_nan() {
        f64::NAN.to_bits()
    } else {
        exemplar.value.to_bits()
    };
    hash = stable_fnv1a64_update(hash, &value_bits.to_le_bytes());
    for label in &exemplar.exemplar_labels {
        hash = stable_fnv1a64_update(hash, label.name.as_bytes());
        hash = stable_fnv1a64_update(hash, b"=");
        hash = stable_fnv1a64_update(hash, label.value.as_bytes());
    }
    hash
}

fn stable_metadata_update_fingerprint(update: &InternalMetricMetadataUpdate) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    hash = stable_fnv1a64_update(hash, update.metric_family_name.as_bytes());
    hash = stable_fnv1a64_update(hash, b":");
    hash = stable_fnv1a64_update(hash, &update.metric_type.to_le_bytes());
    hash = stable_fnv1a64_update(hash, b":");
    hash = stable_fnv1a64_update(hash, update.help.as_bytes());
    hash = stable_fnv1a64_update(hash, b":");
    hash = stable_fnv1a64_update(hash, update.unit.as_bytes());
    hash
}

fn build_metadata_batch_idempotency_key(
    coordinator_node_id: &str,
    owner_node_id: &str,
    tenant_id: &str,
    updates: &[InternalMetricMetadataUpdate],
) -> String {
    let mut fingerprints = updates
        .iter()
        .map(stable_metadata_update_fingerprint)
        .collect::<Vec<_>>();
    fingerprints.sort_unstable();

    let mut hash = 0xcbf29ce484222325u64;
    hash = stable_fnv1a64_update(hash, b"tsink-metadata-idem-v1");
    hash = stable_fnv1a64_update(hash, tenant_id.as_bytes());
    hash = stable_fnv1a64_update(hash, b":");
    hash = stable_fnv1a64_update(hash, coordinator_node_id.as_bytes());
    hash = stable_fnv1a64_update(hash, b"->");
    hash = stable_fnv1a64_update(hash, owner_node_id.as_bytes());
    hash = stable_fnv1a64_update(hash, &(fingerprints.len() as u64).to_le_bytes());
    for fingerprint in fingerprints {
        hash = stable_fnv1a64_update(hash, &fingerprint.to_le_bytes());
    }

    let mut routing_hash = 0xcbf29ce484222325u64;
    routing_hash = stable_fnv1a64_update(routing_hash, tenant_id.as_bytes());
    routing_hash = stable_fnv1a64_update(routing_hash, b":");
    routing_hash = stable_fnv1a64_update(routing_hash, coordinator_node_id.as_bytes());
    routing_hash = stable_fnv1a64_update(routing_hash, b"->");
    routing_hash = stable_fnv1a64_update(routing_hash, owner_node_id.as_bytes());
    format!("tsink:md:v1:{routing_hash:016x}:{hash:016x}")
}

fn build_exemplar_batch_idempotency_key(
    coordinator_node_id: &str,
    owner_node_id: &str,
    exemplars: &[InternalWriteExemplar],
) -> String {
    let mut fingerprints = exemplars
        .iter()
        .map(stable_exemplar_fingerprint)
        .collect::<Vec<_>>();
    fingerprints.sort_unstable();

    let mut hash = 0xcbf29ce484222325u64;
    hash = stable_fnv1a64_update(hash, b"tsink-exemplar-idem-v1");
    hash = stable_fnv1a64_update(hash, coordinator_node_id.as_bytes());
    hash = stable_fnv1a64_update(hash, b"->");
    hash = stable_fnv1a64_update(hash, owner_node_id.as_bytes());
    hash = stable_fnv1a64_update(hash, &(fingerprints.len() as u64).to_le_bytes());
    for fingerprint in fingerprints {
        hash = stable_fnv1a64_update(hash, &fingerprint.to_le_bytes());
    }

    let mut routing_hash = 0xcbf29ce484222325u64;
    routing_hash = stable_fnv1a64_update(routing_hash, coordinator_node_id.as_bytes());
    routing_hash = stable_fnv1a64_update(routing_hash, b"->");
    routing_hash = stable_fnv1a64_update(routing_hash, owner_node_id.as_bytes());
    format!("tsink:ex:v1:{routing_hash:016x}:{hash:016x}")
}

fn effective_write_topology(
    cluster_context: &ClusterRequestContext,
) -> Result<(MembershipView, ShardRing), String> {
    let Some(state) = current_control_state(Some(cluster_context)) else {
        return Ok((
            cluster_context.runtime.membership.clone(),
            cluster_context.runtime.ring.clone(),
        ));
    };
    let membership = membership_from_control_state(cluster_context, &state)?;
    let ring =
        ShardRing::from_snapshot(state.effective_ring_snapshot_at_ring_version(state.ring_version))
            .map_err(|err| {
                format!("failed to restore control-state ring for exemplar routing: {err}")
            })?;
    Ok((membership, ring))
}

async fn route_exemplars_with_consistency_and_ring_version(
    exemplar_store: &Arc<ExemplarStore>,
    cluster_context: &ClusterRequestContext,
    exemplars: Vec<NormalizedExemplar>,
    mode: ClusterWriteConsistency,
    ring_version: u64,
) -> Result<ExemplarClusterWriteStats, String> {
    if exemplars.is_empty() {
        return Ok(ExemplarClusterWriteStats::default());
    }

    let (membership, ring) = effective_write_topology(cluster_context)?;
    let local_node_id = membership.local_node_id.clone();
    let endpoints = membership
        .nodes
        .iter()
        .map(|node| (node.id.clone(), node.endpoint.clone()))
        .collect::<BTreeMap<_, _>>();

    let mut local_exemplars = Vec::new();
    let mut local_shards = BTreeSet::new();
    let mut remote_batches = BTreeMap::<String, RoutedExemplarBatch>::new();
    let mut shard_state = BTreeMap::<u32, ExemplarShardAckState>::new();

    for exemplar in &exemplars {
        let series_hash =
            stable_series_identity_hash(&exemplar.series.metric, &exemplar.series.labels);
        let shard = ring.shard_for_series_id(series_hash);
        let owners = ring
            .owners_for_shard(shard)
            .ok_or_else(|| format!("exemplar routing failed: shard {shard} has no owner"))?
            .to_vec();
        shard_state.entry(shard).or_insert(ExemplarShardAckState {
            required_acks: mode.required_acks(u16::try_from(owners.len()).unwrap_or(u16::MAX)),
            acknowledged_acks: 0,
        });

        let internal = normalized_exemplar_to_internal_write(exemplar);
        for owner in owners {
            if owner == local_node_id {
                local_shards.insert(shard);
                local_exemplars.push(internal.clone());
            } else {
                let endpoint = endpoints.get(&owner).cloned().ok_or_else(|| {
                    format!("exemplar routing failed: owner node '{owner}' has no known endpoint")
                })?;
                let batch =
                    remote_batches
                        .entry(owner.clone())
                        .or_insert_with(|| RoutedExemplarBatch {
                            owner_node_id: owner.clone(),
                            endpoint,
                            exemplars: Vec::new(),
                            shards: BTreeSet::new(),
                        });
                batch.exemplars.push(internal.clone());
                batch.shards.insert(shard);
            }
        }
    }

    let exemplar_required_capabilities =
        payload_required_capabilities(PrometheusPayloadKind::Exemplar);
    let mut preflight_endpoints = BTreeSet::new();
    for batch in remote_batches.values() {
        preflight_endpoints.insert(batch.endpoint.clone());
    }
    for endpoint in preflight_endpoints {
        preflight_ingest_write_capabilities(
            cluster_context,
            &endpoint,
            ring_version,
            &exemplar_required_capabilities,
        )
        .await
        .map_err(|err| {
            format!("exemplar peer capability preflight failed for {endpoint}: {err}")
        })?;
    }

    let mut accepted_exemplars = 0usize;
    let mut dropped_exemplars = 0usize;

    if !local_exemplars.is_empty() {
        let outcome = exemplar_store.apply_writes(
            &local_exemplars
                .into_iter()
                .map(internal_write_exemplar_to_store_write)
                .collect::<Vec<_>>(),
        )?;
        accepted_exemplars = accepted_exemplars.saturating_add(outcome.accepted);
        dropped_exemplars = dropped_exemplars.saturating_add(outcome.dropped);
        for shard in &local_shards {
            if let Some(state) = shard_state.get_mut(shard) {
                state.acknowledged_acks = state.acknowledged_acks.saturating_add(1);
            }
        }
    }

    let remote_exemplar_limit = exemplar_store.config().max_exemplars_per_request;
    for (_, mut batch) in remote_batches {
        if batch.exemplars.len() > remote_exemplar_limit {
            return Err(format!(
                "exemplar routing failed for node '{}': remote batch exceeds exemplar limit {} > {remote_exemplar_limit}",
                batch.owner_node_id,
                batch.exemplars.len(),
            ));
        }

        let response = cluster_context
            .rpc_client
            .ingest_write(
                &batch.endpoint,
                &InternalIngestWriteRequest {
                    ring_version: ring_version.max(1),
                    idempotency_key: Some(build_exemplar_batch_idempotency_key(
                        &local_node_id,
                        &batch.owner_node_id,
                        &batch.exemplars,
                    )),
                    tenant_id: None,
                    required_capabilities: exemplar_required_capabilities.clone(),
                    rows: Vec::new(),
                    metadata_updates: Vec::new(),
                    exemplars: std::mem::take(&mut batch.exemplars),
                },
            )
            .await
            .map_err(|err| {
                format!(
                    "remote exemplar write to node '{}' ({}) failed: {err}",
                    batch.owner_node_id, batch.endpoint
                )
            })?;

        accepted_exemplars = accepted_exemplars.saturating_add(response.accepted_exemplars);
        dropped_exemplars = dropped_exemplars.saturating_add(response.dropped_exemplars);
        for shard in &batch.shards {
            if let Some(state) = shard_state.get_mut(shard) {
                state.acknowledged_acks = state.acknowledged_acks.saturating_add(1);
            }
        }
    }

    if let Some((shard, state)) = shard_state
        .iter()
        .find(|(_, state)| state.acknowledged_acks < state.required_acks)
    {
        return Err(format!(
            "exemplar replication failed for shard {shard} in {mode} mode: required {} acks, got {}",
            state.required_acks, state.acknowledged_acks
        ));
    }

    Ok(ExemplarClusterWriteStats {
        accepted_exemplars,
        dropped_exemplars,
        consistency: shard_state
            .values()
            .map(|state| state.acknowledged_acks)
            .min()
            .map(|acked| WriteConsistencyOutcome {
                mode,
                required_acks: shard_state
                    .values()
                    .map(|state| state.required_acks)
                    .min()
                    .unwrap_or(0),
                acknowledged_replicas_min: acked,
            }),
    })
}

#[derive(Debug, Clone, Default)]
struct AppliedWriteEnvelope {
    consistency: Option<WriteConsistencyOutcome>,
    applied_metadata_updates: usize,
    accepted_exemplars: usize,
    dropped_exemplars: usize,
}

#[allow(clippy::too_many_arguments)]
async fn apply_normalized_write_envelope(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    tenant_request: &tenant::TenantRequestGuard,
    tenant_id: &str,
    envelope: NormalizedWriteEnvelope,
    write_admission: &WriteAdmissionController,
    request_slot: admission::WriteRequestSlotLease,
    preflight_native_histograms: bool,
) -> Result<AppliedWriteEnvelope, HttpResponse> {
    let metadata_updates = envelope.metadata_updates.clone();
    let exemplars = envelope.exemplars.clone();
    let rows = envelope.into_rows();

    if rows.is_empty() && exemplars.is_empty() && metadata_updates.is_empty() {
        return Ok(AppliedWriteEnvelope::default());
    }
    if let Err(err) = tenant::enforce_write_rows_quota(tenant_request.policy(), rows.len()) {
        return Err(HttpResponse::new(413, err).with_header("Content-Type", "text/plain"));
    }
    hotspot::record_ingest_rows(cluster_context.map(|context| &context.runtime.ring), &rows);
    let _write_admission = if rows.is_empty() {
        None
    } else {
        match write_admission.reserve_rows(request_slot, rows.len()).await {
            Ok(lease) => Some(lease),
            Err(err) => return Err(write_admission_error_response(err)),
        }
    };

    if let Some(cluster_context) = cluster_context {
        let ring_version = cluster_ring_version(Some(cluster_context));
        let write_router = match effective_write_router(cluster_context) {
            Ok(router) => router,
            Err(err) => {
                return Err(text_response(
                    503,
                    &format!("cluster write routing topology unavailable: {err}"),
                ))
            }
        };
        let write_router = if let Some(mode) = tenant_request.policy().write_consistency {
            write_router.with_default_write_consistency(mode)
        } else {
            write_router
        };
        let requested_consistency =
            match resolve_request_write_consistency(request, tenant_request.policy()) {
                Ok(mode) => mode,
                Err(err) => return Err(text_response(400, &err)),
            };
        if preflight_native_histograms && !rows.is_empty() {
            if let Err(err) = preflight_histogram_rows_with_cluster(
                &write_router,
                cluster_context,
                &rows,
                ring_version,
            )
            .await
            {
                return Err(text_response(
                    409,
                    &format!("cluster histogram write rejected: {err}"),
                ));
            }
        }

        let row_stats = if rows.is_empty() {
            None
        } else {
            match write_router
                .route_and_write_with_consistency_and_ring_version(
                    storage,
                    &cluster_context.rpc_client,
                    rows,
                    requested_consistency,
                    ring_version,
                )
                .await
            {
                Ok(stats) => Some(stats),
                Err(err) => return Err(write_routing_error_response(err)),
            }
        };

        let applied_metadata_updates = if metadata_updates.is_empty() {
            0usize
        } else {
            match replicate_metadata_updates_with_cluster(
                metadata_store,
                cluster_context,
                tenant_id,
                &metadata_updates,
                ring_version,
            )
            .await
            {
                Ok(applied) => applied,
                Err(err) => {
                    return Err(text_response(
                        409,
                        &format!("cluster metadata write rejected: {err}"),
                    ))
                }
            }
        };

        let exemplar_stats = if exemplars.is_empty() {
            None
        } else {
            match route_exemplars_with_consistency_and_ring_version(
                exemplar_store,
                cluster_context,
                exemplars,
                requested_consistency.unwrap_or(cluster_context.runtime.write_consistency),
                ring_version,
            )
            .await
            {
                Ok(stats) => Some(stats),
                Err(err) => return Err(text_response(409, &err)),
            }
        };

        return Ok(AppliedWriteEnvelope {
            consistency: row_stats
                .as_ref()
                .and_then(|stats| stats.consistency)
                .or_else(|| exemplar_stats.as_ref().and_then(|stats| stats.consistency)),
            applied_metadata_updates,
            accepted_exemplars: exemplar_stats
                .as_ref()
                .map(|stats| stats.accepted_exemplars)
                .unwrap_or(0),
            dropped_exemplars: exemplar_stats
                .as_ref()
                .map(|stats| stats.dropped_exemplars)
                .unwrap_or(0),
        });
    }

    if !rows.is_empty() {
        let edge_rows = rows.clone();
        let storage = Arc::clone(storage);
        let result = tokio::task::spawn_blocking(move || storage.insert_rows(&rows)).await;
        match result {
            Ok(Ok(())) => maybe_enqueue_edge_sync_rows(edge_sync_context, &edge_rows),
            Ok(Err(err)) => return Err(storage_write_error_response("insert", &err)),
            Err(err) => return Err(text_response(500, &format!("insert task failed: {err}"))),
        }
    }

    let applied_metadata_updates = if metadata_updates.is_empty() {
        0usize
    } else {
        match metadata_store.apply_updates(tenant_id, &metadata_updates) {
            Ok(applied) => applied,
            Err(err) => {
                return Err(text_response(
                    500,
                    &format!("metadata update failed: {err}"),
                ))
            }
        }
    };

    if exemplars.is_empty() {
        return Ok(AppliedWriteEnvelope {
            consistency: None,
            applied_metadata_updates,
            accepted_exemplars: 0,
            dropped_exemplars: 0,
        });
    }

    match exemplar_store.apply_writes(
        &exemplars
            .into_iter()
            .map(normalized_exemplar_to_store_write)
            .collect::<Vec<_>>(),
    ) {
        Ok(outcome) => Ok(AppliedWriteEnvelope {
            consistency: None,
            applied_metadata_updates,
            accepted_exemplars: outcome.accepted,
            dropped_exemplars: outcome.dropped,
        }),
        Err(err) => Err(text_response(
            500,
            &format!("exemplar insert failed: {err}"),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn ingest_adapter_write_envelope(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    usage_accounting: Option<&UsageAccounting>,
    tenant_id: &str,
    source: &str,
    envelope: NormalizedWriteEnvelope,
) -> Result<(), String> {
    let started = Instant::now();
    let write_admission = admission::global_public_write_admission()
        .map_err(|err| format!("write admission unavailable: {err}"))?;
    let histogram_count = envelope.histogram_samples.len() as u64;
    let ingest_units = envelope_row_units(&envelope);
    let tenant_plan = tenant::prepare_trusted_request_plan(
        tenant_registry,
        tenant_id,
        tenant::TenantAccessScope::Write,
    )
    .map_err(|err| http_response_message(err.to_http_response()))?;
    if let Err(err) = tenant::enforce_write_rows_quota(tenant_plan.policy(), ingest_units) {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Ingest,
            ingest_units,
            err.clone(),
        );
        return Err(err);
    }
    let tenant_request = tenant_plan
        .admit(tenant::TenantAdmissionSurface::Ingest, ingest_units)
        .map_err(|err| http_response_message(err.to_http_response()))?;
    let request_slot = match write_admission.acquire_request_slot().await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Ingest,
                ingest_units,
                err.to_string(),
            );
            return Err(write_admission_error_text(err));
        }
    };
    let request = HttpRequest {
        method: "POST".to_string(),
        path: "/internal/legacy-ingest".to_string(),
        headers: Default::default(),
        body: Vec::new(),
    };
    apply_normalized_write_envelope(
        storage,
        metadata_store,
        exemplar_store,
        &request,
        cluster_context,
        edge_sync_context,
        &tenant_request,
        tenant_id,
        envelope,
        write_admission,
        request_slot,
        false,
    )
    .await
    .map(|result| {
        record_ingest_usage(
            usage_accounting,
            tenant_id,
            "legacy_ingest",
            source,
            IngestUsageMetrics::new(
                ingest_units as u64,
                result.applied_metadata_updates as u64,
                result.accepted_exemplars as u64,
                result.dropped_exemplars as u64,
                histogram_count,
                elapsed_nanos_since(started),
                0,
            ),
        );
    })
    .map_err(http_response_message)
}

fn maybe_enqueue_edge_sync_rows(
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    rows: &[Row],
) {
    if rows.is_empty() {
        return;
    }
    if let Some(runtime) = edge_sync_context.and_then(|context| context.source.as_ref()) {
        runtime.enqueue_rows(rows);
    }
}

fn http_response_message(response: HttpResponse) -> String {
    let body = String::from_utf8_lossy(&response.body).trim().to_string();
    if body.is_empty() {
        format!("request failed with HTTP {}", response.status)
    } else {
        body
    }
}

fn write_admission_error_text(err: WriteAdmissionError) -> String {
    http_response_message(write_admission_error_response(err))
}

#[allow(clippy::too_many_arguments)]
async fn handle_influx_line_protocol(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let started = Instant::now();
    let config = legacy_ingest::influx_line_protocol_config();
    if !config.enabled {
        legacy_ingest::record_request_rejected(LegacyAdapterKind::InfluxLineProtocol, 0);
        return text_response(422, "influx line protocol ingest is disabled on this node");
    }

    let write_admission = match admission::global_public_write_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("write admission unavailable: {err}")),
    };
    let tenant_id = match tenant_id_for_text_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        tenant_registry,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Write,
    ) {
        Ok(tenant_request) => tenant_request,
        Err(response) => return response,
    };
    let body = match std::str::from_utf8(&request.body) {
        Ok(body) => body,
        Err(_) => {
            legacy_ingest::record_request_rejected(LegacyAdapterKind::InfluxLineProtocol, 0);
            return text_response(400, "influx line protocol body must be valid UTF-8");
        }
    };

    let mut query_labels = Vec::new();
    for (param, label) in [
        ("db", "influx_db"),
        ("rp", "influx_rp"),
        ("bucket", "influx_bucket"),
        ("org", "influx_org"),
    ] {
        if let Some(value) = request.query_param(param) {
            let value = value.trim();
            if !value.is_empty() {
                query_labels.push((label.to_string(), value.to_string()));
            }
        }
    }

    let normalized = match legacy_ingest::normalize_influx_line_protocol(
        body,
        &tenant_id,
        precision,
        current_timestamp(precision),
        config,
        query_labels,
        request.query_param("precision").as_deref(),
    ) {
        Ok(normalized) => normalized,
        Err(err) => {
            if legacy_ingest_error_is_throttled(&err) {
                legacy_ingest::record_request_throttled(LegacyAdapterKind::InfluxLineProtocol, 0);
                return HttpResponse::new(413, err).with_header("Content-Type", "text/plain");
            }
            legacy_ingest::record_request_rejected(LegacyAdapterKind::InfluxLineProtocol, 0);
            return text_response(400, &err);
        }
    };

    if normalized.request_units == 0 {
        legacy_ingest::record_request_accepted(LegacyAdapterKind::InfluxLineProtocol, 0);
        return HttpResponse::new(204, Vec::<u8>::new());
    }

    if let Err(err) =
        tenant::enforce_write_rows_quota(tenant_plan.policy(), normalized.request_units)
    {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Ingest,
            normalized.request_units,
            err.clone(),
        );
        return HttpResponse::new(413, err).with_header("Content-Type", "text/plain");
    }
    let tenant_request = match tenant_plan.admit(
        tenant::TenantAdmissionSurface::Ingest,
        normalized.request_units,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let request_slot = match write_admission.acquire_request_slot().await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Ingest,
                normalized.request_units,
                err.to_string(),
            );
            return write_admission_error_response(err);
        }
    };

    let apply_result = match apply_normalized_write_envelope(
        storage,
        metadata_store,
        exemplar_store,
        request,
        cluster_context,
        edge_sync_context,
        &tenant_request,
        &tenant_id,
        normalized.envelope,
        write_admission,
        request_slot,
        false,
    )
    .await
    {
        Ok(result) => {
            legacy_ingest::record_request_accepted(
                LegacyAdapterKind::InfluxLineProtocol,
                normalized.sample_count,
            );
            result
        }
        Err(response) => {
            record_legacy_ingest_failure(
                LegacyAdapterKind::InfluxLineProtocol,
                response.status,
                normalized.sample_count,
            );
            return response;
        }
    };
    record_ingest_usage(
        usage_accounting,
        &tenant_id,
        "influx_line_protocol",
        request.path_without_query(),
        IngestUsageMetrics::new(
            normalized.request_units as u64,
            apply_result.applied_metadata_updates as u64,
            apply_result.accepted_exemplars as u64,
            apply_result.dropped_exemplars as u64,
            0,
            elapsed_nanos_since(started),
            request.body.len() as u64,
        ),
    );

    let mut response = HttpResponse::new(204, Vec::<u8>::new())
        .with_header(
            "X-Tsink-Influx-Lines-Accepted",
            normalized.request_units.to_string(),
        )
        .with_header(
            "X-Tsink-Influx-Samples-Accepted",
            normalized.sample_count.to_string(),
        );
    if let Some(consistency) = apply_result.consistency {
        response = response
            .with_header("X-Tsink-Write-Consistency", consistency.mode.to_string())
            .with_header(
                "X-Tsink-Write-Required-Acks",
                consistency.required_acks.to_string(),
            )
            .with_header(
                "X-Tsink-Write-Acknowledged-Replicas",
                consistency.acknowledged_replicas_min.to_string(),
            );
    }
    if apply_result.applied_metadata_updates > 0 {
        response = response.with_header(
            "X-Tsink-Metadata-Applied",
            apply_result.applied_metadata_updates.to_string(),
        );
    }
    response
}

fn record_legacy_ingest_failure(kind: LegacyAdapterKind, status: u16, sample_count: usize) {
    if status == 413 || status == 429 {
        legacy_ingest::record_request_throttled(kind, sample_count);
    } else {
        legacy_ingest::record_request_rejected(kind, sample_count);
    }
}

fn legacy_ingest_error_is_throttled(err: &str) -> bool {
    err.contains("exceeds line limit")
        || err.contains("exceeds event limit")
        || err.contains("exceeds byte limit")
}

fn decode_body(request: &HttpRequest) -> Result<Vec<u8>, String> {
    match request.header("content-encoding") {
        None => Ok(request.body.clone()),
        Some(encoding) if encoding.eq_ignore_ascii_case("identity") => Ok(request.body.clone()),
        Some(encoding) if encoding.eq_ignore_ascii_case("snappy") => {
            let decoded_len = decompress_len(&request.body)
                .map_err(|err| format!("snappy decode failed: {err}"))?;
            if decoded_len > MAX_BODY_BYTES {
                return Err(format!(
                    "decoded request body too large: {decoded_len} bytes (max {MAX_BODY_BYTES})"
                ));
            }
            let decoded = SnappyDecoder::new()
                .decompress_vec(&request.body)
                .map_err(|err| format!("snappy decode failed: {err}"))?;
            if decoded.len() > MAX_BODY_BYTES {
                return Err(format!(
                    "decoded request body too large: {} bytes (max {MAX_BODY_BYTES})",
                    decoded.len()
                ));
            }
            Ok(decoded)
        }
        Some(encoding) => Err(format!("unsupported content-encoding: {encoding}")),
    }
}

fn write_routing_error_response(err: WriteRoutingError) -> HttpResponse {
    let status = match &err {
        WriteRoutingError::InvalidConsistencyOverride { .. } => 400,
        WriteRoutingError::ConsistencyTimeout { .. } => 504,
        WriteRoutingError::InsufficientReplicas { .. } => 503,
        WriteRoutingError::OutboxEnqueue { source, .. } if source.retryable() => 503,
        WriteRoutingError::OutboxEnqueue { .. } => 507,
        _ if err.retryable() => 503,
        _ => 500,
    };
    text_response(status, &err.to_string())
}

fn parse_request_write_consistency(
    request: &HttpRequest,
) -> Result<Option<ClusterWriteConsistency>, String> {
    let Some(raw_value) = request.header(WRITE_CONSISTENCY_OVERRIDE_HEADER) else {
        return Ok(None);
    };
    let trimmed = raw_value.trim();
    if trimmed.is_empty() {
        return Err(format!(
            "{WRITE_CONSISTENCY_OVERRIDE_HEADER} must not be empty when provided"
        ));
    }
    WriteRouter::parse_write_consistency_override(trimmed)
        .map(Some)
        .map_err(|err| format!("{WRITE_CONSISTENCY_OVERRIDE_HEADER}: {err}"))
}

fn resolve_request_write_consistency(
    request: &HttpRequest,
    tenant_policy: &tenant::TenantRequestPolicy,
) -> Result<Option<ClusterWriteConsistency>, String> {
    let requested = parse_request_write_consistency(request)?;
    let Some(configured) = tenant_policy.write_consistency else {
        return Ok(requested);
    };
    if requested.is_some_and(|mode| mode != configured) {
        return Err(format!(
            "{WRITE_CONSISTENCY_OVERRIDE_HEADER}: tenant write consistency is fixed to '{configured}'"
        ));
    }
    Ok(Some(configured))
}

fn parse_request_read_partial_response_policy(
    request: &HttpRequest,
) -> Result<Option<ClusterReadPartialResponsePolicy>, String> {
    let Some(raw_value) = request.header(READ_PARTIAL_RESPONSE_OVERRIDE_HEADER) else {
        return Ok(None);
    };
    let trimmed = raw_value.trim();
    if trimmed.is_empty() {
        return Err(format!(
            "{READ_PARTIAL_RESPONSE_OVERRIDE_HEADER} must not be empty when provided"
        ));
    }
    ClusterReadPartialResponsePolicy::from_str(trimmed)
        .map(Some)
        .map_err(|err| format!("{READ_PARTIAL_RESPONSE_OVERRIDE_HEADER}: {err}"))
}

fn apply_request_read_policies(
    request: &HttpRequest,
    cluster_context: &ClusterRequestContext,
    read_fanout: ReadFanoutExecutor,
    tenant_policy: &tenant::TenantRequestPolicy,
) -> Result<ReadFanoutExecutor, String> {
    let read_fanout = if let Some(consistency) = tenant_policy.read_consistency {
        read_fanout.with_read_consistency(consistency)
    } else {
        read_fanout
    };
    let requested = parse_request_read_partial_response_policy(request)?;
    let configured = tenant_policy
        .read_partial_response_policy
        .unwrap_or(cluster_context.runtime.read_partial_response);
    let effective = requested.unwrap_or(configured);
    if matches!(configured, ClusterReadPartialResponsePolicy::Deny)
        && matches!(effective, ClusterReadPartialResponsePolicy::Allow)
    {
        return Err(format!(
            "{READ_PARTIAL_RESPONSE_OVERRIDE_HEADER}: allow is not permitted when cluster read partial response policy is deny"
        ));
    }
    Ok(read_fanout.with_partial_response_policy(effective))
}

fn default_read_response_metadata(read_fanout: &ReadFanoutExecutor) -> ReadFanoutResponseMetadata {
    ReadFanoutResponseMetadata {
        consistency: read_fanout.read_consistency_mode(),
        partial_response_policy: read_fanout.read_partial_response_policy(),
        partial_response: false,
        warnings: Vec::new(),
    }
}

fn merge_read_response_metadata(
    aggregate: &mut ReadFanoutResponseMetadata,
    update: &ReadFanoutResponseMetadata,
) {
    aggregate.consistency = update.consistency;
    aggregate.partial_response_policy = update.partial_response_policy;
    aggregate.partial_response |= update.partial_response;
    if !update.warnings.is_empty() {
        aggregate.warnings.extend(update.warnings.iter().cloned());
        aggregate.warnings.sort();
        aggregate.warnings.dedup();
    }
}

fn with_read_metadata_headers(
    response: HttpResponse,
    metadata: &ReadFanoutResponseMetadata,
) -> HttpResponse {
    response
        .with_header(READ_CONSISTENCY_HEADER, metadata.consistency.to_string())
        .with_header(
            READ_PARTIAL_RESPONSE_POLICY_HEADER,
            metadata.partial_response_policy.to_string(),
        )
        .with_header(
            READ_PARTIAL_RESPONSE_HEADER,
            if metadata.partial_response {
                "true"
            } else {
                "false"
            },
        )
        .with_header(
            READ_PARTIAL_WARNINGS_HEADER,
            metadata.warnings.len().to_string(),
        )
}

fn cluster_json_success_response(
    data: JsonValue,
    metadata: &ReadFanoutResponseMetadata,
) -> HttpResponse {
    let mut payload = json!({
        "status": "success",
        "data": data,
        "partialResponse": {
            "enabled": metadata.partial_response,
            "policy": metadata.partial_response_policy.to_string(),
            "consistency": metadata.consistency.to_string(),
            "warningCount": metadata.warnings.len(),
        }
    });
    if metadata.partial_response && !metadata.warnings.is_empty() {
        payload["warnings"] = JsonValue::Array(
            metadata
                .warnings
                .iter()
                .cloned()
                .map(JsonValue::String)
                .collect(),
        );
    }
    with_read_metadata_headers(json_response(200, &payload), metadata)
}

#[derive(Debug, Serialize)]
struct MetadataApiEntry {
    #[serde(rename = "type")]
    metric_type: String,
    help: String,
    unit: String,
}

fn metadata_query_limit(request: &HttpRequest) -> Result<usize, HttpResponse> {
    let Some(limit) = request.param("limit") else {
        return Ok(METADATA_API_DEFAULT_LIMIT);
    };
    let parsed = limit.parse::<usize>().map_err(|_| {
        promql_error_response("bad_data", "parameter 'limit' must be a positive integer")
    })?;
    if parsed == 0 {
        return Err(promql_error_response(
            "bad_data",
            "parameter 'limit' must be greater than zero",
        ));
    }
    Ok(parsed.min(METADATA_API_MAX_LIMIT))
}

fn metadata_response_payload(
    records: Vec<MetricMetadataRecord>,
) -> BTreeMap<String, Vec<MetadataApiEntry>> {
    let mut data = BTreeMap::new();
    for record in records {
        data.insert(
            record.metric_family_name,
            vec![MetadataApiEntry {
                metric_type: metric_type_to_api_string(record.metric_type).to_string(),
                help: record.help,
                unit: record.unit,
            }],
        );
    }
    data
}

fn write_admission_error_response(err: WriteAdmissionError) -> HttpResponse {
    let (status, error_code, retry_after_seconds) = if err.retryable() {
        (429, "write_overloaded", Some("1"))
    } else {
        (413, "write_resource_limit_exceeded", None)
    };
    let mut response =
        text_response(status, &err.to_string()).with_header(WRITE_ERROR_CODE_HEADER, error_code);
    if let Some(retry_after) = retry_after_seconds {
        response = response.with_header("Retry-After", retry_after);
    }
    response
}

fn classify_storage_write_error(err: &tsink::TsinkError) -> Option<(u16, &'static str)> {
    match err {
        tsink::TsinkError::OutOfRetention { .. } => Some((422, "write_out_of_retention")),
        _ => None,
    }
}

fn storage_write_error_response(action: &str, err: &tsink::TsinkError) -> HttpResponse {
    let (status, error_code) = classify_storage_write_error(err).unwrap_or((500, ""));
    let mut response = text_response(status, &format!("{action} failed: {err}"));
    if !error_code.is_empty() {
        response = response.with_header(WRITE_ERROR_CODE_HEADER, error_code);
    }
    response
}

fn read_admission_error_response(err: ReadAdmissionError) -> HttpResponse {
    let (status, error_code, retry_after_seconds) = if err.retryable() {
        (429, "read_overloaded", Some("1"))
    } else {
        (413, "read_resource_limit_exceeded", None)
    };
    let mut response =
        text_response(status, &err.to_string()).with_header(READ_ERROR_CODE_HEADER, error_code);
    if let Some(retry_after) = retry_after_seconds {
        response = response.with_header("Retry-After", retry_after);
    }
    response
}

fn fanout_error_response(err: ReadFanoutError) -> HttpResponse {
    let (status, error_code, retry_after_seconds) = match &err {
        ReadFanoutError::InvalidRequest { .. } => (400, None, None),
        ReadFanoutError::MergeLimitExceeded { .. } => {
            (413, Some("read_merge_limit_exceeded"), None)
        }
        ReadFanoutError::ResourceLimitExceeded {
            retryable: true, ..
        } => (429, Some("read_overloaded"), Some("1")),
        ReadFanoutError::ResourceLimitExceeded { .. } => {
            (413, Some("read_resource_limit_exceeded"), None)
        }
        ReadFanoutError::ConsistencyUnmet {
            mode: ClusterReadConsistency::Strict,
            ..
        } => (409, Some("strict_consistency_unmet"), None),
        ReadFanoutError::ConsistencyUnmet { .. } => (503, Some("read_consistency_unmet"), None),
        _ if err.retryable() => (503, None, None),
        _ => (500, None, None),
    };
    let mut response = text_response(status, &err.to_string());
    if let Some(error_code) = error_code {
        response = response.with_header(READ_ERROR_CODE_HEADER, error_code);
    }
    if let Some(retry_after) = retry_after_seconds {
        response = response.with_header("Retry-After", retry_after);
    }
    response
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
    let secs = s
        .parse::<f64>()
        .map_err(|_| format!("invalid timestamp: '{s}'"))?;
    scale_seconds_to_units(secs, precision).ok_or_else(|| format!("invalid timestamp: '{s}'"))
}

fn parse_step(s: &str, precision: TimestampPrecision) -> Result<i64, String> {
    if let Ok(secs) = s.parse::<f64>() {
        return parse_step_seconds(secs, precision, s);
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

    parse_step_seconds(secs, precision, s)
}

fn parse_step_seconds(secs: f64, precision: TimestampPrecision, raw: &str) -> Result<i64, String> {
    if !secs.is_finite() {
        return Err(format!("invalid step: '{raw}'"));
    }
    if secs <= 0.0 {
        return Err("step must be positive".to_string());
    }

    scale_seconds_to_units(secs, precision)
        .map(|step| step.max(1))
        .ok_or_else(|| format!("invalid step: '{raw}'"))
}

fn scale_seconds_to_units(secs: f64, precision: TimestampPrecision) -> Option<i64> {
    if !secs.is_finite() {
        return None;
    }

    let scaled = match precision {
        TimestampPrecision::Seconds => secs,
        TimestampPrecision::Milliseconds => secs * 1_000.0,
        TimestampPrecision::Microseconds => secs * 1_000_000.0,
        TimestampPrecision::Nanoseconds => secs * 1_000_000_000.0,
    };

    (scaled.is_finite() && scaled >= i64::MIN as f64 && scaled <= i64::MAX as f64)
        .then_some(scaled as i64)
}

type PromqlStorageSelection = (Arc<dyn Storage>, Option<Arc<DistributedStorageAdapter>>);

fn storage_for_promql_request(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    cluster_context: Option<&ClusterRequestContext>,
    tenant_id: &str,
    tenant_policy: &tenant::TenantRequestPolicy,
) -> Result<PromqlStorageSelection, HttpResponse> {
    let Some(cluster_context) = cluster_context else {
        return Ok((tenant::scoped_storage(Arc::clone(storage), tenant_id), None));
    };
    if cluster_reads_use_local_storage(Some(cluster_context)) {
        return Ok((tenant::scoped_storage(Arc::clone(storage), tenant_id), None));
    }

    let ring_version = cluster_ring_version(Some(cluster_context));
    let read_fanout = match effective_read_fanout(cluster_context) {
        Ok(fanout) => fanout,
        Err(err) => {
            return Err(promql_error_response(
                "execution",
                &format!("cluster read fanout topology unavailable: {err}"),
            ))
        }
    };
    let read_fanout =
        match apply_request_read_policies(request, cluster_context, read_fanout, tenant_policy) {
            Ok(fanout) => fanout,
            Err(err) => return Err(promql_error_response("bad_data", &err)),
        };

    // PromQL evaluation runs in `spawn_blocking`, so distributed `Storage` reads cross back into
    // the async runtime through this dedicated bridge instead of ad hoc runtime handle usage.
    let distributed_storage = Arc::new(DistributedStorageAdapter::new(
        Arc::clone(storage),
        cluster_context.rpc_client.clone(),
        read_fanout,
        ring_version,
        DistributedPromqlReadBridge::from_current_runtime(),
    ));
    let storage: Arc<dyn Storage> = distributed_storage.clone();
    Ok((
        tenant::scoped_storage(storage, tenant_id),
        Some(distributed_storage),
    ))
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
                    if let Some(histogram) = s.histogram.as_deref() {
                        json!({
                            "metric": metric,
                            "histogram": [
                                timestamp_to_f64(s.timestamp, precision),
                                promql_histogram_json(histogram),
                            ],
                        })
                    } else {
                        json!({
                            "metric": metric,
                            "value": [timestamp_to_f64(s.timestamp, precision), format_value(s.value)],
                        })
                    }
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
                    let histograms: Vec<JsonValue> = s
                        .histograms
                        .iter()
                        .map(|(t, histogram)| {
                            json!([
                                timestamp_to_f64(*t, precision),
                                promql_histogram_json(histogram.as_ref()),
                            ])
                        })
                        .collect();
                    let mut item = serde_json::Map::new();
                    item.insert("metric".to_string(), JsonValue::Object(metric));
                    item.insert("values".to_string(), JsonValue::Array(values));
                    if !histograms.is_empty() {
                        item.insert("histograms".to_string(), JsonValue::Array(histograms));
                    }
                    JsonValue::Object(item)
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

fn promql_histogram_json(histogram: &tsink::NativeHistogram) -> JsonValue {
    let buckets = histogram_buckets(histogram).unwrap_or_default();
    json!({
        "count": format_value(histogram_count_value(histogram)),
        "sum": format_value(histogram.sum),
        "buckets": buckets
            .into_iter()
            .map(|bucket| {
                json!([
                    histogram_bucket_boundary_code(bucket.lower_inclusive, bucket.upper_inclusive),
                    format_value(bucket.lower),
                    format_value(bucket.upper),
                    format_value(bucket.count),
                ])
            })
            .collect::<Vec<_>>(),
    })
}

fn histogram_bucket_boundary_code(lower_inclusive: bool, upper_inclusive: bool) -> i32 {
    match (lower_inclusive, upper_inclusive) {
        (false, false) => 0,
        (false, true) => 1,
        (true, false) => 2,
        (true, true) => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::audit::{ClusterAuditConfig, ClusterAuditLog};
    use crate::cluster::config::{ClusterConfig, ClusterNodeRole};
    use crate::cluster::consensus::{ControlConsensusConfig, ControlConsensusRuntime};
    use crate::cluster::control::{
        ControlState, ControlStateStore, ShardHandoffProgress, ShardOwnershipTransition,
    };
    use crate::cluster::dedupe::{self, DedupeConfig, DedupeWindowStore};
    use crate::cluster::outbox::{HintedHandoffOutbox, OutboxConfig};
    use crate::cluster::rpc::{
        CompatibilityProfile, InternalControlAppendRequest, InternalControlInstallSnapshotRequest,
        InternalErrorResponse, InternalListMetricsRequest, InternalListMetricsResponse,
        InternalSelectBatchRequest, InternalSelectBatchResponse, InternalSelectRequest,
        InternalSelectResponse, InternalSelectSeriesRequest, InternalSelectSeriesResponse,
    };
    use crate::cluster::ClusterRuntime;
    use crate::http::{read_http_request, write_http_response, HttpRequest};
    use crate::metadata_store::MetricMetadataStore;
    use crate::otlp::generated::opentelemetry::proto::metrics::v1::{
        ExponentialHistogram as OtlpExponentialHistogram,
        HistogramDataPoint as OtlpHistogramDataPoint, ResourceMetrics as OtlpResourceMetrics,
        ScopeMetrics as OtlpScopeMetrics, SummaryDataPoint as OtlpSummaryDataPoint,
        ValueAtQuantile as OtlpValueAtQuantile,
    };
    use crate::otlp::{
        any_value as otlp_any_value, exemplar as otlp_exemplar, metric as otlp_metric,
        number_data_point as otlp_number_data_point, AggregationTemporality as OtlpTemporality,
        AnyValue as OtlpAnyValue, Exemplar as OtlpExemplar,
        ExportMetricsServiceRequest as OtlpExportMetricsServiceRequest, Gauge as OtlpGauge,
        Histogram as OtlpHistogram, InstrumentationScope as OtlpInstrumentationScope,
        KeyValue as OtlpKeyValue, Metric as OtlpMetric, NumberDataPoint as OtlpNumberDataPoint,
        Resource as OtlpResource, Sum as OtlpSum, Summary as OtlpSummary,
    };
    use crate::prom_remote::{
        BucketSpan, Exemplar, Histogram, HistogramResetHint, LabelMatcher, MatcherType,
        MetricMetadata, MetricType, Query, ReadRequest, ReadResponse, ReadResponseType,
        WriteRequest,
    };
    use crate::rules::{AlertRuleSpec, RuleGroupSpec, RuleSpec};
    use serde::Deserialize;
    use std::cmp::Ordering as CmpOrdering;
    use std::collections::{BTreeMap, HashMap};
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tsink::{
        HistogramBucketSpan, HistogramCount, HistogramResetHint as TsinkHistogramResetHint,
        MetadataShardScope, NativeHistogram, StorageBuilder, StorageRuntimeMode,
        TimestampPrecision, TsinkError, Value,
    };

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build")
    }

    fn make_engine(storage: &Arc<dyn Storage>) -> Engine {
        Engine::with_precision(Arc::clone(storage), TimestampPrecision::Milliseconds)
    }

    fn sample_native_histogram() -> NativeHistogram {
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
            reset_hint: TsinkHistogramResetHint::No,
            custom_values: Vec::new(),
        }
    }

    fn make_persistent_storage(data_path: &Path) -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_data_path(data_path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("persistent storage should build")
    }

    fn make_metadata_store(data_path: Option<&Path>) -> Arc<MetricMetadataStore> {
        Arc::new(MetricMetadataStore::open(data_path).expect("metric metadata store should build"))
    }

    fn make_exemplar_store(data_path: Option<&Path>) -> Arc<ExemplarStore> {
        Arc::new(ExemplarStore::open(data_path).expect("exemplar store should build"))
    }

    fn make_rules_runtime(storage: &Arc<dyn Storage>) -> Arc<RulesRuntime> {
        RulesRuntime::open(
            None,
            Arc::clone(storage),
            TimestampPrecision::Milliseconds,
            None,
            None,
        )
        .expect("rules runtime should build")
    }

    fn local_owned_metadata_scope(
        cluster_context: &ClusterRequestContext,
        ring_version: u64,
    ) -> MetadataShardScope {
        owned_metadata_shard_scope_for_local_node(
            ring_version,
            Some(cluster_context),
            cluster_context.runtime.ring.shard_count(),
        )
        .expect("owned shard scope should resolve")
    }

    fn shard_scope_for_series(
        cluster_context: &ClusterRequestContext,
        metric: &str,
        labels: &[Label],
    ) -> MetadataShardScope {
        let shard_count = cluster_context.runtime.ring.shard_count();
        let shard =
            (stable_series_identity_hash(metric, labels) % u64::from(shard_count.max(1))) as u32;
        MetadataShardScope::new(shard_count, vec![shard])
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

    fn otlp_string_attr(key: &str, value: &str) -> OtlpKeyValue {
        OtlpKeyValue {
            key: key.to_string(),
            value: Some(OtlpAnyValue {
                value: Some(otlp_any_value::Value::StringValue(value.to_string())),
            }),
        }
    }

    fn otlp_int_attr(key: &str, value: i64) -> OtlpKeyValue {
        OtlpKeyValue {
            key: key.to_string(),
            value: Some(OtlpAnyValue {
                value: Some(otlp_any_value::Value::IntValue(value)),
            }),
        }
    }

    fn start_time() -> Instant {
        Instant::now()
    }

    fn response_header<'a>(response: &'a HttpResponse, name: &str) -> Option<&'a str> {
        response
            .headers
            .iter()
            .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn internal_api() -> InternalApiConfig {
        InternalApiConfig::new(
            "cluster-test-token".to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            false,
            Vec::new(),
        )
    }

    fn internal_headers(
        token: Option<&str>,
        version: Option<&str>,
        extra: &[(&str, &str)],
    ) -> HashMap<String, String> {
        internal_headers_with_compatibility(token, version, &CompatibilityProfile::default(), extra)
    }

    fn internal_headers_with_compatibility(
        token: Option<&str>,
        version: Option<&str>,
        compatibility: &CompatibilityProfile,
        extra: &[(&str, &str)],
    ) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        if let Some(token) = token {
            headers.insert(INTERNAL_RPC_AUTH_HEADER.to_string(), token.to_string());
        }
        if let Some(version) = version {
            headers.insert(INTERNAL_RPC_VERSION_HEADER.to_string(), version.to_string());
            headers.insert(
                INTERNAL_RPC_CAPABILITIES_HEADER.to_string(),
                compatibility.capabilities.join(","),
            );
        }
        for (name, value) in extra {
            headers.insert((*name).to_string(), (*value).to_string());
        }
        headers
    }

    async fn dispatch_internal_request(
        storage: &Arc<dyn Storage>,
        engine: &Engine,
        internal_api: &InternalApiConfig,
        cluster_context: Option<&ClusterRequestContext>,
        method: &str,
        path: &str,
        body: Vec<u8>,
    ) -> HttpResponse {
        let request = HttpRequest {
            method: method.to_string(),
            path: path.to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body,
        };
        handle_request_with_admin_and_cluster(
            storage,
            engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(internal_api),
            cluster_context,
        )
        .await
    }

    async fn handle_request_with_metadata_store(
        storage: &Arc<dyn Storage>,
        metadata_store: &Arc<MetricMetadataStore>,
        engine: &Engine,
        request: HttpRequest,
        server_start: Instant,
        timestamp_precision: TimestampPrecision,
    ) -> HttpResponse {
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        handle_request_with_metadata_and_exemplar_store(
            storage,
            metadata_store,
            &exemplar_store,
            engine,
            request,
            server_start,
            timestamp_precision,
        )
        .await
    }

    async fn handle_request_with_metadata_and_exemplar_store(
        storage: &Arc<dyn Storage>,
        metadata_store: &Arc<MetricMetadataStore>,
        exemplar_store: &Arc<ExemplarStore>,
        engine: &Engine,
        request: HttpRequest,
        server_start: Instant,
        timestamp_precision: TimestampPrecision,
    ) -> HttpResponse {
        handle_request_with_admin_and_cluster_and_tenant_and_metadata(
            storage,
            metadata_store,
            exemplar_store,
            None,
            engine,
            request,
            server_start,
            timestamp_precision,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
    }

    fn attach_cluster_audit_log(
        temp_dir: &TempDir,
        file_name: &str,
        context: &mut ClusterRequestContext,
    ) {
        let audit_log = Arc::new(
            ClusterAuditLog::open(
                temp_dir.path().join(file_name),
                ClusterAuditConfig {
                    retention_secs: 24 * 60 * 60,
                    max_log_bytes: 8 * 1024 * 1024,
                    max_query_limit: 2048,
                },
            )
            .expect("cluster audit log should open"),
        );
        context.audit_log = Some(audit_log);
    }

    fn with_test_cluster_auth_token(mut config: ClusterConfig) -> ClusterConfig {
        if config.cluster_internal_auth_token().is_none() {
            config.internal_auth_token = Some("cluster-test-token".to_string());
        }
        config
    }

    fn cluster_context_with_dedupe(temp_dir: &TempDir) -> Arc<ClusterRequestContext> {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: Vec::new(),
            ..ClusterConfig::default()
        };
        let runtime = ClusterRuntime::bootstrap(&with_test_cluster_auth_token(cfg))
            .expect("cluster runtime should build")
            .expect("cluster runtime should be enabled");
        let mut context =
            ClusterRequestContext::from_runtime(runtime).expect("cluster context should build");
        let store = Arc::new(
            DedupeWindowStore::open(
                temp_dir.path().join("dedupe-markers.log"),
                DedupeConfig {
                    window_secs: 300,
                    max_entries: 1000,
                    max_log_bytes: 4 * 1024 * 1024,
                    cleanup_interval_secs: 1,
                },
            )
            .expect("dedupe store should open"),
        );
        context.dedupe_store = Some(store);
        attach_cluster_audit_log(temp_dir, "cluster-audit-dedupe.log", &mut context);
        Arc::new(context)
    }

    fn cluster_context_with_control_state<F>(
        temp_dir: &TempDir,
        mutate: F,
    ) -> Arc<ClusterRequestContext>
    where
        F: FnOnce(&mut ControlState),
    {
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            ..ClusterConfig::default()
        };
        cluster_context_with_control_state_for_config(temp_dir, config, mutate)
    }

    fn cluster_context_with_control_state_for_config<F>(
        temp_dir: &TempDir,
        config: ClusterConfig,
        mutate: F,
    ) -> Arc<ClusterRequestContext>
    where
        F: FnOnce(&mut ControlState),
    {
        let config = with_test_cluster_auth_token(config);
        let runtime = ClusterRuntime::bootstrap(&config)
            .expect("cluster runtime should build")
            .expect("cluster runtime should be enabled");
        let mut context =
            ClusterRequestContext::from_runtime(runtime).expect("cluster context should build");
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state.json"))
                .expect("control state store should open"),
        );
        let mut bootstrap_state =
            ControlState::from_runtime(&context.runtime.membership, &context.runtime.ring);
        mutate(&mut bootstrap_state);
        bootstrap_state
            .validate()
            .expect("mutated control state should validate");
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap control state should persist");
        let consensus = Arc::new(
            ControlConsensusRuntime::open(
                context.runtime.membership.clone(),
                Arc::clone(&state_store),
                bootstrap_state,
                temp_dir.path().join("control-log.json"),
                ControlConsensusConfig::default(),
            )
            .expect("control consensus should open"),
        );
        context.control_state_store = Some(state_store);
        context.control_consensus = Some(consensus);
        attach_cluster_audit_log(temp_dir, "cluster-audit-control.log", &mut context);
        Arc::new(context)
    }

    fn cluster_context_with_control_state_and_outbox_for_config<F>(
        temp_dir: &TempDir,
        config: ClusterConfig,
        mutate: F,
    ) -> Arc<ClusterRequestContext>
    where
        F: FnOnce(&mut ControlState),
    {
        let config = with_test_cluster_auth_token(config);
        let runtime = ClusterRuntime::bootstrap(&config)
            .expect("cluster runtime should build")
            .expect("cluster runtime should be enabled");
        let mut context =
            ClusterRequestContext::from_runtime(runtime).expect("cluster context should build");
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state-with-outbox.json"))
                .expect("control state store should open"),
        );
        let mut bootstrap_state =
            ControlState::from_runtime(&context.runtime.membership, &context.runtime.ring);
        mutate(&mut bootstrap_state);
        bootstrap_state
            .validate()
            .expect("mutated control state should validate");
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap control state should persist");
        let consensus = Arc::new(
            ControlConsensusRuntime::open(
                context.runtime.membership.clone(),
                Arc::clone(&state_store),
                bootstrap_state,
                temp_dir.path().join("control-log-with-outbox.json"),
                ControlConsensusConfig::default(),
            )
            .expect("control consensus should open"),
        );
        context.control_state_store = Some(state_store);
        context.control_consensus = Some(consensus);

        let outbox = Arc::new(
            HintedHandoffOutbox::open(
                temp_dir.path().join("test-handoff-mirror.outbox.log"),
                OutboxConfig::default(),
            )
            .expect("outbox should open"),
        );
        context.write_router = context
            .write_router
            .clone()
            .with_outbox(Arc::clone(&outbox));
        context.outbox = Some(outbox);
        attach_cluster_audit_log(temp_dir, "cluster-audit-outbox.log", &mut context);
        Arc::new(context)
    }

    fn cluster_context_with_control(temp_dir: &TempDir) -> Arc<ClusterRequestContext> {
        cluster_context_with_control_state(temp_dir, |_| {})
    }

    fn cluster_context_with_single_node_control_and_digest_runtime(
        temp_dir: &TempDir,
    ) -> Arc<ClusterRequestContext> {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: Vec::new(),
            ..ClusterConfig::default()
        };
        let runtime = ClusterRuntime::bootstrap(&with_test_cluster_auth_token(cfg))
            .expect("cluster runtime should build")
            .expect("cluster runtime should be enabled");
        let mut context =
            ClusterRequestContext::from_runtime(runtime).expect("cluster context should build");
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state-repair.json"))
                .expect("control state store should open"),
        );
        let bootstrap_state =
            ControlState::from_runtime(&context.runtime.membership, &context.runtime.ring);
        bootstrap_state
            .validate()
            .expect("mutated control state should validate");
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap control state should persist");
        let consensus = Arc::new(
            ControlConsensusRuntime::open(
                context.runtime.membership.clone(),
                Arc::clone(&state_store),
                bootstrap_state,
                temp_dir.path().join("control-log-repair.json"),
                ControlConsensusConfig::default(),
            )
            .expect("control consensus should open"),
        );
        context.control_state_store = Some(state_store);
        context.control_consensus = Some(Arc::clone(&consensus));
        let digest_runtime = Arc::new(crate::cluster::repair::DigestExchangeRuntime::new(
            context.runtime.membership.local_node_id.clone(),
            context.rpc_client.clone(),
            consensus,
            crate::cluster::repair::DigestExchangeConfig::default(),
        ));
        context.digest_runtime = Some(digest_runtime);
        attach_cluster_audit_log(temp_dir, "cluster-audit-digest.log", &mut context);
        Arc::new(context)
    }

    fn cluster_context_with_single_node_control_state<F>(
        temp_dir: &TempDir,
        mutate: F,
    ) -> Arc<ClusterRequestContext>
    where
        F: FnOnce(&mut ControlState),
    {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: Vec::new(),
            ..ClusterConfig::default()
        };
        let runtime = ClusterRuntime::bootstrap(&with_test_cluster_auth_token(cfg))
            .expect("cluster runtime should build")
            .expect("cluster runtime should be enabled");
        let mut context =
            ClusterRequestContext::from_runtime(runtime).expect("cluster context should build");
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state-single-node.json"))
                .expect("control state store should open"),
        );
        let mut bootstrap_state =
            ControlState::from_runtime(&context.runtime.membership, &context.runtime.ring);
        mutate(&mut bootstrap_state);
        bootstrap_state
            .validate()
            .expect("mutated control state should validate");
        state_store
            .persist(&bootstrap_state)
            .expect("bootstrap control state should persist");
        let consensus = Arc::new(
            ControlConsensusRuntime::open(
                context.runtime.membership.clone(),
                Arc::clone(&state_store),
                bootstrap_state,
                temp_dir.path().join("control-log-single-node.json"),
                ControlConsensusConfig::default(),
            )
            .expect("control consensus should open"),
        );
        context.control_state_store = Some(state_store);
        context.control_consensus = Some(consensus);
        attach_cluster_audit_log(temp_dir, "cluster-audit-single-node.log", &mut context);
        Arc::new(context)
    }

    fn cluster_context_with_single_node_control(temp_dir: &TempDir) -> Arc<ClusterRequestContext> {
        cluster_context_with_single_node_control_state(temp_dir, |_| {})
    }

    #[tokio::test]
    async fn admin_control_plane_routes_roundtrip_and_status_exposes_snapshot() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let storage = make_storage();
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let engine = make_engine(&storage);
        let managed_control_plane =
            ManagedControlPlane::open(Some(temp_dir.path())).expect("control-plane should open");

        let provision_response = handle_request_with_admin_and_cluster_and_tenant_and_metadata_and_security_and_managed_control_plane(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/admin/control-plane/deployments/provision".to_string(),
                headers: HashMap::from([(
                    "content-type".to_string(),
                    "application/json".to_string(),
                )]),
                body: serde_json::to_vec(&json!({
                    "deploymentId": "prod-us-east",
                    "displayName": "Production US East",
                    "region": "us-east-1",
                    "plan": "ha",
                    "lifecycle": "ready"
                }))
                .expect("request should encode"),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&managed_control_plane),
        )
        .await;
        assert_eq!(provision_response.status, 200);

        let tenant_response = handle_request_with_admin_and_cluster_and_tenant_and_metadata_and_security_and_managed_control_plane(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/admin/control-plane/tenants/apply".to_string(),
                headers: HashMap::from([(
                    "content-type".to_string(),
                    "application/json".to_string(),
                )]),
                body: serde_json::to_vec(&json!({
                    "tenantId": "acme",
                    "deploymentId": "prod-us-east",
                    "displayName": "Acme",
                    "lifecycle": "active",
                    "retentionDays": 30
                }))
                .expect("request should encode"),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&managed_control_plane),
        )
        .await;
        assert_eq!(tenant_response.status, 200);

        let state_response = handle_request_with_admin_and_cluster_and_tenant_and_metadata_and_security_and_managed_control_plane(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/admin/control-plane/state".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&managed_control_plane),
        )
        .await;
        assert_eq!(state_response.status, 200);
        let state_body: JsonValue =
            serde_json::from_slice(&state_response.body).expect("state response should be JSON");
        assert_eq!(state_body["data"]["status"]["deploymentsTotal"], 1);
        assert_eq!(state_body["data"]["status"]["tenantsTotal"], 1);

        let status_response = handle_request_with_admin_and_cluster_and_tenant_and_metadata_and_security_and_managed_control_plane(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/status/tsdb".to_string(),
                headers: HashMap::from([(
                    tenant::TENANT_HEADER.to_string(),
                    "acme".to_string(),
                )]),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&managed_control_plane),
        )
        .await;
        assert_eq!(status_response.status, 200);
        let status_body: JsonValue =
            serde_json::from_slice(&status_response.body).expect("status response should be JSON");
        assert_eq!(
            status_body["data"]["managedControlPlane"]["status"]["deploymentsTotal"],
            1
        );
        assert_eq!(
            status_body["data"]["managedControlPlane"]["currentTenant"]["id"],
            "acme"
        );
    }

    fn query_only_cluster_context_with_local_object_store_reads(
        temp_dir: &TempDir,
    ) -> Arc<ClusterRequestContext> {
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("query-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            node_role: ClusterNodeRole::Query,
            ..ClusterConfig::default()
        };
        let config = with_test_cluster_auth_token(config);
        let mut runtime = ClusterRuntime::bootstrap(&config)
            .expect("cluster runtime should build")
            .expect("cluster runtime should be enabled");
        runtime.local_reads_serve_global_queries = true;
        let mut context =
            ClusterRequestContext::from_runtime(runtime).expect("cluster context should build");
        let state_store = Arc::new(
            ControlStateStore::open(temp_dir.path().join("control-state-query-only.json"))
                .expect("control state store should open"),
        );
        let bootstrap_state =
            ControlState::from_runtime(&context.runtime.membership, &context.runtime.ring);
        bootstrap_state
            .validate()
            .expect("query-only control state should validate");
        state_store
            .persist(&bootstrap_state)
            .expect("query-only control state should persist");
        let consensus = Arc::new(
            ControlConsensusRuntime::open(
                context.runtime.membership.clone(),
                Arc::clone(&state_store),
                bootstrap_state,
                temp_dir.path().join("control-log-query-only.json"),
                ControlConsensusConfig::default(),
            )
            .expect("control consensus should open"),
        );
        context.control_state_store = Some(state_store);
        context.control_consensus = Some(consensus);
        Arc::new(context)
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct DistributedQueryCorpusFixture {
        local_rows: Vec<DistributedQueryFixtureSeries>,
        remote_rows: Vec<DistributedQueryFixtureSeries>,
        cases: Vec<DistributedQueryFixtureCase>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct DistributedQueryFixtureSeries {
        metric: String,
        labels: BTreeMap<String, String>,
        points: Vec<DistributedQueryFixturePoint>,
    }

    #[derive(Debug, Deserialize)]
    struct DistributedQueryFixturePoint {
        timestamp: i64,
        value: f64,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct DistributedQueryFixtureCase {
        name: String,
        method: String,
        path: String,
        expected_data: JsonValue,
    }

    fn load_distributed_query_corpus_fixture() -> DistributedQueryCorpusFixture {
        serde_json::from_str(include_str!(
            "../tests/fixtures/distributed_query/distributed-query-corpus.json"
        ))
        .expect("distributed query corpus fixture should parse")
    }

    fn insert_distributed_query_fixture_rows(
        storage: &Arc<dyn Storage>,
        series: &[DistributedQueryFixtureSeries],
    ) {
        let mut rows = Vec::new();
        for item in series {
            let labels = item
                .labels
                .iter()
                .map(|(name, value)| Label::new(name.clone(), value.clone()))
                .collect::<Vec<_>>();
            for point in &item.points {
                rows.push(Row::with_labels(
                    item.metric.clone(),
                    labels.clone(),
                    DataPoint::new(point.timestamp, point.value),
                ));
            }
        }
        storage
            .insert_rows(&rows)
            .expect("fixture rows should insert");
    }

    fn mock_internal_peer_response(
        storage: &Arc<dyn Storage>,
        metadata_store: &Arc<MetricMetadataStore>,
        exemplar_store: &Arc<ExemplarStore>,
        request: &HttpRequest,
    ) -> HttpResponse {
        match request.path_without_query() {
            "/internal/v1/ingest_write" => {
                let payload: InternalIngestWriteRequest =
                    match serde_json::from_slice(&request.body) {
                        Ok(payload) => payload,
                        Err(err) => {
                            return text_response(
                                400,
                                &format!("invalid ingest_write request: {err}"),
                            )
                        }
                    };
                let rows = payload
                    .rows
                    .into_iter()
                    .map(InternalRow::into_row)
                    .collect::<Vec<_>>();
                if let Err(err) = storage.insert_rows(&rows) {
                    return text_response(500, &format!("mock ingest_write rows failed: {err}"));
                }
                let accepted_metadata_updates = if payload.metadata_updates.is_empty() {
                    0usize
                } else {
                    let tenant_id = payload
                        .tenant_id
                        .as_deref()
                        .map(str::trim)
                        .filter(|tenant_id| !tenant_id.is_empty());
                    let Some(tenant_id) = tenant_id else {
                        return text_response(
                            400,
                            "mock ingest_write metadata payload is missing tenant_id",
                        );
                    };
                    let metadata_updates = match payload
                        .metadata_updates
                        .into_iter()
                        .map(internal_metadata_update_to_normalized)
                        .collect::<Result<Vec<_>, _>>()
                    {
                        Ok(updates) => updates,
                        Err(err) => return text_response(400, &err),
                    };
                    match metadata_store.apply_updates(tenant_id, &metadata_updates) {
                        Ok(applied) => applied,
                        Err(err) => {
                            return text_response(
                                500,
                                &format!("mock ingest_write metadata failed: {err}"),
                            );
                        }
                    }
                };
                let exemplar_outcome = match exemplar_store.apply_writes(
                    &payload
                        .exemplars
                        .into_iter()
                        .map(internal_write_exemplar_to_store_write)
                        .collect::<Vec<_>>(),
                ) {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        return text_response(
                            500,
                            &format!("mock ingest_write exemplars failed: {err}"),
                        )
                    }
                };
                json_response(
                    200,
                    &InternalIngestWriteResponse {
                        inserted_rows: rows.len(),
                        accepted_metadata_updates,
                        accepted_exemplars: exemplar_outcome.accepted,
                        dropped_exemplars: exemplar_outcome.dropped,
                    },
                )
            }
            "/internal/v1/ingest_rows" => {
                let payload: InternalIngestRowsRequest = match serde_json::from_slice(&request.body)
                {
                    Ok(payload) => payload,
                    Err(err) => {
                        return text_response(400, &format!("invalid ingest request: {err}"))
                    }
                };
                let inserted_rows = payload.rows.len();
                let rows = payload
                    .rows
                    .into_iter()
                    .map(InternalRow::into_row)
                    .collect::<Vec<_>>();
                if let Err(err) = storage.insert_rows(&rows) {
                    return text_response(500, &format!("mock ingest failed: {err}"));
                }
                json_response(200, &InternalIngestRowsResponse { inserted_rows })
            }
            "/internal/v1/select" => {
                let payload: InternalSelectRequest = match serde_json::from_slice(&request.body) {
                    Ok(payload) => payload,
                    Err(err) => {
                        return text_response(400, &format!("invalid select request: {err}"))
                    }
                };
                let points = match storage.select(
                    &payload.metric,
                    &payload.labels,
                    payload.start,
                    payload.end,
                ) {
                    Ok(points) => points,
                    Err(TsinkError::NoDataPoints { .. }) => Vec::new(),
                    Err(err) => {
                        return text_response(500, &format!("mock select failed: {err}"));
                    }
                };
                json_response(200, &InternalSelectResponse { points })
            }
            "/internal/v1/select_batch" => {
                let payload: InternalSelectBatchRequest =
                    match serde_json::from_slice(&request.body) {
                        Ok(payload) => payload,
                        Err(err) => {
                            return text_response(
                                400,
                                &format!("invalid select_batch request: {err}"),
                            )
                        }
                    };
                let series =
                    match storage.select_many(&payload.selectors, payload.start, payload.end) {
                        Ok(series) => series,
                        Err(err) => {
                            return text_response(500, &format!("mock select_batch failed: {err}"));
                        }
                    };
                json_response(200, &InternalSelectBatchResponse { series })
            }
            "/internal/v1/select_series" => {
                let payload: InternalSelectSeriesRequest =
                    match serde_json::from_slice(&request.body) {
                        Ok(payload) => payload,
                        Err(err) => {
                            return text_response(
                                400,
                                &format!("invalid select_series request: {err}"),
                            );
                        }
                    };
                let series_result = match payload.shard_scope.as_ref() {
                    Some(scope) => storage.select_series_in_shards(&payload.selection, scope),
                    None => storage.select_series(&payload.selection),
                };
                let series = match series_result {
                    Ok(series) => series,
                    Err(err) => {
                        return text_response(500, &format!("mock select_series failed: {err}"));
                    }
                };
                json_response(200, &InternalSelectSeriesResponse { series })
            }
            "/internal/v1/query_exemplars" => {
                let payload: InternalQueryExemplarsRequest =
                    match serde_json::from_slice(&request.body) {
                        Ok(payload) => payload,
                        Err(err) => {
                            return text_response(
                                400,
                                &format!("invalid query_exemplars request: {err}"),
                            )
                        }
                    };
                let series = match exemplar_store.query(
                    &payload.selectors,
                    payload.start,
                    payload.end,
                    payload.limit,
                ) {
                    Ok(series) => series,
                    Err(err) => {
                        return text_response(500, &format!("mock query_exemplars failed: {err}"))
                    }
                };
                json_response(
                    200,
                    &InternalQueryExemplarsResponse {
                        series: series
                            .into_iter()
                            .map(exemplar_series_to_internal)
                            .collect(),
                    },
                )
            }
            "/internal/v1/list_metrics" => {
                let payload: InternalListMetricsRequest =
                    match serde_json::from_slice(&request.body) {
                        Ok(payload) => payload,
                        Err(err) => {
                            return text_response(
                                400,
                                &format!("invalid list_metrics request: {err}"),
                            );
                        }
                    };
                let series_result = match payload.shard_scope.as_ref() {
                    Some(scope) => storage.list_metrics_in_shards(scope),
                    None => storage.list_metrics(),
                };
                let series = match series_result {
                    Ok(series) => series,
                    Err(err) => {
                        return text_response(500, &format!("mock list_metrics failed: {err}"));
                    }
                };
                json_response(200, &InternalListMetricsResponse { series })
            }
            "/internal/v1/snapshot_data" => {
                let payload: InternalDataSnapshotRequest =
                    match serde_json::from_slice(&request.body) {
                        Ok(payload) => payload,
                        Err(err) => {
                            return text_response(
                                400,
                                &format!("invalid snapshot_data request: {err}"),
                            );
                        }
                    };
                let path = PathBuf::from(payload.path);
                if let Err(err) = storage.snapshot(&path) {
                    return text_response(500, &format!("mock snapshot_data failed: {err}"));
                }
                if let Err(err) = exemplar_store.snapshot_into(&path) {
                    return text_response(
                        500,
                        &format!("mock snapshot_data exemplar snapshot failed: {err}"),
                    );
                }
                let size_bytes = match std::fs::metadata(&path) {
                    Ok(metadata) => metadata.len(),
                    Err(err) => {
                        return text_response(
                            500,
                            &format!("mock snapshot_data stat failed: {err}"),
                        );
                    }
                };
                json_response(
                    200,
                    &InternalDataSnapshotResponse {
                        node_id: "remote-node".to_string(),
                        path: path.display().to_string(),
                        created_unix_ms: unix_timestamp_millis(),
                        duration_ms: 0,
                        size_bytes,
                    },
                )
            }
            "/internal/v1/restore_data" => {
                let payload: InternalDataRestoreRequest =
                    match serde_json::from_slice(&request.body) {
                        Ok(payload) => payload,
                        Err(err) => {
                            return text_response(
                                400,
                                &format!("invalid restore_data request: {err}"),
                            );
                        }
                    };
                let snapshot_path = PathBuf::from(payload.snapshot_path);
                let data_path = PathBuf::from(payload.data_path);
                if let Err(err) = StorageBuilder::restore_from_snapshot(&snapshot_path, &data_path)
                {
                    return text_response(500, &format!("mock restore_data failed: {err}"));
                }
                json_response(
                    200,
                    &InternalDataRestoreResponse {
                        node_id: "remote-node".to_string(),
                        snapshot_path: snapshot_path.display().to_string(),
                        data_path: data_path.display().to_string(),
                        restored_unix_ms: unix_timestamp_millis(),
                        duration_ms: 0,
                    },
                )
            }
            "/internal/v1/digest_window" => {
                let payload: InternalDigestWindowRequest =
                    match serde_json::from_slice(&request.body) {
                        Ok(payload) => payload,
                        Err(err) => {
                            return text_response(400, &format!("invalid digest request: {err}"));
                        }
                    };
                let digest = match compute_shard_window_digest(
                    storage.as_ref(),
                    payload.shard,
                    64,
                    payload.ring_version,
                    payload.window_start,
                    payload.window_end,
                ) {
                    Ok(digest) => digest,
                    Err(err) => return text_response(500, &format!("mock digest failed: {err}")),
                };
                json_response(200, &digest)
            }
            "/internal/v1/repair_backfill" => {
                let payload: InternalRepairBackfillRequest =
                    match serde_json::from_slice(&request.body) {
                        Ok(payload) => payload,
                        Err(err) => {
                            return text_response(
                                400,
                                &format!("invalid repair_backfill request: {err}"),
                            );
                        }
                    };
                let response = match collect_internal_repair_backfill_rows(
                    storage.as_ref(),
                    payload.ring_version,
                    payload.shard,
                    64,
                    payload.window_start,
                    payload.window_end,
                    payload.max_series,
                    payload.max_rows,
                    payload.row_offset,
                ) {
                    Ok(response) => response,
                    Err(err) => {
                        return text_response(500, &format!("mock repair_backfill failed: {err}"));
                    }
                };
                json_response(200, &response)
            }
            _ => text_response(404, "not found"),
        }
    }

    async fn spawn_internal_storage_peer(
        storage: Arc<dyn Storage>,
    ) -> (
        String,
        Arc<AtomicUsize>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        spawn_internal_storage_peer_with_metadata_and_exemplars(
            storage,
            metadata_store,
            exemplar_store,
        )
        .await
    }

    async fn spawn_internal_storage_peer_with_exemplars(
        storage: Arc<dyn Storage>,
        exemplar_store: Arc<ExemplarStore>,
    ) -> (
        String,
        Arc<AtomicUsize>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        spawn_internal_storage_peer_with_metadata_and_exemplars(
            storage,
            metadata_store,
            exemplar_store,
        )
        .await
    }

    async fn spawn_internal_storage_peer_with_metadata_and_exemplars(
        storage: Arc<dyn Storage>,
        metadata_store: Arc<MetricMetadataStore>,
        exemplar_store: Arc<ExemplarStore>,
    ) -> (
        String,
        Arc<AtomicUsize>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock peer listener should bind");
        let endpoint = listener
            .local_addr()
            .expect("mock peer endpoint should resolve")
            .to_string();
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_task = Arc::clone(&request_count);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accept = listener.accept() => {
                        let Ok((mut stream, _)) = accept else {
                            break;
                        };
                        let mut read_buffer = Vec::new();
                        let request = match read_http_request(&mut stream, &mut read_buffer).await {
                            Ok(request) => request,
                            Err(_) => {
                                continue;
                            }
                        };
                        request_count_task.fetch_add(1, AtomicOrdering::Relaxed);
                        let response = mock_internal_peer_response(
                            &storage,
                            &metadata_store,
                            &exemplar_store,
                            &request,
                        );
                        let _ = write_http_response(&mut stream, &response).await;
                    }
                }
            }
        });
        (endpoint, request_count, shutdown_tx, server)
    }

    fn parse_success_response_data(response: &HttpResponse) -> JsonValue {
        assert_eq!(response.status, 200);
        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("response should be JSON");
        assert_eq!(body["status"], "success");
        body["data"].clone()
    }

    fn series_signature(value: &JsonValue) -> String {
        value
            .as_object()
            .map(|object| {
                let mut fields = object
                    .iter()
                    .map(|(key, value)| {
                        let value = value
                            .as_str()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| value.to_string());
                        format!("{key}={value}")
                    })
                    .collect::<Vec<_>>();
                fields.sort();
                fields.join(",")
            })
            .unwrap_or_default()
    }

    fn promql_metric_signature(value: &JsonValue) -> String {
        value
            .get("metric")
            .map(series_signature)
            .unwrap_or_default()
    }

    fn normalize_case_data(path: &str, data: &JsonValue) -> JsonValue {
        if path.starts_with("/api/v1/labels") {
            let mut values = data
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|value| value.as_str().map(ToString::to_string))
                .collect::<Vec<_>>();
            values.sort();
            return JsonValue::Array(values.into_iter().map(JsonValue::String).collect());
        }

        if path.starts_with("/api/v1/series") {
            let mut series = data.as_array().cloned().unwrap_or_default();
            series.sort_by_key(series_signature);
            return JsonValue::Array(series);
        }

        if path.starts_with("/api/v1/query") {
            let mut normalized = data.clone();
            if let Some(result) = normalized
                .get_mut("result")
                .and_then(JsonValue::as_array_mut)
            {
                result.sort_by_key(promql_metric_signature);
                for entry in result.iter_mut() {
                    if let Some(values) = entry.get_mut("values").and_then(JsonValue::as_array_mut)
                    {
                        values.sort_by(|left, right| {
                            let left_ts =
                                left.get(0).and_then(JsonValue::as_f64).unwrap_or_default();
                            let right_ts =
                                right.get(0).and_then(JsonValue::as_f64).unwrap_or_default();
                            left_ts.partial_cmp(&right_ts).unwrap_or(CmpOrdering::Equal)
                        });
                    }
                }
            }
            return normalized;
        }

        data.clone()
    }

    fn find_series_owned_by(
        cluster_context: &ClusterRequestContext,
        target_owner: &str,
    ) -> (String, Vec<Label>) {
        for idx in 0..10_000u32 {
            let metric = format!("ownership_metric_{idx}");
            let labels = vec![Label::new("candidate", idx.to_string())];
            let owner = cluster_context
                .write_router
                .owner_for_series(&metric, &labels)
                .expect("owner lookup should succeed");
            if owner == target_owner {
                return (metric, labels);
            }
        }
        panic!("failed to find series mapped to owner '{target_owner}'");
    }

    fn find_series_owned_by_local(cluster_context: &ClusterRequestContext) -> (String, Vec<Label>) {
        find_series_owned_by(
            cluster_context,
            cluster_context.runtime.membership.local_node_id.as_str(),
        )
    }

    fn find_distinct_series_owned_by_local(
        cluster_context: &ClusterRequestContext,
        metric: &str,
        labels: &[Label],
    ) -> (String, Vec<Label>) {
        let target_owner = cluster_context.runtime.membership.local_node_id.as_str();
        for idx in 0..20_000u32 {
            let candidate_metric = format!("ownership_metric_distinct_{idx}");
            let candidate_labels = vec![Label::new("candidate", format!("distinct-{idx}"))];
            let owner = cluster_context
                .write_router
                .owner_for_series(&candidate_metric, &candidate_labels)
                .expect("owner lookup should succeed");
            if owner == target_owner
                && (candidate_metric != metric || candidate_labels.as_slice() != labels)
            {
                return (candidate_metric, candidate_labels);
            }
        }
        panic!("failed to find a second series mapped to the local owner");
    }

    fn find_series_owned_by_remote(
        cluster_context: &ClusterRequestContext,
    ) -> (String, Vec<Label>) {
        let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
        let remote = cluster_context
            .runtime
            .membership
            .nodes
            .iter()
            .find(|node| node.id != local_node_id)
            .expect("test membership should include a remote node");
        find_series_owned_by(cluster_context, remote.id.as_str())
    }

    #[tokio::test]
    async fn internal_endpoints_require_internal_auth() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/internal/v1/list_metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            None,
        )
        .await;
        assert_eq!(response.status, 401);

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "internal_auth_failed");
        assert_eq!(body["retryable"], false);
    }

    #[tokio::test]
    async fn internal_endpoints_reject_protocol_version_mismatch() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/internal/v1/list_metrics".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some("99"),
                &[("content-type", "application/json")],
            ),
            body: Vec::new(),
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            None,
        )
        .await;
        assert_eq!(response.status, 409);

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "protocol_version_mismatch");
        assert_eq!(
            body["expected_protocol_version"],
            INTERNAL_RPC_PROTOCOL_VERSION
        );
        assert_eq!(body["received_protocol_version"], "99");
    }

    #[tokio::test]
    async fn internal_control_append_requires_control_runtime() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();

        let payload = serde_json::to_vec(&InternalControlAppendRequest {
            term: 1,
            leader_node_id: "node-a".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        })
        .expect("payload should serialize");

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/control/append".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: payload,
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            None,
        )
        .await;
        assert_eq!(response.status, 503);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "control_plane_unavailable");
    }

    #[tokio::test]
    async fn internal_control_append_commits_leader_command() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);

        let append_payload = serde_json::to_vec(&InternalControlAppendRequest {
            term: 2,
            leader_node_id: "node-a".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![crate::cluster::rpc::InternalControlLogEntry {
                index: 1,
                term: 2,
                command: crate::cluster::rpc::InternalControlCommand::SetLeader {
                    leader_node_id: "node-a".to_string(),
                },
                created_unix_ms: 1,
            }],
            leader_commit: 0,
        })
        .expect("payload should serialize");

        let append_request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/control/append".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: append_payload,
        };
        let append_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            append_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(append_response.status, 200);

        let heartbeat_payload = serde_json::to_vec(&InternalControlAppendRequest {
            term: 2,
            leader_node_id: "node-a".to_string(),
            prev_log_index: 1,
            prev_log_term: 2,
            entries: Vec::new(),
            leader_commit: 1,
        })
        .expect("payload should serialize");
        let heartbeat_request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/control/append".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: heartbeat_payload,
        };
        let heartbeat_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            heartbeat_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(heartbeat_response.status, 200);

        let state = cluster_context
            .control_consensus
            .as_ref()
            .expect("control consensus should be present")
            .current_state();
        assert_eq!(state.applied_log_index, 1);
        assert_eq!(state.leader_node_id.as_deref(), Some("node-a"));
    }

    #[tokio::test]
    async fn internal_ingest_rows_rejects_missing_histogram_capability() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let peer_compatibility =
            CompatibilityProfile::default().with_capabilities([CLUSTER_CAPABILITY_RPC_V1]);
        let payload = serde_json::to_vec(&InternalIngestRowsRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            idempotency_key: Some("tsink:test:histogram-capability".to_string()),
            required_capabilities: payload_required_capabilities(PrometheusPayloadKind::Histogram),
            rows: vec![InternalRow {
                metric: "internal_histogram_metric".to_string(),
                labels: vec![Label::new("node", "a")],
                data_point: DataPoint::new(
                    1_700_000_000_000,
                    tsink::NativeHistogram {
                        count: Some(tsink::HistogramCount::Int(5)),
                        sum: 1.5,
                        schema: 0,
                        zero_threshold: 0.0,
                        zero_count: Some(tsink::HistogramCount::Int(1)),
                        negative_spans: Vec::new(),
                        negative_deltas: Vec::new(),
                        negative_counts: Vec::new(),
                        positive_spans: vec![tsink::HistogramBucketSpan {
                            offset: 0,
                            length: 1,
                        }],
                        positive_deltas: vec![5],
                        positive_counts: Vec::new(),
                        reset_hint: tsink::HistogramResetHint::No,
                        custom_values: Vec::new(),
                    },
                ),
            }],
        })
        .expect("payload should serialize");
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers_with_compatibility(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &peer_compatibility,
                &[("content-type", "application/json")],
            ),
            body: payload,
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            None,
        )
        .await;
        assert_eq!(response.status, 409);
        let body: crate::cluster::rpc::InternalErrorResponse =
            serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body.code, "peer_capability_missing");
        assert!(body
            .missing_capabilities
            .contains(&CLUSTER_CAPABILITY_HISTOGRAM_INGEST_V1.to_string()));
        assert!(body
            .missing_capabilities
            .contains(&CLUSTER_CAPABILITY_HISTOGRAM_STORAGE_V1.to_string()));
        assert!(storage
            .list_metrics()
            .expect("list_metrics should succeed")
            .is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn internal_control_install_snapshot_rpc_rejects_missing_required_capability() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        let internal_api = internal_api();
        let snapshot_state = cluster_context
            .control_consensus
            .as_ref()
            .expect("control consensus should exist")
            .current_state();
        let peer_compatibility = CompatibilityProfile::default().with_capabilities([
            crate::cluster::rpc::CLUSTER_CAPABILITY_RPC_V1,
            crate::cluster::rpc::CLUSTER_CAPABILITY_CONTROL_REPLICATION_V1,
        ]);

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/internal/v1/control/install_snapshot".to_string(),
                headers: internal_headers_with_compatibility(
                    Some(&internal_api.auth_token),
                    Some(INTERNAL_RPC_PROTOCOL_VERSION),
                    &peer_compatibility,
                    &[("content-type", "application/json")],
                ),
                body: serde_json::to_vec(&InternalControlInstallSnapshotRequest {
                    term: 2,
                    leader_node_id: "node-b".to_string(),
                    snapshot_last_index: 1,
                    snapshot_last_term: 2,
                    state: serde_json::to_value(&snapshot_state)
                        .expect("snapshot state should encode"),
                })
                .expect("request should encode"),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 409);
        let body: InternalErrorResponse =
            serde_json::from_slice(&response.body).expect("response should decode");
        assert_eq!(body.code, "peer_capability_missing");
        assert_eq!(
            body.missing_capabilities,
            vec![CLUSTER_CAPABILITY_CONTROL_SNAPSHOT_RPC_V1.to_string()]
        );
    }

    #[tokio::test]
    async fn internal_control_auto_join_accepts_join_request() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_single_node_control(&temp_dir);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/control/auto_join".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalControlAutoJoinRequest {
                node_id: "node-b".to_string(),
                endpoint: "127.0.0.1:9302".to_string(),
            })
            .expect("payload should serialize"),
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 200);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["result"], "accepted");
        assert_eq!(body["nodeStatus"], "joining");
    }

    #[tokio::test]
    async fn internal_ingest_and_select_round_trip() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();

        let ingest_payload = serde_json::to_vec(&InternalIngestRowsRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            idempotency_key: Some("tsink:test:internal-roundtrip".to_string()),
            required_capabilities: Vec::new(),
            rows: vec![InternalRow {
                metric: "internal_metric".to_string(),
                labels: vec![Label::new("node", "a")],
                data_point: DataPoint::new(1_700_000_000_000, 12.5),
            }],
        })
        .expect("payload should serialize");

        let ingest_request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: ingest_payload,
        };

        let ingest_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            ingest_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            None,
        )
        .await;
        assert_eq!(ingest_response.status, 200);
        let ingest_body: InternalIngestRowsResponse =
            serde_json::from_slice(&ingest_response.body).expect("response JSON should decode");
        assert_eq!(ingest_body.inserted_rows, 1);

        let select_payload = serde_json::to_vec(&InternalSelectRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            metric: "internal_metric".to_string(),
            labels: vec![Label::new("node", "a")],
            start: 1_700_000_000_000,
            end: 1_700_000_000_100,
        })
        .expect("payload should serialize");

        let select_request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/select".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: select_payload,
        };

        let select_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            select_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            None,
        )
        .await;
        assert_eq!(select_response.status, 200);
        let select_body: InternalSelectResponse =
            serde_json::from_slice(&select_response.body).expect("response JSON should decode");
        assert_eq!(select_body.points.len(), 1);
        assert_eq!(select_body.points[0].value.as_f64(), Some(12.5));
    }

    #[tokio::test]
    async fn internal_ingest_rejects_stale_ring_version() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);

        let payload = InternalIngestRowsRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION + 1,
            idempotency_key: Some("tsink:test:stale-ring".to_string()),
            required_capabilities: Vec::new(),
            rows: vec![InternalRow {
                metric: "internal_metric".to_string(),
                labels: vec![Label::new("node", "a")],
                data_point: DataPoint::new(1_700_000_000_000, 1.0),
            }],
        };
        let encoded = serde_json::to_vec(&payload).expect("payload should serialize");
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: encoded,
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 409);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "stale_ring_version");
    }

    #[tokio::test]
    async fn internal_ingest_accepts_stale_cutover_ring_version_and_mirrors_rows() {
        let storage = make_storage();
        let remote_storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let (remote_endpoint, remote_request_count, shutdown_tx, remote_server) =
            spawn_internal_storage_peer(Arc::clone(&remote_storage)).await;

        let temp_dir = TempDir::new().expect("tempdir should build");
        let mut selected_series = None::<(String, Vec<Label>)>;
        let mut stale_ring_version = None::<u64>;
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![format!("node-b@{remote_endpoint}")],
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |state| {
                let local_node_id = "node-a".to_string();
                let remote_node_id = state
                    .nodes
                    .iter()
                    .find(|node| node.id != local_node_id)
                    .map(|node| node.id.clone())
                    .expect("test state should include remote node");
                let previous_ring_version = state.ring_version;
                let activation_ring_version = previous_ring_version.saturating_add(1);

                let (metric, labels, shard) = (0..200_000u32)
                    .find_map(|idx| {
                        let metric = format!("handoff_cutover_metric_{idx}");
                        let labels = vec![Label::new("candidate", idx.to_string())];
                        let series_hash = stable_series_identity_hash(metric.as_str(), &labels);
                        let shard = state.shard_for_series_id(series_hash);
                        state
                            .owners_for_shard_at_ring_version(shard, previous_ring_version)
                            .iter()
                            .any(|owner| owner == &local_node_id)
                            .then_some((metric, labels, shard))
                    })
                    .expect("should find series owned by local node before cutover");

                let owners = state
                    .ring
                    .assignments
                    .get_mut(shard as usize)
                    .expect("shard should exist");
                if let Some(owner_idx) = owners.iter().position(|owner| owner == &local_node_id) {
                    owners[owner_idx] = remote_node_id.clone();
                } else if !owners.iter().any(|owner| owner == &remote_node_id) {
                    owners.push(remote_node_id.clone());
                }
                let mut seen = std::collections::BTreeSet::new();
                owners.retain(|owner| seen.insert(owner.clone()));

                state.transitions = vec![ShardOwnershipTransition {
                    shard,
                    from_node_id: local_node_id,
                    to_node_id: remote_node_id,
                    activation_ring_version,
                    handoff: ShardHandoffProgress {
                        phase: ShardHandoffPhase::Cutover,
                        started_unix_ms: 1,
                        updated_unix_ms: 1,
                        ..ShardHandoffProgress::default()
                    },
                }];
                state.ring_version = activation_ring_version;

                selected_series = Some((metric, labels));
                stale_ring_version = Some(previous_ring_version);
            });

        let (metric, labels) = selected_series.expect("series should be selected");
        let payload = InternalIngestRowsRequest {
            ring_version: stale_ring_version.expect("stale ring version should be captured"),
            idempotency_key: Some("tsink:test:stale-cutover-mirror".to_string()),
            required_capabilities: Vec::new(),
            rows: vec![InternalRow {
                metric: metric.clone(),
                labels: labels.clone(),
                data_point: DataPoint::new(1_700_000_000_123, 13.0),
            }],
        };
        let encoded = serde_json::to_vec(&payload).expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/ingest_rows",
            encoded,
        )
        .await;
        assert_eq!(response.status, 200);
        let body: InternalIngestRowsResponse =
            serde_json::from_slice(&response.body).expect("response should decode");
        assert_eq!(body.inserted_rows, 1);

        let local_points = storage
            .select(&metric, &labels, 1_700_000_000_123, 1_700_000_000_124)
            .expect("local mirrored point should be readable");
        assert_eq!(local_points.len(), 1);
        assert_eq!(local_points[0].value.as_f64(), Some(13.0));

        let remote_points = remote_storage
            .select(&metric, &labels, 1_700_000_000_123, 1_700_000_000_124)
            .expect("remote mirrored point should be readable");
        assert_eq!(remote_points.len(), 1);
        assert_eq!(remote_points[0].value.as_f64(), Some(13.0));
        assert!(remote_request_count.load(AtomicOrdering::Relaxed) > 0);

        shutdown_tx
            .send(())
            .expect("mock remote server shutdown should signal");
        tokio::time::timeout(Duration::from_secs(3), remote_server)
            .await
            .expect("mock remote server should shut down in time")
            .expect("mock remote server task should not panic");
    }

    #[tokio::test]
    async fn internal_ingest_stale_cutover_mirror_failure_enqueues_outbox() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");

        let mut selected_series = None::<(String, Vec<Label>)>;
        let mut stale_ring_version = None::<u64>;
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:65534".to_string()],
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_and_outbox_for_config(&temp_dir, config, |state| {
                let local_node_id = "node-a".to_string();
                let remote_node_id = state
                    .nodes
                    .iter()
                    .find(|node| node.id != local_node_id)
                    .map(|node| node.id.clone())
                    .expect("test state should include remote node");
                let previous_ring_version = state.ring_version;
                let activation_ring_version = previous_ring_version.saturating_add(1);

                let (metric, labels, shard) = (0..200_000u32)
                    .find_map(|idx| {
                        let metric = format!("handoff_cutover_outbox_metric_{idx}");
                        let labels = vec![Label::new("candidate", idx.to_string())];
                        let series_hash = stable_series_identity_hash(metric.as_str(), &labels);
                        let shard = state.shard_for_series_id(series_hash);
                        state
                            .owners_for_shard_at_ring_version(shard, previous_ring_version)
                            .iter()
                            .any(|owner| owner == &local_node_id)
                            .then_some((metric, labels, shard))
                    })
                    .expect("should find series owned by local node before cutover");

                let owners = state
                    .ring
                    .assignments
                    .get_mut(shard as usize)
                    .expect("shard should exist");
                if let Some(owner_idx) = owners.iter().position(|owner| owner == &local_node_id) {
                    owners[owner_idx] = remote_node_id.clone();
                } else if !owners.iter().any(|owner| owner == &remote_node_id) {
                    owners.push(remote_node_id.clone());
                }
                let mut seen = std::collections::BTreeSet::new();
                owners.retain(|owner| seen.insert(owner.clone()));

                state.transitions = vec![ShardOwnershipTransition {
                    shard,
                    from_node_id: local_node_id,
                    to_node_id: remote_node_id,
                    activation_ring_version,
                    handoff: ShardHandoffProgress {
                        phase: ShardHandoffPhase::Cutover,
                        started_unix_ms: 1,
                        updated_unix_ms: 1,
                        ..ShardHandoffProgress::default()
                    },
                }];
                state.ring_version = activation_ring_version;

                selected_series = Some((metric, labels));
                stale_ring_version = Some(previous_ring_version);
            });

        let (metric, labels) = selected_series.expect("series should be selected");
        let payload = InternalIngestRowsRequest {
            ring_version: stale_ring_version.expect("stale ring version should be captured"),
            idempotency_key: Some("tsink:test:stale-cutover-outbox".to_string()),
            required_capabilities: Vec::new(),
            rows: vec![InternalRow {
                metric: metric.clone(),
                labels: labels.clone(),
                data_point: DataPoint::new(1_700_000_000_124, 14.0),
            }],
        };
        let encoded = serde_json::to_vec(&payload).expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/ingest_rows",
            encoded,
        )
        .await;
        assert_eq!(response.status, 200);

        let local_points = storage
            .select(&metric, &labels, 1_700_000_000_124, 1_700_000_000_125)
            .expect("local point should be readable");
        assert_eq!(local_points.len(), 1);
        assert_eq!(local_points[0].value.as_f64(), Some(14.0));

        let outbox = cluster_context
            .outbox
            .as_ref()
            .expect("outbox should be configured");
        let backlog = outbox.backlog_snapshot();
        assert_eq!(backlog.queued_entries, 1);
    }

    #[tokio::test]
    async fn internal_ingest_mirror_failure_without_outbox_dedupes_retry() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");

        let mut selected_series = None::<(String, Vec<Label>)>;
        let mut stale_ring_version = None::<u64>;
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:65534".to_string()],
            ..ClusterConfig::default()
        };
        let base_cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |state| {
                let local_node_id = "node-a".to_string();
                let remote_node_id = state
                    .nodes
                    .iter()
                    .find(|node| node.id != local_node_id)
                    .map(|node| node.id.clone())
                    .expect("test state should include remote node");
                let previous_ring_version = state.ring_version;
                let activation_ring_version = previous_ring_version.saturating_add(1);

                let (metric, labels, shard) = (0..200_000u32)
                    .find_map(|idx| {
                        let metric = format!("handoff_cutover_no_outbox_metric_{idx}");
                        let labels = vec![Label::new("candidate", idx.to_string())];
                        let series_hash = stable_series_identity_hash(metric.as_str(), &labels);
                        let shard = state.shard_for_series_id(series_hash);
                        state
                            .owners_for_shard_at_ring_version(shard, previous_ring_version)
                            .iter()
                            .any(|owner| owner == &local_node_id)
                            .then_some((metric, labels, shard))
                    })
                    .expect("should find series owned by local node before cutover");

                let owners = state
                    .ring
                    .assignments
                    .get_mut(shard as usize)
                    .expect("shard should exist");
                if let Some(owner_idx) = owners.iter().position(|owner| owner == &local_node_id) {
                    owners[owner_idx] = remote_node_id.clone();
                } else if !owners.iter().any(|owner| owner == &remote_node_id) {
                    owners.push(remote_node_id.clone());
                }
                let mut seen = std::collections::BTreeSet::new();
                owners.retain(|owner| seen.insert(owner.clone()));

                state.transitions = vec![ShardOwnershipTransition {
                    shard,
                    from_node_id: local_node_id,
                    to_node_id: remote_node_id,
                    activation_ring_version,
                    handoff: ShardHandoffProgress {
                        phase: ShardHandoffPhase::Cutover,
                        started_unix_ms: 1,
                        updated_unix_ms: 1,
                        ..ShardHandoffProgress::default()
                    },
                }];
                state.ring_version = activation_ring_version;

                selected_series = Some((metric, labels));
                stale_ring_version = Some(previous_ring_version);
            });

        let mut cluster_context = base_cluster_context.as_ref().clone();
        let dedupe_store = Arc::new(
            DedupeWindowStore::open(
                temp_dir.path().join("dedupe-no-outbox.log"),
                DedupeConfig {
                    window_secs: 300,
                    max_entries: 1000,
                    max_log_bytes: 4 * 1024 * 1024,
                    cleanup_interval_secs: 1,
                },
            )
            .expect("dedupe store should open"),
        );
        cluster_context.dedupe_store = Some(dedupe_store);
        let cluster_context = Arc::new(cluster_context);

        let (metric, labels) = selected_series.expect("series should be selected");
        let payload = InternalIngestRowsRequest {
            ring_version: stale_ring_version.expect("stale ring version should be captured"),
            idempotency_key: Some("tsink:test:stale-cutover-no-outbox".to_string()),
            required_capabilities: Vec::new(),
            rows: vec![InternalRow {
                metric: metric.clone(),
                labels: labels.clone(),
                data_point: DataPoint::new(1_700_000_000_125, 15.0),
            }],
        };
        let encoded = serde_json::to_vec(&payload).expect("payload should serialize");

        let first_response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/ingest_rows",
            encoded.clone(),
        )
        .await;
        assert_eq!(first_response.status, 503);
        let first_body: JsonValue =
            serde_json::from_slice(&first_response.body).expect("valid JSON");
        assert_eq!(first_body["code"], "handoff_mirror_failed");

        let points_after_first = storage
            .select(&metric, &labels, 1_700_000_000_125, 1_700_000_000_126)
            .expect("local point should be readable");
        assert_eq!(points_after_first.len(), 1);

        let replay_response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/ingest_rows",
            encoded,
        )
        .await;
        assert_eq!(replay_response.status, 200);
        assert!(replay_response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("x-tsink-idempotency-replayed") && value == "true"
        }));

        let points_after_replay = storage
            .select(&metric, &labels, 1_700_000_000_125, 1_700_000_000_126)
            .expect("local point should be readable");
        assert_eq!(points_after_replay.len(), 1);
        assert_eq!(points_after_replay[0].value.as_f64(), Some(15.0));
    }

    #[tokio::test]
    async fn internal_select_accepts_stale_cutover_ring_version_for_transition_source_owner() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");

        let mut selected_series = None::<(String, Vec<Label>)>;
        let mut stale_ring_version = None::<u64>;
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |state| {
                let local_node_id = "node-a".to_string();
                let remote_node_id = state
                    .nodes
                    .iter()
                    .find(|node| node.id != local_node_id)
                    .map(|node| node.id.clone())
                    .expect("test state should include remote node");
                let previous_ring_version = state.ring_version;
                let activation_ring_version = previous_ring_version.saturating_add(1);

                let (metric, labels, shard) = (0..200_000u32)
                    .find_map(|idx| {
                        let metric = format!("handoff_stale_select_metric_{idx}");
                        let labels = vec![Label::new("candidate", idx.to_string())];
                        let series_hash = stable_series_identity_hash(metric.as_str(), &labels);
                        let shard = state.shard_for_series_id(series_hash);
                        state
                            .owners_for_shard_at_ring_version(shard, previous_ring_version)
                            .iter()
                            .any(|owner| owner == &local_node_id)
                            .then_some((metric, labels, shard))
                    })
                    .expect("should find series owned by local node before cutover");

                let owners = state
                    .ring
                    .assignments
                    .get_mut(shard as usize)
                    .expect("shard should exist");
                if let Some(owner_idx) = owners.iter().position(|owner| owner == &local_node_id) {
                    owners[owner_idx] = remote_node_id.clone();
                } else if !owners.iter().any(|owner| owner == &remote_node_id) {
                    owners.push(remote_node_id.clone());
                }
                let mut seen = std::collections::BTreeSet::new();
                owners.retain(|owner| seen.insert(owner.clone()));

                state.transitions = vec![ShardOwnershipTransition {
                    shard,
                    from_node_id: local_node_id,
                    to_node_id: remote_node_id,
                    activation_ring_version,
                    handoff: ShardHandoffProgress {
                        phase: ShardHandoffPhase::Cutover,
                        started_unix_ms: 1,
                        updated_unix_ms: 1,
                        ..ShardHandoffProgress::default()
                    },
                }];
                state.ring_version = activation_ring_version;

                selected_series = Some((metric, labels));
                stale_ring_version = Some(previous_ring_version);
            });

        let (metric, labels) = selected_series.expect("series should be selected");
        let stale_ring_version = stale_ring_version.expect("stale ring version should be captured");
        storage
            .insert_rows(&[Row::with_labels(
                metric.clone(),
                labels.clone(),
                DataPoint::new(1_700_000_000_130, 3.5),
            )])
            .expect("seed insert should succeed");

        let payload = serde_json::to_vec(&InternalSelectRequest {
            ring_version: stale_ring_version,
            metric: metric.clone(),
            labels: labels.clone(),
            start: 1_700_000_000_130,
            end: 1_700_000_000_131,
        })
        .expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/select",
            payload,
        )
        .await;
        assert_eq!(response.status, 200);

        let body: InternalSelectResponse =
            serde_json::from_slice(&response.body).expect("response should decode");
        assert_eq!(body.points.len(), 1);
        assert_eq!(body.points[0].value.as_f64(), Some(3.5));

        let select_series_payload = serde_json::to_vec(&InternalSelectSeriesRequest {
            ring_version: stale_ring_version,
            shard_scope: Some(local_owned_metadata_scope(
                cluster_context.as_ref(),
                stale_ring_version,
            )),
            selection: SeriesSelection::new(),
        })
        .expect("payload should serialize");
        let select_series_response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/select_series",
            select_series_payload,
        )
        .await;
        assert_eq!(select_series_response.status, 200);
        let select_series_body: InternalSelectSeriesResponse =
            serde_json::from_slice(&select_series_response.body).expect("valid JSON");
        assert!(select_series_body
            .series
            .iter()
            .any(|series| series.name == metric && series.labels == labels));

        let list_metrics_payload = serde_json::to_vec(&InternalListMetricsRequest {
            ring_version: stale_ring_version,
            shard_scope: Some(local_owned_metadata_scope(
                cluster_context.as_ref(),
                stale_ring_version,
            )),
        })
        .expect("payload should serialize");
        let list_metrics_response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/list_metrics",
            list_metrics_payload,
        )
        .await;
        assert_eq!(list_metrics_response.status, 200);
        let list_metrics_body: InternalListMetricsResponse =
            serde_json::from_slice(&list_metrics_response.body).expect("valid JSON");
        assert!(list_metrics_body
            .series
            .iter()
            .any(|series| series.name == metric && series.labels == labels));
    }

    #[tokio::test]
    async fn internal_select_current_cutover_owner_bridges_points_from_previous_owner() {
        let storage = make_storage();
        let remote_storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let (remote_endpoint, remote_request_count, shutdown_tx, remote_server) =
            spawn_internal_storage_peer(Arc::clone(&remote_storage)).await;

        let temp_dir = TempDir::new().expect("tempdir should build");
        let mut selected_series = None::<(String, Vec<Label>)>;
        let mut active_ring_version = None::<u64>;
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![format!("node-b@{remote_endpoint}")],
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |state| {
                let local_node_id = "node-a".to_string();
                let remote_node_id = state
                    .nodes
                    .iter()
                    .find(|node| node.id != local_node_id)
                    .map(|node| node.id.clone())
                    .expect("test state should include remote node");
                let previous_ring_version = state.ring_version;
                let activation_ring_version = previous_ring_version.saturating_add(1);

                let (metric, labels, shard) = (0..200_000u32)
                    .find_map(|idx| {
                        let metric = format!("handoff_select_bridge_metric_{idx}");
                        let labels = vec![Label::new("candidate", idx.to_string())];
                        let series_hash = stable_series_identity_hash(metric.as_str(), &labels);
                        let shard = state.shard_for_series_id(series_hash);
                        state
                            .owners_for_shard_at_ring_version(shard, previous_ring_version)
                            .iter()
                            .any(|owner| owner == &remote_node_id)
                            .then_some((metric, labels, shard))
                    })
                    .expect("should find series owned by remote node before cutover");

                let owners = state
                    .ring
                    .assignments
                    .get_mut(shard as usize)
                    .expect("shard should exist");
                if let Some(owner_idx) = owners.iter().position(|owner| owner == &remote_node_id) {
                    owners[owner_idx] = local_node_id.clone();
                } else if !owners.iter().any(|owner| owner == &local_node_id) {
                    owners.push(local_node_id.clone());
                }
                let mut seen = std::collections::BTreeSet::new();
                owners.retain(|owner| seen.insert(owner.clone()));

                state.transitions = vec![ShardOwnershipTransition {
                    shard,
                    from_node_id: remote_node_id,
                    to_node_id: local_node_id,
                    activation_ring_version,
                    handoff: ShardHandoffProgress {
                        phase: ShardHandoffPhase::Cutover,
                        started_unix_ms: 1,
                        updated_unix_ms: 1,
                        ..ShardHandoffProgress::default()
                    },
                }];
                state.ring_version = activation_ring_version;

                selected_series = Some((metric, labels));
                active_ring_version = Some(activation_ring_version);
            });

        let (metric, labels) = selected_series.expect("series should be selected");
        remote_storage
            .insert_rows(&[Row::with_labels(
                metric.clone(),
                labels.clone(),
                DataPoint::new(1_700_000_000_140, 9.0),
            )])
            .expect("remote seed insert should succeed");
        storage
            .insert_rows(&[Row::with_labels(
                metric.clone(),
                labels.clone(),
                DataPoint::new(1_700_000_000_141, 10.0),
            )])
            .expect("local seed insert should succeed");

        let payload = serde_json::to_vec(&InternalSelectRequest {
            ring_version: active_ring_version.expect("active ring version should be captured"),
            metric: metric.clone(),
            labels: labels.clone(),
            start: 1_700_000_000_140,
            end: 1_700_000_000_142,
        })
        .expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/select",
            payload,
        )
        .await;
        assert_eq!(response.status, 200);

        let body: InternalSelectResponse =
            serde_json::from_slice(&response.body).expect("response should decode");
        assert_eq!(body.points.len(), 2);
        assert_eq!(body.points[0].timestamp, 1_700_000_000_140);
        assert_eq!(body.points[0].value.as_f64(), Some(9.0));
        assert_eq!(body.points[1].timestamp, 1_700_000_000_141);
        assert_eq!(body.points[1].value.as_f64(), Some(10.0));
        assert!(remote_request_count.load(AtomicOrdering::Relaxed) > 0);

        shutdown_tx
            .send(())
            .expect("mock remote server shutdown should signal");
        tokio::time::timeout(Duration::from_secs(3), remote_server)
            .await
            .expect("mock remote server should shut down in time")
            .expect("mock remote server task should not panic");
    }

    #[tokio::test]
    async fn internal_metadata_current_cutover_owner_bridges_transition_shards_only() {
        let storage = make_storage();
        let remote_storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let (remote_endpoint, remote_request_count, shutdown_tx, remote_server) =
            spawn_internal_storage_peer(Arc::clone(&remote_storage)).await;

        let temp_dir = TempDir::new().expect("tempdir should build");
        let mut transition_series = None::<(String, Vec<Label>, u32)>;
        let mut non_transition_series = None::<(String, Vec<Label>)>;
        let mut active_ring_version = None::<u64>;
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![format!("node-b@{remote_endpoint}")],
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |state| {
                let local_node_id = "node-a".to_string();
                let remote_node_id = state
                    .nodes
                    .iter()
                    .find(|node| node.id != local_node_id)
                    .map(|node| node.id.clone())
                    .expect("test state should include remote node");
                let previous_ring_version = state.ring_version;
                let activation_ring_version = previous_ring_version.saturating_add(1);

                let (transition_metric, transition_labels, transition_shard) = (0..250_000u32)
                    .find_map(|idx| {
                        let metric = format!("handoff_metadata_transition_metric_{idx}");
                        let labels = vec![Label::new("candidate", idx.to_string())];
                        let series_hash = stable_series_identity_hash(metric.as_str(), &labels);
                        let shard = state.shard_for_series_id(series_hash);
                        state
                            .owners_for_shard_at_ring_version(shard, previous_ring_version)
                            .iter()
                            .any(|owner| owner == &remote_node_id)
                            .then_some((metric, labels, shard))
                    })
                    .expect("should find transition series owned by remote before cutover");

                let (other_metric, other_labels, _) = (250_001..500_000u32)
                    .find_map(|idx| {
                        let metric = format!("handoff_metadata_non_transition_metric_{idx}");
                        let labels = vec![Label::new("candidate", idx.to_string())];
                        let series_hash = stable_series_identity_hash(metric.as_str(), &labels);
                        let shard = state.shard_for_series_id(series_hash);
                        if shard == transition_shard {
                            return None;
                        }
                        state
                            .owners_for_shard_at_ring_version(shard, previous_ring_version)
                            .iter()
                            .any(|owner| owner == &remote_node_id)
                            .then_some((metric, labels, shard))
                    })
                    .expect("should find non-transition remote-owned series");

                let owners = state
                    .ring
                    .assignments
                    .get_mut(transition_shard as usize)
                    .expect("shard should exist");
                if let Some(owner_idx) = owners.iter().position(|owner| owner == &remote_node_id) {
                    owners[owner_idx] = local_node_id.clone();
                } else if !owners.iter().any(|owner| owner == &local_node_id) {
                    owners.push(local_node_id.clone());
                }
                let mut seen = std::collections::BTreeSet::new();
                owners.retain(|owner| seen.insert(owner.clone()));

                state.transitions = vec![ShardOwnershipTransition {
                    shard: transition_shard,
                    from_node_id: remote_node_id,
                    to_node_id: local_node_id,
                    activation_ring_version,
                    handoff: ShardHandoffProgress {
                        phase: ShardHandoffPhase::Cutover,
                        started_unix_ms: 1,
                        updated_unix_ms: 1,
                        ..ShardHandoffProgress::default()
                    },
                }];
                state.ring_version = activation_ring_version;

                transition_series = Some((transition_metric, transition_labels, transition_shard));
                non_transition_series = Some((other_metric, other_labels));
                active_ring_version = Some(activation_ring_version);
            });

        let (transition_metric, transition_labels, _) =
            transition_series.expect("transition series should be selected");
        let (other_metric, other_labels) =
            non_transition_series.expect("non-transition series should be selected");
        let active_ring_version =
            active_ring_version.expect("active ring version should be captured");
        remote_storage
            .insert_rows(&[
                Row::with_labels(
                    transition_metric.clone(),
                    transition_labels.clone(),
                    DataPoint::new(1_700_000_000_150, 1.0),
                ),
                Row::with_labels(
                    other_metric.clone(),
                    other_labels.clone(),
                    DataPoint::new(1_700_000_000_151, 2.0),
                ),
            ])
            .expect("remote metadata seed inserts should succeed");

        let select_series_payload = serde_json::to_vec(&InternalSelectSeriesRequest {
            ring_version: active_ring_version,
            shard_scope: Some(local_owned_metadata_scope(
                cluster_context.as_ref(),
                active_ring_version,
            )),
            selection: SeriesSelection::new(),
        })
        .expect("payload should serialize");
        let select_series_response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/select_series",
            select_series_payload,
        )
        .await;
        assert_eq!(select_series_response.status, 200);
        let select_series_body: InternalSelectSeriesResponse =
            serde_json::from_slice(&select_series_response.body).expect("valid JSON");
        assert!(select_series_body
            .series
            .iter()
            .any(|series| series.name == transition_metric && series.labels == transition_labels));
        assert!(!select_series_body
            .series
            .iter()
            .any(|series| series.name == other_metric && series.labels == other_labels));

        let list_metrics_payload = serde_json::to_vec(&InternalListMetricsRequest {
            ring_version: active_ring_version,
            shard_scope: Some(local_owned_metadata_scope(
                cluster_context.as_ref(),
                active_ring_version,
            )),
        })
        .expect("payload should serialize");
        let list_metrics_response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/list_metrics",
            list_metrics_payload,
        )
        .await;
        assert_eq!(list_metrics_response.status, 200);
        let list_metrics_body: InternalListMetricsResponse =
            serde_json::from_slice(&list_metrics_response.body).expect("valid JSON");
        assert!(list_metrics_body
            .series
            .iter()
            .any(|series| series.name == transition_metric && series.labels == transition_labels));
        assert!(!list_metrics_body
            .series
            .iter()
            .any(|series| series.name == other_metric && series.labels == other_labels));
        assert!(remote_request_count.load(AtomicOrdering::Relaxed) >= 2);

        shutdown_tx
            .send(())
            .expect("mock remote server shutdown should signal");
        tokio::time::timeout(Duration::from_secs(3), remote_server)
            .await
            .expect("mock remote server should shut down in time")
            .expect("mock remote server task should not panic");
    }

    #[tokio::test]
    async fn internal_select_rejects_stale_ring_version() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);

        let payload = serde_json::to_vec(&InternalSelectRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION + 1,
            metric: "internal_metric".to_string(),
            labels: vec![Label::new("node", "a")],
            start: 1_700_000_000_000,
            end: 1_700_000_000_100,
        })
        .expect("payload should serialize");

        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/select",
            payload,
        )
        .await;
        assert_eq!(response.status, 409);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "stale_ring_version");
    }

    #[tokio::test]
    async fn internal_ingest_rejects_stale_ring_owner() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        let (metric, labels) = find_series_owned_by_remote(cluster_context.as_ref());

        let payload = InternalIngestRowsRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            idempotency_key: Some("tsink:test:stale-owner-write".to_string()),
            required_capabilities: Vec::new(),
            rows: vec![InternalRow {
                metric,
                labels,
                data_point: DataPoint::new(1_700_000_000_000, 9.0),
            }],
        };
        let encoded = serde_json::to_vec(&payload).expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/ingest_rows",
            encoded,
        )
        .await;
        assert_eq!(response.status, 409);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "stale_ring_owner");
    }

    #[tokio::test]
    async fn internal_select_rejects_stale_ring_owner() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        let (metric, labels) = find_series_owned_by_remote(cluster_context.as_ref());

        let payload = serde_json::to_vec(&InternalSelectRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            metric,
            labels,
            start: 1_700_000_000_000,
            end: 1_700_000_000_100,
        })
        .expect("payload should serialize");

        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/select",
            payload,
        )
        .await;
        assert_eq!(response.status, 409);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "stale_ring_owner");
    }

    #[tokio::test]
    async fn internal_select_series_rejects_stale_ring_version() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);

        let payload = serde_json::to_vec(&InternalSelectSeriesRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION + 1,
            shard_scope: None,
            selection: SeriesSelection::new(),
        })
        .expect("payload should serialize");

        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/select_series",
            payload,
        )
        .await;
        assert_eq!(response.status, 409);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "stale_ring_version");
    }

    #[tokio::test]
    async fn internal_list_metrics_rejects_stale_ring_version_for_get_and_post() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        let payload = serde_json::to_vec(&InternalListMetricsRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION + 1,
            shard_scope: None,
        })
        .expect("payload should serialize");

        let get_response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "GET",
            "/internal/v1/list_metrics",
            payload.clone(),
        )
        .await;
        assert_eq!(get_response.status, 409);
        let get_body: JsonValue = serde_json::from_slice(&get_response.body).expect("valid JSON");
        assert_eq!(get_body["code"], "stale_ring_version");

        let post_response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/list_metrics",
            payload,
        )
        .await;
        assert_eq!(post_response.status, 409);
        let post_body: JsonValue = serde_json::from_slice(&post_response.body).expect("valid JSON");
        assert_eq!(post_body["code"], "stale_ring_version");
    }

    #[tokio::test]
    async fn internal_digest_window_returns_digest_for_owned_shard() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        let shard_count = cluster_context.runtime.ring.shard_count();
        let (metric, labels) = find_series_owned_by_local(cluster_context.as_ref());
        let shard =
            (stable_series_identity_hash(metric.as_str(), &labels) % u64::from(shard_count)) as u32;

        storage
            .insert_rows(&[Row::with_labels(
                metric.clone(),
                labels.clone(),
                DataPoint::new(1_700_000_000_450, 4.0),
            )])
            .expect("seed insert should succeed");

        let payload = serde_json::to_vec(&InternalDigestWindowRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            shard,
            window_start: 1_700_000_000_000,
            window_end: 1_700_000_001_000,
        })
        .expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/digest_window",
            payload,
        )
        .await;
        assert_eq!(response.status, 200);
        let body: crate::cluster::rpc::InternalDigestWindowResponse =
            serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body.shard, shard);
        assert_eq!(body.ring_version, DEFAULT_INTERNAL_RING_VERSION);
        assert!(body.series_count >= 1);
        assert!(body.point_count >= 1);
    }

    #[tokio::test]
    async fn internal_digest_window_rejects_non_owner_shard() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        let shard_count = cluster_context.runtime.ring.shard_count();
        let (metric, labels) = find_series_owned_by_remote(cluster_context.as_ref());
        let shard =
            (stable_series_identity_hash(metric.as_str(), &labels) % u64::from(shard_count)) as u32;

        let payload = serde_json::to_vec(&InternalDigestWindowRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            shard,
            window_start: 1_700_000_000_000,
            window_end: 1_700_000_001_000,
        })
        .expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/digest_window",
            payload,
        )
        .await;
        assert_eq!(response.status, 409);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "stale_ring_owner");
    }

    #[tokio::test]
    async fn internal_digest_window_accepts_stale_cutover_ring_version_for_transition_source_owner()
    {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");

        let mut selected_series = None::<(String, Vec<Label>, u32)>;
        let mut stale_ring_version = None::<u64>;
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |state| {
                let local_node_id = "node-a".to_string();
                let remote_node_id = state
                    .nodes
                    .iter()
                    .find(|node| node.id != local_node_id)
                    .map(|node| node.id.clone())
                    .expect("test state should include remote node");
                let previous_ring_version = state.ring_version;
                let activation_ring_version = previous_ring_version.saturating_add(1);

                let (metric, labels, shard) = (0..200_000u32)
                    .find_map(|idx| {
                        let metric = format!("handoff_stale_digest_metric_{idx}");
                        let labels = vec![Label::new("candidate", idx.to_string())];
                        let series_hash = stable_series_identity_hash(metric.as_str(), &labels);
                        let shard = state.shard_for_series_id(series_hash);
                        state
                            .owners_for_shard_at_ring_version(shard, previous_ring_version)
                            .iter()
                            .any(|owner| owner == &local_node_id)
                            .then_some((metric, labels, shard))
                    })
                    .expect("should find series owned by local node before cutover");

                let owners = state
                    .ring
                    .assignments
                    .get_mut(shard as usize)
                    .expect("shard should exist");
                if let Some(owner_idx) = owners.iter().position(|owner| owner == &local_node_id) {
                    owners[owner_idx] = remote_node_id.clone();
                } else if !owners.iter().any(|owner| owner == &remote_node_id) {
                    owners.push(remote_node_id.clone());
                }
                let mut seen = std::collections::BTreeSet::new();
                owners.retain(|owner| seen.insert(owner.clone()));

                state.transitions = vec![ShardOwnershipTransition {
                    shard,
                    from_node_id: local_node_id,
                    to_node_id: remote_node_id,
                    activation_ring_version,
                    handoff: ShardHandoffProgress {
                        phase: ShardHandoffPhase::Cutover,
                        started_unix_ms: 1,
                        updated_unix_ms: 1,
                        ..ShardHandoffProgress::default()
                    },
                }];
                state.ring_version = activation_ring_version;

                selected_series = Some((metric, labels, shard));
                stale_ring_version = Some(previous_ring_version);
            });

        let (metric, labels, shard) = selected_series.expect("series should be selected");
        let stale_ring_version = stale_ring_version.expect("stale ring version should be captured");
        storage
            .insert_rows(&[Row::with_labels(
                metric,
                labels,
                DataPoint::new(1_700_000_000_140, 4.0),
            )])
            .expect("seed insert should succeed");

        let payload = serde_json::to_vec(&InternalDigestWindowRequest {
            ring_version: stale_ring_version,
            shard,
            window_start: 1_700_000_000_000,
            window_end: 1_700_000_001_000,
        })
        .expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/digest_window",
            payload,
        )
        .await;
        assert_eq!(response.status, 200);
        let body: crate::cluster::rpc::InternalDigestWindowResponse =
            serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body.shard, shard);
        assert_eq!(body.ring_version, stale_ring_version);
        assert!(body.point_count >= 1);
    }

    #[tokio::test]
    async fn internal_repair_backfill_returns_rows_for_owned_shard() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        let shard_count = cluster_context.runtime.ring.shard_count();
        let (metric, labels) = find_series_owned_by_local(cluster_context.as_ref());
        let shard =
            (stable_series_identity_hash(metric.as_str(), &labels) % u64::from(shard_count)) as u32;

        storage
            .insert_rows(&[
                Row::with_labels(
                    metric.clone(),
                    labels.clone(),
                    DataPoint::new(1_700_000_000_510, 5.0),
                ),
                Row::with_labels(
                    metric.clone(),
                    labels.clone(),
                    DataPoint::new(1_700_000_000_520, 6.0),
                ),
            ])
            .expect("seed inserts should succeed");

        let payload = serde_json::to_vec(&InternalRepairBackfillRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            shard,
            window_start: 1_700_000_000_500,
            window_end: 1_700_000_000_530,
            max_series: Some(1),
            max_rows: Some(1),
            row_offset: None,
        })
        .expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/repair_backfill",
            payload,
        )
        .await;
        assert_eq!(response.status, 200);
        let body: InternalRepairBackfillResponse =
            serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body.shard, shard);
        assert_eq!(body.ring_version, DEFAULT_INTERNAL_RING_VERSION);
        assert_eq!(body.series_scanned, 1);
        assert_eq!(body.rows_scanned, 1);
        assert_eq!(body.rows.len(), 1);
        assert!(body.truncated);
        let next_row_offset = body
            .next_row_offset
            .expect("truncated response should include next_row_offset");

        let payload = serde_json::to_vec(&InternalRepairBackfillRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            shard,
            window_start: 1_700_000_000_500,
            window_end: 1_700_000_000_530,
            max_series: Some(1),
            max_rows: Some(1),
            row_offset: Some(next_row_offset),
        })
        .expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/repair_backfill",
            payload,
        )
        .await;
        assert_eq!(response.status, 200);
        let next_page: InternalRepairBackfillResponse =
            serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(next_page.shard, shard);
        assert_eq!(next_page.rows.len(), 1);
        assert!(!next_page.truncated);
        assert!(next_page.next_row_offset.is_none());
        assert_ne!(
            next_page.rows[0].data_point.timestamp,
            body.rows[0].data_point.timestamp
        );
    }

    #[tokio::test]
    async fn internal_repair_backfill_rejects_non_owner_shard() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        let shard_count = cluster_context.runtime.ring.shard_count();
        let (metric, labels) = find_series_owned_by_remote(cluster_context.as_ref());
        let shard =
            (stable_series_identity_hash(metric.as_str(), &labels) % u64::from(shard_count)) as u32;

        let payload = serde_json::to_vec(&InternalRepairBackfillRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            shard,
            window_start: 1_700_000_000_000,
            window_end: 1_700_000_001_000,
            max_series: Some(8),
            max_rows: Some(64),
            row_offset: None,
        })
        .expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/repair_backfill",
            payload,
        )
        .await;
        assert_eq!(response.status, 409);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "stale_ring_owner");
    }

    #[tokio::test]
    async fn internal_repair_backfill_accepts_stale_cutover_ring_version_for_transition_source_owner(
    ) {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");

        let mut selected_series = None::<(String, Vec<Label>, u32)>;
        let mut stale_ring_version = None::<u64>;
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |state| {
                let local_node_id = "node-a".to_string();
                let remote_node_id = state
                    .nodes
                    .iter()
                    .find(|node| node.id != local_node_id)
                    .map(|node| node.id.clone())
                    .expect("test state should include remote node");
                let previous_ring_version = state.ring_version;
                let activation_ring_version = previous_ring_version.saturating_add(1);

                let (metric, labels, shard) = (0..200_000u32)
                    .find_map(|idx| {
                        let metric = format!("handoff_stale_backfill_metric_{idx}");
                        let labels = vec![Label::new("candidate", idx.to_string())];
                        let series_hash = stable_series_identity_hash(metric.as_str(), &labels);
                        let shard = state.shard_for_series_id(series_hash);
                        state
                            .owners_for_shard_at_ring_version(shard, previous_ring_version)
                            .iter()
                            .any(|owner| owner == &local_node_id)
                            .then_some((metric, labels, shard))
                    })
                    .expect("should find series owned by local node before cutover");

                let owners = state
                    .ring
                    .assignments
                    .get_mut(shard as usize)
                    .expect("shard should exist");
                if let Some(owner_idx) = owners.iter().position(|owner| owner == &local_node_id) {
                    owners[owner_idx] = remote_node_id.clone();
                } else if !owners.iter().any(|owner| owner == &remote_node_id) {
                    owners.push(remote_node_id.clone());
                }
                let mut seen = std::collections::BTreeSet::new();
                owners.retain(|owner| seen.insert(owner.clone()));

                state.transitions = vec![ShardOwnershipTransition {
                    shard,
                    from_node_id: local_node_id,
                    to_node_id: remote_node_id,
                    activation_ring_version,
                    handoff: ShardHandoffProgress {
                        phase: ShardHandoffPhase::Cutover,
                        started_unix_ms: 1,
                        updated_unix_ms: 1,
                        ..ShardHandoffProgress::default()
                    },
                }];
                state.ring_version = activation_ring_version;

                selected_series = Some((metric, labels, shard));
                stale_ring_version = Some(previous_ring_version);
            });

        let (metric, labels, shard) = selected_series.expect("series should be selected");
        let stale_ring_version = stale_ring_version.expect("stale ring version should be captured");
        storage
            .insert_rows(&[Row::with_labels(
                metric.clone(),
                labels.clone(),
                DataPoint::new(1_700_000_000_150, 5.0),
            )])
            .expect("seed insert should succeed");

        let payload = serde_json::to_vec(&InternalRepairBackfillRequest {
            ring_version: stale_ring_version,
            shard,
            window_start: 1_700_000_000_000,
            window_end: 1_700_000_001_000,
            max_series: Some(8),
            max_rows: Some(64),
            row_offset: None,
        })
        .expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/repair_backfill",
            payload,
        )
        .await;
        assert_eq!(response.status, 200);
        let body: InternalRepairBackfillResponse =
            serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body.shard, shard);
        assert_eq!(body.ring_version, stale_ring_version);
        assert!(body
            .rows
            .iter()
            .any(|row| row.metric == metric && row.labels == labels));
    }

    #[tokio::test]
    async fn internal_data_plane_rejects_zero_ring_version() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);

        let payload = serde_json::to_vec(&InternalListMetricsRequest {
            ring_version: 0,
            shard_scope: None,
        })
        .expect("payload should serialize");

        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/list_metrics",
            payload,
        )
        .await;
        assert_eq!(response.status, 400);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "invalid_ring_version");
    }

    #[tokio::test]
    async fn internal_read_endpoints_accept_matching_ring_version_with_control_runtime() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        let (metric, labels) = find_series_owned_by_local(cluster_context.as_ref());

        storage
            .insert_rows(&[Row::with_labels(
                metric.clone(),
                labels.clone(),
                DataPoint::new(1_700_000_000_300, 3.0),
            )])
            .expect("seed insert should succeed");

        let select_series_payload = serde_json::to_vec(&InternalSelectSeriesRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            shard_scope: Some(local_owned_metadata_scope(
                cluster_context.as_ref(),
                DEFAULT_INTERNAL_RING_VERSION,
            )),
            selection: SeriesSelection::new(),
        })
        .expect("payload should serialize");
        let select_series_response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/select_series",
            select_series_payload,
        )
        .await;
        assert_eq!(select_series_response.status, 200);
        let select_series_body: InternalSelectSeriesResponse =
            serde_json::from_slice(&select_series_response.body).expect("valid JSON");
        assert!(!select_series_body.series.is_empty());

        let list_metrics_payload = serde_json::to_vec(&InternalListMetricsRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            shard_scope: Some(local_owned_metadata_scope(
                cluster_context.as_ref(),
                DEFAULT_INTERNAL_RING_VERSION,
            )),
        })
        .expect("payload should serialize");
        let list_metrics_response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/list_metrics",
            list_metrics_payload,
        )
        .await;
        assert_eq!(list_metrics_response.status, 200);
        let list_metrics_body: InternalListMetricsResponse =
            serde_json::from_slice(&list_metrics_response.body).expect("valid JSON");
        assert!(list_metrics_body
            .series
            .iter()
            .any(|series| series.name == metric && series.labels == labels));
    }

    #[tokio::test]
    async fn internal_select_batch_returns_points_for_multiple_owned_series() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        let (metric_a, labels_a) = find_series_owned_by_local(cluster_context.as_ref());
        let (metric_b, labels_b) =
            find_distinct_series_owned_by_local(cluster_context.as_ref(), &metric_a, &labels_a);

        storage
            .insert_rows(&[
                Row::with_labels(
                    metric_a.clone(),
                    labels_a.clone(),
                    DataPoint::new(1_700_000_000_310, 3.0),
                ),
                Row::with_labels(
                    metric_b.clone(),
                    labels_b.clone(),
                    DataPoint::new(1_700_000_000_311, 4.0),
                ),
            ])
            .expect("seed insert should succeed");

        let selectors = vec![
            MetricSeries {
                name: metric_b.clone(),
                labels: labels_b.clone(),
            },
            MetricSeries {
                name: metric_a.clone(),
                labels: labels_a.clone(),
            },
        ];
        let payload = serde_json::to_vec(&InternalSelectBatchRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            selectors: selectors.clone(),
            start: 1_700_000_000_300,
            end: 1_700_000_000_320,
        })
        .expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/select_batch",
            payload,
        )
        .await;
        assert_eq!(response.status, 200);

        let body: InternalSelectBatchResponse =
            serde_json::from_slice(&response.body).expect("response should decode");
        assert_eq!(body.series.len(), 2);
        assert_eq!(body.series[0].series, selectors[0]);
        assert_eq!(
            body.series[0].points,
            vec![DataPoint::new(1_700_000_000_311, 4.0)]
        );
        assert_eq!(body.series[1].series, selectors[1]);
        assert_eq!(
            body.series[1].points,
            vec![DataPoint::new(1_700_000_000_310, 3.0)]
        );
    }

    #[tokio::test]
    async fn internal_metadata_read_filters_non_owned_series() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        let (local_metric, local_labels) = find_series_owned_by_local(cluster_context.as_ref());
        let (remote_metric, remote_labels) = find_series_owned_by_remote(cluster_context.as_ref());

        storage
            .insert_rows(&[
                Row::with_labels(
                    local_metric.clone(),
                    local_labels.clone(),
                    DataPoint::new(1_700_000_000_301, 1.0),
                ),
                Row::with_labels(
                    remote_metric.clone(),
                    remote_labels.clone(),
                    DataPoint::new(1_700_000_000_302, 2.0),
                ),
            ])
            .expect("seed insert should succeed");

        let select_series_payload = serde_json::to_vec(&InternalSelectSeriesRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            shard_scope: Some(local_owned_metadata_scope(
                cluster_context.as_ref(),
                DEFAULT_INTERNAL_RING_VERSION,
            )),
            selection: SeriesSelection::new(),
        })
        .expect("payload should serialize");
        let select_series_response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/select_series",
            select_series_payload,
        )
        .await;
        assert_eq!(select_series_response.status, 200);
        let select_series_body: InternalSelectSeriesResponse =
            serde_json::from_slice(&select_series_response.body).expect("valid JSON");
        assert!(select_series_body
            .series
            .iter()
            .any(|series| series.name == local_metric && series.labels == local_labels));
        assert!(!select_series_body
            .series
            .iter()
            .any(|series| series.name == remote_metric && series.labels == remote_labels));

        let list_metrics_payload = serde_json::to_vec(&InternalListMetricsRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            shard_scope: Some(local_owned_metadata_scope(
                cluster_context.as_ref(),
                DEFAULT_INTERNAL_RING_VERSION,
            )),
        })
        .expect("payload should serialize");
        let list_metrics_response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/list_metrics",
            list_metrics_payload,
        )
        .await;
        assert_eq!(list_metrics_response.status, 200);
        let list_metrics_body: InternalListMetricsResponse =
            serde_json::from_slice(&list_metrics_response.body).expect("valid JSON");
        assert!(list_metrics_body
            .series
            .iter()
            .any(|series| series.name == local_metric && series.labels == local_labels));
        assert!(!list_metrics_body
            .series
            .iter()
            .any(|series| series.name == remote_metric && series.labels == remote_labels));
    }

    #[tokio::test]
    async fn internal_metadata_rejects_explicit_non_owned_shard_scope() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        let (metric, labels) = find_series_owned_by_remote(cluster_context.as_ref());
        let shard_scope = shard_scope_for_series(cluster_context.as_ref(), &metric, &labels);

        let payload = serde_json::to_vec(&InternalSelectSeriesRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            shard_scope: Some(shard_scope.clone()),
            selection: SeriesSelection::new(),
        })
        .expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/select_series",
            payload,
        )
        .await;
        assert_eq!(response.status, 400);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "invalid_shard_scope");

        let payload = serde_json::to_vec(&InternalListMetricsRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            shard_scope: Some(shard_scope),
        })
        .expect("payload should serialize");
        let response = dispatch_internal_request(
            &storage,
            &engine,
            &internal_api,
            Some(cluster_context.as_ref()),
            "POST",
            "/internal/v1/list_metrics",
            payload,
        )
        .await;
        assert_eq!(response.status, 400);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "invalid_shard_scope");
    }

    #[test]
    fn effective_write_router_uses_transition_owner_before_activation() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let mut transition_series = None::<(String, Vec<Label>)>;
        let cluster_context = cluster_context_with_control_state(&temp_dir, |state| {
            let local_node_id = "node-a".to_string();
            let remote_node_id = state
                .nodes
                .iter()
                .find(|node| node.id != local_node_id)
                .map(|node| node.id.clone())
                .expect("test state should include remote node");

            let (metric, labels, shard) = (0..200_000u32)
                .find_map(|idx| {
                    let metric = format!("transition_metric_{idx}");
                    let labels = vec![Label::new("candidate", idx.to_string())];
                    let series_hash = stable_series_identity_hash(metric.as_str(), &labels);
                    let shard = state.shard_for_series_id(series_hash);
                    let owners = state.owners_for_shard_at_ring_version(shard, state.ring_version);
                    owners
                        .iter()
                        .any(|owner| owner == &local_node_id)
                        .then_some((metric, labels, shard))
                })
                .expect("should find series mapped to local owner");
            transition_series = Some((metric, labels));

            let owners = state
                .ring
                .assignments
                .get_mut(shard as usize)
                .expect("shard should exist");
            if let Some(owner_idx) = owners.iter().position(|owner| owner == &local_node_id) {
                owners[owner_idx] = remote_node_id.clone();
            } else if !owners.iter().any(|owner| owner == &remote_node_id) {
                owners.push(remote_node_id.clone());
            }
            let mut seen = std::collections::BTreeSet::new();
            owners.retain(|owner| seen.insert(owner.clone()));

            state.transitions = vec![ShardOwnershipTransition {
                shard,
                from_node_id: local_node_id,
                to_node_id: remote_node_id,
                activation_ring_version: state.ring_version.saturating_add(1),
                handoff: ShardHandoffProgress::default(),
            }];
        });

        let (metric, labels) = transition_series.expect("transition series should be selected");
        let router = effective_write_router(cluster_context.as_ref())
            .expect("effective router should build");
        let owner = router
            .owner_for_series(&metric, &labels)
            .expect("owner lookup should succeed");
        assert_eq!(owner, cluster_context.runtime.membership.local_node_id);
    }

    #[tokio::test]
    async fn internal_ingest_deduplicates_replayed_idempotency_key() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_dedupe(&temp_dir);
        let duplicates_before = dedupe::dedupe_metrics_snapshot().duplicates_total;

        let payload = InternalIngestRowsRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            idempotency_key: Some("tsink:test:replay:1".to_string()),
            required_capabilities: Vec::new(),
            rows: vec![InternalRow {
                metric: "internal_metric".to_string(),
                labels: vec![Label::new("node", "a")],
                data_point: DataPoint::new(1_700_000_000_100, 42.0),
            }],
        };
        let encoded = serde_json::to_vec(&payload).expect("payload should encode");

        let first_request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: encoded.clone(),
        };
        let first_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            first_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(first_response.status, 200);

        let replay_request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: encoded,
        };
        let replay_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            replay_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(replay_response.status, 200);
        assert!(replay_response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("x-tsink-idempotency-replayed") && value == "true"
        }));

        let duplicates_after = dedupe::dedupe_metrics_snapshot().duplicates_total;
        assert!(duplicates_after > duplicates_before);
    }

    #[tokio::test]
    async fn internal_ingest_deduplicates_replayed_idempotency_key_with_edge_sync_accept_context() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let dedupe_config = DedupeConfig {
            window_secs: 3_600,
            ..DedupeConfig::default()
        };
        let dedupe_store = Arc::new(
            DedupeWindowStore::open(temp_dir.path().join("edge-sync-dedupe.log"), dedupe_config)
                .expect("dedupe store should open"),
        );
        let edge_sync_context = edge_sync::EdgeSyncRuntimeContext {
            source: None,
            accept_dedupe_store: Some(Arc::clone(&dedupe_store)),
            accept_dedupe_config: Some(dedupe_config),
        };
        let internal_api = edge_sync::edge_sync_accept_internal_api("edge-sync-token");
        let duplicates_before = dedupe::dedupe_metrics_snapshot().duplicates_total;

        let payload = InternalIngestRowsRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            idempotency_key: Some("tsink:test:edge-sync:replay:1".to_string()),
            required_capabilities: Vec::new(),
            rows: vec![InternalRow {
                metric: "internal_metric".to_string(),
                labels: vec![Label::new("node", "edge-a")],
                data_point: DataPoint::new(1_700_000_000_100, 42.0),
            }],
        };
        let encoded = serde_json::to_vec(&payload).expect("payload should encode");

        for body in [encoded.clone(), encoded] {
            let request = HttpRequest {
                method: "POST".to_string(),
                path: "/internal/v1/ingest_rows".to_string(),
                headers: internal_headers(
                    Some(&internal_api.auth_token),
                    Some(INTERNAL_RPC_PROTOCOL_VERSION),
                    &[("content-type", "application/json")],
                ),
                body,
            };
            let response = handle_request_with_admin_and_cluster_and_metadata(
                &storage,
                &metadata_store,
                &exemplar_store,
                None,
                &engine,
                request,
                start_time(),
                TimestampPrecision::Milliseconds,
                false,
                None,
                Some(&internal_api),
                None,
                Some(&edge_sync_context),
            )
            .await;
            assert_eq!(response.status, 200);
        }

        let duplicates_after = dedupe::dedupe_metrics_snapshot().duplicates_total;
        assert!(duplicates_after > duplicates_before);
    }

    #[tokio::test]
    async fn internal_ingest_requires_idempotency_key_when_dedupe_enabled() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_dedupe(&temp_dir);

        let payload = InternalIngestRowsRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            idempotency_key: None,
            required_capabilities: Vec::new(),
            rows: vec![InternalRow {
                metric: "internal_metric".to_string(),
                labels: vec![Label::new("node", "a")],
                data_point: DataPoint::new(1_700_000_000_200, 7.0),
            }],
        };
        let encoded = serde_json::to_vec(&payload).expect("payload should encode");

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: encoded,
        };
        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 400);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "missing_idempotency_key");
    }

    #[tokio::test]
    async fn internal_ingest_rejects_oversized_payload() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();

        let payload = InternalIngestRowsRequest {
            ring_version: DEFAULT_INTERNAL_RING_VERSION,
            idempotency_key: Some("tsink:test:oversized".to_string()),
            required_capabilities: Vec::new(),
            rows: (0..=MAX_INTERNAL_INGEST_ROWS)
                .map(|idx| InternalRow {
                    metric: "bulk_metric".to_string(),
                    labels: vec![Label::new("batch", "oversized")],
                    data_point: DataPoint::new(1_700_000_000_000 + idx as i64, idx as f64),
                })
                .collect(),
        };
        let encoded = serde_json::to_vec(&payload).expect("payload should serialize");

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: encoded,
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            None,
        )
        .await;
        assert_eq!(response.status, 422);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "payload_too_large");
        assert_eq!(body["retryable"], false);
    }

    #[tokio::test]
    async fn internal_ingest_rejects_malformed_json_payload() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let internal_api = internal_api();

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: b"{not-json".to_vec(),
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&internal_api),
            None,
        )
        .await;
        assert_eq!(response.status, 400);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["code"], "invalid_request");
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
                ..Default::default()
            }],
            metadata: Vec::new(),
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
                &[
                    Label::new("host", "server-a"),
                    Label::new(tenant::TENANT_LABEL, tenant::DEFAULT_TENANT_ID),
                ],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("point must be persisted");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value.as_f64(), Some(11.5));
    }

    #[tokio::test]
    async fn otlp_metrics_endpoint_ingests_mainstream_payloads() {
        let storage = make_storage();
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let engine = make_engine(&storage);

        let export = OtlpExportMetricsServiceRequest {
            resource_metrics: vec![OtlpResourceMetrics {
                resource: Some(OtlpResource {
                    attributes: vec![otlp_string_attr("service.name", "checkout")],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![OtlpScopeMetrics {
                    scope: Some(OtlpInstrumentationScope {
                        name: "otel.scope".to_string(),
                        version: "1.0.0".to_string(),
                        attributes: vec![otlp_string_attr("scope.attr", "yes")],
                        dropped_attributes_count: 0,
                    }),
                    metrics: vec![
                        OtlpMetric {
                            name: "system.cpu.time".to_string(),
                            description: "CPU time".to_string(),
                            unit: "s".to_string(),
                            data: Some(otlp_metric::Data::Gauge(OtlpGauge {
                                data_points: vec![OtlpNumberDataPoint {
                                    attributes: vec![otlp_string_attr("cpu", "0")],
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 1_700_000_000_123_000_000,
                                    value: Some(otlp_number_data_point::Value::AsDouble(12.5)),
                                    exemplars: vec![],
                                    flags: 0,
                                }],
                            })),
                        },
                        OtlpMetric {
                            name: "http.server.active_requests".to_string(),
                            description: "Active requests".to_string(),
                            unit: "{request}".to_string(),
                            data: Some(otlp_metric::Data::Sum(OtlpSum {
                                data_points: vec![OtlpNumberDataPoint {
                                    attributes: vec![otlp_string_attr("method", "GET")],
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 1_700_000_000_124_000_000,
                                    value: Some(otlp_number_data_point::Value::AsInt(5)),
                                    exemplars: vec![OtlpExemplar {
                                        filtered_attributes: vec![otlp_string_attr(
                                            "trace.role",
                                            "frontend",
                                        )],
                                        time_unix_nano: 1_700_000_000_124_000_000,
                                        value: Some(otlp_exemplar::Value::AsInt(5)),
                                        span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
                                        trace_id: vec![
                                            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                                        ],
                                    }],
                                    flags: 0,
                                }],
                                aggregation_temporality: OtlpTemporality::Cumulative as i32,
                                is_monotonic: false,
                            })),
                        },
                        OtlpMetric {
                            name: "http.server.duration".to_string(),
                            description: "Duration".to_string(),
                            unit: "ms".to_string(),
                            data: Some(otlp_metric::Data::Histogram(OtlpHistogram {
                                data_points: vec![OtlpHistogramDataPoint {
                                    attributes: vec![otlp_string_attr("route", "/")],
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 1_700_000_000_125_000_000,
                                    count: 6,
                                    sum: Some(48.0),
                                    bucket_counts: vec![2, 3, 1],
                                    explicit_bounds: vec![10.0, 20.0],
                                    exemplars: vec![],
                                    flags: 0,
                                    min: Some(2.0),
                                    max: Some(22.0),
                                }],
                                aggregation_temporality: OtlpTemporality::Cumulative as i32,
                            })),
                        },
                        OtlpMetric {
                            name: "rpc.latency".to_string(),
                            description: "Latency summary".to_string(),
                            unit: "ms".to_string(),
                            data: Some(otlp_metric::Data::Summary(OtlpSummary {
                                data_points: vec![OtlpSummaryDataPoint {
                                    attributes: vec![otlp_int_attr("status", 200)],
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 1_700_000_000_126_000_000,
                                    count: 3,
                                    sum: 44.0,
                                    quantile_values: vec![
                                        OtlpValueAtQuantile {
                                            quantile: 0.5,
                                            value: 12.0,
                                        },
                                        OtlpValueAtQuantile {
                                            quantile: 0.9,
                                            value: 20.0,
                                        },
                                    ],
                                    flags: 0,
                                }],
                            })),
                        },
                    ],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut encoded = Vec::new();
        export
            .encode(&mut encoded)
            .expect("protobuf encode should work");

        let response = handle_request_with_metadata_and_exemplar_store(
            &storage,
            &metadata_store,
            &exemplar_store,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/v1/metrics".to_string(),
                headers: HashMap::from([(
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                )]),
                body: encoded,
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);
        assert_eq!(
            response_header(&response, "content-type"),
            Some("application/x-protobuf")
        );
        assert_eq!(
            response_header(&response, "X-Tsink-OTLP-Data-Points-Accepted"),
            Some("4")
        );
        assert_eq!(
            response_header(&response, "X-Tsink-Exemplars-Accepted"),
            Some("1")
        );

        let series = storage
            .list_metrics()
            .expect("series listing should succeed");
        let gauge_series = series
            .iter()
            .find(|series| series.name == "system_x2e_cpu_x2e_time")
            .expect("gauge series should exist");
        assert!(gauge_series
            .labels
            .iter()
            .any(|label| label.name == "resource_service_x2e_name" && label.value == "checkout"));
        let gauge_points = storage
            .select(
                &gauge_series.name,
                &gauge_series.labels,
                1_700_000_000_123,
                1_700_000_000_124,
            )
            .expect("gauge points should be queryable");
        assert_eq!(gauge_points[0].value.as_f64(), Some(12.5));

        let histogram_bucket_series = series
            .iter()
            .find(|series| {
                series.name == "http_x2e_server_x2e_duration_bucket"
                    && series
                        .labels
                        .iter()
                        .any(|label| label.name == "le" && label.value == "+Inf")
            })
            .expect("histogram +Inf bucket series should exist");
        let histogram_bucket_points = storage
            .select(
                &histogram_bucket_series.name,
                &histogram_bucket_series.labels,
                1_700_000_000_125,
                1_700_000_000_126,
            )
            .expect("histogram bucket points should be queryable");
        assert_eq!(histogram_bucket_points[0].value.as_f64(), Some(6.0));

        let summary_sum_series = series
            .iter()
            .find(|series| series.name == "rpc_x2e_latency_sum")
            .expect("summary sum series should exist");
        let summary_sum_points = storage
            .select(
                &summary_sum_series.name,
                &summary_sum_series.labels,
                1_700_000_000_126,
                1_700_000_000_127,
            )
            .expect("summary sum points should be queryable");
        assert_eq!(summary_sum_points[0].value.as_f64(), Some(44.0));

        let metadata = metadata_store
            .query(
                tenant::DEFAULT_TENANT_ID,
                Some("http_x2e_server_x2e_duration"),
                10,
            )
            .expect("metadata query should succeed");
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].unit, "ms");
        assert_eq!(
            metric_type_to_api_string(metadata[0].metric_type),
            "histogram"
        );

        let exemplar_metrics = exemplar_store
            .metrics_snapshot()
            .expect("exemplar metrics should be available");
        assert_eq!(exemplar_metrics.accepted_total, 1);
        assert_eq!(exemplar_metrics.stored_exemplars, 1);
    }

    #[tokio::test]
    async fn influx_line_protocol_endpoint_ingests_numeric_fields() {
        let storage = make_storage();
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let engine = make_engine(&storage);

        let response = handle_request_with_metadata_and_exemplar_store(
            &storage,
            &metadata_store,
            &exemplar_store,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/write?db=telegraf&precision=ns".to_string(),
                headers: HashMap::new(),
                body: b"weather,host=web-01 value=42i,temp=22.5 1700000000000000000".to_vec(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 204);
        assert_eq!(
            response_header(&response, "X-Tsink-Influx-Lines-Accepted"),
            Some("1")
        );
        assert_eq!(
            response_header(&response, "X-Tsink-Influx-Samples-Accepted"),
            Some("2")
        );

        let base_labels = vec![
            Label::new("host", "web-01"),
            Label::new("influx_db", "telegraf"),
            Label::new(tenant::TENANT_LABEL, tenant::DEFAULT_TENANT_ID),
        ];
        let weather_points = storage
            .select(
                "weather",
                &base_labels,
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("base field should be stored");
        let weather_temp_points = storage
            .select(
                "weather_temp",
                &base_labels,
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("named field should be stored");
        assert_eq!(weather_points.len(), 1);
        assert_eq!(weather_points[0].value.as_f64(), Some(42.0));
        assert_eq!(weather_temp_points.len(), 1);
        assert_eq!(weather_temp_points[0].value.as_f64(), Some(22.5));

        let metadata = metadata_store
            .query(tenant::DEFAULT_TENANT_ID, Some("weather_temp"), 10)
            .expect("metadata query should succeed");
        assert_eq!(metadata.len(), 1);
        assert_eq!(metric_type_to_api_string(metadata[0].metric_type), "gauge");
    }

    #[tokio::test]
    async fn influx_line_protocol_endpoint_rejects_boolean_fields() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let response = handle_request(
            &storage,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/write".to_string(),
                headers: HashMap::new(),
                body: b"weather ok=true".to_vec(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 400);
        assert!(std::str::from_utf8(&response.body)
            .expect("response body should be utf8")
            .contains("boolean fields are not supported"));
    }

    #[tokio::test]
    async fn otlp_metrics_endpoint_rejects_exponential_histograms() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let export = OtlpExportMetricsServiceRequest {
            resource_metrics: vec![OtlpResourceMetrics {
                resource: None,
                scope_metrics: vec![OtlpScopeMetrics {
                    scope: None,
                    metrics: vec![OtlpMetric {
                        name: "system.memory.usage".to_string(),
                        description: String::new(),
                        unit: "By".to_string(),
                        data: Some(otlp_metric::Data::ExponentialHistogram(
                            OtlpExponentialHistogram {
                                data_points: vec![],
                                aggregation_temporality: OtlpTemporality::Cumulative as i32,
                            },
                        )),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut encoded = Vec::new();
        export
            .encode(&mut encoded)
            .expect("protobuf encode should work");

        let response = handle_request(
            &storage,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/v1/metrics".to_string(),
                headers: HashMap::from([(
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                )]),
                body: encoded,
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 400);
        let body = std::str::from_utf8(&response.body).expect("error body should be utf8");
        assert!(body.contains("exponential histograms"));
    }

    #[tokio::test]
    async fn remote_write_returns_retryable_error_when_public_write_slots_are_saturated() {
        let storage = make_storage();
        let write_admission = WriteAdmissionController::new(admission::WriteAdmissionGuardrails {
            max_inflight_requests: 1,
            max_inflight_rows: 8,
            acquire_timeout: Duration::from_millis(1),
        })
        .expect("write admission should build");
        let _held = write_admission
            .acquire_request_slot()
            .await
            .expect("first request slot should be admitted");

        let write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![PromLabel {
                    name: "__name__".to_string(),
                    value: "cpu_usage".to_string(),
                }],
                samples: vec![PromSample {
                    value: 11.5,
                    timestamp: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            metadata: Vec::new(),
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");

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
            body: snappy_encode(&encoded),
        };

        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let response = handle_remote_write_with_admission(
            &storage,
            &metadata_store,
            &exemplar_store,
            &request,
            None,
            None,
            None,
            None,
            &write_admission,
        )
        .await;
        assert_eq!(response.status, 429);
        assert_eq!(
            response_header(&response, WRITE_ERROR_CODE_HEADER),
            Some("write_overloaded")
        );
        assert_eq!(response_header(&response, "Retry-After"), Some("1"));
    }

    #[tokio::test]
    async fn remote_write_rejects_out_of_retention_samples_with_422() {
        let now = unix_timestamp_millis() as i64;
        let old_ts = now.saturating_sub(120_000);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_retention(Duration::from_secs(60))
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build");
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let engine = make_engine(&storage);

        let write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![PromLabel {
                    name: "__name__".to_string(),
                    value: "cpu_usage".to_string(),
                }],
                samples: vec![PromSample {
                    value: 11.5,
                    timestamp: old_ts,
                }],
                ..Default::default()
            }],
            metadata: Vec::new(),
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");

        let response = handle_request_with_metadata_and_exemplar_store(
            &storage,
            &metadata_store,
            &exemplar_store,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/write".to_string(),
                headers: HashMap::from([
                    ("content-encoding".to_string(), "snappy".to_string()),
                    (
                        "content-type".to_string(),
                        "application/x-protobuf".to_string(),
                    ),
                ]),
                body: snappy_encode(&encoded),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;

        assert_eq!(response.status, 422);
        assert_eq!(
            response_header(&response, WRITE_ERROR_CODE_HEADER),
            Some("write_out_of_retention")
        );
        let body = String::from_utf8(response.body).expect("response body should decode");
        assert!(body.contains("outside the retention window"));
    }

    #[tokio::test]
    async fn remote_write_accepts_metadata_and_metadata_endpoint_is_tenant_scoped() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let data_path = temp_dir.path().join("data");
        let storage = make_persistent_storage(&data_path);
        let metadata_store = make_metadata_store(Some(&data_path));
        let engine = make_engine(&storage);

        let tenant_a_write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![PromLabel {
                    name: "__name__".to_string(),
                    value: "cpu_usage".to_string(),
                }],
                samples: vec![PromSample {
                    value: 11.5,
                    timestamp: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            metadata: vec![
                MetricMetadata {
                    r#type: MetricType::Gauge as i32,
                    metric_family_name: "zeta_metric".to_string(),
                    help: "Zeta metric".to_string(),
                    unit: "widgets".to_string(),
                },
                MetricMetadata {
                    r#type: MetricType::Counter as i32,
                    metric_family_name: "alpha_metric".to_string(),
                    help: "Alpha metric".to_string(),
                    unit: "requests".to_string(),
                },
            ],
        };
        let mut tenant_a_encoded = Vec::new();
        tenant_a_write
            .encode(&mut tenant_a_encoded)
            .expect("protobuf encode should work");
        let tenant_a_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([
                ("content-encoding".to_string(), "snappy".to_string()),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
                ("x-tsink-tenant".to_string(), "tenant-a".to_string()),
            ]),
            body: snappy_encode(&tenant_a_encoded),
        };

        let tenant_b_write = WriteRequest {
            timeseries: Vec::new(),
            metadata: vec![MetricMetadata {
                r#type: MetricType::Counter as i32,
                metric_family_name: "omega_metric".to_string(),
                help: "Omega metric".to_string(),
                unit: "seconds".to_string(),
            }],
        };
        let mut tenant_b_encoded = Vec::new();
        tenant_b_write
            .encode(&mut tenant_b_encoded)
            .expect("protobuf encode should work");
        let tenant_b_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([
                ("content-encoding".to_string(), "snappy".to_string()),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
                ("x-tsink-tenant".to_string(), "tenant-b".to_string()),
            ]),
            body: snappy_encode(&tenant_b_encoded),
        };

        let tenant_a_response = handle_request_with_metadata_store(
            &storage,
            &metadata_store,
            &engine,
            tenant_a_request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(tenant_a_response.status, 200);

        let tenant_b_response = handle_request_with_metadata_store(
            &storage,
            &metadata_store,
            &engine,
            tenant_b_request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(tenant_b_response.status, 200);

        let limited_query = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/metadata?limit=1".to_string(),
            headers: HashMap::from([("x-tsink-tenant".to_string(), "tenant-a".to_string())]),
            body: Vec::new(),
        };
        let limited_response = handle_request_with_metadata_store(
            &storage,
            &metadata_store,
            &engine,
            limited_query,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(limited_response.status, 200);
        let limited_body: JsonValue =
            serde_json::from_slice(&limited_response.body).expect("JSON body should decode");
        let limited_data = limited_body["data"]
            .as_object()
            .expect("metadata data must be an object");
        assert_eq!(
            limited_data.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["alpha_metric"]
        );

        let tenant_a_metadata_response = handle_request_with_metadata_store(
            &storage,
            &metadata_store,
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/metadata".to_string(),
                headers: HashMap::from([("x-tsink-tenant".to_string(), "tenant-a".to_string())]),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(tenant_a_metadata_response.status, 200);
        let tenant_a_body: JsonValue = serde_json::from_slice(&tenant_a_metadata_response.body)
            .expect("tenant metadata JSON should decode");
        let tenant_a_data = tenant_a_body["data"]
            .as_object()
            .expect("metadata data must be an object");
        assert_eq!(
            tenant_a_data.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["alpha_metric", "zeta_metric"]
        );
        assert!(tenant_a_data.get("omega_metric").is_none());
        assert_eq!(tenant_a_data["alpha_metric"][0]["type"], "counter");
        assert_eq!(tenant_a_data["zeta_metric"][0]["unit"], "widgets");

        let points = storage
            .select(
                "cpu_usage",
                &[Label::new(tenant::TENANT_LABEL, "tenant-a")],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("sample should be stored alongside metadata");
        assert_eq!(points.len(), 1);

        storage.close().expect("close should succeed");
    }

    #[tokio::test]
    async fn metric_metadata_survives_restart() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let data_path = temp_dir.path().join("data");
        let storage = make_persistent_storage(&data_path);
        let metadata_store = make_metadata_store(Some(&data_path));
        let engine = make_engine(&storage);

        let write = WriteRequest {
            timeseries: Vec::new(),
            metadata: vec![MetricMetadata {
                r#type: MetricType::Counter as i32,
                metric_family_name: "http_requests_total".to_string(),
                help: "Total requests.".to_string(),
                unit: "requests".to_string(),
            }],
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");

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
            body: snappy_encode(&encoded),
        };

        let response = handle_request_with_metadata_store(
            &storage,
            &metadata_store,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);

        storage.close().expect("close should succeed");
        drop(metadata_store);

        let reopened_storage = make_persistent_storage(&data_path);
        let reopened_metadata_store = make_metadata_store(Some(&data_path));
        let reopened_engine = make_engine(&reopened_storage);
        let query_response = handle_request_with_metadata_store(
            &reopened_storage,
            &reopened_metadata_store,
            &reopened_engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/metadata".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(query_response.status, 200);
        let body: JsonValue =
            serde_json::from_slice(&query_response.body).expect("response body should decode");
        assert_eq!(
            body["data"]["http_requests_total"][0]["help"],
            "Total requests."
        );

        reopened_storage.close().expect("close should succeed");
    }

    #[tokio::test]
    async fn exemplars_survive_restart() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let data_path = temp_dir.path().join("data");
        let storage = make_persistent_storage(&data_path);
        let metadata_store = make_metadata_store(Some(&data_path));
        let exemplar_store = make_exemplar_store(Some(&data_path));
        let engine = make_engine(&storage);

        let write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![PromLabel {
                    name: "__name__".to_string(),
                    value: "restart_metric".to_string(),
                }],
                exemplars: vec![Exemplar {
                    labels: vec![PromLabel {
                        name: "trace_id".to_string(),
                        value: "restart-trace".to_string(),
                    }],
                    value: 7.0,
                    timestamp: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            metadata: Vec::new(),
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");

        let response = handle_request_with_metadata_and_exemplar_store(
            &storage,
            &metadata_store,
            &exemplar_store,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/write".to_string(),
                headers: HashMap::from([
                    ("content-encoding".to_string(), "snappy".to_string()),
                    (
                        "content-type".to_string(),
                        "application/x-protobuf".to_string(),
                    ),
                ]),
                body: snappy_encode(&encoded),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);

        storage.close().expect("close should succeed");
        drop(metadata_store);
        drop(exemplar_store);

        let reopened_storage = make_persistent_storage(&data_path);
        let reopened_metadata_store = make_metadata_store(Some(&data_path));
        let reopened_exemplar_store = make_exemplar_store(Some(&data_path));
        let reopened_engine = make_engine(&reopened_storage);
        let query_response = handle_request_with_metadata_and_exemplar_store(
            &reopened_storage,
            &reopened_metadata_store,
            &reopened_exemplar_store,
            &reopened_engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/query_exemplars?query=restart_metric&start=1699999999.0&end=1700000001.0".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(query_response.status, 200);
        let body: JsonValue =
            serde_json::from_slice(&query_response.body).expect("response body should decode");
        assert_eq!(
            body["data"][0]["exemplars"][0]["labels"]["trace_id"],
            "restart-trace"
        );

        reopened_storage.close().expect("close should succeed");
    }

    #[tokio::test]
    async fn remote_write_accepts_exemplars_and_histograms() {
        let storage = make_storage();
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let engine = make_engine(&storage);

        let exemplar_write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![PromLabel {
                    name: "__name__".to_string(),
                    value: "http_request_duration_seconds".to_string(),
                }],
                exemplars: vec![Exemplar {
                    labels: vec![PromLabel {
                        name: "trace_id".to_string(),
                        value: "abc123".to_string(),
                    }],
                    value: 0.42,
                    timestamp: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            metadata: Vec::new(),
        };
        let mut exemplar_encoded = Vec::new();
        exemplar_write
            .encode(&mut exemplar_encoded)
            .expect("protobuf encode should work");
        let exemplar_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([
                ("content-encoding".to_string(), "snappy".to_string()),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
            ]),
            body: snappy_encode(&exemplar_encoded),
        };
        let exemplar_response = handle_request_with_metadata_and_exemplar_store(
            &storage,
            &metadata_store,
            &exemplar_store,
            &engine,
            exemplar_request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(exemplar_response.status, 200);

        let exemplar_query_response = handle_request_with_metadata_and_exemplar_store(
            &storage,
            &metadata_store,
            &exemplar_store,
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/query_exemplars?query=http_request_duration_seconds&start=1699999999.0&end=1700000001.0".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(exemplar_query_response.status, 200);
        let exemplar_body: JsonValue = serde_json::from_slice(&exemplar_query_response.body)
            .expect("query response should decode");
        assert_eq!(
            exemplar_body["data"][0]["seriesLabels"]["__name__"],
            "http_request_duration_seconds"
        );
        assert_eq!(
            exemplar_body["data"][0]["exemplars"][0]["labels"]["trace_id"],
            "abc123"
        );

        let histogram_write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![PromLabel {
                    name: "__name__".to_string(),
                    value: "rpc_duration_seconds".to_string(),
                }],
                histograms: vec![Histogram {
                    count: Some(crate::prom_remote::histogram::Count::CountInt(5)),
                    sum: 1.5,
                    schema: 0,
                    zero_threshold: 0.0,
                    zero_count: Some(crate::prom_remote::histogram::ZeroCount::ZeroCountInt(1)),
                    negative_spans: Vec::new(),
                    negative_deltas: Vec::new(),
                    negative_counts: Vec::new(),
                    positive_spans: vec![BucketSpan {
                        offset: 0,
                        length: 1,
                    }],
                    positive_deltas: vec![5],
                    positive_counts: Vec::new(),
                    reset_hint: HistogramResetHint::No as i32,
                    timestamp: 1_700_000_000_000,
                    custom_values: Vec::new(),
                }],
                ..Default::default()
            }],
            metadata: Vec::new(),
        };
        let mut histogram_encoded = Vec::new();
        histogram_write
            .encode(&mut histogram_encoded)
            .expect("protobuf encode should work");
        let histogram_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([
                ("content-encoding".to_string(), "snappy".to_string()),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
            ]),
            body: snappy_encode(&histogram_encoded),
        };
        let histogram_response = handle_request(
            &storage,
            &engine,
            histogram_request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(histogram_response.status, 200);
        assert_eq!(
            response_header(&histogram_response, "X-Tsink-Histograms-Accepted"),
            Some("1")
        );
        let histogram_points = storage
            .select(
                "rpc_duration_seconds",
                &[Label::new(tenant::TENANT_LABEL, tenant::DEFAULT_TENANT_ID)],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("histogram sample should be stored");
        assert_eq!(histogram_points.len(), 1);
        let stored_histogram = histogram_points[0]
            .value_as_histogram()
            .expect("stored sample should be a histogram");
        assert_eq!(stored_histogram.sum, 1.5);
        assert_eq!(stored_histogram.positive_deltas, vec![5]);
    }

    #[tokio::test]
    async fn cluster_remote_write_replicates_and_queries_exemplars() {
        let temp_dir = TempDir::new().expect("tempdir");
        let remote_storage = make_storage();
        let remote_exemplar_store = make_exemplar_store(None);
        let (remote_endpoint, remote_requests, shutdown_tx, server_handle) =
            spawn_internal_storage_peer_with_exemplars(
                Arc::clone(&remote_storage),
                Arc::clone(&remote_exemplar_store),
            )
            .await;

        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![format!("node-b@{remote_endpoint}")],
            shards: 1,
            replication_factor: 2,
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |_| {});
        let storage = make_storage();
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let engine = make_engine(&storage);

        let write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![PromLabel {
                    name: "__name__".to_string(),
                    value: "cluster_exemplar_metric".to_string(),
                }],
                exemplars: vec![Exemplar {
                    labels: vec![PromLabel {
                        name: "trace_id".to_string(),
                        value: "cluster-trace".to_string(),
                    }],
                    value: 5.0,
                    timestamp: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            metadata: Vec::new(),
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");

        let response = handle_request_with_admin_and_cluster_and_tenant_and_metadata(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/write".to_string(),
                headers: HashMap::from([
                    ("content-encoding".to_string(), "snappy".to_string()),
                    (
                        "content-type".to_string(),
                        "application/x-protobuf".to_string(),
                    ),
                ]),
                body: snappy_encode(&encoded),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&cluster_context.runtime.internal_api),
            Some(&cluster_context),
            None,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(response.status, 200);
        assert!(remote_requests.load(AtomicOrdering::Relaxed) >= 1);

        let remote_selection = tenant::selection_for_tenant(
            &SeriesSelection::new().with_metric("cluster_exemplar_metric"),
            tenant::DEFAULT_TENANT_ID,
        )
        .expect("tenant selection should build");
        let remote_results = remote_exemplar_store
            .query(
                &[remote_selection],
                1_699_999_999_000,
                1_700_000_001_000,
                10,
            )
            .expect("remote exemplar query should succeed");
        assert_eq!(remote_results.len(), 1);
        assert_eq!(
            remote_results[0].exemplars[0].labels[0].value,
            "cluster-trace"
        );

        let query_response = handle_request_with_admin_and_cluster_and_tenant_and_metadata(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/query_exemplars?query=cluster_exemplar_metric&start=1699999999.0&end=1700000001.0".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&cluster_context.runtime.internal_api),
            Some(&cluster_context),
            None,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(query_response.status, 200);
        let body: JsonValue =
            serde_json::from_slice(&query_response.body).expect("query response should decode");
        assert_eq!(
            body["data"][0]["exemplars"][0]["labels"]["trace_id"],
            "cluster-trace"
        );

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn cluster_remote_write_replicates_metadata_and_histograms() {
        let temp_dir = TempDir::new().expect("tempdir");
        let remote_storage = make_storage();
        let remote_metadata_store = make_metadata_store(None);
        let remote_exemplar_store = make_exemplar_store(None);
        let (remote_endpoint, remote_requests, shutdown_tx, server_handle) =
            spawn_internal_storage_peer_with_metadata_and_exemplars(
                Arc::clone(&remote_storage),
                Arc::clone(&remote_metadata_store),
                Arc::clone(&remote_exemplar_store),
            )
            .await;

        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![format!("node-b@{remote_endpoint}")],
            shards: 1,
            replication_factor: 2,
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |_| {});
        let storage = make_storage();
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let engine = make_engine(&storage);

        let write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![PromLabel {
                    name: "__name__".to_string(),
                    value: "cluster_histogram_metric".to_string(),
                }],
                histograms: vec![Histogram {
                    count: Some(crate::prom_remote::histogram::Count::CountInt(7)),
                    sum: 2.5,
                    schema: 0,
                    zero_threshold: 0.0,
                    zero_count: Some(crate::prom_remote::histogram::ZeroCount::ZeroCountInt(1)),
                    negative_spans: Vec::new(),
                    negative_deltas: Vec::new(),
                    negative_counts: Vec::new(),
                    positive_spans: vec![BucketSpan {
                        offset: 0,
                        length: 1,
                    }],
                    positive_deltas: vec![7],
                    positive_counts: Vec::new(),
                    reset_hint: HistogramResetHint::No as i32,
                    timestamp: 1_700_000_000_000,
                    custom_values: Vec::new(),
                }],
                ..Default::default()
            }],
            metadata: vec![MetricMetadata {
                r#type: MetricType::Histogram as i32,
                metric_family_name: "cluster_histogram_metric".to_string(),
                help: "Cluster histogram".to_string(),
                unit: "seconds".to_string(),
            }],
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");

        let response = handle_request_with_admin_and_cluster_and_tenant_and_metadata(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/write".to_string(),
                headers: HashMap::from([
                    ("content-encoding".to_string(), "snappy".to_string()),
                    (
                        "content-type".to_string(),
                        "application/x-protobuf".to_string(),
                    ),
                ]),
                body: snappy_encode(&encoded),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&cluster_context.runtime.internal_api),
            Some(&cluster_context),
            None,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(response.status, 200);
        assert_eq!(
            response_header(&response, "X-Tsink-Metadata-Applied"),
            Some("1")
        );
        assert_eq!(
            response_header(&response, "X-Tsink-Histograms-Accepted"),
            Some("1")
        );
        assert!(remote_requests.load(AtomicOrdering::Relaxed) >= 2);

        let remote_histograms = remote_storage
            .select(
                "cluster_histogram_metric",
                &[Label::new(tenant::TENANT_LABEL, tenant::DEFAULT_TENANT_ID)],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("remote histogram should be stored");
        assert_eq!(remote_histograms.len(), 1);
        assert!(remote_histograms[0].value_as_histogram().is_some());

        let remote_metadata = remote_metadata_store
            .query(
                tenant::DEFAULT_TENANT_ID,
                Some("cluster_histogram_metric"),
                10,
            )
            .expect("remote metadata query should succeed");
        assert_eq!(remote_metadata.len(), 1);
        assert_eq!(remote_metadata[0].unit, "seconds");

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn remote_write_rejects_missing_metric_name() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![PromLabel {
                    name: "host".to_string(),
                    value: "server-a".to_string(),
                }],
                samples: vec![PromSample {
                    value: 11.5,
                    timestamp: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            metadata: Vec::new(),
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");

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
        assert_eq!(response.status, 400);
        let body = String::from_utf8(response.body).expect("response body should be utf8");
        assert!(body.contains("missing the __name__ label"));
        assert!(storage
            .list_metrics()
            .expect("list_metrics should succeed")
            .is_empty());
    }

    #[tokio::test]
    async fn remote_write_rejects_duplicate_labels() {
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
                    PromLabel {
                        name: "host".to_string(),
                        value: "server-b".to_string(),
                    },
                ],
                samples: vec![PromSample {
                    value: 11.5,
                    timestamp: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            metadata: Vec::new(),
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");

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
        assert_eq!(response.status, 400);
        let body = String::from_utf8(response.body).expect("response body should be utf8");
        assert!(body.contains("duplicate label 'host'"));
        assert!(storage
            .list_metrics()
            .expect("list_metrics should succeed")
            .is_empty());
    }

    #[tokio::test]
    async fn remote_write_accepts_mixed_metadata_exemplar_and_histogram_payloads() {
        let storage = make_storage();
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let engine = make_engine(&storage);

        let write = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![PromLabel {
                        name: "__name__".to_string(),
                        value: "http_request_duration_seconds".to_string(),
                    }],
                    exemplars: vec![Exemplar {
                        labels: vec![PromLabel {
                            name: "trace_id".to_string(),
                            value: "abc123".to_string(),
                        }],
                        value: 0.42,
                        timestamp: 1_700_000_000_000,
                    }],
                    ..Default::default()
                },
                TimeSeries {
                    labels: vec![PromLabel {
                        name: "__name__".to_string(),
                        value: "rpc_duration_seconds".to_string(),
                    }],
                    histograms: vec![Histogram {
                        count: Some(crate::prom_remote::histogram::Count::CountInt(5)),
                        sum: 1.5,
                        schema: 0,
                        zero_threshold: 0.0,
                        zero_count: Some(crate::prom_remote::histogram::ZeroCount::ZeroCountInt(1)),
                        negative_spans: Vec::new(),
                        negative_deltas: Vec::new(),
                        negative_counts: Vec::new(),
                        positive_spans: vec![BucketSpan {
                            offset: 0,
                            length: 1,
                        }],
                        positive_deltas: vec![5],
                        positive_counts: Vec::new(),
                        reset_hint: HistogramResetHint::No as i32,
                        timestamp: 1_700_000_000_000,
                        custom_values: Vec::new(),
                    }],
                    ..Default::default()
                },
            ],
            metadata: vec![MetricMetadata {
                r#type: MetricType::Counter as i32,
                metric_family_name: "http_requests_total".to_string(),
                help: "Total requests.".to_string(),
                unit: "requests".to_string(),
            }],
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");

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
            body: snappy_encode(&encoded),
        };

        let response = handle_request_with_metadata_and_exemplar_store(
            &storage,
            &metadata_store,
            &exemplar_store,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(response.status, 200);
        assert_eq!(
            response_header(&response, "X-Tsink-Metadata-Applied"),
            Some("1")
        );
        assert_eq!(
            response_header(&response, "X-Tsink-Exemplars-Accepted"),
            Some("1")
        );
        assert_eq!(
            response_header(&response, "X-Tsink-Histograms-Accepted"),
            Some("1")
        );

        let metadata_response = handle_request_with_metadata_store(
            &storage,
            &metadata_store,
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/metadata?metric=http_requests_total".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(metadata_response.status, 200);
        let metadata_body: JsonValue =
            serde_json::from_slice(&metadata_response.body).expect("metadata body should decode");
        assert_eq!(
            metadata_body["data"]["http_requests_total"][0]["type"],
            "counter"
        );

        let histogram_points = storage
            .select(
                "rpc_duration_seconds",
                &[Label::new(tenant::TENANT_LABEL, tenant::DEFAULT_TENANT_ID)],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("histogram sample should be stored");
        assert_eq!(histogram_points.len(), 1);
        assert!(histogram_points[0].value_as_histogram().is_some());

        let exemplar_query_response = handle_request_with_metadata_and_exemplar_store(
            &storage,
            &metadata_store,
            &exemplar_store,
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/query_exemplars?query=http_request_duration_seconds&start=1699999999.0&end=1700000001.0".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(exemplar_query_response.status, 200);
        let exemplar_body: JsonValue = serde_json::from_slice(&exemplar_query_response.body)
            .expect("exemplar body should decode");
        assert_eq!(
            exemplar_body["data"][0]["exemplars"][0]["labels"]["trace_id"],
            "abc123"
        );
    }

    #[tokio::test]
    async fn instant_query_returns_retryable_error_when_public_read_slots_are_saturated() {
        let storage = make_storage();
        let read_admission = ReadAdmissionController::new(admission::ReadAdmissionGuardrails {
            max_inflight_requests: 1,
            max_inflight_queries: 8,
            acquire_timeout: Duration::from_millis(1),
        })
        .expect("read admission should build");
        let _held = read_admission
            .admit_request(1)
            .await
            .expect("first read request should be admitted");

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query?query=cpu_usage".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_instant_query_with_admission(
            &storage,
            &request,
            TimestampPrecision::Milliseconds,
            None,
            None,
            None,
            &read_admission,
        )
        .await;
        assert_eq!(response.status, 429);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("read_overloaded")
        );
        assert_eq!(response_header(&response, "Retry-After"), Some("1"));
    }

    #[tokio::test]
    async fn series_returns_non_retryable_error_when_public_read_query_budget_is_exceeded() {
        let storage = make_storage();
        let read_admission = ReadAdmissionController::new(admission::ReadAdmissionGuardrails {
            max_inflight_requests: 4,
            max_inflight_queries: 1,
            acquire_timeout: Duration::from_millis(1),
        })
        .expect("read admission should build");

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/series?match[]=cpu_usage&match[]=memory_usage".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response =
            handle_series_with_admission(&storage, &request, None, None, None, &read_admission)
                .await;
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("read_resource_limit_exceeded")
        );
        assert_eq!(response_header(&response, "Retry-After"), None);
    }

    #[tokio::test]
    async fn tenant_header_isolates_write_read_and_query_apis() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let default_write = WriteRequest {
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
                ..Default::default()
            }],
            metadata: Vec::new(),
        };
        let team_b_write = WriteRequest {
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
                    value: 27.0,
                    timestamp: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            metadata: Vec::new(),
        };

        for (write, tenant_id) in [(default_write, None), (team_b_write, Some("team-b"))] {
            let mut encoded = Vec::new();
            write
                .encode(&mut encoded)
                .expect("protobuf encode should work");
            let mut headers = HashMap::from([
                ("content-encoding".to_string(), "snappy".to_string()),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
            ]);
            if let Some(tenant_id) = tenant_id {
                headers.insert(tenant::TENANT_HEADER.to_string(), tenant_id.to_string());
            }
            let request = HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/write".to_string(),
                headers,
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
        }

        let read = ReadRequest {
            queries: vec![Query {
                start_timestamp_ms: 1_700_000_000_000,
                end_timestamp_ms: 1_700_000_000_000,
                matchers: vec![
                    LabelMatcher {
                        r#type: MatcherType::Eq as i32,
                        name: "__name__".to_string(),
                        value: "cpu_usage".to_string(),
                    },
                    LabelMatcher {
                        r#type: MatcherType::Eq as i32,
                        name: "host".to_string(),
                        value: "server-a".to_string(),
                    },
                ],
                hints: None,
            }],
            accepted_response_types: Vec::new(),
        };
        let mut encoded_read = Vec::new();
        read.encode(&mut encoded_read)
            .expect("protobuf encode should work");

        for (tenant_id, expected_value) in [(None, 11.5), (Some("team-b"), 27.0)] {
            let mut headers = HashMap::from([
                ("content-encoding".to_string(), "snappy".to_string()),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
            ]);
            if let Some(tenant_id) = tenant_id {
                headers.insert(tenant::TENANT_HEADER.to_string(), tenant_id.to_string());
            }
            let request = HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/read".to_string(),
                headers,
                body: snappy_encode(&encoded_read),
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

            let decoded = snappy_decode(&response.body);
            let body = ReadResponse::decode(decoded.as_slice()).expect("response should decode");
            let series = &body.results[0].timeseries[0];
            assert_eq!(series.samples[0].value, expected_value);
            assert!(series
                .labels
                .iter()
                .all(|label| label.name != tenant::TENANT_LABEL));
        }

        let labels_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/labels".to_string(),
            headers: HashMap::from([(tenant::TENANT_HEADER.to_string(), "team-b".to_string())]),
            body: Vec::new(),
        };
        let labels_response = handle_request(
            &storage,
            &engine,
            labels_request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(labels_response.status, 200);
        let labels_body: JsonValue =
            serde_json::from_slice(&labels_response.body).expect("valid JSON");
        let labels = labels_body["data"]
            .as_array()
            .expect("labels data should be an array");
        assert!(labels.iter().any(|label| label == "__name__"));
        assert!(labels.iter().any(|label| label == "host"));
        assert!(!labels.iter().any(|label| label == tenant::TENANT_LABEL));

        let query_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query?query=cpu_usage{host=\"server-a\"}&time=1700000000000".to_string(),
            headers: HashMap::from([(tenant::TENANT_HEADER.to_string(), "team-b".to_string())]),
            body: Vec::new(),
        };
        let query_response = handle_request(
            &storage,
            &engine,
            query_request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(query_response.status, 200);
        let query_body: JsonValue =
            serde_json::from_slice(&query_response.body).expect("valid JSON");
        let result = query_body["data"]["result"]
            .as_array()
            .expect("result should be an array");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["metric"]["__name__"], "cpu_usage");
        assert_eq!(result[0]["metric"]["host"], "server-a");
        assert!(result[0]["metric"].get(tenant::TENANT_LABEL).is_none());
        assert_eq!(result[0]["value"][1], "27");
    }

    #[tokio::test]
    async fn remote_write_rejects_reserved_tenant_label() {
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
                        name: tenant::TENANT_LABEL.to_string(),
                        value: "spoofed".to_string(),
                    },
                ],
                samples: vec![PromSample {
                    value: 11.5,
                    timestamp: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            metadata: Vec::new(),
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");

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
        assert_eq!(response.status, 400);
        let body = String::from_utf8(response.body).expect("response body should be utf8");
        assert!(body.contains(tenant::TENANT_LABEL));
        assert!(storage
            .list_metrics()
            .expect("list_metrics should succeed")
            .is_empty());
    }

    #[test]
    fn decode_body_rejects_oversized_snappy_payload() {
        let oversized = vec![0_u8; MAX_BODY_BYTES + 1];
        let compressed = SnappyEncoder::new()
            .compress_vec(&oversized)
            .expect("snappy encode should succeed");
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([("content-encoding".to_string(), "snappy".to_string())]),
            body: compressed,
        };

        let err = decode_body(&request).expect_err("oversized decoded body must fail");
        assert!(err.contains("decoded request body too large"));
    }

    #[test]
    fn write_routing_error_response_uses_distinct_timeout_and_replica_status_codes() {
        let timeout = write_routing_error_response(WriteRoutingError::ConsistencyTimeout {
            shard: 7,
            mode: crate::cluster::config::ClusterWriteConsistency::Quorum,
            required_acks: 2,
            acknowledged_acks: 1,
            max_possible_acks: 1,
        });
        assert_eq!(timeout.status, 504);

        let insufficient = write_routing_error_response(WriteRoutingError::InsufficientReplicas {
            shard: 7,
            mode: crate::cluster::config::ClusterWriteConsistency::All,
            required_acks: 3,
            acknowledged_acks: 2,
            max_possible_acks: 2,
        });
        assert_eq!(insufficient.status, 503);
    }

    #[test]
    fn parse_request_write_consistency_validates_header_values() {
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([(
                WRITE_CONSISTENCY_OVERRIDE_HEADER.to_string(),
                "all".to_string(),
            )]),
            body: Vec::new(),
        };

        let parsed = parse_request_write_consistency(&request)
            .expect("valid consistency override should parse")
            .expect("header should be present");
        assert_eq!(parsed, crate::cluster::config::ClusterWriteConsistency::All);

        let invalid_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([(
                WRITE_CONSISTENCY_OVERRIDE_HEADER.to_string(),
                "invalid".to_string(),
            )]),
            body: Vec::new(),
        };
        let invalid_err = parse_request_write_consistency(&invalid_request)
            .expect_err("invalid override value should fail");
        assert!(invalid_err.contains("invalid write consistency"));
    }

    #[test]
    fn parse_request_read_partial_response_policy_validates_header_values() {
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/labels".to_string(),
            headers: HashMap::from([(
                READ_PARTIAL_RESPONSE_OVERRIDE_HEADER.to_string(),
                "deny".to_string(),
            )]),
            body: Vec::new(),
        };

        let parsed = parse_request_read_partial_response_policy(&request)
            .expect("valid policy override should parse")
            .expect("header should be present");
        assert_eq!(parsed, ClusterReadPartialResponsePolicy::Deny);

        let invalid_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/labels".to_string(),
            headers: HashMap::from([(
                READ_PARTIAL_RESPONSE_OVERRIDE_HEADER.to_string(),
                "sometimes".to_string(),
            )]),
            body: Vec::new(),
        };
        let invalid_err = parse_request_read_partial_response_policy(&invalid_request)
            .expect_err("invalid policy override should fail");
        assert!(invalid_err.contains("invalid read partial response policy"));
    }

    #[test]
    fn resolve_request_write_consistency_respects_fixed_tenant_policy() {
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([(
                WRITE_CONSISTENCY_OVERRIDE_HEADER.to_string(),
                "all".to_string(),
            )]),
            body: Vec::new(),
        };
        let policy = tenant::TenantRequestPolicy {
            write_consistency: Some(ClusterWriteConsistency::Quorum),
            ..tenant::TenantRequestPolicy::default()
        };

        let err = resolve_request_write_consistency(&request, &policy)
            .expect_err("tenant fixed write consistency should reject mismatched override");
        assert!(err.contains("fixed to 'quorum'"));

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let resolved = resolve_request_write_consistency(&request, &policy)
            .expect("tenant fixed write consistency should apply");
        assert_eq!(resolved, Some(ClusterWriteConsistency::Quorum));
    }

    #[tokio::test]
    async fn tenant_registry_rejects_remote_write_when_row_quota_exceeded() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let tenant_registry = tenant::TenantRegistry::from_json_str(
            r#"{
                "tenants": {
                    "team-b": {
                        "auth": {
                            "tokens": [{ "token": "team-b-write", "scopes": ["write"] }]
                        },
                        "quotas": {
                            "maxWriteRowsPerRequest": 1
                        }
                    }
                }
            }"#,
        )
        .expect("tenant registry should parse");

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
                samples: vec![
                    PromSample {
                        value: 11.5,
                        timestamp: 1_700_000_000_000,
                    },
                    PromSample {
                        value: 12.5,
                        timestamp: 1_700_000_000_001,
                    },
                ],
                ..Default::default()
            }],
            metadata: Vec::new(),
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([
                ("content-encoding".to_string(), "snappy".to_string()),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
                (tenant::TENANT_HEADER.to_string(), "team-b".to_string()),
                (
                    "authorization".to_string(),
                    "Bearer team-b-write".to_string(),
                ),
            ]),
            body: snappy_encode(&encoded),
        };

        let response = handle_request_with_admin_and_cluster_and_tenant(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            None,
            None,
            Some(&tenant_registry),
        )
        .await;
        assert_eq!(response.status, 413);
        let body = String::from_utf8(response.body).expect("response body should be utf8");
        assert!(body.contains("tenant write rows per request limit exceeded"));
    }

    #[tokio::test]
    async fn tenant_registry_rejects_range_query_when_point_quota_exceeded() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let tenant_registry = tenant::TenantRegistry::from_json_str(
            r#"{
                "tenants": {
                    "team-b": {
                        "auth": {
                            "tokens": [{ "token": "team-b-read", "scopes": ["read"] }]
                        },
                        "quotas": {
                            "maxRangePointsPerQuery": 2
                        }
                    }
                }
            }"#,
        )
        .expect("tenant registry should parse");
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query_range?query=up&start=1700000000000&end=1700000003000&step=1s"
                .to_string(),
            headers: HashMap::from([
                (tenant::TENANT_HEADER.to_string(), "team-b".to_string()),
                (
                    "authorization".to_string(),
                    "Bearer team-b-read".to_string(),
                ),
            ]),
            body: Vec::new(),
        };

        let response = handle_request_with_admin_and_cluster_and_tenant(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            None,
            None,
            Some(&tenant_registry),
        )
        .await;
        assert_eq!(response.status, 422);
        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("response should be json");
        assert!(body["error"]
            .as_str()
            .is_some_and(|message| message.contains("tenant range query point limit exceeded")));
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
                hints: None,
            }],
            accepted_response_types: Vec::new(),
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
    async fn remote_read_returns_mixed_float_and_histogram_series() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "http_requests_total",
                    vec![Label::new("job", "api")],
                    DataPoint::new(1_700_000_000_100, 42.0),
                ),
                Row::with_labels(
                    "request_duration_native_seconds",
                    vec![Label::new("job", "api")],
                    DataPoint::new(1_700_000_000_100, Value::from(sample_native_histogram())),
                ),
            ])
            .expect("insert should work");

        let read = ReadRequest {
            queries: vec![Query {
                start_timestamp_ms: 1_700_000_000_000,
                end_timestamp_ms: 1_700_000_000_200,
                matchers: vec![LabelMatcher {
                    r#type: MatcherType::Eq as i32,
                    name: "job".to_string(),
                    value: "api".to_string(),
                }],
                hints: None,
            }],
            accepted_response_types: Vec::new(),
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

        let decoded = snappy_decode(&response.body);
        let read_response =
            ReadResponse::decode(decoded.as_slice()).expect("response should decode");
        assert_eq!(read_response.results.len(), 1);
        assert_eq!(read_response.results[0].timeseries.len(), 2);

        let float_series = read_response.results[0]
            .timeseries
            .iter()
            .find(|series| {
                series
                    .labels
                    .iter()
                    .any(|label| label.name == "__name__" && label.value == "http_requests_total")
            })
            .expect("float series should be present");
        assert_eq!(float_series.samples.len(), 1);
        assert!(float_series.histograms.is_empty());

        let histogram_series = read_response.results[0]
            .timeseries
            .iter()
            .find(|series| {
                series.labels.iter().any(|label| {
                    label.name == "__name__" && label.value == "request_duration_native_seconds"
                })
            })
            .expect("histogram series should be present");
        assert!(histogram_series.samples.is_empty());
        assert_eq!(histogram_series.histograms.len(), 1);
        assert_eq!(
            histogram_series.histograms[0].count,
            Some(crate::prom_remote::histogram::Count::CountFloat(20.0))
        );
    }

    #[tokio::test]
    async fn remote_read_rejects_streamed_response_types_until_supported() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let read = ReadRequest {
            queries: vec![Query {
                start_timestamp_ms: 1_700_000_000_000,
                end_timestamp_ms: 1_700_000_000_100,
                matchers: vec![LabelMatcher {
                    r#type: MatcherType::Eq as i32,
                    name: "__name__".to_string(),
                    value: "up".to_string(),
                }],
                hints: None,
            }],
            accepted_response_types: vec![ReadResponseType::StreamedXorChunks as i32],
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
        assert_eq!(response.status, 400);
        let body = String::from_utf8(response.body).expect("response body should be utf8");
        assert!(body.contains("StreamedXorChunks"));
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
    async fn promql_instant_query_returns_histogram_json() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        storage
            .insert_rows(&[Row::with_labels(
                "request_duration_native_seconds",
                vec![Label::new("job", "api")],
                DataPoint::new(1_700_000_000_000, Value::from(sample_native_histogram())),
            )])
            .expect("insert should work");

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query?query=request_duration_native_seconds&time=1700000000000"
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
        assert_eq!(body["data"]["resultType"], "vector");
        assert_eq!(
            body["data"]["result"][0]["metric"]["__name__"],
            "request_duration_native_seconds"
        );
        assert_eq!(body["data"]["result"][0]["histogram"][1]["count"], "20");
        assert!(body["data"]["result"][0].get("value").is_none());
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
    async fn cluster_promql_instant_query_uses_distributed_storage_adapter() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let cluster_context = cluster_context_with_single_node_control(&temp_dir);
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

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 200);
        assert_eq!(
            response_header(&response, READ_CONSISTENCY_HEADER),
            Some("eventual")
        );
        assert_eq!(
            response_header(&response, READ_PARTIAL_RESPONSE_POLICY_HEADER),
            Some("allow")
        );
        assert_eq!(
            response_header(&response, READ_PARTIAL_RESPONSE_HEADER),
            Some("false")
        );

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["resultType"], "vector");
    }

    #[tokio::test]
    async fn cluster_promql_range_query_uses_distributed_storage_adapter() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let cluster_context = cluster_context_with_single_node_control(&temp_dir);
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

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 200);
        assert_eq!(
            response_header(&response, READ_CONSISTENCY_HEADER),
            Some("eventual")
        );
        assert_eq!(
            response_header(&response, READ_PARTIAL_RESPONSE_POLICY_HEADER),
            Some("allow")
        );
        assert_eq!(
            response_header(&response, READ_PARTIAL_RESPONSE_HEADER),
            Some("false")
        );

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["resultType"], "matrix");
    }

    #[tokio::test]
    async fn query_only_cluster_node_uses_local_object_store_storage_for_promql_reads() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let data_dir = TempDir::new().expect("data dir should create");
        let object_store_dir = TempDir::new().expect("object store dir should create");
        let cluster_context = query_only_cluster_context_with_local_object_store_reads(&temp_dir);
        let query_ts = i64::try_from(unix_timestamp_millis()).unwrap_or(i64::MAX);
        let sample_ts = query_ts.saturating_sub(1_000);

        let source_storage = StorageBuilder::new()
            .with_data_path(data_dir.path())
            .with_object_store_path(object_store_dir.path())
            .with_mirror_hot_segments_to_object_store(true)
            .with_chunk_points(1)
            .with_tiered_retention_policy(Duration::from_secs(60), Duration::from_secs(300))
            .with_retention(Duration::from_secs(600))
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_wal_enabled(false)
            .build()
            .expect("source storage should build");
        source_storage
            .insert_rows(&[Row::with_labels(
                "up",
                vec![Label::new("job", "prom")],
                DataPoint::new(sample_ts, 1.0),
            )])
            .expect("insert should work");
        source_storage.close().expect("source storage should close");

        let storage = StorageBuilder::new()
            .with_object_store_path(object_store_dir.path())
            .with_runtime_mode(StorageRuntimeMode::ComputeOnly)
            .with_tiered_retention_policy(Duration::from_secs(60), Duration::from_secs(300))
            .with_retention(Duration::from_secs(600))
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_wal_enabled(false)
            .build()
            .expect("compute-only storage should build");
        let storage: Arc<dyn Storage> = storage;
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "GET".to_string(),
            path: format!("/api/v1/query?query=up&time={query_ts}"),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 200);
        assert_eq!(response_header(&response, READ_CONSISTENCY_HEADER), None);
        assert_eq!(
            response_header(&response, READ_PARTIAL_RESPONSE_POLICY_HEADER),
            None
        );

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["resultType"], "vector");
    }

    #[tokio::test]
    async fn distributed_query_correctness_corpus_matches_single_node_golden_for_query_series_and_labels(
    ) {
        let fixture = load_distributed_query_corpus_fixture();

        let baseline_storage = make_storage();
        insert_distributed_query_fixture_rows(&baseline_storage, &fixture.local_rows);
        insert_distributed_query_fixture_rows(&baseline_storage, &fixture.remote_rows);
        let baseline_engine = make_engine(&baseline_storage);

        let local_cluster_storage = make_storage();
        insert_distributed_query_fixture_rows(&local_cluster_storage, &fixture.local_rows);
        let local_cluster_engine = make_engine(&local_cluster_storage);

        let remote_storage = make_storage();
        insert_distributed_query_fixture_rows(&remote_storage, &fixture.remote_rows);
        let (remote_endpoint, remote_request_count, shutdown_tx, remote_server) =
            spawn_internal_storage_peer(remote_storage).await;

        let temp_dir = TempDir::new().expect("temp dir should create");
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![format!("node-b@{remote_endpoint}")],
            replication_factor: 2,
            read_consistency: ClusterReadConsistency::Quorum,
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |_| {});

        for case in &fixture.cases {
            let baseline_request = HttpRequest {
                method: case.method.clone(),
                path: case.path.clone(),
                headers: HashMap::new(),
                body: Vec::new(),
            };
            let baseline_response = handle_request(
                &baseline_storage,
                &baseline_engine,
                baseline_request,
                start_time(),
                TimestampPrecision::Milliseconds,
            )
            .await;
            let baseline_data =
                normalize_case_data(&case.path, &parse_success_response_data(&baseline_response));

            let cluster_request = HttpRequest {
                method: case.method.clone(),
                path: case.path.clone(),
                headers: HashMap::new(),
                body: Vec::new(),
            };
            let cluster_response = handle_request_with_admin_and_cluster(
                &local_cluster_storage,
                &local_cluster_engine,
                cluster_request,
                start_time(),
                TimestampPrecision::Milliseconds,
                false,
                None,
                None,
                Some(cluster_context.as_ref()),
            )
            .await;
            let cluster_data =
                normalize_case_data(&case.path, &parse_success_response_data(&cluster_response));

            let expected_data = normalize_case_data(&case.path, &case.expected_data);

            assert_eq!(
                baseline_data, expected_data,
                "single-node golden mismatch for case '{}'",
                case.name
            );
            assert_eq!(
                cluster_data, expected_data,
                "cluster mismatch for case '{}'",
                case.name
            );

            if case.path.starts_with("/api/v1/query") {
                assert_eq!(
                    response_header(&cluster_response, READ_CONSISTENCY_HEADER),
                    Some("quorum"),
                    "missing/invalid read consistency header for case '{}'",
                    case.name
                );
                assert_eq!(
                    response_header(&cluster_response, READ_PARTIAL_RESPONSE_POLICY_HEADER),
                    Some("allow"),
                    "missing/invalid read partial policy header for case '{}'",
                    case.name
                );
                assert_eq!(
                    response_header(&cluster_response, READ_PARTIAL_RESPONSE_HEADER),
                    Some("false"),
                    "missing/invalid read partial response header for case '{}'",
                    case.name
                );
            }

            if case.path.starts_with("/api/v1/labels") || case.path.starts_with("/api/v1/series") {
                let body: JsonValue = serde_json::from_slice(&cluster_response.body)
                    .expect("cluster response should decode");
                assert_eq!(
                    body["partialResponse"]["enabled"],
                    JsonValue::Bool(false),
                    "unexpected partial response flag for case '{}'",
                    case.name
                );
                assert_eq!(
                    body["partialResponse"]["warningCount"],
                    JsonValue::Number(serde_json::Number::from(0)),
                    "unexpected partial response warning count for case '{}'",
                    case.name
                );
            }
        }

        assert!(
            remote_request_count.load(AtomicOrdering::Relaxed) > 0,
            "fixture run should issue at least one remote request"
        );
        shutdown_tx
            .send(())
            .expect("mock remote server shutdown should signal");
        tokio::time::timeout(Duration::from_secs(3), remote_server)
            .await
            .expect("mock remote server should shut down in time")
            .expect("mock remote server task should not panic");
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
    async fn series_endpoint_deduplicates_overlapping_matches() {
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
            path: "/api/v1/series?match[]=http_requests&match[]=http_requests".to_string(),
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
        let data = body["data"].as_array().expect("data should be array");
        assert_eq!(data.len(), 1);
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
    async fn cluster_labels_marks_partial_response_metadata_when_partial_reads_are_allowed() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);
        storage
            .insert_rows(&[Row::with_labels(
                "partial_metric",
                vec![Label::new("job", "local")],
                DataPoint::new(1_700_000_000_000, 1.0),
            )])
            .expect("insert should work");

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/labels".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&cluster_context.runtime.internal_api),
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 200);
        assert_eq!(
            response_header(&response, READ_PARTIAL_RESPONSE_HEADER),
            Some("true")
        );
        assert_eq!(
            response_header(&response, READ_PARTIAL_RESPONSE_POLICY_HEADER),
            Some("allow")
        );
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(body["partialResponse"]["enabled"], true);
        assert!(body["partialResponse"]["warningCount"]
            .as_u64()
            .is_some_and(|count| count > 0));
        assert!(body["warnings"]
            .as_array()
            .is_some_and(|warnings| !warnings.is_empty()));
    }

    #[tokio::test]
    async fn cluster_labels_include_legacy_default_tenant_series() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_single_node_control(&temp_dir);
        storage
            .insert_rows(&[Row::with_labels(
                "up",
                vec![
                    Label::new("job", "prom"),
                    Label::new("instance", "localhost:9090"),
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
        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 200);

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        let data = body["data"].as_array().expect("data should be array");
        let names: Vec<&str> = data.iter().filter_map(|v| v.as_str()).collect();
        assert!(names.contains(&"__name__"));
        assert!(names.contains(&"job"));
        assert!(names.contains(&"instance"));
        assert!(!names.contains(&tenant::TENANT_LABEL));
    }

    #[tokio::test]
    async fn cluster_series_include_legacy_default_tenant_series() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_single_node_control(&temp_dir);
        storage
            .insert_rows(&[Row::with_labels(
                "up",
                vec![
                    Label::new("job", "prom"),
                    Label::new("instance", "localhost:9090"),
                ],
                DataPoint::new(1_700_000_000_000, 1.0),
            )])
            .expect("insert should work");

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/series?match[]=up".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 200);

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        let data = body["data"].as_array().expect("data should be array");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["__name__"], "up");
        assert_eq!(data[0]["job"], "prom");
        assert_eq!(data[0]["instance"], "localhost:9090");
        assert!(data[0].get(tenant::TENANT_LABEL).is_none());
    }

    #[tokio::test]
    async fn cluster_labels_deny_override_returns_consistency_error() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/labels".to_string(),
            headers: HashMap::from([(
                READ_PARTIAL_RESPONSE_OVERRIDE_HEADER.to_string(),
                "deny".to_string(),
            )]),
            body: Vec::new(),
        };
        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&cluster_context.runtime.internal_api),
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 503);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("read_consistency_unmet")
        );
    }

    #[tokio::test]
    async fn cluster_labels_rejects_allow_override_when_global_policy_is_deny() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            read_partial_response: ClusterReadPartialResponsePolicy::Deny,
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |_| {});

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/labels".to_string(),
            headers: HashMap::from([(
                READ_PARTIAL_RESPONSE_OVERRIDE_HEADER.to_string(),
                "allow".to_string(),
            )]),
            body: Vec::new(),
        };
        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&cluster_context.runtime.internal_api),
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 422);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "bad_data");
        assert!(body["error"]
            .as_str()
            .is_some_and(|message| message.contains(READ_PARTIAL_RESPONSE_OVERRIDE_HEADER)));
    }

    #[tokio::test]
    async fn cluster_labels_strict_mode_returns_strict_consistency_error_code() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec!["node-b@127.0.0.1:9302".to_string()],
            read_consistency: ClusterReadConsistency::Strict,
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |_| {});

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/labels".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            Some(&cluster_context.runtime.internal_api),
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 409);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("strict_consistency_unmet")
        );
    }

    #[test]
    fn fanout_error_response_maps_retryable_resource_limit_to_429_with_retry_after() {
        let response = fanout_error_response(ReadFanoutError::ResourceLimitExceeded {
            resource: "global_inflight_queries",
            requested: 1,
            limit: 1,
            retryable: true,
        });
        assert_eq!(response.status, 429);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("read_overloaded")
        );
        assert_eq!(response_header(&response, "Retry-After"), Some("1"));
    }

    #[test]
    fn fanout_error_response_maps_non_retryable_resource_limit_to_413() {
        let response = fanout_error_response(ReadFanoutError::ResourceLimitExceeded {
            resource: "global_inflight_merged_points",
            requested: 1024,
            limit: 512,
            retryable: false,
        });
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("read_resource_limit_exceeded")
        );
        assert_eq!(response_header(&response, "Retry-After"), None);
    }

    #[test]
    fn write_admission_error_response_maps_retryable_resource_limit_to_429_with_retry_after() {
        let response = write_admission_error_response(WriteAdmissionError::ResourceLimitExceeded {
            resource: "global_inflight_write_requests",
            requested: 1,
            limit: 1,
            retryable: true,
        });
        assert_eq!(response.status, 429);
        assert_eq!(
            response_header(&response, WRITE_ERROR_CODE_HEADER),
            Some("write_overloaded")
        );
        assert_eq!(response_header(&response, "Retry-After"), Some("1"));
    }

    #[test]
    fn write_admission_error_response_maps_non_retryable_resource_limit_to_413() {
        let response = write_admission_error_response(WriteAdmissionError::ResourceLimitExceeded {
            resource: "global_inflight_write_rows",
            requested: 1024,
            limit: 512,
            retryable: false,
        });
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, WRITE_ERROR_CODE_HEADER),
            Some("write_resource_limit_exceeded")
        );
        assert_eq!(response_header(&response, "Retry-After"), None);
    }

    #[test]
    fn storage_write_error_response_maps_out_of_retention_to_422() {
        let response =
            storage_write_error_response("insert", &TsinkError::OutOfRetention { timestamp: 123 });
        assert_eq!(response.status, 422);
        assert_eq!(
            response_header(&response, WRITE_ERROR_CODE_HEADER),
            Some("write_out_of_retention")
        );
        let body = String::from_utf8(response.body).expect("response body should decode");
        assert!(body.contains("outside the retention window"));
    }

    #[test]
    fn read_admission_error_response_maps_retryable_resource_limit_to_429_with_retry_after() {
        let response = read_admission_error_response(ReadAdmissionError::ResourceLimitExceeded {
            resource: "global_inflight_read_requests",
            requested: 1,
            limit: 1,
            retryable: true,
        });
        assert_eq!(response.status, 429);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("read_overloaded")
        );
        assert_eq!(response_header(&response, "Retry-After"), Some("1"));
    }

    #[test]
    fn read_admission_error_response_maps_non_retryable_resource_limit_to_413() {
        let response = read_admission_error_response(ReadAdmissionError::ResourceLimitExceeded {
            resource: "global_inflight_read_queries",
            requested: 2,
            limit: 1,
            retryable: false,
        });
        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, READ_ERROR_CODE_HEADER),
            Some("read_resource_limit_exceeded")
        );
        assert_eq!(response_header(&response, "Retry-After"), None);
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
        assert!(body.contains("tsink_memory_excluded_bytes"));
        assert!(body.contains("tsink_memory_persisted_mmap_bytes"));
        assert!(body.contains("tsink_memory_registry_bytes"));
        assert!(body.contains("tsink_series_total"));
        assert!(body.contains("tsink_uptime_seconds"));
        assert!(body.contains("tsink_wal_enabled"));
        assert!(body.contains("tsink_wal_acknowledged_writes_durable"));
        assert!(body.contains("tsink_wal_durable_highwater_segment"));
        assert!(body.contains("tsink_flush_pipeline_runs_total"));
        assert!(body.contains("tsink_flush_hot_segments_visible"));
        assert!(body.contains("tsink_compaction_runs_total"));
        assert!(body.contains("tsink_query_select_calls_total"));
        assert!(body.contains("tsink_query_hot_only_plans_total"));
        assert!(body.contains("tsink_remote_storage_catalog_refreshes_total"));
        assert!(body.contains("tsink_remote_storage_catalog_refresh_consecutive_failures"));
        assert!(body.contains("tsink_remote_storage_catalog_refresh_backoff_active"));
        assert!(body.contains("tsink_remote_storage_accessible"));
        assert!(body.contains("tsink_rollup_worker_runs_total"));
        assert!(body.contains("tsink_query_rollup_plans_total"));
        assert!(body.contains("tsink_cluster_write_shard_rows_total"));
        assert!(body.contains("tsink_cluster_write_remote_request_duration_seconds"));
        assert!(body.contains("tsink_cluster_dedupe_requests_total"));
        assert!(body.contains("tsink_cluster_dedupe_active_keys"));
        assert!(body.contains("tsink_cluster_outbox_enqueued_total"));
        assert!(body.contains("tsink_cluster_outbox_queued_entries"));
        assert!(body.contains("tsink_cluster_outbox_stale_records"));
        assert!(body.contains("tsink_cluster_outbox_cleanup_runs_total"));
        assert!(body.contains("tsink_cluster_outbox_cleanup_compactions_total"));
        assert!(body.contains("tsink_cluster_outbox_cleanup_reclaimed_bytes_total"));
        assert!(body.contains("tsink_cluster_outbox_stalled_alerts_total"));
        assert!(body.contains("tsink_cluster_outbox_stalled_peers"));
        assert!(body.contains("tsink_cluster_outbox_peer_stalled"));
        assert!(body.contains("tsink_cluster_fanout_requests_total"));
        assert!(body.contains("tsink_cluster_fanout_remote_request_duration_seconds"));
        assert!(body.contains("tsink_cluster_fanout_resource_rejections_total"));
        assert!(body.contains("tsink_cluster_fanout_resource_active_queries"));
        assert!(body.contains("tsink_read_admission_rejections_total"));
        assert!(body.contains("tsink_read_admission_active_queries"));
        assert!(body.contains("tsink_write_admission_rejections_total"));
        assert!(body.contains("tsink_write_admission_active_rows"));
        assert!(body.contains("tsink_tenant_admission_write_rejections_total"));
        assert!(body.contains("tsink_tenant_admission_active_reads"));
        assert!(body.contains("tsink_prometheus_payload_feature_enabled"));
        assert!(body.contains("tsink_prometheus_payload_accepted_total"));
        assert!(body.contains("tsink_prometheus_payload_rejected_total"));
        assert!(body.contains("tsink_prometheus_payload_throttled_total"));
        assert!(body.contains("tsink_prometheus_payload_capability_required"));
        assert!(body.contains("tsink_otlp_metrics_enabled"));
        assert!(body.contains("tsink_otlp_requests_total"));
        assert!(body.contains("tsink_otlp_data_points_total"));
        assert!(body.contains("tsink_otlp_supported_shape"));
        assert!(body.contains("tsink_legacy_ingest_enabled"));
        assert!(body.contains("tsink_legacy_ingest_requests_total"));
        assert!(body.contains("tsink_legacy_ingest_samples_total"));
        assert!(body.contains("tsink_legacy_ingest_limits"));
        assert!(body.contains("tsink_cluster_capability_enabled"));
        assert!(body.contains("tsink_cluster_read_planner_requests_total"));
        assert!(body.contains("tsink_cluster_read_planner_operation_requests_total"));
        assert!(body.contains("tsink_cluster_control_current_term"));
        assert!(body.contains("tsink_cluster_control_peer_status"));
        assert!(body.contains("tsink_cluster_handoff_total"));
        assert!(body.contains("tsink_cluster_handoff_shard_phase"));
        assert!(body.contains("tsink_cluster_repair_digest_runs_total"));
        assert!(body.contains("tsink_cluster_repair_digest_mismatches_total"));
        assert!(body.contains("tsink_cluster_repair_digest_bytes_exchanged_total"));
        assert!(body.contains("tsink_cluster_repair_digest_budget_exhaustions_total"));
        assert!(body.contains("tsink_cluster_repair_digest_windows_skipped_budget_total"));
        assert!(body.contains("tsink_cluster_repair_digest_config_max_bytes_per_tick"));
        assert!(body.contains("tsink_cluster_repair_digest_prioritized_shards_last_run"));
        assert!(body.contains("tsink_cluster_repair_digest_mismatch_report"));
        assert!(body.contains("tsink_cluster_repair_backfill_attempts_total"));
        assert!(body.contains("tsink_cluster_repair_backfill_rows_inserted_total"));
        assert!(body.contains("tsink_cluster_repair_config_max_rows_per_tick"));
        assert!(body.contains("tsink_cluster_repair_backfill_cancelled_total"));
        assert!(body.contains("tsink_cluster_repair_backfill_skipped_backoff_total"));
        assert!(body.contains("tsink_cluster_repair_backfill_skipped_paused_total"));
        assert!(body.contains("tsink_cluster_repair_backfill_skipped_time_budget_total"));
        assert!(body.contains("tsink_cluster_repair_backfill_time_budget_exhaustions_total"));
        assert!(body.contains("tsink_cluster_repair_backfill_skipped_backoff_last_run"));
        assert!(body.contains("tsink_cluster_repair_backfill_time_budget_exhausted_last_run"));
        assert!(body.contains("tsink_cluster_repair_config_max_runtime_ms_per_tick"));
        assert!(body.contains("tsink_cluster_repair_config_failure_backoff_seconds"));
        assert!(body.contains("tsink_cluster_repair_control_paused"));
        assert!(body.contains("tsink_cluster_rebalance_runs_total"));
        assert!(body.contains("tsink_cluster_rebalance_rows_scheduled_total"));
        assert!(body.contains("tsink_cluster_rebalance_control_paused"));
        assert!(body.contains("tsink_cluster_rebalance_config_max_rows_per_tick"));
        assert!(body.contains("tsink_cluster_rebalance_job_phase"));
        assert!(body.contains("tsink_cluster_rebalance_moves_blocked_by_slo_total"));
        assert!(body.contains("tsink_cluster_rebalance_slo_guard_block_new_handoffs"));
        assert!(body.contains("tsink_cluster_hotspot_skewed_shards"));
        assert!(body.contains("tsink_cluster_hotspot_shard_pressure_score"));
    }

    #[tokio::test]
    async fn admin_rules_endpoints_apply_run_and_status() {
        let storage = make_storage();
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let rules_runtime = make_rules_runtime(&storage);
        let engine = make_engine(&storage);
        let now = unix_timestamp_millis() as i64;
        let seed_timestamp = now.saturating_sub(1_000);

        tenant::scoped_storage(Arc::clone(&storage), "team-a")
            .insert_rows(&[Row::with_labels(
                "source_metric",
                vec![Label::new("host", "a")],
                DataPoint::new(seed_timestamp, 3.0),
            )])
            .expect("seed write should succeed");

        let apply_response = handle_request_with_admin_and_cluster_and_tenant_and_metadata(
            &storage,
            &metadata_store,
            &exemplar_store,
            Some(rules_runtime.as_ref()),
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/admin/rules/apply".to_string(),
                headers: HashMap::from([(
                    "content-type".to_string(),
                    "application/json".to_string(),
                )]),
                body: serde_json::to_vec(&json!({
                    "groups": [{
                        "name": "recording",
                        "tenantId": "team-a",
                        "interval": "1s",
                        "rules": [{
                            "kind": "recording",
                            "record": "recorded_metric",
                            "expr": "source_metric{host=\"a\"}"
                        }]
                    }]
                }))
                .expect("json should encode"),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(apply_response.status, 200);

        let status_response = handle_request_with_admin_and_cluster_and_tenant_and_metadata(
            &storage,
            &metadata_store,
            &exemplar_store,
            Some(rules_runtime.as_ref()),
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/admin/rules/status".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(status_response.status, 200);
        let status_body: JsonValue =
            serde_json::from_slice(&status_response.body).expect("status body should decode");
        assert_eq!(status_body["data"]["metrics"]["configuredGroups"], 1);
        assert_eq!(
            status_body["data"]["groups"][0]["rules"][0]["name"],
            "recorded_metric"
        );

        let run_response = handle_request_with_admin_and_cluster_and_tenant_and_metadata(
            &storage,
            &metadata_store,
            &exemplar_store,
            Some(rules_runtime.as_ref()),
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/admin/rules/run".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(run_response.status, 200);

        let points = tenant::scoped_storage(Arc::clone(&storage), "team-a")
            .select(
                "recorded_metric",
                &[Label::new("host", "a")],
                seed_timestamp.saturating_sub(120_000),
                now.saturating_add(120_000),
            )
            .expect("recorded metric should be queryable");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value_as_f64(), Some(3.0));
    }

    #[tokio::test]
    async fn metrics_endpoint_includes_rules_metrics_when_runtime_is_present() {
        let storage = make_storage();
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let rules_runtime = make_rules_runtime(&storage);
        let engine = make_engine(&storage);
        rules_runtime
            .apply_groups(vec![RuleGroupSpec {
                name: "alerts".to_string(),
                tenant_id: "team-a".to_string(),
                interval_secs: 60,
                labels: BTreeMap::new(),
                rules: vec![RuleSpec::Alert(AlertRuleSpec {
                    alert: "AlwaysOn".to_string(),
                    expr: "1".to_string(),
                    interval_secs: None,
                    for_secs: 0,
                    labels: BTreeMap::new(),
                    annotations: BTreeMap::new(),
                })],
            }])
            .expect("rules should apply");

        let response = handle_request_with_admin_and_cluster_and_tenant_and_metadata(
            &storage,
            &metadata_store,
            &exemplar_store,
            Some(rules_runtime.as_ref()),
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/metrics".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(response.status, 200);
        let body = std::str::from_utf8(&response.body).expect("metrics should decode");
        assert!(body.contains("tsink_rules_scheduler_runs_total"));
        assert!(body.contains("tsink_rules_configured{kind=\"rules\"} 1"));
        assert!(body.contains("tsink_rules_runtime_limits{kind=\"scheduler_tick_ms\"}"));
    }

    #[tokio::test]
    async fn admin_endpoints_disabled_by_default() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/snapshot".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({"path": "/tmp/tsink-snapshot"}))
                .expect("json should encode"),
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
    async fn admin_cluster_membership_endpoints_disabled_by_default() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/join".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "nodeId": "node-b",
                "endpoint": "127.0.0.1:9302"
            }))
            .expect("json should encode"),
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
    async fn admin_cluster_handoff_endpoints_disabled_by_default() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/handoff/begin".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "shard": 0,
                "fromNodeId": "node-a",
                "toNodeId": "node-b"
            }))
            .expect("json should encode"),
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
    async fn admin_cluster_repair_endpoints_disabled_by_default() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/repair/pause".to_string(),
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
    async fn admin_cluster_rebalance_endpoints_disabled_by_default() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/cluster/rebalance/status".to_string(),
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
    async fn admin_cluster_snapshot_endpoints_disabled_by_default() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/snapshot".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "path": "/tmp/cluster-dr"
            }))
            .expect("json should encode"),
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
    async fn admin_cluster_join_requires_control_runtime() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/join".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "nodeId": "node-b",
                "endpoint": "127.0.0.1:9302"
            }))
            .expect("json should encode"),
        };

        let response = handle_request_with_admin(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(response.status, 503);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "control_plane_unavailable");
    }

    #[tokio::test]
    async fn admin_cluster_handoff_begin_requires_control_runtime() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/handoff/begin".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "shard": 0,
                "fromNodeId": "node-a",
                "toNodeId": "node-b"
            }))
            .expect("json should encode"),
        };

        let response = handle_request_with_admin(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(response.status, 503);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "control_plane_unavailable");
    }

    #[tokio::test]
    async fn admin_cluster_repair_pause_requires_digest_runtime() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_single_node_control(&temp_dir);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/repair/pause".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 503);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "repair_runtime_unavailable");
    }

    #[tokio::test]
    async fn admin_cluster_rebalance_pause_requires_digest_runtime() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_single_node_control(&temp_dir);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/rebalance/pause".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 503);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "rebalance_runtime_unavailable");
    }

    #[tokio::test]
    async fn admin_cluster_control_snapshot_requires_control_runtime() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let snapshot_path = temp_dir.path().join("control-recovery.json");

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/control/snapshot".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "path": snapshot_path.to_string_lossy()
            }))
            .expect("json should encode"),
        };

        let response = handle_request_with_admin(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(response.status, 503);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "control_plane_unavailable");
    }

    #[tokio::test]
    async fn admin_cluster_snapshot_requires_control_runtime() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let snapshot_root = temp_dir.path().join("cluster-dr");

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/snapshot".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "path": snapshot_root.to_string_lossy()
            }))
            .expect("json should encode"),
        };

        let response = handle_request_with_admin(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(response.status, 503);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "control_plane_unavailable");
    }

    #[tokio::test]
    async fn admin_cluster_control_snapshot_and_restore_roundtrip() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_single_node_control_state(&temp_dir, |state| {
            state.leader_node_id = Some("node-a".to_string());
        });
        let snapshot_path = temp_dir.path().join("control-recovery.json");

        let initial_state = cluster_context
            .control_consensus
            .as_ref()
            .expect("control consensus should be present")
            .current_state();

        let snapshot_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/control/snapshot".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "path": snapshot_path.to_string_lossy()
            }))
            .expect("json should encode"),
        };
        let snapshot_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            snapshot_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(snapshot_response.status, 200);
        assert!(snapshot_path.exists());

        let join_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/join".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "nodeId": "node-b",
                "endpoint": "127.0.0.1:9302"
            }))
            .expect("json should encode"),
        };
        let join_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            join_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(join_response.status, 200);
        assert!(cluster_context
            .control_consensus
            .as_ref()
            .expect("control consensus should be present")
            .current_state()
            .node_record("node-b")
            .is_some());

        let restore_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/control/restore".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "snapshotPath": snapshot_path.to_string_lossy()
            }))
            .expect("json should encode"),
        };
        let restore_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            restore_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(restore_response.status, 200);

        let restored_state = cluster_context
            .control_consensus
            .as_ref()
            .expect("control consensus should be present")
            .current_state();
        assert_eq!(restored_state, initial_state);
        assert!(restored_state.node_record("node-b").is_none());
    }

    #[tokio::test]
    async fn admin_cluster_control_restore_can_force_local_leader() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control_state(&temp_dir, |state| {
            state.leader_node_id = Some("node-b".to_string());
        });
        let snapshot_path = temp_dir.path().join("control-recovery-force-leader.json");

        let snapshot_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/control/snapshot".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "path": snapshot_path.to_string_lossy()
            }))
            .expect("json should encode"),
        };
        let snapshot_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            snapshot_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(snapshot_response.status, 200);

        let restore_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/control/restore".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "snapshotPath": snapshot_path.to_string_lossy(),
                "forceLocalLeader": true
            }))
            .expect("json should encode"),
        };
        let restore_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            restore_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(restore_response.status, 200);
        let restore_body: JsonValue =
            serde_json::from_slice(&restore_response.body).expect("valid JSON");
        assert_eq!(restore_body["status"], "success");
        assert_eq!(restore_body["data"]["leaderNodeId"], "node-a");
        assert_eq!(
            cluster_context
                .control_consensus
                .as_ref()
                .expect("control consensus should be present")
                .current_state()
                .leader_node_id
                .as_deref(),
            Some("node-a")
        );
    }

    #[tokio::test]
    async fn admin_cluster_snapshot_and_restore_roundtrip_serves_distributed_query() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let source_local_path = temp_dir.path().join("source-local");
        let source_remote_path = temp_dir.path().join("source-remote");
        let snapshot_root = temp_dir.path().join("cluster-snapshot");
        let restore_root = temp_dir.path().join("cluster-restored");

        let local_storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_data_path(&source_local_path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("local source storage should build");
        let remote_storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_data_path(&source_remote_path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("remote source storage should build");

        let (remote_endpoint, _request_count, remote_shutdown_tx, remote_server) =
            spawn_internal_storage_peer(Arc::clone(&remote_storage)).await;

        let config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![format!("node-b@{remote_endpoint}")],
            ..ClusterConfig::default()
        };
        let cluster_context =
            cluster_context_with_control_state_for_config(&temp_dir, config, |state| {
                state.leader_node_id = Some("node-a".to_string());
            });
        let local_node_id = cluster_context.runtime.membership.local_node_id.clone();
        let remote_node_id = cluster_context
            .runtime
            .membership
            .nodes
            .iter()
            .find(|node| node.id != local_node_id)
            .expect("test membership should include a remote node")
            .id
            .clone();
        let find_owned_labels = |owner: &str, instance: &str, zone_prefix: &str| {
            for idx in 0..20_000u32 {
                let labels = vec![
                    Label::new("instance", instance),
                    Label::new("zone", format!("{zone_prefix}-{idx}")),
                ];
                let mapped_owner = cluster_context
                    .write_router
                    .owner_for_series("dr_restore_metric", &labels)
                    .expect("owner lookup should succeed");
                if mapped_owner == owner {
                    return labels;
                }
            }
            panic!("failed to find owned labels for '{owner}'");
        };
        let local_labels =
            find_owned_labels(local_node_id.as_str(), local_node_id.as_str(), "local");
        let remote_labels =
            find_owned_labels(remote_node_id.as_str(), remote_node_id.as_str(), "remote");
        local_storage
            .insert_rows(&[Row::with_labels(
                "dr_restore_metric",
                local_labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .expect("local insert should succeed");
        remote_storage
            .insert_rows(&[Row::with_labels(
                "dr_restore_metric",
                remote_labels.clone(),
                DataPoint::new(2, 2.0),
            )])
            .expect("remote insert should succeed");
        let engine = make_engine(&local_storage);

        let snapshot_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/snapshot".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "path": snapshot_root.to_string_lossy()
            }))
            .expect("json should encode"),
        };
        let snapshot_response = handle_request_with_admin_and_cluster(
            &local_storage,
            &engine,
            snapshot_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(snapshot_response.status, 200);
        let snapshot_body: JsonValue =
            serde_json::from_slice(&snapshot_response.body).expect("valid JSON");
        assert_eq!(snapshot_body["status"], "success");
        assert_eq!(
            snapshot_body["data"]["clusterNodes"]
                .as_array()
                .map(|nodes| nodes.len()),
            Some(2)
        );
        let manifest_path = snapshot_body["data"]["manifestPath"]
            .as_str()
            .expect("manifestPath should be present");
        assert!(Path::new(manifest_path).exists());

        let restore_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/restore".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "snapshotPath": manifest_path,
                "restoreRoot": restore_root.to_string_lossy(),
                "forceLocalLeader": true
            }))
            .expect("json should encode"),
        };
        let restore_response = handle_request_with_admin_and_cluster(
            &local_storage,
            &engine,
            restore_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(restore_response.status, 200);
        let restore_body: JsonValue =
            serde_json::from_slice(&restore_response.body).expect("valid JSON");
        assert_eq!(restore_body["status"], "success");
        assert!(restore_body["data"]["rpoEstimateMs"].is_number());
        assert!(restore_body["data"]["rtoMs"].is_number());
        let report_path = restore_body["data"]["reportPath"]
            .as_str()
            .expect("reportPath should be present");
        assert!(Path::new(report_path).exists());

        let restored_remote_path = restore_root.join("nodes").join("node-b").join("data");
        let restored_local_path = restore_root.join("nodes").join("node-a").join("data");
        let restored_local_storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_data_path(&restored_local_path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("restored local storage should build");
        let restored_remote_storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_data_path(&restored_remote_path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("restored remote storage should build");
        assert_eq!(
            restored_local_storage
                .select("dr_restore_metric", &local_labels, 0, 10)
                .expect("restored local select should succeed"),
            vec![DataPoint::new(1, 1.0)]
        );
        assert_eq!(
            restored_remote_storage
                .select("dr_restore_metric", &remote_labels, 0, 10)
                .expect("restored remote select should succeed"),
            vec![DataPoint::new(2, 2.0)]
        );
        let (
            restored_remote_endpoint,
            _restored_request_count,
            restored_shutdown_tx,
            restored_remote_server,
        ) = spawn_internal_storage_peer(Arc::clone(&restored_remote_storage)).await;
        let restored_cluster_context = cluster_context_with_control_state_for_config(
            &temp_dir,
            ClusterConfig {
                enabled: true,
                node_id: Some("node-a".to_string()),
                bind: Some("127.0.0.1:9301".to_string()),
                seeds: vec![format!("node-b@{restored_remote_endpoint}")],
                ..ClusterConfig::default()
            },
            |state| {
                state.leader_node_id = Some("node-a".to_string());
            },
        );

        let restored_engine = make_engine(&restored_local_storage);
        let label_values_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/label/instance/values".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let label_values_response = handle_request_with_admin_and_cluster(
            &restored_local_storage,
            &restored_engine,
            label_values_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(restored_cluster_context.as_ref()),
        )
        .await;
        let label_values = parse_success_response_data(&label_values_response);
        assert_eq!(label_values, json!(["node-a", "node-b"]));

        let _ = restored_shutdown_tx.send(());
        restored_remote_server
            .await
            .expect("restored remote server should stop cleanly");
        let _ = remote_shutdown_tx.send(());
        remote_server
            .await
            .expect("remote server should stop cleanly");
        restored_local_storage
            .close()
            .expect("restored local storage should close");
        restored_remote_storage
            .close()
            .expect("restored remote storage should close");
        local_storage
            .close()
            .expect("local source storage should close");
        remote_storage
            .close()
            .expect("remote source storage should close");
    }

    #[tokio::test]
    async fn admin_cluster_join_rejects_when_local_node_is_not_control_leader() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/join".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "nodeId": "node-c",
                "endpoint": "127.0.0.1:9303"
            }))
            .expect("json should encode"),
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 409);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "not_control_leader");
    }

    #[tokio::test]
    async fn admin_cluster_handoff_begin_rejects_when_local_node_is_not_control_leader() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control(&temp_dir);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/handoff/begin".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "shard": 0,
                "fromNodeId": "node-a",
                "toNodeId": "node-b"
            }))
            .expect("json should encode"),
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 409);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "not_control_leader");
    }

    #[tokio::test]
    async fn admin_cluster_join_reports_pending_quorum_progress() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control_state(&temp_dir, |state| {
            state.leader_node_id = Some("node-a".to_string());
        });

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/join".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "nodeId": "node-c",
                "endpoint": "127.0.0.1:9303"
            }))
            .expect("json should encode"),
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 202);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["operation"], "join");
        assert_eq!(body["data"]["result"], "pending");
        assert_eq!(body["data"]["requiredAcks"], 2);
        assert_eq!(body["data"]["acknowledgedAcks"], 1);
    }

    #[tokio::test]
    async fn admin_cluster_handoff_begin_reports_pending_quorum_progress() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_control_state(&temp_dir, |state| {
            state.leader_node_id = Some("node-a".to_string());
        });
        let control_state = cluster_context
            .control_consensus
            .as_ref()
            .expect("control consensus should be present")
            .current_state();
        let (shard, from_node_id) = control_state
            .ring
            .assignments
            .iter()
            .enumerate()
            .find_map(|(index, owners)| owners.first().cloned().map(|owner| (index as u32, owner)))
            .expect("test control ring should include at least one owner");
        let to_node_id = control_state
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .find(|node_id| *node_id != from_node_id.as_str())
            .expect("test control state should include a secondary node")
            .to_string();
        let activation_ring_version = control_state.ring_version.saturating_add(1);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/handoff/begin".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "shard": shard,
                "fromNodeId": from_node_id,
                "toNodeId": to_node_id,
                "activationRingVersion": activation_ring_version
            }))
            .expect("json should encode"),
        };

        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 202);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["operation"], "begin_handoff");
        assert_eq!(body["data"]["result"], "pending");
        assert_eq!(body["data"]["requiredAcks"], 2);
        assert_eq!(body["data"]["acknowledgedAcks"], 1);
    }

    #[tokio::test]
    async fn admin_cluster_handoff_mutation_lifecycle_commits_and_is_idempotent() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_single_node_control_state(&temp_dir, |state| {
            state
                .nodes
                .push(crate::cluster::control::ControlNodeRecord {
                    id: "node-b".to_string(),
                    endpoint: "127.0.0.1:9302".to_string(),
                    membership_generation: 2,
                    status: ControlNodeStatus::Joining,
                });
            state.nodes.sort_by(|left, right| left.id.cmp(&right.id));
        });

        let begin_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/handoff/begin".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "shard": 0,
                "fromNodeId": "node-a",
                "toNodeId": "node-b",
                "activationRingVersion": 2
            }))
            .expect("json should encode"),
        };
        let begin_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            begin_request.clone(),
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(begin_response.status, 200);
        let begin_body: JsonValue =
            serde_json::from_slice(&begin_response.body).expect("valid JSON");
        assert_eq!(begin_body["data"]["operation"], "begin_handoff");
        assert_eq!(begin_body["data"]["result"], "committed");
        assert_eq!(begin_body["data"]["phase"], "warmup");
        assert_eq!(begin_body["data"]["activationRingVersion"], 2);
        assert_eq!(begin_body["data"]["ringVersion"], 1);

        let begin_noop_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            begin_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(begin_noop_response.status, 200);
        let begin_noop_body: JsonValue =
            serde_json::from_slice(&begin_noop_response.body).expect("valid JSON");
        assert_eq!(begin_noop_body["data"]["result"], "noop");

        let progress_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/handoff/progress".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "shard": 0,
                "phase": "cutover",
                "copiedRows": 250,
                "pendingRows": 16
            }))
            .expect("json should encode"),
        };
        let progress_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            progress_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(progress_response.status, 200);
        let progress_body: JsonValue =
            serde_json::from_slice(&progress_response.body).expect("valid JSON");
        assert_eq!(progress_body["data"]["operation"], "update_handoff");
        assert_eq!(progress_body["data"]["phase"], "cutover");
        assert_eq!(progress_body["data"]["copiedRows"], 250);
        assert_eq!(progress_body["data"]["pendingRows"], 16);
        assert_eq!(progress_body["data"]["ringVersion"], 2);

        let complete_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/handoff/complete".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "shard": 0
            }))
            .expect("json should encode"),
        };
        let complete_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            complete_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(complete_response.status, 409);
        let complete_body: JsonValue =
            serde_json::from_slice(&complete_response.body).expect("valid JSON");
        assert_eq!(complete_body["status"], "error");
        assert_eq!(complete_body["errorType"], "invalid_handoff_mutation");

        let final_sync_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/handoff/progress".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "shard": 0,
                "phase": "final_sync",
                "copiedRows": 275,
                "pendingRows": 3
            }))
            .expect("json should encode"),
        };
        let final_sync_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            final_sync_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(final_sync_response.status, 200);

        let complete_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/handoff/complete".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "shard": 0
            }))
            .expect("json should encode"),
        };
        let complete_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            complete_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(complete_response.status, 200);
        let complete_body: JsonValue =
            serde_json::from_slice(&complete_response.body).expect("valid JSON");
        assert_eq!(complete_body["data"]["operation"], "complete_handoff");
        assert_eq!(complete_body["data"]["result"], "committed");
        assert_eq!(complete_body["data"]["phase"], "completed");
        assert_eq!(complete_body["data"]["pendingRows"], 0);
        assert_eq!(complete_body["data"]["ringVersion"], 2);

        let state = cluster_context
            .control_consensus
            .as_ref()
            .expect("control consensus should be present")
            .current_state();
        let shard = state
            .handoff_snapshot()
            .shards
            .into_iter()
            .find(|entry| entry.shard == 0)
            .expect("handoff shard should be present");
        assert_eq!(state.ring_version, 2);
        assert_eq!(shard.phase, ShardHandoffPhase::Completed);
        assert_eq!(shard.copied_rows, 275);
        assert_eq!(shard.pending_rows, 0);
    }

    #[tokio::test]
    async fn admin_cluster_repair_pause_resume_and_cancel_controls_runtime_state() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context =
            cluster_context_with_single_node_control_and_digest_runtime(&temp_dir);

        let pause_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/repair/pause".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let pause_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            pause_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(pause_response.status, 200);
        let pause_body: JsonValue =
            serde_json::from_slice(&pause_response.body).expect("valid JSON");
        assert_eq!(pause_body["status"], "success");
        assert_eq!(pause_body["data"]["operation"], "pause_repair");
        assert_eq!(pause_body["data"]["repairPaused"], true);

        let cancel_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/repair/cancel".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let cancel_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            cancel_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(cancel_response.status, 200);
        let cancel_body: JsonValue =
            serde_json::from_slice(&cancel_response.body).expect("valid JSON");
        assert_eq!(cancel_body["data"]["operation"], "cancel_repair");
        assert!(cancel_body["data"]["repairCancelGeneration"]
            .as_u64()
            .is_some_and(|value| value >= 1));

        let resume_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/repair/resume".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let resume_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            resume_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(resume_response.status, 200);
        let resume_body: JsonValue =
            serde_json::from_slice(&resume_response.body).expect("valid JSON");
        assert_eq!(resume_body["data"]["operation"], "resume_repair");
        assert_eq!(resume_body["data"]["repairPaused"], false);

        let digest_snapshot = cluster_context
            .digest_runtime
            .as_ref()
            .expect("digest runtime should exist")
            .snapshot();
        assert!(!digest_snapshot.repair_paused);
        assert!(digest_snapshot.repair_cancel_generation >= 1);
        assert!(digest_snapshot.repair_cancellations_total >= 1);
    }

    #[tokio::test]
    async fn admin_cluster_handoff_status_returns_progress_and_error_summary_fields() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_single_node_control_state(&temp_dir, |state| {
            state.leader_node_id = Some("node-a".to_string());
        });

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/cluster/handoff/status".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 200);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["operation"], "handoff_status");
        assert!(body["data"]["inProgressShards"].is_number());
        assert!(body["data"]["estimatedEtaSeconds"].is_number());
        assert!(body["data"]["errorSummary"]["jobsWithErrors"].is_number());
        assert!(body["data"]["jobs"].is_array());
    }

    #[tokio::test]
    async fn admin_cluster_repair_status_and_run_include_progress_and_eta_fields() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context =
            cluster_context_with_single_node_control_and_digest_runtime(&temp_dir);

        let status_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/cluster/repair/status".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let status_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            status_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(status_response.status, 200);
        let status_body: JsonValue =
            serde_json::from_slice(&status_response.body).expect("valid JSON");
        assert_eq!(status_body["status"], "success");
        assert_eq!(status_body["data"]["operation"], "repair_status");
        assert!(status_body["data"]["repairRunInFlight"].is_boolean());
        assert!(status_body["data"]["progressPercent"].is_number());
        assert!(status_body["data"]["estimatedEtaSeconds"].is_number());
        assert!(status_body["data"]["errorSummary"]["mismatchBacklog"].is_array());

        let run_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/repair/run".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let run_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            run_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(run_response.status, 200);
        let run_body: JsonValue = serde_json::from_slice(&run_response.body).expect("valid JSON");
        assert_eq!(run_body["data"]["operation"], "run_repair");
        assert!(run_body["data"]["progressPercent"].is_number());
        assert!(run_body["data"]["estimatedEtaSeconds"].is_number());
    }

    #[tokio::test]
    async fn admin_cluster_repair_run_rejects_when_run_is_already_in_progress() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context =
            cluster_context_with_single_node_control_and_digest_runtime(&temp_dir);
        cluster_context
            .digest_runtime
            .as_ref()
            .expect("digest runtime should exist")
            .set_repair_run_inflight_for_test(true);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/repair/run".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 409);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "repair_run_in_progress");
    }

    #[tokio::test]
    async fn admin_cluster_rebalance_pause_resume_and_status_controls_runtime_state() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context =
            cluster_context_with_single_node_control_and_digest_runtime(&temp_dir);

        let pause_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/rebalance/pause".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let pause_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            pause_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(pause_response.status, 200);
        let pause_body: JsonValue =
            serde_json::from_slice(&pause_response.body).expect("valid JSON");
        assert_eq!(pause_body["data"]["operation"], "pause_rebalance");
        assert_eq!(pause_body["data"]["rebalancePaused"], true);

        let status_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/cluster/rebalance/status".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let status_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            status_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(status_response.status, 200);
        let status_body: JsonValue =
            serde_json::from_slice(&status_response.body).expect("valid JSON");
        assert_eq!(status_body["data"]["operation"], "rebalance_status");
        assert_eq!(status_body["data"]["rebalancePaused"], true);
        assert!(status_body["data"]["rebalanceRunInFlight"].is_boolean());
        assert!(status_body["data"]["progressPercent"].is_number());
        assert!(status_body["data"]["estimatedEtaSeconds"].is_number());
        assert!(status_body["data"]["runsTotal"].is_number());
        assert!(status_body["data"]["effectiveMaxRowsPerTickLastRun"].is_number());
        assert!(status_body["data"]["movesBlockedBySloTotal"].is_number());
        assert!(status_body["data"]["sloGuard"]["writePressureRatio"].is_number());
        assert!(status_body["data"]["sloGuard"]["blockNewHandoffs"].is_boolean());
        assert!(status_body["data"]["candidateMoves"].is_array());
        assert!(status_body["data"]["hotspot"]["hotShards"].is_array());
        assert!(status_body["data"]["hotspot"]["tenantHotspots"].is_array());
        assert!(status_body["data"]["jobs"].is_array());

        let resume_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/rebalance/resume".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let resume_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            resume_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(resume_response.status, 200);
        let resume_body: JsonValue =
            serde_json::from_slice(&resume_response.body).expect("valid JSON");
        assert_eq!(resume_body["data"]["operation"], "resume_rebalance");
        assert_eq!(resume_body["data"]["rebalancePaused"], false);

        let snapshot = cluster_context
            .digest_runtime
            .as_ref()
            .expect("digest runtime should exist")
            .rebalance_snapshot();
        assert!(!snapshot.paused);
    }

    #[tokio::test]
    async fn admin_cluster_rebalance_run_rejects_when_run_is_already_in_progress() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context =
            cluster_context_with_single_node_control_and_digest_runtime(&temp_dir);
        cluster_context
            .digest_runtime
            .as_ref()
            .expect("digest runtime should exist")
            .set_rebalance_run_inflight_for_test(true);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/rebalance/run".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(response.status, 409);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "rebalance_run_in_progress");
    }

    #[tokio::test]
    async fn admin_cluster_membership_operations_are_idempotent() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_single_node_control(&temp_dir);

        let join_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/join".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "nodeId": "node-b",
                "endpoint": "127.0.0.1:9302"
            }))
            .expect("json should encode"),
        };
        let join_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            join_request.clone(),
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(join_response.status, 200);
        let join_body: JsonValue = serde_json::from_slice(&join_response.body).expect("valid JSON");
        assert_eq!(join_body["data"]["result"], "committed");
        assert_eq!(join_body["data"]["nodeStatus"], "joining");
        let join_epoch = join_body["data"]["membershipEpoch"]
            .as_u64()
            .expect("membershipEpoch should be numeric");

        let join_noop_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            join_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(join_noop_response.status, 200);
        let join_noop_body: JsonValue =
            serde_json::from_slice(&join_noop_response.body).expect("valid JSON");
        assert_eq!(join_noop_body["data"]["result"], "noop");
        assert_eq!(join_noop_body["data"]["membershipEpoch"], join_epoch);

        let leave_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/leave".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "nodeId": "node-b"
            }))
            .expect("json should encode"),
        };
        let leave_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            leave_request.clone(),
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(leave_response.status, 200);
        let leave_body: JsonValue =
            serde_json::from_slice(&leave_response.body).expect("valid JSON");
        assert_eq!(leave_body["data"]["result"], "committed");
        assert_eq!(leave_body["data"]["nodeStatus"], "leaving");
        let leave_epoch = leave_body["data"]["membershipEpoch"]
            .as_u64()
            .expect("membershipEpoch should be numeric");

        let leave_noop_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            leave_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(leave_noop_response.status, 200);
        let leave_noop_body: JsonValue =
            serde_json::from_slice(&leave_noop_response.body).expect("valid JSON");
        assert_eq!(leave_noop_body["data"]["result"], "noop");
        assert_eq!(leave_noop_body["data"]["membershipEpoch"], leave_epoch);

        let recommission_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/recommission".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "nodeId": "node-b"
            }))
            .expect("json should encode"),
        };
        let recommission_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            recommission_request.clone(),
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(recommission_response.status, 200);
        let recommission_body: JsonValue =
            serde_json::from_slice(&recommission_response.body).expect("valid JSON");
        assert_eq!(recommission_body["data"]["result"], "committed");
        assert_eq!(recommission_body["data"]["nodeStatus"], "active");
        let recommission_epoch = recommission_body["data"]["membershipEpoch"]
            .as_u64()
            .expect("membershipEpoch should be numeric");

        let recommission_noop_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            recommission_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(recommission_noop_response.status, 200);
        let recommission_noop_body: JsonValue =
            serde_json::from_slice(&recommission_noop_response.body).expect("valid JSON");
        assert_eq!(recommission_noop_body["data"]["result"], "noop");
        assert_eq!(
            recommission_noop_body["data"]["membershipEpoch"],
            recommission_epoch
        );
    }

    #[tokio::test]
    async fn admin_cluster_audit_query_and_export_capture_mutations() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir should build");
        let cluster_context = cluster_context_with_single_node_control(&temp_dir);

        let join_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/join".to_string(),
            headers: HashMap::from([
                ("content-type".to_string(), "application/json".to_string()),
                (
                    AUDIT_ACTOR_ID_HEADER.to_string(),
                    "operator-alice".to_string(),
                ),
            ]),
            body: serde_json::to_vec(&json!({
                "nodeId": "node-b",
                "endpoint": "127.0.0.1:9302"
            }))
            .expect("json should encode"),
        };
        let join_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            join_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(join_response.status, 200);

        let invalid_leave_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/cluster/leave".to_string(),
            headers: HashMap::from([
                ("content-type".to_string(), "application/json".to_string()),
                (
                    AUDIT_ACTOR_ID_HEADER.to_string(),
                    "operator-alice".to_string(),
                ),
            ]),
            body: serde_json::to_vec(&json!({})).expect("json should encode"),
        };
        let invalid_leave_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            invalid_leave_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(invalid_leave_response.status, 400);

        let query_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/cluster/audit?actorId=operator-alice&limit=20".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let query_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            query_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(query_response.status, 200);
        let query_body: JsonValue =
            serde_json::from_slice(&query_response.body).expect("valid JSON");
        assert_eq!(query_body["status"], "success");
        let entries = query_body["data"]["entries"]
            .as_array()
            .expect("entries should be an array");
        assert!(entries.len() >= 2);
        assert!(entries
            .iter()
            .any(|entry| entry["operation"] == "join" && entry["outcome"]["status"] == "success"));
        assert!(entries
            .iter()
            .any(|entry| entry["operation"] == "leave" && entry["outcome"]["status"] == "error"));
        assert!(entries.iter().all(|entry| {
            let path = entry["target"]["path"].as_str().unwrap_or_default();
            entry["actor"]["id"] == "operator-alice"
                && entry["timestampUnixMs"].as_u64().is_some()
                && (path == "/api/v1/admin/cluster/leave" || path == "/api/v1/admin/cluster/join")
        }));

        let export_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/cluster/audit/export?operation=join".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let export_response = handle_request_with_admin_and_cluster(
            &storage,
            &engine,
            export_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            Some(cluster_context.as_ref()),
        )
        .await;
        assert_eq!(export_response.status, 200);
        assert_eq!(
            response_header(&export_response, "content-type"),
            Some("application/x-ndjson")
        );
        let export_lines = std::str::from_utf8(&export_response.body)
            .expect("export body should be utf8")
            .lines()
            .collect::<Vec<_>>();
        assert!(!export_lines.is_empty());
        let first_exported: JsonValue =
            serde_json::from_str(export_lines[0]).expect("export line should decode");
        assert_eq!(first_exported["operation"], "join");
        assert_eq!(first_exported["actor"]["id"], "operator-alice");
        assert_eq!(
            first_exported["target"]["path"],
            "/api/v1/admin/cluster/join"
        );
    }

    #[tokio::test]
    async fn admin_snapshot_rejects_path_outside_prefix() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir");
        let prefix = temp_dir.path().join("allowed");
        std::fs::create_dir_all(&prefix).expect("prefix directory should be created");
        let disallowed = temp_dir.path().join("outside").join("snapshot");

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/snapshot".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "path": disallowed.to_string_lossy()
            }))
            .expect("json should encode"),
        };

        let response = handle_request_with_admin(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            Some(prefix.as_path()),
        )
        .await;
        assert_eq!(response.status, 400);
    }

    #[tokio::test]
    async fn admin_snapshot_rejects_nonexistent_path_traversal_outside_prefix() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let temp_dir = TempDir::new().expect("tempdir");
        let prefix = temp_dir.path().join("allowed");
        std::fs::create_dir_all(&prefix).expect("prefix directory should be created");
        let disallowed = prefix
            .join("staging")
            .join("..")
            .join("..")
            .join("outside")
            .join("snapshot");

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/snapshot".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "path": disallowed.to_string_lossy()
            }))
            .expect("json should encode"),
        };

        let response = handle_request_with_admin(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            Some(prefix.as_path()),
        )
        .await;
        assert_eq!(response.status, 400);
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
    async fn admin_console_route_returns_404() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/admin".to_string(),
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
        assert!(body["data"]["memory"]["budgetedBytes"].is_number());
        assert!(body["data"]["memory"]["registryBytes"].is_number());
        assert!(body["data"]["memory"]["persistedIndexBytes"].is_number());
        assert!(body["data"]["memory"]["persistedMmapBytes"].is_number());
        assert!(body["data"]["memory"]["excludedPersistedMmapBytes"].is_number());
        assert!(body["data"]["wal"]["enabled"].is_boolean());
        assert!(body["data"]["wal"]["syncMode"].is_string());
        assert!(body["data"]["wal"]["acknowledgedWritesDurable"].is_boolean());
        assert!(body["data"]["wal"]["appendedHighwaterSegment"].is_number());
        assert!(body["data"]["wal"]["durableHighwaterSegment"].is_number());
        assert!(body["data"]["flush"]["pipelineRunsTotal"].is_number());
        assert!(body["data"]["flush"]["hotSegmentsVisible"].is_number());
        assert!(body["data"]["compaction"]["runsTotal"].is_number());
        assert!(body["data"]["query"]["selectCallsTotal"].is_number());
        assert!(body["data"]["query"]["hotOnlyQueryPlansTotal"].is_number());
        assert!(body["data"]["query"]["rollupQueryPlansTotal"].is_number());
        assert!(body["data"]["query"]["partialRollupQueryPlansTotal"].is_number());
        assert!(body["data"]["query"]["rollupPointsReadTotal"].is_number());
        assert!(body["data"]["rollups"]["workerRunsTotal"].is_number());
        assert!(body["data"]["rollups"]["policies"].is_array());
        assert!(body["data"]["remoteStorage"]["enabled"].is_boolean());
        assert!(body["data"]["remoteStorage"]["runtimeMode"].is_string());
        assert!(body["data"]["remoteStorage"]["cachePolicy"].is_string());
        assert!(body["data"]["remoteStorage"]["catalogRefreshesTotal"].is_number());
        assert!(body["data"]["remoteStorage"]["accessible"].is_boolean());
        assert!(body["data"]["remoteStorage"]["consecutiveRefreshFailures"].is_number());
        assert!(body["data"]["remoteStorage"]["backoffActive"].is_boolean());
        assert!(body["data"]["prometheusPayloads"]["localCapabilities"].is_array());
        assert!(body["data"]["prometheusPayloads"]["metadata"]["enabled"].is_boolean());
        assert!(body["data"]["prometheusPayloads"]["metadata"]["requiredCapabilities"].is_array());
        assert!(body["data"]["prometheusPayloads"]["metadata"]["acceptedTotal"].is_number());
        assert!(body["data"]["prometheusPayloads"]["metadata"]["maxPerRequest"].is_number());
        assert!(body["data"]["prometheusPayloads"]["exemplars"]["enabled"].is_boolean());
        assert!(body["data"]["prometheusPayloads"]["exemplars"]["requiredCapabilities"].is_array());
        assert!(body["data"]["prometheusPayloads"]["exemplars"]["acceptedTotal"].is_number());
        assert!(body["data"]["prometheusPayloads"]["exemplars"]["maxPerRequest"].is_number());
        assert!(body["data"]["prometheusPayloads"]["histograms"]["enabled"].is_boolean());
        assert!(
            body["data"]["prometheusPayloads"]["histograms"]["requiredCapabilities"].is_array()
        );
        assert!(body["data"]["prometheusPayloads"]["histograms"]["acceptedTotal"].is_number());
        assert!(
            body["data"]["prometheusPayloads"]["histograms"]["maxBucketEntriesPerRequest"]
                .is_number()
        );
        assert!(body["data"]["otlpMetrics"]["enabled"].is_boolean());
        assert!(body["data"]["otlpMetrics"]["acceptedRequestsTotal"].is_number());
        assert!(body["data"]["otlpMetrics"]["rejectedRequestsTotal"].is_number());
        assert!(body["data"]["otlpMetrics"]["supportedShapes"].is_array());
        assert!(body["data"]["otlpMetrics"]["gauges"]["acceptedTotal"].is_number());
        assert!(body["data"]["otlpMetrics"]["histograms"]["rejectedTotal"].is_number());
        assert!(body["data"]["legacyIngest"]["influxLineProtocol"]["enabled"].is_boolean());
        assert!(
            body["data"]["legacyIngest"]["influxLineProtocol"]["maxLinesPerRequest"].is_number()
        );
        assert!(body["data"]["legacyIngest"]["statsd"]["enabled"].is_boolean());
        assert!(body["data"]["legacyIngest"]["statsd"]["maxPacketBytes"].is_number());
        assert!(body["data"]["legacyIngest"]["graphite"]["enabled"].is_boolean());
        assert!(body["data"]["legacyIngest"]["graphite"]["maxLineBytes"].is_number());
        assert!(body["data"]["edgeSync"]["source"]["enabled"].is_boolean());
        assert!(body["data"]["edgeSync"]["accept"]["enabled"].is_boolean());
        assert!(body["data"]["admission"]["publicRead"]["rejectionsTotal"].is_number());
        assert!(body["data"]["admission"]["publicRead"]["requestSlotRejectionsTotal"].is_number());
        assert!(body["data"]["admission"]["publicRead"]["queryBudgetRejectionsTotal"].is_number());
        assert!(
            body["data"]["admission"]["publicRead"]["oversizeQueriesRejectionsTotal"].is_number()
        );
        assert!(body["data"]["admission"]["publicRead"]["acquireWaitNanosTotal"].is_number());
        assert!(body["data"]["admission"]["publicRead"]["activeRequests"].is_number());
        assert!(body["data"]["admission"]["publicRead"]["activeQueries"].is_number());
        assert!(body["data"]["admission"]["publicRead"]["globalMaxInflightRequests"].is_number());
        assert!(body["data"]["admission"]["publicRead"]["globalMaxInflightQueries"].is_number());
        assert!(body["data"]["admission"]["publicRead"]["globalAcquireTimeoutMs"].is_number());
        assert!(body["data"]["admission"]["publicWrite"]["rejectionsTotal"].is_number());
        assert!(body["data"]["admission"]["publicWrite"]["requestSlotRejectionsTotal"].is_number());
        assert!(body["data"]["admission"]["publicWrite"]["rowBudgetRejectionsTotal"].is_number());
        assert!(
            body["data"]["admission"]["publicWrite"]["oversizeRowsRejectionsTotal"].is_number()
        );
        assert!(body["data"]["admission"]["publicWrite"]["acquireWaitNanosTotal"].is_number());
        assert!(body["data"]["admission"]["publicWrite"]["activeRequests"].is_number());
        assert!(body["data"]["admission"]["publicWrite"]["activeRows"].is_number());
        assert!(body["data"]["admission"]["publicWrite"]["globalMaxInflightRequests"].is_number());
        assert!(body["data"]["admission"]["publicWrite"]["globalMaxInflightRows"].is_number());
        assert!(body["data"]["admission"]["publicWrite"]["globalAcquireTimeoutMs"].is_number());
        assert!(body["data"]["admission"]["tenant"]["readRejectionsTotal"].is_number());
        assert!(body["data"]["admission"]["tenant"]["writeRejectionsTotal"].is_number());
        assert!(body["data"]["admission"]["tenant"]["activeReads"].is_number());
        assert!(body["data"]["admission"]["tenant"]["activeWrites"].is_number());
        assert!(
            body["data"]["admission"]["tenant"]["surfaceRejectionsTotal"]["ingest"].is_number()
        );
        assert!(body["data"]["admission"]["tenant"]["surfaceActiveRequests"]["query"].is_number());
        assert!(body["data"]["admission"]["tenant"]["surfaceActiveUnits"]["metadata"].is_number());
        assert!(body["data"]["admission"]["tenant"]["currentTenant"].is_null());
        assert!(body["data"]["cluster"]["writeRouting"]["requestsTotal"].is_number());
        assert!(body["data"]["cluster"]["writeRouting"]["hotShards"].is_array());
        assert!(body["data"]["cluster"]["readFanout"]["requestsTotal"].is_number());
        assert!(body["data"]["cluster"]["readFanout"]["operations"].is_array());
        assert!(body["data"]["cluster"]["readFanout"]["resourceRejectionsTotal"].is_number());
        assert!(body["data"]["cluster"]["readFanout"]["resourceAcquireWaitNanosTotal"].is_number());
        assert!(body["data"]["cluster"]["readFanout"]["resourceActiveQueries"].is_number());
        assert!(body["data"]["cluster"]["readFanout"]["resourceActiveMergedPoints"].is_number());
        assert!(body["data"]["cluster"]["readFanout"]["perQueryFanoutConcurrency"].is_number());
        assert!(body["data"]["cluster"]["readFanout"]["globalMaxInflightQueries"].is_number());
        assert!(body["data"]["cluster"]["readFanout"]["globalMaxInflightMergedPoints"].is_number());
        assert!(body["data"]["cluster"]["readFanout"]["globalAcquireTimeoutMs"].is_number());
        assert!(body["data"]["cluster"]["readPlanning"]["requestsTotal"].is_number());
        assert!(body["data"]["cluster"]["readPlanning"]["operations"].is_array());
        assert!(body["data"]["cluster"]["readPlanning"]["lastPlans"].is_array());
        assert!(body["data"]["cluster"]["writeIdempotency"]["requestsTotal"].is_number());
        assert!(body["data"]["cluster"]["writeIdempotency"]["activeKeys"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["enqueuedTotal"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["queuedEntries"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["staleRecords"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["cleanupRunsTotal"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["cleanupCompactionsTotal"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["cleanupReclaimedBytesTotal"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["stalledAlertsTotal"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["stalledPeers"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["stalledOldestAgeMs"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["stalledPeerAgeSecs"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["stalledPeerMinEntries"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["stalledPeerMinBytes"].is_number());
        assert!(body["data"]["cluster"]["writeOutbox"]["peers"].is_array());
        assert!(body["data"]["cluster"]["writeOutbox"]["stalledPeerDetails"].is_array());
        assert!(body["data"]["cluster"]["control"]["currentTerm"].is_number());
        assert!(body["data"]["cluster"]["control"]["leaderStale"].is_boolean());
        assert!(body["data"]["cluster"]["control"]["peers"].is_array());
        assert!(body["data"]["cluster"]["handoff"]["totalShards"].is_number());
        assert!(body["data"]["cluster"]["handoff"]["inProgressShards"].is_number());
        assert!(body["data"]["cluster"]["handoff"]["shards"].is_array());
        assert!(body["data"]["cluster"]["digestExchange"]["intervalSecs"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["windowSecs"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["maxBytesPerTick"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["maxRepairRowsPerTick"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["maxRepairRuntimeMsPerTick"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["repairFailureBackoffSecs"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["repairPaused"].is_boolean());
        assert!(body["data"]["cluster"]["digestExchange"]["repairCancelGeneration"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["runsTotal"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["windowsComparedTotal"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["mismatchesTotal"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["bytesExchangedTotal"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["bytesExchangedLastRun"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["budgetExhaustionsTotal"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["windowsSkippedBudgetTotal"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["budgetExhaustedLastRun"].is_boolean());
        assert!(body["data"]["cluster"]["digestExchange"]["prioritizedShardsLastRun"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["repairsAttemptedTotal"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["repairRowsInsertedTotal"].is_number());
        assert!(body["data"]["cluster"]["digestExchange"]["repairsCancelledTotal"].is_number());
        assert!(
            body["data"]["cluster"]["digestExchange"]["repairsSkippedBackoffTotal"].is_number()
        );
        assert!(body["data"]["cluster"]["digestExchange"]["repairsSkippedPausedTotal"].is_number());
        assert!(
            body["data"]["cluster"]["digestExchange"]["repairsSkippedTimeBudgetTotal"].is_number()
        );
        assert!(
            body["data"]["cluster"]["digestExchange"]["repairTimeBudgetExhaustionsTotal"]
                .is_number()
        );
        assert!(
            body["data"]["cluster"]["digestExchange"]["repairTimeBudgetExhaustedLastRun"]
                .is_boolean()
        );
        assert!(
            body["data"]["cluster"]["digestExchange"]["repairsSkippedBackoffLastRun"].is_number()
        );
        assert!(body["data"]["cluster"]["digestExchange"]["mismatches"].is_array());
        assert!(body["data"]["cluster"]["rebalance"]["intervalSecs"].is_number());
        assert!(body["data"]["cluster"]["rebalance"]["maxRowsPerTick"].is_number());
        assert!(body["data"]["cluster"]["rebalance"]["maxShardsPerTick"].is_number());
        assert!(body["data"]["cluster"]["rebalance"]["effectiveMaxRowsPerTickLastRun"].is_number());
        assert!(body["data"]["cluster"]["rebalance"]["paused"].is_boolean());
        assert!(body["data"]["cluster"]["rebalance"]["runsTotal"].is_number());
        assert!(body["data"]["cluster"]["rebalance"]["rowsScheduledTotal"].is_number());
        assert!(body["data"]["cluster"]["rebalance"]["movesBlockedBySloTotal"].is_number());
        assert!(body["data"]["cluster"]["rebalance"]["sloGuard"]["blockNewHandoffs"].is_boolean());
        assert!(body["data"]["cluster"]["rebalance"]["candidateMoves"].is_array());
        assert!(body["data"]["cluster"]["rebalance"]["jobs"].is_array());
        assert!(body["data"]["cluster"]["hotspot"]["generatedUnixMs"].is_number());
        assert!(body["data"]["cluster"]["hotspot"]["hotShards"].is_array());
        assert!(body["data"]["cluster"]["hotspot"]["tenantPressure"].is_array());
    }

    #[tokio::test]
    async fn status_tsdb_includes_current_tenant_admission_snapshot_when_registry_is_configured() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let tenant_registry = tenant::TenantRegistry::from_json_str(
            r#"{
                "tenants": {
                    "team-b": {
                        "quotas": {
                            "maxReadQueriesPerRequest": 3,
                            "maxQueryLengthBytes": 1024
                        },
                        "admission": {
                            "query": {
                                "maxInflightRequests": 1
                            }
                        },
                        "cluster": {
                            "readConsistency": "strict",
                            "readPartialResponse": "deny"
                        }
                    }
                }
            }"#,
        )
        .expect("tenant registry should parse");
        let prep_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query".to_string(),
            headers: HashMap::from([(tenant::TENANT_HEADER.to_string(), "team-b".to_string())]),
            body: Vec::new(),
        };
        let plan = tenant::prepare_request_plan(
            Some(&tenant_registry),
            &prep_request,
            "team-b",
            tenant::TenantAccessScope::Read,
        )
        .expect("tenant plan should prepare");
        let held = plan
            .admit(tenant::TenantAdmissionSurface::Query, 1)
            .expect("first query budget should admit");
        let _ = plan
            .admit(tenant::TenantAdmissionSurface::Query, 1)
            .expect_err("second query budget should be throttled");

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/status/tsdb".to_string(),
            headers: HashMap::from([(tenant::TENANT_HEADER.to_string(), "team-b".to_string())]),
            body: Vec::new(),
        };
        let response = handle_request_with_admin_and_cluster_and_tenant(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            false,
            None,
            None,
            None,
            Some(&tenant_registry),
        )
        .await;
        drop(held);

        assert_eq!(response.status, 200);
        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(
            body["data"]["admission"]["tenant"]["currentTenant"]["tenantId"],
            "team-b"
        );
        assert_eq!(
            body["data"]["admission"]["tenant"]["currentTenant"]["surfaces"]["query"]
                ["maxInflightRequests"],
            1
        );
        assert_eq!(
            body["data"]["admission"]["tenant"]["currentTenant"]["policy"]["quotas"]
                ["maxReadQueriesPerRequest"],
            3
        );
        assert_eq!(
            body["data"]["admission"]["tenant"]["currentTenant"]["policy"]["cluster"]
                ["readConsistency"],
            "strict"
        );
        assert!(
            body["data"]["admission"]["tenant"]["currentTenant"]["recentDecisions"]
                .as_array()
                .is_some_and(|entries| !entries.is_empty())
        );
    }

    #[tokio::test]
    async fn admin_support_bundle_downloads_selected_tenant_without_public_read_auth() {
        let storage = make_storage();
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let usage_accounting = UsageAccounting::open(None).expect("usage accounting should open");
        let engine = make_engine(&storage);
        let tenant_registry = tenant::TenantRegistry::from_json_str(
            r#"{
                "tenants": {
                    "team-a": {
                        "auth": {
                            "tokens": [
                                {
                                    "token": "tenant-read-token",
                                    "scopes": ["read"]
                                }
                            ]
                        }
                    }
                }
            }"#,
        )
        .expect("tenant registry should parse");

        let response = handle_request_with_admin_and_cluster_and_tenant_and_metadata_and_security(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/admin/support_bundle?tenant=team-a".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            None,
            Some(&tenant_registry),
            None,
            None,
            None,
            Some(&usage_accounting),
        )
        .await;

        assert_eq!(response.status, 200);
        assert!(response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-disposition"))
            .map(|(_, value)| value.as_str())
            .is_some_and(|value| value.contains("tsink-support-bundle-team-a-")));

        let body: JsonValue = serde_json::from_slice(&response.body).expect("bundle should decode");
        assert_eq!(body["tenantId"], "team-a");
        assert_eq!(body["sections"]["statusTsdb"]["httpStatus"], 200);
        assert_eq!(
            body["sections"]["statusTsdb"]["body"]["data"]["admission"]["tenant"]["currentTenant"]
                ["tenantId"],
            "team-a"
        );
        assert_eq!(body["sections"]["usage"]["httpStatus"], 200);
    }

    #[tokio::test]
    async fn admin_usage_report_tracks_ingest_query_storage_and_retention() {
        let storage = make_storage();
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let usage_accounting = UsageAccounting::open(None).expect("usage store should open");
        let engine = make_engine(&storage);

        let import_response = handle_request_with_admin_and_cluster_and_tenant_and_metadata(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/import/prometheus".to_string(),
                headers: HashMap::from([(tenant::TENANT_HEADER.to_string(), "team-a".to_string())]),
                body: b"cpu_usage{host=\"a\"} 1 1000\n".to_vec(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(usage_accounting.as_ref()),
        )
        .await;
        assert_eq!(import_response.status, 200);

        let query_response = handle_request_with_admin_and_cluster_and_tenant_and_metadata(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/query?query=cpu_usage{host=\"a\"}&time=1000".to_string(),
                headers: HashMap::from([(tenant::TENANT_HEADER.to_string(), "team-a".to_string())]),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(usage_accounting.as_ref()),
        )
        .await;
        assert_eq!(query_response.status, 200);

        let report_response = handle_request_with_admin_and_cluster_and_tenant_and_metadata(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/admin/usage/report?tenant=team-a&bucket=none&reconcile=true"
                    .to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(usage_accounting.as_ref()),
        )
        .await;
        assert_eq!(report_response.status, 200);
        let report_body: JsonValue =
            serde_json::from_slice(&report_response.body).expect("report body should decode");
        assert_eq!(
            report_body["data"]["report"]["tenants"][0]["tenantId"],
            "team-a"
        );
        assert_eq!(
            report_body["data"]["report"]["tenants"][0]["ingest"]["rows"],
            1
        );
        assert_eq!(
            report_body["data"]["report"]["tenants"][0]["query"]["eventsTotal"],
            1
        );
        assert!(
            report_body["data"]["report"]["tenants"][0]["latestStorageSnapshot"]["seriesTotal"]
                .as_u64()
                .is_some_and(|count| count >= 1)
        );
        assert_eq!(
            report_body["data"]["reconciledStorageSnapshots"][0]["tenantId"],
            "team-a"
        );

        let delete_response = handle_request_with_admin_and_cluster_and_tenant_and_metadata(
            &storage,
            &metadata_store,
            &exemplar_store,
            None,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/admin/delete_series?match[]=cpu_usage{host=\"a\"}&start=0&end=2000"
                    .to_string(),
                headers: HashMap::from([(tenant::TENANT_HEADER.to_string(), "team-a".to_string())]),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(usage_accounting.as_ref()),
        )
        .await;
        assert_eq!(delete_response.status, 200);

        let retention_report_response =
            handle_request_with_admin_and_cluster_and_tenant_and_metadata(
                &storage,
                &metadata_store,
                &exemplar_store,
                None,
                &engine,
                HttpRequest {
                    method: "GET".to_string(),
                    path: "/api/v1/admin/usage/report?tenant=team-a&bucket=none".to_string(),
                    headers: HashMap::new(),
                    body: Vec::new(),
                },
                start_time(),
                TimestampPrecision::Milliseconds,
                true,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(usage_accounting.as_ref()),
            )
            .await;
        assert_eq!(retention_report_response.status, 200);
        let retention_body: JsonValue =
            serde_json::from_slice(&retention_report_response.body).expect("report body valid");
        assert!(
            retention_body["data"]["report"]["tenants"][0]["retention"]["tombstonesApplied"]
                .as_u64()
                .is_some_and(|count| count >= 1)
        );
    }

    #[tokio::test]
    async fn admin_rollup_endpoints_apply_run_and_report_status() {
        let temp_dir = TempDir::new().expect("tempdir should build");
        let storage = make_persistent_storage(temp_dir.path());
        let engine = make_engine(&storage);
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 2.0)),
                Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 3.0)),
            ])
            .expect("seed writes should succeed");

        let apply_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/rollups/apply".to_string(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&RollupPoliciesApplyRequest {
                policies: vec![RollupPolicy {
                    id: "cpu_1s_avg".to_string(),
                    metric: "cpu_usage".to_string(),
                    match_labels: Vec::new(),
                    interval: 1_000,
                    aggregation: tsink::Aggregation::Avg,
                    bucket_origin: 0,
                }],
            })
            .expect("rollup body should serialize"),
        };

        let apply_response = handle_request_with_admin(
            &storage,
            &engine,
            apply_request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(apply_response.status, 200);

        let apply_body: JsonValue =
            serde_json::from_slice(&apply_response.body).expect("valid apply JSON");
        assert_eq!(apply_body["status"], "success");
        assert_eq!(
            apply_body["data"]["policies"][0]["policy"]["id"],
            "cpu_1s_avg"
        );
        assert!(apply_body["data"]["policies"][0]["materializedThrough"].is_number());

        let run_response = handle_request_with_admin(
            &storage,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/admin/rollups/run".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(run_response.status, 200);

        let status_response = handle_request_with_admin(
            &storage,
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/admin/rollups/status".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(status_response.status, 200);

        let status_body: JsonValue =
            serde_json::from_slice(&status_response.body).expect("valid status JSON");
        assert_eq!(status_body["status"], "success");
        assert_eq!(
            status_body["data"]["policies"][0]["policy"]["metric"],
            "cpu_usage"
        );
        assert!(status_body["data"]["policies"][0]["matchedSeries"].is_number());
        assert!(status_body["data"]["policies"][0]["materializedSeries"].is_number());
        assert!(status_body["data"]["policies"][0]["materializedThrough"].is_number());
    }

    #[tokio::test]
    async fn admin_snapshot_endpoint_creates_snapshot() {
        let temp_dir = TempDir::new().expect("tempdir");
        let source_path = temp_dir.path().join("source");
        let snapshot_path = temp_dir.path().join("snapshot");

        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_data_path(&source_path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build");
        let engine = make_engine(&storage);

        storage
            .insert_rows(&[Row::new("admin_snapshot_metric", DataPoint::new(1, 1.0))])
            .expect("insert should succeed");

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/snapshot".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "path": snapshot_path.to_string_lossy()
            }))
            .expect("json should encode"),
        };

        let response = handle_request_with_admin(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(response.status, 200);
        assert!(snapshot_path.exists());

        storage.close().expect("close should succeed");
    }

    #[tokio::test]
    async fn admin_restore_endpoint_restores_snapshot() {
        let temp_dir = TempDir::new().expect("tempdir");
        let source_path = temp_dir.path().join("source");
        let snapshot_path = temp_dir.path().join("snapshot");
        let restore_path = temp_dir.path().join("restored");

        let source_storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_data_path(&source_path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build");
        source_storage
            .insert_rows(&[Row::new("admin_restore_metric", DataPoint::new(1, 9.0))])
            .expect("insert should succeed");
        source_storage
            .snapshot(&snapshot_path)
            .expect("snapshot should succeed");
        source_storage.close().expect("close should succeed");

        let storage = make_storage();
        let engine = make_engine(&storage);
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/restore".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "snapshotPath": snapshot_path.to_string_lossy(),
                "dataPath": restore_path.to_string_lossy(),
            }))
            .expect("json should encode"),
        };

        let response = handle_request_with_admin(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(response.status, 200);

        let restored_storage = StorageBuilder::new()
            .with_data_path(&restore_path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("restored storage should build");
        let points = restored_storage
            .select("admin_restore_metric", &[], 0, 10)
            .expect("select should succeed");
        assert_eq!(points, vec![DataPoint::new(1, 9.0)]);
        restored_storage.close().expect("close should succeed");
    }

    #[tokio::test]
    async fn admin_snapshot_and_restore_preserve_metric_metadata() {
        let temp_dir = TempDir::new().expect("tempdir");
        let source_path = temp_dir.path().join("source");
        let snapshot_path = temp_dir.path().join("snapshot");
        let restore_path = temp_dir.path().join("restored");

        let storage = make_persistent_storage(&source_path);
        let metadata_store = make_metadata_store(Some(&source_path));
        let exemplar_store = make_exemplar_store(Some(&source_path));
        let engine = make_engine(&storage);

        let write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![PromLabel {
                    name: "__name__".to_string(),
                    value: "snapshot_metric".to_string(),
                }],
                samples: vec![PromSample {
                    value: 3.0,
                    timestamp: 1_700_000_000_000,
                }],
                exemplars: vec![Exemplar {
                    labels: vec![PromLabel {
                        name: "trace_id".to_string(),
                        value: "snapshot-trace".to_string(),
                    }],
                    value: 3.0,
                    timestamp: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            metadata: vec![MetricMetadata {
                r#type: MetricType::Gauge as i32,
                metric_family_name: "snapshot_metric".to_string(),
                help: "Snapshot metadata".to_string(),
                unit: "widgets".to_string(),
            }],
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");
        let write_response = handle_request_with_metadata_and_exemplar_store(
            &storage,
            &metadata_store,
            &exemplar_store,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/write".to_string(),
                headers: HashMap::from([
                    ("content-encoding".to_string(), "snappy".to_string()),
                    (
                        "content-type".to_string(),
                        "application/x-protobuf".to_string(),
                    ),
                ]),
                body: snappy_encode(&encoded),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(write_response.status, 200);

        let snapshot_response = handle_request_with_admin_and_metadata(
            &storage,
            &metadata_store,
            &exemplar_store,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/admin/snapshot".to_string(),
                headers: HashMap::from([(
                    "content-type".to_string(),
                    "application/json".to_string(),
                )]),
                body: serde_json::to_vec(&json!({
                    "path": snapshot_path.to_string_lossy()
                }))
                .expect("json should encode"),
            },
            TestAdminRequestOptions {
                server_start: start_time(),
                timestamp_precision: TimestampPrecision::Milliseconds,
                admin_api_enabled: true,
                admin_path_prefix: None,
            },
        )
        .await;
        assert_eq!(snapshot_response.status, 200);

        storage.close().expect("close should succeed");

        let restore_driver_storage = make_storage();
        let restore_driver_engine = make_engine(&restore_driver_storage);
        let restore_response = handle_request_with_admin(
            &restore_driver_storage,
            &restore_driver_engine,
            HttpRequest {
                method: "POST".to_string(),
                path: "/api/v1/admin/restore".to_string(),
                headers: HashMap::from([(
                    "content-type".to_string(),
                    "application/json".to_string(),
                )]),
                body: serde_json::to_vec(&json!({
                    "snapshotPath": snapshot_path.to_string_lossy(),
                    "dataPath": restore_path.to_string_lossy(),
                }))
                .expect("json should encode"),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(restore_response.status, 200);
        restore_driver_storage
            .close()
            .expect("restore driver storage should close");

        let restored_storage = make_persistent_storage(&restore_path);
        let restored_metadata_store = make_metadata_store(Some(&restore_path));
        let restored_exemplar_store = make_exemplar_store(Some(&restore_path));
        let restored_engine = make_engine(&restored_storage);
        let metadata_response = handle_request_with_metadata_store(
            &restored_storage,
            &restored_metadata_store,
            &restored_engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/metadata".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(metadata_response.status, 200);
        let metadata_body: JsonValue =
            serde_json::from_slice(&metadata_response.body).expect("JSON body should decode");
        assert_eq!(
            metadata_body["data"]["snapshot_metric"][0]["help"],
            "Snapshot metadata"
        );

        let points = restored_storage
            .select(
                "snapshot_metric",
                &[Label::new(tenant::TENANT_LABEL, tenant::DEFAULT_TENANT_ID)],
                0,
                1_800_000_000_000,
            )
            .expect("restored point select should succeed");
        assert_eq!(points, vec![DataPoint::new(1_700_000_000_000, 3.0)]);
        let exemplar_response = handle_request_with_metadata_and_exemplar_store(
            &restored_storage,
            &restored_metadata_store,
            &restored_exemplar_store,
            &restored_engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/query_exemplars?query=snapshot_metric&start=1699999999.0&end=1700000001.0".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(exemplar_response.status, 200);
        let exemplar_body: JsonValue =
            serde_json::from_slice(&exemplar_response.body).expect("exemplar body should decode");
        assert_eq!(
            exemplar_body["data"][0]["exemplars"][0]["labels"]["trace_id"],
            "snapshot-trace"
        );
        restored_storage.close().expect("close should succeed");
    }

    #[tokio::test]
    async fn prometheus_text_import() {
        let storage = make_storage();
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let engine = make_engine(&storage);

        let text = r#"# HELP test_metric A test metric
# TYPE test_metric gauge
test_metric{job="test"} 42 1700000000000 # {trace_id="import-trace"} 42 1700000000000
test_metric{job="test2"} 99 1700000000000
"#;

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/import/prometheus".to_string(),
            headers: HashMap::from([("content-type".to_string(), "text/plain".to_string())]),
            body: text.as_bytes().to_vec(),
        };

        let response = handle_request_with_metadata_and_exemplar_store(
            &storage,
            &metadata_store,
            &exemplar_store,
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
                &[
                    Label::new("job", "test"),
                    Label::new(tenant::TENANT_LABEL, tenant::DEFAULT_TENANT_ID),
                ],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("point must be persisted");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value.as_f64(), Some(42.0));

        let exemplar_response = handle_request_with_metadata_and_exemplar_store(
            &storage,
            &metadata_store,
            &exemplar_store,
            &engine,
            HttpRequest {
                method: "GET".to_string(),
                path: "/api/v1/query_exemplars?query=test_metric{job=\"test\"}&start=1699999999.0&end=1700000001.0".to_string(),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;
        assert_eq!(exemplar_response.status, 200);
        let exemplar_body: JsonValue =
            serde_json::from_slice(&exemplar_response.body).expect("JSON body should decode");
        assert_eq!(
            exemplar_body["data"][0]["exemplars"][0]["labels"]["trace_id"],
            "import-trace"
        );
    }

    #[tokio::test]
    async fn prometheus_text_import_rejects_out_of_retention_samples_with_422() {
        let now = unix_timestamp_millis() as i64;
        let old_ts = now.saturating_sub(120_000);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_retention(Duration::from_secs(60))
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build");
        let metadata_store = make_metadata_store(None);
        let exemplar_store = make_exemplar_store(None);
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/import/prometheus".to_string(),
            headers: HashMap::from([("content-type".to_string(), "text/plain".to_string())]),
            body: format!("test_metric{{job=\"test\"}} 42 {old_ts}\n").into_bytes(),
        };

        let response = handle_request_with_metadata_and_exemplar_store(
            &storage,
            &metadata_store,
            &exemplar_store,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
        )
        .await;

        assert_eq!(response.status, 422);
        assert_eq!(
            response_header(&response, WRITE_ERROR_CODE_HEADER),
            Some("write_out_of_retention")
        );
        let body = String::from_utf8(response.body).expect("response body should decode");
        assert!(body.contains("outside the retention window"));
    }

    #[test]
    fn parse_prom_labels_supports_escaped_values() {
        let labels = parse_prom_labels(r#"job="api,west",path="a\\b",quote="x\"y",nl="line\n2""#)
            .expect("labels should parse");

        assert_eq!(labels[0], Label::new("job", "api,west"));
        assert_eq!(labels[1], Label::new("path", r#"a\b"#));
        assert_eq!(labels[2], Label::new("quote", "x\"y"));
        assert_eq!(labels[3], Label::new("nl", "line\n2"));
    }

    #[test]
    fn parse_prometheus_text_handles_quoted_closing_brace() {
        let rows = parse_prometheus_text(r#"metric{job="a}b",path="c,d"} 1 1000"#, 0)
            .expect("text should parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].metric(), "metric");
        assert!(rows[0].labels().contains(&Label::new("job", "a}b")));
        assert!(rows[0].labels().contains(&Label::new("path", "c,d")));
    }

    #[test]
    fn parse_timestamp_rejects_non_finite_values() {
        assert!(parse_timestamp("NaN", TimestampPrecision::Milliseconds).is_err());
        assert!(parse_timestamp("inf", TimestampPrecision::Milliseconds).is_err());
        assert!(parse_timestamp("1e309", TimestampPrecision::Milliseconds).is_err());
    }

    #[test]
    fn parse_step_rejects_non_finite_values() {
        assert!(parse_step("NaN", TimestampPrecision::Milliseconds).is_err());
        assert!(parse_step("inf", TimestampPrecision::Milliseconds).is_err());
        assert!(parse_step("1e309", TimestampPrecision::Milliseconds).is_err());
        assert!(parse_step("NaNs", TimestampPrecision::Milliseconds).is_err());
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
    async fn delete_series_applies_tombstones() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "delete_metric",
                    vec![Label::new("job", "test")],
                    DataPoint::new(1_700_000_000_000, 42.0),
                ),
                Row::with_labels(
                    "delete_metric",
                    vec![Label::new("job", "test")],
                    DataPoint::new(1_700_000_000_005, 99.0),
                ),
            ])
            .expect("rows should insert");

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/delete_series?match[]=delete_metric{job=\"test\"}&start=1700000000000&end=1700000000003".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request_with_admin(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(response.status, 200);

        let body: JsonValue = serde_json::from_slice(&response.body).expect("valid JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["matchedSeries"], 1);
        assert_eq!(body["data"]["tombstonesApplied"], 1);

        let points = storage
            .select(
                "delete_metric",
                &[Label::new("job", "test")],
                1_700_000_000_000,
                1_700_000_000_010,
            )
            .expect("select should succeed");
        assert_eq!(points, vec![DataPoint::new(1_700_000_000_005, 99.0)]);
    }

    #[tokio::test]
    async fn delete_series_rejects_compute_only_storage() {
        let data_dir = TempDir::new().expect("data dir should create");
        let object_store_dir = TempDir::new().expect("object store dir should create");
        let now_ts = i64::try_from(unix_timestamp_millis()).unwrap_or(i64::MAX);
        let deleted_ts = now_ts.saturating_sub(10_000);
        let retained_ts = deleted_ts.saturating_add(5);

        let source_storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_data_path(data_dir.path())
            .with_object_store_path(object_store_dir.path())
            .with_mirror_hot_segments_to_object_store(true)
            .with_tiered_retention_policy(Duration::from_secs(60), Duration::from_secs(300))
            .with_retention(Duration::from_secs(600))
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_wal_enabled(false)
            .build()
            .expect("source storage should build");
        source_storage
            .insert_rows(&[
                Row::with_labels(
                    "delete_metric",
                    vec![Label::new("job", "test")],
                    DataPoint::new(deleted_ts, 42.0),
                ),
                Row::with_labels(
                    "delete_metric",
                    vec![Label::new("job", "test")],
                    DataPoint::new(retained_ts, 99.0),
                ),
            ])
            .expect("seed rows should insert");
        source_storage.close().expect("source storage should close");

        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_object_store_path(object_store_dir.path())
            .with_runtime_mode(StorageRuntimeMode::ComputeOnly)
            .with_tiered_retention_policy(Duration::from_secs(60), Duration::from_secs(300))
            .with_retention(Duration::from_secs(600))
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_wal_enabled(false)
            .build()
            .expect("compute-only storage should build");
        let engine = make_engine(&storage);

        let response = handle_request_with_admin(
            &storage,
            &engine,
            HttpRequest {
                method: "POST".to_string(),
                path: format!(
                    "/api/v1/admin/delete_series?match[]=delete_metric{{job=\"test\"}}&start={deleted_ts}&end={}",
                    deleted_ts.saturating_add(3)
                ),
                headers: HashMap::new(),
                body: Vec::new(),
            },
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(response.status, 409);
        let body = String::from_utf8(response.body).expect("response body should decode");
        assert!(body.contains("delete_series rejected"));
        assert!(body.contains("compute-only storage mode"));

        let points = storage
            .select(
                "delete_metric",
                &[Label::new("job", "test")],
                deleted_ts,
                retained_ts.saturating_add(1),
            )
            .expect("select should succeed");
        assert_eq!(
            points,
            vec![
                DataPoint::new(deleted_ts, 42.0),
                DataPoint::new(retained_ts, 99.0),
            ]
        );
        storage.close().expect("compute-only storage should close");

        let reopened: Arc<dyn Storage> = StorageBuilder::new()
            .with_object_store_path(object_store_dir.path())
            .with_runtime_mode(StorageRuntimeMode::ComputeOnly)
            .with_tiered_retention_policy(Duration::from_secs(60), Duration::from_secs(300))
            .with_retention(Duration::from_secs(600))
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_wal_enabled(false)
            .build()
            .expect("reopened compute-only storage should build");
        let points = reopened
            .select(
                "delete_metric",
                &[Label::new("job", "test")],
                deleted_ts,
                retained_ts.saturating_add(1),
            )
            .expect("reopened select should succeed");
        assert_eq!(
            points,
            vec![
                DataPoint::new(deleted_ts, 42.0),
                DataPoint::new(retained_ts, 99.0),
            ]
        );
        reopened
            .close()
            .expect("reopened compute-only storage should close");
    }

    #[tokio::test]
    async fn delete_series_is_scoped_to_tenant_header() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let team_b_rows = tenant::scope_rows_for_tenant(
            vec![Row::with_labels(
                "delete_metric",
                vec![Label::new("job", "test")],
                DataPoint::new(1_700_000_000_000, 9.0),
            )],
            "team-b",
        )
        .expect("tenant rows should scope");
        storage
            .insert_rows(&[
                Row::with_labels(
                    "delete_metric",
                    vec![Label::new("job", "test")],
                    DataPoint::new(1_700_000_000_000, 42.0),
                ),
                team_b_rows[0].clone(),
            ])
            .expect("rows should insert");

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/delete_series?match[]=delete_metric{job=\"test\"}&start=1700000000000&end=1700000000001".to_string(),
            headers: HashMap::from([(tenant::TENANT_HEADER.to_string(), "team-b".to_string())]),
            body: Vec::new(),
        };

        let response = handle_request_with_admin(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(response.status, 200);

        let default_points = storage
            .select(
                "delete_metric",
                &[Label::new("job", "test")],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("default tenant points should remain");
        assert_eq!(default_points.len(), 1);
        let team_b_points = tenant::scoped_storage(Arc::clone(&storage), "team-b")
            .select(
                "delete_metric",
                &[Label::new("job", "test")],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("team-b tenant query should succeed");
        assert!(team_b_points.is_empty());
    }

    #[tokio::test]
    async fn delete_series_requires_matcher() {
        let storage = make_storage();
        let engine = make_engine(&storage);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/delete_series".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request_with_admin(
            &storage,
            &engine,
            request,
            start_time(),
            TimestampPrecision::Milliseconds,
            true,
            None,
        )
        .await;
        assert_eq!(response.status, 400);
    }

    #[tokio::test]
    async fn admin_secret_endpoints_expose_rotation_state() {
        let storage = make_storage();
        let engine = make_engine(&storage);
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        let temp_dir = TempDir::new().expect("tempdir should be created");
        let public_token_path = temp_dir.path().join("public.token");
        std::fs::write(&public_token_path, "public-old\n").expect("token file should write");
        let security_manager = SecurityManager::from_config(&crate::server::ServerConfig {
            auth_token_file: Some(public_token_path),
            ..crate::server::ServerConfig::default()
        })
        .expect("security manager should build");

        let rotate_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/admin/secrets/rotate".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&json!({
                "target": "publicAuthToken",
                "mode": "rotate",
                "newValue": "public-new",
                "overlapSeconds": 30
            }))
            .expect("payload should encode"),
        };
        let rotate_response =
            handle_request_with_admin_and_cluster_and_tenant_and_metadata_and_security(
                &storage,
                &metadata_store,
                &exemplar_store,
                None,
                &engine,
                rotate_request,
                start_time(),
                TimestampPrecision::Milliseconds,
                true,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(security_manager.as_ref()),
                None,
            )
            .await;
        assert_eq!(rotate_response.status, 200);
        let rotate_body: JsonValue =
            serde_json::from_slice(&rotate_response.body).expect("valid JSON response");
        assert_eq!(rotate_body["data"]["target"], "publicAuthToken");
        assert_eq!(rotate_body["data"]["issuedCredential"], "public-new");

        let state_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/admin/secrets/state".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let state_response =
            handle_request_with_admin_and_cluster_and_tenant_and_metadata_and_security(
                &storage,
                &metadata_store,
                &exemplar_store,
                None,
                &engine,
                state_request,
                start_time(),
                TimestampPrecision::Milliseconds,
                true,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(security_manager.as_ref()),
                None,
            )
            .await;
        assert_eq!(state_response.status, 200);
        let state_body: JsonValue =
            serde_json::from_slice(&state_response.body).expect("valid JSON response");
        let targets = state_body["data"]["targets"]
            .as_array()
            .expect("targets should be an array");
        assert!(targets.iter().any(|target| {
            target["kind"] == "token"
                && target["target"] == "publicAuthToken"
                && target["generation"] == 2
                && target["acceptsPreviousCredential"] == true
        }));
    }
}
