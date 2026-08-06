use crate::cluster::config::{
    ClusterReadConsistency, ClusterReadPartialResponsePolicy, ClusterWriteConsistency,
};
use crate::http::{HttpRequest, HttpResponse};
use crate::managed_control_plane::{ManagedControlPlane, ManagedTenantRequestPolicy};
use crate::rbac::RBAC_AUTH_VERIFIED_HEADER;
use crate::usage::UsageAccounting;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tsink::{
    BatchWriteResult, DataPoint, DeleteSeriesResult, EffectiveStorageLimits, Label,
    MetadataShardScope, MetricSeries, QueryBudget, QueryCancellationToken, QueryExecution,
    QueryExecutionAccounting, QueryOptions, QueryRowsExecutionResult, QueryRowsPage,
    QueryRowsScanOptions, QueryWorkLimits, Result as TsinkResult, Row, RowWriteOutcome,
    RowWriteStatus, SelectManyExecutionResult, SelectSeriesExecutionResult, SeriesMatcher,
    SeriesPoints, SeriesSelection, Storage, StorageObservabilitySnapshot, TsinkError, WriteMode,
    WriteRejection, WriteRejectionCategory,
};

pub const TENANT_HEADER: &str = "x-tsink-tenant";
pub const SCOPE_ORG_ID_HEADER: &str = "x-scope-orgid";
pub const TENANT_LABEL: &str = "__tsink_tenant__";
pub const DEFAULT_TENANT_ID: &str = "default";
pub const PUBLIC_AUTH_REQUIRED_HEADER: &str = "x-tsink-public-auth-required";
pub const PUBLIC_AUTH_VERIFIED_HEADER: &str = "x-tsink-public-auth-verified";

static TENANT_ADMISSION_READ_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_WRITE_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_ACTIVE_READS: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_ACTIVE_WRITES: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_INGEST_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_INGEST_ACTIVE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_INGEST_ACTIVE_UNITS: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_QUERY_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_QUERY_ACTIVE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_QUERY_ACTIVE_UNITS: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_METADATA_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_METADATA_ACTIVE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_METADATA_ACTIVE_UNITS: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_RETENTION_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_RETENTION_ACTIVE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static TENANT_ADMISSION_RETENTION_ACTIVE_UNITS: AtomicU64 = AtomicU64::new(0);

const TENANT_DECISION_HISTORY_LIMIT: usize = 16;
pub(crate) const DEFAULT_TENANT_RUNTIME_MAX_TENANTS: usize = 4_096;
const TENANT_RUNTIME_CACHE_LIMIT_ERROR_CODE: &str = "tenant_runtime_cache_limit_exceeded";
const UNLABELED_TENANT_FALLBACK_REGEX: &str = ".+";
const TENANT_QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;
const TENANT_STATUS_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;

fn tenant_query_vec_capacity_bytes<T>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    u64::try_from(capacity)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(std::mem::size_of::<T>()).unwrap_or(u64::MAX))
        .saturating_add(TENANT_QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn tenant_query_string_capacity_bytes(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    u64::try_from(capacity)
        .unwrap_or(u64::MAX)
        .saturating_add(TENANT_QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn tenant_query_metric_series_identity_bytes(series: &[MetricSeries]) -> u64 {
    series.iter().fold(0u64, |bytes, series| {
        bytes
            .saturating_add(tenant_query_string_capacity_bytes(series.name.capacity()))
            .saturating_add(tenant_query_vec_capacity_bytes::<Label>(
                series.labels.capacity(),
            ))
            .saturating_add(series.labels.iter().fold(0u64, |label_bytes, label| {
                label_bytes
                    .saturating_add(tenant_query_string_capacity_bytes(label.name.capacity()))
                    .saturating_add(tenant_query_string_capacity_bytes(label.value.capacity()))
            }))
    })
}

fn tenant_query_metric_series_retained_bytes(series: &[MetricSeries], capacity: usize) -> u64 {
    tenant_query_vec_capacity_bytes::<MetricSeries>(capacity)
        .saturating_add(tenant_query_metric_series_identity_bytes(series))
}

fn tenant_query_scoped_series_peak_bytes(series: &[MetricSeries], tenant_id: &str) -> u64 {
    tenant_query_metric_series_retained_bytes(series, series.len()).saturating_add(
        series.iter().fold(0u64, |bytes, item| {
            let required_labels = item.labels.len().saturating_add(1);
            let grown_capacity =
                projected_tenant_query_vec_capacity(item.labels.len(), required_labels);
            bytes
                // `Vec::push` can briefly retain both the exact clone buffer and its grown buffer.
                .saturating_add(tenant_query_vec_capacity_bytes::<Label>(grown_capacity))
                .saturating_add(tenant_query_string_capacity_bytes(TENANT_LABEL.len()))
                .saturating_add(tenant_query_string_capacity_bytes(tenant_id.len()))
        }),
    )
}

fn projected_tenant_query_vec_capacity(current: usize, required: usize) -> usize {
    if required <= current {
        return current;
    }
    current
        .saturating_mul(2)
        .max(required)
        .max(4)
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TenantAccessScope {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TenantAdmissionSurface {
    Ingest,
    Query,
    Metadata,
    Retention,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TenantSurfaceAdmissionBudget {
    pub max_inflight_requests: Option<usize>,
    pub max_inflight_units: Option<usize>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TenantSurfaceAdmissionPolicy {
    pub ingest: TenantSurfaceAdmissionBudget,
    pub query: TenantSurfaceAdmissionBudget,
    pub metadata: TenantSurfaceAdmissionBudget,
    pub retention: TenantSurfaceAdmissionBudget,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TenantRequestPolicy {
    pub max_write_rows_per_request: Option<usize>,
    pub max_read_queries_per_request: Option<usize>,
    pub max_metadata_matchers_per_request: Option<usize>,
    pub max_query_length_bytes: Option<usize>,
    pub max_range_points_per_query: Option<usize>,
    pub write_consistency: Option<ClusterWriteConsistency>,
    pub read_consistency: Option<ClusterReadConsistency>,
    pub read_partial_response_policy: Option<ClusterReadPartialResponsePolicy>,
    pub admission: TenantSurfaceAdmissionPolicy,
}

#[derive(Debug, Default)]
pub struct TenantRequestGuard {
    policy: TenantRequestPolicy,
    _permits: Vec<TenantAdmissionPermit>,
    _managed_guard: Option<crate::managed_control_plane::ManagedTenantRequestGuard>,
}

impl TenantRequestGuard {
    pub fn policy(&self) -> &TenantRequestPolicy {
        &self.policy
    }
}

#[derive(Debug, Clone)]
pub struct TenantRequestPlan {
    tenant_id: String,
    access: TenantAccessScope,
    policy: TenantRequestPolicy,
    runtime: Option<Arc<TenantPolicyRuntime>>,
    managed_policy: Option<ManagedTenantRequestPolicy>,
}

impl TenantRequestPlan {
    #[cfg(test)]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn policy(&self) -> &TenantRequestPolicy {
        &self.policy
    }

    pub fn admit(
        &self,
        surface: TenantAdmissionSurface,
        requested_units: usize,
    ) -> Result<TenantRequestGuard, TenantRequestError> {
        self.admit_with_usage(surface, requested_units, None)
    }

    pub fn admit_with_usage(
        &self,
        surface: TenantAdmissionSurface,
        requested_units: usize,
        usage_accounting: Option<&UsageAccounting>,
    ) -> Result<TenantRequestGuard, TenantRequestError> {
        let managed_guard = self
            .managed_policy
            .as_ref()
            .map(|managed| managed.admit(surface, requested_units, usage_accounting))
            .transpose()?;
        let Some(runtime) = self.runtime.as_ref() else {
            return Ok(TenantRequestGuard {
                policy: self.policy.clone(),
                _permits: Vec::new(),
                _managed_guard: managed_guard,
            });
        };
        let mut guard = runtime.admit(&self.tenant_id, self.access, surface, requested_units)?;
        guard._managed_guard = managed_guard;
        Ok(guard)
    }

    pub fn record_rejected(
        &self,
        surface: TenantAdmissionSurface,
        requested_units: usize,
        reason: impl Into<String>,
    ) {
        if let Some(runtime) = self.runtime.as_ref() {
            runtime.record_decision(
                self.access,
                surface,
                TenantDecisionOutcome::Rejected,
                requested_units,
                reason.into(),
            );
        }
    }

    pub fn record_throttled(
        &self,
        surface: TenantAdmissionSurface,
        requested_units: usize,
        reason: impl Into<String>,
    ) {
        if let Some(runtime) = self.runtime.as_ref() {
            runtime.record_decision(
                self.access,
                surface,
                TenantDecisionOutcome::Throttled,
                requested_units,
                reason.into(),
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantRequestError {
    BadRequest(String),
    Unauthorized(&'static str),
    Forbidden(&'static str),
    TooManyRequests(String),
    Rejected {
        status: u16,
        code: &'static str,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantAdmissionMetricsSnapshot {
    pub read_rejections_total: u64,
    pub write_rejections_total: u64,
    pub active_reads: u64,
    pub active_writes: u64,
    pub ingest_rejections_total: u64,
    pub ingest_active_requests: u64,
    pub ingest_active_units: u64,
    pub query_rejections_total: u64,
    pub query_active_requests: u64,
    pub query_active_units: u64,
    pub metadata_rejections_total: u64,
    pub metadata_active_requests: u64,
    pub metadata_active_units: u64,
    pub retention_rejections_total: u64,
    pub retention_active_requests: u64,
    pub retention_active_units: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TenantRuntimeCacheMetricsSnapshot {
    pub(crate) initialized_runtimes: usize,
    pub(crate) initialized_reserved_runtimes: usize,
    pub(crate) initialized_dynamic_runtimes: usize,
    pub(crate) max_runtimes: usize,
    pub(crate) reserved_runtimes: usize,
    pub(crate) limit_rejections_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantDecisionSnapshot {
    pub unix_ms: u64,
    pub access: String,
    pub surface: String,
    pub outcome: String,
    pub requested_units: u64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantSurfaceStatusSnapshot {
    pub max_inflight_requests: Option<usize>,
    pub max_inflight_units: Option<usize>,
    pub active_requests: u64,
    pub active_units: u64,
    pub rejections_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantRuntimeStatusSnapshot {
    pub tenant_id: String,
    pub policy: TenantRequestPolicy,
    pub max_inflight_reads: Option<usize>,
    pub max_inflight_writes: Option<usize>,
    pub active_reads: u64,
    pub active_writes: u64,
    pub read_rejections_total: u64,
    pub write_rejections_total: u64,
    pub ingest: TenantSurfaceStatusSnapshot,
    pub query: TenantSurfaceStatusSnapshot,
    pub metadata: TenantSurfaceStatusSnapshot,
    pub retention: TenantSurfaceStatusSnapshot,
    pub recent_decisions: Vec<TenantDecisionSnapshot>,
}

/// Tenant-runtime status output whose dynamic allocations stay charged to the caller's query.
///
/// The inner snapshot is intentionally private and this wrapper has no extraction method, so a
/// caller cannot move the output away from its reservation.
#[derive(Debug)]
#[must_use = "dropping the status snapshot releases its query-memory reservation"]
pub(crate) struct AccountedTenantRuntimeStatusSnapshot {
    snapshot: TenantRuntimeStatusSnapshot,
    _reservation: tsink::QueryMemoryReservation,
}

impl AccountedTenantRuntimeStatusSnapshot {
    #[cfg(test)]
    fn accounted_bytes(&self) -> u64 {
        self._reservation.bytes()
    }
}

impl std::ops::Deref for AccountedTenantRuntimeStatusSnapshot {
    type Target = TenantRuntimeStatusSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TenantStatusSnapshotError {
    TenantRequest(TenantRequestError),
    QueryBudget(tsink::QueryBudgetError),
}

impl From<TenantRequestError> for TenantStatusSnapshotError {
    fn from(error: TenantRequestError) -> Self {
        Self::TenantRequest(error)
    }
}

impl From<tsink::QueryBudgetError> for TenantStatusSnapshotError {
    fn from(error: tsink::QueryBudgetError) -> Self {
        Self::QueryBudget(error)
    }
}

fn modeled_tenant_status_vec_bytes<T>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    u64::try_from(capacity)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(std::mem::size_of::<T>()).unwrap_or(u64::MAX))
        .saturating_add(TENANT_STATUS_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_tenant_status_str_bytes(value: &str) -> u64 {
    modeled_tenant_status_str_len_bytes(value.len())
}

fn modeled_tenant_status_str_len_bytes(len: usize) -> u64 {
    if len == 0 {
        return 0;
    }
    u64::try_from(len)
        .unwrap_or(u64::MAX)
        .saturating_add(TENANT_STATUS_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_tenant_status_string_bytes(value: &String) -> u64 {
    if value.capacity() == 0 {
        return 0;
    }
    u64::try_from(value.capacity())
        .unwrap_or(u64::MAX)
        .saturating_add(TENANT_STATUS_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_tenant_status_decision_string_bytes(decision: &TenantDecisionSnapshot) -> u64 {
    modeled_tenant_status_string_bytes(&decision.access)
        .saturating_add(modeled_tenant_status_string_bytes(&decision.surface))
        .saturating_add(modeled_tenant_status_string_bytes(&decision.outcome))
        .saturating_add(modeled_tenant_status_string_bytes(&decision.reason))
}

fn modeled_tenant_runtime_status_retained_bytes(snapshot: &TenantRuntimeStatusSnapshot) -> u64 {
    modeled_tenant_status_string_bytes(&snapshot.tenant_id)
        .saturating_add(modeled_tenant_status_vec_bytes::<TenantDecisionSnapshot>(
            snapshot.recent_decisions.capacity(),
        ))
        .saturating_add(
            snapshot
                .recent_decisions
                .iter()
                .fold(0u64, |bytes, decision| {
                    bytes.saturating_add(modeled_tenant_status_decision_string_bytes(decision))
                }),
        )
}

impl TenantRequestError {
    pub fn to_http_response(&self) -> HttpResponse {
        match self {
            Self::BadRequest(message) => {
                HttpResponse::new(400, message.clone()).with_header("Content-Type", "text/plain")
            }
            Self::Unauthorized(code) => HttpResponse::new(401, "unauthorized")
                .with_header("Content-Type", "text/plain")
                .with_header("WWW-Authenticate", "Bearer")
                .with_header("X-Tsink-Auth-Error-Code", *code),
            Self::Forbidden(code) => HttpResponse::new(403, "forbidden")
                .with_header("Content-Type", "text/plain")
                .with_header("X-Tsink-Auth-Error-Code", *code),
            Self::TooManyRequests(message) => HttpResponse::new(429, message.clone())
                .with_header("Content-Type", "text/plain")
                .with_header("Retry-After", "1")
                .with_header(
                    "X-Tsink-Tenant-Error-Code",
                    "tenant_admission_limit_exceeded",
                ),
            Self::Rejected {
                status,
                code,
                message,
            } => {
                let mut response = HttpResponse::new(*status, message.clone())
                    .with_header("Content-Type", "text/plain")
                    .with_header("X-Tsink-Tenant-Error-Code", *code);
                if *status == 429 {
                    response = response.with_header("Retry-After", "1");
                }
                response
            }
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TenantPolicyFile {
    #[serde(default)]
    max_runtime_tenants: Option<usize>,
    #[serde(default)]
    defaults: TenantPolicyDefinition,
    #[serde(default)]
    tenants: BTreeMap<String, TenantPolicyDefinition>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TenantPolicyDefinition {
    #[serde(default)]
    auth: Option<TenantAuthDefinition>,
    #[serde(default)]
    quotas: Option<TenantQuotaDefinition>,
    #[serde(default)]
    admission: Option<TenantAdmissionDefinition>,
    #[serde(default)]
    cluster: Option<TenantClusterDefinition>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TenantAuthDefinition {
    #[serde(default)]
    tokens: Vec<TenantTokenDefinition>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TenantTokenDefinition {
    token: String,
    #[serde(default)]
    scopes: Vec<TenantAccessScope>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TenantQuotaDefinition {
    max_write_rows_per_request: Option<usize>,
    max_read_queries_per_request: Option<usize>,
    max_metadata_matchers_per_request: Option<usize>,
    max_query_length_bytes: Option<usize>,
    max_range_points_per_query: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TenantAdmissionDefinition {
    max_inflight_reads: Option<usize>,
    max_inflight_writes: Option<usize>,
    #[serde(default)]
    ingest: Option<TenantSurfaceAdmissionDefinition>,
    #[serde(default)]
    query: Option<TenantSurfaceAdmissionDefinition>,
    #[serde(default)]
    metadata: Option<TenantSurfaceAdmissionDefinition>,
    #[serde(default)]
    retention: Option<TenantSurfaceAdmissionDefinition>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TenantSurfaceAdmissionDefinition {
    max_inflight_requests: Option<usize>,
    max_inflight_units: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TenantClusterDefinition {
    write_consistency: Option<ClusterWriteConsistency>,
    read_consistency: Option<ClusterReadConsistency>,
    read_partial_response: Option<ClusterReadPartialResponsePolicy>,
}

#[derive(Debug, Clone, Default)]
struct TenantPolicyTemplate {
    auth_tokens: BTreeMap<String, BTreeSet<TenantAccessScope>>,
    policy: TenantRequestPolicy,
    max_inflight_reads: Option<usize>,
    max_inflight_writes: Option<usize>,
}

#[derive(Debug, Default)]
struct TenantRuntimeCache {
    entries: BTreeMap<String, Arc<TenantPolicyRuntime>>,
    initialized_reserved: usize,
}

#[derive(Debug)]
struct TenantPolicyRuntime {
    policy: TenantRequestPolicy,
    auth_tokens: BTreeMap<String, BTreeSet<TenantAccessScope>>,
    max_inflight_reads: Option<usize>,
    max_inflight_writes: Option<usize>,
    inflight_reads: Option<Arc<Semaphore>>,
    inflight_writes: Option<Arc<Semaphore>>,
    read_rejections_total: AtomicU64,
    write_rejections_total: AtomicU64,
    ingest: TenantSurfaceRuntime,
    query: TenantSurfaceRuntime,
    metadata: TenantSurfaceRuntime,
    retention: TenantSurfaceRuntime,
    recent_decisions: Mutex<VecDeque<TenantDecisionRecord>>,
    #[cfg(test)]
    status_snapshot_string_clones: AtomicU64,
}

#[derive(Debug)]
pub struct TenantRegistry {
    default_template: TenantPolicyTemplate,
    tenant_templates: BTreeMap<String, TenantPolicyTemplate>,
    max_runtime_tenants: usize,
    reserved_runtime_tenants: usize,
    runtimes: Mutex<TenantRuntimeCache>,
    runtime_limit_rejections_total: AtomicU64,
}

#[derive(Debug)]
struct TenantAdmissionPermit {
    _permit: OwnedSemaphorePermit,
    kind: TenantAdmissionPermitKind,
    units: u64,
}

#[derive(Debug)]
enum TenantAdmissionPermitKind {
    SharedAccess(TenantAccessScope),
    SurfaceRequests(TenantAdmissionSurface),
    SurfaceUnits(TenantAdmissionSurface),
}

#[derive(Debug)]
struct TenantSurfaceRuntime {
    budget: TenantSurfaceAdmissionBudget,
    request_slots: Option<Arc<Semaphore>>,
    unit_budget: Option<Arc<Semaphore>>,
    rejections_total: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TenantDecisionOutcome {
    Admitted,
    Throttled,
    Rejected,
}

#[derive(Debug)]
struct TenantDecisionRecord {
    unix_ms: u64,
    access: TenantAccessScope,
    surface: TenantAdmissionSurface,
    outcome: TenantDecisionOutcome,
    requested_units: u64,
    reason: TenantDecisionReason,
}

#[derive(Debug)]
enum TenantDecisionReason {
    Admitted,
    MaxInflightRequests {
        limit: usize,
    },
    RequestedUnitsExceeded {
        requested_units: usize,
        limit: usize,
    },
    MaxInflightUnits {
        limit: usize,
    },
    Owned(String),
}

struct TenantDecisionReasonDisplay<'a> {
    tenant_id: &'a str,
    access: TenantAccessScope,
    surface: TenantAdmissionSurface,
    reason: &'a TenantDecisionReason,
}

#[derive(Default)]
struct TenantDecisionReasonLength {
    bytes: usize,
}

impl Drop for TenantAdmissionPermit {
    fn drop(&mut self) {
        match self.kind {
            TenantAdmissionPermitKind::SharedAccess(TenantAccessScope::Read) => {
                TENANT_ADMISSION_ACTIVE_READS.fetch_sub(1, Ordering::Relaxed);
            }
            TenantAdmissionPermitKind::SharedAccess(TenantAccessScope::Write) => {
                TENANT_ADMISSION_ACTIVE_WRITES.fetch_sub(1, Ordering::Relaxed);
            }
            TenantAdmissionPermitKind::SurfaceRequests(surface) => {
                track_tenant_surface_active_requests(surface, false, 0);
            }
            TenantAdmissionPermitKind::SurfaceUnits(surface) => {
                track_tenant_surface_active_units(surface, false, self.units);
            }
        }
    }
}

impl TenantSurfaceAdmissionBudget {
    fn merged(
        self,
        incoming: &TenantSurfaceAdmissionDefinition,
        field_prefix: &str,
    ) -> Result<Self, String> {
        Ok(Self {
            max_inflight_requests: merge_positive_limit(
                incoming.max_inflight_requests,
                self.max_inflight_requests,
                &format!("{field_prefix}.maxInflightRequests"),
            )?,
            max_inflight_units: merge_positive_limit(
                incoming.max_inflight_units,
                self.max_inflight_units,
                &format!("{field_prefix}.maxInflightUnits"),
            )?,
        })
    }
}

impl TenantSurfaceRuntime {
    fn new(budget: TenantSurfaceAdmissionBudget) -> Self {
        Self {
            budget,
            request_slots: budget
                .max_inflight_requests
                .map(|limit| Arc::new(Semaphore::new(limit))),
            unit_budget: budget
                .max_inflight_units
                .map(|limit| Arc::new(Semaphore::new(limit))),
            rejections_total: AtomicU64::new(0),
        }
    }

    fn active_requests(&self) -> u64 {
        self.budget
            .max_inflight_requests
            .zip(self.request_slots.as_ref())
            .map(|(limit, slots)| {
                u64::try_from(limit.saturating_sub(slots.available_permits())).unwrap_or(u64::MAX)
            })
            .unwrap_or(0)
    }

    fn active_units(&self) -> u64 {
        self.budget
            .max_inflight_units
            .zip(self.unit_budget.as_ref())
            .map(|(limit, slots)| {
                u64::try_from(limit.saturating_sub(slots.available_permits())).unwrap_or(u64::MAX)
            })
            .unwrap_or(0)
    }

    fn snapshot(&self) -> TenantSurfaceStatusSnapshot {
        TenantSurfaceStatusSnapshot {
            max_inflight_requests: self.budget.max_inflight_requests,
            max_inflight_units: self.budget.max_inflight_units,
            active_requests: self.active_requests(),
            active_units: self.active_units(),
            rejections_total: self.rejections_total.load(Ordering::Relaxed),
        }
    }
}

impl TenantDecisionOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Throttled => "throttled",
            Self::Rejected => "rejected",
        }
    }
}

impl std::fmt::Write for TenantDecisionReasonLength {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.bytes = self.bytes.saturating_add(value.len());
        Ok(())
    }
}

impl std::fmt::Display for TenantDecisionReasonDisplay<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.reason {
            TenantDecisionReason::Admitted => write!(
                formatter,
                "tenant request admitted for {} via {} scope",
                self.surface.as_str(),
                self.access.as_str()
            ),
            TenantDecisionReason::MaxInflightRequests { limit } => write!(
                formatter,
                "tenant '{}' exceeded max inflight {} requests ({limit})",
                self.tenant_id,
                self.surface.as_str()
            ),
            TenantDecisionReason::RequestedUnitsExceeded {
                requested_units,
                limit,
            } => write!(
                formatter,
                "tenant '{}' exceeded max inflight {} units: {requested_units} > {limit}",
                self.tenant_id,
                self.surface.as_str()
            ),
            TenantDecisionReason::MaxInflightUnits { limit } => write!(
                formatter,
                "tenant '{}' exceeded max inflight {} units ({limit})",
                self.tenant_id,
                self.surface.as_str()
            ),
            TenantDecisionReason::Owned(reason) => formatter.write_str(reason),
        }
    }
}

impl TenantDecisionReason {
    fn display<'a>(
        &'a self,
        tenant_id: &'a str,
        access: TenantAccessScope,
        surface: TenantAdmissionSurface,
    ) -> TenantDecisionReasonDisplay<'a> {
        TenantDecisionReasonDisplay {
            tenant_id,
            access,
            surface,
            reason: self,
        }
    }

    fn status_text_len(
        &self,
        tenant_id: &str,
        access: TenantAccessScope,
        surface: TenantAdmissionSurface,
    ) -> usize {
        let display = self.display(tenant_id, access, surface);
        let mut length = TenantDecisionReasonLength::default();
        std::fmt::write(&mut length, format_args!("{display}"))
            .expect("tenant decision reason length accounting is infallible");
        length.bytes
    }

    fn status_text(
        &self,
        tenant_id: &str,
        access: TenantAccessScope,
        surface: TenantAdmissionSurface,
    ) -> String {
        let display = self.display(tenant_id, access, surface);
        let mut rendered = String::with_capacity(self.status_text_len(tenant_id, access, surface));
        std::fmt::write(&mut rendered, format_args!("{display}"))
            .expect("writing a tenant decision reason into String is infallible");
        rendered
    }

    #[cfg(test)]
    fn modeled_retained_heap_bytes(&self) -> u64 {
        match self {
            Self::Owned(reason) => modeled_tenant_status_string_bytes(reason),
            Self::Admitted
            | Self::MaxInflightRequests { .. }
            | Self::RequestedUnitsExceeded { .. }
            | Self::MaxInflightUnits { .. } => 0,
        }
    }
}

impl From<String> for TenantDecisionReason {
    fn from(reason: String) -> Self {
        Self::Owned(reason)
    }
}

impl TenantDecisionRecord {
    fn status_snapshot(&self, tenant_id: &str) -> TenantDecisionSnapshot {
        TenantDecisionSnapshot {
            unix_ms: self.unix_ms,
            access: self.access.as_str().to_string(),
            surface: self.surface.as_str().to_string(),
            outcome: self.outcome.as_str().to_string(),
            requested_units: self.requested_units,
            reason: self
                .reason
                .status_text(tenant_id, self.access, self.surface),
        }
    }

    fn modeled_status_output_bytes(&self, tenant_id: &str) -> u64 {
        modeled_tenant_status_str_bytes(self.access.as_str())
            .saturating_add(modeled_tenant_status_str_bytes(self.surface.as_str()))
            .saturating_add(modeled_tenant_status_str_bytes(self.outcome.as_str()))
            .saturating_add(modeled_tenant_status_str_len_bytes(
                self.reason
                    .status_text_len(tenant_id, self.access, self.surface),
            ))
    }
}

fn authorize_tenant_policy(
    auth_tokens: &BTreeMap<String, BTreeSet<TenantAccessScope>>,
    request: &HttpRequest,
    access: TenantAccessScope,
) -> Result<(), TenantRequestError> {
    if request.header(RBAC_AUTH_VERIFIED_HEADER).is_some() {
        return Ok(());
    }
    if !auth_tokens.is_empty() {
        let Some(token) = bearer_token(request) else {
            return Err(TenantRequestError::Unauthorized(
                "tenant_auth_token_missing",
            ));
        };
        let Some(scopes) = auth_tokens.get(token) else {
            return Err(TenantRequestError::Unauthorized(
                "tenant_auth_token_invalid",
            ));
        };
        if !scopes.contains(&access) {
            return Err(TenantRequestError::Forbidden("tenant_auth_scope_denied"));
        }
        return Ok(());
    }

    if request.header(PUBLIC_AUTH_REQUIRED_HEADER).is_some() {
        if request.header(PUBLIC_AUTH_VERIFIED_HEADER).is_some() {
            return Ok(());
        }
        if bearer_token(request).is_some() {
            return Err(TenantRequestError::Unauthorized("auth_token_invalid"));
        }
        return Err(TenantRequestError::Unauthorized("auth_token_missing"));
    }

    Ok(())
}

impl TenantPolicyTemplate {
    fn from_definition(definition: &TenantPolicyDefinition) -> Result<Self, String> {
        Self::default().merged(definition)
    }

    fn merged(&self, definition: &TenantPolicyDefinition) -> Result<Self, String> {
        let auth_tokens = if let Some(auth) = definition.auth.as_ref() {
            parse_auth_tokens(auth)?
        } else {
            self.auth_tokens.clone()
        };

        let mut policy = self.policy.clone();
        if let Some(quotas) = definition.quotas.as_ref() {
            policy.max_write_rows_per_request = merge_positive_limit(
                quotas.max_write_rows_per_request,
                policy.max_write_rows_per_request,
                "maxWriteRowsPerRequest",
            )?;
            policy.max_read_queries_per_request = merge_positive_limit(
                quotas.max_read_queries_per_request,
                policy.max_read_queries_per_request,
                "maxReadQueriesPerRequest",
            )?;
            policy.max_metadata_matchers_per_request = merge_positive_limit(
                quotas.max_metadata_matchers_per_request,
                policy.max_metadata_matchers_per_request,
                "maxMetadataMatchersPerRequest",
            )?;
            policy.max_query_length_bytes = merge_positive_limit(
                quotas.max_query_length_bytes,
                policy.max_query_length_bytes,
                "maxQueryLengthBytes",
            )?;
            policy.max_range_points_per_query = merge_positive_limit(
                quotas.max_range_points_per_query,
                policy.max_range_points_per_query,
                "maxRangePointsPerQuery",
            )?;
        }
        if let Some(cluster) = definition.cluster.as_ref() {
            policy.write_consistency = cluster.write_consistency.or(policy.write_consistency);
            policy.read_consistency = cluster.read_consistency.or(policy.read_consistency);
            policy.read_partial_response_policy = cluster
                .read_partial_response
                .or(policy.read_partial_response_policy);
        }

        let mut max_inflight_reads = self.max_inflight_reads;
        let mut max_inflight_writes = self.max_inflight_writes;
        if let Some(admission) = definition.admission.as_ref() {
            max_inflight_reads = merge_positive_limit(
                admission.max_inflight_reads,
                max_inflight_reads,
                "maxInflightReads",
            )?;
            max_inflight_writes = merge_positive_limit(
                admission.max_inflight_writes,
                max_inflight_writes,
                "maxInflightWrites",
            )?;
            if let Some(ingest) = admission.ingest.as_ref() {
                policy.admission = TenantSurfaceAdmissionPolicy {
                    ingest: policy.admission.ingest.merged(ingest, "ingest")?,
                    ..policy.admission
                };
            }
            if let Some(query) = admission.query.as_ref() {
                policy.admission = TenantSurfaceAdmissionPolicy {
                    query: policy.admission.query.merged(query, "query")?,
                    ..policy.admission
                };
            }
            if let Some(metadata) = admission.metadata.as_ref() {
                policy.admission = TenantSurfaceAdmissionPolicy {
                    metadata: policy.admission.metadata.merged(metadata, "metadata")?,
                    ..policy.admission
                };
            }
            if let Some(retention) = admission.retention.as_ref() {
                policy.admission = TenantSurfaceAdmissionPolicy {
                    retention: policy.admission.retention.merged(retention, "retention")?,
                    ..policy.admission
                };
            }
        }

        Ok(Self {
            auth_tokens,
            policy,
            max_inflight_reads,
            max_inflight_writes,
        })
    }

    fn authorize(
        &self,
        request: &HttpRequest,
        access: TenantAccessScope,
    ) -> Result<(), TenantRequestError> {
        authorize_tenant_policy(&self.auth_tokens, request, access)
    }
}

impl TenantPolicyRuntime {
    fn from_template(template: TenantPolicyTemplate) -> Self {
        let policy = template.policy.clone();
        let ingest_budget = policy.admission.ingest;
        let query_budget = policy.admission.query;
        let metadata_budget = policy.admission.metadata;
        let retention_budget = policy.admission.retention;
        Self {
            policy,
            auth_tokens: template.auth_tokens,
            max_inflight_reads: template.max_inflight_reads,
            max_inflight_writes: template.max_inflight_writes,
            inflight_reads: template
                .max_inflight_reads
                .map(|limit| Arc::new(Semaphore::new(limit))),
            inflight_writes: template
                .max_inflight_writes
                .map(|limit| Arc::new(Semaphore::new(limit))),
            read_rejections_total: AtomicU64::new(0),
            write_rejections_total: AtomicU64::new(0),
            ingest: TenantSurfaceRuntime::new(ingest_budget),
            query: TenantSurfaceRuntime::new(query_budget),
            metadata: TenantSurfaceRuntime::new(metadata_budget),
            retention: TenantSurfaceRuntime::new(retention_budget),
            recent_decisions: Mutex::new(VecDeque::with_capacity(TENANT_DECISION_HISTORY_LIMIT)),
            #[cfg(test)]
            status_snapshot_string_clones: AtomicU64::new(0),
        }
    }

    fn authorize(
        &self,
        request: &HttpRequest,
        access: TenantAccessScope,
    ) -> Result<(), TenantRequestError> {
        authorize_tenant_policy(&self.auth_tokens, request, access)
    }

    fn shared_permit(
        &self,
        tenant_id: &str,
        access: TenantAccessScope,
    ) -> Result<Option<TenantAdmissionPermit>, TenantRequestError> {
        let (limit, semaphore) = match access {
            TenantAccessScope::Read => (self.max_inflight_reads, self.inflight_reads.as_ref()),
            TenantAccessScope::Write => (self.max_inflight_writes, self.inflight_writes.as_ref()),
        };
        let Some(limit) = limit else {
            return Ok(None);
        };
        let Some(semaphore) = semaphore else {
            return Ok(None);
        };
        match Arc::clone(semaphore).try_acquire_owned() {
            Ok(permit) => {
                track_tenant_admission_active(access);
                Ok(Some(TenantAdmissionPermit {
                    _permit: permit,
                    kind: TenantAdmissionPermitKind::SharedAccess(access),
                    units: 0,
                }))
            }
            Err(_) => {
                track_tenant_admission_rejection(access);
                match access {
                    TenantAccessScope::Read => {
                        self.read_rejections_total.fetch_add(1, Ordering::Relaxed);
                    }
                    TenantAccessScope::Write => {
                        self.write_rejections_total.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(TenantRequestError::TooManyRequests(format!(
                    "tenant '{tenant_id}' exceeded max inflight {} requests ({limit})",
                    access.as_str()
                )))
            }
        }
    }

    fn surface_runtime(&self, surface: TenantAdmissionSurface) -> &TenantSurfaceRuntime {
        match surface {
            TenantAdmissionSurface::Ingest => &self.ingest,
            TenantAdmissionSurface::Query => &self.query,
            TenantAdmissionSurface::Metadata => &self.metadata,
            TenantAdmissionSurface::Retention => &self.retention,
        }
    }

    fn record_decision(
        &self,
        access: TenantAccessScope,
        surface: TenantAdmissionSurface,
        outcome: TenantDecisionOutcome,
        requested_units: usize,
        reason: impl Into<TenantDecisionReason>,
    ) {
        let mut recent = self
            .recent_decisions
            .lock()
            .expect("tenant decision log mutex should not be poisoned");
        if recent.len() >= TENANT_DECISION_HISTORY_LIMIT {
            recent.pop_front();
        }
        recent.push_back(TenantDecisionRecord {
            unix_ms: unix_timestamp_millis(),
            access,
            surface,
            outcome,
            requested_units: u64::try_from(requested_units).unwrap_or(u64::MAX),
            reason: reason.into(),
        });
    }

    fn admit(
        &self,
        tenant_id: &str,
        access: TenantAccessScope,
        surface: TenantAdmissionSurface,
        requested_units: usize,
    ) -> Result<TenantRequestGuard, TenantRequestError> {
        let mut permits = Vec::new();
        if let Some(permit) = self.shared_permit(tenant_id, access)? {
            permits.push(permit);
        }

        let surface_runtime = self.surface_runtime(surface);
        if let Some(limit) = surface_runtime.budget.max_inflight_requests {
            let Some(slots) = surface_runtime.request_slots.as_ref() else {
                unreachable!("surface request slots must exist when a limit is configured");
            };
            match Arc::clone(slots).try_acquire_owned() {
                Ok(permit) => {
                    track_tenant_surface_active_requests(surface, true, 0);
                    permits.push(TenantAdmissionPermit {
                        _permit: permit,
                        kind: TenantAdmissionPermitKind::SurfaceRequests(surface),
                        units: 0,
                    });
                }
                Err(_) => {
                    track_tenant_surface_rejection(surface);
                    surface_runtime
                        .rejections_total
                        .fetch_add(1, Ordering::Relaxed);
                    let reason = format!(
                        "tenant '{tenant_id}' exceeded max inflight {} requests ({limit})",
                        surface.as_str()
                    );
                    self.record_decision(
                        access,
                        surface,
                        TenantDecisionOutcome::Throttled,
                        requested_units,
                        TenantDecisionReason::MaxInflightRequests { limit },
                    );
                    return Err(TenantRequestError::TooManyRequests(reason));
                }
            }
        }

        if let Some(limit) = surface_runtime.budget.max_inflight_units {
            if requested_units > limit {
                track_tenant_surface_rejection(surface);
                surface_runtime
                    .rejections_total
                    .fetch_add(1, Ordering::Relaxed);
                let reason = format!(
                    "tenant '{tenant_id}' exceeded max inflight {} units: {requested_units} > {limit}",
                    surface.as_str()
                );
                self.record_decision(
                    access,
                    surface,
                    TenantDecisionOutcome::Rejected,
                    requested_units,
                    TenantDecisionReason::RequestedUnitsExceeded {
                        requested_units,
                        limit,
                    },
                );
                return Err(TenantRequestError::TooManyRequests(reason));
            }
            if requested_units > 0 {
                let Some(unit_budget) = surface_runtime.unit_budget.as_ref() else {
                    unreachable!("surface unit budget must exist when a limit is configured");
                };
                let permits_needed = u32::try_from(requested_units).expect(
                    "requested tenant units must fit into u32 when a tenant unit limit is configured",
                );
                match Arc::clone(unit_budget).try_acquire_many_owned(permits_needed) {
                    Ok(permit) => {
                        let units = u64::try_from(requested_units).unwrap_or(u64::MAX);
                        track_tenant_surface_active_units(surface, true, units);
                        permits.push(TenantAdmissionPermit {
                            _permit: permit,
                            kind: TenantAdmissionPermitKind::SurfaceUnits(surface),
                            units,
                        });
                    }
                    Err(_) => {
                        track_tenant_surface_rejection(surface);
                        surface_runtime
                            .rejections_total
                            .fetch_add(1, Ordering::Relaxed);
                        let reason = format!(
                            "tenant '{tenant_id}' exceeded max inflight {} units ({limit})",
                            surface.as_str()
                        );
                        self.record_decision(
                            access,
                            surface,
                            TenantDecisionOutcome::Throttled,
                            requested_units,
                            TenantDecisionReason::MaxInflightUnits { limit },
                        );
                        return Err(TenantRequestError::TooManyRequests(reason));
                    }
                }
            }
        }

        self.record_decision(
            access,
            surface,
            TenantDecisionOutcome::Admitted,
            requested_units,
            TenantDecisionReason::Admitted,
        );
        Ok(TenantRequestGuard {
            policy: self.policy.clone(),
            _permits: permits,
            _managed_guard: None,
        })
    }

    #[allow(dead_code)]
    fn status_snapshot(&self, tenant_id: &str) -> TenantRuntimeStatusSnapshot {
        let active_reads = self
            .max_inflight_reads
            .zip(self.inflight_reads.as_ref())
            .map(|(limit, slots)| {
                u64::try_from(limit.saturating_sub(slots.available_permits())).unwrap_or(u64::MAX)
            })
            .unwrap_or(0);
        let active_writes = self
            .max_inflight_writes
            .zip(self.inflight_writes.as_ref())
            .map(|(limit, slots)| {
                u64::try_from(limit.saturating_sub(slots.available_permits())).unwrap_or(u64::MAX)
            })
            .unwrap_or(0);
        let recent_decisions = self
            .recent_decisions
            .lock()
            .expect("tenant decision log mutex should not be poisoned")
            .iter()
            .map(|decision| decision.status_snapshot(tenant_id))
            .collect();
        TenantRuntimeStatusSnapshot {
            tenant_id: tenant_id.to_string(),
            policy: self.policy.clone(),
            max_inflight_reads: self.max_inflight_reads,
            max_inflight_writes: self.max_inflight_writes,
            active_reads,
            active_writes,
            read_rejections_total: self.read_rejections_total.load(Ordering::Relaxed),
            write_rejections_total: self.write_rejections_total.load(Ordering::Relaxed),
            ingest: self.ingest.snapshot(),
            query: self.query.snapshot(),
            metadata: self.metadata.snapshot(),
            retention: self.retention.snapshot(),
            recent_decisions,
        }
    }

    /// Captures every schema-visible tenant-runtime status field under one query execution.
    ///
    /// The decision log is the only mutable dynamic source. Its complete retained projection is
    /// measured and reserved while the mutex is held, before any tenant identifier or decision
    /// string is copied. The returned private wrapper keeps that reservation live for as long as
    /// any projected field can be borrowed.
    fn status_snapshot_with_execution(
        &self,
        tenant_id: &str,
        execution: &QueryExecution,
    ) -> Result<AccountedTenantRuntimeStatusSnapshot, tsink::QueryBudgetError> {
        execution.checkpoint()?;
        let active_reads = self
            .max_inflight_reads
            .zip(self.inflight_reads.as_ref())
            .map(|(limit, slots)| {
                u64::try_from(limit.saturating_sub(slots.available_permits())).unwrap_or(u64::MAX)
            })
            .unwrap_or(0);
        let active_writes = self
            .max_inflight_writes
            .zip(self.inflight_writes.as_ref())
            .map(|(limit, slots)| {
                u64::try_from(limit.saturating_sub(slots.available_permits())).unwrap_or(u64::MAX)
            })
            .unwrap_or(0);
        let recent = self
            .recent_decisions
            .lock()
            .expect("tenant decision log mutex should not be poisoned");
        execution.checkpoint()?;

        let mut peak_bytes = modeled_tenant_status_str_bytes(tenant_id).saturating_add(
            modeled_tenant_status_vec_bytes::<TenantDecisionSnapshot>(recent.len()),
        );
        for decision in recent.iter() {
            execution.checkpoint()?;
            peak_bytes = peak_bytes.saturating_add(decision.modeled_status_output_bytes(tenant_id));
        }
        let mut reservation = execution.reserve_memory(peak_bytes)?;
        execution.checkpoint()?;

        let tenant_id = self.clone_status_snapshot_string(tenant_id);
        let mut recent_decisions = Vec::with_capacity(recent.len());
        for decision in recent.iter() {
            execution.checkpoint()?;
            recent_decisions.push(TenantDecisionSnapshot {
                unix_ms: decision.unix_ms,
                access: self.clone_status_snapshot_string(decision.access.as_str()),
                surface: self.clone_status_snapshot_string(decision.surface.as_str()),
                outcome: self.clone_status_snapshot_string(decision.outcome.as_str()),
                requested_units: decision.requested_units,
                reason: self.clone_status_snapshot_reason(decision, &tenant_id),
            });
        }
        execution.checkpoint()?;
        drop(recent);

        let snapshot = TenantRuntimeStatusSnapshot {
            tenant_id,
            policy: self.policy.clone(),
            max_inflight_reads: self.max_inflight_reads,
            max_inflight_writes: self.max_inflight_writes,
            active_reads,
            active_writes,
            read_rejections_total: self.read_rejections_total.load(Ordering::Relaxed),
            write_rejections_total: self.write_rejections_total.load(Ordering::Relaxed),
            ingest: self.ingest.snapshot(),
            query: self.query.snapshot(),
            metadata: self.metadata.snapshot(),
            retention: self.retention.snapshot(),
            recent_decisions,
        };
        let retained_bytes = modeled_tenant_runtime_status_retained_bytes(&snapshot);
        assert!(
            retained_bytes <= peak_bytes,
            "tenant status retained-memory model exceeded its pre-allocation reservation"
        );
        reservation.resize(retained_bytes)?;
        Ok(AccountedTenantRuntimeStatusSnapshot {
            snapshot,
            _reservation: reservation,
        })
    }

    fn clone_status_snapshot_string(&self, value: &str) -> String {
        #[cfg(test)]
        self.status_snapshot_string_clones
            .fetch_add(1, Ordering::Relaxed);
        let mut cloned = String::with_capacity(value.len());
        cloned.push_str(value);
        cloned
    }

    fn clone_status_snapshot_reason(
        &self,
        decision: &TenantDecisionRecord,
        tenant_id: &str,
    ) -> String {
        #[cfg(test)]
        self.status_snapshot_string_clones
            .fetch_add(1, Ordering::Relaxed);
        decision
            .reason
            .status_text(tenant_id, decision.access, decision.surface)
    }

    #[cfg(test)]
    fn reset_status_snapshot_string_clones(&self) {
        self.status_snapshot_string_clones
            .store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn status_snapshot_string_clones(&self) -> u64 {
        self.status_snapshot_string_clones.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn decision_log_retained_state(&self) -> (usize, usize, u64) {
        let recent = self
            .recent_decisions
            .lock()
            .expect("tenant decision log mutex should not be poisoned");
        let retained_bytes =
            modeled_tenant_status_vec_bytes::<TenantDecisionRecord>(recent.capacity())
                .saturating_add(recent.iter().fold(0u64, |bytes, decision| {
                    bytes.saturating_add(decision.reason.modeled_retained_heap_bytes())
                }));
        (recent.len(), recent.capacity(), retained_bytes)
    }
}

impl Default for TenantRegistry {
    fn default() -> Self {
        Self {
            default_template: TenantPolicyTemplate::default(),
            tenant_templates: BTreeMap::new(),
            max_runtime_tenants: DEFAULT_TENANT_RUNTIME_MAX_TENANTS,
            reserved_runtime_tenants: 1,
            runtimes: Mutex::new(TenantRuntimeCache::default()),
            runtime_limit_rejections_total: AtomicU64::new(0),
        }
    }
}

fn tenant_runtime_cache_limit_error(limit: usize) -> TenantRequestError {
    TenantRequestError::Rejected {
        status: 503,
        code: TENANT_RUNTIME_CACHE_LIMIT_ERROR_CODE,
        message: format!("tenant runtime cache limit of {limit} tenants would be exceeded"),
    }
}

impl TenantRegistry {
    pub fn load_from_path(path: &Path) -> Result<Self, String> {
        let raw = fs::read_to_string(path)
            .map_err(|err| format!("failed to read tenant config {}: {err}", path.display()))?;
        Self::from_json_str(&raw)
    }

    pub fn from_json_str(raw: &str) -> Result<Self, String> {
        let file: TenantPolicyFile = serde_json::from_str(raw)
            .map_err(|err| format!("invalid tenant config JSON: {err}"))?;
        Self::from_file(file)
    }

    fn from_file(file: TenantPolicyFile) -> Result<Self, String> {
        let TenantPolicyFile {
            max_runtime_tenants,
            defaults,
            tenants,
        } = file;
        // Preserve the established configuration-error precedence: validate and merge the policy
        // definitions before applying the new cache-capacity relationship.
        let default_template = TenantPolicyTemplate::from_definition(&defaults)?;
        let mut tenant_templates = BTreeMap::new();
        for (tenant_id, definition) in tenants {
            validate_tenant_id(&tenant_id)?;
            tenant_templates.insert(tenant_id, default_template.merged(&definition)?);
        }

        let max_runtime_tenants = max_runtime_tenants.unwrap_or(DEFAULT_TENANT_RUNTIME_MAX_TENANTS);
        if max_runtime_tenants == 0 {
            return Err("maxRuntimeTenants must be greater than zero".to_string());
        }
        let reserved_runtime_tenants = tenant_templates
            .len()
            .checked_add(usize::from(
                !tenant_templates.contains_key(DEFAULT_TENANT_ID),
            ))
            .ok_or_else(|| "tenant runtime reservation count overflowed usize".to_string())?;
        if reserved_runtime_tenants > max_runtime_tenants {
            return Err(format!(
                "maxRuntimeTenants must be at least {reserved_runtime_tenants} to reserve every configured tenant and the default tenant"
            ));
        }

        Ok(Self {
            default_template,
            tenant_templates,
            max_runtime_tenants,
            reserved_runtime_tenants,
            runtimes: Mutex::new(TenantRuntimeCache::default()),
            runtime_limit_rejections_total: AtomicU64::new(0),
        })
    }

    fn is_reserved_runtime_tenant(&self, tenant_id: &str) -> bool {
        tenant_id == DEFAULT_TENANT_ID || self.tenant_templates.contains_key(tenant_id)
    }

    fn template_for(&self, tenant_id: &str) -> &TenantPolicyTemplate {
        self.tenant_templates
            .get(tenant_id)
            .unwrap_or(&self.default_template)
    }

    fn authorize(
        &self,
        tenant_id: &str,
        request: &HttpRequest,
        access: TenantAccessScope,
    ) -> Result<(), TenantRequestError> {
        validate_tenant_id(tenant_id).map_err(TenantRequestError::BadRequest)?;
        self.template_for(tenant_id).authorize(request, access)
    }

    fn runtime_for(&self, tenant_id: &str) -> Result<Arc<TenantPolicyRuntime>, TenantRequestError> {
        validate_tenant_id(tenant_id).map_err(TenantRequestError::BadRequest)?;
        let mut cache = self
            .runtimes
            .lock()
            .expect("tenant runtime cache mutex should not be poisoned");
        if let Some(runtime) = cache.entries.get(tenant_id) {
            return Ok(Arc::clone(runtime));
        }

        let reserved = self.is_reserved_runtime_tenant(tenant_id);
        if !reserved {
            let initialized_dynamic = cache
                .entries
                .len()
                .checked_sub(cache.initialized_reserved)
                .expect("initialized reserved tenant count must not exceed cache entries");
            let max_dynamic = self
                .max_runtime_tenants
                .checked_sub(self.reserved_runtime_tenants)
                .expect("reserved tenant count must fit the validated runtime limit");
            if initialized_dynamic >= max_dynamic {
                self.runtime_limit_rejections_total
                    .fetch_add(1, Ordering::Relaxed);
                return Err(tenant_runtime_cache_limit_error(self.max_runtime_tenants));
            }
        }

        debug_assert!(cache.entries.len() < self.max_runtime_tenants);
        let template = self.template_for(tenant_id).clone();
        let runtime = Arc::new(TenantPolicyRuntime::from_template(template));
        let previous = cache
            .entries
            .insert(tenant_id.to_string(), Arc::clone(&runtime));
        debug_assert!(previous.is_none());
        if reserved {
            cache.initialized_reserved = cache.initialized_reserved.saturating_add(1);
            debug_assert!(cache.initialized_reserved <= self.reserved_runtime_tenants);
        }
        Ok(runtime)
    }

    #[allow(dead_code)]
    pub(crate) fn initialize_tenant_runtime(
        &self,
        tenant_id: &str,
    ) -> Result<(), TenantRequestError> {
        drop(self.runtime_for(tenant_id)?);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn initialized_runtime_count(&self) -> usize {
        self.runtimes
            .lock()
            .expect("tenant runtime cache mutex should not be poisoned")
            .entries
            .len()
    }

    pub(crate) fn runtime_cache_metrics_snapshot(&self) -> TenantRuntimeCacheMetricsSnapshot {
        let cache = self
            .runtimes
            .lock()
            .expect("tenant runtime cache mutex should not be poisoned");
        TenantRuntimeCacheMetricsSnapshot {
            initialized_runtimes: cache.entries.len(),
            initialized_reserved_runtimes: cache.initialized_reserved,
            initialized_dynamic_runtimes: cache
                .entries
                .len()
                .saturating_sub(cache.initialized_reserved),
            max_runtimes: self.max_runtime_tenants,
            reserved_runtimes: self.reserved_runtime_tenants,
            limit_rejections_total: self.runtime_limit_rejections_total.load(Ordering::Relaxed),
        }
    }

    #[allow(dead_code)]
    pub fn status_snapshot_for(
        &self,
        tenant_id: &str,
    ) -> Result<TenantRuntimeStatusSnapshot, TenantRequestError> {
        let runtime = self.runtime_for(tenant_id)?;
        Ok(runtime.status_snapshot(tenant_id))
    }

    pub(crate) fn status_snapshot_for_with_execution(
        &self,
        tenant_id: &str,
        execution: &QueryExecution,
    ) -> Result<AccountedTenantRuntimeStatusSnapshot, TenantStatusSnapshotError> {
        // Resolve the tenant first so invalid identifiers retain the legacy request-error
        // semantics even when the supplied execution has already been cancelled.
        let runtime = self.runtime_for(tenant_id)?;
        Ok(runtime.status_snapshot_with_execution(tenant_id, execution)?)
    }
}

impl TenantAccessScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

impl TenantAdmissionSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ingest => "ingest",
            Self::Query => "query",
            Self::Metadata => "metadata",
            Self::Retention => "retention",
        }
    }
}

#[cfg(test)]
fn default_surface_for_access(access: TenantAccessScope) -> TenantAdmissionSurface {
    match access {
        TenantAccessScope::Read => TenantAdmissionSurface::Query,
        TenantAccessScope::Write => TenantAdmissionSurface::Ingest,
    }
}

pub fn tenant_admission_metrics_snapshot() -> TenantAdmissionMetricsSnapshot {
    TenantAdmissionMetricsSnapshot {
        read_rejections_total: TENANT_ADMISSION_READ_REJECTIONS_TOTAL.load(Ordering::Relaxed),
        write_rejections_total: TENANT_ADMISSION_WRITE_REJECTIONS_TOTAL.load(Ordering::Relaxed),
        active_reads: TENANT_ADMISSION_ACTIVE_READS.load(Ordering::Relaxed),
        active_writes: TENANT_ADMISSION_ACTIVE_WRITES.load(Ordering::Relaxed),
        ingest_rejections_total: TENANT_ADMISSION_INGEST_REJECTIONS_TOTAL.load(Ordering::Relaxed),
        ingest_active_requests: TENANT_ADMISSION_INGEST_ACTIVE_REQUESTS.load(Ordering::Relaxed),
        ingest_active_units: TENANT_ADMISSION_INGEST_ACTIVE_UNITS.load(Ordering::Relaxed),
        query_rejections_total: TENANT_ADMISSION_QUERY_REJECTIONS_TOTAL.load(Ordering::Relaxed),
        query_active_requests: TENANT_ADMISSION_QUERY_ACTIVE_REQUESTS.load(Ordering::Relaxed),
        query_active_units: TENANT_ADMISSION_QUERY_ACTIVE_UNITS.load(Ordering::Relaxed),
        metadata_rejections_total: TENANT_ADMISSION_METADATA_REJECTIONS_TOTAL
            .load(Ordering::Relaxed),
        metadata_active_requests: TENANT_ADMISSION_METADATA_ACTIVE_REQUESTS.load(Ordering::Relaxed),
        metadata_active_units: TENANT_ADMISSION_METADATA_ACTIVE_UNITS.load(Ordering::Relaxed),
        retention_rejections_total: TENANT_ADMISSION_RETENTION_REJECTIONS_TOTAL
            .load(Ordering::Relaxed),
        retention_active_requests: TENANT_ADMISSION_RETENTION_ACTIVE_REQUESTS
            .load(Ordering::Relaxed),
        retention_active_units: TENANT_ADMISSION_RETENTION_ACTIVE_UNITS.load(Ordering::Relaxed),
    }
}

pub fn public_request_access(request: &HttpRequest) -> Option<TenantAccessScope> {
    let path = request.path_without_query();
    match path {
        "/api/v1/query"
        | "/api/v1/query_range"
        | "/api/v1/series"
        | "/api/v1/labels"
        | "/api/v1/metadata"
        | "/api/v1/query_exemplars"
        | "/api/v1/read"
        | "/api/v1/status/tsdb" => Some(TenantAccessScope::Read),
        "/api/v1/write"
        | "/api/v1/import/prometheus"
        | "/write"
        | "/api/v2/write"
        | "/v1/metrics" => Some(TenantAccessScope::Write),
        _ if path.starts_with("/api/v1/label/") && path.ends_with("/values") => {
            Some(TenantAccessScope::Read)
        }
        _ => None,
    }
}

pub fn prepare_trusted_request_plan(
    registry: Option<&TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    tenant_id: &str,
    access: TenantAccessScope,
) -> Result<TenantRequestPlan, TenantRequestError> {
    validate_tenant_id(tenant_id).map_err(TenantRequestError::BadRequest)?;
    let managed_policy = managed_control_plane
        .and_then(|control_plane| control_plane.tenant_request_policy(tenant_id));
    if let Some(managed_policy) = managed_policy.as_ref() {
        managed_policy.authorize()?;
    }
    let Some(registry) = registry else {
        return Ok(TenantRequestPlan {
            tenant_id: tenant_id.to_string(),
            access,
            policy: TenantRequestPolicy::default(),
            runtime: None,
            managed_policy,
        });
    };

    let runtime = registry.runtime_for(tenant_id)?;
    Ok(TenantRequestPlan {
        tenant_id: tenant_id.to_string(),
        access,
        policy: runtime.policy.clone(),
        runtime: Some(runtime),
        managed_policy,
    })
}

#[cfg(test)]
pub fn prepare_trusted_request(
    registry: Option<&TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    tenant_id: &str,
    access: TenantAccessScope,
) -> Result<TenantRequestGuard, TenantRequestError> {
    prepare_trusted_request_plan(registry, managed_control_plane, tenant_id, access)?
        .admit(default_surface_for_access(access), 1)
}

fn track_tenant_admission_rejection(access: TenantAccessScope) {
    match access {
        TenantAccessScope::Read => {
            TENANT_ADMISSION_READ_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        TenantAccessScope::Write => {
            TENANT_ADMISSION_WRITE_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn track_tenant_surface_rejection(surface: TenantAdmissionSurface) {
    match surface {
        TenantAdmissionSurface::Ingest => {
            TENANT_ADMISSION_INGEST_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        TenantAdmissionSurface::Query => {
            TENANT_ADMISSION_QUERY_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        TenantAdmissionSurface::Metadata => {
            TENANT_ADMISSION_METADATA_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        TenantAdmissionSurface::Retention => {
            TENANT_ADMISSION_RETENTION_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn track_tenant_admission_active(access: TenantAccessScope) {
    match access {
        TenantAccessScope::Read => {
            TENANT_ADMISSION_ACTIVE_READS.fetch_add(1, Ordering::Relaxed);
        }
        TenantAccessScope::Write => {
            TENANT_ADMISSION_ACTIVE_WRITES.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn track_tenant_surface_active_requests(
    surface: TenantAdmissionSurface,
    increment: bool,
    _units: u64,
) {
    let counter = match surface {
        TenantAdmissionSurface::Ingest => &TENANT_ADMISSION_INGEST_ACTIVE_REQUESTS,
        TenantAdmissionSurface::Query => &TENANT_ADMISSION_QUERY_ACTIVE_REQUESTS,
        TenantAdmissionSurface::Metadata => &TENANT_ADMISSION_METADATA_ACTIVE_REQUESTS,
        TenantAdmissionSurface::Retention => &TENANT_ADMISSION_RETENTION_ACTIVE_REQUESTS,
    };
    if increment {
        counter.fetch_add(1, Ordering::Relaxed);
    } else {
        counter.fetch_sub(1, Ordering::Relaxed);
    }
}

fn track_tenant_surface_active_units(surface: TenantAdmissionSurface, increment: bool, units: u64) {
    let counter = match surface {
        TenantAdmissionSurface::Ingest => &TENANT_ADMISSION_INGEST_ACTIVE_UNITS,
        TenantAdmissionSurface::Query => &TENANT_ADMISSION_QUERY_ACTIVE_UNITS,
        TenantAdmissionSurface::Metadata => &TENANT_ADMISSION_METADATA_ACTIVE_UNITS,
        TenantAdmissionSurface::Retention => &TENANT_ADMISSION_RETENTION_ACTIVE_UNITS,
    };
    if increment {
        counter.fetch_add(units, Ordering::Relaxed);
    } else {
        counter.fetch_sub(units, Ordering::Relaxed);
    }
}

pub fn prepare_request_plan(
    registry: Option<&TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    request: &HttpRequest,
    tenant_id: &str,
    access: TenantAccessScope,
) -> Result<TenantRequestPlan, TenantRequestError> {
    validate_tenant_id(tenant_id).map_err(TenantRequestError::BadRequest)?;
    let managed_policy = managed_control_plane
        .and_then(|control_plane| control_plane.tenant_request_policy(tenant_id));
    let Some(registry) = registry else {
        if let Some(managed_policy) = managed_policy.as_ref() {
            managed_policy.authorize()?;
        }
        return Ok(TenantRequestPlan {
            tenant_id: tenant_id.to_string(),
            access,
            policy: TenantRequestPolicy::default(),
            runtime: None,
            managed_policy,
        });
    };
    // Authorize against the immutable template before a cache insertion. Invalid credentials must
    // retain their established 401/403 result and must not consume the finite runtime cardinality.
    registry.authorize(tenant_id, request, access)?;
    if let Some(managed_policy) = managed_policy.as_ref() {
        managed_policy.authorize()?;
    }
    let runtime = registry.runtime_for(tenant_id)?;
    // The materialized runtime owns the same normalized token map. Keep the compatibility check
    // here so future template/runtime changes cannot silently drift authorization behavior.
    runtime.authorize(request, access)?;
    Ok(TenantRequestPlan {
        tenant_id: tenant_id.to_string(),
        access,
        policy: runtime.policy.clone(),
        runtime: Some(runtime),
        managed_policy,
    })
}

#[cfg(test)]
pub fn prepare_request(
    registry: Option<&TenantRegistry>,
    managed_control_plane: Option<&ManagedControlPlane>,
    request: &HttpRequest,
    tenant_id: &str,
    access: TenantAccessScope,
) -> Result<TenantRequestGuard, TenantRequestError> {
    prepare_request_plan(registry, managed_control_plane, request, tenant_id, access)?
        .admit(default_surface_for_access(access), 1)
}

pub fn enforce_write_rows_quota(
    policy: &TenantRequestPolicy,
    row_count: usize,
) -> Result<(), String> {
    let Some(limit) = policy.max_write_rows_per_request else {
        return Ok(());
    };
    if row_count > limit {
        return Err(format!(
            "tenant write rows per request limit exceeded: {row_count} > {limit}"
        ));
    }
    Ok(())
}

pub fn enforce_read_queries_quota(
    policy: &TenantRequestPolicy,
    query_count: usize,
) -> Result<(), String> {
    let Some(limit) = policy.max_read_queries_per_request else {
        return Ok(());
    };
    if query_count > limit {
        return Err(format!(
            "tenant remote-read query limit exceeded: {query_count} > {limit}"
        ));
    }
    Ok(())
}

pub fn enforce_metadata_matchers_quota(
    policy: &TenantRequestPolicy,
    matcher_count: usize,
) -> Result<(), String> {
    let Some(limit) = policy.max_metadata_matchers_per_request else {
        return Ok(());
    };
    if matcher_count > limit {
        return Err(format!(
            "tenant metadata matcher limit exceeded: {matcher_count} > {limit}"
        ));
    }
    Ok(())
}

pub fn enforce_query_length_quota(policy: &TenantRequestPolicy, query: &str) -> Result<(), String> {
    let Some(limit) = policy.max_query_length_bytes else {
        return Ok(());
    };
    if query.len() > limit {
        return Err(format!(
            "tenant query length limit exceeded: {} > {limit}",
            query.len()
        ));
    }
    Ok(())
}

pub fn enforce_range_points_quota(
    policy: &TenantRequestPolicy,
    start: i64,
    end: i64,
    step: i64,
) -> Result<(), String> {
    let Some(limit) = policy.max_range_points_per_query else {
        return Ok(());
    };
    let span = end.saturating_sub(start);
    let steps = span.checked_div(step).unwrap_or(i64::MAX);
    let points = usize::try_from(steps.saturating_add(1)).unwrap_or(usize::MAX);
    if points > limit {
        return Err(format!(
            "tenant range query point limit exceeded: {points} > {limit}"
        ));
    }
    Ok(())
}

fn parse_auth_tokens(
    auth: &TenantAuthDefinition,
) -> Result<BTreeMap<String, BTreeSet<TenantAccessScope>>, String> {
    let mut tokens = BTreeMap::<String, BTreeSet<TenantAccessScope>>::new();
    for (index, token) in auth.tokens.iter().enumerate() {
        let raw_token = token.token.trim();
        if raw_token.is_empty() {
            return Err(format!(
                "tenant auth token at index {index} must not be empty"
            ));
        }
        let scopes = if token.scopes.is_empty() {
            BTreeSet::from([TenantAccessScope::Read, TenantAccessScope::Write])
        } else {
            token.scopes.iter().copied().collect()
        };
        tokens
            .entry(raw_token.to_string())
            .or_default()
            .extend(scopes);
    }
    Ok(tokens)
}

fn merge_positive_limit(
    incoming: Option<usize>,
    existing: Option<usize>,
    field_name: &str,
) -> Result<Option<usize>, String> {
    match incoming {
        Some(0) => Err(format!("{field_name} must be greater than zero when set")),
        Some(value) => Ok(Some(value)),
        None => Ok(existing),
    }
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn bearer_token(request: &HttpRequest) -> Option<&str> {
    let authorization = request.header("authorization")?;
    let (scheme, token) = authorization.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

fn tenant_header_value(request: &HttpRequest) -> Result<Option<&str>, String> {
    let tenant_id = request.header(TENANT_HEADER).map(str::trim);
    let scope_org_id = request.header(SCOPE_ORG_ID_HEADER).map(str::trim);
    match (tenant_id, scope_org_id) {
        (Some(tenant_id), Some(scope_org_id)) if tenant_id != scope_org_id => Err(format!(
            "{TENANT_HEADER} and {SCOPE_ORG_ID_HEADER} must match when both headers are set"
        )),
        (Some(tenant_id), _) => Ok(Some(tenant_id)),
        (None, Some(scope_org_id)) => Ok(Some(scope_org_id)),
        (None, None) => Ok(None),
    }
}

pub fn tenant_id_for_request(request: &HttpRequest) -> Result<String, String> {
    let tenant_id = tenant_header_value(request)?
        .unwrap_or(DEFAULT_TENANT_ID)
        .trim();
    validate_tenant_id(tenant_id)?;
    Ok(tenant_id.to_string())
}

pub fn scope_rows_for_tenant(rows: Vec<Row>, tenant_id: &str) -> Result<Vec<Row>, String> {
    validate_tenant_id(tenant_id)?;
    rows.into_iter()
        .map(|row| {
            ensure_reserved_label_not_present(row.labels()).map_err(|err| err.to_string())?;
            let mut labels = row.labels().to_vec();
            labels.push(Label::new(TENANT_LABEL, tenant_id));
            Ok(Row::with_labels(
                row.metric().to_string(),
                labels,
                row.data_point().clone(),
            ))
        })
        .collect()
}

pub fn selection_for_tenant(
    selection: &SeriesSelection,
    tenant_id: &str,
) -> Result<SeriesSelection, String> {
    read_selections_for_tenant(selection, tenant_id).map(|mut selections| {
        selections
            .drain(..1)
            .next()
            .expect("tenant reads always produce a scoped primary selection")
    })
}

pub fn read_selections_for_tenant(
    selection: &SeriesSelection,
    tenant_id: &str,
) -> Result<Vec<SeriesSelection>, String> {
    validate_tenant_id(tenant_id)?;
    ensure_reserved_matcher_not_present(&selection.matchers)?;

    let mut selections = Vec::with_capacity(1 + usize::from(tenant_id == DEFAULT_TENANT_ID));
    let mut scoped = selection.clone();
    scoped
        .matchers
        .push(SeriesMatcher::equal(TENANT_LABEL, tenant_id));
    selections.push(scoped);

    if tenant_id == DEFAULT_TENANT_ID {
        let mut legacy_fallback = selection.clone();
        legacy_fallback.matchers.push(SeriesMatcher::regex_no_match(
            TENANT_LABEL,
            UNLABELED_TENANT_FALLBACK_REGEX,
        ));
        selections.push(legacy_fallback);
    }

    Ok(selections)
}

pub fn visible_metric_series(series: MetricSeries, tenant_id: &str) -> Option<MetricSeries> {
    let labels = visible_labels(series.labels, tenant_id)?;
    Some(MetricSeries {
        name: series.name,
        labels,
    })
}

pub fn scoped_storage(inner: Arc<dyn Storage>, tenant_id: impl Into<String>) -> Arc<dyn Storage> {
    Arc::new(TenantScopedStorage::new(inner, tenant_id.into()))
}

#[derive(Debug)]
pub(crate) enum TenantDeleteSeriesError {
    Storage(TsinkError),
    Partial {
        source: TsinkError,
        matched_series: u64,
        tombstones_applied: u64,
    },
}

impl TenantDeleteSeriesError {
    fn into_source(self) -> TsinkError {
        match self {
            Self::Storage(source) | Self::Partial { source, .. } => source,
        }
    }
}

pub(crate) fn delete_series_with_progress(
    inner: &Arc<dyn Storage>,
    tenant_id: &str,
    selection: &SeriesSelection,
    observer: &mut dyn FnMut(DeleteSeriesResult),
) -> Result<DeleteSeriesResult, TenantDeleteSeriesError> {
    TenantScopedStorage::new(Arc::clone(inner), tenant_id.to_string())
        .delete_series_with_progress(selection, observer)
}

fn exact_selection_for_series(
    series: &MetricSeries,
    time_range: Option<(i64, i64)>,
) -> SeriesSelection {
    let mut selection = SeriesSelection::new().with_metric(series.name.clone());
    for label in &series.labels {
        selection = selection.with_matcher(SeriesMatcher::equal(&label.name, &label.value));
    }
    if let Some((start, end)) = time_range {
        selection = selection.with_time_range(start, end);
    }
    selection
}

fn validate_tenant_id(tenant_id: &str) -> Result<(), String> {
    if tenant_id.is_empty() {
        return Err(format!("{TENANT_HEADER} must not be empty"));
    }
    if tenant_id.len() > tsink::label::MAX_LABEL_VALUE_LEN {
        return Err(format!(
            "{TENANT_HEADER} must be <= {} bytes",
            tsink::label::MAX_LABEL_VALUE_LEN
        ));
    }
    if tenant_id.chars().any(char::is_control) {
        return Err(format!(
            "{TENANT_HEADER} must not contain control characters"
        ));
    }
    Ok(())
}

fn ensure_reserved_label_not_present(labels: &[Label]) -> TsinkResult<()> {
    if labels.iter().any(|label| label.name == TENANT_LABEL) {
        return Err(TsinkError::InvalidLabel(format!(
            "label '{TENANT_LABEL}' is reserved for server-managed tenant isolation"
        )));
    }
    Ok(())
}

fn ensure_reserved_matcher_not_present(matchers: &[SeriesMatcher]) -> Result<(), String> {
    if matchers.iter().any(|matcher| matcher.name == TENANT_LABEL) {
        return Err(format!(
            "matcher '{TENANT_LABEL}' is reserved for server-managed tenant isolation"
        ));
    }
    Ok(())
}

fn visible_labels(labels: Vec<Label>, tenant_id: &str) -> Option<Vec<Label>> {
    let mut visible = Vec::with_capacity(labels.len());
    let mut matched = false;

    for label in labels {
        if label.name == TENANT_LABEL {
            if label.value != tenant_id {
                return None;
            }
            matched = true;
            continue;
        }
        visible.push(label);
    }

    if matched || tenant_id == DEFAULT_TENANT_ID {
        Some(visible)
    } else {
        None
    }
}

fn validate_inner_write_batch_result(
    expected_rows: usize,
    mode: WriteMode,
    result: &BatchWriteResult,
) -> TsinkResult<()> {
    let malformed = |detail: String| {
        TsinkError::Other(format!(
            "tenant-scoped storage received malformed canonical batch result: {detail}"
        ))
    };

    if result.submitted != expected_rows {
        return Err(malformed(format!(
            "submitted count {} does not match {expected_rows} forwarded rows",
            result.submitted
        )));
    }
    if result.outcomes.len() != expected_rows {
        return Err(malformed(format!(
            "outcome count {} does not match {expected_rows} forwarded rows",
            result.outcomes.len()
        )));
    }

    let mut accepted = 0usize;
    let mut rejected = 0usize;
    for (position, outcome) in result.outcomes.iter().enumerate() {
        if outcome.index != position {
            return Err(malformed(format!(
                "outcome at position {position} reported index {}",
                outcome.index
            )));
        }
        match &outcome.status {
            RowWriteStatus::Accepted => accepted += 1,
            RowWriteStatus::Rejected(rejection) => {
                rejected += 1;
                if let Some(cause_index) = rejection.cause_index {
                    if cause_index >= expected_rows {
                        return Err(malformed(format!(
                            "rejection at index {position} reported out-of-range cause index {cause_index}"
                        )));
                    }
                }
            }
            _ => {
                return Err(malformed(format!(
                    "outcome at position {position} used an unsupported status"
                )));
            }
        }
    }

    if result.accepted != accepted || result.rejected != rejected {
        return Err(malformed(format!(
            "reported counts accepted={} rejected={} do not match outcomes accepted={accepted} rejected={rejected}",
            result.accepted, result.rejected
        )));
    }
    if result.acknowledgement.is_some() != (accepted > 0) {
        return Err(malformed(format!(
            "acknowledgement presence does not match accepted count {accepted}"
        )));
    }
    if mode == WriteMode::Atomic && accepted > 0 && rejected > 0 {
        return Err(malformed(format!(
            "atomic result mixed {accepted} accepted and {rejected} rejected rows"
        )));
    }

    Ok(())
}

#[derive(Clone)]
struct TenantScopedStorage {
    inner: Arc<dyn Storage>,
    tenant_id: String,
}

impl TenantScopedStorage {
    fn new(inner: Arc<dyn Storage>, tenant_id: String) -> Self {
        Self { inner, tenant_id }
    }

    fn is_default_tenant(&self) -> bool {
        self.tenant_id == DEFAULT_TENANT_ID
    }

    fn scoped_labels(&self, labels: &[Label]) -> TsinkResult<Vec<Label>> {
        ensure_reserved_label_not_present(labels)?;
        let mut scoped = labels.to_vec();
        scoped.push(Label::new(TENANT_LABEL, self.tenant_id.clone()));
        Ok(scoped)
    }

    fn scoped_query_options(&self, opts: QueryOptions) -> TsinkResult<QueryOptions> {
        let mut scoped = opts;
        scoped.labels = self.scoped_labels(&scoped.labels)?;
        Ok(scoped)
    }

    fn scoped_selection(&self, selection: &SeriesSelection) -> TsinkResult<SeriesSelection> {
        selection_for_tenant(selection, &self.tenant_id)
            .map_err(|err| TsinkError::InvalidLabel(err.to_string()))
    }

    fn read_selections(&self, selection: &SeriesSelection) -> TsinkResult<Vec<SeriesSelection>> {
        read_selections_for_tenant(selection, &self.tenant_id)
            .map_err(|err| TsinkError::InvalidLabel(err.to_string()))
    }

    fn read_series(&self, selection: &SeriesSelection) -> TsinkResult<Vec<MetricSeries>> {
        let mut merged = Vec::new();
        for scoped in self.read_selections(selection)? {
            merged.append(&mut self.inner.select_series(&scoped)?);
        }
        merged.sort_unstable();
        merged.dedup();
        Ok(merged)
    }

    fn read_series_with_execution(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<MetricSeries>> {
        self.read_series_with_execution_result(selection, execution)
            .map(SelectSeriesExecutionResult::into_series)
    }

    fn read_series_with_execution_result(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectSeriesExecutionResult> {
        self.read_series_with_optional_scope_with_execution_result(selection, None, execution)
    }

    fn read_series_in_shards(
        &self,
        selection: &SeriesSelection,
        scope: &MetadataShardScope,
    ) -> TsinkResult<Vec<MetricSeries>> {
        let mut merged = BTreeSet::new();
        for scoped in self.read_selections(selection)? {
            merged.extend(self.inner.select_series_in_shards(&scoped, scope)?);
        }
        Ok(merged.into_iter().collect())
    }

    fn read_series_in_shards_with_execution_result(
        &self,
        selection: &SeriesSelection,
        scope: &MetadataShardScope,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectSeriesExecutionResult> {
        self.read_series_with_optional_scope_with_execution_result(
            selection,
            Some(scope),
            execution,
        )
    }

    fn read_series_with_optional_scope_with_execution_result(
        &self,
        selection: &SeriesSelection,
        scope: Option<&MetadataShardScope>,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectSeriesExecutionResult> {
        let mut merged = Vec::new();
        let mut reservation = execution.reserve_memory(0).map_err(TsinkError::from)?;
        let inner_accounting = match scope {
            Some(_) => self.inner.select_series_in_shards_execution_accounting(),
            None => self.inner.select_series_execution_accounting(),
        };
        for scoped in self.read_selections(selection)? {
            execution.checkpoint().map_err(TsinkError::from)?;
            let mut selected_result = match scope {
                Some(scope) => self
                    .inner
                    .select_series_in_shards_with_execution_result(&scoped, scope, execution)?,
                None => self
                    .inner
                    .select_series_with_execution_result(&scoped, execution)?,
            };
            execution.checkpoint().map_err(TsinkError::from)?;
            if inner_accounting == QueryExecutionAccounting::Complete
                && selected_result.reserved_memory_bytes() == 0
                && !selected_result.series.is_empty()
            {
                return Err(TsinkError::Other(
                    "completely accounted tenant metadata read omitted its result reservation"
                        .to_string(),
                ));
            }
            let mut selected = std::mem::take(&mut selected_result.series);
            if selected.is_empty() {
                continue;
            }

            let desired_len = merged.len().checked_add(selected.len()).ok_or_else(|| {
                TsinkError::Other(
                    "tenant metadata merge length exceeds the supported range".to_string(),
                )
            })?;
            execution
                .observe_intermediate_vector_size(u64::try_from(desired_len).unwrap_or(u64::MAX))
                .map_err(TsinkError::from)?;

            if merged.is_empty() {
                reservation
                    .resize(tenant_query_metric_series_retained_bytes(
                        &selected,
                        selected.capacity(),
                    ))
                    .map_err(TsinkError::from)?;
                merged = selected;
                drop(selected_result);
                continue;
            }

            let current_retained =
                tenant_query_metric_series_retained_bytes(&merged, merged.capacity());
            let selected_retained =
                tenant_query_metric_series_retained_bytes(&selected, selected.capacity());
            let projected_growth_buffer = if desired_len > merged.capacity() {
                tenant_query_vec_capacity_bytes::<MetricSeries>(
                    projected_tenant_query_vec_capacity(merged.capacity(), desired_len),
                )
            } else {
                0
            };
            reservation
                .resize(
                    current_retained
                        .saturating_add(selected_retained)
                        .saturating_add(projected_growth_buffer),
                )
                .map_err(TsinkError::from)?;
            merged.try_reserve(selected.len()).map_err(|err| {
                TsinkError::Other(format!(
                    "failed to reserve tenant metadata merge output: {err}"
                ))
            })?;
            merged.append(&mut selected);
            drop(selected);
            drop(selected_result);
            reservation
                .resize(tenant_query_metric_series_retained_bytes(
                    &merged,
                    merged.capacity(),
                ))
                .map_err(TsinkError::from)?;
        }
        merged.sort_unstable();
        merged.dedup();
        reservation
            .resize(tenant_query_metric_series_retained_bytes(
                &merged,
                merged.capacity(),
            ))
            .map_err(TsinkError::from)?;
        Ok(match inner_accounting {
            QueryExecutionAccounting::Complete => {
                SelectSeriesExecutionResult::accounted(merged, reservation)
            }
            QueryExecutionAccounting::Unaccounted => {
                SelectSeriesExecutionResult::reserved_unaccounted(merged, reservation)
            }
        })
    }

    fn visible_series(&self, mut series: Vec<MetricSeries>) -> Vec<MetricSeries> {
        series.retain_mut(|series| {
            let mut matched = false;
            let mut wrong_tenant = false;
            series.labels.retain(|label| {
                if label.name != TENANT_LABEL {
                    return true;
                }
                if label.value == self.tenant_id {
                    matched = true;
                } else {
                    wrong_tenant = true;
                }
                false
            });
            !wrong_tenant && (matched || self.is_default_tenant())
        });
        series.sort_unstable();
        series.dedup();
        series
    }

    fn visible_selected_points(
        &self,
        rows: Vec<SeriesPoints>,
    ) -> Vec<(Vec<Label>, Vec<DataPoint>)> {
        let mut merged = BTreeMap::<Vec<Label>, Vec<DataPoint>>::new();
        for SeriesPoints { series, points } in rows {
            if points.is_empty() {
                continue;
            }
            let Some(labels) = visible_labels(series.labels, &self.tenant_id) else {
                continue;
            };
            merged.entry(labels).or_default().extend(points);
        }
        merged
            .into_iter()
            .map(|(labels, mut points)| {
                points.sort_by_key(|point| point.timestamp);
                (labels, points)
            })
            .collect()
    }

    fn delete_series_with_progress(
        &self,
        selection: &SeriesSelection,
        observer: &mut dyn FnMut(DeleteSeriesResult),
    ) -> Result<DeleteSeriesResult, TenantDeleteSeriesError> {
        if self.is_default_tenant() {
            let time_range = match (selection.start, selection.end) {
                (Some(start), Some(end)) => Some((start, end)),
                _ => None,
            };
            let series = self
                .read_series(selection)
                .map_err(TenantDeleteSeriesError::Storage)?;
            let mut matched_series = 0u64;
            let mut tombstones_applied = 0u64;
            for series in series {
                match self
                    .inner
                    .delete_series(&exact_selection_for_series(&series, time_range))
                {
                    Ok(outcome) => {
                        matched_series = matched_series.saturating_add(outcome.matched_series);
                        tombstones_applied =
                            tombstones_applied.saturating_add(outcome.tombstones_applied);
                        observer(outcome);
                    }
                    Err(source) if tombstones_applied > 0 => {
                        return Err(TenantDeleteSeriesError::Partial {
                            source,
                            matched_series,
                            tombstones_applied,
                        });
                    }
                    Err(source) => return Err(TenantDeleteSeriesError::Storage(source)),
                }
            }
            return Ok(DeleteSeriesResult {
                matched_series,
                tombstones_applied,
            });
        }
        let scoped = self
            .scoped_selection(selection)
            .map_err(TenantDeleteSeriesError::Storage)?;
        let outcome = self
            .inner
            .delete_series(&scoped)
            .map_err(TenantDeleteSeriesError::Storage)?;
        observer(outcome);
        Ok(outcome)
    }
}

impl Storage for TenantScopedStorage {
    fn query_budget(&self) -> Option<QueryBudget> {
        self.inner.query_budget()
    }

    fn status_observability_snapshot_with_execution(
        &self,
        execution: &QueryExecution,
    ) -> TsinkResult<tsink::StorageStatusObservabilitySnapshot> {
        self.inner
            .status_observability_snapshot_with_execution(execution)
    }

    fn metrics_observability_snapshot_with_execution(
        &self,
        execution: &QueryExecution,
    ) -> TsinkResult<tsink::StorageMetricsObservabilitySnapshot> {
        self.inner
            .metrics_observability_snapshot_with_execution(execution)
    }

    fn insert_rows(&self, rows: &[Row]) -> TsinkResult<()> {
        let scoped = scope_rows_for_tenant(rows.to_vec(), &self.tenant_id)
            .map_err(TsinkError::InvalidLabel)?;
        self.inner.insert_rows(&scoped)
    }

    fn write_batch(&self, rows: &[Row], mode: WriteMode) -> TsinkResult<BatchWriteResult> {
        validate_tenant_id(&self.tenant_id).map_err(TsinkError::InvalidLabel)?;

        let reserved_label_message =
            || format!("label '{TENANT_LABEL}' is reserved for server-managed tenant isolation");
        let reserved_label_indices = rows
            .iter()
            .enumerate()
            .filter_map(|(index, row)| {
                row.labels()
                    .iter()
                    .any(|label| label.name == TENANT_LABEL)
                    .then_some(index)
            })
            .collect::<Vec<_>>();

        match mode {
            WriteMode::Atomic if !reserved_label_indices.is_empty() => {
                let cause_index = reserved_label_indices[0];
                let rejection = WriteRejection::new(
                    WriteRejectionCategory::InvalidLabels,
                    Some(cause_index),
                    reserved_label_message(),
                );
                Ok(BatchWriteResult::from_outcomes(
                    None,
                    (0..rows.len())
                        .map(|index| RowWriteOutcome::rejected(index, rejection.clone()))
                        .collect(),
                ))
            }
            WriteMode::BestEffort if !reserved_label_indices.is_empty() => {
                let mut forwarded_indices =
                    Vec::with_capacity(rows.len().saturating_sub(reserved_label_indices.len()));
                let mut forwarded_rows = Vec::with_capacity(forwarded_indices.capacity());
                let mut outcomes = vec![None; rows.len()];

                for (index, row) in rows.iter().enumerate() {
                    if row.labels().iter().any(|label| label.name == TENANT_LABEL) {
                        outcomes[index] = Some(RowWriteOutcome::rejected(
                            index,
                            WriteRejection::new(
                                WriteRejectionCategory::InvalidLabels,
                                Some(index),
                                reserved_label_message(),
                            ),
                        ));
                    } else {
                        forwarded_indices.push(index);
                        forwarded_rows.push(row.clone());
                    }
                }

                let acknowledgement = if forwarded_rows.is_empty() {
                    None
                } else {
                    let scoped = scope_rows_for_tenant(forwarded_rows, &self.tenant_id)
                        .map_err(TsinkError::InvalidLabel)?;
                    let inner = self.inner.write_batch(&scoped, mode)?;
                    validate_inner_write_batch_result(scoped.len(), mode, &inner)?;

                    for mut outcome in inner.outcomes {
                        let forwarded_index = outcome.index;
                        let original_index = forwarded_indices.get(forwarded_index).copied().ok_or_else(
                            || {
                                TsinkError::Other(format!(
                                    "tenant-scoped batch received invalid inner outcome index {forwarded_index} for {} forwarded rows",
                                    forwarded_indices.len()
                                ))
                            },
                        )?;
                        outcome.index = original_index;
                        if let RowWriteStatus::Rejected(rejection) = &mut outcome.status {
                            if let Some(cause_index) = rejection.cause_index {
                                rejection.cause_index = Some(
                                    forwarded_indices.get(cause_index).copied().ok_or_else(|| {
                                        TsinkError::Other(format!(
                                            "tenant-scoped batch received invalid inner cause index {cause_index} for {} forwarded rows",
                                            forwarded_indices.len()
                                        ))
                                    })?,
                                );
                            }
                        }
                        if outcomes[original_index].replace(outcome).is_some() {
                            return Err(TsinkError::Other(format!(
                                "tenant-scoped batch received duplicate inner outcome for original row {original_index}"
                            )));
                        }
                    }
                    inner.acknowledgement
                };

                let outcomes = outcomes
                    .into_iter()
                    .enumerate()
                    .map(|(index, outcome)| {
                        outcome.ok_or_else(|| {
                            TsinkError::Other(format!(
                                "tenant-scoped batch received no inner outcome for original row {index}"
                            ))
                        })
                    })
                    .collect::<TsinkResult<Vec<_>>>()?;
                Ok(BatchWriteResult::from_outcomes(acknowledgement, outcomes))
            }
            _ => {
                let scoped = scope_rows_for_tenant(rows.to_vec(), &self.tenant_id)
                    .map_err(TsinkError::InvalidLabel)?;
                let result = self.inner.write_batch(&scoped, mode)?;
                validate_inner_write_batch_result(scoped.len(), mode, &result)?;
                Ok(result)
            }
        }
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> TsinkResult<Vec<DataPoint>> {
        let scoped_labels = self.scoped_labels(labels)?;
        match self.inner.select(metric, &scoped_labels, start, end) {
            Ok(points) => {
                if !points.is_empty() || !self.is_default_tenant() {
                    Ok(points)
                } else {
                    self.inner.select(metric, labels, start, end)
                }
            }
            Err(TsinkError::NoDataPoints { .. }) if self.is_default_tenant() => {
                self.inner.select(metric, labels, start, end)
            }
            Err(err) => Err(err),
        }
    }

    fn select_with_execution(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<DataPoint>> {
        let scoped_labels = self.scoped_labels(labels)?;
        match self
            .inner
            .select_with_execution(metric, &scoped_labels, start, end, execution)
        {
            Ok(points) => {
                if !points.is_empty() || !self.is_default_tenant() {
                    Ok(points)
                } else {
                    self.inner
                        .select_with_execution(metric, labels, start, end, execution)
                }
            }
            Err(TsinkError::NoDataPoints { .. }) if self.is_default_tenant() => self
                .inner
                .select_with_execution(metric, labels, start, end, execution),
            Err(err) => Err(err),
        }
    }

    fn select_many_with_execution(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<SeriesPoints>> {
        self.select_many_with_execution_result(series, start, end, execution)
            .map(SelectManyExecutionResult::into_series)
    }

    fn select_many_with_execution_result(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectManyExecutionResult> {
        execution.checkpoint().map_err(TsinkError::from)?;
        execution
            .observe_intermediate_vector_size(u64::try_from(series.len()).unwrap_or(u64::MAX))
            .map_err(TsinkError::from)?;
        let inner_accounting = self.inner.select_many_execution_accounting();
        let scoped_peak_bytes = tenant_query_scoped_series_peak_bytes(series, &self.tenant_id);
        let mut wrapper_reservation = execution
            .reserve_memory(scoped_peak_bytes)
            .map_err(TsinkError::from)?;
        let scoped = series
            .iter()
            .map(|item| {
                Ok(MetricSeries {
                    name: item.name.clone(),
                    labels: self.scoped_labels(&item.labels)?,
                })
            })
            .collect::<TsinkResult<Vec<_>>>()?;
        execution.checkpoint().map_err(TsinkError::from)?;
        let mut scoped_result = self
            .inner
            .select_many_with_execution_result(&scoped, start, end, execution)?;
        execution.checkpoint().map_err(TsinkError::from)?;
        if inner_accounting == QueryExecutionAccounting::Complete
            && scoped_result.reserved_memory_bytes() == 0
            && !scoped_result.series.is_empty()
        {
            return Err(TsinkError::Other(
                "completely accounted tenant batch read omitted its result reservation".to_string(),
            ));
        }
        let scoped_matched_bytes = scoped_result
            .matched_selectors
            .as_ref()
            .map_or(0, |matched| {
                tenant_query_vec_capacity_bytes::<bool>(matched.capacity())
            });
        let selected_retained_bytes =
            crate::cluster::query::modeled_series_points_vec_retained_bytes(&scoped_result.series);
        wrapper_reservation
            .resize(
                scoped_peak_bytes
                    .saturating_add(selected_retained_bytes)
                    .saturating_add(scoped_matched_bytes),
            )
            .map_err(TsinkError::from)?;
        if scoped_result.series.len() != series.len() {
            return Err(TsinkError::Other(format!(
                "tenant-scoped batch read received {} inner results for {} requested series",
                scoped_result.series.len(),
                series.len()
            )));
        }
        if scoped_result
            .series
            .iter()
            .zip(&scoped)
            .any(|(item, selector)| item.series != *selector)
        {
            return Err(TsinkError::Other(
                "tenant-scoped batch read returned identities outside requested order".to_string(),
            ));
        }
        let mut matched_selectors = match inner_accounting {
            QueryExecutionAccounting::Complete => {
                let matched = scoped_result.matched_selectors.take().ok_or_else(|| {
                    TsinkError::Other(
                        "completely accounted tenant batch read omitted selector-existence bits"
                            .to_string(),
                    )
                })?;
                if matched.len() != series.len() {
                    return Err(TsinkError::Other(format!(
                        "tenant-scoped batch read received {} existence bits for {} requested series",
                        matched.len(),
                        series.len()
                    )));
                }
                Some(matched)
            }
            QueryExecutionAccounting::Unaccounted => None,
        };
        let mut selected = std::mem::take(&mut scoped_result.series);
        drop(scoped_result);

        if self.is_default_tenant() {
            let fallback_collection_bytes =
                tenant_query_vec_capacity_bytes::<usize>(series.len()).saturating_add(
                    tenant_query_metric_series_retained_bytes(series, series.len()),
                );
            let retained_before_fallback = scoped_peak_bytes
                .saturating_add(selected_retained_bytes)
                .saturating_add(fallback_collection_bytes);
            wrapper_reservation
                .resize(retained_before_fallback)
                .map_err(TsinkError::from)?;
            let fallback_indices = selected
                .iter()
                .enumerate()
                .filter_map(|(index, item)| {
                    let missing = matched_selectors
                        .as_ref()
                        .map_or_else(|| item.points.is_empty(), |matched| !matched[index]);
                    missing.then_some(index)
                })
                .collect::<Vec<_>>();
            if !fallback_indices.is_empty() {
                execution
                    .observe_intermediate_vector_size(
                        u64::try_from(fallback_indices.len()).unwrap_or(u64::MAX),
                    )
                    .map_err(TsinkError::from)?;
                let fallback_series = fallback_indices
                    .iter()
                    .map(|index| series[*index].clone())
                    .collect::<Vec<_>>();
                let mut fallback_result = self.inner.select_many_with_execution_result(
                    &fallback_series,
                    start,
                    end,
                    execution,
                )?;
                execution.checkpoint().map_err(TsinkError::from)?;
                let fallback_matched_bytes = fallback_result
                    .matched_selectors
                    .as_ref()
                    .map_or(0, |matched| {
                        tenant_query_vec_capacity_bytes::<bool>(matched.capacity())
                    });
                wrapper_reservation
                    .resize(
                        retained_before_fallback
                            .saturating_add(
                                crate::cluster::query::modeled_series_points_vec_retained_bytes(
                                    &fallback_result.series,
                                ),
                            )
                            .saturating_add(fallback_matched_bytes),
                    )
                    .map_err(TsinkError::from)?;
                if fallback_result.series.len() != fallback_indices.len() {
                    return Err(TsinkError::Other(format!(
                        "default-tenant fallback read received {} inner results for {} requested series",
                        fallback_result.series.len(),
                        fallback_indices.len()
                    )));
                }
                if fallback_result
                    .series
                    .iter()
                    .zip(&fallback_series)
                    .any(|(item, selector)| item.series != *selector)
                {
                    return Err(TsinkError::Other(
                        "default-tenant fallback read returned identities outside requested order"
                            .to_string(),
                    ));
                }
                let fallback_matched = match inner_accounting {
                    QueryExecutionAccounting::Complete => {
                        let matched =
                            fallback_result.matched_selectors.take().ok_or_else(|| {
                                TsinkError::Other(
                                    "completely accounted default-tenant fallback omitted selector-existence bits"
                                        .to_string(),
                                )
                            })?;
                        if matched.len() != fallback_indices.len() {
                            return Err(TsinkError::Other(format!(
                                "default-tenant fallback read received {} existence bits for {} requested series",
                                matched.len(),
                                fallback_indices.len()
                            )));
                        }
                        Some(matched)
                    }
                    QueryExecutionAccounting::Unaccounted => None,
                };
                let fallback = std::mem::take(&mut fallback_result.series);
                drop(fallback_result);
                for (fallback_position, (index, fallback_item)) in
                    fallback_indices.into_iter().zip(fallback).enumerate()
                {
                    let fallback_exists = fallback_matched.as_ref().map_or_else(
                        || !fallback_item.points.is_empty(),
                        |matched| matched[fallback_position],
                    );
                    if fallback_exists {
                        selected[index].points = fallback_item.points;
                    }
                    if let Some(matched) = matched_selectors.as_mut() {
                        matched[index] = fallback_exists;
                    }
                }
            }
        }

        let final_output_identity_bytes =
            tenant_query_vec_capacity_bytes::<SeriesPoints>(series.len())
                .saturating_add(tenant_query_metric_series_identity_bytes(series));
        wrapper_reservation
            .resize(
                wrapper_reservation
                    .bytes()
                    .saturating_add(final_output_identity_bytes),
            )
            .map_err(TsinkError::from)?;
        let output = series
            .iter()
            .cloned()
            .zip(selected)
            .map(|(series, selected)| SeriesPoints {
                series,
                points: selected.points,
            })
            .collect::<Vec<_>>();
        drop(scoped);
        let output_retained_bytes =
            crate::cluster::query::modeled_series_points_vec_retained_bytes(&output)
                .saturating_add(matched_selectors.as_ref().map_or(0, |matched| {
                    tenant_query_vec_capacity_bytes::<bool>(matched.capacity())
                }));
        wrapper_reservation
            .resize(output_retained_bytes)
            .map_err(TsinkError::from)?;
        Ok(match matched_selectors {
            Some(matched_selectors) => {
                SelectManyExecutionResult::accounted(output, matched_selectors, wrapper_reservation)
            }
            None => SelectManyExecutionResult::reserved_unaccounted(output, wrapper_reservation),
        })
    }

    fn select_many_execution_accounting(&self) -> QueryExecutionAccounting {
        self.inner.select_many_execution_accounting()
    }

    fn scan_series_rows_with_execution(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        execution: &QueryExecution,
    ) -> TsinkResult<QueryRowsPage> {
        self.scan_series_rows_with_execution_result(series, start, end, options, execution)
            .map(QueryRowsExecutionResult::into_page)
    }

    fn scan_series_rows_with_execution_result(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        execution: &QueryExecution,
    ) -> TsinkResult<QueryRowsExecutionResult> {
        if self.scan_series_rows_execution_accounting() != QueryExecutionAccounting::Complete {
            return self
                .scan_series_rows(series, start, end, options)
                .map(QueryRowsExecutionResult::unaccounted);
        }
        execution.checkpoint().map_err(TsinkError::from)?;
        execution
            .observe_intermediate_vector_size(u64::try_from(series.len()).unwrap_or(u64::MAX))
            .map_err(TsinkError::from)?;
        let scoped_peak_bytes = tenant_query_scoped_series_peak_bytes(series, &self.tenant_id);
        let scoped_reservation = execution
            .reserve_memory(scoped_peak_bytes)
            .map_err(TsinkError::from)?;
        let scoped = series
            .iter()
            .map(|item| {
                Ok(MetricSeries {
                    name: item.name.clone(),
                    labels: self.scoped_labels(&item.labels)?,
                })
            })
            .collect::<TsinkResult<Vec<_>>>()?;
        execution.checkpoint().map_err(TsinkError::from)?;
        let inner_result = self
            .inner
            .scan_series_rows_with_execution_result(&scoped, start, end, options, execution)?;
        execution.checkpoint().map_err(TsinkError::from)?;

        let source_bytes = tsink::modeled_query_rows_retained_bytes(&inner_result.page.rows);
        if inner_result.reserved_memory_bytes() < source_bytes {
            return Err(TsinkError::Other(format!(
                "completely accounted tenant row scan retained {} bytes for a {}-byte result",
                inner_result.reserved_memory_bytes(),
                source_bytes
            )));
        }
        let mut visible_reservation = execution
            .reserve_memory(source_bytes)
            .map_err(TsinkError::from)?;
        let mut visible_rows = Vec::with_capacity(inner_result.page.rows.capacity());
        for row in &inner_result.page.rows {
            execution.checkpoint().map_err(TsinkError::from)?;
            let mut visible_labels = Vec::with_capacity(row.labels_capacity());
            visible_labels.extend(
                row.labels()
                    .iter()
                    .filter(|label| label.name != TENANT_LABEL)
                    .cloned(),
            );
            visible_rows.push(Row::with_labels(
                row.metric().to_string(),
                visible_labels,
                row.data_point().clone(),
            ));
        }
        let page = QueryRowsPage {
            rows_scanned: inner_result.page.rows_scanned,
            truncated: inner_result.page.truncated,
            next_row_offset: inner_result.page.next_row_offset,
            rows: visible_rows,
        };
        drop(inner_result);
        drop(scoped);
        drop(scoped_reservation);
        visible_reservation
            .resize(tsink::modeled_query_rows_retained_bytes(&page.rows))
            .map_err(TsinkError::from)?;
        Ok(QueryRowsExecutionResult::accounted(
            page,
            visible_reservation,
        ))
    }

    fn scan_series_rows_execution_accounting(&self) -> QueryExecutionAccounting {
        if self.is_default_tenant() {
            QueryExecutionAccounting::Unaccounted
        } else {
            self.inner.scan_series_rows_execution_accounting()
        }
    }

    fn scan_metric_rows_with_execution(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        execution: &QueryExecution,
    ) -> TsinkResult<QueryRowsPage> {
        self.scan_metric_rows_with_execution_result(metric, start, end, options, execution)
            .map(QueryRowsExecutionResult::into_page)
    }

    fn scan_metric_rows_with_execution_result(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        execution: &QueryExecution,
    ) -> TsinkResult<QueryRowsExecutionResult> {
        let inner_accounting = self
            .inner
            .scan_metric_rows_with_matchers_execution_accounting();
        if self.is_default_tenant() || inner_accounting != QueryExecutionAccounting::Complete {
            return self
                .scan_metric_rows(metric, start, end, options)
                .map(QueryRowsExecutionResult::unaccounted);
        }

        options.validate_metric_row_request(metric, start, end)?;
        execution.checkpoint().map_err(TsinkError::from)?;
        let matcher_retained_bytes = tenant_query_string_capacity_bytes(TENANT_LABEL.len())
            .saturating_add(tenant_query_string_capacity_bytes(self.tenant_id.len()));
        let matcher_reservation = execution
            .reserve_memory(matcher_retained_bytes)
            .map_err(TsinkError::from)?;
        let tenant_matcher = SeriesMatcher::equal(TENANT_LABEL, self.tenant_id.clone());
        let mut inner_result = self
            .inner
            .scan_metric_rows_with_matchers_with_execution_result(
                metric,
                std::slice::from_ref(&tenant_matcher),
                Some(TENANT_LABEL),
                start,
                end,
                options,
                execution,
            )?;
        execution.checkpoint().map_err(TsinkError::from)?;
        drop(tenant_matcher);
        drop(matcher_reservation);

        let visible_bytes = tsink::modeled_query_rows_retained_bytes(&inner_result.page.rows);
        let Some(mut inner_reservation) = inner_result.take_memory_reservation() else {
            // This faulty result has no guard to preserve. Destroy its rows before allocating the
            // diagnostic so the wrapper never performs additional work around an unguarded page.
            drop(inner_result);
            return Err(TsinkError::Other(
                "completely accounted tenant metric-row scan omitted its result reservation"
                    .to_string(),
            ));
        };
        // `inner_result` no longer owns its guard. Every error below must destroy the page before
        // releasing `inner_reservation`; reverse-order implicit cleanup would do the opposite.
        if inner_reservation.bytes() < visible_bytes {
            let error = TsinkError::Other(format!(
                "completely accounted tenant metric-row scan retained {} bytes for a {}-byte result",
                inner_reservation.bytes(),
                visible_bytes
            ));
            drop(inner_result);
            drop(inner_reservation);
            return Err(error);
        }
        let projection_validation = (|| -> TsinkResult<()> {
            for row in &inner_result.page.rows {
                execution.checkpoint().map_err(TsinkError::from)?;
                if row.labels().iter().any(|label| label.name == TENANT_LABEL) {
                    return Err(TsinkError::Other(
                        "tenant metric-row scan failed to project the tenant label".to_string(),
                    ));
                }
            }
            Ok(())
        })();
        if let Err(error) = projection_validation {
            drop(inner_result);
            drop(inner_reservation);
            return Err(error);
        }
        let page = inner_result.into_page();
        if let Err(error) = inner_reservation.resize(visible_bytes) {
            let error = TsinkError::from(error);
            drop(page);
            drop(inner_reservation);
            return Err(error);
        }
        Ok(QueryRowsExecutionResult::accounted(page, inner_reservation))
    }

    fn scan_metric_rows_execution_accounting(&self) -> QueryExecutionAccounting {
        if self.is_default_tenant() {
            QueryExecutionAccounting::Unaccounted
        } else {
            self.inner
                .scan_metric_rows_with_matchers_execution_accounting()
        }
    }

    fn select_with_options(&self, metric: &str, opts: QueryOptions) -> TsinkResult<Vec<DataPoint>> {
        let scoped = self.scoped_query_options(opts.clone())?;
        match self.inner.select_with_options(metric, scoped) {
            Ok(points) => {
                if !points.is_empty() || !self.is_default_tenant() {
                    Ok(points)
                } else {
                    self.inner.select_with_options(metric, opts)
                }
            }
            Err(TsinkError::NoDataPoints { .. }) if self.is_default_tenant() => {
                self.inner.select_with_options(metric, opts)
            }
            Err(err) => Err(err),
        }
    }

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> TsinkResult<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        let selection = SeriesSelection::new()
            .with_metric(metric)
            .with_time_range(start, end);
        let series = self.read_series(&selection)?;
        let rows = self.inner.select_many(&series, start, end)?;
        Ok(self.visible_selected_points(rows))
    }

    fn select_all_with_execution(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        let selection = SeriesSelection::new()
            .with_metric(metric)
            .with_time_range(start, end);
        let series = self.read_series_with_execution(&selection, execution)?;
        let rows = self
            .inner
            .select_many_with_execution(&series, start, end, execution)?;
        Ok(self.visible_selected_points(rows))
    }

    fn list_metrics(&self) -> TsinkResult<Vec<MetricSeries>> {
        if let Some(execution) = self
            .inner
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
        {
            return self.list_metrics_with_execution(&execution);
        }
        Ok(self.visible_series(self.read_series(&SeriesSelection::new())?))
    }

    fn list_metrics_with_execution(
        &self,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<MetricSeries>> {
        self.list_metrics_with_execution_result(execution)
            .map(SelectSeriesExecutionResult::into_series)
    }

    fn list_metrics_with_execution_result(
        &self,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectSeriesExecutionResult> {
        self.select_series_with_execution_result(&SeriesSelection::new(), execution)
    }

    fn list_metrics_execution_accounting(&self) -> QueryExecutionAccounting {
        self.select_series_execution_accounting()
    }

    fn list_metrics_with_wal(&self) -> TsinkResult<Vec<MetricSeries>> {
        if let Some(execution) = self
            .inner
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
        {
            return self.list_metrics_with_wal_with_execution(&execution);
        }
        Ok(self.visible_series(self.inner.list_metrics_with_wal()?))
    }

    fn list_metrics_with_wal_with_execution(
        &self,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<MetricSeries>> {
        self.list_metrics_with_wal_with_execution_result(execution)
            .map(SelectSeriesExecutionResult::into_series)
    }

    fn list_metrics_with_wal_with_execution_result(
        &self,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectSeriesExecutionResult> {
        let accounting = self.inner.list_metrics_with_wal_execution_accounting();
        let mut listed = self
            .inner
            .list_metrics_with_wal_with_execution_result(execution)?;
        let visible = self.visible_series(std::mem::take(&mut listed.series));
        match accounting {
            QueryExecutionAccounting::Complete => {
                let mut reservation = listed.take_memory_reservation().ok_or_else(|| {
                    TsinkError::Other(
                        "completely accounted tenant WAL metadata listing omitted its result reservation"
                            .to_string(),
                    )
                })?;
                reservation
                    .resize(tenant_query_metric_series_retained_bytes(
                        &visible,
                        visible.capacity(),
                    ))
                    .map_err(TsinkError::from)?;
                Ok(SelectSeriesExecutionResult::accounted(visible, reservation))
            }
            QueryExecutionAccounting::Unaccounted => {
                Ok(SelectSeriesExecutionResult::unaccounted(visible))
            }
        }
    }

    fn list_metrics_with_wal_execution_accounting(&self) -> QueryExecutionAccounting {
        self.inner.list_metrics_with_wal_execution_accounting()
    }

    fn list_metrics_in_shards(&self, scope: &MetadataShardScope) -> TsinkResult<Vec<MetricSeries>> {
        let scope = scope.normalized()?;
        if scope.shards.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(execution) = self
            .inner
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
        {
            return self.select_series_in_shards_with_execution(
                &SeriesSelection::new(),
                &scope,
                &execution,
            );
        }
        Ok(self.visible_series(self.read_series_in_shards(&SeriesSelection::new(), &scope)?))
    }

    fn select_series(&self, selection: &SeriesSelection) -> TsinkResult<Vec<MetricSeries>> {
        if let Some(execution) = self
            .inner
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
        {
            return self.select_series_with_execution(selection, &execution);
        }
        Ok(self.visible_series(self.read_series(selection)?))
    }

    fn select_series_with_execution(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<MetricSeries>> {
        self.select_series_with_execution_result(selection, execution)
            .map(SelectSeriesExecutionResult::into_series)
    }

    fn select_series_with_execution_result(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectSeriesExecutionResult> {
        let mut selected = self.read_series_with_execution_result(selection, execution)?;
        let mut reservation = selected.take_memory_reservation().ok_or_else(|| {
            TsinkError::Other(
                "tenant metadata result omitted its retained-memory reservation".to_string(),
            )
        })?;
        let visible = self.visible_series(std::mem::take(&mut selected.series));
        reservation
            .resize(tenant_query_metric_series_retained_bytes(
                &visible,
                visible.capacity(),
            ))
            .map_err(TsinkError::from)?;
        Ok(match self.inner.select_series_execution_accounting() {
            QueryExecutionAccounting::Complete => {
                SelectSeriesExecutionResult::accounted(visible, reservation)
            }
            QueryExecutionAccounting::Unaccounted => {
                SelectSeriesExecutionResult::reserved_unaccounted(visible, reservation)
            }
        })
    }

    fn select_series_execution_accounting(&self) -> QueryExecutionAccounting {
        self.inner.select_series_execution_accounting()
    }

    fn select_series_in_shards(
        &self,
        selection: &SeriesSelection,
        scope: &MetadataShardScope,
    ) -> TsinkResult<Vec<MetricSeries>> {
        selection.validate().map_err(TsinkError::from)?;
        let scope = scope.normalized()?;
        if scope.shards.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(execution) = self
            .inner
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
        {
            return self.select_series_in_shards_with_execution(selection, &scope, &execution);
        }
        Ok(self.visible_series(self.read_series_in_shards(selection, &scope)?))
    }

    fn select_series_in_shards_with_execution(
        &self,
        selection: &SeriesSelection,
        scope: &MetadataShardScope,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<MetricSeries>> {
        self.select_series_in_shards_with_execution_result(selection, scope, execution)
            .map(SelectSeriesExecutionResult::into_series)
    }

    fn select_series_in_shards_with_execution_result(
        &self,
        selection: &SeriesSelection,
        scope: &MetadataShardScope,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectSeriesExecutionResult> {
        let mut selected =
            self.read_series_in_shards_with_execution_result(selection, scope, execution)?;
        let mut reservation = selected.take_memory_reservation().ok_or_else(|| {
            TsinkError::Other(
                "tenant metadata result omitted its retained-memory reservation".to_string(),
            )
        })?;
        let visible = self.visible_series(std::mem::take(&mut selected.series));
        reservation
            .resize(tenant_query_metric_series_retained_bytes(
                &visible,
                visible.capacity(),
            ))
            .map_err(TsinkError::from)?;
        Ok(
            match self.inner.select_series_in_shards_execution_accounting() {
                QueryExecutionAccounting::Complete => {
                    SelectSeriesExecutionResult::accounted(visible, reservation)
                }
                QueryExecutionAccounting::Unaccounted => {
                    SelectSeriesExecutionResult::reserved_unaccounted(visible, reservation)
                }
            },
        )
    }

    fn select_series_in_shards_execution_accounting(&self) -> QueryExecutionAccounting {
        self.inner.select_series_in_shards_execution_accounting()
    }

    fn delete_series(&self, selection: &SeriesSelection) -> TsinkResult<DeleteSeriesResult> {
        self.delete_series_with_progress(selection, &mut |_| {})
            .map_err(TenantDeleteSeriesError::into_source)
    }

    fn memory_used(&self) -> usize {
        self.inner.memory_used()
    }

    fn memory_budget(&self) -> usize {
        self.inner.memory_budget()
    }

    fn effective_storage_limits(&self) -> EffectiveStorageLimits {
        self.inner.effective_storage_limits()
    }

    fn resource_configuration_snapshot(&self) -> tsink::ResourceConfigurationSnapshot {
        self.inner.resource_configuration_snapshot()
    }

    fn observability_snapshot(&self) -> StorageObservabilitySnapshot {
        self.inner.observability_snapshot()
    }

    fn apply_rollup_policies(
        &self,
        policies: Vec<tsink::RollupPolicy>,
    ) -> TsinkResult<tsink::RollupObservabilitySnapshot> {
        self.inner.apply_rollup_policies(policies)
    }

    fn trigger_rollup_run(&self) -> TsinkResult<tsink::RollupObservabilitySnapshot> {
        self.inner.trigger_rollup_run()
    }

    fn snapshot(&self, destination: &Path) -> TsinkResult<()> {
        self.inner.snapshot(destination)
    }

    fn close(&self) -> TsinkResult<()> {
        self.inner.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::managed_control_plane::{
        DeploymentLifecycleState, ManagedControlPlaneActor, ManagedDeploymentProvisionRequest,
        ManagedTenantApplyRequest, ManagedTenantLifecycleRequest, TenantLifecycleState,
    };
    use crate::usage::{UsageAccounting, UsageCategory, UsageRecordInput};
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Barrier, Condvar};
    use tsink::{
        AsyncRuntimeOptions, AsyncStorage, QueryBudgetError, QueryBudgetLimits,
        QueryCancellationToken, QueryLimitReason, QueryWorkLimits, StorageBuilder,
        TimestampPrecision, WriteAcknowledgement,
    };

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build")
    }

    async fn wait_for_tenant_query_resources_to_release(storage: &Arc<dyn Storage>) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let snapshot = storage.query_budget_snapshot();
                if snapshot.active_queries == 0 && snapshot.shared_reserved_memory_bytes == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("tenant query slots and result reservations must release");
    }

    fn modeled_numeric_query_rows_returned_bytes(rows: &[Row]) -> u64 {
        rows.iter().fold(0u64, |bytes, row| {
            bytes
                .saturating_add(u64::try_from(std::mem::size_of::<Row>()).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(row.metric().len()).unwrap_or(u64::MAX))
                .saturating_add(row.labels().iter().fold(0u64, |label_bytes, label| {
                    label_bytes
                        .saturating_add(
                            u64::try_from(std::mem::size_of::<Label>()).unwrap_or(u64::MAX),
                        )
                        .saturating_add(u64::try_from(label.name.len()).unwrap_or(u64::MAX))
                        .saturating_add(u64::try_from(label.value.len()).unwrap_or(u64::MAX))
                }))
        })
    }

    fn tenant_status_projection_registry() -> TenantRegistry {
        let registry = TenantRegistry::from_json_str(
            r#"{
                "tenants": {
                    "team-a": {
                        "quotas": {
                            "maxWriteRowsPerRequest": 128,
                            "maxReadQueriesPerRequest": 7,
                            "maxMetadataMatchersPerRequest": 9,
                            "maxQueryLengthBytes": 4096,
                            "maxRangePointsPerQuery": 2048
                        },
                        "cluster": {
                            "writeConsistency": "all",
                            "readConsistency": "strict",
                            "readPartialResponse": "deny"
                        },
                        "admission": {
                            "maxInflightReads": 3,
                            "maxInflightWrites": 2,
                            "ingest": {
                                "maxInflightRequests": 4,
                                "maxInflightUnits": 64
                            },
                            "query": {
                                "maxInflightRequests": 5,
                                "maxInflightUnits": 32
                            },
                            "metadata": {
                                "maxInflightRequests": 6,
                                "maxInflightUnits": 16
                            },
                            "retention": {
                                "maxInflightRequests": 1,
                                "maxInflightUnits": 8
                            }
                        }
                    }
                }
            }"#,
        )
        .expect("tenant status projection fixture should parse");
        let runtime = registry
            .runtime_for("team-a")
            .expect("tenant status projection runtime should build");
        runtime.record_decision(
            TenantAccessScope::Read,
            TenantAdmissionSurface::Query,
            TenantDecisionOutcome::Admitted,
            3,
            "query projection admitted with a deliberately retained diagnostic".to_string(),
        );
        runtime.record_decision(
            TenantAccessScope::Write,
            TenantAdmissionSurface::Ingest,
            TenantDecisionOutcome::Throttled,
            65,
            "ingest projection exceeded its configured unit budget".to_string(),
        );
        runtime.record_decision(
            TenantAccessScope::Read,
            TenantAdmissionSurface::Metadata,
            TenantDecisionOutcome::Rejected,
            11,
            "metadata projection rejected an oversized matcher collection".to_string(),
        );
        registry
    }

    fn default_tenant_metadata_storage_with_limits(
        limits: QueryBudgetLimits,
    ) -> (Arc<dyn Storage>, Arc<dyn Storage>) {
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(1)
            .with_query_budget_limits(limits)
            .build()
            .expect("storage with query limits should build");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_millis() as i64;
        storage
            .insert_rows(&[Row::with_labels(
                "tenant_metadata_budget",
                vec![Label::new("host", "legacy")],
                DataPoint::new(now, 1.0),
            )])
            .expect("legacy series should insert");
        let scoped_rows = scope_rows_for_tenant(
            vec![Row::with_labels(
                "tenant_metadata_budget",
                vec![Label::new("host", "current")],
                DataPoint::new(now, 2.0),
            )],
            DEFAULT_TENANT_ID,
        )
        .expect("default-tenant series should scope");
        storage
            .insert_rows(&scoped_rows)
            .expect("scoped series should insert");
        let scoped = scoped_storage(Arc::clone(&storage), DEFAULT_TENANT_ID);
        (storage, scoped)
    }

    fn default_tenant_batch_storage_with_limits(
        limits: QueryBudgetLimits,
    ) -> (Arc<dyn Storage>, Arc<dyn Storage>, Vec<MetricSeries>) {
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_query_budget_limits(limits)
            .build()
            .expect("storage with query limits should build");
        let series = vec![
            MetricSeries {
                name: "tenant_batch_budget".to_string(),
                labels: vec![Label::new("host", "a")],
            },
            MetricSeries {
                name: "tenant_batch_budget".to_string(),
                labels: vec![Label::new("host", "b")],
            },
        ];
        storage
            .insert_rows(
                &series
                    .iter()
                    .enumerate()
                    .map(|(index, series)| {
                        Row::with_labels(
                            series.name.clone(),
                            series.labels.clone(),
                            DataPoint::new(10, (index + 1) as f64),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .expect("legacy default-tenant series should insert");
        let scoped = scoped_storage(Arc::clone(&storage), DEFAULT_TENANT_ID);
        (storage, scoped, series)
    }

    struct FixedBatchResultStorage {
        result: BatchWriteResult,
    }

    impl Storage for FixedBatchResultStorage {
        fn insert_rows(&self, _rows: &[Row]) -> TsinkResult<()> {
            Ok(())
        }

        fn write_batch(&self, _rows: &[Row], _mode: WriteMode) -> TsinkResult<BatchWriteResult> {
            Ok(self.result.clone())
        }

        fn select(
            &self,
            _metric: &str,
            _labels: &[Label],
            _start: i64,
            _end: i64,
        ) -> TsinkResult<Vec<DataPoint>> {
            Ok(Vec::new())
        }

        fn select_with_options(
            &self,
            _metric: &str,
            _opts: QueryOptions,
        ) -> TsinkResult<Vec<DataPoint>> {
            Ok(Vec::new())
        }

        fn select_all(
            &self,
            _metric: &str,
            _start: i64,
            _end: i64,
        ) -> TsinkResult<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            Ok(Vec::new())
        }

        fn close(&self) -> TsinkResult<()> {
            Ok(())
        }
    }

    fn fixed_batch_storage(result: BatchWriteResult) -> Arc<dyn Storage> {
        Arc::new(FixedBatchResultStorage { result })
    }

    #[derive(Debug, Clone, Copy)]
    enum MetricRowGuardFault {
        Missing,
        Undersized,
        UnprojectedLabel,
    }

    struct FalseCompleteMetricRowStorage {
        inner: Arc<dyn Storage>,
        fault: MetricRowGuardFault,
        matcher_scan_calls: AtomicU64,
    }

    impl Storage for FalseCompleteMetricRowStorage {
        fn query_budget(&self) -> Option<QueryBudget> {
            self.inner.query_budget()
        }

        fn insert_rows(&self, rows: &[Row]) -> TsinkResult<()> {
            self.inner.insert_rows(rows)
        }

        fn select(
            &self,
            metric: &str,
            labels: &[Label],
            start: i64,
            end: i64,
        ) -> TsinkResult<Vec<DataPoint>> {
            self.inner.select(metric, labels, start, end)
        }

        fn select_with_options(
            &self,
            metric: &str,
            options: QueryOptions,
        ) -> TsinkResult<Vec<DataPoint>> {
            self.inner.select_with_options(metric, options)
        }

        fn select_all(
            &self,
            metric: &str,
            start: i64,
            end: i64,
        ) -> TsinkResult<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            self.inner.select_all(metric, start, end)
        }

        fn scan_metric_rows_with_matchers_with_execution_result(
            &self,
            metric: &str,
            matchers: &[SeriesMatcher],
            excluded_output_label: Option<&str>,
            start: i64,
            end: i64,
            options: QueryRowsScanOptions,
            execution: &QueryExecution,
        ) -> TsinkResult<QueryRowsExecutionResult> {
            self.matcher_scan_calls.fetch_add(1, Ordering::SeqCst);
            let forwarded_exclusion = match self.fault {
                MetricRowGuardFault::UnprojectedLabel => None,
                MetricRowGuardFault::Missing | MetricRowGuardFault::Undersized => {
                    excluded_output_label
                }
            };
            let mut result = self
                .inner
                .scan_metric_rows_with_matchers_with_execution_result(
                    metric,
                    matchers,
                    forwarded_exclusion,
                    start,
                    end,
                    options,
                    execution,
                )?;
            let reservation = result
                .take_memory_reservation()
                .expect("built-in matcher scan should return a reservation");
            match self.fault {
                MetricRowGuardFault::Missing => {
                    drop(reservation);
                    Ok(QueryRowsExecutionResult::unaccounted(result.into_page()))
                }
                MetricRowGuardFault::Undersized => {
                    let required = tsink::modeled_query_rows_retained_bytes(&result.page.rows);
                    let mut reservation = reservation;
                    reservation
                        .resize(required.saturating_sub(1))
                        .expect("shrinking a test reservation should succeed");
                    Ok(QueryRowsExecutionResult::accounted(
                        result.into_page(),
                        reservation,
                    ))
                }
                MetricRowGuardFault::UnprojectedLabel => Ok(QueryRowsExecutionResult::accounted(
                    result.into_page(),
                    reservation,
                )),
            }
        }

        fn scan_metric_rows_with_matchers_execution_accounting(&self) -> QueryExecutionAccounting {
            QueryExecutionAccounting::Complete
        }

        fn close(&self) -> TsinkResult<()> {
            self.inner.close()
        }
    }

    struct BlockingTenantMetricScanStorage {
        inner: Arc<dyn Storage>,
        block_detailed_scan: AtomicBool,
        detailed_scan_started: tokio::sync::Notify,
        release_detailed_scan: Condvar,
        released: Mutex<bool>,
        compatibility_scan_calls: AtomicU64,
        detailed_scan_calls: AtomicU64,
    }

    impl BlockingTenantMetricScanStorage {
        fn new(inner: Arc<dyn Storage>) -> Self {
            Self {
                inner,
                block_detailed_scan: AtomicBool::new(false),
                detailed_scan_started: tokio::sync::Notify::new(),
                release_detailed_scan: Condvar::new(),
                released: Mutex::new(true),
                compatibility_scan_calls: AtomicU64::new(0),
                detailed_scan_calls: AtomicU64::new(0),
            }
        }

        fn arm_block(&self) {
            *self
                .released
                .lock()
                .expect("scan release state should lock") = false;
            self.block_detailed_scan.store(true, Ordering::SeqCst);
        }

        fn release(&self) {
            *self
                .released
                .lock()
                .expect("scan release state should lock") = true;
            self.block_detailed_scan.store(false, Ordering::SeqCst);
            self.release_detailed_scan.notify_all();
        }
    }

    struct TenantMetricScanReleaseGuard {
        storage: Arc<BlockingTenantMetricScanStorage>,
        armed: bool,
    }

    impl TenantMetricScanReleaseGuard {
        fn new(storage: Arc<BlockingTenantMetricScanStorage>) -> Self {
            Self {
                storage,
                armed: true,
            }
        }

        fn release(&mut self) {
            if self.armed {
                self.storage.release();
                self.armed = false;
            }
        }
    }

    impl Drop for TenantMetricScanReleaseGuard {
        fn drop(&mut self) {
            self.release();
        }
    }

    impl Storage for BlockingTenantMetricScanStorage {
        fn query_budget(&self) -> Option<QueryBudget> {
            self.inner.query_budget()
        }

        fn insert_rows(&self, rows: &[Row]) -> TsinkResult<()> {
            self.inner.insert_rows(rows)
        }

        fn select(
            &self,
            metric: &str,
            labels: &[Label],
            start: i64,
            end: i64,
        ) -> TsinkResult<Vec<DataPoint>> {
            self.inner.select(metric, labels, start, end)
        }

        fn select_with_options(
            &self,
            metric: &str,
            options: QueryOptions,
        ) -> TsinkResult<Vec<DataPoint>> {
            self.inner.select_with_options(metric, options)
        }

        fn select_all(
            &self,
            metric: &str,
            start: i64,
            end: i64,
        ) -> TsinkResult<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            self.inner.select_all(metric, start, end)
        }

        fn scan_metric_rows(
            &self,
            metric: &str,
            start: i64,
            end: i64,
            options: QueryRowsScanOptions,
        ) -> TsinkResult<QueryRowsPage> {
            self.compatibility_scan_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.scan_metric_rows(metric, start, end, options)
        }

        fn scan_metric_rows_with_execution_result(
            &self,
            metric: &str,
            start: i64,
            end: i64,
            options: QueryRowsScanOptions,
            execution: &QueryExecution,
        ) -> TsinkResult<QueryRowsExecutionResult> {
            self.detailed_scan_calls.fetch_add(1, Ordering::SeqCst);
            if self.block_detailed_scan.load(Ordering::SeqCst) {
                self.detailed_scan_started.notify_one();
                let mut released = self
                    .released
                    .lock()
                    .expect("scan release state should lock");
                while !*released {
                    released = self
                        .release_detailed_scan
                        .wait(released)
                        .expect("scan release wait should preserve the lock");
                }
            }
            self.inner
                .scan_metric_rows_with_execution_result(metric, start, end, options, execution)
        }

        fn scan_metric_rows_execution_accounting(&self) -> QueryExecutionAccounting {
            self.inner.scan_metric_rows_execution_accounting()
        }

        fn close(&self) -> TsinkResult<()> {
            self.inner.close()
        }
    }

    fn accepted_batch_result(rows: usize) -> BatchWriteResult {
        BatchWriteResult::from_outcomes(
            Some(WriteAcknowledgement::Volatile),
            (0..rows).map(RowWriteOutcome::accepted).collect(),
        )
    }

    fn test_rejection(cause_index: Option<usize>) -> WriteRejection {
        WriteRejection::new(
            WriteRejectionCategory::Internal,
            cause_index,
            "mock rejection",
        )
    }

    fn assert_malformed_batch_result(err: TsinkError, expected_detail: &str) {
        let TsinkError::Other(message) = err else {
            panic!("malformed backend result should use the outer error channel: {err}");
        };
        assert!(
            message.contains("malformed canonical batch result"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains(expected_detail),
            "expected {expected_detail:?} in error: {message}"
        );
    }

    fn managed_actor() -> ManagedControlPlaneActor {
        ManagedControlPlaneActor {
            id: "test".to_string(),
            scope: "test".to_string(),
        }
    }

    fn provision_ready_deployment(
        control_plane: &ManagedControlPlane,
        deployment_id: &str,
    ) -> Result<(), String> {
        control_plane
            .provision_deployment(
                managed_actor(),
                ManagedDeploymentProvisionRequest {
                    deployment_id: deployment_id.to_string(),
                    display_name: Some(deployment_id.to_string()),
                    region: Some("test-region".to_string()),
                    plan: Some("test-plan".to_string()),
                    lifecycle: Some(DeploymentLifecycleState::Ready),
                    ..ManagedDeploymentProvisionRequest::default()
                },
            )
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    struct RecordingMetadataStorage {
        select_series_calls: Mutex<Vec<SeriesSelection>>,
        select_series_in_shards_calls: Mutex<Vec<(SeriesSelection, MetadataShardScope)>>,
        select_many_calls: Mutex<Vec<(Vec<MetricSeries>, i64, i64)>>,
    }

    impl RecordingMetadataStorage {
        fn new() -> Self {
            Self {
                select_series_calls: Mutex::new(Vec::new()),
                select_series_in_shards_calls: Mutex::new(Vec::new()),
                select_many_calls: Mutex::new(Vec::new()),
            }
        }

        fn scoped_series() -> MetricSeries {
            MetricSeries {
                name: "cpu_usage".to_string(),
                labels: vec![
                    Label::new("host", "current"),
                    Label::new(TENANT_LABEL, DEFAULT_TENANT_ID),
                ],
            }
        }

        fn legacy_series() -> MetricSeries {
            MetricSeries {
                name: "cpu_usage".to_string(),
                labels: vec![Label::new("host", "legacy")],
            }
        }

        fn series_for_selection(
            &self,
            selection: &SeriesSelection,
        ) -> TsinkResult<Vec<MetricSeries>> {
            let tenant_matcher = selection
                .matchers
                .iter()
                .find(|matcher| matcher.name == TENANT_LABEL)
                .unwrap_or_else(|| panic!("tenant matcher missing from selection: {selection:?}"));
            match (&tenant_matcher.op, tenant_matcher.value.as_str()) {
                (tsink::SeriesMatcherOp::Equal, DEFAULT_TENANT_ID) => {
                    Ok(vec![Self::scoped_series()])
                }
                (tsink::SeriesMatcherOp::RegexNoMatch, UNLABELED_TENANT_FALLBACK_REGEX) => {
                    Ok(vec![Self::legacy_series()])
                }
                _ => panic!("unexpected tenant matcher in selection: {selection:?}"),
            }
        }
    }

    impl Storage for RecordingMetadataStorage {
        fn insert_rows(&self, _rows: &[Row]) -> TsinkResult<()> {
            Ok(())
        }

        fn select(
            &self,
            _metric: &str,
            _labels: &[Label],
            _start: i64,
            _end: i64,
        ) -> TsinkResult<Vec<DataPoint>> {
            panic!("exact select should not be used in this test");
        }

        fn select_with_options(
            &self,
            _metric: &str,
            _opts: QueryOptions,
        ) -> TsinkResult<Vec<DataPoint>> {
            panic!("select_with_options should not be used in this test");
        }

        fn select_all(
            &self,
            _metric: &str,
            _start: i64,
            _end: i64,
        ) -> TsinkResult<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            panic!("unscoped select_all should not be used by tenant reads");
        }

        fn list_metrics(&self) -> TsinkResult<Vec<MetricSeries>> {
            panic!("unscoped list_metrics should not be used by tenant reads");
        }

        fn list_metrics_in_shards(
            &self,
            _scope: &MetadataShardScope,
        ) -> TsinkResult<Vec<MetricSeries>> {
            panic!("unscoped list_metrics_in_shards should not be used by tenant reads");
        }

        fn select_series(&self, selection: &SeriesSelection) -> TsinkResult<Vec<MetricSeries>> {
            self.select_series_calls
                .lock()
                .expect("select_series calls should record")
                .push(selection.clone());
            self.series_for_selection(selection)
        }

        fn select_series_in_shards(
            &self,
            selection: &SeriesSelection,
            scope: &MetadataShardScope,
        ) -> TsinkResult<Vec<MetricSeries>> {
            self.select_series_in_shards_calls
                .lock()
                .expect("select_series_in_shards calls should record")
                .push((selection.clone(), scope.clone()));
            self.series_for_selection(selection)
        }

        fn select_many(
            &self,
            series: &[MetricSeries],
            start: i64,
            end: i64,
        ) -> TsinkResult<Vec<SeriesPoints>> {
            self.select_many_calls
                .lock()
                .expect("select_many calls should record")
                .push((series.to_vec(), start, end));
            Ok(series
                .iter()
                .map(|series| SeriesPoints {
                    series: series.clone(),
                    points: vec![DataPoint::new(
                        start,
                        if series.labels.iter().any(|label| label.name == TENANT_LABEL) {
                            2.0
                        } else {
                            1.0
                        },
                    )],
                })
                .collect())
        }

        fn close(&self) -> TsinkResult<()> {
            Ok(())
        }
    }

    #[test]
    fn tenant_id_for_request_accepts_scope_org_id_header() {
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query".to_string(),
            headers: HashMap::from([(SCOPE_ORG_ID_HEADER.to_string(), "team-b".to_string())]),
            body: Vec::new(),
        };

        let tenant_id = tenant_id_for_request(&request).expect("scope org id should resolve");
        assert_eq!(tenant_id, "team-b");
    }

    #[test]
    fn tenant_id_for_request_defaults_when_tenant_headers_are_absent() {
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let tenant_id = tenant_id_for_request(&request).expect("default tenant should resolve");
        assert_eq!(tenant_id, DEFAULT_TENANT_ID);
    }

    #[test]
    fn tenant_id_for_request_rejects_conflicting_tenant_headers() {
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query".to_string(),
            headers: HashMap::from([
                (TENANT_HEADER.to_string(), "team-a".to_string()),
                (SCOPE_ORG_ID_HEADER.to_string(), "team-b".to_string()),
            ]),
            body: Vec::new(),
        };

        let err =
            tenant_id_for_request(&request).expect_err("conflicting tenant headers must fail");
        assert_eq!(
            err,
            format!(
                "{TENANT_HEADER} and {SCOPE_ORG_ID_HEADER} must match when both headers are set"
            )
        );
    }

    #[test]
    fn tenant_metric_row_scan_capability_requires_non_default_and_matcher_aware_inner() {
        let storage = make_storage();
        assert_eq!(
            storage.scan_metric_rows_with_matchers_execution_accounting(),
            QueryExecutionAccounting::Complete,
            "the built-in inner storage fixture should expose complete matcher-aware accounting",
        );
        let tenant = scoped_storage(Arc::clone(&storage), "tenant-a");
        assert_eq!(
            tenant.scan_metric_rows_execution_accounting(),
            QueryExecutionAccounting::Complete,
            "a non-default tenant may expose the complete inner matcher-aware primitive",
        );

        let default_tenant = scoped_storage(storage, DEFAULT_TENANT_ID);
        assert_eq!(
            default_tenant.scan_metric_rows_execution_accounting(),
            QueryExecutionAccounting::Unaccounted,
            "default-tenant legacy fallback cannot preserve one matcher-aware scan envelope",
        );

        let compatibility_inner = fixed_batch_storage(accepted_batch_result(0));
        assert_eq!(
            compatibility_inner.scan_metric_rows_with_matchers_execution_accounting(),
            QueryExecutionAccounting::Unaccounted,
            "third-party compatibility storage must remain conservative by default",
        );
        let compatibility_tenant = scoped_storage(compatibility_inner, "tenant-a");
        assert_eq!(
            compatibility_tenant.scan_metric_rows_execution_accounting(),
            QueryExecutionAccounting::Unaccounted,
            "the tenant adapter must not promote an unaccounted matcher-aware inner",
        );
    }

    #[test]
    fn tenant_metric_row_scan_empty_page_retains_zero_byte_guard_lease() {
        let storage = make_storage();
        let tenant = scoped_storage(Arc::clone(&storage), "tenant-a");
        let execution = tenant
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("empty tenant query should admit")
            .expect("built-in storage should expose a query budget");
        let result = tenant
            .scan_metric_rows_with_execution_result(
                "tenant_metric_empty",
                0,
                20,
                QueryRowsScanOptions {
                    max_rows: Some(1),
                    row_offset: None,
                },
                &execution,
            )
            .expect("empty tenant metric scan should succeed");
        assert!(result.page.rows.is_empty());
        assert_eq!(result.page.rows_scanned, 0);
        assert!(!result.page.truncated);
        assert_eq!(result.page.next_row_offset, None);
        assert_eq!(result.reserved_memory_bytes(), 0);
        drop(execution);
        let held = storage.query_budget_snapshot();
        assert_eq!(held.active_queries, 1);
        assert_eq!(held.shared_reserved_memory_bytes, 0);
        assert_eq!(held.queries_completed_total, 0);
        drop(result);
        let released = storage.query_budget_snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.queries_completed_total, 1);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn tenant_metric_row_scan_offset_beyond_visible_end_is_terminal_empty() {
        let storage = make_storage();
        let metric = "tenant_metric_offset_end";
        storage
            .insert_rows(
                &scope_rows_for_tenant(
                    vec![
                        Row::with_labels(
                            metric,
                            vec![Label::new("host", "a")],
                            DataPoint::new(10, 1.0),
                        ),
                        Row::with_labels(
                            metric,
                            vec![Label::new("host", "a")],
                            DataPoint::new(20, 2.0),
                        ),
                    ],
                    "tenant-a",
                )
                .expect("tenant A rows should scope"),
            )
            .expect("tenant A rows should insert");
        storage
            .insert_rows(
                &scope_rows_for_tenant(
                    vec![Row::with_labels(
                        metric,
                        vec![Label::new("host", "b")],
                        DataPoint::new(15, 3.0),
                    )],
                    "tenant-b",
                )
                .expect("tenant B row should scope"),
            )
            .expect("tenant B row should insert");

        let tenant = scoped_storage(Arc::clone(&storage), "tenant-a");
        let execution = tenant
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("offset query should admit")
            .expect("built-in storage should expose a query budget");
        let result = tenant
            .scan_metric_rows_with_execution_result(
                metric,
                0,
                30,
                QueryRowsScanOptions {
                    max_rows: Some(1),
                    row_offset: Some(99),
                },
                &execution,
            )
            .expect("beyond-end tenant metric scan should succeed");
        assert!(result.page.rows.is_empty());
        assert_eq!(result.page.rows_scanned, 0);
        assert!(!result.page.truncated);
        assert_eq!(result.page.next_row_offset, None);
        assert_eq!(execution.snapshot().series_matched, 1);
        drop(result);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let released = storage.query_budget_snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn tenant_metric_row_scan_filters_before_pagination_and_preserves_continuation() {
        let storage = make_storage();
        let metric = "tenant_metric_page";
        let tenant_a_rows = scope_rows_for_tenant(
            vec![
                Row::with_labels(
                    metric,
                    vec![Label::new("host", "a")],
                    DataPoint::new(10, 1.0),
                ),
                Row::with_labels(
                    metric,
                    vec![Label::new("host", "a")],
                    DataPoint::new(30, 2.0),
                ),
            ],
            "tenant-a",
        )
        .expect("tenant A rows should scope");
        let tenant_b_rows = scope_rows_for_tenant(
            vec![
                Row::with_labels(
                    metric,
                    vec![Label::new("host", "b")],
                    DataPoint::new(5, 10.0),
                ),
                Row::with_labels(
                    metric,
                    vec![Label::new("host", "b")],
                    DataPoint::new(20, 20.0),
                ),
            ],
            "tenant-b",
        )
        .expect("tenant B rows should scope");
        storage
            .insert_rows(
                &tenant_a_rows
                    .into_iter()
                    .chain(tenant_b_rows)
                    .collect::<Vec<_>>(),
            )
            .expect("mixed tenant rows should insert");

        let tenant = scoped_storage(Arc::clone(&storage), "tenant-a");
        let execution = tenant
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("query admission should succeed")
            .expect("built-in storage should expose a query budget");
        let first = tenant
            .scan_metric_rows_with_execution_result(
                metric,
                0,
                40,
                QueryRowsScanOptions {
                    max_rows: Some(1),
                    row_offset: None,
                },
                &execution,
            )
            .expect("first tenant page should succeed");
        assert_eq!(first.page.rows.len(), 1);
        assert!(first.page.truncated);
        assert_eq!(first.page.next_row_offset, Some(1));
        assert_eq!(first.page.rows[0].data_point().value_as_f64(), Some(1.0));
        assert_eq!(first.page.rows[0].labels(), &[Label::new("host", "a")]);
        assert_eq!(
            first.reserved_memory_bytes(),
            tsink::modeled_query_rows_retained_bytes(&first.page.rows)
        );
        let continuation = first
            .page
            .next_row_offset
            .expect("a truncated page should expose a continuation");
        drop(first);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);

        let second = tenant
            .scan_metric_rows_with_execution_result(
                metric,
                0,
                40,
                QueryRowsScanOptions {
                    max_rows: Some(1),
                    row_offset: Some(continuation),
                },
                &execution,
            )
            .expect("continued tenant page should succeed");
        assert_eq!(second.page.rows.len(), 1);
        assert!(!second.page.truncated);
        assert_eq!(second.page.next_row_offset, None);
        assert_eq!(second.page.rows[0].data_point().value_as_f64(), Some(2.0));
        assert_eq!(second.page.rows[0].labels(), &[Label::new("host", "a")]);
        assert!(second.page.rows[0]
            .labels()
            .iter()
            .all(|label| label.name != TENANT_LABEL));
        drop(second);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            0
        );
    }

    #[test]
    fn tenant_metric_row_scan_charges_only_visible_series_amid_other_tenants() {
        let storage = make_storage();
        let metric = "tenant_metric_series_charge";
        let tenant_a_rows = (0..2)
            .map(|index| {
                Row::with_labels(
                    metric,
                    vec![Label::new("host", format!("a-{index}"))],
                    DataPoint::new(10, index as f64),
                )
            })
            .collect::<Vec<_>>();
        storage
            .insert_rows(
                &scope_rows_for_tenant(tenant_a_rows, "tenant-a")
                    .expect("tenant A rows should scope"),
            )
            .expect("tenant A rows should insert");
        let tenant_b_rows = (0..64)
            .map(|index| {
                Row::with_labels(
                    metric,
                    vec![Label::new("host", format!("b-{index}"))],
                    DataPoint::new(10, index as f64),
                )
            })
            .collect::<Vec<_>>();
        storage
            .insert_rows(
                &scope_rows_for_tenant(tenant_b_rows, "tenant-b")
                    .expect("tenant B rows should scope"),
            )
            .expect("tenant B rows should insert");

        let tenant = scoped_storage(storage, "tenant-a");
        let budget = QueryBudget::new(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_series_matched: Some(2),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        })
        .expect("series-limited query budget should build");
        let execution = budget.begin_query().expect("query should admit");
        let result = tenant
            .scan_metric_rows_with_execution_result(
                metric,
                0,
                20,
                QueryRowsScanOptions {
                    max_rows: Some(2),
                    row_offset: None,
                },
                &execution,
            )
            .expect("the exact two visible series should fit");
        assert_eq!(result.page.rows.len(), 2);
        assert_eq!(execution.snapshot().series_matched, 2);
        assert!(result
            .page
            .rows
            .iter()
            .all(|row| row.labels().iter().all(|label| label.name != TENANT_LABEL)));
        drop(result);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let released = budget.snapshot();
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);

        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_series_matched: Some(1),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        })
        .expect("one-under series budget should build");
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under series query should admit");
        let error = tenant
            .scan_metric_rows_with_execution_result(
                metric,
                0,
                20,
                QueryRowsScanOptions {
                    max_rows: Some(2),
                    row_offset: None,
                },
                &one_under,
            )
            .expect_err("one visible-series slot below the exact count must reject");
        assert!(matches!(
            error,
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                if exceeded.reason == QueryLimitReason::SeriesMatched
                    && exceeded.current == 0
                    && exceeded.requested == 2
        ));
        let rejected = one_under.snapshot();
        assert_eq!(rejected.series_matched, 0);
        assert_eq!(rejected.samples_returned, 0);
        assert_eq!(rejected.returned_bytes, 0);
        assert_eq!(rejected.memory_reserved_bytes, 0);
        drop(one_under);
        let one_under_released = one_under_budget.snapshot();
        assert_eq!(one_under_released.shared_reserved_memory_bytes, 0);
        assert_eq!(one_under_released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn tenant_metric_row_scan_charges_exact_visible_returned_bytes() {
        let storage = make_storage();
        let metric = "tenant_metric_returned_bytes";
        storage
            .insert_rows(
                &scope_rows_for_tenant(
                    vec![Row::with_labels(
                        metric,
                        vec![Label::new("host", "visible-a")],
                        DataPoint::new(10, 1.0),
                    )],
                    "tenant-a",
                )
                .expect("tenant A row should scope"),
            )
            .expect("tenant A row should insert");
        storage
            .insert_rows(
                &scope_rows_for_tenant(
                    vec![Row::with_labels(
                        metric,
                        vec![Label::new("host", "hidden-b")],
                        DataPoint::new(10, 2.0),
                    )],
                    "tenant-b",
                )
                .expect("tenant B row should scope"),
            )
            .expect("tenant B row should insert");
        let tenant = scoped_storage(storage, "tenant-a");
        let options = QueryRowsScanOptions {
            max_rows: Some(1),
            row_offset: None,
        };

        let calibration_budget = QueryBudget::new(QueryBudgetLimits::default())
            .expect("returned-byte calibration budget should build");
        let calibration = calibration_budget
            .begin_query()
            .expect("returned-byte calibration query should admit");
        let calibrated = tenant
            .scan_metric_rows_with_execution_result(metric, 0, 20, options, &calibration)
            .expect("returned-byte calibration scan should succeed");
        assert_eq!(calibrated.page.rows.len(), 1);
        assert!(calibrated.page.rows[0]
            .labels()
            .iter()
            .all(|label| label.name != TENANT_LABEL));
        let required = modeled_numeric_query_rows_returned_bytes(&calibrated.page.rows);
        assert!(required > 0);
        let hidden_tenant_label_bytes = u64::try_from(std::mem::size_of::<Label>())
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(TENANT_LABEL.len()).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from("tenant-a".len()).unwrap_or(u64::MAX));
        let physical_required = required.saturating_add(hidden_tenant_label_bytes);
        assert!(
            physical_required > required,
            "the physical row model must include the hidden tenant-label slot and text"
        );
        assert_eq!(
            calibration.snapshot().returned_bytes,
            required,
            "returned-byte accounting must model the projected tenant-visible row"
        );
        drop(calibrated);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        assert_eq!(
            calibration_budget.snapshot().shared_reserved_memory_bytes,
            0
        );

        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_returned_bytes: Some(required),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        })
        .expect("exact returned-byte budget should build");
        let exact = exact_budget
            .begin_query()
            .expect("exact returned-byte query should admit");
        let exact_result = tenant
            .scan_metric_rows_with_execution_result(metric, 0, 20, options, &exact)
            .expect("the exact visible returned-byte budget should pass");
        assert_eq!(exact.snapshot().returned_bytes, required);
        drop(exact_result);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_returned_bytes: Some(required.saturating_sub(1)),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        })
        .expect("one-under returned-byte budget should build");
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under returned-byte query should admit");
        let error = tenant
            .scan_metric_rows_with_execution_result(metric, 0, 20, options, &one_under)
            .expect_err("one byte below the visible returned-byte model must reject");
        assert!(matches!(
            error,
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                if exceeded.reason == QueryLimitReason::ReturnedBytes
                    && exceeded.current == 0
                    && exceeded.requested == required
        ));
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        drop(one_under);
        let one_under_released = one_under_budget.snapshot();
        assert_eq!(one_under_released.shared_reserved_memory_bytes, 0);
        assert_eq!(one_under_released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn tenant_metric_row_scan_enforces_exact_memory_peak_and_guard_lifetime() {
        let storage = make_storage();
        let metric = "tenant_metric_memory";
        storage
            .insert_rows(
                &scope_rows_for_tenant(
                    vec![Row::with_labels(
                        metric,
                        vec![Label::new("host", "a-long-visible-label-value")],
                        DataPoint::new(10, 1.0),
                    )],
                    "tenant-a",
                )
                .expect("tenant row should scope"),
            )
            .expect("tenant row should insert");
        let tenant = scoped_storage(storage, "tenant-a");
        let options = QueryRowsScanOptions {
            max_rows: Some(1),
            row_offset: None,
        };

        let calibration_budget = QueryBudget::new(QueryBudgetLimits::default())
            .expect("calibration budget should build");
        let calibration = calibration_budget
            .begin_query()
            .expect("calibration query should admit");
        let calibrated = tenant
            .scan_metric_rows_with_execution_result(metric, 0, 20, options, &calibration)
            .expect("calibration scan should succeed");
        let retained = tsink::modeled_query_rows_retained_bytes(&calibrated.page.rows);
        assert_eq!(calibrated.reserved_memory_bytes(), retained);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, retained);
        let required_peak = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(required_peak >= retained);
        assert!(required_peak > 0);
        drop(calibrated);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        assert_eq!(
            calibration_budget.snapshot().shared_reserved_memory_bytes,
            0
        );

        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(required_peak),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(required_peak),
                ..QueryWorkLimits::default()
            },
        })
        .expect("exact budget should build");
        let exact = exact_budget
            .begin_query()
            .expect("exact query should admit");
        let exact_result = tenant
            .scan_metric_rows_with_execution_result(metric, 0, 20, options, &exact)
            .expect("the exact modeled peak should pass");
        assert_eq!(exact_result.reserved_memory_bytes(), retained);
        assert_eq!(exact.snapshot().memory_reserved_bytes, retained);
        drop(exact_result);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        let exact_released = exact_budget.snapshot();
        assert_eq!(exact_released.shared_reserved_memory_bytes, 0);
        assert_eq!(exact_released.accounting_invariant_violations_total, 0);

        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(required_peak),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(required_peak.saturating_sub(1)),
                ..QueryWorkLimits::default()
            },
        })
        .expect("one-under budget should build");
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under query should admit");
        let error = tenant
            .scan_metric_rows_with_execution_result(metric, 0, 20, options, &one_under)
            .expect_err("one byte below the modeled peak must reject");
        assert!(matches!(
            error,
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
        ));
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        drop(one_under);
        let one_under_released = one_under_budget.snapshot();
        assert_eq!(one_under_released.shared_reserved_memory_bytes, 0);
        assert_eq!(one_under_released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn tenant_metric_row_scan_rejects_false_complete_result_guards() {
        let storage = make_storage();
        let metric = "tenant_metric_false_guard";
        storage
            .insert_rows(
                &scope_rows_for_tenant(
                    vec![Row::with_labels(
                        metric,
                        vec![Label::new("host", "a")],
                        DataPoint::new(10, 1.0),
                    )],
                    "tenant-a",
                )
                .expect("tenant row should scope"),
            )
            .expect("tenant row should insert");

        for (fault, expected) in [
            (
                MetricRowGuardFault::Missing,
                "omitted its result reservation",
            ),
            (MetricRowGuardFault::Undersized, "retained"),
            (
                MetricRowGuardFault::UnprojectedLabel,
                "failed to project the tenant label",
            ),
        ] {
            let lying: Arc<dyn Storage> = Arc::new(FalseCompleteMetricRowStorage {
                inner: Arc::clone(&storage),
                fault,
                matcher_scan_calls: AtomicU64::new(0),
            });
            let tenant = scoped_storage(lying, "tenant-a");
            assert_eq!(
                tenant.scan_metric_rows_execution_accounting(),
                QueryExecutionAccounting::Complete,
                "the fixture must exercise a falsely advertised Complete result",
            );
            let budget = QueryBudget::new(QueryBudgetLimits::default())
                .expect("guard test budget should build");
            let execution = budget.begin_query().expect("guard test query should admit");
            let error = tenant
                .scan_metric_rows_with_execution_result(
                    metric,
                    0,
                    20,
                    QueryRowsScanOptions {
                        max_rows: Some(1),
                        row_offset: None,
                    },
                    &execution,
                )
                .expect_err("a false Complete guard must be rejected");
            assert!(
                matches!(&error, TsinkError::Other(message) if message.contains(expected)),
                "unexpected false-Complete error: {error}"
            );
            assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
            drop(execution);
            let released = budget.snapshot();
            assert_eq!(released.shared_reserved_memory_bytes, 0);
            assert_eq!(released.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn tenant_metric_row_scan_honors_precancellation_without_allocating() {
        let storage = make_storage();
        let tenant = scoped_storage(storage, "tenant-a");
        let budget = QueryBudget::new(QueryBudgetLimits::default())
            .expect("cancellation budget should build");
        let cancellation = QueryCancellationToken::new();
        let execution = budget
            .begin_query_with(QueryWorkLimits::default(), cancellation.clone())
            .expect("cancellation query should admit");
        cancellation.cancel();

        let error = tenant
            .scan_metric_rows_with_execution_result(
                "tenant_metric_cancelled",
                0,
                20,
                QueryRowsScanOptions {
                    max_rows: Some(1),
                    row_offset: None,
                },
                &execution,
            )
            .expect_err("a pre-cancelled tenant metric scan must stop");
        assert!(matches!(
            error,
            TsinkError::QueryBudget(QueryBudgetError::Cancelled)
        ));
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(execution.snapshot().series_matched, 0);
        assert_eq!(budget.snapshot().peak_shared_reserved_memory_bytes, 0);
        drop(execution);
        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn tenant_metric_row_scan_validates_inputs_before_cancellation_or_matcher_memory() {
        let inner = make_storage();
        let observed = Arc::new(FalseCompleteMetricRowStorage {
            inner,
            fault: MetricRowGuardFault::Missing,
            matcher_scan_calls: AtomicU64::new(0),
        });
        let tenant = scoped_storage(Arc::clone(&observed) as Arc<dyn Storage>, "tenant-a");
        let budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(1),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(1),
                ..QueryWorkLimits::default()
            },
        })
        .expect("tiny validation budget should build");
        let cancellation = QueryCancellationToken::new();
        let execution = budget
            .begin_query_with(QueryWorkLimits::default(), cancellation.clone())
            .expect("tiny validation query should admit");
        let held = execution
            .reserve_memory(1)
            .expect("the only modeled byte should reserve");
        cancellation.cancel();

        let error = tenant
            .scan_metric_rows_with_execution_result(
                "",
                10,
                10,
                QueryRowsScanOptions {
                    max_rows: Some(0),
                    row_offset: None,
                },
                &execution,
            )
            .expect_err("metric validation must win over later invalid inputs and cancellation");
        assert!(matches!(error, TsinkError::MetricRequired));

        let error = tenant
            .scan_metric_rows_with_execution_result(
                "tenant_metric_invalid_request",
                10,
                10,
                QueryRowsScanOptions {
                    max_rows: Some(0),
                    row_offset: None,
                },
                &execution,
            )
            .expect_err("range validation must win over options and cancellation");
        assert!(matches!(
            error,
            TsinkError::InvalidTimeRange { start: 10, end: 10 }
        ));

        let error = tenant
            .scan_metric_rows_with_execution_result(
                "tenant_metric_invalid_request",
                0,
                10,
                QueryRowsScanOptions {
                    max_rows: Some(0),
                    row_offset: None,
                },
                &execution,
            )
            .expect_err("scan-option validation must win over cancellation and matcher memory");
        assert!(matches!(
            error,
            TsinkError::InvalidConfiguration(message)
                if message == "max_rows must be greater than zero when set"
        ));

        assert_eq!(observed.matcher_scan_calls.load(Ordering::SeqCst), 0);
        let validation_snapshot = execution.snapshot();
        assert_eq!(validation_snapshot.memory_reserved_bytes, 1);
        assert_eq!(validation_snapshot.series_matched, 0);
        assert_eq!(validation_snapshot.samples_returned, 0);
        assert_eq!(validation_snapshot.returned_bytes, 0);
        assert_eq!(budget.snapshot().peak_shared_reserved_memory_bytes, 1);
        drop(held);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_non_default_tenant_metric_scan_projects_pages_and_cleans_up_on_drop(
    ) -> TsinkResult<()> {
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_query_budget_limits(QueryBudgetLimits {
                max_concurrent_queries: Some(1),
                max_shared_memory_bytes: Some(8 * 1024 * 1024),
                per_query: QueryWorkLimits {
                    max_series_matched: Some(1),
                    max_returned_bytes: Some(1024 * 1024),
                    max_intermediate_vector_size: Some(2),
                    max_memory_bytes: Some(4 * 1024 * 1024),
                    ..QueryWorkLimits::default()
                },
            })
            .build()
            .expect("finite async tenant storage should build");
        let metric = "tenant_metric_async";
        storage.insert_rows(
            &scope_rows_for_tenant(
                vec![
                    Row::with_labels(
                        metric,
                        vec![Label::new("host", "a")],
                        DataPoint::new(10, 1.0),
                    ),
                    Row::with_labels(
                        metric,
                        vec![Label::new("host", "a")],
                        DataPoint::new(20, 2.0),
                    ),
                ],
                "tenant-a",
            )
            .expect("async tenant A rows should scope"),
        )?;
        storage.insert_rows(
            &scope_rows_for_tenant(
                vec![Row::with_labels(
                    metric,
                    vec![Label::new("host", "b")],
                    DataPoint::new(15, 3.0),
                )],
                "tenant-b",
            )
            .expect("async tenant B row should scope"),
        )?;

        let tenant = scoped_storage(Arc::clone(&storage), "tenant-a");
        assert_eq!(
            tenant.scan_metric_rows_execution_accounting(),
            QueryExecutionAccounting::Complete
        );
        let observed = Arc::new(BlockingTenantMetricScanStorage::new(tenant));
        let async_storage = AsyncStorage::from_storage_with_options(
            Arc::clone(&observed) as Arc<dyn Storage>,
            AsyncRuntimeOptions {
                read_workers: 1,
                ..AsyncRuntimeOptions::default()
            },
        )?;

        let first = async_storage
            .scan_metric_rows(
                metric,
                0,
                30,
                QueryRowsScanOptions {
                    max_rows: Some(1),
                    row_offset: None,
                },
            )
            .await?;
        assert_eq!(first.rows.len(), 1);
        assert!(first.truncated);
        assert_eq!(first.next_row_offset, Some(1));
        assert_eq!(first.rows[0].data_point().value_as_f64(), Some(1.0));
        assert_eq!(first.rows[0].labels(), &[Label::new("host", "a")]);
        wait_for_tenant_query_resources_to_release(&storage).await;

        let second = async_storage
            .scan_metric_rows(
                metric,
                0,
                30,
                QueryRowsScanOptions {
                    max_rows: Some(1),
                    row_offset: first.next_row_offset,
                },
            )
            .await?;
        assert_eq!(second.rows.len(), 1);
        assert!(!second.truncated);
        assert_eq!(second.next_row_offset, None);
        assert_eq!(second.rows[0].data_point().value_as_f64(), Some(2.0));
        assert_eq!(second.rows[0].labels(), &[Label::new("host", "a")]);
        wait_for_tenant_query_resources_to_release(&storage).await;
        assert_eq!(observed.detailed_scan_calls.load(Ordering::SeqCst), 2);
        assert_eq!(observed.compatibility_scan_calls.load(Ordering::SeqCst), 0);

        observed.arm_block();
        let mut release_guard = TenantMetricScanReleaseGuard::new(Arc::clone(&observed));
        let cancelled_storage = async_storage.clone();
        let cancelled = tokio::spawn(async move {
            cancelled_storage
                .scan_metric_rows(
                    metric,
                    0,
                    30,
                    QueryRowsScanOptions {
                        max_rows: Some(1),
                        row_offset: None,
                    },
                )
                .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            observed.detailed_scan_started.notified(),
        )
        .await
        .expect("cancelled tenant scan should reach the detailed backend");
        assert_eq!(storage.query_budget_snapshot().active_queries, 1);
        cancelled.abort();
        let cancelled_error = tokio::time::timeout(std::time::Duration::from_secs(2), cancelled)
            .await
            .expect("aborted async tenant scan task should stop promptly")
            .expect_err("aborted async tenant scan task should be cancelled");
        assert!(cancelled_error.is_cancelled());
        release_guard.release();
        wait_for_tenant_query_resources_to_release(&storage).await;
        let released = storage.query_budget_snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.queries_started_total, 3);
        assert_eq!(released.queries_completed_total, 3);
        assert_eq!(released.accounting_invariant_violations_total, 0);
        assert_eq!(observed.detailed_scan_calls.load(Ordering::SeqCst), 3);
        assert_eq!(observed.compatibility_scan_calls.load(Ordering::SeqCst), 0);

        async_storage.close().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_default_tenant_metric_scan_fails_closed_before_compatibility_read(
    ) -> TsinkResult<()> {
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_query_budget_limits(QueryBudgetLimits {
                max_concurrent_queries: Some(1),
                max_shared_memory_bytes: Some(1024 * 1024),
                per_query: QueryWorkLimits {
                    max_memory_bytes: Some(512 * 1024),
                    ..QueryWorkLimits::default()
                },
            })
            .build()
            .expect("finite default-tenant async storage should build");
        let default_tenant = scoped_storage(Arc::clone(&storage), DEFAULT_TENANT_ID);
        assert_eq!(
            default_tenant.scan_metric_rows_execution_accounting(),
            QueryExecutionAccounting::Unaccounted
        );
        let observed = Arc::new(BlockingTenantMetricScanStorage::new(default_tenant));
        let async_storage = AsyncStorage::from_storage(Arc::clone(&observed) as Arc<dyn Storage>)?;

        let error = async_storage
            .scan_metric_rows(
                "tenant_metric_default_async",
                0,
                20,
                QueryRowsScanOptions {
                    max_rows: Some(1),
                    row_offset: None,
                },
            )
            .await
            .expect_err("budgeted async default-tenant metric scan must fail closed");
        assert!(matches!(
            error,
            TsinkError::UnsupportedOperation {
                operation: "async_scan_metric_rows",
                reason,
            } if reason == "bounded async row scans require complete execution accounting"
        ));
        assert_eq!(observed.detailed_scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(observed.compatibility_scan_calls.load(Ordering::SeqCst), 0);
        wait_for_tenant_query_resources_to_release(&storage).await;
        let released = storage.query_budget_snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.queries_started_total, 1);
        assert_eq!(released.queries_completed_total, 1);
        assert_eq!(released.accounting_invariant_violations_total, 0);

        async_storage.close().await?;
        Ok(())
    }

    #[test]
    fn tenant_series_row_scan_retains_complete_guard_and_pagination() {
        let storage = make_storage();
        let visible_series = MetricSeries {
            name: "tenant_guarded_rows".to_string(),
            labels: vec![Label::new("host", "a")],
        };
        let scoped_rows = scope_rows_for_tenant(
            vec![
                Row::with_labels(
                    visible_series.name.clone(),
                    visible_series.labels.clone(),
                    DataPoint::new(10, 1.0),
                ),
                Row::with_labels(
                    visible_series.name.clone(),
                    visible_series.labels.clone(),
                    DataPoint::new(20, 2.0),
                ),
            ],
            "tenant-a",
        )
        .expect("tenant rows should scope");
        storage
            .insert_rows(&scoped_rows)
            .expect("tenant rows should insert");
        let tenant = scoped_storage(Arc::clone(&storage), "tenant-a");
        assert_eq!(
            tenant.scan_series_rows_execution_accounting(),
            QueryExecutionAccounting::Complete
        );

        let execution = tenant
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("query admission should succeed")
            .expect("built-in storage should expose a query budget");
        let result = tenant
            .scan_series_rows_with_execution_result(
                std::slice::from_ref(&visible_series),
                0,
                30,
                QueryRowsScanOptions {
                    max_rows: Some(1),
                    row_offset: None,
                },
                &execution,
            )
            .expect("tenant row page should succeed");
        assert_eq!(result.page.rows.len(), 1);
        assert!(result.page.truncated);
        assert_eq!(result.page.next_row_offset, Some(1));
        assert_eq!(result.page.rows[0].metric(), visible_series.name);
        assert_eq!(result.page.rows[0].labels(), visible_series.labels);
        assert!(result.page.rows[0]
            .labels()
            .iter()
            .all(|label| label.name != TENANT_LABEL));
        assert_eq!(
            result.reserved_memory_bytes(),
            tsink::modeled_query_rows_retained_bytes(&result.page.rows)
        );
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            result.reserved_memory_bytes()
        );
        assert_eq!(execution.snapshot().series_matched, 1);
        assert_eq!(execution.snapshot().samples_returned, 1);
        drop(result);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            0
        );
    }

    #[test]
    fn default_tenant_series_row_scan_accounting_remains_unaccounted() {
        let storage = make_storage();
        let tenant = scoped_storage(storage, DEFAULT_TENANT_ID);
        assert_eq!(
            tenant.scan_series_rows_execution_accounting(),
            QueryExecutionAccounting::Unaccounted,
            "default-tenant legacy fallback cannot preserve exact page counters through one inner row scan",
        );
    }

    #[test]
    fn tenant_list_metrics_detailed_result_retains_complete_guard() {
        let storage = make_storage();
        let scoped_rows = scope_rows_for_tenant(
            vec![Row::with_labels(
                "tenant_guarded_list",
                vec![Label::new("host", "a")],
                DataPoint::new(10, 1.0),
            )],
            "tenant-a",
        )
        .expect("tenant row should scope");
        storage
            .insert_rows(&scoped_rows)
            .expect("tenant row should insert");
        let tenant = scoped_storage(Arc::clone(&storage), "tenant-a");
        assert_eq!(
            tenant.list_metrics_execution_accounting(),
            QueryExecutionAccounting::Complete
        );

        let execution = tenant
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("query admission should succeed")
            .expect("built-in storage should expose a query budget");
        let result = tenant
            .list_metrics_with_execution_result(&execution)
            .expect("tenant metric listing should succeed");
        assert_eq!(result.series.len(), 1);
        assert_eq!(result.series[0].name, "tenant_guarded_list");
        assert!(result.series[0]
            .labels
            .iter()
            .all(|label| label.name != TENANT_LABEL));
        assert!(result.reserved_memory_bytes() > 0);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            result.reserved_memory_bytes()
        );
        drop(result);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            0
        );
    }

    #[test]
    fn read_selections_for_default_tenant_adds_scoped_and_unlabeled_fallback_matchers() {
        let selection = SeriesSelection::new()
            .with_metric("cpu_usage")
            .with_matcher(SeriesMatcher::equal("host", "a"));

        let selections = read_selections_for_tenant(&selection, DEFAULT_TENANT_ID)
            .expect("default tenant selections should build");

        assert_eq!(selections.len(), 2);
        assert_eq!(selections[0].metric.as_deref(), Some("cpu_usage"));
        assert!(selections[0]
            .matchers
            .contains(&SeriesMatcher::equal("host", "a")));
        assert!(selections[0]
            .matchers
            .contains(&SeriesMatcher::equal(TENANT_LABEL, DEFAULT_TENANT_ID)));
        assert!(selections[1]
            .matchers
            .contains(&SeriesMatcher::equal("host", "a")));
        assert!(selections[1]
            .matchers
            .contains(&SeriesMatcher::regex_no_match(
                TENANT_LABEL,
                UNLABELED_TENANT_FALLBACK_REGEX,
            )));
    }

    #[test]
    fn scoped_storage_filters_and_strips_tenant_label() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_millis() as i64;
        let storage = make_storage();
        let scoped_rows = scope_rows_for_tenant(
            vec![
                Row::with_labels(
                    "cpu_usage",
                    vec![Label::new("host", "a")],
                    DataPoint::new(now, 1.0),
                ),
                Row::with_labels(
                    "cpu_usage",
                    vec![Label::new("host", "b")],
                    DataPoint::new(now, 2.0),
                ),
            ],
            "tenant-a",
        )
        .expect("tenant scoping should succeed");
        storage
            .insert_rows(&scoped_rows)
            .expect("insert should succeed");
        let other_rows = scope_rows_for_tenant(
            vec![Row::with_labels(
                "cpu_usage",
                vec![Label::new("host", "a")],
                DataPoint::new(now, 3.0),
            )],
            "tenant-b",
        )
        .expect("tenant scoping should succeed");
        storage
            .insert_rows(&other_rows)
            .expect("insert should succeed");

        let tenant_a = scoped_storage(Arc::clone(&storage), "tenant-a");
        assert_eq!(
            tenant_a.select_many_execution_accounting(),
            QueryExecutionAccounting::Complete
        );
        assert_eq!(
            tenant_a.select_series_execution_accounting(),
            QueryExecutionAccounting::Complete
        );
        assert_eq!(
            tenant_a.select_series_in_shards_execution_accounting(),
            QueryExecutionAccounting::Complete
        );
        let execution = tenant_a
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("query admission should succeed")
            .expect("built-in storage should expose a query budget");
        let selected = tenant_a
            .select_series_with_execution_result(
                &SeriesSelection::new().with_metric("cpu_usage"),
                &execution,
            )
            .expect("detailed tenant metadata selection should succeed");
        assert_eq!(selected.series.len(), 2);
        assert!(selected.reserved_memory_bytes() > 0);
        assert!(selected
            .series
            .iter()
            .all(|series| series.labels.iter().all(|label| label.name != TENANT_LABEL)));
        assert!(execution.snapshot().memory_reserved_bytes > 0);
        drop(selected);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let series = tenant_a
            .list_metrics()
            .expect("list_metrics should succeed");
        assert_eq!(series.len(), 2);
        assert!(series
            .iter()
            .all(|series| series.labels.iter().all(|label| label.name != TENANT_LABEL)));

        let points = tenant_a
            .select(
                "cpu_usage",
                &[Label::new("host", "a")],
                now.saturating_sub(1),
                now.saturating_add(1),
            )
            .expect("select should succeed");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value_as_f64(), Some(1.0));
    }

    #[test]
    fn scoped_storage_preserves_shard_scoped_metadata_queries() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_millis() as i64;
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(8)
            .build()
            .expect("storage should build");

        let tenant_a_rows = scope_rows_for_tenant(
            vec![Row::with_labels(
                "cpu_usage",
                vec![Label::new("host", "a")],
                DataPoint::new(now, 1.0),
            )],
            "tenant-a",
        )
        .expect("tenant scoping should succeed");
        storage
            .insert_rows(&tenant_a_rows)
            .expect("insert should succeed");
        let tenant_b_rows = scope_rows_for_tenant(
            vec![Row::with_labels(
                "cpu_usage",
                vec![Label::new("host", "b")],
                DataPoint::new(now, 2.0),
            )],
            "tenant-b",
        )
        .expect("tenant scoping should succeed");
        storage
            .insert_rows(&tenant_b_rows)
            .expect("insert should succeed");

        let scope = (0..8u32)
            .map(|shard| MetadataShardScope::new(8, vec![shard]))
            .find(|scope| {
                !storage
                    .list_metrics_in_shards(scope)
                    .expect("base shard-scoped metadata lookup should succeed")
                    .is_empty()
            })
            .expect("one shard should contain the inserted series");

        let tenant_a = scoped_storage(Arc::clone(&storage), "tenant-a");
        let series = tenant_a
            .list_metrics_in_shards(&scope)
            .expect("shard-scoped list_metrics should succeed");
        assert_eq!(
            series,
            vec![MetricSeries {
                name: "cpu_usage".to_string(),
                labels: vec![Label::new("host", "a")],
            }]
        );

        let selected = tenant_a
            .select_series_in_shards(&SeriesSelection::new().with_metric("cpu_usage"), &scope)
            .expect("shard-scoped select_series should succeed");
        assert_eq!(selected, series);
    }

    #[test]
    fn scoped_storage_rejects_reserved_tenant_label() {
        let storage = scoped_storage(make_storage(), "tenant-a");
        let err = storage
            .insert_rows(&[Row::with_labels(
                "cpu_usage",
                vec![Label::new(TENANT_LABEL, "tenant-a")],
                DataPoint::new(10, 1.0),
            )])
            .expect_err("reserved label should be rejected");
        assert!(matches!(err, TsinkError::InvalidLabel(message) if message.contains(TENANT_LABEL)));
    }

    #[test]
    fn scoped_storage_best_effort_batch_preserves_original_rejection_indices() {
        let storage = scoped_storage(make_storage(), "tenant-a");
        let result = storage
            .write_batch(
                &[
                    Row::new("valid_before", DataPoint::new(1, 1_i64)),
                    Row::with_labels(
                        "reserved_label",
                        vec![Label::new(TENANT_LABEL, "caller-supplied")],
                        DataPoint::new(2, 2_i64),
                    ),
                    Row::new("", DataPoint::new(3, 3_i64)),
                    Row::new("valid_after", DataPoint::new(4, 4_i64)),
                ],
                WriteMode::BestEffort,
            )
            .expect("best-effort tenant batch should report indexed outcomes");

        assert_eq!(result.submitted, 4);
        assert_eq!(result.accepted, 2);
        assert_eq!(result.rejected, 2);
        assert_eq!(result.acknowledgement, Some(WriteAcknowledgement::Volatile));
        assert_eq!(
            result
                .outcomes
                .iter()
                .map(|outcome| outcome.index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(result.outcomes[0].status, RowWriteStatus::Accepted);
        let RowWriteStatus::Rejected(reserved) = &result.outcomes[1].status else {
            panic!("caller-supplied tenant label should be rejected");
        };
        assert_eq!(reserved.category, WriteRejectionCategory::InvalidLabels);
        assert_eq!(reserved.cause_index, Some(1));
        let RowWriteStatus::Rejected(invalid_metric) = &result.outcomes[2].status else {
            panic!("invalid metric should be rejected");
        };
        assert_eq!(
            invalid_metric.category,
            WriteRejectionCategory::InvalidMetric
        );
        assert_eq!(invalid_metric.cause_index, Some(2));
        assert_eq!(result.outcomes[3].status, RowWriteStatus::Accepted);

        let series = storage
            .list_metrics()
            .expect("accepted tenant rows should be visible");
        assert_eq!(
            series
                .iter()
                .map(|series| series.name.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["valid_after", "valid_before"])
        );
    }

    #[test]
    fn scoped_storage_rejects_malformed_filtered_best_effort_results_before_remapping() {
        let valid = accepted_batch_result(2);

        let mut invalid_submitted = valid.clone();
        invalid_submitted.submitted = 3;

        let mut missing = valid.clone();
        missing.outcomes.pop();

        let mut duplicate = valid.clone();
        duplicate.outcomes[1].index = 0;

        let mut out_of_range = valid.clone();
        out_of_range.outcomes[1].index = 2;

        let mut invalid_counts = valid.clone();
        invalid_counts.accepted = 1;
        invalid_counts.rejected = 1;

        let mut missing_acknowledgement = valid.clone();
        missing_acknowledgement.acknowledgement = None;

        let mut acknowledgement_without_acceptance = BatchWriteResult::from_outcomes(
            None,
            vec![
                RowWriteOutcome::rejected(0, test_rejection(Some(0))),
                RowWriteOutcome::rejected(1, test_rejection(Some(1))),
            ],
        );
        acknowledgement_without_acceptance.acknowledgement = Some(WriteAcknowledgement::Volatile);

        let invalid_cause_index = BatchWriteResult::from_outcomes(
            None,
            vec![
                RowWriteOutcome::rejected(0, test_rejection(Some(2))),
                RowWriteOutcome::rejected(1, test_rejection(Some(1))),
            ],
        );

        let malformed_results = [
            (invalid_submitted, "submitted count 3"),
            (missing, "outcome count 1"),
            (duplicate, "position 1 reported index 0"),
            (out_of_range, "position 1 reported index 2"),
            (invalid_counts, "reported counts accepted=1 rejected=1"),
            (
                missing_acknowledgement,
                "acknowledgement presence does not match accepted count 2",
            ),
            (
                acknowledgement_without_acceptance,
                "acknowledgement presence does not match accepted count 0",
            ),
            (invalid_cause_index, "out-of-range cause index 2"),
        ];
        let rows = [
            Row::new("valid_before", DataPoint::new(1, 1_i64)),
            Row::with_labels(
                "reserved_label",
                vec![Label::new(TENANT_LABEL, "caller-supplied")],
                DataPoint::new(2, 2_i64),
            ),
            Row::new("valid_after", DataPoint::new(3, 3_i64)),
        ];

        for (result, expected_detail) in malformed_results {
            let storage = scoped_storage(fixed_batch_storage(result), "tenant-a");
            let err = storage
                .write_batch(&rows, WriteMode::BestEffort)
                .expect_err("malformed inner outcomes must not be remapped or normalized");
            assert_malformed_batch_result(err, expected_detail);
        }
    }

    #[test]
    fn scoped_storage_validates_pass_through_results_for_each_write_mode() {
        let mixed_atomic = BatchWriteResult::from_outcomes(
            Some(WriteAcknowledgement::Volatile),
            vec![
                RowWriteOutcome::accepted(0),
                RowWriteOutcome::rejected(1, test_rejection(Some(1))),
            ],
        );
        let atomic = scoped_storage(fixed_batch_storage(mixed_atomic), "tenant-a");
        let rows = [
            Row::new("first", DataPoint::new(1, 1_i64)),
            Row::new("second", DataPoint::new(2, 2_i64)),
        ];
        let err = atomic
            .write_batch(&rows, WriteMode::Atomic)
            .expect_err("atomic backends must not report mixed outcomes");
        assert_malformed_batch_result(err, "atomic result mixed 1 accepted and 1 rejected rows");

        let mut missing_acknowledgement = accepted_batch_result(2);
        missing_acknowledgement.acknowledgement = None;
        let best_effort = scoped_storage(fixed_batch_storage(missing_acknowledgement), "tenant-a");
        let err = best_effort
            .write_batch(&rows, WriteMode::BestEffort)
            .expect_err("best-effort pass-through results must retain canonical acknowledgements");
        assert_malformed_batch_result(
            err,
            "acknowledgement presence does not match accepted count 2",
        );
    }

    #[test]
    fn scoped_storage_atomic_batch_rejects_all_rows_for_reserved_tenant_label() {
        let storage = scoped_storage(make_storage(), "tenant-a");
        let result = storage
            .write_batch(
                &[
                    Row::new("valid_before", DataPoint::new(1, 1_i64)),
                    Row::with_labels(
                        "reserved_label",
                        vec![Label::new(TENANT_LABEL, "caller-supplied")],
                        DataPoint::new(2, 2_i64),
                    ),
                    Row::new("valid_after", DataPoint::new(3, 3_i64)),
                ],
                WriteMode::Atomic,
            )
            .expect("reserved label should be an atomic canonical rejection");

        assert_eq!(result.submitted, 3);
        assert_eq!(result.accepted, 0);
        assert_eq!(result.rejected, 3);
        assert_eq!(result.acknowledgement, None);
        for (index, outcome) in result.outcomes.iter().enumerate() {
            assert_eq!(outcome.index, index);
            let RowWriteStatus::Rejected(rejection) = &outcome.status else {
                panic!("atomic policy rejection should reject every row");
            };
            assert_eq!(rejection.category, WriteRejectionCategory::InvalidLabels);
            assert_eq!(rejection.cause_index, Some(1));
        }
        assert!(storage
            .list_metrics()
            .expect("atomic rejection should leave metadata unchanged")
            .is_empty());
    }

    #[test]
    fn default_tenant_reads_legacy_unlabeled_series() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_millis() as i64;
        let storage = make_storage();
        storage
            .insert_rows(&[Row::with_labels(
                "legacy_metric",
                vec![Label::new("host", "a")],
                DataPoint::new(now, 1.0),
            )])
            .expect("insert should succeed");

        let default_tenant = scoped_storage(Arc::clone(&storage), DEFAULT_TENANT_ID);
        let series = default_tenant
            .select_series(
                &SeriesSelection::new()
                    .with_metric("legacy_metric")
                    .with_matcher(SeriesMatcher::equal("host", "a")),
            )
            .expect("select_series should succeed");
        assert_eq!(series.len(), 1);

        let points = default_tenant
            .select(
                "legacy_metric",
                &[Label::new("host", "a")],
                now.saturating_sub(1),
                now.saturating_add(1),
            )
            .expect("select should succeed");
        assert_eq!(points.len(), 1);

        let all = default_tenant
            .select_all(
                "legacy_metric",
                now.saturating_sub(1),
                now.saturating_add(1),
            )
            .expect("select_all should succeed");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].1.len(), 1);
    }

    #[test]
    fn default_tenant_metadata_reads_use_scoped_series_discovery() {
        let inner = Arc::new(RecordingMetadataStorage::new());
        let storage: Arc<dyn Storage> = inner.clone();
        let scoped = scoped_storage(storage, DEFAULT_TENANT_ID);
        let selection = SeriesSelection::new()
            .with_metric("cpu_usage")
            .with_matcher(SeriesMatcher::equal("host", "a"));
        let scope = MetadataShardScope::new(8, vec![1, 3]);

        let metrics = scoped.list_metrics().expect("list_metrics should succeed");
        assert_eq!(
            metrics,
            vec![
                MetricSeries {
                    name: "cpu_usage".to_string(),
                    labels: vec![Label::new("host", "current")],
                },
                MetricSeries {
                    name: "cpu_usage".to_string(),
                    labels: vec![Label::new("host", "legacy")],
                },
            ]
        );

        let selected = scoped
            .select_series(&selection)
            .expect("select_series should succeed");
        assert_eq!(selected, metrics);

        let shard_metrics = scoped
            .list_metrics_in_shards(&scope)
            .expect("list_metrics_in_shards should succeed");
        assert_eq!(shard_metrics, metrics);

        let shard_selected = scoped
            .select_series_in_shards(&selection, &scope)
            .expect("select_series_in_shards should succeed");
        assert_eq!(shard_selected, metrics);

        let recorded = inner
            .select_series_calls
            .lock()
            .expect("select_series calls should be readable")
            .clone();
        assert_eq!(recorded.len(), 4);
        assert_eq!(
            recorded[0],
            SeriesSelection::new()
                .with_matcher(SeriesMatcher::equal(TENANT_LABEL, DEFAULT_TENANT_ID,))
        );
        assert_eq!(
            recorded[1],
            SeriesSelection::new().with_matcher(SeriesMatcher::regex_no_match(
                TENANT_LABEL,
                UNLABELED_TENANT_FALLBACK_REGEX,
            ))
        );
        assert!(recorded[2]
            .matchers
            .contains(&SeriesMatcher::equal("host", "a")));
        assert!(recorded[2]
            .matchers
            .contains(&SeriesMatcher::equal(TENANT_LABEL, DEFAULT_TENANT_ID)));
        assert!(recorded[3]
            .matchers
            .contains(&SeriesMatcher::equal("host", "a")));
        assert!(recorded[3]
            .matchers
            .contains(&SeriesMatcher::regex_no_match(
                TENANT_LABEL,
                UNLABELED_TENANT_FALLBACK_REGEX,
            )));

        let shard_recorded = inner
            .select_series_in_shards_calls
            .lock()
            .expect("select_series_in_shards calls should be readable")
            .clone();
        assert_eq!(shard_recorded.len(), 4);
        assert_eq!(shard_recorded[0].1, scope);
        assert_eq!(shard_recorded[1].1, scope);
        assert_eq!(shard_recorded[2].1, scope);
        assert_eq!(shard_recorded[3].1, scope);
    }

    #[test]
    fn default_tenant_list_metrics_shares_one_query_envelope_and_bounds_its_merge() {
        let limits = |series_limit, intermediate_limit| QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(8 * 1024 * 1024),
            per_query: QueryWorkLimits {
                max_series_matched: Some(series_limit),
                max_intermediate_vector_size: Some(intermediate_limit),
                max_memory_bytes: Some(4 * 1024 * 1024),
                ..QueryWorkLimits::default()
            },
        };

        let (storage, scoped) = default_tenant_metadata_storage_with_limits(limits(2, 2));
        let before = storage.query_budget_snapshot();
        let listed = scoped
            .list_metrics()
            .expect("the exact request-wide tenant metadata envelope should pass");
        assert_eq!(listed.len(), 2);
        let after = storage.query_budget_snapshot();
        assert_eq!(
            after.queries_started_total - before.queries_started_total,
            1
        );
        assert_eq!(
            after.queries_completed_total - before.queries_completed_total,
            1
        );
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);

        let (_storage, scoped) = default_tenant_metadata_storage_with_limits(limits(1, 2));
        let err = scoped
            .list_metrics()
            .expect_err("two scoped selections must share the series limit");
        assert!(matches!(
            err,
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                if exceeded.reason == QueryLimitReason::SeriesMatched
                    && exceeded.limit == 1
        ));

        let (_storage, scoped) = default_tenant_metadata_storage_with_limits(limits(2, 1));
        let err = scoped
            .list_metrics()
            .expect_err("the tenant merge must observe its cumulative intermediate length");
        assert!(matches!(
            err,
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                if exceeded.reason == QueryLimitReason::IntermediateVectorSize
                    && exceeded.limit == 1
        ));
    }

    #[test]
    fn default_tenant_wal_metadata_list_retains_one_accounted_envelope() {
        let data_dir = tempfile::tempdir().unwrap();
        let storage = StorageBuilder::new()
            .with_data_path(data_dir.path())
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_query_budget_limits(QueryBudgetLimits {
                max_concurrent_queries: Some(1),
                max_shared_memory_bytes: Some(8 * 1024 * 1024),
                per_query: QueryWorkLimits {
                    max_series_matched: Some(8),
                    max_returned_bytes: Some(1024 * 1024),
                    max_intermediate_vector_size: Some(8),
                    max_memory_bytes: Some(4 * 1024 * 1024),
                    ..QueryWorkLimits::default()
                },
            })
            .build()
            .expect("persistent storage with query limits should build");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_millis() as i64;
        storage
            .insert_rows(&[Row::with_labels(
                "tenant_wal_budget",
                vec![Label::new("host", "legacy")],
                DataPoint::new(now, 1.0),
            )])
            .unwrap();
        storage
            .insert_rows(
                &scope_rows_for_tenant(
                    vec![Row::with_labels(
                        "tenant_wal_budget",
                        vec![Label::new("host", "current")],
                        DataPoint::new(now, 2.0),
                    )],
                    DEFAULT_TENANT_ID,
                )
                .unwrap(),
            )
            .unwrap();
        let scoped = scoped_storage(Arc::clone(&storage), DEFAULT_TENANT_ID);

        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .unwrap()
            .unwrap();
        let detailed = scoped
            .list_metrics_with_wal_with_execution_result(&execution)
            .expect("tenant WAL metadata must use the supplied execution");
        assert_eq!(detailed.series.len(), 2);
        assert!(detailed
            .series
            .iter()
            .all(|series| series.labels.iter().all(|label| label.name != TENANT_LABEL)));
        assert!(detailed.reserved_memory_bytes() > 0);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            detailed.reserved_memory_bytes()
        );
        drop(detailed);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);

        let before = storage.query_budget_snapshot();
        assert_eq!(scoped.list_metrics_with_wal().unwrap().len(), 2);
        let after = storage.query_budget_snapshot();
        assert_eq!(
            after.queries_started_total - before.queries_started_total,
            1
        );
        assert_eq!(
            after.queries_completed_total - before.queries_completed_total,
            1
        );
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
        storage.close().unwrap();
    }

    #[test]
    fn default_tenant_shard_metadata_reads_share_one_query_envelope() {
        let limits = |series_limit, intermediate_limit| QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(8 * 1024 * 1024),
            per_query: QueryWorkLimits {
                max_series_matched: Some(series_limit),
                max_intermediate_vector_size: Some(intermediate_limit),
                max_memory_bytes: Some(4 * 1024 * 1024),
                ..QueryWorkLimits::default()
            },
        };
        let scope = MetadataShardScope::new(1, vec![0]);

        let (storage, scoped) = default_tenant_metadata_storage_with_limits(limits(2, 2));
        let before = storage.query_budget_snapshot();
        let listed = scoped
            .list_metrics_in_shards(&scope)
            .expect("the exact request-wide tenant shard list should pass");
        assert_eq!(listed.len(), 2);
        let after_list = storage.query_budget_snapshot();
        assert_eq!(
            after_list.queries_started_total - before.queries_started_total,
            1
        );
        assert_eq!(
            after_list.queries_completed_total - before.queries_completed_total,
            1
        );
        assert_eq!(after_list.active_queries, 0);
        assert_eq!(after_list.shared_reserved_memory_bytes, 0);

        let selected = scoped
            .select_series_in_shards(&SeriesSelection::new(), &scope)
            .expect("the exact request-wide tenant shard selection should pass");
        assert_eq!(selected.len(), 2);
        let after_select = storage.query_budget_snapshot();
        assert_eq!(
            after_select.queries_started_total - after_list.queries_started_total,
            1
        );
        assert_eq!(
            after_select.queries_completed_total - after_list.queries_completed_total,
            1
        );
        assert_eq!(after_select.active_queries, 0);
        assert_eq!(after_select.shared_reserved_memory_bytes, 0);
        assert_eq!(after_select.accounting_invariant_violations_total, 0);

        let (_storage, scoped) = default_tenant_metadata_storage_with_limits(limits(1, 2));
        let err = scoped
            .list_metrics_in_shards(&scope)
            .expect_err("scoped and legacy shard branches must share the series limit");
        assert!(matches!(
            err,
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                if exceeded.reason == QueryLimitReason::SeriesMatched
                    && exceeded.limit == 1
        ));

        let (_storage, scoped) = default_tenant_metadata_storage_with_limits(limits(1, 2));
        let err = scoped
            .select_series_in_shards(&SeriesSelection::new(), &scope)
            .expect_err("direct shard selection must share one tenant query execution");
        assert!(matches!(
            err,
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                if exceeded.reason == QueryLimitReason::SeriesMatched
                    && exceeded.limit == 1
        ));

        let (_storage, scoped) = default_tenant_metadata_storage_with_limits(limits(2, 1));
        let err = scoped
            .list_metrics_in_shards(&scope)
            .expect_err("the shard-list merge must observe its cumulative intermediate length");
        assert!(matches!(
            err,
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                if exceeded.reason == QueryLimitReason::IntermediateVectorSize
                    && exceeded.limit == 1
        ));
    }

    #[test]
    fn default_tenant_batch_fallback_preflights_vector_and_memory_exactly() {
        let limits = QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(64 * 1024 * 1024),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(32 * 1024 * 1024),
                ..QueryWorkLimits::default()
            },
        };
        let (storage, scoped, series) = default_tenant_batch_storage_with_limits(limits);

        let calibration = storage
            .begin_query_execution(
                QueryWorkLimits {
                    max_intermediate_vector_size: Some(2),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("calibration query should start")
            .expect("finite storage should expose one query execution");
        let rows = scoped
            .select_many_with_execution(&series, 9, 11, &calibration)
            .expect("calibration query should succeed");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.points.len() == 1));
        assert_eq!(calibration.snapshot().intermediate_vector_size, 2);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        let calibrated_peak = storage
            .query_budget_snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(calibrated_peak > 0);

        let exact = storage
            .begin_query_execution(
                QueryWorkLimits {
                    max_intermediate_vector_size: Some(2),
                    max_memory_bytes: Some(calibrated_peak),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("exact query should start")
            .expect("finite storage should expose one query execution");
        scoped
            .select_many_with_execution(&series, 9, 11, &exact)
            .expect("the calibrated exact memory and vector limits should pass");
        assert_eq!(exact.snapshot().intermediate_vector_size, 2);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);

        let one_under_memory = storage
            .begin_query_execution(
                QueryWorkLimits {
                    max_intermediate_vector_size: Some(2),
                    max_memory_bytes: Some(calibrated_peak - 1),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("one-under-memory query should start")
            .expect("finite storage should expose one query execution");
        let error = scoped
            .select_many_with_execution(&series, 9, 11, &one_under_memory)
            .expect_err("one byte below the calibrated peak must fail");
        assert!(matches!(
            error,
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
        ));
        assert_eq!(one_under_memory.snapshot().memory_reserved_bytes, 0);
        drop(one_under_memory);

        let one_under_vector = storage
            .begin_query_execution(
                QueryWorkLimits {
                    max_intermediate_vector_size: Some(1),
                    max_memory_bytes: Some(calibrated_peak),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("one-under-vector query should start")
            .expect("finite storage should expose one query execution");
        let error = scoped
            .select_many_with_execution(&series, 9, 11, &one_under_vector)
            .expect_err("a two-item batch must exceed a vector limit of one");
        assert!(matches!(
            error,
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                if exceeded.reason == QueryLimitReason::IntermediateVectorSize
        ));
        assert_eq!(one_under_vector.snapshot().memory_reserved_bytes, 0);
        drop(one_under_vector);

        let released = storage.query_budget_snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn default_tenant_batch_does_not_fallback_when_scoped_series_exists_outside_range() {
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_query_budget_limits(QueryBudgetLimits {
                max_concurrent_queries: Some(1),
                max_shared_memory_bytes: Some(8 * 1024 * 1024),
                per_query: QueryWorkLimits {
                    max_series_matched: Some(1),
                    max_memory_bytes: Some(4 * 1024 * 1024),
                    ..QueryWorkLimits::default()
                },
            })
            .build()
            .expect("storage with query limits should build");
        let visible_series = MetricSeries {
            name: "default_tenant_empty_range".to_string(),
            labels: vec![Label::new("host", "same")],
        };
        storage
            .insert_rows(&[Row::with_labels(
                visible_series.name.clone(),
                visible_series.labels.clone(),
                DataPoint::new(10, 1.0),
            )])
            .expect("legacy series should insert");
        storage
            .insert_rows(
                &scope_rows_for_tenant(
                    vec![Row::with_labels(
                        visible_series.name.clone(),
                        visible_series.labels.clone(),
                        DataPoint::new(20, 2.0),
                    )],
                    DEFAULT_TENANT_ID,
                )
                .expect("scoped row should be valid"),
            )
            .expect("scoped series should insert");

        let scoped = scoped_storage(Arc::clone(&storage), DEFAULT_TENANT_ID);
        let execution = storage
            .begin_query_execution(
                QueryWorkLimits {
                    max_series_matched: Some(1),
                    max_memory_bytes: Some(4 * 1024 * 1024),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("query should start")
            .expect("finite storage should expose one query execution");
        let result = scoped
            .select_many_with_execution_result(&[visible_series], 9, 11, &execution)
            .expect("scoped existence should suppress legacy fallback");

        assert_eq!(result.series.len(), 1);
        assert!(result.series[0].points.is_empty());
        assert_eq!(result.matched_selectors.as_deref(), Some([true].as_slice()));
        assert_eq!(execution.snapshot().series_matched, 1);
        assert!(result.reserved_memory_bytes() > 0);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            result.reserved_memory_bytes()
        );

        drop(result);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let released = storage.query_budget_snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn default_tenant_select_all_uses_scoped_series_discovery() {
        let inner = Arc::new(RecordingMetadataStorage::new());
        let storage: Arc<dyn Storage> = inner.clone();
        let scoped = scoped_storage(storage, DEFAULT_TENANT_ID);

        let all = scoped
            .select_all("cpu_usage", 10, 20)
            .expect("select_all should succeed");

        assert_eq!(
            all,
            vec![
                (
                    vec![Label::new("host", "current")],
                    vec![DataPoint::new(10, 2.0)]
                ),
                (
                    vec![Label::new("host", "legacy")],
                    vec![DataPoint::new(10, 1.0)]
                ),
            ]
        );

        let recorded_series = inner
            .select_series_calls
            .lock()
            .expect("select_series calls should be readable")
            .clone();
        assert_eq!(recorded_series.len(), 2);
        assert_eq!(
            recorded_series[0],
            SeriesSelection::new()
                .with_metric("cpu_usage")
                .with_time_range(10, 20)
                .with_matcher(SeriesMatcher::equal(TENANT_LABEL, DEFAULT_TENANT_ID))
        );
        assert_eq!(
            recorded_series[1],
            SeriesSelection::new()
                .with_metric("cpu_usage")
                .with_time_range(10, 20)
                .with_matcher(SeriesMatcher::regex_no_match(
                    TENANT_LABEL,
                    UNLABELED_TENANT_FALLBACK_REGEX,
                ))
        );

        let recorded_points = inner
            .select_many_calls
            .lock()
            .expect("select_many calls should be readable")
            .clone();
        assert_eq!(recorded_points.len(), 1);
        assert_eq!(recorded_points[0].1, 10);
        assert_eq!(recorded_points[0].2, 20);
        assert_eq!(
            recorded_points[0].0,
            vec![
                RecordingMetadataStorage::scoped_series(),
                RecordingMetadataStorage::legacy_series(),
            ]
        );
    }

    #[test]
    fn tenant_runtime_cache_limit_defaults_and_validates_reserved_capacity() {
        let registry = TenantRegistry::from_json_str("{}")
            .expect("empty tenant policy should use a finite runtime limit");
        assert_eq!(
            registry.runtime_cache_metrics_snapshot(),
            TenantRuntimeCacheMetricsSnapshot {
                initialized_runtimes: 0,
                initialized_reserved_runtimes: 0,
                initialized_dynamic_runtimes: 0,
                max_runtimes: DEFAULT_TENANT_RUNTIME_MAX_TENANTS,
                reserved_runtimes: 1,
                limit_rejections_total: 0,
            }
        );

        assert_eq!(
            TenantRegistry::from_json_str(r#"{"maxRuntimeTenants":0}"#)
                .expect_err("a zero runtime limit must fail"),
            "maxRuntimeTenants must be greater than zero"
        );
        assert_eq!(
            TenantRegistry::from_json_str(
                r#"{
                    "maxRuntimeTenants": 1,
                    "tenants": { "team-a": {} }
                }"#,
            )
            .expect_err("the limit must reserve the configured and default tenants"),
            "maxRuntimeTenants must be at least 2 to reserve every configured tenant and the default tenant"
        );
        assert_eq!(
            TenantRegistry::from_json_str(
                r#"{
                    "maxRuntimeTenants": 1,
                    "tenants": { "": {} }
                }"#,
            )
            .expect_err("legacy tenant validation must precede the new capacity relationship"),
            format!("{TENANT_HEADER} must not be empty")
        );
    }

    #[test]
    fn tenant_runtime_cache_reserves_configured_and_default_slots_without_eviction() {
        let registry = TenantRegistry::from_json_str(
            r#"{
                "maxRuntimeTenants": 4,
                "tenants": { "team-a": {} }
            }"#,
        )
        .expect("bounded tenant registry should parse");

        registry
            .initialize_tenant_runtime("dynamic-a")
            .expect("first dynamic tenant should use an unreserved slot");
        registry
            .initialize_tenant_runtime("dynamic-b")
            .expect("second dynamic tenant should use the last unreserved slot");
        let limit_error = registry
            .initialize_tenant_runtime("dynamic-c")
            .expect_err("a third dynamic tenant must not consume a reserved slot");
        assert_eq!(limit_error, tenant_runtime_cache_limit_error(4));
        let response = limit_error.to_http_response();
        assert_eq!(response.status, 503);
        assert_eq!(
            response
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("X-Tsink-Tenant-Error-Code"))
                .map(|(_, value)| value.as_str()),
            Some(TENANT_RUNTIME_CACHE_LIMIT_ERROR_CODE)
        );
        assert!(!response
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("Retry-After")));
        assert_eq!(
            std::str::from_utf8(&response.body).expect("limit response should be UTF-8"),
            "tenant runtime cache limit of 4 tenants would be exceeded"
        );

        registry
            .initialize_tenant_runtime("team-a")
            .expect("configured tenant must retain its reserved slot");
        registry
            .initialize_tenant_runtime(DEFAULT_TENANT_ID)
            .expect("default tenant must retain its reserved slot");
        registry
            .initialize_tenant_runtime("dynamic-a")
            .expect("an existing tenant must remain available at the limit");
        assert_eq!(
            registry.runtime_cache_metrics_snapshot(),
            TenantRuntimeCacheMetricsSnapshot {
                initialized_runtimes: 4,
                initialized_reserved_runtimes: 2,
                initialized_dynamic_runtimes: 2,
                max_runtimes: 4,
                reserved_runtimes: 2,
                limit_rejections_total: 1,
            }
        );
    }

    #[test]
    fn tenant_runtime_cache_authorizes_before_consuming_capacity() {
        let registry = TenantRegistry::from_json_str(
            r#"{
                "maxRuntimeTenants": 2,
                "defaults": {
                    "auth": {
                        "tokens": [{ "token": "default-read", "scopes": ["read"] }]
                    }
                },
                "tenants": {
                    "secure": {
                        "auth": {
                            "tokens": [
                                { "token": "shared", "scopes": ["read"] },
                                { "token": "shared", "scopes": ["write"] }
                            ]
                        }
                    }
                }
            }"#,
        )
        .expect("bounded authenticated registry should parse");
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        assert_eq!(
            prepare_request_plan(
                Some(&registry),
                None,
                &request,
                "unknown",
                TenantAccessScope::Read,
            )
            .expect_err("missing credentials should retain their authorization error"),
            TenantRequestError::Unauthorized("tenant_auth_token_missing")
        );
        assert_eq!(registry.initialized_runtime_count(), 0);
        assert_eq!(
            registry
                .runtime_cache_metrics_snapshot()
                .limit_rejections_total,
            0
        );

        let default_token_request = HttpRequest {
            headers: HashMap::from([(
                "authorization".to_string(),
                "Bearer default-read".to_string(),
            )]),
            ..request.clone()
        };
        assert_eq!(
            prepare_request_plan(
                Some(&registry),
                None,
                &default_token_request,
                "unknown",
                TenantAccessScope::Read,
            )
            .expect_err("an authorized unconfigured tenant should reach the capacity policy"),
            tenant_runtime_cache_limit_error(2)
        );
        assert_eq!(registry.initialized_runtime_count(), 0);

        let shared_token_request = HttpRequest {
            headers: HashMap::from([("authorization".to_string(), "Bearer shared".to_string())]),
            ..request
        };
        prepare_request_plan(
            Some(&registry),
            None,
            &shared_token_request,
            "secure",
            TenantAccessScope::Read,
        )
        .expect("duplicate token read scope should authorize");
        prepare_request_plan(
            Some(&registry),
            None,
            &shared_token_request,
            "secure",
            TenantAccessScope::Write,
        )
        .expect("duplicate token write scope should remain merged");
        assert_eq!(registry.initialized_runtime_count(), 1);
    }

    #[test]
    fn tenant_runtime_cache_concurrent_misses_cannot_exceed_the_limit() {
        const WORKERS: usize = 32;
        const LIMIT: usize = 10;
        let registry = Arc::new(
            TenantRegistry::from_json_str(r#"{"maxRuntimeTenants":10}"#)
                .expect("bounded tenant registry should parse"),
        );
        let barrier = Arc::new(Barrier::new(WORKERS + 1));
        let mut workers = Vec::with_capacity(WORKERS);
        for index in 0..WORKERS {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                registry.initialize_tenant_runtime(&format!("dynamic-{index}"))
            }));
        }
        barrier.wait();

        let mut admitted = 0usize;
        let mut rejected = 0usize;
        for worker in workers {
            match worker
                .join()
                .expect("tenant initialization worker should join")
            {
                Ok(()) => admitted = admitted.saturating_add(1),
                Err(error) if error == tenant_runtime_cache_limit_error(LIMIT) => {
                    rejected = rejected.saturating_add(1);
                }
                Err(error) => panic!("unexpected tenant initialization error: {error:?}"),
            }
        }

        assert_eq!(
            admitted,
            LIMIT - 1,
            "the default tenant owns one reserved slot"
        );
        assert_eq!(rejected, WORKERS - admitted);
        assert_eq!(registry.initialized_runtime_count(), admitted);
        registry
            .initialize_tenant_runtime(DEFAULT_TENANT_ID)
            .expect("the reserved default slot should remain available");
        assert_eq!(registry.initialized_runtime_count(), LIMIT);
        assert_eq!(
            registry
                .runtime_cache_metrics_snapshot()
                .limit_rejections_total,
            u64::try_from(rejected).expect("rejection count should fit u64")
        );
    }

    #[test]
    fn tenant_registry_enforces_scoped_tokens_and_merges_policies() {
        let registry = TenantRegistry::from_json_str(
            r#"{
                "defaults": {
                    "quotas": {
                        "maxQueryLengthBytes": 64,
                        "maxRangePointsPerQuery": 16
                    },
                    "admission": {
                        "maxInflightReads": 1
                    }
                },
                "tenants": {
                    "team-a": {
                        "auth": {
                            "tokens": [
                                { "token": "team-a-read", "scopes": ["read"] },
                                { "token": "team-a-write", "scopes": ["write"] }
                            ]
                        },
                        "quotas": {
                            "maxQueryLengthBytes": 32
                        },
                        "cluster": {
                            "writeConsistency": "all",
                            "readConsistency": "strict",
                            "readPartialResponse": "deny"
                        }
                    }
                }
            }"#,
        )
        .expect("tenant registry should parse");

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query".to_string(),
            headers: HashMap::from([
                (TENANT_HEADER.to_string(), "team-a".to_string()),
                (
                    "authorization".to_string(),
                    "Bearer team-a-read".to_string(),
                ),
            ]),
            body: Vec::new(),
        };
        let guard = prepare_request(
            Some(&registry),
            None,
            &request,
            "team-a",
            TenantAccessScope::Read,
        )
        .expect("read token should authorize read");
        assert_eq!(guard.policy().max_query_length_bytes, Some(32));
        assert_eq!(guard.policy().max_range_points_per_query, Some(16));
        assert_eq!(
            guard.policy().write_consistency,
            Some(ClusterWriteConsistency::All)
        );
        assert_eq!(
            guard.policy().read_consistency,
            Some(ClusterReadConsistency::Strict)
        );
        assert_eq!(
            guard.policy().read_partial_response_policy,
            Some(ClusterReadPartialResponsePolicy::Deny)
        );

        let write_err = prepare_request(
            Some(&registry),
            None,
            &request,
            "team-a",
            TenantAccessScope::Write,
        )
        .expect_err("read-only token must not authorize writes");
        assert_eq!(
            write_err,
            TenantRequestError::Forbidden("tenant_auth_scope_denied")
        );

        let default_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query_range".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let default_guard = prepare_request(
            Some(&registry),
            None,
            &default_request,
            "dynamic-tenant",
            TenantAccessScope::Read,
        )
        .expect("dynamic tenant should inherit defaults");
        assert_eq!(default_guard.policy().max_query_length_bytes, Some(64));
        assert_eq!(default_guard.policy().max_range_points_per_query, Some(16));
    }

    #[test]
    fn tenant_registry_enforces_inflight_limits() {
        let registry = TenantRegistry::from_json_str(
            r#"{
                "tenants": {
                    "team-a": {
                        "auth": {
                            "tokens": [{ "token": "team-a-read", "scopes": ["read"] }]
                        },
                        "admission": {
                            "maxInflightReads": 1
                        }
                    }
                }
            }"#,
        )
        .expect("tenant registry should parse");
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/labels".to_string(),
            headers: HashMap::from([
                (TENANT_HEADER.to_string(), "team-a".to_string()),
                (
                    "authorization".to_string(),
                    "Bearer team-a-read".to_string(),
                ),
            ]),
            body: Vec::new(),
        };
        let before = tenant_admission_metrics_snapshot();

        let first = prepare_request(
            Some(&registry),
            None,
            &request,
            "team-a",
            TenantAccessScope::Read,
        )
        .expect("first read request should acquire permit");
        let second = prepare_request(
            Some(&registry),
            None,
            &request,
            "team-a",
            TenantAccessScope::Read,
        )
        .expect_err("second read request should be limited");
        assert!(
            matches!(second, TenantRequestError::TooManyRequests(message) if message.contains("max inflight read requests"))
        );
        let during = tenant_admission_metrics_snapshot();
        assert!(during.read_rejections_total >= before.read_rejections_total.saturating_add(1));
        assert!(during.active_reads >= before.active_reads.saturating_add(1));
        drop(first);
        prepare_request(
            Some(&registry),
            None,
            &request,
            "team-a",
            TenantAccessScope::Read,
        )
        .expect("permit should be released after guard drop");
    }

    #[test]
    fn tenant_registry_tracks_surface_budgets_and_recent_decisions() {
        let registry = TenantRegistry::from_json_str(
            r#"{
                "tenants": {
                    "team-a": {
                        "admission": {
                            "query": {
                                "maxInflightRequests": 1
                            },
                            "retention": {
                                "maxInflightRequests": 1
                            }
                        }
                    }
                }
            }"#,
        )
        .expect("tenant registry should parse");
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query".to_string(),
            headers: HashMap::from([(TENANT_HEADER.to_string(), "team-a".to_string())]),
            body: Vec::new(),
        };

        let plan = prepare_request_plan(
            Some(&registry),
            None,
            &request,
            "team-a",
            TenantAccessScope::Read,
        )
        .expect("tenant request plan should prepare");
        assert_eq!(plan.tenant_id(), "team-a");
        let held = plan
            .admit(TenantAdmissionSurface::Query, 1)
            .expect("first query request should acquire surface budget");
        let throttled = plan
            .admit(TenantAdmissionSurface::Query, 1)
            .expect_err("second query request should be throttled");
        assert!(matches!(
            throttled,
            TenantRequestError::TooManyRequests(message) if message.contains("max inflight query requests")
        ));
        plan.record_rejected(
            TenantAdmissionSurface::Metadata,
            3,
            "tenant metadata matcher limit exceeded: 3 > 2",
        );

        let trusted =
            prepare_trusted_request(Some(&registry), None, "team-a", TenantAccessScope::Write)
                .expect("trusted retention request should bypass auth");
        drop(trusted);

        let status = registry
            .status_snapshot_for("team-a")
            .expect("tenant status snapshot should build");
        assert_eq!(status.query.max_inflight_requests, Some(1));
        assert_eq!(status.query.active_requests, 1);
        assert_eq!(status.query.rejections_total, 1);
        assert!(status
            .recent_decisions
            .iter()
            .any(|decision| decision.surface == "query" && decision.outcome == "admitted"));
        assert!(status
            .recent_decisions
            .iter()
            .any(|decision| decision.surface == "query" && decision.outcome == "throttled"));
        assert!(status
            .recent_decisions
            .iter()
            .any(|decision| decision.surface == "metadata" && decision.outcome == "rejected"));
        drop(held);
    }

    #[test]
    fn prewarmed_admission_keeps_decision_log_heap_stable_and_status_exact() {
        let registry = TenantRegistry::from_json_str(
            r#"{
                "tenants": {
                    "team-a": {}
                }
            }"#,
        )
        .expect("tenant registry should parse");
        assert_eq!(registry.initialized_runtime_count(), 0);
        assert!(matches!(
            registry.initialize_tenant_runtime(""),
            Err(TenantRequestError::BadRequest(_))
        ));
        assert_eq!(registry.initialized_runtime_count(), 0);
        registry
            .initialize_tenant_runtime("team-a")
            .expect("tenant runtime should prewarm without admission");
        assert_eq!(registry.initialized_runtime_count(), 1);
        registry
            .initialize_tenant_runtime("team-a")
            .expect("prewarming an initialized tenant should be idempotent");
        assert_eq!(registry.initialized_runtime_count(), 1);
        let runtime = registry
            .runtime_for("team-a")
            .expect("prewarmed tenant runtime should resolve");
        let (before_len, before_capacity, before_bytes) = runtime.decision_log_retained_state();
        assert_eq!(before_len, 0);
        assert!(before_capacity >= TENANT_DECISION_HISTORY_LIMIT);

        let plan =
            prepare_trusted_request_plan(Some(&registry), None, "team-a", TenantAccessScope::Read)
                .expect("prewarmed tenant request plan should prepare");
        let guard = plan
            .admit(TenantAdmissionSurface::Query, 7)
            .expect("prewarmed tenant request should admit");
        let (after_len, after_capacity, after_bytes) = runtime.decision_log_retained_state();
        assert_eq!(after_len, 1);
        assert_eq!(after_capacity, before_capacity);
        assert_eq!(after_bytes, before_bytes);

        let expected = registry
            .status_snapshot_for("team-a")
            .expect("legacy tenant status should build");
        assert_eq!(expected.recent_decisions.len(), 1);
        assert_eq!(expected.recent_decisions[0].access, "read");
        assert_eq!(expected.recent_decisions[0].surface, "query");
        assert_eq!(expected.recent_decisions[0].outcome, "admitted");
        assert_eq!(expected.recent_decisions[0].requested_units, 7);
        assert_eq!(
            expected.recent_decisions[0].reason,
            "tenant request admitted for query via read scope"
        );

        runtime.reset_status_snapshot_string_clones();
        let budget = QueryBudget::new(QueryBudgetLimits::default())
            .expect("tenant status projection budget should build");
        let execution = budget
            .begin_query()
            .expect("tenant status projection query should admit");
        let projected = registry
            .status_snapshot_for_with_execution("team-a", &execution)
            .expect("accounted tenant status should build");
        assert_eq!(&*projected, &expected);
        assert_eq!(runtime.status_snapshot_string_clones(), 5);
        drop(projected);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
        drop(guard);
    }

    #[test]
    fn ordinary_decision_reasons_are_compact_and_status_exact() {
        let registry = TenantRegistry::from_json_str(
            r#"{
                "tenants": {
                    "team-a": {
                        "admission": {
                            "query": {
                                "maxInflightRequests": 1
                            },
                            "metadata": {
                                "maxInflightUnits": 2
                            },
                            "retention": {
                                "maxInflightUnits": 2
                            }
                        }
                    }
                }
            }"#,
        )
        .expect("tenant registry should parse");
        registry
            .initialize_tenant_runtime("team-a")
            .expect("tenant runtime should prewarm without admission");
        let runtime = registry
            .runtime_for("team-a")
            .expect("prewarmed tenant runtime should resolve");
        let (before_len, before_capacity, before_bytes) = runtime.decision_log_retained_state();
        assert_eq!(before_len, 0);

        let plan =
            prepare_trusted_request_plan(Some(&registry), None, "team-a", TenantAccessScope::Read)
                .expect("tenant request plan should prepare");
        let held_query = plan
            .admit(TenantAdmissionSurface::Query, 1)
            .expect("first query request should admit");
        assert_eq!(
            plan.admit(TenantAdmissionSurface::Query, 1)
                .expect_err("second query request should throttle"),
            TenantRequestError::TooManyRequests(
                "tenant 'team-a' exceeded max inflight query requests (1)".to_string()
            )
        );
        assert_eq!(
            plan.admit(TenantAdmissionSurface::Metadata, 3)
                .expect_err("oversized metadata request should reject"),
            TenantRequestError::TooManyRequests(
                "tenant 'team-a' exceeded max inflight metadata units: 3 > 2".to_string()
            )
        );
        let held_retention = plan
            .admit(TenantAdmissionSurface::Retention, 2)
            .expect("first retention request should admit");
        assert_eq!(
            plan.admit(TenantAdmissionSurface::Retention, 1)
                .expect_err("second retention request should throttle"),
            TenantRequestError::TooManyRequests(
                "tenant 'team-a' exceeded max inflight retention units (2)".to_string()
            )
        );

        let (after_len, after_capacity, after_bytes) = runtime.decision_log_retained_state();
        assert_eq!(after_len, 5);
        assert_eq!(after_capacity, before_capacity);
        assert_eq!(after_bytes, before_bytes);

        let status = registry
            .status_snapshot_for("team-a")
            .expect("legacy tenant status should build");
        assert_eq!(
            status
                .recent_decisions
                .iter()
                .map(|decision| (
                    decision.surface.as_str(),
                    decision.outcome.as_str(),
                    decision.requested_units,
                    decision.reason.as_str(),
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    "query",
                    "admitted",
                    1,
                    "tenant request admitted for query via read scope",
                ),
                (
                    "query",
                    "throttled",
                    1,
                    "tenant 'team-a' exceeded max inflight query requests (1)",
                ),
                (
                    "metadata",
                    "rejected",
                    3,
                    "tenant 'team-a' exceeded max inflight metadata units: 3 > 2",
                ),
                (
                    "retention",
                    "admitted",
                    2,
                    "tenant request admitted for retention via read scope",
                ),
                (
                    "retention",
                    "throttled",
                    1,
                    "tenant 'team-a' exceeded max inflight retention units (2)",
                ),
            ]
        );

        let budget = QueryBudget::new(QueryBudgetLimits::default())
            .expect("tenant status projection budget should build");
        let execution = budget
            .begin_query()
            .expect("tenant status projection query should admit");
        let projected = registry
            .status_snapshot_for_with_execution("team-a", &execution)
            .expect("accounted tenant status should build");
        assert_eq!(&*projected, &status);
        drop(projected);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
        drop(held_retention);
        drop(held_query);
    }

    #[test]
    fn tenant_status_projection_is_schema_complete_and_field_equivalent() {
        let registry = tenant_status_projection_registry();
        let runtime = registry
            .runtime_for("team-a")
            .expect("tenant status projection runtime should resolve");
        let held = runtime
            .admit(
                "team-a",
                TenantAccessScope::Read,
                TenantAdmissionSurface::Query,
                2,
            )
            .expect("tenant status fixture request should admit");
        let expected = registry
            .status_snapshot_for("team-a")
            .expect("legacy tenant status snapshot should build");
        runtime.reset_status_snapshot_string_clones();

        let budget = QueryBudget::new(QueryBudgetLimits::default())
            .expect("tenant status projection budget should build");
        let execution = budget
            .begin_query()
            .expect("tenant status query should admit");
        let projected = registry
            .status_snapshot_for_with_execution("team-a", &execution)
            .expect("accounted tenant status snapshot should build");

        assert_eq!(
            &*projected, &expected,
            "the accounted producer must preserve every legacy status field"
        );
        assert_eq!(
            runtime.status_snapshot_string_clones(),
            1 + u64::try_from(expected.recent_decisions.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(4),
            "the producer should copy exactly the tenant id and four strings per decision"
        );
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            projected.accounted_bytes()
        );
        assert_eq!(projected.query.active_requests, 1);
        assert_eq!(projected.query.active_units, 2);
        assert_eq!(projected.active_reads, 1);

        drop(projected);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        drop(held);
        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn tenant_status_projection_enforces_exact_peak_before_materialization() {
        let registry = tenant_status_projection_registry();
        let runtime = registry
            .runtime_for("team-a")
            .expect("tenant status projection runtime should resolve");
        runtime.reset_status_snapshot_string_clones();

        let calibration_budget = QueryBudget::new(QueryBudgetLimits::default())
            .expect("calibration budget should build");
        let calibration = calibration_budget
            .begin_query()
            .expect("calibration query should admit");
        let calibrated = registry
            .status_snapshot_for_with_execution("team-a", &calibration)
            .expect("calibration status projection should build");
        let required_bytes = calibrated.accounted_bytes();
        assert!(required_bytes > 0);
        assert_eq!(
            calibration_budget
                .snapshot()
                .peak_shared_reserved_memory_bytes,
            required_bytes,
            "this projection has no dynamic scratch beyond its retained output"
        );
        assert!(runtime.status_snapshot_string_clones() > 0);
        drop(calibrated);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        let calibration_released = calibration_budget.snapshot();
        assert_eq!(calibration_released.active_queries, 0);
        assert_eq!(calibration_released.shared_reserved_memory_bytes, 0);
        assert_eq!(
            calibration_released.accounting_invariant_violations_total,
            0
        );

        runtime.reset_status_snapshot_string_clones();
        let exact_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(required_bytes),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(required_bytes),
                ..QueryWorkLimits::default()
            },
        })
        .expect("exact tenant status budget should build");
        let exact = exact_budget
            .begin_query()
            .expect("exact query should admit");
        let exact_snapshot = registry
            .status_snapshot_for_with_execution("team-a", &exact)
            .expect("the exact modeled tenant status peak should pass");
        assert_eq!(exact_snapshot.accounted_bytes(), required_bytes);
        assert!(runtime.status_snapshot_string_clones() > 0);
        drop(exact_snapshot);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        let exact_released = exact_budget.snapshot();
        assert_eq!(exact_released.active_queries, 0);
        assert_eq!(exact_released.shared_reserved_memory_bytes, 0);
        assert_eq!(exact_released.accounting_invariant_violations_total, 0);

        runtime.reset_status_snapshot_string_clones();
        let one_under_budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(required_bytes),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(required_bytes.saturating_sub(1)),
                ..QueryWorkLimits::default()
            },
        })
        .expect("one-under tenant status budget should build");
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under query should admit");
        let error = registry
            .status_snapshot_for_with_execution("team-a", &one_under)
            .expect_err("one byte below the tenant status model must reject");
        match error {
            TenantStatusSnapshotError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded)) => {
                assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes);
                assert_eq!(exceeded.current, 0);
                assert_eq!(exceeded.requested, required_bytes);
            }
            other => panic!("unexpected tenant status projection error: {other:?}"),
        }
        assert_eq!(
            runtime.status_snapshot_string_clones(),
            0,
            "failed admission must precede every output string copy"
        );
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        assert_eq!(
            one_under_budget
                .snapshot()
                .peak_shared_reserved_memory_bytes,
            0
        );
        drop(one_under);
        let one_under_released = one_under_budget.snapshot();
        assert_eq!(one_under_released.active_queries, 0);
        assert_eq!(one_under_released.shared_reserved_memory_bytes, 0);
        assert_eq!(one_under_released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn tenant_status_projection_honors_precancellation_and_preserves_request_errors() {
        let registry = tenant_status_projection_registry();
        let runtime = registry
            .runtime_for("team-a")
            .expect("tenant status projection runtime should resolve");
        runtime.reset_status_snapshot_string_clones();
        let budget = QueryBudget::new(QueryBudgetLimits::default())
            .expect("tenant status cancellation budget should build");
        let cancellation = QueryCancellationToken::new();
        let execution = budget
            .begin_query_with(QueryWorkLimits::default(), cancellation.clone())
            .expect("tenant status cancellation query should admit");
        cancellation.cancel();

        let error = registry
            .status_snapshot_for_with_execution("team-a", &execution)
            .expect_err("pre-cancelled tenant status projection must stop");
        assert!(matches!(
            error,
            TenantStatusSnapshotError::QueryBudget(QueryBudgetError::Cancelled)
        ));
        assert_eq!(runtime.status_snapshot_string_clones(), 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(budget.snapshot().peak_shared_reserved_memory_bytes, 0);

        let expected_request_error = registry
            .status_snapshot_for("")
            .expect_err("legacy tenant status must reject an invalid id");
        let projected_request_error = registry
            .status_snapshot_for_with_execution("", &execution)
            .expect_err("accounted tenant status must reject an invalid id");
        assert_eq!(
            projected_request_error,
            TenantStatusSnapshotError::TenantRequest(expected_request_error),
            "tenant-id validation must retain the legacy request error even when cancelled"
        );

        drop(execution);
        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.cancellations_total, 1);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn managed_tenant_request_plan_enforces_lifecycle_query_concurrency_and_ingest_rate() {
        let control_plane = ManagedControlPlane::open(None).expect("control plane should open");
        provision_ready_deployment(&control_plane, "prod").expect("deployment should provision");
        control_plane
            .apply_tenant(
                managed_actor(),
                ManagedTenantApplyRequest {
                    tenant_id: "team-a".to_string(),
                    deployment_id: Some("prod".to_string()),
                    display_name: Some("Team A".to_string()),
                    lifecycle: Some(TenantLifecycleState::Active),
                    retention_days: None,
                    storage_limit_bytes: None,
                    ingest_rate_limit_per_sec: Some(2),
                    query_concurrency_limit: Some(1),
                    labels: None,
                },
            )
            .expect("tenant should apply");

        let read_request = HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/query?query=up".to_string(),
            headers: HashMap::from([(TENANT_HEADER.to_string(), "team-a".to_string())]),
            body: Vec::new(),
        };
        let read_plan = prepare_request_plan(
            None,
            Some(&control_plane),
            &read_request,
            "team-a",
            TenantAccessScope::Read,
        )
        .expect("read plan should prepare");
        let held_query = read_plan
            .admit_with_usage(TenantAdmissionSurface::Query, 1, None)
            .expect("first query should admit");
        let query_err = read_plan
            .admit_with_usage(TenantAdmissionSurface::Query, 1, None)
            .expect_err("second query should be limited");
        assert!(matches!(
            query_err,
            TenantRequestError::Rejected {
                status: 429,
                code: "tenant_managed_query_concurrency_limit_exceeded",
                ..
            }
        ));
        drop(held_query);

        let usage_accounting = UsageAccounting::open(None).expect("usage store should open");
        let write_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([(TENANT_HEADER.to_string(), "team-a".to_string())]),
            body: Vec::new(),
        };
        let write_plan = prepare_request_plan(
            None,
            Some(&control_plane),
            &write_request,
            "team-a",
            TenantAccessScope::Write,
        )
        .expect("write plan should prepare");
        let _first_write = write_plan
            .admit_with_usage(
                TenantAdmissionSurface::Ingest,
                2,
                Some(usage_accounting.as_ref()),
            )
            .expect("first ingest should admit");
        let ingest_err = write_plan
            .admit_with_usage(
                TenantAdmissionSurface::Ingest,
                1,
                Some(usage_accounting.as_ref()),
            )
            .expect_err("second ingest should exceed the managed per-second rate");
        assert!(matches!(
            ingest_err,
            TenantRequestError::Rejected {
                status: 429,
                code: "tenant_managed_ingest_rate_limit_exceeded",
                ..
            }
        ));

        control_plane
            .apply_tenant_lifecycle(
                managed_actor(),
                ManagedTenantLifecycleRequest {
                    tenant_id: "team-a".to_string(),
                    lifecycle: TenantLifecycleState::Suspended,
                    note: Some("billing".to_string()),
                },
            )
            .expect("tenant lifecycle should update");

        let suspended_err = prepare_request_plan(
            None,
            Some(&control_plane),
            &read_request,
            "team-a",
            TenantAccessScope::Read,
        )
        .expect_err("suspended tenant should be rejected before admission");
        assert!(matches!(
            suspended_err,
            TenantRequestError::Rejected {
                status: 403,
                code: "tenant_managed_lifecycle_blocked",
                ..
            }
        ));
    }

    #[test]
    fn managed_tenant_request_plan_enforces_storage_limit() {
        let control_plane = ManagedControlPlane::open(None).expect("control plane should open");
        provision_ready_deployment(&control_plane, "prod").expect("deployment should provision");
        control_plane
            .apply_tenant(
                managed_actor(),
                ManagedTenantApplyRequest {
                    tenant_id: "team-b".to_string(),
                    deployment_id: Some("prod".to_string()),
                    display_name: Some("Team B".to_string()),
                    lifecycle: Some(TenantLifecycleState::Active),
                    retention_days: None,
                    storage_limit_bytes: Some(128),
                    ingest_rate_limit_per_sec: None,
                    query_concurrency_limit: None,
                    labels: None,
                },
            )
            .expect("tenant should apply");

        let usage_accounting = UsageAccounting::open(None).expect("usage store should open");
        let storage_record = UsageRecordInput {
            tenant_id: "team-b",
            category: UsageCategory::Storage,
            operation: "reconcile_storage",
            source: "test",
            status: "success",
            request_units: 0,
            result_units: 0,
            rows: 0,
            metadata_updates: 0,
            exemplars_accepted: 0,
            exemplars_dropped: 0,
            histogram_series: 0,
            matched_series: 0,
            tombstones_applied: 0,
            duration_nanos: 0,
            request_bytes: 0,
            logical_storage_series: 1,
            logical_storage_samples: 8,
            logical_storage_bytes: 128,
        };
        usage_accounting
            .record(storage_record)
            .expect("storage snapshot should record");

        let write_request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([(TENANT_HEADER.to_string(), "team-b".to_string())]),
            body: Vec::new(),
        };
        let write_plan = prepare_request_plan(
            None,
            Some(&control_plane),
            &write_request,
            "team-b",
            TenantAccessScope::Write,
        )
        .expect("write plan should prepare");
        let storage_err = write_plan
            .admit_with_usage(
                TenantAdmissionSurface::Ingest,
                1,
                Some(usage_accounting.as_ref()),
            )
            .expect_err("write should be rejected once the managed storage cap is reached");
        assert!(matches!(
            storage_err,
            TenantRequestError::Rejected {
                status: 413,
                code: "tenant_managed_storage_limit_exceeded",
                ..
            }
        ));
    }
}
