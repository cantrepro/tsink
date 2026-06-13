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
    DataPoint, DeleteSeriesResult, Label, MetadataShardScope, MetricSeries, QueryOptions,
    Result as TsinkResult, Row, SeriesMatcher, SeriesPoints, SeriesSelection, Storage,
    StorageObservabilitySnapshot, TsinkError,
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
const UNLABELED_TENANT_FALLBACK_REGEX: &str = ".+";

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
    auth_tokens: Vec<TenantTokenPolicy>,
    policy: TenantRequestPolicy,
    max_inflight_reads: Option<usize>,
    max_inflight_writes: Option<usize>,
}

#[derive(Debug, Clone)]
struct TenantTokenPolicy {
    token: String,
    scopes: BTreeSet<TenantAccessScope>,
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
    recent_decisions: Mutex<VecDeque<TenantDecisionSnapshot>>,
}

#[derive(Debug, Default)]
pub struct TenantRegistry {
    default_template: TenantPolicyTemplate,
    tenant_templates: BTreeMap<String, TenantPolicyTemplate>,
    runtimes: Mutex<BTreeMap<String, Arc<TenantPolicyRuntime>>>,
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
}

impl TenantPolicyRuntime {
    fn from_template(template: TenantPolicyTemplate) -> Self {
        let policy = template.policy.clone();
        let ingest_budget = policy.admission.ingest;
        let query_budget = policy.admission.query;
        let metadata_budget = policy.admission.metadata;
        let retention_budget = policy.admission.retention;
        let mut auth_tokens = BTreeMap::<String, BTreeSet<TenantAccessScope>>::new();
        for token in template.auth_tokens {
            auth_tokens
                .entry(token.token)
                .or_default()
                .extend(token.scopes);
        }
        Self {
            policy,
            auth_tokens,
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
        }
    }

    fn authorize(
        &self,
        request: &HttpRequest,
        access: TenantAccessScope,
    ) -> Result<(), TenantRequestError> {
        if request.header(RBAC_AUTH_VERIFIED_HEADER).is_some() {
            return Ok(());
        }
        if !self.auth_tokens.is_empty() {
            let Some(token) = bearer_token(request) else {
                return Err(TenantRequestError::Unauthorized(
                    "tenant_auth_token_missing",
                ));
            };
            let Some(scopes) = self.auth_tokens.get(token) else {
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
        reason: String,
    ) {
        let mut recent = self
            .recent_decisions
            .lock()
            .expect("tenant decision log mutex should not be poisoned");
        if recent.len() >= TENANT_DECISION_HISTORY_LIMIT {
            recent.pop_front();
        }
        recent.push_back(TenantDecisionSnapshot {
            unix_ms: unix_timestamp_millis(),
            access: access.as_str().to_string(),
            surface: surface.as_str().to_string(),
            outcome: outcome.as_str().to_string(),
            requested_units: u64::try_from(requested_units).unwrap_or(u64::MAX),
            reason,
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
                        reason.clone(),
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
                    reason.clone(),
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
                            reason.clone(),
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
            format!(
                "tenant request admitted for {} via {} scope",
                surface.as_str(),
                access.as_str()
            ),
        );
        Ok(TenantRequestGuard {
            policy: self.policy.clone(),
            _permits: permits,
            _managed_guard: None,
        })
    }

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
            .cloned()
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
        let default_template = TenantPolicyTemplate::from_definition(&file.defaults)?;
        let mut tenant_templates = BTreeMap::new();
        for (tenant_id, definition) in file.tenants {
            validate_tenant_id(&tenant_id)?;
            tenant_templates.insert(tenant_id, default_template.merged(&definition)?);
        }
        Ok(Self {
            default_template,
            tenant_templates,
            runtimes: Mutex::new(BTreeMap::new()),
        })
    }

    fn runtime_for(&self, tenant_id: &str) -> Result<Arc<TenantPolicyRuntime>, TenantRequestError> {
        validate_tenant_id(tenant_id).map_err(TenantRequestError::BadRequest)?;
        let mut runtimes = self
            .runtimes
            .lock()
            .expect("tenant runtime cache mutex should not be poisoned");
        if let Some(runtime) = runtimes.get(tenant_id) {
            return Ok(Arc::clone(runtime));
        }
        let template = self
            .tenant_templates
            .get(tenant_id)
            .cloned()
            .unwrap_or_else(|| self.default_template.clone());
        let runtime = Arc::new(TenantPolicyRuntime::from_template(template));
        runtimes.insert(tenant_id.to_string(), Arc::clone(&runtime));
        Ok(runtime)
    }

    pub fn status_snapshot_for(
        &self,
        tenant_id: &str,
    ) -> Result<TenantRuntimeStatusSnapshot, TenantRequestError> {
        let runtime = self.runtime_for(tenant_id)?;
        Ok(runtime.status_snapshot(tenant_id))
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
    let runtime = registry.runtime_for(tenant_id)?;
    runtime.authorize(request, access)?;
    if let Some(managed_policy) = managed_policy.as_ref() {
        managed_policy.authorize()?;
    }
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

fn parse_auth_tokens(auth: &TenantAuthDefinition) -> Result<Vec<TenantTokenPolicy>, String> {
    let mut tokens = Vec::with_capacity(auth.tokens.len());
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
        tokens.push(TenantTokenPolicy {
            token: raw_token.to_string(),
            scopes,
        });
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
        let mut merged = BTreeSet::new();
        for scoped in self.read_selections(selection)? {
            merged.extend(self.inner.select_series(&scoped)?);
        }
        Ok(merged.into_iter().collect())
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

    fn visible_series(&self, series: Vec<MetricSeries>) -> Vec<MetricSeries> {
        series
            .into_iter()
            .filter_map(|series| visible_metric_series(series, &self.tenant_id))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
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
}

impl Storage for TenantScopedStorage {
    fn insert_rows(&self, rows: &[Row]) -> TsinkResult<()> {
        let scoped = scope_rows_for_tenant(rows.to_vec(), &self.tenant_id)
            .map_err(TsinkError::InvalidLabel)?;
        self.inner.insert_rows(&scoped)
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

    fn list_metrics(&self) -> TsinkResult<Vec<MetricSeries>> {
        Ok(self.visible_series(self.read_series(&SeriesSelection::new())?))
    }

    fn list_metrics_with_wal(&self) -> TsinkResult<Vec<MetricSeries>> {
        Ok(self.visible_series(self.inner.list_metrics_with_wal()?))
    }

    fn list_metrics_in_shards(&self, scope: &MetadataShardScope) -> TsinkResult<Vec<MetricSeries>> {
        Ok(self.visible_series(self.read_series_in_shards(&SeriesSelection::new(), scope)?))
    }

    fn select_series(&self, selection: &SeriesSelection) -> TsinkResult<Vec<MetricSeries>> {
        Ok(self.visible_series(self.read_series(selection)?))
    }

    fn select_series_in_shards(
        &self,
        selection: &SeriesSelection,
        scope: &MetadataShardScope,
    ) -> TsinkResult<Vec<MetricSeries>> {
        Ok(self.visible_series(self.read_series_in_shards(selection, scope)?))
    }

    fn delete_series(&self, selection: &SeriesSelection) -> TsinkResult<DeleteSeriesResult> {
        if self.is_default_tenant() {
            let time_range = match (selection.start, selection.end) {
                (Some(start), Some(end)) => Some((start, end)),
                _ => None,
            };
            let series = self.read_series(selection)?;
            let mut matched_series = 0u64;
            let mut tombstones_applied = 0u64;
            for series in series {
                let outcome = self
                    .inner
                    .delete_series(&exact_selection_for_series(&series, time_range))?;
                matched_series = matched_series.saturating_add(outcome.matched_series);
                tombstones_applied = tombstones_applied.saturating_add(outcome.tombstones_applied);
            }
            return Ok(DeleteSeriesResult {
                matched_series,
                tombstones_applied,
            });
        }
        let scoped = self.scoped_selection(selection)?;
        self.inner.delete_series(&scoped)
    }

    fn memory_used(&self) -> usize {
        self.inner.memory_used()
    }

    fn memory_budget(&self) -> usize {
        self.inner.memory_budget()
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
    use tsink::{StorageBuilder, TimestampPrecision};

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build")
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
