use super::*;

macro_rules! metrics_write {
    ($body:expr, $($argument:tt)*) => {{
        let _ = std::fmt::Write::write_fmt($body, format_args!($($argument)*));
    }};
}

mod cluster;

use self::cluster::ClusterMetrics;

pub(crate) const METRICS_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const METRICS_FALLBACK_RESPONSE_BYTES: usize = 4 * 1024;
const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4";

pub(super) trait MetricsBodyWriter: std::fmt::Write {
    fn push_str(&mut self, value: &str);
}

impl MetricsBodyWriter for String {
    fn push_str(&mut self, value: &str) {
        String::push_str(self, value);
    }
}

struct BoundedMetricsBody {
    output: String,
    limit: usize,
    overflowed: bool,
}

impl BoundedMetricsBody {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_limit(METRICS_MAX_RESPONSE_BYTES)
    }

    fn with_limit(limit: usize) -> Self {
        Self {
            output: String::with_capacity(limit),
            limit,
            overflowed: false,
        }
    }

    fn try_with_exact_capacity(limit: usize) -> Result<Self, std::collections::TryReserveError> {
        let mut output = String::new();
        output.try_reserve_exact(limit)?;
        Ok(Self {
            output,
            limit,
            overflowed: false,
        })
    }

    fn len(&self) -> usize {
        self.output.len()
    }

    fn capacity(&self) -> usize {
        self.output.capacity()
    }

    fn into_bytes(self) -> Result<Vec<u8>, ()> {
        if self.overflowed {
            Err(())
        } else {
            Ok(self.output.into_bytes())
        }
    }
}

impl MetricsBodyWriter for BoundedMetricsBody {
    fn push_str(&mut self, value: &str) {
        if self.overflowed {
            return;
        }
        let Some(next_len) = self.output.len().checked_add(value.len()) else {
            self.overflowed = true;
            return;
        };
        if next_len > self.limit {
            self.overflowed = true;
            return;
        }
        self.output.push_str(value);
    }
}

impl std::fmt::Write for BoundedMetricsBody {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.push_str(value);
        if self.overflowed {
            Err(std::fmt::Error)
        } else {
            Ok(())
        }
    }
}

struct CountingMetricsBody<'a> {
    len: usize,
    execution: &'a tsink::QueryExecution,
    control_error: Option<tsink::QueryBudgetError>,
    overflowed: bool,
}

impl<'a> CountingMetricsBody<'a> {
    fn new(execution: &'a tsink::QueryExecution) -> Self {
        Self {
            len: 0,
            execution,
            control_error: None,
            overflowed: false,
        }
    }
}

impl MetricsBodyWriter for CountingMetricsBody<'_> {
    fn push_str(&mut self, value: &str) {
        if self.control_error.is_some() || self.overflowed {
            return;
        }
        if let Err(error) = self.execution.checkpoint() {
            self.control_error = Some(error);
            return;
        }
        let Some(next_len) = self.len.checked_add(value.len()) else {
            self.overflowed = true;
            return;
        };
        if next_len > METRICS_MAX_RESPONSE_BYTES {
            self.overflowed = true;
            return;
        }
        self.len = next_len;
    }
}

impl std::fmt::Write for CountingMetricsBody<'_> {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.push_str(value);
        if self.control_error.is_some() || self.overflowed {
            Err(std::fmt::Error)
        } else {
            Ok(())
        }
    }
}

struct ControlledMetricsBody<'a> {
    body: BoundedMetricsBody,
    execution: &'a tsink::QueryExecution,
    control_error: Option<tsink::QueryBudgetError>,
}

impl<'a> ControlledMetricsBody<'a> {
    fn new(body: BoundedMetricsBody, execution: &'a tsink::QueryExecution) -> Self {
        Self {
            body,
            execution,
            control_error: None,
        }
    }
}

impl MetricsBodyWriter for ControlledMetricsBody<'_> {
    fn push_str(&mut self, value: &str) {
        if self.control_error.is_some() || self.body.overflowed {
            return;
        }
        if let Err(error) = self.execution.checkpoint() {
            self.control_error = Some(error);
            return;
        }
        self.body.push_str(value);
    }
}

impl std::fmt::Write for ControlledMetricsBody<'_> {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.push_str(value);
        if self.control_error.is_some() || self.body.overflowed {
            Err(std::fmt::Error)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy)]
struct PrometheusEscapedLabel<'a>(&'a str);

impl std::fmt::Display for PrometheusEscapedLabel<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut remainder = self.0;
        while let Some(index) = remainder.find(['\\', '\n', '"']) {
            formatter.write_str(&remainder[..index])?;
            let escaped = match remainder.as_bytes()[index] {
                b'\\' => "\\\\",
                b'\n' => "\\n",
                b'"' => "\\\"",
                _ => unreachable!("find only returns configured label escape characters"),
            };
            formatter.write_str(escaped)?;
            remainder = &remainder[index + 1..];
        }
        formatter.write_str(remainder)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum MetricsCollector {
    StorageListMetrics,
    ClusterHotspot,
    MetricMetadataStore,
    ExemplarStore,
    RulesRuntime,
    OperationalSnapshots,
    ExpositionBody,
}

impl MetricsCollector {
    const ALL: [Self; 7] = [
        Self::StorageListMetrics,
        Self::ClusterHotspot,
        Self::MetricMetadataStore,
        Self::ExemplarStore,
        Self::RulesRuntime,
        Self::OperationalSnapshots,
        Self::ExpositionBody,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::StorageListMetrics => "storage_list_metrics",
            Self::ClusterHotspot => "cluster_hotspot",
            Self::MetricMetadataStore => "metric_metadata_store",
            Self::ExemplarStore => "exemplar_store",
            Self::RulesRuntime => "rules_runtime",
            Self::OperationalSnapshots => "operational_snapshots",
            Self::ExpositionBody => "exposition_body",
        }
    }

    const fn bit(self) -> u16 {
        1_u16 << (self as u8)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct MetricsCollectionErrors {
    bits: u16,
}

impl MetricsCollectionErrors {
    fn insert(&mut self, collector: MetricsCollector) {
        self.bits |= collector.bit();
    }

    fn len(self) -> usize {
        self.bits.count_ones() as usize
    }

    fn iter(self) -> impl Iterator<Item = MetricsCollector> {
        MetricsCollector::ALL
            .into_iter()
            .filter(move |collector| self.bits & collector.bit() != 0)
    }
}

#[derive(Debug, Clone, Copy)]
struct WriteRejectionMetricsSnapshot {
    reason_totals: [u64; WRITE_REJECTION_REASON_COUNT],
    indeterminate_requests_total: u64,
}

fn write_rejection_metrics_snapshot() -> WriteRejectionMetricsSnapshot {
    let mut reason_totals = [0; WRITE_REJECTION_REASON_COUNT];
    for (index, total) in WRITE_REJECTION_REASON_TOTALS.iter().enumerate() {
        reason_totals[index] = total.load(Ordering::Relaxed);
    }
    WriteRejectionMetricsSnapshot {
        reason_totals,
        indeterminate_requests_total: WRITE_INDETERMINATE_REQUESTS_TOTAL.load(Ordering::Relaxed),
    }
}

#[derive(Debug)]
enum MetricsHotspotCollectionError {
    IncompleteMetricAccounting,
    QueryAdmissionUnavailable,
    QueryAdmission(tsink::TsinkError),
    MetricEnumeration(tsink::TsinkError),
    InvalidMetricAccounting(MetricEnumerationAccountingError),
    Hotspot(tsink::QueryBudgetError),
}

impl MetricsHotspotCollectionError {
    fn collector(&self) -> MetricsCollector {
        match self {
            Self::Hotspot(_) => MetricsCollector::ClusterHotspot,
            Self::IncompleteMetricAccounting
            | Self::QueryAdmissionUnavailable
            | Self::QueryAdmission(_)
            | Self::MetricEnumeration(_)
            | Self::InvalidMetricAccounting(_) => MetricsCollector::StorageListMetrics,
        }
    }
}

impl std::fmt::Display for MetricsHotspotCollectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IncompleteMetricAccounting => {
                formatter.write_str("storage does not provide complete metric-list accounting")
            }
            Self::QueryAdmissionUnavailable => {
                formatter.write_str("storage does not expose query execution admission")
            }
            Self::QueryAdmission(error) => write!(formatter, "query admission failed: {error}"),
            Self::MetricEnumeration(error) => {
                write!(formatter, "metric enumeration failed: {error}")
            }
            Self::InvalidMetricAccounting(error) => {
                write!(
                    formatter,
                    "metric enumeration accounting is invalid: {error}"
                )
            }
            Self::Hotspot(error) => write!(formatter, "hotspot accounting failed: {error}"),
        }
    }
}

#[derive(Debug)]
struct AccountedMetricsHotspot {
    hotspot: hotspot::AccountedClusterHotspotSnapshot,
}

impl AccountedMetricsHotspot {
    fn snapshot(&self) -> &ClusterHotspotSnapshot {
        &self.hotspot
    }
}

struct AccountedEmptyControlMetrics {
    snapshot: ControlMetricsSnapshot,
    _reservation: tsink::QueryMemoryReservation,
}

fn empty_control_metrics_with_execution(
    local_node_id: &str,
    execution: &tsink::QueryExecution,
) -> Result<AccountedEmptyControlMetrics, tsink::QueryBudgetError> {
    execution.checkpoint()?;
    let modeled_bytes = if local_node_id.is_empty() {
        0
    } else {
        u64::try_from(local_node_id.len())
            .unwrap_or(u64::MAX)
            .saturating_add(64)
    };
    let mut reservation = execution.reserve_memory(modeled_bytes)?;
    let snapshot = ControlMetricsSnapshot::empty(local_node_id.to_string());
    let retained_bytes = if snapshot.liveness.local_node_id.capacity() == 0 {
        0
    } else {
        u64::try_from(snapshot.liveness.local_node_id.capacity())
            .unwrap_or(u64::MAX)
            .saturating_add(64)
    };
    reservation.resize(retained_bytes)?;
    Ok(AccountedEmptyControlMetrics {
        snapshot,
        _reservation: reservation,
    })
}

fn empty_metrics_hotspot_snapshot() -> ClusterHotspotSnapshot {
    ClusterHotspotSnapshot {
        generated_unix_ms: 0,
        hot_shards: Vec::new(),
        tenant_hotspots: Vec::new(),
        skewed_shards: 0,
        skewed_tenants: 0,
        max_shard_score: 0.0,
        max_tenant_score: 0.0,
    }
}

fn collect_accounted_metrics_hotspot(
    storage: &Arc<dyn Storage>,
    cluster_context: Option<&ClusterRequestContext>,
    control: Option<&crate::cluster::control::ControlHotspotSnapshot>,
    execution: &tsink::QueryExecution,
) -> Result<AccountedMetricsHotspot, MetricsHotspotCollectionError> {
    if storage.list_metrics_execution_accounting() != tsink::QueryExecutionAccounting::Complete {
        return Err(MetricsHotspotCollectionError::IncompleteMetricAccounting);
    }
    let metrics = storage
        .list_metrics_with_execution_result(execution)
        .map_err(MetricsHotspotCollectionError::MetricEnumeration)?;
    let guarded_metrics = validate_complete_metric_enumeration_result(metrics)
        .map_err(MetricsHotspotCollectionError::InvalidMetricAccounting)?;
    let hotspot = hotspot::build_cluster_hotspot_snapshot_with_control_metrics_execution(
        &guarded_metrics.series,
        cluster_context.map(|context| &context.runtime.ring),
        control,
        None,
        execution,
    )
    .map_err(MetricsHotspotCollectionError::Hotspot)?;
    drop(guarded_metrics);
    Ok(AccountedMetricsHotspot { hotspot })
}

fn begin_metrics_execution(
    storage: &Arc<dyn Storage>,
) -> Result<tsink::QueryExecution, MetricsHotspotCollectionError> {
    storage
        .begin_query_execution(
            tsink::QueryWorkLimits::default(),
            tsink::QueryCancellationToken::new(),
        )
        .map_err(MetricsHotspotCollectionError::QueryAdmission)?
        .ok_or(MetricsHotspotCollectionError::QueryAdmissionUnavailable)
}

fn modeled_metrics_header_preflight_bytes() -> u64 {
    modeled_tsdb_status_vec_capacity_bytes::<(String, String)>(4).saturating_add(
        tsdb_status_saturating_u64_from_usize(
            "Content-Type"
                .len()
                .saturating_add(METRICS_CONTENT_TYPE.len()),
        )
        .saturating_add(TSDB_STATUS_COLLECTION_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(2)),
    )
}

fn modeled_metrics_response_preflight_bytes(body_capacity: usize) -> u64 {
    modeled_tsdb_status_vec_capacity_bytes::<u8>(body_capacity)
        .saturating_add(modeled_metrics_header_preflight_bytes())
}

fn metrics_fallback_response(errors: MetricsCollectionErrors) -> HttpResponse {
    let mut body = BoundedMetricsBody::with_limit(METRICS_FALLBACK_RESPONSE_BYTES);
    body.push_str(
        "# HELP tsink_cluster_hotspot_skewed_shards Number of shards currently above hotspot skew threshold\n\
         # TYPE tsink_cluster_hotspot_skewed_shards gauge\n\
         tsink_cluster_hotspot_skewed_shards 0\n\
         # HELP tsink_cluster_hotspot_skewed_tenants Number of tenants currently above hotspot skew threshold\n\
         # TYPE tsink_cluster_hotspot_skewed_tenants gauge\n\
         tsink_cluster_hotspot_skewed_tenants 0\n\
         # HELP tsink_cluster_hotspot_max_shard_score Highest hotspot pressure score among tracked shards\n\
         # TYPE tsink_cluster_hotspot_max_shard_score gauge\n\
         tsink_cluster_hotspot_max_shard_score 0\n\
         # HELP tsink_cluster_hotspot_max_tenant_score Highest hotspot pressure score among tracked tenants\n\
         # TYPE tsink_cluster_hotspot_max_tenant_score gauge\n\
         tsink_cluster_hotspot_max_tenant_score 0\n",
    );
    append_metrics_collection_errors(&mut body, errors);
    if body.overflowed {
        body.output.clear();
        body.overflowed = false;
        body.push_str(
            "# TYPE tsink_cluster_hotspot_skewed_shards gauge\n\
             tsink_cluster_hotspot_skewed_shards 0\n\
             # TYPE tsink_cluster_hotspot_skewed_tenants gauge\n\
             tsink_cluster_hotspot_skewed_tenants 0\n\
             # TYPE tsink_cluster_hotspot_max_shard_score gauge\n\
             tsink_cluster_hotspot_max_shard_score 0\n\
             # TYPE tsink_cluster_hotspot_max_tenant_score gauge\n\
             tsink_cluster_hotspot_max_tenant_score 0\n\
             # TYPE tsink_metrics_collection_errors gauge\n\
             tsink_metrics_collection_errors 1\n\
             # TYPE tsink_metrics_collection_error gauge\n\
             tsink_metrics_collection_error{collector=\"exposition_body\"} 1\n",
        );
    }
    HttpResponse::new(
        200,
        body.into_bytes()
            .unwrap_or_else(|_| b"tsink_metrics_collection_errors 1\n".to_vec()),
    )
    .with_header("Content-Type", METRICS_CONTENT_TYPE)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn render_metrics(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    server_start: Instant,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
    rbac_registry: Option<&RbacRegistry>,
    security_manager: Option<&SecurityManager>,
    usage_accounting: Option<&UsageAccounting>,
    tenant_registry: Option<&tenant::TenantRegistry>,
    local_disk_budget: Option<&tsink::LocalDiskBudget>,
    offline_restore_disk_budget: Option<&tsink::LocalDiskBudget>,
) -> HttpResponse {
    if storage.list_metrics_execution_accounting() != tsink::QueryExecutionAccounting::Complete {
        let mut errors = MetricsCollectionErrors::default();
        errors.insert(MetricsCollector::StorageListMetrics);
        eprintln!(
            "metrics collection error in {}: storage does not provide complete metric-list accounting",
            MetricsCollector::StorageListMetrics.name()
        );
        return metrics_fallback_response(errors);
    }
    let execution = match begin_metrics_execution(storage) {
        Ok(execution) => execution,
        Err(err) => {
            let collector = err.collector();
            eprintln!("metrics collection error in {}: {err}", collector.name());
            let mut errors = MetricsCollectionErrors::default();
            errors.insert(collector);
            return metrics_fallback_response(errors);
        }
    };
    let mut collection_errors = MetricsCollectionErrors::default();
    let memory_used = storage.memory_used();
    let memory_budget = storage.memory_budget();
    let accounted_cluster_control = cluster_context
        .and_then(|context| context.control_consensus.as_ref())
        .and_then(
            |consensus| match consensus.metrics_snapshot_with_execution(&execution) {
                Ok(snapshot) => Some(snapshot),
                Err(err) => {
                    eprintln!("metrics collection error in operational_snapshots: {err}");
                    collection_errors.insert(MetricsCollector::OperationalSnapshots);
                    None
                }
            },
        );
    let empty_cluster_control = if accounted_cluster_control.is_none() {
        let local_node_id = cluster_context
            .map(|context| context.runtime.membership.local_node_id.as_str())
            .unwrap_or("standalone");
        match empty_control_metrics_with_execution(local_node_id, &execution) {
            Ok(snapshot) => Some(snapshot),
            Err(err) => {
                eprintln!("metrics collection error in operational_snapshots: {err}");
                collection_errors.insert(MetricsCollector::OperationalSnapshots);
                return metrics_fallback_response(collection_errors);
            }
        }
    } else {
        None
    };
    let cluster_control = accounted_cluster_control
        .as_deref()
        .or_else(|| {
            empty_cluster_control
                .as_ref()
                .map(|snapshot| &snapshot.snapshot)
        })
        .expect("one control metrics projection is always present");
    let accounted_cluster_hotspot = match collect_accounted_metrics_hotspot(
        storage,
        cluster_context,
        Some(&cluster_control.hotspot),
        &execution,
    ) {
        Ok(snapshot) => Some(snapshot),
        Err(err) => {
            let collector = err.collector();
            eprintln!("metrics collection error in {}: {err}", collector.name());
            collection_errors.insert(collector);
            None
        }
    };
    let empty_cluster_hotspot = empty_metrics_hotspot_snapshot();
    let cluster_hotspot = accounted_cluster_hotspot
        .as_ref()
        .map(AccountedMetricsHotspot::snapshot)
        .unwrap_or(&empty_cluster_hotspot);
    let uptime = server_start.elapsed().as_secs();
    let obs = match storage.metrics_observability_snapshot_with_execution(&execution) {
        Ok(snapshot) => snapshot,
        Err(err) => {
            eprintln!("metrics collection error in operational_snapshots: {err}");
            collection_errors.insert(MetricsCollector::OperationalSnapshots);
            return metrics_fallback_response(collection_errors);
        }
    };
    let series_count = obs.cardinality.series_count;
    let local_disk = match local_disk_budget {
        Some(budget) => match budget.metrics_snapshot_with_execution(&execution) {
            Ok(snapshot) => Some(snapshot),
            Err(err) => {
                eprintln!("metrics collection error in operational_snapshots: {err}");
                collection_errors.insert(MetricsCollector::OperationalSnapshots);
                None
            }
        },
        None => obs.local_disk,
    };
    let offline_restore_disk = match offline_restore_disk_budget {
        Some(budget) => match budget.metrics_snapshot_with_execution(&execution) {
            Ok(snapshot) => Some(snapshot),
            Err(err) => {
                eprintln!("metrics collection error in operational_snapshots: {err}");
                collection_errors.insert(MetricsCollector::OperationalSnapshots);
                None
            }
        },
        None => None,
    };
    let memory_obs = &obs.memory;
    let memory_pressure_normal = u8::from(matches!(
        memory_obs.pressure.level,
        Some(tsink::MemoryPressureLevel::Normal)
    ));
    let memory_pressure_approaching = u8::from(matches!(
        memory_obs.pressure.level,
        Some(tsink::MemoryPressureLevel::ApproachingLimit)
    ));
    let memory_pressure_backpressured = u8::from(matches!(
        memory_obs.pressure.level,
        Some(tsink::MemoryPressureLevel::Backpressured)
    ));
    let memory_pressure_rejecting = u8::from(matches!(
        memory_obs.pressure.level,
        Some(tsink::MemoryPressureLevel::Rejecting)
    ));
    let memory_pressure_degraded = u8::from(matches!(
        memory_obs.pressure.level,
        Some(tsink::MemoryPressureLevel::Degraded)
    ));
    let wal_enabled = u8::from(obs.wal.enabled);
    let cluster_write_metrics = write_routing_metrics_snapshot();
    let accounted_cluster_write_labeled_metrics =
        match write_routing_labeled_metrics_snapshot_with_execution(&execution) {
            Ok(snapshot) => Some(snapshot),
            Err(err) => {
                eprintln!("metrics collection error in operational_snapshots: {err}");
                collection_errors.insert(MetricsCollector::OperationalSnapshots);
                None
            }
        };
    let empty_cluster_write_labeled_metrics =
        WriteRoutingLabeledMetricsExpositionSnapshot::default();
    let cluster_write_labeled_metrics = accounted_cluster_write_labeled_metrics
        .as_ref()
        .map(|snapshot| snapshot.snapshot())
        .unwrap_or(&empty_cluster_write_labeled_metrics);
    let cluster_fanout_metrics = read_fanout_metrics_snapshot();
    let accounted_cluster_fanout_labeled_metrics =
        match read_fanout_labeled_metrics_snapshot_with_execution(&execution) {
            Ok(snapshot) => Some(snapshot),
            Err(err) => {
                eprintln!("metrics collection error in operational_snapshots: {err}");
                collection_errors.insert(MetricsCollector::OperationalSnapshots);
                None
            }
        };
    let empty_cluster_fanout_labeled_metrics =
        ReadFanoutLabeledMetricsExpositionSnapshot::default();
    let cluster_fanout_labeled_metrics = accounted_cluster_fanout_labeled_metrics
        .as_ref()
        .map(|snapshot| snapshot.snapshot())
        .unwrap_or(&empty_cluster_fanout_labeled_metrics);
    let cluster_read_planner_metrics = read_planner_metrics_snapshot();
    let cluster_read_planner_labeled_metrics = read_planner_labeled_metrics_exposition_snapshot();
    let cluster_dedupe_metrics = dedupe_metrics_snapshot();
    let read_admission_metrics = admission::read_admission_metrics_snapshot();
    let write_admission_metrics = admission::write_admission_metrics_snapshot();
    let write_rejection_metrics = write_rejection_metrics_snapshot();
    let tenant_admission_metrics = tenant::tenant_admission_metrics_snapshot();
    let tenant_runtime_cache_metrics =
        tenant_registry.map(tenant::TenantRegistry::runtime_cache_metrics_snapshot);
    let metadata_store_metrics = match metadata_store.metrics_snapshot() {
        Ok(snapshot) => snapshot,
        Err(err) => {
            eprintln!("metrics collection error in metric_metadata_store: {err}");
            collection_errors.insert(MetricsCollector::MetricMetadataStore);
            MetricMetadataStoreMetricsSnapshot {
                limits: metadata_store.config(),
                entries: 0,
                retained_bytes: 0,
                peak_retained_bytes: 0,
                durable_file_bytes: 0,
                transient_bytes: 0,
                peak_transient_bytes: 0,
                query_result_bytes: 0,
                peak_query_result_bytes: 0,
                rejections_total: 0,
                entry_rejections_total: 0,
                record_rejections_total: 0,
                update_batch_rejections_total: 0,
                retained_rejections_total: 0,
                durable_file_rejections_total: 0,
                transient_rejections_total: 0,
                query_rejections_total: 0,
                persistence_rejections_total: 0,
            }
        }
    };
    let exemplar_metrics = match exemplar_store.metrics_snapshot() {
        Ok(snapshot) => snapshot,
        Err(err) => {
            eprintln!("metrics collection error in exemplar_store: {err}");
            collection_errors.insert(MetricsCollector::ExemplarStore);
            ExemplarStoreMetricsSnapshot {
                accepted_total: 0,
                rejected_total: 0,
                dropped_total: 0,
                query_requests_total: 0,
                query_series_total: 0,
                query_exemplars_total: 0,
                stored_series: 0,
                stored_exemplars: 0,
                ..ExemplarStoreMetricsSnapshot::default()
            }
        }
    };
    let payload_status = payload_status_snapshot(cluster_context);
    let otlp_status = otlp_metrics_status_snapshot();
    let legacy_ingest_status = legacy_ingest::status_snapshot();
    let edge_sync_metrics = edge_sync_context
        .map(edge_sync::EdgeSyncRuntimeContext::metrics_snapshot)
        .unwrap_or_default();
    let cluster_audit_health = cluster_context
        .and_then(|context| context.audit_log.as_ref())
        .map(|audit_log| audit_log.metrics_snapshot())
        .unwrap_or_default();
    let accounted_cluster_outbox = cluster_context
        .and_then(|context| context.outbox.as_ref())
        .and_then(
            |outbox| match outbox.metrics_snapshot_with_execution(&execution) {
                Ok(snapshot) => Some(snapshot),
                Err(err) => {
                    eprintln!("metrics collection error in operational_snapshots: {err}");
                    collection_errors.insert(MetricsCollector::OperationalSnapshots);
                    None
                }
            },
        );
    let empty_cluster_outbox = OutboxMetricsExpositionSnapshot { peers: Vec::new() };
    let cluster_outbox_peers = accounted_cluster_outbox
        .as_ref()
        .map(|snapshot| snapshot.snapshot())
        .unwrap_or(&empty_cluster_outbox);
    let cluster_outbox_metrics = outbox_metrics_snapshot();
    let accounted_cluster_digest = cluster_context
        .and_then(|context| context.digest_runtime.as_ref())
        .and_then(
            |runtime| match runtime.metrics_snapshot_with_execution(&execution) {
                Ok(snapshot) => Some(snapshot),
                Err(err) => {
                    eprintln!("metrics collection error in operational_snapshots: {err}");
                    collection_errors.insert(MetricsCollector::OperationalSnapshots);
                    None
                }
            },
        );
    let empty_cluster_digest = DigestExchangeMetricsSnapshot::default();
    let cluster_digest = accounted_cluster_digest
        .as_ref()
        .map(|snapshot| snapshot.snapshot())
        .unwrap_or(&empty_cluster_digest);
    let accounted_cluster_rebalance = cluster_context
        .and_then(|context| context.digest_runtime.as_ref())
        .and_then(
            |runtime| match runtime.rebalance_metrics_snapshot_with_execution(&execution) {
                Ok(snapshot) => Some(snapshot),
                Err(err) => {
                    eprintln!("metrics collection error in operational_snapshots: {err}");
                    collection_errors.insert(MetricsCollector::OperationalSnapshots);
                    None
                }
            },
        );
    let empty_cluster_rebalance = RebalanceSchedulerMetricsSnapshot::default();
    let cluster_rebalance = accounted_cluster_rebalance
        .as_ref()
        .map(|snapshot| snapshot.snapshot())
        .unwrap_or(&empty_cluster_rebalance);
    let rules_snapshot = match rules_runtime {
        Some(runtime) => match runtime.metrics_snapshot_with_execution(&execution) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                eprintln!("metrics collection error in rules_runtime: {err}");
                collection_errors.insert(MetricsCollector::RulesRuntime);
                rules::RulesExpositionSnapshot::default()
            }
        },
        None => rules::RulesExpositionSnapshot::default(),
    };
    let usage_status = usage_accounting
        .map(UsageAccounting::metrics_snapshot)
        .unwrap_or_default();
    let security_metrics = match security_manager {
        Some(manager) => match manager.metrics_snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => {
                eprintln!("metrics collection error in operational_snapshots: {err}");
                collection_errors.insert(MetricsCollector::OperationalSnapshots);
                crate::security::SecurityMetricsSnapshot::default()
            }
        },
        None => crate::security::SecurityMetricsSnapshot::default(),
    };
    let rbac_metrics = match rbac_registry {
        Some(registry) => match registry.service_account_metrics_snapshot() {
            Ok(snapshot) => Some(snapshot),
            Err(err) => {
                eprintln!("metrics collection error in operational_snapshots: {err}");
                collection_errors.insert(MetricsCollector::OperationalSnapshots);
                None
            }
        },
        None => None,
    };
    let rendered_collection_errors = collection_errors;

    let append_body = |body: &mut dyn MetricsBodyWriter| {
        metrics_write!(body,
        "# HELP tsink_memory_used_bytes Estimated bytes counted against the configured memory budget\n\
         # TYPE tsink_memory_used_bytes gauge\n\
         tsink_memory_used_bytes {memory_used}\n\
         # HELP tsink_memory_budget_bytes Configured memory budget\n\
         # TYPE tsink_memory_budget_bytes gauge\n\
         tsink_memory_budget_bytes {memory_budget}\n\
         # HELP tsink_memory_excluded_bytes Measured excluded bytes; incomplete unless tsink_memory_excluded_bytes_known is 1\n\
         # TYPE tsink_memory_excluded_bytes gauge\n\
         tsink_memory_excluded_bytes {memory_excluded}\n\
         # HELP tsink_memory_registry_bytes Estimated budgeted bytes used by the in-memory series registry\n\
         # TYPE tsink_memory_registry_bytes gauge\n\
         tsink_memory_registry_bytes {memory_registry}\n\
         # HELP tsink_memory_metadata_cache_bytes Estimated budgeted bytes used by metadata caches and indexes\n\
         # TYPE tsink_memory_metadata_cache_bytes gauge\n\
         tsink_memory_metadata_cache_bytes {memory_metadata}\n\
         # HELP tsink_memory_persisted_index_bytes Estimated budgeted bytes used by persisted chunk refs and timestamp indexes\n\
         # TYPE tsink_memory_persisted_index_bytes gauge\n\
         tsink_memory_persisted_index_bytes {memory_persisted_index}\n\
         # HELP tsink_memory_persisted_mmap_bytes Budgeted virtual length of persisted mmap-backed segment payloads, not resident bytes\n\
         # TYPE tsink_memory_persisted_mmap_bytes gauge\n\
         tsink_memory_persisted_mmap_bytes {memory_persisted_mmap}\n\
         # HELP tsink_memory_tombstone_bytes Estimated budgeted bytes used by tombstone state\n\
         # TYPE tsink_memory_tombstone_bytes gauge\n\
         tsink_memory_tombstone_bytes {memory_tombstones}\n\
         # HELP tsink_memory_remote_catalog_staging_bytes Modeled budgeted bytes retained by finite catalog reader and read-write publication staging\n\
         # TYPE tsink_memory_remote_catalog_staging_bytes gauge\n\
         tsink_memory_remote_catalog_staging_bytes {memory_remote_catalog_staging}\n\
         # HELP tsink_memory_wal_writer_buffer_bytes Budgeted bytes retained by the live WAL writer buffer\n\
         # TYPE tsink_memory_wal_writer_buffer_bytes gauge\n\
         tsink_memory_wal_writer_buffer_bytes {memory_wal_writer_buffer}\n\
         # HELP tsink_memory_wal_series_definition_cache_bytes Modeled budgeted bytes retained by the WAL series-definition cache\n\
         # TYPE tsink_memory_wal_series_definition_cache_bytes gauge\n\
         tsink_memory_wal_series_definition_cache_bytes {memory_wal_series_definition_cache}\n\
         # HELP tsink_memory_write_transient_bytes Current modeled budgeted bytes reserved for foreground write and startup replay scratch\n\
         # TYPE tsink_memory_write_transient_bytes gauge\n\
         tsink_memory_write_transient_bytes {memory_write_transient}\n\
         # HELP tsink_memory_write_transient_peak_bytes Peak concurrent modeled write and replay scratch reservation\n\
         # TYPE tsink_memory_write_transient_peak_bytes gauge\n\
         tsink_memory_write_transient_peak_bytes {memory_write_transient_peak}\n\
         # HELP tsink_memory_write_transient_reservations_total Write and replay scratch leases admitted by the shared memory budget\n\
         # TYPE tsink_memory_write_transient_reservations_total counter\n\
         tsink_memory_write_transient_reservations_total {memory_write_transient_reservations_total}\n\
         # HELP tsink_memory_write_transient_rejections_total Write and replay scratch leases rejected by the shared memory budget\n\
         # TYPE tsink_memory_write_transient_rejections_total counter\n\
         tsink_memory_write_transient_rejections_total {memory_write_transient_rejections_total}\n\
         # HELP tsink_memory_excluded_bytes_known Whether excluded memory bytes are completely measured\n\
         # TYPE tsink_memory_excluded_bytes_known gauge\n\
         tsink_memory_excluded_bytes_known {memory_excluded_known}\n\
         # HELP tsink_memory_pressure_level Current modeled storage-memory pressure level\n\
         # TYPE tsink_memory_pressure_level gauge\n\
         tsink_memory_pressure_level{{level=\"normal\"}} {memory_pressure_normal}\n\
         tsink_memory_pressure_level{{level=\"approaching_limit\"}} {memory_pressure_approaching}\n\
         tsink_memory_pressure_level{{level=\"backpressured\"}} {memory_pressure_backpressured}\n\
         tsink_memory_pressure_level{{level=\"rejecting\"}} {memory_pressure_rejecting}\n\
         tsink_memory_pressure_level{{level=\"degraded\"}} {memory_pressure_degraded}\n\
         # HELP tsink_memory_backpressured_writers Writers currently waiting on modeled storage memory\n\
         # TYPE tsink_memory_backpressured_writers gauge\n\
         tsink_memory_backpressured_writers {memory_active_backpressured_writers}\n\
         # HELP tsink_memory_backpressure_events_total Writes that entered modeled storage-memory backpressure\n\
         # TYPE tsink_memory_backpressure_events_total counter\n\
         tsink_memory_backpressure_events_total {memory_backpressure_events_total}\n\
         # HELP tsink_memory_rejections_total Modeled storage-memory admissions rejected with MemoryBudgetExceeded\n\
         # TYPE tsink_memory_rejections_total counter\n\
         tsink_memory_rejections_total {memory_rejections_total}\n\
         # HELP tsink_series_total Number of known metric series\n\
         # TYPE tsink_series_total gauge\n\
         tsink_series_total {series_count}\n\
         # HELP tsink_uptime_seconds Server uptime in seconds\n\
         # TYPE tsink_uptime_seconds gauge\n\
         tsink_uptime_seconds {uptime}\n\
         # HELP tsink_wal_enabled WAL enabled state (1 enabled, 0 disabled)\n\
         # TYPE tsink_wal_enabled gauge\n\
         tsink_wal_enabled {wal_enabled}\n\
         # HELP tsink_wal_size_bytes WAL size on disk in bytes\n\
         # TYPE tsink_wal_size_bytes gauge\n\
         tsink_wal_size_bytes {wal_size_bytes}\n\
         # HELP tsink_wal_segments WAL segment files present\n\
         # TYPE tsink_wal_segments gauge\n\
         tsink_wal_segments {wal_segments}\n\
         # HELP tsink_wal_active_segment Current WAL segment id\n\
         # TYPE tsink_wal_active_segment gauge\n\
         tsink_wal_active_segment {wal_active_segment}\n\
         # HELP tsink_wal_acknowledged_writes_durable Whether successful writes are fsync-durable at acknowledgement time (1 durable, 0 append-only)\n\
         # TYPE tsink_wal_acknowledged_writes_durable gauge\n\
         tsink_wal_acknowledged_writes_durable {wal_acknowledged_writes_durable}\n\
         # HELP tsink_wal_highwater_segment Last appended WAL highwater segment\n\
         # TYPE tsink_wal_highwater_segment gauge\n\
         tsink_wal_highwater_segment {wal_highwater_segment}\n\
         # HELP tsink_wal_highwater_frame Last appended WAL highwater frame\n\
         # TYPE tsink_wal_highwater_frame gauge\n\
         tsink_wal_highwater_frame {wal_highwater_frame}\n\
         # HELP tsink_wal_durable_highwater_segment Last durable WAL highwater segment\n\
         # TYPE tsink_wal_durable_highwater_segment gauge\n\
         tsink_wal_durable_highwater_segment {wal_durable_highwater_segment}\n\
         # HELP tsink_wal_durable_highwater_frame Last durable WAL highwater frame\n\
         # TYPE tsink_wal_durable_highwater_frame gauge\n\
         tsink_wal_durable_highwater_frame {wal_durable_highwater_frame}\n\
         # HELP tsink_wal_replay_runs_total WAL replay runs\n\
         # TYPE tsink_wal_replay_runs_total counter\n\
         tsink_wal_replay_runs_total {wal_replay_runs_total}\n\
         # HELP tsink_wal_replay_frames_total WAL replayed frames\n\
         # TYPE tsink_wal_replay_frames_total counter\n\
         tsink_wal_replay_frames_total {wal_replay_frames_total}\n\
         # HELP tsink_wal_replay_series_definitions_total WAL replayed series definitions\n\
         # TYPE tsink_wal_replay_series_definitions_total counter\n\
         tsink_wal_replay_series_definitions_total {wal_replay_series_definitions_total}\n\
         # HELP tsink_wal_replay_sample_batches_total WAL replayed sample batches\n\
         # TYPE tsink_wal_replay_sample_batches_total counter\n\
         tsink_wal_replay_sample_batches_total {wal_replay_sample_batches_total}\n\
         # HELP tsink_wal_replay_points_total WAL replayed points\n\
         # TYPE tsink_wal_replay_points_total counter\n\
         tsink_wal_replay_points_total {wal_replay_points_total}\n\
         # HELP tsink_wal_replay_errors_total WAL replay errors\n\
         # TYPE tsink_wal_replay_errors_total counter\n\
         tsink_wal_replay_errors_total {wal_replay_errors_total}\n\
         # HELP tsink_wal_replay_duration_nanoseconds_total WAL replay runtime\n\
         # TYPE tsink_wal_replay_duration_nanoseconds_total counter\n\
         tsink_wal_replay_duration_nanoseconds_total {wal_replay_duration_nanos_total}\n\
         # HELP tsink_wal_append_series_definitions_total WAL appended series definitions\n\
         # TYPE tsink_wal_append_series_definitions_total counter\n\
         tsink_wal_append_series_definitions_total {wal_append_series_definitions_total}\n\
         # HELP tsink_wal_append_sample_batches_total WAL appended sample batches\n\
         # TYPE tsink_wal_append_sample_batches_total counter\n\
         tsink_wal_append_sample_batches_total {wal_append_sample_batches_total}\n\
         # HELP tsink_wal_append_points_total WAL appended points\n\
         # TYPE tsink_wal_append_points_total counter\n\
         tsink_wal_append_points_total {wal_append_points_total}\n\
         # HELP tsink_wal_append_bytes_total WAL appended bytes\n\
         # TYPE tsink_wal_append_bytes_total counter\n\
         tsink_wal_append_bytes_total {wal_append_bytes_total}\n\
         # HELP tsink_wal_append_errors_total WAL append errors\n\
         # TYPE tsink_wal_append_errors_total counter\n\
         tsink_wal_append_errors_total {wal_append_errors_total}\n\
         # HELP tsink_wal_resets_total WAL resets\n\
         # TYPE tsink_wal_resets_total counter\n\
         tsink_wal_resets_total {wal_resets_total}\n\
         # HELP tsink_wal_reset_errors_total WAL reset errors\n\
         # TYPE tsink_wal_reset_errors_total counter\n\
         tsink_wal_reset_errors_total {wal_reset_errors_total}\n\
         # HELP tsink_flush_pipeline_runs_total Flush pipeline runs\n\
         # TYPE tsink_flush_pipeline_runs_total counter\n\
         tsink_flush_pipeline_runs_total {flush_pipeline_runs_total}\n\
         # HELP tsink_flush_pipeline_success_total Successful flush pipeline runs\n\
         # TYPE tsink_flush_pipeline_success_total counter\n\
         tsink_flush_pipeline_success_total {flush_pipeline_success_total}\n\
         # HELP tsink_flush_pipeline_timeout_total Flush pipeline write-timeout skips\n\
         # TYPE tsink_flush_pipeline_timeout_total counter\n\
         tsink_flush_pipeline_timeout_total {flush_pipeline_timeout_total}\n\
         # HELP tsink_flush_pipeline_errors_total Flush pipeline errors\n\
         # TYPE tsink_flush_pipeline_errors_total counter\n\
         tsink_flush_pipeline_errors_total {flush_pipeline_errors_total}\n\
         # HELP tsink_flush_pipeline_duration_nanoseconds_total Flush pipeline runtime\n\
         # TYPE tsink_flush_pipeline_duration_nanoseconds_total counter\n\
         tsink_flush_pipeline_duration_nanoseconds_total {flush_pipeline_duration_nanos_total}\n\
         # HELP tsink_flush_active_runs_total Active chunk flush runs\n\
         # TYPE tsink_flush_active_runs_total counter\n\
         tsink_flush_active_runs_total {active_flush_runs_total}\n\
         # HELP tsink_flush_active_errors_total Active chunk flush errors\n\
         # TYPE tsink_flush_active_errors_total counter\n\
         tsink_flush_active_errors_total {active_flush_errors_total}\n\
         # HELP tsink_flush_active_inspected_series_total Active series inspected by bounded background flushes\n\
         # TYPE tsink_flush_active_inspected_series_total counter\n\
         tsink_flush_active_inspected_series_total {active_flush_inspected_series_total}\n\
         # HELP tsink_flush_active_selected_input_bytes_total Modeled active-head input bytes selected by bounded background flushes\n\
         # TYPE tsink_flush_active_selected_input_bytes_total counter\n\
         tsink_flush_active_selected_input_bytes_total {active_flush_selected_input_bytes_total}\n\
         # HELP tsink_flush_active_item_limit_hits_total Bounded background flush passes that consumed the series inspection allowance\n\
         # TYPE tsink_flush_active_item_limit_hits_total counter\n\
         tsink_flush_active_item_limit_hits_total {active_flush_item_limit_hits_total}\n\
         # HELP tsink_flush_active_byte_limit_skips_total Active heads skipped because they did not fit the bounded background flush byte allowance\n\
         # TYPE tsink_flush_active_byte_limit_skips_total counter\n\
         tsink_flush_active_byte_limit_skips_total {active_flush_byte_limit_skips_total}\n\
         # HELP tsink_flush_active_series_total Active series flushed into sealed chunks\n\
         # TYPE tsink_flush_active_series_total counter\n\
         tsink_flush_active_series_total {active_flushed_series_total}\n\
         # HELP tsink_flush_active_chunks_total Active chunks flushed\n\
         # TYPE tsink_flush_active_chunks_total counter\n\
         tsink_flush_active_chunks_total {active_flushed_chunks_total}\n\
         # HELP tsink_flush_active_points_total Active points flushed\n\
         # TYPE tsink_flush_active_points_total counter\n\
         tsink_flush_active_points_total {active_flushed_points_total}\n\
         # HELP tsink_flush_persist_runs_total Persist attempts\n\
         # TYPE tsink_flush_persist_runs_total counter\n\
         tsink_flush_persist_runs_total {persist_runs_total}\n\
         # HELP tsink_flush_persist_success_total Successful persist runs\n\
         # TYPE tsink_flush_persist_success_total counter\n\
         tsink_flush_persist_success_total {persist_success_total}\n\
         # HELP tsink_flush_persist_noop_total Persist runs with no new chunks\n\
         # TYPE tsink_flush_persist_noop_total counter\n\
         tsink_flush_persist_noop_total {persist_noop_total}\n\
         # HELP tsink_flush_persist_errors_total Persist errors\n\
         # TYPE tsink_flush_persist_errors_total counter\n\
         tsink_flush_persist_errors_total {persist_errors_total}\n\
         # HELP tsink_flush_persist_inspected_chunks_total Sealed chunks inspected by bounded background persistence windows\n\
         # TYPE tsink_flush_persist_inspected_chunks_total counter\n\
         tsink_flush_persist_inspected_chunks_total {persist_inspected_chunks_total}\n\
         # HELP tsink_flush_persist_selected_input_bytes_total Modeled sealed-chunk input bytes selected by bounded background persistence windows\n\
         # TYPE tsink_flush_persist_selected_input_bytes_total counter\n\
         tsink_flush_persist_selected_input_bytes_total {persist_selected_input_bytes_total}\n\
         # HELP tsink_flush_persist_item_limit_hits_total Bounded background persistence windows that exhausted their item allowance\n\
         # TYPE tsink_flush_persist_item_limit_hits_total counter\n\
         tsink_flush_persist_item_limit_hits_total {persist_item_limit_hits_total}\n\
         # HELP tsink_flush_persist_byte_limit_hits_total Bounded background persistence windows stopped by their byte allowance\n\
         # TYPE tsink_flush_persist_byte_limit_hits_total counter\n\
         tsink_flush_persist_byte_limit_hits_total {persist_byte_limit_hits_total}\n\
         # HELP tsink_flush_persisted_series_total Series persisted\n\
         # TYPE tsink_flush_persisted_series_total counter\n\
         tsink_flush_persisted_series_total {persisted_series_total}\n\
         # HELP tsink_flush_persisted_chunks_total Chunks persisted\n\
         # TYPE tsink_flush_persisted_chunks_total counter\n\
         tsink_flush_persisted_chunks_total {persisted_chunks_total}\n\
         # HELP tsink_flush_persisted_points_total Points persisted\n\
         # TYPE tsink_flush_persisted_points_total counter\n\
         tsink_flush_persisted_points_total {persisted_points_total}\n\
         # HELP tsink_flush_persisted_segments_total Segments emitted by persist\n\
         # TYPE tsink_flush_persisted_segments_total counter\n\
         tsink_flush_persisted_segments_total {persisted_segments_total}\n\
         # HELP tsink_flush_persist_duration_nanoseconds_total Persist runtime\n\
         # TYPE tsink_flush_persist_duration_nanoseconds_total counter\n\
         tsink_flush_persist_duration_nanoseconds_total {persist_duration_nanos_total}\n\
         # HELP tsink_flush_evicted_sealed_chunks_total Sealed chunks evicted after persistence\n\
         # TYPE tsink_flush_evicted_sealed_chunks_total counter\n\
         tsink_flush_evicted_sealed_chunks_total {evicted_sealed_chunks_total}\n\
         # HELP tsink_flush_tier_moves_total Persisted segments moved across storage tiers\n\
         # TYPE tsink_flush_tier_moves_total counter\n\
         tsink_flush_tier_moves_total {flush_tier_moves_total}\n\
         # HELP tsink_flush_tier_move_errors_total Persisted segment tier-move errors\n\
         # TYPE tsink_flush_tier_move_errors_total counter\n\
         tsink_flush_tier_move_errors_total {flush_tier_move_errors_total}\n\
         # HELP tsink_flush_expired_segments_total Persisted segments expired by retention\n\
         # TYPE tsink_flush_expired_segments_total counter\n\
         tsink_flush_expired_segments_total {flush_expired_segments_total}\n\
         # HELP tsink_flush_hot_segments_visible Hot-tier persisted segments visible to queries\n\
         # TYPE tsink_flush_hot_segments_visible gauge\n\
         tsink_flush_hot_segments_visible {flush_hot_segments_visible}\n\
         # HELP tsink_flush_warm_segments_visible Warm-tier persisted segments visible to queries\n\
         # TYPE tsink_flush_warm_segments_visible gauge\n\
         tsink_flush_warm_segments_visible {flush_warm_segments_visible}\n\
         # HELP tsink_flush_cold_segments_visible Cold-tier persisted segments visible to queries\n\
         # TYPE tsink_flush_cold_segments_visible gauge\n\
         tsink_flush_cold_segments_visible {flush_cold_segments_visible}\n\
         # HELP tsink_compaction_runs_total Compaction invocations\n\
         # TYPE tsink_compaction_runs_total counter\n\
         tsink_compaction_runs_total {compaction_runs_total}\n\
         # HELP tsink_compaction_success_total Compaction runs that rewrote segments\n\
         # TYPE tsink_compaction_success_total counter\n\
         tsink_compaction_success_total {compaction_success_total}\n\
         # HELP tsink_compaction_noop_total Compaction runs with no rewrite\n\
         # TYPE tsink_compaction_noop_total counter\n\
         tsink_compaction_noop_total {compaction_noop_total}\n\
         # HELP tsink_compaction_errors_total Compaction errors\n\
         # TYPE tsink_compaction_errors_total counter\n\
         tsink_compaction_errors_total {compaction_errors_total}\n\
         # HELP tsink_compaction_source_segments_total Source segments considered by compaction\n\
         # TYPE tsink_compaction_source_segments_total counter\n\
         tsink_compaction_source_segments_total {compaction_source_segments_total}\n\
         # HELP tsink_compaction_output_segments_total Output segments emitted by compaction\n\
         # TYPE tsink_compaction_output_segments_total counter\n\
         tsink_compaction_output_segments_total {compaction_output_segments_total}\n\
         # HELP tsink_compaction_source_chunks_total Source chunks considered by compaction\n\
         # TYPE tsink_compaction_source_chunks_total counter\n\
         tsink_compaction_source_chunks_total {compaction_source_chunks_total}\n\
         # HELP tsink_compaction_output_chunks_total Output chunks emitted by compaction\n\
         # TYPE tsink_compaction_output_chunks_total counter\n\
         tsink_compaction_output_chunks_total {compaction_output_chunks_total}\n\
         # HELP tsink_compaction_source_points_total Source points considered by compaction\n\
         # TYPE tsink_compaction_source_points_total counter\n\
         tsink_compaction_source_points_total {compaction_source_points_total}\n\
         # HELP tsink_compaction_output_points_total Output points emitted by compaction\n\
         # TYPE tsink_compaction_output_points_total counter\n\
         tsink_compaction_output_points_total {compaction_output_points_total}\n\
         # HELP tsink_compaction_duration_nanoseconds_total Compaction runtime\n\
         # TYPE tsink_compaction_duration_nanoseconds_total counter\n\
         tsink_compaction_duration_nanoseconds_total {compaction_duration_nanos_total}\n\
         # HELP tsink_query_select_calls_total Storage select calls\n\
         # TYPE tsink_query_select_calls_total counter\n\
         tsink_query_select_calls_total {query_select_calls_total}\n\
         # HELP tsink_query_select_errors_total Storage select errors\n\
         # TYPE tsink_query_select_errors_total counter\n\
         tsink_query_select_errors_total {query_select_errors_total}\n\
         # HELP tsink_query_select_duration_nanoseconds_total Storage select runtime\n\
         # TYPE tsink_query_select_duration_nanoseconds_total counter\n\
         tsink_query_select_duration_nanoseconds_total {query_select_duration_nanos_total}\n\
         # HELP tsink_query_select_points_returned_total Points returned by select\n\
         # TYPE tsink_query_select_points_returned_total counter\n\
         tsink_query_select_points_returned_total {query_select_points_returned_total}\n\
         # HELP tsink_query_select_with_options_calls_total Storage select_with_options calls\n\
         # TYPE tsink_query_select_with_options_calls_total counter\n\
         tsink_query_select_with_options_calls_total {query_select_with_options_calls_total}\n\
         # HELP tsink_query_select_with_options_errors_total Storage select_with_options errors\n\
         # TYPE tsink_query_select_with_options_errors_total counter\n\
         tsink_query_select_with_options_errors_total {query_select_with_options_errors_total}\n\
         # HELP tsink_query_select_with_options_duration_nanoseconds_total Storage select_with_options runtime\n\
         # TYPE tsink_query_select_with_options_duration_nanoseconds_total counter\n\
         tsink_query_select_with_options_duration_nanoseconds_total {query_select_with_options_duration_nanos_total}\n\
         # HELP tsink_query_select_with_options_points_returned_total Points returned by select_with_options\n\
         # TYPE tsink_query_select_with_options_points_returned_total counter\n\
         tsink_query_select_with_options_points_returned_total {query_select_with_options_points_returned_total}\n\
         # HELP tsink_query_select_all_calls_total Storage select_all calls\n\
         # TYPE tsink_query_select_all_calls_total counter\n\
         tsink_query_select_all_calls_total {query_select_all_calls_total}\n\
         # HELP tsink_query_select_all_errors_total Storage select_all errors\n\
         # TYPE tsink_query_select_all_errors_total counter\n\
         tsink_query_select_all_errors_total {query_select_all_errors_total}\n\
         # HELP tsink_query_select_all_duration_nanoseconds_total Storage select_all runtime\n\
         # TYPE tsink_query_select_all_duration_nanoseconds_total counter\n\
         tsink_query_select_all_duration_nanoseconds_total {query_select_all_duration_nanos_total}\n\
         # HELP tsink_query_select_all_series_returned_total Series returned by select_all\n\
         # TYPE tsink_query_select_all_series_returned_total counter\n\
         tsink_query_select_all_series_returned_total {query_select_all_series_returned_total}\n\
         # HELP tsink_query_select_all_points_returned_total Points returned by select_all\n\
         # TYPE tsink_query_select_all_points_returned_total counter\n\
         tsink_query_select_all_points_returned_total {query_select_all_points_returned_total}\n\
         # HELP tsink_query_select_series_calls_total Storage select_series calls\n\
         # TYPE tsink_query_select_series_calls_total counter\n\
         tsink_query_select_series_calls_total {query_select_series_calls_total}\n\
         # HELP tsink_query_select_series_errors_total Storage select_series errors\n\
         # TYPE tsink_query_select_series_errors_total counter\n\
         tsink_query_select_series_errors_total {query_select_series_errors_total}\n\
         # HELP tsink_query_select_series_duration_nanoseconds_total Storage select_series runtime\n\
         # TYPE tsink_query_select_series_duration_nanoseconds_total counter\n\
         tsink_query_select_series_duration_nanoseconds_total {query_select_series_duration_nanos_total}\n\
         # HELP tsink_query_select_series_returned_total Series returned by select_series\n\
         # TYPE tsink_query_select_series_returned_total counter\n\
         tsink_query_select_series_returned_total {query_select_series_returned_total}\n\
         # HELP tsink_query_merge_path_queries_total Query series collections using merge path\n\
         # TYPE tsink_query_merge_path_queries_total counter\n\
         tsink_query_merge_path_queries_total {query_merge_path_queries_total}\n\
         # HELP tsink_query_merge_path_shard_snapshots_total Merge-path shard snapshots taken before persisted decode\n\
         # TYPE tsink_query_merge_path_shard_snapshots_total counter\n\
         tsink_query_merge_path_shard_snapshots_total {query_merge_path_shard_snapshots_total}\n\
         # HELP tsink_query_merge_path_shard_snapshot_wait_nanoseconds_total Merge-path runtime waiting to acquire shard read locks for an in-memory snapshot\n\
         # TYPE tsink_query_merge_path_shard_snapshot_wait_nanoseconds_total counter\n\
         tsink_query_merge_path_shard_snapshot_wait_nanoseconds_total {query_merge_path_shard_snapshot_wait_nanos_total}\n\
         # HELP tsink_query_merge_path_shard_snapshot_hold_nanoseconds_total Merge-path runtime holding shard read locks while copying the in-memory snapshot\n\
         # TYPE tsink_query_merge_path_shard_snapshot_hold_nanoseconds_total counter\n\
         tsink_query_merge_path_shard_snapshot_hold_nanoseconds_total {query_merge_path_shard_snapshot_hold_nanos_total}\n\
         # HELP tsink_query_append_sort_path_queries_total Query series collections using append/sort path\n\
         # TYPE tsink_query_append_sort_path_queries_total counter\n\
         tsink_query_append_sort_path_queries_total {query_append_sort_path_queries_total}\n\
         # HELP tsink_query_hot_only_plans_total Query plans satisfied from the hot tier only\n\
         # TYPE tsink_query_hot_only_plans_total counter\n\
         tsink_query_hot_only_plans_total {query_hot_only_plans_total}\n\
         # HELP tsink_query_warm_tier_plans_total Query plans that include the warm tier\n\
         # TYPE tsink_query_warm_tier_plans_total counter\n\
         tsink_query_warm_tier_plans_total {query_warm_tier_plans_total}\n\
         # HELP tsink_query_cold_tier_plans_total Query plans that include the cold tier\n\
         # TYPE tsink_query_cold_tier_plans_total counter\n\
         tsink_query_cold_tier_plans_total {query_cold_tier_plans_total}\n\
         # HELP tsink_query_hot_tier_persisted_chunks_read_total Hot-tier persisted chunks decoded by queries\n\
         # TYPE tsink_query_hot_tier_persisted_chunks_read_total counter\n\
         tsink_query_hot_tier_persisted_chunks_read_total {query_hot_tier_persisted_chunks_read_total}\n\
         # HELP tsink_query_warm_tier_persisted_chunks_read_total Warm-tier persisted chunks decoded by queries\n\
         # TYPE tsink_query_warm_tier_persisted_chunks_read_total counter\n\
         tsink_query_warm_tier_persisted_chunks_read_total {query_warm_tier_persisted_chunks_read_total}\n\
         # HELP tsink_query_cold_tier_persisted_chunks_read_total Cold-tier persisted chunks decoded by queries\n\
         # TYPE tsink_query_cold_tier_persisted_chunks_read_total counter\n\
         tsink_query_cold_tier_persisted_chunks_read_total {query_cold_tier_persisted_chunks_read_total}\n\
         # HELP tsink_query_warm_tier_fetch_duration_nanoseconds_total Warm-tier persisted chunk fetch and decode time\n\
         # TYPE tsink_query_warm_tier_fetch_duration_nanoseconds_total counter\n\
         tsink_query_warm_tier_fetch_duration_nanoseconds_total {query_warm_tier_fetch_duration_nanos_total}\n\
         # HELP tsink_query_cold_tier_fetch_duration_nanoseconds_total Cold-tier persisted chunk fetch and decode time\n\
         # TYPE tsink_query_cold_tier_fetch_duration_nanoseconds_total counter\n\
         tsink_query_cold_tier_fetch_duration_nanoseconds_total {query_cold_tier_fetch_duration_nanos_total}\n\
         # HELP tsink_remote_storage_catalog_refreshes_total Remote object-store catalog refreshes\n\
         # TYPE tsink_remote_storage_catalog_refreshes_total counter\n\
         tsink_remote_storage_catalog_refreshes_total {remote_storage_catalog_refreshes_total}\n\
         # HELP tsink_remote_storage_catalog_refresh_errors_total Remote object-store catalog refresh errors\n\
         # TYPE tsink_remote_storage_catalog_refresh_errors_total counter\n\
         tsink_remote_storage_catalog_refresh_errors_total {remote_storage_catalog_refresh_errors_total}\n\
         # HELP tsink_remote_storage_catalog_refresh_consecutive_failures Consecutive remote object-store catalog refresh failures\n\
         # TYPE tsink_remote_storage_catalog_refresh_consecutive_failures gauge\n\
         tsink_remote_storage_catalog_refresh_consecutive_failures {remote_storage_catalog_refresh_consecutive_failures}\n\
         # HELP tsink_remote_storage_catalog_refresh_backoff_active Whether remote catalog refresh retry backoff is currently active\n\
         # TYPE tsink_remote_storage_catalog_refresh_backoff_active gauge\n\
         tsink_remote_storage_catalog_refresh_backoff_active {remote_storage_catalog_refresh_backoff_active}\n\
         # HELP tsink_remote_storage_accessible Whether remote object-store access is currently healthy\n\
         # TYPE tsink_remote_storage_accessible gauge\n\
         tsink_remote_storage_accessible {remote_storage_accessible}\n\
         # HELP tsink_remote_storage_mirror_hot_segments Whether hot segments are mirrored into object-store hot/\n\
         # TYPE tsink_remote_storage_mirror_hot_segments gauge\n\
         tsink_remote_storage_mirror_hot_segments {remote_storage_mirror_hot_segments}\n\
         # HELP tsink_remote_storage_compute_only Whether the local storage runtime is compute-only\n\
         # TYPE tsink_remote_storage_compute_only gauge\n\
         tsink_remote_storage_compute_only {remote_storage_compute_only}\n\
         # HELP tsink_cluster_write_requests_total Cluster write requests routed through the coordinator\n\
         # TYPE tsink_cluster_write_requests_total counter\n\
         tsink_cluster_write_requests_total {cluster_write_requests_total}\n\
         # HELP tsink_cluster_write_local_rows_total Rows inserted on local owner by write router\n\
         # TYPE tsink_cluster_write_local_rows_total counter\n\
         tsink_cluster_write_local_rows_total {cluster_write_local_rows_total}\n\
         # HELP tsink_cluster_write_routed_rows_total Rows forwarded to remote owners by write router\n\
         # TYPE tsink_cluster_write_routed_rows_total counter\n\
         tsink_cluster_write_routed_rows_total {cluster_write_routed_rows_total}\n\
         # HELP tsink_cluster_write_routed_batches_total Remote write batches sent by write router\n\
         # TYPE tsink_cluster_write_routed_batches_total counter\n\
         tsink_cluster_write_routed_batches_total {cluster_write_routed_batches_total}\n\
         # HELP tsink_cluster_write_failures_total Cluster write routing failures\n\
         # TYPE tsink_cluster_write_failures_total counter\n\
         tsink_cluster_write_failures_total {cluster_write_failures_total}\n\
         # HELP tsink_cluster_dedupe_requests_total Internal ingest idempotency key checks\n\
         # TYPE tsink_cluster_dedupe_requests_total counter\n\
         tsink_cluster_dedupe_requests_total {cluster_dedupe_requests_total}\n\
         # HELP tsink_cluster_dedupe_accepted_total Internal ingest requests accepted for dedupe tracking\n\
         # TYPE tsink_cluster_dedupe_accepted_total counter\n\
         tsink_cluster_dedupe_accepted_total {cluster_dedupe_accepted_total}\n\
         # HELP tsink_cluster_dedupe_duplicates_total Internal ingest requests deduplicated by idempotency key\n\
         # TYPE tsink_cluster_dedupe_duplicates_total counter\n\
         tsink_cluster_dedupe_duplicates_total {cluster_dedupe_duplicates_total}\n\
         # HELP tsink_cluster_dedupe_inflight_rejections_total Internal ingest idempotency conflicts while key is in-flight\n\
         # TYPE tsink_cluster_dedupe_inflight_rejections_total counter\n\
         tsink_cluster_dedupe_inflight_rejections_total {cluster_dedupe_inflight_rejections_total}\n\
         # HELP tsink_cluster_dedupe_commits_total Internal ingest dedupe marker commits\n\
         # TYPE tsink_cluster_dedupe_commits_total counter\n\
         tsink_cluster_dedupe_commits_total {cluster_dedupe_commits_total}\n\
         # HELP tsink_cluster_dedupe_aborts_total Internal ingest dedupe marker aborts after failed writes\n\
         # TYPE tsink_cluster_dedupe_aborts_total counter\n\
         tsink_cluster_dedupe_aborts_total {cluster_dedupe_aborts_total}\n\
         # HELP tsink_cluster_dedupe_cleanup_runs_total Internal ingest dedupe cleanup runs\n\
         # TYPE tsink_cluster_dedupe_cleanup_runs_total counter\n\
         tsink_cluster_dedupe_cleanup_runs_total {cluster_dedupe_cleanup_runs_total}\n\
         # HELP tsink_cluster_dedupe_expired_keys_total Internal ingest dedupe markers expired by TTL\n\
         # TYPE tsink_cluster_dedupe_expired_keys_total counter\n\
         tsink_cluster_dedupe_expired_keys_total {cluster_dedupe_expired_keys_total}\n\
         # HELP tsink_cluster_dedupe_evicted_keys_total Internal ingest dedupe markers evicted by size bound\n\
         # TYPE tsink_cluster_dedupe_evicted_keys_total counter\n\
         tsink_cluster_dedupe_evicted_keys_total {cluster_dedupe_evicted_keys_total}\n\
         # HELP tsink_cluster_dedupe_persistence_failures_total Internal ingest dedupe marker persistence failures\n\
         # TYPE tsink_cluster_dedupe_persistence_failures_total counter\n\
         tsink_cluster_dedupe_persistence_failures_total {cluster_dedupe_persistence_failures_total}\n\
         # HELP tsink_cluster_dedupe_active_keys Active internal ingest dedupe keys in window\n\
         # TYPE tsink_cluster_dedupe_active_keys gauge\n\
         tsink_cluster_dedupe_active_keys {cluster_dedupe_active_keys}\n\
         # HELP tsink_cluster_dedupe_inflight_keys Internal ingest dedupe keys currently in-flight\n\
         # TYPE tsink_cluster_dedupe_inflight_keys gauge\n\
         tsink_cluster_dedupe_inflight_keys {cluster_dedupe_inflight_keys}\n\
         # HELP tsink_cluster_dedupe_log_bytes Durable internal ingest dedupe marker log bytes on disk\n\
         # TYPE tsink_cluster_dedupe_log_bytes gauge\n\
         tsink_cluster_dedupe_log_bytes {cluster_dedupe_log_bytes}\n",
        memory_excluded = memory_obs.excluded_bytes,
        memory_registry = memory_obs.registry_bytes,
        memory_metadata = memory_obs.metadata_cache_bytes,
        memory_persisted_index = memory_obs.persisted_index_bytes,
        memory_persisted_mmap = memory_obs.persisted_mmap_bytes,
        memory_tombstones = memory_obs.tombstone_bytes,
        memory_remote_catalog_staging = memory_obs.remote_catalog_staging_bytes,
        memory_wal_writer_buffer = memory_obs.wal_writer_buffer_bytes,
        memory_wal_series_definition_cache = memory_obs.wal_series_definition_cache_bytes,
        memory_write_transient = memory_obs.write_transient_bytes,
        memory_write_transient_peak = memory_obs.peak_write_transient_bytes,
        memory_write_transient_reservations_total = memory_obs.write_transient_reservations_total,
        memory_write_transient_rejections_total = memory_obs.write_transient_rejections_total,
        memory_excluded_known = u8::from(memory_obs.excluded_bytes_known),
        memory_active_backpressured_writers = memory_obs.pressure.active_backpressured_writers,
        memory_backpressure_events_total = memory_obs.pressure.backpressure_events_total,
        memory_rejections_total = memory_obs.pressure.rejections_total,
        wal_size_bytes = obs.wal.size_bytes,
        wal_segments = obs.wal.segment_count,
        wal_active_segment = obs.wal.active_segment,
        wal_acknowledged_writes_durable = u8::from(obs.wal.acknowledged_writes_durable),
        wal_highwater_segment = obs.wal.highwater_segment,
        wal_highwater_frame = obs.wal.highwater_frame,
        wal_durable_highwater_segment = obs.wal.durable_highwater_segment,
        wal_durable_highwater_frame = obs.wal.durable_highwater_frame,
        wal_replay_runs_total = obs.wal.replay_runs_total,
        wal_replay_frames_total = obs.wal.replay_frames_total,
        wal_replay_series_definitions_total = obs.wal.replay_series_definitions_total,
        wal_replay_sample_batches_total = obs.wal.replay_sample_batches_total,
        wal_replay_points_total = obs.wal.replay_points_total,
        wal_replay_errors_total = obs.wal.replay_errors_total,
        wal_replay_duration_nanos_total = obs.wal.replay_duration_nanos_total,
        wal_append_series_definitions_total = obs.wal.append_series_definitions_total,
        wal_append_sample_batches_total = obs.wal.append_sample_batches_total,
        wal_append_points_total = obs.wal.append_points_total,
        wal_append_bytes_total = obs.wal.append_bytes_total,
        wal_append_errors_total = obs.wal.append_errors_total,
        wal_resets_total = obs.wal.resets_total,
        wal_reset_errors_total = obs.wal.reset_errors_total,
        flush_pipeline_runs_total = obs.flush.pipeline_runs_total,
        flush_pipeline_success_total = obs.flush.pipeline_success_total,
        flush_pipeline_timeout_total = obs.flush.pipeline_timeout_total,
        flush_pipeline_errors_total = obs.flush.pipeline_errors_total,
        flush_pipeline_duration_nanos_total = obs.flush.pipeline_duration_nanos_total,
        active_flush_runs_total = obs.flush.active_flush_runs_total,
        active_flush_errors_total = obs.flush.active_flush_errors_total,
        active_flush_inspected_series_total = obs.flush.active_flush_inspected_series_total,
        active_flush_selected_input_bytes_total = obs
            .flush
            .active_flush_selected_input_bytes_total,
        active_flush_item_limit_hits_total = obs.flush.active_flush_item_limit_hits_total,
        active_flush_byte_limit_skips_total = obs.flush.active_flush_byte_limit_skips_total,
        active_flushed_series_total = obs.flush.active_flushed_series_total,
        active_flushed_chunks_total = obs.flush.active_flushed_chunks_total,
        active_flushed_points_total = obs.flush.active_flushed_points_total,
        persist_runs_total = obs.flush.persist_runs_total,
        persist_success_total = obs.flush.persist_success_total,
        persist_noop_total = obs.flush.persist_noop_total,
        persist_errors_total = obs.flush.persist_errors_total,
        persist_inspected_chunks_total = obs.flush.persist_inspected_chunks_total,
        persist_selected_input_bytes_total = obs.flush.persist_selected_input_bytes_total,
        persist_item_limit_hits_total = obs.flush.persist_item_limit_hits_total,
        persist_byte_limit_hits_total = obs.flush.persist_byte_limit_hits_total,
        persisted_series_total = obs.flush.persisted_series_total,
        persisted_chunks_total = obs.flush.persisted_chunks_total,
        persisted_points_total = obs.flush.persisted_points_total,
        persisted_segments_total = obs.flush.persisted_segments_total,
        persist_duration_nanos_total = obs.flush.persist_duration_nanos_total,
        evicted_sealed_chunks_total = obs.flush.evicted_sealed_chunks_total,
        flush_tier_moves_total = obs.flush.tier_moves_total,
        flush_tier_move_errors_total = obs.flush.tier_move_errors_total,
        flush_expired_segments_total = obs.flush.expired_segments_total,
        flush_hot_segments_visible = obs.flush.hot_segments_visible,
        flush_warm_segments_visible = obs.flush.warm_segments_visible,
        flush_cold_segments_visible = obs.flush.cold_segments_visible,
        compaction_runs_total = obs.compaction.runs_total,
        compaction_success_total = obs.compaction.success_total,
        compaction_noop_total = obs.compaction.noop_total,
        compaction_errors_total = obs.compaction.errors_total,
        compaction_source_segments_total = obs.compaction.source_segments_total,
        compaction_output_segments_total = obs.compaction.output_segments_total,
        compaction_source_chunks_total = obs.compaction.source_chunks_total,
        compaction_output_chunks_total = obs.compaction.output_chunks_total,
        compaction_source_points_total = obs.compaction.source_points_total,
        compaction_output_points_total = obs.compaction.output_points_total,
        compaction_duration_nanos_total = obs.compaction.duration_nanos_total,
        query_select_calls_total = obs.query.select_calls_total,
        query_select_errors_total = obs.query.select_errors_total,
        query_select_duration_nanos_total = obs.query.select_duration_nanos_total,
        query_select_points_returned_total = obs.query.select_points_returned_total,
        query_select_with_options_calls_total = obs.query.select_with_options_calls_total,
        query_select_with_options_errors_total = obs.query.select_with_options_errors_total,
        query_select_with_options_duration_nanos_total =
            obs.query.select_with_options_duration_nanos_total,
        query_select_with_options_points_returned_total =
            obs.query.select_with_options_points_returned_total,
        query_select_all_calls_total = obs.query.select_all_calls_total,
        query_select_all_errors_total = obs.query.select_all_errors_total,
        query_select_all_duration_nanos_total = obs.query.select_all_duration_nanos_total,
        query_select_all_series_returned_total = obs.query.select_all_series_returned_total,
        query_select_all_points_returned_total = obs.query.select_all_points_returned_total,
        query_select_series_calls_total = obs.query.select_series_calls_total,
        query_select_series_errors_total = obs.query.select_series_errors_total,
        query_select_series_duration_nanos_total = obs.query.select_series_duration_nanos_total,
        query_select_series_returned_total = obs.query.select_series_returned_total,
        query_merge_path_queries_total = obs.query.merge_path_queries_total,
        query_merge_path_shard_snapshots_total = obs.query.merge_path_shard_snapshots_total,
        query_merge_path_shard_snapshot_wait_nanos_total =
            obs.query.merge_path_shard_snapshot_wait_nanos_total,
        query_merge_path_shard_snapshot_hold_nanos_total =
            obs.query.merge_path_shard_snapshot_hold_nanos_total,
        query_append_sort_path_queries_total = obs.query.append_sort_path_queries_total,
        query_hot_only_plans_total = obs.query.hot_only_query_plans_total,
        query_warm_tier_plans_total = obs.query.warm_tier_query_plans_total,
        query_cold_tier_plans_total = obs.query.cold_tier_query_plans_total,
        query_hot_tier_persisted_chunks_read_total =
            obs.query.hot_tier_persisted_chunks_read_total,
        query_warm_tier_persisted_chunks_read_total =
            obs.query.warm_tier_persisted_chunks_read_total,
        query_cold_tier_persisted_chunks_read_total =
            obs.query.cold_tier_persisted_chunks_read_total,
        query_warm_tier_fetch_duration_nanos_total =
            obs.query.warm_tier_fetch_duration_nanos_total,
        query_cold_tier_fetch_duration_nanos_total =
            obs.query.cold_tier_fetch_duration_nanos_total,
        remote_storage_catalog_refreshes_total = obs.remote.catalog_refreshes_total,
        remote_storage_catalog_refresh_errors_total = obs.remote.catalog_refresh_errors_total,
        remote_storage_catalog_refresh_consecutive_failures =
            obs.remote.consecutive_refresh_failures,
        remote_storage_catalog_refresh_backoff_active = u8::from(obs.remote.backoff_active),
        remote_storage_accessible = u8::from(obs.remote.accessible),
        remote_storage_mirror_hot_segments = u8::from(obs.remote.mirror_hot_segments),
        remote_storage_compute_only =
            u8::from(obs.remote.runtime_mode == tsink::StorageRuntimeMode::ComputeOnly),
        cluster_write_requests_total = cluster_write_metrics.requests_total,
        cluster_write_local_rows_total = cluster_write_metrics.local_rows_total,
        cluster_write_routed_rows_total = cluster_write_metrics.routed_rows_total,
        cluster_write_routed_batches_total = cluster_write_metrics.routed_batches_total,
        cluster_write_failures_total = cluster_write_metrics.failures_total,
        cluster_dedupe_requests_total = cluster_dedupe_metrics.requests_total,
        cluster_dedupe_accepted_total = cluster_dedupe_metrics.accepted_total,
        cluster_dedupe_duplicates_total = cluster_dedupe_metrics.duplicates_total,
        cluster_dedupe_inflight_rejections_total =
            cluster_dedupe_metrics.inflight_rejections_total,
        cluster_dedupe_commits_total = cluster_dedupe_metrics.commits_total,
        cluster_dedupe_aborts_total = cluster_dedupe_metrics.aborts_total,
        cluster_dedupe_cleanup_runs_total = cluster_dedupe_metrics.cleanup_runs_total,
        cluster_dedupe_expired_keys_total = cluster_dedupe_metrics.expired_keys_total,
        cluster_dedupe_evicted_keys_total = cluster_dedupe_metrics.evicted_keys_total,
        cluster_dedupe_persistence_failures_total =
            cluster_dedupe_metrics.persistence_failures_total,
        cluster_dedupe_active_keys = cluster_dedupe_metrics.active_keys,
        cluster_dedupe_inflight_keys = cluster_dedupe_metrics.inflight_keys,
        cluster_dedupe_log_bytes = cluster_dedupe_metrics.log_bytes,
    );

        append_local_disk_metrics(body, local_disk.as_ref());
        append_offline_restore_disk_metrics(body, offline_restore_disk.as_ref());
        cluster::append_metrics(
            body,
            ClusterMetrics {
                write_labeled: cluster_write_labeled_metrics,
                fanout: cluster_fanout_metrics,
                fanout_labeled: cluster_fanout_labeled_metrics,
                read_planner: cluster_read_planner_metrics,
                read_planner_labeled: &cluster_read_planner_labeled_metrics,
                outbox: cluster_outbox_metrics,
                outbox_peers: cluster_outbox_peers,
                control: &cluster_control.liveness,
                handoff: &cluster_control.handoff,
                digest: cluster_digest,
                rebalance: cluster_rebalance,
                hotspot: cluster_hotspot,
            },
        );
        append_cluster_audit_metrics(body, &cluster_audit_health);
        append_cluster_control_persistence_metrics(body, &cluster_control.persistence);
        append_metric_metadata_store_metrics(body, &metadata_store_metrics);
        append_exemplar_metrics(
            body,
            &exemplar_metrics,
            exemplar_store.config(),
            exemplar_store.resource_limits(),
        );
        append_rules_metrics(body, &rules_snapshot);
        append_rollup_metrics(body, &obs.rollups, &obs.query);
        append_cardinality_metrics(body, &obs.cardinality);
        append_query_budget_metrics(body, &obs.query_budget);
        append_background_work_metrics(body, &obs.background);
        append_prometheus_payload_metrics(body, &payload_status);
        append_otlp_metrics(body, &otlp_status);
        append_legacy_ingest_metrics(body, &legacy_ingest_status);
        append_edge_sync_metrics(body, &edge_sync_metrics.source, &edge_sync_metrics.accept);
        append_read_admission_metrics(body, read_admission_metrics);
        append_write_admission_metrics(body, write_admission_metrics);
        append_write_rejection_metrics(body, write_rejection_metrics);
        append_tenant_admission_metrics(body, tenant_admission_metrics);
        append_tenant_runtime_cache_metrics(body, tenant_runtime_cache_metrics);
        append_security_metrics(body, &security_metrics, rbac_metrics.as_ref());
        append_usage_metrics(body, &usage_status);
        append_metrics_collection_errors(body, rendered_collection_errors);
    };

    let mut counter = CountingMetricsBody::new(&execution);
    append_body(&mut counter);
    let CountingMetricsBody {
        len: body_len,
        control_error,
        overflowed,
        ..
    } = counter;
    if let Some(err) = control_error {
        eprintln!(
            "metrics collection error in {}: {err}",
            MetricsCollector::ExpositionBody.name()
        );
        collection_errors.insert(MetricsCollector::ExpositionBody);
        return metrics_fallback_response(collection_errors);
    }
    if overflowed {
        collection_errors.insert(MetricsCollector::ExpositionBody);
        return metrics_fallback_response(collection_errors);
    }
    if let Err(err) = execution.charge_returned_bytes(u64::try_from(body_len).unwrap_or(u64::MAX)) {
        eprintln!(
            "metrics collection error in {}: {err}",
            MetricsCollector::ExpositionBody.name()
        );
        collection_errors.insert(MetricsCollector::ExpositionBody);
        return metrics_fallback_response(collection_errors);
    }
    let mut response_reservation =
        match execution.reserve_memory(modeled_metrics_response_preflight_bytes(body_len)) {
            Ok(reservation) => reservation,
            Err(err) => {
                eprintln!(
                    "metrics collection error in {}: {err}",
                    MetricsCollector::ExpositionBody.name()
                );
                collection_errors.insert(MetricsCollector::ExpositionBody);
                return metrics_fallback_response(collection_errors);
            }
        };
    let body = match BoundedMetricsBody::try_with_exact_capacity(body_len) {
        Ok(body) => body,
        Err(err) => {
            eprintln!(
                "metrics collection error in {}: body allocation failed: {err}",
                MetricsCollector::ExpositionBody.name()
            );
            drop(response_reservation);
            collection_errors.insert(MetricsCollector::ExpositionBody);
            return metrics_fallback_response(collection_errors);
        }
    };
    if let Err(err) =
        response_reservation.resize(modeled_metrics_response_preflight_bytes(body.capacity()))
    {
        eprintln!(
            "metrics collection error in {}: {err}",
            MetricsCollector::ExpositionBody.name()
        );
        drop(body);
        drop(response_reservation);
        collection_errors.insert(MetricsCollector::ExpositionBody);
        return metrics_fallback_response(collection_errors);
    }
    let mut controlled_body = ControlledMetricsBody::new(body, &execution);
    append_body(&mut controlled_body);
    let ControlledMetricsBody {
        body,
        control_error,
        ..
    } = controlled_body;
    if let Some(err) = control_error {
        eprintln!(
            "metrics collection error in {}: {err}",
            MetricsCollector::ExpositionBody.name()
        );
        drop(body);
        drop(response_reservation);
        collection_errors.insert(MetricsCollector::ExpositionBody);
        return metrics_fallback_response(collection_errors);
    }
    if body.overflowed || body.len() != body_len {
        drop(body);
        drop(response_reservation);
        collection_errors.insert(MetricsCollector::ExpositionBody);
        return metrics_fallback_response(collection_errors);
    }
    let body = match body.into_bytes() {
        Ok(body) => body,
        Err(()) => {
            drop(response_reservation);
            collection_errors.insert(MetricsCollector::ExpositionBody);
            return metrics_fallback_response(collection_errors);
        }
    };
    let response = HttpResponse::new(200, body).with_header("Content-Type", METRICS_CONTENT_TYPE);
    let retained_bytes = modeled_tsdb_status_response_retained_bytes(&response);
    drop(accounted_cluster_hotspot);
    if let Err(err) = response_reservation.resize(retained_bytes) {
        eprintln!(
            "metrics collection error in {}: {err}",
            MetricsCollector::ExpositionBody.name()
        );
        drop(response);
        drop(response_reservation);
        collection_errors.insert(MetricsCollector::ExpositionBody);
        return metrics_fallback_response(collection_errors);
    }
    drop(response_reservation);
    drop(execution);
    response
}

fn append_background_work_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &tsink::BackgroundWorkObservabilitySnapshot,
) {
    body.push_str(
        "# HELP tsink_background_threads Instance-owned background thread bounds and current state\n\
         # TYPE tsink_background_threads gauge\n",
    );
    metrics_write!(
        body,
        "tsink_background_threads{{state=\"limit\"}} {}\n",
        snapshot.max_threads
    );
    metrics_write!(
        body,
        "tsink_background_threads{{state=\"installed\"}} {}\n",
        snapshot.installed_threads
    );
    metrics_write!(
        body,
        "tsink_background_threads{{state=\"running\"}} {}\n",
        snapshot.running_threads
    );
    body.push_str(
        "# HELP tsink_background_worker_state Fixed-cardinality lifecycle and cadence state for each engine worker slot\n\
         # TYPE tsink_background_worker_state gauge\n\
         # HELP tsink_background_worker_events_total Fixed-cardinality wait, pass, notification, exit, and join counters for each engine worker slot\n\
         # TYPE tsink_background_worker_events_total counter\n",
    );
    for (worker, state) in [
        ("flush", snapshot.flush),
        ("compaction", snapshot.compaction),
        ("persisted_refresh", snapshot.persisted_refresh),
        ("rollup", snapshot.rollup),
    ] {
        metrics_write!(
            body,
            "tsink_background_worker_state{{worker=\"{worker}\",state=\"installed\"}} {}\n",
            u8::from(state.installed)
        );
        metrics_write!(
            body,
            "tsink_background_worker_state{{worker=\"{worker}\",state=\"running\"}} {}\n",
            u8::from(state.running)
        );
        metrics_write!(
            body,
            "tsink_background_worker_state{{worker=\"{worker}\",state=\"max_concurrency\"}} {}\n",
            state.max_concurrency
        );
        metrics_write!(
            body,
            "tsink_background_worker_state{{worker=\"{worker}\",state=\"interval_nanos\"}} {}\n",
            state.interval_nanos.unwrap_or(0)
        );
        for (event, value) in [
            ("starts", state.starts_total),
            ("exits", state.exits_total),
            ("notifications", state.notifications_total),
            ("idle_waits", state.idle_waits_total),
            ("passes_started", state.passes_started_total),
            ("passes_completed", state.passes_completed_total),
            ("shutdown_joins", state.shutdown_joins_total),
        ] {
            metrics_write!(body,
                "tsink_background_worker_events_total{{worker=\"{worker}\",event=\"{event}\"}} {value}\n"
            );
        }
    }
    body.push_str(
        "# HELP tsink_storage_close_events_total Fixed-cardinality close lifecycle outcomes\n\
         # TYPE tsink_storage_close_events_total counter\n\
         # HELP tsink_storage_close_wait_nanos_total Time spent in close coordination and worker joins\n\
         # TYPE tsink_storage_close_wait_nanos_total counter\n\
         # HELP tsink_storage_close_duration_nanos_total Wall-clock duration of close attempts, including filesystem calls\n\
         # TYPE tsink_storage_close_duration_nanos_total counter\n\
         # HELP tsink_storage_close_compaction_passes_total Compaction passes attempted by close\n\
         # TYPE tsink_storage_close_compaction_passes_total counter\n\
         # HELP tsink_storage_close_compaction_pass_limit Maximum compaction passes attempted by one close\n\
         # TYPE tsink_storage_close_compaction_pass_limit gauge\n",
    );
    for (event, value) in [
        ("attempts", snapshot.close_attempts_total),
        ("success", snapshot.close_success_total),
        ("errors", snapshot.close_errors_total),
        (
            "coordination_timeouts",
            snapshot.close_coordination_timeouts_total,
        ),
    ] {
        metrics_write!(
            body,
            "tsink_storage_close_events_total{{event=\"{event}\"}} {value}\n"
        );
    }
    for (wait, value) in [
        ("coordination", snapshot.close_coordination_wait_nanos_total),
        ("worker_join", snapshot.shutdown_join_wait_nanos_total),
    ] {
        metrics_write!(
            body,
            "tsink_storage_close_wait_nanos_total{{wait=\"{wait}\"}} {value}\n"
        );
    }
    metrics_write!(
        body,
        "tsink_storage_close_duration_nanos_total {}\n",
        snapshot.close_duration_nanos_total
    );
    metrics_write!(
        body,
        "tsink_storage_close_compaction_passes_total {}\n",
        snapshot.close_compaction_passes_total
    );
    metrics_write!(
        body,
        "tsink_storage_close_compaction_pass_limit {}\n",
        snapshot.close_compaction_pass_limit
    );
}

fn append_cardinality_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &tsink::CardinalityObservabilitySnapshot,
) {
    body.push_str(
        "# HELP tsink_series_creation_pending New-series reservations awaiting write publication\n\
         # TYPE tsink_series_creation_pending gauge\n",
    );
    metrics_write!(
        body,
        "tsink_series_creation_pending {}\n",
        snapshot.pending_new_series
    );
    body.push_str(
        "# HELP tsink_series_creation_committed_in_window New series committed in the current fixed creation-rate window\n\
         # TYPE tsink_series_creation_committed_in_window gauge\n",
    );
    metrics_write!(
        body,
        "tsink_series_creation_committed_in_window {}\n",
        snapshot.committed_in_window
    );
    body.push_str(
        "# HELP tsink_series_creation_window_initialized Whether a creation-rate window has been initialized\n\
         # TYPE tsink_series_creation_window_initialized gauge\n",
    );
    metrics_write!(
        body,
        "tsink_series_creation_window_initialized {}\n",
        u8::from(snapshot.current_window_start.is_some())
    );
    body.push_str(
        "# HELP tsink_series_creation_window_start Storage timestamp-unit start of the current creation-rate window, or zero when uninitialized\n\
         # TYPE tsink_series_creation_window_start gauge\n",
    );
    metrics_write!(
        body,
        "tsink_series_creation_window_start {}\n",
        snapshot.current_window_start.unwrap_or(0)
    );
    body.push_str(
        "# HELP tsink_series_creation_admitted_total New-series reservations admitted since the storage instance opened\n\
         # TYPE tsink_series_creation_admitted_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_series_creation_admitted_total {}\n",
        snapshot.admitted_new_series_total
    );
    body.push_str(
        "# HELP tsink_series_creation_committed_total New series successfully published since the storage instance opened\n\
         # TYPE tsink_series_creation_committed_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_series_creation_committed_total {}\n",
        snapshot.committed_new_series_total
    );
    body.push_str(
        "# HELP tsink_series_creation_rejections_total New-series creation-rate admission rejections since the storage instance opened\n\
         # TYPE tsink_series_creation_rejections_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_series_creation_rejections_total {}\n",
        snapshot.creation_rate_rejections_total
    );
}

pub(super) fn append_query_budget_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &tsink::QueryBudgetSnapshot,
) {
    body.push_str(
        "# HELP tsink_query_budget_active_queries Queries currently holding a core query permit\n\
         # TYPE tsink_query_budget_active_queries gauge\n\
         # HELP tsink_query_budget_peak_active_queries Peak core query permits held concurrently\n\
         # TYPE tsink_query_budget_peak_active_queries gauge\n\
         # HELP tsink_query_budget_reserved_memory_bytes Modeled query memory currently reserved across live queries\n\
         # TYPE tsink_query_budget_reserved_memory_bytes gauge\n\
         # HELP tsink_query_budget_peak_reserved_memory_bytes Peak modeled query memory reserved across live queries\n\
         # TYPE tsink_query_budget_peak_reserved_memory_bytes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_query_budget_active_queries {}\n\
         tsink_query_budget_peak_active_queries {}\n\
         tsink_query_budget_reserved_memory_bytes {}\n\
         tsink_query_budget_peak_reserved_memory_bytes {}\n",
        snapshot.active_queries,
        snapshot.peak_active_queries,
        snapshot.shared_reserved_memory_bytes,
        snapshot.peak_shared_reserved_memory_bytes,
    );
    body.push_str(
        "# HELP tsink_query_budget_queries_started_total Core queries admitted since the storage instance opened\n\
         # TYPE tsink_query_budget_queries_started_total counter\n\
         # HELP tsink_query_budget_queries_completed_total Admitted core query permits released since the storage instance opened\n\
         # TYPE tsink_query_budget_queries_completed_total counter\n\
         # HELP tsink_query_budget_limit_rejections_total Core query limit rejections across all reasons\n\
         # TYPE tsink_query_budget_limit_rejections_total counter\n\
         # HELP tsink_query_budget_limit_rejections_by_reason_total Core query limit rejections by stable reason\n\
         # TYPE tsink_query_budget_limit_rejections_by_reason_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_query_budget_queries_started_total {}\n\
         tsink_query_budget_queries_completed_total {}\n\
         tsink_query_budget_limit_rejections_total {}\n",
        snapshot.queries_started_total,
        snapshot.queries_completed_total,
        snapshot.limit_rejections_total,
    );
    for (reason, value) in [
        ("concurrent_queries", snapshot.concurrency_rejections_total),
        (
            "shared_memory_bytes",
            snapshot.shared_memory_rejections_total,
        ),
        (
            "per_query_memory_bytes",
            snapshot.per_query_memory_rejections_total,
        ),
        ("series_matched", snapshot.series_matched_rejections_total),
        ("samples_scanned", snapshot.samples_scanned_rejections_total),
        (
            "samples_returned",
            snapshot.samples_returned_rejections_total,
        ),
        ("returned_bytes", snapshot.returned_bytes_rejections_total),
        (
            "pattern_expansion",
            snapshot.pattern_expansion_rejections_total,
        ),
        ("steps", snapshot.steps_rejections_total),
        (
            "intermediate_vector_size",
            snapshot.intermediate_vector_size_rejections_total,
        ),
    ] {
        metrics_write!(
            body,
            "tsink_query_budget_limit_rejections_by_reason_total{{reason=\"{reason}\"}} {value}\n"
        );
    }
    body.push_str(
        "# HELP tsink_query_budget_cancellations_total Query admission or execution attempts that observed cooperative cancellation\n\
         # TYPE tsink_query_budget_cancellations_total counter\n\
         # HELP tsink_query_budget_deadline_exceeded_total Query admission or execution attempts that observed their effective deadline\n\
         # TYPE tsink_query_budget_deadline_exceeded_total counter\n\
         # HELP tsink_query_budget_accounting_invariant_violations_total Internal query permit or memory release inconsistencies\n\
         # TYPE tsink_query_budget_accounting_invariant_violations_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_query_budget_cancellations_total {}\n\
         tsink_query_budget_deadline_exceeded_total {}\n\
         tsink_query_budget_accounting_invariant_violations_total {}\n",
        snapshot.cancellations_total,
        snapshot.deadline_exceeded_total,
        snapshot.accounting_invariant_violations_total,
    );

    body.push_str(
        "# HELP tsink_query_budget_configured_limit Effective finite core query limits; absent kinds are unbounded\n\
         # TYPE tsink_query_budget_configured_limit gauge\n",
    );
    let per_query = snapshot.limits.per_query;
    let wall_time_nanos = per_query
        .max_wall_time
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX));
    for (kind, value) in [
        (
            "max_concurrent_queries",
            snapshot.limits.max_concurrent_queries,
        ),
        (
            "max_shared_memory_bytes",
            snapshot.limits.max_shared_memory_bytes,
        ),
        ("max_series_matched", per_query.max_series_matched),
        ("max_samples_scanned", per_query.max_samples_scanned),
        ("max_samples_returned", per_query.max_samples_returned),
        ("max_returned_bytes", per_query.max_returned_bytes),
        ("max_pattern_expansion", per_query.max_pattern_expansion),
        ("max_steps", per_query.max_steps),
        (
            "max_intermediate_vector_size",
            per_query.max_intermediate_vector_size,
        ),
        ("max_memory_bytes", per_query.max_memory_bytes),
        ("max_wall_time_nanos", wall_time_nanos),
    ] {
        if let Some(value) = value {
            metrics_write!(
                body,
                "tsink_query_budget_configured_limit{{kind=\"{kind}\"}} {value}\n"
            );
        }
    }
}

fn append_local_disk_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: Option<&tsink::LocalDiskMetricsSnapshot>,
) {
    let Some(snapshot) = snapshot else {
        return;
    };

    body.push_str(
        "# HELP tsink_local_disk_accounted_bytes Bytes accounted beneath the shared managed data directory\n\
         # TYPE tsink_local_disk_accounted_bytes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_local_disk_accounted_bytes {}\n",
        snapshot.accounted_bytes
    );
    body.push_str(
        "# HELP tsink_local_disk_reserved_bytes Bytes held by live managed local-disk reservations\n\
         # TYPE tsink_local_disk_reserved_bytes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_local_disk_reserved_bytes {}\n",
        snapshot.reserved_bytes
    );
    body.push_str(
        "# HELP tsink_local_disk_maintenance_reserved_bytes Live reservation bytes held by managed maintenance work\n\
         # TYPE tsink_local_disk_maintenance_reserved_bytes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_local_disk_maintenance_reserved_bytes {}\n",
        snapshot.maintenance_reserved_bytes
    );
    body.push_str(
        "# HELP tsink_local_disk_unknown_bytes Accounted bytes not recognized as tsink-owned files\n\
         # TYPE tsink_local_disk_unknown_bytes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_local_disk_unknown_bytes {}\n",
        snapshot.unknown_bytes
    );
    if let Some(available) = snapshot.filesystem_available_bytes {
        body.push_str(
            "# HELP tsink_local_disk_filesystem_available_bytes Filesystem bytes available to the current user for the managed data directory\n\
             # TYPE tsink_local_disk_filesystem_available_bytes gauge\n",
        );
        metrics_write!(
            body,
            "tsink_local_disk_filesystem_available_bytes {available}\n"
        );
    }
    if let Some(limit) = snapshot.limits.max_bytes {
        body.push_str(
            "# HELP tsink_local_disk_limit_bytes Configured logical byte limit for the shared managed data directory\n\
             # TYPE tsink_local_disk_limit_bytes gauge\n",
        );
        metrics_write!(body, "tsink_local_disk_limit_bytes {limit}\n");
    }
    body.push_str(
        "# HELP tsink_local_disk_filesystem_headroom_bytes Configured filesystem free-space floor for managed local storage\n\
         # TYPE tsink_local_disk_filesystem_headroom_bytes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_local_disk_filesystem_headroom_bytes {}\n",
        snapshot.limits.filesystem_free_headroom_bytes
    );
    body.push_str(
        "# HELP tsink_local_disk_maintenance_reserve_bytes Configured logical bytes reserved for managed maintenance output\n\
         # TYPE tsink_local_disk_maintenance_reserve_bytes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_local_disk_maintenance_reserve_bytes {}\n",
        snapshot.limits.maintenance_temp_reserve_bytes
    );
    body.push_str(
        "# HELP tsink_local_disk_over_limit Whether reconciled managed usage exceeds its logical limit\n\
         # TYPE tsink_local_disk_over_limit gauge\n",
    );
    metrics_write!(
        body,
        "tsink_local_disk_over_limit {}\n",
        u8::from(snapshot.over_limit)
    );
    body.push_str(
        "# HELP tsink_local_disk_active_reservations Live managed local-disk reservations\n\
         # TYPE tsink_local_disk_active_reservations gauge\n",
    );
    metrics_write!(
        body,
        "tsink_local_disk_active_reservations {}\n",
        snapshot.active_reservations
    );
    for (name, help, value) in [
        (
            "tsink_local_disk_rejections_total",
            "Managed local-disk reservations rejected by logical or physical limits",
            snapshot.rejections_total,
        ),
        (
            "tsink_local_disk_reconciliations_total",
            "Successful full scans of the shared managed data directory",
            snapshot.reconciliations_total,
        ),
        (
            "tsink_local_disk_reservation_overruns_total",
            "Managed disk commits whose surviving growth exceeded their reservation",
            snapshot.reservation_overruns_total,
        ),
    ] {
        metrics_write!(
            body,
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
        );
    }
    body.push_str(
        "# HELP tsink_local_disk_category_bytes Accounted bytes beneath the shared managed data directory by category\n\
         # TYPE tsink_local_disk_category_bytes gauge\n",
    );
    for usage in snapshot.categories() {
        metrics_write!(
            body,
            "tsink_local_disk_category_bytes{{category=\"{}\"}} {}\n",
            disk_category_name(usage.category),
            usage.bytes
        );
    }
}

fn append_offline_restore_disk_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: Option<&tsink::LocalDiskMetricsSnapshot>,
) {
    let Some(snapshot) = snapshot else {
        return;
    };

    for (suffix, help, value, metric_type) in [
        (
            "accounted_bytes",
            "Bytes accounted beneath the dedicated offline restore root",
            snapshot.accounted_bytes,
            "gauge",
        ),
        (
            "reserved_bytes",
            "Bytes held by live offline restore reservations",
            snapshot.reserved_bytes,
            "gauge",
        ),
        (
            "maintenance_reserved_bytes",
            "Offline restore reservation bytes classified as maintenance",
            snapshot.maintenance_reserved_bytes,
            "gauge",
        ),
        (
            "unknown_bytes",
            "Bytes beneath the offline restore root not recognized as tsink-owned files",
            snapshot.unknown_bytes,
            "gauge",
        ),
        (
            "filesystem_headroom_bytes",
            "Configured filesystem free-space floor for offline restore work",
            snapshot.limits.filesystem_free_headroom_bytes,
            "gauge",
        ),
        (
            "maintenance_reserve_bytes",
            "Configured logical maintenance reserve beneath the offline restore root",
            snapshot.limits.maintenance_temp_reserve_bytes,
            "gauge",
        ),
        (
            "over_limit",
            "Whether reconciled offline restore usage exceeds its logical limit",
            u64::from(snapshot.over_limit),
            "gauge",
        ),
        (
            "active_reservations",
            "Live offline restore disk reservations",
            snapshot.active_reservations,
            "gauge",
        ),
        (
            "rejections_total",
            "Offline restore reservations rejected by logical or physical limits",
            snapshot.rejections_total,
            "counter",
        ),
        (
            "reconciliations_total",
            "Successful full scans of the offline restore root",
            snapshot.reconciliations_total,
            "counter",
        ),
        (
            "reservation_overruns_total",
            "Offline restore commits whose surviving growth exceeded their reservation",
            snapshot.reservation_overruns_total,
            "counter",
        ),
    ] {
        metrics_write!(
            body,
            "# HELP tsink_offline_restore_disk_{suffix} {help}\n\
             # TYPE tsink_offline_restore_disk_{suffix} {metric_type}\n\
             tsink_offline_restore_disk_{suffix} {value}\n"
        );
    }

    if let Some(available) = snapshot.filesystem_available_bytes {
        body.push_str(
            "# HELP tsink_offline_restore_disk_filesystem_available_bytes Filesystem bytes available to the current user for the offline restore root\n\
             # TYPE tsink_offline_restore_disk_filesystem_available_bytes gauge\n",
        );
        metrics_write!(
            body,
            "tsink_offline_restore_disk_filesystem_available_bytes {available}\n"
        );
    }
    if let Some(limit) = snapshot.limits.max_bytes {
        body.push_str(
            "# HELP tsink_offline_restore_disk_limit_bytes Configured logical byte limit for the offline restore root\n\
             # TYPE tsink_offline_restore_disk_limit_bytes gauge\n",
        );
        metrics_write!(body, "tsink_offline_restore_disk_limit_bytes {limit}\n");
    }
    body.push_str(
        "# HELP tsink_offline_restore_disk_category_bytes Accounted bytes beneath the offline restore root by category\n\
         # TYPE tsink_offline_restore_disk_category_bytes gauge\n",
    );
    for usage in snapshot.categories() {
        metrics_write!(
            body,
            "tsink_offline_restore_disk_category_bytes{{category=\"{}\"}} {}\n",
            disk_category_name(usage.category),
            usage.bytes
        );
    }
}

fn disk_category_name(category: tsink::DiskCategory) -> &'static str {
    match category {
        tsink::DiskCategory::Wal => "wal",
        tsink::DiskCategory::Segments => "segments",
        tsink::DiskCategory::Registry => "registry",
        tsink::DiskCategory::Tombstones => "tombstones",
        tsink::DiskCategory::Rollups => "rollups",
        tsink::DiskCategory::Metadata => "metadata",
        tsink::DiskCategory::Exemplars => "exemplars",
        tsink::DiskCategory::Cluster => "cluster",
        tsink::DiskCategory::EdgeSync => "edge_sync",
        tsink::DiskCategory::ServerState => "server_state",
        tsink::DiskCategory::Temporary => "temporary",
        tsink::DiskCategory::Unknown => "unknown",
        _ => "unknown",
    }
}

fn append_metrics_collection_errors(
    body: &mut dyn MetricsBodyWriter,
    errors: MetricsCollectionErrors,
) {
    body.push_str(
        "# HELP tsink_metrics_collection_errors Number of metrics collectors that failed during this scrape\n\
         # TYPE tsink_metrics_collection_errors gauge\n",
    );
    metrics_write!(body, "tsink_metrics_collection_errors {}\n", errors.len());
    body.push_str(
        "# HELP tsink_metrics_collection_error Whether a metrics collector failed during this scrape\n\
         # TYPE tsink_metrics_collection_error gauge\n",
    );
    for collector in errors.iter() {
        let collector = prometheus_escape_label_value(collector.name());
        metrics_write!(
            body,
            "tsink_metrics_collection_error{{collector=\"{collector}\"}} 1\n"
        );
    }
}

fn append_security_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &crate::security::SecurityMetricsSnapshot,
    service_accounts: Option<&crate::rbac::RbacServiceAccountMetricsSnapshot>,
) {
    if snapshot.targets().next().is_some() {
        body.push_str(
            "# HELP tsink_secret_rotation_generation Current secret rotation generation\n\
             # TYPE tsink_secret_rotation_generation gauge\n\
             # HELP tsink_secret_rotation_reload_total Secret reload operations attempted\n\
             # TYPE tsink_secret_rotation_reload_total counter\n\
             # HELP tsink_secret_rotation_total Secret rotation operations attempted\n\
             # TYPE tsink_secret_rotation_total counter\n\
             # HELP tsink_secret_rotation_failures_total Secret reload or rotation failures\n\
             # TYPE tsink_secret_rotation_failures_total counter\n\
             # HELP tsink_secret_rotation_last_success_unix_ms Last successful secret reload or rotation\n\
             # TYPE tsink_secret_rotation_last_success_unix_ms gauge\n\
             # HELP tsink_secret_rotation_last_failure_unix_ms Last failed secret reload or rotation\n\
             # TYPE tsink_secret_rotation_last_failure_unix_ms gauge\n\
             # HELP tsink_secret_rotation_previous_credential_active Previous credential overlap window active (1 yes, 0 no)\n\
             # TYPE tsink_secret_rotation_previous_credential_active gauge\n",
        );
    }
    for secret in snapshot.targets() {
        let target = secret.target.as_str();
        metrics_write!(
            body,
            "tsink_secret_rotation_generation{{target=\"{target}\"}} {}\n\
             tsink_secret_rotation_reload_total{{target=\"{target}\"}} {}\n\
             tsink_secret_rotation_total{{target=\"{target}\"}} {}\n\
             tsink_secret_rotation_failures_total{{target=\"{target}\"}} {}\n\
             tsink_secret_rotation_last_success_unix_ms{{target=\"{target}\"}} {}\n\
             tsink_secret_rotation_last_failure_unix_ms{{target=\"{target}\"}} {}\n\
             tsink_secret_rotation_previous_credential_active{{target=\"{target}\"}} {}\n",
            secret.generation,
            secret.reloads_total,
            secret.rotations_total,
            secret.failures_total,
            secret.last_success_unix_ms,
            secret.last_failure_unix_ms,
            u8::from(secret.previous_credential_active),
        );
    }

    if let Some(summary) = service_accounts {
        body.push_str(
            "# HELP tsink_rbac_service_accounts_total Configured RBAC service accounts\n\
             # TYPE tsink_rbac_service_accounts_total gauge\n",
        );
        metrics_write!(
            body,
            "tsink_rbac_service_accounts_total {}\n",
            summary.total
        );
        body.push_str(
            "# HELP tsink_rbac_service_accounts_disabled Total disabled RBAC service accounts\n\
             # TYPE tsink_rbac_service_accounts_disabled gauge\n",
        );
        metrics_write!(
            body,
            "tsink_rbac_service_accounts_disabled {}\n",
            summary.disabled
        );
        body.push_str(
            "# HELP tsink_rbac_service_accounts_last_rotated_unix_ms Latest RBAC service-account rotation timestamp\n\
             # TYPE tsink_rbac_service_accounts_last_rotated_unix_ms gauge\n",
        );
        metrics_write!(
            body,
            "tsink_rbac_service_accounts_last_rotated_unix_ms {}\n",
            summary.last_rotated_unix_ms
        );
    }
}

fn append_usage_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &crate::usage::UsageAccountingMetricsSnapshot,
) {
    body.push_str(
        "# HELP tsink_usage_ledger_records_total Cumulative durable or in-memory tenant usage ledger records\n\
         # TYPE tsink_usage_ledger_records_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_usage_ledger_records_total {}\n",
        snapshot.records_total
    );
    body.push_str(
        "# HELP tsink_usage_ledger_retained_records Records retained in memory for bounded raw and bucketed reads\n\
         # TYPE tsink_usage_ledger_retained_records gauge\n",
    );
    metrics_write!(
        body,
        "tsink_usage_ledger_retained_records {}\n",
        snapshot.retained_records
    );
    body.push_str(
        "# HELP tsink_usage_ledger_earliest_retained_sequence Earliest raw sequence available for bounded reads, or zero when empty\n\
         # TYPE tsink_usage_ledger_earliest_retained_sequence gauge\n",
    );
    metrics_write!(
        body,
        "tsink_usage_ledger_earliest_retained_sequence {}\n",
        snapshot.earliest_retained_sequence.unwrap_or(0)
    );
    body.push_str(
        "# HELP tsink_usage_ledger_recent_record_limit Configured in-memory recent-record bound\n\
         # TYPE tsink_usage_ledger_recent_record_limit gauge\n",
    );
    metrics_write!(
        body,
        "tsink_usage_ledger_recent_record_limit {}\n",
        snapshot.recent_record_limit
    );
    body.push_str(
        "# HELP tsink_usage_ledger_tenants_total Distinct tenants observed in the usage ledger\n\
         # TYPE tsink_usage_ledger_tenants_total gauge\n",
    );
    metrics_write!(
        body,
        "tsink_usage_ledger_tenants_total {}\n",
        snapshot.tenant_count
    );
    body.push_str(
        "# HELP tsink_usage_ledger_tenant_limit Configured exact-summary tenant bound\n\
         # TYPE tsink_usage_ledger_tenant_limit gauge\n",
    );
    metrics_write!(
        body,
        "tsink_usage_ledger_tenant_limit {}\n",
        snapshot.tenant_limit
    );
    body.push_str(
        "# HELP tsink_usage_ledger_storage_reconciliations_total Storage reconciliation snapshots recorded in the usage ledger\n\
         # TYPE tsink_usage_ledger_storage_reconciliations_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_usage_ledger_storage_reconciliations_total {}\n",
        snapshot.storage_reconciliations_total
    );
    body.push_str(
        "# HELP tsink_usage_ledger_record_failures_total Usage ledger append attempts that did not complete successfully\n\
         # TYPE tsink_usage_ledger_record_failures_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_usage_ledger_record_failures_total {}\n",
        snapshot.record_failures_total
    );
    body.push_str(
        "# HELP tsink_usage_ledger_durable Whether usage accounting is backed by a durable on-disk ledger\n\
         # TYPE tsink_usage_ledger_durable gauge\n",
    );
    metrics_write!(
        body,
        "tsink_usage_ledger_durable {}\n",
        u8::from(snapshot.durable)
    );
}

fn append_rules_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &rules::RulesExpositionSnapshot,
) {
    body.push_str(
        "# HELP tsink_rules_scheduler_runs_total Rules scheduler ticks attempted on this node\n\
         # TYPE tsink_rules_scheduler_runs_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rules_scheduler_runs_total {}\n",
        snapshot.metrics.scheduler_runs_total
    );
    body.push_str(
        "# HELP tsink_rules_scheduler_skipped_not_leader_total Rules scheduler ticks skipped because this node is not the active cluster leader\n\
         # TYPE tsink_rules_scheduler_skipped_not_leader_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rules_scheduler_skipped_not_leader_total {}\n",
        snapshot.metrics.scheduler_skipped_not_leader_total
    );
    body.push_str(
        "# HELP tsink_rules_scheduler_skipped_inflight_total Rules scheduler ticks skipped because a previous evaluation run was still in flight\n\
         # TYPE tsink_rules_scheduler_skipped_inflight_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rules_scheduler_skipped_inflight_total {}\n",
        snapshot.metrics.scheduler_skipped_inflight_total
    );
    body.push_str(
        "# HELP tsink_rules_evaluated_total Rules evaluated by the built-in rules engine\n\
         # TYPE tsink_rules_evaluated_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rules_evaluated_total {}\n",
        snapshot.metrics.evaluated_rules_total
    );
    body.push_str(
        "# HELP tsink_rules_evaluation_failures_total Rules evaluations that ended in an error\n\
         # TYPE tsink_rules_evaluation_failures_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rules_evaluation_failures_total {}\n",
        snapshot.metrics.evaluation_failures_total
    );
    body.push_str(
        "# HELP tsink_rules_recording_rows_written_total Samples written by recording rules\n\
         # TYPE tsink_rules_recording_rows_written_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rules_recording_rows_written_total {}\n",
        snapshot.metrics.recording_rows_written_total
    );
    body.push_str(
        "# HELP tsink_rules_configured Configured rules-engine inventory and alert state gauges\n\
         # TYPE tsink_rules_configured gauge\n",
    );
    metrics_write!(
        body,
        "tsink_rules_configured{{kind=\"groups\"}} {}\n",
        snapshot.metrics.configured_groups
    );
    metrics_write!(
        body,
        "tsink_rules_configured{{kind=\"rules\"}} {}\n",
        snapshot.metrics.configured_rules
    );
    metrics_write!(
        body,
        "tsink_rules_configured{{kind=\"pending_alerts\"}} {}\n",
        snapshot.metrics.pending_alerts
    );
    metrics_write!(
        body,
        "tsink_rules_configured{{kind=\"firing_alerts\"}} {}\n",
        snapshot.metrics.firing_alerts
    );
    body.push_str(
        "# HELP tsink_rules_scheduler_active Whether this node is currently allowed to execute scheduled rule evaluations (1=yes,0=no)\n\
         # TYPE tsink_rules_scheduler_active gauge\n",
    );
    metrics_write!(
        body,
        "tsink_rules_scheduler_active {}\n",
        u8::from(snapshot.metrics.local_scheduler_active)
    );
    body.push_str(
        "# HELP tsink_rules_runtime_limits Rules runtime configuration guardrails\n\
         # TYPE tsink_rules_runtime_limits gauge\n",
    );
    metrics_write!(
        body,
        "tsink_rules_runtime_limits{{kind=\"scheduler_tick_ms\"}} {}\n",
        snapshot.scheduler_tick_ms
    );
    metrics_write!(
        body,
        "tsink_rules_runtime_limits{{kind=\"max_recording_rows_per_eval\"}} {}\n",
        snapshot.max_recording_rows_per_eval
    );
    metrics_write!(
        body,
        "tsink_rules_runtime_limits{{kind=\"max_alert_instances_per_rule\"}} {}\n",
        snapshot.max_alert_instances_per_rule
    );
    body.push_str(
        "# HELP tsink_rules_store_limits Finite rules sidecar count and byte limits\n\
         # TYPE tsink_rules_store_limits gauge\n",
    );
    for (kind, value) in [
        ("max_groups", snapshot.store_limits.max_groups),
        (
            "max_rules_per_group",
            snapshot.store_limits.max_rules_per_group,
        ),
        ("max_rules_total", snapshot.store_limits.max_rules_total),
        (
            "max_alert_instances_per_rule",
            snapshot.store_limits.max_alert_instances_per_rule,
        ),
        (
            "max_labels_per_set",
            snapshot.store_limits.max_labels_per_set,
        ),
        (
            "max_label_set_bytes",
            snapshot.store_limits.max_label_set_bytes,
        ),
        ("max_name_bytes", snapshot.store_limits.max_name_bytes),
        (
            "max_expression_bytes",
            snapshot.store_limits.max_expression_bytes,
        ),
        (
            "max_annotation_bytes",
            snapshot.store_limits.max_annotation_bytes,
        ),
        (
            "max_total_retained_state_bytes",
            snapshot.store_limits.max_total_retained_state_bytes,
        ),
        (
            "max_durable_file_bytes",
            snapshot.store_limits.max_durable_file_bytes,
        ),
        (
            "max_startup_transient_bytes",
            snapshot.store_limits.max_startup_transient_bytes,
        ),
        (
            "max_replacement_transient_bytes",
            snapshot.store_limits.max_replacement_transient_bytes,
        ),
        (
            "max_runtime_update_transient_bytes",
            snapshot.store_limits.max_runtime_update_transient_bytes,
        ),
        (
            "max_snapshot_status_bytes",
            snapshot.store_limits.max_snapshot_status_bytes,
        ),
    ] {
        metrics_write!(
            body,
            "tsink_rules_store_limits{{kind=\"{kind}\"}} {value}\n"
        );
    }
    body.push_str(
        "# HELP tsink_rules_store_bytes Current modeled rules sidecar bytes\n\
         # TYPE tsink_rules_store_bytes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_rules_store_bytes{{kind=\"retained_state\"}} {}\n",
        snapshot.metrics.retained_state_bytes
    );
    metrics_write!(
        body,
        "tsink_rules_store_bytes{{kind=\"durable_file\"}} {}\n",
        snapshot.metrics.durable_file_bytes
    );
    body.push_str(
        "# HELP tsink_rules_store_peak_bytes Peak modeled rules sidecar bytes by bounded operation\n\
         # TYPE tsink_rules_store_peak_bytes gauge\n",
    );
    for (kind, value) in [
        ("retained_state", snapshot.metrics.peak_retained_state_bytes),
        (
            "startup_transient",
            snapshot.metrics.peak_startup_transient_bytes,
        ),
        (
            "replacement_transient",
            snapshot.metrics.peak_replacement_transient_bytes,
        ),
        (
            "runtime_update_transient",
            snapshot.metrics.peak_runtime_update_transient_bytes,
        ),
        (
            "snapshot_status",
            snapshot.metrics.peak_snapshot_status_bytes,
        ),
        ("snapshot_file", snapshot.metrics.peak_snapshot_file_bytes),
    ] {
        metrics_write!(
            body,
            "tsink_rules_store_peak_bytes{{kind=\"{kind}\"}} {value}\n"
        );
    }
    body.push_str(
        "# HELP tsink_rules_store_rejections_total Rules sidecar limit rejections by bounded operation\n\
         # TYPE tsink_rules_store_rejections_total counter\n",
    );
    for (kind, value) in [
        ("all", snapshot.metrics.limit_rejections_total),
        ("startup", snapshot.metrics.startup_rejections_total),
        ("replacement", snapshot.metrics.replacement_rejections_total),
        (
            "runtime_update",
            snapshot.metrics.runtime_update_rejections_total,
        ),
        ("snapshot", snapshot.metrics.snapshot_rejections_total),
    ] {
        metrics_write!(
            body,
            "tsink_rules_store_rejections_total{{kind=\"{kind}\"}} {value}\n"
        );
    }
    body.push_str(
        "# HELP tsink_rules_store_persistence_failures_total Rules sidecar durable write failures\n\
         # TYPE tsink_rules_store_persistence_failures_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rules_store_persistence_failures_total {}\n",
        snapshot.metrics.persistence_failures_total
    );
}

fn append_rollup_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &tsink::RollupMetricsObservabilitySnapshot,
    query: &tsink::QueryObservabilitySnapshot,
) {
    body.push_str(
        "# HELP tsink_rollup_worker_runs_total Rollup maintenance passes attempted by the background worker or admin trigger\n\
         # TYPE tsink_rollup_worker_runs_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rollup_worker_runs_total {}\n",
        snapshot.worker_runs_total
    );
    body.push_str(
        "# HELP tsink_rollup_worker_success_total Successful rollup maintenance passes\n\
         # TYPE tsink_rollup_worker_success_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rollup_worker_success_total {}\n",
        snapshot.worker_success_total
    );
    body.push_str(
        "# HELP tsink_rollup_worker_errors_total Rollup maintenance passes that ended in an error\n\
         # TYPE tsink_rollup_worker_errors_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rollup_worker_errors_total {}\n",
        snapshot.worker_errors_total
    );
    body.push_str(
        "# HELP tsink_rollup_policy_runs_total Individual rollup policy evaluations attempted\n\
         # TYPE tsink_rollup_policy_runs_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rollup_policy_runs_total {}\n",
        snapshot.policy_runs_total
    );
    body.push_str(
        "# HELP tsink_rollup_buckets_materialized_total Materialized rollup buckets written as persisted artifacts\n\
         # TYPE tsink_rollup_buckets_materialized_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rollup_buckets_materialized_total {}\n",
        snapshot.buckets_materialized_total
    );
    body.push_str(
        "# HELP tsink_rollup_points_materialized_total Materialized rollup points written as persisted artifacts\n\
         # TYPE tsink_rollup_points_materialized_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_rollup_points_materialized_total {}\n",
        snapshot.points_materialized_total
    );
    body.push_str(
        "# HELP tsink_rollup_last_run_duration_nanoseconds Duration of the most recent rollup maintenance pass\n\
         # TYPE tsink_rollup_last_run_duration_nanoseconds gauge\n",
    );
    metrics_write!(
        body,
        "tsink_rollup_last_run_duration_nanoseconds {}\n",
        snapshot.last_run_duration_nanos
    );
    body.push_str(
        "# HELP tsink_rollup_source_traversal_complete Whether the current bounded source traversal reached every active policy\n\
         # TYPE tsink_rollup_source_traversal_complete gauge\n",
    );
    metrics_write!(
        body,
        "tsink_rollup_source_traversal_complete {}\n",
        u8::from(snapshot.source_traversal_complete)
    );
    body.push_str(
        "# HELP tsink_query_rollup_plans_total Queries that used persisted rollup artifacts\n\
         # TYPE tsink_query_rollup_plans_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_query_rollup_plans_total {}\n",
        query.rollup_query_plans_total
    );
    body.push_str(
        "# HELP tsink_query_partial_rollup_plans_total Queries that mixed persisted rollups with raw tail reads\n\
         # TYPE tsink_query_partial_rollup_plans_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_query_partial_rollup_plans_total {}\n",
        query.partial_rollup_query_plans_total
    );
    body.push_str(
        "# HELP tsink_query_rollup_points_read_total Persisted rollup points read by query planning\n\
         # TYPE tsink_query_rollup_points_read_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_query_rollup_points_read_total {}\n",
        query.rollup_points_read_total
    );
    body.push_str(
        "# HELP tsink_rollup_policy_status Rollup policy coverage, freshness, and runtime state\n\
         # TYPE tsink_rollup_policy_status gauge\n",
    );
    for policy in &snapshot.policies {
        let policy_id = prometheus_escape_label_value(snapshot.policy_id(policy));
        let metric = prometheus_escape_label_value(snapshot.metric(policy));
        let aggregation = policy.aggregation;
        metrics_write!(body,
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{aggregation:?}\",kind=\"matched_series\"}} {}\n",
            policy_id,
            metric,
            policy.matched_series
        );
        metrics_write!(body,
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{aggregation:?}\",kind=\"materialized_series\"}} {}\n",
            policy_id,
            metric,
            policy.materialized_series
        );
        metrics_write!(body,
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{aggregation:?}\",kind=\"source_traversal_complete\"}} {}\n",
            policy_id,
            metric,
            u8::from(policy.source_traversal_complete)
        );
        metrics_write!(body,
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{aggregation:?}\",kind=\"interval\"}} {}\n",
            policy_id,
            metric,
            policy.interval
        );
        metrics_write!(body,
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{aggregation:?}\",kind=\"materialized_through\"}} {}\n",
            policy_id,
            metric,
            policy.materialized_through.unwrap_or(0)
        );
        metrics_write!(body,
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{aggregation:?}\",kind=\"lag\"}} {}\n",
            policy_id,
            metric,
            policy.lag.unwrap_or(0)
        );
        metrics_write!(body,
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{aggregation:?}\",kind=\"last_run_duration_nanos\"}} {}\n",
            policy_id,
            metric,
            policy.last_run_duration_nanos
        );
        metrics_write!(body,
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{aggregation:?}\",kind=\"last_run_started_at_ms\"}} {}\n",
            policy_id,
            metric,
            policy.last_run_started_at_ms.unwrap_or(0)
        );
        metrics_write!(body,
            "tsink_rollup_policy_status{{policy=\"{}\",metric=\"{}\",aggregation=\"{aggregation:?}\",kind=\"last_run_completed_at_ms\"}} {}\n",
            policy_id,
            metric,
            policy.last_run_completed_at_ms.unwrap_or(0)
        );
    }
}

fn append_metric_metadata_store_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &MetricMetadataStoreMetricsSnapshot,
) {
    body.push_str(
        "# HELP tsink_metric_metadata_store_entries Metric-family metadata records retained by the sidecar store\n\
         # TYPE tsink_metric_metadata_store_entries gauge\n",
    );
    metrics_write!(
        body,
        "tsink_metric_metadata_store_entries {}\n",
        snapshot.entries
    );
    body.push_str(
        "# HELP tsink_metric_metadata_store_memory_bytes Modeled current and peak heap bytes owned by metric-metadata operations\n\
         # TYPE tsink_metric_metadata_store_memory_bytes gauge\n",
    );
    for (kind, bytes) in [
        ("retained", snapshot.retained_bytes),
        ("peak_retained", snapshot.peak_retained_bytes),
        ("transient", snapshot.transient_bytes),
        ("peak_transient", snapshot.peak_transient_bytes),
        ("query_result", snapshot.query_result_bytes),
        ("peak_query_result", snapshot.peak_query_result_bytes),
    ] {
        metrics_write!(
            body,
            "tsink_metric_metadata_store_memory_bytes{{kind=\"{kind}\"}} {bytes}\n"
        );
    }
    body.push_str(
        "# HELP tsink_metric_metadata_store_durable_file_bytes Bytes in the current durable metric-metadata sidecar file\n\
         # TYPE tsink_metric_metadata_store_durable_file_bytes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_metric_metadata_store_durable_file_bytes {}\n",
        snapshot.durable_file_bytes
    );
    body.push_str(
        "# HELP tsink_metric_metadata_store_rejections_total Metric-metadata operations rejected by bounded-resource category\n\
         # TYPE tsink_metric_metadata_store_rejections_total counter\n",
    );
    for (reason, count) in [
        ("all", snapshot.rejections_total),
        ("entry", snapshot.entry_rejections_total),
        ("record", snapshot.record_rejections_total),
        ("update_batch", snapshot.update_batch_rejections_total),
        ("retained", snapshot.retained_rejections_total),
        ("durable_file", snapshot.durable_file_rejections_total),
        ("transient", snapshot.transient_rejections_total),
        ("query", snapshot.query_rejections_total),
        ("persistence", snapshot.persistence_rejections_total),
    ] {
        metrics_write!(
            body,
            "tsink_metric_metadata_store_rejections_total{{reason=\"{reason}\"}} {count}\n"
        );
    }
    body.push_str(
        "# HELP tsink_metric_metadata_store_limit Configured hard limits for the metric-metadata sidecar\n\
         # TYPE tsink_metric_metadata_store_limit gauge\n",
    );
    for (kind, limit) in [
        ("entries", snapshot.limits.max_entries),
        ("record_bytes", snapshot.limits.max_record_bytes),
        (
            "update_batch_entries",
            snapshot.limits.max_update_batch_entries,
        ),
        ("update_batch_bytes", snapshot.limits.max_update_batch_bytes),
        ("retained_bytes", snapshot.limits.max_retained_bytes),
        ("durable_file_bytes", snapshot.limits.max_durable_file_bytes),
        (
            "startup_transient_bytes",
            snapshot.limits.max_startup_transient_bytes,
        ),
        (
            "write_transient_bytes",
            snapshot.limits.max_write_transient_bytes,
        ),
        ("query_records", snapshot.limits.max_query_records),
        ("query_result_bytes", snapshot.limits.max_query_result_bytes),
    ] {
        metrics_write!(
            body,
            "tsink_metric_metadata_store_limit{{kind=\"{kind}\"}} {limit}\n"
        );
    }
}

fn append_exemplar_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &ExemplarStoreMetricsSnapshot,
    config: ExemplarStoreConfig,
    resource_limits: crate::exemplar_store::ExemplarStoreResourceLimits,
) {
    body.push_str(
        "# HELP tsink_exemplars_accepted_total Exemplars accepted into the bounded exemplar store\n\
         # TYPE tsink_exemplars_accepted_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_exemplars_accepted_total {}\n",
        snapshot.accepted_total
    );
    body.push_str(
        "# HELP tsink_exemplars_rejected_total Exemplars rejected before storage due to unsupported payloads or configured quotas\n\
         # TYPE tsink_exemplars_rejected_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_exemplars_rejected_total {}\n",
        snapshot.rejected_total
    );
    body.push_str(
        "# HELP tsink_exemplars_dropped_total Exemplars dropped from the bounded exemplar store due to retention guardrails\n\
         # TYPE tsink_exemplars_dropped_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_exemplars_dropped_total {}\n",
        snapshot.dropped_total
    );
    body.push_str(
        "# HELP tsink_exemplars_query_requests_total Exemplar query requests served by the exemplar store\n\
         # TYPE tsink_exemplars_query_requests_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_exemplars_query_requests_total {}\n",
        snapshot.query_requests_total
    );
    body.push_str(
        "# HELP tsink_exemplars_query_series_total Exemplar series returned by exemplar queries\n\
         # TYPE tsink_exemplars_query_series_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_exemplars_query_series_total {}\n",
        snapshot.query_series_total
    );
    body.push_str(
        "# HELP tsink_exemplars_query_results_total Exemplars returned by exemplar queries\n\
         # TYPE tsink_exemplars_query_results_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_exemplars_query_results_total {}\n",
        snapshot.query_exemplars_total
    );
    body.push_str(
        "# HELP tsink_exemplars_stored Series currently present in the exemplar sidecar store\n\
         # TYPE tsink_exemplars_stored gauge\n",
    );
    metrics_write!(
        body,
        "tsink_exemplars_stored{{kind=\"series\"}} {}\n",
        snapshot.stored_series
    );
    metrics_write!(
        body,
        "tsink_exemplars_stored{{kind=\"exemplars\"}} {}\n",
        snapshot.stored_exemplars
    );
    body.push_str(
        "# HELP tsink_exemplar_store_memory_bytes Current and peak modeled bytes owned by exemplar-store state and operations\n\
         # TYPE tsink_exemplar_store_memory_bytes gauge\n",
    );
    for (kind, bytes) in [
        ("retained", snapshot.retained_bytes),
        ("peak_retained", snapshot.peak_retained_bytes),
        ("transient", snapshot.transient_bytes),
        ("peak_transient", snapshot.peak_transient_bytes),
    ] {
        metrics_write!(
            body,
            "tsink_exemplar_store_memory_bytes{{kind=\"{kind}\"}} {bytes}\n"
        );
    }
    body.push_str(
        "# HELP tsink_exemplar_store_durable_file_bytes Current and peak bytes in the durable exemplar sidecar file\n\
         # TYPE tsink_exemplar_store_durable_file_bytes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_exemplar_store_durable_file_bytes{{kind=\"current\"}} {}\n",
        snapshot.durable_file_bytes
    );
    metrics_write!(
        body,
        "tsink_exemplar_store_durable_file_bytes{{kind=\"peak\"}} {}\n",
        snapshot.peak_durable_file_bytes
    );
    body.push_str(
        "# HELP tsink_exemplar_store_resource_rejections_total Exemplar-store operations rejected by bounded-resource category\n\
         # TYPE tsink_exemplar_store_resource_rejections_total counter\n",
    );
    for (reason, count) in [
        ("all", snapshot.resource_rejections_total),
        ("shape", snapshot.shape_rejections_total),
        ("batch", snapshot.batch_rejections_total),
        ("retained", snapshot.retained_rejections_total),
        ("transient", snapshot.transient_rejections_total),
        ("durable", snapshot.durable_rejections_total),
        ("startup", snapshot.startup_rejections_total),
        ("snapshot", snapshot.snapshot_rejections_total),
    ] {
        metrics_write!(
            body,
            "tsink_exemplar_store_resource_rejections_total{{reason=\"{reason}\"}} {count}\n"
        );
    }
    body.push_str(
        "# HELP tsink_exemplar_store_last_rejection Last stable exemplar-store rejection code observed by this live store\n\
         # TYPE tsink_exemplar_store_last_rejection gauge\n",
    );
    metrics_write!(
        body,
        "tsink_exemplar_store_last_rejection{{code=\"{}\"}} 1\n",
        snapshot.last_rejection_code.map_or(
            "none",
            crate::exemplar_store::ExemplarStoreErrorCode::as_str
        )
    );
    body.push_str(
        "# HELP tsink_exemplar_limits Configured exemplar request, query, and storage guardrails\n\
         # TYPE tsink_exemplar_limits gauge\n",
    );
    metrics_write!(
        body,
        "tsink_exemplar_limits{{kind=\"max_total\"}} {}\n",
        config.max_total_exemplars
    );
    metrics_write!(
        body,
        "tsink_exemplar_limits{{kind=\"max_per_series\"}} {}\n",
        config.max_exemplars_per_series
    );
    metrics_write!(
        body,
        "tsink_exemplar_limits{{kind=\"max_per_request\"}} {}\n",
        config.max_exemplars_per_request
    );
    metrics_write!(
        body,
        "tsink_exemplar_limits{{kind=\"max_query_results\"}} {}\n",
        config.max_query_results
    );
    metrics_write!(
        body,
        "tsink_exemplar_limits{{kind=\"max_query_selectors\"}} {}\n",
        config.max_query_selectors
    );
    for (kind, limit) in [
        (
            "max_total_series",
            u64::try_from(resource_limits.max_total_series).unwrap_or(u64::MAX),
        ),
        (
            "max_metric_name_bytes",
            u64::try_from(resource_limits.max_metric_name_bytes).unwrap_or(u64::MAX),
        ),
        (
            "max_labels_per_series",
            u64::try_from(resource_limits.max_labels_per_series).unwrap_or(u64::MAX),
        ),
        (
            "max_labels_per_exemplar",
            u64::try_from(resource_limits.max_labels_per_exemplar).unwrap_or(u64::MAX),
        ),
        (
            "max_label_name_bytes",
            u64::try_from(resource_limits.max_label_name_bytes).unwrap_or(u64::MAX),
        ),
        (
            "max_label_value_bytes",
            u64::try_from(resource_limits.max_label_value_bytes).unwrap_or(u64::MAX),
        ),
        (
            "max_series_identity_bytes",
            u64::try_from(resource_limits.max_series_identity_bytes).unwrap_or(u64::MAX),
        ),
        (
            "max_exemplar_label_bytes",
            u64::try_from(resource_limits.max_exemplar_label_bytes).unwrap_or(u64::MAX),
        ),
        (
            "max_retained_bytes",
            resource_limits.max_total_retained_bytes,
        ),
        (
            "max_update_batch_bytes",
            resource_limits.max_update_batch_bytes,
        ),
        (
            "max_write_transient_bytes",
            resource_limits.max_write_transient_bytes,
        ),
        (
            "max_replacement_peak_bytes",
            resource_limits.max_replacement_peak_bytes,
        ),
        (
            "max_persistence_serialization_bytes",
            resource_limits.max_persistence_serialization_bytes,
        ),
        (
            "max_durable_file_bytes",
            resource_limits.max_durable_file_bytes,
        ),
        (
            "max_startup_transient_bytes",
            resource_limits.max_startup_transient_bytes,
        ),
        ("max_snapshot_bytes", resource_limits.max_snapshot_bytes),
        (
            "max_snapshot_transient_bytes",
            resource_limits.max_snapshot_transient_bytes,
        ),
        (
            "max_concurrent_transient_bytes",
            resource_limits.max_concurrent_transient_bytes,
        ),
    ] {
        metrics_write!(body, "tsink_exemplar_limits{{kind=\"{kind}\"}} {limit}\n");
    }
}

fn append_prometheus_payload_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &PrometheusPayloadStatusSnapshot,
) {
    body.push_str(
        "# HELP tsink_prometheus_payload_feature_enabled Expanded Prometheus payload feature flags (1 enabled, 0 disabled)\n\
         # TYPE tsink_prometheus_payload_feature_enabled gauge\n",
    );
    for (payload, enabled) in [
        ("metadata", snapshot.metadata.enabled),
        ("exemplar", snapshot.exemplars.enabled),
        ("histogram", snapshot.histograms.enabled),
    ] {
        metrics_write!(
            body,
            "tsink_prometheus_payload_feature_enabled{{payload=\"{payload}\"}} {}\n",
            u8::from(enabled)
        );
    }

    body.push_str(
        "# HELP tsink_prometheus_payload_accepted_total Expanded Prometheus payload items accepted by this node\n\
         # TYPE tsink_prometheus_payload_accepted_total counter\n",
    );
    for (payload, counters) in [
        ("metadata", &snapshot.metadata),
        ("exemplar", &snapshot.exemplars),
        ("histogram", &snapshot.histograms),
    ] {
        metrics_write!(
            body,
            "tsink_prometheus_payload_accepted_total{{payload=\"{payload}\"}} {}\n",
            counters.accepted_total
        );
    }

    body.push_str(
        "# HELP tsink_prometheus_payload_rejected_total Expanded Prometheus payload items rejected by this node\n\
         # TYPE tsink_prometheus_payload_rejected_total counter\n",
    );
    for (payload, counters) in [
        ("metadata", &snapshot.metadata),
        ("exemplar", &snapshot.exemplars),
        ("histogram", &snapshot.histograms),
    ] {
        metrics_write!(
            body,
            "tsink_prometheus_payload_rejected_total{{payload=\"{payload}\"}} {}\n",
            counters.rejected_total
        );
    }

    body.push_str(
        "# HELP tsink_prometheus_payload_throttled_total Expanded Prometheus payload items throttled by request guardrails\n\
         # TYPE tsink_prometheus_payload_throttled_total counter\n",
    );
    for (payload, counters) in [
        ("metadata", &snapshot.metadata),
        ("exemplar", &snapshot.exemplars),
        ("histogram", &snapshot.histograms),
    ] {
        metrics_write!(
            body,
            "tsink_prometheus_payload_throttled_total{{payload=\"{payload}\"}} {}\n",
            counters.throttled_total
        );
    }

    body.push_str(
        "# HELP tsink_prometheus_payload_capability_required Required compatibility capability markers for expanded Prometheus payloads\n\
         # TYPE tsink_prometheus_payload_capability_required gauge\n",
    );
    for (payload, capabilities) in [
        ("metadata", &snapshot.metadata.required_capabilities),
        ("exemplar", &snapshot.exemplars.required_capabilities),
        ("histogram", &snapshot.histograms.required_capabilities),
    ] {
        for capability in capabilities {
            let capability = prometheus_escape_label_value(capability);
            metrics_write!(body,
                "tsink_prometheus_payload_capability_required{{payload=\"{payload}\",capability=\"{capability}\"}} 1\n"
            );
        }
    }

    body.push_str(
        "# HELP tsink_cluster_capability_enabled Local cluster capability markers exported by this node\n\
         # TYPE tsink_cluster_capability_enabled gauge\n",
    );
    for capability in &snapshot.local_capabilities {
        let capability = prometheus_escape_label_value(capability);
        metrics_write!(
            body,
            "tsink_cluster_capability_enabled{{capability=\"{capability}\"}} 1\n"
        );
    }

    body.push_str(
        "# HELP tsink_prometheus_payload_limits Expanded Prometheus payload request guardrails\n\
         # TYPE tsink_prometheus_payload_limits gauge\n",
    );
    if let Some(limit) = snapshot.metadata.max_per_request {
        metrics_write!(
            body,
            "tsink_prometheus_payload_limits{{payload=\"metadata\",kind=\"max_per_request\"}} {}\n",
            limit
        );
    }
    if let Some(limit) = snapshot.histograms.max_bucket_entries_per_request {
        metrics_write!(body,
            "tsink_prometheus_payload_limits{{payload=\"histogram\",kind=\"max_bucket_entries_per_request\"}} {}\n",
            limit
        );
    }
}

fn append_otlp_metrics(body: &mut dyn MetricsBodyWriter, snapshot: &OtlpMetricsStatusSnapshot) {
    body.push_str(
        "# HELP tsink_otlp_metrics_enabled OTLP metrics ingest feature flag (1 enabled, 0 disabled)\n\
         # TYPE tsink_otlp_metrics_enabled gauge\n",
    );
    metrics_write!(
        body,
        "tsink_otlp_metrics_enabled {}\n",
        u8::from(snapshot.enabled)
    );

    body.push_str(
        "# HELP tsink_otlp_requests_total OTLP /v1/metrics requests accepted or rejected by this node\n\
         # TYPE tsink_otlp_requests_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_otlp_requests_total{{outcome=\"accepted\"}} {}\n",
        snapshot.accepted_requests_total
    );
    metrics_write!(
        body,
        "tsink_otlp_requests_total{{outcome=\"rejected\"}} {}\n",
        snapshot.rejected_requests_total
    );

    body.push_str(
        "# HELP tsink_otlp_data_points_total OTLP data points accepted or rejected by metric kind\n\
         # TYPE tsink_otlp_data_points_total counter\n",
    );
    for (kind, counters) in [
        ("gauge", snapshot.gauges),
        ("sum", snapshot.sums),
        ("histogram", snapshot.histograms),
        ("summary", snapshot.summaries),
        ("exponential_histogram", snapshot.exponential_histograms),
    ] {
        metrics_write!(
            body,
            "tsink_otlp_data_points_total{{kind=\"{kind}\",outcome=\"accepted\"}} {}\n",
            counters.accepted_total
        );
        metrics_write!(
            body,
            "tsink_otlp_data_points_total{{kind=\"{kind}\",outcome=\"rejected\"}} {}\n",
            counters.rejected_total
        );
    }

    body.push_str(
        "# HELP tsink_otlp_exemplars_total OTLP exemplars accepted or rejected by this node\n\
         # TYPE tsink_otlp_exemplars_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_otlp_exemplars_total{{outcome=\"accepted\"}} {}\n",
        snapshot.accepted_exemplars_total
    );
    metrics_write!(
        body,
        "tsink_otlp_exemplars_total{{outcome=\"rejected\"}} {}\n",
        snapshot.rejected_exemplars_total
    );

    body.push_str(
        "# HELP tsink_otlp_supported_shape OTLP metric shapes supported by /v1/metrics\n\
         # TYPE tsink_otlp_supported_shape gauge\n",
    );
    for shape in &snapshot.supported_shapes {
        let shape = prometheus_escape_label_value(shape);
        metrics_write!(body, "tsink_otlp_supported_shape{{shape=\"{shape}\"}} 1\n");
    }
}

fn append_legacy_ingest_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &LegacyIngestStatusSnapshot,
) {
    body.push_str(
        "# HELP tsink_legacy_ingest_enabled Legacy protocol adapter availability on this node (1 enabled, 0 disabled)\n\
         # TYPE tsink_legacy_ingest_enabled gauge\n",
    );
    for (adapter, enabled) in [
        ("influx_line_protocol", snapshot.influx.enabled),
        ("statsd", snapshot.statsd.enabled),
        ("graphite", snapshot.graphite.enabled),
    ] {
        metrics_write!(
            body,
            "tsink_legacy_ingest_enabled{{adapter=\"{adapter}\"}} {}\n",
            u8::from(enabled)
        );
    }

    body.push_str(
        "# HELP tsink_legacy_ingest_requests_total Legacy protocol adapter requests accepted, rejected, or throttled by this node\n\
         # TYPE tsink_legacy_ingest_requests_total counter\n",
    );
    append_legacy_request_metrics(body, "influx_line_protocol", snapshot.influx.counters);
    append_legacy_request_metrics(body, "statsd", snapshot.statsd.counters);
    append_legacy_request_metrics(body, "graphite", snapshot.graphite.counters);

    body.push_str(
        "# HELP tsink_legacy_ingest_samples_total Legacy protocol adapter samples accepted or rejected by this node\n\
         # TYPE tsink_legacy_ingest_samples_total counter\n",
    );
    append_legacy_sample_metrics(body, "influx_line_protocol", snapshot.influx.counters);
    append_legacy_sample_metrics(body, "statsd", snapshot.statsd.counters);
    append_legacy_sample_metrics(body, "graphite", snapshot.graphite.counters);

    body.push_str(
        "# HELP tsink_legacy_ingest_write_acknowledgements_total Legacy adapter storage attempts by bounded acknowledgement level\n\
         # TYPE tsink_legacy_ingest_write_acknowledgements_total counter\n",
    );
    body.push_str(
        "# HELP tsink_legacy_ingest_write_outcomes_total Legacy adapter storage attempts by complete, rejected, partial, or indeterminate outcome\n\
         # TYPE tsink_legacy_ingest_write_outcomes_total counter\n",
    );
    body.push_str(
        "# HELP tsink_legacy_ingest_write_errors_total Legacy adapter storage failures by bounded structured reason\n\
         # TYPE tsink_legacy_ingest_write_errors_total counter\n",
    );
    body.push_str(
        "# HELP tsink_legacy_ingest_sidecar_items_total Legacy adapter metadata and exemplar items accepted or applied during storage attempts\n\
         # TYPE tsink_legacy_ingest_sidecar_items_total counter\n",
    );
    append_legacy_write_observability(
        body,
        "influx_line_protocol",
        &snapshot.influx.write_observability,
    );
    append_legacy_write_observability(body, "statsd", &snapshot.statsd.write_observability);
    append_legacy_write_observability(body, "graphite", &snapshot.graphite.write_observability);

    body.push_str(
        "# HELP tsink_legacy_ingest_limits Legacy protocol adapter request and listener guardrails\n\
         # TYPE tsink_legacy_ingest_limits gauge\n",
    );
    metrics_write!(body,
        "tsink_legacy_ingest_limits{{adapter=\"influx_line_protocol\",kind=\"max_lines_per_request\"}} {}\n",
        snapshot.influx.max_lines_per_request
    );
    metrics_write!(
        body,
        "tsink_legacy_ingest_limits{{adapter=\"statsd\",kind=\"max_packet_bytes\"}} {}\n",
        snapshot.statsd.max_packet_bytes
    );
    metrics_write!(
        body,
        "tsink_legacy_ingest_limits{{adapter=\"statsd\",kind=\"max_events_per_packet\"}} {}\n",
        snapshot.statsd.max_events_per_packet
    );
    metrics_write!(
        body,
        "tsink_legacy_ingest_limits{{adapter=\"graphite\",kind=\"max_line_bytes\"}} {}\n",
        snapshot.graphite.max_line_bytes
    );
}

fn append_edge_sync_metrics(
    body: &mut dyn MetricsBodyWriter,
    source: &edge_sync::EdgeSyncSourceMetricsSnapshot,
    accept: &edge_sync::EdgeSyncAcceptMetricsSnapshot,
) {
    body.push_str(
        "# HELP tsink_edge_sync_enabled Edge sync source and accept mode enablement on this node\n\
         # TYPE tsink_edge_sync_enabled gauge\n",
    );
    metrics_write!(
        body,
        "tsink_edge_sync_enabled{{role=\"source\"}} {}\n",
        u8::from(source.enabled)
    );
    metrics_write!(
        body,
        "tsink_edge_sync_enabled{{role=\"accept\"}} {}\n",
        u8::from(accept.enabled)
    );

    body.push_str(
        "# HELP tsink_edge_sync_queue Gauge view of the local edge sync backlog and retention window\n\
         # TYPE tsink_edge_sync_queue gauge\n",
    );
    metrics_write!(
        body,
        "tsink_edge_sync_queue{{kind=\"queued_entries\"}} {}\n",
        source.queued_entries
    );
    metrics_write!(
        body,
        "tsink_edge_sync_queue{{kind=\"queued_bytes\"}} {}\n",
        source.queued_bytes
    );
    metrics_write!(
        body,
        "tsink_edge_sync_queue{{kind=\"log_bytes\"}} {}\n",
        source.log_bytes
    );
    metrics_write!(
        body,
        "tsink_edge_sync_queue{{kind=\"oldest_queued_age_ms\"}} {}\n",
        source.oldest_queued_age_ms.unwrap_or(0)
    );
    metrics_write!(
        body,
        "tsink_edge_sync_queue{{kind=\"pre_ack_retention_secs\"}} {}\n",
        source.pre_ack_retention_secs
    );

    body.push_str(
        "# HELP tsink_edge_sync_queue_health Durable edge sync queue health flags\n\
         # TYPE tsink_edge_sync_queue_health gauge\n",
    );
    for (state, active) in [
        ("persistence_fenced", source.persistence_fenced),
        ("cleanup_pending", source.cleanup_pending),
        ("degraded", source.degraded),
    ] {
        metrics_write!(
            body,
            "tsink_edge_sync_queue_health{{state=\"{state}\"}} {}\n",
            u8::from(active)
        );
    }

    body.push_str(
        "# HELP tsink_edge_sync_events_total Edge sync enqueue, replay, and retention-drop counters\n\
         # TYPE tsink_edge_sync_events_total counter\n",
    );
    for (event, value) in [
        ("enqueued", source.enqueued_total),
        ("enqueue_rejected", source.enqueue_rejected_total),
        ("replay_attempt", source.replay_attempts_total),
        ("replay_success", source.replay_success_total),
        ("replay_failure", source.replay_failures_total),
        ("expired_entry", source.expired_entries_total),
    ] {
        metrics_write!(
            body,
            "tsink_edge_sync_events_total{{event=\"{event}\"}} {value}\n"
        );
    }

    body.push_str(
        "# HELP tsink_edge_sync_replayed_rows_total Rows successfully replayed upstream by edge sync\n\
         # TYPE tsink_edge_sync_replayed_rows_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_edge_sync_replayed_rows_total {}\n",
        source.replayed_rows_total
    );

    body.push_str(
        "# HELP tsink_edge_sync_accept_dedupe Edge sync accept-side idempotency window gauges and counters\n\
         # TYPE tsink_edge_sync_accept_dedupe gauge\n",
    );
    for (kind, value) in [
        ("window_secs", accept.dedupe_window_secs),
        ("max_entries", accept.max_entries as u64),
        ("max_log_bytes", accept.max_log_bytes),
        ("cleanup_interval_secs", accept.cleanup_interval_secs),
        ("active_keys", accept.active_keys),
        ("inflight_keys", accept.inflight_keys),
        ("log_bytes", accept.log_bytes),
    ] {
        metrics_write!(
            body,
            "tsink_edge_sync_accept_dedupe{{kind=\"{kind}\"}} {value}\n"
        );
    }
}

fn append_cluster_audit_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &crate::cluster::audit::ClusterAuditMetricsSnapshot,
) {
    body.push_str(
        "# HELP tsink_cluster_audit_log Durable cluster audit log gauges\n\
         # TYPE tsink_cluster_audit_log gauge\n",
    );
    for (kind, value) in [
        ("enabled", u64::from(snapshot.enabled)),
        ("retained_entries", snapshot.retained_entries),
        ("log_bytes", snapshot.log_bytes),
    ] {
        metrics_write!(body, "tsink_cluster_audit_log{{kind=\"{kind}\"}} {value}\n");
    }
    body.push_str(
        "# HELP tsink_cluster_audit_health Durable cluster audit persistence health flags\n\
         # TYPE tsink_cluster_audit_health gauge\n",
    );
    for (state, active) in [
        ("persistence_fenced", snapshot.persistence_fenced),
        ("cleanup_pending", snapshot.cleanup_pending),
        ("degraded", snapshot.degraded),
    ] {
        metrics_write!(
            body,
            "tsink_cluster_audit_health{{state=\"{state}\"}} {}\n",
            u8::from(active)
        );
    }
}

fn append_cluster_control_persistence_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: &ControlPersistenceStatus,
) {
    let checkpoint_pending = snapshot.pending_checkpoint.is_some();
    body.push_str(
        "# HELP tsink_cluster_control_persistence_health Durable control-log and checkpoint persistence health flags\n\
         # TYPE tsink_cluster_control_persistence_health gauge\n",
    );
    for (state, active) in [
        ("fenced", snapshot.fenced),
        ("checkpoint_pending", checkpoint_pending),
        ("cleanup_debt", snapshot.cleanup_debt),
        (
            "degraded",
            snapshot.fenced || checkpoint_pending || snapshot.cleanup_debt,
        ),
    ] {
        metrics_write!(
            body,
            "tsink_cluster_control_persistence_health{{state=\"{state}\"}} {}\n",
            u8::from(active)
        );
    }
    let pending = snapshot
        .pending_checkpoint
        .unwrap_or(crate::cluster::consensus::ControlCommitPosition { index: 0, term: 0 });
    body.push_str(
        "# HELP tsink_cluster_control_persistence_pending_checkpoint_index Durable control-log index whose state mirror is pending repair, or zero when none is pending\n\
         # TYPE tsink_cluster_control_persistence_pending_checkpoint_index gauge\n",
    );
    metrics_write!(
        body,
        "tsink_cluster_control_persistence_pending_checkpoint_index {}\n",
        pending.index
    );
    body.push_str(
        "# HELP tsink_cluster_control_persistence_pending_checkpoint_term Durable control-log term whose state mirror is pending repair, or zero when none is pending\n\
         # TYPE tsink_cluster_control_persistence_pending_checkpoint_term gauge\n",
    );
    metrics_write!(
        body,
        "tsink_cluster_control_persistence_pending_checkpoint_term {}\n",
        pending.term
    );
}

fn append_legacy_request_metrics(
    body: &mut dyn MetricsBodyWriter,
    adapter: &str,
    counters: AdapterCounterSnapshot,
) {
    metrics_write!(
        body,
        "tsink_legacy_ingest_requests_total{{adapter=\"{adapter}\",outcome=\"accepted\"}} {}\n",
        counters.accepted_requests_total
    );
    metrics_write!(
        body,
        "tsink_legacy_ingest_requests_total{{adapter=\"{adapter}\",outcome=\"rejected\"}} {}\n",
        counters.rejected_requests_total
    );
    metrics_write!(
        body,
        "tsink_legacy_ingest_requests_total{{adapter=\"{adapter}\",outcome=\"throttled\"}} {}\n",
        counters.throttled_requests_total
    );
}

fn append_legacy_sample_metrics(
    body: &mut dyn MetricsBodyWriter,
    adapter: &str,
    counters: AdapterCounterSnapshot,
) {
    metrics_write!(
        body,
        "tsink_legacy_ingest_samples_total{{adapter=\"{adapter}\",outcome=\"accepted\"}} {}\n",
        counters.accepted_samples_total
    );
    metrics_write!(
        body,
        "tsink_legacy_ingest_samples_total{{adapter=\"{adapter}\",outcome=\"rejected\"}} {}\n",
        counters.rejected_samples_total
    );
}

fn append_legacy_write_observability(
    body: &mut dyn MetricsBodyWriter,
    adapter: &str,
    snapshot: &legacy_ingest::AdapterWriteObservabilitySnapshot,
) {
    for (index, level) in ["none", "volatile", "appended", "durable"]
        .iter()
        .enumerate()
    {
        metrics_write!(body,
            "tsink_legacy_ingest_write_acknowledgements_total{{adapter=\"{adapter}\",level=\"{level}\"}} {}\n",
            snapshot.acknowledgements_total[index]
        );
    }
    for (index, outcome) in ["complete", "rejected", "partial", "indeterminate"]
        .iter()
        .enumerate()
    {
        metrics_write!(body,
            "tsink_legacy_ingest_write_outcomes_total{{adapter=\"{adapter}\",outcome=\"{outcome}\"}} {}\n",
            snapshot.outcomes_total[index]
        );
    }
    for (index, reason) in legacy_ingest::LEGACY_WRITE_ERROR_REASON_NAMES
        .iter()
        .enumerate()
    {
        metrics_write!(body,
            "tsink_legacy_ingest_write_errors_total{{adapter=\"{adapter}\",reason=\"{reason}\"}} {}\n",
            snapshot.error_reasons_total[index]
        );
    }
    for (kind, value) in [
        (
            "metadata_accepted",
            snapshot.accepted_metadata_updates_total,
        ),
        ("metadata_applied", snapshot.applied_metadata_updates_total),
        ("exemplars_accepted", snapshot.accepted_exemplars_total),
    ] {
        metrics_write!(body,
            "tsink_legacy_ingest_sidecar_items_total{{adapter=\"{adapter}\",kind=\"{kind}\"}} {value}\n"
        );
    }
}

fn append_write_admission_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: WriteAdmissionMetricsSnapshot,
) {
    body.push_str(
        "# HELP tsink_write_admission_rejections_total Public write admission rejections across request-slot, row-budget, and oversize guardrails\n\
         # TYPE tsink_write_admission_rejections_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_write_admission_rejections_total {}\n",
        snapshot.rejections_total
    );
    body.push_str(
        "# HELP tsink_write_admission_request_slot_rejections_total Public write request-slot admission rejections due to global concurrency saturation\n\
         # TYPE tsink_write_admission_request_slot_rejections_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_write_admission_request_slot_rejections_total {}\n",
        snapshot.request_slot_rejections_total
    );
    body.push_str(
        "# HELP tsink_write_admission_row_budget_rejections_total Public write row-budget admission rejections due to global in-flight row saturation\n\
         # TYPE tsink_write_admission_row_budget_rejections_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_write_admission_row_budget_rejections_total {}\n",
        snapshot.row_budget_rejections_total
    );
    body.push_str(
        "# HELP tsink_write_admission_oversize_rows_rejections_total Public write rejections for single requests that exceed the global row budget\n\
         # TYPE tsink_write_admission_oversize_rows_rejections_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_write_admission_oversize_rows_rejections_total {}\n",
        snapshot.oversize_rows_rejections_total
    );
    body.push_str(
        "# HELP tsink_write_admission_acquire_wait_nanoseconds_total Total wait time for admitted public writes to acquire request-slot and row-budget permits\n\
         # TYPE tsink_write_admission_acquire_wait_nanoseconds_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_write_admission_acquire_wait_nanoseconds_total {}\n",
        snapshot.acquire_wait_nanos_total
    );
    body.push_str(
        "# HELP tsink_write_admission_active_requests Active public write requests currently holding global admission request slots\n\
         # TYPE tsink_write_admission_active_requests gauge\n",
    );
    metrics_write!(
        body,
        "tsink_write_admission_active_requests {}\n",
        snapshot.active_requests
    );
    body.push_str(
        "# HELP tsink_write_admission_active_rows Active public write rows currently reserved against the global admission budget\n\
         # TYPE tsink_write_admission_active_rows gauge\n",
    );
    metrics_write!(
        body,
        "tsink_write_admission_active_rows {}\n",
        snapshot.active_rows
    );
}

fn append_write_rejection_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: WriteRejectionMetricsSnapshot,
) {
    body.push_str(
        "# HELP tsink_write_rejections_total Rows rejected by the canonical storage write path, partitioned by structured reason\n\
         # TYPE tsink_write_rejections_total counter\n",
    );
    for (index, reason) in WRITE_REJECTION_REASON_NAMES.iter().enumerate() {
        metrics_write!(
            body,
            "tsink_write_rejections_total{{reason=\"{reason}\"}} {}\n",
            snapshot.reason_totals[index]
        );
    }
    body.push_str(
        "# HELP tsink_write_indeterminate_requests_total Write requests whose failure may nevertheless have committed rows\n\
         # TYPE tsink_write_indeterminate_requests_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_write_indeterminate_requests_total {}\n",
        snapshot.indeterminate_requests_total
    );
}

fn append_read_admission_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: ReadAdmissionMetricsSnapshot,
) {
    body.push_str(
        "# HELP tsink_read_admission_rejections_total Public read admission rejections across request-slot, query-budget, and oversize guardrails\n\
         # TYPE tsink_read_admission_rejections_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_read_admission_rejections_total {}\n",
        snapshot.rejections_total
    );
    body.push_str(
        "# HELP tsink_read_admission_request_slot_rejections_total Public read request-slot admission rejections due to global concurrency saturation\n\
         # TYPE tsink_read_admission_request_slot_rejections_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_read_admission_request_slot_rejections_total {}\n",
        snapshot.request_slot_rejections_total
    );
    body.push_str(
        "# HELP tsink_read_admission_query_budget_rejections_total Public read query-budget admission rejections due to global in-flight query saturation\n\
         # TYPE tsink_read_admission_query_budget_rejections_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_read_admission_query_budget_rejections_total {}\n",
        snapshot.query_budget_rejections_total
    );
    body.push_str(
        "# HELP tsink_read_admission_oversize_queries_rejections_total Public read rejections for single requests that exceed the global query budget\n\
         # TYPE tsink_read_admission_oversize_queries_rejections_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_read_admission_oversize_queries_rejections_total {}\n",
        snapshot.oversize_queries_rejections_total
    );
    body.push_str(
        "# HELP tsink_read_admission_acquire_wait_nanoseconds_total Total wait time for admitted public reads to acquire request-slot and query-budget permits\n\
         # TYPE tsink_read_admission_acquire_wait_nanoseconds_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_read_admission_acquire_wait_nanoseconds_total {}\n",
        snapshot.acquire_wait_nanos_total
    );
    body.push_str(
        "# HELP tsink_read_admission_active_requests Active public read requests currently holding global admission request slots\n\
         # TYPE tsink_read_admission_active_requests gauge\n",
    );
    metrics_write!(
        body,
        "tsink_read_admission_active_requests {}\n",
        snapshot.active_requests
    );
    body.push_str(
        "# HELP tsink_read_admission_active_queries Active public read query units currently reserved against the global admission budget\n\
         # TYPE tsink_read_admission_active_queries gauge\n",
    );
    metrics_write!(
        body,
        "tsink_read_admission_active_queries {}\n",
        snapshot.active_queries
    );
}

fn append_tenant_admission_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: tenant::TenantAdmissionMetricsSnapshot,
) {
    body.push_str(
        "# HELP tsink_tenant_admission_read_rejections_total Tenant-scoped read admission rejections from per-tenant in-flight limits\n\
         # TYPE tsink_tenant_admission_read_rejections_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_tenant_admission_read_rejections_total {}\n",
        snapshot.read_rejections_total
    );
    body.push_str(
        "# HELP tsink_tenant_admission_write_rejections_total Tenant-scoped write admission rejections from per-tenant in-flight limits\n\
         # TYPE tsink_tenant_admission_write_rejections_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_tenant_admission_write_rejections_total {}\n",
        snapshot.write_rejections_total
    );
    body.push_str(
        "# HELP tsink_tenant_admission_active_reads Active tenant-scoped read requests currently holding per-tenant admission permits\n\
         # TYPE tsink_tenant_admission_active_reads gauge\n",
    );
    metrics_write!(
        body,
        "tsink_tenant_admission_active_reads {}\n",
        snapshot.active_reads
    );
    body.push_str(
        "# HELP tsink_tenant_admission_active_writes Active tenant-scoped write requests currently holding per-tenant admission permits\n\
         # TYPE tsink_tenant_admission_active_writes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_tenant_admission_active_writes {}\n",
        snapshot.active_writes
    );
    body.push_str(
        "# HELP tsink_tenant_admission_surface_rejections_total Tenant-scoped admission rejections by protected surface\n\
         # TYPE tsink_tenant_admission_surface_rejections_total counter\n",
    );
    for (surface, value) in [
        ("ingest", snapshot.ingest_rejections_total),
        ("query", snapshot.query_rejections_total),
        ("metadata", snapshot.metadata_rejections_total),
        ("retention", snapshot.retention_rejections_total),
    ] {
        metrics_write!(
            body,
            "tsink_tenant_admission_surface_rejections_total{{surface=\"{surface}\"}} {value}\n"
        );
    }
    body.push_str(
        "# HELP tsink_tenant_admission_surface_active_requests Active tenant-scoped requests by protected surface\n\
         # TYPE tsink_tenant_admission_surface_active_requests gauge\n",
    );
    for (surface, value) in [
        ("ingest", snapshot.ingest_active_requests),
        ("query", snapshot.query_active_requests),
        ("metadata", snapshot.metadata_active_requests),
        ("retention", snapshot.retention_active_requests),
    ] {
        metrics_write!(
            body,
            "tsink_tenant_admission_surface_active_requests{{surface=\"{surface}\"}} {value}\n"
        );
    }
    body.push_str(
        "# HELP tsink_tenant_admission_surface_active_units Active tenant-scoped resource units reserved by protected surface\n\
         # TYPE tsink_tenant_admission_surface_active_units gauge\n",
    );
    for (surface, value) in [
        ("ingest", snapshot.ingest_active_units),
        ("query", snapshot.query_active_units),
        ("metadata", snapshot.metadata_active_units),
        ("retention", snapshot.retention_active_units),
    ] {
        metrics_write!(
            body,
            "tsink_tenant_admission_surface_active_units{{surface=\"{surface}\"}} {value}\n"
        );
    }
}

fn append_tenant_runtime_cache_metrics(
    body: &mut dyn MetricsBodyWriter,
    snapshot: Option<tenant::TenantRuntimeCacheMetricsSnapshot>,
) {
    let configured = u8::from(snapshot.is_some());
    let initialized_runtimes = snapshot.map_or(0, |value| value.initialized_runtimes);
    let initialized_reserved_runtimes =
        snapshot.map_or(0, |value| value.initialized_reserved_runtimes);
    let initialized_dynamic_runtimes =
        snapshot.map_or(0, |value| value.initialized_dynamic_runtimes);
    let max_runtimes = snapshot.map_or(0, |value| value.max_runtimes);
    let reserved_runtimes = snapshot.map_or(0, |value| value.reserved_runtimes);
    let limit_rejections_total = snapshot.map_or(0, |value| value.limit_rejections_total);

    body.push_str(
        "# HELP tsink_tenant_runtime_cache_configured Whether a tenant runtime registry is configured (1 configured, 0 absent)\n\
         # TYPE tsink_tenant_runtime_cache_configured gauge\n",
    );
    metrics_write!(body, "tsink_tenant_runtime_cache_configured {configured}\n");
    body.push_str(
        "# HELP tsink_tenant_runtime_cache_initialized_runtimes Process-lifetime tenant runtime entries currently initialized\n\
         # TYPE tsink_tenant_runtime_cache_initialized_runtimes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_tenant_runtime_cache_initialized_runtimes {initialized_runtimes}\n"
    );
    body.push_str(
        "# HELP tsink_tenant_runtime_cache_initialized_reserved_runtimes Initialized tenant runtimes using configured-or-default reserved slots\n\
         # TYPE tsink_tenant_runtime_cache_initialized_reserved_runtimes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_tenant_runtime_cache_initialized_reserved_runtimes {initialized_reserved_runtimes}\n"
    );
    body.push_str(
        "# HELP tsink_tenant_runtime_cache_initialized_dynamic_runtimes Initialized unconfigured tenant runtimes using dynamic slots\n\
         # TYPE tsink_tenant_runtime_cache_initialized_dynamic_runtimes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_tenant_runtime_cache_initialized_dynamic_runtimes {initialized_dynamic_runtimes}\n"
    );
    body.push_str(
        "# HELP tsink_tenant_runtime_cache_max_runtimes Configured process-lifetime tenant runtime entry limit\n\
         # TYPE tsink_tenant_runtime_cache_max_runtimes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_tenant_runtime_cache_max_runtimes {max_runtimes}\n"
    );
    body.push_str(
        "# HELP tsink_tenant_runtime_cache_reserved_runtimes Runtime slots reserved for configured tenants and the default tenant\n\
         # TYPE tsink_tenant_runtime_cache_reserved_runtimes gauge\n",
    );
    metrics_write!(
        body,
        "tsink_tenant_runtime_cache_reserved_runtimes {reserved_runtimes}\n"
    );
    body.push_str(
        "# HELP tsink_tenant_runtime_cache_limit_rejections_total Tenant runtime initializations rejected at the process-lifetime cache limit\n\
         # TYPE tsink_tenant_runtime_cache_limit_rejections_total counter\n",
    );
    metrics_write!(
        body,
        "tsink_tenant_runtime_cache_limit_rejections_total {limit_rejections_total}\n"
    );
}

fn prometheus_escape_label_value(value: &str) -> PrometheusEscapedLabel<'_> {
    PrometheusEscapedLabel(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_metrics_body_accepts_exact_ceiling_and_rejects_next_byte() {
        let exact = "x".repeat(METRICS_MAX_RESPONSE_BYTES);
        let mut accepted = BoundedMetricsBody::new();
        accepted.push_str(&exact);
        assert_eq!(accepted.len(), METRICS_MAX_RESPONSE_BYTES);
        assert_eq!(
            accepted
                .into_bytes()
                .expect("the exact response ceiling should be accepted")
                .len(),
            METRICS_MAX_RESPONSE_BYTES
        );

        let mut rejected = BoundedMetricsBody::new();
        rejected.push_str(&exact);
        rejected.push_str("y");
        assert_eq!(rejected.len(), METRICS_MAX_RESPONSE_BYTES);
        assert!(
            rejected.into_bytes().is_err(),
            "the first byte over the ceiling must latch overflow without growing the body"
        );
    }

    #[test]
    fn streaming_label_escaping_honors_the_body_ceiling() {
        let mut short = String::new();
        let escaped = prometheus_escape_label_value("a\\b\n\"c");
        metrics_write!(&mut short, "{escaped}");
        assert_eq!(short, "a\\\\b\\n\\\"c");
        assert!(!std::mem::needs_drop::<PrometheusEscapedLabel<'_>>());

        let oversized_label = "\"".repeat(METRICS_MAX_RESPONSE_BYTES / 2 + 1);
        let escaped = prometheus_escape_label_value(&oversized_label);
        let mut body = BoundedMetricsBody::new();
        metrics_write!(&mut body, "{escaped}");
        assert_eq!(body.len(), METRICS_MAX_RESPONSE_BYTES);
        assert!(
            body.into_bytes().is_err(),
            "streaming escaping must stop at the response cap without an escaped String"
        );
    }

    #[test]
    fn metrics_body_replay_honors_cancellation_and_releases_accounting() {
        let budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits::default())
            .expect("query budget should build");
        let cancellation = tsink::QueryCancellationToken::new();
        let execution = budget
            .begin_query_with_token(cancellation.clone())
            .expect("query should admit");
        let render = |body: &mut dyn MetricsBodyWriter| {
            let label = prometheus_escape_label_value("tenant\\a\n\"");
            metrics_write!(body, "tsink_test_metric{{tenant=\"{label}\"}} 1\n");
        };

        let mut counter = CountingMetricsBody::new(&execution);
        render(&mut counter);
        assert!(counter.control_error.is_none());
        assert!(!counter.overflowed);
        let expected_len = counter.len;
        execution
            .charge_returned_bytes(u64::try_from(expected_len).expect("length should fit"))
            .expect("measured bytes should admit");
        let reservation = execution
            .reserve_memory(modeled_metrics_response_preflight_bytes(expected_len))
            .expect("measured response memory should admit");
        let body = BoundedMetricsBody::try_with_exact_capacity(expected_len)
            .expect("test body should allocate");

        cancellation.cancel();
        let mut controlled = ControlledMetricsBody::new(body, &execution);
        render(&mut controlled);
        assert!(matches!(
            controlled.control_error,
            Some(tsink::QueryBudgetError::Cancelled)
        ));
        drop(controlled);
        drop(reservation);
        drop(execution);

        let after = budget.snapshot();
        assert_eq!(after.queries_started_total, 1);
        assert_eq!(after.queries_completed_total, 1);
        assert_eq!(after.cancellations_total, 1);
        assert_eq!(after.active_queries, 0);
        assert_eq!(after.shared_reserved_memory_bytes, 0);
        assert_eq!(after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn metrics_response_reservation_enforces_exact_memory_boundary() {
        const SOURCE_BYTES: u64 = 32 * 1024;
        const BODY_LEN: usize = 4 * 1024;
        let response_bytes = modeled_metrics_response_preflight_bytes(BODY_LEN);
        let exact_peak = SOURCE_BYTES.saturating_add(response_bytes);

        let exact_budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits {
            max_shared_memory_bytes: Some(exact_peak),
            per_query: tsink::QueryWorkLimits {
                max_memory_bytes: Some(exact_peak),
                ..tsink::QueryWorkLimits::default()
            },
            ..tsink::QueryBudgetLimits::default()
        })
        .expect("exact budget should build");
        let exact_execution = exact_budget.begin_query().expect("query should admit");
        let source = exact_execution
            .reserve_memory(SOURCE_BYTES)
            .expect("source snapshot should fit");
        let mut response = exact_execution
            .reserve_memory(response_bytes)
            .expect("response memory M should fit beside the source snapshot");
        let body = BoundedMetricsBody::try_with_exact_capacity(BODY_LEN)
            .expect("test body should allocate");
        response
            .resize(modeled_metrics_response_preflight_bytes(body.capacity()))
            .expect("actual exact capacity should remain within M");
        drop(body);
        drop(response);
        drop(source);
        drop(exact_execution);
        let exact_after = exact_budget.snapshot();
        assert_eq!(exact_after.peak_shared_reserved_memory_bytes, exact_peak);
        assert_eq!(exact_after.queries_started_total, 1);
        assert_eq!(exact_after.queries_completed_total, 1);
        assert_eq!(exact_after.active_queries, 0);
        assert_eq!(exact_after.shared_reserved_memory_bytes, 0);
        assert_eq!(exact_after.accounting_invariant_violations_total, 0);

        let one_under_budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits {
            max_shared_memory_bytes: Some(exact_peak - 1),
            per_query: tsink::QueryWorkLimits {
                max_memory_bytes: Some(exact_peak - 1),
                ..tsink::QueryWorkLimits::default()
            },
            ..tsink::QueryBudgetLimits::default()
        })
        .expect("one-under budget should build");
        let one_under_execution = one_under_budget.begin_query().expect("query should admit");
        let source = one_under_execution
            .reserve_memory(SOURCE_BYTES)
            .expect("source snapshot should still fit at M-1");
        assert!(
            one_under_execution.reserve_memory(response_bytes).is_err(),
            "response reservation must reject before body allocation at M-1"
        );
        let mut errors = MetricsCollectionErrors::default();
        errors.insert(MetricsCollector::ExpositionBody);
        let fallback = metrics_fallback_response(errors);
        assert!(fallback.body.len() <= METRICS_FALLBACK_RESPONSE_BYTES);
        drop(source);
        drop(one_under_execution);
        let one_under_after = one_under_budget.snapshot();
        assert_eq!(one_under_after.queries_started_total, 1);
        assert_eq!(one_under_after.queries_completed_total, 1);
        assert_eq!(one_under_after.active_queries, 0);
        assert_eq!(one_under_after.shared_reserved_memory_bytes, 0);
        assert_eq!(
            one_under_after
                .shared_memory_rejections_total
                .saturating_add(one_under_after.per_query_memory_rejections_total),
            1
        );
        assert_eq!(one_under_after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn metrics_collection_errors_deduplicate_and_render_in_stable_order() {
        let mut errors = MetricsCollectionErrors::default();
        errors.insert(MetricsCollector::RulesRuntime);
        errors.insert(MetricsCollector::StorageListMetrics);
        errors.insert(MetricsCollector::RulesRuntime);
        errors.insert(MetricsCollector::ExpositionBody);

        let mut body = String::new();
        append_metrics_collection_errors(&mut body, errors);
        assert!(body.contains("tsink_metrics_collection_errors 3\n"));
        let storage = body
            .find("collector=\"storage_list_metrics\"")
            .expect("storage error should render");
        let rules = body
            .find("collector=\"rules_runtime\"")
            .expect("rules error should render");
        let exposition = body
            .find("collector=\"exposition_body\"")
            .expect("exposition error should render");
        assert!(storage < rules && rules < exposition);
        assert_eq!(body.matches("collector=\"rules_runtime\"").count(), 1);
    }

    #[test]
    fn metrics_fallback_is_fixed_parseable_and_bounded() {
        let mut errors = MetricsCollectionErrors::default();
        errors.insert(MetricsCollector::OperationalSnapshots);
        errors.insert(MetricsCollector::ExpositionBody);
        let response = metrics_fallback_response(errors);

        assert_eq!(response.status, 200);
        assert!(response.body.len() <= METRICS_FALLBACK_RESPONSE_BYTES);
        assert_eq!(
            response
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
                .map(|(_, value)| value.as_str()),
            Some(METRICS_CONTENT_TYPE)
        );
        let body = std::str::from_utf8(&response.body).expect("fallback is Prometheus text");
        assert!(body.contains("tsink_metrics_collection_errors 2\n"));
        assert!(body.contains("collector=\"operational_snapshots\""));
        assert!(body.contains("collector=\"exposition_body\""));
        assert!(!body.contains("tsink_memory_used_bytes"));
        for sample in [
            "tsink_cluster_hotspot_skewed_shards 0\n",
            "tsink_cluster_hotspot_skewed_tenants 0\n",
            "tsink_cluster_hotspot_max_shard_score 0\n",
            "tsink_cluster_hotspot_max_tenant_score 0\n",
        ] {
            assert!(body.contains(sample));
        }
    }

    #[test]
    fn tenant_runtime_cache_metrics_are_fixed_cardinality_with_and_without_a_registry() {
        let mut absent = String::new();
        append_tenant_runtime_cache_metrics(&mut absent, None);
        for sample in [
            "tsink_tenant_runtime_cache_configured 0\n",
            "tsink_tenant_runtime_cache_initialized_runtimes 0\n",
            "tsink_tenant_runtime_cache_initialized_reserved_runtimes 0\n",
            "tsink_tenant_runtime_cache_initialized_dynamic_runtimes 0\n",
            "tsink_tenant_runtime_cache_max_runtimes 0\n",
            "tsink_tenant_runtime_cache_reserved_runtimes 0\n",
            "tsink_tenant_runtime_cache_limit_rejections_total 0\n",
        ] {
            assert!(absent.contains(sample));
        }

        let mut configured = String::new();
        append_tenant_runtime_cache_metrics(
            &mut configured,
            Some(tenant::TenantRuntimeCacheMetricsSnapshot {
                initialized_runtimes: 7,
                initialized_reserved_runtimes: 2,
                initialized_dynamic_runtimes: 5,
                max_runtimes: 11,
                reserved_runtimes: 3,
                limit_rejections_total: 13,
            }),
        );
        for sample in [
            "tsink_tenant_runtime_cache_configured 1\n",
            "tsink_tenant_runtime_cache_initialized_runtimes 7\n",
            "tsink_tenant_runtime_cache_initialized_reserved_runtimes 2\n",
            "tsink_tenant_runtime_cache_initialized_dynamic_runtimes 5\n",
            "tsink_tenant_runtime_cache_max_runtimes 11\n",
            "tsink_tenant_runtime_cache_reserved_runtimes 3\n",
            "tsink_tenant_runtime_cache_limit_rejections_total 13\n",
        ] {
            assert!(configured.contains(sample));
        }
        assert_eq!(
            absent
                .lines()
                .filter(|line| line.starts_with("tsink_tenant_runtime_cache_configured "))
                .count(),
            1
        );
        assert_eq!(
            configured
                .lines()
                .filter(|line| line.starts_with("tsink_tenant_runtime_cache_configured "))
                .count(),
            1
        );
    }

    #[test]
    fn background_work_metrics_render_fixed_worker_and_event_labels() {
        let mut body = String::new();
        append_background_work_metrics(
            &mut body,
            &tsink::BackgroundWorkObservabilitySnapshot {
                max_threads: 4,
                installed_threads: 3,
                running_threads: 2,
                close_attempts_total: 3,
                close_success_total: 2,
                close_errors_total: 1,
                close_coordination_timeouts_total: 1,
                close_coordination_wait_nanos_total: 17,
                close_compaction_passes_total: 9,
                close_compaction_pass_limit: 128,
                close_duration_nanos_total: 23,
                shutdown_join_wait_nanos_total: 5,
                flush: tsink::BackgroundWorkerObservabilitySnapshot {
                    installed: true,
                    running: true,
                    interval_nanos: Some(250_000_000),
                    max_concurrency: 1,
                    idle_waits_total: 7,
                    passes_completed_total: 5,
                    ..tsink::BackgroundWorkerObservabilitySnapshot::default()
                },
                ..tsink::BackgroundWorkObservabilitySnapshot::default()
            },
        );

        assert!(body.contains("tsink_background_threads{state=\"limit\"} 4\n"));
        assert!(body.contains(
            "tsink_background_worker_state{worker=\"flush\",state=\"interval_nanos\"} 250000000\n"
        ));
        assert!(body.contains(
            "tsink_background_worker_events_total{worker=\"flush\",event=\"idle_waits\"} 7\n"
        ));
        assert!(body.contains(
            "tsink_background_worker_events_total{worker=\"flush\",event=\"passes_completed\"} 5\n"
        ));
        for worker in ["flush", "compaction", "persisted_refresh", "rollup"] {
            assert!(body.contains(&format!(
                "tsink_background_worker_state{{worker=\"{worker}\",state=\"installed\"}}"
            )));
        }
        assert!(
            body.contains("tsink_storage_close_events_total{event=\"coordination_timeouts\"} 1\n")
        );
        assert!(body.contains("tsink_storage_close_wait_nanos_total{wait=\"coordination\"} 17\n"));
        assert!(body.contains("tsink_storage_close_compaction_pass_limit 128\n"));
    }

    #[test]
    fn cardinality_metrics_render_creation_window_and_lifetime_state() {
        let mut body = String::new();
        append_cardinality_metrics(
            &mut body,
            &tsink::CardinalityObservabilitySnapshot {
                series_count: 17,
                pending_new_series: 2,
                committed_in_window: 5,
                current_window_start: Some(123),
                admitted_new_series_total: 11,
                committed_new_series_total: 9,
                creation_rate_rejections_total: 3,
            },
        );

        assert!(body.contains("tsink_series_creation_pending 2\n"));
        assert!(body.contains("tsink_series_creation_committed_in_window 5\n"));
        assert!(body.contains("tsink_series_creation_window_initialized 1\n"));
        assert!(body.contains("tsink_series_creation_window_start 123\n"));
        assert!(body.contains("tsink_series_creation_admitted_total 11\n"));
        assert!(body.contains("tsink_series_creation_committed_total 9\n"));
        assert!(body.contains("tsink_series_creation_rejections_total 3\n"));
    }

    #[test]
    fn exemplar_metrics_render_resource_ownership_limits_and_stable_rejection_code() {
        let mut body = String::new();
        append_exemplar_metrics(
            &mut body,
            &ExemplarStoreMetricsSnapshot {
                retained_bytes: 101,
                peak_retained_bytes: 202,
                durable_file_bytes: 303,
                peak_durable_file_bytes: 404,
                transient_bytes: 0,
                peak_transient_bytes: 505,
                resource_rejections_total: 8,
                shape_rejections_total: 1,
                batch_rejections_total: 2,
                retained_rejections_total: 3,
                transient_rejections_total: 4,
                durable_rejections_total: 5,
                startup_rejections_total: 6,
                snapshot_rejections_total: 7,
                last_rejection_code: Some(
                    crate::exemplar_store::ExemplarStoreErrorCode::RetainedBytes,
                ),
                ..ExemplarStoreMetricsSnapshot::default()
            },
            ExemplarStoreConfig {
                max_total_exemplars: 11,
                max_exemplars_per_series: 12,
                max_exemplars_per_request: 13,
                max_query_results: 14,
                max_query_selectors: 15,
            },
            crate::exemplar_store::ExemplarStoreResourceLimits {
                max_total_series: 16,
                max_total_retained_bytes: 17,
                max_update_batch_bytes: 18,
                max_write_transient_bytes: 19,
                max_replacement_peak_bytes: 20,
                max_persistence_serialization_bytes: 21,
                max_durable_file_bytes: 22,
                max_startup_transient_bytes: 23,
                max_snapshot_bytes: 24,
                max_snapshot_transient_bytes: 25,
                max_concurrent_transient_bytes: 26,
                ..crate::exemplar_store::ExemplarStoreResourceLimits::default()
            },
        );

        for expected in [
            "tsink_exemplar_store_memory_bytes{kind=\"retained\"} 101\n",
            "tsink_exemplar_store_memory_bytes{kind=\"peak_retained\"} 202\n",
            "tsink_exemplar_store_memory_bytes{kind=\"transient\"} 0\n",
            "tsink_exemplar_store_memory_bytes{kind=\"peak_transient\"} 505\n",
            "tsink_exemplar_store_durable_file_bytes{kind=\"current\"} 303\n",
            "tsink_exemplar_store_durable_file_bytes{kind=\"peak\"} 404\n",
            "tsink_exemplar_store_resource_rejections_total{reason=\"all\"} 8\n",
            "tsink_exemplar_store_resource_rejections_total{reason=\"snapshot\"} 7\n",
            "tsink_exemplar_store_last_rejection{code=\"exemplar_retained_bytes_limit\"} 1\n",
            "tsink_exemplar_limits{kind=\"max_total_series\"} 16\n",
            "tsink_exemplar_limits{kind=\"max_retained_bytes\"} 17\n",
            "tsink_exemplar_limits{kind=\"max_concurrent_transient_bytes\"} 26\n",
        ] {
            assert!(body.contains(expected), "missing metric line: {expected}");
        }
    }

    #[test]
    fn usage_metrics_render_failed_durable_publications() {
        let mut body = String::new();
        append_usage_metrics(
            &mut body,
            &crate::usage::UsageAccountingMetricsSnapshot {
                record_failures_total: 7,
                retained_records: 3,
                earliest_retained_sequence: Some(9),
                recent_record_limit: crate::usage::DEFAULT_USAGE_LEDGER_RECENT_RECORDS,
                ..crate::usage::UsageAccountingMetricsSnapshot::default()
            },
        );

        assert!(body.contains("tsink_usage_ledger_record_failures_total 7\n"));
        assert!(body.contains("tsink_usage_ledger_retained_records 3\n"));
        assert!(body.contains("tsink_usage_ledger_earliest_retained_sequence 9\n"));
        assert!(body.contains(&format!(
            "tsink_usage_ledger_recent_record_limit {}\n",
            crate::usage::DEFAULT_USAGE_LEDGER_RECENT_RECORDS
        )));
    }

    #[test]
    fn legacy_write_observability_renders_fixed_cardinality_labels() {
        let snapshot = legacy_ingest::AdapterWriteObservabilitySnapshot {
            acknowledgements_total: [1, 2, 3, 4],
            outcomes_total: [5, 6, 7, 8],
            error_reasons_total: [0; legacy_ingest::LEGACY_WRITE_ERROR_REASON_NAMES.len()],
            accepted_metadata_updates_total: 9,
            applied_metadata_updates_total: 10,
            accepted_exemplars_total: 11,
        };
        let mut body = String::new();
        append_legacy_write_observability(&mut body, "statsd", &snapshot);

        assert!(body.contains(
            "tsink_legacy_ingest_write_acknowledgements_total{adapter=\"statsd\",level=\"durable\"} 4"
        ));
        assert!(body.contains(
            "tsink_legacy_ingest_write_outcomes_total{adapter=\"statsd\",outcome=\"indeterminate\"} 8"
        ));
        assert!(body.contains(
            "tsink_legacy_ingest_write_errors_total{adapter=\"statsd\",reason=\"other\"} 0"
        ));
        assert!(body.contains(
            "tsink_legacy_ingest_sidecar_items_total{adapter=\"statsd\",kind=\"exemplars_accepted\"} 11"
        ));
    }

    #[test]
    fn edge_sync_metrics_render_queue_health_flags() {
        let mut body = String::new();
        append_edge_sync_metrics(
            &mut body,
            &edge_sync::EdgeSyncSourceMetricsSnapshot {
                persistence_fenced: true,
                cleanup_pending: true,
                degraded: true,
                ..edge_sync::EdgeSyncSourceMetricsSnapshot::default()
            },
            &edge_sync::EdgeSyncAcceptMetricsSnapshot::default(),
        );

        assert!(body.contains("tsink_edge_sync_queue_health{state=\"persistence_fenced\"} 1\n"));
        assert!(body.contains("tsink_edge_sync_queue_health{state=\"cleanup_pending\"} 1\n"));
        assert!(body.contains("tsink_edge_sync_queue_health{state=\"degraded\"} 1\n"));
    }

    #[test]
    fn cluster_audit_metrics_render_persistence_health() {
        let mut body = String::new();
        append_cluster_audit_metrics(
            &mut body,
            &crate::cluster::audit::ClusterAuditMetricsSnapshot {
                enabled: true,
                retained_entries: 3,
                log_bytes: 512,
                cleanup_pending: true,
                persistence_fenced: true,
                degraded: true,
            },
        );

        assert!(body.contains("tsink_cluster_audit_log{kind=\"retained_entries\"} 3\n"));
        assert!(body.contains("tsink_cluster_audit_log{kind=\"log_bytes\"} 512\n"));
        assert!(body.contains("tsink_cluster_audit_health{state=\"persistence_fenced\"} 1\n"));
        assert!(body.contains("tsink_cluster_audit_health{state=\"cleanup_pending\"} 1\n"));
    }

    #[test]
    fn control_persistence_metrics_render_fixed_cardinality_health() {
        let mut body = String::new();
        append_cluster_control_persistence_metrics(
            &mut body,
            &ControlPersistenceStatus {
                fenced: false,
                pending_checkpoint: Some(crate::cluster::consensus::ControlCommitPosition {
                    index: 17,
                    term: 4,
                }),
                cleanup_debt: false,
                detail: Some("checkpoint repair pending".to_string()),
            },
        );

        assert!(body.contains("tsink_cluster_control_persistence_health{state=\"fenced\"} 0\n"));
        assert!(body.contains(
            "tsink_cluster_control_persistence_health{state=\"checkpoint_pending\"} 1\n"
        ));
        assert!(
            body.contains("tsink_cluster_control_persistence_health{state=\"cleanup_debt\"} 0\n")
        );
        assert!(body.contains("tsink_cluster_control_persistence_health{state=\"degraded\"} 1\n"));
        assert!(body.contains("tsink_cluster_control_persistence_pending_checkpoint_index 17\n"));
        assert!(body.contains("tsink_cluster_control_persistence_pending_checkpoint_term 4\n"));
    }
}
