use crate::cluster::control::{ControlHotspotSnapshot, ControlState};
use crate::cluster::replication::stable_series_identity_hash;
use crate::cluster::ring::ShardRing;
use crate::tenant;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tsink::{Label, MetricSeries, Row};

const DEFAULT_TOP_N: usize = 8;
const SHARD_SKEW_THRESHOLD: f64 = 4.0;
const TENANT_SKEW_THRESHOLD: f64 = 4.0;
const HOTSPOT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;
const HOTSPOT_BTREE_ENTRY_ALLOWANCE_BYTES: u64 = 256;
const HOTSPOT_FIXED_SCRATCH_BYTES: u64 = 16 * 1024;

#[derive(Clone, Copy)]
enum HotspotControlInput<'a> {
    State(&'a ControlState),
    Metrics(&'a ControlHotspotSnapshot),
}

impl HotspotControlInput<'_> {
    fn transition_count(self) -> usize {
        match self {
            Self::State(state) => state.transitions.len(),
            Self::Metrics(snapshot) => snapshot.handoff_shards.len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HotspotShardCountersSnapshot {
    pub shard: u32,
    pub ingest_rows_total: u64,
    pub query_requests_total: u64,
    pub query_shard_hits_total: u64,
    pub repair_mismatches_total: u64,
    pub repair_series_gap_total: u64,
    pub repair_point_gap_total: u64,
    pub repair_rows_inserted_total: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HotspotTenantCountersSnapshot {
    pub tenant_id: String,
    pub ingest_rows_total: u64,
    pub query_requests_total: u64,
    pub query_units_total: u64,
    pub repair_rows_inserted_total: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HotspotTrackerSnapshot {
    pub generated_unix_ms: u64,
    pub shards: Vec<HotspotShardCountersSnapshot>,
    pub tenants: Vec<HotspotTenantCountersSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HotShardSnapshot {
    pub shard: u32,
    pub ingest_rows_total: u64,
    pub query_shard_hits_total: u64,
    pub storage_series: u64,
    pub repair_mismatches_total: u64,
    pub repair_series_gap_total: u64,
    pub repair_point_gap_total: u64,
    pub repair_rows_inserted_total: u64,
    pub handoff_pending_rows: u64,
    pub pressure_score: f64,
    pub movement_cost_score: f64,
    pub skew_factor: f64,
    pub recommend_move: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantHotspotSnapshot {
    pub tenant_id: String,
    pub ingest_rows_total: u64,
    pub query_requests_total: u64,
    pub query_units_total: u64,
    pub storage_series: u64,
    pub repair_rows_inserted_total: u64,
    pub pressure_score: f64,
    pub skew_factor: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterHotspotSnapshot {
    pub generated_unix_ms: u64,
    pub hot_shards: Vec<HotShardSnapshot>,
    pub tenant_hotspots: Vec<TenantHotspotSnapshot>,
    pub skewed_shards: usize,
    pub skewed_tenants: usize,
    pub max_shard_score: f64,
    pub max_tenant_score: f64,
}

/// Hotspot output whose modeled allocations remain charged to the status query.
#[derive(Debug)]
pub struct AccountedClusterHotspotSnapshot {
    pub snapshot: ClusterHotspotSnapshot,
    _reservation: tsink::QueryMemoryReservation,
}

/// Admin-rebalance hotspot output plus the exact tracker generation used for move candidates.
#[derive(Debug)]
pub(crate) struct AccountedRebalanceHotspotSnapshot {
    pub snapshot: ClusterHotspotSnapshot,
    pub tracker: HotspotTrackerSnapshot,
    _reservation: tsink::QueryMemoryReservation,
}

impl std::ops::Deref for AccountedClusterHotspotSnapshot {
    type Target = ClusterHotspotSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

#[derive(Debug, Clone, Default)]
struct HotspotTracker {
    shards: BTreeMap<u32, HotspotShardCounters>,
    tenants: BTreeMap<String, HotspotTenantCounters>,
}

#[derive(Debug, Clone, Default)]
struct HotspotShardCounters {
    ingest_rows_total: u64,
    query_requests_total: u64,
    query_shard_hits_total: u64,
    repair_mismatches_total: u64,
    repair_series_gap_total: u64,
    repair_point_gap_total: u64,
    repair_rows_inserted_total: u64,
}

#[derive(Debug, Clone, Default)]
struct HotspotTenantCounters {
    ingest_rows_total: u64,
    query_requests_total: u64,
    query_units_total: u64,
    repair_rows_inserted_total: u64,
}

static HOTSPOT_TRACKER: OnceLock<Mutex<HotspotTracker>> = OnceLock::new();

pub fn record_ingest_rows(ring: Option<&ShardRing>, rows: &[Row]) {
    if rows.is_empty() {
        return;
    }
    with_hotspot_tracker(|tracker| {
        record_ingest_rows_on_tracker(tracker, ring, rows);
    });
}

pub fn record_query_plan(candidate_shards: &[u32]) {
    if candidate_shards.is_empty() {
        return;
    }
    with_hotspot_tracker(|tracker| {
        for shard in candidate_shards {
            let counters = tracker.shards.entry(*shard).or_default();
            counters.query_shard_hits_total = counters.query_shard_hits_total.saturating_add(1);
        }
        for shard in candidate_shards {
            let counters = tracker.shards.entry(*shard).or_default();
            counters.query_requests_total = counters.query_requests_total.saturating_add(1);
        }
    });
}

pub fn record_tenant_query(tenant_id: &str, requests: u64, units: u64) {
    let tenant_id = normalize_tenant_id(tenant_id);
    with_hotspot_tracker(|tracker| {
        let tenant = tracker.tenants.entry(tenant_id.clone()).or_default();
        tenant.query_requests_total = tenant.query_requests_total.saturating_add(requests.max(1));
        tenant.query_units_total = tenant.query_units_total.saturating_add(units);
    });
}

pub fn record_repair_mismatch(shard: u32, series_gap: u64, point_gap: u64) {
    with_hotspot_tracker(|tracker| {
        let counters = tracker.shards.entry(shard).or_default();
        counters.repair_mismatches_total = counters.repair_mismatches_total.saturating_add(1);
        counters.repair_series_gap_total =
            counters.repair_series_gap_total.saturating_add(series_gap);
        counters.repair_point_gap_total = counters.repair_point_gap_total.saturating_add(point_gap);
    });
}

pub fn record_repair_rows_inserted(shard: u32, rows: &[Row]) {
    if rows.is_empty() {
        return;
    }
    with_hotspot_tracker(|tracker| {
        let shard_counters = tracker.shards.entry(shard).or_default();
        shard_counters.repair_rows_inserted_total = shard_counters
            .repair_rows_inserted_total
            .saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
        for row in rows {
            let tenant_id = tenant_id_from_labels(row.labels()).to_string();
            let tenant = tracker.tenants.entry(tenant_id).or_default();
            tenant.repair_rows_inserted_total = tenant.repair_rows_inserted_total.saturating_add(1);
        }
    });
}

pub fn hotspot_tracker_snapshot() -> HotspotTrackerSnapshot {
    with_hotspot_tracker(|tracker| hotspot_tracker_snapshot_from_tracker(tracker))
}

#[cfg(test)]
pub(crate) fn hotspot_tracker_snapshot_for_rows(
    ring: Option<&ShardRing>,
    rows: &[Row],
) -> HotspotTrackerSnapshot {
    let mut tracker = HotspotTracker::default();
    record_ingest_rows_on_tracker(&mut tracker, ring, rows);
    hotspot_tracker_snapshot_from_tracker(&tracker)
}

#[allow(dead_code)]
pub fn build_cluster_hotspot_snapshot(
    metrics: &[MetricSeries],
    ring: Option<&ShardRing>,
    control_state: Option<&ControlState>,
    tenant_scope: Option<&str>,
) -> ClusterHotspotSnapshot {
    build_cluster_hotspot_snapshot_with_limit(
        metrics,
        ring,
        control_state,
        tenant_scope,
        DEFAULT_TOP_N,
    )
}

#[allow(dead_code)]
pub fn build_cluster_hotspot_snapshot_with_limit(
    metrics: &[MetricSeries],
    ring: Option<&ShardRing>,
    control_state: Option<&ControlState>,
    tenant_scope: Option<&str>,
    top_n: usize,
) -> ClusterHotspotSnapshot {
    let tracker = hotspot_tracker_snapshot();
    build_cluster_hotspot_snapshot_from_tracker_controlled(
        &tracker,
        metrics,
        ring,
        control_state.map(HotspotControlInput::State),
        tenant_scope,
        top_n,
        None,
    )
    .expect("an uncontrolled hotspot transform cannot fail")
}

/// Builds a hotspot snapshot while accounting its tracker clone, maps, unions, sort vectors, and
/// retained top-N result to one query execution.
///
/// The reservation is established while the tracker lock is held, so a concurrent new
/// tenant/shard cannot grow the cloned tracker beyond the admitted model between measurement and
/// materialization.
#[allow(dead_code)]
pub fn build_cluster_hotspot_snapshot_with_execution(
    metrics: &[MetricSeries],
    ring: Option<&ShardRing>,
    control_state: Option<&ControlState>,
    tenant_scope: Option<&str>,
    execution: &tsink::QueryExecution,
) -> Result<AccountedClusterHotspotSnapshot, tsink::QueryBudgetError> {
    build_cluster_hotspot_snapshot_with_control_input_execution(
        metrics,
        ring,
        control_state.map(HotspotControlInput::State),
        tenant_scope,
        execution,
    )
}

pub(crate) fn build_cluster_hotspot_snapshot_with_control_metrics_execution(
    metrics: &[MetricSeries],
    ring: Option<&ShardRing>,
    control: Option<&ControlHotspotSnapshot>,
    tenant_scope: Option<&str>,
    execution: &tsink::QueryExecution,
) -> Result<AccountedClusterHotspotSnapshot, tsink::QueryBudgetError> {
    build_cluster_hotspot_snapshot_with_control_input_execution(
        metrics,
        ring,
        control.map(HotspotControlInput::Metrics),
        tenant_scope,
        execution,
    )
}

/// Builds the admin rebalance hotspot and retains the same accounted tracker snapshot for live
/// move-candidate scoring. This prevents the response from cloning the global tracker twice or
/// mixing candidate and hotspot data from different tracker generations.
pub(crate) fn build_rebalance_hotspot_snapshot_with_control_metrics_execution(
    metrics: &[MetricSeries],
    ring: Option<&ShardRing>,
    control: Option<&ControlHotspotSnapshot>,
    execution: &tsink::QueryExecution,
) -> Result<AccountedRebalanceHotspotSnapshot, tsink::QueryBudgetError> {
    build_rebalance_hotspot_snapshot_with_control_metrics_tenant_execution(
        metrics, ring, control, None, execution,
    )
}

/// Builds the tracker-retaining hotspot projection for a tenant-scoped status response.
///
/// The returned tracker is the exact generation used for the tenant-scoped hotspot output, so a
/// downstream rebalance projection can score candidates without a second global tracker clone.
pub(crate) fn build_rebalance_hotspot_snapshot_with_control_metrics_tenant_execution(
    metrics: &[MetricSeries],
    ring: Option<&ShardRing>,
    control: Option<&ControlHotspotSnapshot>,
    tenant_scope: Option<&str>,
    execution: &tsink::QueryExecution,
) -> Result<AccountedRebalanceHotspotSnapshot, tsink::QueryBudgetError> {
    execution.checkpoint()?;
    let metric_tenant_string_bytes = modeled_metric_tenant_string_bytes(metrics, Some(execution))?;
    let control = control.map(HotspotControlInput::Metrics);
    let (tracker, reservation) = with_hotspot_tracker(|tracker| {
        prepare_rebalance_hotspot_tracker_with_execution(
            tracker,
            metrics.len(),
            metric_tenant_string_bytes,
            ring,
            control,
            execution,
        )
    })?;
    finish_rebalance_hotspot_snapshot_with_execution(
        tracker,
        reservation,
        metrics,
        ring,
        control,
        tenant_scope,
        execution,
    )
}

fn prepare_rebalance_hotspot_tracker_with_execution(
    tracker: &HotspotTracker,
    metric_count: usize,
    metric_tenant_string_bytes: u64,
    ring: Option<&ShardRing>,
    control: Option<HotspotControlInput<'_>>,
    execution: &tsink::QueryExecution,
) -> Result<(HotspotTrackerSnapshot, tsink::QueryMemoryReservation), tsink::QueryBudgetError> {
    let peak_bytes = modeled_hotspot_peak_bytes(
        tracker,
        metric_count,
        metric_tenant_string_bytes,
        ring,
        control,
        DEFAULT_TOP_N,
        Some(execution),
    )?;
    let reservation = execution.reserve_memory(peak_bytes)?;
    let snapshot = hotspot_tracker_snapshot_from_tracker_controlled(tracker, Some(execution))?;
    Ok((snapshot, reservation))
}

fn finish_rebalance_hotspot_snapshot_with_execution(
    tracker: HotspotTrackerSnapshot,
    mut reservation: tsink::QueryMemoryReservation,
    metrics: &[MetricSeries],
    ring: Option<&ShardRing>,
    control: Option<HotspotControlInput<'_>>,
    tenant_scope: Option<&str>,
    execution: &tsink::QueryExecution,
) -> Result<AccountedRebalanceHotspotSnapshot, tsink::QueryBudgetError> {
    execution.checkpoint()?;
    let snapshot = build_cluster_hotspot_snapshot_from_tracker_controlled(
        &tracker,
        metrics,
        ring,
        control,
        tenant_scope,
        DEFAULT_TOP_N,
        Some(execution),
    )?;
    reservation.resize(
        modeled_hotspot_tracker_snapshot_retained_bytes(&tracker)
            .saturating_add(modeled_hotspot_result_retained_bytes(&snapshot)),
    )?;
    Ok(AccountedRebalanceHotspotSnapshot {
        snapshot,
        tracker,
        _reservation: reservation,
    })
}

fn build_cluster_hotspot_snapshot_with_control_input_execution(
    metrics: &[MetricSeries],
    ring: Option<&ShardRing>,
    control: Option<HotspotControlInput<'_>>,
    tenant_scope: Option<&str>,
    execution: &tsink::QueryExecution,
) -> Result<AccountedClusterHotspotSnapshot, tsink::QueryBudgetError> {
    execution.checkpoint()?;
    let metric_tenant_string_bytes = modeled_metric_tenant_string_bytes(metrics, Some(execution))?;
    let (tracker, mut reservation) = with_hotspot_tracker(|tracker| {
        let peak_bytes = modeled_hotspot_peak_bytes(
            tracker,
            metrics.len(),
            metric_tenant_string_bytes,
            ring,
            control,
            DEFAULT_TOP_N,
            Some(execution),
        )?;
        let reservation = execution.reserve_memory(peak_bytes)?;
        let snapshot = hotspot_tracker_snapshot_from_tracker_controlled(tracker, Some(execution))?;
        Ok::<_, tsink::QueryBudgetError>((snapshot, reservation))
    })?;
    execution.checkpoint()?;
    let snapshot = build_cluster_hotspot_snapshot_from_tracker_controlled(
        &tracker,
        metrics,
        ring,
        control,
        tenant_scope,
        DEFAULT_TOP_N,
        Some(execution),
    )?;
    reservation.resize(modeled_hotspot_result_retained_bytes(&snapshot))?;
    Ok(AccountedClusterHotspotSnapshot {
        snapshot,
        _reservation: reservation,
    })
}

fn build_cluster_hotspot_snapshot_from_tracker_controlled(
    tracker: &HotspotTrackerSnapshot,
    metrics: &[MetricSeries],
    ring: Option<&ShardRing>,
    control: Option<HotspotControlInput<'_>>,
    tenant_scope: Option<&str>,
    top_n: usize,
    execution: Option<&tsink::QueryExecution>,
) -> Result<ClusterHotspotSnapshot, tsink::QueryBudgetError> {
    hotspot_checkpoint(execution)?;
    let tenant_scope = tenant_scope.map(normalize_tenant_id);

    let mut storage_series_by_shard = BTreeMap::<u32, u64>::new();
    let mut storage_series_by_tenant = BTreeMap::<String, u64>::new();
    for series in metrics {
        hotspot_checkpoint(execution)?;
        let tenant_id = tenant_id_from_labels(&series.labels).to_string();
        *storage_series_by_tenant
            .entry(tenant_id.clone())
            .or_insert(0) += 1;
        if let Some(ring) = ring {
            let shard =
                ring.shard_for_series_id(stable_series_identity_hash(&series.name, &series.labels));
            *storage_series_by_shard.entry(shard).or_insert(0) += 1;
        }
        hotspot_observe_intermediate(
            execution,
            storage_series_by_tenant
                .len()
                .max(storage_series_by_shard.len()),
        )?;
    }

    let mut handoff_pending_by_shard = BTreeMap::<u32, u64>::new();
    let mut record_handoff = |shard: u32, pending_rows: u64| {
        hotspot_checkpoint(execution)?;
        *handoff_pending_by_shard.entry(shard).or_insert(0) = handoff_pending_by_shard
            .get(&shard)
            .copied()
            .unwrap_or(0)
            .saturating_add(pending_rows);
        hotspot_observe_intermediate(execution, handoff_pending_by_shard.len())
    };
    match control {
        Some(HotspotControlInput::State(state)) => {
            for transition in &state.transitions {
                if transition.handoff.phase.is_active() {
                    record_handoff(transition.shard, transition.handoff.pending_rows)?;
                } else {
                    hotspot_checkpoint(execution)?;
                }
            }
        }
        Some(HotspotControlInput::Metrics(snapshot)) => {
            for handoff in &snapshot.handoff_shards {
                record_handoff(handoff.shard, handoff.pending_rows)?;
            }
        }
        None => {}
    }

    let mut shard_ingest = BTreeMap::<u32, u64>::new();
    let mut shard_query = BTreeMap::<u32, u64>::new();
    let mut shard_repair_mismatches = BTreeMap::<u32, u64>::new();
    let mut shard_repair_series_gap = BTreeMap::<u32, u64>::new();
    let mut shard_repair_point_gap = BTreeMap::<u32, u64>::new();
    let mut shard_repair_rows = BTreeMap::<u32, u64>::new();
    for shard in &tracker.shards {
        hotspot_checkpoint(execution)?;
        shard_ingest.insert(shard.shard, shard.ingest_rows_total);
        shard_query.insert(shard.shard, shard.query_shard_hits_total);
        shard_repair_mismatches.insert(shard.shard, shard.repair_mismatches_total);
        shard_repair_series_gap.insert(shard.shard, shard.repair_series_gap_total);
        shard_repair_point_gap.insert(shard.shard, shard.repair_point_gap_total);
        shard_repair_rows.insert(shard.shard, shard.repair_rows_inserted_total);
        hotspot_observe_intermediate(execution, shard_ingest.len())?;
    }

    let shard_ids = shard_union_controlled(
        &shard_ingest,
        &shard_query,
        &storage_series_by_shard,
        &handoff_pending_by_shard,
        &shard_repair_point_gap,
        execution,
    )?;
    let total_shard_slots = shard_ids.len().max(1);
    let total_ingest = shard_ingest.values().copied().sum::<u64>();
    let total_query = shard_query.values().copied().sum::<u64>();
    let total_storage = storage_series_by_shard.values().copied().sum::<u64>();
    let total_repair = handoff_pending_by_shard
        .values()
        .copied()
        .sum::<u64>()
        .saturating_add(shard_repair_point_gap.values().copied().sum::<u64>())
        .saturating_add(shard_repair_rows.values().copied().sum::<u64>());

    let mut hot_shards = shard_ids
        .into_iter()
        .map(|shard| {
            hotspot_checkpoint(execution)?;
            let ingest_rows_total = shard_ingest.get(&shard).copied().unwrap_or(0);
            let query_shard_hits_total = shard_query.get(&shard).copied().unwrap_or(0);
            let storage_series = storage_series_by_shard.get(&shard).copied().unwrap_or(0);
            let repair_mismatches_total = shard_repair_mismatches.get(&shard).copied().unwrap_or(0);
            let repair_series_gap_total = shard_repair_series_gap.get(&shard).copied().unwrap_or(0);
            let repair_point_gap_total = shard_repair_point_gap.get(&shard).copied().unwrap_or(0);
            let repair_rows_inserted_total = shard_repair_rows.get(&shard).copied().unwrap_or(0);
            let handoff_pending_rows = handoff_pending_by_shard.get(&shard).copied().unwrap_or(0);

            let ingest_ratio = pressure_ratio(ingest_rows_total, total_ingest, total_shard_slots);
            let query_ratio =
                pressure_ratio(query_shard_hits_total, total_query, total_shard_slots);
            let storage_ratio = pressure_ratio(storage_series, total_storage, total_shard_slots);
            let repair_ratio = pressure_ratio(
                handoff_pending_rows
                    .saturating_add(repair_point_gap_total)
                    .saturating_add(repair_rows_inserted_total),
                total_repair,
                total_shard_slots,
            );
            let pressure_score =
                ingest_ratio * 3.0 + query_ratio * 2.5 + storage_ratio * 1.5 + repair_ratio * 2.5;
            let movement_cost_score =
                ingest_ratio * 2.5 + query_ratio * 2.0 + storage_ratio * 1.0 + repair_ratio * 1.5;
            let skew_factor = ingest_ratio
                .max(query_ratio)
                .max(storage_ratio)
                .max(repair_ratio);
            Ok(HotShardSnapshot {
                shard,
                ingest_rows_total,
                query_shard_hits_total,
                storage_series,
                repair_mismatches_total,
                repair_series_gap_total,
                repair_point_gap_total,
                repair_rows_inserted_total,
                handoff_pending_rows,
                pressure_score,
                movement_cost_score,
                skew_factor,
                recommend_move: skew_factor >= SHARD_SKEW_THRESHOLD && handoff_pending_rows == 0,
            })
        })
        .collect::<Result<Vec<_>, tsink::QueryBudgetError>>()?;
    hotspot_observe_intermediate(execution, hot_shards.len())?;
    sort_hot_shards_controlled(&mut hot_shards, execution)?;
    let skewed_shards = hot_shards
        .iter()
        .filter(|item| item.skew_factor >= SHARD_SKEW_THRESHOLD)
        .count();
    let max_shard_score = hot_shards
        .iter()
        .map(|item| item.pressure_score)
        .fold(0.0, f64::max);
    hot_shards.truncate(top_n);

    let mut tenant_ingest = BTreeMap::<String, u64>::new();
    let mut tenant_query_requests = BTreeMap::<String, u64>::new();
    let mut tenant_query_units = BTreeMap::<String, u64>::new();
    let mut tenant_repair_rows = BTreeMap::<String, u64>::new();
    for tenant in &tracker.tenants {
        hotspot_checkpoint(execution)?;
        tenant_ingest.insert(tenant.tenant_id.clone(), tenant.ingest_rows_total);
        tenant_query_requests.insert(tenant.tenant_id.clone(), tenant.query_requests_total);
        tenant_query_units.insert(tenant.tenant_id.clone(), tenant.query_units_total);
        tenant_repair_rows.insert(tenant.tenant_id.clone(), tenant.repair_rows_inserted_total);
        hotspot_observe_intermediate(execution, tenant_ingest.len())?;
    }

    let tenant_ids = tenant_union_controlled(
        &tenant_ingest,
        &tenant_query_requests,
        &tenant_query_units,
        &storage_series_by_tenant,
        &tenant_repair_rows,
        execution,
    )?;
    let total_tenant_slots = tenant_ids.len().max(1);
    let total_tenant_ingest = tenant_ingest.values().copied().sum::<u64>();
    let total_tenant_query_requests = tenant_query_requests.values().copied().sum::<u64>();
    let total_tenant_query_units = tenant_query_units.values().copied().sum::<u64>();
    let total_tenant_storage = storage_series_by_tenant.values().copied().sum::<u64>();
    let total_tenant_repair = tenant_repair_rows.values().copied().sum::<u64>();

    let mut tenant_hotspots = tenant_ids
        .into_iter()
        .filter(|tenant_id| {
            tenant_scope
                .as_ref()
                .is_none_or(|scope| scope.as_str() == tenant_id.as_str())
        })
        .map(|tenant_id| {
            hotspot_checkpoint(execution)?;
            let ingest_rows_total = tenant_ingest.get(&tenant_id).copied().unwrap_or(0);
            let query_requests_total = tenant_query_requests.get(&tenant_id).copied().unwrap_or(0);
            let query_units_total = tenant_query_units.get(&tenant_id).copied().unwrap_or(0);
            let storage_series = storage_series_by_tenant
                .get(&tenant_id)
                .copied()
                .unwrap_or(0);
            let repair_rows_inserted_total =
                tenant_repair_rows.get(&tenant_id).copied().unwrap_or(0);
            let ingest_ratio =
                pressure_ratio(ingest_rows_total, total_tenant_ingest, total_tenant_slots);
            let query_request_ratio = pressure_ratio(
                query_requests_total,
                total_tenant_query_requests,
                total_tenant_slots,
            );
            let query_units_ratio = pressure_ratio(
                query_units_total,
                total_tenant_query_units,
                total_tenant_slots,
            );
            let storage_ratio =
                pressure_ratio(storage_series, total_tenant_storage, total_tenant_slots);
            let repair_ratio = pressure_ratio(
                repair_rows_inserted_total,
                total_tenant_repair,
                total_tenant_slots,
            );
            let pressure_score = ingest_ratio * 3.0
                + query_request_ratio * 1.5
                + query_units_ratio * 2.0
                + storage_ratio * 1.5
                + repair_ratio * 2.0;
            let skew_factor = ingest_ratio
                .max(query_request_ratio)
                .max(query_units_ratio)
                .max(storage_ratio)
                .max(repair_ratio);
            Ok(TenantHotspotSnapshot {
                tenant_id,
                ingest_rows_total,
                query_requests_total,
                query_units_total,
                storage_series,
                repair_rows_inserted_total,
                pressure_score,
                skew_factor,
            })
        })
        .collect::<Result<Vec<_>, tsink::QueryBudgetError>>()?;
    hotspot_observe_intermediate(execution, tenant_hotspots.len())?;
    sort_tenant_hotspots_controlled(&mut tenant_hotspots, execution)?;
    let skewed_tenants = tenant_hotspots
        .iter()
        .filter(|item| item.skew_factor >= TENANT_SKEW_THRESHOLD)
        .count();
    let max_tenant_score = tenant_hotspots
        .iter()
        .map(|item| item.pressure_score)
        .fold(0.0, f64::max);
    tenant_hotspots.truncate(top_n);

    hotspot_checkpoint(execution)?;
    Ok(ClusterHotspotSnapshot {
        generated_unix_ms: tracker.generated_unix_ms,
        hot_shards,
        tenant_hotspots,
        skewed_shards,
        skewed_tenants,
        max_shard_score,
        max_tenant_score,
    })
}

fn saturating_u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn modeled_vec_capacity_bytes<T>(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    saturating_u64_from_usize(capacity)
        .saturating_mul(saturating_u64_from_usize(std::mem::size_of::<T>()))
        .saturating_add(HOTSPOT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_string_len_bytes(len: usize) -> u64 {
    if len == 0 {
        return 0;
    }
    saturating_u64_from_usize(len).saturating_add(HOTSPOT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
}

fn modeled_btree_entries_bytes<K, V>(entries: usize) -> u64 {
    saturating_u64_from_usize(entries).saturating_mul(
        saturating_u64_from_usize(std::mem::size_of::<K>())
            .saturating_add(saturating_u64_from_usize(std::mem::size_of::<V>()))
            .saturating_add(HOTSPOT_BTREE_ENTRY_ALLOWANCE_BYTES),
    )
}

fn modeled_hotspot_peak_bytes(
    tracker: &HotspotTracker,
    metric_count: usize,
    metric_tenant_string_bytes: u64,
    ring: Option<&ShardRing>,
    control: Option<HotspotControlInput<'_>>,
    top_n: usize,
    execution: Option<&tsink::QueryExecution>,
) -> Result<u64, tsink::QueryBudgetError> {
    hotspot_checkpoint(execution)?;
    let tracker_shards = tracker.shards.len();
    let tracker_tenants = tracker.tenants.len();
    let transition_count = control.map_or(0, HotspotControlInput::transition_count);

    let mut tracker_tenant_string_bytes = 0u64;
    for tenant in tracker.tenants.keys() {
        hotspot_checkpoint(execution)?;
        tracker_tenant_string_bytes =
            tracker_tenant_string_bytes.saturating_add(modeled_string_len_bytes(tenant.len()));
    }

    let tracker_snapshot =
        modeled_vec_capacity_bytes::<HotspotShardCountersSnapshot>(tracker_shards)
            .saturating_add(modeled_vec_capacity_bytes::<HotspotTenantCountersSnapshot>(
                tracker_tenants,
            ))
            .saturating_add(tracker_tenant_string_bytes);

    // The transform retains six shard counter maps beside the storage and handoff maps. The union
    // vector and full pre-truncation result are modeled simultaneously.
    let shard_slots = tracker_shards
        .saturating_add(metric_count)
        .saturating_add(transition_count);
    let shard_work = modeled_btree_entries_bytes::<u32, u64>(tracker_shards.saturating_mul(6))
        .saturating_add(modeled_btree_entries_bytes::<u32, u64>(metric_count))
        .saturating_add(modeled_btree_entries_bytes::<u32, u64>(transition_count))
        .saturating_add(modeled_vec_capacity_bytes::<u32>(shard_slots))
        .saturating_add(modeled_vec_capacity_bytes::<HotShardSnapshot>(shard_slots));

    // Tenant counters are cloned into four tracker maps plus the storage map. Each map owns its
    // String key; the union and full result own another copy before top-N truncation.
    let tenant_slots = tracker_tenants.saturating_add(metric_count);
    let tenant_key_copies = tracker_tenant_string_bytes
        .saturating_mul(6)
        .saturating_add(metric_tenant_string_bytes.saturating_mul(3));
    let tenant_work = modeled_btree_entries_bytes::<String, u64>(tracker_tenants.saturating_mul(4))
        .saturating_add(modeled_btree_entries_bytes::<String, u64>(metric_count))
        .saturating_add(modeled_vec_capacity_bytes::<String>(tenant_slots))
        .saturating_add(modeled_vec_capacity_bytes::<TenantHotspotSnapshot>(
            tenant_slots,
        ))
        .saturating_add(tenant_key_copies);

    Ok(tracker_snapshot
        .saturating_add(shard_work)
        .saturating_add(tenant_work)
        .saturating_add(modeled_vec_capacity_bytes::<HotShardSnapshot>(top_n))
        .saturating_add(modeled_vec_capacity_bytes::<TenantHotspotSnapshot>(top_n))
        .saturating_add(
            saturating_u64_from_usize(usize::from(ring.is_some()))
                .saturating_mul(HOTSPOT_COLLECTION_ALLOCATION_ALLOWANCE_BYTES),
        )
        .saturating_add(HOTSPOT_FIXED_SCRATCH_BYTES))
}

fn modeled_metric_tenant_string_bytes(
    metrics: &[MetricSeries],
    execution: Option<&tsink::QueryExecution>,
) -> Result<u64, tsink::QueryBudgetError> {
    let mut bytes = 0u64;
    for series in metrics {
        hotspot_checkpoint(execution)?;
        bytes = bytes.saturating_add(modeled_string_len_bytes(
            tenant_id_from_labels(&series.labels).len(),
        ));
    }
    Ok(bytes)
}

fn modeled_hotspot_result_retained_bytes(snapshot: &ClusterHotspotSnapshot) -> u64 {
    modeled_vec_capacity_bytes::<HotShardSnapshot>(snapshot.hot_shards.capacity())
        .saturating_add(modeled_vec_capacity_bytes::<TenantHotspotSnapshot>(
            snapshot.tenant_hotspots.capacity(),
        ))
        .saturating_add(snapshot.tenant_hotspots.iter().fold(0u64, |bytes, tenant| {
            bytes.saturating_add(modeled_string_len_bytes(tenant.tenant_id.capacity()))
        }))
}

fn modeled_hotspot_tracker_snapshot_retained_bytes(snapshot: &HotspotTrackerSnapshot) -> u64 {
    modeled_vec_capacity_bytes::<HotspotShardCountersSnapshot>(snapshot.shards.capacity())
        .saturating_add(modeled_vec_capacity_bytes::<HotspotTenantCountersSnapshot>(
            snapshot.tenants.capacity(),
        ))
        .saturating_add(snapshot.tenants.iter().fold(0u64, |bytes, tenant| {
            bytes.saturating_add(modeled_string_len_bytes(tenant.tenant_id.capacity()))
        }))
}

fn with_hotspot_tracker<T>(mut f: impl FnMut(&mut HotspotTracker) -> T) -> T {
    let lock = HOTSPOT_TRACKER.get_or_init(|| Mutex::new(HotspotTracker::default()));
    let mut guard = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    f(&mut guard)
}

fn record_ingest_rows_on_tracker(
    tracker: &mut HotspotTracker,
    ring: Option<&ShardRing>,
    rows: &[Row],
) {
    for row in rows {
        let tenant_id = tenant_id_from_labels(row.labels()).to_string();
        let tenant = tracker.tenants.entry(tenant_id).or_default();
        tenant.ingest_rows_total = tenant.ingest_rows_total.saturating_add(1);

        if let Some(ring) = ring {
            let shard =
                ring.shard_for_series_id(stable_series_identity_hash(row.metric(), row.labels()));
            let shard_counters = tracker.shards.entry(shard).or_default();
            shard_counters.ingest_rows_total = shard_counters.ingest_rows_total.saturating_add(1);
        }
    }
}

fn hotspot_tracker_snapshot_from_tracker(tracker: &HotspotTracker) -> HotspotTrackerSnapshot {
    hotspot_tracker_snapshot_from_tracker_controlled(tracker, None)
        .expect("an uncontrolled hotspot tracker snapshot cannot fail")
}

fn hotspot_tracker_snapshot_from_tracker_controlled(
    tracker: &HotspotTracker,
    execution: Option<&tsink::QueryExecution>,
) -> Result<HotspotTrackerSnapshot, tsink::QueryBudgetError> {
    let mut shards = Vec::with_capacity(tracker.shards.len());
    for (shard, counters) in &tracker.shards {
        hotspot_checkpoint(execution)?;
        shards.push(HotspotShardCountersSnapshot {
            shard: *shard,
            ingest_rows_total: counters.ingest_rows_total,
            query_requests_total: counters.query_requests_total,
            query_shard_hits_total: counters.query_shard_hits_total,
            repair_mismatches_total: counters.repair_mismatches_total,
            repair_series_gap_total: counters.repair_series_gap_total,
            repair_point_gap_total: counters.repair_point_gap_total,
            repair_rows_inserted_total: counters.repair_rows_inserted_total,
        });
        hotspot_observe_intermediate(execution, shards.len())?;
    }
    hotspot_sort_unstable_by(&mut shards, execution, |left, right| {
        left.shard.cmp(&right.shard)
    })?;

    let mut tenants = Vec::with_capacity(tracker.tenants.len());
    for (tenant_id, counters) in &tracker.tenants {
        hotspot_checkpoint(execution)?;
        tenants.push(HotspotTenantCountersSnapshot {
            tenant_id: tenant_id.clone(),
            ingest_rows_total: counters.ingest_rows_total,
            query_requests_total: counters.query_requests_total,
            query_units_total: counters.query_units_total,
            repair_rows_inserted_total: counters.repair_rows_inserted_total,
        });
        hotspot_observe_intermediate(execution, tenants.len())?;
    }
    hotspot_sort_unstable_by(&mut tenants, execution, |left, right| {
        left.tenant_id.cmp(&right.tenant_id)
    })?;

    Ok(HotspotTrackerSnapshot {
        generated_unix_ms: unix_timestamp_millis(),
        shards,
        tenants,
    })
}

fn tenant_id_from_labels(labels: &[Label]) -> &str {
    labels
        .iter()
        .find(|label| label.name == tenant::TENANT_LABEL)
        .map(|label| label.value.as_str())
        .unwrap_or(tenant::DEFAULT_TENANT_ID)
}

fn normalize_tenant_id(tenant_id: &str) -> String {
    if tenant_id.trim().is_empty() {
        tenant::DEFAULT_TENANT_ID.to_string()
    } else {
        tenant_id.trim().to_string()
    }
}

fn pressure_ratio(value: u64, total: u64, slots: usize) -> f64 {
    if value == 0 || total == 0 || slots == 0 {
        return 0.0;
    }
    let average = total as f64 / slots as f64;
    if average <= 0.0 {
        0.0
    } else {
        value as f64 / average
    }
}

fn hotspot_checkpoint(
    execution: Option<&tsink::QueryExecution>,
) -> Result<(), tsink::QueryBudgetError> {
    execution.map_or(Ok(()), tsink::QueryExecution::checkpoint)
}

fn hotspot_observe_intermediate(
    execution: Option<&tsink::QueryExecution>,
    size: usize,
) -> Result<(), tsink::QueryBudgetError> {
    if let Some(execution) = execution {
        execution.observe_intermediate_vector_size(saturating_u64_from_usize(size))?;
    }
    Ok(())
}

fn hotspot_sort_unstable_by<T>(
    values: &mut [T],
    execution: Option<&tsink::QueryExecution>,
    mut compare: impl FnMut(&T, &T) -> std::cmp::Ordering,
) -> Result<(), tsink::QueryBudgetError> {
    hotspot_observe_intermediate(execution, values.len())?;
    hotspot_checkpoint(execution)?;
    // A fallible in-place heap sort avoids unaccounted sort scratch and can stop immediately at a
    // cancellation or deadline checkpoint instead of waiting for an infallible std sort to finish.
    let len = values.len();
    for root in (0..len / 2).rev() {
        hotspot_sift_down(values, root, len, execution, &mut compare)?;
    }
    for end in (1..len).rev() {
        hotspot_checkpoint(execution)?;
        values.swap(0, end);
        hotspot_sift_down(values, 0, end, execution, &mut compare)?;
    }
    hotspot_checkpoint(execution)
}

fn hotspot_sift_down<T>(
    values: &mut [T],
    mut root: usize,
    end: usize,
    execution: Option<&tsink::QueryExecution>,
    compare: &mut impl FnMut(&T, &T) -> std::cmp::Ordering,
) -> Result<(), tsink::QueryBudgetError> {
    loop {
        hotspot_checkpoint(execution)?;
        let child = root.saturating_mul(2).saturating_add(1);
        if child >= end {
            return Ok(());
        }
        let greater_child = if child + 1 < end
            && compare(&values[child], &values[child + 1]) == std::cmp::Ordering::Less
        {
            child + 1
        } else {
            child
        };
        if compare(&values[root], &values[greater_child]) != std::cmp::Ordering::Less {
            return Ok(());
        }
        values.swap(root, greater_child);
        root = greater_child;
    }
}

fn sort_hot_shards_controlled(
    values: &mut [HotShardSnapshot],
    execution: Option<&tsink::QueryExecution>,
) -> Result<(), tsink::QueryBudgetError> {
    hotspot_sort_unstable_by(values, execution, |left, right| {
        right
            .pressure_score
            .total_cmp(&left.pressure_score)
            .then_with(|| left.shard.cmp(&right.shard))
    })
}

fn sort_tenant_hotspots_controlled(
    values: &mut [TenantHotspotSnapshot],
    execution: Option<&tsink::QueryExecution>,
) -> Result<(), tsink::QueryBudgetError> {
    hotspot_sort_unstable_by(values, execution, |left, right| {
        right
            .pressure_score
            .total_cmp(&left.pressure_score)
            .then_with(|| left.tenant_id.cmp(&right.tenant_id))
    })
}

fn shard_union_controlled(
    ingest: &BTreeMap<u32, u64>,
    query: &BTreeMap<u32, u64>,
    storage: &BTreeMap<u32, u64>,
    handoff: &BTreeMap<u32, u64>,
    repair: &BTreeMap<u32, u64>,
    execution: Option<&tsink::QueryExecution>,
) -> Result<Vec<u32>, tsink::QueryBudgetError> {
    let mut keys = Vec::new();
    for key in ingest.keys() {
        hotspot_checkpoint(execution)?;
        keys.push(*key);
        hotspot_observe_intermediate(execution, keys.len())?;
    }
    for key in query.keys() {
        hotspot_checkpoint(execution)?;
        if !keys.contains(key) {
            keys.push(*key);
            hotspot_observe_intermediate(execution, keys.len())?;
        }
    }
    for key in storage.keys() {
        hotspot_checkpoint(execution)?;
        if !keys.contains(key) {
            keys.push(*key);
            hotspot_observe_intermediate(execution, keys.len())?;
        }
    }
    for key in handoff.keys() {
        hotspot_checkpoint(execution)?;
        if !keys.contains(key) {
            keys.push(*key);
            hotspot_observe_intermediate(execution, keys.len())?;
        }
    }
    for key in repair.keys() {
        hotspot_checkpoint(execution)?;
        if !keys.contains(key) {
            keys.push(*key);
            hotspot_observe_intermediate(execution, keys.len())?;
        }
    }
    hotspot_sort_unstable_by(&mut keys, execution, Ord::cmp)?;
    Ok(keys)
}

fn tenant_union_controlled(
    ingest: &BTreeMap<String, u64>,
    query_requests: &BTreeMap<String, u64>,
    query_units: &BTreeMap<String, u64>,
    storage: &BTreeMap<String, u64>,
    repair: &BTreeMap<String, u64>,
    execution: Option<&tsink::QueryExecution>,
) -> Result<Vec<String>, tsink::QueryBudgetError> {
    let mut keys = Vec::new();
    for key in ingest.keys() {
        hotspot_checkpoint(execution)?;
        keys.push(key.clone());
        hotspot_observe_intermediate(execution, keys.len())?;
    }
    for key in query_requests.keys() {
        hotspot_checkpoint(execution)?;
        if !keys.contains(key) {
            keys.push(key.clone());
            hotspot_observe_intermediate(execution, keys.len())?;
        }
    }
    for key in query_units.keys() {
        hotspot_checkpoint(execution)?;
        if !keys.contains(key) {
            keys.push(key.clone());
            hotspot_observe_intermediate(execution, keys.len())?;
        }
    }
    for key in storage.keys() {
        hotspot_checkpoint(execution)?;
        if !keys.contains(key) {
            keys.push(key.clone());
            hotspot_observe_intermediate(execution, keys.len())?;
        }
    }
    for key in repair.keys() {
        hotspot_checkpoint(execution)?;
        if !keys.contains(key) {
            keys.push(key.clone());
            hotspot_observe_intermediate(execution, keys.len())?;
        }
    }
    hotspot_sort_unstable_by(&mut keys, execution, |left, right| left.cmp(right))?;
    Ok(keys)
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsink::{
        QueryBudget, QueryBudgetError, QueryBudgetLimits, QueryCancellationToken, QueryLimitReason,
        QueryWorkLimits,
    };

    fn tenant_counters<'a>(
        snapshot: &'a HotspotTrackerSnapshot,
        tenant_id: &str,
    ) -> Option<&'a HotspotTenantCountersSnapshot> {
        snapshot
            .tenants
            .iter()
            .find(|tenant| tenant.tenant_id == tenant_id)
    }

    #[test]
    fn process_global_tracker_combines_logically_independent_server_activity() {
        const SERVER_A_TENANT: &str = "hotspot-characterization-server-a";
        const SERVER_B_TENANT: &str = "hotspot-characterization-server-b";

        let before = hotspot_tracker_snapshot();
        let before_a = tenant_counters(&before, SERVER_A_TENANT)
            .map(|tenant| (tenant.query_requests_total, tenant.query_units_total))
            .unwrap_or_default();
        let before_b = tenant_counters(&before, SERVER_B_TENANT)
            .map(|tenant| (tenant.query_requests_total, tenant.query_units_total))
            .unwrap_or_default();

        // The producer API has no instance identity. These calls model activity from two
        // otherwise-independent server runtimes in the same process.
        record_tenant_query(SERVER_A_TENANT, 2, 3);
        record_tenant_query(SERVER_B_TENANT, 5, 7);

        let combined = hotspot_tracker_snapshot();
        let server_a = tenant_counters(&combined, SERVER_A_TENANT)
            .expect("server A identity must be retained");
        let server_b = tenant_counters(&combined, SERVER_B_TENANT)
            .expect("server B identity must be retained");
        assert_eq!(
            (server_a.query_requests_total, server_a.query_units_total),
            (before_a.0.saturating_add(2), before_a.1.saturating_add(3))
        );
        assert_eq!(
            (server_b.query_requests_total, server_b.query_units_total),
            (before_b.0.saturating_add(5), before_b.1.saturating_add(7))
        );
    }

    #[test]
    fn process_global_tracker_retains_inactive_tenant_identity_and_totals() {
        const RETAINED_TENANT: &str = "hotspot-characterization-retained";
        const LATER_TENANT: &str = "hotspot-characterization-later";

        record_tenant_query(RETAINED_TENANT, 4, 9);
        let first = hotspot_tracker_snapshot();
        let retained_before = tenant_counters(&first, RETAINED_TENANT)
            .expect("the first identity must be present")
            .clone();

        record_tenant_query(LATER_TENANT, 1, 1);
        let later = hotspot_tracker_snapshot();
        let retained_after = tenant_counters(&later, RETAINED_TENANT)
            .expect("inactive identity must remain present");
        assert_eq!(retained_after, &retained_before);
        assert!(
            tenant_counters(&later, LATER_TENANT).is_some(),
            "new activity must grow the retained identity set without removing the old identity"
        );
    }

    #[test]
    fn tenant_scope_filters_output_after_global_pressure_denominators() {
        let scoped_tenant = HotspotTenantCountersSnapshot {
            tenant_id: "scope-a".to_string(),
            ingest_rows_total: 100,
            query_requests_total: 0,
            query_units_total: 0,
            repair_rows_inserted_total: 0,
        };
        let scoped_only = HotspotTrackerSnapshot {
            generated_unix_ms: 1,
            shards: Vec::new(),
            tenants: vec![scoped_tenant.clone()],
        };
        let with_other_tenant = HotspotTrackerSnapshot {
            generated_unix_ms: 1,
            shards: Vec::new(),
            tenants: vec![
                scoped_tenant,
                HotspotTenantCountersSnapshot {
                    tenant_id: "scope-b".to_string(),
                    ingest_rows_total: 1,
                    query_requests_total: 0,
                    query_units_total: 0,
                    repair_rows_inserted_total: 0,
                },
            ],
        };

        let isolated = build_cluster_hotspot_snapshot_from_tracker_controlled(
            &scoped_only,
            &[],
            None,
            None,
            Some("scope-a"),
            DEFAULT_TOP_N,
            None,
        )
        .expect("isolated scoped snapshot should build");
        let global = build_cluster_hotspot_snapshot_from_tracker_controlled(
            &with_other_tenant,
            &[],
            None,
            None,
            Some("scope-a"),
            DEFAULT_TOP_N,
            None,
        )
        .expect("global scoped snapshot should build");

        assert_eq!(isolated.tenant_hotspots.len(), 1);
        assert_eq!(global.tenant_hotspots.len(), 1);
        assert_eq!(
            global.tenant_hotspots[0].ingest_rows_total,
            isolated.tenant_hotspots[0].ingest_rows_total
        );
        assert!(
            global.tenant_hotspots[0].pressure_score > isolated.tenant_hotspots[0].pressure_score,
            "an unreturned global identity currently changes the scoped tenant's denominator"
        );
    }

    #[test]
    fn default_top_eight_truncation_preserves_full_set_skew_and_max_aggregates() {
        let tenants = (0..10)
            .map(|index| {
                let (ingest, requests, units, repair) = match index {
                    0 | 1 => (1, 0, 0, 0),
                    2 | 3 => (0, 1, 0, 0),
                    4 | 5 => (0, 0, 1, 0),
                    6 | 7 => (0, 0, 0, 1),
                    8 | 9 => (0, 0, 0, 0),
                    _ => unreachable!("fixture contains exactly ten tenants"),
                };
                HotspotTenantCountersSnapshot {
                    tenant_id: format!("aggregate-{index:02}"),
                    ingest_rows_total: ingest,
                    query_requests_total: requests,
                    query_units_total: units,
                    repair_rows_inserted_total: repair,
                }
            })
            .collect::<Vec<_>>();
        let tracker = HotspotTrackerSnapshot {
            generated_unix_ms: 1,
            shards: Vec::new(),
            tenants,
        };
        let metrics = [8, 9]
            .into_iter()
            .map(|index| MetricSeries {
                name: format!("aggregate_storage_{index}"),
                labels: vec![Label::new(
                    tenant::TENANT_LABEL,
                    format!("aggregate-{index:02}"),
                )],
            })
            .collect::<Vec<_>>();

        let default_top = build_cluster_hotspot_snapshot_from_tracker_controlled(
            &tracker,
            &metrics,
            None,
            None,
            None,
            DEFAULT_TOP_N,
            None,
        )
        .expect("default top-eight snapshot should build");
        assert_eq!(default_top.tenant_hotspots.len(), DEFAULT_TOP_N);
        assert_eq!(
            default_top.skewed_tenants, 10,
            "the skew aggregate is computed before truncating ten identities to eight"
        );
        assert!((default_top.max_tenant_score - 15.0).abs() < f64::EPSILON);

        let aggregates_without_rows = build_cluster_hotspot_snapshot_from_tracker_controlled(
            &tracker, &metrics, None, None, None, 0, None,
        )
        .expect("zero-row snapshot should still compute aggregates");
        assert!(aggregates_without_rows.tenant_hotspots.is_empty());
        assert_eq!(
            aggregates_without_rows.skewed_tenants,
            default_top.skewed_tenants
        );
        assert_eq!(
            aggregates_without_rows.max_tenant_score,
            default_top.max_tenant_score
        );
    }

    #[test]
    fn accounted_hotspot_honors_cancellation_without_leaking_a_query_slot() {
        let budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("test budget should build");
        let cancellation = QueryCancellationToken::new();
        let execution = budget
            .begin_query_with(QueryWorkLimits::default(), cancellation.clone())
            .expect("test query should admit");
        cancellation.cancel();

        let error =
            build_cluster_hotspot_snapshot_with_execution(&[], None, None, None, &execution)
                .expect_err("a cancelled hotspot transform must stop");
        assert!(matches!(error, QueryBudgetError::Cancelled));
        drop(execution);

        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.cancellations_total, 1);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn metrics_control_projection_preserves_active_handoff_pressure() {
        let control = ControlHotspotSnapshot {
            handoff_shards: vec![
                crate::cluster::control::ControlHotspotShardSnapshot {
                    shard: 3,
                    pending_rows: 11,
                },
                crate::cluster::control::ControlHotspotShardSnapshot {
                    shard: 3,
                    pending_rows: 7,
                },
            ],
        };
        let snapshot = build_cluster_hotspot_snapshot_from_tracker_controlled(
            &HotspotTrackerSnapshot {
                generated_unix_ms: 1,
                shards: Vec::new(),
                tenants: Vec::new(),
            },
            &[],
            None,
            Some(HotspotControlInput::Metrics(&control)),
            None,
            DEFAULT_TOP_N,
            None,
        )
        .expect("metrics-only control input should transform");

        assert_eq!(snapshot.hot_shards.len(), 1);
        assert_eq!(snapshot.hot_shards[0].shard, 3);
        assert_eq!(snapshot.hot_shards[0].handoff_pending_rows, 18);
    }

    #[test]
    fn hotspot_transform_observes_intermediate_map_and_union_sizes() {
        let budget = QueryBudget::new(QueryBudgetLimits {
            per_query: QueryWorkLimits {
                max_intermediate_vector_size: Some(2),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        })
        .expect("test budget should build");
        let execution = budget.begin_query().expect("test query should admit");
        let tracker = HotspotTrackerSnapshot {
            generated_unix_ms: 1,
            shards: Vec::new(),
            tenants: ["a", "b", "c"]
                .into_iter()
                .map(|tenant_id| HotspotTenantCountersSnapshot {
                    tenant_id: tenant_id.to_string(),
                    ingest_rows_total: 1,
                    query_requests_total: 0,
                    query_units_total: 0,
                    repair_rows_inserted_total: 0,
                })
                .collect(),
        };

        let error = build_cluster_hotspot_snapshot_from_tracker_controlled(
            &tracker,
            &[],
            None,
            None,
            None,
            DEFAULT_TOP_N,
            Some(&execution),
        )
        .expect_err("a three-entry intermediate must exceed a two-entry limit");
        match error {
            QueryBudgetError::LimitExceeded(exceeded) => {
                assert_eq!(exceeded.reason, QueryLimitReason::IntermediateVectorSize);
            }
            other => panic!("unexpected hotspot error: {other}"),
        }
        assert_eq!(execution.snapshot().intermediate_vector_size, 2);
        drop(execution);

        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.intermediate_vector_size_rejections_total, 1);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn rebalance_hotspot_tracker_input_enforces_exact_peak_before_clone() {
        fn budget_with_memory_limit(limit: Option<u64>) -> QueryBudget {
            QueryBudget::new(QueryBudgetLimits {
                max_concurrent_queries: Some(1),
                max_shared_memory_bytes: limit,
                per_query: QueryWorkLimits {
                    max_memory_bytes: limit,
                    ..QueryWorkLimits::default()
                },
            })
            .expect("hotspot producer test budget should build")
        }

        fn build_from_local_tracker(
            tracker: &HotspotTracker,
            metrics: &[MetricSeries],
            control: &ControlHotspotSnapshot,
            execution: &tsink::QueryExecution,
        ) -> Result<AccountedRebalanceHotspotSnapshot, QueryBudgetError> {
            let metric_tenant_string_bytes =
                modeled_metric_tenant_string_bytes(metrics, Some(execution))?;
            let control = Some(HotspotControlInput::Metrics(control));
            let (tracker, reservation) = prepare_rebalance_hotspot_tracker_with_execution(
                tracker,
                metrics.len(),
                metric_tenant_string_bytes,
                None,
                control,
                execution,
            )?;
            finish_rebalance_hotspot_snapshot_with_execution(
                tracker,
                reservation,
                metrics,
                None,
                control,
                None,
                execution,
            )
        }

        let mut tracker = HotspotTracker::default();
        tracker.shards.insert(
            7,
            HotspotShardCounters {
                ingest_rows_total: 11,
                query_requests_total: 2,
                query_shard_hits_total: 3,
                repair_mismatches_total: 1,
                repair_series_gap_total: 1,
                repair_point_gap_total: 2,
                repair_rows_inserted_total: 1,
            },
        );
        tracker.tenants.insert(
            "team-a".to_string(),
            HotspotTenantCounters {
                ingest_rows_total: 11,
                query_requests_total: 2,
                query_units_total: 3,
                repair_rows_inserted_total: 1,
            },
        );
        let metrics = vec![MetricSeries {
            name: "hotspot_exact".to_string(),
            labels: vec![Label::new(tenant::TENANT_LABEL, "team-a")],
        }];
        let control = ControlHotspotSnapshot {
            handoff_shards: vec![crate::cluster::control::ControlHotspotShardSnapshot {
                shard: 7,
                pending_rows: 5,
            }],
        };

        let calibration_budget = budget_with_memory_limit(None);
        let calibration = calibration_budget
            .begin_query()
            .expect("calibration query should admit");
        let snapshot = build_from_local_tracker(&tracker, &metrics, &control, &calibration)
            .expect("calibration hotspot should build");
        let retained = calibration.snapshot().memory_reserved_bytes;
        let peak = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(peak >= retained);
        assert!(retained > 0);
        assert_eq!(snapshot.snapshot.hot_shards.len(), 1);
        assert_eq!(snapshot.snapshot.tenant_hotspots.len(), 1);
        drop(snapshot);
        assert_eq!(calibration.snapshot().memory_reserved_bytes, 0);
        drop(calibration);
        assert_eq!(calibration_budget.snapshot().active_queries, 0);
        assert_eq!(
            calibration_budget.snapshot().shared_reserved_memory_bytes,
            0
        );

        let exact_budget = budget_with_memory_limit(Some(peak));
        let exact = exact_budget
            .begin_query()
            .expect("exact query should admit");
        let snapshot = build_from_local_tracker(&tracker, &metrics, &control, &exact)
            .expect("the exact hotspot peak should pass");
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            peak
        );
        drop(snapshot);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);
        assert_eq!(exact_budget.snapshot().active_queries, 0);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let one_under_budget = budget_with_memory_limit(Some(peak.saturating_sub(1)));
        let one_under = one_under_budget
            .begin_query()
            .expect("one-under query should admit");
        let error = build_from_local_tracker(&tracker, &metrics, &control, &one_under)
            .expect_err("one byte below the hotspot peak must reject");
        match error {
            QueryBudgetError::LimitExceeded(exceeded) => {
                assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes);
                assert_eq!(exceeded.current, 0);
                assert_eq!(exceeded.requested, peak);
            }
            other => panic!("unexpected rebalance hotspot error: {other}"),
        }
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        assert_eq!(
            one_under_budget
                .snapshot()
                .peak_shared_reserved_memory_bytes,
            0,
            "the failed reservation must precede the tracker clone"
        );
        drop(one_under);
        let one_under_after = one_under_budget.snapshot();
        assert_eq!(one_under_after.active_queries, 0);
        assert_eq!(one_under_after.shared_reserved_memory_bytes, 0);
        assert_eq!(one_under_after.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn accounted_hotspot_retains_and_releases_its_result_guard() {
        let budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("test budget should build");
        let execution = budget.begin_query().expect("test query should admit");
        let metrics = vec![MetricSeries {
            name: "hotspot_guard".to_string(),
            labels: vec![Label::new(tenant::TENANT_LABEL, "team-a")],
        }];

        let snapshot = build_cluster_hotspot_snapshot_with_execution(
            &metrics,
            None,
            None,
            Some("team-a"),
            &execution,
        )
        .expect("accounted hotspot transform should succeed");
        assert!(
            snapshot
                .tenant_hotspots
                .iter()
                .any(|tenant| tenant.tenant_id == "team-a"),
            "the accounted result must retain the fixture tenant even when the process-global tracker contains other identities"
        );
        assert!(execution.snapshot().memory_reserved_bytes > 0);
        assert!(execution.snapshot().intermediate_vector_size >= 1);
        assert!(budget.snapshot().shared_reserved_memory_bytes > 0);

        drop(snapshot);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
        drop(execution);

        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert!(snapshot.peak_shared_reserved_memory_bytes > 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }
}
