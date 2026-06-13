use crate::cluster::control::ControlState;
use crate::cluster::replication::stable_series_identity_hash;
use crate::cluster::ring::ShardRing;
use crate::tenant;
use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tsink::{Label, MetricSeries, Row};

const DEFAULT_TOP_N: usize = 8;
const SHARD_SKEW_THRESHOLD: f64 = 4.0;
const TENANT_SKEW_THRESHOLD: f64 = 4.0;

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

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterHotspotSnapshot {
    pub generated_unix_ms: u64,
    pub hot_shards: Vec<HotShardSnapshot>,
    pub tenant_hotspots: Vec<TenantHotspotSnapshot>,
    pub skewed_shards: usize,
    pub skewed_tenants: usize,
    pub max_shard_score: f64,
    pub max_tenant_score: f64,
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

pub fn build_cluster_hotspot_snapshot_with_limit(
    metrics: &[MetricSeries],
    ring: Option<&ShardRing>,
    control_state: Option<&ControlState>,
    tenant_scope: Option<&str>,
    top_n: usize,
) -> ClusterHotspotSnapshot {
    let tracker = hotspot_tracker_snapshot();
    let tenant_scope = tenant_scope.map(normalize_tenant_id);

    let mut storage_series_by_shard = BTreeMap::<u32, u64>::new();
    let mut storage_series_by_tenant = BTreeMap::<String, u64>::new();
    for series in metrics {
        let tenant_id = tenant_id_from_labels(&series.labels).to_string();
        *storage_series_by_tenant
            .entry(tenant_id.clone())
            .or_insert(0) += 1;
        if let Some(ring) = ring {
            let shard =
                ring.shard_for_series_id(stable_series_identity_hash(&series.name, &series.labels));
            *storage_series_by_shard.entry(shard).or_insert(0) += 1;
        }
    }

    let mut handoff_pending_by_shard = BTreeMap::<u32, u64>::new();
    if let Some(state) = control_state {
        for transition in &state.transitions {
            if transition.handoff.phase.is_active() {
                *handoff_pending_by_shard
                    .entry(transition.shard)
                    .or_insert(0) = handoff_pending_by_shard
                    .get(&transition.shard)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(transition.handoff.pending_rows);
            }
        }
    }

    let mut shard_ingest = BTreeMap::<u32, u64>::new();
    let mut shard_query = BTreeMap::<u32, u64>::new();
    let mut shard_repair_mismatches = BTreeMap::<u32, u64>::new();
    let mut shard_repair_series_gap = BTreeMap::<u32, u64>::new();
    let mut shard_repair_point_gap = BTreeMap::<u32, u64>::new();
    let mut shard_repair_rows = BTreeMap::<u32, u64>::new();
    for shard in &tracker.shards {
        shard_ingest.insert(shard.shard, shard.ingest_rows_total);
        shard_query.insert(shard.shard, shard.query_shard_hits_total);
        shard_repair_mismatches.insert(shard.shard, shard.repair_mismatches_total);
        shard_repair_series_gap.insert(shard.shard, shard.repair_series_gap_total);
        shard_repair_point_gap.insert(shard.shard, shard.repair_point_gap_total);
        shard_repair_rows.insert(shard.shard, shard.repair_rows_inserted_total);
    }

    let total_shard_slots = shard_union_count(
        &shard_ingest,
        &shard_query,
        &storage_series_by_shard,
        &handoff_pending_by_shard,
        &shard_repair_point_gap,
    );
    let total_ingest = shard_ingest.values().copied().sum::<u64>();
    let total_query = shard_query.values().copied().sum::<u64>();
    let total_storage = storage_series_by_shard.values().copied().sum::<u64>();
    let total_repair = handoff_pending_by_shard
        .values()
        .copied()
        .sum::<u64>()
        .saturating_add(shard_repair_point_gap.values().copied().sum::<u64>())
        .saturating_add(shard_repair_rows.values().copied().sum::<u64>());

    let mut hot_shards = shard_union(
        &shard_ingest,
        &shard_query,
        &storage_series_by_shard,
        &handoff_pending_by_shard,
        &shard_repair_point_gap,
    )
    .into_iter()
    .map(|shard| {
        let ingest_rows_total = shard_ingest.get(&shard).copied().unwrap_or(0);
        let query_shard_hits_total = shard_query.get(&shard).copied().unwrap_or(0);
        let storage_series = storage_series_by_shard.get(&shard).copied().unwrap_or(0);
        let repair_mismatches_total = shard_repair_mismatches.get(&shard).copied().unwrap_or(0);
        let repair_series_gap_total = shard_repair_series_gap.get(&shard).copied().unwrap_or(0);
        let repair_point_gap_total = shard_repair_point_gap.get(&shard).copied().unwrap_or(0);
        let repair_rows_inserted_total = shard_repair_rows.get(&shard).copied().unwrap_or(0);
        let handoff_pending_rows = handoff_pending_by_shard.get(&shard).copied().unwrap_or(0);

        let ingest_ratio = pressure_ratio(ingest_rows_total, total_ingest, total_shard_slots);
        let query_ratio = pressure_ratio(query_shard_hits_total, total_query, total_shard_slots);
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
        HotShardSnapshot {
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
        }
    })
    .collect::<Vec<_>>();
    hot_shards.sort_by(|left, right| {
        right
            .pressure_score
            .total_cmp(&left.pressure_score)
            .then_with(|| left.shard.cmp(&right.shard))
    });
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
        tenant_ingest.insert(tenant.tenant_id.clone(), tenant.ingest_rows_total);
        tenant_query_requests.insert(tenant.tenant_id.clone(), tenant.query_requests_total);
        tenant_query_units.insert(tenant.tenant_id.clone(), tenant.query_units_total);
        tenant_repair_rows.insert(tenant.tenant_id.clone(), tenant.repair_rows_inserted_total);
    }

    let total_tenant_slots = tenant_union_count(
        &tenant_ingest,
        &tenant_query_requests,
        &tenant_query_units,
        &storage_series_by_tenant,
        &tenant_repair_rows,
    );
    let total_tenant_ingest = tenant_ingest.values().copied().sum::<u64>();
    let total_tenant_query_requests = tenant_query_requests.values().copied().sum::<u64>();
    let total_tenant_query_units = tenant_query_units.values().copied().sum::<u64>();
    let total_tenant_storage = storage_series_by_tenant.values().copied().sum::<u64>();
    let total_tenant_repair = tenant_repair_rows.values().copied().sum::<u64>();

    let tenant_ids = tenant_union(
        &tenant_ingest,
        &tenant_query_requests,
        &tenant_query_units,
        &storage_series_by_tenant,
        &tenant_repair_rows,
    );
    let mut tenant_hotspots = tenant_ids
        .into_iter()
        .filter(|tenant_id| {
            tenant_scope
                .as_ref()
                .is_none_or(|scope| scope.as_str() == tenant_id.as_str())
        })
        .map(|tenant_id| {
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
            TenantHotspotSnapshot {
                tenant_id,
                ingest_rows_total,
                query_requests_total,
                query_units_total,
                storage_series,
                repair_rows_inserted_total,
                pressure_score,
                skew_factor,
            }
        })
        .collect::<Vec<_>>();
    tenant_hotspots.sort_by(|left, right| {
        right
            .pressure_score
            .total_cmp(&left.pressure_score)
            .then_with(|| left.tenant_id.cmp(&right.tenant_id))
    });
    let skewed_tenants = tenant_hotspots
        .iter()
        .filter(|item| item.skew_factor >= TENANT_SKEW_THRESHOLD)
        .count();
    let max_tenant_score = tenant_hotspots
        .iter()
        .map(|item| item.pressure_score)
        .fold(0.0, f64::max);
    tenant_hotspots.truncate(top_n);

    ClusterHotspotSnapshot {
        generated_unix_ms: tracker.generated_unix_ms,
        hot_shards,
        tenant_hotspots,
        skewed_shards,
        skewed_tenants,
        max_shard_score,
        max_tenant_score,
    }
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
    let mut shards = tracker
        .shards
        .iter()
        .map(|(shard, counters)| HotspotShardCountersSnapshot {
            shard: *shard,
            ingest_rows_total: counters.ingest_rows_total,
            query_requests_total: counters.query_requests_total,
            query_shard_hits_total: counters.query_shard_hits_total,
            repair_mismatches_total: counters.repair_mismatches_total,
            repair_series_gap_total: counters.repair_series_gap_total,
            repair_point_gap_total: counters.repair_point_gap_total,
            repair_rows_inserted_total: counters.repair_rows_inserted_total,
        })
        .collect::<Vec<_>>();
    shards.sort_by(|left, right| left.shard.cmp(&right.shard));

    let mut tenants = tracker
        .tenants
        .iter()
        .map(|(tenant_id, counters)| HotspotTenantCountersSnapshot {
            tenant_id: tenant_id.clone(),
            ingest_rows_total: counters.ingest_rows_total,
            query_requests_total: counters.query_requests_total,
            query_units_total: counters.query_units_total,
            repair_rows_inserted_total: counters.repair_rows_inserted_total,
        })
        .collect::<Vec<_>>();
    tenants.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));

    HotspotTrackerSnapshot {
        generated_unix_ms: unix_timestamp_millis(),
        shards,
        tenants,
    }
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

fn shard_union_count(
    ingest: &BTreeMap<u32, u64>,
    query: &BTreeMap<u32, u64>,
    storage: &BTreeMap<u32, u64>,
    handoff: &BTreeMap<u32, u64>,
    repair: &BTreeMap<u32, u64>,
) -> usize {
    shard_union(ingest, query, storage, handoff, repair)
        .len()
        .max(1)
}

fn shard_union(
    ingest: &BTreeMap<u32, u64>,
    query: &BTreeMap<u32, u64>,
    storage: &BTreeMap<u32, u64>,
    handoff: &BTreeMap<u32, u64>,
    repair: &BTreeMap<u32, u64>,
) -> Vec<u32> {
    let mut keys = ingest.keys().copied().collect::<Vec<_>>();
    for key in query.keys() {
        if !keys.contains(key) {
            keys.push(*key);
        }
    }
    for key in storage.keys() {
        if !keys.contains(key) {
            keys.push(*key);
        }
    }
    for key in handoff.keys() {
        if !keys.contains(key) {
            keys.push(*key);
        }
    }
    for key in repair.keys() {
        if !keys.contains(key) {
            keys.push(*key);
        }
    }
    keys.sort_unstable();
    keys
}

fn tenant_union_count(
    ingest: &BTreeMap<String, u64>,
    query_requests: &BTreeMap<String, u64>,
    query_units: &BTreeMap<String, u64>,
    storage: &BTreeMap<String, u64>,
    repair: &BTreeMap<String, u64>,
) -> usize {
    tenant_union(ingest, query_requests, query_units, storage, repair)
        .len()
        .max(1)
}

fn tenant_union(
    ingest: &BTreeMap<String, u64>,
    query_requests: &BTreeMap<String, u64>,
    query_units: &BTreeMap<String, u64>,
    storage: &BTreeMap<String, u64>,
    repair: &BTreeMap<String, u64>,
) -> Vec<String> {
    let mut keys = ingest.keys().cloned().collect::<Vec<_>>();
    for key in query_requests.keys() {
        if !keys.contains(key) {
            keys.push(key.clone());
        }
    }
    for key in query_units.keys() {
        if !keys.contains(key) {
            keys.push(key.clone());
        }
    }
    for key in storage.keys() {
        if !keys.contains(key) {
            keys.push(key.clone());
        }
    }
    for key in repair.keys() {
        if !keys.contains(key) {
            keys.push(key.clone());
        }
    }
    keys.sort();
    keys
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
