use crate::cluster::hotspot;
use crate::cluster::membership::MembershipView;
use crate::cluster::replication::stable_series_identity_hash;
use crate::cluster::ring::ShardRing;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use tsink::{MetricSeries, SeriesMatcherOp, SeriesSelection};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPlanOperation {
    SelectSeries,
    ListMetrics,
    SelectPoints,
}

impl ReadPlanOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SelectSeries => "select_series",
            Self::ListMetrics => "list_metrics",
            Self::SelectPoints => "select_points",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPlanOwnerMode {
    AllReplicas,
    PrimaryOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPlanTarget {
    pub node_id: String,
    pub endpoint: String,
    pub shards: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadExecutionPlan {
    pub operation: ReadPlanOperation,
    pub ring_version: u64,
    pub time_range: Option<(i64, i64)>,
    pub candidate_shards: Vec<u32>,
    pub pruned_shards: u32,
    pub local_shards: Vec<u32>,
    pub remote_targets: Vec<ReadPlanTarget>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadPlannerMetricsSnapshot {
    pub requests_total: u64,
    pub candidate_shards_total: u64,
    pub pruned_shards_total: u64,
    pub local_shards_total: u64,
    pub remote_targets_total: u64,
    pub remote_shards_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPlannerOperationMetricsSnapshot {
    pub operation: String,
    pub requests_total: u64,
    pub candidate_shards_total: u64,
    pub pruned_shards_total: u64,
    pub remote_targets_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPlannerLabeledMetricsSnapshot {
    pub operations: Vec<ReadPlannerOperationMetricsSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPlannerLastPlanSnapshot {
    pub operation: String,
    pub ring_version: u64,
    pub time_range: Option<(i64, i64)>,
    pub candidate_shards: u32,
    pub pruned_shards: u32,
    pub local_shards: u32,
    pub remote_targets: u32,
    pub remote_shards: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadPlannerError {
    MissingShardOwners { shard: u32 },
    MissingOwnerEndpoint { node_id: String },
}

impl fmt::Display for ReadPlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingShardOwners { shard } => {
                write!(f, "read planner failed: shard {shard} has no owner")
            }
            Self::MissingOwnerEndpoint { node_id } => {
                write!(
                    f,
                    "read planner failed: owner node '{node_id}' has no known endpoint"
                )
            }
        }
    }
}

impl std::error::Error for ReadPlannerError {}

#[derive(Debug, Clone)]
pub struct ShardAwareQueryPlanner {
    local_node_id: String,
    ring: ShardRing,
    endpoints_by_node: BTreeMap<String, String>,
}

static READ_PLANNER_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_PLANNER_CANDIDATE_SHARDS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_PLANNER_PRUNED_SHARDS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_PLANNER_LOCAL_SHARDS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_PLANNER_REMOTE_TARGETS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_PLANNER_REMOTE_SHARDS_TOTAL: AtomicU64 = AtomicU64::new(0);

static READ_PLANNER_LABELED_METRICS: OnceLock<Mutex<ReadPlannerLabeledMetrics>> = OnceLock::new();
static READ_PLANNER_LAST_PLANS: OnceLock<Mutex<BTreeMap<String, ReadPlannerLastPlanSnapshot>>> =
    OnceLock::new();

#[derive(Debug, Clone, Default)]
struct ReadPlannerLabeledMetrics {
    operation_requests_total: BTreeMap<String, u64>,
    operation_candidate_shards_total: BTreeMap<String, u64>,
    operation_pruned_shards_total: BTreeMap<String, u64>,
    operation_remote_targets_total: BTreeMap<String, u64>,
}

pub fn read_planner_metrics_snapshot() -> ReadPlannerMetricsSnapshot {
    ReadPlannerMetricsSnapshot {
        requests_total: READ_PLANNER_REQUESTS_TOTAL.load(Ordering::Relaxed),
        candidate_shards_total: READ_PLANNER_CANDIDATE_SHARDS_TOTAL.load(Ordering::Relaxed),
        pruned_shards_total: READ_PLANNER_PRUNED_SHARDS_TOTAL.load(Ordering::Relaxed),
        local_shards_total: READ_PLANNER_LOCAL_SHARDS_TOTAL.load(Ordering::Relaxed),
        remote_targets_total: READ_PLANNER_REMOTE_TARGETS_TOTAL.load(Ordering::Relaxed),
        remote_shards_total: READ_PLANNER_REMOTE_SHARDS_TOTAL.load(Ordering::Relaxed),
    }
}

pub fn read_planner_labeled_metrics_snapshot() -> ReadPlannerLabeledMetricsSnapshot {
    with_read_planner_labeled_metrics(|metrics| {
        let mut operations = metrics
            .operation_requests_total
            .iter()
            .map(
                |(operation, requests_total)| ReadPlannerOperationMetricsSnapshot {
                    operation: operation.clone(),
                    requests_total: *requests_total,
                    candidate_shards_total: metrics
                        .operation_candidate_shards_total
                        .get(operation)
                        .copied()
                        .unwrap_or(0),
                    pruned_shards_total: metrics
                        .operation_pruned_shards_total
                        .get(operation)
                        .copied()
                        .unwrap_or(0),
                    remote_targets_total: metrics
                        .operation_remote_targets_total
                        .get(operation)
                        .copied()
                        .unwrap_or(0),
                },
            )
            .collect::<Vec<_>>();
        operations.sort_by(|left, right| left.operation.cmp(&right.operation));
        ReadPlannerLabeledMetricsSnapshot { operations }
    })
}

pub fn read_planner_last_plans_snapshot() -> Vec<ReadPlannerLastPlanSnapshot> {
    with_read_planner_last_plans(|plans| plans.values().cloned().collect())
}

fn with_read_planner_labeled_metrics<T>(
    mut f: impl FnMut(&mut ReadPlannerLabeledMetrics) -> T,
) -> T {
    let lock = READ_PLANNER_LABELED_METRICS
        .get_or_init(|| Mutex::new(ReadPlannerLabeledMetrics::default()));
    let mut guard = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    f(&mut guard)
}

fn with_read_planner_last_plans<T>(
    mut f: impl FnMut(&mut BTreeMap<String, ReadPlannerLastPlanSnapshot>) -> T,
) -> T {
    let lock = READ_PLANNER_LAST_PLANS.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut guard = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    f(&mut guard)
}

fn record_plan_metrics(plan: &ReadExecutionPlan) {
    let operation = plan.operation.as_str().to_string();
    let candidate_shards =
        u64::from(u32::try_from(plan.candidate_shards.len()).unwrap_or(u32::MAX));
    let local_shards = u64::from(u32::try_from(plan.local_shards.len()).unwrap_or(u32::MAX));
    let remote_targets = u64::from(u32::try_from(plan.remote_targets.len()).unwrap_or(u32::MAX));
    let remote_shards = u64::from(
        u32::try_from(
            plan.remote_targets
                .iter()
                .map(|target| target.shards.len())
                .sum::<usize>(),
        )
        .unwrap_or(u32::MAX),
    );

    READ_PLANNER_REQUESTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    READ_PLANNER_CANDIDATE_SHARDS_TOTAL.fetch_add(candidate_shards, Ordering::Relaxed);
    READ_PLANNER_PRUNED_SHARDS_TOTAL.fetch_add(u64::from(plan.pruned_shards), Ordering::Relaxed);
    READ_PLANNER_LOCAL_SHARDS_TOTAL.fetch_add(local_shards, Ordering::Relaxed);
    READ_PLANNER_REMOTE_TARGETS_TOTAL.fetch_add(remote_targets, Ordering::Relaxed);
    READ_PLANNER_REMOTE_SHARDS_TOTAL.fetch_add(remote_shards, Ordering::Relaxed);

    with_read_planner_labeled_metrics(|metrics| {
        let requests = metrics
            .operation_requests_total
            .entry(operation.clone())
            .or_insert(0);
        *requests = requests.saturating_add(1);

        let candidates = metrics
            .operation_candidate_shards_total
            .entry(operation.clone())
            .or_insert(0);
        *candidates = candidates.saturating_add(candidate_shards);

        let pruned = metrics
            .operation_pruned_shards_total
            .entry(operation.clone())
            .or_insert(0);
        *pruned = pruned.saturating_add(u64::from(plan.pruned_shards));

        let remotes = metrics
            .operation_remote_targets_total
            .entry(operation.clone())
            .or_insert(0);
        *remotes = remotes.saturating_add(remote_targets);
    });

    let snapshot = ReadPlannerLastPlanSnapshot {
        operation: operation.clone(),
        ring_version: plan.ring_version,
        time_range: plan.time_range,
        candidate_shards: u32::try_from(plan.candidate_shards.len()).unwrap_or(u32::MAX),
        pruned_shards: plan.pruned_shards,
        local_shards: u32::try_from(plan.local_shards.len()).unwrap_or(u32::MAX),
        remote_targets: u32::try_from(plan.remote_targets.len()).unwrap_or(u32::MAX),
        remote_shards: u32::try_from(
            plan.remote_targets
                .iter()
                .map(|target| target.shards.len())
                .sum::<usize>(),
        )
        .unwrap_or(u32::MAX),
    };
    with_read_planner_last_plans(|plans| {
        plans.insert(operation.clone(), snapshot.clone());
    });
}

impl ShardAwareQueryPlanner {
    pub fn new(
        local_node_id: String,
        ring: ShardRing,
        membership: &MembershipView,
    ) -> Result<Self, String> {
        let endpoints_by_node = membership
            .nodes
            .iter()
            .map(|node| (node.id.clone(), node.endpoint.clone()))
            .collect::<BTreeMap<_, _>>();
        if !endpoints_by_node.contains_key(&local_node_id) {
            return Err(format!(
                "local node '{local_node_id}' is missing from cluster membership view"
            ));
        }
        Ok(Self {
            local_node_id,
            ring,
            endpoints_by_node,
        })
    }

    pub fn reconfigured_for_topology(
        &self,
        ring: ShardRing,
        membership: &MembershipView,
    ) -> Result<Self, String> {
        Self::new(self.local_node_id.clone(), ring, membership)
    }

    #[allow(dead_code)]
    pub fn plan_select_series(
        &self,
        selection: &SeriesSelection,
        ring_version: u64,
    ) -> Result<ReadExecutionPlan, ReadPlannerError> {
        self.plan_select_series_with_owner_mode(
            selection,
            ring_version,
            ReadPlanOwnerMode::AllReplicas,
        )
    }

    pub fn plan_select_series_with_owner_mode(
        &self,
        selection: &SeriesSelection,
        ring_version: u64,
        owner_mode: ReadPlanOwnerMode,
    ) -> Result<ReadExecutionPlan, ReadPlannerError> {
        let candidate_shards = candidate_shards_for_selection(selection, self.ring.shard_count());
        self.plan_with_candidates(
            ReadPlanOperation::SelectSeries,
            ring_version,
            selection
                .start
                .zip(selection.end)
                .filter(|(start, end)| end > start),
            candidate_shards,
            owner_mode,
        )
    }

    #[allow(dead_code)]
    pub fn plan_list_metrics(
        &self,
        ring_version: u64,
    ) -> Result<ReadExecutionPlan, ReadPlannerError> {
        self.plan_list_metrics_with_owner_mode(ring_version, ReadPlanOwnerMode::AllReplicas)
    }

    pub fn plan_list_metrics_with_owner_mode(
        &self,
        ring_version: u64,
        owner_mode: ReadPlanOwnerMode,
    ) -> Result<ReadExecutionPlan, ReadPlannerError> {
        self.plan_with_candidates(
            ReadPlanOperation::ListMetrics,
            ring_version,
            None,
            all_shards(self.ring.shard_count()),
            owner_mode,
        )
    }

    #[allow(dead_code)]
    pub fn plan_select_points(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        ring_version: u64,
    ) -> Result<ReadExecutionPlan, ReadPlannerError> {
        self.plan_select_points_with_owner_mode(
            series,
            start,
            end,
            ring_version,
            ReadPlanOwnerMode::PrimaryOnly,
        )
    }

    pub fn plan_select_points_with_owner_mode(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        ring_version: u64,
        owner_mode: ReadPlanOwnerMode,
    ) -> Result<ReadExecutionPlan, ReadPlannerError> {
        self.plan_with_candidates(
            ReadPlanOperation::SelectPoints,
            ring_version,
            Some((start, end)),
            candidate_shards_for_series(series, &self.ring),
            owner_mode,
        )
    }

    fn plan_with_candidates(
        &self,
        operation: ReadPlanOperation,
        ring_version: u64,
        time_range: Option<(i64, i64)>,
        candidate_shards: BTreeSet<u32>,
        owner_mode: ReadPlanOwnerMode,
    ) -> Result<ReadExecutionPlan, ReadPlannerError> {
        let ring_version = ring_version.max(1);
        let candidate_shards = candidate_shards.into_iter().collect::<Vec<_>>();
        let total_shards = self.ring.shard_count();
        let pruned_shards = total_shards.saturating_sub(candidate_shards.len() as u32);

        let mut shards_by_owner: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();
        for shard in &candidate_shards {
            let Some(owners) = self.ring.owners_for_shard(*shard) else {
                return Err(ReadPlannerError::MissingShardOwners { shard: *shard });
            };
            if owners.is_empty() {
                return Err(ReadPlannerError::MissingShardOwners { shard: *shard });
            }

            match owner_mode {
                ReadPlanOwnerMode::AllReplicas => {
                    for owner in owners {
                        shards_by_owner
                            .entry(owner.clone())
                            .or_default()
                            .insert(*shard);
                    }
                }
                ReadPlanOwnerMode::PrimaryOnly => {
                    let owner = owners.first().cloned().expect("checked non-empty");
                    shards_by_owner.entry(owner).or_default().insert(*shard);
                }
            }
        }

        let local_shards = shards_by_owner
            .remove(&self.local_node_id)
            .map(|shards| shards.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();

        let mut remote_targets = Vec::new();
        for (node_id, shards) in shards_by_owner {
            let Some(endpoint) = self.endpoints_by_node.get(&node_id).cloned() else {
                return Err(ReadPlannerError::MissingOwnerEndpoint { node_id });
            };
            remote_targets.push(ReadPlanTarget {
                node_id,
                endpoint,
                shards: shards.into_iter().collect(),
            });
        }

        let plan = ReadExecutionPlan {
            operation,
            ring_version,
            time_range,
            candidate_shards,
            pruned_shards,
            local_shards,
            remote_targets,
        };
        hotspot::record_query_plan(&plan.candidate_shards);
        record_plan_metrics(&plan);
        Ok(plan)
    }
}

fn all_shards(shard_count: u32) -> BTreeSet<u32> {
    (0..shard_count).collect()
}

fn candidate_shards_for_series(series: &[MetricSeries], ring: &ShardRing) -> BTreeSet<u32> {
    series
        .iter()
        .map(|series| {
            let series_hash = stable_series_identity_hash(&series.name, &series.labels);
            ring.shard_for_series_id(series_hash)
        })
        .collect()
}

fn candidate_shards_for_selection(selection: &SeriesSelection, shard_count: u32) -> BTreeSet<u32> {
    if selection_is_definitely_empty(selection) {
        return BTreeSet::new();
    }
    all_shards(shard_count)
}

fn selection_is_definitely_empty(selection: &SeriesSelection) -> bool {
    let mut equals = BTreeMap::<String, String>::new();
    let mut not_equals = BTreeMap::<String, BTreeSet<String>>::new();

    if let Some(metric) = selection.metric.as_ref() {
        equals.insert("__name__".to_string(), metric.clone());
    }

    for matcher in &selection.matchers {
        let name = matcher.name.clone();
        match matcher.op {
            SeriesMatcherOp::Equal => {
                if let Some(existing) = equals.get(&name) {
                    if existing != &matcher.value {
                        return true;
                    }
                }
                equals.insert(name.clone(), matcher.value.clone());
                if not_equals
                    .get(&name)
                    .is_some_and(|values| values.contains(&matcher.value))
                {
                    return true;
                }
            }
            SeriesMatcherOp::NotEqual => {
                if equals
                    .get(&name)
                    .is_some_and(|existing| existing == &matcher.value)
                {
                    return true;
                }
                not_equals
                    .entry(name)
                    .or_default()
                    .insert(matcher.value.clone());
            }
            SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {}
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::ClusterConfig;
    use crate::cluster::membership::MembershipView;
    use tsink::{Label, SeriesMatcher};

    fn build_planner() -> ShardAwareQueryPlanner {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: vec![
                "node-b@127.0.0.1:9302".to_string(),
                "node-c@127.0.0.1:9303".to_string(),
            ],
            shards: 64,
            replication_factor: 1,
            ..ClusterConfig::default()
        };
        let membership = MembershipView::from_config(&cfg).expect("membership should build");
        let ring = ShardRing::build(cfg.shards, cfg.replication_factor, &membership)
            .expect("ring should build");
        ShardAwareQueryPlanner::new("node-a".to_string(), ring, &membership)
            .expect("planner should build")
    }

    #[test]
    fn select_points_plan_prunes_to_series_shards() {
        let planner = build_planner();
        let series = vec![
            MetricSeries {
                name: "cpu_usage".to_string(),
                labels: vec![Label::new("host", "a"), Label::new("job", "app")],
            },
            MetricSeries {
                name: "cpu_usage".to_string(),
                labels: vec![Label::new("host", "b"), Label::new("job", "app")],
            },
        ];

        let plan = planner
            .plan_select_points(&series, 10, 20, 1)
            .expect("plan should succeed");
        assert_eq!(plan.operation, ReadPlanOperation::SelectPoints);
        assert!(!plan.candidate_shards.is_empty());
        assert!(plan.candidate_shards.len() <= series.len());
        assert_eq!(
            usize::try_from(plan.pruned_shards).expect("fits usize"),
            64usize.saturating_sub(plan.candidate_shards.len())
        );

        let assigned_shards = plan.local_shards.len()
            + plan
                .remote_targets
                .iter()
                .map(|target| target.shards.len())
                .sum::<usize>();
        assert_eq!(assigned_shards, plan.candidate_shards.len());
    }

    #[test]
    fn conflicting_equal_matchers_prune_all_shards() {
        let planner = build_planner();
        let selection = SeriesSelection::new()
            .with_metric("metric_a")
            .with_matchers(vec![
                SeriesMatcher::equal("__name__", "metric_a"),
                SeriesMatcher::equal("__name__", "metric_b"),
            ]);

        let plan = planner
            .plan_select_series(&selection, 1)
            .expect("plan should succeed");
        assert!(plan.candidate_shards.is_empty());
        assert!(plan.local_shards.is_empty());
        assert!(plan.remote_targets.is_empty());
        assert_eq!(plan.pruned_shards, 64);
    }

    #[test]
    fn select_series_plan_is_deterministic() {
        let planner = build_planner();
        let selection = SeriesSelection::new()
            .with_metric("http_requests_total")
            .with_matcher(SeriesMatcher::regex_match("instance", "api-[ab]+"));

        let left = planner
            .plan_select_series(&selection, 9)
            .expect("first plan should succeed");
        let right = planner
            .plan_select_series(&selection, 9)
            .expect("second plan should succeed");
        assert_eq!(left, right);
    }

    #[test]
    fn list_metrics_plan_is_stable_and_covers_all_shards() {
        let planner = build_planner();
        let plan = planner.plan_list_metrics(5).expect("plan should succeed");
        assert_eq!(plan.operation, ReadPlanOperation::ListMetrics);
        assert_eq!(plan.candidate_shards.len(), 64);
        let coverage = plan.local_shards.len()
            + plan
                .remote_targets
                .iter()
                .map(|target| target.shards.len())
                .sum::<usize>();
        assert_eq!(coverage, 64);
    }
}
