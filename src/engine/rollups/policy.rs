use super::runtime::pending_delete_blocks_rollup_candidate;
use super::*;

pub(super) fn normalize_policy(mut policy: RollupPolicy) -> Result<RollupPolicy> {
    if policy.id.trim().is_empty() {
        return Err(TsinkError::InvalidConfiguration(
            "rollup policy id must not be empty".to_string(),
        ));
    }
    if policy.interval <= 0 {
        return Err(TsinkError::InvalidConfiguration(format!(
            "rollup policy {} interval must be positive",
            policy.id
        )));
    }
    if policy.aggregation == Aggregation::None {
        return Err(TsinkError::InvalidConfiguration(format!(
            "rollup policy {} requires a concrete aggregation",
            policy.id
        )));
    }
    if is_internal_rollup_metric(&policy.metric) {
        return Err(TsinkError::InvalidConfiguration(format!(
            "rollup policy {} cannot target internal rollup metric {}",
            policy.id, policy.metric
        )));
    }
    validate_metric(&policy.metric)?;
    policy.match_labels.sort();
    policy.match_labels.dedup();
    validate_labels(&policy.match_labels)?;
    Ok(policy)
}

pub(super) fn load_rollup_policies(path: Option<&Path>) -> Result<Vec<RollupPolicy>> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source: err,
            });
        }
    };

    let file: PersistedRollupPoliciesFile = serde_json::from_slice(&bytes)?;
    if file.magic != ROLLUP_POLICIES_MAGIC || file.version != ROLLUP_SCHEMA_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported rollup policies file {}",
            path.display()
        )));
    }

    file.policies
        .into_iter()
        .map(normalize_policy)
        .collect::<Result<Vec<_>>>()
}

pub(super) fn persist_rollup_policies(
    path: Option<&Path>,
    policies: &[RollupPolicy],
) -> Result<()> {
    let Some(path) = path else {
        if policies.is_empty() {
            return Ok(());
        }
        return Err(TsinkError::InvalidConfiguration(
            "rollup policies require persistent storage".to_string(),
        ));
    };

    let payload = PersistedRollupPoliciesFile {
        magic: ROLLUP_POLICIES_MAGIC.to_string(),
        version: ROLLUP_SCHEMA_VERSION,
        policies: policies.to_vec(),
    };
    let encoded = serde_json::to_vec_pretty(&payload)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    write_file_atomically_and_sync_parent(path, &encoded)
}

impl RollupQuerySelectionContext<'_> {
    fn rollup_query_candidate(
        self,
        metric: &str,
        labels: &[Label],
        interval: i64,
        aggregation: Aggregation,
        start: i64,
        end: i64,
    ) -> Option<RollupQueryCandidate> {
        if is_internal_rollup_metric(metric) || aggregation == Aggregation::None || interval <= 0 {
            return None;
        }
        let series_id = self.registry.resolve_existing_series_id(metric, labels)?;

        let checkpoints = self.store.state.checkpoints.read();
        let generations = self.store.state.generations.read();
        let pending_delete_invalidations = self.store.state.pending_delete_invalidations.read();
        let source_key = source_series_key(metric, labels);
        self.store
            .state
            .policies
            .read()
            .iter()
            .filter(|policy| {
                policy.interval == interval
                    && policy.aggregation == aggregation
                    && is_bucket_aligned(start, policy)
                    && policy_matches_source(policy, metric, labels)
            })
            .filter_map(|policy| {
                let materialized_through = checkpoints
                    .get(&policy.id)
                    .and_then(|items| items.get(&source_key))
                    .copied()?;
                let covered_end = materialized_through.min(end);
                (covered_end > start
                    && !pending_delete_blocks_rollup_candidate(
                        &pending_delete_invalidations,
                        series_id,
                        &policy.id,
                        start,
                        covered_end,
                    ))
                .then(|| RollupQueryCandidate {
                    policy: policy.clone(),
                    metric: rollup_metric_name(
                        policy,
                        generations.get(&policy.id).copied().unwrap_or(0),
                    ),
                    materialized_through,
                })
            })
            .max_by(|left, right| {
                (
                    left.policy.match_labels.len(),
                    left.materialized_through,
                    &left.policy.id,
                )
                    .cmp(&(
                        right.policy.match_labels.len(),
                        right.materialized_through,
                        &right.policy.id,
                    ))
            })
    }

    fn record_rollup_query_use(self, points_read: usize, partial: bool) {
        self.query_observability
            .rollup_query_plans_total
            .fetch_add(1, Ordering::Relaxed);
        if partial {
            self.query_observability
                .partial_rollup_query_plans_total
                .fetch_add(1, Ordering::Relaxed);
        }
        self.query_observability
            .rollup_points_read_total
            .fetch_add(saturating_u64_from_usize(points_read), Ordering::Relaxed);
    }

    fn rollup_observability_snapshot(self) -> RollupObservabilitySnapshot {
        let policies = self.store.policies_snapshot();
        let checkpoints = self.store.checkpoints_snapshot();
        let policy_stats = self.store.policy_stats_snapshot();
        let max_observed = self
            .source_reads
            .bounded_recency_reference_timestamp()
            .unwrap_or(i64::MIN);

        let policies = policies
            .into_iter()
            .map(|policy| {
                let matched_sources = self
                    .source_reads
                    .matching_rollup_sources_best_effort(&policy);
                let mut min_through = None::<i64>;
                let mut materialized_series = 0u64;
                if let Some(entries) = checkpoints.get(&policy.id) {
                    for source in &matched_sources {
                        if let Some(materialized_through) = entries.get(&source.source_key).copied()
                        {
                            materialized_series = materialized_series.saturating_add(1);
                            min_through = Some(
                                min_through
                                    .map(|current| current.min(materialized_through))
                                    .unwrap_or(materialized_through),
                            );
                        }
                    }
                }
                let runtime = policy_stats.get(&policy.id).cloned().unwrap_or_default();

                RollupPolicyStatus {
                    policy,
                    matched_series: u64::try_from(matched_sources.len()).unwrap_or(u64::MAX),
                    materialized_series,
                    materialized_through: runtime.materialized_through.or(min_through),
                    lag: runtime
                        .materialized_through
                        .or(min_through)
                        .and_then(|through| {
                            (max_observed != i64::MIN)
                                .then_some(max_observed.saturating_sub(through))
                        }),
                    last_run_started_at_ms: runtime.last_run_started_at_ms,
                    last_run_completed_at_ms: runtime.last_run_completed_at_ms,
                    last_run_duration_nanos: runtime.last_run_duration_nanos,
                    last_error: runtime.last_error,
                }
            })
            .collect::<Vec<_>>();

        RollupObservabilitySnapshot {
            worker_runs_total: self
                .rollup_observability
                .worker_runs_total
                .load(Ordering::Relaxed),
            worker_success_total: self
                .rollup_observability
                .worker_success_total
                .load(Ordering::Relaxed),
            worker_errors_total: self
                .rollup_observability
                .worker_errors_total
                .load(Ordering::Relaxed),
            policy_runs_total: self
                .rollup_observability
                .policy_runs_total
                .load(Ordering::Relaxed),
            buckets_materialized_total: self
                .rollup_observability
                .buckets_materialized_total
                .load(Ordering::Relaxed),
            points_materialized_total: self
                .rollup_observability
                .points_materialized_total
                .load(Ordering::Relaxed),
            last_run_duration_nanos: self
                .rollup_observability
                .last_run_duration_nanos
                .load(Ordering::Relaxed),
            policies,
        }
    }
}

impl ChunkStorage {
    pub(in crate::engine) fn apply_rollup_policies_impl(
        &self,
        policies: Vec<RollupPolicy>,
    ) -> Result<RollupObservabilitySnapshot> {
        self.ensure_open()?;
        let state_store = self.rollup_state_store_context();
        if state_store.dir_path().is_none() {
            return Err(TsinkError::InvalidConfiguration(
                "rollup policies require persistent storage (data_path)".to_string(),
            ));
        }

        let mut normalized = Vec::with_capacity(policies.len());
        let mut ids = BTreeSet::new();
        for policy in policies {
            let policy = normalize_policy(policy)?;
            if !ids.insert(policy.id.clone()) {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "duplicate rollup policy id {}",
                    policy.id
                )));
            }
            normalized.push(policy);
        }
        normalized.sort_by(|left, right| left.id.cmp(&right.id));

        let _run_guard = self.rollup_run_coordination_context().run_lock.lock();
        let snapshot = state_store.next_snapshot_for_policies(normalized);
        state_store.persist_snapshot(&snapshot)?;
        state_store.install_snapshot(snapshot);
        self.run_rollup_pipeline_once_locked()?;
        Ok(self.rollup_observability_snapshot())
    }

    pub(in crate::engine) fn rollup_query_candidate(
        &self,
        metric: &str,
        labels: &[Label],
        interval: i64,
        aggregation: Aggregation,
        start: i64,
        end: i64,
    ) -> Option<RollupQueryCandidate> {
        self.rollup_query_selection_context()
            .rollup_query_candidate(metric, labels, interval, aggregation, start, end)
    }

    pub(in crate::engine) fn record_rollup_query_use(&self, points_read: usize, partial: bool) {
        self.rollup_query_selection_context()
            .record_rollup_query_use(points_read, partial);
    }

    pub(in crate::engine) fn rollup_observability_snapshot(&self) -> RollupObservabilitySnapshot {
        self.rollup_query_selection_context()
            .rollup_observability_snapshot()
    }

    #[cfg(test)]
    pub(in crate::engine) fn set_rollup_policy_start_hook<F>(&self, hook: F)
    where
        F: Fn(&RollupPolicy) + Send + Sync + 'static,
    {
        self.rollup_state_store_context()
            .set_policy_start_hook(hook);
    }

    #[cfg(test)]
    pub(in crate::engine) fn clear_rollup_policy_start_hook(&self) {
        self.rollup_state_store_context().clear_policy_start_hook();
    }

    #[cfg(test)]
    pub(in crate::engine) fn set_rollup_state_persist_hook<F>(&self, hook: F)
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        self.rollup_state_store_context()
            .set_state_persist_hook(hook);
    }

    #[cfg(test)]
    pub(in crate::engine) fn clear_rollup_state_persist_hook(&self) {
        self.rollup_state_store_context().clear_state_persist_hook();
    }
}
