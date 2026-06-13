use super::runtime::pending_materialized_through;
use super::*;

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

impl RollupStateStoreContext<'_> {
    fn stage_pending_rollup_materialization(
        self,
        policy_id: &str,
        source_key: &str,
        pending: PendingRollupMaterialization,
    ) -> Result<()> {
        let checkpoints = self.checkpoints_snapshot();
        let generations = self.generations_snapshot();
        let mut pending_materializations = self.pending_materializations_snapshot();
        let pending_delete_invalidations = self.pending_delete_invalidations_snapshot();
        pending_materializations
            .entry(policy_id.to_string())
            .or_default()
            .insert(source_key.to_string(), pending);
        self.persist_state_snapshot(
            &checkpoints,
            &generations,
            &pending_materializations,
            &pending_delete_invalidations,
        )?;
        *self.state.pending_materializations.write() = pending_materializations;
        Ok(())
    }

    fn mark_rollup_checkpoint_in_memory(
        self,
        policy_id: &str,
        source_key: &str,
        materialized_through: i64,
    ) {
        self.state
            .checkpoints
            .write()
            .entry(policy_id.to_string())
            .or_default()
            .insert(source_key.to_string(), materialized_through);
    }

    fn clear_pending_rollup_materialization_in_memory(self, policy_id: &str, source_key: &str) {
        let mut pending_materializations = self.state.pending_materializations.write();
        let remove_policy = pending_materializations
            .get_mut(policy_id)
            .is_some_and(|entries| {
                entries.remove(source_key);
                entries.is_empty()
            });
        if remove_policy {
            pending_materializations.remove(policy_id);
        }
    }

    fn policy_generation(self, policy_id: &str) -> u64 {
        self.state
            .generations
            .read()
            .get(policy_id)
            .copied()
            .unwrap_or(0)
    }

    fn set_rollup_policy_run_state(
        self,
        policy_id: &str,
        report: &PolicyRunReport,
        started_at_ms: u64,
        completed_at_ms: u64,
        duration_nanos: u64,
        error: Option<String>,
    ) {
        let mut stats = self.state.policy_stats.write();
        let state = stats.entry(policy_id.to_string()).or_default();
        state.matched_series = report.matched_series;
        state.materialized_series = report.materialized_series;
        state.materialized_through = report.materialized_through;
        state.last_run_started_at_ms = Some(started_at_ms);
        state.last_run_completed_at_ms = Some(completed_at_ms);
        state.last_run_duration_nanos = duration_nanos;
        state.last_error = error;
    }
}

impl RollupSourceReadContext<'_> {
    fn live_series_ids(
        self,
        candidate_series_ids: Vec<SeriesId>,
        prune_dead: bool,
    ) -> Result<Vec<SeriesId>> {
        self.ops.live_series_ids(candidate_series_ids, prune_dead)
    }

    fn query_tier_plan(self, start: i64, end: i64) -> TieredQueryPlan {
        self.ops.query_tier_plan(start, end)
    }

    fn collect_points_for_series_with_plan(
        self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
    ) -> Result<Vec<DataPoint>> {
        self.ops
            .collect_points_for_series_with_plan(series_id, start, end, plan)
    }

    pub(super) fn bounded_recency_reference_timestamp(self) -> Option<i64> {
        self.ops.bounded_recency_reference_timestamp()
    }

    fn rollup_sources_for_series_ids(
        self,
        policy: &RollupPolicy,
        series_ids: Vec<SeriesId>,
    ) -> Vec<RollupSourceSeries> {
        series_ids
            .into_iter()
            .filter_map(|series_id| {
                let series = self.registry.decode_series_key(series_id)?;
                if !policy_matches_source(policy, &series.metric, &series.labels) {
                    return None;
                }
                Some(RollupSourceSeries {
                    series_id,
                    source_key: source_series_key(&series.metric, &series.labels),
                    labels: series.labels,
                })
            })
            .collect()
    }

    fn matching_rollup_sources(self, policy: &RollupPolicy) -> Result<Vec<RollupSourceSeries>> {
        let candidate_series_ids = self.registry.series_ids_for_metric(&policy.metric);
        let live_series_ids = self.live_series_ids(candidate_series_ids, true)?;
        Ok(self.rollup_sources_for_series_ids(policy, live_series_ids))
    }

    pub(super) fn matching_rollup_sources_best_effort(
        self,
        policy: &RollupPolicy,
    ) -> Vec<RollupSourceSeries> {
        self.matching_rollup_sources(policy).unwrap_or_else(|_| {
            let candidate_series_ids = self.registry.series_ids_for_metric(&policy.metric);
            self.rollup_sources_for_series_ids(policy, candidate_series_ids)
        })
    }
}

impl RollupMaterializedWriteContext<'_> {
    fn insert_rows(self, rows: &[Row]) -> Result<WriteResult> {
        self.ops.insert_rows(rows)
    }
}

impl RollupInvalidationContext<'_> {
    fn rollup_policy_ids_needing_rebuild_for_rows(self, rows: &[Row]) -> BTreeSet<String> {
        if rows.is_empty() {
            return BTreeSet::new();
        }

        let policies = self.store.policies_snapshot();
        if policies.is_empty() {
            return BTreeSet::new();
        }
        let checkpoints = self.store.checkpoints_snapshot();
        let pending_materializations = self.store.pending_materializations_snapshot();
        let generations = self.store.generations_snapshot();

        let mut affected_policy_ids = BTreeSet::new();
        for row in rows {
            if is_internal_rollup_metric(row.metric()) {
                continue;
            }

            let source_key = source_series_key(row.metric(), row.labels());
            let timestamp = row.data_point().timestamp;
            for policy in &policies {
                if !policy_matches_source(policy, row.metric(), row.labels()) {
                    continue;
                }

                let Some(materialized_through) = checkpoints
                    .get(&policy.id)
                    .and_then(|entries| entries.get(&source_key))
                    .copied()
                    .or_else(|| {
                        pending_materialized_through(
                            &pending_materializations,
                            &generations,
                            &policy.id,
                            &source_key,
                        )
                    })
                else {
                    continue;
                };

                if timestamp < materialized_through {
                    affected_policy_ids.insert(policy.id.clone());
                }
            }
        }

        affected_policy_ids
    }
}

fn run_rollup_policy_once(
    store: RollupStateStoreContext<'_>,
    source_reads: RollupSourceReadContext<'_>,
    materialized_writes: RollupMaterializedWriteContext<'_>,
    policy: &RollupPolicy,
    max_observed: i64,
) -> Result<PolicyRunReport> {
    let sources = source_reads.matching_rollup_sources(policy)?;
    let mut report = PolicyRunReport {
        matched_series: u64::try_from(sources.len()).unwrap_or(u64::MAX),
        ..PolicyRunReport::default()
    };

    let Some(stable_end) = aligned_materialized_end(policy, max_observed) else {
        return Ok(report);
    };

    let generation = store.policy_generation(&policy.id);
    let rollup_metric = rollup_metric_name(policy, generation);
    let existing_checkpoints = store
        .state
        .checkpoints
        .read()
        .get(&policy.id)
        .cloned()
        .unwrap_or_default();
    let existing_pending = store
        .state
        .pending_materializations
        .read()
        .get(&policy.id)
        .cloned()
        .unwrap_or_default();
    let mut updated_checkpoints = existing_checkpoints.clone();
    let mut checkpoint_changed = false;

    for source in &sources {
        let checkpoint = existing_checkpoints
            .get(&source.source_key)
            .copied()
            .unwrap_or(i64::MIN);
        let target_end = existing_pending
            .get(&source.source_key)
            .filter(|pending| pending.generation == generation)
            .map(|pending| pending.materialized_through.max(stable_end))
            .unwrap_or(stable_end);
        if checkpoint >= target_end {
            continue;
        }

        let plan = source_reads.query_tier_plan(checkpoint, target_end);
        let raw_points = source_reads.collect_points_for_series_with_plan(
            source.series_id,
            checkpoint,
            target_end,
            plan,
        )?;
        let rollup_points = downsample_points_with_origin(
            &raw_points,
            policy.interval,
            policy.aggregation,
            policy.bucket_origin,
            checkpoint,
            target_end,
        )?;

        if !rollup_points.is_empty() {
            store.stage_pending_rollup_materialization(
                &policy.id,
                &source.source_key,
                PendingRollupMaterialization {
                    checkpoint,
                    materialized_through: target_end,
                    generation,
                },
            )?;

            let existing_rollup_series_id = source_reads
                .registry
                .resolve_existing_series_id(rollup_metric.as_str(), &source.labels);
            let existing_rollup_points = existing_rollup_series_id
                .map(|series_id| {
                    source_reads.collect_points_for_series_with_plan(
                        series_id,
                        checkpoint,
                        target_end,
                        source_reads.query_tier_plan(checkpoint, target_end),
                    )
                })
                .transpose()?
                .unwrap_or_default();
            let existing_bucket_timestamps = existing_rollup_points
                .into_iter()
                .map(|point| point.timestamp)
                .collect::<BTreeSet<_>>();

            let rows = rollup_points
                .into_iter()
                .filter(|point| !existing_bucket_timestamps.contains(&point.timestamp))
                .map(|point| Row::with_labels(rollup_metric.clone(), source.labels.clone(), point))
                .collect::<Vec<_>>();

            if !rows.is_empty() {
                report.buckets_materialized = report
                    .buckets_materialized
                    .saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
                report.points_materialized = report
                    .points_materialized
                    .saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
                let _ = materialized_writes.insert_rows(&rows)?;
            }
        }

        updated_checkpoints.insert(source.source_key.clone(), target_end);
        store.mark_rollup_checkpoint_in_memory(&policy.id, &source.source_key, target_end);
        store.clear_pending_rollup_materialization_in_memory(&policy.id, &source.source_key);
        checkpoint_changed = true;
    }

    if checkpoint_changed {
        store
            .state
            .checkpoints
            .write()
            .insert(policy.id.clone(), updated_checkpoints.clone());
        report.checkpoint_changed = true;
    }

    let mut min_through = None::<i64>;
    let mut materialized_series = 0u64;
    for source in &sources {
        if let Some(materialized_through) = updated_checkpoints.get(&source.source_key).copied() {
            materialized_series = materialized_series.saturating_add(1);
            min_through = Some(
                min_through
                    .map(|current| current.min(materialized_through))
                    .unwrap_or(materialized_through),
            );
        }
    }
    report.materialized_series = materialized_series;
    report.materialized_through = min_through;
    Ok(report)
}

// Caller must hold `rollup_run_lock`. This keeps policy-set replacement, persistence,
// and worker execution linearizable with each other so a worker only runs against a
// fully persisted policy snapshot.
fn run_rollup_pipeline_once_locked_impl(
    store: RollupStateStoreContext<'_>,
    source_reads: RollupSourceReadContext<'_>,
    materialized_writes: RollupMaterializedWriteContext<'_>,
    observability: &RollupObservabilityCounters,
) -> Result<()> {
    let started = Instant::now();
    observability
        .worker_runs_total
        .fetch_add(1, Ordering::Relaxed);

    let policies = store.policies_snapshot();
    if policies.is_empty() {
        let duration_nanos = elapsed_nanos_u64(started);
        observability
            .worker_success_total
            .fetch_add(1, Ordering::Relaxed);
        observability
            .last_run_duration_nanos
            .store(duration_nanos, Ordering::Relaxed);
        return Ok(());
    }

    let max_observed = source_reads
        .bounded_recency_reference_timestamp()
        .unwrap_or(i64::MIN);
    let mut checkpoints_dirty = false;
    let mut first_error: Option<TsinkError> = None;

    for policy in policies {
        let policy_started_at_ms = now_unix_ms();
        let policy_started = Instant::now();
        observability
            .policy_runs_total
            .fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        store.invoke_policy_start_hook(&policy);

        match run_rollup_policy_once(
            store,
            source_reads,
            materialized_writes,
            &policy,
            max_observed,
        ) {
            Ok(report) => {
                checkpoints_dirty |= report.checkpoint_changed;
                observability
                    .buckets_materialized_total
                    .fetch_add(report.buckets_materialized, Ordering::Relaxed);
                observability
                    .points_materialized_total
                    .fetch_add(report.points_materialized, Ordering::Relaxed);
                store.set_rollup_policy_run_state(
                    &policy.id,
                    &report,
                    policy_started_at_ms,
                    now_unix_ms(),
                    elapsed_nanos_u64(policy_started),
                    None,
                );
            }
            Err(err) => {
                store.set_rollup_policy_run_state(
                    &policy.id,
                    &PolicyRunReport::default(),
                    policy_started_at_ms,
                    now_unix_ms(),
                    elapsed_nanos_u64(policy_started),
                    Some(err.to_string()),
                );
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }

    if checkpoints_dirty {
        let checkpoints = store.checkpoints_snapshot();
        let generations = store.generations_snapshot();
        let pending_materializations = store.pending_materializations_snapshot();
        let pending_delete_invalidations = store.pending_delete_invalidations_snapshot();
        store.persist_state_snapshot(
            &checkpoints,
            &generations,
            &pending_materializations,
            &pending_delete_invalidations,
        )?;
    }

    let duration_nanos = elapsed_nanos_u64(started);
    observability
        .last_run_duration_nanos
        .store(duration_nanos, Ordering::Relaxed);

    if let Some(err) = first_error {
        observability
            .worker_errors_total
            .fetch_add(1, Ordering::Relaxed);
        return Err(err);
    }

    observability
        .worker_success_total
        .fetch_add(1, Ordering::Relaxed);
    Ok(())
}

impl ChunkStorage {
    pub(in crate::engine) fn rollup_policy_ids_needing_rebuild_for_rows(
        &self,
        rows: &[Row],
    ) -> BTreeSet<String> {
        self.rollup_invalidation_context()
            .rollup_policy_ids_needing_rebuild_for_rows(rows)
    }

    pub(in crate::engine) fn run_rollup_pipeline_once_locked(&self) -> Result<()> {
        run_rollup_pipeline_once_locked_impl(
            self.rollup_state_store_context(),
            self.rollup_source_read_context(),
            self.rollup_materialized_write_context(),
            &self.observability.rollup,
        )
    }

    pub(in crate::engine) fn run_rollup_pipeline_once(&self) -> Result<()> {
        self.ensure_open()?;
        let _run_guard = self.rollup_run_coordination_context().run_lock.lock();
        self.run_rollup_pipeline_once_locked()
    }
}
