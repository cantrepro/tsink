use super::json_decode::{preflight_rollup_policies, read_json_to_end_budgeted, RollupJsonLimits};
use super::runtime::{
    ensure_rollup_policies_within_limits, pending_delete_blocks_rollup_candidate,
    rollup_policy_envelope_usage,
};
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
    policy.match_labels.sort_unstable();
    policy.match_labels.dedup();
    validate_labels(&policy.match_labels)?;
    Ok(policy)
}

#[cfg(test)]
pub(super) fn load_rollup_policies(path: Option<&Path>) -> Result<Vec<RollupPolicy>> {
    load_rollup_policies_budgeted(path, usize::MAX).map(|(policies, _)| policies)
}

pub(super) fn load_rollup_policies_budgeted(
    path: Option<&Path>,
    memory_limit_bytes: usize,
) -> Result<(Vec<RollupPolicy>, RollupStateEnvelopeUsage)> {
    let initial_usage = RollupStateEnvelopeUsage::empty();
    let Some(path) = path else {
        return Ok((Vec::new(), initial_usage));
    };
    let bytes = match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                || !metadata.file_type().is_file()
            {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup policies path is link-like or not a regular file: {}",
                    path.display()
                )));
            }
            if metadata.len() > ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES as u64 {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup policies file size {} exceeds the bounded decode limit {ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES}",
                    metadata.len()
                )));
            }
            let mut file = fs::File::open(path)?;
            read_json_to_end_budgeted(
                &mut file,
                ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES,
                usize::try_from(metadata.len())
                    .unwrap_or(ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES)
                    .min(ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES),
                "rollup policies snapshot",
                memory_limit_bytes,
                initial_usage.modeled_bytes,
            )?
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), initial_usage));
        }
        Err(err) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source: err,
            });
        }
    };

    let (_, _, decode_plan) = preflight_rollup_policies(
        &bytes,
        path,
        initial_usage,
        memory_limit_bytes,
        RollupJsonLimits::default(),
    )?;
    debug_assert!(decode_plan.usage.items >= initial_usage.items);
    decode_plan.admit_typed_decode(
        bytes.capacity(),
        bytes.len(),
        initial_usage.modeled_bytes,
        memory_limit_bytes,
        false,
    )?;
    super::json_decode::note_typed_json_materialization();
    let mut file: PersistedRollupPoliciesFile = serde_json::from_slice(&bytes)?;
    debug_assert_eq!(file.magic, ROLLUP_POLICIES_MAGIC);
    debug_assert_eq!(file.version, ROLLUP_SCHEMA_VERSION);
    drop(bytes);
    for policy in &mut file.policies {
        *policy = normalize_policy(std::mem::take(policy))?;
    }
    let usage = rollup_policy_envelope_usage(&file.policies)?;
    crate::disk_budget::admit_startup_memory(memory_limit_bytes, usage.modeled_bytes)?;
    Ok((file.policies, usage))
}

pub(super) fn encode_rollup_policies(policies: &[RollupPolicy]) -> Result<Vec<u8>> {
    ensure_rollup_policies_within_limits(policies)?;
    let payload = PersistedRollupPoliciesFile {
        magic: ROLLUP_POLICIES_MAGIC.to_string(),
        version: ROLLUP_SCHEMA_VERSION,
        policies: policies.to_vec(),
    };
    Ok(serde_json::to_vec_pretty(&payload)?)
}

pub(in crate::engine) struct RollupStatusSnapshotSource<'a> {
    #[cfg(test)]
    state: &'a RollupRuntimeState,
    counters: &'a RollupObservabilityCounters,
    max_observed_timestamp: i64,
    cursor: parking_lot::MutexGuard<'a, BackgroundRollupCursor>,
    _snapshot_visibility: parking_lot::RwLockReadGuard<'a, ()>,
    policies: parking_lot::RwLockReadGuard<'a, Vec<RollupPolicy>>,
    policy_stats: parking_lot::RwLockReadGuard<'a, BTreeMap<String, PolicyRunState>>,
    _run_guard: parking_lot::MutexGuard<'a, ()>,
}

fn add_status_snapshot_bytes(total: &mut u64, bytes: u64) -> Result<()> {
    *total = total.checked_add(bytes).ok_or_else(|| {
        TsinkError::Other(
            "rollup status observability retained-byte model exceeds the supported range"
                .to_string(),
        )
    })?;
    Ok(())
}

fn modeled_status_string_bytes(value: &str) -> Result<u64> {
    crate::storage::modeled_status_observability_string_bytes(value.len())
}

fn clone_status_string(value: &str) -> Result<String> {
    let mut cloned = String::new();
    cloned.try_reserve_exact(value.len()).map_err(|_| {
        TsinkError::Other("rollup status observability string allocation failed".to_string())
    })?;
    cloned.push_str(value);
    Ok(cloned)
}

impl RollupStatusSnapshotSource<'_> {
    fn traversal_progress(&self) -> (bool, Option<&str>, Option<SeriesId>) {
        if !self.cursor.cycle_complete {
            return (
                false,
                self.cursor.policy_id.as_deref(),
                self.cursor.after_series_id,
            );
        }
        if let Some(policy) = self.policies.iter().find(|policy| {
            !self
                .policy_stats
                .get(&policy.id)
                .is_some_and(|state| state.source_traversal_complete)
        }) {
            return (false, Some(policy.id.as_str()), None);
        }
        (true, None, None)
    }

    pub(in crate::engine) fn modeled_retained_bytes(&self) -> Result<u64> {
        let mut total = crate::storage::modeled_status_observability_vec_bytes::<RollupPolicyStatus>(
            self.policies.len(),
        )?;
        let (_, continuation_policy_id, _) = self.traversal_progress();
        if let Some(policy_id) = continuation_policy_id {
            add_status_snapshot_bytes(&mut total, modeled_status_string_bytes(policy_id)?)?;
        }
        for policy in self.policies.iter() {
            add_status_snapshot_bytes(&mut total, modeled_status_string_bytes(&policy.id)?)?;
            add_status_snapshot_bytes(&mut total, modeled_status_string_bytes(&policy.metric)?)?;
            add_status_snapshot_bytes(
                &mut total,
                crate::storage::modeled_status_observability_vec_bytes::<Label>(
                    policy.match_labels.len(),
                )?,
            )?;
            for label in &policy.match_labels {
                add_status_snapshot_bytes(&mut total, modeled_status_string_bytes(&label.name)?)?;
                add_status_snapshot_bytes(&mut total, modeled_status_string_bytes(&label.value)?)?;
            }
            if let Some(error) = self
                .policy_stats
                .get(&policy.id)
                .and_then(|state| state.last_error.as_deref())
            {
                add_status_snapshot_bytes(&mut total, modeled_status_string_bytes(error)?)?;
            }
        }
        Ok(total)
    }

    pub(in crate::engine) fn materialize(
        self,
        execution: &QueryExecution,
    ) -> Result<RollupObservabilitySnapshot> {
        execution.checkpoint()?;
        let (source_traversal_complete, continuation_policy_id, continuation_after_series_id) =
            self.traversal_progress();
        let continuation_policy_id = continuation_policy_id
            .map(clone_status_string)
            .transpose()?;

        let mut policies = Vec::new();
        policies
            .try_reserve_exact(self.policies.len())
            .map_err(|_| {
                TsinkError::Other(
                    "rollup status observability policy allocation failed".to_string(),
                )
            })?;
        for policy in self.policies.iter() {
            execution.checkpoint()?;
            #[cfg(test)]
            self.state
                .test_hooks
                .status_snapshot_policy_copies
                .fetch_add(1, Ordering::Relaxed);

            let mut match_labels = Vec::new();
            match_labels
                .try_reserve_exact(policy.match_labels.len())
                .map_err(|_| {
                    TsinkError::Other(
                        "rollup status observability label allocation failed".to_string(),
                    )
                })?;
            for label in &policy.match_labels {
                match_labels.push(Label {
                    name: clone_status_string(&label.name)?,
                    value: clone_status_string(&label.value)?,
                });
            }
            let runtime = self.policy_stats.get(&policy.id);
            let materialized_through = runtime
                .filter(|state| state.source_traversal_complete)
                .and_then(|state| state.materialized_through);
            policies.push(RollupPolicyStatus {
                policy: RollupPolicy {
                    id: clone_status_string(&policy.id)?,
                    metric: clone_status_string(&policy.metric)?,
                    match_labels,
                    interval: policy.interval,
                    aggregation: policy.aggregation,
                    bucket_origin: policy.bucket_origin,
                },
                matched_series: runtime.map_or(0, |state| state.matched_series),
                materialized_series: runtime.map_or(0, |state| state.materialized_series),
                materialized_through,
                lag: materialized_through.and_then(|through| {
                    (self.max_observed_timestamp != i64::MIN)
                        .then_some(self.max_observed_timestamp.saturating_sub(through))
                }),
                source_traversal_complete: runtime
                    .is_some_and(|state| state.source_traversal_complete),
                last_run_started_at_ms: runtime.and_then(|state| state.last_run_started_at_ms),
                last_run_completed_at_ms: runtime.and_then(|state| state.last_run_completed_at_ms),
                last_run_duration_nanos: runtime.map_or(0, |state| state.last_run_duration_nanos),
                last_error: runtime
                    .and_then(|state| state.last_error.as_deref())
                    .map(clone_status_string)
                    .transpose()?,
            });
        }

        Ok(RollupObservabilitySnapshot {
            worker_runs_total: self.counters.worker_runs_total.load(Ordering::Relaxed),
            worker_success_total: self.counters.worker_success_total.load(Ordering::Relaxed),
            worker_errors_total: self.counters.worker_errors_total.load(Ordering::Relaxed),
            policy_runs_total: self.counters.policy_runs_total.load(Ordering::Relaxed),
            buckets_materialized_total: self
                .counters
                .buckets_materialized_total
                .load(Ordering::Relaxed),
            points_materialized_total: self
                .counters
                .points_materialized_total
                .load(Ordering::Relaxed),
            last_run_duration_nanos: self
                .counters
                .last_run_duration_nanos
                .load(Ordering::Relaxed),
            source_traversal_complete,
            continuation_policy_id,
            continuation_after_series_id,
            policies,
        })
    }
}

impl ChunkStorage {
    pub(in crate::engine) fn rollup_status_snapshot_source(
        &self,
    ) -> RollupStatusSnapshotSource<'_> {
        let max_observed_timestamp = self
            .rollup_source_read_context()
            .bounded_recency_reference_timestamp()
            .unwrap_or(i64::MIN);
        let coordination = self.rollup_run_coordination_context();
        let run_guard = coordination.run_lock.lock();
        let cursor = coordination.traversal_cursor.lock();
        let store = self.rollup_state_store_context();
        let snapshot_visibility = store.state.snapshot_visibility.read();
        let policies = store.state.policies.read();
        let policy_stats = store.state.policy_stats.read();
        RollupStatusSnapshotSource {
            #[cfg(test)]
            state: store.state,
            counters: &self.observability.rollup,
            max_observed_timestamp,
            cursor,
            _snapshot_visibility: snapshot_visibility,
            policies,
            policy_stats,
            _run_guard: run_guard,
        }
    }

    #[cfg(test)]
    pub(in crate::engine) fn rollup_status_snapshot_policy_copies(&self) -> u64 {
        self.rollup_state_store_context()
            .state
            .test_hooks
            .status_snapshot_policy_copies
            .load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(in crate::engine) fn reset_rollup_status_snapshot_policy_copies(&self) {
        self.rollup_state_store_context()
            .state
            .test_hooks
            .status_snapshot_policy_copies
            .store(0, Ordering::Relaxed);
    }
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
        let _snapshot_visibility = self.store.state.snapshot_visibility.read();
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

    fn rollup_observability_snapshot(
        self,
        progress: RollupTraversalProgress,
    ) -> RollupObservabilitySnapshot {
        let policies = self.store.policies_snapshot();
        let policy_stats = self.store.policy_stats_snapshot();
        let max_observed = self
            .source_reads
            .bounded_recency_reference_timestamp()
            .unwrap_or(i64::MIN);

        let policies = policies
            .into_iter()
            .map(|policy| {
                let runtime = policy_stats.get(&policy.id).cloned().unwrap_or_default();
                let materialized_through = runtime
                    .source_traversal_complete
                    .then_some(runtime.materialized_through)
                    .flatten();

                RollupPolicyStatus {
                    policy,
                    matched_series: runtime.matched_series,
                    materialized_series: runtime.materialized_series,
                    materialized_through,
                    lag: materialized_through.and_then(|through| {
                        (max_observed != i64::MIN).then_some(max_observed.saturating_sub(through))
                    }),
                    source_traversal_complete: runtime.source_traversal_complete,
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
            source_traversal_complete: progress.complete,
            continuation_policy_id: progress.continuation_policy_id,
            continuation_after_series_id: progress.continuation_after_series_id,
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
        ensure_rollup_policies_within_limits(&policies)?;

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

        let write_permits = self
            .runtime
            .write_limiter
            .acquire_all(self.runtime.write_timeout)?;
        let write_permit = write_permits
            .first()
            .expect("the write limiter always owns at least one permit");
        self.ensure_open()?;
        let _run_guard = self.rollup_run_coordination_context().run_lock.lock();
        let snapshot = state_store.next_snapshot_for_policies(normalized)?;
        let persistence = state_store.persist_snapshot(&snapshot)?;
        let cleanup_debt = persistence.into_cleanup_debt();
        state_store.install_snapshot(snapshot);
        let progress = match self.run_rollup_pipeline_once_locked(write_permit) {
            Ok(progress) => progress,
            Err(err) => {
                // The policy/state snapshot is already durable and visible. Returning Err here
                // would tell callers the apply was rejected even though retrying or reopening
                // observes the new policy set. Keep the committed result authoritative and expose
                // initial materialization degradation through per-policy status instead.
                state_store.record_rollup_pipeline_error(&err);
                self.rollup_traversal_progress()
            }
        };
        if let Some(error) = cleanup_debt {
            tracing::warn!(
                error = %error,
                "committed rollup policy update left conservatively-accounted postcommit cleanup debt"
            );
            state_store.record_rollup_pipeline_error(&TsinkError::Other(format!(
                "postcommit rollup snapshot cleanup debt: {error}"
            )));
        }
        // Snapshot before releasing the run lock so the background worker cannot begin a new
        // traversal and reset the completion fields returned for this policy application.
        let result = self.rollup_observability_snapshot_with_progress(progress);
        drop(_run_guard);
        drop(write_permits);
        self.enforce_post_commit_memory_budget_best_effort();
        Ok(result)
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
        self.rollup_observability_snapshot_with_progress(self.rollup_traversal_progress())
    }

    pub(in crate::engine) fn rollup_observability_snapshot_with_progress(
        &self,
        progress: RollupTraversalProgress,
    ) -> RollupObservabilitySnapshot {
        self.rollup_query_selection_context()
            .rollup_observability_snapshot(progress)
    }

    pub(in crate::engine) fn rollup_metrics_observability_snapshot(
        &self,
        execution: &QueryExecution,
        reservation: &mut crate::QueryMemoryReservation,
    ) -> Result<RollupMetricsObservabilitySnapshot> {
        let traversal_cycle_complete = self.rollup_traversal_cycle_complete();
        let max_observed_timestamp = self
            .rollup_source_read_context()
            .bounded_recency_reference_timestamp();
        let (policies, label_arena, source_traversal_complete) =
            self.rollup_state_store_context().metrics_policy_snapshot(
                execution,
                reservation,
                max_observed_timestamp,
                traversal_cycle_complete,
            )?;
        Ok(RollupMetricsObservabilitySnapshot::new(
            self.observability
                .rollup
                .worker_runs_total
                .load(Ordering::Relaxed),
            self.observability
                .rollup
                .worker_success_total
                .load(Ordering::Relaxed),
            self.observability
                .rollup
                .worker_errors_total
                .load(Ordering::Relaxed),
            self.observability
                .rollup
                .policy_runs_total
                .load(Ordering::Relaxed),
            self.observability
                .rollup
                .buckets_materialized_total
                .load(Ordering::Relaxed),
            self.observability
                .rollup
                .points_materialized_total
                .load(Ordering::Relaxed),
            self.observability
                .rollup
                .last_run_duration_nanos
                .load(Ordering::Relaxed),
            source_traversal_complete,
            policies,
            label_arena,
        ))
    }

    #[cfg(test)]
    pub(in crate::engine) fn reset_rollup_metrics_snapshot_policy_copies(&self) {
        self.rollup_state_store_context()
            .reset_metrics_snapshot_policy_copies();
    }

    #[cfg(test)]
    pub(in crate::engine) fn rollup_metrics_snapshot_policy_copies(&self) -> u64 {
        self.rollup_state_store_context()
            .metrics_snapshot_policy_copies()
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
