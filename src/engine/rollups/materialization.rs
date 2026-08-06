use super::runtime::pending_materialized_through;
use super::state_journal::{RollupSourceStateEvent, RollupStateJournalWriter};
use super::*;
use crate::engine::storage_engine::query_exec::{
    modeled_point_output_upper_bound_bytes, modeled_points_retained_bytes,
    modeled_vec_capacity_bytes, QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES,
};

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

struct RollupPolicyPageStateUpdate<'a> {
    report: &'a PolicyRunReport,
    traversal_complete: bool,
    accumulate_page: bool,
    started_at_ms: u64,
    completed_at_ms: u64,
    duration_nanos: u64,
    error: Option<String>,
}

#[cfg(test)]
thread_local! {
    static ROLLUP_SOURCE_PAGE_STATE_LOOKUPS: std::cell::Cell<u64> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(test)]
fn reset_rollup_source_page_state_lookups() {
    ROLLUP_SOURCE_PAGE_STATE_LOOKUPS.with(|lookups| lookups.set(0));
}

#[cfg(test)]
fn rollup_source_page_state_lookups() -> u64 {
    ROLLUP_SOURCE_PAGE_STATE_LOOKUPS.with(std::cell::Cell::get)
}

impl RollupStateStoreContext<'_> {
    /// Reads only the selected source's materialization state.
    ///
    /// The caller holds `rollup_run_lock`, which serializes policy changes, historical-write
    /// invalidation, deletes, and source checkpoint publication. Keep the two guards short and
    /// disjoint so query reads, materialized writes, and durable journal I/O never run while a
    /// whole state component is locked.
    fn source_materialization_state(
        self,
        policy_id: &str,
        source_key: &str,
    ) -> (i64, Option<PendingRollupMaterialization>) {
        #[cfg(test)]
        ROLLUP_SOURCE_PAGE_STATE_LOOKUPS.with(|lookups| {
            lookups.set(lookups.get().saturating_add(1));
        });

        let checkpoint = self
            .state
            .checkpoints
            .read()
            .get(policy_id)
            .and_then(|entries| entries.get(source_key))
            .copied()
            .unwrap_or(i64::MIN);
        let pending = self
            .state
            .pending_materializations
            .read()
            .get(policy_id)
            .and_then(|entries| entries.get(source_key))
            .cloned();
        (checkpoint, pending)
    }

    fn source_state_journal_writer(
        self,
        journal: &mut Option<RollupStateJournalWriter>,
    ) -> Result<&mut RollupStateJournalWriter> {
        if journal.is_none() {
            *journal = Some(self.begin_source_state_journal_writer()?);
        }
        Ok(journal
            .as_mut()
            .expect("lazy rollup state journal writer was initialized"))
    }

    fn stage_pending_rollup_materialization(
        self,
        journal: &mut Option<RollupStateJournalWriter>,
        policy_id: &str,
        source_key: &str,
        pending: PendingRollupMaterialization,
    ) -> Result<()> {
        let checkpoint = self
            .state
            .checkpoints
            .read()
            .get(policy_id)
            .and_then(|entries| entries.get(source_key))
            .copied();
        let event = RollupSourceStateEvent::pending(
            self.state.journal_epoch.load(Ordering::Acquire),
            policy_id,
            source_key,
            pending.generation,
            checkpoint,
            &pending,
        );
        self.persist_source_state_event(self.source_state_journal_writer(journal)?, event)?;
        Ok(())
    }

    fn commit_rollup_source_materialization(
        self,
        journal: &mut Option<RollupStateJournalWriter>,
        policy_id: &str,
        source_key: &str,
        generation: u64,
        materialized_through: i64,
    ) -> Result<()> {
        let event = RollupSourceStateEvent::completed(
            self.state.journal_epoch.load(Ordering::Acquire),
            policy_id,
            source_key,
            generation,
            materialized_through,
        );
        self.persist_source_state_event(self.source_state_journal_writer(journal)?, event)?;
        Ok(())
    }

    fn policy_generation(self, policy_id: &str) -> u64 {
        self.state
            .generations
            .read()
            .get(policy_id)
            .copied()
            .unwrap_or(0)
    }

    fn begin_rollup_policy_traversal(self, policy_id: &str, started_at_ms: u64) {
        let mut stats = self.state.policy_stats.write();
        let state = stats.entry(policy_id.to_string()).or_default();
        state.matched_series = 0;
        state.materialized_series = 0;
        state.materialized_through = None;
        state.source_traversal_complete = false;
        state.last_run_started_at_ms = Some(started_at_ms);
        state.last_run_completed_at_ms = None;
        state.last_run_duration_nanos = 0;
        state.last_error = None;
    }

    fn record_rollup_policy_page_state(
        self,
        policy_id: &str,
        update: RollupPolicyPageStateUpdate<'_>,
    ) {
        let mut stats = self.state.policy_stats.write();
        let state = stats.entry(policy_id.to_string()).or_default();
        if update.accumulate_page {
            state.matched_series = state
                .matched_series
                .saturating_add(update.report.matched_series);
            state.materialized_series = state
                .materialized_series
                .saturating_add(update.report.materialized_series);
            state.materialized_through = match (
                state.materialized_through,
                update.report.materialized_through,
            ) {
                (Some(current), Some(page)) => Some(current.min(page)),
                (None, Some(page)) => Some(page),
                (current, None) => current,
            };
        }
        state.source_traversal_complete = update.traversal_complete;
        if update.traversal_complete
            && (state.matched_series == 0 || state.materialized_series != state.matched_series)
        {
            state.materialized_through = None;
        }
        state
            .last_run_started_at_ms
            .get_or_insert(update.started_at_ms);
        state.last_run_completed_at_ms = Some(update.completed_at_ms);
        state.last_run_duration_nanos = state
            .last_run_duration_nanos
            .saturating_add(update.duration_nanos);
        if update.error.is_some() {
            state.last_error = update.error;
        }
    }

    pub(super) fn record_rollup_pipeline_error(self, error: &TsinkError) {
        let policy_ids = self
            .state
            .policies
            .read()
            .iter()
            .map(|policy| policy.id.clone())
            .collect::<Vec<_>>();
        let message = format!("policy set committed; initial materialization failed: {error}");
        let mut stats = self.state.policy_stats.write();
        for policy_id in policy_ids {
            let state = stats.entry(policy_id).or_default();
            if state.last_error.is_none() {
                state.last_error = Some(message.clone());
            }
        }
    }

    fn policy_for_background_cursor(
        self,
        cursor: &mut BackgroundRollupCursor,
    ) -> Option<RollupPolicy> {
        let policies = self.state.policies.read();
        let index = cursor
            .policy_id
            .as_deref()
            .and_then(|policy_id| {
                policies
                    .binary_search_by(|policy| policy.id.as_str().cmp(policy_id))
                    .ok()
            })
            .unwrap_or(0);
        let policy = policies.get(index)?.clone();
        drop(policies);
        cursor.cycle_complete = false;

        let generation = self.policy_generation(&policy.id);
        if cursor.policy_id.as_deref() != Some(policy.id.as_str())
            || cursor.policy_generation != generation
        {
            cursor.policy_id = Some(policy.id.clone());
            cursor.policy_generation = generation;
            cursor.after_series_id = None;
        }
        Some(policy)
    }

    fn advance_background_cursor_after_policy(
        self,
        cursor: &mut BackgroundRollupCursor,
        completed_policy_id: &str,
    ) {
        let policies = self.state.policies.read();
        let next = policies
            .binary_search_by(|policy| policy.id.as_str().cmp(completed_policy_id))
            .ok()
            .and_then(|index| policies.get(index.saturating_add(1)))
            .cloned();
        drop(policies);

        if let Some(policy) = next {
            cursor.policy_generation = self.policy_generation(&policy.id);
            cursor.policy_id = Some(policy.id);
            cursor.after_series_id = None;
            cursor.cycle_complete = false;
        } else {
            cursor.policy_id = None;
            cursor.policy_generation = 0;
            cursor.after_series_id = None;
            cursor.cycle_complete = true;
        }
    }

    fn persist_current_runtime_state(self) -> Result<()> {
        // Per-source state is write-through: a source-state replacement is durable before its
        // in-memory checkpoint/pending pair changes. Page-level callers retain this fence check,
        // but no longer serialize the complete checkpoint map after every bounded page.
        self.ensure_snapshot_mutations_unfenced()
    }
}

impl BackgroundRollupCursor {
    fn progress(&self) -> RollupTraversalProgress {
        RollupTraversalProgress {
            complete: self.cycle_complete,
            continuation_policy_id: (!self.cycle_complete)
                .then(|| self.policy_id.clone())
                .flatten(),
            continuation_after_series_id: (!self.cycle_complete)
                .then_some(self.after_series_id)
                .flatten(),
        }
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
        execution: &QueryExecution,
    ) -> Result<(Vec<DataPoint>, crate::QueryMemoryReservation)> {
        self.ops
            .collect_points_for_series_with_plan(series_id, start, end, plan, execution)
    }

    fn begin_rollup_source_execution(self) -> Result<QueryExecution> {
        self.ops.begin_rollup_source_execution()
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

    fn matching_rollup_sources_bounded(
        self,
        policy: &RollupPolicy,
        after_series_id: Option<SeriesId>,
        limit: usize,
    ) -> Result<BoundedRollupSourceBatch> {
        let (candidate_series_ids, has_more) =
            self.registry
                .series_ids_for_metric_after(&policy.metric, after_series_id, limit);
        let last_visited_series_id = candidate_series_ids.last().copied();
        let live_series_ids = self.live_series_ids(candidate_series_ids, true)?;
        Ok(BoundedRollupSourceBatch {
            sources: self.rollup_sources_for_series_ids(policy, live_series_ids),
            last_visited_series_id,
            has_more,
        })
    }
}

fn modeled_vec_growth_capacity_upper(elements: usize) -> usize {
    if elements == 0 {
        return 0;
    }
    elements
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .max(4)
}

fn rollup_downsample_output_count_upper(
    points: &[DataPoint],
    interval: i64,
    origin: i64,
    start: i64,
    end: i64,
) -> usize {
    if points.is_empty() || interval <= 0 || start >= end {
        return 0;
    }

    let mut idx = 0usize;
    let mut buckets = 0usize;
    while idx < points.len() && points[idx].timestamp < start {
        idx = idx.saturating_add(1);
    }
    while idx < points.len() && points[idx].timestamp < end {
        buckets = buckets.saturating_add(1);
        let bucket_start = bucket_start_for_origin(points[idx].timestamp, origin, interval);
        let bucket_end = bucket_start.saturating_add(interval);
        while idx < points.len()
            && points[idx].timestamp < end
            && points[idx].timestamp < bucket_end
        {
            idx = idx.saturating_add(1);
        }
    }
    buckets
}

fn rollup_aggregation_scratch_bytes(points: usize, aggregation: Aggregation) -> u64 {
    if matches!(
        aggregation,
        Aggregation::Sum
            | Aggregation::Avg
            | Aggregation::Median
            | Aggregation::Range
            | Aggregation::Variance
            | Aggregation::StdDev
    ) {
        saturating_u64_from_usize(points)
            .saturating_mul(u64::try_from(std::mem::size_of::<f64>()).unwrap_or(u64::MAX))
    } else {
        0
    }
}

fn downsample_rollup_points(
    execution: &QueryExecution,
    points: &[DataPoint],
    interval: i64,
    aggregation: Aggregation,
    origin: i64,
    start: i64,
    end: i64,
) -> Result<(Vec<DataPoint>, crate::QueryMemoryReservation)> {
    execution.checkpoint()?;
    let output_count = rollup_downsample_output_count_upper(points, interval, origin, start, end);
    execution.observe_intermediate_vector_size(saturating_u64_from_usize(output_count))?;
    let output_capacity = modeled_vec_growth_capacity_upper(output_count);
    let mut output_reservation = execution.reserve_memory(
        modeled_point_output_upper_bound_bytes(points, output_capacity),
    )?;
    // The numeric aggregation helpers can materialize one f64 scratch vector for a bucket.
    // Charging the complete input length is conservative across all bucket shapes and admits it
    // before the helper can allocate.
    let _scratch_reservation =
        execution.reserve_memory(rollup_aggregation_scratch_bytes(points.len(), aggregation))?;

    let output = downsample_points_with_origin(points, interval, aggregation, origin, start, end)?;
    output_reservation.resize(modeled_points_retained_bytes(&output))?;
    execution.checkpoint()?;
    Ok((output, output_reservation))
}

fn modeled_string_clone_bytes(value: &str) -> u64 {
    if value.is_empty() {
        0
    } else {
        saturating_u64_from_usize(value.len())
            .saturating_add(QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_labels_clone_bytes(labels: &[Label]) -> u64 {
    modeled_vec_capacity_bytes::<Label>(labels.len()).saturating_add(labels.iter().fold(
        0u64,
        |bytes, label| {
            bytes
                .saturating_add(modeled_string_clone_bytes(&label.name))
                .saturating_add(modeled_string_clone_bytes(&label.value))
        },
    ))
}

fn rollup_row_output_count(
    rollup_points: &[DataPoint],
    existing_rollup_points: &[DataPoint],
) -> usize {
    rollup_points
        .iter()
        .filter(|point| {
            existing_rollup_points
                .binary_search_by_key(&point.timestamp, |existing| existing.timestamp)
                .is_err()
        })
        .count()
}

fn modeled_rollup_row_output_upper_bytes(
    rollup_metric: &str,
    labels: &[Label],
    rollup_points: &[DataPoint],
    existing_rollup_points: &[DataPoint],
) -> u64 {
    let output_count = rollup_row_output_count(rollup_points, existing_rollup_points);
    let output_capacity = modeled_vec_growth_capacity_upper(output_count);
    modeled_vec_capacity_bytes::<Row>(output_capacity).saturating_add(
        rollup_points
            .iter()
            .filter(|point| {
                existing_rollup_points
                    .binary_search_by_key(&point.timestamp, |existing| existing.timestamp)
                    .is_err()
            })
            .fold(0u64, |bytes, point| {
                bytes
                    .saturating_add(modeled_string_clone_bytes(rollup_metric))
                    .saturating_add(modeled_labels_clone_bytes(labels))
                    .saturating_add(
                        u64::try_from(value_heap_bytes(&point.value)).unwrap_or(u64::MAX),
                    )
            }),
    )
}

fn assemble_rollup_rows(
    execution: &QueryExecution,
    rollup_metric: &str,
    labels: &[Label],
    rollup_points: Vec<DataPoint>,
    existing_rollup_points: &[DataPoint],
) -> Result<(Vec<Row>, crate::QueryMemoryReservation)> {
    execution.checkpoint()?;
    let output_count = rollup_row_output_count(&rollup_points, existing_rollup_points);
    execution.observe_intermediate_vector_size(saturating_u64_from_usize(output_count))?;

    let modeled_bytes = modeled_rollup_row_output_upper_bytes(
        rollup_metric,
        labels,
        &rollup_points,
        existing_rollup_points,
    );
    let mut row_reservation = execution.reserve_memory(modeled_bytes)?;

    let mut rows = Vec::with_capacity(output_count);
    for point in rollup_points {
        execution.checkpoint()?;
        if existing_rollup_points
            .binary_search_by_key(&point.timestamp, |existing| existing.timestamp)
            .is_err()
        {
            rows.push(Row::with_labels(
                rollup_metric.to_string(),
                labels.to_vec(),
                point,
            ));
        }
    }
    debug_assert_eq!(rows.len(), output_count);
    let retained_bytes = modeled_vec_capacity_bytes::<Row>(rows.capacity()).saturating_add(
        rows.iter().fold(0u64, |bytes, row| {
            bytes
                .saturating_add(modeled_string_clone_bytes(row.metric()))
                .saturating_add(modeled_labels_clone_bytes(row.labels()))
                .saturating_add(
                    u64::try_from(value_heap_bytes(&row.data_point().value)).unwrap_or(u64::MAX),
                )
        }),
    );
    row_reservation.resize(retained_bytes)?;
    Ok((rows, row_reservation))
}

impl RollupMaterializedWriteContext<'_, '_, '_> {
    fn insert_rows(self, rows: &[Row]) -> Result<WriteResult> {
        if rows.is_empty() {
            return self
                .ops
                .insert_rows_with_held_permit(rows, self.write_permit);
        }

        let mut first_unwritten = 0;
        let mut acknowledgement = crate::WriteAcknowledgement::Durable;
        while first_unwritten < rows.len() {
            let chunk_end =
                next_rollup_write_chunk_end(rows, first_unwritten, self.write_batch_limits)?;
            let result = self.ops.insert_rows_with_held_permit(
                &rows[first_unwritten..chunk_end],
                self.write_permit,
            )?;
            acknowledgement = acknowledgement.weakest(result.acknowledgement);
            first_unwritten = chunk_end;
        }

        Ok(WriteResult::new(acknowledgement))
    }
}

fn next_rollup_write_chunk_end(
    rows: &[Row],
    first_unwritten: usize,
    limits: crate::WriteBatchLimits,
) -> Result<usize> {
    debug_assert!(first_unwritten < rows.len());

    let row_limited_end = limits
        .max_rows
        .map(|max_rows| first_unwritten.saturating_add(max_rows))
        .unwrap_or(rows.len())
        .min(rows.len());
    if row_limited_end == first_unwritten {
        return Err(TsinkError::InvalidConfiguration(
            "configured rollup write-batch row limit must be greater than zero".to_string(),
        ));
    }

    let Some(byte_limit) = limits.max_modeled_input_bytes else {
        return Ok(row_limited_end);
    };

    let mut modeled_bytes = 0usize;
    let mut chunk_end = first_unwritten;
    while chunk_end < row_limited_end {
        let row_bytes =
            crate::modeled_write_batch_input_bytes(std::slice::from_ref(&rows[chunk_end]))?;
        if row_bytes > byte_limit.saturating_sub(modeled_bytes) {
            break;
        }
        modeled_bytes += row_bytes;
        chunk_end += 1;
    }

    if chunk_end == first_unwritten {
        let submitted =
            crate::modeled_write_batch_input_bytes(std::slice::from_ref(&rows[first_unwritten]))?;
        return Err(TsinkError::WriteBatchInputLimitExceeded {
            limit: byte_limit,
            submitted,
        });
    }

    Ok(chunk_end)
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

enum PolicySourceRunError {
    Isolated(TsinkError),
    Global(TsinkError),
}

impl PolicySourceRunError {
    fn error(&self) -> &TsinkError {
        match self {
            Self::Isolated(error) | Self::Global(error) => error,
        }
    }

    fn into_error(self) -> TsinkError {
        match self {
            Self::Isolated(error) | Self::Global(error) => error,
        }
    }

    fn cursor_can_advance(&self) -> bool {
        matches!(self, Self::Isolated(_))
    }
}

fn run_rollup_policy_sources_once(
    store: RollupStateStoreContext<'_>,
    source_reads: RollupSourceReadContext<'_>,
    materialized_writes: RollupMaterializedWriteContext<'_, '_, '_>,
    policy: &RollupPolicy,
    max_observed: i64,
    sources: &[RollupSourceSeries],
    continue_after_source_error: bool,
) -> std::result::Result<PolicyRunReport, PolicySourceRunError> {
    let mut report = PolicyRunReport {
        matched_series: u64::try_from(sources.len()).unwrap_or(u64::MAX),
        ..PolicyRunReport::default()
    };

    let Some(stable_end) = aligned_materialized_end(policy, max_observed) else {
        return Ok(report);
    };

    let generation = store.policy_generation(&policy.id);
    let rollup_metric = rollup_metric_name(policy, generation);
    let mut journal = None;
    let mut checkpoint_changed = false;
    let mut first_source_error = None;

    for source in sources {
        let source_result = (|| -> std::result::Result<(), PolicySourceRunError> {
            let (checkpoint, pending) =
                store.source_materialization_state(&policy.id, &source.source_key);
            let target_end = pending
                .as_ref()
                .filter(|pending| pending.generation == generation)
                .map(|pending| pending.materialized_through.max(stable_end))
                .unwrap_or(stable_end);
            if checkpoint >= target_end {
                return Ok(());
            }

            let execution = source_reads
                .begin_rollup_source_execution()
                .map_err(PolicySourceRunError::Isolated)?;
            let plan = source_reads.query_tier_plan(checkpoint, target_end);
            let (raw_points, raw_points_reservation) = source_reads
                .collect_points_for_series_with_plan(
                    source.series_id,
                    checkpoint,
                    target_end,
                    plan,
                    &execution,
                )
                .map_err(PolicySourceRunError::Isolated)?;
            #[cfg(test)]
            store
                .state
                .invoke_source_read_hook(policy, source.series_id)
                .map_err(PolicySourceRunError::Isolated)?;
            let (rollup_points, rollup_points_reservation) = downsample_rollup_points(
                &execution,
                &raw_points,
                policy.interval,
                policy.aggregation,
                policy.bucket_origin,
                checkpoint,
                target_end,
            )
            .map_err(PolicySourceRunError::Isolated)?;
            drop(raw_points_reservation);
            drop(raw_points);

            if !rollup_points.is_empty() {
                let existing_rollup_series_id = source_reads
                    .registry
                    .resolve_existing_series_id(rollup_metric.as_str(), &source.labels);
                let (existing_rollup_points, existing_rollup_points_reservation) =
                    if let Some(series_id) = existing_rollup_series_id {
                        let result = source_reads
                            .collect_points_for_series_with_plan(
                                series_id,
                                checkpoint,
                                target_end,
                                source_reads.query_tier_plan(checkpoint, target_end),
                                &execution,
                            )
                            .map_err(PolicySourceRunError::Isolated)?;
                        (result.0, Some(result.1))
                    } else {
                        (Vec::new(), None)
                    };

                let (rows, rows_reservation) = assemble_rollup_rows(
                    &execution,
                    &rollup_metric,
                    &source.labels,
                    rollup_points,
                    &existing_rollup_points,
                )
                .map_err(PolicySourceRunError::Isolated)?;
                drop(rollup_points_reservation);
                drop(existing_rollup_points_reservation);
                drop(existing_rollup_points);

                if !rows.is_empty() {
                    store
                        .stage_pending_rollup_materialization(
                            &mut journal,
                            &policy.id,
                            &source.source_key,
                            PendingRollupMaterialization {
                                checkpoint,
                                materialized_through: target_end,
                                generation,
                            },
                        )
                        .map_err(PolicySourceRunError::Global)?;
                    report.buckets_materialized = report
                        .buckets_materialized
                        .saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
                    report.points_materialized = report
                        .points_materialized
                        .saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
                    // The write context may commit several bounded batches. If a later batch
                    // fails, `?` leaves the durable pending marker in place and returns before the
                    // checkpoint update below. A retry filters the already committed bucket
                    // timestamps and resumes with only the missing rows.
                    let _ = materialized_writes
                        .insert_rows(&rows)
                        .map_err(PolicySourceRunError::Global)?;
                }
                drop(rows_reservation);
            }

            store
                .commit_rollup_source_materialization(
                    &mut journal,
                    &policy.id,
                    &source.source_key,
                    generation,
                    target_end,
                )
                .map_err(PolicySourceRunError::Global)?;
            checkpoint_changed = true;
            Ok(())
        })();

        if let Err(failure) = source_result {
            let isolated = failure.cursor_can_advance();
            if !continue_after_source_error || !isolated {
                return Err(failure);
            }
            if first_source_error.is_none() {
                first_source_error = Some(failure.into_error());
            }
        }
    }

    report.checkpoint_changed = checkpoint_changed;
    let page_coverage = rollup_policy_page_coverage(store, &policy.id, sources);
    report.materialized_series = page_coverage.materialized_series;
    report.materialized_through = page_coverage.materialized_through;
    if let Some(error) = first_source_error {
        Err(PolicySourceRunError::Isolated(error))
    } else {
        Ok(report)
    }
}

fn rollup_policy_page_coverage(
    store: RollupStateStoreContext<'_>,
    policy_id: &str,
    sources: &[RollupSourceSeries],
) -> PolicyRunReport {
    let checkpoints = store.state.checkpoints.read();
    let policy_checkpoints = checkpoints.get(policy_id);
    let mut report = PolicyRunReport {
        matched_series: u64::try_from(sources.len()).unwrap_or(u64::MAX),
        ..PolicyRunReport::default()
    };
    let mut min_through = None::<i64>;
    for source in sources {
        let materialized_through =
            policy_checkpoints.and_then(|entries| entries.get(&source.source_key).copied());
        if let Some(materialized_through) = materialized_through {
            report.materialized_series = report.materialized_series.saturating_add(1);
            min_through = Some(
                min_through
                    .map(|current| current.min(materialized_through))
                    .unwrap_or(materialized_through),
            );
        }
    }
    report.materialized_through = min_through;
    report
}

fn begin_rollup_pipeline_page_once_locked(
    store: RollupStateStoreContext<'_>,
    observability: &RollupObservabilityCounters,
    cursor: &mut BackgroundRollupCursor,
) -> Result<Instant> {
    store.ensure_snapshot_mutations_unfenced()?;

    let started = Instant::now();
    observability
        .worker_runs_total
        .fetch_add(1, Ordering::Relaxed);

    if cursor.checkpoint_persistence_pending {
        if let Err(err) = store.persist_current_runtime_state() {
            observability
                .worker_errors_total
                .fetch_add(1, Ordering::Relaxed);
            observability
                .last_run_duration_nanos
                .store(elapsed_nanos_u64(started), Ordering::Relaxed);
            return Err(err);
        }
        cursor.checkpoint_persistence_pending = false;
    }

    Ok(started)
}

fn complete_idle_rollup_pipeline_page(
    observability: &RollupObservabilityCounters,
    cursor: &mut BackgroundRollupCursor,
    started: Instant,
) {
    cursor.policy_id = None;
    cursor.policy_generation = 0;
    cursor.after_series_id = None;
    cursor.cycle_complete = true;
    observability
        .worker_success_total
        .fetch_add(1, Ordering::Relaxed);
    observability
        .last_run_duration_nanos
        .store(elapsed_nanos_u64(started), Ordering::Relaxed);
}

// Caller must hold `rollup_run_lock`. One bounded pass visits one policy and at most
// `source_limit` postings. The cursor is advanced only after any changed checkpoint state is
// durable, so a persistence failure retries the same page.
fn run_rollup_pipeline_page_once_locked_impl(
    store: RollupStateStoreContext<'_>,
    source_reads: RollupSourceReadContext<'_>,
    materialized_writes: RollupMaterializedWriteContext<'_, '_, '_>,
    observability: &RollupObservabilityCounters,
    cursor: &mut BackgroundRollupCursor,
    limits: BackgroundRollupPassLimits,
) -> Result<()> {
    let started = begin_rollup_pipeline_page_once_locked(store, observability, cursor)?;

    let Some(policy) = store.policy_for_background_cursor(cursor) else {
        complete_idle_rollup_pipeline_page(observability, cursor, started);
        return Ok(());
    };

    let policy_started_at_ms = now_unix_ms();
    let policy_started = Instant::now();
    if cursor.after_series_id.is_none() {
        store.begin_rollup_policy_traversal(&policy.id, policy_started_at_ms);
    }
    observability
        .policy_runs_total
        .fetch_add(1, Ordering::Relaxed);
    #[cfg(test)]
    store.invoke_policy_start_hook(&policy);

    if limits.source_limit == 0 && !source_reads.registry.has_series_for_metric(&policy.metric) {
        let report = PolicyRunReport::default();
        store.advance_background_cursor_after_policy(cursor, &policy.id);
        store.record_rollup_policy_page_state(
            &policy.id,
            RollupPolicyPageStateUpdate {
                report: &report,
                traversal_complete: true,
                accumulate_page: true,
                started_at_ms: policy_started_at_ms,
                completed_at_ms: now_unix_ms(),
                duration_nanos: elapsed_nanos_u64(policy_started),
                error: None,
            },
        );
        observability
            .worker_success_total
            .fetch_add(1, Ordering::Relaxed);
        observability
            .last_run_duration_nanos
            .store(elapsed_nanos_u64(started), Ordering::Relaxed);
        return Ok(());
    }

    if limits.source_limit == 0 {
        let err = if limits.item_limit == 0 {
            TsinkError::MaintenanceDependencyWindowExceeded {
                operation: "rollup source traversal",
                item_limit: limits.item_limit,
                byte_limit: limits.byte_limit,
                selected_items: 1,
                selected_bytes: limits.modeled_source_bytes,
            }
        } else {
            TsinkError::MaintenanceWorkItemTooLarge {
                operation: "rollup source traversal",
                limit: limits.byte_limit,
                required: limits.modeled_source_bytes,
            }
        };
        observability
            .worker_errors_total
            .fetch_add(1, Ordering::Relaxed);
        observability
            .last_run_duration_nanos
            .store(elapsed_nanos_u64(started), Ordering::Relaxed);
        store.record_rollup_policy_page_state(
            &policy.id,
            RollupPolicyPageStateUpdate {
                report: &PolicyRunReport::default(),
                traversal_complete: false,
                accumulate_page: false,
                started_at_ms: policy_started_at_ms,
                completed_at_ms: now_unix_ms(),
                duration_nanos: elapsed_nanos_u64(policy_started),
                error: Some(err.to_string()),
            },
        );
        return Err(err);
    }

    let batch = match source_reads.matching_rollup_sources_bounded(
        &policy,
        cursor.after_series_id,
        limits.source_limit,
    ) {
        Ok(batch) => batch,
        Err(err) => {
            store.record_rollup_policy_page_state(
                &policy.id,
                RollupPolicyPageStateUpdate {
                    report: &PolicyRunReport::default(),
                    traversal_complete: false,
                    accumulate_page: false,
                    started_at_ms: policy_started_at_ms,
                    completed_at_ms: now_unix_ms(),
                    duration_nanos: elapsed_nanos_u64(policy_started),
                    error: Some(err.to_string()),
                },
            );
            observability
                .worker_errors_total
                .fetch_add(1, Ordering::Relaxed);
            observability
                .last_run_duration_nanos
                .store(elapsed_nanos_u64(started), Ordering::Relaxed);
            return Err(err);
        }
    };

    let max_observed = source_reads
        .bounded_recency_reference_timestamp()
        .unwrap_or(i64::MIN);
    let policy_result = run_rollup_policy_sources_once(
        store,
        source_reads,
        materialized_writes,
        &policy,
        max_observed,
        &batch.sources,
        true,
    );
    let page_coverage = rollup_policy_page_coverage(store, &policy.id, &batch.sources);
    let checkpoint_may_have_changed = match &policy_result {
        Ok(report) => report.checkpoint_changed,
        // The policy helper can update earlier sources before a later source fails. Persist the
        // conservative current snapshot before allowing this cursor to move past the page.
        Err(_) => !batch.sources.is_empty(),
    };
    if checkpoint_may_have_changed {
        cursor.checkpoint_persistence_pending = true;
        if let Err(persistence_error) = store.persist_current_runtime_state() {
            let error = match policy_result {
                Ok(_) => persistence_error,
                Err(policy_error) => TsinkError::Other(format!(
                    "background rollup policy failed: {}; checkpoint persistence failed: {persistence_error}",
                    policy_error.error()
                )),
            };
            store.record_rollup_policy_page_state(
                &policy.id,
                RollupPolicyPageStateUpdate {
                    report: &page_coverage,
                    traversal_complete: false,
                    accumulate_page: false,
                    started_at_ms: policy_started_at_ms,
                    completed_at_ms: now_unix_ms(),
                    duration_nanos: elapsed_nanos_u64(policy_started),
                    error: Some(error.to_string()),
                },
            );
            observability
                .worker_errors_total
                .fetch_add(1, Ordering::Relaxed);
            observability
                .last_run_duration_nanos
                .store(elapsed_nanos_u64(started), Ordering::Relaxed);
            return Err(error);
        }
        cursor.checkpoint_persistence_pending = false;
    }

    let cursor_can_advance = policy_result
        .as_ref()
        .err()
        .is_none_or(PolicySourceRunError::cursor_can_advance);
    if cursor_can_advance {
        if batch.has_more {
            cursor.after_series_id = batch.last_visited_series_id;
        } else {
            store.advance_background_cursor_after_policy(cursor, &policy.id);
        }
    }
    let policy_traversal_complete = cursor_can_advance && !batch.has_more;

    let duration_nanos = elapsed_nanos_u64(started);
    let policy_duration_nanos = elapsed_nanos_u64(policy_started);
    observability
        .last_run_duration_nanos
        .store(duration_nanos, Ordering::Relaxed);
    match policy_result {
        Ok(report) => {
            observability
                .buckets_materialized_total
                .fetch_add(report.buckets_materialized, Ordering::Relaxed);
            observability
                .points_materialized_total
                .fetch_add(report.points_materialized, Ordering::Relaxed);
            store.record_rollup_policy_page_state(
                &policy.id,
                RollupPolicyPageStateUpdate {
                    report: &page_coverage,
                    traversal_complete: policy_traversal_complete,
                    accumulate_page: cursor_can_advance,
                    started_at_ms: policy_started_at_ms,
                    completed_at_ms: now_unix_ms(),
                    duration_nanos: policy_duration_nanos,
                    error: None,
                },
            );
            observability
                .worker_success_total
                .fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(policy_error) => {
            let err = policy_error.into_error();
            store.record_rollup_policy_page_state(
                &policy.id,
                RollupPolicyPageStateUpdate {
                    report: &page_coverage,
                    traversal_complete: policy_traversal_complete,
                    accumulate_page: cursor_can_advance,
                    started_at_ms: policy_started_at_ms,
                    completed_at_ms: now_unix_ms(),
                    duration_nanos: policy_duration_nanos,
                    error: Some(err.to_string()),
                },
            );
            observability
                .worker_errors_total
                .fetch_add(1, Ordering::Relaxed);
            Err(err)
        }
    }
}

impl ChunkStorage {
    #[cfg(test)]
    pub(in crate::engine) fn set_rollup_source_read_hook<F>(&self, hook: F)
    where
        F: Fn(&RollupPolicy, SeriesId) -> Result<()> + Send + Sync + 'static,
    {
        self.rollup_state_store_context()
            .state
            .set_source_read_hook(hook);
    }

    #[cfg(test)]
    pub(in crate::engine) fn clear_rollup_source_read_hook(&self) {
        self.rollup_state_store_context()
            .state
            .clear_source_read_hook();
    }

    pub(in crate::engine) fn rollup_policy_ids_needing_rebuild_for_rows(
        &self,
        rows: &[Row],
    ) -> BTreeSet<String> {
        self.rollup_invalidation_context()
            .rollup_policy_ids_needing_rebuild_for_rows(rows)
    }

    pub(in crate::engine) fn rollup_traversal_progress(&self) -> RollupTraversalProgress {
        let progress = self
            .rollup_run_coordination_context()
            .traversal_cursor
            .lock()
            .progress();
        if !progress.complete {
            return progress;
        }

        let policies = self.rollup_state_store_context().policies_snapshot();
        let stats = self.rollup_state_store_context().policy_stats_snapshot();
        if let Some(policy) = policies.iter().find(|policy| {
            !stats
                .get(&policy.id)
                .is_some_and(|state| state.source_traversal_complete)
        }) {
            return RollupTraversalProgress {
                complete: false,
                continuation_policy_id: Some(policy.id.clone()),
                continuation_after_series_id: None,
            };
        }
        progress
    }

    pub(super) fn rollup_traversal_cycle_complete(&self) -> bool {
        self.rollup_run_coordination_context()
            .traversal_cursor
            .lock()
            .cycle_complete
    }

    fn rollup_source_page_limits(&self) -> BackgroundRollupPassLimits {
        // A bounded page retains decoded labels plus the canonical source key for every selected
        // series. Model the temporary canonical bytes conservatively as another three identities;
        // label structs and posting ids are accounted separately. Raw point/query work is not
        // retained across sources, but its per-source calibration remains a documented boundary.
        let modeled_source_bytes = self
            .runtime
            .max_series_identity_bytes
            .saturating_mul(5)
            .saturating_add(
                self.runtime
                    .max_labels_per_series
                    .saturating_mul(std::mem::size_of::<Label>().saturating_add(4)),
            )
            .saturating_add(std::mem::size_of::<RollupSourceSeries>())
            .saturating_add(std::mem::size_of::<SeriesId>())
            .max(1);
        let modeled_source_bytes_u64 = u64::try_from(modeled_source_bytes).unwrap_or(u64::MAX);
        let byte_limited = self.runtime.maintenance_max_bytes_per_pass / modeled_source_bytes_u64;
        let byte_limited = usize::try_from(byte_limited).unwrap_or(usize::MAX);
        BackgroundRollupPassLimits {
            source_limit: self
                .runtime
                .maintenance_max_items_per_pass
                .min(byte_limited),
            item_limit: self.runtime.maintenance_max_items_per_pass,
            byte_limit: self.runtime.maintenance_max_bytes_per_pass,
            modeled_source_bytes: modeled_source_bytes_u64,
        }
    }

    fn run_rollup_page_locked(
        &self,
        write_permit: &crate::concurrency::SemaphoreGuard<'_>,
        cursor: &mut BackgroundRollupCursor,
    ) -> Result<()> {
        run_rollup_pipeline_page_once_locked_impl(
            self.rollup_state_store_context(),
            self.rollup_source_read_context(),
            self.rollup_materialized_write_context(write_permit),
            &self.observability.rollup,
            cursor,
            self.rollup_source_page_limits(),
        )
    }

    pub(in crate::engine) fn run_rollup_pipeline_once_locked(
        &self,
        write_permit: &crate::concurrency::SemaphoreGuard<'_>,
    ) -> Result<RollupTraversalProgress> {
        let coordination = self.rollup_run_coordination_context();
        let mut cursor = coordination.traversal_cursor.lock();
        let drain_complete_cycle = self.resource_configuration.read().selected_profile
            == crate::ResourceProfileName::ExpertUnlimited;
        loop {
            self.run_rollup_page_locked(write_permit, &mut cursor)?;
            let progress = cursor.progress();
            if progress.complete || !drain_complete_cycle {
                return Ok(progress);
            }
        }
    }

    pub(in crate::engine) fn run_shared_background_rollup_pipeline_once(&self) -> Result<()> {
        self.ensure_open()?;
        let store = self.rollup_state_store_context();
        let idle_result = if store.policies_are_empty() {
            // An idle pass needs no materialized write capability. Taking only the rollup lock
            // cannot form the permit -> rollup-lock cycle guarded against by the write paths:
            // this branch never waits for a permit. After the optimistic read above, the lock
            // makes the decisive empty-policy check atomic with policy publication/removal and
            // lets us preserve the normal empty-pass cursor cleanup and observability updates.
            let coordination = self.rollup_run_coordination_context();
            let _run_guard = coordination.run_lock.lock();
            self.ensure_open()?;
            if store.policies_are_empty() {
                let mut cursor = coordination.traversal_cursor.lock();
                Some(
                    begin_rollup_pipeline_page_once_locked(
                        store,
                        &self.observability.rollup,
                        &mut cursor,
                    )
                    .map(|started| {
                        complete_idle_rollup_pipeline_page(
                            &self.observability.rollup,
                            &mut cursor,
                            started,
                        );
                    }),
                )
            } else {
                None
            }
        } else {
            None
        };
        if let Some(result) = idle_result {
            self.enforce_post_commit_memory_budget_best_effort();
            return result;
        }

        let write_permits = self
            .runtime
            .write_limiter
            .acquire_all(self.runtime.write_timeout)?;
        let write_permit = write_permits
            .first()
            .expect("the write limiter always owns at least one permit");
        self.ensure_open()?;
        let result = {
            let coordination = self.rollup_run_coordination_context();
            let _run_guard = coordination.run_lock.lock();
            let mut cursor = coordination.traversal_cursor.lock();
            self.run_rollup_page_locked(write_permit, &mut cursor)
        };
        drop(write_permits);
        self.enforce_post_commit_memory_budget_best_effort();
        result
    }

    #[cfg(test)]
    pub(in crate::engine) fn run_background_rollup_pipeline_once(
        &self,
        cursor: &mut BackgroundRollupCursor,
    ) -> Result<()> {
        self.ensure_open()?;
        let write_permits = self
            .runtime
            .write_limiter
            .acquire_all(self.runtime.write_timeout)?;
        let write_permit = write_permits
            .first()
            .expect("the write limiter always owns at least one permit");
        self.ensure_open()?;
        let result = {
            let _run_guard = self.rollup_run_coordination_context().run_lock.lock();
            self.run_rollup_page_locked(write_permit, cursor)
        };
        drop(write_permits);
        self.enforce_post_commit_memory_budget_best_effort();
        result
    }

    pub(in crate::engine) fn run_rollup_pipeline_once_with_snapshot(
        &self,
    ) -> Result<crate::storage::RollupObservabilitySnapshot> {
        self.ensure_open()?;
        let write_permits = self
            .runtime
            .write_limiter
            .acquire_all(self.runtime.write_timeout)?;
        let write_permit = write_permits
            .first()
            .expect("the write limiter always owns at least one permit");
        self.ensure_open()?;
        let result = {
            let _run_guard = self.rollup_run_coordination_context().run_lock.lock();
            self.run_rollup_pipeline_once_locked(write_permit)
                // Keep the status snapshot in the same serialization window as the explicit run.
                // Otherwise the background worker can begin the next traversal, clear
                // `source_traversal_complete`, and make this completed call return stale status.
                .map(|progress| self.rollup_observability_snapshot_with_progress(progress))
        };
        drop(write_permits);
        self.enforce_post_commit_memory_budget_best_effort();
        result
    }
}

#[cfg(test)]
mod transformation_memory_tests {
    use super::*;
    use crate::{QueryBudgetError, QueryBudgetLimits, QueryLimitReason, QueryWorkLimits, Value};
    use tempfile::TempDir;

    fn finite_execution(memory_limit: u64) -> (crate::QueryBudget, crate::QueryExecution) {
        let budget = crate::QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(memory_limit),
            per_query: QueryWorkLimits {
                max_intermediate_vector_size: Some(1_024),
                max_memory_bytes: Some(memory_limit),
                ..QueryWorkLimits::default()
            },
        })
        .unwrap();
        let execution = budget.begin_query().unwrap();
        (budget, execution)
    }

    struct FixedSourceReads {
        budget: crate::QueryBudget,
        points: Vec<DataPoint>,
    }

    impl RollupSourceReadOps for FixedSourceReads {
        fn live_series_ids(
            &self,
            candidate_series_ids: Vec<SeriesId>,
            _prune_dead: bool,
        ) -> Result<Vec<SeriesId>> {
            Ok(candidate_series_ids)
        }

        fn query_tier_plan(&self, start: i64, end: i64) -> TieredQueryPlan {
            TieredQueryPlan::from_cutoffs(start, end, None, None)
        }

        fn begin_rollup_source_execution(&self) -> Result<QueryExecution> {
            let execution = self.budget.begin_query()?;
            execution.charge_series_matched(1)?;
            Ok(execution)
        }

        fn collect_points_for_series_with_plan(
            &self,
            _series_id: SeriesId,
            _start: i64,
            _end: i64,
            _plan: TieredQueryPlan,
            execution: &QueryExecution,
        ) -> Result<(Vec<DataPoint>, crate::QueryMemoryReservation)> {
            let mut reservation =
                execution.reserve_memory(modeled_points_retained_bytes(&self.points))?;
            let points = self.points.clone();
            reservation.resize(modeled_points_retained_bytes(&points))?;
            Ok((points, reservation))
        }

        fn bounded_recency_reference_timestamp(&self) -> Option<i64> {
            self.points.last().map(|point| point.timestamp)
        }
    }

    #[derive(Default)]
    struct CapturingRollupWrites {
        rows: Mutex<Vec<Row>>,
    }

    impl RollupMaterializedWriteOps for CapturingRollupWrites {
        fn insert_rows_with_held_permit(
            &self,
            rows: &[Row],
            _write_permit: &crate::concurrency::SemaphoreGuard<'_>,
        ) -> Result<WriteResult> {
            self.rows.lock().extend(rows.iter().cloned());
            Ok(WriteResult::new(crate::WriteAcknowledgement::Durable))
        }
    }

    #[test]
    fn bounded_page_reads_only_selected_source_state() {
        let runtime = RollupRuntimeState::new_with_disk_budget(None, None);
        let store = RollupStateStoreContext { state: &runtime };
        let policy = RollupPolicy {
            id: "bounded-page-state".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 1_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        };
        let source = RollupSourceSeries {
            series_id: 1,
            source_key: source_series_key(&policy.metric, &[]),
            labels: Vec::new(),
        };
        let mut policy_checkpoints = BTreeMap::new();
        let mut policy_pending = BTreeMap::new();
        for index in 0_i64..4_096 {
            let source_key = format!("unrelated-source-{index:04}");
            policy_checkpoints.insert(source_key.clone(), index);
            policy_pending.insert(
                source_key,
                PendingRollupMaterialization {
                    checkpoint: index,
                    materialized_through: index.saturating_add(1_000),
                    generation: 0,
                },
            );
        }
        policy_checkpoints.insert(source.source_key.clone(), 2_000);
        store.install_snapshot(RollupRuntimeSnapshot {
            policies: vec![policy.clone()],
            checkpoints: HashMap::from([(policy.id.clone(), policy_checkpoints)]),
            pending_materializations: HashMap::from([(policy.id.clone(), policy_pending)]),
            pending_delete_invalidations: Vec::new(),
            generations: HashMap::from([(policy.id.clone(), 0)]),
            policy_stats: BTreeMap::new(),
        });
        let reads = FixedSourceReads {
            budget: crate::QueryBudget::new(QueryBudgetLimits::default()).unwrap(),
            points: Vec::new(),
        };
        let registry = RwLock::new(SeriesRegistry::new());
        let writes = CapturingRollupWrites::default();
        let semaphore = crate::concurrency::Semaphore::new(1);
        let write_permit = semaphore.acquire();

        reset_rollup_source_page_state_lookups();
        let report = match run_rollup_policy_sources_once(
            store,
            RollupSourceReadContext {
                registry: RollupRegistryReadContext {
                    registry: &registry,
                },
                ops: &reads,
            },
            RollupMaterializedWriteContext {
                ops: &writes,
                write_permit: &write_permit,
                write_batch_limits: crate::WriteBatchLimits::default(),
            },
            &policy,
            2_000,
            std::slice::from_ref(&source),
            false,
        ) {
            Ok(report) => report,
            Err(error) => panic!("bounded page state lookup failed: {}", error.error()),
        };

        assert_eq!(rollup_source_page_state_lookups(), 1);
        assert_eq!(report.matched_series, 1);
        assert_eq!(report.materialized_series, 1);
        assert_eq!(report.materialized_through, Some(2_000));
        assert!(!report.checkpoint_changed);
        assert_eq!(
            runtime
                .checkpoints
                .read()
                .get(&policy.id)
                .map(BTreeMap::len),
            Some(4_097)
        );
        assert_eq!(
            runtime
                .pending_materializations
                .read()
                .get(&policy.id)
                .map(BTreeMap::len),
            Some(4_096)
        );
        assert!(writes.rows.lock().is_empty());
    }

    #[test]
    fn page_local_lookup_preserves_pending_retry_target() {
        let data_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(data_dir.path().join(ROLLUP_DIR_NAME)).unwrap();
        let runtime =
            RollupRuntimeState::new_with_disk_budget(Some(data_dir.path().to_path_buf()), None);
        let store = RollupStateStoreContext { state: &runtime };
        let policy = RollupPolicy {
            id: "pending-retry-target".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 1_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        };
        let source = RollupSourceSeries {
            series_id: 1,
            source_key: source_series_key(&policy.metric, &[]),
            labels: Vec::new(),
        };
        store.install_snapshot(RollupRuntimeSnapshot {
            policies: vec![policy.clone()],
            checkpoints: HashMap::from([(
                policy.id.clone(),
                BTreeMap::from([(source.source_key.clone(), 1_000)]),
            )]),
            pending_materializations: HashMap::from([(
                policy.id.clone(),
                BTreeMap::from([(
                    source.source_key.clone(),
                    PendingRollupMaterialization {
                        checkpoint: 1_000,
                        materialized_through: 4_000,
                        generation: 0,
                    },
                )]),
            )]),
            pending_delete_invalidations: Vec::new(),
            generations: HashMap::from([(policy.id.clone(), 0)]),
            policy_stats: BTreeMap::new(),
        });
        let reads = FixedSourceReads {
            budget: crate::QueryBudget::new(QueryBudgetLimits::default()).unwrap(),
            points: Vec::new(),
        };
        let registry = RwLock::new(SeriesRegistry::new());
        let writes = CapturingRollupWrites::default();
        let semaphore = crate::concurrency::Semaphore::new(1);
        let write_permit = semaphore.acquire();

        reset_rollup_source_page_state_lookups();
        let report = match run_rollup_policy_sources_once(
            store,
            RollupSourceReadContext {
                registry: RollupRegistryReadContext {
                    registry: &registry,
                },
                ops: &reads,
            },
            RollupMaterializedWriteContext {
                ops: &writes,
                write_permit: &write_permit,
                write_batch_limits: crate::WriteBatchLimits::default(),
            },
            &policy,
            2_000,
            std::slice::from_ref(&source),
            false,
        ) {
            Ok(report) => report,
            Err(error) => panic!("pending retry target failed: {}", error.error()),
        };

        assert_eq!(rollup_source_page_state_lookups(), 1);
        assert!(report.checkpoint_changed);
        assert_eq!(report.materialized_series, 1);
        assert_eq!(report.materialized_through, Some(4_000));
        assert_eq!(
            runtime
                .checkpoints
                .read()
                .get(&policy.id)
                .and_then(|entries| entries.get(&source.source_key))
                .copied(),
            Some(4_000)
        );
        assert!(runtime.pending_materializations.read().is_empty());
        assert!(writes.rows.lock().is_empty());
    }

    #[test]
    fn earlier_success_survives_later_isolated_failure_in_page_coverage() {
        let data_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(data_dir.path().join(ROLLUP_DIR_NAME)).unwrap();
        let runtime =
            RollupRuntimeState::new_with_disk_budget(Some(data_dir.path().to_path_buf()), None);
        let store = RollupStateStoreContext { state: &runtime };
        let policy = RollupPolicy {
            id: "partial-page-coverage".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 1_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        };
        let sources = [
            RollupSourceSeries {
                series_id: 1,
                source_key: "cpu_usage{host=\"a\"}".to_string(),
                labels: vec![Label::new("host", "a")],
            },
            RollupSourceSeries {
                series_id: 2,
                source_key: "cpu_usage{host=\"b\"}".to_string(),
                labels: vec![Label::new("host", "b")],
            },
        ];
        store.install_snapshot(RollupRuntimeSnapshot {
            policies: vec![policy.clone()],
            checkpoints: HashMap::new(),
            pending_materializations: HashMap::new(),
            pending_delete_invalidations: Vec::new(),
            generations: HashMap::from([(policy.id.clone(), 0)]),
            policy_stats: BTreeMap::new(),
        });
        let reads = FixedSourceReads {
            budget: crate::QueryBudget::new(QueryBudgetLimits::default()).unwrap(),
            points: vec![DataPoint::new(0, 1.0), DataPoint::new(1_000, 3.0)],
        };
        let failing_series_id = sources[1].series_id;
        runtime.set_source_read_hook(move |_policy, series_id| {
            if series_id == failing_series_id {
                Err(TsinkError::Other(
                    "injected later-source read failure".to_string(),
                ))
            } else {
                Ok(())
            }
        });
        let registry = RwLock::new(SeriesRegistry::new());
        let writes = CapturingRollupWrites::default();
        let semaphore = crate::concurrency::Semaphore::new(1);
        let write_permit = semaphore.acquire();

        reset_rollup_source_page_state_lookups();
        let error = run_rollup_policy_sources_once(
            store,
            RollupSourceReadContext {
                registry: RollupRegistryReadContext {
                    registry: &registry,
                },
                ops: &reads,
            },
            RollupMaterializedWriteContext {
                ops: &writes,
                write_permit: &write_permit,
                write_batch_limits: crate::WriteBatchLimits::default(),
            },
            &policy,
            2_000,
            &sources,
            true,
        )
        .expect_err("the later source must fail in isolation");
        runtime.clear_source_read_hook();

        assert!(matches!(
            error,
            PolicySourceRunError::Isolated(TsinkError::Other(message))
                if message == "injected later-source read failure"
        ));
        assert_eq!(rollup_source_page_state_lookups(), 2);
        assert_eq!(
            runtime
                .checkpoints
                .read()
                .get(&policy.id)
                .and_then(|entries| entries.get(&sources[0].source_key))
                .copied(),
            Some(2_000)
        );
        assert_eq!(
            runtime
                .checkpoints
                .read()
                .get(&policy.id)
                .and_then(|entries| entries.get(&sources[1].source_key))
                .copied(),
            None
        );
        assert!(runtime.pending_materializations.read().is_empty());
        let coverage = rollup_policy_page_coverage(store, &policy.id, &sources);
        assert_eq!(coverage.matched_series, 2);
        assert_eq!(coverage.materialized_series, 1);
        assert_eq!(coverage.materialized_through, Some(2_000));
        assert_eq!(writes.rows.lock().len(), 2);
    }

    #[test]
    fn fully_checkpointed_page_skips_corrupt_journal_discovery() {
        let data_dir = TempDir::new().unwrap();
        let rollup_dir = data_dir.path().join(ROLLUP_DIR_NAME);
        std::fs::create_dir_all(&rollup_dir).unwrap();
        let batch = rollup_dir.join("state-journal-batch-0000000000000001.d");
        std::fs::create_dir(&batch).unwrap();
        let unknown = batch.join("operator.keep");
        std::fs::write(&unknown, b"preserved").unwrap();

        let runtime =
            RollupRuntimeState::new_with_disk_budget(Some(data_dir.path().to_path_buf()), None);
        let store = RollupStateStoreContext { state: &runtime };
        let policy = RollupPolicy {
            id: "no-event-page".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 1_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        };
        let source = RollupSourceSeries {
            series_id: 1,
            source_key: source_series_key(&policy.metric, &[]),
            labels: Vec::new(),
        };
        runtime.generations.write().insert(policy.id.clone(), 0);
        runtime
            .checkpoints
            .write()
            .entry(policy.id.clone())
            .or_default()
            .insert(source.source_key.clone(), 2_000);
        let reads = FixedSourceReads {
            budget: crate::QueryBudget::new(QueryBudgetLimits::default()).unwrap(),
            points: Vec::new(),
        };
        let registry = RwLock::new(SeriesRegistry::new());
        let registry_context = RollupRegistryReadContext {
            registry: &registry,
        };
        let writes = CapturingRollupWrites::default();
        let semaphore = crate::concurrency::Semaphore::new(1);
        let write_permit = semaphore.acquire();
        let report = match run_rollup_policy_sources_once(
            store,
            RollupSourceReadContext {
                registry: registry_context,
                ops: &reads,
            },
            RollupMaterializedWriteContext {
                ops: &writes,
                write_permit: &write_permit,
                write_batch_limits: crate::WriteBatchLimits::default(),
            },
            &policy,
            2_000,
            std::slice::from_ref(&source),
            false,
        ) {
            Ok(report) => report,
            Err(error) => panic!(
                "a no-event page must not discover an unrelated corrupt journal batch: {}",
                error.error()
            ),
        };
        assert!(!report.checkpoint_changed);
        assert_eq!(std::fs::read(unknown).unwrap(), b"preserved");
        assert!(writes.rows.lock().is_empty());
    }

    #[test]
    fn multi_source_page_batches_journal_events_without_full_root_reconciliation() {
        let data_dir = TempDir::new().unwrap();
        let rollup_dir = data_dir.path().join(ROLLUP_DIR_NAME);
        std::fs::create_dir_all(&rollup_dir).unwrap();
        let budget =
            crate::LocalDiskBudget::open(data_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let runtime = RollupRuntimeState::new_with_disk_budget(
            Some(data_dir.path().to_path_buf()),
            Some(Arc::clone(&budget)),
        );
        let store = RollupStateStoreContext { state: &runtime };
        let policy = RollupPolicy {
            id: "batched-page".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 1_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        };
        runtime.generations.write().insert(policy.id.clone(), 0);
        let sources = [
            RollupSourceSeries {
                series_id: 1,
                source_key: "cpu_usage{host=\"a\"}".to_string(),
                labels: vec![Label::new("host", "a")],
            },
            RollupSourceSeries {
                series_id: 2,
                source_key: "cpu_usage{host=\"b\"}".to_string(),
                labels: vec![Label::new("host", "b")],
            },
        ];
        let reads = FixedSourceReads {
            budget: crate::QueryBudget::new(QueryBudgetLimits::default()).unwrap(),
            points: vec![DataPoint::new(0, 1.0), DataPoint::new(1_000, 3.0)],
        };
        let registry = RwLock::new(SeriesRegistry::new());
        let registry_context = RollupRegistryReadContext {
            registry: &registry,
        };
        let writes = CapturingRollupWrites::default();
        let semaphore = crate::concurrency::Semaphore::new(1);
        let write_permit = semaphore.acquire();
        let before = budget.snapshot();

        let report = match run_rollup_policy_sources_once(
            store,
            RollupSourceReadContext {
                registry: registry_context,
                ops: &reads,
            },
            RollupMaterializedWriteContext {
                ops: &writes,
                write_permit: &write_permit,
                write_batch_limits: crate::WriteBatchLimits::default(),
            },
            &policy,
            2_000,
            &sources,
            false,
        ) {
            Ok(report) => report,
            Err(error) => panic!("batched source page failed: {}", error.error()),
        };
        assert_eq!(report.materialized_series, 2);
        let after = budget.snapshot();
        assert_eq!(after.reconciliations_total, before.reconciliations_total);
        assert_eq!(after.active_reservations, 0);
        assert_eq!(after.reserved_bytes, 0);
        let batches = std::fs::read_dir(&rollup_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("state-journal-batch-"))
            })
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 1);
        assert_eq!(std::fs::read_dir(&batches[0]).unwrap().count(), 4);
    }

    #[test]
    fn rollup_downsample_memory_boundary_rejects_before_output_and_releases_for_retry() {
        let points = vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 2.0),
            DataPoint::new(2_000, 3.0),
            DataPoint::new(3_000, 4.0),
        ];
        let retained_input = modeled_points_retained_bytes(&points);
        let output_count = rollup_downsample_output_count_upper(&points, 1_000, 0, 0, 4_000);
        let output_upper = modeled_point_output_upper_bound_bytes(
            &points,
            modeled_vec_growth_capacity_upper(output_count),
        );
        let scratch = rollup_aggregation_scratch_bytes(points.len(), Aggregation::Avg);
        let exact_peak = retained_input
            .saturating_add(output_upper)
            .saturating_add(scratch);

        let (rejected_budget, rejected_execution) = finite_execution(exact_peak.saturating_sub(1));
        let rejected_input_reservation = rejected_execution.reserve_memory(retained_input).unwrap();
        let error = downsample_rollup_points(
            &rejected_execution,
            &points,
            1_000,
            Aggregation::Avg,
            0,
            0,
            4_000,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
                    && exceeded.limit == exact_peak.saturating_sub(1)
                    && exceeded.current == retained_input.saturating_add(output_upper)
                    && exceeded.requested == scratch
        ));
        assert_eq!(
            rejected_execution.snapshot().memory_reserved_bytes,
            retained_input,
            "a rejected transform must release its preflighted output reservation"
        );
        drop(rejected_input_reservation);
        drop(rejected_execution);
        assert_eq!(rejected_budget.snapshot().active_queries, 0);
        assert_eq!(rejected_budget.snapshot().shared_reserved_memory_bytes, 0);

        let (exact_budget, exact_execution) = finite_execution(exact_peak);
        let exact_input_reservation = exact_execution.reserve_memory(retained_input).unwrap();
        let (output, output_reservation) = downsample_rollup_points(
            &exact_execution,
            &points,
            1_000,
            Aggregation::Avg,
            0,
            0,
            4_000,
        )
        .unwrap();
        assert_eq!(
            output,
            vec![
                DataPoint::new(0, 1.0),
                DataPoint::new(1_000, 2.0),
                DataPoint::new(2_000, 3.0),
                DataPoint::new(3_000, 4.0),
            ]
        );
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            exact_peak
        );
        drop(output_reservation);
        drop(exact_input_reservation);
        drop(exact_execution);
        assert_eq!(exact_budget.snapshot().active_queries, 0);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn downsample_reservation_failure_preserves_source_state_and_exact_retry_commits() {
        let data_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(data_dir.path().join(ROLLUP_DIR_NAME)).unwrap();
        let runtime =
            RollupRuntimeState::new_with_disk_budget(Some(data_dir.path().to_path_buf()), None);
        let store = RollupStateStoreContext { state: &runtime };
        let registry = RwLock::new(SeriesRegistry::new());
        let registry_context = RollupRegistryReadContext {
            registry: &registry,
        };
        let writes = CapturingRollupWrites::default();
        let semaphore = crate::concurrency::Semaphore::new(1);
        let write_permit = semaphore.acquire();
        let materialized_writes = RollupMaterializedWriteContext {
            ops: &writes,
            write_permit: &write_permit,
            write_batch_limits: crate::WriteBatchLimits::default(),
        };
        let policy = RollupPolicy {
            id: "downsample-memory-state".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 1_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        };
        let source = RollupSourceSeries {
            series_id: 1,
            source_key: source_series_key(&policy.metric, &[]),
            labels: Vec::new(),
        };
        let raw_points = (0..64)
            .map(|timestamp| DataPoint::new(timestamp, timestamp as f64))
            .collect::<Vec<_>>();
        let output_count =
            rollup_downsample_output_count_upper(&raw_points, 1_000, 0, i64::MIN, 1_000);
        assert_eq!(output_count, 1);
        let retained_raw = modeled_points_retained_bytes(&raw_points);
        let output_upper = modeled_point_output_upper_bound_bytes(
            &raw_points,
            modeled_vec_growth_capacity_upper(output_count),
        );
        let scratch = rollup_aggregation_scratch_bytes(raw_points.len(), policy.aggregation);
        let exact_transform_peak = retained_raw
            .saturating_add(output_upper)
            .saturating_add(scratch);
        let expected_rollup_points = downsample_points_with_origin(
            &raw_points,
            policy.interval,
            policy.aggregation,
            policy.bucket_origin,
            i64::MIN,
            1_000,
        )
        .unwrap();
        let later_row_peak = modeled_points_retained_bytes(&expected_rollup_points).saturating_add(
            modeled_rollup_row_output_upper_bytes(
                &rollup_metric_name(&policy, 0),
                &[],
                &expected_rollup_points,
                &[],
            ),
        );
        assert!(
            later_row_peak <= exact_transform_peak,
            "the exact transform boundary must also admit the smaller row stage"
        );

        let rejected_budget = crate::QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(exact_transform_peak.saturating_sub(1)),
            per_query: QueryWorkLimits {
                max_series_matched: Some(1),
                max_intermediate_vector_size: Some(64),
                max_memory_bytes: Some(exact_transform_peak.saturating_sub(1)),
                ..QueryWorkLimits::default()
            },
        })
        .unwrap();
        let rejected_reads = FixedSourceReads {
            budget: rejected_budget.clone(),
            points: raw_points.clone(),
        };
        let error = run_rollup_policy_sources_once(
            store,
            RollupSourceReadContext {
                registry: registry_context,
                ops: &rejected_reads,
            },
            materialized_writes,
            &policy,
            1_000,
            std::slice::from_ref(&source),
            false,
        )
        .expect_err("one byte below the modeled transform peak must reject");
        assert!(matches!(
            error,
            PolicySourceRunError::Isolated(TsinkError::QueryBudget(
                QueryBudgetError::LimitExceeded(exceeded)
            )) if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
                && exceeded.limit == exact_transform_peak.saturating_sub(1)
                && exceeded.current == retained_raw.saturating_add(output_upper)
                && exceeded.requested == scratch
        ));
        assert!(runtime.checkpoints.read().is_empty());
        assert!(runtime.pending_materializations.read().is_empty());
        assert!(writes.rows.lock().is_empty());
        let rejected_snapshot = rejected_budget.snapshot();
        assert_eq!(rejected_snapshot.active_queries, 0);
        assert_eq!(rejected_snapshot.shared_reserved_memory_bytes, 0);

        let exact_budget = crate::QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(exact_transform_peak),
            per_query: QueryWorkLimits {
                max_series_matched: Some(1),
                max_intermediate_vector_size: Some(64),
                max_memory_bytes: Some(exact_transform_peak),
                ..QueryWorkLimits::default()
            },
        })
        .unwrap();
        let exact_reads = FixedSourceReads {
            budget: exact_budget.clone(),
            points: raw_points,
        };
        let report = match run_rollup_policy_sources_once(
            store,
            RollupSourceReadContext {
                registry: registry_context,
                ops: &exact_reads,
            },
            materialized_writes,
            &policy,
            1_000,
            std::slice::from_ref(&source),
            false,
        ) {
            Ok(report) => report,
            Err(error) => panic!(
                "the exact modeled transform peak must admit a complete retry: {}",
                error.error()
            ),
        };
        assert_eq!(report.buckets_materialized, 1);
        assert_eq!(
            runtime
                .checkpoints
                .read()
                .get(&policy.id)
                .and_then(|entries| entries.get(&source.source_key))
                .copied(),
            Some(1_000)
        );
        assert!(runtime.pending_materializations.read().is_empty());
        assert_eq!(writes.rows.lock().len(), 1);
        let exact_snapshot = exact_budget.snapshot();
        assert_eq!(exact_snapshot.active_queries, 0);
        assert_eq!(exact_snapshot.shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn rollup_row_memory_boundary_is_complete_or_error_and_expert_unlimited_is_explicit() {
        let rollup_metric = "__tsink_rollup__:bounded-rows:cpu_usage";
        let labels = vec![Label::new("host", "a-long-but-bounded-series-identity")];
        let rollup_points = vec![
            DataPoint::new(0, Value::String("first".to_string())),
            DataPoint::new(1_000, Value::String("already-present".to_string())),
        ];
        let existing_rollup_points =
            vec![DataPoint::new(1_000, Value::String("existing".to_string()))];
        let retained_points = modeled_points_retained_bytes(&rollup_points);
        let row_upper = modeled_rollup_row_output_upper_bytes(
            rollup_metric,
            &labels,
            &rollup_points,
            &existing_rollup_points,
        );
        let exact_peak = retained_points.saturating_add(row_upper);

        let (rejected_budget, rejected_execution) = finite_execution(exact_peak.saturating_sub(1));
        let rejected_points_reservation =
            rejected_execution.reserve_memory(retained_points).unwrap();
        let error = assemble_rollup_rows(
            &rejected_execution,
            rollup_metric,
            &labels,
            rollup_points.clone(),
            &existing_rollup_points,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded))
                if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
                    && exceeded.limit == exact_peak.saturating_sub(1)
                    && exceeded.current == retained_points
                    && exceeded.requested == row_upper
        ));
        assert_eq!(
            rejected_execution.snapshot().memory_reserved_bytes,
            retained_points,
            "row rejection must not retain a partial output allocation"
        );
        drop(rejected_points_reservation);
        drop(rejected_execution);
        assert_eq!(rejected_budget.snapshot().active_queries, 0);
        assert_eq!(rejected_budget.snapshot().shared_reserved_memory_bytes, 0);

        let (exact_budget, exact_execution) = finite_execution(exact_peak);
        let exact_points_reservation = exact_execution.reserve_memory(retained_points).unwrap();
        let (rows, rows_reservation) = assemble_rollup_rows(
            &exact_execution,
            rollup_metric,
            &labels,
            rollup_points.clone(),
            &existing_rollup_points,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].metric(), rollup_metric);
        assert_eq!(rows[0].labels(), labels);
        assert_eq!(
            rows[0].data_point(),
            &DataPoint::new(0, Value::String("first".to_string()))
        );
        assert_eq!(
            exact_budget.snapshot().peak_shared_reserved_memory_bytes,
            exact_peak
        );
        drop(rows_reservation);
        drop(exact_points_reservation);
        drop(exact_execution);
        assert_eq!(exact_budget.snapshot().active_queries, 0);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let unlimited_budget = crate::QueryBudget::new(QueryBudgetLimits::default()).unwrap();
        let unlimited_execution = unlimited_budget.begin_query().unwrap();
        assert_eq!(unlimited_execution.limits(), QueryWorkLimits::default());
        let (unlimited_rows, unlimited_reservation) = assemble_rollup_rows(
            &unlimited_execution,
            rollup_metric,
            &labels,
            rollup_points,
            &existing_rollup_points,
        )
        .unwrap();
        assert_eq!(unlimited_rows.len(), 1);
        drop(unlimited_reservation);
        drop(unlimited_execution);
        let unlimited_snapshot = unlimited_budget.snapshot();
        assert_eq!(unlimited_snapshot.limit_rejections_total, 0);
        assert_eq!(unlimited_snapshot.shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn row_reservation_failure_preserves_source_state_and_exact_retry_commits() {
        let data_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(data_dir.path().join(ROLLUP_DIR_NAME)).unwrap();
        let runtime =
            RollupRuntimeState::new_with_disk_budget(Some(data_dir.path().to_path_buf()), None);
        let store = RollupStateStoreContext { state: &runtime };
        let registry = RwLock::new(SeriesRegistry::new());
        let registry_context = RollupRegistryReadContext {
            registry: &registry,
        };
        let writes = CapturingRollupWrites::default();
        let semaphore = crate::concurrency::Semaphore::new(1);
        let write_permit = semaphore.acquire();
        let materialized_writes = RollupMaterializedWriteContext {
            ops: &writes,
            write_permit: &write_permit,
            write_batch_limits: crate::WriteBatchLimits::default(),
        };
        let policy = RollupPolicy {
            id: "row-memory-state".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 1_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        };
        let labels = vec![Label::new("host", "x".repeat(2_048))];
        let source = RollupSourceSeries {
            series_id: 1,
            source_key: source_series_key(&policy.metric, &labels),
            labels: labels.clone(),
        };
        let raw_points = vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 2.0),
            DataPoint::new(2_000, 3.0),
            DataPoint::new(3_000, 4.0),
        ];
        let rollup_points = downsample_points_with_origin(
            &raw_points,
            policy.interval,
            policy.aggregation,
            policy.bucket_origin,
            i64::MIN,
            4_000,
        )
        .unwrap();
        let retained_raw = modeled_points_retained_bytes(&raw_points);
        let transform_peak = retained_raw
            .saturating_add(modeled_point_output_upper_bound_bytes(
                &raw_points,
                modeled_vec_growth_capacity_upper(rollup_points.len()),
            ))
            .saturating_add(rollup_aggregation_scratch_bytes(
                raw_points.len(),
                policy.aggregation,
            ));
        let retained_rollup = modeled_points_retained_bytes(&rollup_points);
        let row_upper = modeled_rollup_row_output_upper_bytes(
            &rollup_metric_name(&policy, 0),
            &labels,
            &rollup_points,
            &[],
        );
        let exact_row_peak = retained_rollup.saturating_add(row_upper);
        assert!(
            exact_row_peak > transform_peak,
            "the fixture must reach row admission after its source transform fits"
        );

        let rejected_budget = crate::QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(exact_row_peak.saturating_sub(1)),
            per_query: QueryWorkLimits {
                max_series_matched: Some(1),
                max_intermediate_vector_size: Some(16),
                max_memory_bytes: Some(exact_row_peak.saturating_sub(1)),
                ..QueryWorkLimits::default()
            },
        })
        .unwrap();
        let rejected_reads = FixedSourceReads {
            budget: rejected_budget.clone(),
            points: raw_points.clone(),
        };
        let rejected_source_reads = RollupSourceReadContext {
            registry: registry_context,
            ops: &rejected_reads,
        };
        let error = run_rollup_policy_sources_once(
            store,
            rejected_source_reads,
            materialized_writes,
            &policy,
            4_000,
            std::slice::from_ref(&source),
            false,
        )
        .expect_err("one byte below the modeled row peak must reject");
        assert!(matches!(
            error,
            PolicySourceRunError::Isolated(TsinkError::QueryBudget(
                QueryBudgetError::LimitExceeded(exceeded)
            )) if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
                && exceeded.limit == exact_row_peak.saturating_sub(1)
                && exceeded.current == retained_rollup
                && exceeded.requested == row_upper
        ));
        assert!(runtime.checkpoints.read().is_empty());
        assert!(runtime.pending_materializations.read().is_empty());
        assert!(writes.rows.lock().is_empty());
        let rejected_snapshot = rejected_budget.snapshot();
        assert_eq!(rejected_snapshot.active_queries, 0);
        assert_eq!(rejected_snapshot.shared_reserved_memory_bytes, 0);

        let exact_budget = crate::QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(exact_row_peak),
            per_query: QueryWorkLimits {
                max_series_matched: Some(1),
                max_intermediate_vector_size: Some(16),
                max_memory_bytes: Some(exact_row_peak),
                ..QueryWorkLimits::default()
            },
        })
        .unwrap();
        let exact_reads = FixedSourceReads {
            budget: exact_budget.clone(),
            points: raw_points,
        };
        let exact_source_reads = RollupSourceReadContext {
            registry: registry_context,
            ops: &exact_reads,
        };
        let report = match run_rollup_policy_sources_once(
            store,
            exact_source_reads,
            materialized_writes,
            &policy,
            4_000,
            std::slice::from_ref(&source),
            false,
        ) {
            Ok(report) => report,
            Err(error) => panic!(
                "the exact modeled row peak must admit a complete retry: {}",
                error.error()
            ),
        };
        assert_eq!(report.buckets_materialized, 4);
        assert_eq!(
            runtime
                .checkpoints
                .read()
                .get(&policy.id)
                .and_then(|entries| entries.get(&source.source_key))
                .copied(),
            Some(4_000)
        );
        assert!(runtime.pending_materializations.read().is_empty());
        assert_eq!(writes.rows.lock().len(), 4);
        let exact_snapshot = exact_budget.snapshot();
        assert_eq!(exact_snapshot.active_queries, 0);
        assert_eq!(exact_snapshot.shared_reserved_memory_bytes, 0);
    }
}
