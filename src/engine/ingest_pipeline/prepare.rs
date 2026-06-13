use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::super::super::{
    partition_id_for_timestamp, state, value_heap_bytes, ActiveSeriesState, ChunkPoint, FramedWal,
    Result, SeriesDefinitionFrame, SeriesId, SeriesRegistry, SeriesResolution, SeriesValueFamily,
    SeriesVisibilitySummary, TsinkError, ValueLane, WriteAdmissionControlContext,
    WritePrepareContext, WritePrepareMemoryBudgetContext, WritePrepareVisibilityContext,
    WritePrepareWalContext,
};
use super::apply::WriteApplier;
use super::phases::{
    PendingPoint, PrepareResolvedWriteError, PreparedWalWrite, PreparedWrite, ResolvedWrite,
};
use super::resolve::WriteResolver;

struct PendingPartitionHeadState {
    point_cap: usize,
    partition_heads: BTreeMap<i64, usize>,
    current_partition_id: Option<i64>,
}

impl PendingPartitionHeadState {
    fn new(point_cap: usize) -> Self {
        Self {
            point_cap: point_cap.max(1),
            partition_heads: BTreeMap::new(),
            current_partition_id: None,
        }
    }

    fn from_active_state(state: &ActiveSeriesState) -> Self {
        Self {
            point_cap: state.point_cap,
            partition_heads: state
                .partition_heads
                .iter()
                .map(|(partition_id, head)| (*partition_id, head.builder.len()))
                .collect(),
            current_partition_id: state.current_partition_id,
        }
    }

    fn rotate_partition_if_needed(
        &mut self,
        ts: i64,
        partition_window: i64,
        max_partition_heads: usize,
    ) -> Result<()> {
        let partition_window = partition_window.max(1);
        let next_partition = partition_id_for_timestamp(ts, partition_window);
        match state::plan_partition_head_open(
            &self.partition_heads,
            next_partition,
            ts,
            max_partition_heads,
        )? {
            state::PartitionHeadOpenAction::UseExisting => {
                self.current_partition_id = Some(next_partition);
                Ok(())
            }
            state::PartitionHeadOpenAction::OpenNew { evict_partition_id } => {
                if let Some(partition_id) = evict_partition_id {
                    self.finalize_partition_head(partition_id);
                }
                self.current_partition_id = Some(next_partition);
                self.partition_heads.entry(next_partition).or_insert(0);
                Ok(())
            }
        }
    }

    fn append_point(&mut self) {
        let partition_id = self
            .current_partition_id
            .expect("rotate_partition_if_needed must run before append_point");
        let head = self
            .partition_heads
            .get_mut(&partition_id)
            .expect("active partition head must exist before append_point");
        *head = head.saturating_add(1);
    }

    fn rotate_full_if_needed(&mut self) {
        let Some(partition_id) = self.current_partition_id else {
            return;
        };
        if self
            .partition_heads
            .get(&partition_id)
            .is_some_and(|point_count| *point_count >= self.point_cap)
        {
            self.partition_heads.insert(partition_id, 0);
        }
    }

    fn finalize_partition_head(&mut self, partition_id: i64) {
        if self.partition_heads.remove(&partition_id).is_none() {
            return;
        }
        if self.current_partition_id == Some(partition_id) {
            self.current_partition_id = self.partition_heads.keys().next_back().copied();
        }
    }
}

impl<'a> WritePrepareVisibilityContext<'a> {
    fn validate_points_against_retention(self, points: &[PendingPoint]) -> Result<()> {
        if !self.retention_enforced || points.is_empty() {
            return Ok(());
        }

        let wall_clock = self.clock.current_timestamp_units();
        let skew_cutoff = wall_clock.saturating_add(self.future_skew_window);
        let mut effective_reference = wall_clock;
        let current_bounded = self.max_bounded_observed_timestamp.load(Ordering::Acquire);
        if current_bounded != i64::MIN {
            effective_reference = effective_reference.max(current_bounded);
        }
        for point in points {
            if point.ts <= skew_cutoff {
                effective_reference = effective_reference.max(point.ts);
            }
        }

        let cutoff = effective_reference.saturating_sub(self.retention_window);
        for point in points {
            if point.ts < cutoff {
                return Err(TsinkError::OutOfRetention {
                    timestamp: point.ts,
                });
            }
        }
        Ok(())
    }

    fn estimate_metadata_growth_bytes(
        self,
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
        created_series: &[SeriesResolution],
    ) -> usize {
        let registry_pending = self.pending_series_ids.read();
        let bounded_cutoff = self.clock.current_future_skew_cutoff();

        let mut metadata_bytes =
            self.materialized_series
                .with_materialized_series(|materialized_series| {
                    self.visibility_cache.with_visibility_cache_state(
                        |visibility_summaries, visible_cache, bounded_visible_cache| {
                            let mut metadata_bytes = 0usize;
                            for (series_id, (_, indexes)) in grouped {
                                if !materialized_series.contains(series_id) {
                                    metadata_bytes = metadata_bytes
                                        .saturating_add(std::mem::size_of::<SeriesId>());
                                    if self.has_metadata_shards {
                                        metadata_bytes = metadata_bytes
                                            .saturating_add(std::mem::size_of::<SeriesId>());
                                    }
                                }
                                if !visible_cache.contains_key(series_id) {
                                    metadata_bytes = metadata_bytes.saturating_add(
                                        std::mem::size_of::<(SeriesId, Option<i64>)>(),
                                    );
                                }
                                if !visibility_summaries.contains_key(series_id) {
                                    metadata_bytes = metadata_bytes.saturating_add(
                                        std::mem::size_of::<(SeriesId, SeriesVisibilitySummary)>(),
                                    );
                                }
                                let needs_bounded_entry =
                                    indexes.iter().any(|idx| points[*idx].ts <= bounded_cutoff);
                                if needs_bounded_entry
                                    && !bounded_visible_cache.contains_key(series_id)
                                {
                                    metadata_bytes = metadata_bytes.saturating_add(
                                        std::mem::size_of::<(SeriesId, Option<i64>)>(),
                                    );
                                }
                            }
                            metadata_bytes
                        },
                    )
                });
        for series in created_series {
            if !registry_pending.contains(&series.series_id) {
                metadata_bytes = metadata_bytes.saturating_add(std::mem::size_of::<SeriesId>());
            }
        }

        metadata_bytes
    }
}

impl<'a> WritePrepareMemoryBudgetContext<'a> {
    fn shortfall(self, estimated_growth_bytes: usize) -> Option<(usize, usize)> {
        let budget = self
            .budget_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize;
        if budget == usize::MAX {
            return None;
        }

        let used = self
            .used_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize;
        let required = used.saturating_add(estimated_growth_bytes);
        (required > budget).then_some((budget, required))
    }
}

impl<'a> WritePrepareWalContext<'a> {
    fn size_shortfall(self, estimated_growth_bytes: u64) -> Result<Option<(u64, u64)>> {
        let limit = self.wal_size_limit_bytes;
        if limit == u64::MAX || estimated_growth_bytes == 0 {
            return Ok(None);
        }

        let Some(wal) = self.wal else {
            return Ok(None);
        };

        if estimated_growth_bytes > limit {
            return Ok(Some((limit, estimated_growth_bytes)));
        }

        let current = wal.total_size_bytes()?;
        let required = current.saturating_add(estimated_growth_bytes);
        Ok((required > limit).then_some((limit, required)))
    }

    fn prepare_wal_write(
        self,
        new_series_defs: &[SeriesDefinitionFrame],
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
    ) -> Result<Option<PreparedWalWrite>> {
        let Some(_wal) = self.wal else {
            return Ok(None);
        };

        let mut encoded_series_definition_payloads = Vec::with_capacity(new_series_defs.len());
        let mut encoded_bytes = 0u64;
        for definition in new_series_defs {
            let payload = FramedWal::encode_series_definition_frame_payload(definition)?;
            encoded_bytes = encoded_bytes
                .saturating_add(FramedWal::frame_size_bytes_for_payload_len(payload.len()));
            encoded_series_definition_payloads.push(payload);
        }

        let mut encoded_samples_payload = None;
        let mut sample_batch_count = 0usize;
        let mut sample_point_count = 0usize;
        if !grouped.is_empty() {
            let batches = WriteApplier::encode_wal_batches(points, grouped)?;
            sample_batch_count = batches.len();
            sample_point_count = batches.iter().map(|batch| batch.point_count as usize).sum();
            let payload = FramedWal::encode_samples_frame_payload(&batches)?;
            encoded_bytes = encoded_bytes
                .saturating_add(FramedWal::frame_size_bytes_for_payload_len(payload.len()));
            encoded_samples_payload = Some(payload);
        }

        Ok(Some(PreparedWalWrite {
            encoded_series_definition_payloads,
            encoded_samples_payload,
            encoded_bytes,
            sample_batch_count,
            sample_point_count,
        }))
    }
}

impl<'a> WriteAdmissionControlContext<'a> {
    fn request_admission_pressure_relief(self) -> bool {
        let Some(_backpressure_guard) = self.admission_backpressure_lock.try_lock() else {
            return false;
        };

        self.observability
            .record_admission_pressure_relief_request();
        self.workers.notify_flush_thread();
        self.workers.notify_persisted_refresh_thread();
        true
    }

    fn delay_for_admission_backpressure(self, deadline: Instant) {
        let delay = self
            .admission_poll_interval
            .min(deadline.saturating_duration_since(Instant::now()));
        if delay.is_zero() {
            return;
        }

        self.observability
            .record_admission_backpressure_delay(delay);
        std::thread::sleep(delay);
    }

    fn enforce_admission_controls(
        self,
        memory_budget: WritePrepareMemoryBudgetContext<'a>,
        wal: WritePrepareWalContext<'a>,
        estimated_memory_growth_bytes: usize,
        estimated_wal_growth_bytes: u64,
    ) -> Result<()> {
        let deadline = Instant::now() + self.write_timeout;
        let mut relief_requested = false;

        loop {
            if let Some((budget, _required)) =
                memory_budget.shortfall(estimated_memory_growth_bytes)
            {
                self.budget.evict_persisted_sealed_chunks_to_budget(budget);

                if let Some((post_budget, post_required)) =
                    memory_budget.shortfall(estimated_memory_growth_bytes)
                {
                    if Instant::now() >= deadline {
                        return Err(TsinkError::MemoryBudgetExceeded {
                            budget: post_budget,
                            required: post_required,
                        });
                    }
                    if !relief_requested {
                        relief_requested = self.request_admission_pressure_relief();
                    }
                    self.delay_for_admission_backpressure(deadline);
                    continue;
                }
            }

            if let Some((_limit, _required)) = wal.size_shortfall(estimated_wal_growth_bytes)? {
                if let Some((post_limit, post_required)) =
                    wal.size_shortfall(estimated_wal_growth_bytes)?
                {
                    if Instant::now() >= deadline {
                        return Err(TsinkError::WalSizeLimitExceeded {
                            limit: post_limit,
                            required: post_required,
                        });
                    }
                    if !relief_requested {
                        relief_requested = self.request_admission_pressure_relief();
                    }
                    self.delay_for_admission_backpressure(deadline);
                    continue;
                }
            }

            if relief_requested {
                self.observability
                    .record_admission_pressure_relief_observed();
            }
            return Ok(());
        }
    }
}

impl<'a> WritePrepareContext<'a> {
    fn planner_for_series(self, series_id: SeriesId) -> PendingPartitionHeadState {
        let active = self.series_validation.chunks.active_shard(series_id).read();
        active
            .get(&series_id)
            .map(PendingPartitionHeadState::from_active_state)
            .unwrap_or_else(|| PendingPartitionHeadState::new(self.config.chunk_point_cap))
    }

    fn planned_additional_partition_heads(
        self,
        series_id: SeriesId,
        pending_partitions: &BTreeSet<i64>,
    ) -> (usize, usize) {
        let active = self.series_validation.chunks.active_shard(series_id).read();
        if let Some(state) = active.get(&series_id) {
            let additional_partitions = pending_partitions
                .iter()
                .filter(|partition_id| !state.contains_partition_head(**partition_id))
                .count();
            let bounded_partition_heads = state
                .partition_head_count()
                .saturating_add(additional_partitions)
                .min(self.config.max_active_partition_heads_per_series);
            (
                0,
                bounded_partition_heads.saturating_sub(state.partition_head_count()),
            )
        } else {
            (
                1,
                pending_partitions
                    .len()
                    .min(self.config.max_active_partition_heads_per_series),
            )
        }
    }
}

pub(super) struct WritePreparer<'a> {
    engine: WritePrepareContext<'a>,
}

impl<'a> WritePreparer<'a> {
    pub(super) fn new(engine: WritePrepareContext<'a>) -> Self {
        Self { engine }
    }

    pub(super) fn prepare_resolved_write_or_rollback(
        &self,
        resolver: &WriteResolver<'a>,
        resolved: ResolvedWrite,
    ) -> Result<PreparedWrite> {
        match self.prepare_resolved_write(resolved) {
            Ok(prepared) => Ok(prepared),
            Err(err) => {
                let (resolved, err) = *err;
                resolver.rollback_resolved_write(resolved);
                Err(err)
            }
        }
    }

    pub(super) fn prepare_resolved_write(
        &self,
        resolved: ResolvedWrite,
    ) -> std::result::Result<PreparedWrite, PrepareResolvedWriteError> {
        for point in &resolved.pending_points {
            if let Err(err) = WriteApplier::validate_series_lane_compatible(
                self.engine.series_validation,
                point.series_id,
                point.lane,
            ) {
                return Err(Box::new((resolved, err)));
            }
        }

        let grouped_points =
            match WriteApplier::group_pending_point_indexes_by_series(&resolved.pending_points) {
                Ok(grouped_points) => grouped_points,
                Err(err) => return Err(Box::new((resolved, err))),
            };
        let pending_series_families = match WriteApplier::validate_pending_point_families(
            self.engine.series_validation,
            &resolved.pending_points,
            &grouped_points,
        ) {
            Ok(pending_series_families) => pending_series_families,
            Err(err) => return Err(Box::new((resolved, err))),
        };
        if let Err(err) = self
            .engine
            .visibility
            .validate_points_against_retention(&resolved.pending_points)
        {
            return Err(Box::new((resolved, err)));
        }
        if let Err(err) =
            self.validate_pending_partition_heads(&resolved.pending_points, &grouped_points)
        {
            return Err(Box::new((resolved, err)));
        }
        let prepared_wal = match self.engine.wal.prepare_wal_write(
            &resolved.new_series_defs,
            &resolved.pending_points,
            &grouped_points,
        ) {
            Ok(prepared_wal) => prepared_wal,
            Err(err) => return Err(Box::new((resolved, err))),
        };
        let estimated_memory_growth = self.estimate_write_memory_growth_bytes(
            &resolved.pending_points,
            &grouped_points,
            &resolved.created_series,
            &pending_series_families,
        );
        let estimated_wal_growth = prepared_wal
            .as_ref()
            .map(|prepared| prepared.encoded_bytes)
            .unwrap_or(0);
        if let Err(err) = self.engine.admission.enforce_admission_controls(
            self.engine.memory_budget,
            self.engine.wal,
            estimated_memory_growth,
            estimated_wal_growth,
        ) {
            return Err(Box::new((resolved, err)));
        }

        Ok(PreparedWrite {
            resolved,
            prepared_wal,
            pending_series_families,
        })
    }

    fn validate_pending_partition_heads(
        &self,
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
    ) -> Result<()> {
        for (series_id, (_, indexes)) in grouped {
            let mut planner = self.engine.planner_for_series(*series_id);

            for idx in indexes {
                planner.rotate_partition_if_needed(
                    points[*idx].ts,
                    self.engine.config.partition_window,
                    self.engine.config.max_active_partition_heads_per_series,
                )?;
                planner.append_point();
                planner.rotate_full_if_needed();
            }
        }

        Ok(())
    }

    fn estimate_write_memory_growth_bytes(
        &self,
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
        created_series: &[SeriesResolution],
        pending_series_families: &BTreeMap<SeriesId, SeriesValueFamily>,
    ) -> usize {
        let per_point_bytes = std::mem::size_of::<ChunkPoint>();
        let point_storage_bytes = points.len().saturating_mul(per_point_bytes);
        let heap_bytes = points.iter().fold(0usize, |acc, point| {
            acc.saturating_add(value_heap_bytes(&point.value))
        });

        let per_new_partition_head = std::mem::size_of::<state::ActivePartitionHead>()
            .saturating_add(
                self.engine
                    .config
                    .chunk_point_cap
                    .saturating_mul(std::mem::size_of::<ChunkPoint>()),
            );
        let mut new_active_series = 0usize;
        let mut new_partition_heads = 0usize;
        for (series_id, (_, indexes)) in grouped {
            let mut pending_partitions = BTreeSet::new();
            for idx in indexes {
                pending_partitions.insert(partition_id_for_timestamp(
                    points[*idx].ts,
                    self.engine.config.partition_window,
                ));
            }

            let (additional_active_series, additional_partition_heads) = self
                .engine
                .planned_additional_partition_heads(*series_id, &pending_partitions);
            new_active_series = new_active_series.saturating_add(additional_active_series);
            new_partition_heads = new_partition_heads.saturating_add(additional_partition_heads);
        }

        let active_state_bytes =
            new_active_series.saturating_mul(std::mem::size_of::<ActiveSeriesState>());
        let partition_head_bytes = new_partition_heads.saturating_mul(per_new_partition_head);
        let value_family_bytes = pending_series_families
            .len()
            .saturating_mul(SeriesRegistry::value_family_entry_bytes());

        let metadata_bytes =
            self.engine
                .visibility
                .estimate_metadata_growth_bytes(points, grouped, created_series);

        point_storage_bytes
            .saturating_add(heap_bytes)
            .saturating_add(active_state_bytes)
            .saturating_add(partition_head_bytes)
            .saturating_add(value_family_bytes)
            .saturating_add(metadata_bytes)
    }
}
