use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::super::super::{
    partition_id_for_timestamp, state, value_heap_bytes, ActiveSeriesState, ChunkBuilder,
    ChunkPoint, FramedWal, Result, SeriesDefinitionFrame, SeriesId, SeriesRegistry,
    SeriesResolution, SeriesValueFamily, SeriesVisibilitySummary, TsinkError, ValueLane,
    WalHighWatermark, WriteAdmissionControlContext, WritePrepareContext,
    WritePrepareMemoryBudgetContext, WritePrepareVisibilityContext, WritePrepareWalContext,
    WriteTransientMemoryReservation, STORAGE_OPEN,
};
use super::apply::WriteApplier;
use super::phases::{
    PendingPoint, PrepareResolvedWriteError, PreparedWalWrite, PreparedWrite, ResolvedWrite,
};
use super::resolve::WriteResolver;

struct PendingPartitionHeadState {
    point_cap: usize,
    partition_heads: BTreeMap<i64, PendingChunkBuilderAllocation>,
    current_partition_id: Option<i64>,
    active_series_was_present: bool,
    allocation_growth_bytes: usize,
}

#[derive(Clone, Copy)]
struct PendingChunkBuilderAllocation {
    point_count: usize,
    point_block_max_points: usize,
    tail_point_count: usize,
    frozen_point_block_count: usize,
    frozen_point_block_capacity: usize,
}

impl PendingChunkBuilderAllocation {
    fn new(point_cap: usize) -> Self {
        let initial_point_capacity = ChunkBuilder::initial_point_capacity(point_cap);
        Self {
            point_count: 0,
            point_block_max_points: initial_point_capacity,
            tail_point_count: 0,
            frozen_point_block_count: 0,
            frozen_point_block_capacity: 0,
        }
    }

    fn from_builder(builder: &ChunkBuilder) -> Self {
        Self {
            point_count: builder.len(),
            point_block_max_points: builder.point_block_max_points(),
            tail_point_count: builder.tail_point_count(),
            frozen_point_block_count: builder.frozen_point_block_count(),
            frozen_point_block_capacity: builder.point_block_capacity(),
        }
    }

    fn initial_allocation_bytes(point_cap: usize) -> usize {
        ChunkBuilder::initial_point_capacity(point_cap)
            .saturating_mul(std::mem::size_of::<ChunkPoint>())
    }

    fn append_point(&mut self) -> usize {
        self.point_count = self.point_count.saturating_add(1);
        self.tail_point_count = self.tail_point_count.saturating_add(1);
        if self.tail_point_count < self.point_block_max_points {
            return 0;
        }

        self.tail_point_count = 0;
        self.frozen_point_block_count = self.frozen_point_block_count.saturating_add(1);
        let next_block_capacity = ChunkBuilder::projected_point_block_capacity(
            self.frozen_point_block_capacity,
            self.frozen_point_block_count,
        );
        let block_capacity_growth = next_block_capacity
            .saturating_sub(self.frozen_point_block_capacity)
            .saturating_mul(std::mem::size_of::<Arc<Vec<ChunkPoint>>>());
        self.frozen_point_block_capacity = next_block_capacity;

        self.point_block_max_points
            .saturating_mul(std::mem::size_of::<ChunkPoint>())
            .saturating_add(block_capacity_growth)
            .saturating_add(std::mem::size_of::<Vec<ChunkPoint>>())
    }
}

impl PendingPartitionHeadState {
    fn new(point_cap: usize) -> Self {
        Self {
            point_cap: point_cap.max(1),
            partition_heads: BTreeMap::new(),
            current_partition_id: None,
            active_series_was_present: false,
            allocation_growth_bytes: 0,
        }
    }

    fn from_active_state(state: &ActiveSeriesState) -> Self {
        Self {
            point_cap: state.point_cap,
            partition_heads: state
                .partition_heads
                .iter()
                .map(|(partition_id, head)| {
                    (
                        *partition_id,
                        PendingChunkBuilderAllocation::from_builder(&head.builder),
                    )
                })
                .collect(),
            current_partition_id: state.current_partition_id,
            active_series_was_present: true,
            allocation_growth_bytes: 0,
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
                let head_count_grows = evict_partition_id.is_none();
                self.partition_heads
                    .entry(next_partition)
                    .or_insert_with(|| PendingChunkBuilderAllocation::new(self.point_cap));
                self.allocation_growth_bytes = self.allocation_growth_bytes.saturating_add(
                    PendingChunkBuilderAllocation::initial_allocation_bytes(self.point_cap),
                );
                if head_count_grows {
                    self.allocation_growth_bytes = self
                        .allocation_growth_bytes
                        .saturating_add(std::mem::size_of::<state::ActivePartitionHead>())
                        .saturating_add(std::mem::size_of::<(WalHighWatermark, usize)>());
                }
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
        self.allocation_growth_bytes = self
            .allocation_growth_bytes
            .saturating_add(head.append_point());
    }

    fn rotate_full_if_needed(&mut self) {
        let Some(partition_id) = self.current_partition_id else {
            return;
        };
        if self
            .partition_heads
            .get(&partition_id)
            .is_some_and(|head| head.point_count >= self.point_cap)
        {
            self.partition_heads.insert(
                partition_id,
                PendingChunkBuilderAllocation::new(self.point_cap),
            );
            self.allocation_growth_bytes = self.allocation_growth_bytes.saturating_add(
                PendingChunkBuilderAllocation::initial_allocation_bytes(self.point_cap),
            );
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

    fn active_series_growth_bytes(&self) -> usize {
        if self.active_series_was_present {
            0
        } else {
            std::mem::size_of::<ActiveSeriesState>()
        }
    }

    fn allocation_growth_bytes(&self) -> usize {
        self.allocation_growth_bytes
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
                                    metadata_bytes = metadata_bytes.saturating_add(
                                        std::mem::size_of::<(SeriesId, u64)>(),
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
    fn retained_budget_after_reservations(self, estimated_growth_bytes: usize) -> usize {
        let budget = self
            .memory_reservation_admission
            .budget_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize;
        let staged = self
            .memory_reservation_admission
            .tombstone_staged_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize;
        let remote_catalog_staged = self
            .memory_reservation_admission
            .remote_catalog_staging
            .current_bytes();
        let transient = self
            .memory_reservation_admission
            .write_transient
            .current_bytes();

        budget
            .saturating_sub(staged)
            .saturating_sub(remote_catalog_staged)
            .saturating_sub(transient)
            .saturating_sub(estimated_growth_bytes)
    }

    fn shortfall(self, estimated_growth_bytes: usize) -> Option<(usize, usize)> {
        let budget = self
            .memory_reservation_admission
            .budget_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize;
        if budget == usize::MAX {
            return None;
        }

        let used = self
            .memory_reservation_admission
            .used_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize;
        let staged = self
            .memory_reservation_admission
            .tombstone_staged_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize;
        let remote_catalog_staged = self
            .memory_reservation_admission
            .remote_catalog_staging
            .current_bytes();
        let transient = self
            .memory_reservation_admission
            .write_transient
            .current_bytes();
        let required = used
            .saturating_add(staged)
            .saturating_add(remote_catalog_staged)
            .saturating_add(transient)
            .saturating_add(estimated_growth_bytes);
        (required > budget).then_some((budget, required))
    }

    fn reserve_retained_growth(
        self,
        reservation: &WriteTransientMemoryReservation,
        retained_growth_bytes: usize,
    ) -> Result<()> {
        let required_reservation = reservation
            .base_reserved_bytes()
            .checked_add(retained_growth_bytes)
            .ok_or(TsinkError::WriteBatchSizeOverflow)?;
        reservation.ensure(required_reservation, self.memory_reservation_admission)
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

fn increment_atomic_saturating(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        Some(value.saturating_add(1))
    });
}

impl<'a> WriteAdmissionControlContext<'a> {
    fn ensure_accepting_writes(self) -> Result<()> {
        if self
            .observability
            .health
            .fail_fast_triggered
            .load(Ordering::SeqCst)
        {
            return Err(TsinkError::StorageShuttingDown);
        }
        if self.lifecycle.load(Ordering::SeqCst) != STORAGE_OPEN {
            return Err(TsinkError::StorageClosed);
        }
        Ok(())
    }

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

    pub(in crate::engine::storage_engine) fn enforce_admission_controls(
        self,
        memory_budget: WritePrepareMemoryBudgetContext<'a>,
        wal: WritePrepareWalContext<'a>,
        estimated_memory_growth_bytes: usize,
        estimated_wal_growth_bytes: u64,
    ) -> Result<()> {
        let deadline = Instant::now() + self.write_timeout;
        let mut relief_requested = false;
        let mut memory_backpressure_recorded = false;
        let mut active_memory_backpressure = None;

        loop {
            // A writer owns a limiter permit while it waits here. Shutdown pauses the workers
            // that could relieve pressure before draining those permits, so continuing to poll
            // after the lifecycle transition would hold close() hostage until the write timeout.
            self.ensure_accepting_writes()?;

            let memory_shortfall = if let Some((_budget, _required)) =
                memory_budget.shortfall(estimated_memory_growth_bytes)
            {
                let retained_budget =
                    memory_budget.retained_budget_after_reservations(estimated_memory_growth_bytes);
                self.budget
                    .evict_persisted_sealed_chunks_to_budget(retained_budget);
                memory_budget.shortfall(estimated_memory_growth_bytes)
            } else {
                None
            };

            if let Some((post_budget, post_required)) = memory_shortfall {
                if Instant::now() >= deadline {
                    increment_atomic_saturating(self.memory_rejections_total);
                    return Err(TsinkError::MemoryBudgetExceeded {
                        budget: post_budget,
                        required: post_required,
                    });
                }
                if active_memory_backpressure.is_none() {
                    active_memory_backpressure = Some(ActiveMemoryBackpressureGuard::new(
                        self.active_memory_backpressured_writers,
                    ));
                }
                if !memory_backpressure_recorded {
                    increment_atomic_saturating(self.memory_backpressure_events_total);
                    memory_backpressure_recorded = true;
                }
                if !relief_requested {
                    relief_requested = self.request_admission_pressure_relief();
                }
                self.delay_for_admission_backpressure(deadline);
                continue;
            }
            active_memory_backpressure = None;

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

struct ActiveMemoryBackpressureGuard<'a> {
    counter: &'a AtomicU64,
}

impl<'a> ActiveMemoryBackpressureGuard<'a> {
    fn new(counter: &'a AtomicU64) -> Self {
        increment_atomic_saturating(counter);
        Self { counter }
    }
}

impl Drop for ActiveMemoryBackpressureGuard<'_> {
    fn drop(&mut self) {
        let _ = self
            .counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                Some(value.saturating_sub(1))
            });
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
        if let Err(err) = self
            .engine
            .memory_budget
            .reserve_retained_growth(&resolved.transient_memory, estimated_memory_growth)
        {
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
        let heap_bytes = points.iter().fold(0usize, |acc, point| {
            acc.saturating_add(value_heap_bytes(&point.value))
        });

        let mut active_state_bytes = 0usize;
        let mut partition_head_growth_bytes = 0usize;
        for (series_id, (_, indexes)) in grouped {
            let mut planner = self.engine.planner_for_series(*series_id);
            for idx in indexes {
                if planner
                    .rotate_partition_if_needed(
                        points[*idx].ts,
                        self.engine.config.partition_window,
                        self.engine.config.max_active_partition_heads_per_series,
                    )
                    .is_err()
                {
                    // Validation just replayed the same plan. If concurrent maintenance changed
                    // the active-head topology between the two reads, reject conservatively
                    // instead of admitting an unmodeled allocation.
                    return usize::MAX;
                }
                planner.append_point();
                planner.rotate_full_if_needed();
            }

            active_state_bytes =
                active_state_bytes.saturating_add(planner.active_series_growth_bytes());
            partition_head_growth_bytes =
                partition_head_growth_bytes.saturating_add(planner.allocation_growth_bytes());
        }

        let value_family_bytes = pending_series_families
            .len()
            .saturating_mul(SeriesRegistry::value_family_entry_bytes());

        let metadata_bytes =
            self.engine
                .visibility
                .estimate_metadata_growth_bytes(points, grouped, created_series);

        heap_bytes
            .saturating_add(active_state_bytes)
            .saturating_add(partition_head_growth_bytes)
            .saturating_add(value_family_bytes)
            .saturating_add(metadata_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    fn modeled_builder_allocation_bytes(builder: &ChunkBuilder) -> usize {
        builder
            .capacity()
            .saturating_mul(std::mem::size_of::<ChunkPoint>())
            .saturating_add(
                builder
                    .point_block_capacity()
                    .saturating_mul(std::mem::size_of::<Arc<Vec<ChunkPoint>>>()),
            )
            .saturating_add(
                builder
                    .frozen_point_block_count()
                    .saturating_mul(std::mem::size_of::<Vec<ChunkPoint>>()),
            )
    }

    fn one_point_head_growth(planner: &mut PendingPartitionHeadState) -> usize {
        planner.rotate_partition_if_needed(1, 1_000, 8).unwrap();
        planner.append_point();
        planner.rotate_full_if_needed();
        planner.allocation_growth_bytes()
    }

    #[test]
    fn one_point_new_and_reopened_heads_charge_one_initial_block_exactly() {
        const SERIES_COUNT: usize = 4_096;
        const POINT_CAP: usize = 2_048;

        let expected_head_growth = std::mem::size_of::<state::ActivePartitionHead>()
            + std::mem::size_of::<(WalHighWatermark, usize)>()
            + ChunkBuilder::initial_point_capacity(POINT_CAP) * std::mem::size_of::<ChunkPoint>();

        let mut new_total = 0usize;
        let mut reopened_total = 0usize;
        for series_id in 0..SERIES_COUNT {
            let mut new = PendingPartitionHeadState::new(POINT_CAP);
            assert_eq!(
                new.active_series_growth_bytes(),
                std::mem::size_of::<ActiveSeriesState>()
            );
            new_total = new_total.saturating_add(one_point_head_growth(&mut new));

            // A bounded partial flush can leave an active-series object with no open head until
            // the empty state is pruned. Reopening it must allocate the same one-block builder,
            // but must not charge another ActiveSeriesState.
            let empty_state = ActiveSeriesState::new(
                u64::try_from(series_id).unwrap(),
                ValueLane::Numeric,
                POINT_CAP,
            );
            let mut reopened = PendingPartitionHeadState::from_active_state(&empty_state);
            assert_eq!(reopened.active_series_growth_bytes(), 0);
            reopened_total = reopened_total.saturating_add(one_point_head_growth(&mut reopened));
        }

        assert_eq!(new_total, SERIES_COUNT * expected_head_growth);
        assert_eq!(reopened_total, SERIES_COUNT * expected_head_growth);
        assert_eq!(ChunkBuilder::initial_point_capacity(POINT_CAP), 64);
    }

    #[test]
    fn builder_growth_model_matches_exact_tail_and_block_capacity_boundaries() {
        const POINT_CAP: usize = 2_048;
        let mut builder = ChunkBuilder::new(1, ValueLane::Numeric, POINT_CAP);
        let mut model = PendingChunkBuilderAllocation::new(POINT_CAP);

        assert_eq!(
            PendingChunkBuilderAllocation::initial_allocation_bytes(POINT_CAP),
            modeled_builder_allocation_bytes(&builder)
        );

        // Cross four frozen-block boundaries and the outer block vector's 4 -> 8 boundary.
        for timestamp in 0..321 {
            let before = modeled_builder_allocation_bytes(&builder);
            let predicted_growth = model.append_point();
            builder.append(timestamp, Value::I64(timestamp));
            let actual_growth = modeled_builder_allocation_bytes(&builder).saturating_sub(before);
            assert_eq!(
                predicted_growth,
                actual_growth,
                "allocation mismatch while appending point {}",
                timestamp + 1
            );
        }
    }

    #[test]
    fn full_head_reopen_charges_boundary_growth_and_fresh_initial_block() {
        const POINT_CAP: usize = 64;
        let initial = PendingChunkBuilderAllocation::initial_allocation_bytes(POINT_CAP);
        let freeze_growth = POINT_CAP * std::mem::size_of::<ChunkPoint>()
            + 4 * std::mem::size_of::<Arc<Vec<ChunkPoint>>>()
            + std::mem::size_of::<Vec<ChunkPoint>>();
        let head_entry = std::mem::size_of::<state::ActivePartitionHead>()
            + std::mem::size_of::<(WalHighWatermark, usize)>();

        let mut planner = PendingPartitionHeadState::new(POINT_CAP);
        planner.rotate_partition_if_needed(1, 1_000, 8).unwrap();
        for _ in 0..POINT_CAP {
            planner.append_point();
            planner.rotate_full_if_needed();
        }

        assert_eq!(
            planner.allocation_growth_bytes(),
            head_entry + initial + freeze_growth + initial
        );
    }
}
