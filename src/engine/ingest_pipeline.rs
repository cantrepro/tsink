use super::super::{
    rollups::is_internal_rollup_metric, saturating_u64_from_usize, ChunkStorage, Result, Row,
    SamplesBatchFrame, SeriesId, StorageRuntimeMode, TsinkError, Value, ValueLane,
    WalHighWatermark, WriteTransientMemoryReservation,
};
use crate::engine::wal::MAX_WAL_REPLAY_DECODED_BATCH_BYTES;
use crate::WriteResult;

#[path = "ingest_pipeline/apply.rs"]
mod apply;
#[path = "ingest_pipeline/capabilities.rs"]
mod capabilities;
#[path = "ingest_pipeline/commit.rs"]
mod commit;
#[path = "ingest_pipeline/phases.rs"]
mod phases;
#[path = "ingest_pipeline/prepare.rs"]
mod prepare;
#[path = "ingest_pipeline/resolve.rs"]
mod resolve;

use apply::WriteApplier;
use commit::WriteCommitter;
use phases::{CommittedWrite, PendingPoint};
use prepare::WritePreparer;
use resolve::WriteResolver;

// Internal coordinator for live writes and WAL replay.
pub(super) struct IngestPipeline<'a> {
    storage: &'a ChunkStorage,
}

struct CommitRowsAttemptError {
    error: TsinkError,
    wal_stage: bool,
}

impl CommitRowsAttemptError {
    fn before_or_after_wal_stage(error: TsinkError) -> Self {
        Self {
            error,
            wal_stage: false,
        }
    }

    fn during_wal_stage(error: TsinkError) -> Self {
        Self {
            error,
            wal_stage: true,
        }
    }
}

impl<'a> IngestPipeline<'a> {
    pub(super) fn new(storage: &'a ChunkStorage) -> Self {
        Self { storage }
    }

    pub(super) fn insert_rows(&self, rows: &[Row]) -> Result<WriteResult> {
        self.storage.ensure_open()?;
        if self.storage.runtime.runtime_mode == StorageRuntimeMode::ComputeOnly {
            return Err(TsinkError::InvalidConfiguration(
                "compute-only storage mode cannot accept writes".to_string(),
            ));
        }
        if rows.is_empty() {
            return Ok(WriteResult::durable());
        }
        let transient_memory = self.admit_write_rows(rows)?;
        self.insert_rows_with_admission(rows, transient_memory)
    }

    pub(super) fn admit_write_rows(&self, rows: &[Row]) -> Result<WriteTransientMemoryReservation> {
        let resolver = self.resolver();
        let scratch = resolver.preflight_write_rows_scratch_bytes(rows)?;
        let prepare = self.storage.write_prepare_context();
        prepare.admission.enforce_admission_controls(
            prepare.memory_budget,
            prepare.wal,
            scratch,
            0,
        )?;
        resolver.reserve_write_scratch(scratch)
    }

    pub(super) fn insert_rows_with_admission(
        &self,
        rows: &[Row],
        transient_memory: WriteTransientMemoryReservation,
    ) -> Result<WriteResult> {
        self.storage.ensure_open()?;
        if self.storage.runtime.runtime_mode == StorageRuntimeMode::ComputeOnly {
            return Err(TsinkError::InvalidConfiguration(
                "compute-only storage mode cannot accept writes".to_string(),
            ));
        }
        if rows.is_empty() {
            return Ok(WriteResult::durable());
        }
        let write_permit = self
            .storage
            .runtime
            .write_limiter
            .try_acquire_for(self.storage.runtime.write_timeout)?;
        // A write may pass the first lifecycle check and then block on permits while close starts.
        // Re-check after acquiring a permit so shutdown cannot race new writes through.
        self.storage.ensure_open()?;
        let result = self.commit_rows_with_held_permit(rows, transient_memory)?;

        drop(write_permit);
        self.enforce_post_commit_memory_budget_best_effort();
        Ok(result)
    }

    /// Commits rollup materialization rows while the caller retains a writer permit across the
    /// whole run. Internal rollup metrics cannot trigger historical-source invalidation, so this
    /// path also cannot re-enter `rollups.run_lock`.
    pub(super) fn insert_rollup_rows_with_held_permit(
        &self,
        rows: &[Row],
        _write_permit: &crate::concurrency::SemaphoreGuard<'_>,
    ) -> Result<WriteResult> {
        if rows
            .iter()
            .any(|row| !is_internal_rollup_metric(row.metric()))
        {
            return Err(TsinkError::InvalidConfiguration(
                "permit-held rollup writes require internal rollup metrics".to_string(),
            ));
        }
        if rows.is_empty() {
            return Ok(WriteResult::durable());
        }
        let transient_memory = self.admit_write_rows(rows)?;
        debug_assert!(
            self.storage
                .rollup_policy_ids_needing_rebuild_for_rows(rows)
                .is_empty(),
            "internal rollup writes must not acquire the rollup invalidation lock"
        );
        self.commit_rows_with_held_permit(rows, transient_memory)
    }

    fn commit_rows_with_held_permit(
        &self,
        rows: &[Row],
        transient_memory: WriteTransientMemoryReservation,
    ) -> Result<WriteResult> {
        let committed = match self.commit_rows_once(rows, transient_memory.clone()) {
            Err(initial_error)
                if initial_error.wal_stage
                    && Self::is_disk_capacity_rejection(&initial_error.error) =>
            {
                match self
                    .storage
                    .reclaim_fully_expired_segments_after_capacity_rejection()
                {
                    Ok(true) => self
                        .commit_rows_once(rows, transient_memory.clone())
                        .map_err(|attempt| attempt.error)?,
                    Ok(false) => return Err(initial_error.error),
                    Err(cleanup_error) if Self::is_disk_capacity_rejection(&cleanup_error) => {
                        tracing::warn!(
                            initial_error = %initial_error.error,
                            cleanup_error = %cleanup_error,
                            "Retention cleanup could not reclaim capacity before WAL rejection"
                        );
                        return Err(initial_error.error);
                    }
                    Err(cleanup_error) => return Err(cleanup_error),
                }
            }
            Ok(committed) => committed,
            Err(attempt) => return Err(attempt.error),
        };

        self.storage.notify_flush_thread();
        self.storage.notify_rollup_thread();
        Ok(WriteResult::new(committed.acknowledgement))
    }

    pub(super) fn enforce_post_commit_memory_budget_best_effort(&self) {
        if self.storage.memory_budget_value() == usize::MAX {
            return;
        }
        if let Err(err) = self.storage.enforce_memory_budget_if_needed() {
            self.storage
                .observability
                .record_maintenance_error("post-commit memory budget enforcement", &err);
            tracing::warn!(
                error = %err,
                "Committed write left post-commit memory maintenance degraded"
            );
        }
    }

    fn is_disk_capacity_rejection(error: &TsinkError) -> bool {
        matches!(
            error,
            TsinkError::DiskQuotaExceeded { .. }
                | TsinkError::InsufficientCompactionHeadroom { .. }
                | TsinkError::InsufficientDiskSpace { .. }
        )
    }

    fn commit_rows_once(
        &self,
        rows: &[Row],
        transient_memory: WriteTransientMemoryReservation,
    ) -> std::result::Result<CommittedWrite, CommitRowsAttemptError> {
        let pending_rollup_rebuilds = self
            .storage
            .rollup_policy_ids_needing_rebuild_for_rows(rows);

        Ok({
            // Historical raw writes must serialize with the rollup worker so an invalidation
            // cannot race a checkpoint advance or a generation switch.
            let _rollup_guard =
                (!pending_rollup_rebuilds.is_empty()).then(|| self.storage.rollups.run_lock.lock());
            let pending_rollup_rebuilds = if _rollup_guard.is_some() {
                self.storage
                    .rollup_policy_ids_needing_rebuild_for_rows(rows)
            } else {
                pending_rollup_rebuilds
            };

            // This outer transaction lock is only about series-definition visibility. Once a
            // writer creates a series id, same-shard followers must stay behind it until the
            // defining WAL entry is either published or rolled back.
            let _registry_write_txn = self.storage.lock_registry_write_shards_for_rows(rows);

            let resolver = self.resolver();
            let preparer = self.preparer();
            let applier = self.applier();
            let committer = self.committer();

            let resolved = resolver
                .resolve_write_rows_with_reservation(rows, transient_memory)
                .map_err(CommitRowsAttemptError::before_or_after_wal_stage)?;
            let prepared = preparer
                .prepare_resolved_write_or_rollback(&resolver, resolved)
                .map_err(CommitRowsAttemptError::before_or_after_wal_stage)?;
            if !pending_rollup_rebuilds.is_empty() {
                if let Err(err) = self
                    .storage
                    .invalidate_rollup_policy_ids(&pending_rollup_rebuilds)
                {
                    return Err(CommitRowsAttemptError::before_or_after_wal_stage(
                        applier.rollback_prepared_write_error(prepared, err),
                    ));
                }
            }
            let staged = committer
                .stage_prepared_write_or_rollback(&applier, prepared)
                .map_err(CommitRowsAttemptError::during_wal_stage)?;
            let applied = applier
                .apply_staged_write_or_rollback(staged)
                .map_err(CommitRowsAttemptError::before_or_after_wal_stage)?;
            committer.publish_applied_write(applied)
        })
    }

    pub(super) fn replay_wal_sample_batches(
        &self,
        sample_batches: Vec<SamplesBatchFrame>,
        wal_highwater: WalHighWatermark,
        transient_memory: &WriteTransientMemoryReservation,
    ) -> Result<u64> {
        if sample_batches.is_empty() {
            return Ok(0);
        }

        let mut total_points = 0usize;
        let mut total_modeled_input = 0usize;
        for batch in &sample_batches {
            total_points = total_points
                .checked_add(usize::from(batch.point_count))
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            total_modeled_input = total_modeled_input
                .checked_add(batch.modeled_decoded_points_peak_bytes()?)
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
        }
        if let Some(limit) = self.storage.runtime.write_batch_limits.max_rows {
            if total_points > limit {
                return Err(TsinkError::WriteBatchRowLimitExceeded {
                    limit,
                    submitted: total_points,
                });
            }
        }
        if let Some(limit) = self
            .storage
            .runtime
            .write_batch_limits
            .max_modeled_input_bytes
        {
            if total_modeled_input > limit {
                return Err(TsinkError::WriteBatchInputLimitExceeded {
                    limit,
                    submitted: total_modeled_input,
                });
            }
        }

        let frame_scratch = transient_memory.reserved_bytes();
        let mut replayed_points = 0usize;
        for batch in sample_batches {
            let decoded_scratch = batch.modeled_decoded_points_peak_bytes()?;
            if decoded_scratch > MAX_WAL_REPLAY_DECODED_BATCH_BYTES {
                return Err(TsinkError::DataCorruption(format!(
                    "decoded WAL batch scratch {decoded_scratch} exceeds the format safety limit {MAX_WAL_REPLAY_DECODED_BATCH_BYTES}"
                )));
            }
            let required = frame_scratch
                .checked_add(decoded_scratch)
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            self.storage
                .ensure_write_transient_memory(transient_memory, required)?;
            let series_id = batch.series_id;
            let lane = batch.lane;
            let points = batch.decode_points()?;
            let pending_points = points
                .into_iter()
                .map(|point| PendingPoint {
                    series_id,
                    lane,
                    ts: point.ts,
                    value: point.value,
                    wal_highwater,
                })
                .collect::<Vec<_>>();
            replayed_points = replayed_points
                .checked_add(pending_points.len())
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            self.applier()
                .ingest_replayed_pending_points(pending_points)?;
        }

        Ok(saturating_u64_from_usize(replayed_points))
    }

    pub(super) fn append_point_to_series(
        &self,
        series_id: SeriesId,
        lane: ValueLane,
        ts: i64,
        value: Value,
    ) -> Result<()> {
        self.applier()
            .append_point_to_series(series_id, lane, ts, value)
    }

    fn resolver(&self) -> WriteResolver<'a> {
        WriteResolver::new(self.storage.write_resolve_context())
    }

    fn preparer(&self) -> WritePreparer<'a> {
        WritePreparer::new(self.storage.write_prepare_context())
    }

    fn applier(&self) -> WriteApplier<'a> {
        WriteApplier::new(self.storage.write_apply_context())
    }

    fn committer(&self) -> WriteCommitter<'a> {
        WriteCommitter::new(self.storage.write_commit_context())
    }
}
