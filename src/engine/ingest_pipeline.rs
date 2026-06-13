use super::super::{
    saturating_u64_from_usize, ChunkStorage, Result, Row, SamplesBatchFrame, SeriesId,
    StorageRuntimeMode, TsinkError, Value, ValueLane, WalHighWatermark,
};
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
use phases::PendingPoint;
use prepare::WritePreparer;
use resolve::WriteResolver;

// Internal coordinator for live writes and WAL replay.
pub(super) struct IngestPipeline<'a> {
    storage: &'a ChunkStorage,
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
        let write_permit = self
            .storage
            .runtime
            .write_limiter
            .try_acquire_for(self.storage.runtime.write_timeout)?;
        // A write may pass the first lifecycle check and then block on permits while close starts.
        // Re-check after acquiring a permit so shutdown cannot race new writes through.
        self.storage.ensure_open()?;
        let pending_rollup_rebuilds = self
            .storage
            .rollup_policy_ids_needing_rebuild_for_rows(rows);

        let committed = {
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

            let resolved = resolver.resolve_write_rows(rows)?;
            let prepared = preparer.prepare_resolved_write_or_rollback(&resolver, resolved)?;
            if !pending_rollup_rebuilds.is_empty() {
                if let Err(err) = self
                    .storage
                    .invalidate_rollup_policy_ids(&pending_rollup_rebuilds)
                {
                    return Err(applier.rollback_prepared_write_error(prepared, err));
                }
            }
            let staged = committer.stage_prepared_write_or_rollback(&applier, prepared)?;
            let applied = applier.apply_staged_write_or_rollback(staged)?;
            committer.publish_applied_write(applied)
        };

        if self.storage.memory_budget_value() != usize::MAX {
            drop(write_permit);
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

        self.storage.notify_flush_thread();
        self.storage.notify_rollup_thread();
        Ok(WriteResult::new(committed.acknowledgement))
    }

    pub(super) fn replay_wal_sample_batches(
        &self,
        sample_batches: Vec<SamplesBatchFrame>,
        wal_highwater: WalHighWatermark,
    ) -> Result<u64> {
        if sample_batches.is_empty() {
            return Ok(0);
        }

        let mut pending_points =
            Vec::with_capacity(sample_batches.iter().fold(0usize, |acc, batch| {
                acc.saturating_add(batch.point_count as usize)
            }));
        for batch in sample_batches {
            let series_id = batch.series_id;
            let lane = batch.lane;
            let points = batch.decode_points()?;
            pending_points.extend(points.into_iter().map(|point| PendingPoint {
                series_id,
                lane,
                ts: point.ts,
                value: point.value,
                wal_highwater,
            }));
        }

        let replayed_points = pending_points.len();
        self.applier()
            .ingest_replayed_pending_points(pending_points)?;
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
