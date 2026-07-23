use super::{
    ChunkStorage, Result, Row, SamplesBatchFrame, SeriesId, Value, ValueLane, WalHighWatermark,
};
use crate::WriteResult;

#[path = "ingest_pipeline.rs"]
mod pipeline;

impl ChunkStorage {
    fn ingest_pipeline(&self) -> pipeline::IngestPipeline<'_> {
        pipeline::IngestPipeline::new(self)
    }

    pub(super) fn insert_rows_impl(&self, rows: &[Row]) -> Result<WriteResult> {
        self.ingest_pipeline().insert_rows(rows)
    }

    pub(super) fn admit_write_rows_impl(
        &self,
        rows: &[Row],
    ) -> Result<super::WriteTransientMemoryReservation> {
        self.ingest_pipeline().admit_write_rows(rows)
    }

    pub(super) fn insert_rows_with_admission_impl(
        &self,
        rows: &[Row],
        transient_memory: super::WriteTransientMemoryReservation,
    ) -> Result<WriteResult> {
        self.ingest_pipeline()
            .insert_rows_with_admission(rows, transient_memory)
    }

    pub(in crate::engine) fn insert_rollup_rows_with_held_permit(
        &self,
        rows: &[Row],
        write_permit: &crate::concurrency::SemaphoreGuard<'_>,
    ) -> Result<WriteResult> {
        self.ingest_pipeline()
            .insert_rollup_rows_with_held_permit(rows, write_permit)
    }

    pub(in crate::engine) fn enforce_post_commit_memory_budget_best_effort(&self) {
        self.ingest_pipeline()
            .enforce_post_commit_memory_budget_best_effort();
    }

    pub(super) fn replay_wal_sample_batches(
        &self,
        sample_batches: Vec<SamplesBatchFrame>,
        wal_highwater: WalHighWatermark,
        transient_memory: &super::WriteTransientMemoryReservation,
    ) -> Result<u64> {
        self.ingest_pipeline().replay_wal_sample_batches(
            sample_batches,
            wal_highwater,
            transient_memory,
        )
    }

    #[allow(dead_code)]
    pub(super) fn append_point_to_series(
        &self,
        series_id: SeriesId,
        lane: ValueLane,
        ts: i64,
        value: Value,
    ) -> Result<()> {
        self.ingest_pipeline()
            .append_point_to_series(series_id, lane, ts, value)
    }
}

fn lane_name(lane: ValueLane) -> &'static str {
    match lane {
        ValueLane::Numeric => "numeric",
        ValueLane::Blob => "blob",
    }
}
