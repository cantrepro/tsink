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

    pub(super) fn replay_wal_sample_batches(
        &self,
        sample_batches: Vec<SamplesBatchFrame>,
        wal_highwater: WalHighWatermark,
    ) -> Result<u64> {
        self.ingest_pipeline()
            .replay_wal_sample_batches(sample_batches, wal_highwater)
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
