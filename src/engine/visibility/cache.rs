use super::*;

#[path = "cache/materialized_series.rs"]
mod materialized_series;
#[path = "cache/summaries.rs"]
mod summaries;

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn with_series_visibility_summaries<R>(
        &self,
        f: impl FnOnce(&HashMap<SeriesId, SeriesVisibilitySummary>) -> R,
    ) -> R {
        let summaries = self.visibility.series_visibility_summaries.read();
        f(&summaries)
    }
}
