use super::*;
use super::super::core_impl::VisibilityCacheMapView;

#[path = "cache/materialized_series.rs"]
mod materialized_series;
#[path = "cache/summaries.rs"]
mod summaries;

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn with_series_visibility_summaries<R>(
        &self,
        f: impl FnOnce(VisibilityCacheMapView<'_, SeriesVisibilitySummary>) -> R,
    ) -> R {
        let summaries = self.visibility.series_visibility_summaries.read();
        let epochs = self.visibility.series_visibility_cache_epochs.read();
        f(VisibilityCacheMapView::new(
            &summaries,
            &epochs,
            self.remote_tombstone_epoch(),
        ))
    }
}
