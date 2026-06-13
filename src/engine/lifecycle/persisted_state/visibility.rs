use super::*;

impl ChunkStorage {
    pub(super) fn install_visible_persisted_state(
        &self,
        install: PersistedVisibleStateInstall,
    ) -> Result<()> {
        let PersistedVisibleStateInstall {
            refresh_series_ids,
            mark_refreshed_series_materialized,
            loaded_max_timestamp,
            next_segment_id,
        } = install;
        let refresh_series_ids = Self::normalize_series_ids(refresh_series_ids);
        let publication = self.lifecycle_publication_context();

        // Keep cache refresh ahead of materialization and publication bumps.
        self.refresh_series_visible_timestamp_cache_locked(refresh_series_ids.iter().copied())?;
        if mark_refreshed_series_materialized {
            let inserted_series_ids = publication
                .materialized_series
                .insert_materialized_series_ids(refresh_series_ids.iter().copied());
            if !inserted_series_ids.is_empty() {
                publication
                    .runtime_metadata_delta
                    .reconcile_series_ids(inserted_series_ids.iter().copied());
                publication
                    .metadata_shards
                    .publish_materialized_series_ids(inserted_series_ids.iter().copied());
            }
        }
        if let Some(loaded_max_timestamp) = loaded_max_timestamp {
            self.update_max_observed_timestamp(loaded_max_timestamp);
        }
        if let Some(next_segment_id) = next_segment_id {
            self.persisted
                .next_segment_id
                .store(next_segment_id.max(1), Ordering::SeqCst);
        }
        self.bump_visibility_state_generation();
        Ok(())
    }
}
