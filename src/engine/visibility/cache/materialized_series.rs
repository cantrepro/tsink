use super::*;

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn mark_materialized_series_ids<I>(
        &self,
        series_ids: I,
    ) -> Vec<SeriesId>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        self.materialized_series_write_context()
            .insert_materialized_series_ids(series_ids)
    }

    pub(in crate::engine::storage_engine) fn materialized_series_page_after(
        &self,
        cursor: Option<SeriesId>,
        limit: usize,
    ) -> Vec<SeriesId> {
        if limit == 0 {
            return Vec::new();
        }

        let materialized_series = self.visibility.materialized_series.read();
        match cursor {
            Some(cursor) => materialized_series
                .range((
                    std::ops::Bound::Excluded(cursor),
                    std::ops::Bound::Unbounded,
                ))
                .take(limit)
                .copied()
                .collect(),
            None => materialized_series.iter().take(limit).copied().collect(),
        }
    }

    pub(in crate::engine::storage_engine) fn live_series_pruning_generation(&self) -> u64 {
        self.visibility
            .live_series_pruning_generation
            .load(Ordering::Acquire)
    }

    pub(in crate::engine::storage_engine) fn bump_live_series_pruning_generation(&self) {
        self.visibility
            .live_series_pruning_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::engine::storage_engine) fn prune_dead_materialized_series_ids_if_stable(
        &self,
        dead_series_ids: Vec<SeriesId>,
        generation_before: Option<u64>,
    ) {
        #[cfg(test)]
        if generation_before.is_some() && !dead_series_ids.is_empty() {
            self.invoke_metadata_live_series_pre_prune_hook();
        }

        if generation_before.is_some_and(|generation_before| {
            self.live_series_pruning_generation() == generation_before
        }) && !dead_series_ids.is_empty()
        {
            let removed_series_ids = self.remove_materialized_series_ids(dead_series_ids);
            if removed_series_ids.is_empty() {
                return;
            }
            self.runtime_metadata_delta_write_context()
                .reconcile_series_ids(removed_series_ids.iter().copied());
            self.metadata_shard_publication_context()
                .unpublish_materialized_series_ids(removed_series_ids.iter().copied());
        }
    }

    pub(in crate::engine::storage_engine) fn materialized_series_snapshot(&self) -> Vec<SeriesId> {
        #[cfg(test)]
        self.invoke_metadata_live_series_snapshot_hook();
        self.visibility
            .materialized_series
            .read()
            .iter()
            .copied()
            .collect::<Vec<_>>()
    }

    #[cfg(test)]
    fn invoke_metadata_live_series_snapshot_hook(&self) {
        let hook = self
            .persist_test_hooks
            .metadata_live_series_snapshot_hook
            .read()
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn invoke_metadata_live_series_pre_prune_hook(&self) {
        let hook = self
            .persist_test_hooks
            .metadata_live_series_pre_prune_hook
            .read()
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(in crate::engine::storage_engine) fn remove_materialized_series_ids<I>(
        &self,
        series_ids: I,
    ) -> Vec<SeriesId>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let series_ids = self
            .materialized_series_write_context()
            .remove_materialized_series_ids(series_ids);
        if series_ids.is_empty() {
            return Vec::new();
        }

        self.clear_series_visible_timestamp_cache(series_ids.iter().copied());
        series_ids
    }

    pub(in crate::engine::storage_engine) fn reconcile_live_metadata_indexes(&self) -> Result<()> {
        let materialized_series = self.materialized_series_snapshot();
        let _ = self.live_series_ids(materialized_series, true)?;
        Ok(())
    }
}
