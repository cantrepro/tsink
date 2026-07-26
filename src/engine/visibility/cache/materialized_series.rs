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
        let Some(generation_before) = generation_before else {
            return;
        };
        if dead_series_ids.is_empty() || self.live_series_pruning_generation() != generation_before
        {
            return;
        }

        #[cfg(test)]
        // Deliberately after the optimistic load and before the set-lock recheck.
        self.invoke_metadata_live_series_pre_prune_hook();

        let mut removed_series_ids =
            self.remove_materialized_series_ids_if_generation(dead_series_ids, generation_before);
        if removed_series_ids.is_empty() {
            return;
        }

        #[cfg(test)]
        self.invoke_metadata_live_series_post_remove_pre_unpublish_hook();

        self.runtime_metadata_delta_write_context()
            .reconcile_series_ids(removed_series_ids.iter().copied());
        self.metadata_shard_publication_context()
            .unpublish_materialized_series_ids(removed_series_ids.iter().copied());

        // A writer may reinsert and fully publish one of these IDs after removal but before the
        // unpublish above. Filter the same bounded vector in place, then repair both secondary
        // indexes after unpublication. If insertion begins after this snapshot, its ordinary
        // publication necessarily follows and supplies the same repair.
        {
            let materialized_series = self.visibility.materialized_series.read();
            removed_series_ids.retain(|series_id| materialized_series.contains(series_id));
            if !removed_series_ids.is_empty() {
                // Hold the set read lock through shard repair. A later removal must therefore
                // follow this publication and will unpublish it; a later insertion will publish
                // after this repair. Runtime-delta reconciliation reads the set internally, so it
                // runs after dropping this guard to preserve its persisted-index lock order.
                self.metadata_shard_publication_context()
                    .publish_materialized_series_ids(removed_series_ids.iter().copied());
            }
        }
        if !removed_series_ids.is_empty() {
            self.runtime_metadata_delta_write_context()
                .reconcile_series_ids(removed_series_ids.iter().copied());
        }
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn materialized_series_snapshot(&self) -> Vec<SeriesId> {
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

    #[cfg(test)]
    fn invoke_metadata_live_series_post_remove_pre_unpublish_hook(&self) {
        let hook = self
            .persist_test_hooks
            .metadata_live_series_post_remove_pre_unpublish_hook
            .read()
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    fn remove_materialized_series_ids_if_generation<I>(
        &self,
        series_ids: I,
        expected_generation: u64,
    ) -> Vec<SeriesId>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let series_ids = self
            .materialized_series_write_context()
            .remove_materialized_series_ids_if_generation(series_ids, expected_generation);
        if series_ids.is_empty() {
            return Vec::new();
        }

        self.clear_series_visible_timestamp_cache(series_ids.iter().copied());
        series_ids
    }

    pub(in crate::engine::storage_engine) fn reconcile_live_metadata_indexes(&self) -> Result<()> {
        self.drain_live_metadata_reconciliation_pages()
    }
}
