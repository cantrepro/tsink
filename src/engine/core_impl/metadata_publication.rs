use super::*;

fn normalized_series_ids<I>(series_ids: I) -> Vec<SeriesId>
where
    I: IntoIterator<Item = SeriesId>,
{
    let mut series_ids = series_ids.into_iter().collect::<Vec<_>>();
    series_ids.sort_unstable();
    series_ids.dedup();
    series_ids
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct RuntimeMetadataDeltaWriteContext<'a> {
    pub(in crate::engine::storage_engine) accounting_enabled: bool,
    pub(in crate::engine::storage_engine) materialized_series: &'a RwLock<BTreeSet<SeriesId>>,
    pub(in crate::engine::storage_engine) persisted_index: &'a RwLock<PersistedIndexState>,
    pub(in crate::engine::storage_engine) persisted_index_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) shared_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) used_bytes: &'a AtomicU64,
}

impl<'a> RuntimeMetadataDeltaWriteContext<'a> {
    pub(in crate::engine::storage_engine) fn reconcile_series_ids<I>(self, series_ids: I)
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let series_ids = normalized_series_ids(series_ids);
        if series_ids.is_empty() {
            return;
        }

        let mut persisted_index = self.persisted_index.write();
        with_included_memory_delta(
            self.accounting_enabled,
            self.persisted_index_used_bytes,
            self.shared_used_bytes,
            self.used_bytes,
            &mut persisted_index,
            |persisted_index| {
                ChunkStorage::bitmap_memory_usage_bytes(
                    &persisted_index.runtime_metadata_delta_series_ids,
                )
            },
            |persisted_index| self.reconcile_series_ids_locked(persisted_index, series_ids),
        );
    }

    pub(in crate::engine::storage_engine) fn reconcile_series_ids_locked<I>(
        self,
        persisted_index: &mut PersistedIndexState,
        series_ids: I,
    ) where
        I: IntoIterator<Item = SeriesId>,
    {
        let materialized_series = self.materialized_series.read();
        for series_id in series_ids {
            if materialized_series.contains(&series_id)
                && !persisted_index.chunk_refs.contains_key(&series_id)
            {
                persisted_index
                    .runtime_metadata_delta_series_ids
                    .insert(series_id);
            } else {
                persisted_index
                    .runtime_metadata_delta_series_ids
                    .remove(series_id);
            }
        }
    }

    pub(in crate::engine::storage_engine) fn rebuild_locked(
        self,
        persisted_index: &mut PersistedIndexState,
    ) {
        let materialized_series = self.materialized_series.read();
        let mut delta_series_ids = roaring::RoaringTreemap::new();
        for series_id in materialized_series.iter().copied() {
            if !persisted_index.chunk_refs.contains_key(&series_id) {
                delta_series_ids.insert(series_id);
            }
        }
        persisted_index.runtime_metadata_delta_series_ids = delta_series_ids;
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct MetadataShardPublicationContext<'a> {
    pub(in crate::engine::storage_engine) accounting_enabled: bool,
    pub(in crate::engine::storage_engine) metadata_shard_index: Option<&'a MetadataShardIndex>,
    pub(in crate::engine::storage_engine) registry: &'a RwLock<SeriesRegistry>,
    pub(in crate::engine::storage_engine) metadata_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) shared_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) used_bytes: &'a AtomicU64,
}

impl<'a> MetadataShardPublicationContext<'a> {
    pub(in crate::engine::storage_engine) fn publish_materialized_series_ids<I>(self, series_ids: I)
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let Some(index) = self.metadata_shard_index else {
            return;
        };
        let series_ids = normalized_series_ids(series_ids);
        if series_ids.is_empty() {
            return;
        }

        let registry = self.registry.read();
        let mut shard_buckets = index.series_ids_by_shard.write();
        let mut inserted_bucket_entries = 0usize;
        for series_id in series_ids {
            let Some(series_key) = registry.decode_series_key(series_id) else {
                continue;
            };
            let shard = index.shard_for_series(&series_key.metric, &series_key.labels);
            if let Some(bucket) = shard_buckets.get_mut(shard as usize) {
                inserted_bucket_entries =
                    inserted_bucket_entries.saturating_add(usize::from(bucket.insert(series_id)));
            }
        }
        drop(shard_buckets);

        add_included_memory_component_bytes(
            self.accounting_enabled,
            self.metadata_used_bytes,
            self.shared_used_bytes,
            self.used_bytes,
            inserted_bucket_entries.saturating_mul(std::mem::size_of::<SeriesId>()),
        );
    }

    pub(in crate::engine::storage_engine) fn unpublish_materialized_series_ids<I>(
        self,
        series_ids: I,
    ) where
        I: IntoIterator<Item = SeriesId>,
    {
        let Some(index) = self.metadata_shard_index else {
            return;
        };
        let series_ids = normalized_series_ids(series_ids);
        if series_ids.is_empty() {
            return;
        }

        let registry = self.registry.read();
        let mut shard_buckets = index.series_ids_by_shard.write();
        let mut removed_bucket_entries = 0usize;
        for series_id in series_ids {
            if let Some(series_key) = registry.decode_series_key(series_id) {
                let shard = index.shard_for_series(&series_key.metric, &series_key.labels);
                if let Some(bucket) = shard_buckets.get_mut(shard as usize) {
                    removed_bucket_entries = removed_bucket_entries
                        .saturating_add(usize::from(bucket.remove(&series_id)));
                }
            } else {
                for bucket in shard_buckets.iter_mut() {
                    removed_bucket_entries = removed_bucket_entries
                        .saturating_add(usize::from(bucket.remove(&series_id)));
                }
            }
        }
        drop(shard_buckets);

        sub_included_memory_component_bytes(
            self.accounting_enabled,
            self.metadata_used_bytes,
            self.shared_used_bytes,
            self.used_bytes,
            removed_bucket_entries.saturating_mul(std::mem::size_of::<SeriesId>()),
        );
    }
}

impl<'a> LifecyclePublicationContext<'a> {
    pub(in crate::engine::storage_engine) fn mutate_registry<R>(
        self,
        mutate: impl FnOnce(&mut SeriesRegistry) -> R,
    ) -> R {
        let mut registry = self.registry.write();
        with_included_memory_delta(
            self.accounting_enabled,
            self.registry_used_bytes,
            self.shared_used_bytes,
            self.used_bytes,
            &mut registry,
            |registry| registry.memory_usage_bytes(),
            |registry| mutate(&mut *registry),
        )
    }
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn runtime_metadata_delta_write_context(
        &self,
    ) -> RuntimeMetadataDeltaWriteContext<'_> {
        RuntimeMetadataDeltaWriteContext {
            accounting_enabled: self.memory.accounting_enabled,
            materialized_series: &self.visibility.materialized_series,
            persisted_index: &self.persisted.persisted_index,
            persisted_index_used_bytes: &self.memory.persisted_index_used_bytes,
            shared_used_bytes: &self.memory.shared_used_bytes,
            used_bytes: &self.memory.used_bytes,
        }
    }

    pub(in crate::engine::storage_engine) fn metadata_shard_publication_context(
        &self,
    ) -> MetadataShardPublicationContext<'_> {
        MetadataShardPublicationContext {
            accounting_enabled: self.memory.accounting_enabled,
            metadata_shard_index: self.catalog.metadata_shard_index.as_ref(),
            registry: &self.catalog.registry,
            metadata_used_bytes: &self.memory.metadata_used_bytes,
            shared_used_bytes: &self.memory.shared_used_bytes,
            used_bytes: &self.memory.used_bytes,
        }
    }
}
