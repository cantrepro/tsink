use super::super::super::core_impl::{
    ChunkContext, LifecyclePublicationContext, MaterializedSeriesReadContext,
    MetadataShardPublicationContext, PersistedSealedBudgetContext, RegistryBookkeepingContext,
    RegistryMemoryContext, VisibilityCacheReadContext,
};
use super::super::super::*;

#[derive(Clone, Copy)]
struct VisibilityMemoryMeasurementContext<'a> {
    materialized_series: MaterializedSeriesReadContext<'a>,
    visibility_cache: VisibilityCacheReadContext<'a>,
}

impl<'a> VisibilityMemoryMeasurementContext<'a> {
    fn metadata_visibility_memory_usage_bytes(self) -> usize {
        let materialized_series = self.materialized_series.materialized_series.read();
        let summaries = self.visibility_cache.series_visibility_summaries.read();
        let visible_cache = self.visibility_cache.series_visible_max_timestamps.read();
        let bounded_visible_cache = self
            .visibility_cache
            .series_visible_bounded_max_timestamps
            .read();

        ChunkStorage::btree_set_series_id_memory_usage_bytes(&materialized_series).saturating_add(
            ChunkStorage::series_visibility_state_memory_usage_bytes(
                &summaries,
                &visible_cache,
                &bounded_visible_cache,
            ),
        )
    }
}

#[derive(Clone, Copy)]
struct TombstoneMemoryMeasurementContext<'a> {
    tombstones: &'a RwLock<crate::engine::tombstone::TombstoneMap>,
}

impl<'a> TombstoneMemoryMeasurementContext<'a> {
    fn tombstone_memory_usage_bytes(self) -> usize {
        let tombstones = self.tombstones.read();
        ChunkStorage::tombstone_map_memory_usage_bytes(&tombstones)
    }
}

impl<'a> ChunkContext<'a> {
    pub(super) fn reconciled_shard_memory_usage_bytes(self, shard_idx: usize) -> usize {
        let active = self.active_builders[shard_idx].read();
        let active_total = active.values().fold(0usize, |acc, state| {
            acc.saturating_add(ChunkStorage::active_state_memory_usage_bytes_reconciled(
                state,
            ))
        });

        let sealed = self.sealed_chunks[shard_idx].read();
        let sealed_total = sealed.values().fold(0usize, |series_acc, chunks| {
            series_acc.saturating_add(chunks.values().fold(0usize, |chunk_acc, chunk| {
                chunk_acc.saturating_add(ChunkStorage::chunk_memory_usage_bytes(chunk))
            }))
        });

        active_total.saturating_add(sealed_total)
    }
}

impl<'a> RegistryMemoryContext<'a> {
    pub(super) fn measured_registry_memory_usage_bytes(self) -> usize {
        self.registry.read().memory_usage_bytes()
    }
}

impl<'a> RegistryBookkeepingContext<'a> {
    pub(super) fn pending_series_memory_usage_bytes(self) -> usize {
        let pending = self.pending_series_ids.read();
        ChunkStorage::btree_set_series_id_memory_usage_bytes(&pending)
    }
}

impl<'a> MetadataShardPublicationContext<'a> {
    pub(super) fn metadata_shard_index_memory_usage_bytes(self) -> usize {
        self.metadata_shard_index
            .map(ChunkStorage::metadata_shard_index_memory_usage_bytes)
            .unwrap_or(0)
    }
}

impl<'a> PersistedSealedBudgetContext<'a> {
    pub(super) fn persisted_chunk_watermarks_memory_usage_bytes(self) -> usize {
        let persisted = self.persisted_chunk_watermarks.read();
        ChunkStorage::hash_map_memory_usage_bytes::<SeriesId, u64>(&persisted)
    }
}

impl<'a> LifecyclePublicationContext<'a> {
    pub(super) fn measured_persisted_index_memory_usage_bytes(self) -> usize {
        let persisted_index = self.persisted_index.read();
        ChunkStorage::persisted_index_included_memory_usage_bytes(&persisted_index)
    }

    pub(super) fn measured_persisted_mmap_memory_usage_bytes(self) -> usize {
        let persisted_index = self.persisted_index.read();
        ChunkStorage::persisted_index_persisted_mmap_bytes(&persisted_index)
    }
}

#[derive(Clone, Copy)]
pub(super) struct MemoryAccountingContext<'a> {
    budget_bytes: &'a AtomicU64,
    used_bytes: &'a AtomicU64,
    used_bytes_by_shard: &'a [AtomicU64; IN_MEMORY_SHARD_COUNT],
    registry_used_bytes: &'a AtomicU64,
    metadata_used_bytes: &'a AtomicU64,
    persisted_index_used_bytes: &'a AtomicU64,
    persisted_mmap_used_bytes: &'a AtomicU64,
    tombstone_used_bytes: &'a AtomicU64,
    shared_used_bytes: &'a AtomicU64,
    chunks: ChunkContext<'a>,
    registry_memory: RegistryMemoryContext<'a>,
    registry_bookkeeping: RegistryBookkeepingContext<'a>,
    visibility: VisibilityMemoryMeasurementContext<'a>,
    metadata_shards: MetadataShardPublicationContext<'a>,
    persisted_sealed: PersistedSealedBudgetContext<'a>,
    lifecycle: LifecyclePublicationContext<'a>,
    tombstones: TombstoneMemoryMeasurementContext<'a>,
}

impl<'a> MemoryAccountingContext<'a> {
    fn tracked_bytes(counter: &AtomicU64) -> usize {
        counter.load(Ordering::Acquire).min(usize::MAX as u64) as usize
    }

    fn store_tracked_bytes(counter: &AtomicU64, bytes: usize) {
        counter.store(saturating_u64_from_usize(bytes), Ordering::Release);
    }

    pub(super) fn budget_value(self) -> usize {
        Self::tracked_bytes(self.budget_bytes)
    }

    pub(super) fn used_value(self) -> usize {
        Self::tracked_bytes(self.used_bytes)
    }

    pub(super) fn component_value(component: &AtomicU64) -> usize {
        Self::tracked_bytes(component)
    }

    pub(super) fn active_and_sealed_used_value(self) -> usize {
        self.used_bytes_by_shard.iter().fold(0usize, |acc, shard| {
            acc.saturating_add(Self::component_value(shard))
        })
    }

    fn sync_shard_memory_usage(self, shard_idx: usize) -> usize {
        let shard_used = self.chunks.reconciled_shard_memory_usage_bytes(shard_idx);
        Self::store_tracked_bytes(&self.used_bytes_by_shard[shard_idx], shard_used);
        shard_used
    }

    fn metadata_memory_usage_bytes(self) -> usize {
        self.registry_bookkeeping
            .pending_series_memory_usage_bytes()
            .saturating_add(self.visibility.metadata_visibility_memory_usage_bytes())
            .saturating_add(
                self.persisted_sealed
                    .persisted_chunk_watermarks_memory_usage_bytes(),
            )
            .saturating_add(
                self.metadata_shards
                    .metadata_shard_index_memory_usage_bytes(),
            )
    }

    pub(super) fn refresh_memory_usage(self) -> usize {
        let mut active_and_sealed_used = 0usize;
        for shard_idx in 0..IN_MEMORY_SHARD_COUNT {
            active_and_sealed_used =
                active_and_sealed_used.saturating_add(self.sync_shard_memory_usage(shard_idx));
        }

        let registry_used = self.registry_memory.measured_registry_memory_usage_bytes();
        let metadata_used = self.metadata_memory_usage_bytes();
        let persisted_index_used = self.lifecycle.measured_persisted_index_memory_usage_bytes();
        let persisted_mmap_used = self.lifecycle.measured_persisted_mmap_memory_usage_bytes();
        let tombstone_used = self.tombstones.tombstone_memory_usage_bytes();

        let shared_used = registry_used
            .saturating_add(metadata_used)
            .saturating_add(persisted_index_used)
            .saturating_add(persisted_mmap_used)
            .saturating_add(tombstone_used);
        let used = active_and_sealed_used.saturating_add(shared_used);

        Self::store_tracked_bytes(self.registry_used_bytes, registry_used);
        Self::store_tracked_bytes(self.metadata_used_bytes, metadata_used);
        Self::store_tracked_bytes(self.persisted_index_used_bytes, persisted_index_used);
        Self::store_tracked_bytes(self.persisted_mmap_used_bytes, persisted_mmap_used);
        Self::store_tracked_bytes(self.tombstone_used_bytes, tombstone_used);
        Self::store_tracked_bytes(self.shared_used_bytes, shared_used);
        Self::store_tracked_bytes(self.used_bytes, used);
        used
    }
}

impl ChunkStorage {
    pub(super) fn memory_accounting_context(&self) -> MemoryAccountingContext<'_> {
        MemoryAccountingContext {
            budget_bytes: &self.memory.budget_bytes,
            used_bytes: &self.memory.used_bytes,
            used_bytes_by_shard: &self.memory.used_bytes_by_shard,
            registry_used_bytes: &self.memory.registry_used_bytes,
            metadata_used_bytes: &self.memory.metadata_used_bytes,
            persisted_index_used_bytes: &self.memory.persisted_index_used_bytes,
            persisted_mmap_used_bytes: &self.memory.persisted_mmap_used_bytes,
            tombstone_used_bytes: &self.memory.tombstone_used_bytes,
            shared_used_bytes: &self.memory.shared_used_bytes,
            chunks: self.chunk_context(),
            registry_memory: self.registry_memory_context(),
            registry_bookkeeping: self.registry_bookkeeping_context(),
            visibility: VisibilityMemoryMeasurementContext {
                materialized_series: self.materialized_series_read_context(),
                visibility_cache: self.visibility_cache_read_context(),
            },
            metadata_shards: self.metadata_shard_publication_context(),
            persisted_sealed: self.persisted_sealed_budget_context(),
            lifecycle: self.lifecycle_publication_context(),
            tombstones: TombstoneMemoryMeasurementContext {
                tombstones: &self.visibility.tombstones,
            },
        }
    }
}
