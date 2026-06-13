#[path = "memory_accounting/context.rs"]
mod context;

use roaring::RoaringTreemap;

use super::super::MemoryDeltaBytes;
use super::super::*;

impl ChunkStorage {
    pub(in super::super) fn memory_budget_value(&self) -> usize {
        self.memory_accounting_context().budget_value()
    }

    pub(in super::super) fn memory_used_value(&self) -> usize {
        self.memory_accounting_context().used_value()
    }

    pub(in super::super) fn add_included_memory_component_bytes(
        &self,
        component: &AtomicU64,
        bytes: usize,
    ) {
        if !self.memory.accounting_enabled || bytes == 0 {
            return;
        }

        let increment = saturating_u64_from_usize(bytes);
        component.fetch_add(increment, Ordering::AcqRel);
        self.memory
            .shared_used_bytes
            .fetch_add(increment, Ordering::AcqRel);
        self.memory
            .used_bytes
            .fetch_add(increment, Ordering::AcqRel);
    }

    pub(in super::super) fn sub_included_memory_component_bytes(
        &self,
        component: &AtomicU64,
        bytes: usize,
    ) {
        if !self.memory.accounting_enabled || bytes == 0 {
            return;
        }

        let decrement = saturating_u64_from_usize(bytes);
        component.fetch_sub(decrement, Ordering::AcqRel);
        self.memory
            .shared_used_bytes
            .fetch_sub(decrement, Ordering::AcqRel);
        self.memory
            .used_bytes
            .fetch_sub(decrement, Ordering::AcqRel);
    }

    pub(in super::super) fn account_included_memory_component_delta_bytes(
        &self,
        component: &AtomicU64,
        before: usize,
        after: usize,
    ) {
        self.account_included_memory_component_delta(
            component,
            MemoryDeltaBytes::between(before, after),
        );
    }

    pub(in super::super) fn account_included_memory_component_delta(
        &self,
        component: &AtomicU64,
        delta: MemoryDeltaBytes,
    ) {
        if !self.memory.accounting_enabled {
            return;
        }

        if delta.added_bytes >= delta.removed_bytes {
            self.add_included_memory_component_bytes(
                component,
                delta.added_bytes.saturating_sub(delta.removed_bytes),
            );
        } else {
            self.sub_included_memory_component_bytes(
                component,
                delta.removed_bytes.saturating_sub(delta.added_bytes),
            );
        }
    }

    pub(in super::super) fn with_included_memory_delta<T, R, Measure, Mutate>(
        &self,
        component: &AtomicU64,
        state: &mut T,
        measure: Measure,
        mutate: Mutate,
    ) -> R
    where
        Measure: Fn(&T) -> usize,
        Mutate: FnOnce(&mut T) -> R,
    {
        if !self.memory.accounting_enabled {
            return mutate(state);
        }

        let before = measure(state);
        let result = mutate(state);
        let after = measure(state);
        self.account_included_memory_component_delta_bytes(component, before, after);
        result
    }

    pub(in super::super) fn with_visibility_state_memory_delta<R>(
        &self,
        summaries: &mut HashMap<SeriesId, SeriesVisibilitySummary>,
        cache: &mut HashMap<SeriesId, Option<i64>>,
        bounded_cache: &mut HashMap<SeriesId, Option<i64>>,
        mutate: impl FnOnce(
            &mut HashMap<SeriesId, SeriesVisibilitySummary>,
            &mut HashMap<SeriesId, Option<i64>>,
            &mut HashMap<SeriesId, Option<i64>>,
        ) -> R,
    ) -> R {
        if !self.memory.accounting_enabled {
            return mutate(summaries, cache, bounded_cache);
        }

        let before =
            Self::series_visibility_state_memory_usage_bytes(summaries, cache, bounded_cache);
        let result = mutate(summaries, cache, bounded_cache);
        let after =
            Self::series_visibility_state_memory_usage_bytes(summaries, cache, bounded_cache);
        self.account_included_memory_component_delta_bytes(
            &self.memory.metadata_used_bytes,
            before,
            after,
        );
        result
    }

    pub(in super::super) fn add_memory_usage_bytes(&self, shard_idx: usize, bytes: usize) {
        if !self.memory.accounting_enabled || bytes == 0 {
            return;
        }

        let increment = saturating_u64_from_usize(bytes);
        self.memory.used_bytes_by_shard[shard_idx].fetch_add(increment, Ordering::AcqRel);
        self.memory
            .used_bytes
            .fetch_add(increment, Ordering::AcqRel);
    }

    pub(in super::super) fn sub_memory_usage_bytes(&self, shard_idx: usize, bytes: usize) {
        if !self.memory.accounting_enabled || bytes == 0 {
            return;
        }

        let decrement = saturating_u64_from_usize(bytes);
        self.memory.used_bytes_by_shard[shard_idx].fetch_sub(decrement, Ordering::AcqRel);
        self.memory
            .used_bytes
            .fetch_sub(decrement, Ordering::AcqRel);
    }

    pub(in super::super) fn account_memory_delta(&self, shard_idx: usize, delta: MemoryDeltaBytes) {
        if !self.memory.accounting_enabled {
            return;
        }
        if delta.added_bytes >= delta.removed_bytes {
            self.add_memory_usage_bytes(
                shard_idx,
                delta.added_bytes.saturating_sub(delta.removed_bytes),
            );
        } else {
            self.sub_memory_usage_bytes(
                shard_idx,
                delta.removed_bytes.saturating_sub(delta.added_bytes),
            );
        }
    }

    #[allow(dead_code)]
    pub(in super::super) fn account_memory_delta_bytes(
        &self,
        shard_idx: usize,
        before: usize,
        after: usize,
    ) {
        self.account_memory_delta(shard_idx, MemoryDeltaBytes::between(before, after));
    }

    pub(in super::super) fn refresh_memory_usage(&self) -> usize {
        self.memory_accounting_context().refresh_memory_usage()
    }

    pub(in super::super) fn memory_observability_snapshot(
        &self,
    ) -> crate::MemoryObservabilitySnapshot {
        let memory_accounting = self.memory_accounting_context();
        if !self.memory.accounting_enabled {
            memory_accounting.refresh_memory_usage();
        }

        crate::MemoryObservabilitySnapshot {
            budgeted_bytes: memory_accounting.used_value(),
            excluded_bytes: 0,
            active_and_sealed_bytes: memory_accounting.active_and_sealed_used_value(),
            registry_bytes: context::MemoryAccountingContext::component_value(
                &self.memory.registry_used_bytes,
            ),
            metadata_cache_bytes: context::MemoryAccountingContext::component_value(
                &self.memory.metadata_used_bytes,
            ),
            persisted_index_bytes: context::MemoryAccountingContext::component_value(
                &self.memory.persisted_index_used_bytes,
            ),
            persisted_mmap_bytes: context::MemoryAccountingContext::component_value(
                &self.memory.persisted_mmap_used_bytes,
            ),
            tombstone_bytes: context::MemoryAccountingContext::component_value(
                &self.memory.tombstone_used_bytes,
            ),
            excluded_persisted_mmap_bytes: 0,
        }
    }

    pub(in super::super) fn active_state_memory_usage_bytes(state: &ActiveSeriesState) -> usize {
        std::mem::size_of::<ActiveSeriesState>()
            .saturating_add(
                state
                    .partition_head_count()
                    .saturating_mul(
                        std::mem::size_of::<super::super::state::ActivePartitionHead>(),
                    ),
            )
            .saturating_add(state.partition_heads.values().fold(0usize, |acc, head| {
                acc.saturating_add(
                    head.builder
                        .capacity()
                        .saturating_mul(std::mem::size_of::<ChunkPoint>()),
                )
                .saturating_add(
                    head.builder
                        .point_block_capacity()
                        .saturating_mul(std::mem::size_of::<Arc<Vec<ChunkPoint>>>()),
                )
                .saturating_add(
                    head.builder
                        .frozen_point_block_count()
                        .saturating_mul(std::mem::size_of::<Vec<ChunkPoint>>()),
                )
                .saturating_add(head.builder_value_heap_bytes)
            }))
    }

    fn active_state_memory_usage_bytes_reconciled(state: &ActiveSeriesState) -> usize {
        let mut bytes = std::mem::size_of::<ActiveSeriesState>().saturating_add(
            state
                .partition_head_count()
                .saturating_mul(std::mem::size_of::<super::super::state::ActivePartitionHead>()),
        );
        for head in state.partition_heads.values() {
            bytes = bytes.saturating_add(
                head.builder
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ChunkPoint>()),
            );
            bytes = bytes.saturating_add(
                head.builder
                    .point_block_capacity()
                    .saturating_mul(std::mem::size_of::<Arc<Vec<ChunkPoint>>>()),
            );
            bytes = bytes.saturating_add(
                head.builder
                    .frozen_point_block_count()
                    .saturating_mul(std::mem::size_of::<Vec<ChunkPoint>>()),
            );
            for point in head.builder.iter_points() {
                bytes = bytes.saturating_add(value_heap_bytes(&point.value));
            }
        }
        bytes
    }

    pub(in super::super) fn chunk_memory_usage_bytes(chunk: &Chunk) -> usize {
        let mut bytes = std::mem::size_of::<Chunk>()
            .saturating_add(
                chunk
                    .points
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ChunkPoint>()),
            )
            .saturating_add(chunk.encoded_payload.capacity());

        for point in &chunk.points {
            bytes = bytes.saturating_add(value_heap_bytes(&point.value));
        }

        bytes
    }

    pub(in super::super) fn hash_map_memory_usage_bytes<K, V>(map: &HashMap<K, V>) -> usize {
        map.capacity().saturating_mul(std::mem::size_of::<(K, V)>())
    }

    pub(in super::super) fn btree_set_series_id_memory_usage_bytes(
        set: &BTreeSet<SeriesId>,
    ) -> usize {
        set.len().saturating_mul(std::mem::size_of::<SeriesId>())
    }

    pub(in super::super) fn bitmap_memory_usage_bytes(bitmap: &RoaringTreemap) -> usize {
        if bitmap.is_empty() {
            0
        } else {
            bitmap.serialized_size()
        }
    }

    pub(in super::super) fn tombstone_map_memory_usage_bytes(
        tombstones: &crate::engine::tombstone::TombstoneMap,
    ) -> usize {
        let mut bytes = Self::hash_map_memory_usage_bytes::<
            SeriesId,
            Vec<crate::engine::tombstone::TombstoneRange>,
        >(tombstones);
        for ranges in tombstones.values() {
            bytes =
                bytes.saturating_add(ranges.capacity().saturating_mul(std::mem::size_of::<
                    crate::engine::tombstone::TombstoneRange,
                >()));
        }
        bytes
    }

    fn metadata_shard_index_memory_usage_bytes(index: &MetadataShardIndex) -> usize {
        index
            .series_ids_by_shard
            .read()
            .iter()
            .fold(0usize, |acc, bucket| {
                acc.saturating_add(Self::btree_set_series_id_memory_usage_bytes(bucket))
            })
    }

    fn persisted_segment_state_memory_usage_bytes(
        root: &std::path::Path,
        state: &super::super::state::PersistedSegmentState,
    ) -> usize {
        let mut bytes = root.as_os_str().len().saturating_add(std::mem::size_of::<
            super::super::state::PersistedSegmentState,
        >());
        if let Some(time_bucket_postings) = &state.time_bucket_postings {
            bytes = bytes.saturating_add(
                std::mem::size_of::<super::super::state::PersistedSegmentTimeBucketIndex>()
                    .saturating_add(
                        time_bucket_postings
                            .buckets
                            .capacity()
                            .saturating_mul(std::mem::size_of::<RoaringTreemap>()),
                    ),
            );
            for bucket in &time_bucket_postings.buckets {
                if !bucket.is_empty() {
                    bytes = bytes.saturating_add(bucket.serialized_size());
                }
            }
        }
        bytes = bytes.saturating_add(Self::hash_map_memory_usage_bytes::<
            SeriesId,
            super::super::state::PersistedSeriesTimeRangeSummary,
        >(&state.series_time_summaries));
        bytes = bytes.saturating_add(Self::hash_map_memory_usage_bytes::<
            SeriesId,
            Vec<PersistedChunkRef>,
        >(&state.chunk_refs_by_series));
        for refs in state.chunk_refs_by_series.values() {
            bytes = bytes.saturating_add(
                refs.capacity()
                    .saturating_mul(std::mem::size_of::<PersistedChunkRef>()),
            );
        }
        bytes
    }

    pub(in super::super) fn persisted_index_included_memory_usage_bytes(
        index: &PersistedIndexState,
    ) -> usize {
        let mut bytes = Self::hash_map_memory_usage_bytes::<SeriesId, Vec<PersistedChunkRef>>(
            &index.chunk_refs,
        )
        .saturating_add(Self::hash_map_memory_usage_bytes::<
            u64,
            std::sync::OnceLock<crate::engine::encoder::TimestampSearchIndex>,
        >(&index.chunk_timestamp_indexes))
        .saturating_add(
            Self::hash_map_memory_usage_bytes::<usize, Arc<PlatformMmap>>(&index.segment_maps),
        )
        .saturating_add(Self::hash_map_memory_usage_bytes::<
            usize,
            super::super::tiering::PersistedSegmentTier,
        >(&index.segment_tiers))
        .saturating_add(index.merged_postings.memory_usage_bytes())
        .saturating_add(Self::bitmap_memory_usage_bytes(
            &index.runtime_metadata_delta_series_ids,
        ));

        for refs in index.chunk_refs.values() {
            bytes = bytes.saturating_add(
                refs.capacity()
                    .saturating_mul(std::mem::size_of::<PersistedChunkRef>()),
            );
        }
        for search_index in index.chunk_timestamp_indexes.values() {
            if let Some(search_index) = search_index.get() {
                bytes = bytes.saturating_add(search_index.memory_usage_bytes());
            }
        }
        for (root, state) in &index.segments_by_root {
            bytes = bytes.saturating_add(Self::persisted_segment_state_memory_usage_bytes(
                root, state,
            ));
        }
        bytes
    }

    pub(in super::super) fn persisted_index_persisted_mmap_bytes(
        index: &PersistedIndexState,
    ) -> usize {
        index
            .segment_maps
            .values()
            .fold(0usize, |acc, mapped| acc.saturating_add(mapped.len()))
    }

    pub(in super::super) fn prune_empty_active_series(&self) {
        if !self.memory.accounting_enabled {
            for shard in &self.chunks.active_builders {
                shard.write().retain(|_, state| !state.is_empty());
            }
            return;
        }

        for (shard_idx, shard) in self.chunks.active_builders.iter().enumerate() {
            let mut active = shard.write();
            let mut removed_bytes = 0usize;
            active.retain(|_, state| {
                let keep = !state.is_empty();
                if !keep {
                    removed_bytes =
                        removed_bytes.saturating_add(Self::active_state_memory_usage_bytes(state));
                }
                keep
            });
            self.account_memory_delta(shard_idx, MemoryDeltaBytes::from_totals(0, removed_bytes));
        }
    }

    pub(in super::super) fn mark_persisted_chunk_watermarks(
        &self,
        watermarks: &HashMap<SeriesId, u64>,
    ) {
        if watermarks.is_empty() {
            return;
        }

        let mut persisted = self.chunks.persisted_chunk_watermarks.write();
        self.with_included_memory_delta(
            &self.memory.metadata_used_bytes,
            &mut persisted,
            |persisted| Self::hash_map_memory_usage_bytes::<SeriesId, u64>(persisted),
            |persisted| {
                for (series_id, watermark) in watermarks {
                    let entry = persisted.entry(*series_id).or_insert(0);
                    *entry = (*entry).max(*watermark);
                }
            },
        );
        drop(persisted);
    }
}
