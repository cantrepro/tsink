use super::super::super::core_impl::PersistedSealedBudgetContext;
use super::super::super::MemoryDeltaBytes;
use super::super::super::*;

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct BudgetEnforcementContext<'a> {
    budget_bytes: &'a AtomicU64,
    used_bytes: &'a AtomicU64,
    persisted_sealed: PersistedSealedBudgetContext<'a>,
}

impl<'a> BudgetEnforcementContext<'a> {
    fn tracked_bytes(counter: &AtomicU64) -> usize {
        counter.load(Ordering::Acquire).min(usize::MAX as u64) as usize
    }

    fn sealed_chunk_is_present_in_persisted_chunks(
        persisted_chunks: Option<&[PersistedChunkRef]>,
        key: SealedChunkKey,
        chunk: &Chunk,
    ) -> bool {
        persisted_chunks.is_some_and(|persisted_chunks| {
            persisted_chunks.iter().any(|chunk_ref| {
                chunk_ref.min_ts == key.min_ts
                    && chunk_ref.max_ts == key.max_ts
                    && chunk_ref.point_count == key.point_count
                    && chunk_ref.lane == chunk.header.lane
                    && chunk_ref.ts_codec == chunk.header.ts_codec
                    && chunk_ref.value_codec == chunk.header.value_codec
            })
        })
    }

    pub(in crate::engine::storage_engine) fn budget_value(self) -> usize {
        Self::tracked_bytes(self.budget_bytes)
    }

    pub(in crate::engine::storage_engine) fn used_value(self) -> usize {
        Self::tracked_bytes(self.used_bytes)
    }

    pub(in crate::engine::storage_engine) fn can_reclaim_persisted_memory(self) -> bool {
        self.persisted_sealed.has_persisted_lanes
    }

    pub(in crate::engine::storage_engine) fn lock_backpressure(
        self,
    ) -> parking_lot::MutexGuard<'a, ()> {
        self.persisted_sealed.backpressure_lock.lock()
    }

    fn find_oldest_evictable_sealed_chunk(self) -> Option<(usize, SeriesId, SealedChunkKey)> {
        let persisted = self.persisted_sealed.persisted_chunk_watermarks.read();
        let persisted_index = self.persisted_sealed.persisted_index.read();
        let mut oldest = None;

        for (shard_idx, shard) in self.persisted_sealed.sealed_chunks.iter().enumerate() {
            let sealed = shard.read();
            for (series_id, chunks) in sealed.iter() {
                let persisted_sequence = persisted.get(series_id).copied().unwrap_or(0);
                let persisted_chunks = persisted_index
                    .chunk_refs
                    .get(series_id)
                    .map(|chunks| chunks.as_slice());
                for (key, chunk) in chunks {
                    if key.sequence > persisted_sequence
                        || !Self::sealed_chunk_is_present_in_persisted_chunks(
                            persisted_chunks,
                            *key,
                            chunk,
                        )
                    {
                        continue;
                    }
                    if oldest
                        .map(|(_, _, current): (usize, SeriesId, SealedChunkKey)| {
                            key.sequence < current.sequence
                        })
                        .unwrap_or(true)
                    {
                        oldest = Some((shard_idx, *series_id, *key));
                    }
                }
            }
        }

        oldest
    }

    fn evict_oldest_persisted_sealed_chunk(self) -> bool {
        let Some((shard_idx, series_id, key)) = self.find_oldest_evictable_sealed_chunk() else {
            return false;
        };

        let mut sealed = self.persisted_sealed.sealed_chunks[shard_idx].write();
        let Some(chunks) = sealed.get_mut(&series_id) else {
            return false;
        };
        let removed_chunk = chunks.remove(&key);
        if chunks.is_empty() {
            sealed.remove(&series_id);
        }
        let removed = removed_chunk.is_some();
        if self.persisted_sealed.memory.accounting_enabled {
            if let Some(chunk) = removed_chunk.as_ref() {
                self.persisted_sealed.memory.account_memory_delta(
                    shard_idx,
                    MemoryDeltaBytes::from_totals(0, ChunkStorage::chunk_memory_usage_bytes(chunk)),
                );
            }
        }
        if removed {
            self.persisted_sealed
                .flush_metrics
                .evicted_sealed_chunks_total
                .fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    pub(in crate::engine::storage_engine) fn evict_persisted_sealed_chunks_to_budget_locked(
        self,
        budget: usize,
    ) {
        let mut used = self.used_value();
        while used > budget {
            if !self.evict_oldest_persisted_sealed_chunk() {
                break;
            }
            used = self.used_value();
        }
    }

    pub(in crate::engine::storage_engine) fn evict_persisted_sealed_chunks(self) -> usize {
        let persisted = self.persisted_sealed.persisted_chunk_watermarks.read();
        let persisted_index = self.persisted_sealed.persisted_index.read();
        let mut evicted = 0usize;

        for (shard_idx, shard) in self.persisted_sealed.sealed_chunks.iter().enumerate() {
            let mut sealed = shard.write();
            let mut removed_bytes = 0usize;
            sealed.retain(|series_id, chunks| {
                let persisted_sequence = persisted.get(series_id).copied().unwrap_or(0);
                let persisted_chunks = persisted_index
                    .chunk_refs
                    .get(series_id)
                    .map(|chunks| chunks.as_slice());

                chunks.retain(|key, chunk| {
                    let remove = key.sequence <= persisted_sequence
                        && Self::sealed_chunk_is_present_in_persisted_chunks(
                            persisted_chunks,
                            *key,
                            chunk,
                        );
                    if remove {
                        evicted = evicted.saturating_add(1);
                        if self.persisted_sealed.memory.accounting_enabled {
                            removed_bytes = removed_bytes
                                .saturating_add(ChunkStorage::chunk_memory_usage_bytes(chunk));
                        }
                    }
                    !remove
                });

                !chunks.is_empty()
            });
            if self.persisted_sealed.memory.accounting_enabled {
                self.persisted_sealed.memory.account_memory_delta(
                    shard_idx,
                    MemoryDeltaBytes::from_totals(0, removed_bytes),
                );
            }
        }

        evicted
    }
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn budget_enforcement_context(
        &self,
    ) -> BudgetEnforcementContext<'_> {
        BudgetEnforcementContext {
            budget_bytes: &self.memory.budget_bytes,
            used_bytes: &self.memory.used_bytes,
            persisted_sealed: self.persisted_sealed_budget_context(),
        }
    }
}
