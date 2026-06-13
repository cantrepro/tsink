use super::*;

impl<'a> CatalogContext<'a> {
    fn lock_write_shards_for_rows(self, rows: &[Row]) -> Vec<MutexGuard<'a, ()>> {
        let mut shard_bits = [0u64; REGISTRY_TXN_SHARD_COUNT.div_ceil(u64::BITS as usize)];
        for row in rows {
            let shard_idx = ChunkStorage::registry_series_shard_idx(row.metric(), row.labels());
            let word_idx = shard_idx / (u64::BITS as usize);
            let bit_idx = shard_idx % (u64::BITS as usize);
            shard_bits[word_idx] |= 1u64 << bit_idx;
        }

        let guard_count = shard_bits
            .iter()
            .map(|bits| bits.count_ones() as usize)
            .sum();
        let mut guards = Vec::with_capacity(guard_count);
        for (word_idx, bits) in shard_bits.into_iter().enumerate() {
            let mut remaining = bits;
            while remaining != 0 {
                let bit_idx = remaining.trailing_zeros() as usize;
                let shard_idx = word_idx * (u64::BITS as usize) + bit_idx;
                guards.push(self.write_txn_shards[shard_idx].lock());
                remaining &= remaining - 1;
            }
        }
        guards
    }

    fn registry_series_shard_idx(metric: &str, labels: &[Label]) -> usize {
        (crate::label::stable_series_identity_hash(metric, labels) as usize)
            % REGISTRY_TXN_SHARD_COUNT
    }
}

impl<'a> ChunkContext<'a> {
    pub(in crate::engine::storage_engine) fn active_shard(
        self,
        series_id: SeriesId,
    ) -> &'a ActiveBuilderShard {
        &self.active_builders[Self::series_shard_idx(series_id)]
    }

    pub(in crate::engine::storage_engine) fn sealed_shard(
        self,
        series_id: SeriesId,
    ) -> &'a SealedChunkShard {
        &self.sealed_chunks[Self::series_shard_idx(series_id)]
    }

    pub(in crate::engine::storage_engine) fn series_shard_idx(series_id: SeriesId) -> usize {
        (series_id % IN_MEMORY_SHARD_COUNT as u64) as usize
    }
}

impl ChunkStorage {
    pub(super) fn series_shard_idx(series_id: SeriesId) -> usize {
        ChunkContext::series_shard_idx(series_id)
    }

    pub(super) fn registry_series_shard_idx(metric: &str, labels: &[Label]) -> usize {
        CatalogContext::registry_series_shard_idx(metric, labels)
    }

    pub(super) fn lock_registry_write_shards_for_rows<'a>(
        &'a self,
        rows: &[Row],
    ) -> Vec<MutexGuard<'a, ()>> {
        self.catalog_context().lock_write_shards_for_rows(rows)
    }

    pub(super) fn active_shard(&self, series_id: SeriesId) -> &ActiveBuilderShard {
        self.chunk_context().active_shard(series_id)
    }

    pub(super) fn sealed_shard(&self, series_id: SeriesId) -> &SealedChunkShard {
        self.chunk_context().sealed_shard(series_id)
    }
}
