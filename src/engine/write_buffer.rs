//! In-memory write-buffer ownership lives here: active-head draining, finalized
//! chunk publication into sealed buffers, and the memory accounting that keeps
//! those transitions visible to the rest of the engine.

use super::MemoryDeltaBytes;
use super::*;

#[derive(Clone, Copy)]
enum ActiveFlushPolicy {
    All,
    BackgroundEligible,
    BackgroundBounded,
}

impl ChunkStorage {
    fn append_finalized_chunks_to_sealed_locked<I>(
        &self,
        sealed: &mut HashMap<SeriesId, SealedChunkSeriesMap>,
        finalized: I,
        account_memory: bool,
        memory_delta: &mut MemoryDeltaBytes,
    ) where
        I: IntoIterator<Item = (SeriesId, Chunk)>,
    {
        for (series_id, chunk) in finalized {
            let chunk = chunk.into_sealed_storage();
            let chunk_bytes = if account_memory {
                Self::chunk_memory_usage_bytes(&chunk)
            } else {
                0
            };
            let sequence = self
                .chunks
                .next_chunk_sequence
                .fetch_add(1, Ordering::SeqCst);
            let key = SealedChunkKey::from_chunk(&chunk, sequence);
            let replaced = sealed
                .entry(series_id)
                .or_default()
                .insert(key, Arc::new(chunk));
            if account_memory {
                memory_delta.record_replacement(chunk_bytes, replaced.as_ref(), |chunk| {
                    Self::chunk_memory_usage_bytes(chunk)
                });
            }
        }
    }

    pub(super) fn append_finalized_chunks_to_sealed_shard<I>(&self, shard_idx: usize, finalized: I)
    where
        I: IntoIterator<Item = (SeriesId, Chunk)>,
    {
        let account_memory = self.memory.accounting_enabled;
        let mut sealed = self.chunks.sealed_chunks[shard_idx].write();
        let mut memory_delta = MemoryDeltaBytes::default();
        self.append_finalized_chunks_to_sealed_locked(
            &mut sealed,
            finalized,
            account_memory,
            &mut memory_delta,
        );
        drop(sealed);
        if account_memory {
            self.account_memory_delta(shard_idx, memory_delta);
        }
    }

    #[allow(dead_code)]
    pub(super) fn append_sealed_chunk(&self, series_id: SeriesId, chunk: Chunk) {
        let shard_idx = Self::series_shard_idx(series_id);
        self.append_finalized_chunks_to_sealed_shard(
            shard_idx,
            std::iter::once((series_id, chunk)),
        );
        let _ = self.refresh_series_visible_timestamp_cache_locked(std::iter::once(series_id));
        let inserted_series_ids = self.mark_materialized_series_ids(std::iter::once(series_id));
        if !inserted_series_ids.is_empty() {
            self.runtime_metadata_delta_write_context()
                .reconcile_series_ids(inserted_series_ids.iter().copied());
            self.metadata_shard_publication_context()
                .publish_materialized_series_ids(inserted_series_ids.iter().copied());
        }
    }

    pub(super) fn flush_all_active(&self) -> Result<()> {
        self.flush_active_with_policy(ActiveFlushPolicy::All)
    }

    pub(super) fn flush_background_eligible_active(&self) -> Result<()> {
        self.flush_active_with_policy(ActiveFlushPolicy::BackgroundEligible)
    }

    pub(super) fn flush_background_bounded_active(&self) -> Result<()> {
        self.flush_active_with_policy(ActiveFlushPolicy::BackgroundBounded)
    }

    fn flush_active_with_policy(&self, policy: ActiveFlushPolicy) -> Result<()> {
        self.observability
            .flush
            .active_flush_runs_total
            .fetch_add(1, Ordering::Relaxed);

        let mut flushed_series = 0usize;
        let mut flushed_chunks = 0usize;
        let mut flushed_points = 0usize;

        let result = (|| -> Result<()> {
            let account_memory = self.memory.accounting_enabled;
            for (shard_idx, shard) in self.chunks.active_builders.iter().enumerate() {
                let mut active = shard.write();
                let mut shard_delta = MemoryDeltaBytes::default();
                for (series_id, state) in active.iter_mut() {
                    let mut flushed_any_for_series = false;
                    loop {
                        let state_bytes_before = if account_memory {
                            Self::active_state_memory_usage_bytes(state)
                        } else {
                            0
                        };
                        let chunk = match policy {
                            ActiveFlushPolicy::All => state.flush_partial()?,
                            ActiveFlushPolicy::BackgroundEligible => {
                                state.flush_background_eligible_partial()?
                            }
                            ActiveFlushPolicy::BackgroundBounded => {
                                let chunk = state.flush_background_eligible_partial()?;
                                if chunk.is_some() {
                                    chunk
                                } else {
                                    state.flush_current_partial()?
                                }
                            }
                        };
                        let Some(chunk) = chunk else {
                            break;
                        };
                        if account_memory {
                            let state_bytes_after = Self::active_state_memory_usage_bytes(state);
                            shard_delta.record_change(state_bytes_before, state_bytes_after);
                        }

                        if !flushed_any_for_series {
                            flushed_series = flushed_series.saturating_add(1);
                            flushed_any_for_series = true;
                        }
                        flushed_chunks = flushed_chunks.saturating_add(1);
                        flushed_points =
                            flushed_points.saturating_add(chunk.header.point_count as usize);

                        let mut sealed = self.chunks.sealed_chunks[shard_idx].write();
                        self.append_finalized_chunks_to_sealed_locked(
                            &mut sealed,
                            std::iter::once((*series_id, chunk)),
                            account_memory,
                            &mut shard_delta,
                        );
                    }
                }
                if account_memory {
                    self.account_memory_delta(shard_idx, shard_delta);
                }
            }

            Ok(())
        })();

        match result {
            Ok(()) => {
                self.observability
                    .flush
                    .active_flushed_series_total
                    .fetch_add(saturating_u64_from_usize(flushed_series), Ordering::Relaxed);
                self.observability
                    .flush
                    .active_flushed_chunks_total
                    .fetch_add(saturating_u64_from_usize(flushed_chunks), Ordering::Relaxed);
                self.observability
                    .flush
                    .active_flushed_points_total
                    .fetch_add(saturating_u64_from_usize(flushed_points), Ordering::Relaxed);
                Ok(())
            }
            Err(err) => {
                self.observability
                    .flush
                    .active_flush_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }
}
