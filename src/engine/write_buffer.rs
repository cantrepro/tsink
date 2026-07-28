//! In-memory write-buffer ownership lives here: active-head draining, finalized
//! chunk publication into sealed buffers, and the memory accounting that keeps
//! those transitions visible to the rest of the engine.

use super::MemoryDeltaBytes;
use super::*;
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Clone, Copy)]
enum ActiveFlushPolicy {
    All,
    BackgroundEligibleBounded,
    BackgroundBounded,
}

impl ChunkStorage {
    fn append_finalized_chunks_to_sealed_locked<I>(
        &self,
        sealed: &mut BTreeMap<SeriesId, SealedChunkSeriesMap>,
        pending_sealed_chunks: &RwLock<PendingSealedChunkIndex>,
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
            let pending_key = PendingSealedChunkIndexKey {
                wal_lowwater: chunk.wal_lowwater,
                sequence,
            };
            let pending_location = PendingSealedChunkLocation {
                shard_idx: Self::series_shard_idx(series_id),
                series_id,
                sealed_key: key,
                wal_lowwater: chunk.wal_lowwater,
                wal_highwater: chunk.wal_highwater,
                input_bytes: saturating_u64_from_usize(Self::chunk_memory_usage_bytes(&chunk)),
            };
            // Publish the pending locator before the sealed map entry while retaining the sealed
            // shard lock. A snapshot that observes the locator then blocks on this shard until
            // the chunk itself is visible; it can never advance past an unindexed sealed chunk.
            pending_sealed_chunks
                .write()
                .insert(pending_key, pending_location);
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
            &self.chunks.pending_sealed_chunks,
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
            .map(|_| ())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn flush_background_eligible_active(&self) -> Result<()> {
        self.flush_active_with_policy(ActiveFlushPolicy::BackgroundEligibleBounded)
            .map(|_| ())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn flush_background_bounded_active(&self) -> Result<()> {
        self.flush_active_with_policy(ActiveFlushPolicy::BackgroundBounded)
            .map(|_| ())
    }

    pub(super) fn flush_background_eligible_active_with_selection(
        &self,
    ) -> Result<MaintenancePassSelection> {
        self.flush_active_with_policy(ActiveFlushPolicy::BackgroundEligibleBounded)
    }

    pub(super) fn flush_background_bounded_active_with_selection(
        &self,
    ) -> Result<MaintenancePassSelection> {
        self.flush_active_with_policy(ActiveFlushPolicy::BackgroundBounded)
    }

    fn flush_background_bounded_active_pass(
        &self,
        include_current: bool,
        flushed_series: &mut usize,
        flushed_chunks: &mut usize,
        flushed_points: &mut usize,
    ) -> Result<MaintenancePassSelection> {
        let max_items = self.runtime.maintenance_max_items_per_pass;
        let max_bytes = self.runtime.maintenance_max_bytes_per_pass;
        if max_items == 0 || max_bytes == 0 {
            return Ok(MaintenancePassSelection::default());
        }

        let account_memory = self.memory.accounting_enabled;
        let mut cursor = self.chunks.background_active_flush_cursor.lock();
        cursor.shard_idx %= IN_MEMORY_SHARD_COUNT;
        let mut inspected = 0usize;
        let mut selected_bytes = 0u64;
        let mut byte_limit_skips = 0u64;
        let mut oversized_candidate_bytes = None;
        let mut completed_shards = 0usize;
        let include_underfilled_current =
            include_current && self.background_flush_requires_underfilled_current_heads();

        while inspected < max_items && completed_shards < IN_MEMORY_SHARD_COUNT {
            let shard_idx = cursor.shard_idx;
            let shard = &self.chunks.active_builders[shard_idx];
            let mut active = shard.write();
            let next_series_id = match cursor.after_series_id {
                Some(after) => active
                    .range((Excluded(after), Unbounded))
                    .next()
                    .map(|(series_id, _)| *series_id),
                None => active.keys().next().copied(),
            };

            let Some(series_id) = next_series_id else {
                cursor.shard_idx = (shard_idx + 1) % IN_MEMORY_SHARD_COUNT;
                cursor.after_series_id = None;
                completed_shards = completed_shards.saturating_add(1);
                continue;
            };

            cursor.after_series_id = Some(series_id);
            inspected = inspected.saturating_add(1);

            let state = active
                .get_mut(&series_id)
                .expect("range-selected active series must remain present under the write lock");
            let Some(candidate_bytes) = state
                .background_bounded_flush_input_bytes(include_current, include_underfilled_current)
            else {
                continue;
            };
            let candidate_bytes = u64::try_from(candidate_bytes).unwrap_or(u64::MAX);
            if candidate_bytes > max_bytes.saturating_sub(selected_bytes) {
                byte_limit_skips = byte_limit_skips.saturating_add(1);
                if selected_bytes == 0 {
                    oversized_candidate_bytes = Some(
                        oversized_candidate_bytes
                            .map_or(candidate_bytes, |current: u64| current.max(candidate_bytes)),
                    );
                }
                continue;
            }

            let state_bytes_before = if account_memory {
                Self::active_state_memory_usage_bytes(state)
            } else {
                0
            };
            let Some(chunk) = state
                .flush_background_bounded_partial(include_current, include_underfilled_current)?
            else {
                continue;
            };
            let mut shard_delta = MemoryDeltaBytes::default();
            if account_memory {
                shard_delta.record_change(
                    state_bytes_before,
                    Self::active_state_memory_usage_bytes(state),
                );
            }

            selected_bytes = selected_bytes.saturating_add(candidate_bytes);
            *flushed_series = flushed_series.saturating_add(1);
            *flushed_chunks = flushed_chunks.saturating_add(1);
            *flushed_points = flushed_points.saturating_add(chunk.header.point_count as usize);
            let wal_lowwater = chunk.wal_lowwater;

            let mut sealed = self.chunks.sealed_chunks[shard_idx].write();
            self.append_finalized_chunks_to_sealed_locked(
                &mut sealed,
                &self.chunks.pending_sealed_chunks,
                std::iter::once((series_id, chunk)),
                account_memory,
                &mut shard_delta,
            );
            drop(sealed);
            self.chunks.active_wal_index.lock().remove(wal_lowwater);
            if account_memory {
                self.account_memory_delta(shard_idx, shard_delta);
            }
        }

        self.observability
            .flush
            .active_flush_inspected_series_total
            .fetch_add(saturating_u64_from_usize(inspected), Ordering::Relaxed);
        self.observability
            .flush
            .active_flush_selected_input_bytes_total
            .fetch_add(selected_bytes, Ordering::Relaxed);
        self.observability
            .flush
            .active_flush_byte_limit_skips_total
            .fetch_add(byte_limit_skips, Ordering::Relaxed);
        if inspected == max_items {
            self.observability
                .flush
                .active_flush_item_limit_hits_total
                .fetch_add(1, Ordering::Relaxed);
        }

        if selected_bytes == 0 {
            if let Some(required) = oversized_candidate_bytes {
                return Err(TsinkError::MaintenanceWorkItemTooLarge {
                    operation: "active chunk finalization",
                    limit: max_bytes,
                    required,
                });
            }
        }

        Ok(MaintenancePassSelection {
            inspected_items: inspected,
            input_bytes: selected_bytes,
        })
    }

    fn background_flush_requires_underfilled_current_heads(&self) -> bool {
        // Without a WAL, periodic persistence is the only crash-survival path. Tiered writers
        // likewise need to publish even low-rate current heads for remote readers.
        if self.persisted.wal.is_none() || self.persisted.tiered_storage.is_some() {
            return true;
        }

        // Under real retained-memory pressure, prefer bounded fragmentation over rejecting work
        // while persistable current heads still occupy the constrained envelope.
        let budget = self.memory.budget_bytes.load(Ordering::Acquire);
        if budget != u64::MAX {
            let pressure_threshold = budget.saturating_sub(budget / 4);
            if self.memory.used_bytes.load(Ordering::Acquire) >= pressure_threshold {
                return true;
            }
        }

        // A finite WAL must be able to make progress before admission reaches its hard edge. The
        // timed pass remains item/byte bounded even when it temporarily admits young heads.
        let wal_limit = self.runtime.wal_size_limit_bytes;
        wal_limit != u64::MAX
            && self
                .persisted
                .wal
                .as_ref()
                .and_then(|wal| wal.total_size_bytes().ok())
                .is_some_and(|wal_bytes| wal_bytes >= wal_limit.saturating_sub(wal_limit / 4))
    }

    fn flush_active_with_policy(
        &self,
        policy: ActiveFlushPolicy,
    ) -> Result<MaintenancePassSelection> {
        self.observability
            .flush
            .active_flush_runs_total
            .fetch_add(1, Ordering::Relaxed);

        let mut flushed_series = 0usize;
        let mut flushed_chunks = 0usize;
        let mut flushed_points = 0usize;

        let result = (|| -> Result<MaintenancePassSelection> {
            if matches!(
                policy,
                ActiveFlushPolicy::BackgroundEligibleBounded | ActiveFlushPolicy::BackgroundBounded
            ) {
                return self.flush_background_bounded_active_pass(
                    matches!(policy, ActiveFlushPolicy::BackgroundBounded),
                    &mut flushed_series,
                    &mut flushed_chunks,
                    &mut flushed_points,
                );
            }

            let account_memory = self.memory.accounting_enabled;
            for (shard_idx, shard) in self.chunks.active_builders.iter().enumerate() {
                let mut active = shard.write();
                let mut shard_delta = MemoryDeltaBytes::default();
                let shard_result = (|| -> Result<()> {
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
                                ActiveFlushPolicy::BackgroundEligibleBounded
                                | ActiveFlushPolicy::BackgroundBounded => unreachable!(
                                    "bounded policies are handled by the cursor-driven pass"
                                ),
                            };
                            if account_memory {
                                let state_bytes_after =
                                    Self::active_state_memory_usage_bytes(state);
                                shard_delta.record_change(state_bytes_before, state_bytes_after);
                            }
                            let Some(chunk) = chunk else {
                                if state.is_empty() {
                                    break;
                                }
                                continue;
                            };

                            if !flushed_any_for_series {
                                flushed_series = flushed_series.saturating_add(1);
                                flushed_any_for_series = true;
                            }
                            flushed_chunks = flushed_chunks.saturating_add(1);
                            flushed_points =
                                flushed_points.saturating_add(chunk.header.point_count as usize);
                            let wal_lowwater = chunk.wal_lowwater;

                            let mut sealed = self.chunks.sealed_chunks[shard_idx].write();
                            self.append_finalized_chunks_to_sealed_locked(
                                &mut sealed,
                                &self.chunks.pending_sealed_chunks,
                                std::iter::once((*series_id, chunk)),
                                account_memory,
                                &mut shard_delta,
                            );
                            self.chunks.active_wal_index.lock().remove(wal_lowwater);
                        }
                    }
                    Ok(())
                })();
                if account_memory {
                    self.account_memory_delta(shard_idx, shard_delta);
                }
                shard_result?;
            }

            Ok(MaintenancePassSelection::default())
        })();

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

        match result {
            Ok(selection) => Ok(selection),
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn flush_all_active_accounts_empty_head_removal_and_continues_to_later_heads() {
        let temp_dir = TempDir::new().unwrap();
        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                partition_window: 10,
                max_active_partition_heads_per_series: 2,
                memory_budget_bytes: 32 * 1024 * 1024,
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();
        storage
            .insert_rows(&[
                Row::new("empty_oldest_head", DataPoint::new(1, 1.0)),
                Row::new("empty_oldest_head", DataPoint::new(2, 2.0)),
                Row::new("empty_oldest_head", DataPoint::new(11, 3.0)),
            ])
            .unwrap();
        let series_id = storage
            .catalog
            .registry
            .read()
            .resolve_existing("empty_oldest_head", &[])
            .unwrap()
            .series_id;
        let shard_idx = ChunkStorage::series_shard_idx(series_id);
        {
            let active = storage.chunks.active_builders[shard_idx].read();
            let state = active.get(&series_id).unwrap();
            assert_eq!(state.partition_head_count(), 2);
            assert_eq!(state.point_count(), 1);
        }
        assert_eq!(sealed_chunk_count(&storage), 1);

        storage.flush_all_active().unwrap();

        {
            let active = storage.chunks.active_builders[shard_idx].read();
            assert!(active.get(&series_id).unwrap().is_empty());
        }
        assert_eq!(
            sealed_chunk_count(&storage),
            2,
            "full flush must continue past an empty oldest head"
        );
        let incremental = storage.memory_observability_snapshot();
        let reconciled = storage.refresh_memory_usage();
        assert_eq!(incremental.budgeted_bytes, reconciled);
        assert_eq!(
            incremental.active_and_sealed_bytes,
            storage
                .memory_observability_snapshot()
                .active_and_sealed_bytes
        );

        let persisted = storage.persist_segment_with_outcome().unwrap();
        assert!(persisted.persisted);
        assert_eq!(persisted.chunks, 2);
        assert_eq!(sealed_chunk_count(&storage), 0);
        let incremental = storage.memory_observability_snapshot();
        let reconciled = storage.refresh_memory_usage();
        assert_eq!(incremental.budgeted_bytes, reconciled);
        assert_eq!(
            incremental.active_and_sealed_bytes,
            storage
                .memory_observability_snapshot()
                .active_and_sealed_bytes
        );
    }

    fn bounded_flush_test_storage(max_items: usize, max_bytes: u64) -> ChunkStorage {
        ChunkStorage::new_with_data_path_and_options(
            16,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                maintenance_max_items_per_pass: max_items,
                maintenance_max_bytes_per_pass: max_bytes,
                background_threads_enabled: false,
                background_fail_fast: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap()
    }

    fn sealed_chunk_count(storage: &ChunkStorage) -> usize {
        storage
            .chunks
            .sealed_chunks
            .iter()
            .map(|shard| {
                shard
                    .read()
                    .values()
                    .map(SealedChunkSeriesMap::len)
                    .sum::<usize>()
            })
            .sum()
    }

    fn active_wal_index_count(storage: &ChunkStorage) -> usize {
        storage
            .chunks
            .active_wal_index
            .lock()
            .lowwater_counts
            .values()
            .copied()
            .sum()
    }

    #[test]
    fn bounded_background_active_flush_resumes_fairly_at_item_limit() {
        let storage = bounded_flush_test_storage(1, 1024 * 1024);
        storage
            .insert_rows(&[
                Row::new("bounded_flush_a", DataPoint::new(1, 1.0)),
                Row::new("bounded_flush_b", DataPoint::new(1, 2.0)),
                Row::new("bounded_flush_c", DataPoint::new(1, 3.0)),
            ])
            .unwrap();
        assert_eq!(active_wal_index_count(&storage), 3);

        storage.flush_background_bounded_active().unwrap();
        assert_eq!(sealed_chunk_count(&storage), 1);
        assert_eq!(active_wal_index_count(&storage), 2);
        storage.flush_background_bounded_active().unwrap();
        assert_eq!(sealed_chunk_count(&storage), 2);
        assert_eq!(active_wal_index_count(&storage), 1);
        storage.flush_background_bounded_active().unwrap();
        assert_eq!(sealed_chunk_count(&storage), 3);
        assert_eq!(active_wal_index_count(&storage), 0);
        let flush = storage.observability_snapshot().flush;
        assert_eq!(flush.active_flush_inspected_series_total, 3);
        assert_eq!(flush.active_flush_item_limit_hits_total, 3);
        assert!(flush.active_flush_selected_input_bytes_total > 0);
    }

    #[test]
    fn bounded_background_active_flush_does_not_exceed_byte_limit() {
        let storage = bounded_flush_test_storage(8, 1);
        storage
            .insert_rows(&[Row::new("bounded_flush_bytes", DataPoint::new(1, 1.0))])
            .unwrap();

        let error = storage.flush_background_bounded_active().unwrap_err();
        assert!(matches!(
            error,
            TsinkError::MaintenanceWorkItemTooLarge {
                operation: "active chunk finalization",
                limit: 1,
                required,
            } if required > 1
        ));
        assert_eq!(sealed_chunk_count(&storage), 0);
        assert_eq!(active_wal_index_count(&storage), 1);
        assert_eq!(
            storage
                .observability_snapshot()
                .flush
                .active_flush_byte_limit_skips_total,
            1
        );
    }

    #[test]
    fn bounded_background_pipeline_does_not_reuse_byte_rejected_active_item_slot() {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = ChunkStorage::new_with_data_path_and_options(
            256,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                maintenance_max_items_per_pass: 8,
                maintenance_max_bytes_per_pass: u64::MAX,
                background_threads_enabled: false,
                background_fail_fast: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();

        storage
            .insert_rows(&[Row::new(
                "bounded_pipeline_presealed",
                DataPoint::new(1, 1.0),
            )])
            .unwrap();
        storage.flush_background_bounded_active().unwrap();
        let presealed_id = storage
            .catalog
            .registry
            .read()
            .resolve_existing("bounded_pipeline_presealed", &[])
            .unwrap()
            .series_id;
        storage.chunks.active_builders[ChunkStorage::series_shard_idx(presealed_id)]
            .write()
            .remove(&presealed_id);
        assert_eq!(sealed_chunk_count(&storage), 1);
        let presealed_input_bytes = storage
            .chunks
            .pending_sealed_chunks
            .read()
            .by_sequence
            .values()
            .next()
            .unwrap()
            .input_bytes;

        let active_names = ["bounded_pipeline_active_a", "bounded_pipeline_active_b"];
        storage
            .insert_rows(&[
                Row::new(active_names[0], DataPoint::new(1, 1.0)),
                Row::new(active_names[1], DataPoint::new(1, 2.0)),
            ])
            .unwrap();
        let mut active_series = active_names.map(|name| {
            let series_id = storage
                .catalog
                .registry
                .read()
                .resolve_existing(name, &[])
                .unwrap()
                .series_id;
            (ChunkStorage::series_shard_idx(series_id), series_id, name)
        });
        active_series.sort_by_key(|(shard_idx, series_id, _)| (*shard_idx, *series_id));
        let large_series_name = active_series[1].2;
        let large_rows = (2..130)
            .map(|timestamp| {
                Row::new(
                    large_series_name,
                    DataPoint::new(timestamp, timestamp as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&large_rows).unwrap();

        let active_input_bytes = |series_id| {
            let shard_idx = ChunkStorage::series_shard_idx(series_id);
            u64::try_from(
                storage.chunks.active_builders[shard_idx]
                    .read()
                    .get(&series_id)
                    .unwrap()
                    .background_bounded_flush_input_bytes(true, true)
                    .unwrap(),
            )
            .unwrap()
        };
        let first_active_input_bytes = active_input_bytes(active_series[0].1);
        let second_active_input_bytes = active_input_bytes(active_series[1].1);
        assert!(
            second_active_input_bytes > presealed_input_bytes,
            "the second active head must be rejected by the bytes left after the first"
        );

        storage.runtime.maintenance_max_items_per_pass = 2;
        storage.runtime.maintenance_max_bytes_per_pass =
            first_active_input_bytes.saturating_add(presealed_input_bytes);
        *storage.chunks.background_active_flush_cursor.lock() =
            BackgroundActiveFlushCursor::default();

        storage.background_flush_pipeline_once().unwrap();

        let flush = storage.observability_snapshot().flush;
        assert_eq!(flush.active_flush_inspected_series_total, 3);
        assert_eq!(flush.active_flushed_chunks_total, 2);
        assert_eq!(flush.active_flush_byte_limit_skips_total, 1);
        assert_eq!(
            sealed_chunk_count(&storage),
            2,
            "the finalized active head and preexisting sealed chunk must both remain buffered"
        );
        assert!(
            storage
                .persisted
                .persisted_index
                .read()
                .chunk_refs
                .is_empty(),
            "the byte-rejected active lookahead consumed the second item slot, so sealed persistence cannot reuse it in the same wake"
        );
    }

    #[test]
    fn bounded_background_active_flush_stops_after_one_catalog_cycle() {
        let storage = bounded_flush_test_storage(8, 1024 * 1024);
        storage
            .insert_rows(&[
                Row::new("bounded_cycle_a", DataPoint::new(1, 1.0)),
                Row::new("bounded_cycle_b", DataPoint::new(1, 2.0)),
                Row::new("bounded_cycle_c", DataPoint::new(1, 3.0)),
            ])
            .unwrap();

        storage.flush_background_bounded_active().unwrap();
        assert_eq!(sealed_chunk_count(&storage), 3);
        let flush = storage.observability_snapshot().flush;
        assert_eq!(flush.active_flush_inspected_series_total, 3);
        assert_eq!(flush.active_flush_item_limit_hits_total, 0);
    }

    #[test]
    fn active_wal_index_minimum_matches_a_full_multiset_scan() {
        let mut index = ActiveWalIndex::default();
        let mut model = Vec::new();
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;

        for step in 0..10_000usize {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            if model.is_empty() || seed & 3 != 0 {
                let lowwater = WalHighWatermark {
                    segment: (seed >> 48) & 7,
                    frame: (seed >> 16) & 255,
                };
                index.add(lowwater);
                model.push(lowwater);
            } else {
                let remove_idx = (seed as usize) % model.len();
                let lowwater = model.swap_remove(remove_idx);
                index.remove(lowwater);
            }

            assert_eq!(
                index.minimum(),
                model.iter().copied().min(),
                "active WAL index diverged at modeled operation {step}"
            );
        }

        for lowwater in model.drain(..) {
            index.remove(lowwater);
        }
        assert_eq!(index.minimum(), None);
    }
}
