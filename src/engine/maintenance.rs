use super::*;

impl ChunkStorage {
    pub(super) fn active_retention_cutoff(&self) -> Option<i64> {
        if !self.retention_enforced {
            return None;
        }
        let max_observed = self.max_observed_timestamp.load(Ordering::Acquire);
        if max_observed == i64::MIN {
            return None;
        }
        Some(max_observed.saturating_sub(self.retention_window))
    }

    pub(super) fn apply_retention_filter(&self, points: &mut Vec<DataPoint>) {
        let Some(cutoff) = self.active_retention_cutoff() else {
            return;
        };
        points.retain(|point| point.timestamp >= cutoff);
    }

    pub(super) fn memory_budget_value(&self) -> usize {
        self.memory_budget_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize
    }

    pub(super) fn memory_used_value(&self) -> usize {
        self.memory_used_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize
    }

    pub(super) fn cardinality_limit_value(&self) -> usize {
        self.cardinality_limit
    }

    pub(super) fn wal_size_limit_value(&self) -> u64 {
        self.wal_size_limit_bytes
    }

    pub(super) fn add_memory_usage_bytes(&self, shard_idx: usize, bytes: usize) {
        if !self.memory_accounting_enabled || bytes == 0 {
            return;
        }

        let increment = saturating_u64_from_usize(bytes);
        self.memory_used_bytes_by_shard[shard_idx].fetch_add(increment, Ordering::AcqRel);
        self.memory_used_bytes
            .fetch_add(increment, Ordering::AcqRel);
    }

    pub(super) fn sub_memory_usage_bytes(&self, shard_idx: usize, bytes: usize) {
        if !self.memory_accounting_enabled || bytes == 0 {
            return;
        }

        let decrement = saturating_u64_from_usize(bytes);
        self.memory_used_bytes_by_shard[shard_idx].fetch_sub(decrement, Ordering::AcqRel);
        self.memory_used_bytes
            .fetch_sub(decrement, Ordering::AcqRel);
    }

    pub(super) fn account_memory_delta_bytes(&self, shard_idx: usize, before: usize, after: usize) {
        if !self.memory_accounting_enabled {
            return;
        }
        if after >= before {
            self.add_memory_usage_bytes(shard_idx, after.saturating_sub(before));
        } else {
            self.sub_memory_usage_bytes(shard_idx, before.saturating_sub(after));
        }
    }

    pub(super) fn account_memory_delta_from_totals(
        &self,
        shard_idx: usize,
        added_bytes: usize,
        removed_bytes: usize,
    ) {
        if !self.memory_accounting_enabled {
            return;
        }
        if added_bytes >= removed_bytes {
            self.add_memory_usage_bytes(shard_idx, added_bytes.saturating_sub(removed_bytes));
        } else {
            self.sub_memory_usage_bytes(shard_idx, removed_bytes.saturating_sub(added_bytes));
        }
    }

    fn compute_shard_memory_usage_bytes(&self, shard_idx: usize) -> usize {
        let active = self.active_builders[shard_idx].read();
        let active_total = active.values().fold(0usize, |acc, state| {
            acc.saturating_add(Self::active_state_memory_usage_bytes_reconciled(state))
        });

        let sealed = self.sealed_chunks[shard_idx].read();
        let sealed_total = sealed.values().fold(0usize, |series_acc, chunks| {
            series_acc.saturating_add(chunks.values().fold(0usize, |chunk_acc, chunk| {
                chunk_acc.saturating_add(Self::chunk_memory_usage_bytes(chunk))
            }))
        });

        active_total.saturating_add(sealed_total)
    }

    pub(super) fn refresh_memory_usage(&self) -> usize {
        let mut used = 0usize;
        for shard_idx in 0..IN_MEMORY_SHARD_COUNT {
            let shard_used = self.compute_shard_memory_usage_bytes(shard_idx);
            self.memory_used_bytes_by_shard[shard_idx]
                .store(saturating_u64_from_usize(shard_used), Ordering::Release);
            used = used.saturating_add(shard_used);
        }
        self.memory_used_bytes
            .store(used.min(u64::MAX as usize) as u64, Ordering::Release);
        used
    }

    pub(super) fn active_state_memory_usage_bytes(state: &ActiveSeriesState) -> usize {
        std::mem::size_of::<ActiveSeriesState>()
            .saturating_add(
                state
                    .builder
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ChunkPoint>()),
            )
            .saturating_add(state.builder_value_heap_bytes)
    }

    fn active_state_memory_usage_bytes_reconciled(state: &ActiveSeriesState) -> usize {
        let mut bytes = std::mem::size_of::<ActiveSeriesState>().saturating_add(
            state
                .builder
                .capacity()
                .saturating_mul(std::mem::size_of::<ChunkPoint>()),
        );
        for point in state.builder.points() {
            bytes = bytes.saturating_add(value_heap_bytes(&point.value));
        }
        bytes
    }

    pub(super) fn chunk_memory_usage_bytes(chunk: &Chunk) -> usize {
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

    pub(super) fn prune_empty_active_series(&self) {
        if !self.memory_accounting_enabled {
            for shard in &self.active_builders {
                shard.write().retain(|_, state| !state.builder.is_empty());
            }
            return;
        }

        for (shard_idx, shard) in self.active_builders.iter().enumerate() {
            let mut active = shard.write();
            let mut removed_bytes = 0usize;
            active.retain(|_, state| {
                let keep = !state.builder.is_empty();
                if !keep {
                    removed_bytes =
                        removed_bytes.saturating_add(Self::active_state_memory_usage_bytes(state));
                }
                keep
            });
            self.sub_memory_usage_bytes(shard_idx, removed_bytes);
        }
    }

    pub(super) fn mark_persisted_chunk_watermarks(&self, watermarks: &HashMap<SeriesId, u64>) {
        if watermarks.is_empty() {
            return;
        }

        let mut persisted = self.persisted_chunk_watermarks.write();
        for (series_id, watermark) in watermarks {
            let entry = persisted.entry(*series_id).or_insert(0);
            *entry = (*entry).max(*watermark);
        }
    }

    pub(super) fn replace_registry_from_snapshot(&self, registry: SeriesRegistry) {
        *self.registry.write() = registry;
    }

    pub(super) fn persist_series_registry_index(&self) -> Result<()> {
        let Some(path) = &self.series_index_path else {
            return Ok(());
        };
        self.registry.read().persist_to_path(path)
    }

    fn reload_persisted_indexes_from_disk(&self) -> Result<()> {
        self.reload_persisted_indexes_from_disk_with_exclusions(None, None)
    }

    fn reload_persisted_indexes_from_disk_with_exclusions(
        &self,
        numeric_exclusions: Option<&HashSet<PathBuf>>,
        blob_exclusions: Option<&HashSet<PathBuf>>,
    ) -> Result<()> {
        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
            return Ok(());
        }

        let mut loaded_numeric = if let Some(path) = &self.numeric_lane_path {
            load_segment_indexes(path)?
        } else {
            crate::engine::segment::LoadedSegmentIndexes::default()
        };
        let mut loaded_blob = if let Some(path) = &self.blob_lane_path {
            load_segment_indexes(path)?
        } else {
            crate::engine::segment::LoadedSegmentIndexes::default()
        };

        if let Some(exclusions) = numeric_exclusions {
            if !exclusions.is_empty() {
                loaded_numeric
                    .indexed_segments
                    .retain(|segment| !exclusions.contains(&segment.root));
            }
        }
        if let Some(exclusions) = blob_exclusions {
            if !exclusions.is_empty() {
                loaded_blob
                    .indexed_segments
                    .retain(|segment| !exclusions.contains(&segment.root));
            }
        }

        let loaded_segments = bootstrap::merge_loaded_segment_indexes(
            loaded_numeric,
            loaded_blob,
            self.numeric_lane_path.is_some(),
            self.blob_lane_path.is_some(),
        )?;
        self.apply_loaded_segment_indexes(loaded_segments, false)?;
        self.persist_series_registry_index()
    }

    pub(super) fn refresh_persisted_indexes_and_evict_flushed_sealed_chunks(&self) -> Result<()> {
        let _visibility_guard = self.flush_visibility_lock.write();
        self.reload_persisted_indexes_from_disk()?;
        let evicted = self.evict_persisted_sealed_chunks();
        self.observability
            .flush
            .evicted_sealed_chunks_total
            .fetch_add(saturating_u64_from_usize(evicted), Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn sweep_expired_persisted_segments(&self) -> Result<usize> {
        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
            return Ok(0);
        }

        let Some(cutoff) = self.active_retention_cutoff() else {
            return Ok(0);
        };

        let _compaction_guard = self.compaction_lock.lock();
        let _visibility_guard = self.flush_visibility_lock.write();

        let mut expired_dirs = Vec::new();
        let mut expired_numeric_dirs = HashSet::new();
        let mut expired_blob_dirs = HashSet::new();
        if let Some(path) = &self.numeric_lane_path {
            let numeric_expired = collect_expired_segment_dirs(path, cutoff)?;
            expired_numeric_dirs.extend(numeric_expired.iter().cloned());
            expired_dirs.extend(numeric_expired);
        }
        if let Some(path) = &self.blob_lane_path {
            let blob_expired = collect_expired_segment_dirs(path, cutoff)?;
            expired_blob_dirs.extend(blob_expired.iter().cloned());
            expired_dirs.extend(blob_expired);
        }

        if expired_dirs.is_empty() {
            return Ok(0);
        }

        // Drop mmaps for just-expired segments while preserving retained persisted visibility.
        self.reload_persisted_indexes_from_disk_with_exclusions(
            Some(&expired_numeric_dirs),
            Some(&expired_blob_dirs),
        )?;
        self.evict_persisted_sealed_chunks();

        let mut removed = 0usize;
        for dir in expired_dirs {
            if crate::engine::fs_utils::remove_dir_if_exists(&dir).map_err(|source| {
                TsinkError::IoWithPath {
                    path: dir.clone(),
                    source,
                }
            })? {
                removed = removed.saturating_add(1);
            }
        }

        Ok(removed)
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

    fn find_oldest_evictable_sealed_chunk(&self) -> Option<(usize, SeriesId, SealedChunkKey)> {
        let persisted = self.persisted_chunk_watermarks.read();
        let persisted_index = self.persisted_index.read();
        let mut oldest: Option<(usize, SeriesId, SealedChunkKey)> = None;

        for (shard_idx, shard) in self.sealed_chunks.iter().enumerate() {
            let sealed = shard.read();
            for (series_id, chunks) in sealed.iter() {
                let persisted_sequence = persisted.get(series_id).copied().unwrap_or(0);
                let persisted_chunks = persisted_index
                    .chunk_refs
                    .get(series_id)
                    .map(|chunks| chunks.as_slice());
                for (key, chunk) in chunks {
                    if key.sequence > persisted_sequence {
                        continue;
                    }
                    if !Self::sealed_chunk_is_present_in_persisted_chunks(
                        persisted_chunks,
                        *key,
                        chunk,
                    ) {
                        continue;
                    }
                    let replace = oldest
                        .map(|(_, _, current)| key.sequence < current.sequence)
                        .unwrap_or(true);
                    if replace {
                        oldest = Some((shard_idx, *series_id, *key));
                    }
                }
            }
        }

        oldest
    }

    fn evict_oldest_persisted_sealed_chunk(&self) -> bool {
        let Some((shard_idx, series_id, key)) = self.find_oldest_evictable_sealed_chunk() else {
            return false;
        };

        let mut sealed = self.sealed_chunks[shard_idx].write();
        let Some(chunks) = sealed.get_mut(&series_id) else {
            return false;
        };
        let removed_chunk = chunks.remove(&key);
        if chunks.is_empty() {
            sealed.remove(&series_id);
        }
        let removed = removed_chunk.is_some();
        if self.memory_accounting_enabled {
            if let Some(chunk) = removed_chunk.as_ref() {
                self.sub_memory_usage_bytes(shard_idx, Self::chunk_memory_usage_bytes(chunk));
            }
        }

        if removed {
            self.observability
                .flush
                .evicted_sealed_chunks_total
                .fetch_add(1, Ordering::Relaxed);
        }

        removed
    }

    fn evict_persisted_sealed_chunks(&self) -> usize {
        let persisted = self.persisted_chunk_watermarks.read();
        let persisted_index = self.persisted_index.read();
        let mut evicted = 0usize;

        for (shard_idx, shard) in self.sealed_chunks.iter().enumerate() {
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
                        if self.memory_accounting_enabled {
                            removed_bytes =
                                removed_bytes.saturating_add(Self::chunk_memory_usage_bytes(chunk));
                        }
                    }
                    !remove
                });

                !chunks.is_empty()
            });
            if self.memory_accounting_enabled {
                self.sub_memory_usage_bytes(shard_idx, removed_bytes);
            }
        }

        evicted
    }

    pub(super) fn enforce_memory_budget_if_needed_with_writers_already_drained(
        &self,
    ) -> Result<()> {
        let budget = self.memory_budget_value();
        if budget == usize::MAX || self.memory_used_value() <= budget {
            return Ok(());
        }

        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
            return Ok(());
        }

        let _backpressure_guard = self.memory_backpressure_lock.lock();
        self.enforce_memory_budget_locked(budget)
    }

    pub(super) fn enforce_memory_budget_if_needed(&self) -> Result<()> {
        let budget = self.memory_budget_value();
        if budget == usize::MAX || self.memory_used_value() <= budget {
            return Ok(());
        }

        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
            return Ok(());
        }

        let _backpressure_guard = self.memory_backpressure_lock.lock();
        let used = self.memory_used_value();
        if used <= budget {
            return Ok(());
        }

        // Drain all writers before any flush/persist path that may reset or truncate WAL.
        let _write_permits = self.write_limiter.acquire_all(self.write_timeout)?;
        self.enforce_memory_budget_locked(budget)
    }

    fn enforce_memory_budget_locked(&self, budget: usize) -> Result<()> {
        let mut used = self.memory_used_value();
        if used <= budget {
            return Ok(());
        }

        self.flush_all_active()?;
        self.prune_empty_active_series();
        used = self.memory_used_value();
        if used <= budget {
            return Ok(());
        }

        if self.persist_segment(false)? {
            self.refresh_persisted_indexes_and_evict_flushed_sealed_chunks()?;
            used = self.memory_used_value();
        }

        while used > budget {
            if !self.evict_oldest_persisted_sealed_chunk() {
                break;
            }
            used = self.memory_used_value();
        }

        Ok(())
    }
}
