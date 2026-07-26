use super::*;

struct VisibilityRefreshQueryBudget<'a> {
    execution: &'a QueryExecution,
    retained_staging_bytes: u64,
    active_tombstone_union_bytes: u64,
    reservation: &'a mut crate::QueryMemoryReservation,
}

impl VisibilityRefreshQueryBudget<'_> {
    fn checkpoint(&self) -> Result<()> {
        self.execution.checkpoint().map_err(Into::into)
    }

    fn admit_tombstone_union(&mut self, range_count: usize) -> Result<()> {
        self.execution
            .observe_intermediate_vector_size(u64::try_from(range_count).unwrap_or(u64::MAX))?;
        self.active_tombstone_union_bytes =
            super::super::super::query_exec::modeled_vec_capacity_bytes::<tombstone::TombstoneRange>(
                range_count,
            );
        let required = self
            .retained_staging_bytes
            .saturating_add(self.active_tombstone_union_bytes);
        if required > self.reservation.bytes() {
            self.reservation.resize(required)?;
        }
        Ok(())
    }

    fn admit_actual_range_rebuild(&mut self, raw_ranges: usize) -> Result<()> {
        self.execution
            .observe_intermediate_vector_size(u64::try_from(raw_ranges).unwrap_or(u64::MAX))?;
        // The raw source vector and normalization output coexist. A fixed allowance also covers
        // the bounded retained-range compaction vector and allocator slack.
        let scratch_bytes = u64::try_from(raw_ranges)
            .unwrap_or(u64::MAX)
            .saturating_mul(
                u64::try_from(std::mem::size_of::<SeriesVisibilityRangeSummary>())
                    .unwrap_or(u64::MAX),
            )
            .saturating_mul(2)
            .saturating_add(4096);
        let required = self
            .retained_staging_bytes
            .saturating_add(self.active_tombstone_union_bytes)
            .saturating_add(scratch_bytes);
        if required > self.reservation.bytes() {
            self.reservation.resize(required)?;
        }
        Ok(())
    }

    fn finish_tombstone_union(&mut self) {
        self.active_tombstone_union_bytes = 0;
    }
}

impl ChunkStorage {
    fn visibility_query_id_vector_bytes(series_count: u64, copies: u64) -> u64 {
        let series_count = usize::try_from(series_count).unwrap_or(usize::MAX);
        crate::engine::storage_engine::query_exec::modeled_vec_capacity_bytes::<SeriesId>(
            crate::engine::storage_engine::query_exec::modeled_vec_growth_capacity_upper(
                series_count,
            ),
        )
        .saturating_mul(copies)
    }

    pub(in crate::engine::storage_engine) fn missing_visibility_summary_series_ids<I>(
        &self,
        series_ids: I,
    ) -> Vec<SeriesId>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        self.with_series_visibility_summaries(|summaries| {
            series_ids
                .into_iter()
                .filter(|series_id| !summaries.contains_key(series_id))
                .collect()
        })
    }

    pub(in crate::engine::storage_engine) fn partition_series_by_retention<I>(
        &self,
        series_ids: I,
        retention_cutoff: i64,
    ) -> (Vec<SeriesId>, Vec<SeriesId>)
    where
        I: IntoIterator<Item = SeriesId>,
    {
        self.with_series_visibility_summaries(|summaries| {
            let mut live = Vec::new();
            let mut dead = Vec::new();
            for series_id in series_ids {
                if summaries
                    .get(&series_id)
                    .and_then(|summary| summary.latest_visible_timestamp)
                    .is_some_and(|latest| latest >= retention_cutoff)
                {
                    live.push(series_id);
                } else {
                    dead.push(series_id);
                }
            }
            (live, dead)
        })
    }

    fn partition_series_by_retention_for_query<I>(
        &self,
        series_ids: I,
        retention_cutoff: i64,
        execution: &QueryExecution,
    ) -> Result<(Vec<SeriesId>, Vec<SeriesId>)>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        self.with_series_visibility_summaries(|summaries| {
            let mut live = Vec::new();
            let mut dead = Vec::new();
            for series_id in series_ids {
                execution.checkpoint()?;
                if summaries
                    .get(&series_id)
                    .and_then(|summary| summary.latest_visible_timestamp)
                    .is_some_and(|latest| latest >= retention_cutoff)
                {
                    live.push(series_id);
                } else {
                    dead.push(series_id);
                }
            }
            Ok((live, dead))
        })
    }

    pub(in crate::engine::storage_engine) fn series_visibility_summary_payload_bytes(
        summary: &SeriesVisibilitySummary,
    ) -> usize {
        summary
            .ranges
            .capacity()
            .saturating_mul(std::mem::size_of::<SeriesVisibilityRangeSummary>())
    }

    pub(in crate::engine::storage_engine) fn series_visibility_state_map_memory_usage_bytes(
        summaries: &HashMap<SeriesId, SeriesVisibilitySummary>,
        cache: &HashMap<SeriesId, Option<i64>>,
        bounded_cache: &HashMap<SeriesId, Option<i64>>,
        epochs: &HashMap<SeriesId, u64>,
    ) -> usize {
        Self::hash_map_memory_usage_bytes(summaries)
            .saturating_add(Self::hash_map_memory_usage_bytes(cache))
            .saturating_add(Self::hash_map_memory_usage_bytes(bounded_cache))
            .saturating_add(Self::hash_map_memory_usage_bytes(epochs))
    }

    pub(in crate::engine::storage_engine) fn series_visibility_state_memory_usage_bytes(
        summaries: &HashMap<SeriesId, SeriesVisibilitySummary>,
        cache: &HashMap<SeriesId, Option<i64>>,
        bounded_cache: &HashMap<SeriesId, Option<i64>>,
        epochs: &HashMap<SeriesId, u64>,
    ) -> usize {
        Self::series_visibility_state_map_memory_usage_bytes(
            summaries,
            cache,
            bounded_cache,
            epochs,
        )
        .saturating_add(summaries.values().fold(0usize, |bytes, summary| {
            bytes.saturating_add(Self::series_visibility_summary_payload_bytes(summary))
        }))
    }

    fn normalize_series_visibility_ranges(ranges: &mut Vec<SeriesVisibilityRangeSummary>) {
        if ranges.len() <= 1 {
            return;
        }
        ranges.sort_by_key(|range| (range.min_ts, range.max_ts, !range.exact));
        let mut merged = Vec::<SeriesVisibilityRangeSummary>::with_capacity(ranges.len());
        for range in ranges.drain(..) {
            if let Some(current) = merged.last_mut() {
                if range.min_ts <= current.max_ts.saturating_add(1) {
                    current.max_ts = current.max_ts.max(range.max_ts);
                    current.exact &= range.exact;
                    continue;
                }
            }
            merged.push(range);
        }
        *ranges = merged;
    }

    fn single_timestamp_visibility_range(
        timestamp: i64,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
    ) -> Option<SeriesVisibilityRangeSummary> {
        Self::timestamp_survives_tombstones(timestamp, tombstone_ranges).then_some(
            SeriesVisibilityRangeSummary {
                min_ts: timestamp,
                max_ts: timestamp,
                exact: true,
            },
        )
    }

    fn chunk_bounds_visibility_range(
        min_ts: i64,
        max_ts: i64,
        point_count: u16,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
    ) -> Option<SeriesVisibilityRangeSummary> {
        if point_count == 0 || min_ts > max_ts {
            return None;
        }
        if tombstone_ranges
            .is_some_and(|ranges| tombstone::interval_fully_tombstoned(min_ts, max_ts, ranges))
        {
            return None;
        }
        if point_count == 1 {
            return Self::single_timestamp_visibility_range(max_ts, tombstone_ranges);
        }
        Some(SeriesVisibilityRangeSummary {
            min_ts,
            max_ts,
            exact: false,
        })
    }

    fn rebuild_series_visibility_summary_locked(
        &self,
        series_id: SeriesId,
        persisted_index: &PersistedIndexState,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
        bounded_cutoff: i64,
        mut query_budget: Option<&mut VisibilityRefreshQueryBudget<'_>>,
    ) -> Result<SeriesVisibilitySummary> {
        if let Some(query_budget) = query_budget.as_deref() {
            query_budget.checkpoint()?;
        }
        let latest_visible = self.latest_visible_timestamp_for_series_locked(
            series_id,
            persisted_index,
            tombstone_ranges,
        )?;
        let latest_bounded_visible = match latest_visible {
            Some(latest) if latest <= bounded_cutoff => Some(latest),
            Some(_) => self.latest_visible_bounded_timestamp_for_series_locked(
                series_id,
                persisted_index,
                tombstone_ranges,
                bounded_cutoff,
            )?,
            None => None,
        };
        if let Some(query_budget) = query_budget.as_deref() {
            query_budget.checkpoint()?;
        }

        // Ingest does not take the visibility fence. Hold both per-series shard guards while
        // recounting, admitting, allocating, and traversing so a post-preflight write cannot
        // make the actual range vector larger than its query reservation.
        let active = self.active_shard(series_id).read();
        let sealed = self.sealed_shard(series_id).read();
        let active_ranges = if let Some(state) = active.get(&series_id) {
            let mut count = 0usize;
            for _ in state.points_in_partition_order() {
                if let Some(query_budget) = query_budget.as_deref() {
                    query_budget.checkpoint()?;
                }
                count = count.saturating_add(1);
            }
            count
        } else {
            0
        };
        let sealed_ranges = sealed.get(&series_id).map_or(0, BTreeMap::len);
        let persisted_ranges = persisted_index
            .chunk_refs
            .get(&series_id)
            .map_or(0, Vec::len);
        let raw_ranges = active_ranges
            .saturating_add(sealed_ranges)
            .saturating_add(persisted_ranges);
        if let Some(query_budget) = query_budget.as_deref_mut() {
            query_budget.admit_actual_range_rebuild(raw_ranges)?;
        }

        let mut ranges = Vec::with_capacity(raw_ranges);
        if let Some(state) = active.get(&series_id) {
            for point in state.points_in_partition_order() {
                if let Some(query_budget) = query_budget.as_deref() {
                    query_budget.checkpoint()?;
                }
                if let Some(range) =
                    Self::single_timestamp_visibility_range(point.ts, tombstone_ranges)
                {
                    ranges.push(range);
                }
            }
        }
        if let Some(chunks) = sealed.get(&series_id) {
            for chunk in chunks.values() {
                if let Some(query_budget) = query_budget.as_deref() {
                    query_budget.checkpoint()?;
                }
                if let Some(range) = Self::chunk_bounds_visibility_range(
                    chunk.header.min_ts,
                    chunk.header.max_ts,
                    chunk.header.point_count,
                    tombstone_ranges,
                ) {
                    ranges.push(range);
                }
            }
        }
        if let Some(chunks) = persisted_index.chunk_refs.get(&series_id) {
            for chunk_ref in chunks {
                if let Some(query_budget) = query_budget.as_deref() {
                    query_budget.checkpoint()?;
                }
                if let Some(range) = Self::chunk_bounds_visibility_range(
                    chunk_ref.min_ts,
                    chunk_ref.max_ts,
                    chunk_ref.point_count,
                    tombstone_ranges,
                ) {
                    ranges.push(range);
                }
            }
        }
        drop(sealed);
        drop(active);

        if let Some(query_budget) = query_budget.as_deref() {
            query_budget.checkpoint()?;
        }
        Self::normalize_series_visibility_ranges(&mut ranges);
        if let Some(query_budget) = query_budget.as_deref() {
            query_budget.checkpoint()?;
        }
        let truncated_before_floor = ranges.len() > SERIES_VISIBILITY_SUMMARY_MAX_RANGES;
        if truncated_before_floor {
            let drop_count = ranges
                .len()
                .saturating_sub(SERIES_VISIBILITY_SUMMARY_MAX_RANGES);
            ranges.drain(..drop_count);
        }

        // Normalization deliberately allocates a full-size merge buffer. Compact the retained
        // summary afterward so every update/map entry is bounded by the documented range cap
        // rather than retaining capacity proportional to the raw source history.
        let mut retained_ranges = Vec::with_capacity(ranges.len());
        retained_ranges.extend(ranges);
        Ok(SeriesVisibilitySummary {
            latest_visible_timestamp: latest_visible,
            latest_bounded_visible_timestamp: latest_bounded_visible,
            exhaustive_floor_inclusive: retained_ranges.first().map(|range| range.min_ts),
            truncated_before_floor,
            ranges: retained_ranges,
        })
    }

    fn replace_series_visible_timestamp_cache_entries(
        &self,
        updates: Vec<(SeriesId, SeriesVisibilitySummary)>,
    ) {
        // The caller holds recency_state_lock from before it snapshots active/sealed state. Ingest
        // publishes points before taking that lock to merge timestamps, so it either precedes
        // this rebuild and is included or follows this replacement and merges afterward.
        let mut next_bounded = i64::MIN;
        let mut summaries = self.visibility.series_visibility_summaries.write();
        let mut cache = self.visibility.series_visible_max_timestamps.write();
        let mut bounded_cache = self
            .visibility
            .series_visible_bounded_max_timestamps
            .write();
        let mut epochs = self.visibility.series_visibility_cache_epochs.write();
        let current_epoch = self.remote_tombstone_epoch();
        let map_bytes_before = Self::series_visibility_state_map_memory_usage_bytes(
            &summaries,
            &cache,
            &bounded_cache,
            &epochs,
        );
        let mut payload_bytes_before = 0usize;
        let mut payload_bytes_after = 0usize;
        for (series_id, summary) in updates {
            #[cfg(test)]
            self.visibility
                .visibility_cache_accounting_entries_visited
                .fetch_add(1, Ordering::AcqRel);
            let latest = summary.latest_visible_timestamp;
            let latest_bounded = summary.latest_bounded_visible_timestamp;
            payload_bytes_before = payload_bytes_before.saturating_add(
                summaries
                    .get(&series_id)
                    .map_or(0, Self::series_visibility_summary_payload_bytes),
            );
            payload_bytes_after = payload_bytes_after
                .saturating_add(Self::series_visibility_summary_payload_bytes(&summary));
            summaries.insert(series_id, summary);
            cache.insert(series_id, latest);
            bounded_cache.insert(series_id, latest_bounded);
            epochs.insert(series_id, current_epoch);
            if let Some(latest_bounded) = latest_bounded {
                next_bounded = next_bounded.max(latest_bounded);
            }
        }
        let map_bytes_after = Self::series_visibility_state_map_memory_usage_bytes(
            &summaries,
            &cache,
            &bounded_cache,
            &epochs,
        );
        self.account_included_memory_component_delta_bytes(
            &self.memory.metadata_used_bytes,
            map_bytes_before.saturating_add(payload_bytes_before),
            map_bytes_after.saturating_add(payload_bytes_after),
        );

        drop(summaries);
        drop(cache);

        if next_bounded != i64::MIN {
            self.visibility
                .max_bounded_observed_timestamp
                .fetch_max(next_bounded, Ordering::AcqRel);
        }
        self.bump_live_series_pruning_generation();
    }

    pub(in crate::engine::storage_engine) fn clear_series_visible_timestamp_cache<I>(
        &self,
        series_ids: I,
    ) where
        I: IntoIterator<Item = SeriesId>,
    {
        let _recency_guard = self.visibility.recency_state_lock.lock();
        let mut summaries = self.visibility.series_visibility_summaries.write();
        let mut cache = self.visibility.series_visible_max_timestamps.write();
        let mut bounded_cache = self
            .visibility
            .series_visible_bounded_max_timestamps
            .write();
        let mut epochs = self.visibility.series_visibility_cache_epochs.write();
        let map_bytes_before = Self::series_visibility_state_map_memory_usage_bytes(
            &summaries,
            &cache,
            &bounded_cache,
            &epochs,
        );
        let mut payload_bytes_before = 0usize;
        for series_id in series_ids {
            #[cfg(test)]
            self.visibility
                .visibility_cache_accounting_entries_visited
                .fetch_add(1, Ordering::AcqRel);
            if let Some(summary) = summaries.remove(&series_id) {
                payload_bytes_before = payload_bytes_before
                    .saturating_add(Self::series_visibility_summary_payload_bytes(&summary));
            }
            cache.remove(&series_id);
            epochs.remove(&series_id);
            bounded_cache.remove(&series_id);
        }
        let map_bytes_after = Self::series_visibility_state_map_memory_usage_bytes(
            &summaries,
            &cache,
            &bounded_cache,
            &epochs,
        );
        self.account_included_memory_component_delta_bytes(
            &self.memory.metadata_used_bytes,
            map_bytes_before.saturating_add(payload_bytes_before),
            map_bytes_after,
        );

        drop(summaries);
        drop(cache);

        // This aggregate is deliberately a monotonic upper bound. Keeping a removed or
        // epoch-stale bounded timestamp can temporarily reject additional old writes, but it
        // cannot admit a write that violates retention. Recomputing it here would turn a
        // one-series cache mutation into an unbounded scan of stale physical cache entries.
        self.bump_live_series_pruning_generation();
    }

    pub(in crate::engine::storage_engine) fn refresh_series_visible_timestamp_cache<I>(
        &self,
        series_ids: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let _visibility_guard = self.visibility_read_fence();
        self.refresh_series_visible_timestamp_cache_locked(series_ids)
    }

    pub(in crate::engine::storage_engine) fn series_visibility_refresh_staging_upper_bound<'a>(
        &self,
        series_ids: impl IntoIterator<Item = &'a SeriesId>,
    ) -> usize {
        self.series_visibility_refresh_staging_upper_bound_impl(series_ids, None, true)
            .expect("visibility refresh staging without a query execution cannot fail")
            .0
    }

    fn series_visibility_refresh_staging_upper_bound_impl<'a>(
        &self,
        series_ids: impl IntoIterator<Item = &'a SeriesId>,
        execution: Option<&QueryExecution>,
        include_id_vectors: bool,
    ) -> Result<(usize, usize, usize)> {
        let persisted_index = self.persisted.persisted_index.read();
        let local_tombstones = self.visibility.tombstones.read();
        let remote_tombstones = self.visibility.remote_tombstones.read();
        let mut changed_count = 0usize;
        let mut retained_updates = 0usize;
        let mut largest_series_rebuild = 0usize;
        let mut largest_intermediate_vector = 0usize;
        for &series_id in series_ids {
            if let Some(execution) = execution {
                execution.checkpoint()?;
            }
            changed_count = changed_count.saturating_add(1);
            let active_ranges = {
                let active = self.active_shard(series_id).read();
                if let Some(state) = active.get(&series_id) {
                    if let Some(execution) = execution {
                        let mut count = 0usize;
                        for _ in state.points_in_partition_order() {
                            execution.checkpoint()?;
                            count = count.saturating_add(1);
                        }
                        count
                    } else {
                        let count = state.point_count();
                        #[cfg(test)]
                        assert_eq!(
                            count,
                            state.points_in_partition_order().count(),
                            "constant-time active-point accounting must match traversal",
                        );
                        count
                    }
                } else {
                    0
                }
            };
            let sealed_ranges = {
                let sealed = self.sealed_shard(series_id).read();
                if let Some(chunks) = sealed.get(&series_id) {
                    let mut count = 0usize;
                    for _ in chunks {
                        if let Some(execution) = execution {
                            execution.checkpoint()?;
                        }
                        count = count.saturating_add(1);
                    }
                    count
                } else {
                    0
                }
            };
            let persisted_ranges = if let Some(chunks) = persisted_index.chunk_refs.get(&series_id)
            {
                let mut count = 0usize;
                for _ in chunks {
                    if let Some(execution) = execution {
                        execution.checkpoint()?;
                    }
                    count = count.saturating_add(1);
                }
                count
            } else {
                0
            };
            let raw_ranges = active_ranges
                .saturating_add(sealed_ranges)
                .saturating_add(persisted_ranges);
            let tombstone_union_ranges = match (
                local_tombstones.get(&series_id),
                remote_tombstones.ranges(series_id),
            ) {
                (Some(local), Some(remote)) => local.len().saturating_add(remote.len()),
                _ => 0,
            };
            largest_intermediate_vector = largest_intermediate_vector
                .max(raw_ranges)
                .max(tombstone_union_ranges);
            // Normalization can hold the source and merged vectors together. The retained
            // summary is capped, but the source scan is not, so charge its exact item count.
            largest_series_rebuild = largest_series_rebuild.max(
                raw_ranges
                    .saturating_mul(std::mem::size_of::<SeriesVisibilityRangeSummary>())
                    .saturating_mul(2)
                    .saturating_add(
                        tombstone_union_ranges
                            .saturating_mul(std::mem::size_of::<tombstone::TombstoneRange>()),
                    )
                    .saturating_add(4096),
            );
            retained_updates = retained_updates.saturating_add(
                std::mem::size_of::<(SeriesId, SeriesVisibilitySummary)>().saturating_add(
                    SERIES_VISIBILITY_SUMMARY_MAX_RANGES
                        .saturating_mul(std::mem::size_of::<SeriesVisibilityRangeSummary>()),
                ),
            );
        }

        let id_vector = changed_count.saturating_mul(std::mem::size_of::<SeriesId>());
        largest_intermediate_vector = largest_intermediate_vector.max(changed_count);
        // Cache hash-map growth can retain predecessor allocations while the update vector and
        // one per-series rebuild remain live. Four retained-update copies conservatively cover
        // the three cache maps and allocator growth.
        let retained_staging_bytes = usize::from(include_id_vectors)
            .saturating_mul(id_vector.saturating_mul(2))
            .saturating_add(retained_updates.saturating_mul(4))
            .saturating_add(4096);
        let bytes = retained_staging_bytes.saturating_add(largest_series_rebuild);
        Ok((bytes, retained_staging_bytes, largest_intermediate_vector))
    }

    pub(in crate::engine::storage_engine) fn refresh_series_visible_timestamp_cache_for_query(
        &self,
        series_ids: Vec<SeriesId>,
        execution: &QueryExecution,
    ) -> Result<()> {
        if series_ids.is_empty() {
            return Ok(());
        }

        let _visibility_guard = self.visibility_read_fence();
        // list_metrics has already reserved its source, missing-ID, and retention-partition
        // vectors. Admit only the additional update/range/cache staging here so the same IDs are
        // not charged twice.
        let (staging_bytes, retained_staging_bytes, largest_intermediate_vector) = self
            .series_visibility_refresh_staging_upper_bound_impl(
                series_ids.iter(),
                Some(execution),
                false,
            )?;
        execution.observe_intermediate_vector_size(
            u64::try_from(largest_intermediate_vector).unwrap_or(u64::MAX),
        )?;
        let mut reservation =
            execution.reserve_memory(u64::try_from(staging_bytes).unwrap_or(u64::MAX))?;
        execution.checkpoint()?;

        #[cfg(test)]
        self.invoke_metadata_visibility_refresh_post_preflight_hook();

        self.refresh_series_visible_timestamp_cache_locked_impl(
            series_ids,
            Some(&mut VisibilityRefreshQueryBudget {
                execution,
                retained_staging_bytes: u64::try_from(retained_staging_bytes).unwrap_or(u64::MAX),
                active_tombstone_union_bytes: 0,
                reservation: &mut reservation,
            }),
        )
    }

    pub(in crate::engine::storage_engine) fn refresh_missing_visibility_summaries_for_query(
        &self,
        series_ids: &RoaringTreemap,
        execution: &QueryExecution,
    ) -> Result<()> {
        execution.checkpoint()?;
        let series_count = series_ids.len();
        execution.observe_intermediate_vector_size(series_count)?;
        // The missing-ID collection is allocated before the detailed refresh can inspect it.
        // Admit its worst-case growth first; the refresh accounts its update, cache, and
        // per-series range-rebuild staging separately while this reservation remains live.
        let _missing_ids_reservation =
            execution.reserve_memory(Self::visibility_query_id_vector_bytes(series_count, 1))?;
        let missing_series_ids = self.missing_visibility_summary_series_ids(series_ids.iter());
        if !missing_series_ids.is_empty() {
            self.refresh_series_visible_timestamp_cache_for_query(missing_series_ids, execution)?;
        }
        Ok(())
    }

    pub(in crate::engine::storage_engine) fn live_series_postings_for_query(
        &self,
        series_ids: RoaringTreemap,
        prune_dead: bool,
        execution: &QueryExecution,
    ) -> Result<RoaringTreemap> {
        execution.checkpoint()?;
        if series_ids.is_empty() {
            return Ok(RoaringTreemap::new());
        }

        let series_count = series_ids.len();
        execution.observe_intermediate_vector_size(series_count)?;
        self.refresh_missing_visibility_summaries_for_query(&series_ids, execution)?;

        // Retention partitioning can hold the live and dead ID vectors together. The candidate
        // bitmap reservation covers the input and output postings, but not these owned vectors.
        // Keep this guard through stable dead-series pruning, whose removal path can reuse one
        // vector lifetime for its companion result.
        let _partition_reservation =
            execution.reserve_memory(Self::visibility_query_id_vector_bytes(series_count, 2))?;
        execution.checkpoint()?;
        let generation_before = prune_dead.then(|| self.live_series_pruning_generation());
        let retention_cutoff = self.active_retention_cutoff().unwrap_or(i64::MIN);
        let (live_series_ids, dead_series_ids) =
            self.partition_series_by_retention_for_query(series_ids, retention_cutoff, execution)?;
        execution.checkpoint()?;
        let live_series_ids = live_series_ids.into_iter().collect();

        self.prune_dead_materialized_series_ids_if_stable(dead_series_ids, generation_before);
        execution.checkpoint()?;

        Ok(live_series_ids)
    }

    pub(in crate::engine::storage_engine) fn refresh_series_visible_timestamp_cache_locked<I>(
        &self,
        series_ids: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        self.refresh_series_visible_timestamp_cache_locked_impl(series_ids, None)
    }

    fn refresh_series_visible_timestamp_cache_locked_impl<I>(
        &self,
        series_ids: I,
        mut query_budget: Option<&mut VisibilityRefreshQueryBudget<'_>>,
    ) -> Result<()>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        if let Some(query_budget) = query_budget.as_deref() {
            query_budget.checkpoint()?;
        }
        let mut series_ids = series_ids.into_iter().collect::<Vec<_>>();
        if series_ids.is_empty() {
            return Ok(());
        }
        series_ids.sort_unstable();
        series_ids.dedup();
        if let Some(query_budget) = query_budget.as_deref() {
            query_budget.checkpoint()?;
        }

        let _recency_guard = self.visibility.recency_state_lock.lock();
        let local_tombstones = self.visibility.tombstones.read();
        let remote_tombstones = self.visibility.remote_tombstones.read();
        let persisted_index = self.persisted.persisted_index.read();
        let bounded_cutoff = self.current_future_skew_cutoff();
        let mut updates = Vec::with_capacity(series_ids.len());

        for series_id in series_ids {
            if let Some(query_budget) = query_budget.as_deref() {
                query_budget.checkpoint()?;
            }
            let local_ranges = local_tombstones.get(&series_id).map(Vec::as_slice);
            let remote_ranges = remote_tombstones.ranges(series_id);
            let merged_ranges;
            let tombstone_ranges = match (local_ranges, remote_ranges) {
                (None, None) => None,
                (Some(ranges), None) | (None, Some(ranges)) => Some(ranges),
                (Some(local), Some(remote)) => {
                    if let Some(query_budget) = query_budget.as_deref_mut() {
                        query_budget
                            .admit_tombstone_union(local.len().saturating_add(remote.len()))?;
                    }
                    merged_ranges = tombstone::union_normalized_tombstone_ranges(local, remote);
                    Some(merged_ranges.as_slice())
                }
            };
            let summary = self.rebuild_series_visibility_summary_locked(
                series_id,
                &persisted_index,
                tombstone_ranges,
                bounded_cutoff,
                query_budget.as_deref_mut(),
            )?;
            if let Some(query_budget) = query_budget.as_deref_mut() {
                query_budget.finish_tombstone_union();
            }
            updates.push((series_id, summary));
        }

        drop(persisted_index);
        drop(remote_tombstones);
        drop(local_tombstones);

        if let Some(query_budget) = query_budget.as_deref() {
            query_budget.checkpoint()?;
        }
        self.replace_series_visible_timestamp_cache_entries(updates);
        Ok(())
    }

    pub(in crate::engine::storage_engine) fn live_series_postings(
        &self,
        series_ids: RoaringTreemap,
        prune_dead: bool,
    ) -> Result<RoaringTreemap> {
        if series_ids.is_empty() {
            return Ok(RoaringTreemap::new());
        }

        let missing_series_ids = self.missing_visibility_summary_series_ids(series_ids.iter());
        if !missing_series_ids.is_empty() {
            self.refresh_series_visible_timestamp_cache(missing_series_ids)?;
        }

        let generation_before = prune_dead.then(|| self.live_series_pruning_generation());
        let retention_cutoff = self.active_retention_cutoff().unwrap_or(i64::MIN);
        let (live_series_ids, dead_series_ids) =
            self.partition_series_by_retention(series_ids, retention_cutoff);
        let live_series_ids = live_series_ids.into_iter().collect();

        self.prune_dead_materialized_series_ids_if_stable(dead_series_ids, generation_before);

        Ok(live_series_ids)
    }

    pub(in crate::engine::storage_engine) fn live_series_ids<I>(
        &self,
        series_ids: I,
        prune_dead: bool,
    ) -> Result<Vec<SeriesId>>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        Ok(self
            .live_series_postings(series_ids.into_iter().collect(), prune_dead)?
            .iter()
            .collect())
    }
}
