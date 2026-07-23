use super::*;

struct VisibilityRefreshQueryBudget<'a> {
    execution: &'a QueryExecution,
    retained_staging_bytes: u64,
    reservation: &'a mut crate::QueryMemoryReservation,
}

impl VisibilityRefreshQueryBudget<'_> {
    fn checkpoint(&self) -> Result<()> {
        self.execution.checkpoint().map_err(Into::into)
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
        let required = self.retained_staging_bytes.saturating_add(scratch_bytes);
        if required > self.reservation.bytes() {
            self.reservation.resize(required)?;
        }
        Ok(())
    }
}

impl ChunkStorage {
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

    fn repair_max_bounded_observed_timestamp_from_cache(
        &self,
        bounded_cache: &HashMap<SeriesId, Option<i64>>,
    ) {
        let repaired = bounded_cache
            .values()
            .copied()
            .flatten()
            .max()
            .unwrap_or(i64::MIN);
        self.visibility
            .max_bounded_observed_timestamp
            .store(repaired, Ordering::Release);
    }

    fn series_visibility_summary_map_memory_usage_bytes(
        summaries: &HashMap<SeriesId, SeriesVisibilitySummary>,
    ) -> usize {
        let mut bytes =
            Self::hash_map_memory_usage_bytes::<SeriesId, SeriesVisibilitySummary>(summaries);
        for summary in summaries.values() {
            bytes = bytes.saturating_add(
                summary
                    .ranges
                    .capacity()
                    .saturating_mul(std::mem::size_of::<SeriesVisibilityRangeSummary>()),
            );
        }
        bytes
    }

    pub(in crate::engine::storage_engine) fn series_visibility_state_memory_usage_bytes(
        summaries: &HashMap<SeriesId, SeriesVisibilitySummary>,
        cache: &HashMap<SeriesId, Option<i64>>,
        bounded_cache: &HashMap<SeriesId, Option<i64>>,
    ) -> usize {
        Self::series_visibility_summary_map_memory_usage_bytes(summaries)
            .saturating_add(Self::hash_map_memory_usage_bytes::<SeriesId, Option<i64>>(
                cache,
            ))
            .saturating_add(Self::hash_map_memory_usage_bytes::<SeriesId, Option<i64>>(
                bounded_cache,
            ))
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
        let current_bounded = self
            .visibility
            .max_bounded_observed_timestamp
            .load(Ordering::Acquire);
        let mut repair_needed = false;
        let mut next_bounded = current_bounded;
        let mut summaries = self.visibility.series_visibility_summaries.write();
        let mut cache = self.visibility.series_visible_max_timestamps.write();
        let mut bounded_cache = self
            .visibility
            .series_visible_bounded_max_timestamps
            .write();
        self.with_visibility_state_memory_delta(
            &mut summaries,
            &mut cache,
            &mut bounded_cache,
            |summaries, cache, bounded_cache| {
                for (series_id, summary) in updates {
                    let latest = summary.latest_visible_timestamp;
                    let latest_bounded = summary.latest_bounded_visible_timestamp;
                    let previous_bounded = bounded_cache.get(&series_id).copied().flatten();
                    if previous_bounded == Some(current_bounded)
                        && latest_bounded.unwrap_or(i64::MIN) < current_bounded
                    {
                        repair_needed = true;
                    }

                    summaries.insert(series_id, summary);
                    cache.insert(series_id, latest);
                    bounded_cache.insert(series_id, latest_bounded);
                    if let Some(latest_bounded) = latest_bounded {
                        next_bounded = next_bounded.max(latest_bounded);
                    }
                }
            },
        );

        drop(summaries);
        drop(cache);

        if repair_needed {
            self.repair_max_bounded_observed_timestamp_from_cache(&bounded_cache);
        } else if next_bounded != current_bounded {
            self.visibility
                .max_bounded_observed_timestamp
                .store(next_bounded, Ordering::Release);
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
        let current_bounded = self
            .visibility
            .max_bounded_observed_timestamp
            .load(Ordering::Acquire);
        let mut repair_needed = false;
        let mut summaries = self.visibility.series_visibility_summaries.write();
        let mut cache = self.visibility.series_visible_max_timestamps.write();
        let mut bounded_cache = self
            .visibility
            .series_visible_bounded_max_timestamps
            .write();
        self.with_visibility_state_memory_delta(
            &mut summaries,
            &mut cache,
            &mut bounded_cache,
            |summaries, cache, bounded_cache| {
                for series_id in series_ids {
                    summaries.remove(&series_id);
                    cache.remove(&series_id);
                    if bounded_cache.remove(&series_id).flatten() == Some(current_bounded) {
                        repair_needed = true;
                    }
                }
            },
        );

        drop(summaries);
        drop(cache);

        if repair_needed {
            self.repair_max_bounded_observed_timestamp_from_cache(&bounded_cache);
        }
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
                    let mut count = 0usize;
                    for _ in state.points_in_partition_order() {
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
            largest_intermediate_vector = largest_intermediate_vector.max(raw_ranges);
            // Normalization can hold the source and merged vectors together. The retained
            // summary is capped, but the source scan is not, so charge its exact item count.
            largest_series_rebuild = largest_series_rebuild.max(
                raw_ranges
                    .saturating_mul(std::mem::size_of::<SeriesVisibilityRangeSummary>())
                    .saturating_mul(2)
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
                reservation: &mut reservation,
            }),
        )
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
        let tombstones = self.visibility.tombstones.read();
        let persisted_index = self.persisted.persisted_index.read();
        let bounded_cutoff = self.current_future_skew_cutoff();
        let mut updates = Vec::with_capacity(series_ids.len());

        for series_id in series_ids {
            if let Some(query_budget) = query_budget.as_deref() {
                query_budget.checkpoint()?;
            }
            let summary = self.rebuild_series_visibility_summary_locked(
                series_id,
                &persisted_index,
                tombstones.get(&series_id).map(Vec::as_slice),
                bounded_cutoff,
                query_budget.as_deref_mut(),
            )?;
            updates.push((series_id, summary));
        }

        drop(persisted_index);
        drop(tombstones);

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
