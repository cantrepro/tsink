use super::*;

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
    ) -> Result<SeriesVisibilitySummary> {
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

        let mut ranges = Vec::new();
        {
            let active = self.active_shard(series_id).read();
            if let Some(state) = active.get(&series_id) {
                ranges.extend(state.points_in_partition_order().filter_map(|point| {
                    Self::single_timestamp_visibility_range(point.ts, tombstone_ranges)
                }));
            }
        }
        {
            let sealed = self.sealed_shard(series_id).read();
            if let Some(chunks) = sealed.get(&series_id) {
                ranges.extend(chunks.values().filter_map(|chunk| {
                    Self::chunk_bounds_visibility_range(
                        chunk.header.min_ts,
                        chunk.header.max_ts,
                        chunk.header.point_count,
                        tombstone_ranges,
                    )
                }));
            }
        }
        if let Some(chunks) = persisted_index.chunk_refs.get(&series_id) {
            ranges.extend(chunks.iter().filter_map(|chunk_ref| {
                Self::chunk_bounds_visibility_range(
                    chunk_ref.min_ts,
                    chunk_ref.max_ts,
                    chunk_ref.point_count,
                    tombstone_ranges,
                )
            }));
        }

        Self::normalize_series_visibility_ranges(&mut ranges);
        if ranges.len() > SERIES_VISIBILITY_SUMMARY_MAX_RANGES {
            let drop_count = ranges
                .len()
                .saturating_sub(SERIES_VISIBILITY_SUMMARY_MAX_RANGES);
            ranges.drain(..drop_count);
            return Ok(SeriesVisibilitySummary {
                latest_visible_timestamp: latest_visible,
                latest_bounded_visible_timestamp: latest_bounded_visible,
                exhaustive_floor_inclusive: ranges.first().map(|range| range.min_ts),
                truncated_before_floor: true,
                ranges,
            });
        }

        Ok(SeriesVisibilitySummary {
            latest_visible_timestamp: latest_visible,
            latest_bounded_visible_timestamp: latest_bounded_visible,
            exhaustive_floor_inclusive: ranges.first().map(|range| range.min_ts),
            truncated_before_floor: false,
            ranges,
        })
    }

    fn replace_series_visible_timestamp_cache_entries(
        &self,
        updates: Vec<(SeriesId, SeriesVisibilitySummary)>,
    ) {
        let _recency_guard = self.visibility.recency_state_lock.lock();
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

    pub(super) fn clear_series_visible_timestamp_cache<I>(&self, series_ids: I)
    where
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

    pub(in crate::engine::storage_engine) fn refresh_series_visible_timestamp_cache_locked<I>(
        &self,
        series_ids: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let mut series_ids = series_ids.into_iter().collect::<Vec<_>>();
        if series_ids.is_empty() {
            return Ok(());
        }
        series_ids.sort_unstable();
        series_ids.dedup();

        let tombstones = self.visibility.tombstones.read();
        let persisted_index = self.persisted.persisted_index.read();
        let bounded_cutoff = self.current_future_skew_cutoff();
        let mut updates = Vec::with_capacity(series_ids.len());

        for series_id in series_ids {
            let summary = self.rebuild_series_visibility_summary_locked(
                series_id,
                &persisted_index,
                tombstones.get(&series_id).map(Vec::as_slice),
                bounded_cutoff,
            )?;
            updates.push((series_id, summary));
        }

        drop(persisted_index);
        drop(tombstones);

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
