use super::*;

fn update_atomic_max_timestamp(target: &AtomicI64, ts: i64) {
    let mut current = target.load(Ordering::Acquire);
    while ts > current {
        match target.compare_exchange_weak(current, ts, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

fn normalized_series_ids<I>(series_ids: I) -> Vec<SeriesId>
where
    I: IntoIterator<Item = SeriesId>,
{
    let mut series_ids = series_ids.into_iter().collect::<Vec<_>>();
    series_ids.sort_unstable();
    series_ids.dedup();
    series_ids
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct ClockContext<'a> {
    pub(in crate::engine::storage_engine) timestamp_precision: TimestampPrecision,
    pub(in crate::engine::storage_engine) future_skew_window: i64,
    #[cfg(test)]
    pub(in crate::engine::storage_engine) current_time_override: &'a AtomicI64,
    #[cfg(not(test))]
    pub(in crate::engine::storage_engine) marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> ClockContext<'a> {
    pub(in crate::engine::storage_engine) fn current_timestamp_units(self) -> i64 {
        #[cfg(test)]
        {
            let override_timestamp = self.current_time_override.load(Ordering::Acquire);
            if override_timestamp != i64::MIN {
                return override_timestamp;
            }
        }

        current_unix_timestamp_units(self.timestamp_precision)
    }

    pub(in crate::engine::storage_engine) fn current_future_skew_cutoff(self) -> i64 {
        self.current_timestamp_units()
            .saturating_add(self.future_skew_window)
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct MaterializedSeriesReadContext<'a> {
    pub(in crate::engine::storage_engine) materialized_series: &'a RwLock<BTreeSet<SeriesId>>,
}

impl<'a> MaterializedSeriesReadContext<'a> {
    pub(in crate::engine::storage_engine) fn with_materialized_series<R>(
        self,
        f: impl FnOnce(&BTreeSet<SeriesId>) -> R,
    ) -> R {
        let materialized_series = self.materialized_series.read();
        f(&materialized_series)
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct VisibilityCacheMapView<'a, V> {
    values: &'a HashMap<SeriesId, V>,
    epochs: &'a HashMap<SeriesId, u64>,
    current_epoch: u64,
}

impl<'a, V> VisibilityCacheMapView<'a, V> {
    pub(in crate::engine::storage_engine) fn new(
        values: &'a HashMap<SeriesId, V>,
        epochs: &'a HashMap<SeriesId, u64>,
        current_epoch: u64,
    ) -> Self {
        Self {
            values,
            epochs,
            current_epoch,
        }
    }

    pub(in crate::engine::storage_engine) fn get(&self, series_id: &SeriesId) -> Option<&'a V> {
        (self.epochs.get(series_id).copied() == Some(self.current_epoch))
            .then(|| self.values.get(series_id))
            .flatten()
    }

    pub(in crate::engine::storage_engine) fn contains_key(&self, series_id: &SeriesId) -> bool {
        self.get(series_id).is_some()
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct VisibilityCacheReadContext<'a> {
    pub(in crate::engine::storage_engine) series_visibility_summaries:
        &'a RwLock<HashMap<SeriesId, SeriesVisibilitySummary>>,
    pub(in crate::engine::storage_engine) series_visible_max_timestamps:
        &'a RwLock<HashMap<SeriesId, Option<i64>>>,
    pub(in crate::engine::storage_engine) series_visible_bounded_max_timestamps:
        &'a RwLock<HashMap<SeriesId, Option<i64>>>,
    pub(in crate::engine::storage_engine) series_visibility_cache_epochs:
        &'a RwLock<HashMap<SeriesId, u64>>,
    pub(in crate::engine::storage_engine) remote_tombstone_epoch: &'a AtomicU64,
}

impl<'a> VisibilityCacheReadContext<'a> {
    pub(in crate::engine::storage_engine) fn with_visibility_cache_state<R>(
        self,
        f: impl FnOnce(
            VisibilityCacheMapView<'_, SeriesVisibilitySummary>,
            VisibilityCacheMapView<'_, Option<i64>>,
            VisibilityCacheMapView<'_, Option<i64>>,
        ) -> R,
    ) -> R {
        let summaries = self.series_visibility_summaries.read();
        let visible_cache = self.series_visible_max_timestamps.read();
        let bounded_visible_cache = self.series_visible_bounded_max_timestamps.read();
        let epochs = self.series_visibility_cache_epochs.read();
        let current_epoch = self.remote_tombstone_epoch.load(Ordering::Acquire);
        f(
            VisibilityCacheMapView::new(&summaries, &epochs, current_epoch),
            VisibilityCacheMapView::new(&visible_cache, &epochs, current_epoch),
            VisibilityCacheMapView::new(&bounded_visible_cache, &epochs, current_epoch),
        )
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct SeriesTimestampWriteContext<'a> {
    pub(in crate::engine::storage_engine) clock: ClockContext<'a>,
    pub(in crate::engine::storage_engine) retention_metrics:
        &'a super::super::metrics::RetentionObservabilityCounters,
    pub(in crate::engine::storage_engine) tombstones:
        &'a RwLock<crate::engine::tombstone::TombstoneMap>,
    pub(in crate::engine::storage_engine) series_visibility_summaries:
        &'a RwLock<HashMap<SeriesId, SeriesVisibilitySummary>>,
    pub(in crate::engine::storage_engine) series_visible_max_timestamps:
        &'a RwLock<HashMap<SeriesId, Option<i64>>>,
    pub(in crate::engine::storage_engine) series_visible_bounded_max_timestamps:
        &'a RwLock<HashMap<SeriesId, Option<i64>>>,
    pub(in crate::engine::storage_engine) series_visibility_cache_epochs:
        &'a RwLock<HashMap<SeriesId, u64>>,
    pub(in crate::engine::storage_engine) remote_tombstone_epoch: &'a AtomicU64,
    pub(in crate::engine::storage_engine) recency_state_lock: &'a Mutex<()>,
    pub(in crate::engine::storage_engine) max_observed_timestamp: &'a AtomicI64,
    pub(in crate::engine::storage_engine) max_bounded_observed_timestamp: &'a AtomicI64,
    pub(in crate::engine::storage_engine) live_series_pruning_generation: &'a AtomicU64,
    pub(in crate::engine::storage_engine) accounting_enabled: bool,
    pub(in crate::engine::storage_engine) metadata_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) shared_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) used_bytes: &'a AtomicU64,
}

impl<'a> SeriesTimestampWriteContext<'a> {
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

    fn merge_ranges_into_visibility_summary(
        summary: &mut SeriesVisibilitySummary,
        new_ranges: Vec<SeriesVisibilityRangeSummary>,
    ) {
        if new_ranges.is_empty() {
            return;
        }

        let previous_floor = summary.exhaustive_floor_inclusive;
        let previous_truncated = summary.truncated_before_floor;
        summary.ranges.extend(new_ranges);
        Self::normalize_series_visibility_ranges(&mut summary.ranges);
        if summary.ranges.len() > SERIES_VISIBILITY_SUMMARY_MAX_RANGES {
            let drop_count = summary
                .ranges
                .len()
                .saturating_sub(SERIES_VISIBILITY_SUMMARY_MAX_RANGES);
            summary.ranges.drain(..drop_count);
            summary.truncated_before_floor = true;
            summary.exhaustive_floor_inclusive = summary.ranges.first().map(|range| range.min_ts);
            return;
        }

        summary.truncated_before_floor = previous_truncated;
        summary.exhaustive_floor_inclusive = if previous_truncated {
            previous_floor.or_else(|| summary.ranges.first().map(|range| range.min_ts))
        } else {
            summary.ranges.first().map(|range| range.min_ts)
        };
    }

    fn record_future_skew_point(self, ts: i64) {
        self.retention_metrics
            .future_skew_points_total
            .fetch_add(1, Ordering::Relaxed);

        let mut current = self
            .retention_metrics
            .future_skew_max_timestamp
            .load(Ordering::Acquire);
        while ts > current {
            match self
                .retention_metrics
                .future_skew_max_timestamp
                .compare_exchange_weak(current, ts, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    fn merge_series_visible_timestamp_cache_updates(
        self,
        visible_timestamps: HashMap<SeriesId, i64>,
        bounded_timestamps: HashMap<SeriesId, i64>,
        summary_ranges: HashMap<SeriesId, Vec<SeriesVisibilityRangeSummary>>,
    ) {
        let _recency_guard = self.recency_state_lock.lock();
        let mut summaries = self.series_visibility_summaries.write();
        let mut cache = self.series_visible_max_timestamps.write();
        let mut bounded_cache = self.series_visible_bounded_max_timestamps.write();
        let mut epochs = self.series_visibility_cache_epochs.write();
        let current_epoch = self.remote_tombstone_epoch.load(Ordering::Acquire);
        let map_bytes_before = ChunkStorage::series_visibility_state_map_memory_usage_bytes(
            &summaries,
            &cache,
            &bounded_cache,
            &epochs,
        );
        let mut payload_bytes_before = 0usize;
        let mut payload_bytes_after = 0usize;
        for (series_id, timestamp) in visible_timestamps {
            let summary = summaries.entry(series_id).or_default();
            payload_bytes_before = payload_bytes_before.saturating_add(
                ChunkStorage::series_visibility_summary_payload_bytes(summary),
            );
            summary.latest_visible_timestamp = Some(
                summary
                    .latest_visible_timestamp
                    .map_or(timestamp, |current| current.max(timestamp)),
            );
            payload_bytes_after = payload_bytes_after.saturating_add(
                ChunkStorage::series_visibility_summary_payload_bytes(summary),
            );
            let entry = cache.entry(series_id).or_insert(Some(timestamp));
            match entry {
                Some(current) => *current = (*current).max(timestamp),
                None => *entry = Some(timestamp),
            }
            epochs.insert(series_id, current_epoch);
        }

        let mut bounded_max = i64::MIN;
        for (series_id, timestamp) in bounded_timestamps {
            bounded_max = bounded_max.max(timestamp);
            let summary = summaries.entry(series_id).or_default();
            payload_bytes_before = payload_bytes_before.saturating_add(
                ChunkStorage::series_visibility_summary_payload_bytes(summary),
            );
            summary.latest_bounded_visible_timestamp = Some(
                summary
                    .latest_bounded_visible_timestamp
                    .map_or(timestamp, |current| current.max(timestamp)),
            );
            payload_bytes_after = payload_bytes_after.saturating_add(
                ChunkStorage::series_visibility_summary_payload_bytes(summary),
            );
            let entry = bounded_cache.entry(series_id).or_insert(Some(timestamp));
            match entry {
                Some(current) => *current = (*current).max(timestamp),
                None => *entry = Some(timestamp),
            }
            epochs.insert(series_id, current_epoch);
        }
        for (series_id, ranges) in summary_ranges {
            let summary = summaries.entry(series_id).or_default();
            payload_bytes_before = payload_bytes_before.saturating_add(
                ChunkStorage::series_visibility_summary_payload_bytes(summary),
            );
            Self::merge_ranges_into_visibility_summary(summary, ranges);
            payload_bytes_after = payload_bytes_after.saturating_add(
                ChunkStorage::series_visibility_summary_payload_bytes(summary),
            );
            epochs.insert(series_id, current_epoch);
        }
        let map_bytes_after = ChunkStorage::series_visibility_state_map_memory_usage_bytes(
            &summaries,
            &cache,
            &bounded_cache,
            &epochs,
        );
        if self.accounting_enabled {
            let before = map_bytes_before.saturating_add(payload_bytes_before);
            let after = map_bytes_after.saturating_add(payload_bytes_after);
            if after >= before {
                add_included_memory_component_bytes(
                    true,
                    self.metadata_used_bytes,
                    self.shared_used_bytes,
                    self.used_bytes,
                    after.saturating_sub(before),
                );
            } else {
                sub_included_memory_component_bytes(
                    true,
                    self.metadata_used_bytes,
                    self.shared_used_bytes,
                    self.used_bytes,
                    before.saturating_sub(after),
                );
            }
        }

        if bounded_max != i64::MIN {
            update_atomic_max_timestamp(self.max_bounded_observed_timestamp, bounded_max);
        }
        self.live_series_pruning_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::engine::storage_engine) fn record_ingested_timestamp(self, ts: i64) {
        update_atomic_max_timestamp(self.max_observed_timestamp, ts);
        if ts > self.clock.current_future_skew_cutoff() {
            self.record_future_skew_point(ts);
        }
    }

    pub(in crate::engine::storage_engine) fn note_series_timestamps<I>(self, series_timestamps: I)
    where
        I: IntoIterator<Item = (SeriesId, i64)>,
    {
        let tombstones = self.tombstones.read();
        let mut visible_timestamps = HashMap::<SeriesId, i64>::new();
        let mut bounded_timestamps = HashMap::<SeriesId, i64>::new();
        let mut summary_ranges = HashMap::<SeriesId, Vec<SeriesVisibilityRangeSummary>>::new();
        let bounded_cutoff = self.clock.current_future_skew_cutoff();

        for (series_id, timestamp) in series_timestamps {
            if !ChunkStorage::timestamp_survives_tombstones(
                timestamp,
                tombstones.get(&series_id).map(Vec::as_slice),
            ) {
                continue;
            }

            let entry = visible_timestamps.entry(series_id).or_insert(timestamp);
            *entry = (*entry).max(timestamp);
            summary_ranges
                .entry(series_id)
                .or_default()
                .push(SeriesVisibilityRangeSummary {
                    min_ts: timestamp,
                    max_ts: timestamp,
                    exact: true,
                });
            if timestamp <= bounded_cutoff {
                let entry = bounded_timestamps.entry(series_id).or_insert(timestamp);
                *entry = (*entry).max(timestamp);
            }
        }

        drop(tombstones);
        self.merge_series_visible_timestamp_cache_updates(
            visible_timestamps,
            bounded_timestamps,
            summary_ranges,
        );
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct MaterializedSeriesWriteContext<'a> {
    pub(in crate::engine::storage_engine) accounting_enabled: bool,
    pub(in crate::engine::storage_engine) materialized_series: &'a RwLock<BTreeSet<SeriesId>>,
    pub(in crate::engine::storage_engine) live_series_pruning_generation: &'a AtomicU64,
    pub(in crate::engine::storage_engine) metadata_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) shared_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) used_bytes: &'a AtomicU64,
}

impl<'a> MaterializedSeriesWriteContext<'a> {
    pub(in crate::engine::storage_engine) fn insert_materialized_series_ids<I>(
        self,
        series_ids: I,
    ) -> Vec<SeriesId>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let series_ids = normalized_series_ids(series_ids);
        if series_ids.is_empty() {
            return Vec::new();
        }

        let mut materialized_series = self.materialized_series.write();
        let inserted_series_ids = with_included_memory_delta(
            self.accounting_enabled,
            self.metadata_used_bytes,
            self.shared_used_bytes,
            self.used_bytes,
            &mut materialized_series,
            |materialized_series| {
                ChunkStorage::btree_set_series_id_memory_usage_bytes(materialized_series)
            },
            |materialized_series| {
                let mut inserted = Vec::with_capacity(series_ids.len());
                for series_id in series_ids {
                    if materialized_series.insert(series_id) {
                        inserted.push(series_id);
                    }
                }
                inserted
            },
        );
        if !inserted_series_ids.is_empty() {
            self.live_series_pruning_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        inserted_series_ids
    }

    pub(in crate::engine::storage_engine) fn remove_materialized_series_ids_if_generation<I>(
        self,
        series_ids: I,
        expected_generation: u64,
    ) -> Vec<SeriesId>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let series_ids = normalized_series_ids(series_ids);
        if series_ids.is_empty() {
            return Vec::new();
        }

        let mut materialized_series = self.materialized_series.write();
        if self.live_series_pruning_generation.load(Ordering::Acquire) != expected_generation {
            return Vec::new();
        }
        let removed = with_included_memory_delta(
            self.accounting_enabled,
            self.metadata_used_bytes,
            self.shared_used_bytes,
            self.used_bytes,
            &mut materialized_series,
            |materialized_series| {
                ChunkStorage::btree_set_series_id_memory_usage_bytes(materialized_series)
            },
            |materialized_series| {
                let mut removed = Vec::with_capacity(series_ids.len());
                for series_id in series_ids {
                    if materialized_series.remove(&series_id) {
                        removed.push(series_id);
                    }
                }
                removed
            },
        );
        if !removed.is_empty() {
            // Publish the removal generation while the same set lock still excludes a reinsertion.
            self.live_series_pruning_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        removed
    }
}
