use std::collections::{btree_map::Entry as BTreeEntry, BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;

use super::*;

impl SeriesRegistry {
    pub fn series_ids_for_metric(&self, metric: &str) -> Vec<SeriesId> {
        let Some(metric_id) = self.metric_dict.read().get_id(metric) else {
            return Vec::new();
        };
        self.metric_postings_shards[Self::metric_postings_shard_idx(metric_id)]
            .read()
            .metric_postings
            .get(&metric_id)
            .map(|ids| ids.iter().collect())
            .unwrap_or_default()
    }

    pub fn series_id_postings_for_metric(&self, metric: &str) -> Option<RoaringTreemap> {
        let metric_id = self.metric_dict.read().get_id(metric)?;
        self.metric_postings_shards[Self::metric_postings_shard_idx(metric_id)]
            .read()
            .metric_postings
            .get(&metric_id)
            .cloned()
    }

    pub fn for_each_series_id(&self, mut visit: impl FnMut(SeriesId)) {
        for shard in &self.all_series_shards {
            for series_id in shard.read().series_ids.iter() {
                visit(series_id);
            }
        }
    }

    pub fn all_series_postings(&self) -> RoaringTreemap {
        let mut merged = RoaringTreemap::new();
        for shard in &self.all_series_shards {
            merged |= shard.read().series_ids.clone();
        }
        merged
    }

    pub fn postings_for_ids(&self, pair: LabelPairId) -> Option<RoaringTreemap> {
        self.label_postings_shards[Self::label_postings_shard_idx(pair.name_id)]
            .read()
            .postings
            .get(&pair)
            .cloned()
    }

    pub fn postings_for_label(
        &self,
        label_name: &str,
        label_value: &str,
    ) -> Option<RoaringTreemap> {
        let label_name_dict = self.label_name_dict.read();
        let label_value_dict = self.label_value_dict.read();
        let name_id = label_name_dict.get_id(label_name)?;
        let value_id = label_value_dict.get_id(label_value)?;
        self.postings_for_ids(LabelPairId { name_id, value_id })
    }

    pub fn metric_postings_count(&self) -> usize {
        self.metric_postings_shards
            .iter()
            .map(|shard| shard.read().metric_postings.len())
            .sum()
    }

    pub fn label_name_postings_count(&self) -> usize {
        self.label_postings_shards
            .iter()
            .map(|shard| {
                shard
                    .read()
                    .label_name_states
                    .values()
                    .filter(|state| !state.present.is_empty())
                    .count()
            })
            .sum()
    }

    pub fn metric_postings_entries(&self) -> Vec<(String, RoaringTreemap)> {
        let metric_dict = self.metric_dict.read();
        let mut entries = Vec::new();
        for shard in &self.metric_postings_shards {
            for (metric_id, series_ids) in &shard.read().metric_postings {
                if let Some(metric) = metric_dict.get_value(*metric_id) {
                    entries.push((metric.to_string(), series_ids.clone()));
                }
            }
        }
        entries
    }

    pub fn for_each_metric_postings(&self, mut visit: impl FnMut(&str, &RoaringTreemap)) {
        let metric_dict = self.metric_dict.read();
        for shard in &self.metric_postings_shards {
            for (metric_id, series_ids) in &shard.read().metric_postings {
                let Some(metric) = metric_dict.get_value(*metric_id) else {
                    continue;
                };
                visit(metric, series_ids);
            }
        }
    }

    pub fn postings_entries(&self) -> Vec<(LabelPairId, RoaringTreemap)> {
        let mut entries = Vec::new();
        for shard in &self.label_postings_shards {
            entries.extend(
                shard
                    .read()
                    .postings
                    .iter()
                    .map(|(pair, ids)| (*pair, ids.clone())),
            );
        }
        entries
    }

    pub fn postings_entries_for_label_name_id(
        &self,
        name_id: DictionaryId,
    ) -> Vec<(DictionaryId, RoaringTreemap)> {
        let start = LabelPairId {
            name_id,
            value_id: DictionaryId::MIN,
        };
        self.label_postings_shards[Self::label_postings_shard_idx(name_id)]
            .read()
            .postings
            .range(start..)
            .take_while(move |(pair, _)| pair.name_id == name_id)
            .map(|(pair, ids)| (pair.value_id, ids.clone()))
            .collect()
    }

    pub fn for_each_postings_for_label_name_id(
        &self,
        name_id: DictionaryId,
        mut visit: impl FnMut(DictionaryId, &RoaringTreemap),
    ) {
        let start = LabelPairId {
            name_id,
            value_id: DictionaryId::MIN,
        };
        let shard = self.label_postings_shards[Self::label_postings_shard_idx(name_id)].read();
        for (pair, ids) in shard
            .postings
            .range(start..)
            .take_while(move |(pair, _)| pair.name_id == name_id)
        {
            visit(pair.value_id, ids);
        }
    }

    pub fn series_id_postings_for_label_name(&self, label_name: &str) -> Option<RoaringTreemap> {
        let name_id = self.label_name_dict.read().get_id(label_name)?;
        self.label_name_postings_for_id(name_id)
    }

    pub fn label_name_postings_for_id(&self, name_id: DictionaryId) -> Option<RoaringTreemap> {
        self.label_postings_shards[Self::label_postings_shard_idx(name_id)]
            .read()
            .label_name_states
            .get(&name_id)
            .map(|state| state.present.clone())
            .filter(|bitmap| !bitmap.is_empty())
    }

    pub fn missing_label_postings_for_id(&self, name_id: DictionaryId) -> RoaringTreemap {
        let shard_idx = Self::label_postings_shard_idx(name_id);
        loop {
            let observed_generation = self.postings_generation.load(Ordering::Acquire);
            {
                let shard = self.label_postings_shards[shard_idx].read();
                if let Some(state) = shard.label_name_states.get(&name_id) {
                    if let Some(cache) = &state.missing_cache {
                        if cache.postings_generation == observed_generation {
                            return cache.bitmap.clone();
                        }
                    }
                }
            }

            let mut missing = self.all_series_postings();
            if let Some(present) = self.label_name_postings_for_id(name_id) {
                missing -= &present;
            }

            let mut shard = self.label_postings_shards[shard_idx].write();
            let state = shard.label_name_states.entry(name_id).or_default();
            let current_generation = self.postings_generation.load(Ordering::Acquire);
            if let Some(cache) = &state.missing_cache {
                if cache.postings_generation == current_generation {
                    return cache.bitmap.clone();
                }
            }
            if current_generation != observed_generation {
                continue;
            }

            let before = state
                .missing_cache
                .as_ref()
                .map(|cache| Self::bitmap_memory_usage_bytes(&cache.bitmap))
                .unwrap_or(0);
            state.missing_cache = Some(MissingLabelPostingsCacheEntry {
                bitmap: missing.clone(),
                postings_generation: current_generation,
            });
            let after = Self::bitmap_memory_usage_bytes(&missing);
            if after >= before {
                let added = after.saturating_sub(before);
                shard.estimated_postings_bytes =
                    shard.estimated_postings_bytes.saturating_add(added);
                self.add_estimated_memory_bytes(added);
            } else {
                let removed = before.saturating_sub(after);
                shard.estimated_postings_bytes =
                    shard.estimated_postings_bytes.saturating_sub(removed);
                self.sub_estimated_memory_bytes(removed);
            }
            return missing;
        }
    }

    pub fn postings_bucket_count_for_label_name_id(&self, name_id: DictionaryId) -> usize {
        self.label_postings_shards[Self::label_postings_shard_idx(name_id)]
            .read()
            .label_name_states
            .get(&name_id)
            .map(|state| state.bucket_count)
            .unwrap_or(0)
    }

    pub fn postings_count(&self) -> usize {
        self.label_postings_shards
            .iter()
            .map(|shard| shard.read().postings.len())
            .sum()
    }

    pub(super) fn index_series_locked(
        &self,
        metric_id: DictionaryId,
        series_id: SeriesId,
        label_pairs: &[LabelPairId],
    ) {
        let mut added_postings_bytes = Self::update_all_series_postings_locked(
            &mut self.all_series_shards[Self::all_series_shard_idx(series_id)].write(),
            series_id,
        );
        added_postings_bytes =
            added_postings_bytes.saturating_add(Self::update_metric_postings_locked(
                &mut self.metric_postings_shards[Self::metric_postings_shard_idx(metric_id)]
                    .write(),
                metric_id,
                series_id,
            ));

        let mut pairs_by_shard = BTreeMap::<usize, Vec<LabelPairId>>::new();
        for pair in label_pairs {
            pairs_by_shard
                .entry(Self::label_postings_shard_idx(pair.name_id))
                .or_default()
                .push(*pair);
        }
        let touched_shards = pairs_by_shard.keys().copied().collect::<BTreeSet<_>>();
        for shard_idx in touched_shards.iter().copied() {
            let pairs = pairs_by_shard.remove(&shard_idx).unwrap_or_default();
            let mut shard = self.label_postings_shards[shard_idx].write();
            if !pairs.is_empty() {
                added_postings_bytes = added_postings_bytes.saturating_add(
                    Self::update_label_postings_locked(&mut shard, series_id, &pairs),
                );
            }
        }
        self.add_estimated_memory_bytes(added_postings_bytes);
    }

    fn update_all_series_postings_locked(
        postings: &mut AllSeriesPostingsShard,
        series_id: SeriesId,
    ) -> usize {
        let before = Self::bitmap_memory_usage_bytes(&postings.series_ids);
        postings.series_ids.insert(series_id);
        let after = Self::bitmap_memory_usage_bytes(&postings.series_ids);
        let added_bytes = after.saturating_sub(before);
        postings.estimated_postings_bytes = postings
            .estimated_postings_bytes
            .saturating_add(added_bytes);
        added_bytes
    }

    fn update_label_postings_locked(
        postings: &mut LabelPostingsShard,
        series_id: SeriesId,
        label_pairs: &[LabelPairId],
    ) -> usize {
        let mut added_bytes = 0usize;
        for pair in label_pairs {
            let label_name_before = postings
                .label_name_states
                .get(&pair.name_id)
                .map(|state| Self::bitmap_memory_usage_bytes(&state.present))
                .unwrap_or(0);
            let label_name_after = {
                let state = postings.label_name_states.entry(pair.name_id).or_default();
                state.present.insert(series_id);
                Self::bitmap_memory_usage_bytes(&state.present)
            };
            let label_name_added = label_name_after.saturating_sub(label_name_before);
            postings.estimated_postings_bytes = postings
                .estimated_postings_bytes
                .saturating_add(label_name_added);
            added_bytes = added_bytes.saturating_add(label_name_added);

            let posting_before = postings
                .postings
                .get(pair)
                .map(Self::bitmap_memory_usage_bytes)
                .unwrap_or(0);
            let created_bucket = match postings.postings.entry(*pair) {
                BTreeEntry::Occupied(mut entry) => {
                    entry.get_mut().insert(series_id);
                    false
                }
                BTreeEntry::Vacant(entry) => {
                    let mut series_ids = RoaringTreemap::new();
                    series_ids.insert(series_id);
                    entry.insert(series_ids);
                    true
                }
            };
            let posting_after = postings
                .postings
                .get(pair)
                .map(Self::bitmap_memory_usage_bytes)
                .unwrap_or(0);
            let posting_added = posting_after.saturating_sub(posting_before);
            postings.estimated_postings_bytes = postings
                .estimated_postings_bytes
                .saturating_add(posting_added);
            added_bytes = added_bytes.saturating_add(posting_added);
            if created_bucket {
                let state = postings.label_name_states.entry(pair.name_id).or_default();
                let created_count_entry = state.bucket_count == 0;
                state.bucket_count = state.bucket_count.saturating_add(1);
                if created_count_entry {
                    let count_entry_bytes = Self::label_name_postings_count_entry_bytes();
                    postings.estimated_postings_bytes = postings
                        .estimated_postings_bytes
                        .saturating_add(count_entry_bytes);
                    added_bytes = added_bytes.saturating_add(count_entry_bytes);
                }
            }
        }
        added_bytes
    }

    fn update_metric_postings_locked(
        postings: &mut MetricPostingsShard,
        metric_id: DictionaryId,
        series_id: SeriesId,
    ) -> usize {
        let before = postings
            .metric_postings
            .get(&metric_id)
            .map(Self::bitmap_memory_usage_bytes)
            .unwrap_or(0);
        postings
            .metric_postings
            .entry(metric_id)
            .or_default()
            .insert(series_id);
        let after = postings
            .metric_postings
            .get(&metric_id)
            .map(Self::bitmap_memory_usage_bytes)
            .unwrap_or(0);
        let added_bytes = after.saturating_sub(before);
        postings.estimated_postings_bytes = postings
            .estimated_postings_bytes
            .saturating_add(added_bytes);
        added_bytes
    }

    pub(super) fn remove_series_from_indexes_locked(
        &self,
        metric_id: DictionaryId,
        series_id: SeriesId,
        label_pairs: &[LabelPairId],
    ) {
        let mut removed_postings_bytes = Self::remove_all_series_postings_locked(
            &mut self.all_series_shards[Self::all_series_shard_idx(series_id)].write(),
            series_id,
        );
        removed_postings_bytes =
            removed_postings_bytes.saturating_add(Self::remove_metric_postings_locked(
                &mut self.metric_postings_shards[Self::metric_postings_shard_idx(metric_id)]
                    .write(),
                metric_id,
                series_id,
            ));

        let mut pairs_by_shard = BTreeMap::<usize, Vec<LabelPairId>>::new();
        for pair in label_pairs {
            pairs_by_shard
                .entry(Self::label_postings_shard_idx(pair.name_id))
                .or_default()
                .push(*pair);
        }
        let touched_shards = pairs_by_shard.keys().copied().collect::<BTreeSet<_>>();
        for shard_idx in touched_shards.iter().copied() {
            let pairs = pairs_by_shard.remove(&shard_idx).unwrap_or_default();
            let mut shard = self.label_postings_shards[shard_idx].write();
            if !pairs.is_empty() {
                removed_postings_bytes = removed_postings_bytes.saturating_add(
                    Self::remove_label_postings_locked(&mut shard, series_id, &pairs),
                );
            }
        }
        self.sub_estimated_memory_bytes(removed_postings_bytes);
    }

    fn remove_all_series_postings_locked(
        postings: &mut AllSeriesPostingsShard,
        series_id: SeriesId,
    ) -> usize {
        let before = Self::bitmap_memory_usage_bytes(&postings.series_ids);
        postings.series_ids.remove(series_id);
        let after = Self::bitmap_memory_usage_bytes(&postings.series_ids);
        let removed_bytes = before.saturating_sub(after);
        postings.estimated_postings_bytes = postings
            .estimated_postings_bytes
            .saturating_sub(removed_bytes);
        removed_bytes
    }

    fn remove_metric_postings_locked(
        postings: &mut MetricPostingsShard,
        metric_id: DictionaryId,
        series_id: SeriesId,
    ) -> usize {
        let (removed_bytes, remove_bucket) =
            if let Some(series_ids) = postings.metric_postings.get_mut(&metric_id) {
                let before = Self::bitmap_memory_usage_bytes(series_ids);
                series_ids.remove(series_id);
                let after = Self::bitmap_memory_usage_bytes(series_ids);
                (before.saturating_sub(after), series_ids.is_empty())
            } else {
                (0, false)
            };
        postings.estimated_postings_bytes = postings
            .estimated_postings_bytes
            .saturating_sub(removed_bytes);
        if remove_bucket {
            postings.metric_postings.remove(&metric_id);
        }
        removed_bytes
    }

    fn remove_label_postings_locked(
        postings: &mut LabelPostingsShard,
        series_id: SeriesId,
        label_pairs: &[LabelPairId],
    ) -> usize {
        let mut touched_label_names = BTreeSet::new();
        let mut removed_bytes = 0usize;
        for pair in label_pairs {
            touched_label_names.insert(pair.name_id);
            let (posting_removed_bytes, remove_posting) =
                if let Some(series_ids) = postings.postings.get_mut(pair) {
                    let before = Self::bitmap_memory_usage_bytes(series_ids);
                    series_ids.remove(series_id);
                    let after = Self::bitmap_memory_usage_bytes(series_ids);
                    (before.saturating_sub(after), series_ids.is_empty())
                } else {
                    (0, false)
                };
            postings.estimated_postings_bytes = postings
                .estimated_postings_bytes
                .saturating_sub(posting_removed_bytes);
            removed_bytes = removed_bytes.saturating_add(posting_removed_bytes);
            if remove_posting {
                postings.postings.remove(pair);
                if let Some(state) = postings.label_name_states.get_mut(&pair.name_id) {
                    if state.bucket_count > 0 {
                        state.bucket_count -= 1;
                        if state.bucket_count == 0 {
                            let count_entry_bytes = Self::label_name_postings_count_entry_bytes();
                            postings.estimated_postings_bytes = postings
                                .estimated_postings_bytes
                                .saturating_sub(count_entry_bytes);
                            removed_bytes = removed_bytes.saturating_add(count_entry_bytes);
                        }
                    }
                }
            }
        }

        let mut empty_states = Vec::new();
        for name_id in touched_label_names {
            let mut remove_state = false;
            if let Some(state) = postings.label_name_states.get_mut(&name_id) {
                let before = Self::bitmap_memory_usage_bytes(&state.present);
                state.present.remove(series_id);
                let after = Self::bitmap_memory_usage_bytes(&state.present);
                let label_name_removed = before.saturating_sub(after);
                postings.estimated_postings_bytes = postings
                    .estimated_postings_bytes
                    .saturating_sub(label_name_removed);
                removed_bytes = removed_bytes.saturating_add(label_name_removed);
                remove_state = state.present.is_empty()
                    && state.bucket_count == 0
                    && state.missing_cache.is_none();
            }
            if remove_state {
                empty_states.push(name_id);
            }
        }
        for name_id in empty_states {
            postings.label_name_states.remove(&name_id);
        }
        removed_bytes
    }

    pub(super) fn rebuild_postings_indexes(&self) {
        let definitions = self.series_definitions();
        for shard in &self.all_series_shards {
            *shard.write() = AllSeriesPostingsShard::default();
        }
        for shard in &self.metric_postings_shards {
            *shard.write() = MetricPostingsShard::default();
        }
        for shard in &self.label_postings_shards {
            *shard.write() = LabelPostingsShard::default();
        }
        for series in definitions {
            self.index_series_locked(series.metric_id, series.series_id, &series.label_pairs);
        }
        self.refresh_estimated_memory_usage_bytes();
    }
}
