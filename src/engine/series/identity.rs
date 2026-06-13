use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::Ordering;

use super::*;
use crate::validation::{canonicalize_labels, validate_metric};
use crate::{Label, Result, Row, TsinkError};

impl SeriesRegistry {
    pub(super) fn series_definitions(&self) -> Vec<SeriesDefinition> {
        let mut definitions = Vec::new();
        for shard in &self.series_shards {
            definitions.extend(shard.read().by_id.values().cloned());
        }
        definitions
    }

    pub(super) fn series_definitions_with_families(
        &self,
    ) -> Vec<(SeriesDefinition, Option<SeriesValueFamily>)> {
        let mut definitions = Vec::new();
        for shard in &self.series_shards {
            let shard = shard.read();
            definitions.extend(shard.by_id.values().cloned().map(|definition| {
                let family = shard.value_families.get(&definition.series_id).copied();
                (definition, family)
            }));
        }
        definitions
    }

    fn alloc_series_id(&self) -> Result<SeriesId> {
        let mut current = self.next_series_id.load(Ordering::Acquire).max(1);
        loop {
            let next = current.checked_add(1).ok_or_else(|| {
                TsinkError::InvalidConfiguration(
                    "series id space exhausted (u64 overflow)".to_string(),
                )
            })?;
            match self.next_series_id.compare_exchange(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(current),
                Err(observed) => current = observed.max(1),
            }
        }
    }

    pub(crate) fn estimate_new_series_memory_growth_bytes(
        &self,
        series: &[SeriesKey],
    ) -> Result<usize> {
        if series.is_empty() {
            return Ok(0);
        }

        #[derive(Debug)]
        struct EstimatedNewSeries {
            series_id: SeriesId,
            metric_id: DictionaryId,
            label_pairs: Vec<LabelPairId>,
            present_label_names: BTreeSet<DictionaryId>,
        }

        let metric_dict = self.metric_dict.read();
        let label_name_dict = self.label_name_dict.read();
        let label_value_dict = self.label_value_dict.read();

        let mut pending_metric_ids = HashMap::<String, DictionaryId>::new();
        let mut pending_label_name_ids = HashMap::<String, DictionaryId>::new();
        let mut pending_label_value_ids = HashMap::<String, DictionaryId>::new();
        let mut next_metric_len = metric_dict.len();
        let mut next_label_name_len = label_name_dict.len();
        let mut next_label_value_len = label_value_dict.len();
        let mut predicted_next_series_id = self.next_series_id_value();
        let mut planned_keys = HashSet::<SeriesKeyIds>::new();
        let mut estimated_dictionary_bytes = 0usize;
        let mut estimated_series_bytes = 0usize;
        let mut estimated_series = Vec::<EstimatedNewSeries>::new();

        for key in series {
            validate_metric(&key.metric)?;

            let (metric_id, metric_bytes) = Self::estimate_string_dictionary_entry(
                &metric_dict,
                &mut pending_metric_ids,
                &mut next_metric_len,
                &key.metric,
                "metric",
            )?;
            estimated_dictionary_bytes = estimated_dictionary_bytes.saturating_add(metric_bytes);

            let normalized_labels = canonicalize_labels(&key.labels)?;

            let mut label_pairs = Vec::with_capacity(normalized_labels.len());
            let mut present_label_names = BTreeSet::new();
            for label in &normalized_labels {
                let (name_id, name_bytes) = Self::estimate_string_dictionary_entry(
                    &label_name_dict,
                    &mut pending_label_name_ids,
                    &mut next_label_name_len,
                    &label.name,
                    "label name",
                )?;
                estimated_dictionary_bytes = estimated_dictionary_bytes.saturating_add(name_bytes);

                let (value_id, value_bytes) = Self::estimate_string_dictionary_entry(
                    &label_value_dict,
                    &mut pending_label_value_ids,
                    &mut next_label_value_len,
                    &label.value,
                    "label value",
                )?;
                estimated_dictionary_bytes = estimated_dictionary_bytes.saturating_add(value_bytes);

                label_pairs.push(LabelPairId { name_id, value_id });
                present_label_names.insert(name_id);
            }
            label_pairs.sort_unstable();

            let key_ids = SeriesKeyIds {
                metric_id,
                label_pairs: label_pairs.clone(),
            };
            let shard_idx = Self::key_shard_idx_for_key(&key_ids);
            if self.series_shards[shard_idx]
                .read()
                .by_key
                .contains_key(&key_ids)
            {
                continue;
            }
            if !planned_keys.insert(key_ids) {
                continue;
            }

            let series_id = predicted_next_series_id;
            predicted_next_series_id =
                predicted_next_series_id.checked_add(1).ok_or_else(|| {
                    TsinkError::InvalidConfiguration(
                        "series id space exhausted (u64 overflow)".to_string(),
                    )
                })?;
            estimated_series_bytes = estimated_series_bytes
                .saturating_add(Self::series_label_pairs_memory_bytes(&label_pairs));
            estimated_series.push(EstimatedNewSeries {
                series_id,
                metric_id,
                label_pairs,
                present_label_names,
            });
        }

        drop(label_value_dict);
        drop(label_name_dict);
        drop(metric_dict);

        let mut estimated_bytes = estimated_dictionary_bytes.saturating_add(estimated_series_bytes);
        if estimated_series.is_empty() {
            return Ok(estimated_bytes);
        }
        let mut all_series_inserts = BTreeMap::<usize, Vec<SeriesId>>::new();
        let mut metric_inserts = BTreeMap::<DictionaryId, Vec<SeriesId>>::new();
        let mut label_name_inserts = BTreeMap::<DictionaryId, Vec<SeriesId>>::new();
        let mut pair_inserts = BTreeMap::<LabelPairId, Vec<SeriesId>>::new();

        for series in &estimated_series {
            all_series_inserts
                .entry(Self::all_series_shard_idx(series.series_id))
                .or_default()
                .push(series.series_id);
            metric_inserts
                .entry(series.metric_id)
                .or_default()
                .push(series.series_id);
            for name_id in &series.present_label_names {
                label_name_inserts
                    .entry(*name_id)
                    .or_default()
                    .push(series.series_id);
            }
            for pair in &series.label_pairs {
                pair_inserts
                    .entry(*pair)
                    .or_default()
                    .push(series.series_id);
            }
        }

        for (shard_idx, series_ids) in all_series_inserts {
            let mut updated = self.all_series_shards[shard_idx].read().series_ids.clone();
            let before = Self::bitmap_memory_usage_bytes(&updated);
            for series_id in series_ids {
                updated.insert(series_id);
            }
            estimated_bytes = estimated_bytes
                .saturating_add(Self::bitmap_memory_usage_bytes(&updated).saturating_sub(before));
        }

        for (metric_id, series_ids) in metric_inserts {
            let shard_idx = Self::metric_postings_shard_idx(metric_id);
            let mut updated = self.metric_postings_shards[shard_idx]
                .read()
                .metric_postings
                .get(&metric_id)
                .cloned()
                .unwrap_or_default();
            let before = Self::bitmap_memory_usage_bytes(&updated);
            for series_id in series_ids {
                updated.insert(series_id);
            }
            estimated_bytes = estimated_bytes
                .saturating_add(Self::bitmap_memory_usage_bytes(&updated).saturating_sub(before));
        }

        for (name_id, series_ids) in label_name_inserts {
            let shard_idx = Self::label_postings_shard_idx(name_id);
            let mut updated = self.label_postings_shards[shard_idx]
                .read()
                .label_name_states
                .get(&name_id)
                .map(|state| state.present.clone())
                .unwrap_or_default();
            let before = Self::bitmap_memory_usage_bytes(&updated);
            for series_id in series_ids {
                updated.insert(series_id);
            }
            estimated_bytes = estimated_bytes
                .saturating_add(Self::bitmap_memory_usage_bytes(&updated).saturating_sub(before));
        }

        let mut created_count_entries = BTreeSet::new();
        for (pair, series_ids) in pair_inserts {
            let shard_idx = Self::label_postings_shard_idx(pair.name_id);
            let shard = self.label_postings_shards[shard_idx].read();
            let mut updated = shard.postings.get(&pair).cloned().unwrap_or_default();
            let before = Self::bitmap_memory_usage_bytes(&updated);
            for series_id in series_ids {
                updated.insert(series_id);
            }
            estimated_bytes = estimated_bytes
                .saturating_add(Self::bitmap_memory_usage_bytes(&updated).saturating_sub(before));
            if !shard.postings.contains_key(&pair)
                && shard
                    .label_name_states
                    .get(&pair.name_id)
                    .map(|state| state.bucket_count)
                    .unwrap_or(0)
                    == 0
                && created_count_entries.insert(pair.name_id)
            {
                estimated_bytes =
                    estimated_bytes.saturating_add(Self::label_name_postings_count_entry_bytes());
            }
        }

        Ok(estimated_bytes)
    }

    pub fn resolve_or_insert(&self, metric: &str, labels: &[Label]) -> Result<SeriesResolution> {
        validate_metric(metric)?;
        let normalized_labels = canonicalize_labels(labels)?;

        let metric_id = self.intern_metric_id(metric)?;
        let label_pairs = self.intern_label_pairs(&normalized_labels)?;
        let key = SeriesKeyIds {
            metric_id,
            label_pairs: label_pairs.clone(),
        };
        let shard_idx = Self::key_shard_idx_for_key(&key);
        let mut shard = self.series_shards[shard_idx].write();

        if let Some(existing) = shard.by_key.get(&key) {
            return Ok(SeriesResolution {
                series_id: *existing,
                metric_id,
                label_pairs,
                created: false,
            });
        }

        let series_id = self.alloc_series_id()?;
        shard.by_id.insert(
            series_id,
            SeriesDefinition {
                series_id,
                metric_id,
                label_pairs: label_pairs.clone(),
            },
        );
        let added_series_bytes = Self::series_label_pairs_memory_bytes(&label_pairs);
        shard.estimated_series_bytes = shard
            .estimated_series_bytes
            .saturating_add(added_series_bytes);
        self.series_id_shards[Self::series_id_index_shard_idx(series_id)]
            .write()
            .series_id_to_registry_shard
            .insert(series_id, shard_idx);
        self.index_series_locked(metric_id, series_id, &label_pairs);
        shard.by_key.insert(key, series_id);
        self.series_count.fetch_add(1, Ordering::AcqRel);
        self.add_estimated_memory_bytes(added_series_bytes);
        self.bump_postings_generation();

        Ok(SeriesResolution {
            series_id,
            metric_id,
            label_pairs,
            created: true,
        })
    }

    pub fn resolve_existing(&self, metric: &str, labels: &[Label]) -> Option<SeriesResolution> {
        validate_metric(metric).ok()?;
        let metric_id = self.metric_dict.read().get_id(metric)?;
        let normalized_labels = canonicalize_labels(labels).ok()?;
        let label_pairs = self.lookup_label_pairs(&normalized_labels)?;
        let key = SeriesKeyIds {
            metric_id,
            label_pairs: label_pairs.clone(),
        };
        let shard_idx = Self::key_shard_idx_for_key(&key);
        let series_id = *self.series_shards[shard_idx].read().by_key.get(&key)?;
        Some(SeriesResolution {
            series_id,
            metric_id,
            label_pairs,
            created: false,
        })
    }

    pub fn resolve_rows(&self, rows: &[Row]) -> Result<Vec<SeriesResolution>> {
        let planned_series = rows
            .iter()
            .map(|row| SeriesKey {
                metric: row.metric().to_string(),
                labels: row.labels().to_vec(),
            })
            .collect::<Vec<_>>();
        self.prime_dictionaries_for_series(&planned_series)?;
        let mut resolutions = Vec::with_capacity(rows.len());
        for row in rows {
            resolutions.push(self.resolve_or_insert(row.metric(), row.labels())?);
        }
        Ok(resolutions)
    }

    pub fn reserve_new_series_capacity(&self, limit: usize, requested: usize) -> Result<()> {
        if limit == usize::MAX || requested == 0 {
            return Ok(());
        }

        let mut pending = self.pending_series_reservations.load(Ordering::Acquire);
        loop {
            let current = self.series_count();
            if current.saturating_add(pending).saturating_add(requested) > limit {
                return Err(TsinkError::CardinalityLimitExceeded {
                    limit,
                    current: current.saturating_add(pending),
                    requested,
                });
            }
            match self.pending_series_reservations.compare_exchange(
                pending,
                pending.saturating_add(requested),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => pending = observed,
            }
        }
    }

    pub fn release_new_series_capacity(&self, released: usize) {
        if released == 0 {
            return;
        }
        self.pending_series_reservations
            .fetch_sub(released, Ordering::AcqRel);
    }

    pub(crate) fn rollback_created_series(&self, created: &[SeriesResolution]) {
        if created.is_empty() {
            return;
        }

        let mut removed_any = false;
        for resolution in created {
            let key = SeriesKeyIds {
                metric_id: resolution.metric_id,
                label_pairs: resolution.label_pairs.clone(),
            };
            let shard_idx = Self::key_shard_idx_for_key(&key);
            let mut shard = self.series_shards[shard_idx].write();
            if shard.by_key.remove(&key).is_none() {
                continue;
            }
            if shard.value_families.remove(&resolution.series_id).is_some() {
                self.sub_estimated_memory_bytes(Self::value_family_entry_bytes());
                shard.estimated_series_bytes = shard
                    .estimated_series_bytes
                    .saturating_sub(Self::value_family_entry_bytes());
            }
            if shard.by_id.remove(&resolution.series_id).is_some() {
                let removed_series_bytes =
                    Self::series_label_pairs_memory_bytes(&resolution.label_pairs);
                shard.estimated_series_bytes = shard
                    .estimated_series_bytes
                    .saturating_sub(removed_series_bytes);
                self.remove_series_from_indexes_locked(
                    resolution.metric_id,
                    resolution.series_id,
                    &resolution.label_pairs,
                );
                self.series_id_shards[Self::series_id_index_shard_idx(resolution.series_id)]
                    .write()
                    .series_id_to_registry_shard
                    .remove(&resolution.series_id);
                self.series_count.fetch_sub(1, Ordering::AcqRel);
                self.sub_estimated_memory_bytes(removed_series_bytes);
                removed_any = true;
            }
        }
        if removed_any {
            self.bump_postings_generation();
        }
    }

    pub fn register_series_with_id(
        &self,
        series_id: SeriesId,
        metric: &str,
        labels: &[Label],
    ) -> Result<SeriesResolution> {
        validate_metric(metric)?;
        let normalized_labels = canonicalize_labels(labels)?;

        let metric_id = self.intern_metric_id(metric)?;
        let label_pairs = self.intern_label_pairs(&normalized_labels)?;
        let key = SeriesKeyIds {
            metric_id,
            label_pairs: label_pairs.clone(),
        };
        let shard_idx = Self::key_shard_idx_for_key(&key);
        let mut shard = self.series_shards[shard_idx].write();

        if let Some(existing_id) = shard.by_key.get(&key) {
            if *existing_id != series_id {
                return Err(TsinkError::DataCorruption(format!(
                    "series key already bound to id {}, replay tried to bind {}",
                    existing_id, series_id
                )));
            }
            return Ok(SeriesResolution {
                series_id,
                metric_id,
                label_pairs,
                created: false,
            });
        }

        if let Some(existing) = self.get_by_id(series_id) {
            if existing.metric_id != metric_id || existing.label_pairs != label_pairs {
                return Err(TsinkError::DataCorruption(format!(
                    "series id {} already exists with a different series definition",
                    series_id
                )));
            }
            return Ok(SeriesResolution {
                series_id,
                metric_id,
                label_pairs,
                created: false,
            });
        }

        shard.by_id.insert(
            series_id,
            SeriesDefinition {
                series_id,
                metric_id,
                label_pairs: label_pairs.clone(),
            },
        );
        let added_series_bytes = Self::series_label_pairs_memory_bytes(&label_pairs);
        shard.estimated_series_bytes = shard
            .estimated_series_bytes
            .saturating_add(added_series_bytes);
        self.series_id_shards[Self::series_id_index_shard_idx(series_id)]
            .write()
            .series_id_to_registry_shard
            .insert(series_id, shard_idx);
        self.index_series_locked(metric_id, series_id, &label_pairs);
        shard.by_key.insert(key, series_id);
        self.series_count.fetch_add(1, Ordering::AcqRel);
        self.add_estimated_memory_bytes(added_series_bytes);
        self.reserve_series_id(series_id)?;
        self.bump_postings_generation();

        Ok(SeriesResolution {
            series_id,
            metric_id,
            label_pairs,
            created: true,
        })
    }

    pub fn get_by_id(&self, series_id: SeriesId) -> Option<SeriesDefinition> {
        let shard_idx = self.load_series_registry_shard_idx(series_id)?;
        self.series_shards[shard_idx]
            .read()
            .by_id
            .get(&series_id)
            .cloned()
    }

    pub fn all_series_ids(&self) -> Vec<SeriesId> {
        let mut series_ids = Vec::new();
        self.for_each_series_id(|series_id| series_ids.push(series_id));
        series_ids.sort_unstable();
        series_ids
    }

    pub fn series_count(&self) -> usize {
        self.series_count.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.series_count() == 0
    }

    pub fn reserve_series_id(&self, series_id: SeriesId) -> Result<()> {
        let required_next = series_id.checked_add(1).ok_or_else(|| {
            TsinkError::InvalidConfiguration("series id space exhausted (u64 overflow)".to_string())
        })?;
        let mut current = self.next_series_id.load(Ordering::Acquire).max(1);
        while current < required_next {
            match self.next_series_id.compare_exchange(
                current,
                required_next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed.max(1),
            }
        }
        Ok(())
    }

    pub fn retain_series_ids(&self, keep: &BTreeSet<SeriesId>) {
        let mut retained_max = 0u64;
        let mut retained_count = 0usize;
        for shard in &self.series_shards {
            let mut shard = shard.write();
            shard.by_id.retain(|series_id, _| keep.contains(series_id));
            shard.by_key.retain(|_, series_id| keep.contains(series_id));
            shard
                .value_families
                .retain(|series_id, _| keep.contains(series_id));
            retained_count = retained_count.saturating_add(shard.by_id.len());
            retained_max = retained_max.max(shard.by_id.keys().copied().max().unwrap_or(0));
            let label_pair_bytes = shard.by_id.values().fold(0usize, |acc, series| {
                acc.saturating_add(Self::series_label_pairs_memory_bytes(&series.label_pairs))
            });
            let value_family_bytes = shard
                .value_families
                .len()
                .saturating_mul(Self::value_family_entry_bytes());
            shard.estimated_series_bytes = label_pair_bytes.saturating_add(value_family_bytes);
        }
        for shard in &self.series_id_shards {
            shard
                .write()
                .series_id_to_registry_shard
                .retain(|series_id, _| keep.contains(series_id));
        }
        self.series_count.store(retained_count, Ordering::Release);
        self.rebuild_postings_indexes();
        if retained_max > 0 {
            let _ = self.reserve_series_id(retained_max);
        }
    }

    pub fn merge_from(&self, other: &SeriesRegistry) -> Result<usize> {
        let mut created = 0usize;
        for series_id in other.all_series_ids() {
            let Some(series_key) = other.decode_series_key(series_id) else {
                continue;
            };
            let resolution =
                self.register_series_with_id(series_id, &series_key.metric, &series_key.labels)?;
            created = created.saturating_add(usize::from(resolution.created));
            if let Some(family) = other.series_value_family(series_id) {
                self.record_series_value_family(series_id, family)?;
            }
        }
        self.reserve_series_id(other.next_series_id_value().saturating_sub(1))?;
        Ok(created)
    }

    pub fn series_subset<I>(&self, series_ids: I) -> Result<Self>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let subset = Self::new();
        let mut series_ids = series_ids.into_iter().collect::<Vec<_>>();
        if series_ids.is_empty() {
            return Ok(subset);
        }
        series_ids.sort_unstable();
        series_ids.dedup();

        for series_id in series_ids {
            let Some(series_key) = self.decode_series_key(series_id) else {
                continue;
            };
            subset.register_series_with_id(series_id, &series_key.metric, &series_key.labels)?;
            if let Some(family) = self.series_value_family(series_id) {
                subset.record_series_value_family(series_id, family)?;
            }
        }
        subset.reserve_series_id(self.next_series_id_value().saturating_sub(1))?;
        Ok(subset)
    }
}
