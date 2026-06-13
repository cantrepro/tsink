use std::collections::{HashMap, HashSet};

use super::*;
use crate::validation::{validate_labels, validate_metric};
use crate::{Label, Result, TsinkError};

impl StringDictionary {
    pub(super) fn intern(&mut self, value: &str, dict_name: &str) -> Result<DictionaryId> {
        if let Some(id) = self.by_value.get(value) {
            return Ok(*id);
        }

        let id = DictionaryId::try_from(self.by_id.len()).map_err(|_| {
            TsinkError::InvalidConfiguration(format!(
                "{dict_name} dictionary exhausted u32 id space"
            ))
        })?;

        let owned = value.to_string();
        self.by_value.insert(owned.clone(), id);
        self.estimated_heap_bytes = self
            .estimated_heap_bytes
            .saturating_add(owned.capacity().saturating_mul(2));
        self.by_id.push(owned);

        Ok(id)
    }

    pub(super) fn get_id(&self, value: &str) -> Option<DictionaryId> {
        self.by_value.get(value).copied()
    }

    pub(super) fn get_value(&self, id: DictionaryId) -> Option<&str> {
        self.by_id.get(id as usize).map(|value| value.as_str())
    }

    pub(super) fn len(&self) -> usize {
        self.by_id.len()
    }

    pub(super) fn memory_usage_bytes(&self) -> usize {
        self.estimated_heap_bytes
    }

    pub(super) fn entries(&self) -> impl Iterator<Item = (DictionaryId, &str)> {
        self.by_id
            .iter()
            .enumerate()
            .map(|(idx, value)| (idx as DictionaryId, value.as_str()))
    }

    pub(super) fn from_values(values: Vec<String>, dict_name: &str) -> Result<Self> {
        if values.len() > DictionaryId::MAX as usize + 1 {
            return Err(TsinkError::InvalidConfiguration(format!(
                "{dict_name} dictionary exceeded u32 id space"
            )));
        }

        let mut by_value = HashMap::with_capacity(values.len());
        for (idx, value) in values.iter().enumerate() {
            by_value.insert(value.clone(), idx as DictionaryId);
        }
        let estimated_heap_bytes = values.iter().fold(0usize, |acc, value| {
            acc.saturating_add(value.capacity().saturating_mul(2))
        });

        Ok(Self {
            by_value,
            by_id: values,
            estimated_heap_bytes,
        })
    }
}

impl SeriesRegistry {
    pub(super) fn intern_metric_id(&self, metric: &str) -> Result<DictionaryId> {
        if let Some(existing) = self.metric_dict.read().get_id(metric) {
            return Ok(existing);
        }

        let mut metric_dict = self.metric_dict.write();
        let before = metric_dict.memory_usage_bytes();
        let metric_id = metric_dict.intern(metric, "metric")?;
        let after = metric_dict.memory_usage_bytes();
        drop(metric_dict);
        self.add_estimated_memory_bytes(after.saturating_sub(before));
        Ok(metric_id)
    }

    pub(super) fn intern_label_pairs(&self, labels: &[Label]) -> Result<Vec<LabelPairId>> {
        validate_labels(labels)?;

        let mut pairs = Vec::with_capacity(labels.len());
        for label in labels {
            let existing_name_id = self.label_name_dict.read().get_id(&label.name);
            let name_id = if let Some(existing) = existing_name_id {
                existing
            } else {
                let mut label_name_dict = self.label_name_dict.write();
                let before = label_name_dict.memory_usage_bytes();
                let name_id = label_name_dict.intern(&label.name, "label name")?;
                let after = label_name_dict.memory_usage_bytes();
                drop(label_name_dict);
                self.add_estimated_memory_bytes(after.saturating_sub(before));
                name_id
            };
            let existing_value_id = self.label_value_dict.read().get_id(&label.value);
            let value_id = if let Some(existing) = existing_value_id {
                existing
            } else {
                let mut label_value_dict = self.label_value_dict.write();
                let before = label_value_dict.memory_usage_bytes();
                let value_id = label_value_dict.intern(&label.value, "label value")?;
                let after = label_value_dict.memory_usage_bytes();
                drop(label_value_dict);
                self.add_estimated_memory_bytes(after.saturating_sub(before));
                value_id
            };
            pairs.push(LabelPairId { name_id, value_id });
        }
        pairs.sort_unstable();
        Ok(pairs)
    }

    pub(super) fn lookup_label_pairs(&self, labels: &[Label]) -> Option<Vec<LabelPairId>> {
        validate_labels(labels).ok()?;

        let label_name_dict = self.label_name_dict.read();
        let label_value_dict = self.label_value_dict.read();
        let mut pairs = Vec::with_capacity(labels.len());
        for label in labels {
            let name_id = label_name_dict.get_id(&label.name)?;
            let value_id = label_value_dict.get_id(&label.value)?;
            pairs.push(LabelPairId { name_id, value_id });
        }
        pairs.sort_unstable();
        Some(pairs)
    }

    pub(super) fn estimate_string_dictionary_entry(
        dict: &StringDictionary,
        pending: &mut HashMap<String, DictionaryId>,
        next_len: &mut usize,
        value: &str,
        dict_name: &str,
    ) -> Result<(DictionaryId, usize)> {
        if let Some(existing) = dict.get_id(value) {
            return Ok((existing, 0));
        }
        if let Some(existing) = pending.get(value) {
            return Ok((*existing, 0));
        }

        let id = DictionaryId::try_from(*next_len).map_err(|_| {
            TsinkError::InvalidConfiguration(format!(
                "{dict_name} dictionary exhausted u32 id space"
            ))
        })?;
        *next_len = next_len.saturating_add(1);
        pending.insert(value.to_string(), id);
        Ok((id, value.len().saturating_mul(2)))
    }

    pub(crate) fn prime_dictionaries_for_series(&self, series: &[SeriesKey]) -> Result<()> {
        if series.is_empty() {
            return Ok(());
        }

        let mut metric_values = HashSet::<&str>::new();
        let mut label_names = HashSet::<&str>::new();
        let mut label_values = HashSet::<&str>::new();

        for key in series {
            validate_metric(&key.metric)?;
            validate_labels(&key.labels)?;
            metric_values.insert(key.metric.as_str());
            for label in &key.labels {
                label_names.insert(label.name.as_str());
                label_values.insert(label.value.as_str());
            }
        }

        {
            let mut dict = self.metric_dict.write();
            let before = dict.memory_usage_bytes();
            for value in metric_values {
                dict.intern(value, "metric")?;
            }
            let after = dict.memory_usage_bytes();
            self.add_estimated_memory_bytes(after.saturating_sub(before));
        }
        {
            let mut dict = self.label_name_dict.write();
            let before = dict.memory_usage_bytes();
            for value in label_names {
                dict.intern(value, "label name")?;
            }
            let after = dict.memory_usage_bytes();
            self.add_estimated_memory_bytes(after.saturating_sub(before));
        }
        {
            let mut dict = self.label_value_dict.write();
            let before = dict.memory_usage_bytes();
            for value in label_values {
                dict.intern(value, "label value")?;
            }
            let after = dict.memory_usage_bytes();
            self.add_estimated_memory_bytes(after.saturating_sub(before));
        }

        Ok(())
    }

    pub fn metric_id(&self, metric: &str) -> Option<DictionaryId> {
        self.metric_dict.read().get_id(metric)
    }

    pub fn label_name_id(&self, label_name: &str) -> Option<DictionaryId> {
        self.label_name_dict.read().get_id(label_name)
    }

    pub fn label_value_id(&self, label_value: &str) -> Option<DictionaryId> {
        self.label_value_dict.read().get_id(label_value)
    }

    pub fn label_value_by_id(&self, value_id: DictionaryId) -> Option<String> {
        self.label_value_dict
            .read()
            .get_value(value_id)
            .map(str::to_string)
    }

    pub fn metric_entries(&self) -> Vec<(DictionaryId, String)> {
        self.metric_dict
            .read()
            .entries()
            .map(|(id, value)| (id, value.to_string()))
            .collect()
    }

    pub fn label_name_entries(&self) -> Vec<(DictionaryId, String)> {
        self.label_name_dict
            .read()
            .entries()
            .map(|(id, value)| (id, value.to_string()))
            .collect()
    }

    pub fn label_value_entries(&self) -> Vec<(DictionaryId, String)> {
        self.label_value_dict
            .read()
            .entries()
            .map(|(id, value)| (id, value.to_string()))
            .collect()
    }

    pub fn metric_dictionary_len(&self) -> usize {
        self.metric_dict.read().len()
    }

    pub fn label_name_dictionary_len(&self) -> usize {
        self.label_name_dict.read().len()
    }

    pub fn label_value_dictionary_len(&self) -> usize {
        self.label_value_dict.read().len()
    }

    pub fn decode_series_key(&self, series_id: SeriesId) -> Option<SeriesKey> {
        let definition = self.get_by_id(series_id)?;
        let metric = self
            .metric_dict
            .read()
            .get_value(definition.metric_id)?
            .to_string();
        let label_name_dict = self.label_name_dict.read();
        let label_value_dict = self.label_value_dict.read();
        let mut labels = Vec::with_capacity(definition.label_pairs.len());
        for pair in &definition.label_pairs {
            let name = label_name_dict.get_value(pair.name_id)?;
            let value = label_value_dict.get_value(pair.value_id)?;
            labels.push(Label::new(name, value));
        }
        labels.sort();
        Some(SeriesKey { metric, labels })
    }

    pub fn series_metric(&self, series_id: SeriesId) -> Option<String> {
        let definition = self.get_by_id(series_id)?;
        self.metric_dict
            .read()
            .get_value(definition.metric_id)
            .map(str::to_string)
    }

    pub fn series_label_value_by_name_id(
        &self,
        series_id: SeriesId,
        name_id: DictionaryId,
    ) -> Option<String> {
        let definition = self.get_by_id(series_id)?;
        let idx = definition
            .label_pairs
            .binary_search_by_key(&name_id, |pair| pair.name_id)
            .ok()?;
        self.label_value_dict
            .read()
            .get_value(definition.label_pairs.get(idx)?.value_id)
            .map(str::to_string)
    }
}
