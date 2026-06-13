use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::{Label, Result, Row, TsinkError};
use roaring::RoaringTreemap;

pub type SeriesId = u64;
pub type DictionaryId = u32;

const REGISTRY_INDEX_MAGIC: [u8; 4] = *b"RIDX";
const REGISTRY_INDEX_VERSION: u16 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LabelPairId {
    pub name_id: DictionaryId,
    pub value_id: DictionaryId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SeriesKeyIds {
    metric_id: DictionaryId,
    label_pairs: Vec<LabelPairId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesDefinition {
    pub series_id: SeriesId,
    pub metric_id: DictionaryId,
    pub label_pairs: Vec<LabelPairId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesKey {
    pub metric: String,
    pub labels: Vec<Label>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesResolution {
    pub series_id: SeriesId,
    pub metric_id: DictionaryId,
    pub label_pairs: Vec<LabelPairId>,
    pub created: bool,
}

#[derive(Debug, Default)]
struct StringDictionary {
    by_value: HashMap<String, DictionaryId>,
    by_id: Vec<String>,
}

impl StringDictionary {
    fn intern(&mut self, value: &str, dict_name: &str) -> Result<DictionaryId> {
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
        self.by_id.push(owned);

        Ok(id)
    }

    fn get_id(&self, value: &str) -> Option<DictionaryId> {
        self.by_value.get(value).copied()
    }

    fn get_value(&self, id: DictionaryId) -> Option<&str> {
        self.by_id.get(id as usize).map(|value| value.as_str())
    }

    fn len(&self) -> usize {
        self.by_id.len()
    }

    fn entries(&self) -> impl Iterator<Item = (DictionaryId, &str)> {
        self.by_id
            .iter()
            .enumerate()
            .map(|(idx, value)| (idx as DictionaryId, value.as_str()))
    }

    fn from_values(values: Vec<String>, dict_name: &str) -> Result<Self> {
        if values.len() > DictionaryId::MAX as usize + 1 {
            return Err(TsinkError::InvalidConfiguration(format!(
                "{dict_name} dictionary exceeded u32 id space"
            )));
        }

        let mut by_value = HashMap::with_capacity(values.len());
        for (idx, value) in values.iter().enumerate() {
            by_value.insert(value.clone(), idx as DictionaryId);
        }

        Ok(Self {
            by_value,
            by_id: values,
        })
    }
}

#[derive(Debug, Default)]
pub struct SeriesRegistry {
    next_series_id: SeriesId,
    metric_dict: StringDictionary,
    label_name_dict: StringDictionary,
    label_value_dict: StringDictionary,
    by_key: HashMap<SeriesKeyIds, SeriesId>,
    by_id: HashMap<SeriesId, SeriesDefinition>,
    metric_postings: HashMap<DictionaryId, RoaringTreemap>,
    postings: BTreeMap<LabelPairId, RoaringTreemap>,
}

pub(crate) fn validate_metric(metric: &str) -> Result<()> {
    if metric.is_empty() {
        return Err(TsinkError::MetricRequired);
    }
    if metric.len() > u16::MAX as usize {
        return Err(TsinkError::InvalidMetricName(format!(
            "metric name too long: {} bytes (max {})",
            metric.len(),
            u16::MAX as usize
        )));
    }
    Ok(())
}

pub(crate) fn validate_labels(labels: &[Label]) -> Result<()> {
    for label in labels {
        if !label.is_valid() {
            return Err(TsinkError::InvalidLabel(
                "label name and value must be non-empty".to_string(),
            ));
        }
        if label.name.len() > crate::label::MAX_LABEL_NAME_LEN
            || label.value.len() > crate::label::MAX_LABEL_VALUE_LEN
        {
            return Err(TsinkError::InvalidLabel(format!(
                "label name/value must be within limits (name <= {}, value <= {})",
                crate::label::MAX_LABEL_NAME_LEN,
                crate::label::MAX_LABEL_VALUE_LEN
            )));
        }
    }
    Ok(())
}

impl SeriesRegistry {
    pub fn new() -> Self {
        Self {
            next_series_id: 1,
            ..Self::default()
        }
    }

    pub fn resolve_or_insert(
        &mut self,
        metric: &str,
        labels: &[Label],
    ) -> Result<SeriesResolution> {
        validate_metric(metric)?;
        validate_labels(labels)?;

        let metric_id = self.metric_dict.intern(metric, "metric")?;
        let label_pairs = self.intern_label_pairs(labels)?;

        let key = SeriesKeyIds {
            metric_id,
            label_pairs: label_pairs.clone(),
        };

        if let Some(existing) = self.by_key.get(&key) {
            return Ok(SeriesResolution {
                series_id: *existing,
                metric_id,
                label_pairs,
                created: false,
            });
        }

        let series_id = self.alloc_series_id()?;
        let definition = SeriesDefinition {
            series_id,
            metric_id,
            label_pairs: label_pairs.clone(),
        };

        self.by_key.insert(key, series_id);
        self.by_id.insert(series_id, definition);
        self.update_metric_postings(metric_id, series_id);
        self.update_postings(series_id, &label_pairs);

        Ok(SeriesResolution {
            series_id,
            metric_id,
            label_pairs,
            created: true,
        })
    }

    pub fn resolve_existing(&self, metric: &str, labels: &[Label]) -> Option<SeriesResolution> {
        let metric_id = self.metric_dict.get_id(metric)?;
        let label_pairs = self.lookup_label_pairs(labels)?;
        let key = SeriesKeyIds {
            metric_id,
            label_pairs: label_pairs.clone(),
        };

        let series_id = *self.by_key.get(&key)?;
        Some(SeriesResolution {
            series_id,
            metric_id,
            label_pairs,
            created: false,
        })
    }

    pub fn resolve_rows(&mut self, rows: &[Row]) -> Result<Vec<SeriesResolution>> {
        let mut resolutions = Vec::with_capacity(rows.len());
        for row in rows {
            resolutions.push(self.resolve_or_insert(row.metric(), row.labels())?);
        }
        Ok(resolutions)
    }

    pub(crate) fn rollback_created_series(&mut self, created: &[SeriesResolution]) {
        if created.is_empty() {
            return;
        }

        for resolution in created {
            self.by_id.remove(&resolution.series_id);
            self.by_key.remove(&SeriesKeyIds {
                metric_id: resolution.metric_id,
                label_pairs: resolution.label_pairs.clone(),
            });

            let remove_metric_posting =
                if let Some(series_ids) = self.metric_postings.get_mut(&resolution.metric_id) {
                    series_ids.remove(resolution.series_id);
                    series_ids.is_empty()
                } else {
                    false
                };
            if remove_metric_posting {
                self.metric_postings.remove(&resolution.metric_id);
            }

            for pair in &resolution.label_pairs {
                let remove_posting = if let Some(series_ids) = self.postings.get_mut(pair) {
                    series_ids.remove(resolution.series_id);
                    series_ids.is_empty()
                } else {
                    false
                };
                if remove_posting {
                    self.postings.remove(pair);
                }
            }
        }
    }

    pub fn register_series_with_id(
        &mut self,
        series_id: SeriesId,
        metric: &str,
        labels: &[Label],
    ) -> Result<SeriesResolution> {
        validate_metric(metric)?;
        validate_labels(labels)?;

        let metric_id = self.metric_dict.intern(metric, "metric")?;
        let label_pairs = self.intern_label_pairs(labels)?;
        let key = SeriesKeyIds {
            metric_id,
            label_pairs: label_pairs.clone(),
        };

        if let Some(existing_id) = self.by_key.get(&key) {
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

        if let Some(existing) = self.by_id.get(&series_id) {
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

        let definition = SeriesDefinition {
            series_id,
            metric_id,
            label_pairs: label_pairs.clone(),
        };
        self.by_key.insert(key, series_id);
        self.by_id.insert(series_id, definition);
        self.update_metric_postings(metric_id, series_id);
        self.update_postings(series_id, &label_pairs);

        self.next_series_id =
            self.next_series_id
                .max(series_id.checked_add(1).ok_or_else(|| {
                    TsinkError::InvalidConfiguration(
                        "series id space exhausted (u64 overflow)".to_string(),
                    )
                })?);

        Ok(SeriesResolution {
            series_id,
            metric_id,
            label_pairs,
            created: true,
        })
    }

    pub fn series_ids_for_metric(&self, metric: &str) -> Vec<SeriesId> {
        let Some(metric_id) = self.metric_dict.get_id(metric) else {
            return Vec::new();
        };

        self.metric_postings
            .get(&metric_id)
            .map(|ids| ids.iter().collect())
            .unwrap_or_default()
    }

    pub fn series_id_postings_for_metric(&self, metric: &str) -> Option<&RoaringTreemap> {
        let metric_id = self.metric_dict.get_id(metric)?;
        self.metric_postings.get(&metric_id)
    }

    pub fn all_series_ids(&self) -> Vec<SeriesId> {
        let mut ids = self.by_id.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub fn get_by_id(&self, series_id: SeriesId) -> Option<&SeriesDefinition> {
        self.by_id.get(&series_id)
    }

    pub fn decode_series_key(&self, series_id: SeriesId) -> Option<SeriesKey> {
        let series = self.by_id.get(&series_id)?;
        let metric = self.metric_dict.get_value(series.metric_id)?.to_string();

        let mut labels = Vec::with_capacity(series.label_pairs.len());
        for pair in &series.label_pairs {
            let name = self.label_name_dict.get_value(pair.name_id)?;
            let value = self.label_value_dict.get_value(pair.value_id)?;
            labels.push(Label::new(name, value));
        }
        labels.sort();

        Some(SeriesKey { metric, labels })
    }

    pub fn postings_for_ids(&self, pair: LabelPairId) -> Option<&RoaringTreemap> {
        self.postings.get(&pair)
    }

    pub fn postings_for_label(
        &self,
        label_name: &str,
        label_value: &str,
    ) -> Option<&RoaringTreemap> {
        let name_id = self.label_name_dict.get_id(label_name)?;
        let value_id = self.label_value_dict.get_id(label_value)?;
        self.postings_for_ids(LabelPairId { name_id, value_id })
    }

    pub fn metric_id(&self, metric: &str) -> Option<DictionaryId> {
        self.metric_dict.get_id(metric)
    }

    pub fn label_name_id(&self, label_name: &str) -> Option<DictionaryId> {
        self.label_name_dict.get_id(label_name)
    }

    pub fn label_value_id(&self, label_value: &str) -> Option<DictionaryId> {
        self.label_value_dict.get_id(label_value)
    }

    pub fn label_value_by_id(&self, value_id: DictionaryId) -> Option<&str> {
        self.label_value_dict.get_value(value_id)
    }

    pub fn metric_entries(&self) -> impl Iterator<Item = (DictionaryId, &str)> {
        self.metric_dict.entries()
    }

    pub fn metric_postings_entries(&self) -> impl Iterator<Item = (&str, &RoaringTreemap)> {
        self.metric_postings
            .iter()
            .filter_map(|(metric_id, series_ids)| {
                self.metric_dict
                    .get_value(*metric_id)
                    .map(|metric| (metric, series_ids))
            })
    }

    pub fn label_name_entries(&self) -> impl Iterator<Item = (DictionaryId, &str)> {
        self.label_name_dict.entries()
    }

    pub fn label_value_entries(&self) -> impl Iterator<Item = (DictionaryId, &str)> {
        self.label_value_dict.entries()
    }

    pub fn postings_entries(&self) -> impl Iterator<Item = (LabelPairId, &RoaringTreemap)> {
        self.postings.iter().map(|(pair, ids)| (*pair, ids))
    }

    pub fn series_count(&self) -> usize {
        self.by_id.len()
    }

    pub fn metric_dictionary_len(&self) -> usize {
        self.metric_dict.len()
    }

    pub fn label_name_dictionary_len(&self) -> usize {
        self.label_name_dict.len()
    }

    pub fn label_value_dictionary_len(&self) -> usize {
        self.label_value_dict.len()
    }

    pub fn postings_count(&self) -> usize {
        self.postings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    fn intern_label_pairs(&mut self, labels: &[Label]) -> Result<Vec<LabelPairId>> {
        let mut pairs = Vec::with_capacity(labels.len());
        for label in labels {
            let name_id = self.label_name_dict.intern(&label.name, "label name")?;
            let value_id = self.label_value_dict.intern(&label.value, "label value")?;
            pairs.push(LabelPairId { name_id, value_id });
        }
        pairs.sort_unstable();
        pairs.dedup();
        Ok(pairs)
    }

    fn lookup_label_pairs(&self, labels: &[Label]) -> Option<Vec<LabelPairId>> {
        let mut pairs = Vec::with_capacity(labels.len());
        for label in labels {
            let name_id = self.label_name_dict.get_id(&label.name)?;
            let value_id = self.label_value_dict.get_id(&label.value)?;
            pairs.push(LabelPairId { name_id, value_id });
        }
        pairs.sort_unstable();
        pairs.dedup();
        Some(pairs)
    }

    fn update_postings(&mut self, series_id: SeriesId, label_pairs: &[LabelPairId]) {
        for pair in label_pairs {
            self.postings.entry(*pair).or_default().insert(series_id);
        }
    }

    fn update_metric_postings(&mut self, metric_id: DictionaryId, series_id: SeriesId) {
        self.metric_postings
            .entry(metric_id)
            .or_default()
            .insert(series_id);
    }

    fn rebuild_postings_indexes(&mut self) {
        self.metric_postings.clear();
        self.postings.clear();

        for series in self.by_id.values() {
            self.metric_postings
                .entry(series.metric_id)
                .or_default()
                .insert(series.series_id);
            for pair in &series.label_pairs {
                self.postings
                    .entry(*pair)
                    .or_default()
                    .insert(series.series_id);
            }
        }
    }

    pub fn retain_series_ids(&mut self, keep: &BTreeSet<SeriesId>) {
        self.by_id.retain(|series_id, _| keep.contains(series_id));
        self.by_key.retain(|_, series_id| keep.contains(series_id));
        self.rebuild_postings_indexes();

        let next = self
            .by_id
            .keys()
            .copied()
            .max()
            .and_then(|max_id| max_id.checked_add(1))
            .unwrap_or(1);
        self.next_series_id = next;
    }

    pub fn persist_to_path(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&REGISTRY_INDEX_MAGIC);
        bytes.extend_from_slice(&REGISTRY_INDEX_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&self.next_series_id.to_le_bytes());
        bytes.extend_from_slice(&(self.metric_dict.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.label_name_dict.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.label_value_dict.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.by_id.len() as u64).to_le_bytes());
        // Reserved for future index sections.
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());

        for (id, value) in self.metric_dict.entries() {
            write_dict_entry(&mut bytes, id, value)?;
        }
        for (id, value) in self.label_name_dict.entries() {
            write_dict_entry(&mut bytes, id, value)?;
        }
        for (id, value) in self.label_value_dict.entries() {
            write_dict_entry(&mut bytes, id, value)?;
        }

        let mut series = self.by_id.values().cloned().collect::<Vec<_>>();
        series.sort_by_key(|entry| entry.series_id);
        for entry in &series {
            bytes.extend_from_slice(&entry.series_id.to_le_bytes());
            bytes.extend_from_slice(&entry.metric_id.to_le_bytes());
            let pair_count = u16::try_from(entry.label_pairs.len()).map_err(|_| {
                TsinkError::InvalidConfiguration(
                    "series label pair count exceeds u16 in registry index".to_string(),
                )
            })?;
            bytes.extend_from_slice(&pair_count.to_le_bytes());
            bytes.extend_from_slice(&0u16.to_le_bytes());
            for pair in &entry.label_pairs {
                bytes.extend_from_slice(&pair.name_id.to_le_bytes());
                bytes.extend_from_slice(&pair.value_id.to_le_bytes());
            }
        }

        let tmp_path = path.with_extension("tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, path)?;
        if let Some(parent) = path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }

    pub fn load_from_path(path: &Path) -> Result<Self> {
        let bytes = fs::read(path)?;
        if bytes.len() < 52 {
            return Err(TsinkError::DataCorruption(
                "series index file is too short".to_string(),
            ));
        }

        let mut pos = 0usize;
        let magic = read_array::<4>(&bytes, &mut pos)?;
        if magic != REGISTRY_INDEX_MAGIC {
            return Err(TsinkError::DataCorruption(
                "series index magic mismatch".to_string(),
            ));
        }

        let version = read_u16(&bytes, &mut pos)?;
        if version != REGISTRY_INDEX_VERSION {
            return Err(TsinkError::DataCorruption(format!(
                "unsupported series index version {version}"
            )));
        }

        let _flags = read_u16(&bytes, &mut pos)?;
        let next_series_id = read_u64(&bytes, &mut pos)?;
        let metric_count = read_u32(&bytes, &mut pos)? as usize;
        let label_name_count = read_u32(&bytes, &mut pos)? as usize;
        let label_value_count = read_u32(&bytes, &mut pos)? as usize;
        let series_count = read_u64(&bytes, &mut pos)? as usize;
        let _reserved_metric_postings_count = read_u64(&bytes, &mut pos)? as usize;
        let _reserved_postings_count = read_u64(&bytes, &mut pos)? as usize;

        let metric_values = parse_dictionary(&bytes, &mut pos, metric_count)?;
        let label_name_values = parse_dictionary(&bytes, &mut pos, label_name_count)?;
        let label_value_values = parse_dictionary(&bytes, &mut pos, label_value_count)?;

        let metric_dict = StringDictionary::from_values(metric_values, "metric")?;
        let label_name_dict = StringDictionary::from_values(label_name_values, "label name")?;
        let label_value_dict = StringDictionary::from_values(label_value_values, "label value")?;

        let mut by_id = HashMap::<SeriesId, SeriesDefinition>::with_capacity(series_count);
        let mut by_key = HashMap::<SeriesKeyIds, SeriesId>::with_capacity(series_count);
        for _ in 0..series_count {
            let series_id = read_u64(&bytes, &mut pos)?;
            let metric_id = read_u32(&bytes, &mut pos)?;
            let pair_count = read_u16(&bytes, &mut pos)? as usize;
            let _reserved = read_u16(&bytes, &mut pos)?;
            let mut label_pairs = Vec::with_capacity(pair_count);
            for _ in 0..pair_count {
                let name_id = read_u32(&bytes, &mut pos)?;
                let value_id = read_u32(&bytes, &mut pos)?;
                label_pairs.push(LabelPairId { name_id, value_id });
            }
            label_pairs.sort_unstable();
            label_pairs.dedup();

            let key = SeriesKeyIds {
                metric_id,
                label_pairs: label_pairs.clone(),
            };
            by_key.insert(key, series_id);
            by_id.insert(
                series_id,
                SeriesDefinition {
                    series_id,
                    metric_id,
                    label_pairs,
                },
            );
        }

        if pos != bytes.len() {
            return Err(TsinkError::DataCorruption(
                "series index has trailing bytes".to_string(),
            ));
        }

        let mut registry = Self {
            next_series_id: next_series_id.max(1),
            metric_dict,
            label_name_dict,
            label_value_dict,
            by_key,
            by_id,
            metric_postings: HashMap::new(),
            postings: BTreeMap::new(),
        };

        registry.rebuild_postings_indexes();
        if registry.next_series_id == 0 {
            registry.next_series_id = 1;
        }
        Ok(registry)
    }

    fn alloc_series_id(&mut self) -> Result<SeriesId> {
        let id = self.next_series_id;
        self.next_series_id = self.next_series_id.checked_add(1).ok_or_else(|| {
            TsinkError::InvalidConfiguration("series id space exhausted (u64 overflow)".to_string())
        })?;
        Ok(id)
    }
}

fn write_dict_entry(out: &mut Vec<u8>, id: u32, value: &str) -> Result<()> {
    let value_bytes = value.as_bytes();
    let len = u32::try_from(value_bytes.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("registry dictionary string exceeds u32".to_string())
    })?;
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value_bytes);
    Ok(())
}

fn parse_dictionary(bytes: &[u8], pos: &mut usize, count: usize) -> Result<Vec<String>> {
    let mut values = Vec::with_capacity(count);
    for expected_id in 0..count {
        let id = read_u32(bytes, pos)? as usize;
        if id != expected_id {
            return Err(TsinkError::DataCorruption(format!(
                "registry dictionary id {} is not dense at expected {}",
                id, expected_id
            )));
        }

        let len = read_u32(bytes, pos)? as usize;
        let value_bytes = read_bytes(bytes, pos, len)?;
        let value = String::from_utf8(value_bytes.to_vec())?;
        values.push(value);
    }
    Ok(values)
}

fn read_u16(bytes: &[u8], pos: &mut usize) -> Result<u16> {
    let mut raw = [0u8; 2];
    raw.copy_from_slice(read_bytes(bytes, pos, 2)?);
    Ok(u16::from_le_bytes(raw))
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> Result<u32> {
    let mut raw = [0u8; 4];
    raw.copy_from_slice(read_bytes(bytes, pos, 4)?);
    Ok(u32::from_le_bytes(raw))
}

fn read_u64(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(read_bytes(bytes, pos, 8)?);
    Ok(u64::from_le_bytes(raw))
}

fn read_array<const N: usize>(bytes: &[u8], pos: &mut usize) -> Result<[u8; N]> {
    let mut raw = [0u8; N];
    raw.copy_from_slice(read_bytes(bytes, pos, N)?);
    Ok(raw)
}

fn read_bytes<'a>(bytes: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = pos.saturating_add(len);
    if end > bytes.len() {
        return Err(TsinkError::DataCorruption(format!(
            "registry payload truncated: need {} bytes, have {}",
            len,
            bytes.len().saturating_sub(*pos)
        )));
    }
    let out = &bytes[*pos..end];
    *pos = end;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::SeriesRegistry;
    use crate::{DataPoint, Label, Row, TsinkError};

    #[test]
    fn resolves_same_series_to_same_id_regardless_of_label_order() {
        let mut registry = SeriesRegistry::new();

        let r1 = registry
            .resolve_or_insert(
                "cpu",
                &[Label::new("host", "a"), Label::new("region", "use1")],
            )
            .unwrap();
        let r2 = registry
            .resolve_or_insert(
                "cpu",
                &[Label::new("region", "use1"), Label::new("host", "a")],
            )
            .unwrap();

        assert_eq!(r1.series_id, r2.series_id);
        assert!(r1.created);
        assert!(!r2.created);
        assert_eq!(registry.series_count(), 1);
    }

    #[test]
    fn interns_metric_and_label_dictionaries_once() {
        let mut registry = SeriesRegistry::new();

        for _ in 0..100 {
            registry
                .resolve_or_insert(
                    "http_requests_total",
                    &[
                        Label::new("method", "GET"),
                        Label::new("status", "200"),
                        Label::new("route", "/users"),
                    ],
                )
                .unwrap();
        }

        assert_eq!(registry.series_count(), 1);
        assert_eq!(registry.metric_dictionary_len(), 1);
        assert_eq!(registry.label_name_dictionary_len(), 3);
        assert_eq!(registry.label_value_dictionary_len(), 3);
    }

    #[test]
    fn postings_track_all_series_for_same_label_pair() {
        let mut registry = SeriesRegistry::new();

        let a = registry
            .resolve_or_insert(
                "cpu",
                &[Label::new("host", "h1"), Label::new("region", "use1")],
            )
            .unwrap();
        let b = registry
            .resolve_or_insert(
                "cpu",
                &[Label::new("host", "h2"), Label::new("region", "use1")],
            )
            .unwrap();

        let postings = registry.postings_for_label("region", "use1").unwrap();
        assert_eq!(postings.len(), 2);
        assert!(postings.contains(a.series_id));
        assert!(postings.contains(b.series_id));
    }

    #[test]
    fn resolve_rows_returns_series_ids_for_batch_inserts() {
        let mut registry = SeriesRegistry::new();
        let rows = vec![
            Row::with_labels(
                "latency",
                vec![Label::new("host", "a"), Label::new("path", "/")],
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "latency",
                vec![Label::new("path", "/"), Label::new("host", "a")],
                DataPoint::new(2, 2.0),
            ),
            Row::with_labels(
                "latency",
                vec![Label::new("host", "b"), Label::new("path", "/")],
                DataPoint::new(3, 3.0),
            ),
        ];

        let resolutions = registry.resolve_rows(&rows).unwrap();

        assert_eq!(resolutions.len(), 3);
        assert_eq!(resolutions[0].series_id, resolutions[1].series_id);
        assert_ne!(resolutions[0].series_id, resolutions[2].series_id);
        assert_eq!(registry.series_count(), 2);
    }

    #[test]
    fn resolve_existing_and_series_ids_for_metric_work() {
        let mut registry = SeriesRegistry::new();
        let labels_a = vec![Label::new("host", "a")];
        let labels_b = vec![Label::new("host", "b")];

        let a = registry.resolve_or_insert("cpu", &labels_a).unwrap();
        let b = registry.resolve_or_insert("cpu", &labels_b).unwrap();

        let resolved = registry.resolve_existing("cpu", &labels_a).unwrap();
        assert_eq!(resolved.series_id, a.series_id);

        let ids = registry.series_ids_for_metric("cpu");
        assert_eq!(ids, vec![a.series_id, b.series_id]);
    }

    #[test]
    fn register_series_with_id_restores_series_identity() {
        let mut registry = SeriesRegistry::new();
        let labels = vec![Label::new("host", "a"), Label::new("region", "use1")];

        let registered = registry
            .register_series_with_id(42, "cpu", &labels)
            .unwrap();
        assert_eq!(registered.series_id, 42);
        assert!(registered.created);

        let resolved = registry.resolve_existing("cpu", &labels).unwrap();
        assert_eq!(resolved.series_id, 42);

        let fresh = registry
            .resolve_or_insert("cpu", &[Label::new("host", "b")])
            .unwrap();
        assert!(fresh.series_id > 42);
    }

    #[test]
    fn decode_series_key_recovers_metric_and_labels() {
        let mut registry = SeriesRegistry::new();
        let resolution = registry
            .resolve_or_insert(
                "memory_usage",
                &[Label::new("host", "a"), Label::new("service", "api")],
            )
            .unwrap();

        let decoded = registry.decode_series_key(resolution.series_id).unwrap();
        assert_eq!(decoded.metric, "memory_usage");
        assert_eq!(
            decoded.labels,
            vec![Label::new("host", "a"), Label::new("service", "api")]
        );
    }

    #[test]
    fn rollback_created_series_removes_series_without_rewinding_ids_or_dictionaries() {
        let mut registry = SeriesRegistry::new();
        registry
            .resolve_or_insert("cpu", &[Label::new("host", "a")])
            .unwrap();

        let baseline_series = registry.series_count();
        let baseline_metric_dict_len = registry.metric_dictionary_len();
        let baseline_label_name_dict_len = registry.label_name_dictionary_len();
        let baseline_label_value_dict_len = registry.label_value_dictionary_len();

        let phantom_labels = vec![Label::new("zone", "use1"), Label::new("env", "prod")];
        let created = registry
            .resolve_or_insert("phantom_metric", &phantom_labels)
            .unwrap();
        assert!(created.created);
        assert!(registry
            .resolve_existing("phantom_metric", &phantom_labels)
            .is_some());

        registry.rollback_created_series(std::slice::from_ref(&created));

        assert_eq!(registry.series_count(), baseline_series);
        assert!(registry.metric_dictionary_len() >= baseline_metric_dict_len);
        assert!(registry.label_name_dictionary_len() >= baseline_label_name_dict_len);
        assert!(registry.label_value_dictionary_len() >= baseline_label_value_dict_len);
        assert!(registry
            .resolve_existing("phantom_metric", &phantom_labels)
            .is_none());

        let next = registry
            .resolve_or_insert("cpu", &[Label::new("host", "b")])
            .unwrap();
        assert!(next.series_id > created.series_id);
    }

    #[test]
    fn rejects_invalid_input() {
        let mut registry = SeriesRegistry::new();

        let metric_err = registry.resolve_or_insert("", &[]).unwrap_err();
        assert!(matches!(metric_err, TsinkError::MetricRequired));

        let label_err = registry
            .resolve_or_insert("m", &[Label::new("", "x")])
            .unwrap_err();
        assert!(matches!(label_err, TsinkError::InvalidLabel(_)));

        let oversized = Label::new("k", "x".repeat(crate::label::MAX_LABEL_VALUE_LEN + 1));
        let oversized_err = registry.resolve_or_insert("m", &[oversized]).unwrap_err();
        assert!(matches!(oversized_err, TsinkError::InvalidLabel(_)));
    }
}
