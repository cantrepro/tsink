use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use roaring::RoaringTreemap;

#[derive(Debug, Clone, Default)]
pub(crate) struct SegmentPostingsIndex {
    pub(crate) series_postings: RoaringTreemap,
    pub(crate) metric_postings: BTreeMap<String, RoaringTreemap>,
    pub(crate) label_name_postings: BTreeMap<String, RoaringTreemap>,
    pub(crate) label_postings: BTreeMap<(String, String), RoaringTreemap>,
    missing_label_postings_cache: Arc<RwLock<BTreeMap<String, RoaringTreemap>>>,
}

impl SegmentPostingsIndex {
    pub(super) fn from_series_postings(series_postings: RoaringTreemap) -> Self {
        Self {
            series_postings,
            ..Default::default()
        }
    }

    fn bitmap_memory_usage_bytes(bitmap: &RoaringTreemap) -> usize {
        std::mem::size_of::<RoaringTreemap>().saturating_add(bitmap.serialized_size())
    }

    pub(crate) fn memory_usage_bytes(&self) -> usize {
        let mut bytes = std::mem::size_of::<Self>()
            .saturating_add(Self::bitmap_memory_usage_bytes(&self.series_postings));
        for (metric, series_ids) in &self.metric_postings {
            bytes = bytes
                .saturating_add(std::mem::size_of::<(String, RoaringTreemap)>())
                .saturating_add(metric.capacity())
                .saturating_add(Self::bitmap_memory_usage_bytes(series_ids));
        }
        for (label_name, series_ids) in &self.label_name_postings {
            bytes = bytes
                .saturating_add(std::mem::size_of::<(String, RoaringTreemap)>())
                .saturating_add(label_name.capacity())
                .saturating_add(Self::bitmap_memory_usage_bytes(series_ids));
        }
        for ((label_name, label_value), series_ids) in &self.label_postings {
            bytes = bytes
                .saturating_add(std::mem::size_of::<((String, String), RoaringTreemap)>())
                .saturating_add(label_name.capacity())
                .saturating_add(label_value.capacity())
                .saturating_add(Self::bitmap_memory_usage_bytes(series_ids));
        }
        for (label_name, series_ids) in self.missing_label_postings_cache.read().iter() {
            bytes = bytes
                .saturating_add(std::mem::size_of::<(String, RoaringTreemap)>())
                .saturating_add(label_name.capacity())
                .saturating_add(Self::bitmap_memory_usage_bytes(series_ids));
        }
        bytes
    }

    pub(crate) fn series_id_postings_for_metric(&self, metric: &str) -> Option<&RoaringTreemap> {
        self.metric_postings.get(metric)
    }

    pub(crate) fn missing_label_postings_for_name(&self, label_name: &str) -> RoaringTreemap {
        if let Some(cached) = self
            .missing_label_postings_cache
            .read()
            .get(label_name)
            .cloned()
        {
            return cached;
        }

        let mut missing = self.series_postings.clone();
        if let Some(present) = self.label_name_postings.get(label_name) {
            missing -= present;
        }
        self.missing_label_postings_cache
            .write()
            .insert(label_name.to_string(), missing.clone());
        missing
    }

    pub(crate) fn clear_missing_label_postings_cache(&self) {
        self.missing_label_postings_cache.write().clear();
    }

    pub(crate) fn series_id_postings_for_label_name(
        &self,
        label_name: &str,
    ) -> Option<&RoaringTreemap> {
        self.label_name_postings.get(label_name)
    }

    pub(crate) fn postings_for_label(
        &self,
        label_name: &str,
        label_value: &str,
    ) -> Option<&RoaringTreemap> {
        self.label_postings
            .get(&(label_name.to_string(), label_value.to_string()))
    }

    pub(crate) fn for_each_metric_postings(&self, mut visitor: impl FnMut(&str, &RoaringTreemap)) {
        for (metric, series_ids) in &self.metric_postings {
            visitor(metric, series_ids);
        }
    }

    pub(crate) fn for_each_postings_for_label_name(
        &self,
        label_name: &str,
        mut visitor: impl FnMut(&str, &RoaringTreemap),
    ) {
        let start = (label_name.to_string(), String::new());
        for ((candidate_name, label_value), series_ids) in self.label_postings.range(start..) {
            if candidate_name != label_name {
                break;
            }
            visitor(label_value, series_ids);
        }
    }
}
