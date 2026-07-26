use std::collections::{BTreeMap, BTreeSet};

use roaring::RoaringTreemap;

#[derive(Debug, Default)]
pub(crate) struct SegmentPostingsIndex {
    pub(crate) series_postings: RoaringTreemap,
    pub(crate) metric_postings: BTreeMap<String, RoaringTreemap>,
    pub(crate) label_name_postings: BTreeMap<String, RoaringTreemap>,
    pub(crate) label_postings: BTreeMap<(String, String), RoaringTreemap>,
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
        bytes
    }

    /// Measures only the postings entries that a bounded persisted-index mutation can change.
    ///
    /// `series_postings` is included because every series insertion/removal can change it. The
    /// remaining maps are measured only at the exact metric/label keys named by the mutation.
    pub(crate) fn scoped_memory_usage_bytes(
        &self,
        metrics: &BTreeSet<String>,
        label_names: &BTreeSet<String>,
        labels: &BTreeSet<(String, String)>,
    ) -> usize {
        let mut bytes = Self::bitmap_memory_usage_bytes(&self.series_postings);
        for metric in metrics {
            if let Some(series_ids) = self.metric_postings.get(metric) {
                bytes = bytes
                    .saturating_add(std::mem::size_of::<(String, RoaringTreemap)>())
                    .saturating_add(metric.capacity())
                    .saturating_add(Self::bitmap_memory_usage_bytes(series_ids));
            }
        }
        for label_name in label_names {
            if let Some(series_ids) = self.label_name_postings.get(label_name) {
                bytes = bytes
                    .saturating_add(std::mem::size_of::<(String, RoaringTreemap)>())
                    .saturating_add(label_name.capacity())
                    .saturating_add(Self::bitmap_memory_usage_bytes(series_ids));
            }
        }
        for label in labels {
            if let Some(series_ids) = self.label_postings.get(label) {
                bytes = bytes
                    .saturating_add(std::mem::size_of::<((String, String), RoaringTreemap)>())
                    .saturating_add(label.0.capacity())
                    .saturating_add(label.1.capacity())
                    .saturating_add(Self::bitmap_memory_usage_bytes(series_ids));
            }
        }
        bytes
    }

    pub(crate) fn series_id_postings_for_metric(&self, metric: &str) -> Option<&RoaringTreemap> {
        self.metric_postings.get(metric)
    }

    pub(crate) fn missing_label_postings_for_name(&self, label_name: &str) -> RoaringTreemap {
        let mut missing = self.series_postings.clone();
        if let Some(present) = self.label_name_postings.get(label_name) {
            missing -= present;
        }
        missing
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_label_postings_are_query_transient_and_reflect_mutation_without_invalidation() {
        let mut postings = SegmentPostingsIndex::default();
        postings.series_postings.extend([1, 2, 3]);
        postings
            .label_name_postings
            .insert("job".to_string(), [1].into_iter().collect());

        let before_query = postings.memory_usage_bytes();
        assert_eq!(
            postings
                .missing_label_postings_for_name("job")
                .iter()
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(postings.memory_usage_bytes(), before_query);

        postings.series_postings.insert(4);
        let before_refresh = postings.memory_usage_bytes();
        assert_eq!(
            postings
                .missing_label_postings_for_name("job")
                .iter()
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(postings.memory_usage_bytes(), before_refresh);
    }
}
