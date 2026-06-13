#[cfg(test)]
use std::sync::Arc;

use roaring::RoaringTreemap;

#[cfg(test)]
use super::IngestCommitHook;
#[cfg(test)]
use super::SeriesId;
use super::{PersistedIndexState, SeriesRegistry};

pub(super) trait MetadataPostingsProvider {
    fn all_series_postings(&self) -> RoaringTreemap;
    fn series_count(&self) -> usize;
    fn metric_postings_count(&self) -> usize;
    fn for_each_metric_postings(&self, visit: impl FnMut(&str, &RoaringTreemap));
    fn series_id_postings_for_metric(&self, metric: &str) -> Option<RoaringTreemap>;
    fn series_id_postings_for_label_name(&self, label_name: &str) -> Option<RoaringTreemap>;
    fn missing_label_postings_for_name(&self, label_name: &str) -> Option<RoaringTreemap>;
    fn postings_for_label(&self, label_name: &str, label_value: &str) -> Option<RoaringTreemap>;
    fn for_each_postings_for_label_name(
        &self,
        label_name: &str,
        visit: impl FnMut(&str, &RoaringTreemap),
    );

    fn metric_postings_where(&self, mut predicate: impl FnMut(&str) -> bool) -> RoaringTreemap {
        let mut matched = RoaringTreemap::new();
        self.for_each_metric_postings(|metric, series_ids| {
            if predicate(metric) {
                matched |= series_ids;
            }
        });
        matched
    }

    fn label_postings_where_for_name(
        &self,
        label_name: &str,
        mut predicate: impl FnMut(&str) -> bool,
    ) -> RoaringTreemap {
        let mut matched = RoaringTreemap::new();
        self.for_each_postings_for_label_name(label_name, |label_value, series_ids| {
            if predicate(label_value) {
                matched |= series_ids;
            }
        });
        matched
    }
}

impl MetadataPostingsProvider for SeriesRegistry {
    fn all_series_postings(&self) -> RoaringTreemap {
        self.all_series_postings()
    }

    fn series_count(&self) -> usize {
        self.series_count()
    }

    fn metric_postings_count(&self) -> usize {
        self.metric_postings_count()
    }

    fn for_each_metric_postings(&self, visit: impl FnMut(&str, &RoaringTreemap)) {
        SeriesRegistry::for_each_metric_postings(self, visit);
    }

    fn series_id_postings_for_metric(&self, metric: &str) -> Option<RoaringTreemap> {
        self.series_id_postings_for_metric(metric)
    }

    fn series_id_postings_for_label_name(&self, label_name: &str) -> Option<RoaringTreemap> {
        self.series_id_postings_for_label_name(label_name)
    }

    fn missing_label_postings_for_name(&self, label_name: &str) -> Option<RoaringTreemap> {
        let name_id = self.label_name_id(label_name)?;
        Some(self.missing_label_postings_for_id(name_id))
    }

    fn postings_for_label(&self, label_name: &str, label_value: &str) -> Option<RoaringTreemap> {
        self.postings_for_label(label_name, label_value)
    }

    fn for_each_postings_for_label_name(
        &self,
        label_name: &str,
        mut visit: impl FnMut(&str, &RoaringTreemap),
    ) {
        let Some(label_name_id) = self.label_name_id(label_name) else {
            return;
        };

        self.for_each_postings_for_label_name_id(label_name_id, |value_id, series_ids| {
            let Some(label_value) = self.label_value_by_id(value_id) else {
                return;
            };
            visit(&label_value, series_ids);
        });
    }
}

pub(super) struct PersistedMetadataPostings<'a> {
    index: &'a PersistedIndexState,
}

impl MetadataPostingsProvider for PersistedMetadataPostings<'_> {
    fn all_series_postings(&self) -> RoaringTreemap {
        self.index.merged_postings.series_postings.clone()
    }

    fn series_count(&self) -> usize {
        self.index.chunk_refs.len()
    }

    fn metric_postings_count(&self) -> usize {
        self.index.merged_postings.metric_postings.len()
    }

    fn for_each_metric_postings(&self, visit: impl FnMut(&str, &RoaringTreemap)) {
        self.index.merged_postings.for_each_metric_postings(visit);
    }

    fn series_id_postings_for_metric(&self, metric: &str) -> Option<RoaringTreemap> {
        self.index
            .merged_postings
            .series_id_postings_for_metric(metric)
            .cloned()
    }

    fn series_id_postings_for_label_name(&self, label_name: &str) -> Option<RoaringTreemap> {
        self.index
            .merged_postings
            .series_id_postings_for_label_name(label_name)
            .cloned()
    }

    fn missing_label_postings_for_name(&self, label_name: &str) -> Option<RoaringTreemap> {
        Some(
            self.index
                .merged_postings
                .missing_label_postings_for_name(label_name),
        )
    }

    fn postings_for_label(&self, label_name: &str, label_value: &str) -> Option<RoaringTreemap> {
        self.index
            .merged_postings
            .postings_for_label(label_name, label_value)
            .cloned()
    }

    fn for_each_postings_for_label_name(
        &self,
        label_name: &str,
        mut visit: impl FnMut(&str, &RoaringTreemap),
    ) {
        self.index.merged_postings.for_each_postings_for_label_name(
            label_name,
            |label_value, series_ids| {
                visit(label_value, series_ids);
            },
        );
    }
}

pub(super) struct HybridMetadataPostings<'a> {
    registry: &'a SeriesRegistry,
    persisted: PersistedMetadataPostings<'a>,
    delta_series_ids: RoaringTreemap,
}

impl<'a> HybridMetadataPostings<'a> {
    fn from_runtime_state(
        registry: &'a SeriesRegistry,
        persisted_index: &'a PersistedIndexState,
        delta_series_ids: RoaringTreemap,
    ) -> Self {
        let persisted = PersistedMetadataPostings {
            index: persisted_index,
        };
        Self {
            registry,
            persisted,
            delta_series_ids,
        }
    }

    fn union_postings(
        left: Option<RoaringTreemap>,
        right: Option<RoaringTreemap>,
    ) -> Option<RoaringTreemap> {
        match (left, right) {
            (Some(mut left), Some(right)) => {
                left |= right;
                Some(left)
            }
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        }
    }

    fn delta_bitmap(&self, bitmap: Option<RoaringTreemap>) -> Option<RoaringTreemap> {
        let mut bitmap = bitmap?;
        bitmap &= &self.delta_series_ids;
        (!bitmap.is_empty()).then_some(bitmap)
    }

    fn delta_metric_postings(&self, metric: &str) -> Option<RoaringTreemap> {
        self.delta_bitmap(self.registry.series_id_postings_for_metric(metric))
    }

    fn delta_label_name_postings(&self, label_name: &str) -> Option<RoaringTreemap> {
        self.delta_bitmap(self.registry.series_id_postings_for_label_name(label_name))
    }

    fn delta_label_postings(&self, label_name: &str, label_value: &str) -> Option<RoaringTreemap> {
        self.delta_bitmap(self.registry.postings_for_label(label_name, label_value))
    }

    fn delta_all_series_postings(&self) -> RoaringTreemap {
        self.delta_series_ids.clone()
    }
}

impl MetadataPostingsProvider for HybridMetadataPostings<'_> {
    fn all_series_postings(&self) -> RoaringTreemap {
        let mut merged = self.persisted.all_series_postings();
        merged |= self.delta_all_series_postings();
        merged
    }

    fn series_count(&self) -> usize {
        self.registry.series_count()
    }

    fn metric_postings_count(&self) -> usize {
        if self.delta_series_ids.is_empty() {
            return self.persisted.index.merged_postings.metric_postings.len();
        }

        let mut metrics = self
            .persisted
            .index
            .merged_postings
            .metric_postings
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        self.registry
            .for_each_metric_postings(|metric, series_ids| {
                if !(series_ids & &self.delta_series_ids).is_empty() {
                    metrics.insert(metric.to_string());
                }
            });
        metrics.len()
    }

    fn for_each_metric_postings(&self, mut visit: impl FnMut(&str, &RoaringTreemap)) {
        self.persisted
            .index
            .merged_postings
            .for_each_metric_postings(|metric, series_ids| visit(metric, series_ids));
        self.registry
            .for_each_metric_postings(|metric, series_ids| {
                let mut delta_series_ids = series_ids.clone();
                delta_series_ids &= &self.delta_series_ids;
                if !delta_series_ids.is_empty() {
                    visit(metric, &delta_series_ids);
                }
            });
    }

    fn series_id_postings_for_metric(&self, metric: &str) -> Option<RoaringTreemap> {
        Self::union_postings(
            self.persisted.series_id_postings_for_metric(metric),
            self.delta_metric_postings(metric),
        )
    }

    fn series_id_postings_for_label_name(&self, label_name: &str) -> Option<RoaringTreemap> {
        Self::union_postings(
            self.persisted.series_id_postings_for_label_name(label_name),
            self.delta_label_name_postings(label_name),
        )
    }

    fn missing_label_postings_for_name(&self, label_name: &str) -> Option<RoaringTreemap> {
        let persisted = self
            .persisted
            .missing_label_postings_for_name(label_name)
            .unwrap_or_default();
        let delta = if let Some(name_id) = self.registry.label_name_id(label_name) {
            let mut bitmap = self.registry.missing_label_postings_for_id(name_id);
            bitmap &= &self.delta_series_ids;
            bitmap
        } else {
            self.delta_all_series_postings()
        };
        Self::union_postings(Some(persisted), Some(delta))
    }

    fn postings_for_label(&self, label_name: &str, label_value: &str) -> Option<RoaringTreemap> {
        Self::union_postings(
            self.persisted.postings_for_label(label_name, label_value),
            self.delta_label_postings(label_name, label_value),
        )
    }

    fn for_each_postings_for_label_name(
        &self,
        label_name: &str,
        mut visit: impl FnMut(&str, &RoaringTreemap),
    ) {
        self.persisted
            .index
            .merged_postings
            .for_each_postings_for_label_name(label_name, |label_value, series_ids| {
                visit(label_value, series_ids);
            });
        let Some(label_name_id) = self.registry.label_name_id(label_name) else {
            return;
        };
        self.registry
            .for_each_postings_for_label_name_id(label_name_id, |value_id, series_ids| {
                let Some(label_value) = self.registry.label_value_by_id(value_id) else {
                    return;
                };
                let mut delta_series_ids = series_ids.clone();
                delta_series_ids &= &self.delta_series_ids;
                if !delta_series_ids.is_empty() {
                    visit(&label_value, &delta_series_ids);
                }
            });
    }
}

enum RuntimeMetadataPostingsSource<'a> {
    Registry(&'a SeriesRegistry),
    Hybrid(HybridMetadataPostings<'a>),
}

pub(super) struct RuntimeMetadataPostingsProvider<'a> {
    source: RuntimeMetadataPostingsSource<'a>,
    #[cfg(test)]
    all_series_postings_hook: Option<Arc<IngestCommitHook>>,
}

impl<'a> RuntimeMetadataPostingsProvider<'a> {
    pub(super) fn new(
        registry: &'a SeriesRegistry,
        persisted_index: &'a PersistedIndexState,
        #[cfg(test)] all_series_postings_hook: Option<Arc<IngestCommitHook>>,
    ) -> Self {
        if !persisted_index.merged_postings.metric_postings.is_empty() {
            Self {
                source: RuntimeMetadataPostingsSource::Hybrid(
                    HybridMetadataPostings::from_runtime_state(
                        registry,
                        persisted_index,
                        persisted_index.runtime_metadata_delta_series_ids.clone(),
                    ),
                ),
                #[cfg(test)]
                all_series_postings_hook,
            }
        } else {
            Self {
                source: RuntimeMetadataPostingsSource::Registry(registry),
                #[cfg(test)]
                all_series_postings_hook,
            }
        }
    }

    #[cfg(test)]
    pub(super) fn uses_persisted_postings(&self) -> bool {
        matches!(self.source, RuntimeMetadataPostingsSource::Hybrid(_))
    }

    #[cfg(test)]
    fn invoke_all_series_postings_hook(&self) {
        if let Some(hook) = self.all_series_postings_hook.as_ref() {
            hook();
        }
    }
}

impl MetadataPostingsProvider for RuntimeMetadataPostingsProvider<'_> {
    fn all_series_postings(&self) -> RoaringTreemap {
        #[cfg(test)]
        self.invoke_all_series_postings_hook();
        match &self.source {
            RuntimeMetadataPostingsSource::Registry(registry) => registry.all_series_postings(),
            RuntimeMetadataPostingsSource::Hybrid(hybrid) => hybrid.all_series_postings(),
        }
    }

    fn series_count(&self) -> usize {
        match &self.source {
            RuntimeMetadataPostingsSource::Registry(registry) => registry.series_count(),
            RuntimeMetadataPostingsSource::Hybrid(hybrid) => hybrid.series_count(),
        }
    }

    fn metric_postings_count(&self) -> usize {
        match &self.source {
            RuntimeMetadataPostingsSource::Registry(registry) => registry.metric_postings_count(),
            RuntimeMetadataPostingsSource::Hybrid(hybrid) => hybrid.metric_postings_count(),
        }
    }

    fn for_each_metric_postings(&self, visit: impl FnMut(&str, &RoaringTreemap)) {
        match &self.source {
            RuntimeMetadataPostingsSource::Registry(registry) => {
                registry.for_each_metric_postings(visit)
            }
            RuntimeMetadataPostingsSource::Hybrid(hybrid) => hybrid.for_each_metric_postings(visit),
        }
    }

    fn series_id_postings_for_metric(&self, metric: &str) -> Option<RoaringTreemap> {
        match &self.source {
            RuntimeMetadataPostingsSource::Registry(registry) => {
                registry.series_id_postings_for_metric(metric)
            }
            RuntimeMetadataPostingsSource::Hybrid(hybrid) => {
                hybrid.series_id_postings_for_metric(metric)
            }
        }
    }

    fn series_id_postings_for_label_name(&self, label_name: &str) -> Option<RoaringTreemap> {
        match &self.source {
            RuntimeMetadataPostingsSource::Registry(registry) => {
                registry.series_id_postings_for_label_name(label_name)
            }
            RuntimeMetadataPostingsSource::Hybrid(hybrid) => {
                hybrid.series_id_postings_for_label_name(label_name)
            }
        }
    }

    fn missing_label_postings_for_name(&self, label_name: &str) -> Option<RoaringTreemap> {
        match &self.source {
            RuntimeMetadataPostingsSource::Registry(registry) => {
                registry.missing_label_postings_for_name(label_name)
            }
            RuntimeMetadataPostingsSource::Hybrid(hybrid) => {
                hybrid.missing_label_postings_for_name(label_name)
            }
        }
    }

    fn postings_for_label(&self, label_name: &str, label_value: &str) -> Option<RoaringTreemap> {
        match &self.source {
            RuntimeMetadataPostingsSource::Registry(registry) => {
                registry.postings_for_label(label_name, label_value)
            }
            RuntimeMetadataPostingsSource::Hybrid(hybrid) => {
                hybrid.postings_for_label(label_name, label_value)
            }
        }
    }

    fn for_each_postings_for_label_name(
        &self,
        label_name: &str,
        visit: impl FnMut(&str, &RoaringTreemap),
    ) {
        match &self.source {
            RuntimeMetadataPostingsSource::Registry(registry) => {
                registry.for_each_postings_for_label_name(label_name, visit)
            }
            RuntimeMetadataPostingsSource::Hybrid(hybrid) => {
                hybrid.for_each_postings_for_label_name(label_name, visit)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use roaring::RoaringTreemap;

    use super::*;
    use crate::engine::segment::SegmentPostingsIndex;
    use crate::Label;

    fn bitmap(series_ids: &[SeriesId]) -> RoaringTreemap {
        series_ids.iter().copied().collect()
    }

    #[test]
    fn hybrid_postings_union_persisted_and_runtime_delta_series() {
        let registry = SeriesRegistry::new();
        let persisted = registry
            .register_series_with_id(1, "cpu", &[Label::new("host", "persisted")])
            .unwrap();
        let runtime = registry
            .register_series_with_id(
                2,
                "cpu",
                &[Label::new("host", "runtime"), Label::new("job", "api")],
            )
            .unwrap();

        let mut persisted_index = PersistedIndexState::default();
        let mut merged_postings = SegmentPostingsIndex::default();
        merged_postings.series_postings = bitmap(&[persisted.series_id]);
        merged_postings
            .metric_postings
            .insert("cpu".to_string(), bitmap(&[persisted.series_id]));
        merged_postings
            .label_name_postings
            .insert("host".to_string(), bitmap(&[persisted.series_id]));
        merged_postings.label_postings.insert(
            ("host".to_string(), "persisted".to_string()),
            bitmap(&[persisted.series_id]),
        );
        persisted_index.merged_postings = merged_postings;
        persisted_index.runtime_metadata_delta_series_ids = bitmap(&[runtime.series_id]);

        let hybrid = RuntimeMetadataPostingsProvider::new(
            &registry,
            &persisted_index,
            Some(Arc::new(|| {})),
        );

        assert_eq!(
            hybrid
                .series_id_postings_for_metric("cpu")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![persisted.series_id, runtime.series_id],
        );
        assert_eq!(
            hybrid
                .postings_for_label("host", "runtime")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![runtime.series_id],
        );
        assert_eq!(
            hybrid
                .missing_label_postings_for_name("job")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![persisted.series_id],
        );
    }
}
