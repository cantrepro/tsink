use super::*;

impl ChunkStorage {
    pub(super) fn insert_series_into_merged_postings(
        merged_postings: &mut SegmentPostingsIndex,
        series_id: SeriesId,
        metric: &str,
        labels: &[Label],
    ) {
        merged_postings.series_postings.insert(series_id);
        merged_postings
            .metric_postings
            .entry(metric.to_string())
            .or_default()
            .insert(series_id);
        for label in labels {
            merged_postings
                .label_name_postings
                .entry(label.name.clone())
                .or_default()
                .insert(series_id);
            merged_postings
                .label_postings
                .entry((label.name.clone(), label.value.clone()))
                .or_default()
                .insert(series_id);
        }
    }

    pub(super) fn remove_series_from_merged_postings(
        merged_postings: &mut SegmentPostingsIndex,
        series_id: SeriesId,
        metric: &str,
        labels: &[Label],
    ) {
        merged_postings.series_postings.remove(series_id);

        let remove_metric =
            merged_postings
                .metric_postings
                .get_mut(metric)
                .is_some_and(|series_ids| {
                    series_ids.remove(series_id);
                    series_ids.is_empty()
                });
        if remove_metric {
            merged_postings.metric_postings.remove(metric);
        }

        for label in labels {
            let remove_label_name = merged_postings
                .label_name_postings
                .get_mut(label.name.as_str())
                .is_some_and(|series_ids| {
                    series_ids.remove(series_id);
                    series_ids.is_empty()
                });
            if remove_label_name {
                merged_postings
                    .label_name_postings
                    .remove(label.name.as_str());
            }

            let label_key = (label.name.clone(), label.value.clone());
            let remove_label = merged_postings
                .label_postings
                .get_mut(&label_key)
                .is_some_and(|series_ids| {
                    series_ids.remove(series_id);
                    series_ids.is_empty()
                });
            if remove_label {
                merged_postings.label_postings.remove(&label_key);
            }
        }
    }

    pub(super) fn rebuild_merged_postings_from_registry(
        &self,
        persisted_index: &mut PersistedIndexState,
    ) -> Result<()> {
        let publication = self.lifecycle_publication_context();
        let registry = publication.registry.read();
        let mut merged_postings = SegmentPostingsIndex::default();
        for series_id in persisted_index.chunk_refs.keys().copied() {
            let Some(series_key) = registry.decode_series_key(series_id) else {
                return Err(TsinkError::DataCorruption(format!(
                    "persisted series id {} is missing from the runtime registry",
                    series_id
                )));
            };
            Self::insert_series_into_merged_postings(
                &mut merged_postings,
                series_id,
                &series_key.metric,
                &series_key.labels,
            );
        }
        persisted_index.merged_postings = merged_postings;
        Ok(())
    }

    pub(super) fn insert_loaded_series_into_merged_postings(
        persisted_index: &mut PersistedIndexState,
        series: &[PersistedSeries],
    ) {
        if series.is_empty() {
            return;
        }

        persisted_index
            .merged_postings
            .clear_missing_label_postings_cache();
        for series in series {
            Self::insert_series_into_merged_postings(
                &mut persisted_index.merged_postings,
                series.series_id,
                &series.metric,
                &series.labels,
            );
        }
    }

    pub(super) fn remove_series_ids_from_merged_postings(
        &self,
        persisted_index: &mut PersistedIndexState,
        series_ids: &[SeriesId],
    ) -> Result<()> {
        if series_ids.is_empty() {
            return Ok(());
        }

        let publication = self.lifecycle_publication_context();
        let registry = publication.registry.read();
        persisted_index
            .merged_postings
            .clear_missing_label_postings_cache();
        for &series_id in series_ids {
            let Some(series_key) = registry.decode_series_key(series_id) else {
                return Err(TsinkError::DataCorruption(format!(
                    "persisted series id {} is missing from the runtime registry",
                    series_id
                )));
            };
            Self::remove_series_from_merged_postings(
                &mut persisted_index.merged_postings,
                series_id,
                &series_key.metric,
                &series_key.labels,
            );
        }

        Ok(())
    }
}
