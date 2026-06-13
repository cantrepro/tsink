use super::*;

impl ChunkStorage {
    pub(super) fn sort_persisted_chunk_refs(chunks: &mut [PersistedChunkRef]) {
        chunks.sort_by_key(|chunk| {
            // Preserve segment/chunk load order ahead of chunk size so overlap dedupe prefers
            // newer persisted generations after crash recovery or interrupted compaction.
            (
                chunk.min_ts,
                chunk.max_ts,
                chunk.sequence,
                chunk.chunk_offset,
                chunk.point_count,
            )
        });
    }

    fn collect_persisted_series_for_segments(
        segments: &[IndexedSegment],
    ) -> Result<Vec<PersistedSeries>> {
        let mut series_by_id = BTreeMap::<SeriesId, PersistedSeries>::new();
        for segment in segments {
            for series in &segment.series {
                match series_by_id.get_mut(&series.series_id) {
                    Some(existing)
                        if existing.metric == series.metric && existing.labels == series.labels =>
                    {
                        match (existing.value_family, series.value_family) {
                            (Some(expected), Some(actual)) if expected != actual => {
                                return Err(TsinkError::DataCorruption(format!(
                                    "series id {} conflicts across incremental segment value families",
                                    series.series_id
                                )));
                            }
                            (None, Some(value_family)) => {
                                existing.value_family = Some(value_family);
                            }
                            _ => {}
                        }
                    }
                    Some(_) => {
                        return Err(TsinkError::DataCorruption(format!(
                            "series id {} conflicts across incremental segment updates",
                            series.series_id
                        )));
                    }
                    None => {
                        series_by_id.insert(series.series_id, series.clone());
                    }
                }
            }
        }

        Ok(series_by_id.into_values().collect())
    }

    fn validate_persisted_series_against_registry(
        registry: &SeriesRegistry,
        series: &[PersistedSeries],
    ) -> Result<()> {
        for series in series {
            if let Some(existing) = registry.decode_series_key(series.series_id) {
                if existing.metric != series.metric || existing.labels != series.labels {
                    return Err(TsinkError::DataCorruption(format!(
                        "series id {} already exists with a different series definition",
                        series.series_id
                    )));
                }
            }

            if let Some(existing) = registry.resolve_existing(&series.metric, &series.labels) {
                if existing.series_id != series.series_id {
                    return Err(TsinkError::DataCorruption(format!(
                        "series key already bound to id {}, incremental update tried to bind {}",
                        existing.series_id, series.series_id
                    )));
                }
            }
        }

        Ok(())
    }

    pub(super) fn register_persisted_series_and_value_families(
        &self,
        series: &[PersistedSeries],
        indexed_segments: &[IndexedSegment],
    ) -> Result<()> {
        let publication = self.lifecycle_publication_context();
        let created_series_ids =
            publication.mutate_registry(|registry| -> Result<Vec<SeriesId>> {
                Self::validate_persisted_series_against_registry(registry, series)?;
                let mut created_series_ids = Vec::new();
                for series in series {
                    let resolution = registry.register_series_with_id(
                        series.series_id,
                        &series.metric,
                        &series.labels,
                    )?;
                    if resolution.created {
                        created_series_ids.push(series.series_id);
                    }
                }
                Self::record_series_value_families_for_persisted_series(
                    registry,
                    series,
                    indexed_segments,
                )?;
                Ok(created_series_ids)
            })?;
        publication
            .registry_bookkeeping
            .mark_series_pending(created_series_ids);
        Ok(())
    }

    fn record_series_value_families_for_persisted_series(
        registry: &mut SeriesRegistry,
        series: &[PersistedSeries],
        indexed_segments: &[IndexedSegment],
    ) -> Result<()> {
        let mut missing = BTreeSet::new();
        for series in series {
            if let Some(value_family) = series.value_family {
                registry.record_series_value_family(series.series_id, value_family)?;
            } else {
                missing.insert(series.series_id);
            }
        }
        if missing.is_empty() {
            return Ok(());
        }

        let mut inferred = HashMap::<SeriesId, SeriesValueFamily>::new();
        for indexed_segment in indexed_segments {
            for entry in &indexed_segment.chunk_index.entries {
                if !missing.contains(&entry.series_id) {
                    continue;
                }

                let payload = crate::engine::segment::chunk_payload_from_record(
                    indexed_segment.chunks_mmap.as_slice(),
                    entry.chunk_offset,
                    entry.chunk_len,
                )?;
                let family = SeriesValueFamily::from_encoded(
                    entry.lane,
                    entry.value_codec,
                    payload.as_ref(),
                )?;
                match inferred.entry(entry.series_id) {
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        slot.insert(family);
                    }
                    std::collections::hash_map::Entry::Occupied(existing)
                        if *existing.get() != family =>
                    {
                        return Err(TsinkError::DataCorruption(format!(
                            "series id {} conflicts across persisted chunk value families",
                            entry.series_id
                        )));
                    }
                    std::collections::hash_map::Entry::Occupied(_) => {}
                }
            }
        }

        for series_id in missing {
            let Some(value_family) = inferred.remove(&series_id) else {
                return Err(TsinkError::DataCorruption(format!(
                    "missing persisted chunk to infer value family for series id {}",
                    series_id
                )));
            };
            registry.record_series_value_family(series_id, value_family)?;
        }

        Ok(())
    }

    fn sort_loaded_segments_for_publication(loaded_segments: &mut [IndexedSegment]) {
        loaded_segments.sort_by(|a, b| {
            (a.manifest.level, a.manifest.segment_id, &a.root).cmp(&(
                b.manifest.level,
                b.manifest.segment_id,
                &b.root,
            ))
        });
    }

    pub(super) fn prepare_loaded_persisted_segments(
        mut indexed_segments: Vec<IndexedSegment>,
    ) -> Result<Option<PreparedLoadedPersistedSegments>> {
        if indexed_segments.is_empty() {
            return Ok(None);
        }

        Self::sort_loaded_segments_for_publication(&mut indexed_segments);
        let series = Self::collect_persisted_series_for_segments(&indexed_segments)?;
        Ok(Some(PreparedLoadedPersistedSegments {
            indexed_segments,
            series,
        }))
    }

    pub(super) fn validate_loaded_segment_indexes(
        loaded: &LoadedSegmentIndexes,
        reconcile_registry_with_persisted: bool,
    ) -> Result<()> {
        if loaded.series.is_empty()
            && !reconcile_registry_with_persisted
            && !loaded.indexed_segments.is_empty()
        {
            return Err(TsinkError::DataCorruption(
                "persisted segment indexes loaded without series metadata and no persisted registry snapshot is available"
                    .to_string(),
            ));
        }

        Ok(())
    }
}
