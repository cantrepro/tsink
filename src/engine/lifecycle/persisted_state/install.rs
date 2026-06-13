use super::*;

impl ChunkStorage {
    pub(super) fn accumulate_max_timestamp(current: &mut Option<i64>, candidate: Option<i64>) {
        if let Some(candidate) = candidate {
            *current = Some(
                current
                    .map(|existing| existing.max(candidate))
                    .unwrap_or(candidate),
            );
        }
    }

    pub(super) fn normalize_series_ids(mut series_ids: Vec<SeriesId>) -> Vec<SeriesId> {
        series_ids.sort_unstable();
        series_ids.dedup();
        series_ids
    }

    pub(super) fn with_lifecycle_persisted_index_publication<R>(
        publication: LifecyclePublicationContext<'_>,
        mutate: impl FnOnce(&mut PersistedIndexState) -> R,
    ) -> R {
        let mut persisted_index = publication.persisted_index.write();
        let before_included = if publication.accounting_enabled {
            Self::persisted_index_included_memory_usage_bytes(&persisted_index)
        } else {
            0
        };
        let before_mmap = if publication.accounting_enabled {
            Self::persisted_index_persisted_mmap_bytes(&persisted_index)
        } else {
            0
        };
        let result = mutate(&mut persisted_index);
        let account = |component: &std::sync::atomic::AtomicU64, before: usize, after: usize| {
            if !publication.accounting_enabled || before == after {
                return;
            }
            let delta = saturating_u64_from_usize(before.abs_diff(after));
            if after >= before {
                component.fetch_add(delta, Ordering::AcqRel);
                publication
                    .shared_used_bytes
                    .fetch_add(delta, Ordering::AcqRel);
                publication.used_bytes.fetch_add(delta, Ordering::AcqRel);
            } else {
                component.fetch_sub(delta, Ordering::AcqRel);
                publication
                    .shared_used_bytes
                    .fetch_sub(delta, Ordering::AcqRel);
                publication.used_bytes.fetch_sub(delta, Ordering::AcqRel);
            }
        };
        account(
            publication.persisted_index_used_bytes,
            before_included,
            Self::persisted_index_included_memory_usage_bytes(&persisted_index),
        );
        account(
            publication.persisted_mmap_used_bytes,
            before_mmap,
            Self::persisted_index_persisted_mmap_bytes(&persisted_index),
        );
        result
    }

    pub(super) fn apply_loaded_segments_state_update(
        &self,
        loaded_segments: Vec<IndexedSegment>,
        loaded_series: &[PersistedSeries],
    ) -> Result<PersistedSegmentsStateUpdate> {
        let publication = self.lifecycle_publication_context();
        let metadata_delta = publication.runtime_metadata_delta;
        let state_update = Self::with_lifecycle_persisted_index_publication(
            publication,
            move |persisted_index| -> Result<PersistedSegmentsStateUpdate> {
                let mut state_update = PersistedSegmentsStateUpdate::default();
                for segment in loaded_segments {
                    let (segment_lane, segment_tier) =
                        self.persisted_segment_location_for_root(&segment.root)?;
                    let (segment_series, segment_max_ts, _segment_bounded_max_ts) = self
                        .insert_indexed_segment_into_state(
                            segment_lane,
                            segment_tier,
                            persisted_index,
                            segment,
                        )?;
                    state_update.affected_series.extend(segment_series);
                    Self::accumulate_max_timestamp(
                        &mut state_update.loaded_max_timestamp,
                        segment_max_ts,
                    );
                }

                state_update.affected_series =
                    Self::normalize_series_ids(state_update.affected_series);
                Self::insert_loaded_series_into_merged_postings(persisted_index, loaded_series);
                metadata_delta.reconcile_series_ids_locked(
                    persisted_index,
                    state_update.affected_series.iter().copied(),
                );
                Ok(state_update)
            },
        )?;
        Ok(state_update)
    }

    pub(super) fn apply_segment_root_removal_state_update(
        &self,
        roots: &[PathBuf],
    ) -> Result<PersistedSegmentRemovalStateUpdate> {
        let publication = self.lifecycle_publication_context();
        let metadata_delta = publication.runtime_metadata_delta;
        let state_update = Self::with_lifecycle_persisted_index_publication(
            publication,
            |persisted_index| -> Result<PersistedSegmentRemovalStateUpdate> {
                let mut affected_series = Vec::new();
                let mut fully_removed_series = BTreeSet::new();
                let mut removed_any = false;

                for root in roots {
                    let (removed_series, removed_from_postings) =
                        Self::remove_segment_root_from_state(persisted_index, root);
                    removed_any |= !removed_series.is_empty();
                    affected_series.extend(removed_series);
                    fully_removed_series.extend(removed_from_postings);
                }

                let affected_series = Self::normalize_series_ids(affected_series);
                let fully_removed_series =
                    fully_removed_series.into_iter().collect::<Vec<SeriesId>>();
                self.remove_series_ids_from_merged_postings(
                    persisted_index,
                    &fully_removed_series,
                )?;
                metadata_delta
                    .reconcile_series_ids_locked(persisted_index, affected_series.iter().copied());
                Ok(PersistedSegmentRemovalStateUpdate {
                    removed_any,
                    affected_series,
                })
            },
        )?;
        Ok(state_update)
    }

    pub(super) fn prepare_persisted_index_install(
        &self,
        loaded: LoadedSegmentIndexes,
    ) -> Result<PreparedPersistedIndexInstall> {
        let LoadedSegmentIndexes {
            indexed_segments,
            next_segment_id,
            ..
        } = loaded;
        let mut persisted_index = PersistedIndexState::default();
        let mut loaded_max_timestamp = None;
        for indexed_segment in indexed_segments {
            let (segment_lane, segment_tier) =
                self.persisted_segment_location_for_root(&indexed_segment.root)?;
            let (_, segment_max_ts, _segment_bounded_max_ts) = self
                .insert_indexed_segment_into_state(
                    segment_lane,
                    segment_tier,
                    &mut persisted_index,
                    indexed_segment,
                )?;
            Self::accumulate_max_timestamp(&mut loaded_max_timestamp, segment_max_ts);
        }

        let persisted_series_ids =
            Self::normalize_series_ids(persisted_index.chunk_refs.keys().copied().collect());
        Ok(PreparedPersistedIndexInstall {
            persisted_index,
            persisted_series_ids,
            loaded_max_timestamp,
            next_segment_id: next_segment_id.max(1),
        })
    }

    pub(super) fn reconcile_registry_with_persisted_series_ids(&self, series_ids: &[SeriesId]) {
        let keep = series_ids.iter().copied().collect::<BTreeSet<_>>();
        self.lifecycle_publication_context()
            .mutate_registry(|registry| registry.retain_series_ids(&keep));
    }

    pub(super) fn install_persisted_index_rebuild(
        &self,
        prepared: PreparedPersistedIndexInstall,
    ) -> Result<()> {
        let PreparedPersistedIndexInstall {
            persisted_index,
            persisted_series_ids,
            loaded_max_timestamp,
            next_segment_id,
        } = prepared;
        let publication = self.lifecycle_publication_context();
        let metadata_delta = publication.runtime_metadata_delta;
        Self::with_lifecycle_persisted_index_publication(
            publication,
            move |current_persisted_index| {
                *current_persisted_index = persisted_index;
                metadata_delta.rebuild_locked(current_persisted_index);
            },
        );

        self.install_visible_persisted_state(PersistedVisibleStateInstall {
            refresh_series_ids: persisted_series_ids,
            mark_refreshed_series_materialized: true,
            loaded_max_timestamp,
            next_segment_id: Some(next_segment_id),
        })
    }

    fn insert_indexed_segment_into_state(
        &self,
        segment_lane: SegmentLaneFamily,
        segment_tier: PersistedSegmentTier,
        persisted_index: &mut PersistedIndexState,
        indexed_segment: IndexedSegment,
    ) -> Result<(Vec<SeriesId>, Option<i64>, Option<i64>)> {
        debug_assert!(
            indexed_segment.manifest.series_count == 0
                || indexed_segment.postings.series_postings.is_empty()
                || indexed_segment.series.is_empty()
                || !indexed_segment.postings.metric_postings.is_empty(),
            "segments with loaded postings should carry metric postings"
        );
        let root = indexed_segment.root;
        if persisted_index.segments_by_root.contains_key(&root) {
            return Ok((Vec::new(), None, None));
        }
        let segment_manifest = indexed_segment.manifest;
        let segment_level = segment_manifest.level;
        let chunk_map = Arc::new(indexed_segment.chunks_mmap);
        let chunk_entries = indexed_segment.chunk_index.entries;

        let segment_slot = persisted_index.next_segment_slot;
        let mut refs_by_series = HashMap::<SeriesId, Vec<PersistedChunkRef>>::new();
        let mut series_time_summaries =
            HashMap::<SeriesId, state::PersistedSeriesTimeRangeSummary>::new();
        let mut time_bucket_postings = segment_manifest
            .min_ts
            .zip(segment_manifest.max_ts)
            .map(|(min_ts, max_ts)| state::PersistedSegmentTimeBucketIndex::new(min_ts, max_ts));
        let mut affected_series = BTreeSet::new();
        let mut loaded_max_timestamp: Option<i64> = None;
        let mut loaded_bounded_max_timestamp: Option<i64> = None;
        let future_skew_cutoff = self.current_future_skew_cutoff();
        let mut next_chunk_sequence = persisted_index.next_chunk_sequence.max(1);

        for entry in chunk_entries {
            let sequence = next_chunk_sequence;
            next_chunk_sequence = sequence.saturating_add(1);
            Self::accumulate_max_timestamp(&mut loaded_max_timestamp, Some(entry.max_ts));
            let bounded_candidate = if entry.max_ts <= future_skew_cutoff {
                Some(entry.max_ts)
            } else if entry.min_ts <= future_skew_cutoff {
                Some(entry.min_ts)
            } else {
                None
            };
            Self::accumulate_max_timestamp(&mut loaded_bounded_max_timestamp, bounded_candidate);

            let chunk_ref = PersistedChunkRef {
                level: segment_level,
                min_ts: entry.min_ts,
                max_ts: entry.max_ts,
                point_count: entry.point_count,
                sequence,
                chunk_offset: entry.chunk_offset,
                chunk_len: entry.chunk_len,
                lane: entry.lane,
                ts_codec: entry.ts_codec,
                value_codec: entry.value_codec,
                segment_slot,
            };

            refs_by_series
                .entry(entry.series_id)
                .or_default()
                .push(chunk_ref);
            if let Some(time_bucket_postings) = time_bucket_postings.as_mut() {
                time_bucket_postings.insert_series_range(
                    entry.series_id,
                    entry.min_ts,
                    entry.max_ts,
                );
            }
            series_time_summaries
                .entry(entry.series_id)
                .and_modify(|summary| {
                    summary.min_ts = summary.min_ts.min(entry.min_ts);
                    summary.max_ts = summary.max_ts.max(entry.max_ts);
                })
                .or_insert(state::PersistedSeriesTimeRangeSummary {
                    min_ts: entry.min_ts,
                    max_ts: entry.max_ts,
                });
            persisted_index
                .chunk_refs
                .entry(entry.series_id)
                .or_default()
                .push(chunk_ref);
            persisted_index
                .chunk_timestamp_indexes
                .insert(sequence, std::sync::OnceLock::new());
            affected_series.insert(entry.series_id);
        }

        persisted_index.next_segment_slot = persisted_index.next_segment_slot.saturating_add(1);
        persisted_index.next_chunk_sequence = next_chunk_sequence;
        persisted_index
            .segment_maps
            .insert(segment_slot, Arc::clone(&chunk_map));
        persisted_index
            .segment_tiers
            .insert(segment_slot, segment_tier);

        for refs in refs_by_series.values_mut() {
            Self::sort_persisted_chunk_refs(refs.as_mut_slice());
        }
        for series_id in &affected_series {
            if let Some(refs) = persisted_index.chunk_refs.get_mut(series_id) {
                Self::sort_persisted_chunk_refs(refs.as_mut_slice());
            }
        }

        persisted_index.segments_by_root.insert(
            root,
            state::PersistedSegmentState {
                segment_slot,
                lane: segment_lane,
                tier: segment_tier,
                manifest: segment_manifest,
                time_bucket_postings,
                series_time_summaries,
                chunk_refs_by_series: refs_by_series,
            },
        );

        Ok((
            affected_series.into_iter().collect(),
            loaded_max_timestamp,
            loaded_bounded_max_timestamp,
        ))
    }

    fn remove_segment_root_from_state(
        persisted_index: &mut PersistedIndexState,
        root: &Path,
    ) -> (Vec<SeriesId>, Vec<SeriesId>) {
        let Some(mut removed_segment) = persisted_index.segments_by_root.remove(root) else {
            return (Vec::new(), Vec::new());
        };

        persisted_index
            .segment_maps
            .remove(&removed_segment.segment_slot);
        persisted_index
            .segment_tiers
            .remove(&removed_segment.segment_slot);
        let mut affected_series = BTreeSet::new();
        let mut fully_removed_series = Vec::new();
        for (series_id, removed_refs) in std::mem::take(&mut removed_segment.chunk_refs_by_series) {
            affected_series.insert(series_id);
            for removed_ref in &removed_refs {
                persisted_index
                    .chunk_timestamp_indexes
                    .remove(&removed_ref.sequence);
            }
            let mut remove_series = false;
            if let Some(existing_refs) = persisted_index.chunk_refs.get_mut(&series_id) {
                existing_refs
                    .retain(|candidate| !removed_refs.iter().any(|removed| removed == candidate));
                remove_series = existing_refs.is_empty();
            }
            if remove_series {
                persisted_index.chunk_refs.remove(&series_id);
                fully_removed_series.push(series_id);
            }
        }

        (affected_series.into_iter().collect(), fully_removed_series)
    }
}
