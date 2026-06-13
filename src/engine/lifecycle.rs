use super::*;

impl ChunkStorage {
    pub(super) fn replay_from_wal(&self, replay_highwater: WalHighWatermark) -> Result<()> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };

        self.observability
            .wal
            .replay_runs_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let mut replay_frames = 0u64;
        let mut replay_series_defs = 0u64;
        let mut replay_sample_batches = 0u64;
        let mut replay_points = 0u64;

        let replay_result = (|| -> Result<()> {
            let mut stream = wal.replay_stream_after(replay_highwater)?;
            while let Some(frame) = stream.next_frame()? {
                replay_frames = replay_frames.saturating_add(1);
                match frame {
                    ReplayFrame::SeriesDefinition(definition) => {
                        replay_series_defs = replay_series_defs.saturating_add(1);
                        self.registry.write().register_series_with_id(
                            definition.series_id,
                            &definition.metric,
                            &definition.labels,
                        )?;
                    }
                    ReplayFrame::Samples(batches) => {
                        replay_sample_batches =
                            replay_sample_batches.saturating_add(batches.len() as u64);
                        for batch in batches {
                            let points = batch.decode_points()?;
                            replay_points = replay_points
                                .saturating_add(saturating_u64_from_usize(points.len()));
                            for point in points {
                                self.append_point_to_series(
                                    batch.series_id,
                                    batch.lane,
                                    point.ts,
                                    point.value,
                                )?;
                            }
                        }
                    }
                }
            }

            Ok(())
        })();

        self.observability
            .wal
            .replay_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match replay_result {
            Ok(()) => {
                self.observability
                    .wal
                    .replay_frames_total
                    .fetch_add(replay_frames, Ordering::Relaxed);
                self.observability
                    .wal
                    .replay_series_definitions_total
                    .fetch_add(replay_series_defs, Ordering::Relaxed);
                self.observability
                    .wal
                    .replay_sample_batches_total
                    .fetch_add(replay_sample_batches, Ordering::Relaxed);
                self.observability
                    .wal
                    .replay_points_total
                    .fetch_add(replay_points, Ordering::Relaxed);
                Ok(())
            }
            Err(err) => {
                self.observability
                    .wal
                    .replay_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    pub(super) fn apply_loaded_segment_indexes(
        &self,
        loaded: crate::engine::segment::LoadedSegmentIndexes,
        reconcile_registry_with_persisted: bool,
    ) -> Result<()> {
        {
            let mut registry = self.registry.write();
            for series in &loaded.series {
                registry.register_series_with_id(
                    series.series_id,
                    &series.metric,
                    &series.labels,
                )?;
            }
        }

        let mut persisted_refs = HashMap::<SeriesId, Vec<PersistedChunkRef>>::new();
        let mut persisted_maps = Vec::<Arc<PlatformMmap>>::new();
        let mut sequence = 1u64;
        let mut loaded_max_timestamp = i64::MIN;

        {
            for indexed_segment in loaded.indexed_segments {
                let segment_slot = persisted_maps.len();
                persisted_maps.push(Arc::new(indexed_segment.chunks_mmap));

                for entry in indexed_segment.chunk_index.entries {
                    loaded_max_timestamp = loaded_max_timestamp.max(entry.max_ts);
                    persisted_refs
                        .entry(entry.series_id)
                        .or_default()
                        .push(PersistedChunkRef {
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
                        });
                    sequence = sequence.saturating_add(1);
                }
            }
        }

        for chunks in persisted_refs.values_mut() {
            chunks.sort_by_key(|chunk| {
                (
                    chunk.min_ts,
                    chunk.max_ts,
                    chunk.point_count,
                    chunk.sequence,
                    chunk.chunk_offset,
                )
            });
        }

        if reconcile_registry_with_persisted {
            let keep = persisted_refs
                .keys()
                .copied()
                .collect::<BTreeSet<SeriesId>>();
            self.registry.write().retain_series_ids(&keep);
        }

        self.mark_materialized_series_ids(persisted_refs.keys().copied());

        {
            let mut persisted_index = self.persisted_index.write();
            persisted_index.chunk_refs = persisted_refs;
            persisted_index.segment_maps = persisted_maps;
        }

        if loaded_max_timestamp != i64::MIN {
            self.update_max_observed_timestamp(loaded_max_timestamp);
        }

        self.next_segment_id
            .store(loaded.next_segment_id.max(1), Ordering::SeqCst);
        Ok(())
    }

    fn rollback_published_segments(&self, segment_roots: &[PathBuf]) -> Result<()> {
        for root in segment_roots.iter().rev() {
            crate::engine::fs_utils::remove_dir_if_exists(root).map_err(|source| {
                TsinkError::IoWithPath {
                    path: root.clone(),
                    source,
                }
            })?;
        }

        Ok(())
    }

    fn reset_wal_with_stats(&self, wal: &FramedWal) -> Result<()> {
        match wal.reset() {
            Ok(()) => {
                self.observability
                    .wal
                    .resets_total
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(err) => {
                self.observability
                    .wal
                    .reset_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    pub(super) fn persist_segment(&self, include_wal_highwater: bool) -> Result<bool> {
        self.observability
            .flush
            .persist_runs_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let persist_result = (|| -> Result<(bool, usize, usize, usize, usize)> {
            if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
                return Ok((false, 0, 0, 0, 0));
            }

            let wal_highwater = if include_wal_highwater {
                self.wal
                    .as_ref()
                    .map(|wal| wal.current_highwater())
                    .unwrap_or_default()
            } else {
                WalHighWatermark::default()
            };

            let (delta_chunks, delta_watermarks) = {
                let persisted = self.persisted_chunk_watermarks.read();

                let mut delta = HashMap::new();
                let mut watermarks = HashMap::new();
                for shard in &self.sealed_chunks {
                    let sealed = shard.read();
                    for (series_id, chunks) in sealed.iter() {
                        let persisted_sequence = persisted.get(series_id).copied().unwrap_or(0);
                        let mut updates = Vec::new();
                        let mut max_sequence = persisted_sequence;
                        for (key, chunk) in chunks {
                            if key.sequence <= persisted_sequence {
                                continue;
                            }
                            max_sequence = max_sequence.max(key.sequence);
                            updates.push(chunk.clone());
                        }
                        if !updates.is_empty() {
                            delta.insert(*series_id, updates);
                            watermarks.insert(*series_id, max_sequence);
                        }
                    }
                }
                (delta, watermarks)
            };

            if delta_chunks.is_empty() {
                if let Some(wal) = &self.wal {
                    self.reset_wal_with_stats(wal)?;
                }
                return Ok((false, 0, 0, 0, 0));
            }

            let persisted_series = delta_chunks.len();
            let persisted_chunks = delta_chunks.values().map(std::vec::Vec::len).sum::<usize>();
            let persisted_points = delta_chunks
                .values()
                .flat_map(|chunks| chunks.iter())
                .map(|chunk| chunk.header.point_count as usize)
                .sum::<usize>();

            let mut numeric_chunks = HashMap::new();
            let mut blob_chunks = HashMap::new();
            let mut numeric_watermarks = HashMap::new();
            let mut blob_watermarks = HashMap::new();
            for (series_id, chunks) in &delta_chunks {
                let Some(first) = chunks.first() else {
                    continue;
                };
                let Some(watermark) = delta_watermarks.get(series_id).copied() else {
                    continue;
                };

                match first.header.lane {
                    ValueLane::Numeric => {
                        numeric_chunks.insert(*series_id, chunks.clone());
                        numeric_watermarks.insert(*series_id, watermark);
                    }
                    ValueLane::Blob => {
                        blob_chunks.insert(*series_id, chunks.clone());
                        blob_watermarks.insert(*series_id, watermark);
                    }
                }
            }

            if !numeric_chunks.is_empty() && self.numeric_lane_path.is_none() {
                return Err(TsinkError::InvalidConfiguration(
                    "cannot persist numeric chunks without numeric lane path".to_string(),
                ));
            }
            if !blob_chunks.is_empty() && self.blob_lane_path.is_none() {
                return Err(TsinkError::InvalidConfiguration(
                    "cannot persist blob chunks without blob lane path".to_string(),
                ));
            }

            let persisted_segments = {
                let registry = self.registry.read();
                let mut published_segment_roots = Vec::new();

                let persist_result = (|| -> Result<()> {
                    if let (Some(path), false) =
                        (&self.numeric_lane_path, numeric_chunks.is_empty())
                    {
                        let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
                        let writer = SegmentWriter::new(path, 0, segment_id)?;
                        writer.write_segment_with_wal_highwater(
                            &registry,
                            &numeric_chunks,
                            wal_highwater,
                        )?;
                        published_segment_roots.push(writer.layout().root.clone());
                    }

                    if let (Some(path), false) = (&self.blob_lane_path, blob_chunks.is_empty()) {
                        let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
                        let writer = SegmentWriter::new(path, 0, segment_id)?;
                        writer.write_segment_with_wal_highwater(
                            &registry,
                            &blob_chunks,
                            wal_highwater,
                        )?;
                        published_segment_roots.push(writer.layout().root.clone());
                    }

                    Ok(())
                })();

                if let Err(persist_err) = persist_result {
                    if let Err(rollback_err) =
                        self.rollback_published_segments(&published_segment_roots)
                    {
                        return Err(TsinkError::Other(format!(
                            "persist failed and rollback failed: persist={persist_err}, rollback={rollback_err}"
                        )));
                    }
                    return Err(persist_err);
                }

                published_segment_roots.len()
            };

            let mut flushed_watermarks = numeric_watermarks;
            flushed_watermarks.extend(blob_watermarks);
            self.mark_persisted_chunk_watermarks(&flushed_watermarks);

            if let Some(wal) = &self.wal {
                self.reset_wal_with_stats(wal)?;
            }

            Ok((
                true,
                persisted_series,
                persisted_chunks,
                persisted_points,
                persisted_segments,
            ))
        })();

        self.observability
            .flush
            .persist_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match persist_result {
            Ok((persisted, series, chunks, points, segments)) => {
                if persisted {
                    self.observability
                        .flush
                        .persist_success_total
                        .fetch_add(1, Ordering::Relaxed);
                    self.observability
                        .flush
                        .persisted_series_total
                        .fetch_add(saturating_u64_from_usize(series), Ordering::Relaxed);
                    self.observability
                        .flush
                        .persisted_chunks_total
                        .fetch_add(saturating_u64_from_usize(chunks), Ordering::Relaxed);
                    self.observability
                        .flush
                        .persisted_points_total
                        .fetch_add(saturating_u64_from_usize(points), Ordering::Relaxed);
                    self.observability
                        .flush
                        .persisted_segments_total
                        .fetch_add(saturating_u64_from_usize(segments), Ordering::Relaxed);
                } else {
                    self.observability
                        .flush
                        .persist_noop_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(persisted)
            }
            Err(err) => {
                self.observability
                    .flush
                    .persist_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    pub(super) fn compact_compactors(
        numeric_compactor: Option<&Compactor>,
        blob_compactor: Option<&Compactor>,
        observability: Option<&StorageObservabilityCounters>,
    ) -> Result<bool> {
        let mut compacted = false;
        if let Some(compactor) = numeric_compactor {
            compacted |= Self::run_compactor_once(compactor, observability)?;
        }
        if let Some(compactor) = blob_compactor {
            compacted |= Self::run_compactor_once(compactor, observability)?;
        }
        Ok(compacted)
    }

    fn run_compactor_once(
        compactor: &Compactor,
        observability: Option<&StorageObservabilityCounters>,
    ) -> Result<bool> {
        let started = Instant::now();
        match compactor.compact_once_with_stats() {
            Ok(stats) => {
                if let Some(obs) = observability {
                    obs.record_compaction_result(stats, elapsed_nanos_u64(started));
                }
                Ok(stats.compacted)
            }
            Err(err) => {
                if let Some(obs) = observability {
                    obs.record_compaction_error(elapsed_nanos_u64(started));
                }
                Err(err)
            }
        }
    }

    pub(super) fn compact_until_settled(&self, max_passes: usize) -> Result<usize> {
        let _compaction_guard = self.compaction_lock.lock();
        let mut passes = 0usize;
        for _ in 0..max_passes.max(1) {
            if !Self::compact_compactors(
                self.numeric_compactor.as_ref(),
                self.blob_compactor.as_ref(),
                Some(self.observability.as_ref()),
            )? {
                break;
            }
            passes = passes.saturating_add(1);
        }
        Ok(passes)
    }

    pub(super) fn close_impl(&self) -> Result<()> {
        if self
            .lifecycle
            .compare_exchange(
                STORAGE_OPEN,
                STORAGE_CLOSING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return Err(TsinkError::StorageClosed);
        }

        self.notify_compaction_thread();
        self.notify_flush_thread();

        let mut close_result = (|| {
            let _write_permits = self.write_limiter.acquire_all(self.write_timeout)?;
            self.flush_all_active()?;
            self.persist_segment(true)?;
            self.sweep_expired_persisted_segments()?;
            if self.memory_budget_value() != usize::MAX {
                self.refresh_memory_usage();
            }
            self.compact_until_settled(CLOSE_COMPACTION_MAX_PASSES)?;
            self.persist_series_registry_index()?;
            Ok(())
        })();

        if close_result.is_ok() {
            self.lifecycle.store(STORAGE_CLOSED, Ordering::SeqCst);
            self.notify_compaction_thread();
            self.notify_flush_thread();
            if let Err(err) = self.join_background_threads() {
                close_result = Err(err);
            }
        } else {
            self.lifecycle.store(STORAGE_OPEN, Ordering::SeqCst);
            self.notify_compaction_thread();
            self.notify_flush_thread();
        }

        close_result
    }
}
