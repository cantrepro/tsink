use super::super::maintenance::{
    PersistedCatalogPublication, PersistedCatalogTransition, PlannedPersistedCatalogRefresh,
};
use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64};

use parking_lot::RwLock;

#[derive(Clone, Copy, Debug, Default)]
pub(in super::super) struct PersistSegmentOutcome {
    pub(in super::super) persisted: bool,
    pub(in super::super) series: usize,
    pub(in super::super) chunks: usize,
    pub(in super::super) points: usize,
    pub(in super::super) segments: usize,
}

#[derive(Default)]
struct FlushPersistSnapshot {
    numeric_chunks: HashMap<SeriesId, Vec<Arc<Chunk>>>,
    blob_chunks: HashMap<SeriesId, Vec<Arc<Chunk>>>,
    numeric_watermarks: HashMap<SeriesId, u64>,
    blob_watermarks: HashMap<SeriesId, u64>,
    wal_highwater: WalHighWatermark,
    series: usize,
    chunks: usize,
    points: usize,
}

impl FlushPersistSnapshot {
    fn is_empty(&self) -> bool {
        self.numeric_chunks.is_empty() && self.blob_chunks.is_empty()
    }
}

struct StagedFlushPublish {
    published_segment_roots: Vec<PathBuf>,
    flushed_watermarks: HashMap<SeriesId, u64>,
    wal_highwater: WalHighWatermark,
    outcome: PersistSegmentOutcome,
}

struct VerifiedFlushPublish {
    published_segment_roots: Vec<PathBuf>,
    loaded_segments: Vec<crate::engine::segment::IndexedSegment>,
    flushed_watermarks: HashMap<SeriesId, u64>,
    wal_highwater: WalHighWatermark,
    outcome: PersistSegmentOutcome,
}

#[derive(Clone, Copy)]
struct FlushSnapshotContext<'a> {
    chunks: super::super::ChunkContext<'a>,
    persisted_chunk_watermarks: &'a RwLock<HashMap<SeriesId, u64>>,
    registry: &'a RwLock<SeriesRegistry>,
    next_segment_id: &'a AtomicU64,
    numeric_lane_path: Option<&'a Path>,
    blob_lane_path: Option<&'a Path>,
    wal: Option<&'a crate::engine::wal::FramedWal>,
}

#[derive(Clone, Copy)]
struct FlushPublishContext<'a>(&'a AtomicBool);

impl ChunkStorage {
    fn previous_wal_highwater(highwater: WalHighWatermark) -> WalHighWatermark {
        if highwater.frame > 0 {
            return WalHighWatermark {
                segment: highwater.segment,
                frame: highwater.frame - 1,
            };
        }

        if highwater.segment > 0 {
            return WalHighWatermark {
                segment: highwater.segment - 1,
                frame: u64::MAX,
            };
        }

        WalHighWatermark::default()
    }

    fn collect_flush_persist_snapshot(
        snapshot_ctx: FlushSnapshotContext<'_>,
    ) -> Result<FlushPersistSnapshot> {
        let persisted = snapshot_ctx.persisted_chunk_watermarks.read();
        let active_wal_floor = snapshot_ctx.wal.and_then(|_| {
            snapshot_ctx
                .chunks
                .active_builders
                .iter()
                .filter_map(|shard| {
                    let active = shard.read();
                    active
                        .values()
                        .filter_map(ActiveSeriesState::min_wal_highwater)
                        .min()
                })
                .min()
        });
        let mut snapshot = FlushPersistSnapshot::default();
        let mut max_chunk_wal_highwater = WalHighWatermark::default();

        for shard in snapshot_ctx.chunks.sealed_chunks {
            let sealed = shard.read();
            for (series_id, chunks) in sealed.iter() {
                let persisted_sequence = persisted.get(series_id).copied().unwrap_or(0);
                let mut snapshot_chunks = Vec::new();
                let mut max_sequence = persisted_sequence;
                let mut lane = None;
                let mut point_count = 0usize;

                for (key, chunk) in chunks {
                    if key.sequence <= persisted_sequence {
                        continue;
                    }

                    if let Some(expected_lane) = lane {
                        if expected_lane != chunk.header.lane {
                            return Err(TsinkError::DataCorruption(format!(
                                "series id {} has mixed sealed chunk lanes during flush",
                                series_id
                            )));
                        }
                    } else {
                        lane = Some(chunk.header.lane);
                    }

                    max_sequence = max_sequence.max(key.sequence);
                    point_count = point_count.saturating_add(chunk.header.point_count as usize);
                    max_chunk_wal_highwater = max_chunk_wal_highwater.max(chunk.wal_highwater);
                    snapshot_chunks.push(Arc::clone(chunk));
                }

                if snapshot_chunks.is_empty() {
                    continue;
                }

                snapshot.series = snapshot.series.saturating_add(1);
                snapshot.chunks = snapshot.chunks.saturating_add(snapshot_chunks.len());
                snapshot.points = snapshot.points.saturating_add(point_count);

                match lane.expect("lane is set for non-empty snapshot chunks") {
                    ValueLane::Numeric => {
                        snapshot.numeric_watermarks.insert(*series_id, max_sequence);
                        snapshot.numeric_chunks.insert(*series_id, snapshot_chunks);
                    }
                    ValueLane::Blob => {
                        snapshot.blob_watermarks.insert(*series_id, max_sequence);
                        snapshot.blob_chunks.insert(*series_id, snapshot_chunks);
                    }
                }
            }
        }

        snapshot.wal_highwater = active_wal_floor
            .map(Self::previous_wal_highwater)
            .map_or(max_chunk_wal_highwater, |floor| {
                max_chunk_wal_highwater.min(floor)
            });

        Ok(snapshot)
    }

    #[cfg(test)]
    fn invoke_persist_post_publish_hook(&self, segment_roots: &[PathBuf]) {
        let hook = self.persist_test_hooks.post_publish_hook.read().clone();
        if let Some(hook) = hook {
            hook(segment_roots);
        }
    }

    #[cfg(test)]
    pub(in super::super) fn set_persist_post_publish_hook<F>(&self, hook: F)
    where
        F: Fn(&[PathBuf]) + Send + Sync + 'static,
    {
        *self.persist_test_hooks.post_publish_hook.write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(in super::super) fn clear_persist_post_publish_hook(&self) {
        *self.persist_test_hooks.post_publish_hook.write() = None;
    }

    fn write_flush_segment_stage(
        snapshot_ctx: FlushSnapshotContext<'_>,
        registry: &SeriesRegistry,
        lane_path: &Path,
        chunks: &HashMap<SeriesId, Vec<Arc<Chunk>>>,
        wal_highwater: WalHighWatermark,
        published_segment_roots: &mut Vec<PathBuf>,
    ) -> Result<()> {
        let segment_id = snapshot_ctx.next_segment_id.fetch_add(1, Ordering::SeqCst);
        let writer = SegmentWriter::new(lane_path, 0, segment_id)?;
        writer.write_segment_with_wal_highwater(registry, chunks, wal_highwater)?;
        published_segment_roots.push(writer.layout().root.clone());
        Ok(())
    }

    fn stage_flush_segment_publication(
        &self,
        snapshot_ctx: FlushSnapshotContext<'_>,
    ) -> Result<Option<StagedFlushPublish>> {
        if snapshot_ctx.numeric_lane_path.is_none() && snapshot_ctx.blob_lane_path.is_none() {
            return Ok(None);
        }

        let flush_snapshot = Self::collect_flush_persist_snapshot(snapshot_ctx)?;
        if flush_snapshot.is_empty() {
            return Ok(None);
        }

        let FlushPersistSnapshot {
            numeric_chunks,
            blob_chunks,
            numeric_watermarks,
            blob_watermarks,
            wal_highwater,
            series,
            chunks,
            points,
        } = flush_snapshot;

        if !numeric_chunks.is_empty() && snapshot_ctx.numeric_lane_path.is_none() {
            return Err(TsinkError::InvalidConfiguration(
                "cannot persist numeric chunks without numeric lane path".to_string(),
            ));
        }
        if !blob_chunks.is_empty() && snapshot_ctx.blob_lane_path.is_none() {
            return Err(TsinkError::InvalidConfiguration(
                "cannot persist blob chunks without blob lane path".to_string(),
            ));
        }

        let published_segment_roots = {
            let registry = snapshot_ctx.registry.read();
            let mut published_segment_roots = Vec::new();

            let persist_result = (|| -> Result<()> {
                if let (Some(path), false) =
                    (snapshot_ctx.numeric_lane_path, numeric_chunks.is_empty())
                {
                    Self::write_flush_segment_stage(
                        snapshot_ctx,
                        &registry,
                        path,
                        &numeric_chunks,
                        wal_highwater,
                        &mut published_segment_roots,
                    )?;
                }

                if let (Some(path), false) = (snapshot_ctx.blob_lane_path, blob_chunks.is_empty()) {
                    Self::write_flush_segment_stage(
                        snapshot_ctx,
                        &registry,
                        path,
                        &blob_chunks,
                        wal_highwater,
                        &mut published_segment_roots,
                    )?;
                }

                Ok(())
            })();

            if let Err(persist_err) = persist_result {
                if let Err(rollback_err) =
                    self.rollback_published_segment_roots(&published_segment_roots)
                {
                    return Err(TsinkError::Other(format!(
                        "persist failed and rollback failed: persist={persist_err}, rollback={rollback_err}"
                    )));
                }
                return Err(persist_err);
            }

            published_segment_roots
        };

        #[cfg(test)]
        self.invoke_persist_post_publish_hook(&published_segment_roots);

        let mut flushed_watermarks = numeric_watermarks;
        flushed_watermarks.extend(blob_watermarks);

        Ok(Some(StagedFlushPublish {
            outcome: PersistSegmentOutcome {
                persisted: true,
                series,
                chunks,
                points,
                segments: published_segment_roots.len(),
            },
            published_segment_roots,
            flushed_watermarks,
            wal_highwater,
        }))
    }

    fn verify_flush_segment_publication(
        &self,
        staged: StagedFlushPublish,
    ) -> Result<VerifiedFlushPublish> {
        let mut loaded_segments = Vec::with_capacity(staged.published_segment_roots.len());
        for root in &staged.published_segment_roots {
            match crate::engine::segment::load_segment_index(root) {
                Ok(segment) => loaded_segments.push(segment),
                Err(err) => {
                    if let Err(rollback_err) =
                        self.rollback_published_segment_roots(&staged.published_segment_roots)
                    {
                        return Err(TsinkError::Other(format!(
                            "persist published unreadable segment and rollback failed: persist={err}, rollback={rollback_err}"
                        )));
                    }
                    return Err(err);
                }
            }
        }

        Ok(VerifiedFlushPublish {
            published_segment_roots: staged.published_segment_roots,
            loaded_segments,
            flushed_watermarks: staged.flushed_watermarks,
            wal_highwater: staged.wal_highwater,
            outcome: staged.outcome,
        })
    }

    fn persist_flush_recovery_metadata_stage(
        &self,
        publish_ctx: FlushPublishContext<'_>,
        published_segment_roots: &[PathBuf],
    ) -> Result<()> {
        // Recovery metadata must reach disk before the newly published segment roots are
        // allowed to become the engine's durable view. If this step fails, roll the new
        // roots back rather than exposing data that restart cannot fully recover.
        let _compaction_guard = self.compaction_gate();
        if publish_ctx.0.load(Ordering::SeqCst) {
            if let Err(err) = self.apply_known_dirty_persisted_refresh_if_pending() {
                tracing::warn!(
                    error = %err,
                    "Failed to apply known dirty persisted refresh before flush registry persistence; deferring reconcile"
                );
            }
        }

        let registry_catalog_sources = self
            .persisted_registry_catalog_sources_with_root_changes(published_segment_roots, &[])?;
        if let Err(err) =
            self.persist_series_registry_index_with_catalog_sources(&registry_catalog_sources)
        {
            if let Err(rollback_err) =
                self.rollback_published_segment_roots(published_segment_roots)
            {
                return Err(TsinkError::Other(format!(
                    "persist updated recovery metadata and rollback failed: persist={err}, rollback={rollback_err}"
                )));
            }
            return Err(err);
        }

        Ok(())
    }

    fn plan_flush_dirty_refresh_stage(
        &self,
        publish_ctx: FlushPublishContext<'_>,
    ) -> Option<PlannedPersistedCatalogRefresh> {
        if !publish_ctx.0.load(Ordering::SeqCst) {
            return None;
        }

        match self.plan_known_dirty_catalog_refresh() {
            Ok(planned) => planned,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "Failed to plan dirty persisted refresh after flush publish; deferring reconcile"
                );
                None
            }
        }
    }

    fn publish_verified_flush_visibility_stage(
        &self,
        publish_ctx: FlushPublishContext<'_>,
        verified: VerifiedFlushPublish,
        planned_dirty_refresh: Option<PlannedPersistedCatalogRefresh>,
    ) -> Result<(WalHighWatermark, PersistSegmentOutcome)> {
        let VerifiedFlushPublish {
            published_segment_roots,
            loaded_segments,
            flushed_watermarks,
            wal_highwater,
            outcome,
        } = verified;

        // Publish the new persisted view as one visibility transition: install segment
        // indexes, refresh catalog/caches, and only then consider trimming WAL state.
        let publication = self.begin_persisted_catalog_publication();
        let flush_transition = PersistedCatalogTransition {
            visibility_fence: None,
            loaded_segments,
            removed_roots: Vec::new(),
            publication: PersistedCatalogPublication::PersistedState {
                published_segment_roots: published_segment_roots.clone(),
            },
            registry_catalog_sources: None,
        };
        if let Err(err) = publication.publish_transition(flush_transition) {
            drop(publication);
            if let Err(rollback_err) =
                self.rollback_published_segment_roots(&published_segment_roots)
            {
                return Err(TsinkError::Other(format!(
                    "flush publication failed and rollback failed: publish={err}, rollback={rollback_err}"
                )));
            }
            if let Err(registry_err) = self.persist_series_registry_index() {
                return Err(TsinkError::Other(format!(
                    "flush publication failed and registry rollback failed: publish={err}, registry={registry_err}"
                )));
            }
            return Err(err);
        }
        self.mark_persisted_chunk_watermarks(&flushed_watermarks);
        let evicted = self.evict_persisted_sealed_chunks();
        self.observability
            .flush
            .evicted_sealed_chunks_total
            .fetch_add(saturating_u64_from_usize(evicted), Ordering::Relaxed);

        if let Some(planned_dirty_refresh) = planned_dirty_refresh {
            let restore_diff = planned_dirty_refresh.restore_known_dirty_diff();
            match publication.apply_planned_refresh(planned_dirty_refresh) {
                Ok(result) if result.is_applied() => {
                    publish_ctx
                        .0
                        .store(self.has_known_persisted_segment_changes(), Ordering::SeqCst);
                }
                Ok(_) => {
                    unreachable!("flush should only preplan known dirty catalog refreshes");
                }
                Err(err) => {
                    if let Some(restore_diff) = restore_diff {
                        self.restore_known_persisted_segment_changes(restore_diff);
                    }
                    publish_ctx.0.store(true, Ordering::SeqCst);
                    tracing::warn!(
                        error = %err,
                        "Failed to apply known dirty persisted refresh during flush; serving the last visible catalog until retry"
                    );
                }
            }
        }

        drop(publication);

        Ok((wal_highwater, outcome))
    }

    fn reset_flush_wal_stage(
        &self,
        snapshot_ctx: FlushSnapshotContext<'_>,
        wal_highwater: WalHighWatermark,
    ) -> Result<()> {
        let Some(wal) = snapshot_ctx.wal else {
            return Ok(());
        };

        // Only reset the WAL while holding the WAL writer lock and only if no newer
        // committed write has appeared since the flush snapshot was taken. This keeps
        // background and memory-pressure persists off the global writer permit hot path.
        let reset = match wal.reset_if_current_highwater_at_most(wal_highwater) {
            Ok(reset) => reset,
            Err(err) => {
                self.observability
                    .wal
                    .reset_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                return Err(err);
            }
        };
        if reset {
            self.observability
                .wal
                .resets_total
                .fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    pub(in super::super) fn persist_segment_with_outcome(&self) -> Result<PersistSegmentOutcome> {
        self.observability
            .flush
            .persist_runs_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let persist_result = (|| -> Result<PersistSegmentOutcome> {
            let snapshot_ctx = FlushSnapshotContext {
                chunks: self.chunk_context(),
                persisted_chunk_watermarks: &self.chunks.persisted_chunk_watermarks,
                registry: &self.catalog.registry,
                next_segment_id: &self.persisted.next_segment_id,
                numeric_lane_path: self.persisted.numeric_lane_path.as_deref(),
                blob_lane_path: self.persisted.blob_lane_path.as_deref(),
                wal: self.persisted.wal.as_ref(),
            };
            let publish_ctx = FlushPublishContext(&self.persisted.persisted_index_dirty);
            let Some(staged_flush) = self.stage_flush_segment_publication(snapshot_ctx)? else {
                return Ok(PersistSegmentOutcome::default());
            };
            let verified_flush = self.verify_flush_segment_publication(staged_flush)?;
            self.persist_flush_recovery_metadata_stage(
                publish_ctx,
                &verified_flush.published_segment_roots,
            )?;

            if let Some(wal) = snapshot_ctx.wal {
                wal.mark_durable_through(verified_flush.wal_highwater);
            }

            let planned_dirty_refresh = self.plan_flush_dirty_refresh_stage(publish_ctx);
            let (wal_highwater, outcome) = self.publish_verified_flush_visibility_stage(
                publish_ctx,
                verified_flush,
                planned_dirty_refresh,
            )?;
            self.reset_flush_wal_stage(snapshot_ctx, wal_highwater)?;
            Ok(outcome)
        })();

        self.observability
            .flush
            .persist_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match persist_result {
            Ok(outcome) => {
                if outcome.persisted {
                    self.observability
                        .flush
                        .persist_success_total
                        .fetch_add(1, Ordering::Relaxed);
                    self.observability
                        .flush
                        .persisted_series_total
                        .fetch_add(saturating_u64_from_usize(outcome.series), Ordering::Relaxed);
                    self.observability
                        .flush
                        .persisted_chunks_total
                        .fetch_add(saturating_u64_from_usize(outcome.chunks), Ordering::Relaxed);
                    self.observability
                        .flush
                        .persisted_points_total
                        .fetch_add(saturating_u64_from_usize(outcome.points), Ordering::Relaxed);
                    self.observability.flush.persisted_segments_total.fetch_add(
                        saturating_u64_from_usize(outcome.segments),
                        Ordering::Relaxed,
                    );
                } else {
                    self.observability
                        .flush
                        .persist_noop_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                if self.memory.accounting_enabled && outcome.persisted {
                    self.refresh_memory_usage();
                }
                Ok(outcome)
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

    pub(in super::super) fn persist_segment(&self) -> Result<bool> {
        Ok(self.persist_segment_with_outcome()?.persisted)
    }
}
