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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PersistSnapshotPolicy {
    All,
    BackgroundBounded { max_items: usize, max_bytes: u64 },
}

#[derive(Default)]
struct FlushPersistSnapshot {
    numeric_chunks: HashMap<SeriesId, Vec<Arc<Chunk>>>,
    blob_chunks: HashMap<SeriesId, Vec<Arc<Chunk>>>,
    numeric_watermarks: HashMap<SeriesId, u64>,
    blob_watermarks: HashMap<SeriesId, u64>,
    selected_sealed_locations: Vec<PendingSealedChunkLocation>,
    wal_highwater: WalHighWatermark,
    series: usize,
    chunks: usize,
    inspected_chunks: usize,
    points: usize,
    selected_input_bytes: u64,
}

impl FlushPersistSnapshot {
    fn is_empty(&self) -> bool {
        self.numeric_chunks.is_empty() && self.blob_chunks.is_empty()
    }
}

struct StagedFlushPublish {
    published_segment_roots: Vec<PathBuf>,
    flushed_watermarks: HashMap<SeriesId, u64>,
    selected_sealed_locations: Vec<PendingSealedChunkLocation>,
    wal_highwater: WalHighWatermark,
    outcome: PersistSegmentOutcome,
    inspected_chunks: usize,
    selected_input_bytes: u64,
}

struct VerifiedFlushPublish {
    published_segment_roots: Vec<PathBuf>,
    loaded_segments: Vec<crate::engine::segment::IndexedSegment>,
    flushed_watermarks: HashMap<SeriesId, u64>,
    selected_sealed_locations: Vec<PendingSealedChunkLocation>,
    wal_highwater: WalHighWatermark,
    outcome: PersistSegmentOutcome,
    inspected_chunks: usize,
    selected_input_bytes: u64,
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
    local_disk_budget: Option<&'a Arc<crate::LocalDiskBudget>>,
}

#[derive(Clone, Copy)]
struct FlushPublishContext<'a>(&'a AtomicBool);

impl ChunkStorage {
    fn uses_finite_background_flush_fence(&self, policy: PersistSnapshotPolicy) -> bool {
        policy != PersistSnapshotPolicy::All
            && (self.runtime.maintenance_max_items_per_pass != usize::MAX
                || self.runtime.maintenance_max_bytes_per_pass != u64::MAX)
    }

    fn is_disk_capacity_rejection(error: &TsinkError) -> bool {
        matches!(
            error,
            TsinkError::DiskQuotaExceeded { .. }
                | TsinkError::InsufficientCompactionHeadroom { .. }
                | TsinkError::InsufficientDiskSpace { .. }
        )
    }

    fn collect_flush_persist_snapshot(
        &self,
        snapshot_ctx: FlushSnapshotContext<'_>,
        policy: PersistSnapshotPolicy,
    ) -> Result<FlushPersistSnapshot> {
        let persisted = snapshot_ctx.persisted_chunk_watermarks.read();
        let active_wal_floor = snapshot_ctx
            .wal
            .and_then(|_| snapshot_ctx.chunks.active_wal_index.lock().minimum());
        let (max_items, max_bytes) = match policy {
            PersistSnapshotPolicy::All => (usize::MAX, u64::MAX),
            PersistSnapshotPolicy::BackgroundBounded {
                max_items,
                max_bytes,
            } => (max_items, max_bytes),
        };
        if max_items == 0 || max_bytes == 0 {
            return Ok(FlushPersistSnapshot::default());
        }

        // The secondary index makes the selected work proportional to the configured pass
        // allowance instead of cloning every sealed Arc on every background wake. Sequence
        // prefix selection preserves the existing scalar persisted-watermark invariant, while
        // the WAL view gives an exact replay floor for deferred chunks.
        let pending = snapshot_ctx.chunks.pending_sealed_chunks.read();
        if pending.is_empty() {
            return Ok(FlushPersistSnapshot::default());
        }
        let mut candidates = Vec::with_capacity(max_items.min(pending.len()));
        let mut selected_input_bytes = 0u64;
        let mut inspected_chunks = 0usize;
        let mut byte_limit_hit = false;
        let mut oversized_required = None;
        for (sequence, location) in &pending.by_sequence {
            if candidates.len() >= max_items {
                break;
            }
            inspected_chunks = inspected_chunks.saturating_add(1);
            if location.input_bytes > max_bytes.saturating_sub(selected_input_bytes) {
                byte_limit_hit = true;
                if candidates.is_empty()
                    && matches!(policy, PersistSnapshotPolicy::BackgroundBounded { .. })
                {
                    oversized_required = Some(location.input_bytes);
                }
                break;
            }
            selected_input_bytes = selected_input_bytes.saturating_add(location.input_bytes);
            candidates.push((
                PendingSealedChunkIndexKey {
                    wal_lowwater: location.wal_lowwater,
                    sequence: *sequence,
                },
                *location,
            ));
        }
        let item_limit_hit = candidates.len() >= max_items && candidates.len() < pending.len();
        let last_selected_sequence = candidates.last().map(|(key, _)| key.sequence);
        let first_unselected_wal_floor = pending
            .by_wal
            .iter()
            .find(|key| last_selected_sequence.is_none_or(|last| key.sequence > last))
            .map(|key| key.wal_lowwater);
        drop(pending);

        if matches!(policy, PersistSnapshotPolicy::BackgroundBounded { .. }) {
            self.observability
                .flush
                .persist_inspected_chunks_total
                .fetch_add(
                    saturating_u64_from_usize(inspected_chunks),
                    Ordering::Relaxed,
                );
            self.observability
                .flush
                .persist_selected_input_bytes_total
                .fetch_add(selected_input_bytes, Ordering::Relaxed);
            if item_limit_hit {
                self.observability
                    .flush
                    .persist_item_limit_hits_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            if byte_limit_hit {
                self.observability
                    .flush
                    .persist_byte_limit_hits_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Some(required) = oversized_required {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: "sealed chunk persistence",
                limit: max_bytes,
                required,
            });
        }

        let mut snapshot = FlushPersistSnapshot::default();
        let mut max_chunk_wal_highwater = WalHighWatermark::default();
        for (index_key, location) in candidates {
            let persisted_sequence = persisted.get(&location.series_id).copied().unwrap_or(0);
            if location.sealed_key.sequence <= persisted_sequence {
                // A successful publication removes the pending key before another persist can
                // start under the compaction gate. Treat a transient/stale locator as a no-op
                // rather than advancing the WAL checkpoint past data we did not snapshot.
                return Ok(FlushPersistSnapshot::default());
            }

            let sealed = snapshot_ctx.chunks.sealed_chunks[location.shard_idx].read();
            let Some(chunk) = sealed
                .get(&location.series_id)
                .and_then(|chunks| chunks.get(&location.sealed_key))
            else {
                // Publication installs the index entry while holding the sealed shard lock, so
                // this is only reachable for a stale locator. Staying put is recovery-safe.
                return Ok(FlushPersistSnapshot::default());
            };
            if chunk.wal_lowwater != index_key.wal_lowwater
                || chunk.wal_highwater != location.wal_highwater
            {
                return Err(TsinkError::DataCorruption(format!(
                    "sealed chunk WAL range changed for series id {} sequence {}",
                    location.series_id, location.sealed_key.sequence
                )));
            }

            let (chunks_by_series, watermarks) = match chunk.header.lane {
                ValueLane::Numeric => (
                    &mut snapshot.numeric_chunks,
                    &mut snapshot.numeric_watermarks,
                ),
                ValueLane::Blob => (&mut snapshot.blob_chunks, &mut snapshot.blob_watermarks),
            };
            let entry = chunks_by_series.entry(location.series_id).or_default();
            if entry.is_empty() {
                snapshot.series = snapshot.series.saturating_add(1);
            }
            entry.push(Arc::clone(chunk));
            let watermark = watermarks.entry(location.series_id).or_insert(0);
            *watermark = (*watermark).max(location.sealed_key.sequence);
            snapshot.chunks = snapshot.chunks.saturating_add(1);
            snapshot.points = snapshot
                .points
                .saturating_add(chunk.header.point_count as usize);
            max_chunk_wal_highwater = max_chunk_wal_highwater.max(location.wal_highwater);
            snapshot.selected_sealed_locations.push(location);
        }
        drop(persisted);

        // A segment replay watermark is a scalar prefix: every WAL frame at or below it is
        // skipped after restart. Publishing selected data while lowering that watermark below
        // one of the selected chunks would replay the chunk and duplicate its points; raising it
        // to the selected maximum while a deferred chunk/head starts at or below that frame would
        // skip unpersisted data. Therefore a partial snapshot is publishable only when its WAL
        // interval is closed strictly before every deferred interval.
        if snapshot_ctx.wal.is_some() {
            if active_wal_floor.is_some_and(|floor| floor <= max_chunk_wal_highwater) {
                // A timed active-finalization pass can move this dependency into the sealed
                // prefix. Until then, staying put is the only replay-exact outcome.
                return Ok(FlushPersistSnapshot::default());
            }
            if first_unselected_wal_floor.is_some_and(|floor| floor <= max_chunk_wal_highwater) {
                if matches!(policy, PersistSnapshotPolicy::BackgroundBounded { .. })
                    && (max_items < self.runtime.maintenance_max_items_per_pass
                        || max_bytes < self.runtime.maintenance_max_bytes_per_pass)
                {
                    // Active finalization and sealed persistence share one pass allowance. A
                    // dependency window that does not fit only the *remainder* may still fit the
                    // configured full allowance on the next wake, so it is a safe deferral rather
                    // than a configuration error.
                    return Ok(FlushPersistSnapshot::default());
                }
                return Err(TsinkError::MaintenanceDependencyWindowExceeded {
                    operation: "sealed chunk persistence",
                    item_limit: max_items,
                    byte_limit: max_bytes,
                    selected_items: inspected_chunks,
                    selected_bytes: selected_input_bytes,
                });
            }
        }
        snapshot.wal_highwater = max_chunk_wal_highwater;
        snapshot.inspected_chunks = inspected_chunks;
        snapshot.selected_input_bytes = selected_input_bytes;

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

    #[cfg(test)]
    fn invoke_flush_pre_visibility_publish_hook(&self) -> Result<()> {
        let hook = self
            .persist_test_hooks
            .flush_pre_visibility_publish_hook
            .read()
            .clone();
        match hook {
            Some(hook) => hook(),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub(in super::super) fn set_flush_pre_visibility_publish_hook<F>(&self, hook: F)
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        *self
            .persist_test_hooks
            .flush_pre_visibility_publish_hook
            .write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(in super::super) fn clear_flush_pre_visibility_publish_hook(&self) {
        self.persist_test_hooks
            .flush_pre_visibility_publish_hook
            .write()
            .take();
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
        let writer = SegmentWriter::new_with_disk_budget(
            lane_path,
            0,
            segment_id,
            snapshot_ctx.local_disk_budget.cloned(),
            crate::DiskReservationKind::Maintenance,
        )?;
        writer.write_segment_with_wal_highwater(registry, chunks, wal_highwater)?;
        published_segment_roots.push(writer.layout().root.clone());
        Ok(())
    }

    fn stage_flush_segment_publication(
        &self,
        snapshot_ctx: FlushSnapshotContext<'_>,
        policy: PersistSnapshotPolicy,
    ) -> Result<Option<StagedFlushPublish>> {
        if snapshot_ctx.numeric_lane_path.is_none() && snapshot_ctx.blob_lane_path.is_none() {
            return Ok(None);
        }

        let flush_snapshot = self.collect_flush_persist_snapshot(snapshot_ctx, policy)?;
        if flush_snapshot.is_empty() {
            return Ok(None);
        }

        let FlushPersistSnapshot {
            numeric_chunks,
            blob_chunks,
            numeric_watermarks,
            blob_watermarks,
            selected_sealed_locations,
            wal_highwater,
            series,
            chunks,
            inspected_chunks,
            points,
            selected_input_bytes,
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

        if !self.uses_finite_background_flush_fence(policy) {
            if let Some(data_path) = self
                .persisted
                .series_index_path
                .as_deref()
                .and_then(Path::parent)
            {
                // The caller already holds the compaction gate. Prove the namespace clean after
                // finding real flush work but before writing any discoverable segment root, then
                // retain that gate through recovery metadata and visibility publication.
                super::super::maintenance::ensure_no_pending_post_flush_replacement(data_path)?;
            }
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
            selected_sealed_locations,
            wal_highwater,
            inspected_chunks,
            selected_input_bytes,
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
            selected_sealed_locations: staged.selected_sealed_locations,
            wal_highwater: staged.wal_highwater,
            outcome: staged.outcome,
            inspected_chunks: staged.inspected_chunks,
            selected_input_bytes: staged.selected_input_bytes,
        })
    }

    fn persist_flush_recovery_metadata_stage(
        &self,
        publish_ctx: FlushPublishContext<'_>,
        published_segment_roots: &[PathBuf],
        selected_series_ids: &[SeriesId],
        policy: PersistSnapshotPolicy,
    ) -> Result<()> {
        // Recovery metadata must reach disk before the newly published segment roots are
        // allowed to become the engine's durable view. The caller holds the compaction gate
        // from before segment staging through the visibility swap, so a compactor cannot
        // discover and retire these roots while the flush transaction is still preparing.
        // If this step fails, roll the new roots back rather than exposing data that restart
        // cannot fully recover. Both finite and exhaustive clean-fence modes have already proved
        // the marker namespace safe under the caller's still-live compaction gate.
        if publish_ctx.0.load(Ordering::SeqCst) && policy == PersistSnapshotPolicy::All {
            if let Err(err) = self.drain_known_dirty_persisted_refresh_if_pending() {
                if let Err(rollback_err) =
                    self.rollback_published_segment_roots(published_segment_roots)
                {
                    return Err(TsinkError::Other(format!(
                        "known-dirty catalog drain failed before flush publication and rollback failed: drain={err}, rollback={rollback_err}"
                    )));
                }
                return Err(err);
            }
        }

        let persist_result = match policy {
            PersistSnapshotPolicy::All => self
                .persisted_registry_catalog_sources_with_root_changes(published_segment_roots, &[])
                .and_then(|registry_catalog_sources| {
                    self.persist_series_registry_index_with_catalog_sources(
                        &registry_catalog_sources,
                    )
                }),
            PersistSnapshotPolicy::BackgroundBounded { .. } => {
                self.persist_selected_series_registry_index_without_catalog(selected_series_ids)
            }
        };
        if let Err(err) = persist_result {
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
        policy: PersistSnapshotPolicy,
    ) -> Option<PlannedPersistedCatalogRefresh> {
        if policy != PersistSnapshotPolicy::All {
            return None;
        }
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
        policy: PersistSnapshotPolicy,
    ) -> Result<(WalHighWatermark, PersistSegmentOutcome)> {
        let VerifiedFlushPublish {
            published_segment_roots,
            loaded_segments,
            flushed_watermarks,
            selected_sealed_locations,
            wal_highwater,
            outcome,
            inspected_chunks,
            selected_input_bytes,
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
                refresh_tombstones: false,
            },
            registry_catalog_update: None,
        };
        let publish_result = (|| -> Result<_> {
            #[cfg(test)]
            self.invoke_flush_pre_visibility_publish_hook()?;
            match policy {
                PersistSnapshotPolicy::All => publication.publish_transition(flush_transition),
                PersistSnapshotPolicy::BackgroundBounded {
                    max_items,
                    max_bytes,
                } => {
                    // Chunk selection already spent this wake's source-work allowance. The
                    // visibility transition is part of that same atomic work item, so reserve
                    // its exact staging peak and give committed-tombstone recovery only the true
                    // remainder rather than a fresh (or unlimited) maintenance envelope.
                    let staging_bytes =
                        self.modeled_finite_transition_staging_bytes(&flush_transition);
                    let staging_work = u64::try_from(staging_bytes).unwrap_or(u64::MAX);
                    let selected_work = selected_input_bytes.max(staging_work);
                    if selected_work > max_bytes {
                        return Err(TsinkError::MaintenanceWorkItemTooLarge {
                            operation: "bounded flush catalog publication",
                            limit: max_bytes,
                            required: selected_work,
                        });
                    }
                    let transition_reservation =
                        self.remote_catalog_memory_reservation(staging_bytes)?;
                    let result = publication.publish_transition_with_finite_recovery_budget(
                        flush_transition,
                        max_items.saturating_sub(inspected_chunks),
                        max_bytes.saturating_sub(selected_work),
                        true,
                    );
                    drop(transition_reservation);
                    result
                }
            }
        })();
        let catalog_publication_deferred = publish_result
            .as_ref()
            .is_ok_and(|result| result.is_deferred());
        if let Err(err) = publish_result {
            // A transition can fail after installing the loaded indexes but before publishing
            // catalog/registry state. Remove any installed roots while the visibility fence is
            // still exclusive so queries can never observe the failed transition.
            let has_visible_published_root = {
                let persisted_index = self.persisted.persisted_index.read();
                published_segment_roots
                    .iter()
                    .any(|root| persisted_index.segments_by_root.contains_key(root))
            };
            let visibility_rollback = if has_visible_published_root {
                self.remove_persisted_segment_roots_with_observability_delta(
                    &published_segment_roots,
                )
            } else {
                Ok(false)
            };
            drop(publication);
            let disk_rollback = self.rollback_published_segment_roots(&published_segment_roots);
            let (catalog_rollback, registry_rollback) = match policy {
                PersistSnapshotPolicy::All => (
                    self.refresh_segment_catalog_and_observability_from_persisted_state(&[]),
                    self.persist_series_registry_index(),
                ),
                PersistSnapshotPolicy::BackgroundBounded { .. } => {
                    // Do not turn a failed finite flush into an all-roots repair. The exact
                    // rollback roots are sufficient to replay the catalog and registry removal
                    // through the ordinary one-root dirty-refresh path. A tiered writer may have
                    // reached any point in its catalog/registry publication, so retain the
                    // complete-registry sticky debt across cursor invalidation as well.
                    self.persisted
                        .pending_persisted_segment_diff
                        .lock()
                        .record_changes(Vec::<PathBuf>::new(), published_segment_roots.clone());
                    if self.finite_tiered_catalog_publication_enabled() {
                        self.coordination
                            .bounded_registry_reconciliation_required
                            .store(true, Ordering::Release);
                    }
                    self.persisted
                        .persisted_index_dirty
                        .store(true, Ordering::SeqCst);
                    (Ok(()), Ok(()))
                }
            };
            let mut rollback_errors = Vec::new();
            if let Err(rollback_err) = visibility_rollback {
                rollback_errors.push(format!("persisted visibility: {rollback_err}"));
            }
            if let Err(rollback_err) = disk_rollback {
                rollback_errors.push(format!("segment roots: {rollback_err}"));
            }
            if let Err(rollback_err) = catalog_rollback {
                rollback_errors.push(format!("segment catalog/observability: {rollback_err}"));
            }
            if let Err(rollback_err) = registry_rollback {
                rollback_errors.push(format!("registry: {rollback_err}"));
            }
            if !rollback_errors.is_empty() {
                return Err(TsinkError::Other(format!(
                    "flush publication failed and rollback failed: publish={err}; {}",
                    rollback_errors.join("; ")
                )));
            }
            return Err(err);
        }
        if catalog_publication_deferred {
            publish_ctx.0.store(true, Ordering::SeqCst);
        }
        self.mark_persisted_chunk_watermarks(&flushed_watermarks);
        {
            let mut pending = self.chunks.pending_sealed_chunks.write();
            for location in &selected_sealed_locations {
                pending.remove(PendingSealedChunkIndexKey {
                    wal_lowwater: location.wal_lowwater,
                    sequence: location.sealed_key.sequence,
                });
            }
        }
        let evicted = self.evict_selected_persisted_sealed_chunks(&selected_sealed_locations);
        self.observability
            .flush
            .evicted_sealed_chunks_total
            .fetch_add(saturating_u64_from_usize(evicted), Ordering::Relaxed);

        if let Some(planned_dirty_refresh) = planned_dirty_refresh {
            let restore_diff = planned_dirty_refresh.restore_known_dirty_diff();
            let restore_diff_conditionally =
                planned_dirty_refresh.restore_known_dirty_diff_conditionally();
            match publication.apply_planned_refresh(planned_dirty_refresh) {
                Ok(result) if result.is_applied() => {
                    self.synchronize_persisted_index_dirty_with_pending();
                }
                Ok(result) if result.is_deferred() => {
                    // The planned diff is already installed in visible persisted state. The
                    // retained finite writer cursor owns the resulting complete catalog image;
                    // replaying this diff would invalidate that cursor on every retry.
                    publish_ctx.0.store(true, Ordering::SeqCst);
                }
                Ok(_) => {
                    unreachable!("flush should only preplan known dirty catalog refreshes");
                }
                Err(err) => {
                    if let Some(restore_diff) = restore_diff {
                        if restore_diff_conditionally {
                            self.restore_known_persisted_segment_change_if_unmodified(restore_diff);
                        } else {
                            self.restore_known_persisted_segment_changes(restore_diff);
                        }
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
        let publication = self.lifecycle_publication_context();
        let reset = match wal.reset_if_current_highwater_at_most(wal_highwater, |bytes| {
            publication.reset_wal_series_definition_cache_memory_usage(bytes);
        }) {
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

    fn prepare_and_publish_flush_once(
        &self,
        snapshot_ctx: FlushSnapshotContext<'_>,
        publish_ctx: FlushPublishContext<'_>,
        policy: PersistSnapshotPolicy,
    ) -> Result<Option<(WalHighWatermark, PersistSegmentOutcome)>> {
        // Segment directories become discoverable as soon as their atomic writer publish
        // completes. Fence the whole transaction, not just recovery-metadata persistence, so
        // directory-scanning compaction cannot consume and remove a staged root before verify
        // and catalog publication finish.
        let _compaction_guard = self.compaction_gate();
        if policy != PersistSnapshotPolicy::All && publish_ctx.0.load(Ordering::SeqCst) {
            // Persisted-catalog reconciliation owns a separate bounded wake. Starting a flush
            // while that work is pending would either spend two complete envelopes or strand a
            // preplanned delta behind the flush transition. Leave sealed/WAL state untouched and
            // let the persisted-refresh worker establish the current catalog first.
            return Ok(None);
        }
        if self.uses_finite_background_flush_fence(policy) {
            let PersistSnapshotPolicy::BackgroundBounded {
                max_items,
                max_bytes,
            } = policy
            else {
                unreachable!("only bounded persistence can have a finite maintenance envelope")
            };
            // A pass with no remaining envelope or no sealed candidate preserves the established
            // bounded no-op behavior. In particular, active finalization may have consumed the
            // complete shared pass before persistence is reached.
            if max_items == 0
                || max_bytes == 0
                || snapshot_ctx.chunks.pending_sealed_chunks.read().is_empty()
            {
                return Ok(None);
            }
            if let Some(required) = snapshot_ctx
                .chunks
                .pending_sealed_chunks
                .read()
                .by_sequence
                .values()
                .next()
                .map(|location| location.input_bytes)
                .filter(|required| *required > max_bytes)
            {
                // Preserve the direct sealed-work dependency error without allocating or
                // inspecting a snapshot that cannot fit this pass. The fence is relevant only
                // once there is a publishable candidate to protect.
                return Err(TsinkError::MaintenanceWorkItemTooLarge {
                    operation: "sealed chunk persistence",
                    limit: max_bytes,
                    required,
                });
            }

            if let Some(data_path) = self
                .persisted
                .series_index_path
                .as_deref()
                .and_then(Path::parent)
            {
                // Prove the marker namespace clean before writing any discoverable segment root.
                // The compaction gate remains held through staging and visibility publication, so
                // a marker publisher cannot invalidate this terminal generation in between.
                let fence =
                    self.advance_finite_post_flush_clean_fence(data_path, max_items, max_bytes)?;
                if matches!(
                    fence,
                    super::super::maintenance::BackgroundPostFlushCleanFenceStep::EnvelopeConsumed
                ) {
                    return Ok(None);
                }
            }
        }
        let Some(staged_flush) = self.stage_flush_segment_publication(snapshot_ctx, policy)? else {
            return Ok(None);
        };
        let verified_flush = self.verify_flush_segment_publication(staged_flush)?;
        let selected_series_ids = verified_flush
            .flushed_watermarks
            .keys()
            .copied()
            .collect::<Vec<_>>();
        self.persist_flush_recovery_metadata_stage(
            publish_ctx,
            &verified_flush.published_segment_roots,
            &selected_series_ids,
            policy,
        )?;

        let planned_dirty_refresh = self.plan_flush_dirty_refresh_stage(publish_ctx, policy);
        let published = self.publish_verified_flush_visibility_stage(
            publish_ctx,
            verified_flush,
            planned_dirty_refresh,
            policy,
        )?;
        // Durability is monotonic and cannot be rolled back. Advance it only after the segment
        // roots and catalog visibility have committed successfully; a failed publication rolls
        // the roots back and must leave a concurrent periodic-WAL acknowledgement non-durable.
        if let Some(wal) = snapshot_ctx.wal {
            wal.mark_durable_through(published.0);
        }
        Ok(Some(published))
    }

    fn prepare_and_publish_flush(
        &self,
        snapshot_ctx: FlushSnapshotContext<'_>,
        publish_ctx: FlushPublishContext<'_>,
        policy: PersistSnapshotPolicy,
    ) -> Result<Option<(WalHighWatermark, PersistSegmentOutcome)>> {
        match self.prepare_and_publish_flush_once(snapshot_ctx, publish_ctx, policy) {
            Err(error) if Self::is_disk_capacity_rejection(&error) => {
                // The failed attempt has released the compaction gate. Retention cleanup takes
                // that gate itself, then a successful reclaim retries the complete flush
                // transaction under a fresh guard.
                match self.reclaim_fully_expired_segments_after_capacity_rejection() {
                    Ok(true) => {
                        self.prepare_and_publish_flush_once(snapshot_ctx, publish_ctx, policy)
                    }
                    Ok(false) => Err(error),
                    Err(cleanup_error) if Self::is_disk_capacity_rejection(&cleanup_error) => {
                        tracing::warn!(
                            initial_error = %error,
                            cleanup_error = %cleanup_error,
                            "Retention cleanup could not reclaim capacity before flush rejection"
                        );
                        Err(error)
                    }
                    Err(cleanup_error) => Err(cleanup_error),
                }
            }
            result => result,
        }
    }

    fn persist_segment_once(&self, policy: PersistSnapshotPolicy) -> Result<PersistSegmentOutcome> {
        let snapshot_ctx = FlushSnapshotContext {
            chunks: self.chunk_context(),
            persisted_chunk_watermarks: &self.chunks.persisted_chunk_watermarks,
            registry: &self.catalog.registry,
            next_segment_id: &self.persisted.next_segment_id,
            numeric_lane_path: self.persisted.numeric_lane_path.as_deref(),
            blob_lane_path: self.persisted.blob_lane_path.as_deref(),
            wal: self.persisted.wal.as_ref(),
            local_disk_budget: self.persisted.local_disk_budget.as_ref(),
        };
        let publish_ctx = FlushPublishContext(&self.persisted.persisted_index_dirty);
        let Some((wal_highwater, outcome)) =
            self.prepare_and_publish_flush(snapshot_ctx, publish_ctx, policy)?
        else {
            return Ok(PersistSegmentOutcome::default());
        };
        self.reset_flush_wal_stage(snapshot_ctx, wal_highwater)?;
        Ok(outcome)
    }

    pub(in crate::engine) fn reclaim_fully_expired_segments_after_capacity_rejection(
        &self,
    ) -> Result<bool> {
        if !self.runtime.retention_enforced
            || (self.persisted.numeric_lane_path.is_none()
                && self.persisted.blob_lane_path.is_none())
        {
            return Ok(false);
        }
        let Some(local_disk_budget) = self.persisted.local_disk_budget.as_ref() else {
            return Ok(false);
        };

        let accounted_before = local_disk_budget.snapshot().accounted_bytes;
        let removed_segments = self.sweep_fully_expired_persisted_segments()?;
        let accounted_after = local_disk_budget.snapshot().accounted_bytes;
        Ok(removed_segments > 0 || accounted_after < accounted_before)
    }

    fn persist_segment_with_outcome_policy(
        &self,
        policy: PersistSnapshotPolicy,
    ) -> Result<PersistSegmentOutcome> {
        self.observability
            .flush
            .persist_runs_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let persist_result = self.persist_segment_once(policy);

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

    pub(in super::super) fn persist_segment_with_outcome(&self) -> Result<PersistSegmentOutcome> {
        self.persist_segment_with_outcome_policy(PersistSnapshotPolicy::All)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in super::super) fn persist_segment_background_bounded_with_outcome(
        &self,
    ) -> Result<PersistSegmentOutcome> {
        self.persist_segment_background_bounded_with_limits(
            self.runtime.maintenance_max_items_per_pass,
            self.runtime.maintenance_max_bytes_per_pass,
        )
    }

    pub(in super::super) fn persist_segment_background_bounded_with_limits(
        &self,
        max_items: usize,
        max_bytes: u64,
    ) -> Result<PersistSegmentOutcome> {
        self.persist_segment_with_outcome_policy(PersistSnapshotPolicy::BackgroundBounded {
            max_items,
            max_bytes,
        })
    }

    pub(in super::super) fn persist_segment(&self) -> Result<bool> {
        Ok(self.persist_segment_with_outcome()?.persisted)
    }
}
