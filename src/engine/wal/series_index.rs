use super::replay::WalReplayContext;
use super::*;

#[derive(Debug, Clone)]
pub(super) enum CachedSeriesDefinitionFrame {
    SeriesDefinition(SeriesDefinitionFrame),
    Samples(BTreeSet<SeriesId>),
}

#[derive(Debug, Default, Clone)]
pub(super) struct CachedSeriesDefinitionIndex {
    pub(super) initialized: bool,
    pub(super) building: bool,
    pub(super) committed: BTreeMap<SeriesId, SeriesDefinitionFrame>,
    pub(super) pending: BTreeMap<SeriesId, SeriesDefinitionFrame>,
    pub(super) buffered_frames: Vec<CachedSeriesDefinitionFrame>,
}

impl CachedSeriesDefinitionIndex {
    pub(super) fn overlay_uninitialized_pending_from(&mut self, other: &Self) {
        for definition in other.pending.values() {
            self.pending
                .insert(definition.series_id, definition.clone());
        }
    }

    pub(super) fn apply_frame(&mut self, frame: CachedSeriesDefinitionFrame) {
        match frame {
            CachedSeriesDefinitionFrame::SeriesDefinition(definition) => {
                self.pending.insert(definition.series_id, definition);
            }
            CachedSeriesDefinitionFrame::Samples(series_ids) => {
                if self.pending.is_empty() {
                    return;
                }

                for (series_id, definition) in std::mem::take(&mut self.pending) {
                    if series_ids.contains(&series_id) {
                        self.committed.insert(series_id, definition);
                    }
                }
            }
        }
    }

    pub(super) fn snapshot(&self) -> Vec<SeriesDefinitionFrame> {
        self.committed.values().cloned().collect()
    }

    fn replace_committed<I>(&mut self, definitions: I)
    where
        I: IntoIterator<Item = SeriesDefinitionFrame>,
    {
        self.committed = definitions
            .into_iter()
            .map(|definition| (definition.series_id, definition))
            .collect();
        self.pending.clear();
        self.buffered_frames.clear();
        self.building = false;
        self.initialized = true;
    }

    fn extend_committed<I>(&mut self, definitions: I)
    where
        I: IntoIterator<Item = SeriesDefinitionFrame>,
    {
        for definition in definitions {
            self.committed.insert(definition.series_id, definition);
        }
    }

    fn clear_for_reset(&mut self) {
        self.building = false;
        self.committed.clear();
        self.pending.clear();
        self.buffered_frames.clear();
        self.initialized = true;
    }
}

impl FramedWal {
    pub(crate) fn committed_series_definitions_snapshot(
        &self,
    ) -> Result<Vec<SeriesDefinitionFrame>> {
        loop {
            let mut index = self.cached_series_definition_index.lock();
            if index.initialized {
                return Ok(index.snapshot());
            }
            if index.building {
                self.cached_series_definition_index_ready.wait(&mut index);
                continue;
            }

            index.building = true;
            drop(index);

            match self.rebuild_cached_series_definition_index() {
                Ok(mut rebuilt) => {
                    let mut index = self.cached_series_definition_index.lock();
                    if !index.initialized {
                        rebuilt.overlay_uninitialized_pending_from(&index);
                        for frame in index.buffered_frames.drain(..) {
                            rebuilt.apply_frame(frame);
                        }
                        rebuilt.initialized = true;
                        rebuilt.building = false;
                        *index = rebuilt;
                    }
                    let snapshot = index.snapshot();
                    self.cached_series_definition_index_ready.notify_all();
                    return Ok(snapshot);
                }
                Err(err) => {
                    let mut index = self.cached_series_definition_index.lock();
                    if !index.initialized {
                        index.building = false;
                        index.buffered_frames.clear();
                    }
                    self.cached_series_definition_index_ready.notify_all();
                    return Err(err);
                }
            }
        }
    }

    fn rebuild_cached_series_definition_index(&self) -> Result<CachedSeriesDefinitionIndex> {
        self.invoke_cached_series_definition_rebuild_hook();
        let mut index = CachedSeriesDefinitionIndex::default();
        let replay_mode = self.configured_replay_mode();
        let mut stream = self.replay_committed_write_stream_after_with_context(
            WalHighWatermark::default(),
            replay_mode,
            WalReplayContext::SeriesDefinitionCacheRebuild,
        )?;
        while let Some(write) = stream.next_write()? {
            index.extend_committed(write.series_definitions);
        }
        Ok(index)
    }

    pub(super) fn apply_cached_series_definition_frame_if_initialized(
        &self,
        frame: CachedSeriesDefinitionFrame,
    ) {
        self.apply_cached_series_definition_frames_if_initialized(std::iter::once(frame));
    }

    pub(super) fn apply_cached_series_definition_frames_if_initialized(
        &self,
        frames: impl IntoIterator<Item = CachedSeriesDefinitionFrame>,
    ) {
        let mut index = self.cached_series_definition_index.lock();
        for frame in frames {
            if index.initialized {
                index.apply_frame(frame);
            } else if index.building {
                index.buffered_frames.push(frame);
            } else {
                index.apply_frame(frame);
            }
        }
    }

    pub(super) fn clear_cached_series_definition_index_if_initialized(&self) {
        let mut index = self.cached_series_definition_index.lock();
        if index.initialized || index.building {
            index.clear_for_reset();
        }
    }

    pub(crate) fn prime_committed_series_definitions_snapshot<I>(&self, definitions: I)
    where
        I: IntoIterator<Item = SeriesDefinitionFrame>,
    {
        let mut index = self.cached_series_definition_index.lock();
        index.replace_committed(definitions);
        self.cached_series_definition_index_ready.notify_all();
    }

    pub(crate) fn record_committed_series_definitions_if_initialized<I>(&self, definitions: I)
    where
        I: IntoIterator<Item = SeriesDefinitionFrame>,
    {
        let mut index = self.cached_series_definition_index.lock();
        if !index.initialized {
            return;
        }
        index.extend_committed(definitions);
    }

    pub(in crate::engine) fn set_configured_replay_mode(&self, replay_mode: WalReplayMode) {
        *self.configured_replay_mode.lock() = replay_mode;
    }

    fn configured_replay_mode(&self) -> WalReplayMode {
        *self.configured_replay_mode.lock()
    }

    fn invoke_cached_series_definition_rebuild_hook(&self) {
        #[cfg(test)]
        if let Some(hook) = self.cached_series_definition_rebuild_hook.lock().clone() {
            hook();
        }
    }

    #[cfg(test)]
    pub(in crate::engine) fn set_cached_series_definition_rebuild_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self.cached_series_definition_rebuild_hook.lock() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(in crate::engine) fn clear_cached_series_definition_rebuild_hook(&self) {
        *self.cached_series_definition_rebuild_hook.lock() = None;
    }

    #[cfg(test)]
    pub(in crate::engine) fn invalidate_cached_series_definition_snapshot_for_test(&self) {
        *self.cached_series_definition_index.lock() = CachedSeriesDefinitionIndex::default();
        self.cached_series_definition_index_ready.notify_all();
    }
}
