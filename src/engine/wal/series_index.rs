use super::replay::WalReplayContext;
use super::*;

// `BTreeMap` does not expose allocation capacity. Model one root allocation plus portable
// per-entry bookkeeping so the retained cache remains a conservative, inspectable budget
// component without depending on the standard library's private node layout.
const WAL_CACHE_BTREE_ROOT_ALLOWANCE_BYTES: usize = 2 * 1024;
const WAL_CACHE_BTREE_BOOKKEEPING_WORDS_PER_ENTRY: usize = 4;

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
    fn definition_heap_bytes(definition: &SeriesDefinitionFrame) -> usize {
        definition
            .metric
            .capacity()
            .saturating_add(
                definition
                    .labels
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Label>()),
            )
            .saturating_add(definition.labels.iter().fold(0usize, |bytes, label| {
                bytes
                    .saturating_add(label.name.capacity())
                    .saturating_add(label.value.capacity())
            }))
    }

    fn definition_map_memory_usage_bytes(
        definitions: &BTreeMap<SeriesId, SeriesDefinitionFrame>,
    ) -> usize {
        if definitions.is_empty() {
            return 0;
        }

        let entry_bytes = std::mem::size_of::<(SeriesId, SeriesDefinitionFrame)>().saturating_add(
            WAL_CACHE_BTREE_BOOKKEEPING_WORDS_PER_ENTRY
                .saturating_mul(std::mem::size_of::<usize>()),
        );
        WAL_CACHE_BTREE_ROOT_ALLOWANCE_BYTES.saturating_add(definitions.values().fold(
            0usize,
            |bytes, definition| {
                bytes
                    .saturating_add(entry_bytes)
                    .saturating_add(Self::definition_heap_bytes(definition))
            },
        ))
    }

    fn series_id_set_memory_usage_bytes(series_ids: &BTreeSet<SeriesId>) -> usize {
        if series_ids.is_empty() {
            return 0;
        }

        let entry_bytes = std::mem::size_of::<SeriesId>().saturating_add(
            WAL_CACHE_BTREE_BOOKKEEPING_WORDS_PER_ENTRY
                .saturating_mul(std::mem::size_of::<usize>()),
        );
        WAL_CACHE_BTREE_ROOT_ALLOWANCE_BYTES
            .saturating_add(series_ids.len().saturating_mul(entry_bytes))
    }

    fn memory_usage_bytes(&self) -> usize {
        let buffered_bytes = self
            .buffered_frames
            .capacity()
            .saturating_mul(std::mem::size_of::<CachedSeriesDefinitionFrame>())
            .saturating_add(self.buffered_frames.iter().fold(0usize, |bytes, frame| {
                bytes.saturating_add(match frame {
                    CachedSeriesDefinitionFrame::SeriesDefinition(definition) => {
                        Self::definition_heap_bytes(definition)
                    }
                    CachedSeriesDefinitionFrame::Samples(series_ids) => {
                        Self::series_id_set_memory_usage_bytes(series_ids)
                    }
                })
            }));

        Self::definition_map_memory_usage_bytes(&self.committed)
            .saturating_add(Self::definition_map_memory_usage_bytes(&self.pending))
            .saturating_add(buffered_bytes)
    }

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
    pub(crate) fn cached_series_definition_index_memory_usage_bytes(&self) -> usize {
        self.cached_series_definition_index
            .lock()
            .memory_usage_bytes()
    }

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
        // An uninitialized index can still retain pending definitions appended before its first
        // snapshot. Reset invalidates those definitions too and must release their memory.
        index.clear_for_reset();
    }

    pub(crate) fn prime_committed_series_definitions_snapshot<I>(&self, definitions: I)
    where
        I: IntoIterator<Item = SeriesDefinitionFrame>,
    {
        let mut index = self.cached_series_definition_index.lock();
        index.replace_committed(definitions);
        self.cached_series_definition_index_ready.notify_all();
    }

    pub(in crate::engine) fn record_replayed_series_definition_if_initialized(
        &self,
        definition: SeriesDefinitionFrame,
    ) {
        self.apply_cached_series_definition_frame_if_initialized(
            CachedSeriesDefinitionFrame::SeriesDefinition(definition),
        );
    }

    pub(in crate::engine) fn record_replayed_samples_if_initialized<I>(&self, series_ids: I)
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let mut index = self.cached_series_definition_index.lock();
        if !index.initialized {
            return;
        }
        for series_id in series_ids {
            if let Some(definition) = index.pending.remove(&series_id) {
                index.committed.insert(series_id, definition);
            }
        }
        // A samples frame closes the preceding logical write. Definitions not referenced by
        // that frame were never committed and must not leak into a later write.
        index.pending.clear();
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
