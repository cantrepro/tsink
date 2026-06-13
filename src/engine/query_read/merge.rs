use super::pagination::{RawSeriesPagination, RawSeriesScanPage, SortedSeriesPageCollector};
use super::snapshot::{PersistedSeriesSourceSnapshot, SeriesReadSnapshot};
use super::*;

pub(super) trait QueryMergeCursor {
    fn peek_timestamp(&mut self) -> Result<Option<i64>>;
    fn pop_point(&mut self) -> Result<Option<DataPoint>>;
}

pub(super) struct QueryMergeSourceRef<'a> {
    cursor: &'a mut dyn QueryMergeCursor,
}

impl<'a> QueryMergeSourceRef<'a> {
    pub(super) fn new(cursor: &'a mut dyn QueryMergeCursor) -> Self {
        Self { cursor }
    }
}

pub(super) fn pop_next_point_from_sources(
    sources: &mut [QueryMergeSourceRef<'_>],
) -> Result<Option<DataPoint>> {
    let mut selected = None;
    let mut selected_ts = i64::MAX;

    for (idx, source) in sources.iter_mut().enumerate() {
        if let Some(ts) = source.cursor.peek_timestamp()? {
            if selected.is_none() || ts < selected_ts {
                selected = Some(idx);
                selected_ts = ts;
            }
        }
    }

    match selected {
        Some(idx) => sources[idx].cursor.pop_point(),
        None => Ok(None),
    }
}

pub(super) struct PersistedSourceMergeCursor {
    chunk_refs: Vec<PersistedChunkRef>,
    segment_maps: HashMap<usize, Arc<PlatformMmap>>,
    segment_tiers: HashMap<usize, PersistedSegmentTier>,
    start: i64,
    end: i64,
    next_chunk_idx: usize,
    current_points: Vec<DataPoint>,
    next_point_idx: usize,
    stats: PersistedTierFetchStats,
    #[cfg(test)]
    chunk_decode_hook: Option<Arc<IngestCommitHook>>,
}

impl PersistedSourceMergeCursor {
    pub(super) fn new(
        chunk_refs: Vec<PersistedChunkRef>,
        segment_maps: HashMap<usize, Arc<PlatformMmap>>,
        segment_tiers: HashMap<usize, PersistedSegmentTier>,
        start: i64,
        end: i64,
        #[cfg(test)] chunk_decode_hook: Option<Arc<IngestCommitHook>>,
    ) -> Self {
        Self {
            chunk_refs,
            segment_maps,
            segment_tiers,
            start,
            end,
            next_chunk_idx: 0,
            current_points: Vec::new(),
            next_point_idx: 0,
            stats: PersistedTierFetchStats::default(),
            #[cfg(test)]
            chunk_decode_hook,
        }
    }

    fn ensure_head(&mut self) -> Result<Option<&DataPoint>> {
        loop {
            if self.next_point_idx < self.current_points.len() {
                return Ok(self.current_points.get(self.next_point_idx));
            }
            if self.next_chunk_idx >= self.chunk_refs.len() {
                return Ok(None);
            }

            let chunk_ref = self.chunk_refs[self.next_chunk_idx];
            self.next_chunk_idx = self.next_chunk_idx.saturating_add(1);
            self.current_points.clear();
            self.next_point_idx = 0;

            let tier = self
                .segment_tiers
                .get(&chunk_ref.segment_slot)
                .copied()
                .unwrap_or(PersistedSegmentTier::Hot);
            #[cfg(test)]
            if let Some(hook) = self.chunk_decode_hook.as_ref() {
                hook();
            }
            let decode_started = Instant::now();
            let payload = persisted_chunk_payload(&self.segment_maps, &chunk_ref)?;
            decode_encoded_chunk_payload_in_range_into(
                EncodedChunkDescriptor {
                    lane: chunk_ref.lane,
                    ts_codec: chunk_ref.ts_codec,
                    value_codec: chunk_ref.value_codec,
                    point_count: chunk_ref.point_count as usize,
                },
                payload.as_ref(),
                self.start,
                self.end,
                &mut self.current_points,
            )?;
            self.stats
                .record_chunk(tier, elapsed_nanos_u64(decode_started));
        }
    }

    pub(super) fn into_stats(self) -> PersistedTierFetchStats {
        self.stats
    }
}

impl QueryMergeCursor for PersistedSourceMergeCursor {
    fn peek_timestamp(&mut self) -> Result<Option<i64>> {
        Ok(self.ensure_head()?.map(|point| point.timestamp))
    }

    fn pop_point(&mut self) -> Result<Option<DataPoint>> {
        if self.ensure_head()?.is_none() {
            return Ok(None);
        }

        let point = self.current_points[self.next_point_idx].clone();
        self.next_point_idx = self.next_point_idx.saturating_add(1);
        Ok(Some(point))
    }
}

pub(super) struct SealedSourceMergeCursor {
    chunks: Vec<Arc<Chunk>>,
    start: i64,
    end: i64,
    next_chunk_idx: usize,
    current_points: Vec<DataPoint>,
    next_point_idx: usize,
}

impl SealedSourceMergeCursor {
    pub(super) fn new(chunks: Vec<Arc<Chunk>>, start: i64, end: i64) -> Self {
        Self {
            chunks,
            start,
            end,
            next_chunk_idx: 0,
            current_points: Vec::new(),
            next_point_idx: 0,
        }
    }

    fn ensure_head(&mut self) -> Result<Option<&DataPoint>> {
        loop {
            if self.next_point_idx < self.current_points.len() {
                return Ok(self.current_points.get(self.next_point_idx));
            }
            if self.next_chunk_idx >= self.chunks.len() {
                return Ok(None);
            }

            let chunk = &self.chunks[self.next_chunk_idx];
            self.next_chunk_idx = self.next_chunk_idx.saturating_add(1);
            self.current_points.clear();
            self.next_point_idx = 0;
            decode_chunk_points_in_range_into(
                chunk,
                self.start,
                self.end,
                &mut self.current_points,
            )?;
        }
    }
}

impl QueryMergeCursor for SealedSourceMergeCursor {
    fn peek_timestamp(&mut self) -> Result<Option<i64>> {
        Ok(self.ensure_head()?.map(|point| point.timestamp))
    }

    fn pop_point(&mut self) -> Result<Option<DataPoint>> {
        if self.ensure_head()?.is_none() {
            return Ok(None);
        }

        let point = self.current_points[self.next_point_idx].clone();
        self.next_point_idx = self.next_point_idx.saturating_add(1);
        Ok(Some(point))
    }
}

pub(super) struct ActiveSourceMergeCursor {
    points: ActiveSeriesSnapshotCursor,
    start: i64,
    end: i64,
}

impl ActiveSourceMergeCursor {
    pub(super) fn new(points: ActiveSeriesSnapshot, start: i64, end: i64) -> Self {
        Self {
            points: points.into_cursor(),
            start,
            end,
        }
    }

    fn seek_in_range(&mut self) {
        while self
            .points
            .peek()
            .is_some_and(|point| point.ts < self.start)
        {
            self.points.advance();
        }
    }
}

impl QueryMergeCursor for ActiveSourceMergeCursor {
    fn peek_timestamp(&mut self) -> Result<Option<i64>> {
        self.seek_in_range();
        let Some(point) = self.points.peek() else {
            return Ok(None);
        };
        Ok((point.ts < self.end).then_some(point.ts))
    }

    fn pop_point(&mut self) -> Result<Option<DataPoint>> {
        self.seek_in_range();
        let Some(point) = self.points.peek() else {
            return Ok(None);
        };
        if point.ts >= self.end {
            return Ok(None);
        }
        let data_point = DataPoint::new(point.ts, point.value.clone());
        self.points.advance();
        Ok(Some(data_point))
    }
}

struct SeriesSourceMergeCursors {
    persisted: PersistedSourceMergeCursor,
    sealed: SealedSourceMergeCursor,
    active: ActiveSourceMergeCursor,
}

impl SeriesSourceMergeCursors {
    fn new(
        persisted: PersistedSeriesSourceSnapshot,
        sealed_chunks: Vec<Arc<Chunk>>,
        active_points: ActiveSeriesSnapshot,
        start: i64,
        end: i64,
        #[cfg(test)] chunk_decode_hook: Option<Arc<IngestCommitHook>>,
    ) -> Self {
        Self {
            persisted: PersistedSourceMergeCursor::new(
                persisted.chunks,
                persisted.segment_maps,
                persisted.segment_tiers,
                start,
                end,
                #[cfg(test)]
                chunk_decode_hook,
            ),
            sealed: SealedSourceMergeCursor::new(sealed_chunks, start, end),
            active: ActiveSourceMergeCursor::new(active_points, start, end),
        }
    }

    fn pop_next_point(&mut self) -> Result<Option<DataPoint>> {
        let mut sources = [
            QueryMergeSourceRef::new(&mut self.persisted),
            QueryMergeSourceRef::new(&mut self.sealed),
            QueryMergeSourceRef::new(&mut self.active),
        ];
        pop_next_point_from_sources(&mut sources)
    }

    fn merge_all_into(&mut self, out: &mut Vec<DataPoint>) -> Result<()> {
        while let Some(point) = self.pop_next_point()? {
            out.push(point);
        }
        Ok(())
    }

    fn collect_page_with(&mut self, collector: &mut SortedSeriesPageCollector<'_>) -> Result<bool> {
        while let Some(point) = self.pop_next_point()? {
            if collector.push(point) {
                return Ok(false);
            }
        }
        collector.finish();
        Ok(true)
    }

    fn into_stats(self) -> PersistedTierFetchStats {
        self.persisted.into_stats()
    }
}

impl ChunkStorage {
    pub(super) fn execute_series_read_merge_path(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        snapshot: SeriesReadSnapshot,
        out: &mut Vec<DataPoint>,
    ) -> Result<PersistedTierFetchStats> {
        let SeriesReadSnapshot {
            persisted,
            sealed_chunks,
            active_points,
            analysis,
        } = snapshot;

        out.clear();
        out.reserve(analysis.estimated_points);

        let mut cursors = SeriesSourceMergeCursors::new(
            persisted,
            sealed_chunks,
            active_points,
            start,
            end,
            #[cfg(test)]
            self.persist_test_hooks
                .query_persisted_chunk_decode_hook
                .read()
                .clone(),
        );

        cursors.merge_all_into(out)?;
        let persisted_stats = cursors.into_stats();
        self.apply_retention_filter(out);
        match analysis.sorted_dedupe_mode() {
            super::pagination::SortedSeriesDedupeMode::Timestamp => {
                dedupe_last_value_per_timestamp(out);
            }
            super::pagination::SortedSeriesDedupeMode::Exact => {
                dedupe_exact_duplicate_points(out);
            }
            super::pagination::SortedSeriesDedupeMode::None => {}
        }
        self.apply_tombstone_filter(series_id, out);
        Ok(persisted_stats)
    }

    pub(super) fn collect_raw_series_page_with_merge(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        snapshot: SeriesReadSnapshot,
        pagination: RawSeriesPagination,
    ) -> Result<RawSeriesScanPage> {
        let SeriesReadSnapshot {
            persisted,
            sealed_chunks,
            active_points,
            analysis,
        } = snapshot;

        let mut cursors = SeriesSourceMergeCursors::new(
            persisted,
            sealed_chunks,
            active_points,
            start,
            end,
            #[cfg(test)]
            self.persist_test_hooks
                .query_persisted_chunk_decode_hook
                .read()
                .clone(),
        );

        self.tombstone_read_context()
            .with_series_tombstone_ranges(series_id, |tombstone_ranges| {
                let mut collector = SortedSeriesPageCollector::new(
                    self.active_retention_cutoff(),
                    tombstone_ranges,
                    analysis.sorted_dedupe_mode(),
                    pagination,
                );
                let reached_end = cursors.collect_page_with(&mut collector)?;
                let final_rows_seen = collector.final_rows_seen();
                let points = collector.into_points();

                Ok(RawSeriesScanPage {
                    points,
                    final_rows_seen,
                    reached_end,
                    stats: cursors.into_stats(),
                })
            })
    }
}
