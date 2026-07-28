use super::pagination::{RawSeriesPagination, RawSeriesScanPage, SortedSeriesPageCollector};
use super::snapshot::{
    PersistedSeriesSourceSnapshot, PersistedSeriesSourceSnapshotParts, SeriesReadSnapshot,
};
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
    execution: Option<QueryExecution>,
    #[cfg(test)]
    chunk_decode_hook: Option<Arc<IngestCommitHook>>,
    _query_reservation: Option<crate::QueryMemoryReservation>,
}

impl PersistedSourceMergeCursor {
    pub(super) fn new(
        persisted: PersistedSeriesSourceSnapshot,
        start: i64,
        end: i64,
        execution: Option<QueryExecution>,
        #[cfg(test)] chunk_decode_hook: Option<Arc<IngestCommitHook>>,
    ) -> Self {
        let decode_capacity = modeled_vec_growth_capacity_upper(
            persisted
                .chunks
                .iter()
                .map(|chunk| usize::from(chunk.point_count))
                .max()
                .unwrap_or(0),
        );
        let PersistedSeriesSourceSnapshotParts {
            chunks,
            segment_maps,
            segment_tiers,
            query_reservation,
        } = persisted.into_parts();
        Self {
            chunk_refs: chunks,
            segment_maps,
            segment_tiers,
            start,
            end,
            next_chunk_idx: 0,
            current_points: Vec::with_capacity(decode_capacity),
            next_point_idx: 0,
            stats: PersistedTierFetchStats::default(),
            execution,
            #[cfg(test)]
            chunk_decode_hook,
            _query_reservation: query_reservation,
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

            if let Some(execution) = self.execution.as_ref() {
                execution.checkpoint()?;
                execution.charge_samples_scanned(u64::from(chunk_ref.point_count))?;
            }

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
    execution: Option<QueryExecution>,
}

impl SealedSourceMergeCursor {
    pub(super) fn new(
        chunks: Vec<Arc<Chunk>>,
        decode_capacity: usize,
        start: i64,
        end: i64,
        execution: Option<QueryExecution>,
    ) -> Self {
        Self {
            chunks,
            start,
            end,
            next_chunk_idx: 0,
            current_points: Vec::with_capacity(decode_capacity),
            next_point_idx: 0,
            execution,
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
            if let Some(execution) = self.execution.as_ref() {
                execution.checkpoint()?;
                execution.charge_samples_scanned(u64::from(chunk.header.point_count))?;
            }
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
        execution: Option<&QueryExecution>,
        #[cfg(test)] chunk_decode_hook: Option<Arc<IngestCommitHook>>,
    ) -> Self {
        let sealed_decode_capacity = modeled_vec_growth_capacity_upper(
            sealed_chunks
                .iter()
                .map(|chunk| usize::from(chunk.header.point_count))
                .max()
                .unwrap_or(0),
        );
        Self {
            persisted: PersistedSourceMergeCursor::new(
                persisted,
                start,
                end,
                execution.cloned(),
                #[cfg(test)]
                chunk_decode_hook,
            ),
            sealed: SealedSourceMergeCursor::new(
                sealed_chunks,
                sealed_decode_capacity,
                start,
                end,
                execution.cloned(),
            ),
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
        Ok(!collector.finish())
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
        execution: Option<&QueryExecution>,
    ) -> Result<PersistedTierFetchStats> {
        let mut working_reservation = reserve_query_read_working_set(
            execution,
            &snapshot,
            snapshot.analysis.estimated_points,
            0,
        )?;
        let SeriesReadSnapshot {
            persisted,
            sealed_chunks,
            active_points,
            analysis,
            query_reservation: _source_snapshot_reservation,
        } = snapshot;

        let mut decoded = Vec::with_capacity(analysis.estimated_points);

        let mut cursors = SeriesSourceMergeCursors::new(
            persisted,
            sealed_chunks,
            active_points,
            start,
            end,
            execution,
            #[cfg(test)]
            self.persist_test_hooks
                .query_persisted_chunk_decode_hook
                .read()
                .clone(),
        );

        cursors.merge_all_into(&mut decoded)?;
        let persisted_stats = cursors.into_stats();
        self.apply_retention_filter(&mut decoded);
        match analysis.sorted_dedupe_mode() {
            super::pagination::SortedSeriesDedupeMode::Timestamp => {
                dedupe_last_value_per_timestamp(&mut decoded);
            }
            super::pagination::SortedSeriesDedupeMode::Exact => {
                dedupe_exact_duplicate_points(&mut decoded);
            }
            super::pagination::SortedSeriesDedupeMode::None => {}
        }
        self.apply_tombstone_filter_for_query(series_id, &mut decoded, execution)?;
        if let Some(reservation) = working_reservation.as_mut() {
            reservation.resize(modeled_points_retained_bytes(&decoded))?;
        }
        publish_vec_reusing_capacity(out, decoded);
        Ok(persisted_stats)
    }

    pub(super) fn collect_raw_series_page_with_merge(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        snapshot: SeriesReadSnapshot,
        pagination: RawSeriesPagination,
        execution: Option<&QueryExecution>,
    ) -> Result<RawSeriesScanPage> {
        let output_capacity = pagination
            .limit
            .unwrap_or(snapshot.analysis.estimated_points)
            .min(snapshot.analysis.estimated_points);
        let mut working_reservation =
            reserve_query_read_working_set(execution, &snapshot, output_capacity, 0)?;
        let SeriesReadSnapshot {
            persisted,
            sealed_chunks,
            active_points,
            analysis,
            query_reservation: _source_snapshot_reservation,
        } = snapshot;

        let mut cursors = SeriesSourceMergeCursors::new(
            persisted,
            sealed_chunks,
            active_points,
            start,
            end,
            execution,
            #[cfg(test)]
            self.persist_test_hooks
                .query_persisted_chunk_decode_hook
                .read()
                .clone(),
        );

        let mut page = self
            .tombstone_read_context()
            .with_series_tombstone_ranges_for_query(
                series_id,
                execution,
                |tombstone_ranges| -> Result<RawSeriesScanPage> {
                    let mut collector = SortedSeriesPageCollector::new(
                        self.active_retention_cutoff(),
                        tombstone_ranges,
                        analysis.sorted_dedupe_mode(),
                        pagination,
                        output_capacity,
                    );
                    let reached_end = cursors.collect_page_with(&mut collector)?;
                    let final_rows_seen = collector.final_rows_seen();
                    let points = collector.into_points();

                    Ok(RawSeriesScanPage {
                        points,
                        final_rows_seen,
                        reached_end,
                        stats: cursors.into_stats(),
                        query_reservation: None,
                    })
                },
            )?;
        if let Some(reservation) = working_reservation.as_mut() {
            reservation.resize(modeled_points_retained_bytes(&page.points))?;
        }
        page.query_reservation = working_reservation;
        Ok(page)
    }
}
