use super::pagination::{RawSeriesPagination, RawSeriesScanPage};
use super::snapshot::SeriesReadSnapshot;
use super::*;

impl ChunkStorage {
    pub(super) fn execute_series_read_append_sort_path(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        snapshot: SeriesReadSnapshot,
        out: &mut Vec<DataPoint>,
        execution: Option<&QueryExecution>,
    ) -> Result<PersistedTierFetchStats> {
        let (stats, _query_reservation) = self
            .execute_series_read_append_sort_path_with_reservation(
                series_id, start, end, snapshot, out, execution,
            )?;
        Ok(stats)
    }

    fn execute_series_read_append_sort_path_with_reservation(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        snapshot: SeriesReadSnapshot,
        out: &mut Vec<DataPoint>,
        execution: Option<&QueryExecution>,
    ) -> Result<(
        PersistedTierFetchStats,
        Option<crate::QueryMemoryReservation>,
    )> {
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
        let persisted_stats = decode_append_sort_sources_into(
            &persisted,
            &sealed_chunks,
            &active_points,
            start,
            end,
            &mut decoded,
            execution,
        )?;
        self.finalize_append_sort_points(series_id, analysis, &mut decoded, execution)?;
        if let Some(reservation) = working_reservation.as_mut() {
            reservation.resize(modeled_points_retained_bytes(&decoded))?;
        }
        publish_vec_reusing_capacity(out, decoded);
        Ok((persisted_stats, working_reservation))
    }

    pub(super) fn collect_raw_series_page_with_append_sort(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        snapshot: SeriesReadSnapshot,
        pagination: RawSeriesPagination,
        execution: Option<&QueryExecution>,
    ) -> Result<RawSeriesScanPage> {
        let mut points = Vec::new();
        let (stats, mut query_reservation) = self
            .execute_series_read_append_sort_path_with_reservation(
                series_id,
                start,
                end,
                snapshot,
                &mut points,
                execution,
            )?;
        let total_rows = points.len();
        let rows_consumed = pagination.rows_consumed(total_rows);
        apply_offset_limit_in_place(&mut points, pagination.offset, pagination.limit);
        if let Some(reservation) = query_reservation.as_mut() {
            if let Err(error) = reservation.resize(modeled_points_retained_bytes(&points)) {
                // `query_reservation` was bound after `points`; destroy the protected allocation
                // explicitly before returning the resize failure.
                drop(points);
                drop(query_reservation);
                return Err(error.into());
            }
        }

        Ok(RawSeriesScanPage {
            points,
            final_rows_seen: saturating_u64_from_usize(rows_consumed),
            reached_end: rows_consumed >= total_rows,
            stats,
            query_reservation,
        })
    }

    fn finalize_append_sort_points(
        &self,
        series_id: SeriesId,
        analysis: super::analysis::SeriesReadAnalysis,
        out: &mut Vec<DataPoint>,
        execution: Option<&QueryExecution>,
    ) -> Result<()> {
        self.apply_retention_filter(out);

        if analysis.needs_append_sort_reorder() {
            if !points_are_sorted_by_timestamp(out) {
                out.sort_by_key(|point| point.timestamp);
            }

            match analysis.sorted_dedupe_mode() {
                super::pagination::SortedSeriesDedupeMode::Timestamp => {
                    dedupe_last_value_per_timestamp(out);
                }
                super::pagination::SortedSeriesDedupeMode::Exact => {
                    dedupe_exact_duplicate_points(out);
                }
                super::pagination::SortedSeriesDedupeMode::None => {}
            }
        }

        self.apply_tombstone_filter_for_query(series_id, out, execution)
    }
}

fn decode_append_sort_sources_into(
    persisted: &super::snapshot::PersistedSeriesSourceSnapshot,
    sealed_chunks: &[Arc<Chunk>],
    active_points: &ActiveSeriesSnapshot,
    start: i64,
    end: i64,
    out: &mut Vec<DataPoint>,
    execution: Option<&QueryExecution>,
) -> Result<PersistedTierFetchStats> {
    let mut persisted_stats = PersistedTierFetchStats::default();

    for chunk_ref in &persisted.chunks {
        if let Some(execution) = execution {
            execution.checkpoint()?;
            execution.charge_samples_scanned(u64::from(chunk_ref.point_count))?;
        }
        let decode_started = Instant::now();
        let payload = persisted_chunk_payload(&persisted.segment_maps, chunk_ref)?;
        decode_encoded_chunk_payload_in_range_into(
            EncodedChunkDescriptor {
                lane: chunk_ref.lane,
                ts_codec: chunk_ref.ts_codec,
                value_codec: chunk_ref.value_codec,
                point_count: chunk_ref.point_count as usize,
            },
            payload.as_ref(),
            start,
            end,
            out,
        )?;
        persisted_stats.record_chunk(
            persisted.chunk_tier(chunk_ref),
            elapsed_nanos_u64(decode_started),
        );
    }

    for chunk in sealed_chunks {
        if let Some(execution) = execution {
            execution.checkpoint()?;
            execution.charge_samples_scanned(u64::from(chunk.header.point_count))?;
        }
        decode_chunk_points_in_range_into(chunk, start, end, out)?;
    }

    for point in active_points.iter_points_in_partition_order() {
        if point.ts >= start && point.ts < end {
            out.push(DataPoint::new(point.ts, point.value.clone()));
        }
    }

    Ok(persisted_stats)
}
