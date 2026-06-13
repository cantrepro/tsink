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
    ) -> Result<PersistedTierFetchStats> {
        let SeriesReadSnapshot {
            persisted,
            sealed_chunks,
            active_points,
            analysis,
        } = snapshot;

        out.clear();
        let persisted_stats = decode_append_sort_sources_into(
            &persisted,
            &sealed_chunks,
            &active_points,
            start,
            end,
            out,
        )?;
        self.finalize_append_sort_points(series_id, analysis, out);
        Ok(persisted_stats)
    }

    pub(super) fn collect_raw_series_page_with_append_sort(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        snapshot: SeriesReadSnapshot,
        pagination: RawSeriesPagination,
    ) -> Result<RawSeriesScanPage> {
        let mut points = Vec::new();
        let stats = self.execute_series_read_append_sort_path(
            series_id,
            start,
            end,
            snapshot,
            &mut points,
        )?;
        let total_rows = points.len();
        let rows_consumed = pagination.rows_consumed(total_rows);
        apply_offset_limit_in_place(&mut points, pagination.offset, pagination.limit);

        Ok(RawSeriesScanPage {
            points,
            final_rows_seen: saturating_u64_from_usize(rows_consumed),
            reached_end: rows_consumed >= total_rows,
            stats,
        })
    }

    fn finalize_append_sort_points(
        &self,
        series_id: SeriesId,
        analysis: super::analysis::SeriesReadAnalysis,
        out: &mut Vec<DataPoint>,
    ) {
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

        self.apply_tombstone_filter(series_id, out);
    }
}

fn decode_append_sort_sources_into(
    persisted: &super::snapshot::PersistedSeriesSourceSnapshot,
    sealed_chunks: &[Arc<Chunk>],
    active_points: &ActiveSeriesSnapshot,
    start: i64,
    end: i64,
    out: &mut Vec<DataPoint>,
) -> Result<PersistedTierFetchStats> {
    let mut persisted_stats = PersistedTierFetchStats::default();

    for chunk_ref in &persisted.chunks {
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
        decode_chunk_points_in_range_into(chunk, start, end, out)?;
    }

    for point in active_points.iter_points_in_partition_order() {
        if point.ts >= start && point.ts < end {
            out.push(DataPoint::new(point.ts, point.value.clone()));
        }
    }

    Ok(persisted_stats)
}
