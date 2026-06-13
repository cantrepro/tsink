use super::pagination::SortedSeriesDedupeMode;
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SeriesReadAnalysis {
    pub(super) estimated_points: usize,
    pub(super) has_overlap: bool,
    pub(super) requires_output_validation: bool,
    pub(super) requires_timestamp_dedupe: bool,
    pub(super) requires_exact_dedupe: bool,
    pub(super) persisted_source_sorted: bool,
    pub(super) sealed_source_sorted: bool,
}

impl SeriesReadAnalysis {
    pub(super) fn can_use_merge_path(self) -> bool {
        !self.requires_output_validation
            && self.persisted_source_sorted
            && self.sealed_source_sorted
    }

    pub(super) fn needs_append_sort_reorder(self) -> bool {
        self.has_overlap || self.requires_output_validation
    }

    pub(super) fn sorted_dedupe_mode(self) -> SortedSeriesDedupeMode {
        if self.requires_timestamp_dedupe {
            SortedSeriesDedupeMode::Timestamp
        } else if self.requires_exact_dedupe {
            SortedSeriesDedupeMode::Exact
        } else {
            SortedSeriesDedupeMode::None
        }
    }
}

pub(super) fn analyze_series_read_sources(
    persisted_chunks: &[PersistedChunkRef],
    sealed_chunks: &[Arc<Chunk>],
    active_points: &ActiveSeriesSnapshot,
    start: i64,
    end: i64,
) -> SeriesReadAnalysis {
    let mut has_overlap = false;
    let mut has_previous_chunk = false;
    let mut has_previous_persisted_chunk = false;
    let mut previous_max_ts = i64::MIN;
    let mut previous_persisted_max_ts = i64::MIN;
    let mut requires_output_validation = false;
    let mut requires_timestamp_dedupe = false;
    let mut requires_exact_dedupe = false;
    let mut persisted_source_sorted = true;
    let mut sealed_source_sorted = true;
    let mut estimated_points = 0usize;
    let mut previous_persisted_source_max_ts = i64::MIN;
    let mut has_previous_persisted_source_chunk = false;
    let mut overlapping_persisted_cluster_level = 0u8;
    for chunk_ref in persisted_chunks {
        let overlaps_previous_persisted_source = has_previous_persisted_source_chunk
            && chunk_ref.min_ts <= previous_persisted_source_max_ts;

        if has_previous_chunk && chunk_ref.min_ts <= previous_max_ts {
            has_overlap = true;
            if has_previous_persisted_chunk && chunk_ref.min_ts <= previous_persisted_max_ts {
                requires_exact_dedupe = true;
            }
        }
        if has_previous_persisted_source_chunk
            && chunk_ref.min_ts < previous_persisted_source_max_ts
        {
            persisted_source_sorted = false;
        }
        if overlaps_previous_persisted_source {
            if chunk_ref.level != overlapping_persisted_cluster_level {
                requires_timestamp_dedupe = true;
            }
            overlapping_persisted_cluster_level =
                overlapping_persisted_cluster_level.max(chunk_ref.level);
        } else {
            overlapping_persisted_cluster_level = chunk_ref.level;
        }

        has_previous_chunk = true;
        has_previous_persisted_chunk = true;
        previous_max_ts = previous_max_ts.max(chunk_ref.max_ts);
        previous_persisted_max_ts = previous_persisted_max_ts.max(chunk_ref.max_ts);
        previous_persisted_source_max_ts = previous_persisted_source_max_ts.max(chunk_ref.max_ts);
        has_previous_persisted_source_chunk = true;
        estimated_points = estimated_points.saturating_add(chunk_ref.point_count as usize);
    }

    let mut previous_sealed_source_max_ts = i64::MIN;
    let mut has_previous_sealed_source_chunk = false;
    for chunk in sealed_chunks {
        if has_previous_chunk && chunk.header.min_ts <= previous_max_ts {
            has_overlap = true;
            if has_previous_persisted_chunk && chunk.header.min_ts <= previous_persisted_max_ts {
                requires_exact_dedupe = true;
            }
        }
        if has_previous_sealed_source_chunk && chunk.header.min_ts < previous_sealed_source_max_ts {
            sealed_source_sorted = false;
        }

        has_previous_chunk = true;
        previous_max_ts = previous_max_ts.max(chunk.header.max_ts);
        previous_sealed_source_max_ts = previous_sealed_source_max_ts.max(chunk.header.max_ts);
        has_previous_sealed_source_chunk = true;

        // Chunks without encoded payload may be ad-hoc/manual and not guaranteed sorted.
        if chunk.points.len() > 1 && chunk.encoded_payload.is_empty() {
            requires_output_validation = true;
        }
        estimated_points = estimated_points.saturating_add(chunk.header.point_count as usize);
    }

    let mut previous_active_ts = i64::MIN;
    let mut has_previous_active = false;
    for point in active_points.iter_points_in_partition_order() {
        if point.ts < start || point.ts >= end {
            continue;
        }

        if has_previous_chunk && point.ts <= previous_max_ts {
            has_overlap = true;
        }
        if has_previous_persisted_chunk && point.ts <= previous_persisted_max_ts {
            requires_exact_dedupe = true;
        }
        if has_previous_active && point.ts < previous_active_ts {
            requires_output_validation = true;
        }

        has_previous_active = true;
        previous_active_ts = point.ts;
        estimated_points = estimated_points.saturating_add(1);
    }

    SeriesReadAnalysis {
        estimated_points,
        has_overlap,
        requires_output_validation,
        requires_timestamp_dedupe,
        requires_exact_dedupe,
        persisted_source_sorted,
        sealed_source_sorted,
    }
}
