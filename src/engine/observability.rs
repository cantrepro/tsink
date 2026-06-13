use super::metrics::{
    CompactionObservabilityCounters, FlushObservabilityCounters, QueryObservabilityCounters,
    WalObservabilityCounters,
};
use super::*;
use crate::{
    CompactionObservabilitySnapshot, FlushObservabilitySnapshot, QueryObservabilitySnapshot,
    WalObservabilitySnapshot,
};

#[derive(Debug, Clone, Copy)]
struct WalRuntimeSnapshot {
    enabled: bool,
    size_bytes: u64,
    segment_count: u64,
    active_segment: u64,
    highwater_segment: u64,
    highwater_frame: u64,
}

impl WalRuntimeSnapshot {
    fn from_wal(wal: Option<&FramedWal>) -> Self {
        match wal {
            Some(wal) => {
                let highwater = wal.current_highwater();
                Self {
                    enabled: true,
                    size_bytes: wal.total_size_bytes().unwrap_or(0),
                    segment_count: wal.segment_count().unwrap_or(0),
                    active_segment: wal.active_segment(),
                    highwater_segment: highwater.segment,
                    highwater_frame: highwater.frame,
                }
            }
            None => Self {
                enabled: false,
                size_bytes: 0,
                segment_count: 0,
                active_segment: 0,
                highwater_segment: 0,
                highwater_frame: 0,
            },
        }
    }
}

#[derive(Clone, Copy)]
struct WalSnapshotView<'a> {
    counters: &'a WalObservabilityCounters,
    runtime: WalRuntimeSnapshot,
}

impl From<WalSnapshotView<'_>> for WalObservabilitySnapshot {
    fn from(value: WalSnapshotView<'_>) -> Self {
        Self {
            enabled: value.runtime.enabled,
            size_bytes: value.runtime.size_bytes,
            segment_count: value.runtime.segment_count,
            active_segment: value.runtime.active_segment,
            highwater_segment: value.runtime.highwater_segment,
            highwater_frame: value.runtime.highwater_frame,
            replay_runs_total: value.counters.replay_runs_total.load(Ordering::Relaxed),
            replay_frames_total: value.counters.replay_frames_total.load(Ordering::Relaxed),
            replay_series_definitions_total: value
                .counters
                .replay_series_definitions_total
                .load(Ordering::Relaxed),
            replay_sample_batches_total: value
                .counters
                .replay_sample_batches_total
                .load(Ordering::Relaxed),
            replay_points_total: value.counters.replay_points_total.load(Ordering::Relaxed),
            replay_errors_total: value.counters.replay_errors_total.load(Ordering::Relaxed),
            replay_duration_nanos_total: value
                .counters
                .replay_duration_nanos_total
                .load(Ordering::Relaxed),
            append_series_definitions_total: value
                .counters
                .append_series_definitions_total
                .load(Ordering::Relaxed),
            append_sample_batches_total: value
                .counters
                .append_sample_batches_total
                .load(Ordering::Relaxed),
            append_points_total: value.counters.append_points_total.load(Ordering::Relaxed),
            append_bytes_total: value.counters.append_bytes_total.load(Ordering::Relaxed),
            append_errors_total: value.counters.append_errors_total.load(Ordering::Relaxed),
            resets_total: value.counters.resets_total.load(Ordering::Relaxed),
            reset_errors_total: value.counters.reset_errors_total.load(Ordering::Relaxed),
        }
    }
}

impl From<&FlushObservabilityCounters> for FlushObservabilitySnapshot {
    fn from(counters: &FlushObservabilityCounters) -> Self {
        Self {
            pipeline_runs_total: counters.pipeline_runs_total.load(Ordering::Relaxed),
            pipeline_success_total: counters.pipeline_success_total.load(Ordering::Relaxed),
            pipeline_timeout_total: counters.pipeline_timeout_total.load(Ordering::Relaxed),
            pipeline_errors_total: counters.pipeline_errors_total.load(Ordering::Relaxed),
            pipeline_duration_nanos_total: counters
                .pipeline_duration_nanos_total
                .load(Ordering::Relaxed),
            active_flush_runs_total: counters.active_flush_runs_total.load(Ordering::Relaxed),
            active_flush_errors_total: counters.active_flush_errors_total.load(Ordering::Relaxed),
            active_flushed_series_total: counters
                .active_flushed_series_total
                .load(Ordering::Relaxed),
            active_flushed_chunks_total: counters
                .active_flushed_chunks_total
                .load(Ordering::Relaxed),
            active_flushed_points_total: counters
                .active_flushed_points_total
                .load(Ordering::Relaxed),
            persist_runs_total: counters.persist_runs_total.load(Ordering::Relaxed),
            persist_success_total: counters.persist_success_total.load(Ordering::Relaxed),
            persist_noop_total: counters.persist_noop_total.load(Ordering::Relaxed),
            persist_errors_total: counters.persist_errors_total.load(Ordering::Relaxed),
            persisted_series_total: counters.persisted_series_total.load(Ordering::Relaxed),
            persisted_chunks_total: counters.persisted_chunks_total.load(Ordering::Relaxed),
            persisted_points_total: counters.persisted_points_total.load(Ordering::Relaxed),
            persisted_segments_total: counters.persisted_segments_total.load(Ordering::Relaxed),
            persist_duration_nanos_total: counters
                .persist_duration_nanos_total
                .load(Ordering::Relaxed),
            evicted_sealed_chunks_total: counters
                .evicted_sealed_chunks_total
                .load(Ordering::Relaxed),
        }
    }
}

impl From<&CompactionObservabilityCounters> for CompactionObservabilitySnapshot {
    fn from(counters: &CompactionObservabilityCounters) -> Self {
        Self {
            runs_total: counters.runs_total.load(Ordering::Relaxed),
            success_total: counters.success_total.load(Ordering::Relaxed),
            noop_total: counters.noop_total.load(Ordering::Relaxed),
            errors_total: counters.errors_total.load(Ordering::Relaxed),
            source_segments_total: counters.source_segments_total.load(Ordering::Relaxed),
            output_segments_total: counters.output_segments_total.load(Ordering::Relaxed),
            source_chunks_total: counters.source_chunks_total.load(Ordering::Relaxed),
            output_chunks_total: counters.output_chunks_total.load(Ordering::Relaxed),
            source_points_total: counters.source_points_total.load(Ordering::Relaxed),
            output_points_total: counters.output_points_total.load(Ordering::Relaxed),
            duration_nanos_total: counters.duration_nanos_total.load(Ordering::Relaxed),
        }
    }
}

impl From<&QueryObservabilityCounters> for QueryObservabilitySnapshot {
    fn from(counters: &QueryObservabilityCounters) -> Self {
        Self {
            select_calls_total: counters.select_calls_total.load(Ordering::Relaxed),
            select_errors_total: counters.select_errors_total.load(Ordering::Relaxed),
            select_duration_nanos_total: counters
                .select_duration_nanos_total
                .load(Ordering::Relaxed),
            select_points_returned_total: counters
                .select_points_returned_total
                .load(Ordering::Relaxed),
            select_with_options_calls_total: counters
                .select_with_options_calls_total
                .load(Ordering::Relaxed),
            select_with_options_errors_total: counters
                .select_with_options_errors_total
                .load(Ordering::Relaxed),
            select_with_options_duration_nanos_total: counters
                .select_with_options_duration_nanos_total
                .load(Ordering::Relaxed),
            select_with_options_points_returned_total: counters
                .select_with_options_points_returned_total
                .load(Ordering::Relaxed),
            select_all_calls_total: counters.select_all_calls_total.load(Ordering::Relaxed),
            select_all_errors_total: counters.select_all_errors_total.load(Ordering::Relaxed),
            select_all_duration_nanos_total: counters
                .select_all_duration_nanos_total
                .load(Ordering::Relaxed),
            select_all_series_returned_total: counters
                .select_all_series_returned_total
                .load(Ordering::Relaxed),
            select_all_points_returned_total: counters
                .select_all_points_returned_total
                .load(Ordering::Relaxed),
            select_series_calls_total: counters.select_series_calls_total.load(Ordering::Relaxed),
            select_series_errors_total: counters.select_series_errors_total.load(Ordering::Relaxed),
            select_series_duration_nanos_total: counters
                .select_series_duration_nanos_total
                .load(Ordering::Relaxed),
            select_series_returned_total: counters
                .select_series_returned_total
                .load(Ordering::Relaxed),
            merge_path_queries_total: counters.merge_path_queries_total.load(Ordering::Relaxed),
            append_sort_path_queries_total: counters
                .append_sort_path_queries_total
                .load(Ordering::Relaxed),
        }
    }
}

impl ChunkStorage {
    pub(super) fn observability_snapshot_impl(&self) -> StorageObservabilitySnapshot {
        StorageObservabilitySnapshot {
            wal: WalObservabilitySnapshot::from(WalSnapshotView {
                counters: &self.observability.wal,
                runtime: WalRuntimeSnapshot::from_wal(self.wal.as_ref()),
            }),
            flush: FlushObservabilitySnapshot::from(&self.observability.flush),
            compaction: CompactionObservabilitySnapshot::from(&self.observability.compaction),
            query: QueryObservabilitySnapshot::from(&self.observability.query),
        }
    }
}
