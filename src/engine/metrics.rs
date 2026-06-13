use super::*;
use crate::engine::compactor::CompactionRunStats;

#[derive(Default)]
pub(super) struct StorageObservabilityCounters {
    pub(super) wal: WalObservabilityCounters,
    pub(super) flush: FlushObservabilityCounters,
    pub(super) compaction: CompactionObservabilityCounters,
    pub(super) query: QueryObservabilityCounters,
}

impl StorageObservabilityCounters {
    pub(super) fn record_compaction_result(&self, stats: CompactionRunStats, duration_nanos: u64) {
        self.compaction.runs_total.fetch_add(1, Ordering::Relaxed);
        self.compaction
            .duration_nanos_total
            .fetch_add(duration_nanos, Ordering::Relaxed);
        self.compaction.source_segments_total.fetch_add(
            saturating_u64_from_usize(stats.source_segments),
            Ordering::Relaxed,
        );
        self.compaction.output_segments_total.fetch_add(
            saturating_u64_from_usize(stats.output_segments),
            Ordering::Relaxed,
        );
        self.compaction.source_chunks_total.fetch_add(
            saturating_u64_from_usize(stats.source_chunks),
            Ordering::Relaxed,
        );
        self.compaction.output_chunks_total.fetch_add(
            saturating_u64_from_usize(stats.output_chunks),
            Ordering::Relaxed,
        );
        self.compaction.source_points_total.fetch_add(
            saturating_u64_from_usize(stats.source_points),
            Ordering::Relaxed,
        );
        self.compaction.output_points_total.fetch_add(
            saturating_u64_from_usize(stats.output_points),
            Ordering::Relaxed,
        );
        if stats.compacted {
            self.compaction
                .success_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.compaction.noop_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn record_compaction_error(&self, duration_nanos: u64) {
        self.compaction.runs_total.fetch_add(1, Ordering::Relaxed);
        self.compaction.errors_total.fetch_add(1, Ordering::Relaxed);
        self.compaction
            .duration_nanos_total
            .fetch_add(duration_nanos, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub(super) struct WalObservabilityCounters {
    pub(super) replay_runs_total: AtomicU64,
    pub(super) replay_frames_total: AtomicU64,
    pub(super) replay_series_definitions_total: AtomicU64,
    pub(super) replay_sample_batches_total: AtomicU64,
    pub(super) replay_points_total: AtomicU64,
    pub(super) replay_errors_total: AtomicU64,
    pub(super) replay_duration_nanos_total: AtomicU64,
    pub(super) append_series_definitions_total: AtomicU64,
    pub(super) append_sample_batches_total: AtomicU64,
    pub(super) append_points_total: AtomicU64,
    pub(super) append_bytes_total: AtomicU64,
    pub(super) append_errors_total: AtomicU64,
    pub(super) resets_total: AtomicU64,
    pub(super) reset_errors_total: AtomicU64,
}

#[derive(Default)]
pub(super) struct FlushObservabilityCounters {
    pub(super) pipeline_runs_total: AtomicU64,
    pub(super) pipeline_success_total: AtomicU64,
    pub(super) pipeline_timeout_total: AtomicU64,
    pub(super) pipeline_errors_total: AtomicU64,
    pub(super) pipeline_duration_nanos_total: AtomicU64,
    pub(super) active_flush_runs_total: AtomicU64,
    pub(super) active_flush_errors_total: AtomicU64,
    pub(super) active_flushed_series_total: AtomicU64,
    pub(super) active_flushed_chunks_total: AtomicU64,
    pub(super) active_flushed_points_total: AtomicU64,
    pub(super) persist_runs_total: AtomicU64,
    pub(super) persist_success_total: AtomicU64,
    pub(super) persist_noop_total: AtomicU64,
    pub(super) persist_errors_total: AtomicU64,
    pub(super) persisted_series_total: AtomicU64,
    pub(super) persisted_chunks_total: AtomicU64,
    pub(super) persisted_points_total: AtomicU64,
    pub(super) persisted_segments_total: AtomicU64,
    pub(super) persist_duration_nanos_total: AtomicU64,
    pub(super) evicted_sealed_chunks_total: AtomicU64,
}

#[derive(Default)]
pub(super) struct CompactionObservabilityCounters {
    pub(super) runs_total: AtomicU64,
    pub(super) success_total: AtomicU64,
    pub(super) noop_total: AtomicU64,
    pub(super) errors_total: AtomicU64,
    pub(super) source_segments_total: AtomicU64,
    pub(super) output_segments_total: AtomicU64,
    pub(super) source_chunks_total: AtomicU64,
    pub(super) output_chunks_total: AtomicU64,
    pub(super) source_points_total: AtomicU64,
    pub(super) output_points_total: AtomicU64,
    pub(super) duration_nanos_total: AtomicU64,
}

#[derive(Default)]
pub(super) struct QueryObservabilityCounters {
    pub(super) select_calls_total: AtomicU64,
    pub(super) select_errors_total: AtomicU64,
    pub(super) select_duration_nanos_total: AtomicU64,
    pub(super) select_points_returned_total: AtomicU64,
    pub(super) select_with_options_calls_total: AtomicU64,
    pub(super) select_with_options_errors_total: AtomicU64,
    pub(super) select_with_options_duration_nanos_total: AtomicU64,
    pub(super) select_with_options_points_returned_total: AtomicU64,
    pub(super) select_all_calls_total: AtomicU64,
    pub(super) select_all_errors_total: AtomicU64,
    pub(super) select_all_duration_nanos_total: AtomicU64,
    pub(super) select_all_series_returned_total: AtomicU64,
    pub(super) select_all_points_returned_total: AtomicU64,
    pub(super) select_series_calls_total: AtomicU64,
    pub(super) select_series_errors_total: AtomicU64,
    pub(super) select_series_duration_nanos_total: AtomicU64,
    pub(super) select_series_returned_total: AtomicU64,
    pub(super) merge_path_queries_total: AtomicU64,
    pub(super) append_sort_path_queries_total: AtomicU64,
}
