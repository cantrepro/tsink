use super::*;

impl ChunkStorage {
    #[allow(dead_code)]
    pub(in super::super) fn invoke_query_persisted_chunk_decode_hook(&self) {
        super::invoke_commit_hook(&self.persist_test_hooks.query_persisted_chunk_decode_hook);
    }

    pub(in super::super) fn set_shard_metadata_fallback_scan_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self.persist_test_hooks.shard_metadata_fallback_scan_hook,
            hook,
        );
    }

    pub(in super::super) fn set_metadata_live_series_snapshot_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self.persist_test_hooks.metadata_live_series_snapshot_hook,
            hook,
        );
    }

    pub(in super::super) fn set_metadata_all_series_seed_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(&self.persist_test_hooks.metadata_all_series_seed_hook, hook);
    }

    pub(in super::super) fn set_metadata_all_series_postings_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self.persist_test_hooks.metadata_all_series_postings_hook,
            hook,
        );
    }

    pub(in super::super) fn set_metadata_persisted_postings_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self.persist_test_hooks.metadata_persisted_postings_hook,
            hook,
        );
    }

    pub(in super::super) fn set_metadata_direct_candidate_scan_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self.persist_test_hooks.metadata_direct_candidate_scan_hook,
            hook,
        );
    }

    pub(in super::super) fn set_metadata_live_series_pre_prune_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self.persist_test_hooks.metadata_live_series_pre_prune_hook,
            hook,
        );
    }

    pub(in super::super) fn set_metadata_time_range_summary_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self.persist_test_hooks.metadata_time_range_summary_hook,
            hook,
        );
    }

    pub(in super::super) fn set_metadata_time_range_segment_prune_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self
                .persist_test_hooks
                .metadata_time_range_segment_prune_hook,
            hook,
        );
    }

    pub(in super::super) fn set_metadata_time_range_segment_spill_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self
                .persist_test_hooks
                .metadata_time_range_segment_spill_hook,
            hook,
        );
    }

    pub(in super::super) fn set_metadata_time_range_persisted_exact_scan_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self
                .persist_test_hooks
                .metadata_time_range_persisted_exact_scan_hook,
            hook,
        );
    }

    pub(in super::super) fn clear_shard_metadata_fallback_scan_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.shard_metadata_fallback_scan_hook);
    }

    pub(in super::super) fn clear_metadata_live_series_snapshot_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.metadata_live_series_snapshot_hook);
    }

    pub(in super::super) fn clear_metadata_all_series_seed_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.metadata_all_series_seed_hook);
    }

    pub(in super::super) fn clear_metadata_all_series_postings_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.metadata_all_series_postings_hook);
    }

    pub(in super::super) fn clear_metadata_persisted_postings_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.metadata_persisted_postings_hook);
    }

    pub(in super::super) fn clear_metadata_direct_candidate_scan_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.metadata_direct_candidate_scan_hook);
    }

    pub(in super::super) fn clear_metadata_live_series_pre_prune_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.metadata_live_series_pre_prune_hook);
    }

    pub(in super::super) fn clear_metadata_time_range_summary_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.metadata_time_range_summary_hook);
    }

    pub(in super::super) fn clear_metadata_time_range_segment_prune_hook(&self) {
        super::clear_commit_hook(
            &self
                .persist_test_hooks
                .metadata_time_range_segment_prune_hook,
        );
    }

    pub(in super::super) fn clear_metadata_time_range_segment_spill_hook(&self) {
        super::clear_commit_hook(
            &self
                .persist_test_hooks
                .metadata_time_range_segment_spill_hook,
        );
    }

    pub(in super::super) fn clear_metadata_time_range_persisted_exact_scan_hook(&self) {
        super::clear_commit_hook(
            &self
                .persist_test_hooks
                .metadata_time_range_persisted_exact_scan_hook,
        );
    }

    pub(in super::super) fn set_query_merge_in_memory_source_snapshot_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self
                .persist_test_hooks
                .query_merge_in_memory_source_snapshot_hook,
            hook,
        );
    }

    pub(in super::super) fn clear_query_merge_in_memory_source_snapshot_hook(&self) {
        super::clear_commit_hook(
            &self
                .persist_test_hooks
                .query_merge_in_memory_source_snapshot_hook,
        );
    }

    pub(in super::super) fn set_query_append_sort_in_memory_source_snapshot_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self
                .persist_test_hooks
                .query_append_sort_in_memory_source_snapshot_hook,
            hook,
        );
    }

    pub(in super::super) fn clear_query_append_sort_in_memory_source_snapshot_hook(&self) {
        super::clear_commit_hook(
            &self
                .persist_test_hooks
                .query_append_sort_in_memory_source_snapshot_hook,
        );
    }

    pub(in super::super) fn set_query_persisted_chunk_decode_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self.persist_test_hooks.query_persisted_chunk_decode_hook,
            hook,
        );
    }

    pub(in super::super) fn clear_query_persisted_chunk_decode_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.query_persisted_chunk_decode_hook);
    }

    pub(in super::super) fn invoke_metadata_all_series_seed_hook(&self) {
        super::invoke_commit_hook(&self.persist_test_hooks.metadata_all_series_seed_hook);
    }

    pub(in super::super) fn invoke_metadata_persisted_postings_hook(&self) {
        super::invoke_commit_hook(&self.persist_test_hooks.metadata_persisted_postings_hook);
    }

    pub(in super::super) fn invoke_metadata_direct_candidate_scan_hook(&self) {
        super::invoke_commit_hook(&self.persist_test_hooks.metadata_direct_candidate_scan_hook);
    }

    pub(in super::super) fn invoke_metadata_time_range_summary_hook(&self) {
        super::invoke_commit_hook(&self.persist_test_hooks.metadata_time_range_summary_hook);
    }

    pub(in super::super) fn invoke_metadata_time_range_segment_prune_hook(&self) {
        super::invoke_commit_hook(
            &self
                .persist_test_hooks
                .metadata_time_range_segment_prune_hook,
        );
    }

    #[allow(dead_code)]
    pub(in super::super) fn invoke_metadata_time_range_segment_spill_hook(&self) {
        super::invoke_commit_hook(
            &self
                .persist_test_hooks
                .metadata_time_range_segment_spill_hook,
        );
    }

    pub(in super::super) fn invoke_metadata_time_range_persisted_exact_scan_hook(&self) {
        super::invoke_commit_hook(
            &self
                .persist_test_hooks
                .metadata_time_range_persisted_exact_scan_hook,
        );
    }

    pub(in super::super) fn invoke_query_merge_in_memory_source_snapshot_hook(&self) {
        super::invoke_commit_hook(
            &self
                .persist_test_hooks
                .query_merge_in_memory_source_snapshot_hook,
        );
    }

    pub(in super::super) fn invoke_query_append_sort_in_memory_source_snapshot_hook(&self) {
        super::invoke_commit_hook(
            &self
                .persist_test_hooks
                .query_append_sort_in_memory_source_snapshot_hook,
        );
    }
}
