use super::*;

impl ChunkStorage {
    pub(in super::super) fn set_ingest_post_series_definitions_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(
            &self.persist_test_hooks.post_series_definitions_append_hook,
            hook,
        );
    }

    pub(in super::super) fn clear_ingest_post_series_definitions_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.post_series_definitions_append_hook);
    }

    pub(in super::super) fn set_ingest_post_samples_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(&self.persist_test_hooks.post_samples_append_hook, hook);
    }

    pub(in super::super) fn clear_ingest_post_samples_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.post_samples_append_hook);
    }

    pub(in super::super) fn set_ingest_pre_sealed_chunk_publish_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(&self.persist_test_hooks.pre_sealed_chunk_publish_hook, hook);
    }

    pub(in super::super) fn clear_ingest_pre_sealed_chunk_publish_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.pre_sealed_chunk_publish_hook);
    }

    pub(in super::super) fn set_ingest_crash_after_wal_persist_before_ingest(&self) {
        super::set_crash_flag(&self.persist_test_hooks.crash_after_samples_persisted);
    }

    pub(in super::super) fn set_ingest_crash_after_memory_ingest_before_publish(&self) {
        super::set_crash_flag(&self.persist_test_hooks.crash_before_publish_persisted);
    }
}
