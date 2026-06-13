use super::*;

impl ChunkStorage {
    pub(in super::super) fn set_snapshot_pre_wal_copy_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        super::set_commit_hook(&self.persist_test_hooks.snapshot_pre_wal_copy_hook, hook);
    }

    pub(in super::super) fn clear_snapshot_pre_wal_copy_hook(&self) {
        super::clear_commit_hook(&self.persist_test_hooks.snapshot_pre_wal_copy_hook);
    }

    pub(in super::super) fn invoke_snapshot_pre_wal_copy_hook(&self) {
        super::invoke_commit_hook(&self.persist_test_hooks.snapshot_pre_wal_copy_hook);
    }
}
