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

    pub(in super::super) fn set_snapshot_pre_publication_hook<F>(&self, hook: F)
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        *self
            .persist_test_hooks
            .snapshot_pre_publication_hook
            .write() = Some(Arc::new(hook));
    }

    pub(in super::super) fn clear_snapshot_pre_publication_hook(&self) {
        self.persist_test_hooks
            .snapshot_pre_publication_hook
            .write()
            .take();
    }

    pub(in super::super) fn invoke_snapshot_pre_publication_hook(&self) -> Result<()> {
        match self
            .persist_test_hooks
            .snapshot_pre_publication_hook
            .read()
            .clone()
        {
            Some(hook) => hook(),
            None => Ok(()),
        }
    }
}
