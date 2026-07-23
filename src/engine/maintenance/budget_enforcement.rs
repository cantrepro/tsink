#[path = "budget_enforcement/context.rs"]
mod context;

use super::super::*;

impl ChunkStorage {
    pub(in super::super) fn evict_selected_persisted_sealed_chunks(
        &self,
        selected: &[PendingSealedChunkLocation],
    ) -> usize {
        self.budget_enforcement_context()
            .evict_selected_persisted_sealed_chunks(selected)
    }

    #[cfg(test)]
    pub(in super::super) fn set_exact_sealed_eviction_inspect_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self
            .persist_test_hooks
            .exact_sealed_eviction_inspect_hook
            .write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(in super::super) fn clear_exact_sealed_eviction_inspect_hook(&self) {
        self.persist_test_hooks
            .exact_sealed_eviction_inspect_hook
            .write()
            .take();
    }

    pub(in super::super) fn enforce_memory_budget_if_needed(&self) -> Result<()> {
        let budget_enforcement = self.budget_enforcement_context();
        let budget = budget_enforcement.budget_value();
        if budget == usize::MAX || budget_enforcement.used_value() <= budget {
            return Ok(());
        }

        if !budget_enforcement.can_reclaim_persisted_memory() {
            return Ok(());
        }

        let _backpressure_guard = budget_enforcement.lock_backpressure();
        if budget_enforcement.used_value() <= budget {
            return Ok(());
        }

        self.enforce_memory_budget_locked(budget)
    }

    fn enforce_memory_budget_locked(&self, budget: usize) -> Result<()> {
        let budget_enforcement = self.budget_enforcement_context();
        if budget_enforcement.used_value() <= budget {
            return Ok(());
        }

        self.flush_all_active()?;
        self.prune_empty_active_series();
        if budget_enforcement.used_value() <= budget {
            return Ok(());
        }

        self.persist_segment_with_outcome()?;

        if budget_enforcement.used_value() > budget {
            budget_enforcement.evict_persisted_sealed_chunks_to_budget_locked(budget);
        }

        Ok(())
    }
}
