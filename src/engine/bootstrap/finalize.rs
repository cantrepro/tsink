use super::*;

pub(super) struct StartupFinalizePhase;

#[derive(Clone, Copy)]
pub(super) struct StartupFinalizeState {
    pub(super) force_registry_checkpoint_after_startup: bool,
    pub(super) reconcile_registry_with_persisted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RegistryPersistenceAction {
    Checkpoint,
    Persist,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StartupFinalizeActions {
    pub(super) registry_persistence: RegistryPersistenceAction,
    pub(super) run_sync_maintenance: bool,
    pub(super) start_background_threads: bool,
    pub(super) schedule_startup_maintenance: bool,
}

impl StartupFinalizeState {
    pub(super) fn actions(
        self,
        background_threads_enabled: bool,
        background_fail_fast: bool,
    ) -> StartupFinalizeActions {
        StartupFinalizeActions {
            registry_persistence: if self.force_registry_checkpoint_after_startup
                || !self.reconcile_registry_with_persisted
            {
                RegistryPersistenceAction::Checkpoint
            } else {
                RegistryPersistenceAction::Persist
            },
            run_sync_maintenance: !background_threads_enabled || background_fail_fast,
            start_background_threads: background_threads_enabled,
            schedule_startup_maintenance: background_threads_enabled && !background_fail_fast,
        }
    }
}

impl StartupFinalizePhase {
    pub(super) fn run_snapshot_validation(storage: &Arc<ChunkStorage>) -> Result<()> {
        // Exercise the same bounded in-memory reconciliation and resource checks that make a
        // normal instance serviceable, while deliberately suppressing every durable publication,
        // retention deletion, maintenance schedule, and worker start.
        storage.reconcile_live_metadata_indexes()?;
        if storage.memory_budget_value() != usize::MAX {
            storage.refresh_memory_usage();
            storage.enforce_memory_budget_if_needed()?;
        }
        if let Some(local_disk_budget) = &storage.persisted.local_disk_budget {
            local_disk_budget.reconcile()?;
        }
        Ok(())
    }

    pub(super) fn run(
        storage: &Arc<ChunkStorage>,
        finalize_state: StartupFinalizeState,
        background_threads_enabled: bool,
        background_fail_fast: bool,
    ) -> Result<()> {
        let actions = finalize_state.actions(background_threads_enabled, background_fail_fast);

        if actions.run_sync_maintenance {
            storage.sweep_expired_persisted_segments()?;
            storage.reconcile_live_metadata_indexes()?;
        }

        // Startup must be able to repair or create the registry even when the configured logical
        // cap is below existing usage. Recovery admission still enforces physical free-space
        // headroom, and final reconciliation exposes over-limit state before foreground growth.
        match actions.registry_persistence {
            RegistryPersistenceAction::Checkpoint => {
                storage.checkpoint_series_registry_index_for_recovery()?
            }
            RegistryPersistenceAction::Persist => {
                storage.persist_series_registry_index_for_recovery()?
            }
        }

        if storage.memory_budget_value() != usize::MAX {
            storage.refresh_memory_usage();
            storage.enforce_memory_budget_if_needed()?;
        }

        if actions.schedule_startup_maintenance {
            storage.schedule_startup_maintenance();
        }

        if let Some(local_disk_budget) = &storage.persisted.local_disk_budget {
            local_disk_budget.reconcile()?;
        }

        if actions.start_background_threads {
            storage.start_background_compaction_thread()?;
            storage.start_background_flush_thread(DEFAULT_FLUSH_INTERVAL)?;
            storage.start_background_persisted_refresh_thread()?;
            storage.start_background_rollup_thread(DEFAULT_ROLLUP_INTERVAL)?;
        }

        Ok(())
    }
}
