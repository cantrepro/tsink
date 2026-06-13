use super::super::super::*;
use crate::engine::segment::segment_validation_error_message;

#[derive(Clone, Copy)]
pub(super) struct RegistryPersistenceContext<'a> {
    accounting_enabled: bool,
    registry: &'a RwLock<SeriesRegistry>,
    pending_series_ids: &'a RwLock<BTreeSet<SeriesId>>,
    delta_series_count: &'a AtomicU64,
    persistence_lock: &'a Mutex<()>,
    registry_used_bytes: &'a AtomicU64,
    metadata_used_bytes: &'a AtomicU64,
    shared_used_bytes: &'a AtomicU64,
    used_bytes: &'a AtomicU64,
}

impl<'a> RegistryPersistenceContext<'a> {
    fn add_included_memory_component_bytes(self, component: &AtomicU64, bytes: usize) {
        if !self.accounting_enabled || bytes == 0 {
            return;
        }

        let increment = saturating_u64_from_usize(bytes);
        component.fetch_add(increment, Ordering::AcqRel);
        self.shared_used_bytes
            .fetch_add(increment, Ordering::AcqRel);
        self.used_bytes.fetch_add(increment, Ordering::AcqRel);
    }

    fn sub_included_memory_component_bytes(self, component: &AtomicU64, bytes: usize) {
        if !self.accounting_enabled || bytes == 0 {
            return;
        }

        let decrement = saturating_u64_from_usize(bytes);
        component.fetch_sub(decrement, Ordering::AcqRel);
        self.shared_used_bytes
            .fetch_sub(decrement, Ordering::AcqRel);
        self.used_bytes.fetch_sub(decrement, Ordering::AcqRel);
    }

    fn with_included_memory_delta<T, R>(
        self,
        component: &AtomicU64,
        state: &mut T,
        measure: impl Fn(&T) -> usize,
        mutate: impl FnOnce(&mut T) -> R,
    ) -> R {
        if !self.accounting_enabled {
            return mutate(state);
        }

        let before = measure(state);
        let result = mutate(state);
        let after = measure(state);
        if after >= before {
            self.add_included_memory_component_bytes(component, after.saturating_sub(before));
        } else {
            self.sub_included_memory_component_bytes(component, before.saturating_sub(after));
        }
        result
    }

    fn replace_registry(self, next_registry: SeriesRegistry) {
        let mut registry = self.registry.write();
        self.with_included_memory_delta(
            self.registry_used_bytes,
            &mut registry,
            |registry| registry.memory_usage_bytes(),
            |registry| {
                **registry = next_registry;
            },
        );
    }

    fn clear_pending_series_ids(self) {
        let mut pending = self.pending_series_ids.write();
        self.with_included_memory_delta(
            self.metadata_used_bytes,
            &mut pending,
            |pending| ChunkStorage::btree_set_series_id_memory_usage_bytes(pending),
            |pending| pending.clear(),
        );
    }

    fn remove_pending_series_ids<I>(self, series_ids: I)
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let series_ids = series_ids.into_iter().collect::<Vec<_>>();
        if series_ids.is_empty() {
            return;
        }

        let mut pending = self.pending_series_ids.write();
        self.with_included_memory_delta(
            self.metadata_used_bytes,
            &mut pending,
            |pending| ChunkStorage::btree_set_series_id_memory_usage_bytes(pending),
            |pending| {
                for series_id in &series_ids {
                    pending.remove(series_id);
                }
            },
        );
    }

    fn pending_series_ids_snapshot(self) -> Vec<SeriesId> {
        self.pending_series_ids.read().iter().copied().collect()
    }

    fn pending_registry_subset(self, pending_series_ids: &[SeriesId]) -> Result<SeriesRegistry> {
        self.registry
            .read()
            .series_subset(pending_series_ids.iter().copied())
    }

    fn persist_registry_snapshot(self, checkpoint_path: &Path) -> Result<()> {
        self.registry.read().persist_to_path(checkpoint_path)
    }

    fn delta_series_count_value(self) -> u64 {
        self.delta_series_count.load(Ordering::Acquire)
    }

    fn set_delta_series_count(self, series_count: u64) {
        self.delta_series_count
            .store(series_count, Ordering::Release);
    }

    fn persist_series_registry_checkpoint_locked<PersistCatalog>(
        self,
        checkpoint_path: &Path,
        delta_path: &Path,
        delta_dir_path: &Path,
        allow_invalid_catalog: bool,
        persist_catalog_index: PersistCatalog,
    ) -> Result<()>
    where
        PersistCatalog: Fn(&Path) -> Result<()>,
    {
        self.persist_registry_snapshot(checkpoint_path)?;
        crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(delta_path)?;
        crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(delta_dir_path)?;
        self.clear_pending_series_ids();
        self.set_delta_series_count(0);
        if let Err(err) = persist_catalog_index(checkpoint_path) {
            if allow_invalid_catalog && segment_validation_error_message(&err).is_some() {
                tracing::warn!(
                    error = %err,
                    "Skipped persisted registry catalog checkpoint because a visible segment failed validation"
                );
            } else {
                return Err(err);
            }
        }
        Ok(())
    }

    pub(super) fn replace_registry_from_persisted_state(
        self,
        registry: SeriesRegistry,
        max_tombstoned_series_id: Option<SeriesId>,
        delta_series_count: usize,
    ) -> Result<()> {
        if let Some(max_series_id) = max_tombstoned_series_id {
            registry.reserve_series_id(max_series_id)?;
        }
        self.replace_registry(registry);
        self.clear_pending_series_ids();
        self.set_delta_series_count(saturating_u64_from_usize(delta_series_count));
        Ok(())
    }

    pub(super) fn checkpoint_series_registry_index<PersistCatalog>(
        self,
        checkpoint_path: &Path,
        delta_path: &Path,
        delta_dir_path: &Path,
        allow_invalid_catalog: bool,
        persist_catalog_index: PersistCatalog,
    ) -> Result<()>
    where
        PersistCatalog: Fn(&Path) -> Result<()>,
    {
        let _registry_persistence_guard = self.persistence_lock.lock();
        self.persist_series_registry_checkpoint_locked(
            checkpoint_path,
            delta_path,
            delta_dir_path,
            allow_invalid_catalog,
            persist_catalog_index,
        )
    }

    pub(super) fn persist_series_registry_index<PersistCatalog>(
        self,
        checkpoint_path: &Path,
        delta_path: &Path,
        delta_dir_path: &Path,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
        persist_catalog_index: PersistCatalog,
    ) -> Result<()>
    where
        PersistCatalog:
            Fn(&Path, &[registry_catalog::PersistedRegistryCatalogSource]) -> Result<()>,
    {
        let _registry_persistence_guard = self.persistence_lock.lock();
        let pending_series_ids = self.pending_series_ids_snapshot();
        if pending_series_ids.is_empty() {
            return persist_catalog_index(checkpoint_path, sources);
        }

        let pending_registry = self.pending_registry_subset(&pending_series_ids)?;
        let persisted_series_ids = pending_registry.all_series_ids();
        if persisted_series_ids.is_empty() {
            self.remove_pending_series_ids(pending_series_ids);
            return Ok(());
        }

        let current_delta_series_count = self.delta_series_count_value();
        if current_delta_series_count
            .saturating_add(saturating_u64_from_usize(persisted_series_ids.len()))
            >= REGISTRY_INCREMENTAL_CHECKPOINT_MAX_SERIES as u64
        {
            return self.persist_series_registry_checkpoint_locked(
                checkpoint_path,
                delta_path,
                delta_dir_path,
                false,
                |checkpoint_path| persist_catalog_index(checkpoint_path, sources),
            );
        }

        if let Err(err) = pending_registry.persist_incremental_to_snapshot_path(checkpoint_path) {
            return match err {
                TsinkError::DataCorruption(_) | TsinkError::InvalidConfiguration(_) => self
                    .persist_series_registry_checkpoint_locked(
                        checkpoint_path,
                        delta_path,
                        delta_dir_path,
                        false,
                        |checkpoint_path| persist_catalog_index(checkpoint_path, sources),
                    ),
                _ => Err(err),
            };
        }

        self.remove_pending_series_ids(persisted_series_ids.iter().copied());
        self.set_delta_series_count(
            current_delta_series_count
                .saturating_add(saturating_u64_from_usize(persisted_series_ids.len())),
        );
        persist_catalog_index(checkpoint_path, sources)
    }
}

impl ChunkStorage {
    pub(super) fn registry_persistence_context(&self) -> RegistryPersistenceContext<'_> {
        RegistryPersistenceContext {
            accounting_enabled: self.memory.accounting_enabled,
            registry: &self.catalog.registry,
            pending_series_ids: &self.catalog.pending_series_ids,
            delta_series_count: &self.catalog.delta_series_count,
            persistence_lock: &self.catalog.persistence_lock,
            registry_used_bytes: &self.memory.registry_used_bytes,
            metadata_used_bytes: &self.memory.metadata_used_bytes,
            shared_used_bytes: &self.memory.shared_used_bytes,
            used_bytes: &self.memory.used_bytes,
        }
    }
}
