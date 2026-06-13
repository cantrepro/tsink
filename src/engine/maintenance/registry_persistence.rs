mod context;

use super::super::*;

impl ChunkStorage {
    pub(in super::super) fn replace_registry_from_persisted_state(
        &self,
        registry: SeriesRegistry,
        delta_series_count: usize,
    ) -> Result<()> {
        self.registry_persistence_context()
            .replace_registry_from_persisted_state(
                registry,
                self.tombstone_read_context().max_tombstoned_series_id(),
                delta_series_count,
            )
    }

    pub(super) fn persisted_registry_catalog_sources(
        &self,
    ) -> Vec<registry_catalog::PersistedRegistryCatalogSource> {
        let persisted_index = self.persisted.persisted_index.read();
        persisted_index
            .segments_by_root
            .iter()
            .map(
                |(root, segment)| registry_catalog::PersistedRegistryCatalogSource {
                    lane: segment.lane,
                    root: root.clone(),
                },
            )
            .collect()
    }

    pub(in super::super) fn persisted_registry_catalog_sources_with_root_changes(
        &self,
        added_roots: &[PathBuf],
        removed_roots: &[PathBuf],
    ) -> Result<Vec<registry_catalog::PersistedRegistryCatalogSource>> {
        let mut sources = self
            .persisted_registry_catalog_sources()
            .into_iter()
            .map(|source| (source.root.clone(), source))
            .collect::<BTreeMap<_, _>>();
        for root in removed_roots {
            sources.remove(root);
        }
        for root in added_roots {
            let (lane, _tier) = self.persisted_segment_location_for_root(root)?;
            sources.insert(
                root.clone(),
                registry_catalog::PersistedRegistryCatalogSource {
                    lane,
                    root: root.clone(),
                },
            );
        }
        Ok(sources.into_values().collect())
    }

    fn persist_series_registry_catalog_index_with_sources(
        &self,
        checkpoint_path: &Path,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
    ) -> Result<()> {
        if matches!(
            registry_catalog::validate_registry_catalog(checkpoint_path, sources)?,
            Some(registry_catalog::ValidatedRegistryCatalog {
                series_fingerprint: Some(_),
            })
        ) {
            return Ok(());
        }
        registry_catalog::persist_registry_catalog(checkpoint_path, sources)
    }

    fn persist_series_registry_catalog_index(&self, checkpoint_path: &Path) -> Result<()> {
        let sources = self.persisted_registry_catalog_sources();
        self.persist_series_registry_catalog_index_with_sources(checkpoint_path, &sources)
    }

    fn checkpoint_series_registry_index_with_policy(
        &self,
        allow_invalid_catalog: bool,
    ) -> Result<()> {
        let Some(checkpoint_path) = &self.persisted.series_index_path else {
            return Ok(());
        };
        let delta_path = SeriesRegistry::incremental_path(checkpoint_path);
        let delta_dir_path = SeriesRegistry::incremental_dir(checkpoint_path);
        self.registry_persistence_context()
            .checkpoint_series_registry_index(
                checkpoint_path,
                &delta_path,
                &delta_dir_path,
                allow_invalid_catalog,
                |checkpoint_path| self.persist_series_registry_catalog_index(checkpoint_path),
            )
    }

    pub(in super::super) fn checkpoint_series_registry_index(&self) -> Result<()> {
        self.checkpoint_series_registry_index_with_policy(false)
    }

    pub(in super::super) fn checkpoint_series_registry_index_allow_invalid_catalog(
        &self,
    ) -> Result<()> {
        self.checkpoint_series_registry_index_with_policy(true)
    }

    pub(in super::super) fn persist_series_registry_index(&self) -> Result<()> {
        let sources = self.persisted_registry_catalog_sources();
        self.persist_series_registry_index_with_catalog_sources(&sources)
    }

    pub(in super::super) fn persist_series_registry_index_with_catalog_sources(
        &self,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
    ) -> Result<()> {
        let Some(checkpoint_path) = &self.persisted.series_index_path else {
            return Ok(());
        };
        let delta_path = SeriesRegistry::incremental_path(checkpoint_path);
        let delta_dir_path = SeriesRegistry::incremental_dir(checkpoint_path);
        self.registry_persistence_context()
            .persist_series_registry_index(
                checkpoint_path,
                &delta_path,
                &delta_dir_path,
                sources,
                |checkpoint_path, sources| {
                    self.persist_series_registry_catalog_index_with_sources(
                        checkpoint_path,
                        sources,
                    )
                },
            )
    }
}
