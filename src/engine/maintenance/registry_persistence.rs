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

    pub(in super::super) fn persisted_registry_catalog_delta_for_root_changes(
        &self,
        added_roots: &[PathBuf],
        removed_roots: &[PathBuf],
    ) -> Result<registry_catalog::PersistedRegistryCatalogDelta> {
        let removed_states = {
            let persisted_index = self.persisted.persisted_index.read();
            removed_roots
                .iter()
                .map(|root| {
                    persisted_index.segments_by_root.get(root).map(|segment| {
                        registry_catalog::catalog_entry_key(segment.lane, &segment.manifest)
                    })
                })
                .collect::<Vec<_>>()
        };
        let mut removed = Vec::with_capacity(removed_roots.len());
        for (root, key) in removed_roots.iter().zip(removed_states) {
            if let Some(key) = key {
                removed.push(key);
                continue;
            }
            // A publication may have mutated the visible index before a later catalog write
            // failed. The known-dirty/replacement retry still carries the exact removed root, and
            // its canonical path encodes the lane-independent level/id needed to replay the same
            // sidecar intent even when the physical source has already disappeared.
            let (lane, _tier) = self.persisted_segment_location_for_root(root)?;
            removed.push(registry_catalog::catalog_entry_key_from_root(lane, root)?);
        }
        let added = added_roots
            .iter()
            .map(|root| {
                let (lane, _tier) = self.persisted_segment_location_for_root(root)?;
                Ok(registry_catalog::PersistedRegistryCatalogSource {
                    lane,
                    root: root.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(registry_catalog::PersistedRegistryCatalogDelta { added, removed })
    }

    fn persist_series_registry_catalog_index_with_sources_and_kind(
        &self,
        checkpoint_path: &Path,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
        reservation_kind: crate::DiskReservationKind,
    ) -> Result<()> {
        match registry_catalog::validate_registry_catalog(checkpoint_path, sources) {
            Ok(Some(registry_catalog::ValidatedRegistryCatalog {
                series_fingerprint: Some(_),
                incremental_store: true,
                legacy_snapshot: true,
            })) => return Ok(()),
            Ok(_) | Err(TsinkError::DataCorruption(_) | TsinkError::Json(_)) => {}
            Err(err) => return Err(err),
        }
        registry_catalog::persist_registry_catalog_budgeted_with_kind(
            checkpoint_path,
            sources,
            self.persisted.local_disk_budget.as_ref(),
            reservation_kind,
        )
    }

    #[cfg(test)]
    fn checkpoint_series_registry_index_with_policy(
        &self,
        allow_invalid_catalog: bool,
    ) -> Result<()> {
        self.checkpoint_series_registry_index_with_policy_and_kind(
            allow_invalid_catalog,
            crate::DiskReservationKind::Maintenance,
        )
    }

    fn checkpoint_series_registry_index_with_policy_and_kind(
        &self,
        allow_invalid_catalog: bool,
        reservation_kind: crate::DiskReservationKind,
    ) -> Result<()> {
        let Some(checkpoint_path) = &self.persisted.series_index_path else {
            return Ok(());
        };
        let delta_path = SeriesRegistry::incremental_path(checkpoint_path);
        let delta_dir_path = SeriesRegistry::incremental_dir(checkpoint_path);
        self.registry_persistence_context_with_disk_reservation_kind(reservation_kind)
            .checkpoint_series_registry_index(
                checkpoint_path,
                &delta_path,
                &delta_dir_path,
                allow_invalid_catalog,
                |checkpoint_path| {
                    let sources = self.persisted_registry_catalog_sources();
                    self.persist_series_registry_catalog_index_with_sources_and_kind(
                        checkpoint_path,
                        &sources,
                        reservation_kind,
                    )
                },
            )
    }

    #[cfg(test)]
    pub(in super::super) fn checkpoint_series_registry_index(&self) -> Result<()> {
        self.checkpoint_series_registry_index_with_policy(false)
    }

    pub(in super::super) fn checkpoint_series_registry_index_for_recovery(&self) -> Result<()> {
        self.checkpoint_series_registry_index_with_policy_and_kind(
            false,
            crate::DiskReservationKind::Recovery,
        )
    }

    pub(in super::super) fn checkpoint_series_registry_index_allow_invalid_catalog_for_recovery(
        &self,
    ) -> Result<()> {
        self.checkpoint_series_registry_index_with_policy_and_kind(
            true,
            crate::DiskReservationKind::Recovery,
        )
    }

    pub(in super::super) fn persist_series_registry_index(&self) -> Result<()> {
        let sources = self.persisted_registry_catalog_sources();
        self.persist_series_registry_index_with_catalog_sources(&sources)
    }

    pub(in super::super) fn persist_series_registry_index_for_recovery(&self) -> Result<()> {
        let sources = self.persisted_registry_catalog_sources();
        self.persist_series_registry_index_with_catalog_sources_and_kind(
            &sources,
            crate::DiskReservationKind::Recovery,
        )
    }

    pub(in super::super) fn persist_series_registry_index_with_catalog_sources(
        &self,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
    ) -> Result<()> {
        self.persist_series_registry_index_with_catalog_sources_and_kind(
            sources,
            crate::DiskReservationKind::Maintenance,
        )
    }

    pub(in super::super) fn persist_series_registry_index_with_catalog_update(
        &self,
        update: &registry_catalog::PersistedRegistryCatalogUpdate,
    ) -> Result<()> {
        match update {
            registry_catalog::PersistedRegistryCatalogUpdate::Complete(sources) => {
                self.persist_series_registry_index_with_catalog_sources(sources)
            }
            registry_catalog::PersistedRegistryCatalogUpdate::Delta(delta) => {
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
                        &[],
                        |checkpoint_path, _sources| {
                            registry_catalog::persist_registry_catalog_delta_budgeted_with_kind(
                                checkpoint_path,
                                delta,
                                self.persisted.local_disk_budget.as_ref(),
                                crate::DiskReservationKind::Maintenance,
                            )
                        },
                    )
            }
        }
    }

    pub(in super::super) fn persist_selected_series_registry_index_without_catalog(
        &self,
        selected_series_ids: &[SeriesId],
    ) -> Result<()> {
        let Some(checkpoint_path) = &self.persisted.series_index_path else {
            return Ok(());
        };
        self.registry_persistence_context()
            .persist_selected_series_registry_index(
                checkpoint_path,
                selected_series_ids,
                &[],
                |_checkpoint_path, _sources| Ok(()),
            )
    }

    fn persist_series_registry_index_with_catalog_sources_and_kind(
        &self,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
        reservation_kind: crate::DiskReservationKind,
    ) -> Result<()> {
        let Some(checkpoint_path) = &self.persisted.series_index_path else {
            return Ok(());
        };
        let delta_path = SeriesRegistry::incremental_path(checkpoint_path);
        let delta_dir_path = SeriesRegistry::incremental_dir(checkpoint_path);
        self.registry_persistence_context_with_disk_reservation_kind(reservation_kind)
            .persist_series_registry_index(
                checkpoint_path,
                &delta_path,
                &delta_dir_path,
                sources,
                |checkpoint_path, sources| {
                    self.persist_series_registry_catalog_index_with_sources_and_kind(
                        checkpoint_path,
                        sources,
                        reservation_kind,
                    )
                },
            )
    }
}
