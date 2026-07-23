use super::*;

impl ChunkStorage {
    fn persisted_inventory_entries_for_roots(
        &self,
        roots: &BTreeSet<PathBuf>,
    ) -> Vec<SegmentInventoryEntry> {
        let persisted_index = self.persisted.persisted_index.read();
        roots
            .iter()
            .filter_map(|root| {
                persisted_index
                    .segments_by_root
                    .get(root)
                    .map(|state| SegmentInventoryEntry {
                        lane: state.lane,
                        tier: state.tier,
                        root: root.clone(),
                        manifest: state.manifest.clone(),
                    })
            })
            .collect()
    }

    fn update_visible_segment_counter(
        counter: &std::sync::atomic::AtomicU64,
        before: u64,
        after: u64,
    ) {
        if before == after {
            return;
        }
        let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(before).saturating_add(after))
        });
    }

    fn publish_non_tiered_segment_inventory_delta(
        &self,
        before: &[SegmentInventoryEntry],
        after: &[SegmentInventoryEntry],
    ) {
        let tier_count = |entries: &[SegmentInventoryEntry], tier| {
            saturating_u64_from_usize(entries.iter().filter(|entry| entry.tier == tier).count())
        };
        Self::update_visible_segment_counter(
            &self.observability.flush.hot_segments_visible,
            tier_count(before, PersistedSegmentTier::Hot),
            tier_count(after, PersistedSegmentTier::Hot),
        );
        Self::update_visible_segment_counter(
            &self.observability.flush.warm_segments_visible,
            tier_count(before, PersistedSegmentTier::Warm),
            tier_count(after, PersistedSegmentTier::Warm),
        );
        Self::update_visible_segment_counter(
            &self.observability.flush.cold_segments_visible,
            tier_count(before, PersistedSegmentTier::Cold),
            tier_count(after, PersistedSegmentTier::Cold),
        );
    }

    #[cfg(test)]
    fn invoke_catalog_transition_post_catalog_publication_hook(&self) -> Result<()> {
        let hook = self
            .persist_test_hooks
            .catalog_transition_post_catalog_publication_hook
            .read()
            .clone();
        match hook {
            Some(hook) => hook(),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn set_catalog_transition_post_catalog_publication_hook<
        F,
    >(
        &self,
        hook: F,
    ) where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        *self
            .persist_test_hooks
            .catalog_transition_post_catalog_publication_hook
            .write() = Some(std::sync::Arc::new(hook));
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn clear_catalog_transition_post_catalog_publication_hook(
        &self,
    ) {
        self.persist_test_hooks
            .catalog_transition_post_catalog_publication_hook
            .write()
            .take();
    }

    #[cfg(test)]
    fn invoke_catalog_transition_post_index_mutation_hook(&self) -> Result<()> {
        let hook = self
            .persist_test_hooks
            .catalog_transition_post_index_mutation_hook
            .read()
            .clone();
        match hook {
            Some(hook) => hook(),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn set_catalog_transition_post_index_mutation_hook<F>(
        &self,
        hook: F,
    ) where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        *self
            .persist_test_hooks
            .catalog_transition_post_index_mutation_hook
            .write() = Some(std::sync::Arc::new(hook));
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn clear_catalog_transition_post_index_mutation_hook(
        &self,
    ) {
        self.persist_test_hooks
            .catalog_transition_post_index_mutation_hook
            .write()
            .take();
    }

    pub(super) fn mirror_segment_inventory_entries_if_configured(
        &self,
        entries: &[SegmentInventoryEntry],
    ) -> Result<()> {
        let Some(config) = &self.persisted.tiered_storage else {
            return Ok(());
        };
        if !config.mirror_hot_segments {
            return Ok(());
        }

        self.validate_shared_object_store_writer_lock()?;

        for entry in entries {
            if entry.tier != PersistedSegmentTier::Hot {
                continue;
            }
            let destination = tiering::destination_segment_root(
                config,
                entry.lane,
                PersistedSegmentTier::Hot,
                &entry.manifest,
            );
            if destination == entry.root {
                continue;
            }
            tiering::move_segment_to_tier(&entry.root, &destination)?;
        }

        Ok(())
    }

    pub(super) fn publish_segment_inventory(&self, inventory: &SegmentInventory) -> Result<()> {
        let (hot, warm, cold) = inventory.tier_counts();
        self.observability
            .flush
            .hot_segments_visible
            .store(hot, Ordering::Relaxed);
        self.observability
            .flush
            .warm_segments_visible
            .store(warm, Ordering::Relaxed);
        self.observability
            .flush
            .cold_segments_visible
            .store(cold, Ordering::Relaxed);
        if let Some(config) = &self.persisted.tiered_storage {
            if let Some(path) = config.segment_catalog_path.as_deref() {
                tiering::persist_segment_catalog_budgeted(
                    path,
                    inventory,
                    self.persisted.local_disk_budget.as_ref(),
                )?;
            }
            if self.runtime.runtime_mode != StorageRuntimeMode::ComputeOnly {
                self.validate_shared_object_store_writer_lock()?;
                let shared_inventory = self.shared_remote_segment_inventory(inventory);
                tiering::persist_segment_catalog_budgeted(
                    &tiering::shared_segment_catalog_path(config),
                    &shared_inventory,
                    self.persisted.local_disk_budget.as_ref(),
                )?;
            }
        }
        Ok(())
    }

    fn publish_scanned_segment_inventory(&self, inventory: &SegmentInventory) -> Result<()> {
        self.mirror_segment_inventory_entries_if_configured(inventory.entries())?;
        self.publish_segment_inventory(inventory)
    }

    pub(in super::super) fn apply_persisted_catalog_transition_phase(
        &self,
        transition: PersistedCatalogTransition,
        current_visibility_generation: u64,
    ) -> Result<PersistedCatalogRefreshApply> {
        if transition
            .visibility_fence
            .is_some_and(|fence| !fence.matches(current_visibility_generation))
        {
            return Ok(PersistedCatalogRefreshApply::SkippedStaleVisibleState);
        }

        // Without tiered storage there is no durable segment-catalog file to rewrite. Capture
        // only the roots named by this transition so publication can update visibility counters
        // by exact delta instead of rebuilding an inventory of every live segment. Tiered mode
        // deliberately retains the complete inventory path because its local/shared catalogs are
        // monolithic snapshots whose crash-safe replacement requires the complete final image.
        let non_tiered_delta_roots = if self.persisted.tiered_storage.is_none() {
            match &transition.publication {
                PersistedCatalogPublication::PersistedState {
                    published_segment_roots,
                    refresh_tombstones: _,
                } => {
                    let mut roots = transition
                        .removed_roots
                        .iter()
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    roots.extend(published_segment_roots.iter().cloned());
                    Some(roots)
                }
                PersistedCatalogPublication::Inventory { .. } => None,
            }
        } else {
            None
        };
        let non_tiered_delta_before = non_tiered_delta_roots
            .as_ref()
            .map(|roots| self.persisted_inventory_entries_for_roots(roots));

        let refresh_tombstones = match &transition.publication {
            PersistedCatalogPublication::PersistedState {
                refresh_tombstones, ..
            }
            | PersistedCatalogPublication::Inventory {
                refresh_tombstones, ..
            } => *refresh_tombstones,
        };
        // Ordinary segment transitions update their accounted components incrementally. A full
        // engine recount here made every bounded flush proportional to all live state. Tombstone
        // replacement is the exception: its conservative admission compares a newly decoded map
        // with the complete current visibility footprint, so reconcile before creating that
        // reservation only on that path.
        if refresh_tombstones {
            self.refresh_memory_usage();
        }
        let mut tombstone_reservation = self.tombstone_memory_reservation();
        let loaded_tombstones = if refresh_tombstones {
            self.validate_shared_object_store_writer_lock()?;
            let tombstone_index = self.tombstone_index_context();
            // A durable Committing coordinator is authoritative even before its first lane
            // manifest is published. Recover (or fail closed) before reading any manifest so a
            // catalog refresh cannot replace the live map with a stale predecessor image.
            tombstone_index.recover_pending_transaction(&mut tombstone_reservation)?;
            let merged = tombstone_index.read_tombstones_index(&mut tombstone_reservation)?;
            let transition_visibility_headroom =
                transition
                    .loaded_segments
                    .iter()
                    .fold(16 * 1024usize, |total, segment| {
                        // A loaded chunk can add at most two transient visibility-range entries
                        // (source plus normalization destination, currently under 64 bytes total)
                        // and one persisted ref. 1 KiB per chunk therefore dominates the second
                        // post-install visibility estimate; removals can only reduce it. The per-
                        // series allowance covers capped retained summaries and cache-map growth.
                        let chunk_headroom = segment.chunk_index.entries.len().saturating_mul(1024);
                        let series_headroom = segment
                            .series
                            .len()
                            .max(segment.manifest.series_count)
                            .saturating_mul(4096);
                        total
                            .saturating_add(chunk_headroom)
                            .saturating_add(series_headroom)
                    });
            self.tombstone_publication_context()
                .admit_loaded_tombstone_publication(
                    self,
                    &merged,
                    transition_visibility_headroom,
                    &mut tombstone_reservation,
                )?;
            Some(merged)
        } else {
            None
        };

        // Publish authoritative deletes first. If a later segment/catalog operation fails, the
        // conservative state hides data rather than exposing newly visible deleted samples.
        if let Some(tombstones) = loaded_tombstones {
            self.tombstone_publication_context()
                .replace_loaded_tombstones_index_locked(
                    self,
                    tombstones,
                    &mut tombstone_reservation,
                )?;
        }

        self.add_persisted_segments_from_loaded(transition.loaded_segments)?;
        self.remove_persisted_segment_roots(&transition.removed_roots)?;

        #[cfg(test)]
        self.invoke_catalog_transition_post_index_mutation_hook()?;

        match transition.publication {
            PersistedCatalogPublication::PersistedState {
                published_segment_roots,
                refresh_tombstones: _,
            } => {
                if let (Some(roots), Some(before)) =
                    (non_tiered_delta_roots.as_ref(), non_tiered_delta_before)
                {
                    let after = self.persisted_inventory_entries_for_roots(roots);
                    self.publish_non_tiered_segment_inventory_delta(&before, &after);
                } else {
                    self.refresh_segment_catalog_and_observability_from_persisted_state(
                        &published_segment_roots,
                    )?;
                }
            }
            PersistedCatalogPublication::Inventory {
                inventory,
                refresh_tombstones: _,
            } => {
                self.publish_scanned_segment_inventory(&inventory)?;
            }
        }

        #[cfg(test)]
        self.invoke_catalog_transition_post_catalog_publication_hook()?;

        if let Some(registry_catalog_update) = transition.registry_catalog_update {
            self.persist_series_registry_index_with_catalog_update(&registry_catalog_update)?;
        }

        Ok(PersistedCatalogRefreshApply::Applied)
    }
}
