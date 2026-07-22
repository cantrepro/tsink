use super::*;
use crate::engine::tombstone::{self, TombstoneMap, TombstoneRange};

impl TombstoneRange {
    fn from_selection(selection: &SeriesSelection) -> Result<Self> {
        let (start, end) = selection
            .normalized_time_range()?
            .unwrap_or((i64::MIN, i64::MAX));
        Ok(Self { start, end })
    }
}

#[derive(Clone, Copy)]
struct DeleteBridgeContext<'a> {
    storage: &'a ChunkStorage,
}

impl<'a> DeleteBridgeContext<'a> {
    fn publish_tombstone_delete(
        self,
        tombstone: TombstoneRange,
        matched_series_ids: &[SeriesId],
    ) -> Result<usize> {
        let (updated_series_ids, has_affected_rollups) =
            self.storage.with_rollup_run_lock(|| -> Result<_> {
                self.storage.with_visibility_write_stage(|| {
                    // Tombstone updates are read-modify-write operations. Recompute them after
                    // acquiring the visibility fence so concurrent deletes cannot overwrite one
                    // another with snapshots prepared before either publication committed.
                    let updated_tombstones =
                        self.storage
                            .tombstone_read_context()
                            .with_tombstones(|current| {
                                let mut updates =
                                    TombstoneMap::with_capacity(matched_series_ids.len());
                                for series_id in matched_series_ids {
                                    let mut ranges =
                                        current.get(series_id).cloned().unwrap_or_default();
                                    tombstone::merge_tombstone_range(&mut ranges, tombstone);
                                    if current.get(series_id) != Some(&ranges) {
                                        updates.insert(*series_id, ranges);
                                    }
                                }
                                updates
                            });
                    if updated_tombstones.is_empty() {
                        return Ok((Vec::new(), false));
                    }

                    let updated_series_ids = updated_tombstones.keys().copied().collect::<Vec<_>>();
                    let affected_policy_ids = self
                        .storage
                        .affected_rollup_policy_ids_for_series(&updated_series_ids);
                    if !affected_policy_ids.is_empty() {
                        self.storage.stage_pending_rollup_delete_invalidation(
                            tombstone,
                            &updated_series_ids,
                            &affected_policy_ids,
                        )?;
                    }

                    let tombstone_persist_result = self
                        .storage
                        .tombstone_publication_context()
                        .publish_tombstone_updates_locked(
                            self.storage,
                            self.storage.tombstone_index_context(),
                            updated_tombstones,
                        );
                    if let Err(tombstone_failure) = tombstone_persist_result {
                        let definitively_clean = tombstone_failure.is_definitively_clean();
                        let tombstone_error = tombstone_failure.into_tsink_error();
                        if !definitively_clean {
                            // A failed rollback or accounting reconciliation means some durable
                            // manifests may still expose the candidate tombstones. Retain the
                            // pending marker so same-process queries avoid stale rollups and
                            // startup can repair rollup state from the durable tombstone outcome.
                            tracing::warn!(
                                error = %tombstone_error,
                                "Delete retained pending rollup invalidation after indeterminate tombstone persistence failure"
                            );
                            return Err(tombstone_error);
                        }
                        let cancel_result = if affected_policy_ids.is_empty() {
                            Ok(())
                        } else {
                            self.storage.cancel_pending_rollup_delete_invalidation(
                                tombstone,
                                &updated_series_ids,
                                &affected_policy_ids,
                            )
                        };
                        return match cancel_result {
                            Ok(()) => Err(tombstone_error),
                            Err(cancel_error) => Err(TsinkError::Other(format!(
                                "delete tombstone persistence failed: {tombstone_error}; pending rollup invalidation rollback failed: {cancel_error}"
                            ))),
                        };
                    }
                    if !affected_policy_ids.is_empty() {
                        let finalize_result =
                            self.storage.finalize_pending_rollup_delete_invalidation(
                                tombstone,
                                &updated_series_ids,
                                &affected_policy_ids,
                            );
                        if finalize_result.is_err() {
                            // Tombstones are already durable and the pending delete marker is
                            // durable, so the delete has committed even if we could not immediately
                            // rewrite rollup state.
                        }
                    }
                    Ok((updated_series_ids, !affected_policy_ids.is_empty()))
                })
            })?;
        if updated_series_ids.is_empty() {
            return Ok(0);
        }

        #[cfg(test)]
        let live_series_result = self
            .storage
            .invoke_tombstone_post_commit_error_hook()
            .and_then(|()| {
                self.storage
                    .live_series_ids(updated_series_ids.iter().copied(), true)
            });
        #[cfg(not(test))]
        let live_series_result = self
            .storage
            .live_series_ids(updated_series_ids.iter().copied(), true);
        if let Err(err) = live_series_result {
            // Tombstones and query visibility have already committed. Metadata pruning is
            // repairable cleanup, so never turn its failure into a definitive delete rejection.
            tracing::warn!(
                error = %err,
                "Committed delete deferred live-series metadata pruning"
            );
        }
        self.storage.notify_compaction_thread();
        if has_affected_rollups {
            self.storage.notify_rollup_thread();
        }
        Ok(updated_series_ids.len())
    }
}

impl ChunkStorage {
    fn delete_bridge_context(&self) -> DeleteBridgeContext<'_> {
        DeleteBridgeContext { storage: self }
    }

    pub(super) fn delete_series_api(
        &self,
        selection: &SeriesSelection,
    ) -> Result<DeleteSeriesResult> {
        self.ensure_open()?;
        self.tombstone_index_context()
            .ensure_delete_tombstone_persistence_supported()?;
        let tombstone = TombstoneRange::from_selection(selection)?;
        let matched_series = self.select_series_impl(selection)?;
        if matched_series.is_empty() {
            return Ok(DeleteSeriesResult::default());
        }

        let series_ids = self.existing_series_ids_for_metric_series(matched_series);
        if series_ids.is_empty() {
            return Ok(DeleteSeriesResult::default());
        }

        let matched_series = saturating_u64_from_usize(series_ids.len());
        #[cfg(test)]
        self.invoke_tombstone_pre_publication_hook();
        let tombstones_applied = saturating_u64_from_usize(
            self.delete_bridge_context()
                .publish_tombstone_delete(tombstone, &series_ids)?,
        );

        Ok(DeleteSeriesResult {
            matched_series,
            tombstones_applied,
        })
    }

    pub(super) fn apply_tombstone_filter(&self, series_id: SeriesId, points: &mut Vec<DataPoint>) {
        if points.is_empty() {
            return;
        }

        self.tombstone_read_context()
            .with_series_tombstone_ranges(series_id, |ranges| {
                let Some(ranges) = ranges else {
                    return;
                };
                if ranges.is_empty() {
                    return;
                }
                points.retain(|point| !tombstone::timestamp_is_tombstoned(point.timestamp, ranges));
            });
    }

    #[cfg(test)]
    pub(super) fn invoke_tombstone_pre_publication_hook(&self) {
        let hook = self
            .persist_test_hooks
            .tombstone_pre_publication_hook
            .read()
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    pub(super) fn set_tombstone_pre_publication_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self
            .persist_test_hooks
            .tombstone_pre_publication_hook
            .write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(super) fn clear_tombstone_pre_publication_hook(&self) {
        self.persist_test_hooks
            .tombstone_pre_publication_hook
            .write()
            .take();
    }

    #[cfg(test)]
    pub(super) fn invoke_tombstone_post_swap_pre_visibility_hook(&self) {
        let hook = self
            .persist_test_hooks
            .tombstone_post_swap_pre_visibility_hook
            .read()
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    pub(super) fn set_tombstone_post_swap_pre_visibility_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self
            .persist_test_hooks
            .tombstone_post_swap_pre_visibility_hook
            .write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(super) fn clear_tombstone_post_swap_pre_visibility_hook(&self) {
        *self
            .persist_test_hooks
            .tombstone_post_swap_pre_visibility_hook
            .write() = None;
    }

    #[cfg(test)]
    pub(super) fn invoke_tombstone_post_commit_error_hook(&self) -> Result<()> {
        let hook = self
            .persist_test_hooks
            .tombstone_post_commit_error_hook
            .read()
            .clone();
        match hook {
            Some(hook) => hook(),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub(super) fn set_tombstone_post_commit_error_hook<F>(&self, hook: F)
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        *self
            .persist_test_hooks
            .tombstone_post_commit_error_hook
            .write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(super) fn clear_tombstone_post_commit_error_hook(&self) {
        self.persist_test_hooks
            .tombstone_post_commit_error_hook
            .write()
            .take();
    }
}
