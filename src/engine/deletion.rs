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
        updated_tombstones: TombstoneMap,
    ) -> Result<()> {
        let updated_series_ids = updated_tombstones.keys().copied().collect::<Vec<_>>();
        let has_affected_rollups = self.storage.with_rollup_run_lock(|| -> Result<bool> {
            let affected_policy_ids = self
                .storage
                .affected_rollup_policy_ids_for_series(&updated_series_ids);
            self.storage.with_visibility_write_stage(|| {
                if !affected_policy_ids.is_empty() {
                    self.storage.stage_pending_rollup_delete_invalidation(
                        tombstone,
                        &updated_series_ids,
                        &affected_policy_ids,
                    )?;
                }

                self.storage
                    .tombstone_publication_context()
                    .publish_tombstone_updates_locked(
                        self.storage,
                        self.storage.tombstone_index_context(),
                        updated_tombstones,
                    )?;
                if !affected_policy_ids.is_empty() {
                    let finalize_result = self.storage.finalize_pending_rollup_delete_invalidation(
                        tombstone,
                        &updated_series_ids,
                        &affected_policy_ids,
                    );
                    if finalize_result.is_err() {
                        // Tombstones are already durable and the pending delete marker is durable,
                        // so the delete has committed even if we could not immediately rewrite
                        // rollup state.
                    }
                }
                Ok(!affected_policy_ids.is_empty())
            })
        })?;
        let _ = self
            .storage
            .live_series_ids(updated_series_ids.iter().copied(), true)?;
        self.storage.notify_compaction_thread();
        if has_affected_rollups {
            self.storage.notify_rollup_thread();
        }
        Ok(())
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

        let updated_tombstones = self.tombstone_read_context().with_tombstones(|current| {
            let mut updates = TombstoneMap::with_capacity(series_ids.len());
            for series_id in &series_ids {
                let mut ranges = current.get(series_id).cloned().unwrap_or_default();
                tombstone::merge_tombstone_range(&mut ranges, tombstone);
                if current.get(series_id) != Some(&ranges) {
                    updates.insert(*series_id, ranges);
                }
            }
            updates
        });
        let matched_series = saturating_u64_from_usize(series_ids.len());
        let tombstones_applied = saturating_u64_from_usize(updated_tombstones.len());
        if updated_tombstones.is_empty() {
            return Ok(DeleteSeriesResult {
                matched_series,
                tombstones_applied,
            });
        }

        self.delete_bridge_context()
            .publish_tombstone_delete(tombstone, updated_tombstones)?;

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
}
