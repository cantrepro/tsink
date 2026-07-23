use crate::engine::query::TieredQueryPlan;
use crate::engine::segment::SegmentManifest;

use super::super::config::TieredStorageConfig;
use super::{PersistedSegmentTier, SegmentInventory, SegmentInventoryEntry};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::storage_engine) struct RetentionTierPolicy {
    retention_cutoff: i64,
    hot_cutoff: Option<i64>,
    warm_cutoff: Option<i64>,
    tier_moves_enabled: bool,
}

impl RetentionTierPolicy {
    pub(in crate::engine::storage_engine) fn new(
        retention_cutoff: i64,
        recency_reference: Option<i64>,
        tiered_storage: Option<&TieredStorageConfig>,
    ) -> Self {
        let hot_cutoff = recency_reference
            .zip(tiered_storage)
            .map(|(reference, config)| reference.saturating_sub(config.hot_retention_window));
        let warm_cutoff = recency_reference
            .zip(tiered_storage)
            .map(|(reference, config)| reference.saturating_sub(config.warm_retention_window));
        Self {
            retention_cutoff,
            hot_cutoff,
            warm_cutoff,
            tier_moves_enabled: tiered_storage.is_some(),
        }
    }

    pub(in crate::engine::storage_engine) fn retention_cutoff(self) -> i64 {
        self.retention_cutoff
    }

    pub(in crate::engine::storage_engine) fn query_plan(
        self,
        start: i64,
        end: i64,
    ) -> TieredQueryPlan {
        TieredQueryPlan::from_cutoffs(start, end, self.hot_cutoff, self.warm_cutoff)
    }

    pub(in crate::engine::storage_engine) fn desired_tier_for_manifest(
        self,
        manifest: &SegmentManifest,
    ) -> Option<PersistedSegmentTier> {
        let Some(max_ts) = manifest.max_ts else {
            return Some(PersistedSegmentTier::Hot);
        };
        if max_ts < self.retention_cutoff {
            return None;
        }
        if self.warm_cutoff.is_some_and(|cutoff| max_ts < cutoff) {
            Some(PersistedSegmentTier::Cold)
        } else if self.hot_cutoff.is_some_and(|cutoff| max_ts < cutoff) {
            Some(PersistedSegmentTier::Warm)
        } else {
            Some(PersistedSegmentTier::Hot)
        }
    }

    pub(in crate::engine::storage_engine) fn requires_retention_rewrite(
        self,
        manifest: &SegmentManifest,
    ) -> bool {
        matches!(
            (manifest.min_ts, manifest.max_ts),
            (Some(min_ts), Some(max_ts))
                if min_ts < self.retention_cutoff && max_ts >= self.retention_cutoff
        )
    }

    pub(in crate::engine::storage_engine) fn post_flush_maintenance_plan(
        self,
        inventory: &SegmentInventory,
    ) -> PostFlushMaintenancePolicyPlan {
        let mut plan = PostFlushMaintenancePolicyPlan::default();

        for entry in inventory.entries() {
            if let Some(action) = self.post_flush_maintenance_action(entry) {
                plan.push(entry, action);
            }
        }

        plan
    }

    pub(in crate::engine::storage_engine) fn post_flush_maintenance_action(
        self,
        entry: &SegmentInventoryEntry,
    ) -> Option<PostFlushMaintenanceAction> {
        let Some(desired_tier) = self.desired_tier_for_manifest(&entry.manifest) else {
            return Some(PostFlushMaintenanceAction::Expire);
        };

        if self.requires_retention_rewrite(&entry.manifest) {
            return Some(PostFlushMaintenanceAction::Rewrite);
        }

        if !self.tier_moves_enabled
            || desired_tier == PersistedSegmentTier::Hot
            || desired_tier <= entry.tier
        {
            return None;
        }

        Some(PostFlushMaintenanceAction::Move(desired_tier))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::storage_engine) enum PostFlushMaintenanceAction {
    Rewrite,
    Move(PersistedSegmentTier),
    Expire,
}

#[derive(Debug, Default, Clone)]
pub(in crate::engine::storage_engine) struct PostFlushMaintenancePolicyPlan {
    pub(in crate::engine::storage_engine) rewrite_actions: Vec<SegmentInventoryEntry>,
    pub(in crate::engine::storage_engine) move_actions: Vec<SegmentTierMoveAction>,
    pub(in crate::engine::storage_engine) expired_actions: Vec<SegmentInventoryEntry>,
}

impl PostFlushMaintenancePolicyPlan {
    pub(in crate::engine::storage_engine) fn is_empty(&self) -> bool {
        self.rewrite_actions.is_empty()
            && self.move_actions.is_empty()
            && self.expired_actions.is_empty()
    }

    pub(in crate::engine::storage_engine) fn push(
        &mut self,
        entry: &SegmentInventoryEntry,
        action: PostFlushMaintenanceAction,
    ) {
        match action {
            PostFlushMaintenanceAction::Rewrite => self.rewrite_actions.push(entry.clone()),
            PostFlushMaintenanceAction::Move(target_tier) => {
                self.move_actions.push(SegmentTierMoveAction {
                    entry: entry.clone(),
                    target_tier,
                });
            }
            PostFlushMaintenanceAction::Expire => self.expired_actions.push(entry.clone()),
        }
    }

    pub(in crate::engine::storage_engine) fn action_count(&self) -> usize {
        self.rewrite_actions
            .len()
            .saturating_add(self.move_actions.len())
            .saturating_add(self.expired_actions.len())
    }

    pub(in crate::engine::storage_engine) fn source_entries(&self) -> Vec<SegmentInventoryEntry> {
        let mut entries = Vec::with_capacity(self.action_count());
        entries.extend(self.rewrite_actions.iter().cloned());
        entries.extend(self.move_actions.iter().map(|action| action.entry.clone()));
        entries.extend(self.expired_actions.iter().cloned());
        entries
    }
}

#[derive(Debug, Clone)]
pub(in crate::engine::storage_engine) struct SegmentTierMoveAction {
    pub(in crate::engine::storage_engine) entry: SegmentInventoryEntry,
    pub(in crate::engine::storage_engine) target_tier: PersistedSegmentTier,
}
