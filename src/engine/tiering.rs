#[path = "tiering/catalog.rs"]
mod catalog;
#[path = "tiering/discovery.rs"]
mod discovery;
#[path = "tiering/inventory.rs"]
mod inventory;
#[path = "tiering/layout.rs"]
mod layout;
#[path = "tiering/policy.rs"]
mod policy;

pub(super) use catalog::{
    load_segment_catalog, persist_segment_catalog, shared_segment_catalog_path,
    SEGMENT_CATALOG_FILE_NAME,
};
#[allow(unused_imports)]
pub(super) use discovery::{
    build_segment_inventory_fail_on_invalid, build_segment_inventory_runtime_strict,
    build_segment_inventory_startup_recoverable, StartupRecoveredSegmentInventory,
};
pub(super) use inventory::{
    PersistedSegmentTier, SegmentInventory, SegmentInventoryEntry, SegmentLaneFamily,
};
pub(super) use layout::{destination_segment_root, move_segment_to_tier, SegmentPathResolver};
#[allow(unused_imports)]
pub(super) use policy::{
    PostFlushMaintenancePolicyPlan, RetentionTierPolicy, SegmentTierMoveAction,
};

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::super::config::TieredStorageConfig;
    use super::*;
    use crate::engine::segment::{SegmentManifest, WalHighWatermark};

    fn manifest(segment_id: u64, min_ts: Option<i64>, max_ts: Option<i64>) -> SegmentManifest {
        SegmentManifest {
            segment_id,
            level: 0,
            chunk_count: 1,
            point_count: 1,
            series_count: 1,
            min_ts,
            max_ts,
            wal_highwater: WalHighWatermark::default(),
        }
    }

    fn entry(
        segment_id: u64,
        tier: PersistedSegmentTier,
        min_ts: Option<i64>,
        max_ts: Option<i64>,
    ) -> SegmentInventoryEntry {
        SegmentInventoryEntry {
            lane: SegmentLaneFamily::Numeric,
            tier,
            root: PathBuf::from(format!("/segments/{segment_id}")),
            manifest: manifest(segment_id, min_ts, max_ts),
        }
    }

    #[test]
    fn retention_tier_policy_reuses_cutoffs_for_query_planning() {
        let policy = RetentionTierPolicy::new(
            100,
            Some(200),
            Some(&TieredStorageConfig {
                object_store_root: PathBuf::from("/object-store"),
                segment_catalog_path: None,
                mirror_hot_segments: false,
                hot_retention_window: 10,
                warm_retention_window: 50,
            }),
        );

        let hot_only = policy.query_plan(195, 200);
        assert!(hot_only.is_hot_only());

        let includes_warm = policy.query_plan(180, 200);
        assert!(includes_warm.includes_warm());
        assert!(!includes_warm.includes_cold());

        let includes_cold = policy.query_plan(120, 200);
        assert!(includes_cold.includes_warm());
        assert!(includes_cold.includes_cold());
    }

    #[test]
    fn post_flush_maintenance_plan_separates_rewrite_move_and_expire_actions() {
        let policy = RetentionTierPolicy::new(
            100,
            Some(200),
            Some(&TieredStorageConfig {
                object_store_root: PathBuf::from("/object-store"),
                segment_catalog_path: None,
                mirror_hot_segments: false,
                hot_retention_window: 10,
                warm_retention_window: 50,
            }),
        );
        let inventory = SegmentInventory::from_entries(vec![
            entry(1, PersistedSegmentTier::Hot, Some(80), Some(90)),
            entry(2, PersistedSegmentTier::Hot, Some(90), Some(120)),
            entry(3, PersistedSegmentTier::Hot, Some(150), Some(160)),
            entry(4, PersistedSegmentTier::Warm, Some(120), Some(130)),
            entry(5, PersistedSegmentTier::Hot, Some(195), Some(199)),
        ]);

        let plan = policy.post_flush_maintenance_plan(&inventory);

        assert_eq!(plan.expired_actions.len(), 1);
        assert_eq!(plan.expired_actions[0].manifest.segment_id, 1);

        assert_eq!(plan.rewrite_actions.len(), 1);
        assert_eq!(plan.rewrite_actions[0].manifest.segment_id, 2);

        assert_eq!(plan.move_actions.len(), 2);
        assert_eq!(plan.move_actions[0].entry.manifest.segment_id, 3);
        assert_eq!(plan.move_actions[0].target_tier, PersistedSegmentTier::Warm);
        assert_eq!(plan.move_actions[1].entry.manifest.segment_id, 4);
        assert_eq!(plan.move_actions[1].target_tier, PersistedSegmentTier::Cold);
    }
}
