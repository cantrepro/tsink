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

#[cfg(test)]
pub(super) use catalog::persist_segment_catalog;
pub(super) use catalog::{
    load_segment_catalog, persist_segment_catalog_budgeted, shared_segment_catalog_path,
    SEGMENT_CATALOG_FILE_NAME,
};
#[allow(unused_imports)]
pub(super) use discovery::{
    build_segment_inventory_fail_on_invalid, build_segment_inventory_runtime_strict,
    build_segment_inventory_startup_recoverable, preflight_segment_inventory_startup_memory,
    StartupRecoveredSegmentInventory,
};
pub(super) use inventory::{
    PersistedSegmentTier, SegmentInventory, SegmentInventoryEntry, SegmentLaneFamily,
};
pub(super) use layout::{destination_segment_root, move_segment_to_tier, SegmentPathResolver};
#[allow(unused_imports)]
pub(super) use policy::{
    PostFlushMaintenanceAction, PostFlushMaintenancePolicyPlan, RetentionTierPolicy,
    SegmentTierMoveAction,
};

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::super::config::TieredStorageConfig;
    use super::*;
    use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
    use crate::engine::encoder::Encoder;
    use crate::engine::segment::{SegmentManifest, SegmentWriter, WalHighWatermark};
    use crate::engine::series::{SeriesRegistry, SeriesValueFamily};
    use crate::{Label, TsinkError, Value};

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

    #[test]
    fn startup_segment_admission_is_aggregate_across_numeric_and_blob_lanes() {
        let temp = TempDir::new().unwrap();
        let numeric = temp.path().join("numeric");
        let blob = temp.path().join("blob");
        write_test_segment(&numeric, 1, "numeric", Value::F64(1.0));
        write_test_segment(&blob, 2, "blob", Value::String("payload".to_string()));

        let measure = |numeric_path: Option<&std::path::Path>,
                       blob_path: Option<&std::path::Path>| {
            let mut reserved = 0usize;
            preflight_segment_inventory_startup_memory(
                numeric_path,
                blob_path,
                None,
                |additional| {
                    reserved = reserved.saturating_add(additional);
                    Ok(())
                },
            )
            .unwrap();
            reserved
        };
        let numeric_only = measure(Some(&numeric), None);
        let blob_only = measure(None, Some(&blob));
        let aggregate = measure(Some(&numeric), Some(&blob));
        assert!(numeric_only > 0);
        assert!(blob_only > 0);
        assert_eq!(aggregate, numeric_only.saturating_add(blob_only));

        let mut exact_reserved = 0usize;
        preflight_segment_inventory_startup_memory(
            Some(&numeric),
            Some(&blob),
            None,
            |additional| {
                let required = exact_reserved.saturating_add(additional);
                if required > aggregate {
                    return Err(TsinkError::MemoryBudgetExceeded {
                        budget: aggregate,
                        required,
                    });
                }
                exact_reserved = required;
                Ok(())
            },
        )
        .expect("the exact two-lane threshold must succeed");
        assert_eq!(exact_reserved, aggregate);

        let one_less = aggregate - 1;
        let mut rejected_reserved = 0usize;
        let err = preflight_segment_inventory_startup_memory(
            Some(&numeric),
            Some(&blob),
            None,
            |additional| {
                let required = rejected_reserved.saturating_add(additional);
                if required > one_less {
                    return Err(TsinkError::MemoryBudgetExceeded {
                        budget: one_less,
                        required,
                    });
                }
                rejected_reserved = required;
                Ok(())
            },
        )
        .expect_err("one byte below the aggregate two-lane threshold must fail");
        assert!(matches!(
            err,
            TsinkError::MemoryBudgetExceeded { budget, required }
                if budget == one_less && required == aggregate
        ));
    }

    fn write_test_segment(base: &std::path::Path, segment_id: u64, metric: &str, value: Value) {
        let registry = SeriesRegistry::new();
        let resolution = registry
            .resolve_or_insert(metric, &[Label::new("host", metric)])
            .unwrap();
        let lane = match &value {
            Value::F64(_) | Value::I64(_) | Value::U64(_) | Value::Bool(_) => ValueLane::Numeric,
            Value::Bytes(_) | Value::String(_) | Value::Histogram(_) => ValueLane::Blob,
        };
        let family = SeriesValueFamily::from_value(&value, lane).unwrap();
        let points = vec![ChunkPoint { ts: 1, value }];
        let encoded = Encoder::encode_chunk_points(&points, lane).unwrap();
        let chunk = Chunk {
            header: ChunkHeader {
                series_id: resolution.series_id,
                lane,
                value_family: Some(family),
                point_count: 1,
                min_ts: 1,
                max_ts: 1,
                ts_codec: encoded.ts_codec,
                value_codec: encoded.value_codec,
            },
            points,
            encoded_payload: encoded.payload,
            wal_lowwater: WalHighWatermark::default(),
            wal_highwater: WalHighWatermark::default(),
        };
        SegmentWriter::new(base, 0, segment_id)
            .unwrap()
            .write_segment(
                &registry,
                &HashMap::from([(resolution.series_id, vec![chunk])]),
            )
            .unwrap();
        assert_eq!(
            lane,
            if metric == "numeric" {
                ValueLane::Numeric
            } else {
                ValueLane::Blob
            }
        );
    }
}
