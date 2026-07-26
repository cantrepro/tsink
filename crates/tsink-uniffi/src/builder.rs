use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::db::TsinkDB;
use crate::enums::{
    URemoteSegmentCachePolicy, UResourceProfile, UStorageRuntimeMode, UTimestampPrecision,
    UWalReplayMode, UWalSyncMode,
};
use crate::error::{Result, TsinkUniFFIError};
use crate::types::{UQueryBudgetLimits, UWriteBatchLimits};

fn checked_usize(field: &str, value: u64) -> Result<usize> {
    usize::try_from(value).map_err(|_| TsinkUniFFIError::InvalidInput {
        msg: format!("{field} value {value} does not fit this platform's usize"),
    })
}

#[derive(uniffi::Object)]
pub struct TsinkStorageBuilder {
    inner: Mutex<Option<tsink_core::StorageBuilder>>,
}

impl TsinkStorageBuilder {
    fn with_builder<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(tsink_core::StorageBuilder) -> tsink_core::StorageBuilder,
    {
        let mut guard = self.inner.lock();
        let builder = guard.take().ok_or(TsinkUniFFIError::InvalidInput {
            msg: "Builder already consumed by build()".into(),
        })?;
        *guard = Some(f(builder));
        Ok(())
    }
}

#[uniffi::export]
impl TsinkStorageBuilder {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Some(tsink_core::StorageBuilder::new())),
        })
    }

    pub fn with_data_path(&self, path: String) -> Result<()> {
        self.with_builder(|b| b.with_data_path(path))
    }

    pub fn with_resource_profile(&self, profile: UResourceProfile) -> Result<()> {
        self.with_builder(|b| b.with_resource_profile(profile.into()))
    }

    pub fn with_object_store_path(&self, path: String) -> Result<()> {
        self.with_builder(|b| b.with_object_store_path(path))
    }

    pub fn with_retention(&self, duration: Duration) -> Result<()> {
        self.with_builder(|b| b.with_retention(duration))
    }

    pub fn with_retention_enforced(&self, enforced: bool) -> Result<()> {
        self.with_builder(|b| b.with_retention_enforced(enforced))
    }

    pub fn with_tiered_retention_policy(
        &self,
        hot_retention: Duration,
        warm_retention: Duration,
    ) -> Result<()> {
        self.with_builder(|b| b.with_tiered_retention_policy(hot_retention, warm_retention))
    }

    pub fn with_runtime_mode(&self, mode: UStorageRuntimeMode) -> Result<()> {
        self.with_builder(|b| b.with_runtime_mode(mode.into()))
    }

    pub fn with_remote_segment_cache_policy(
        &self,
        policy: URemoteSegmentCachePolicy,
    ) -> Result<()> {
        self.with_builder(|b| b.with_remote_segment_cache_policy(policy.into()))
    }

    pub fn with_remote_segment_refresh_interval(&self, interval: Duration) -> Result<()> {
        self.with_builder(|b| b.with_remote_segment_refresh_interval(interval))
    }

    pub fn with_mirror_hot_segments_to_object_store(&self, enabled: bool) -> Result<()> {
        self.with_builder(|b| b.with_mirror_hot_segments_to_object_store(enabled))
    }

    pub fn with_timestamp_precision(&self, precision: UTimestampPrecision) -> Result<()> {
        self.with_builder(|b| b.with_timestamp_precision(precision.into()))
    }

    pub fn with_chunk_points(&self, points: u64) -> Result<()> {
        self.with_builder(|b| b.with_chunk_points(points as usize))
    }

    pub fn with_max_writers(&self, max_writers: u64) -> Result<()> {
        self.with_builder(|b| b.with_max_writers(max_writers as usize))
    }

    pub fn with_write_timeout(&self, timeout: Duration) -> Result<()> {
        self.with_builder(|b| b.with_write_timeout(timeout))
    }

    pub fn with_partition_duration(&self, duration: Duration) -> Result<()> {
        self.with_builder(|b| b.with_partition_duration(duration))
    }

    pub fn with_max_active_partition_heads_per_series(&self, max_heads: u64) -> Result<()> {
        self.with_builder(|b| b.with_max_active_partition_heads_per_series(max_heads as usize))
    }

    pub fn with_memory_limit(&self, bytes: u64) -> Result<()> {
        self.with_builder(|b| b.with_memory_limit(bytes as usize))
    }

    pub fn with_cardinality_limit(&self, series: u64) -> Result<()> {
        self.with_builder(|b| b.with_cardinality_limit(series as usize))
    }

    pub fn with_max_labels_per_series(&self, labels: u64) -> Result<()> {
        let labels = checked_usize("max_labels_per_series", labels)?;
        self.with_builder(|b| b.with_max_labels_per_series(labels))
    }

    pub fn with_max_series_identity_bytes(&self, bytes: u64) -> Result<()> {
        let bytes = checked_usize("max_series_identity_bytes", bytes)?;
        self.with_builder(|b| b.with_max_series_identity_bytes(bytes))
    }

    pub fn with_series_creation_rate_limit(
        &self,
        max_new_series: u64,
        window: Duration,
    ) -> Result<()> {
        let max_new_series = checked_usize("max_new_series_per_window", max_new_series)?;
        self.with_builder(|b| b.with_series_creation_rate_limit(max_new_series, window))
    }

    pub fn with_write_batch_limits(&self, limits: UWriteBatchLimits) -> Result<()> {
        let max_rows = limits
            .max_rows
            .map(|value| checked_usize("max_write_batch_rows", value))
            .transpose()?;
        let max_modeled_input_bytes = limits
            .max_modeled_input_bytes
            .map(|value| checked_usize("max_write_batch_input_bytes", value))
            .transpose()?;
        self.with_builder(|b| {
            b.with_write_batch_limits(tsink_core::WriteBatchLimits {
                max_rows,
                max_modeled_input_bytes,
            })
        })
    }

    pub fn with_wal_enabled(&self, enabled: bool) -> Result<()> {
        self.with_builder(|b| b.with_wal_enabled(enabled))
    }

    pub fn with_wal_size_limit(&self, bytes: u64) -> Result<()> {
        self.with_builder(|b| b.with_wal_size_limit(bytes as usize))
    }

    pub fn with_local_disk_limit(&self, bytes: u64) -> Result<()> {
        self.with_builder(|b| b.with_local_disk_limit(bytes))
    }

    pub fn with_filesystem_free_headroom(&self, bytes: u64) -> Result<()> {
        self.with_builder(|b| b.with_filesystem_free_headroom(bytes))
    }

    pub fn with_maintenance_temp_reserve(&self, bytes: u64) -> Result<()> {
        self.with_builder(|b| b.with_maintenance_temp_reserve(bytes))
    }

    pub fn with_wal_buffer_size(&self, size: u64) -> Result<()> {
        self.with_builder(|b| b.with_wal_buffer_size(size as usize))
    }

    pub fn with_wal_sync_mode(&self, mode: UWalSyncMode) -> Result<()> {
        self.with_builder(|b| b.with_wal_sync_mode(mode.into()))
    }

    pub fn with_wal_replay_mode(&self, mode: UWalReplayMode) -> Result<()> {
        self.with_builder(|b| b.with_wal_replay_mode(mode.into()))
    }

    pub fn with_background_fail_fast(&self, enabled: bool) -> Result<()> {
        self.with_builder(|b| b.with_background_fail_fast(enabled))
    }

    pub fn with_maintenance_max_items_per_pass(&self, max_items: u64) -> Result<()> {
        let max_items = checked_usize("maintenance_max_items_per_pass", max_items)?;
        self.with_builder(|b| b.with_maintenance_max_items_per_pass(max_items))
    }

    pub fn with_maintenance_max_bytes_per_pass(&self, max_bytes: u64) -> Result<()> {
        self.with_builder(|b| b.with_maintenance_max_bytes_per_pass(max_bytes))
    }

    pub fn clear_resource_limit_overrides(&self) -> Result<()> {
        self.with_builder(tsink_core::StorageBuilder::clear_resource_limit_overrides)
    }

    pub fn with_metadata_shard_count(&self, shard_count: u32) -> Result<()> {
        self.with_builder(|b| b.with_metadata_shard_count(shard_count))
    }

    pub fn with_query_budget_limits(&self, limits: UQueryBudgetLimits) -> Result<()> {
        self.with_builder(|b| b.with_query_budget_limits(limits.into()))
    }

    pub fn build(&self) -> Result<Arc<TsinkDB>> {
        let builder = self
            .inner
            .lock()
            .take()
            .ok_or(TsinkUniFFIError::InvalidInput {
                msg: "Builder already consumed by build()".into(),
            })?;

        let storage = builder.build().map_err(TsinkUniFFIError::from)?;
        Ok(Arc::new(TsinkDB::from_storage(storage)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_consume_once() {
        let builder = TsinkStorageBuilder::new();
        let result = builder.build();
        assert!(result.is_ok());
        let result = builder.build();
        assert!(result.is_err());
        match result.unwrap_err() {
            TsinkUniFFIError::InvalidInput { msg } => {
                assert!(msg.contains("already consumed"));
            }
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_builder_setter_after_consume() {
        let builder = TsinkStorageBuilder::new();
        let _ = builder.build();

        let result = builder.with_wal_enabled(false);
        assert!(result.is_err());
    }

    #[test]
    fn resource_profile_and_override_provenance_are_forwarded() {
        let builder = TsinkStorageBuilder::new();
        builder.with_memory_limit(123_456).unwrap();
        builder
            .with_resource_profile(UResourceProfile::Edge)
            .unwrap();

        let db = builder.build().unwrap();
        let snapshot = db.resource_configuration_snapshot();
        assert!(matches!(
            snapshot.selected_profile,
            crate::enums::UResourceProfileName::Edge
        ));
        assert_eq!(
            snapshot.resolved_limits.storage.accounted_memory_bytes,
            Some(123_456)
        );
        assert_eq!(snapshot.overrides, vec!["accounted_memory".to_string()]);
        assert_eq!(
            db.observability_snapshot()
                .resource_configuration
                .schema_version,
            snapshot.schema_version
        );
        db.close().unwrap();
    }

    #[test]
    fn cardinality_controls_are_forwarded_and_observable() {
        let builder = TsinkStorageBuilder::new();
        builder.with_max_labels_per_series(7).unwrap();
        builder.with_max_series_identity_bytes(2_048).unwrap();
        builder
            .with_series_creation_rate_limit(11, Duration::from_secs(2))
            .unwrap();

        let db = builder.build().unwrap();
        let limits = db.effective_storage_limits();
        assert_eq!(limits.max_labels_per_series, Some(7));
        assert_eq!(limits.max_series_identity_bytes, Some(2_048));
        assert_eq!(limits.max_new_series_per_window, Some(11));
        assert_eq!(limits.new_series_window_nanos, Some(2_000_000_000));
        let cardinality = db.observability_snapshot().cardinality;
        assert_eq!(cardinality.series_count, 0);
        assert_eq!(cardinality.pending_new_series, 0);

        db.close().unwrap();
    }

    #[test]
    fn write_batch_controls_are_forwarded_and_observable() {
        let builder = TsinkStorageBuilder::new();
        builder
            .with_write_batch_limits(UWriteBatchLimits {
                max_rows: Some(23),
                max_modeled_input_bytes: Some(8_192),
            })
            .unwrap();

        let db = builder.build().unwrap();
        let limits = db.effective_storage_limits();
        assert_eq!(limits.max_write_batch_rows, Some(23));
        assert_eq!(limits.max_write_batch_input_bytes, Some(8_192));
        let memory = db.observability_snapshot().memory;
        assert_eq!(memory.wal_writer_buffer_bytes, 0);
        assert_eq!(memory.wal_series_definition_cache_bytes, 0);
        assert_eq!(memory.remote_catalog_staging_bytes, 0);
        assert_eq!(memory.write_transient_bytes, 0);
        assert!(memory.write_transient_bytes_estimated);

        db.close().unwrap();
    }

    #[test]
    fn query_budget_controls_are_forwarded_and_observable() {
        let builder = TsinkStorageBuilder::new();
        builder
            .with_query_budget_limits(UQueryBudgetLimits {
                max_concurrent_queries: Some(2),
                max_shared_memory_bytes: Some(8_192),
                per_query: crate::types::UQueryWorkLimits {
                    max_series_matched: Some(3),
                    max_samples_scanned: Some(4),
                    max_samples_returned: Some(5),
                    max_returned_bytes: Some(6_144),
                    max_pattern_expansion: Some(7),
                    max_steps: Some(8),
                    max_intermediate_vector_size: Some(9),
                    max_memory_bytes: Some(4_096),
                    max_wall_time_nanos: Some(10_000_000),
                },
            })
            .unwrap();

        let db = builder.build().unwrap();
        let budget = db.observability_snapshot().query_budget;
        assert_eq!(budget.limits.max_concurrent_queries, Some(2));
        assert_eq!(budget.limits.max_shared_memory_bytes, Some(8_192));
        assert_eq!(budget.limits.per_query.max_steps, Some(8));
        assert_eq!(budget.limits.per_query.max_memory_bytes, Some(4_096));
        assert_eq!(
            budget.limits.per_query.max_wall_time_nanos,
            Some(10_000_000)
        );
        assert_eq!(budget.active_queries, 0);

        db.close().unwrap();
    }
}

#[uniffi::export]
pub fn restore_from_snapshot(snapshot_path: String, data_path: String) -> Result<()> {
    tsink_core::StorageBuilder::restore_from_snapshot(
        std::path::Path::new(snapshot_path.as_str()),
        std::path::Path::new(data_path.as_str()),
    )
    .map_err(TsinkUniFFIError::from)
}
