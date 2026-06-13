use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::db::TsinkDB;
use crate::enums::{
    URemoteSegmentCachePolicy, UStorageRuntimeMode, UTimestampPrecision, UWalReplayMode,
    UWalSyncMode,
};
use crate::error::{Result, TsinkUniFFIError};

#[derive(uniffi::Object)]
pub struct TsinkStorageBuilder {
    inner: Mutex<Option<tsink::StorageBuilder>>,
}

impl TsinkStorageBuilder {
    fn with_builder<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(tsink::StorageBuilder) -> tsink::StorageBuilder,
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
            inner: Mutex::new(Some(tsink::StorageBuilder::new())),
        })
    }

    pub fn with_data_path(&self, path: String) -> Result<()> {
        self.with_builder(|b| b.with_data_path(path))
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

    pub fn with_wal_enabled(&self, enabled: bool) -> Result<()> {
        self.with_builder(|b| b.with_wal_enabled(enabled))
    }

    pub fn with_wal_size_limit(&self, bytes: u64) -> Result<()> {
        self.with_builder(|b| b.with_wal_size_limit(bytes as usize))
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

    pub fn with_metadata_shard_count(&self, shard_count: u32) -> Result<()> {
        self.with_builder(|b| b.with_metadata_shard_count(shard_count))
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
}

#[uniffi::export]
pub fn restore_from_snapshot(snapshot_path: String, data_path: String) -> Result<()> {
    tsink::StorageBuilder::restore_from_snapshot(
        std::path::Path::new(snapshot_path.as_str()),
        std::path::Path::new(data_path.as_str()),
    )
    .map_err(TsinkUniFFIError::from)
}
