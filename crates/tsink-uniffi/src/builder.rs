use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::db::TsinkDB;
use crate::enums::{UTimestampPrecision, UWalSyncMode};
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

    pub fn with_retention(&self, duration: Duration) -> Result<()> {
        self.with_builder(|b| b.with_retention(duration))
    }

    pub fn with_retention_enforced(&self, enforced: bool) -> Result<()> {
        self.with_builder(|b| b.with_retention_enforced(enforced))
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
