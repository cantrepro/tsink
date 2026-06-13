use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, MutexGuard, RwLock};
use rayon::prelude::*;

use crate::concurrency::{Semaphore, SemaphoreGuard};
use crate::engine::chunk::{self, Chunk, ChunkBuilder, ChunkPoint, ValueLane};
use crate::engine::compactor::Compactor;
use crate::engine::encoder::Encoder;
use crate::engine::segment::{
    collect_expired_segment_dirs, load_segment_indexes, SegmentWriter, WalHighWatermark,
};
use crate::engine::series::{SeriesId, SeriesRegistry, SeriesResolution};
use crate::engine::wal::{FramedWal, ReplayFrame, SamplesBatchFrame, SeriesDefinitionFrame};
use crate::mmap::PlatformMmap;
use crate::storage::{SeriesSelection, TimestampPrecision};
use crate::validation::{validate_labels, validate_metric};
use crate::{
    DataPoint, Label, MetricSeries, QueryOptions, Result, Row, Storage, StorageBuilder,
    StorageObservabilitySnapshot, TsinkError, Value,
};

#[path = "bootstrap.rs"]
mod bootstrap;
#[path = "config.rs"]
mod config;
#[path = "core_impl.rs"]
mod core_impl;
#[path = "ingest.rs"]
mod ingest;
#[path = "lifecycle.rs"]
mod lifecycle;
#[path = "maintenance.rs"]
mod maintenance;
#[path = "metrics.rs"]
mod metrics;
#[path = "observability.rs"]
mod observability;
#[path = "process_lock.rs"]
mod process_lock;
#[path = "query_exec.rs"]
mod query_exec;
#[path = "runtime.rs"]
mod runtime;
#[path = "state.rs"]
mod state;

use config::ChunkStorageOptions;
use metrics::StorageObservabilityCounters;
use process_lock::DataPathProcessLock;
use state::{
    ActiveSeriesState, PersistedChunkRef, PersistedIndexState, SealedChunkKey, BLOB_LANE_ROOT,
    NUMERIC_LANE_ROOT, SERIES_INDEX_FILE_NAME, WAL_DIR_NAME,
};

const STORAGE_OPEN: u8 = 0;
const STORAGE_CLOSING: u8 = 1;
const STORAGE_CLOSED: u8 = 2;
const DEFAULT_RETENTION: Duration = Duration::from_secs(14 * 24 * 3600);
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_PARTITION_DURATION: Duration = Duration::from_secs(3600);
const DEFAULT_COMPACTION_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(250);
const DEFAULT_ADMISSION_POLL_INTERVAL: Duration = Duration::from_millis(10);
const CLOSE_COMPACTION_MAX_PASSES: usize = 128;
const IN_MEMORY_SHARD_COUNT: usize = 64;
const REGISTRY_TXN_SHARD_COUNT: usize = IN_MEMORY_SHARD_COUNT;

fn saturating_u64_from_usize(value: usize) -> u64 {
    value.min(u64::MAX as usize) as u64
}

fn elapsed_nanos_u64(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

pub struct ChunkStorage {
    registry: RwLock<SeriesRegistry>,
    materialized_series: RwLock<BTreeSet<SeriesId>>,
    registry_write_txn_shards: [Mutex<()>; REGISTRY_TXN_SHARD_COUNT],
    active_builders: [RwLock<HashMap<SeriesId, ActiveSeriesState>>; IN_MEMORY_SHARD_COUNT],
    sealed_chunks:
        [RwLock<HashMap<SeriesId, BTreeMap<SealedChunkKey, Chunk>>>; IN_MEMORY_SHARD_COUNT],
    persisted_index: RwLock<PersistedIndexState>,
    persisted_chunk_watermarks: RwLock<HashMap<SeriesId, u64>>,
    next_chunk_sequence: AtomicU64,
    chunk_point_cap: usize,
    numeric_lane_path: Option<PathBuf>,
    blob_lane_path: Option<PathBuf>,
    series_index_path: Option<PathBuf>,
    next_segment_id: Arc<AtomicU64>,
    numeric_compactor: Option<Compactor>,
    blob_compactor: Option<Compactor>,
    wal: Option<FramedWal>,
    retention_window: i64,
    retention_enforced: bool,
    partition_window: i64,
    write_limiter: Semaphore,
    write_timeout: Duration,
    memory_accounting_enabled: bool,
    memory_used_bytes: AtomicU64,
    memory_used_bytes_by_shard: [AtomicU64; IN_MEMORY_SHARD_COUNT],
    memory_budget_bytes: AtomicU64,
    cardinality_limit: usize,
    wal_size_limit_bytes: u64,
    admission_poll_interval: Duration,
    memory_backpressure_lock: Mutex<()>,
    admission_backpressure_lock: Mutex<()>,
    max_observed_timestamp: AtomicI64,
    lifecycle: Arc<AtomicU8>,
    compaction_lock: Arc<Mutex<()>>,
    flush_visibility_lock: RwLock<()>,
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    data_path_process_lock: Mutex<Option<DataPathProcessLock>>,
    observability: Arc<StorageObservabilityCounters>,
    background_fail_fast: bool,
}

impl Storage for ChunkStorage {
    fn insert_rows(&self, rows: &[Row]) -> Result<()> {
        self.insert_rows_impl(rows)
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        self.select_api(metric, labels, start, end)
    }

    fn select_into(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        self.select_into_api(metric, labels, start, end, out)
    }

    fn select_with_options(&self, metric: &str, opts: QueryOptions) -> Result<Vec<DataPoint>> {
        self.select_with_options_api(metric, opts)
    }

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.select_all_api(metric, start, end)
    }

    fn list_metrics(&self) -> Result<Vec<MetricSeries>> {
        self.list_metrics_api()
    }

    fn list_metrics_with_wal(&self) -> Result<Vec<MetricSeries>> {
        self.list_metrics_with_wal_api()
    }

    fn select_series(&self, selection: &SeriesSelection) -> Result<Vec<MetricSeries>> {
        self.select_series_api(selection)
    }

    fn memory_used(&self) -> usize {
        if self.memory_accounting_enabled {
            self.memory_used_value()
        } else {
            self.refresh_memory_usage()
        }
    }

    fn memory_budget(&self) -> usize {
        self.memory_budget_value()
    }

    fn observability_snapshot(&self) -> StorageObservabilitySnapshot {
        self.observability_snapshot_impl()
    }

    fn snapshot(&self, destination: &Path) -> Result<()> {
        self.ensure_open()?;
        let write_permits = self.write_limiter.acquire_all(self.write_timeout)?;
        self.ensure_open()?;
        let _compaction_guard = self.compaction_lock.lock();

        if destination.exists() {
            drop(write_permits);
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot destination already exists: {}",
                destination.display()
            )));
        }

        let Some(destination_parent) = destination.parent() else {
            drop(write_permits);
            return Err(TsinkError::InvalidConfiguration(format!(
                "snapshot destination has no parent directory: {}",
                destination.display()
            )));
        };
        std::fs::create_dir_all(destination_parent)?;

        let wal_dir = self
            .wal
            .as_ref()
            .and_then(|wal| wal.path().parent().map(|path| path.to_path_buf()));

        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() && wal_dir.is_none() {
            drop(write_permits);
            return Err(TsinkError::InvalidConfiguration(
                "snapshot requires persistent storage (data_path with segments and/or WAL)"
                    .to_string(),
            ));
        }

        let staging = crate::engine::fs_utils::stage_dir_path(destination, "snapshot")?;
        std::fs::create_dir_all(&staging)?;
        let snapshot_result = (|| -> Result<()> {
            if let Some(path) = &self.numeric_lane_path {
                crate::engine::fs_utils::copy_dir_if_exists(
                    path,
                    &staging.join(NUMERIC_LANE_ROOT),
                )?;
            }
            if let Some(path) = &self.blob_lane_path {
                crate::engine::fs_utils::copy_dir_if_exists(path, &staging.join(BLOB_LANE_ROOT))?;
            }
            if let Some(path) = wal_dir.as_deref() {
                crate::engine::fs_utils::copy_dir_if_exists(path, &staging.join(WAL_DIR_NAME))?;
            }
            // Persist the current in-memory registry into the snapshot staging directory.
            // Copying the on-disk index can race with background refresh and capture a stale
            // mapping that omits series already present in WAL/segments.
            self.registry
                .read()
                .persist_to_path(&staging.join(SERIES_INDEX_FILE_NAME))?;
            Ok(())
        })();

        if let Err(err) = snapshot_result {
            let _ = crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&staging);
            drop(write_permits);
            return Err(err);
        }

        if let Err(err) = crate::engine::fs_utils::sync_dir(&staging) {
            let _ = crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&staging);
            drop(write_permits);
            return Err(err);
        }

        if let Err(err) = crate::engine::fs_utils::rename_and_sync_parents(&staging, destination) {
            let _ = crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&staging);
            drop(write_permits);
            return Err(err);
        }

        drop(write_permits);
        Ok(())
    }

    fn close(&self) -> Result<()> {
        self.close_impl()
    }
}

impl Drop for ChunkStorage {
    fn drop(&mut self) {
        if self.lifecycle.load(Ordering::SeqCst) != STORAGE_OPEN {
            return;
        }

        // Best-effort shutdown to avoid losing in-memory active chunks on last Arc drop.
        let _ = <Self as Storage>::close(self);
        self.lifecycle.store(STORAGE_CLOSED, Ordering::SeqCst);
        self.notify_compaction_thread();
        self.notify_flush_thread();
        let _ = self.join_background_threads();
    }
}

pub fn build_storage(builder: StorageBuilder) -> Result<Arc<dyn Storage>> {
    bootstrap::build_storage(builder)
}

pub fn restore_storage_from_snapshot(snapshot_path: &Path, data_path: &Path) -> Result<()> {
    bootstrap::restore_storage_from_snapshot(snapshot_path, data_path)
}

fn duration_to_timestamp_units(duration: Duration, precision: TimestampPrecision) -> i64 {
    match precision {
        TimestampPrecision::Seconds => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        TimestampPrecision::Milliseconds => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        TimestampPrecision::Microseconds => i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
        TimestampPrecision::Nanoseconds => i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX),
    }
}

fn partition_id_for_timestamp(timestamp: i64, partition_window: i64) -> i64 {
    timestamp.div_euclid(partition_window.max(1))
}

fn value_heap_bytes(value: &Value) -> usize {
    match value {
        Value::Bytes(bytes) => bytes.capacity(),
        Value::String(text) => text.capacity(),
        _ => 0,
    }
}

fn lane_for_value(value: &Value) -> ValueLane {
    match value {
        Value::Bytes(_) | Value::String(_) => ValueLane::Blob,
        _ => ValueLane::Numeric,
    }
}

#[cfg(test)]
mod tests;
