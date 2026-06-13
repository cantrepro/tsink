//! Core storage-engine state and subsystem wiring.
//!
//! Refactors touching ingest, lifecycle, visibility publication, retention,
//! tiering, or registry persistence should preserve the ordering guarantees
//! encoded across those owners rather than reasoning about one module in
//! isolation.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::concurrency::Semaphore;
use crate::engine::chunk::{self, Chunk, ChunkBuilder, ChunkPoint, ValueLane};
use crate::engine::compactor::Compactor;
use crate::engine::encoder::Encoder;
use crate::engine::segment::{SegmentWriter, WalHighWatermark};
use crate::engine::series::{SeriesId, SeriesRegistry, SeriesResolution, SeriesValueFamily};
use crate::engine::wal::{FramedWal, SamplesBatchFrame, SeriesDefinitionFrame};
use crate::mmap::PlatformMmap;
use crate::storage::{SeriesSelection, TimestampPrecision};
use crate::validation::{validate_labels, validate_metric};
use crate::{
    DataPoint, DeleteSeriesResult, Label, MetricSeries, QueryOptions, RemoteSegmentCachePolicy,
    RemoteStorageObservabilitySnapshot, Result, Row, SeriesPoints, Storage, StorageBuilder,
    StorageObservabilitySnapshot, StorageRuntimeMode, TsinkError, Value, WriteResult,
};
use parking_lot::{Mutex, MutexGuard, RwLock};

#[path = "bootstrap.rs"]
mod bootstrap;
#[path = "config.rs"]
mod config;
#[path = "construction.rs"]
mod construction;
#[path = "core_impl.rs"]
mod core_impl;
#[path = "deletion.rs"]
mod deletion;
#[path = "ingest.rs"]
mod ingest;
#[path = "lifecycle.rs"]
mod lifecycle;
#[path = "maintenance/mod.rs"]
mod maintenance;
#[path = "metadata_lookup.rs"]
mod metadata_lookup;
#[path = "metrics.rs"]
mod metrics;
#[path = "observability.rs"]
mod observability;
#[path = "process_lock.rs"]
mod process_lock;
#[path = "query_exec.rs"]
mod query_exec;
#[path = "query_read.rs"]
mod query_read;
#[path = "registry_catalog.rs"]
mod registry_catalog;
#[path = "rollups.rs"]
mod rollups;
#[path = "runtime.rs"]
mod runtime;
#[path = "shard_routing.rs"]
mod shard_routing;
#[path = "state.rs"]
mod state;
#[cfg(test)]
#[path = "test_hooks/mod.rs"]
mod test_hooks;
#[path = "tiering.rs"]
pub(crate) mod tiering;
#[path = "visibility.rs"]
mod visibility;
#[path = "write_buffer.rs"]
mod write_buffer;

use config::ChunkStorageOptions;
pub(in crate::engine::storage_engine) use construction::{
    PendingPersistedSegmentDiff, RemoteCatalogRefreshState,
};
pub(in crate::engine::storage_engine) use core_impl::{
    current_unix_millis_u64, duration_to_timestamp_units, elapsed_nanos_u64, lane_for_value,
    partition_id_for_timestamp, persisted_chunk_payload, saturating_u64_from_usize,
    value_heap_bytes, CatalogContext, ChunkContext, LifecyclePublicationContext, MemoryDeltaBytes,
    PersistedRefreshContext, WriteAdmissionControlContext, WriteApplyContext,
    WriteApplyMemoryAccountingContext, WriteApplyPublicationContext, WriteApplyRegistryContext,
    WriteApplyShardMutationContext, WriteApplyWalContext, WriteCommitContext,
    WriteCommitStageContext, WriteCommitWalCompletionContext, WritePrepareContext,
    WritePrepareMemoryBudgetContext, WritePrepareVisibilityContext, WritePrepareWalContext,
    WriteResolveContext, WriteSeriesValidationContext,
};
use metrics::StorageObservabilityCounters;
use process_lock::DataPathProcessLock;
use state::{
    ActiveSeriesState, PersistedChunkRef, PersistedIndexState, SealedChunkKey,
    SeriesVisibilityRangeSummary, SeriesVisibilitySummary, BLOB_LANE_ROOT, NUMERIC_LANE_ROOT,
    SERIES_INDEX_FILE_NAME, SERIES_VISIBILITY_SUMMARY_MAX_RANGES, WAL_DIR_NAME,
};
#[cfg(test)]
use test_hooks::{IngestCommitHook, PersistTestHooks};

const STORAGE_OPEN: u8 = 0;
const STORAGE_CLOSING: u8 = 1;
const STORAGE_CLOSED: u8 = 2;
const DEFAULT_RETENTION: Duration = Duration::from_secs(14 * 24 * 3600);
const DEFAULT_FUTURE_SKEW_ALLOWANCE: Duration = Duration::from_secs(15 * 60);
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_PARTITION_DURATION: Duration = Duration::from_secs(3600);
const DEFAULT_COMPACTION_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(250);
const DEFAULT_ROLLUP_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_ADMISSION_POLL_INTERVAL: Duration = Duration::from_millis(10);
const MIN_REMOTE_CATALOG_FAILURE_BACKOFF: Duration = Duration::from_millis(100);
const MAX_REMOTE_CATALOG_FAILURE_BACKOFF: Duration = Duration::from_secs(30);
const CLOSE_COMPACTION_MAX_PASSES: usize = 128;
const IN_MEMORY_SHARD_COUNT: usize = 64;
const REGISTRY_TXN_SHARD_COUNT: usize = IN_MEMORY_SHARD_COUNT;
const REGISTRY_INCREMENTAL_CHECKPOINT_MAX_SERIES: usize = 4096;

type ActiveBuilderShard = RwLock<HashMap<SeriesId, ActiveSeriesState>>;
type SealedChunkSeriesMap = BTreeMap<SealedChunkKey, Arc<Chunk>>;
type SealedChunkShard = RwLock<HashMap<SeriesId, SealedChunkSeriesMap>>;

// Shared state is split by subsystem so most refactors can stay local to one
// state bucket. Keep the lock boundaries below aligned with those owners.
//
// Coordination notes:
// - Use `background_maintenance_gate()` and `compaction_gate()` for the outer
//   lifecycle locks. Do not wait on them after taking `flush_visibility_lock`.
// - Use `begin_persisted_catalog_publication()` for persisted-view swaps. Stage
//   file work before acquiring that fence and keep the critical section short.
// - `write_txn_shards` are ingest-only identity fences; they must not be held
//   across background maintenance or persisted publication.
// - `recency_state_lock` only protects visibility-summary cache recomputation
//   and should stay disjoint from slow I/O.

/// Catalog, registry, and registry-persistence coordination state.
struct CatalogState {
    registry: RwLock<SeriesRegistry>,
    pending_series_ids: RwLock<BTreeSet<SeriesId>>,
    delta_series_count: AtomicU64,
    persistence_lock: Mutex<()>,
    metadata_shard_index: Option<MetadataShardIndex>,
    write_txn_shards: [Mutex<()>; REGISTRY_TXN_SHARD_COUNT],
}

/// In-memory active heads, sealed chunks, and per-series persisted watermarks.
struct ChunkBufferState {
    active_builders: [ActiveBuilderShard; IN_MEMORY_SHARD_COUNT],
    sealed_chunks: [SealedChunkShard; IN_MEMORY_SHARD_COUNT],
    persisted_chunk_watermarks: RwLock<HashMap<SeriesId, u64>>,
    next_chunk_sequence: AtomicU64,
    chunk_point_cap: usize,
}

/// Query-visible tombstones, visibility summaries, and publication fencing.
struct VisibilityState {
    tombstones: RwLock<HashMap<SeriesId, Vec<crate::engine::tombstone::TombstoneRange>>>,
    materialized_series: RwLock<BTreeSet<SeriesId>>,
    series_visibility_summaries: RwLock<HashMap<SeriesId, state::SeriesVisibilitySummary>>,
    series_visible_max_timestamps: RwLock<HashMap<SeriesId, Option<i64>>>,
    series_visible_bounded_max_timestamps: RwLock<HashMap<SeriesId, Option<i64>>>,
    visibility_state_generation: AtomicU64,
    live_series_pruning_generation: AtomicU64,
    max_observed_timestamp: AtomicI64,
    max_bounded_observed_timestamp: AtomicI64,
    recency_state_lock: Mutex<()>,
    flush_visibility_lock: RwLock<()>,
}

/// Persisted segment inventory, WAL handles, and remote refresh state.
struct PersistedStorageState {
    persisted_index: RwLock<PersistedIndexState>,
    persisted_index_dirty: Arc<AtomicBool>,
    numeric_lane_path: Option<PathBuf>,
    blob_lane_path: Option<PathBuf>,
    series_index_path: Option<PathBuf>,
    next_segment_id: Arc<AtomicU64>,
    numeric_compactor: Option<Compactor>,
    blob_compactor: Option<Compactor>,
    wal: Option<FramedWal>,
    tiered_storage: Option<config::TieredStorageConfig>,
    remote_segment_cache_policy: RemoteSegmentCachePolicy,
    remote_segment_refresh_interval: Duration,
    remote_catalog_refresh_state: Mutex<RemoteCatalogRefreshState>,
    pending_persisted_segment_diff: Arc<Mutex<PendingPersistedSegmentDiff>>,
    persisted_refresh_in_progress: AtomicBool,
}

/// Runtime configuration that is fixed for the life of one storage instance.
struct RuntimeConfigState {
    timestamp_precision: TimestampPrecision,
    retention_window: i64,
    future_skew_window: i64,
    retention_enforced: bool,
    runtime_mode: StorageRuntimeMode,
    partition_window: i64,
    max_active_partition_heads_per_series: usize,
    write_limiter: Semaphore,
    write_timeout: Duration,
    cardinality_limit: usize,
    wal_size_limit_bytes: u64,
    admission_poll_interval: Duration,
}

/// Memory-accounting counters and backpressure coordination.
struct MemoryAccountingState {
    accounting_enabled: bool,
    used_bytes: AtomicU64,
    used_bytes_by_shard: [AtomicU64; IN_MEMORY_SHARD_COUNT],
    shared_used_bytes: AtomicU64,
    registry_used_bytes: AtomicU64,
    metadata_used_bytes: AtomicU64,
    persisted_index_used_bytes: AtomicU64,
    persisted_mmap_used_bytes: AtomicU64,
    tombstone_used_bytes: AtomicU64,
    budget_bytes: AtomicU64,
    backpressure_lock: Mutex<()>,
    admission_backpressure_lock: Mutex<()>,
}

/// Storage lifecycle, process-lock ownership, and outer coordination locks.
struct CoordinationState {
    post_flush_maintenance_pending: AtomicBool,
    startup_metadata_reconcile_pending: AtomicBool,
    lifecycle: Arc<AtomicU8>,
    background_maintenance_lock: Mutex<()>,
    compaction_lock: Arc<Mutex<()>>,
    data_path_process_lock: Mutex<Option<DataPathProcessLock>>,
}

/// Background worker ownership, explicit wakeup policy, and shutdown joins.
struct BackgroundWorkerSupervisorState {
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    flush_thread_wakeup_requested: AtomicBool,
    persisted_refresh_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    rollup_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    fail_fast_enabled: bool,
}

/// Rollup runtime plus serialization around rollup policy execution.
struct RollupState {
    runtime: rollups::RollupRuntimeState,
    run_lock: Mutex<()>,
}

/// The storage engine runtime composed from subsystem-scoped state buckets.
pub struct ChunkStorage {
    catalog: CatalogState,
    chunks: ChunkBufferState,
    visibility: VisibilityState,
    persisted: PersistedStorageState,
    runtime: RuntimeConfigState,
    memory: MemoryAccountingState,
    coordination: CoordinationState,
    background: BackgroundWorkerSupervisorState,
    rollups: RollupState,
    observability: Arc<StorageObservabilityCounters>,
    #[cfg(test)]
    current_time_override: AtomicI64,
    #[cfg(test)]
    persist_test_hooks: PersistTestHooks,
}

struct MetadataShardIndex {
    shard_count: u32,
    series_ids_by_shard: RwLock<Vec<BTreeSet<SeriesId>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MetadataScopeSeriesLookup {
    Indexed(Vec<SeriesId>),
    Unavailable(MetadataScopeSeriesLookupUnavailable),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MetadataScopeSeriesLookupUnavailable {
    Disabled,
    ShardGeometryMismatch {
        indexed_shard_count: u32,
        requested_shard_count: u32,
    },
    Stale,
}

impl MetadataScopeSeriesLookupUnavailable {
    fn unsupported_operation(self, operation: &'static str) -> TsinkError {
        let reason = match self {
            Self::Disabled => {
                "bounded shard-scoped metadata requires metadata shard indexing to be enabled"
                    .to_string()
            }
            Self::ShardGeometryMismatch {
                indexed_shard_count,
                requested_shard_count,
            } => format!(
                "bounded shard-scoped metadata requires requested shard_count {requested_shard_count} to match indexed shard_count {indexed_shard_count}"
            ),
            Self::Stale => {
                "bounded shard-scoped metadata is temporarily unavailable because the shard index is stale or inconsistent"
                    .to_string()
            }
        };

        TsinkError::UnsupportedOperation { operation, reason }
    }
}

impl MetadataShardIndex {
    fn new(shard_count: u32) -> Self {
        Self {
            shard_count,
            series_ids_by_shard: RwLock::new(
                (0..shard_count)
                    .map(|_| BTreeSet::new())
                    .collect::<Vec<_>>(),
            ),
        }
    }

    fn shard_for_series(&self, metric: &str, labels: &[Label]) -> u32 {
        (crate::label::stable_series_identity_hash(metric, labels) % u64::from(self.shard_count))
            as u32
    }
}

impl Storage for ChunkStorage {
    fn insert_rows(&self, rows: &[Row]) -> Result<()> {
        self.insert_rows_impl(rows).map(|_| ())
    }

    fn insert_rows_with_result(&self, rows: &[Row]) -> Result<WriteResult> {
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

    fn select_many(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
    ) -> Result<Vec<SeriesPoints>> {
        self.select_many_api(series, start, end)
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

    fn list_metrics_in_shards(
        &self,
        scope: &crate::storage::MetadataShardScope,
    ) -> Result<Vec<MetricSeries>> {
        self.list_metrics_in_shards_api(scope)
    }

    fn select_series(&self, selection: &SeriesSelection) -> Result<Vec<MetricSeries>> {
        self.select_series_api(selection)
    }

    fn select_series_in_shards(
        &self,
        selection: &SeriesSelection,
        scope: &crate::storage::MetadataShardScope,
    ) -> Result<Vec<MetricSeries>> {
        self.select_series_in_shards_api(selection, scope)
    }

    fn compute_shard_window_digest(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
    ) -> Result<crate::storage::ShardWindowDigest> {
        self.compute_shard_window_digest_api(shard, shard_count, window_start, window_end)
    }

    fn scan_shard_window_rows(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        options: crate::storage::ShardWindowScanOptions,
    ) -> Result<crate::storage::ShardWindowRowsPage> {
        self.scan_shard_window_rows_api(shard, shard_count, window_start, window_end, options)
    }

    fn scan_series_rows(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: crate::storage::QueryRowsScanOptions,
    ) -> Result<crate::storage::QueryRowsPage> {
        self.scan_series_rows_api(series, start, end, options)
    }

    fn scan_metric_rows(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        options: crate::storage::QueryRowsScanOptions,
    ) -> Result<crate::storage::QueryRowsPage> {
        self.scan_metric_rows_api(metric, start, end, options)
    }

    fn delete_series(&self, selection: &SeriesSelection) -> Result<DeleteSeriesResult> {
        self.delete_series_api(selection)
    }

    fn memory_used(&self) -> usize {
        if self.memory.accounting_enabled {
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

    fn apply_rollup_policies(
        &self,
        policies: Vec<crate::storage::RollupPolicy>,
    ) -> Result<crate::storage::RollupObservabilitySnapshot> {
        self.apply_rollup_policies_impl(policies)
    }

    fn trigger_rollup_run(&self) -> Result<crate::storage::RollupObservabilitySnapshot> {
        self.run_rollup_pipeline_once()?;
        Ok(self.rollup_observability_snapshot())
    }

    fn snapshot(&self, destination: &Path) -> Result<()> {
        self.ensure_open()?;
        // Quiesce background maintenance before draining writer permits so a rollup worker
        // cannot hold the maintenance gate while waiting on the same permit pool.
        let _background_maintenance_guard = self.background_maintenance_gate();
        let write_permits = self
            .runtime
            .write_limiter
            .acquire_all(self.runtime.write_timeout)?;
        self.ensure_open()?;
        let _compaction_guard = self.compaction_gate();

        if crate::engine::fs_utils::path_exists_no_follow(destination)? {
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
            .persisted
            .wal
            .as_ref()
            .and_then(|wal| wal.path().parent().map(|path| path.to_path_buf()));

        if self.persisted.numeric_lane_path.is_none()
            && self.persisted.blob_lane_path.is_none()
            && wal_dir.is_none()
        {
            drop(write_permits);
            return Err(TsinkError::InvalidConfiguration(
                "snapshot requires persistent storage (data_path with segments and/or WAL)"
                    .to_string(),
            ));
        }

        let staging = crate::engine::fs_utils::stage_dir_path(destination, "snapshot")?;
        std::fs::create_dir_all(&staging)?;
        let snapshot_result = (|| -> Result<()> {
            if let Some(path) = &self.persisted.numeric_lane_path {
                crate::engine::fs_utils::copy_dir_if_exists(
                    path,
                    &staging.join(NUMERIC_LANE_ROOT),
                )?;
            }
            if let Some(path) = &self.persisted.blob_lane_path {
                crate::engine::fs_utils::copy_dir_if_exists(path, &staging.join(BLOB_LANE_ROOT))?;
            }
            if let Some(config) = &self.persisted.tiered_storage {
                if let Some(catalog_path) = config.segment_catalog_path.as_ref() {
                    if catalog_path.exists() {
                        std::fs::copy(
                            catalog_path,
                            staging.join(tiering::SEGMENT_CATALOG_FILE_NAME),
                        )?;
                    }
                }
            }
            #[cfg(test)]
            self.invoke_snapshot_pre_wal_copy_hook();
            if let Some(path) = wal_dir.as_deref() {
                crate::engine::fs_utils::copy_dir_if_exists(path, &staging.join(WAL_DIR_NAME))?;
            }
            if let Some(path) = self.rollups.runtime.dir_path() {
                crate::engine::fs_utils::copy_dir_if_exists(
                    path,
                    &staging.join(rollups::ROLLUP_DIR_NAME),
                )?;
            }
            // Persist the current in-memory registry into the snapshot staging directory.
            // Copying the on-disk index can race with background refresh and capture a stale
            // mapping that omits series already present in WAL/segments.
            self.catalog
                .registry
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
        if self.coordination.lifecycle.load(Ordering::SeqCst) != STORAGE_OPEN {
            return;
        }

        // Best-effort shutdown to avoid losing in-memory active chunks on last Arc drop.
        let _ = <Self as Storage>::close(self);
        self.coordination
            .lifecycle
            .store(STORAGE_CLOSED, Ordering::SeqCst);
        self.notify_background_threads();
        let _ = self.join_background_threads();
    }
}

pub fn build_storage(builder: StorageBuilder) -> Result<Arc<dyn Storage>> {
    bootstrap::build_storage(builder)
}

pub fn restore_storage_from_snapshot(snapshot_path: &Path, data_path: &Path) -> Result<()> {
    bootstrap::restore_storage_from_snapshot(snapshot_path, data_path)
}

#[cfg(test)]
mod tests;
