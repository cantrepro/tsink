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
use crate::engine::series::{
    SeriesCreationRateLimiter, SeriesCreationRateReservation, SeriesId, SeriesRegistry,
    SeriesResolution, SeriesValueFamily,
};
use crate::engine::wal::{FramedWal, SamplesBatchFrame, SeriesDefinitionFrame};
use crate::mmap::PlatformMmap;
use crate::storage::{
    BatchWriteResult, RowWriteOutcome, SeriesSelection, TimestampPrecision, WriteMode,
    WriteRejection,
};
use crate::{
    CardinalityObservabilitySnapshot, DataPoint, DeleteSeriesResult, EffectiveStorageLimits, Label,
    MetricSeries, QueryBudget, QueryBudgetSnapshot, QueryCancellationToken, QueryExecution,
    QueryOptions, QueryWorkLimits, RemoteSegmentCachePolicy, RemoteStorageObservabilitySnapshot,
    ResourceConfigurationSnapshot, Result, Row, SeriesPoints, Storage, StorageBuilder,
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
pub(in crate::engine::storage_engine) use maintenance::{
    BackgroundCatalogRefreshCursor, WriteTransientMemoryAccounting, WriteTransientMemoryReservation,
};
use metrics::StorageObservabilityCounters;
use process_lock::{DataPathProcessLock, SharedObjectStoreProcessLock};
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

type ActiveBuilderShard = RwLock<BTreeMap<SeriesId, ActiveSeriesState>>;
type SealedChunkSeriesMap = BTreeMap<SealedChunkKey, Arc<Chunk>>;
type SealedChunkShard = RwLock<BTreeMap<SeriesId, SealedChunkSeriesMap>>;

/// Stable ordering key for sealed chunks that have not yet been published into a segment.
/// WAL order matters here: a partial flush may publish only when its selected maximum WAL
/// high-water mark is strictly below the first unselected chunk (and every active-head floor).
/// Lowering the checkpoint below selected data would duplicate it on replay; advancing through
/// an unselected floor would lose it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PendingSealedChunkIndexKey {
    wal_lowwater: WalHighWatermark,
    sequence: u64,
}

#[derive(Debug, Clone, Copy)]
struct PendingSealedChunkLocation {
    shard_idx: usize,
    series_id: SeriesId,
    sealed_key: SealedChunkKey,
    wal_lowwater: WalHighWatermark,
    wal_highwater: WalHighWatermark,
    input_bytes: u64,
}

#[derive(Debug, Default)]
struct PendingSealedChunkIndex {
    /// Global sequence order preserves the scalar per-series persisted watermark invariant:
    /// every bounded snapshot is a prefix, so it cannot publish sequence N while leaving an
    /// older sequence for the same series unpersisted.
    by_sequence: BTreeMap<u64, PendingSealedChunkLocation>,
    /// WAL order supplies the first deferred replay floor needed to prove a selected sequence
    /// prefix is replay-closed without scanning the complete sealed inventory.
    by_wal: BTreeSet<PendingSealedChunkIndexKey>,
}

impl PendingSealedChunkIndex {
    fn insert(&mut self, key: PendingSealedChunkIndexKey, location: PendingSealedChunkLocation) {
        if let Some(replaced) = self.by_sequence.insert(key.sequence, location) {
            self.by_wal.remove(&PendingSealedChunkIndexKey {
                wal_lowwater: replaced.wal_lowwater,
                sequence: key.sequence,
            });
        }
        self.by_wal.insert(key);
    }

    fn remove(&mut self, key: PendingSealedChunkIndexKey) {
        self.by_sequence.remove(&key.sequence);
        self.by_wal.remove(&key);
    }

    fn len(&self) -> usize {
        self.by_sequence.len()
    }

    fn is_empty(&self) -> bool {
        self.by_sequence.is_empty()
    }
}

#[derive(Debug, Default)]
struct ActiveWalIndex {
    lowwater_counts: BTreeMap<WalHighWatermark, usize>,
}

impl ActiveWalIndex {
    fn add(&mut self, lowwater: WalHighWatermark) {
        let count = self.lowwater_counts.entry(lowwater).or_insert(0);
        *count = count.saturating_add(1);
    }

    fn remove(&mut self, lowwater: WalHighWatermark) {
        let Some(count) = self.lowwater_counts.get_mut(&lowwater) else {
            debug_assert!(
                false,
                "active WAL low-water index removal must have a matching entry"
            );
            return;
        };
        if *count <= 1 {
            self.lowwater_counts.remove(&lowwater);
        } else {
            *count -= 1;
        }
    }

    fn minimum(&self) -> Option<WalHighWatermark> {
        self.lowwater_counts.first_key_value().map(|(key, _)| *key)
    }
}

#[derive(Debug, Default)]
struct BackgroundActiveFlushCursor {
    shard_idx: usize,
    after_series_id: Option<SeriesId>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BackgroundRetentionMaintenanceCursor {
    after_root: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, Default)]
struct MaintenancePassSelection {
    items: usize,
    input_bytes: u64,
}

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
    series_creation_rate_limiter: Arc<SeriesCreationRateLimiter>,
}

/// In-memory active heads, sealed chunks, and per-series persisted watermarks.
struct ChunkBufferState {
    active_builders: [ActiveBuilderShard; IN_MEMORY_SHARD_COUNT],
    active_wal_index: Mutex<ActiveWalIndex>,
    sealed_chunks: [SealedChunkShard; IN_MEMORY_SHARD_COUNT],
    pending_sealed_chunks: RwLock<PendingSealedChunkIndex>,
    persisted_chunk_watermarks: RwLock<HashMap<SeriesId, u64>>,
    next_chunk_sequence: AtomicU64,
    chunk_point_cap: usize,
    background_active_flush_cursor: Mutex<BackgroundActiveFlushCursor>,
}

/// Query-visible tombstones, visibility summaries, and publication fencing.
struct VisibilityState {
    tombstones: Arc<RwLock<HashMap<SeriesId, Vec<crate::engine::tombstone::TombstoneRange>>>>,
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
    local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
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
    max_future_skew_window: Option<i64>,
    retention_enforced: bool,
    runtime_mode: StorageRuntimeMode,
    partition_window: i64,
    max_active_partition_heads_per_series: usize,
    write_limiter: Semaphore,
    write_timeout: Duration,
    cardinality_limit: usize,
    max_labels_per_series: usize,
    max_series_identity_bytes: usize,
    max_new_series_per_window: Option<usize>,
    new_series_window_nanos: u64,
    write_batch_limits: crate::WriteBatchLimits,
    wal_size_limit_bytes: u64,
    admission_poll_interval: Duration,
    maintenance_max_items_per_pass: usize,
    maintenance_max_bytes_per_pass: u64,
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
    tombstone_staged_bytes: AtomicU64,
    wal_series_definition_cache_used_bytes: AtomicU64,
    write_transient: Arc<WriteTransientMemoryAccounting>,
    budget_bytes: AtomicU64,
    active_backpressured_writers: AtomicU64,
    backpressure_events_total: AtomicU64,
    rejections_total: AtomicU64,
    backpressure_lock: Mutex<()>,
    admission_backpressure_lock: Mutex<()>,
}

/// Storage lifecycle, process-lock ownership, and outer coordination locks.
struct CoordinationState {
    post_flush_maintenance_pending: AtomicBool,
    startup_metadata_reconcile_pending: AtomicBool,
    background_retention_maintenance_cursor: Mutex<BackgroundRetentionMaintenanceCursor>,
    background_catalog_refresh_cursor: Mutex<BackgroundCatalogRefreshCursor>,
    lifecycle: Arc<AtomicU8>,
    background_maintenance_lock: Mutex<()>,
    compaction_lock: Arc<Mutex<()>>,
    data_path_process_lock: Mutex<Option<DataPathProcessLock>>,
    shared_object_store_process_lock: Mutex<Option<SharedObjectStoreProcessLock>>,
}

/// Background worker ownership, explicit wakeup policy, and shutdown joins.
#[derive(Default)]
struct BackgroundWorkerRuntimeState {
    running: AtomicBool,
    interval_nanos: AtomicU64,
    starts_total: AtomicU64,
    exits_total: AtomicU64,
    notifications_total: AtomicU64,
    idle_waits_total: AtomicU64,
    passes_started_total: AtomicU64,
    passes_completed_total: AtomicU64,
    shutdown_joins_total: AtomicU64,
}

/// Background worker ownership, explicit wakeup policy, and shutdown joins.
struct BackgroundWorkerSupervisorState {
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    compaction_runtime: Arc<BackgroundWorkerRuntimeState>,
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    flush_runtime: Arc<BackgroundWorkerRuntimeState>,
    flush_thread_wakeup_requested: AtomicBool,
    persisted_refresh_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    persisted_refresh_runtime: Arc<BackgroundWorkerRuntimeState>,
    rollup_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    rollup_runtime: Arc<BackgroundWorkerRuntimeState>,
    close_attempts_total: AtomicU64,
    close_success_total: AtomicU64,
    close_errors_total: AtomicU64,
    close_coordination_wait_nanos_total: AtomicU64,
    close_coordination_timeouts_total: AtomicU64,
    close_compaction_passes_total: AtomicU64,
    close_duration_nanos_total: AtomicU64,
    shutdown_join_wait_nanos_total: AtomicU64,
    compaction_interval: Duration,
    fail_fast_enabled: bool,
}

/// Rollup runtime plus serialization around rollup policy execution.
struct RollupState {
    runtime: rollups::RollupRuntimeState,
    run_lock: Mutex<()>,
    traversal_cursor: Mutex<rollups::BackgroundRollupCursor>,
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
    query_budget: QueryBudget,
    resource_configuration: RwLock<ResourceConfigurationSnapshot>,
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
    fn query_budget(&self) -> Option<QueryBudget> {
        Some(self.query_budget.clone())
    }

    fn begin_query_execution(
        &self,
        request_limits: QueryWorkLimits,
        cancellation: QueryCancellationToken,
    ) -> Result<Option<QueryExecution>> {
        self.query_budget
            .begin_query_with(request_limits, cancellation)
            .map(Some)
            .map_err(Into::into)
    }

    fn query_budget_snapshot(&self) -> QueryBudgetSnapshot {
        self.query_budget.snapshot()
    }

    fn insert_rows(&self, rows: &[Row]) -> Result<()> {
        self.insert_rows_impl(rows).map(|_| ())
    }

    fn insert_rows_with_result(&self, rows: &[Row]) -> Result<WriteResult> {
        self.insert_rows_impl(rows)
    }

    fn write_batch(&self, rows: &[Row], mode: WriteMode) -> Result<BatchWriteResult> {
        // Preserve the compatibility API's lifecycle and runtime-mode checks even though an open
        // engine treats an empty canonical batch as a no-op with no acknowledgement.
        if rows.is_empty() {
            self.insert_rows_impl(rows)?;
            return Ok(BatchWriteResult::empty());
        }

        // Admit the complete top-level batch once. Best-effort mode reuses this lease for each
        // row so row-at-a-time execution cannot bypass batch limits or double-reserve scratch.
        let transient_memory = match self.admit_write_rows_impl(rows) {
            Ok(transient_memory) => transient_memory,
            Err(error)
                if matches!(
                    error,
                    TsinkError::StorageClosed | TsinkError::StorageShuttingDown
                ) =>
            {
                let outcomes = (0..rows.len())
                    .map(|index| {
                        let cause_index = match mode {
                            WriteMode::Atomic => None,
                            WriteMode::BestEffort => Some(index),
                        };
                        RowWriteOutcome::rejected(
                            index,
                            WriteRejection::from_error(&error, cause_index),
                        )
                    })
                    .collect();
                return Ok(BatchWriteResult::from_outcomes(None, outcomes));
            }
            Err(error) => return Err(error),
        };

        match mode {
            WriteMode::Atomic => {
                match self.insert_rows_with_admission_impl(rows, transient_memory.clone()) {
                    Ok(result) => Ok(BatchWriteResult::from_outcomes(
                        Some(result.acknowledgement),
                        (0..rows.len()).map(RowWriteOutcome::accepted).collect(),
                    )),
                    Err(error) => {
                        // The compatibility error does not carry an input index. Atomic rollback
                        // makes the outcome trustworthy for every row, but the causal index remains
                        // unknown until the ingest pipeline exposes it directly.
                        let rejection = WriteRejection::from_error(&error, None);
                        Ok(BatchWriteResult::from_outcomes(
                            None,
                            (0..rows.len())
                                .map(|index| RowWriteOutcome::rejected(index, rejection.clone()))
                                .collect(),
                        ))
                    }
                }
            }
            WriteMode::BestEffort => {
                let mut acknowledgement: Option<crate::WriteAcknowledgement> = None;
                let mut outcomes = Vec::with_capacity(rows.len());

                for (index, row) in rows.iter().enumerate() {
                    match self.insert_rows_with_admission_impl(
                        std::slice::from_ref(row),
                        transient_memory.clone(),
                    ) {
                        Ok(result) => {
                            acknowledgement = Some(match acknowledgement {
                                Some(current) => current.weakest(result.acknowledgement),
                                None => result.acknowledgement,
                            });
                            outcomes.push(RowWriteOutcome::accepted(index));
                        }
                        Err(error) => outcomes.push(RowWriteOutcome::rejected(
                            index,
                            WriteRejection::from_error(&error, Some(index)),
                        )),
                    }
                }

                Ok(BatchWriteResult::from_outcomes(acknowledgement, outcomes))
            }
        }
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.select_with_execution(metric, labels, start, end, &execution)
    }

    fn select_with_execution(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> Result<Vec<DataPoint>> {
        execution.checkpoint()?;
        let matched = u64::from(self.series_exists(metric, labels));
        execution.charge_series_matched(matched)?;
        self.select_api(metric, labels, start, end, execution)
    }

    fn select_into(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.select_into_with_execution(metric, labels, start, end, out, &execution)
    }

    fn select_into_with_execution(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
        execution: &QueryExecution,
    ) -> Result<()> {
        execution.checkpoint()?;
        let matched = u64::from(self.series_exists(metric, labels));
        execution.charge_series_matched(matched)?;
        self.select_into_api(metric, labels, start, end, out, execution)
    }

    fn select_many(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
    ) -> Result<Vec<SeriesPoints>> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.select_many_with_execution(series, start, end, &execution)
    }

    fn select_many_with_execution(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> Result<Vec<SeriesPoints>> {
        execution.checkpoint()?;
        let matched = self.count_existing_series(series);
        execution.charge_series_matched(matched)?;
        self.select_many_api(series, start, end, execution)
    }

    fn select_with_options(&self, metric: &str, opts: QueryOptions) -> Result<Vec<DataPoint>> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.select_with_options_with_execution(metric, opts, &execution)
    }

    fn select_with_options_with_execution(
        &self,
        metric: &str,
        opts: QueryOptions,
        execution: &QueryExecution,
    ) -> Result<Vec<DataPoint>> {
        execution.checkpoint()?;
        let matched = u64::from(self.series_exists(metric, &opts.labels));
        execution.charge_series_matched(matched)?;
        self.select_with_options_api(metric, opts, execution)
    }

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.select_all_with_execution(metric, start, end, &execution)
    }

    fn select_all_with_execution(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        execution.checkpoint()?;
        let matched = {
            let registry = self.catalog.registry.read();
            saturating_u64_from_usize(registry.series_ids_for_metric(metric).len())
        };
        execution.charge_series_matched(matched)?;
        self.select_all_api(metric, start, end, execution)
    }

    fn list_metrics(&self) -> Result<Vec<MetricSeries>> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.list_metrics_with_execution(&execution)
    }

    fn list_metrics_with_execution(&self, execution: &QueryExecution) -> Result<Vec<MetricSeries>> {
        execution.checkpoint()?;
        self.list_metrics_api(execution)
    }

    fn list_metrics_with_wal(&self) -> Result<Vec<MetricSeries>> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.list_metrics_with_wal_api(&execution)
    }

    fn list_metrics_in_shards(
        &self,
        scope: &crate::storage::MetadataShardScope,
    ) -> Result<Vec<MetricSeries>> {
        self.list_metrics_in_shards_api(scope)
    }

    fn select_series(&self, selection: &SeriesSelection) -> Result<Vec<MetricSeries>> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.select_series_with_execution(selection, &execution)
    }

    fn select_series_with_execution(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> Result<Vec<MetricSeries>> {
        execution.checkpoint()?;
        self.select_series_api(selection, execution)
    }

    #[cfg(test)]
    fn sync_persisted_segments_from_disk_if_dirty_for_tests(&self) -> Result<()> {
        self.sync_persisted_segments_from_disk_if_dirty()
    }

    fn select_series_in_shards(
        &self,
        selection: &SeriesSelection,
        scope: &crate::storage::MetadataShardScope,
    ) -> Result<Vec<MetricSeries>> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.select_series_in_shards_with_execution(selection, scope, &execution)
    }

    fn select_series_in_shards_with_execution(
        &self,
        selection: &SeriesSelection,
        scope: &crate::storage::MetadataShardScope,
        execution: &QueryExecution,
    ) -> Result<Vec<MetricSeries>> {
        execution.checkpoint()?;
        self.select_series_in_shards_api(selection, scope, execution)
    }

    fn compute_shard_window_digest(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
    ) -> Result<crate::storage::ShardWindowDigest> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.compute_shard_window_digest_with_execution(
            shard,
            shard_count,
            window_start,
            window_end,
            &execution,
        )
    }

    fn compute_shard_window_digest_with_execution(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        execution: &QueryExecution,
    ) -> Result<crate::storage::ShardWindowDigest> {
        execution.checkpoint()?;
        let digest = self.compute_shard_window_digest_api(
            shard,
            shard_count,
            window_start,
            window_end,
            execution,
        )?;
        execution.charge_returned_bytes(
            u64::try_from(std::mem::size_of_val(&digest)).unwrap_or(u64::MAX),
        )?;
        Ok(digest)
    }

    fn scan_shard_window_rows(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        options: crate::storage::ShardWindowScanOptions,
    ) -> Result<crate::storage::ShardWindowRowsPage> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.scan_shard_window_rows_with_execution(
            shard,
            shard_count,
            window_start,
            window_end,
            options,
            &execution,
        )
    }

    fn scan_shard_window_rows_with_execution(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        options: crate::storage::ShardWindowScanOptions,
        execution: &QueryExecution,
    ) -> Result<crate::storage::ShardWindowRowsPage> {
        execution.checkpoint()?;
        let page = self.scan_shard_window_rows_api(
            shard,
            shard_count,
            window_start,
            window_end,
            options,
            execution,
        )?;
        Ok(page)
    }

    fn scan_series_rows(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: crate::storage::QueryRowsScanOptions,
    ) -> Result<crate::storage::QueryRowsPage> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.scan_series_rows_with_execution(series, start, end, options, &execution)
    }

    fn scan_series_rows_with_execution(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: crate::storage::QueryRowsScanOptions,
        execution: &QueryExecution,
    ) -> Result<crate::storage::QueryRowsPage> {
        execution.checkpoint()?;
        let matched = self.count_existing_series(series);
        execution.charge_series_matched(matched)?;
        self.scan_series_rows_api(series, start, end, options, execution)
    }

    fn scan_metric_rows(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        options: crate::storage::QueryRowsScanOptions,
    ) -> Result<crate::storage::QueryRowsPage> {
        let execution = self
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
            .expect("built-in storage always exposes a query budget");
        self.scan_metric_rows_with_execution(metric, start, end, options, &execution)
    }

    fn scan_metric_rows_with_execution(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        options: crate::storage::QueryRowsScanOptions,
        execution: &QueryExecution,
    ) -> Result<crate::storage::QueryRowsPage> {
        execution.checkpoint()?;
        let matched = {
            let registry = self.catalog.registry.read();
            saturating_u64_from_usize(registry.series_ids_for_metric(metric).len())
        };
        execution.charge_series_matched(matched)?;
        self.scan_metric_rows_api(metric, start, end, options, execution)
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

    fn effective_storage_limits(&self) -> EffectiveStorageLimits {
        let persistent =
            self.persisted.numeric_lane_path.is_some() || self.persisted.blob_lane_path.is_some();
        let wal_enabled = self.persisted.wal.is_some();
        let finite_usize =
            |value: usize| (value != usize::MAX).then(|| u64::try_from(value).unwrap_or(u64::MAX));
        let wal_unlimited_sentinel = usize::MAX as u64;
        let write_timeout_nanos = self.runtime.write_timeout.as_nanos().min(u64::MAX.into()) as u64;
        let local_disk_limits = self
            .persisted
            .local_disk_budget
            .as_ref()
            .map(|budget| budget.limits());
        let duration_nanos =
            |duration: Duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        let flush_concurrency = u64::from(persistent);
        let compaction_concurrency = u64::from(
            self.persisted.numeric_compactor.is_some() || self.persisted.blob_compactor.is_some(),
        );
        let persisted_refresh_concurrency = u64::from(
            persistent
                || (self.runtime.runtime_mode == StorageRuntimeMode::ComputeOnly
                    && self.persisted.tiered_storage.is_some()),
        );
        let rollup_concurrency = u64::from(self.rollups.runtime.dir_path().is_some());
        let retention_tiering_concurrency =
            u64::from(persistent && self.runtime.retention_enforced);
        let remote_catalog_refresh_concurrency = u64::from(
            self.runtime.runtime_mode == StorageRuntimeMode::ComputeOnly
                && self.persisted.tiered_storage.is_some(),
        );
        let persisted_refresh_poll_interval = if remote_catalog_refresh_concurrency > 0 {
            self.persisted
                .remote_segment_refresh_interval
                .min(DEFAULT_FLUSH_INTERVAL)
                .max(Duration::from_millis(1))
        } else {
            DEFAULT_FLUSH_INTERVAL
        };

        EffectiveStorageLimits {
            reported_by_backend: true,
            persistent,
            wal_enabled,
            accounted_memory_bytes: finite_usize(self.memory_budget_value()),
            cardinality: finite_usize(self.runtime.cardinality_limit),
            max_labels_per_series: finite_usize(self.runtime.max_labels_per_series),
            max_series_identity_bytes: finite_usize(self.runtime.max_series_identity_bytes),
            max_new_series_per_window: self
                .runtime
                .max_new_series_per_window
                .map(|limit| u64::try_from(limit).unwrap_or(u64::MAX)),
            new_series_window_nanos: self
                .runtime
                .max_new_series_per_window
                .map(|_| self.runtime.new_series_window_nanos),
            max_write_batch_rows: self
                .runtime
                .write_batch_limits
                .max_rows
                .map(|limit| u64::try_from(limit).unwrap_or(u64::MAX)),
            max_write_batch_input_bytes: self
                .runtime
                .write_batch_limits
                .max_modeled_input_bytes
                .map(|limit| u64::try_from(limit).unwrap_or(u64::MAX)),
            wal_bytes: wal_enabled
                .then_some(self.runtime.wal_size_limit_bytes)
                .filter(|limit| *limit != wal_unlimited_sentinel),
            wal_write_buffer_bytes: self
                .persisted
                .wal
                .as_ref()
                .map(|wal| u64::try_from(wal.write_buffer_capacity_bytes()).unwrap_or(u64::MAX)),
            local_disk_bytes: local_disk_limits.and_then(|limits| limits.max_bytes),
            filesystem_free_headroom_bytes: local_disk_limits
                .map(|limits| limits.filesystem_free_headroom_bytes),
            maintenance_temp_reserve_bytes: local_disk_limits
                .map(|limits| limits.maintenance_temp_reserve_bytes),
            max_concurrent_writers: Some(
                u64::try_from(self.runtime.write_limiter.capacity()).unwrap_or(u64::MAX),
            ),
            write_timeout_nanos: Some(write_timeout_nanos),
            max_background_threads: Some(
                flush_concurrency
                    .saturating_add(compaction_concurrency)
                    .saturating_add(persisted_refresh_concurrency)
                    .saturating_add(rollup_concurrency),
            ),
            max_flush_concurrency: Some(flush_concurrency),
            max_compaction_concurrency: Some(compaction_concurrency),
            max_retention_tiering_concurrency: Some(retention_tiering_concurrency),
            max_remote_catalog_refresh_concurrency: Some(remote_catalog_refresh_concurrency),
            max_remote_tier_fetch_concurrency: if self.persisted.tiered_storage.is_some() {
                self.query_budget.limits().max_concurrent_queries
            } else {
                Some(0)
            },
            max_rollup_concurrency: Some(rollup_concurrency),
            flush_interval_nanos: (flush_concurrency > 0)
                .then(|| duration_nanos(DEFAULT_FLUSH_INTERVAL)),
            compaction_interval_nanos: (compaction_concurrency > 0).then(|| {
                duration_nanos(
                    self.background
                        .compaction_interval
                        .max(Duration::from_millis(1)),
                )
            }),
            persisted_refresh_poll_interval_nanos: (persisted_refresh_concurrency > 0)
                .then(|| duration_nanos(persisted_refresh_poll_interval)),
            rollup_interval_nanos: (rollup_concurrency > 0)
                .then(|| duration_nanos(DEFAULT_ROLLUP_INTERVAL)),
            max_active_partition_heads_per_series: Some(
                u64::try_from(self.runtime.max_active_partition_heads_per_series)
                    .unwrap_or(u64::MAX),
            ),
        }
    }

    fn resource_configuration_snapshot(&self) -> ResourceConfigurationSnapshot {
        let mut snapshot = self.resource_configuration.read().clone();
        snapshot.resolved_limits.storage = self.effective_storage_limits();
        snapshot.resolved_limits.query = self.query_budget.limits();
        snapshot
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
        let progress = self.run_rollup_pipeline_once()?;
        Ok(self.rollup_observability_snapshot_with_progress(progress))
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
        // Ingest and materialization acquire permits before the rollup transaction lock. Snapshot
        // drains every permit first, then takes the same rollup/visibility order as delete so it
        // cannot copy a mixed tombstone manifest set or an active coordinator decision.
        let _rollup_transaction_guard = self.rollups.run_lock.lock();
        let _compaction_guard = self.compaction_gate();
        let _visibility_guard = self.visibility_write_fence();
        self.recover_and_reload_tombstones_locked()?;
        if let Some(data_path) = self
            .persisted
            .series_index_path
            .as_deref()
            .and_then(Path::parent)
        {
            maintenance::ensure_no_pending_post_flush_replacement(data_path)?;
        }

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

        if let Some(local_disk_budget) = &self.persisted.local_disk_budget {
            if local_disk_budget.governs(destination)? {
                drop(write_permits);
                return Err(TsinkError::InvalidConfiguration(format!(
                    "snapshot destination must be outside the managed data directory: {}",
                    destination.display()
                )));
            }
        }

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

    #[cfg(test)]
    fn abandon_without_close_for_tests(&self) -> Result<()> {
        self.coordination
            .lifecycle
            .store(STORAGE_CLOSED, Ordering::SeqCst);
        self.notify_background_threads();
        self.join_background_threads()?;
        self.release_data_path_process_lock();
        Ok(())
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
        self.release_data_path_process_lock();
    }
}

pub fn build_storage(builder: StorageBuilder) -> Result<Arc<dyn Storage>> {
    bootstrap::build_storage(builder)
}

pub fn restore_storage_from_snapshot(snapshot_path: &Path, data_path: &Path) -> Result<()> {
    bootstrap::restore_storage_from_snapshot(snapshot_path, data_path)
}

pub fn restore_storage_from_snapshot_with_disk_budget(
    snapshot_path: &Path,
    data_path: &Path,
    disk_budget: Arc<crate::LocalDiskBudget>,
) -> Result<()> {
    bootstrap::restore_storage_from_snapshot_with_disk_budget(snapshot_path, data_path, disk_budget)
}

#[cfg(test)]
mod tests;
