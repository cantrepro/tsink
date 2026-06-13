use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, MutexGuard, RwLock};
use rayon::prelude::*;

use crate::concurrency::{Semaphore, SemaphoreGuard};
use crate::engine::chunk::{Chunk, ChunkBuilder, ChunkPoint, ValueLane};
use crate::engine::compactor::{CompactionRunStats, Compactor};
use crate::engine::encoder::Encoder;
use crate::engine::query::{
    decode_chunk_points_in_range_into, decode_encoded_chunk_payload_in_range_into,
    EncodedChunkDescriptor,
};
use crate::engine::segment::{
    collect_expired_segment_dirs, load_segment_indexes, SegmentWriter, WalHighWatermark,
};
use crate::engine::series_registry::{
    validate_labels, validate_metric, SeriesId, SeriesRegistry, SeriesResolution,
};
use crate::engine::wal::{FramedWal, ReplayFrame, SamplesBatchFrame, SeriesDefinitionFrame};
use crate::mmap::PlatformMmap;
use crate::storage::{
    aggregate_series, compile_series_matchers, downsample_points, downsample_points_with_custom,
    CompiledSeriesMatcher, SeriesMatcherOp, SeriesSelection, TimestampPrecision,
};
use crate::{
    Aggregation, CompactionObservabilitySnapshot, DataPoint, FlushObservabilitySnapshot, Label,
    MetricSeries, QueryObservabilitySnapshot, QueryOptions, Result, Row, Storage, StorageBuilder,
    StorageObservabilitySnapshot, TsinkError, Value, WalObservabilitySnapshot,
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

#[derive(Default)]
struct StorageObservabilityCounters {
    wal: WalObservabilityCounters,
    flush: FlushObservabilityCounters,
    compaction: CompactionObservabilityCounters,
    query: QueryObservabilityCounters,
}

impl StorageObservabilityCounters {
    fn record_compaction_result(&self, stats: CompactionRunStats, duration_nanos: u64) {
        self.compaction.runs_total.fetch_add(1, Ordering::Relaxed);
        self.compaction
            .duration_nanos_total
            .fetch_add(duration_nanos, Ordering::Relaxed);
        self.compaction.source_segments_total.fetch_add(
            saturating_u64_from_usize(stats.source_segments),
            Ordering::Relaxed,
        );
        self.compaction.output_segments_total.fetch_add(
            saturating_u64_from_usize(stats.output_segments),
            Ordering::Relaxed,
        );
        self.compaction.source_chunks_total.fetch_add(
            saturating_u64_from_usize(stats.source_chunks),
            Ordering::Relaxed,
        );
        self.compaction.output_chunks_total.fetch_add(
            saturating_u64_from_usize(stats.output_chunks),
            Ordering::Relaxed,
        );
        self.compaction.source_points_total.fetch_add(
            saturating_u64_from_usize(stats.source_points),
            Ordering::Relaxed,
        );
        self.compaction.output_points_total.fetch_add(
            saturating_u64_from_usize(stats.output_points),
            Ordering::Relaxed,
        );
        if stats.compacted {
            self.compaction
                .success_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.compaction.noop_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_compaction_error(&self, duration_nanos: u64) {
        self.compaction.runs_total.fetch_add(1, Ordering::Relaxed);
        self.compaction.errors_total.fetch_add(1, Ordering::Relaxed);
        self.compaction
            .duration_nanos_total
            .fetch_add(duration_nanos, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct WalObservabilityCounters {
    replay_runs_total: AtomicU64,
    replay_frames_total: AtomicU64,
    replay_series_definitions_total: AtomicU64,
    replay_sample_batches_total: AtomicU64,
    replay_points_total: AtomicU64,
    replay_errors_total: AtomicU64,
    replay_duration_nanos_total: AtomicU64,
    append_series_definitions_total: AtomicU64,
    append_sample_batches_total: AtomicU64,
    append_points_total: AtomicU64,
    append_bytes_total: AtomicU64,
    append_errors_total: AtomicU64,
    resets_total: AtomicU64,
    reset_errors_total: AtomicU64,
}

#[derive(Default)]
struct FlushObservabilityCounters {
    pipeline_runs_total: AtomicU64,
    pipeline_success_total: AtomicU64,
    pipeline_timeout_total: AtomicU64,
    pipeline_errors_total: AtomicU64,
    pipeline_duration_nanos_total: AtomicU64,
    active_flush_runs_total: AtomicU64,
    active_flush_errors_total: AtomicU64,
    active_flushed_series_total: AtomicU64,
    active_flushed_chunks_total: AtomicU64,
    active_flushed_points_total: AtomicU64,
    persist_runs_total: AtomicU64,
    persist_success_total: AtomicU64,
    persist_noop_total: AtomicU64,
    persist_errors_total: AtomicU64,
    persisted_series_total: AtomicU64,
    persisted_chunks_total: AtomicU64,
    persisted_points_total: AtomicU64,
    persisted_segments_total: AtomicU64,
    persist_duration_nanos_total: AtomicU64,
    evicted_sealed_chunks_total: AtomicU64,
}

#[derive(Default)]
struct CompactionObservabilityCounters {
    runs_total: AtomicU64,
    success_total: AtomicU64,
    noop_total: AtomicU64,
    errors_total: AtomicU64,
    source_segments_total: AtomicU64,
    output_segments_total: AtomicU64,
    source_chunks_total: AtomicU64,
    output_chunks_total: AtomicU64,
    source_points_total: AtomicU64,
    output_points_total: AtomicU64,
    duration_nanos_total: AtomicU64,
}

#[derive(Default)]
struct QueryObservabilityCounters {
    select_calls_total: AtomicU64,
    select_errors_total: AtomicU64,
    select_duration_nanos_total: AtomicU64,
    select_points_returned_total: AtomicU64,
    select_with_options_calls_total: AtomicU64,
    select_with_options_errors_total: AtomicU64,
    select_with_options_duration_nanos_total: AtomicU64,
    select_with_options_points_returned_total: AtomicU64,
    select_all_calls_total: AtomicU64,
    select_all_errors_total: AtomicU64,
    select_all_duration_nanos_total: AtomicU64,
    select_all_series_returned_total: AtomicU64,
    select_all_points_returned_total: AtomicU64,
    select_series_calls_total: AtomicU64,
    select_series_errors_total: AtomicU64,
    select_series_duration_nanos_total: AtomicU64,
    select_series_returned_total: AtomicU64,
    merge_path_queries_total: AtomicU64,
    append_sort_path_queries_total: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct ChunkStorageOptions {
    retention_window: i64,
    retention_enforced: bool,
    partition_window: i64,
    max_writers: usize,
    write_timeout: Duration,
    memory_budget_bytes: u64,
    cardinality_limit: usize,
    wal_size_limit_bytes: u64,
    admission_poll_interval: Duration,
    compaction_interval: Duration,
    background_threads_enabled: bool,
}

impl Default for ChunkStorageOptions {
    fn default() -> Self {
        Self {
            retention_window: duration_to_timestamp_units(
                DEFAULT_RETENTION,
                TimestampPrecision::Nanoseconds,
            ),
            retention_enforced: true,
            partition_window: duration_to_timestamp_units(
                DEFAULT_PARTITION_DURATION,
                TimestampPrecision::Nanoseconds,
            )
            .max(1),
            max_writers: crate::cgroup::default_workers_limit().max(1),
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: true,
        }
    }
}

struct ActiveSeriesState {
    series_id: SeriesId,
    lane: ValueLane,
    point_cap: usize,
    builder: ChunkBuilder,
    partition_id: Option<i64>,
}

impl ActiveSeriesState {
    fn new(series_id: SeriesId, lane: ValueLane, point_cap: usize) -> Self {
        Self {
            series_id,
            lane,
            point_cap,
            builder: ChunkBuilder::new(series_id, lane, point_cap),
            partition_id: None,
        }
    }

    fn rotate_partition_if_needed(
        &mut self,
        ts: i64,
        partition_window: i64,
    ) -> Result<Option<Chunk>> {
        let partition_window = partition_window.max(1);
        let next_partition = partition_id_for_timestamp(ts, partition_window);

        if self.builder.is_empty() {
            self.partition_id = Some(next_partition);
            return Ok(None);
        }

        if self.partition_id.is_none() {
            self.partition_id = self
                .builder
                .first_point()
                .map(|first| partition_id_for_timestamp(first.ts, partition_window));
        }

        if self.partition_id == Some(next_partition) {
            return Ok(None);
        }

        let chunk = self.finalize_current()?;
        self.partition_id = Some(next_partition);
        Ok(chunk)
    }

    fn rotate_full_if_needed(&mut self) -> Result<Option<Chunk>> {
        if !self.builder.is_full() {
            return Ok(None);
        }
        let chunk = self.finalize_current()?;
        self.partition_id = None;
        Ok(chunk)
    }

    fn flush_partial(&mut self) -> Result<Option<Chunk>> {
        if self.builder.is_empty() {
            return Ok(None);
        }
        let chunk = self.finalize_current()?;
        self.partition_id = None;
        Ok(chunk)
    }

    fn finalize_current(&mut self) -> Result<Option<Chunk>> {
        let old_builder = std::mem::replace(
            &mut self.builder,
            ChunkBuilder::new(self.series_id, self.lane, self.point_cap),
        );
        let mut chunk = old_builder
            .finalize(
                super::chunk::TimestampCodecId::DeltaVarint,
                super::chunk::ValueCodecId::ConstantRle,
            )
            .ok_or_else(|| {
                TsinkError::InvalidConfiguration("failed to finalize chunk".to_string())
            })?;

        // Preserve a monotonic timestamp stream per chunk for better timestamp codec density.
        chunk.points.sort_by_key(|point| point.ts);

        let encoded = Encoder::encode_chunk_points(&chunk.points, self.lane)?;
        chunk.header.ts_codec = encoded.ts_codec;
        chunk.header.value_codec = encoded.value_codec;
        chunk.encoded_payload = encoded.payload;

        Ok(Some(chunk))
    }
}

struct PendingPoint {
    series_id: SeriesId,
    lane: ValueLane,
    ts: i64,
    value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RawSeriesKey {
    metric: String,
    labels: Vec<Label>,
}

impl RawSeriesKey {
    fn new(metric: &str, labels: &[Label]) -> Self {
        let mut normalized_labels = labels.to_vec();
        normalized_labels.sort();
        normalized_labels.dedup();
        Self {
            metric: metric.to_string(),
            labels: normalized_labels,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SealedChunkKey {
    min_ts: i64,
    max_ts: i64,
    point_count: u16,
    sequence: u64,
}

impl SealedChunkKey {
    fn from_chunk(chunk: &Chunk, sequence: u64) -> Self {
        Self {
            min_ts: chunk.header.min_ts,
            max_ts: chunk.header.max_ts,
            point_count: chunk.header.point_count,
            sequence,
        }
    }

    fn upper_bound_for_min_ts(min_ts_exclusive: i64) -> Self {
        Self {
            min_ts: min_ts_exclusive,
            max_ts: i64::MIN,
            point_count: 0,
            sequence: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PersistedChunkRef {
    min_ts: i64,
    max_ts: i64,
    point_count: u16,
    sequence: u64,
    chunk_offset: u64,
    chunk_len: u32,
    lane: ValueLane,
    ts_codec: super::chunk::TimestampCodecId,
    value_codec: super::chunk::ValueCodecId,
    segment_slot: usize,
}

#[derive(Default)]
struct PersistedIndexState {
    chunk_refs: HashMap<SeriesId, Vec<PersistedChunkRef>>,
    segment_maps: Vec<Arc<PlatformMmap>>,
}

const NUMERIC_LANE_ROOT: &str = "lane_numeric";
const BLOB_LANE_ROOT: &str = "lane_blob";
const WAL_DIR_NAME: &str = "wal";

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
    next_segment_id: Arc<AtomicU64>,
    numeric_compactor: Option<Compactor>,
    blob_compactor: Option<Compactor>,
    wal: Option<FramedWal>,
    retention_window: i64,
    retention_enforced: bool,
    partition_window: i64,
    write_limiter: Semaphore,
    write_timeout: Duration,
    memory_used_bytes: AtomicU64,
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
    observability: Arc<StorageObservabilityCounters>,
}

impl ChunkStorage {
    pub fn new(chunk_point_cap: usize, wal: Option<FramedWal>) -> Self {
        Self::new_with_data_path_and_options(
            chunk_point_cap,
            wal,
            None,
            None,
            1,
            ChunkStorageOptions::default(),
        )
        .expect("failed to initialize chunk storage")
    }

    pub fn new_with_data_path(
        chunk_point_cap: usize,
        wal: Option<FramedWal>,
        numeric_lane_path: Option<PathBuf>,
        blob_lane_path: Option<PathBuf>,
        next_segment_id: u64,
    ) -> Self {
        Self::new_with_data_path_and_options(
            chunk_point_cap,
            wal,
            numeric_lane_path,
            blob_lane_path,
            next_segment_id,
            ChunkStorageOptions::default(),
        )
        .expect("failed to initialize chunk storage")
    }

    fn new_with_data_path_and_options(
        chunk_point_cap: usize,
        wal: Option<FramedWal>,
        numeric_lane_path: Option<PathBuf>,
        blob_lane_path: Option<PathBuf>,
        next_segment_id: u64,
        options: ChunkStorageOptions,
    ) -> Result<Self> {
        let next_segment_id = Arc::new(AtomicU64::new(next_segment_id.max(1)));
        let numeric_compactor = numeric_lane_path.as_ref().map(|path| {
            Compactor::new_with_segment_id_allocator(
                path,
                chunk_point_cap,
                Arc::clone(&next_segment_id),
            )
        });
        let blob_compactor = blob_lane_path.as_ref().map(|path| {
            Compactor::new_with_segment_id_allocator(
                path,
                chunk_point_cap,
                Arc::clone(&next_segment_id),
            )
        });
        let lifecycle = Arc::new(AtomicU8::new(STORAGE_OPEN));
        let compaction_lock = Arc::new(Mutex::new(()));
        let observability = Arc::new(StorageObservabilityCounters::default());
        let compaction_thread = if options.background_threads_enabled {
            Self::spawn_background_compaction_thread(
                Arc::downgrade(&lifecycle),
                Arc::clone(&compaction_lock),
                numeric_compactor.clone(),
                blob_compactor.clone(),
                options.compaction_interval,
                Arc::clone(&observability),
            )
        } else {
            Ok(None)
        };

        Ok(Self {
            registry: RwLock::new(SeriesRegistry::new()),
            materialized_series: RwLock::new(BTreeSet::new()),
            registry_write_txn_shards: std::array::from_fn(|_| Mutex::new(())),
            active_builders: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            sealed_chunks: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            persisted_index: RwLock::new(PersistedIndexState::default()),
            persisted_chunk_watermarks: RwLock::new(HashMap::new()),
            next_chunk_sequence: AtomicU64::new(1),
            chunk_point_cap: chunk_point_cap.clamp(1, u16::MAX as usize),
            numeric_compactor,
            blob_compactor,
            numeric_lane_path,
            blob_lane_path,
            next_segment_id,
            wal,
            retention_window: options.retention_window.max(0),
            retention_enforced: options.retention_enforced,
            partition_window: options.partition_window.max(1),
            write_limiter: Semaphore::new(options.max_writers.max(1)),
            write_timeout: options.write_timeout,
            memory_used_bytes: AtomicU64::new(0),
            memory_budget_bytes: AtomicU64::new(options.memory_budget_bytes),
            cardinality_limit: options.cardinality_limit,
            wal_size_limit_bytes: options.wal_size_limit_bytes,
            admission_poll_interval: options.admission_poll_interval,
            memory_backpressure_lock: Mutex::new(()),
            admission_backpressure_lock: Mutex::new(()),
            max_observed_timestamp: AtomicI64::new(i64::MIN),
            lifecycle,
            compaction_lock,
            flush_visibility_lock: RwLock::new(()),
            compaction_thread: Mutex::new(compaction_thread?),
            flush_thread: Mutex::new(None),
            observability,
        })
    }

    fn series_shard_idx(series_id: SeriesId) -> usize {
        (series_id % IN_MEMORY_SHARD_COUNT as u64) as usize
    }

    fn registry_metric_shard_idx(metric: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        metric.hash(&mut hasher);
        (hasher.finish() as usize) % REGISTRY_TXN_SHARD_COUNT
    }

    fn lock_registry_write_shards_for_rows<'a>(&'a self, rows: &[Row]) -> Vec<MutexGuard<'a, ()>> {
        let mut shard_bits = [0u64; REGISTRY_TXN_SHARD_COUNT.div_ceil(u64::BITS as usize)];
        for row in rows {
            let shard_idx = Self::registry_metric_shard_idx(row.metric());
            let word_idx = shard_idx / (u64::BITS as usize);
            let bit_idx = shard_idx % (u64::BITS as usize);
            shard_bits[word_idx] |= 1u64 << bit_idx;
        }

        let guard_count = shard_bits
            .iter()
            .map(|bits| bits.count_ones() as usize)
            .sum();
        let mut guards = Vec::with_capacity(guard_count);
        for (word_idx, bits) in shard_bits.into_iter().enumerate() {
            let mut remaining = bits;
            while remaining != 0 {
                let bit_idx = remaining.trailing_zeros() as usize;
                let shard_idx = word_idx * (u64::BITS as usize) + bit_idx;
                guards.push(self.registry_write_txn_shards[shard_idx].lock());
                remaining &= remaining - 1;
            }
        }
        guards
    }

    fn active_shard(&self, series_id: SeriesId) -> &RwLock<HashMap<SeriesId, ActiveSeriesState>> {
        &self.active_builders[Self::series_shard_idx(series_id)]
    }

    fn sealed_shard(
        &self,
        series_id: SeriesId,
    ) -> &RwLock<HashMap<SeriesId, BTreeMap<SealedChunkKey, Chunk>>> {
        &self.sealed_chunks[Self::series_shard_idx(series_id)]
    }

    fn mark_materialized_series_ids<I>(&self, series_ids: I)
    where
        I: IntoIterator<Item = SeriesId>,
    {
        self.materialized_series.write().extend(series_ids);
    }

    fn metric_series_for_ids<I>(&self, series_ids: I) -> Vec<MetricSeries>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let registry = self.registry.read();
        series_ids
            .into_iter()
            .filter_map(|series_id| {
                registry
                    .decode_series_key(series_id)
                    .map(|series_key| MetricSeries {
                        name: series_key.metric,
                        labels: series_key.labels,
                    })
            })
            .collect()
    }

    fn spawn_background_compaction_thread(
        lifecycle: std::sync::Weak<AtomicU8>,
        compaction_lock: Arc<Mutex<()>>,
        numeric_compactor: Option<Compactor>,
        blob_compactor: Option<Compactor>,
        compaction_interval: Duration,
        observability: Arc<StorageObservabilityCounters>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if numeric_compactor.is_none() && blob_compactor.is_none() {
            return Ok(None);
        }

        let handle = std::thread::Builder::new()
            .name("tsink-compaction".to_string())
            .spawn(move || loop {
                std::thread::park_timeout(compaction_interval);

                let Some(lifecycle) = lifecycle.upgrade() else {
                    break;
                };

                match lifecycle.load(Ordering::SeqCst) {
                    STORAGE_OPEN => {}
                    STORAGE_CLOSED => break,
                    _ => continue,
                }

                let _compaction_guard = compaction_lock.lock();
                let _ = Self::compact_compactors(
                    numeric_compactor.as_ref(),
                    blob_compactor.as_ref(),
                    Some(observability.as_ref()),
                );
            })?;

        Ok(Some(handle))
    }

    fn notify_compaction_thread(&self) {
        if let Some(compaction_thread) = self.compaction_thread.lock().as_ref() {
            compaction_thread.thread().unpark();
        }
    }

    fn spawn_background_flush_thread(
        storage: std::sync::Weak<Self>,
        flush_interval: Duration,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let handle = std::thread::Builder::new()
            .name("tsink-flush".to_string())
            .spawn(move || loop {
                std::thread::park_timeout(flush_interval);

                let Some(storage) = storage.upgrade() else {
                    break;
                };

                match storage.lifecycle.load(Ordering::SeqCst) {
                    STORAGE_OPEN => {}
                    STORAGE_CLOSED => break,
                    _ => continue,
                }

                let _ = storage.flush_pipeline_once();
            })?;

        Ok(Some(handle))
    }

    fn start_background_flush_thread(self: &Arc<Self>, flush_interval: Duration) -> Result<()> {
        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
            return Ok(());
        }

        let mut flush_thread = self.flush_thread.lock();
        if flush_thread.is_some() {
            return Ok(());
        }

        *flush_thread = Self::spawn_background_flush_thread(Arc::downgrade(self), flush_interval)?;
        Ok(())
    }

    fn notify_flush_thread(&self) {
        if let Some(flush_thread) = self.flush_thread.lock().as_ref() {
            flush_thread.thread().unpark();
        }
    }

    fn panic_payload_message(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
        let payload = match payload.downcast::<String>() {
            Ok(message) => return *message,
            Err(payload) => payload,
        };
        let payload = match payload.downcast::<&'static str>() {
            Ok(message) => return (*message).to_string(),
            Err(payload) => payload,
        };
        format!("unknown panic payload type: {:?}", payload.type_id())
    }

    fn join_background_thread(
        handle: std::thread::JoinHandle<()>,
        worker_name: &str,
    ) -> Result<()> {
        handle.join().map_err(|payload| {
            TsinkError::Other(format!(
                "{worker_name} worker panicked: {}",
                Self::panic_payload_message(payload)
            ))
        })
    }

    fn join_background_threads(&self) -> Result<()> {
        if let Some(compaction_thread) = self.compaction_thread.lock().take() {
            Self::join_background_thread(compaction_thread, "compaction")?;
        }
        if let Some(flush_thread) = self.flush_thread.lock().take() {
            Self::join_background_thread(flush_thread, "flush")?;
        }
        Ok(())
    }

    fn flush_pipeline_once(&self) -> Result<()> {
        self.observability
            .flush
            .pipeline_runs_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
            self.observability
                .flush
                .pipeline_success_total
                .fetch_add(1, Ordering::Relaxed);
            self.observability
                .flush
                .pipeline_duration_nanos_total
                .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
            return Ok(());
        }

        // Drain writer permits with a bounded wait so background flush can still make progress
        // under sustained write load instead of bailing immediately when one permit is busy.
        let write_permits = match self.write_limiter.acquire_all(self.write_timeout) {
            Ok(permits) => permits,
            Err(TsinkError::WriteTimeout { .. }) => {
                self.observability
                    .flush
                    .pipeline_timeout_total
                    .fetch_add(1, Ordering::Relaxed);
                self.observability
                    .flush
                    .pipeline_duration_nanos_total
                    .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
                return Ok(());
            }
            Err(err) => {
                self.observability
                    .flush
                    .pipeline_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                self.observability
                    .flush
                    .pipeline_duration_nanos_total
                    .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
                return Err(err);
            }
        };
        let flush_result = self.flush_all_active();
        if let Err(err) = flush_result {
            drop(write_permits);
            self.observability
                .flush
                .pipeline_errors_total
                .fetch_add(1, Ordering::Relaxed);
            self.observability
                .flush
                .pipeline_duration_nanos_total
                .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
            return Err(err);
        }

        let persisted = match self.persist_segment(true) {
            Ok(persisted) => persisted,
            Err(err) => {
                drop(write_permits);
                self.observability
                    .flush
                    .pipeline_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                self.observability
                    .flush
                    .pipeline_duration_nanos_total
                    .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
                return Err(err);
            }
        };
        drop(write_permits);
        if persisted {
            if let Err(err) = self.refresh_persisted_indexes_and_evict_flushed_sealed_chunks() {
                self.observability
                    .flush
                    .pipeline_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                self.observability
                    .flush
                    .pipeline_duration_nanos_total
                    .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
                return Err(err);
            }
        }
        if let Err(err) = self.sweep_expired_persisted_segments() {
            self.observability
                .flush
                .pipeline_errors_total
                .fetch_add(1, Ordering::Relaxed);
            self.observability
                .flush
                .pipeline_duration_nanos_total
                .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
            return Err(err);
        }
        if self.memory_budget_value() != usize::MAX {
            self.refresh_memory_usage();
        }
        self.observability
            .flush
            .pipeline_success_total
            .fetch_add(1, Ordering::Relaxed);
        self.observability
            .flush
            .pipeline_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);
        Ok(())
    }

    fn ensure_open(&self) -> Result<()> {
        if self.lifecycle.load(Ordering::SeqCst) != STORAGE_OPEN {
            return Err(TsinkError::StorageClosed);
        }
        Ok(())
    }

    fn update_max_observed_timestamp(&self, ts: i64) {
        let mut current = self.max_observed_timestamp.load(Ordering::Acquire);
        while ts > current {
            match self.max_observed_timestamp.compare_exchange_weak(
                current,
                ts,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    fn active_retention_cutoff(&self) -> Option<i64> {
        if !self.retention_enforced {
            return None;
        }
        let max_observed = self.max_observed_timestamp.load(Ordering::Acquire);
        if max_observed == i64::MIN {
            return None;
        }
        Some(max_observed.saturating_sub(self.retention_window))
    }

    fn apply_retention_filter(&self, points: &mut Vec<DataPoint>) {
        let Some(cutoff) = self.active_retention_cutoff() else {
            return;
        };
        points.retain(|point| point.timestamp >= cutoff);
    }

    fn validate_points_against_retention(&self, points: &[PendingPoint]) -> Result<()> {
        if !self.retention_enforced {
            return Ok(());
        }
        if points.is_empty() {
            return Ok(());
        }

        let existing_max = self.max_observed_timestamp.load(Ordering::Acquire);
        let incoming_max = points
            .iter()
            .map(|point| point.ts)
            .max()
            .unwrap_or(i64::MIN);
        let effective_max = existing_max.max(incoming_max);
        if effective_max == i64::MIN {
            return Ok(());
        }

        let cutoff = effective_max.saturating_sub(self.retention_window);
        for point in points {
            if point.ts < cutoff {
                return Err(TsinkError::OutOfRetention {
                    timestamp: point.ts,
                });
            }
        }
        Ok(())
    }

    fn memory_budget_value(&self) -> usize {
        self.memory_budget_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize
    }

    fn memory_used_value(&self) -> usize {
        self.memory_used_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize
    }

    fn cardinality_limit_value(&self) -> usize {
        self.cardinality_limit
    }

    fn projected_new_series_count(registry: &SeriesRegistry, rows: &[Row]) -> Result<usize> {
        let mut pending_new_series = HashSet::<RawSeriesKey>::new();

        for row in rows {
            validate_metric(row.metric())?;
            validate_labels(row.labels())?;

            if registry
                .resolve_existing(row.metric(), row.labels())
                .is_some()
            {
                continue;
            }

            pending_new_series.insert(RawSeriesKey::new(row.metric(), row.labels()));
        }

        Ok(pending_new_series.len())
    }

    fn wal_size_limit_value(&self) -> u64 {
        self.wal_size_limit_bytes
    }

    fn compute_memory_usage_bytes(&self) -> usize {
        let active_total = self
            .active_builders
            .par_iter()
            .map(|shard| {
                let active = shard.read();
                active.values().fold(0usize, |acc, state| {
                    acc.saturating_add(Self::active_state_memory_usage_bytes(state))
                })
            })
            .reduce(|| 0usize, |acc, bytes| acc.saturating_add(bytes));

        let sealed_total = self
            .sealed_chunks
            .par_iter()
            .map(|shard| {
                let sealed = shard.read();
                sealed.values().fold(0usize, |series_acc, chunks| {
                    series_acc.saturating_add(chunks.values().fold(0usize, |chunk_acc, chunk| {
                        chunk_acc.saturating_add(Self::chunk_memory_usage_bytes(chunk))
                    }))
                })
            })
            .reduce(|| 0usize, |acc, bytes| acc.saturating_add(bytes));

        active_total.saturating_add(sealed_total)
    }

    fn refresh_memory_usage(&self) -> usize {
        let used = self.compute_memory_usage_bytes();
        self.memory_used_bytes
            .store(used.min(u64::MAX as usize) as u64, Ordering::Release);
        used
    }

    fn active_state_memory_usage_bytes(state: &ActiveSeriesState) -> usize {
        let mut bytes = std::mem::size_of::<ActiveSeriesState>().saturating_add(
            state
                .builder
                .capacity()
                .saturating_mul(std::mem::size_of::<ChunkPoint>()),
        );
        for point in state.builder.points() {
            bytes = bytes.saturating_add(value_heap_bytes(&point.value));
        }
        bytes
    }

    fn chunk_memory_usage_bytes(chunk: &Chunk) -> usize {
        let mut bytes = std::mem::size_of::<Chunk>()
            .saturating_add(
                chunk
                    .points
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ChunkPoint>()),
            )
            .saturating_add(chunk.encoded_payload.capacity());

        for point in &chunk.points {
            bytes = bytes.saturating_add(value_heap_bytes(&point.value));
        }

        bytes
    }

    fn estimate_write_memory_growth_bytes(
        &self,
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
    ) -> usize {
        let per_point_bytes = std::mem::size_of::<ChunkPoint>();
        let point_storage_bytes = points.len().saturating_mul(per_point_bytes);
        let heap_bytes = points.iter().fold(0usize, |acc, point| {
            acc.saturating_add(value_heap_bytes(&point.value))
        });

        // New active states preallocate a chunk-sized point buffer.
        let per_new_active_series = std::mem::size_of::<ActiveSeriesState>().saturating_add(
            self.chunk_point_cap
                .saturating_mul(std::mem::size_of::<ChunkPoint>()),
        );
        let new_active_series = grouped
            .keys()
            .filter(|series_id| {
                !self
                    .active_shard(**series_id)
                    .read()
                    .contains_key(series_id)
            })
            .count();
        let active_state_bytes = new_active_series.saturating_mul(per_new_active_series);

        point_storage_bytes
            .saturating_add(heap_bytes)
            .saturating_add(active_state_bytes)
    }

    fn estimate_write_wal_growth_bytes(
        &self,
        new_series_defs: &[SeriesDefinitionFrame],
        batches: &[SamplesBatchFrame],
    ) -> Result<u64> {
        let Some(_wal) = &self.wal else {
            return Ok(0);
        };

        let mut bytes = 0u64;
        for definition in new_series_defs {
            bytes = bytes.saturating_add(FramedWal::estimate_series_definition_frame_bytes(
                definition,
            )?);
        }
        bytes = bytes.saturating_add(FramedWal::estimate_samples_frame_bytes(batches)?);
        Ok(bytes)
    }

    fn memory_budget_shortfall(&self, estimated_growth_bytes: usize) -> Option<(usize, usize)> {
        let budget = self.memory_budget_value();
        if budget == usize::MAX {
            return None;
        }

        let used = self.refresh_memory_usage();
        let required = used.saturating_add(estimated_growth_bytes);
        (required > budget).then_some((budget, required))
    }

    fn wal_size_shortfall(&self, estimated_growth_bytes: u64) -> Result<Option<(u64, u64)>> {
        let limit = self.wal_size_limit_value();
        if limit == u64::MAX || estimated_growth_bytes == 0 {
            return Ok(None);
        }

        let Some(wal) = &self.wal else {
            return Ok(None);
        };

        if estimated_growth_bytes > limit {
            return Ok(Some((limit, estimated_growth_bytes)));
        }

        let current = wal.total_size_bytes()?;
        let required = current.saturating_add(estimated_growth_bytes);
        Ok((required > limit).then_some((limit, required)))
    }

    fn acquire_write_permits_excluding_current_writer(&self) -> Result<Vec<SemaphoreGuard<'_>>> {
        let permits_needed = self.write_limiter.capacity().saturating_sub(1);
        if permits_needed == 0 {
            return Ok(Vec::new());
        }

        let deadline = Instant::now() + self.write_timeout;
        let mut guards = Vec::with_capacity(permits_needed);
        for _ in 0..permits_needed {
            let now = Instant::now();
            let remaining = deadline.saturating_duration_since(now);
            guards.push(self.write_limiter.try_acquire_for(remaining)?);
        }

        Ok(guards)
    }

    fn relieve_pressure_once(&self) -> Result<()> {
        let _backpressure_guard = self.admission_backpressure_lock.lock();
        // `relieve_pressure_once` is entered while the caller already holds one writer permit.
        // Drain the remaining permits so WAL reset/truncate cannot race in-flight writers.
        let _drained_writers = self.acquire_write_permits_excluding_current_writer()?;
        self.flush_all_active()?;
        self.prune_empty_active_series();
        if self.persist_segment(true)? {
            self.refresh_persisted_indexes_and_evict_flushed_sealed_chunks()?;
        }
        self.enforce_memory_budget_if_needed_with_writers_already_drained()?;
        Ok(())
    }

    fn enforce_admission_controls(
        &self,
        estimated_memory_growth_bytes: usize,
        estimated_wal_growth_bytes: u64,
    ) -> Result<()> {
        let deadline = Instant::now() + self.write_timeout;

        loop {
            if let Some((_budget, _required)) =
                self.memory_budget_shortfall(estimated_memory_growth_bytes)
            {
                self.relieve_pressure_once()?;

                if let Some((post_budget, post_required)) =
                    self.memory_budget_shortfall(estimated_memory_growth_bytes)
                {
                    if Instant::now() >= deadline {
                        return Err(TsinkError::MemoryBudgetExceeded {
                            budget: post_budget,
                            required: post_required,
                        });
                    }
                    self.notify_flush_thread();
                    std::thread::sleep(
                        self.admission_poll_interval
                            .min(deadline.saturating_duration_since(Instant::now())),
                    );
                    continue;
                }
            }

            if let Some((_limit, _required)) =
                self.wal_size_shortfall(estimated_wal_growth_bytes)?
            {
                self.relieve_pressure_once()?;

                if let Some((post_limit, post_required)) =
                    self.wal_size_shortfall(estimated_wal_growth_bytes)?
                {
                    if Instant::now() >= deadline {
                        return Err(TsinkError::WalSizeLimitExceeded {
                            limit: post_limit,
                            required: post_required,
                        });
                    }
                    self.notify_flush_thread();
                    std::thread::sleep(
                        self.admission_poll_interval
                            .min(deadline.saturating_duration_since(Instant::now())),
                    );
                    continue;
                }
            }

            return Ok(());
        }
    }

    fn prune_empty_active_series(&self) {
        for shard in &self.active_builders {
            let mut active = shard.write();
            active.retain(|_, state| !state.builder.is_empty());
        }
    }

    fn mark_persisted_chunk_watermarks(&self, watermarks: &HashMap<SeriesId, u64>) {
        if watermarks.is_empty() {
            return;
        }

        let mut persisted = self.persisted_chunk_watermarks.write();
        for (series_id, watermark) in watermarks {
            let entry = persisted.entry(*series_id).or_insert(0);
            *entry = (*entry).max(*watermark);
        }
    }

    fn reload_persisted_indexes_from_disk(&self) -> Result<()> {
        self.reload_persisted_indexes_from_disk_with_exclusions(None, None)
    }

    fn reload_persisted_indexes_from_disk_with_exclusions(
        &self,
        numeric_exclusions: Option<&HashSet<PathBuf>>,
        blob_exclusions: Option<&HashSet<PathBuf>>,
    ) -> Result<()> {
        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
            return Ok(());
        }

        let mut loaded_numeric = if let Some(path) = &self.numeric_lane_path {
            load_segment_indexes(path)?
        } else {
            crate::engine::segment::LoadedSegmentIndexes::default()
        };
        let mut loaded_blob = if let Some(path) = &self.blob_lane_path {
            load_segment_indexes(path)?
        } else {
            crate::engine::segment::LoadedSegmentIndexes::default()
        };

        if let Some(exclusions) = numeric_exclusions {
            if !exclusions.is_empty() {
                loaded_numeric
                    .indexed_segments
                    .retain(|segment| !exclusions.contains(&segment.root));
            }
        }
        if let Some(exclusions) = blob_exclusions {
            if !exclusions.is_empty() {
                loaded_blob
                    .indexed_segments
                    .retain(|segment| !exclusions.contains(&segment.root));
            }
        }

        let loaded_segments = merge_loaded_segment_indexes(
            loaded_numeric,
            loaded_blob,
            self.numeric_lane_path.is_some(),
            self.blob_lane_path.is_some(),
        )?;
        self.apply_loaded_segment_indexes(loaded_segments)
    }

    fn refresh_persisted_indexes_and_evict_flushed_sealed_chunks(&self) -> Result<()> {
        let _visibility_guard = self.flush_visibility_lock.write();
        self.reload_persisted_indexes_from_disk()?;
        let evicted = self.evict_persisted_sealed_chunks();
        self.observability
            .flush
            .evicted_sealed_chunks_total
            .fetch_add(saturating_u64_from_usize(evicted), Ordering::Relaxed);
        Ok(())
    }

    fn sweep_expired_persisted_segments(&self) -> Result<usize> {
        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
            return Ok(0);
        }

        let Some(cutoff) = self.active_retention_cutoff() else {
            return Ok(0);
        };

        let _compaction_guard = self.compaction_lock.lock();
        let _visibility_guard = self.flush_visibility_lock.write();

        let mut expired_dirs = Vec::new();
        let mut expired_numeric_dirs = HashSet::new();
        let mut expired_blob_dirs = HashSet::new();
        if let Some(path) = &self.numeric_lane_path {
            let numeric_expired = collect_expired_segment_dirs(path, cutoff)?;
            expired_numeric_dirs.extend(numeric_expired.iter().cloned());
            expired_dirs.extend(numeric_expired);
        }
        if let Some(path) = &self.blob_lane_path {
            let blob_expired = collect_expired_segment_dirs(path, cutoff)?;
            expired_blob_dirs.extend(blob_expired.iter().cloned());
            expired_dirs.extend(blob_expired);
        }

        if expired_dirs.is_empty() {
            return Ok(0);
        }

        // Drop mmaps for just-expired segments while preserving retained persisted visibility.
        self.reload_persisted_indexes_from_disk_with_exclusions(
            Some(&expired_numeric_dirs),
            Some(&expired_blob_dirs),
        )?;
        self.evict_persisted_sealed_chunks();

        let mut removed = 0usize;
        for dir in expired_dirs {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => removed = removed.saturating_add(1),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }

        Ok(removed)
    }

    fn sealed_chunk_is_present_in_persisted_chunks(
        persisted_chunks: Option<&[PersistedChunkRef]>,
        key: SealedChunkKey,
        chunk: &Chunk,
    ) -> bool {
        persisted_chunks.is_some_and(|persisted_chunks| {
            persisted_chunks.iter().any(|chunk_ref| {
                chunk_ref.min_ts == key.min_ts
                    && chunk_ref.max_ts == key.max_ts
                    && chunk_ref.point_count == key.point_count
                    && chunk_ref.lane == chunk.header.lane
                    && chunk_ref.ts_codec == chunk.header.ts_codec
                    && chunk_ref.value_codec == chunk.header.value_codec
            })
        })
    }

    fn find_oldest_evictable_sealed_chunk(&self) -> Option<(usize, SeriesId, SealedChunkKey)> {
        let persisted = self.persisted_chunk_watermarks.read();
        let persisted_index = self.persisted_index.read();
        let mut oldest: Option<(usize, SeriesId, SealedChunkKey)> = None;

        for (shard_idx, shard) in self.sealed_chunks.iter().enumerate() {
            let sealed = shard.read();
            for (series_id, chunks) in sealed.iter() {
                let persisted_sequence = persisted.get(series_id).copied().unwrap_or(0);
                let persisted_chunks = persisted_index
                    .chunk_refs
                    .get(series_id)
                    .map(|chunks| chunks.as_slice());
                for (key, chunk) in chunks {
                    if key.sequence > persisted_sequence {
                        continue;
                    }
                    if !Self::sealed_chunk_is_present_in_persisted_chunks(
                        persisted_chunks,
                        *key,
                        chunk,
                    ) {
                        continue;
                    }
                    let replace = oldest
                        .map(|(_, _, current)| key.sequence < current.sequence)
                        .unwrap_or(true);
                    if replace {
                        oldest = Some((shard_idx, *series_id, *key));
                    }
                }
            }
        }

        oldest
    }

    fn evict_oldest_persisted_sealed_chunk(&self) -> bool {
        let Some((shard_idx, series_id, key)) = self.find_oldest_evictable_sealed_chunk() else {
            return false;
        };

        let mut sealed = self.sealed_chunks[shard_idx].write();
        let Some(chunks) = sealed.get_mut(&series_id) else {
            return false;
        };
        let removed = chunks.remove(&key).is_some();
        if chunks.is_empty() {
            sealed.remove(&series_id);
        }

        if removed {
            self.observability
                .flush
                .evicted_sealed_chunks_total
                .fetch_add(1, Ordering::Relaxed);
        }

        removed
    }

    fn evict_persisted_sealed_chunks(&self) -> usize {
        let persisted = self.persisted_chunk_watermarks.read();
        let persisted_index = self.persisted_index.read();
        let mut evicted = 0usize;

        for shard in &self.sealed_chunks {
            let mut sealed = shard.write();
            sealed.retain(|series_id, chunks| {
                let persisted_sequence = persisted.get(series_id).copied().unwrap_or(0);
                let persisted_chunks = persisted_index
                    .chunk_refs
                    .get(series_id)
                    .map(|chunks| chunks.as_slice());

                chunks.retain(|key, chunk| {
                    let remove = key.sequence <= persisted_sequence
                        && Self::sealed_chunk_is_present_in_persisted_chunks(
                            persisted_chunks,
                            *key,
                            chunk,
                        );
                    if remove {
                        evicted = evicted.saturating_add(1);
                    }
                    !remove
                });

                !chunks.is_empty()
            });
        }

        evicted
    }

    fn enforce_memory_budget_if_needed_with_writers_already_drained(&self) -> Result<()> {
        let budget = self.memory_budget_value();
        if budget == usize::MAX || self.memory_used_value() <= budget {
            return Ok(());
        }

        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
            // In-memory-only mode cannot spill to L0 segments.
            return Ok(());
        }

        let _backpressure_guard = self.memory_backpressure_lock.lock();
        self.enforce_memory_budget_locked(budget)
    }

    fn enforce_memory_budget_if_needed(&self) -> Result<()> {
        let budget = self.memory_budget_value();
        if budget == usize::MAX || self.memory_used_value() <= budget {
            return Ok(());
        }

        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
            // In-memory-only mode cannot spill to L0 segments.
            return Ok(());
        }

        let _backpressure_guard = self.memory_backpressure_lock.lock();
        let used = self.refresh_memory_usage();
        if used <= budget {
            return Ok(());
        }

        // Drain all writers before any flush/persist path that may reset or truncate WAL.
        let _write_permits = self.write_limiter.acquire_all(self.write_timeout)?;
        self.enforce_memory_budget_locked(budget)
    }

    fn enforce_memory_budget_locked(&self, budget: usize) -> Result<()> {
        let mut used = self.refresh_memory_usage();
        if used <= budget {
            return Ok(());
        }

        self.flush_all_active()?;
        self.prune_empty_active_series();
        used = self.refresh_memory_usage();
        if used <= budget {
            return Ok(());
        }

        if self.persist_segment(false)? {
            self.refresh_persisted_indexes_and_evict_flushed_sealed_chunks()?;
            used = self.refresh_memory_usage();
        }

        while used > budget {
            if !self.evict_oldest_persisted_sealed_chunk() {
                break;
            }
            used = self.refresh_memory_usage();
        }

        Ok(())
    }

    fn append_sealed_chunk(&self, series_id: SeriesId, chunk: Chunk) {
        let sequence = self.next_chunk_sequence.fetch_add(1, Ordering::SeqCst);
        let key = SealedChunkKey::from_chunk(&chunk, sequence);
        let mut sealed = self.sealed_shard(series_id).write();
        sealed.entry(series_id).or_default().insert(key, chunk);
    }

    fn flush_all_active(&self) -> Result<()> {
        self.observability
            .flush
            .active_flush_runs_total
            .fetch_add(1, Ordering::Relaxed);

        let mut flushed_series = 0usize;
        let mut flushed_chunks = 0usize;
        let mut flushed_points = 0usize;

        let result = (|| -> Result<()> {
            for shard in &self.active_builders {
                let mut active = shard.write();
                for (series_id, state) in active.iter_mut() {
                    let Some(chunk) = state.flush_partial()? else {
                        continue;
                    };
                    flushed_series = flushed_series.saturating_add(1);
                    flushed_chunks = flushed_chunks.saturating_add(1);
                    flushed_points =
                        flushed_points.saturating_add(chunk.header.point_count as usize);
                    let sequence = self.next_chunk_sequence.fetch_add(1, Ordering::SeqCst);
                    let key = SealedChunkKey::from_chunk(&chunk, sequence);
                    let mut sealed = self.sealed_shard(*series_id).write();
                    sealed.entry(*series_id).or_default().insert(key, chunk);
                }
            }

            Ok(())
        })();

        match result {
            Ok(()) => {
                self.observability
                    .flush
                    .active_flushed_series_total
                    .fetch_add(saturating_u64_from_usize(flushed_series), Ordering::Relaxed);
                self.observability
                    .flush
                    .active_flushed_chunks_total
                    .fetch_add(saturating_u64_from_usize(flushed_chunks), Ordering::Relaxed);
                self.observability
                    .flush
                    .active_flushed_points_total
                    .fetch_add(saturating_u64_from_usize(flushed_points), Ordering::Relaxed);
                Ok(())
            }
            Err(err) => {
                self.observability
                    .flush
                    .active_flush_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    fn collect_points_for_series_into(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        if self.try_collect_points_for_series_with_merge(series_id, start, end, out)? {
            self.observability
                .query
                .merge_path_queries_total
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.observability
            .query
            .append_sort_path_queries_total
            .fetch_add(1, Ordering::Relaxed);
        self.collect_points_for_series_append_sort_path(series_id, start, end, out)
    }

    fn try_collect_points_for_series_with_merge(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<bool> {
        let _visibility_guard = self.flush_visibility_lock.read();
        let mut has_previous_chunk = false;
        let mut has_previous_persisted_chunk = false;
        let mut previous_max_ts = i64::MIN;
        let mut previous_persisted_max_ts = i64::MIN;
        let mut requires_output_validation = false;
        let mut requires_timestamp_dedupe = false;
        let mut requires_exact_dedupe = false;
        let mut persisted_source_sorted = true;
        let mut sealed_source_sorted = true;
        let mut estimated_points = 0usize;

        {
            let persisted_index = self.persisted_index.read();
            let mut persisted_chunks = Vec::<PersistedChunkRef>::new();

            if let Some(chunks) = persisted_index.chunk_refs.get(&series_id) {
                let end_idx = chunks.partition_point(|chunk| chunk.min_ts < end);
                persisted_chunks.reserve(end_idx);

                let mut previous_persisted_source_max_ts = i64::MIN;
                let mut has_previous_persisted_source_chunk = false;
                for chunk_ref in &chunks[..end_idx] {
                    if chunk_ref.max_ts < start {
                        continue;
                    }

                    if has_previous_chunk
                        && chunk_ref.min_ts <= previous_max_ts
                        && has_previous_persisted_chunk
                        && chunk_ref.min_ts <= previous_persisted_max_ts
                    {
                        requires_timestamp_dedupe = true;
                    }
                    if has_previous_persisted_source_chunk
                        && chunk_ref.min_ts < previous_persisted_source_max_ts
                    {
                        persisted_source_sorted = false;
                    }

                    has_previous_chunk = true;
                    has_previous_persisted_chunk = true;
                    previous_max_ts = previous_max_ts.max(chunk_ref.max_ts);
                    previous_persisted_max_ts = previous_persisted_max_ts.max(chunk_ref.max_ts);
                    previous_persisted_source_max_ts =
                        previous_persisted_source_max_ts.max(chunk_ref.max_ts);
                    has_previous_persisted_source_chunk = true;
                    estimated_points =
                        estimated_points.saturating_add(chunk_ref.point_count as usize);
                    persisted_chunks.push(*chunk_ref);
                }
            }
            // Hold active read lock while reading sealed+active to prevent observing a transient
            // "moved out of active but not yet visible in sealed" flush transition.
            let active = self.active_shard(series_id).read();
            let sealed = self.sealed_shard(series_id).read();
            let mut sealed_chunks = Vec::<&Chunk>::new();
            if let Some(chunks) = sealed.get(&series_id) {
                let end_bound = SealedChunkKey::upper_bound_for_min_ts(end);
                let mut previous_sealed_source_max_ts = i64::MIN;
                let mut has_previous_sealed_source_chunk = false;
                for (_, chunk) in chunks.range(..end_bound) {
                    if chunk.header.max_ts < start {
                        continue;
                    }

                    if has_previous_chunk
                        && chunk.header.min_ts <= previous_max_ts
                        && has_previous_persisted_chunk
                        && chunk.header.min_ts <= previous_persisted_max_ts
                    {
                        requires_exact_dedupe = true;
                    }
                    if has_previous_sealed_source_chunk
                        && chunk.header.min_ts < previous_sealed_source_max_ts
                    {
                        sealed_source_sorted = false;
                    }

                    has_previous_chunk = true;
                    previous_max_ts = previous_max_ts.max(chunk.header.max_ts);
                    previous_sealed_source_max_ts =
                        previous_sealed_source_max_ts.max(chunk.header.max_ts);
                    has_previous_sealed_source_chunk = true;

                    // Chunks without encoded payload may be ad-hoc/manual and not guaranteed sorted.
                    if chunk.points.len() > 1 && chunk.encoded_payload.is_empty() {
                        requires_output_validation = true;
                    }
                    estimated_points =
                        estimated_points.saturating_add(chunk.header.point_count as usize);
                    sealed_chunks.push(chunk);
                }
            }

            let mut active_points: &[ChunkPoint] = &[];
            if let Some(state) = active.get(&series_id) {
                active_points = state.builder.points();
                let mut previous_active_ts = i64::MIN;
                let mut has_previous_active = false;

                for point in state.builder.points() {
                    if point.ts < start || point.ts >= end {
                        continue;
                    }

                    if has_previous_persisted_chunk && point.ts <= previous_persisted_max_ts {
                        requires_exact_dedupe = true;
                    }
                    if has_previous_active && point.ts < previous_active_ts {
                        requires_output_validation = true;
                    }

                    has_previous_active = true;
                    previous_active_ts = point.ts;
                    estimated_points = estimated_points.saturating_add(1);
                }
            }
            if requires_output_validation || !persisted_source_sorted || !sealed_source_sorted {
                return Ok(false);
            }

            out.clear();
            out.reserve(estimated_points);

            let mut persisted_cursor = PersistedSourceMergeCursor::new(
                persisted_chunks,
                persisted_index.segment_maps.as_slice(),
                start,
                end,
            );
            let mut sealed_cursor = SealedSourceMergeCursor::new(sealed_chunks, start, end);
            let mut active_cursor = ActiveSourceMergeCursor::new(active_points, start, end);

            merge_sorted_query_sources_into(
                &mut persisted_cursor,
                &mut sealed_cursor,
                &mut active_cursor,
                out,
            )?;
        }

        self.apply_retention_filter(out);
        if requires_timestamp_dedupe {
            dedupe_last_value_per_timestamp(out);
        } else if requires_exact_dedupe {
            dedupe_exact_duplicate_points(out);
        }
        Ok(true)
    }

    fn collect_points_for_series_append_sort_path(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        let _visibility_guard = self.flush_visibility_lock.read();
        out.clear();
        let mut has_overlap = false;
        let mut has_previous_chunk = false;
        let mut has_previous_persisted_chunk = false;
        let mut previous_max_ts = i64::MIN;
        let mut previous_persisted_max_ts = i64::MIN;
        let mut requires_output_validation = false;
        let mut requires_timestamp_dedupe = false;
        let mut requires_exact_dedupe = false;

        {
            let persisted_index = self.persisted_index.read();

            if let Some(chunks) = persisted_index.chunk_refs.get(&series_id) {
                let end_idx = chunks.partition_point(|chunk| chunk.min_ts < end);
                for chunk_ref in &chunks[..end_idx] {
                    if chunk_ref.max_ts < start {
                        continue;
                    }

                    if has_previous_chunk && chunk_ref.min_ts <= previous_max_ts {
                        has_overlap = true;
                        if has_previous_persisted_chunk
                            && chunk_ref.min_ts <= previous_persisted_max_ts
                        {
                            requires_timestamp_dedupe = true;
                        }
                    }

                    has_previous_chunk = true;
                    has_previous_persisted_chunk = true;
                    previous_max_ts = previous_max_ts.max(chunk_ref.max_ts);
                    previous_persisted_max_ts = previous_persisted_max_ts.max(chunk_ref.max_ts);

                    let payload = persisted_chunk_payload(
                        persisted_index.segment_maps.as_slice(),
                        chunk_ref,
                    )?;
                    decode_encoded_chunk_payload_in_range_into(
                        EncodedChunkDescriptor {
                            lane: chunk_ref.lane,
                            ts_codec: chunk_ref.ts_codec,
                            value_codec: chunk_ref.value_codec,
                            point_count: chunk_ref.point_count as usize,
                        },
                        payload,
                        start,
                        end,
                        out,
                    )?;
                }
            }
        }

        // Hold active read lock while reading sealed+active to prevent observing a transient
        // "moved out of active but not yet visible in sealed" flush transition.
        let active = self.active_shard(series_id).read();
        {
            let sealed = self.sealed_shard(series_id).read();
            if let Some(chunks) = sealed.get(&series_id) {
                let end_bound = SealedChunkKey::upper_bound_for_min_ts(end);
                for (_, chunk) in chunks.range(..end_bound) {
                    if chunk.header.max_ts < start {
                        continue;
                    }

                    if has_previous_chunk && chunk.header.min_ts <= previous_max_ts {
                        has_overlap = true;
                        if has_previous_persisted_chunk
                            && chunk.header.min_ts <= previous_persisted_max_ts
                        {
                            requires_exact_dedupe = true;
                        }
                    }

                    has_previous_chunk = true;
                    previous_max_ts = previous_max_ts.max(chunk.header.max_ts);

                    // Chunks without encoded payload may be ad-hoc/manual and not guaranteed sorted.
                    if chunk.points.len() > 1 && chunk.encoded_payload.is_empty() {
                        requires_output_validation = true;
                    }

                    decode_chunk_points_in_range_into(chunk, start, end, out)?;
                }
            }
        }

        if let Some(state) = active.get(&series_id) {
            let mut previous_active_ts = i64::MIN;
            let mut has_previous_active = false;

            for point in state.builder.points() {
                if point.ts < start || point.ts >= end {
                    continue;
                }

                if has_previous_chunk && point.ts <= previous_max_ts {
                    has_overlap = true;
                }
                if has_previous_persisted_chunk && point.ts <= previous_persisted_max_ts {
                    requires_exact_dedupe = true;
                }
                if has_previous_active && point.ts < previous_active_ts {
                    requires_output_validation = true;
                }

                has_previous_active = true;
                previous_active_ts = point.ts;
                out.push(DataPoint::new(point.ts, point.value.clone()));
            }
        }

        self.apply_retention_filter(out);

        if has_overlap || requires_output_validation {
            if !points_are_sorted_by_timestamp(out) {
                out.sort_by_key(|point| point.timestamp);
            }
            if requires_timestamp_dedupe {
                dedupe_last_value_per_timestamp(out);
            } else if requires_exact_dedupe {
                dedupe_exact_duplicate_points(out);
            }
        }
        Ok(())
    }

    fn collect_points_for_series(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        let mut out = Vec::new();
        self.collect_points_for_series_into(series_id, start, end, &mut out)?;
        Ok(out)
    }

    fn intersect_candidates(
        candidates: &mut BTreeSet<SeriesId>,
        filter: Option<&BTreeSet<SeriesId>>,
    ) {
        match filter {
            Some(filter) => candidates.retain(|series_id| filter.contains(series_id)),
            None => candidates.clear(),
        }
    }

    fn subtract_candidates(
        candidates: &mut BTreeSet<SeriesId>,
        filter: Option<&BTreeSet<SeriesId>>,
    ) {
        if let Some(filter) = filter {
            candidates.retain(|series_id| !filter.contains(series_id));
        }
    }

    fn apply_postings_matcher_to_candidates(
        registry: &SeriesRegistry,
        candidates: &mut BTreeSet<SeriesId>,
        matcher: &CompiledSeriesMatcher,
    ) -> Result<()> {
        if candidates.is_empty() {
            return Ok(());
        }

        if matcher.name == "__name__" {
            match matcher.op {
                SeriesMatcherOp::Equal => {
                    Self::intersect_candidates(
                        candidates,
                        registry.series_id_postings_for_metric(&matcher.value),
                    );
                }
                SeriesMatcherOp::NotEqual => {
                    Self::subtract_candidates(
                        candidates,
                        registry.series_id_postings_for_metric(&matcher.value),
                    );
                }
                SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {
                    let Some(regex) = matcher.regex.as_ref() else {
                        return Err(TsinkError::InvalidConfiguration(
                            "regex matcher missing compiled regex".to_string(),
                        ));
                    };

                    let mut matched = BTreeSet::<SeriesId>::new();
                    for (metric, series_ids) in registry.metric_postings_entries() {
                        if regex.is_match(metric) {
                            matched.extend(series_ids.iter().copied());
                        }
                    }

                    if matcher.op == SeriesMatcherOp::RegexMatch {
                        Self::intersect_candidates(candidates, Some(&matched));
                    } else {
                        Self::subtract_candidates(candidates, Some(&matched));
                    }
                }
            }

            return Ok(());
        }

        match matcher.op {
            SeriesMatcherOp::Equal => {
                Self::intersect_candidates(
                    candidates,
                    registry.postings_for_label(&matcher.name, &matcher.value),
                );
            }
            SeriesMatcherOp::NotEqual => {
                Self::subtract_candidates(
                    candidates,
                    registry.postings_for_label(&matcher.name, &matcher.value),
                );
            }
            SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {
                let Some(label_name_id) = registry.label_name_id(&matcher.name) else {
                    if matcher.op == SeriesMatcherOp::RegexMatch {
                        candidates.clear();
                    }
                    return Ok(());
                };

                let Some(regex) = matcher.regex.as_ref() else {
                    return Err(TsinkError::InvalidConfiguration(
                        "regex matcher missing compiled regex".to_string(),
                    ));
                };

                let mut matched = BTreeSet::<SeriesId>::new();
                for (pair, series_ids) in registry.postings_entries() {
                    if pair.name_id != label_name_id {
                        continue;
                    }
                    let Some(label_value) = registry.label_value_by_id(pair.value_id) else {
                        continue;
                    };
                    if regex.is_match(label_value) {
                        matched.extend(series_ids.iter().copied());
                    }
                }

                if matcher.op == SeriesMatcherOp::RegexMatch {
                    Self::intersect_candidates(candidates, Some(&matched));
                } else {
                    Self::subtract_candidates(candidates, Some(&matched));
                }
            }
        }

        Ok(())
    }

    fn select_series_candidate_ids(
        &self,
        selection: &SeriesSelection,
        compiled_matchers: &[CompiledSeriesMatcher],
    ) -> Result<Vec<SeriesId>> {
        let registry = self.registry.read();

        let mut candidates = if let Some(metric) = selection.metric.as_ref() {
            registry
                .series_id_postings_for_metric(metric)
                .cloned()
                .unwrap_or_default()
        } else {
            registry
                .all_series_ids()
                .into_iter()
                .collect::<BTreeSet<_>>()
        };

        for matcher in compiled_matchers {
            Self::apply_postings_matcher_to_candidates(&registry, &mut candidates, matcher)?;
            if candidates.is_empty() {
                break;
            }
        }

        Ok(candidates.into_iter().collect())
    }

    fn series_ids_with_data_in_time_range(
        &self,
        series_ids: Vec<SeriesId>,
        start: i64,
        end: i64,
    ) -> Vec<SeriesId> {
        if series_ids.is_empty() {
            return series_ids;
        }

        let _visibility_guard = self.flush_visibility_lock.read();
        let persisted_index = self.persisted_index.read();
        let mut filtered = Vec::with_capacity(series_ids.len());

        for series_id in series_ids {
            let persisted_overlap =
                persisted_index
                    .chunk_refs
                    .get(&series_id)
                    .is_some_and(|chunks| {
                        let end_idx = chunks.partition_point(|chunk| chunk.min_ts < end);
                        chunks[..end_idx]
                            .iter()
                            .any(|chunk| chunk.max_ts >= start && chunk.min_ts < end)
                    });

            if persisted_overlap {
                filtered.push(series_id);
                continue;
            }

            let active = self.active_shard(series_id).read();
            {
                let sealed = self.sealed_shard(series_id).read();
                let sealed_overlap = sealed.get(&series_id).is_some_and(|chunks| {
                    let end_bound = SealedChunkKey::upper_bound_for_min_ts(end);
                    chunks
                        .range(..end_bound)
                        .any(|(_, chunk)| chunk.header.max_ts >= start && chunk.header.min_ts < end)
                });
                if sealed_overlap {
                    filtered.push(series_id);
                    continue;
                }
            }

            let active_overlap = active.get(&series_id).is_some_and(|state| {
                state
                    .builder
                    .points()
                    .iter()
                    .any(|point| point.ts >= start && point.ts < end)
            });
            if active_overlap {
                filtered.push(series_id);
            }
        }

        filtered
    }

    fn select_series_impl(&self, selection: &SeriesSelection) -> Result<Vec<MetricSeries>> {
        if let Some(metric) = selection.metric.as_ref() {
            validate_metric(metric)?;
        }

        let time_range = selection.normalized_time_range()?;
        let compiled_matchers = compile_series_matchers(&selection.matchers)?;
        let mut candidate_ids = self.select_series_candidate_ids(selection, &compiled_matchers)?;

        if let Some((start, end)) = time_range {
            candidate_ids = self.series_ids_with_data_in_time_range(candidate_ids, start, end);
        }

        let registry = self.registry.read();
        let mut series = Vec::with_capacity(candidate_ids.len());
        for series_id in candidate_ids {
            if let Some(key) = registry.decode_series_key(series_id) {
                series.push(MetricSeries {
                    name: key.metric,
                    labels: key.labels,
                });
            }
        }

        series.sort();
        Ok(series)
    }

    fn validate_select_request(metric: &str, labels: &[Label], start: i64, end: i64) -> Result<()> {
        validate_metric(metric)?;
        validate_labels(labels)?;
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        Ok(())
    }

    fn select_into_impl(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        let Some(series_id) = self
            .registry
            .read()
            .resolve_existing(metric, labels)
            .map(|resolution| resolution.series_id)
        else {
            out.clear();
            return Ok(());
        };
        self.collect_points_for_series_into(series_id, start, end, out)
    }

    fn validate_series_lane_compatible(&self, series_id: SeriesId, lane: ValueLane) -> Result<()> {
        if let Some(active_lane) = self
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map(|state| state.lane)
        {
            if active_lane != lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(active_lane).to_string(),
                    actual: lane_name(lane).to_string(),
                });
            }
        }

        if let Some(sealed_lane) = self
            .sealed_shard(series_id)
            .read()
            .get(&series_id)
            .and_then(|chunks| chunks.last_key_value().map(|(_, chunk)| chunk))
            .map(|chunk| chunk.header.lane)
        {
            if sealed_lane != lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(sealed_lane).to_string(),
                    actual: lane_name(lane).to_string(),
                });
            }
        }

        if let Some(persisted_lane) = self
            .persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .and_then(|chunks| chunks.last())
            .map(|chunk_ref| chunk_ref.lane)
        {
            if persisted_lane != lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(persisted_lane).to_string(),
                    actual: lane_name(lane).to_string(),
                });
            }
        }

        Ok(())
    }

    fn collect_pending_series_lanes(
        points: &[PendingPoint],
    ) -> Result<BTreeMap<SeriesId, ValueLane>> {
        let mut series_lanes = BTreeMap::new();

        for point in points {
            if let Some(existing_lane) = series_lanes.get(&point.series_id) {
                if *existing_lane != point.lane {
                    return Err(TsinkError::ValueTypeMismatch {
                        expected: lane_name(*existing_lane).to_string(),
                        actual: lane_name(point.lane).to_string(),
                    });
                }
            } else {
                series_lanes.insert(point.series_id, point.lane);
            }
        }

        Ok(series_lanes)
    }

    fn reserve_series_lanes(&self, points: &[PendingPoint]) -> Result<Vec<SeriesId>> {
        let series_lanes = Self::collect_pending_series_lanes(points)?;
        let mut lanes_by_shard = BTreeMap::<usize, Vec<(SeriesId, ValueLane)>>::new();
        for (series_id, lane) in series_lanes {
            lanes_by_shard
                .entry(Self::series_shard_idx(series_id))
                .or_default()
                .push((series_id, lane));
        }

        let mut shard_guards = Vec::with_capacity(lanes_by_shard.len());
        for (shard_idx, entries) in lanes_by_shard {
            shard_guards.push((entries, self.active_builders[shard_idx].write()));
        }

        for (entries, active) in &shard_guards {
            for (series_id, lane) in entries {
                if let Some(state) = active.get(series_id) {
                    if state.lane != *lane {
                        return Err(TsinkError::ValueTypeMismatch {
                            expected: lane_name(state.lane).to_string(),
                            actual: lane_name(*lane).to_string(),
                        });
                    }
                }
            }
        }

        let mut reserved = Vec::new();
        for (entries, active) in &mut shard_guards {
            for (series_id, lane) in entries {
                if active.contains_key(series_id) {
                    continue;
                }
                active.insert(
                    *series_id,
                    ActiveSeriesState::new(*series_id, *lane, self.chunk_point_cap),
                );
                reserved.push(*series_id);
            }
        }

        Ok(reserved)
    }

    fn rollback_empty_series_lane_reservations(&self, series_ids: &[SeriesId]) {
        if series_ids.is_empty() {
            return;
        }

        let mut ids_by_shard = BTreeMap::<usize, Vec<SeriesId>>::new();
        for &series_id in series_ids {
            ids_by_shard
                .entry(Self::series_shard_idx(series_id))
                .or_default()
                .push(series_id);
        }

        for (shard_idx, shard_series_ids) in ids_by_shard {
            let mut active = self.active_builders[shard_idx].write();
            for series_id in shard_series_ids {
                let should_remove = active
                    .get(&series_id)
                    .map(|state| state.builder.is_empty())
                    .unwrap_or(false);
                if should_remove {
                    active.remove(&series_id);
                }
            }
        }
    }

    fn append_point_to_series(
        &self,
        series_id: SeriesId,
        lane: ValueLane,
        ts: i64,
        value: Value,
    ) -> Result<()> {
        let finalized = {
            let mut active = self.active_shard(series_id).write();
            let state = active
                .entry(series_id)
                .or_insert_with(|| ActiveSeriesState::new(series_id, lane, self.chunk_point_cap));

            if state.lane != lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(state.lane).to_string(),
                    actual: lane_name(lane).to_string(),
                });
            }

            let mut finalized = Vec::new();
            if let Some(chunk) = state.rotate_partition_if_needed(ts, self.partition_window)? {
                finalized.push(chunk);
            }
            state.builder.append(ts, value);
            if let Some(chunk) = state.rotate_full_if_needed()? {
                finalized.push(chunk);
            }
            finalized
        };

        for chunk in finalized {
            self.append_sealed_chunk(series_id, chunk);
        }
        self.update_max_observed_timestamp(ts);
        self.mark_materialized_series_ids(std::iter::once(series_id));

        Ok(())
    }

    fn ingest_pending_points(&self, points: Vec<PendingPoint>) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }

        let mut points_by_shard: [Vec<PendingPoint>; IN_MEMORY_SHARD_COUNT] =
            std::array::from_fn(|_| Vec::new());
        for point in points {
            let shard_idx = Self::series_shard_idx(point.series_id);
            points_by_shard[shard_idx].push(point);
        }

        for (shard_idx, shard_points) in points_by_shard.into_iter().enumerate() {
            if shard_points.is_empty() {
                continue;
            }
            self.ingest_pending_points_for_shard(shard_idx, shard_points)?;
        }

        Ok(())
    }

    fn ingest_pending_points_for_shard(
        &self,
        shard_idx: usize,
        shard_points: Vec<PendingPoint>,
    ) -> Result<()> {
        if shard_points.is_empty() {
            return Ok(());
        }

        let mut finalized = Vec::<(SeriesId, Chunk)>::new();
        let mut materialized_series_ids = BTreeSet::new();
        {
            let mut active = self.active_builders[shard_idx].write();

            for point in shard_points {
                let state = active.entry(point.series_id).or_insert_with(|| {
                    ActiveSeriesState::new(point.series_id, point.lane, self.chunk_point_cap)
                });

                if state.lane != point.lane {
                    return Err(TsinkError::ValueTypeMismatch {
                        expected: lane_name(state.lane).to_string(),
                        actual: lane_name(point.lane).to_string(),
                    });
                }
                materialized_series_ids.insert(point.series_id);

                if let Some(chunk) =
                    state.rotate_partition_if_needed(point.ts, self.partition_window)?
                {
                    finalized.push((point.series_id, chunk));
                }

                state.builder.append(point.ts, point.value);
                self.update_max_observed_timestamp(point.ts);

                if let Some(chunk) = state.rotate_full_if_needed()? {
                    finalized.push((point.series_id, chunk));
                }
            }
        }

        self.mark_materialized_series_ids(materialized_series_ids);

        if finalized.is_empty() {
            return Ok(());
        }

        let mut sealed = self.sealed_chunks[shard_idx].write();
        for (series_id, chunk) in finalized {
            let sequence = self.next_chunk_sequence.fetch_add(1, Ordering::SeqCst);
            let key = SealedChunkKey::from_chunk(&chunk, sequence);
            sealed.entry(series_id).or_default().insert(key, chunk);
        }

        Ok(())
    }

    fn group_pending_point_indexes_by_series(
        points: &[PendingPoint],
    ) -> Result<BTreeMap<SeriesId, (ValueLane, Vec<usize>)>> {
        let mut grouped: BTreeMap<SeriesId, (ValueLane, Vec<usize>)> = BTreeMap::new();

        for (idx, point) in points.iter().enumerate() {
            let entry = grouped
                .entry(point.series_id)
                .or_insert_with(|| (point.lane, Vec::new()));

            if entry.0 != point.lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(entry.0).to_string(),
                    actual: lane_name(point.lane).to_string(),
                });
            }

            entry.1.push(idx);
        }

        Ok(grouped)
    }

    fn validate_pending_point_families(
        &self,
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
    ) -> Result<()> {
        for (series_id, (lane, indexes)) in grouped {
            let Some((&first_idx, remaining)) = indexes.split_first() else {
                continue;
            };

            let first_point = &points[first_idx];
            let first_family = value_family_for_lane(&first_point.value, *lane)?;

            for idx in remaining {
                let point = &points[*idx];
                let family = value_family_for_lane(&point.value, *lane)?;
                if family != first_family {
                    return Err(TsinkError::ValueTypeMismatch {
                        expected: value_family_name(first_family).to_string(),
                        actual: point.value.kind().to_string(),
                    });
                }
            }

            if let Some(state) = self.active_shard(*series_id).read().get(series_id) {
                for existing_point in state.builder.points() {
                    let existing_family = value_family_for_lane(&existing_point.value, *lane)?;
                    if existing_family != first_family {
                        return Err(TsinkError::ValueTypeMismatch {
                            expected: value_family_name(existing_family).to_string(),
                            actual: first_point.value.kind().to_string(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn encode_wal_batches(
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
    ) -> Result<Vec<SamplesBatchFrame>> {
        let mut batches = Vec::with_capacity(grouped.len());
        for (series_id, (lane, indexes)) in grouped {
            let mut chunk_points = Vec::with_capacity(indexes.len());
            for idx in indexes {
                let point = &points[*idx];
                chunk_points.push(ChunkPoint {
                    ts: point.ts,
                    value: point.value.clone(),
                });
            }

            batches.push(SamplesBatchFrame::from_points(
                *series_id,
                *lane,
                &chunk_points,
            )?);
        }

        Ok(batches)
    }

    fn replay_from_wal(&self, replay_highwater: WalHighWatermark) -> Result<()> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };

        self.observability
            .wal
            .replay_runs_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let mut replay_frames = 0u64;
        let mut replay_series_defs = 0u64;
        let mut replay_sample_batches = 0u64;
        let mut replay_points = 0u64;

        let replay_result = (|| -> Result<()> {
            let mut stream = wal.replay_stream_after(replay_highwater)?;
            while let Some(frame) = stream.next_frame()? {
                replay_frames = replay_frames.saturating_add(1);
                match frame {
                    ReplayFrame::SeriesDefinition(definition) => {
                        replay_series_defs = replay_series_defs.saturating_add(1);
                        self.registry.write().register_series_with_id(
                            definition.series_id,
                            &definition.metric,
                            &definition.labels,
                        )?;
                    }
                    ReplayFrame::Samples(batches) => {
                        replay_sample_batches =
                            replay_sample_batches.saturating_add(batches.len() as u64);
                        for batch in batches {
                            let points = batch.decode_points()?;
                            replay_points = replay_points
                                .saturating_add(saturating_u64_from_usize(points.len()));
                            for point in points {
                                self.append_point_to_series(
                                    batch.series_id,
                                    batch.lane,
                                    point.ts,
                                    point.value,
                                )?;
                            }
                        }
                    }
                }
            }

            Ok(())
        })();

        self.observability
            .wal
            .replay_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match replay_result {
            Ok(()) => {
                self.observability
                    .wal
                    .replay_frames_total
                    .fetch_add(replay_frames, Ordering::Relaxed);
                self.observability
                    .wal
                    .replay_series_definitions_total
                    .fetch_add(replay_series_defs, Ordering::Relaxed);
                self.observability
                    .wal
                    .replay_sample_batches_total
                    .fetch_add(replay_sample_batches, Ordering::Relaxed);
                self.observability
                    .wal
                    .replay_points_total
                    .fetch_add(replay_points, Ordering::Relaxed);
                Ok(())
            }
            Err(err) => {
                self.observability
                    .wal
                    .replay_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    fn apply_loaded_segment_indexes(
        &self,
        loaded: crate::engine::segment::LoadedSegmentIndexes,
    ) -> Result<()> {
        {
            let mut registry = self.registry.write();
            for series in &loaded.series {
                registry.register_series_with_id(
                    series.series_id,
                    &series.metric,
                    &series.labels,
                )?;
            }
        }

        let mut persisted_refs = HashMap::<SeriesId, Vec<PersistedChunkRef>>::new();
        let mut persisted_maps = Vec::<Arc<PlatformMmap>>::new();
        let mut sequence = 1u64;
        let mut loaded_max_timestamp = i64::MIN;

        {
            for indexed_segment in loaded.indexed_segments {
                let segment_slot = persisted_maps.len();
                persisted_maps.push(Arc::new(indexed_segment.chunks_mmap));

                for entry in indexed_segment.chunk_index.entries {
                    loaded_max_timestamp = loaded_max_timestamp.max(entry.max_ts);
                    persisted_refs
                        .entry(entry.series_id)
                        .or_default()
                        .push(PersistedChunkRef {
                            min_ts: entry.min_ts,
                            max_ts: entry.max_ts,
                            point_count: entry.point_count,
                            sequence,
                            chunk_offset: entry.chunk_offset,
                            chunk_len: entry.chunk_len,
                            lane: entry.lane,
                            ts_codec: entry.ts_codec,
                            value_codec: entry.value_codec,
                            segment_slot,
                        });
                    sequence = sequence.saturating_add(1);
                }
            }
        }

        for chunks in persisted_refs.values_mut() {
            chunks.sort_by_key(|chunk| {
                (
                    chunk.min_ts,
                    chunk.max_ts,
                    chunk.point_count,
                    chunk.sequence,
                    chunk.chunk_offset,
                )
            });
        }

        self.mark_materialized_series_ids(persisted_refs.keys().copied());

        {
            let mut persisted_index = self.persisted_index.write();
            persisted_index.chunk_refs = persisted_refs;
            persisted_index.segment_maps = persisted_maps;
        }

        if loaded_max_timestamp != i64::MIN {
            self.update_max_observed_timestamp(loaded_max_timestamp);
        }

        self.next_segment_id
            .store(loaded.next_segment_id.max(1), Ordering::SeqCst);
        Ok(())
    }

    fn rollback_published_segments(&self, segment_roots: &[PathBuf]) -> Result<()> {
        for root in segment_roots.iter().rev() {
            match std::fs::remove_dir_all(root) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(TsinkError::IoWithPath {
                        path: root.clone(),
                        source: err,
                    });
                }
            }
        }

        Ok(())
    }

    fn reset_wal_with_stats(&self, wal: &FramedWal) -> Result<()> {
        match wal.reset() {
            Ok(()) => {
                self.observability
                    .wal
                    .resets_total
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(err) => {
                self.observability
                    .wal
                    .reset_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    fn persist_segment(&self, include_wal_highwater: bool) -> Result<bool> {
        self.observability
            .flush
            .persist_runs_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let persist_result = (|| -> Result<(bool, usize, usize, usize, usize)> {
            if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
                return Ok((false, 0, 0, 0, 0));
            }

            let wal_highwater = if include_wal_highwater {
                self.wal
                    .as_ref()
                    .map(|wal| wal.current_highwater())
                    .unwrap_or_default()
            } else {
                WalHighWatermark::default()
            };

            let (delta_chunks, delta_watermarks) = {
                let persisted = self.persisted_chunk_watermarks.read();

                let mut delta = HashMap::new();
                let mut watermarks = HashMap::new();
                for shard in &self.sealed_chunks {
                    let sealed = shard.read();
                    for (series_id, chunks) in sealed.iter() {
                        let persisted_sequence = persisted.get(series_id).copied().unwrap_or(0);
                        let mut updates = Vec::new();
                        let mut max_sequence = persisted_sequence;
                        for (key, chunk) in chunks {
                            if key.sequence <= persisted_sequence {
                                continue;
                            }
                            max_sequence = max_sequence.max(key.sequence);
                            updates.push(chunk.clone());
                        }
                        if !updates.is_empty() {
                            delta.insert(*series_id, updates);
                            watermarks.insert(*series_id, max_sequence);
                        }
                    }
                }
                (delta, watermarks)
            };

            if delta_chunks.is_empty() {
                if let Some(wal) = &self.wal {
                    self.reset_wal_with_stats(wal)?;
                }
                return Ok((false, 0, 0, 0, 0));
            }

            let persisted_series = delta_chunks.len();
            let persisted_chunks = delta_chunks.values().map(std::vec::Vec::len).sum::<usize>();
            let persisted_points = delta_chunks
                .values()
                .flat_map(|chunks| chunks.iter())
                .map(|chunk| chunk.header.point_count as usize)
                .sum::<usize>();

            let mut numeric_chunks = HashMap::new();
            let mut blob_chunks = HashMap::new();
            let mut numeric_watermarks = HashMap::new();
            let mut blob_watermarks = HashMap::new();
            for (series_id, chunks) in &delta_chunks {
                let Some(first) = chunks.first() else {
                    continue;
                };
                let Some(watermark) = delta_watermarks.get(series_id).copied() else {
                    continue;
                };

                match first.header.lane {
                    ValueLane::Numeric => {
                        numeric_chunks.insert(*series_id, chunks.clone());
                        numeric_watermarks.insert(*series_id, watermark);
                    }
                    ValueLane::Blob => {
                        blob_chunks.insert(*series_id, chunks.clone());
                        blob_watermarks.insert(*series_id, watermark);
                    }
                }
            }

            if !numeric_chunks.is_empty() && self.numeric_lane_path.is_none() {
                return Err(TsinkError::InvalidConfiguration(
                    "cannot persist numeric chunks without numeric lane path".to_string(),
                ));
            }
            if !blob_chunks.is_empty() && self.blob_lane_path.is_none() {
                return Err(TsinkError::InvalidConfiguration(
                    "cannot persist blob chunks without blob lane path".to_string(),
                ));
            }

            let persisted_segments = {
                let registry = self.registry.read();
                let mut published_segment_roots = Vec::new();

                let persist_result = (|| -> Result<()> {
                    if let (Some(path), false) =
                        (&self.numeric_lane_path, numeric_chunks.is_empty())
                    {
                        let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
                        let writer = SegmentWriter::new(path, 0, segment_id)?;
                        writer.write_segment_with_wal_highwater(
                            &registry,
                            &numeric_chunks,
                            wal_highwater,
                        )?;
                        published_segment_roots.push(writer.layout().root.clone());
                    }

                    if let (Some(path), false) = (&self.blob_lane_path, blob_chunks.is_empty()) {
                        let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
                        let writer = SegmentWriter::new(path, 0, segment_id)?;
                        writer.write_segment_with_wal_highwater(
                            &registry,
                            &blob_chunks,
                            wal_highwater,
                        )?;
                        published_segment_roots.push(writer.layout().root.clone());
                    }

                    Ok(())
                })();

                if let Err(persist_err) = persist_result {
                    if let Err(rollback_err) =
                        self.rollback_published_segments(&published_segment_roots)
                    {
                        return Err(TsinkError::Other(format!(
                            "persist failed and rollback failed: persist={persist_err}, rollback={rollback_err}"
                        )));
                    }
                    return Err(persist_err);
                }

                published_segment_roots.len()
            };

            let mut flushed_watermarks = numeric_watermarks;
            flushed_watermarks.extend(blob_watermarks);
            self.mark_persisted_chunk_watermarks(&flushed_watermarks);

            if let Some(wal) = &self.wal {
                self.reset_wal_with_stats(wal)?;
            }

            Ok((
                true,
                persisted_series,
                persisted_chunks,
                persisted_points,
                persisted_segments,
            ))
        })();

        self.observability
            .flush
            .persist_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match persist_result {
            Ok((persisted, series, chunks, points, segments)) => {
                if persisted {
                    self.observability
                        .flush
                        .persist_success_total
                        .fetch_add(1, Ordering::Relaxed);
                    self.observability
                        .flush
                        .persisted_series_total
                        .fetch_add(saturating_u64_from_usize(series), Ordering::Relaxed);
                    self.observability
                        .flush
                        .persisted_chunks_total
                        .fetch_add(saturating_u64_from_usize(chunks), Ordering::Relaxed);
                    self.observability
                        .flush
                        .persisted_points_total
                        .fetch_add(saturating_u64_from_usize(points), Ordering::Relaxed);
                    self.observability
                        .flush
                        .persisted_segments_total
                        .fetch_add(saturating_u64_from_usize(segments), Ordering::Relaxed);
                } else {
                    self.observability
                        .flush
                        .persist_noop_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(persisted)
            }
            Err(err) => {
                self.observability
                    .flush
                    .persist_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    fn compact_compactors(
        numeric_compactor: Option<&Compactor>,
        blob_compactor: Option<&Compactor>,
        observability: Option<&StorageObservabilityCounters>,
    ) -> Result<bool> {
        let mut compacted = false;
        if let Some(compactor) = numeric_compactor {
            compacted |= Self::run_compactor_once(compactor, observability)?;
        }
        if let Some(compactor) = blob_compactor {
            compacted |= Self::run_compactor_once(compactor, observability)?;
        }
        Ok(compacted)
    }

    fn run_compactor_once(
        compactor: &Compactor,
        observability: Option<&StorageObservabilityCounters>,
    ) -> Result<bool> {
        let started = Instant::now();
        match compactor.compact_once_with_stats() {
            Ok(stats) => {
                if let Some(obs) = observability {
                    obs.record_compaction_result(stats, elapsed_nanos_u64(started));
                }
                Ok(stats.compacted)
            }
            Err(err) => {
                if let Some(obs) = observability {
                    obs.record_compaction_error(elapsed_nanos_u64(started));
                }
                Err(err)
            }
        }
    }

    fn compact_until_settled(&self, max_passes: usize) -> Result<usize> {
        let _compaction_guard = self.compaction_lock.lock();
        let mut passes = 0usize;
        for _ in 0..max_passes.max(1) {
            if !Self::compact_compactors(
                self.numeric_compactor.as_ref(),
                self.blob_compactor.as_ref(),
                Some(self.observability.as_ref()),
            )? {
                break;
            }
            passes = passes.saturating_add(1);
        }
        Ok(passes)
    }
}

impl Storage for ChunkStorage {
    fn insert_rows(&self, rows: &[Row]) -> Result<()> {
        self.ensure_open()?;
        let write_permit = self.write_limiter.try_acquire_for(self.write_timeout)?;
        // A write may pass the first lifecycle check and then block on permits while close starts.
        // Re-check after acquiring a permit so shutdown cannot race new writes through.
        self.ensure_open()?;
        // Serialize writers that touch the same metric shard while allowing unrelated metrics
        // to progress concurrently through WAL and ingestion work.
        let _registry_write_txn_shards = self.lock_registry_write_shards_for_rows(rows);

        let mut pending_points = Vec::with_capacity(rows.len());
        let mut new_series_defs = Vec::new();
        let mut reserved_series = Vec::new();
        let mut created_series = Vec::<SeriesResolution>::new();
        {
            let mut registry = self.registry.write();

            if let Err(err) = (|| -> Result<()> {
                let cardinality_limit = self.cardinality_limit_value();
                if cardinality_limit != usize::MAX {
                    let current = registry.series_count();
                    let requested = Self::projected_new_series_count(&registry, rows)?;
                    if requested > 0 && current.saturating_add(requested) > cardinality_limit {
                        return Err(TsinkError::CardinalityLimitExceeded {
                            limit: cardinality_limit,
                            current,
                            requested,
                        });
                    }
                }

                for row in rows {
                    let data_point = row.data_point();
                    let lane = lane_for_value(&data_point.value);
                    let resolution = registry.resolve_or_insert(row.metric(), row.labels())?;

                    if resolution.created {
                        created_series.push(resolution.clone());
                        new_series_defs.push(SeriesDefinitionFrame {
                            series_id: resolution.series_id,
                            metric: row.metric().to_string(),
                            labels: row.labels().to_vec(),
                        });
                    }

                    pending_points.push(PendingPoint {
                        series_id: resolution.series_id,
                        lane,
                        ts: data_point.timestamp,
                        value: data_point.value.clone(),
                    });
                }

                Ok(())
            })() {
                registry.rollback_created_series(&created_series);
                return Err(err);
            }
        }

        let mut ingest_committed = false;
        let write_result = (|| -> Result<()> {
            for point in &pending_points {
                self.validate_series_lane_compatible(point.series_id, point.lane)?;
            }

            let grouped_points = Self::group_pending_point_indexes_by_series(&pending_points)?;
            self.validate_pending_point_families(&pending_points, &grouped_points)?;
            self.validate_points_against_retention(&pending_points)?;
            let wal_batches = if self.wal.is_some() {
                Some(Self::encode_wal_batches(&pending_points, &grouped_points)?)
            } else {
                None
            };
            let estimated_memory_growth =
                self.estimate_write_memory_growth_bytes(&pending_points, &grouped_points);
            let estimated_wal_growth = self.estimate_write_wal_growth_bytes(
                &new_series_defs,
                wal_batches.as_deref().unwrap_or(&[]),
            )?;
            self.enforce_admission_controls(estimated_memory_growth, estimated_wal_growth)?;
            reserved_series = self.reserve_series_lanes(&pending_points)?;

            if let (Some(wal), Some(batches)) = (&self.wal, wal_batches.as_deref()) {
                for definition in &new_series_defs {
                    if let Err(err) = wal.append_series_definition(definition) {
                        self.observability
                            .wal
                            .append_errors_total
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(err);
                    }
                }

                if let Err(err) = wal.append_samples(batches) {
                    self.observability
                        .wal
                        .append_errors_total
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(err);
                }

                let batch_points = batches
                    .iter()
                    .map(|batch| batch.point_count as usize)
                    .sum::<usize>();
                self.observability
                    .wal
                    .append_series_definitions_total
                    .fetch_add(
                        saturating_u64_from_usize(new_series_defs.len()),
                        Ordering::Relaxed,
                    );
                self.observability
                    .wal
                    .append_sample_batches_total
                    .fetch_add(saturating_u64_from_usize(batches.len()), Ordering::Relaxed);
                self.observability
                    .wal
                    .append_points_total
                    .fetch_add(saturating_u64_from_usize(batch_points), Ordering::Relaxed);
                self.observability
                    .wal
                    .append_bytes_total
                    .fetch_add(estimated_wal_growth, Ordering::Relaxed);
            }
            self.ingest_pending_points(std::mem::take(&mut pending_points))?;
            ingest_committed = true;
            Ok(())
        })();

        if let Err(err) = write_result {
            if !ingest_committed {
                self.rollback_empty_series_lane_reservations(&reserved_series);
                let mut registry = self.registry.write();
                registry.rollback_created_series(&created_series);
            } else {
                self.notify_flush_thread();
            }
            if self.memory_budget_value() != usize::MAX {
                self.refresh_memory_usage();
            }
            return Err(err);
        }

        if self.memory_budget_value() != usize::MAX {
            drop(write_permit);
            self.refresh_memory_usage();
            self.enforce_memory_budget_if_needed()?;
        }

        self.notify_flush_thread();
        Ok(())
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        self.observability
            .query
            .select_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<DataPoint>> {
            self.ensure_open()?;
            Self::validate_select_request(metric, labels, start, end)?;

            let mut out = Vec::new();
            self.select_into_impl(metric, labels, start, end, &mut out)?;
            Ok(out)
        })();

        self.observability
            .query
            .select_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(points) => {
                self.observability
                    .query
                    .select_points_returned_total
                    .fetch_add(saturating_u64_from_usize(points.len()), Ordering::Relaxed);
                Ok(points)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    fn select_into(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        self.ensure_open()?;
        Self::validate_select_request(metric, labels, start, end)?;
        self.select_into_impl(metric, labels, start, end, out)
    }

    fn select_with_options(&self, metric: &str, opts: QueryOptions) -> Result<Vec<DataPoint>> {
        self.observability
            .query
            .select_with_options_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<DataPoint>> {
            self.ensure_open()?;
            validate_metric(metric)?;
            validate_labels(&opts.labels)?;

            if opts.start >= opts.end {
                return Err(TsinkError::InvalidTimeRange {
                    start: opts.start,
                    end: opts.end,
                });
            }

            if let Some(downsample) = opts.downsample {
                if downsample.interval <= 0 {
                    return Err(TsinkError::InvalidConfiguration(
                        "downsample interval must be positive".to_string(),
                    ));
                }
            }

            let mut points = Vec::new();
            self.select_into_impl(metric, &opts.labels, opts.start, opts.end, &mut points)?;

            let aggregation = match (opts.downsample.is_some(), opts.aggregation) {
                (true, Aggregation::None) => Aggregation::Last,
                _ => opts.aggregation,
            };

            let mut processed = if let Some(custom) = opts.custom_aggregation {
                if let Some(downsample) = opts.downsample {
                    downsample_points_with_custom(
                        &points,
                        downsample.interval,
                        custom.as_ref(),
                        opts.start,
                        opts.end,
                    )?
                } else {
                    custom
                        .aggregate_series(&points)?
                        .into_iter()
                        .collect::<Vec<DataPoint>>()
                }
            } else if let Some(downsample) = opts.downsample {
                downsample_points(
                    &points,
                    downsample.interval,
                    aggregation,
                    opts.start,
                    opts.end,
                )?
            } else if aggregation != Aggregation::None {
                aggregate_series(&points, aggregation)?
                    .into_iter()
                    .collect::<Vec<DataPoint>>()
            } else {
                points
            };

            if opts.offset > 0 && opts.offset < processed.len() {
                processed.drain(0..opts.offset);
            } else if opts.offset >= processed.len() {
                processed.clear();
            }

            if let Some(limit) = opts.limit {
                processed.truncate(limit);
            }

            Ok(processed)
        })();

        self.observability
            .query
            .select_with_options_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(points) => {
                self.observability
                    .query
                    .select_with_options_points_returned_total
                    .fetch_add(saturating_u64_from_usize(points.len()), Ordering::Relaxed);
                Ok(points)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_with_options_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.observability
            .query
            .select_all_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            self.ensure_open()?;
            validate_metric(metric)?;

            if start >= end {
                return Err(TsinkError::InvalidTimeRange { start, end });
            }

            let series_ids = self.registry.read().series_ids_for_metric(metric);
            if series_ids.is_empty() {
                return Ok(Vec::new());
            }

            let series_with_labels = {
                let registry = self.registry.read();
                series_ids
                    .into_iter()
                    .map(|series_id| {
                        let labels = registry
                            .decode_series_key(series_id)
                            .map(|key| key.labels)
                            .unwrap_or_default();
                        (series_id, labels)
                    })
                    .collect::<Vec<_>>()
            };

            let mut out = series_with_labels
                .into_par_iter()
                .map(|(series_id, labels)| {
                    let points = self.collect_points_for_series(series_id, start, end)?;
                    if points.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some((labels, points)))
                    }
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();

            out.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(out)
        })();

        self.observability
            .query
            .select_all_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(series) => {
                let points_returned = series.iter().map(|(_, points)| points.len()).sum::<usize>();
                self.observability
                    .query
                    .select_all_series_returned_total
                    .fetch_add(saturating_u64_from_usize(series.len()), Ordering::Relaxed);
                self.observability
                    .query
                    .select_all_points_returned_total
                    .fetch_add(
                        saturating_u64_from_usize(points_returned),
                        Ordering::Relaxed,
                    );
                Ok(series)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_all_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    fn list_metrics(&self) -> Result<Vec<MetricSeries>> {
        self.ensure_open()?;
        let materialized_series_ids = self
            .materialized_series
            .read()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        Ok(self.metric_series_for_ids(materialized_series_ids))
    }

    fn list_metrics_with_wal(&self) -> Result<Vec<MetricSeries>> {
        self.ensure_open()?;
        let all_series_ids = self.registry.read().all_series_ids();
        Ok(self.metric_series_for_ids(all_series_ids))
    }

    fn select_series(&self, selection: &SeriesSelection) -> Result<Vec<MetricSeries>> {
        self.observability
            .query
            .select_series_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<MetricSeries>> {
            self.ensure_open()?;
            self.select_series_impl(selection)
        })();

        self.observability
            .query
            .select_series_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(series) => {
                self.observability
                    .query
                    .select_series_returned_total
                    .fetch_add(saturating_u64_from_usize(series.len()), Ordering::Relaxed);
                Ok(series)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_series_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    fn memory_used(&self) -> usize {
        let used = self.compute_memory_usage_bytes();
        self.memory_used_bytes
            .store(used.min(u64::MAX as usize) as u64, Ordering::Release);
        used
    }

    fn memory_budget(&self) -> usize {
        self.memory_budget_value()
    }

    fn observability_snapshot(&self) -> StorageObservabilitySnapshot {
        let (wal_enabled, wal_size_bytes, wal_segment_count, wal_active_segment, wal_highwater) =
            match &self.wal {
                Some(wal) => (
                    true,
                    wal.total_size_bytes().unwrap_or(0),
                    wal.segment_count().unwrap_or(0),
                    wal.active_segment(),
                    wal.current_highwater(),
                ),
                None => (false, 0, 0, 0, WalHighWatermark::default()),
            };

        StorageObservabilitySnapshot {
            wal: WalObservabilitySnapshot {
                enabled: wal_enabled,
                size_bytes: wal_size_bytes,
                segment_count: wal_segment_count,
                active_segment: wal_active_segment,
                highwater_segment: wal_highwater.segment,
                highwater_frame: wal_highwater.frame,
                replay_runs_total: self
                    .observability
                    .wal
                    .replay_runs_total
                    .load(Ordering::Relaxed),
                replay_frames_total: self
                    .observability
                    .wal
                    .replay_frames_total
                    .load(Ordering::Relaxed),
                replay_series_definitions_total: self
                    .observability
                    .wal
                    .replay_series_definitions_total
                    .load(Ordering::Relaxed),
                replay_sample_batches_total: self
                    .observability
                    .wal
                    .replay_sample_batches_total
                    .load(Ordering::Relaxed),
                replay_points_total: self
                    .observability
                    .wal
                    .replay_points_total
                    .load(Ordering::Relaxed),
                replay_errors_total: self
                    .observability
                    .wal
                    .replay_errors_total
                    .load(Ordering::Relaxed),
                replay_duration_nanos_total: self
                    .observability
                    .wal
                    .replay_duration_nanos_total
                    .load(Ordering::Relaxed),
                append_series_definitions_total: self
                    .observability
                    .wal
                    .append_series_definitions_total
                    .load(Ordering::Relaxed),
                append_sample_batches_total: self
                    .observability
                    .wal
                    .append_sample_batches_total
                    .load(Ordering::Relaxed),
                append_points_total: self
                    .observability
                    .wal
                    .append_points_total
                    .load(Ordering::Relaxed),
                append_bytes_total: self
                    .observability
                    .wal
                    .append_bytes_total
                    .load(Ordering::Relaxed),
                append_errors_total: self
                    .observability
                    .wal
                    .append_errors_total
                    .load(Ordering::Relaxed),
                resets_total: self.observability.wal.resets_total.load(Ordering::Relaxed),
                reset_errors_total: self
                    .observability
                    .wal
                    .reset_errors_total
                    .load(Ordering::Relaxed),
            },
            flush: FlushObservabilitySnapshot {
                pipeline_runs_total: self
                    .observability
                    .flush
                    .pipeline_runs_total
                    .load(Ordering::Relaxed),
                pipeline_success_total: self
                    .observability
                    .flush
                    .pipeline_success_total
                    .load(Ordering::Relaxed),
                pipeline_timeout_total: self
                    .observability
                    .flush
                    .pipeline_timeout_total
                    .load(Ordering::Relaxed),
                pipeline_errors_total: self
                    .observability
                    .flush
                    .pipeline_errors_total
                    .load(Ordering::Relaxed),
                pipeline_duration_nanos_total: self
                    .observability
                    .flush
                    .pipeline_duration_nanos_total
                    .load(Ordering::Relaxed),
                active_flush_runs_total: self
                    .observability
                    .flush
                    .active_flush_runs_total
                    .load(Ordering::Relaxed),
                active_flush_errors_total: self
                    .observability
                    .flush
                    .active_flush_errors_total
                    .load(Ordering::Relaxed),
                active_flushed_series_total: self
                    .observability
                    .flush
                    .active_flushed_series_total
                    .load(Ordering::Relaxed),
                active_flushed_chunks_total: self
                    .observability
                    .flush
                    .active_flushed_chunks_total
                    .load(Ordering::Relaxed),
                active_flushed_points_total: self
                    .observability
                    .flush
                    .active_flushed_points_total
                    .load(Ordering::Relaxed),
                persist_runs_total: self
                    .observability
                    .flush
                    .persist_runs_total
                    .load(Ordering::Relaxed),
                persist_success_total: self
                    .observability
                    .flush
                    .persist_success_total
                    .load(Ordering::Relaxed),
                persist_noop_total: self
                    .observability
                    .flush
                    .persist_noop_total
                    .load(Ordering::Relaxed),
                persist_errors_total: self
                    .observability
                    .flush
                    .persist_errors_total
                    .load(Ordering::Relaxed),
                persisted_series_total: self
                    .observability
                    .flush
                    .persisted_series_total
                    .load(Ordering::Relaxed),
                persisted_chunks_total: self
                    .observability
                    .flush
                    .persisted_chunks_total
                    .load(Ordering::Relaxed),
                persisted_points_total: self
                    .observability
                    .flush
                    .persisted_points_total
                    .load(Ordering::Relaxed),
                persisted_segments_total: self
                    .observability
                    .flush
                    .persisted_segments_total
                    .load(Ordering::Relaxed),
                persist_duration_nanos_total: self
                    .observability
                    .flush
                    .persist_duration_nanos_total
                    .load(Ordering::Relaxed),
                evicted_sealed_chunks_total: self
                    .observability
                    .flush
                    .evicted_sealed_chunks_total
                    .load(Ordering::Relaxed),
            },
            compaction: CompactionObservabilitySnapshot {
                runs_total: self
                    .observability
                    .compaction
                    .runs_total
                    .load(Ordering::Relaxed),
                success_total: self
                    .observability
                    .compaction
                    .success_total
                    .load(Ordering::Relaxed),
                noop_total: self
                    .observability
                    .compaction
                    .noop_total
                    .load(Ordering::Relaxed),
                errors_total: self
                    .observability
                    .compaction
                    .errors_total
                    .load(Ordering::Relaxed),
                source_segments_total: self
                    .observability
                    .compaction
                    .source_segments_total
                    .load(Ordering::Relaxed),
                output_segments_total: self
                    .observability
                    .compaction
                    .output_segments_total
                    .load(Ordering::Relaxed),
                source_chunks_total: self
                    .observability
                    .compaction
                    .source_chunks_total
                    .load(Ordering::Relaxed),
                output_chunks_total: self
                    .observability
                    .compaction
                    .output_chunks_total
                    .load(Ordering::Relaxed),
                source_points_total: self
                    .observability
                    .compaction
                    .source_points_total
                    .load(Ordering::Relaxed),
                output_points_total: self
                    .observability
                    .compaction
                    .output_points_total
                    .load(Ordering::Relaxed),
                duration_nanos_total: self
                    .observability
                    .compaction
                    .duration_nanos_total
                    .load(Ordering::Relaxed),
            },
            query: QueryObservabilitySnapshot {
                select_calls_total: self
                    .observability
                    .query
                    .select_calls_total
                    .load(Ordering::Relaxed),
                select_errors_total: self
                    .observability
                    .query
                    .select_errors_total
                    .load(Ordering::Relaxed),
                select_duration_nanos_total: self
                    .observability
                    .query
                    .select_duration_nanos_total
                    .load(Ordering::Relaxed),
                select_points_returned_total: self
                    .observability
                    .query
                    .select_points_returned_total
                    .load(Ordering::Relaxed),
                select_with_options_calls_total: self
                    .observability
                    .query
                    .select_with_options_calls_total
                    .load(Ordering::Relaxed),
                select_with_options_errors_total: self
                    .observability
                    .query
                    .select_with_options_errors_total
                    .load(Ordering::Relaxed),
                select_with_options_duration_nanos_total: self
                    .observability
                    .query
                    .select_with_options_duration_nanos_total
                    .load(Ordering::Relaxed),
                select_with_options_points_returned_total: self
                    .observability
                    .query
                    .select_with_options_points_returned_total
                    .load(Ordering::Relaxed),
                select_all_calls_total: self
                    .observability
                    .query
                    .select_all_calls_total
                    .load(Ordering::Relaxed),
                select_all_errors_total: self
                    .observability
                    .query
                    .select_all_errors_total
                    .load(Ordering::Relaxed),
                select_all_duration_nanos_total: self
                    .observability
                    .query
                    .select_all_duration_nanos_total
                    .load(Ordering::Relaxed),
                select_all_series_returned_total: self
                    .observability
                    .query
                    .select_all_series_returned_total
                    .load(Ordering::Relaxed),
                select_all_points_returned_total: self
                    .observability
                    .query
                    .select_all_points_returned_total
                    .load(Ordering::Relaxed),
                select_series_calls_total: self
                    .observability
                    .query
                    .select_series_calls_total
                    .load(Ordering::Relaxed),
                select_series_errors_total: self
                    .observability
                    .query
                    .select_series_errors_total
                    .load(Ordering::Relaxed),
                select_series_duration_nanos_total: self
                    .observability
                    .query
                    .select_series_duration_nanos_total
                    .load(Ordering::Relaxed),
                select_series_returned_total: self
                    .observability
                    .query
                    .select_series_returned_total
                    .load(Ordering::Relaxed),
                merge_path_queries_total: self
                    .observability
                    .query
                    .merge_path_queries_total
                    .load(Ordering::Relaxed),
                append_sort_path_queries_total: self
                    .observability
                    .query
                    .append_sort_path_queries_total
                    .load(Ordering::Relaxed),
            },
        }
    }

    fn close(&self) -> Result<()> {
        if self
            .lifecycle
            .compare_exchange(
                STORAGE_OPEN,
                STORAGE_CLOSING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return Err(TsinkError::StorageClosed);
        }

        self.notify_compaction_thread();
        self.notify_flush_thread();

        let mut close_result = (|| {
            let _write_permits = self.write_limiter.acquire_all(self.write_timeout)?;
            self.flush_all_active()?;
            self.persist_segment(true)?;
            self.sweep_expired_persisted_segments()?;
            if self.memory_budget_value() != usize::MAX {
                self.refresh_memory_usage();
            }
            self.compact_until_settled(CLOSE_COMPACTION_MAX_PASSES)?;
            Ok(())
        })();

        if close_result.is_ok() {
            self.lifecycle.store(STORAGE_CLOSED, Ordering::SeqCst);
            self.notify_compaction_thread();
            self.notify_flush_thread();
            if let Err(err) = self.join_background_threads() {
                close_result = Err(err);
            }
        } else {
            self.lifecycle.store(STORAGE_OPEN, Ordering::SeqCst);
            self.notify_compaction_thread();
            self.notify_flush_thread();
        }

        close_result
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
    let timestamp_precision = builder.timestamp_precision();
    let retention = builder.retention();
    let wal_enabled = builder.wal_enabled();
    let storage_options = ChunkStorageOptions {
        retention_window: duration_to_timestamp_units(retention, timestamp_precision),
        retention_enforced: builder.retention_enforced(),
        partition_window: duration_to_timestamp_units(
            builder.partition_duration(),
            timestamp_precision,
        )
        .max(1),
        max_writers: builder.max_writers(),
        write_timeout: builder.write_timeout(),
        memory_budget_bytes: builder.memory_limit_bytes().min(u64::MAX as usize) as u64,
        cardinality_limit: builder.cardinality_limit(),
        wal_size_limit_bytes: builder.wal_size_limit_bytes().min(u64::MAX as usize) as u64,
        admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
        compaction_interval: DEFAULT_COMPACTION_INTERVAL,
        background_threads_enabled: wal_enabled,
    };

    let base_data_path = builder.data_path().map(|path| path.to_path_buf());
    let (numeric_lane_path, blob_lane_path) = if let Some(base_path) = &base_data_path {
        (
            Some(base_path.join(NUMERIC_LANE_ROOT)),
            Some(base_path.join(BLOB_LANE_ROOT)),
        )
    } else {
        (None, None)
    };

    let loaded_numeric = if let Some(path) = &numeric_lane_path {
        load_segment_indexes(path)?
    } else {
        crate::engine::segment::LoadedSegmentIndexes::default()
    };
    let loaded_blob = if let Some(path) = &blob_lane_path {
        load_segment_indexes(path)?
    } else {
        crate::engine::segment::LoadedSegmentIndexes::default()
    };
    let loaded_segments = merge_loaded_segment_indexes(
        loaded_numeric,
        loaded_blob,
        numeric_lane_path.is_some(),
        blob_lane_path.is_some(),
    )?;
    let replay_highwater = loaded_segments.wal_replay_highwater;

    let wal = if let Some(data_path) = builder.data_path() {
        let wal_path = data_path.join(WAL_DIR_NAME);
        if wal_enabled {
            let wal = FramedWal::open_with_buffer_size(
                wal_path,
                builder.wal_sync_mode(),
                builder.wal_buffer_size(),
            )?;
            wal.ensure_min_highwater(replay_highwater)?;
            Some(wal)
        } else {
            clear_wal_dir_if_present(&wal_path)?;
            None
        }
    } else {
        None
    };

    let storage = Arc::new(ChunkStorage::new_with_data_path_and_options(
        builder.chunk_points(),
        wal,
        numeric_lane_path,
        blob_lane_path,
        loaded_segments.next_segment_id,
        storage_options,
    )?);
    storage.apply_loaded_segment_indexes(loaded_segments)?;
    storage.replay_from_wal(replay_highwater)?;
    storage.sweep_expired_persisted_segments()?;
    if storage.memory_budget_value() != usize::MAX {
        storage.refresh_memory_usage();
        storage.enforce_memory_budget_if_needed()?;
    }
    if wal_enabled {
        storage.start_background_flush_thread(DEFAULT_FLUSH_INTERVAL)?;
    }

    Ok(storage as Arc<dyn Storage>)
}

fn clear_wal_dir_if_present(wal_path: &Path) -> Result<()> {
    if !wal_path.exists() {
        return Ok(());
    }

    if wal_path.is_dir() {
        std::fs::remove_dir_all(wal_path)?;
    } else {
        std::fs::remove_file(wal_path)?;
    }

    Ok(())
}

fn merge_loaded_segment_indexes(
    mut numeric: crate::engine::segment::LoadedSegmentIndexes,
    mut blob: crate::engine::segment::LoadedSegmentIndexes,
    numeric_lane_enabled: bool,
    blob_lane_enabled: bool,
) -> Result<crate::engine::segment::LoadedSegmentIndexes> {
    let mut series_by_id = BTreeMap::new();
    for series in numeric.series.drain(..) {
        series_by_id.insert(series.series_id, series);
    }

    for series in blob.series.drain(..) {
        match series_by_id.get(&series.series_id) {
            Some(existing)
                if existing.metric == series.metric && existing.labels == series.labels => {}
            Some(_) => {
                return Err(TsinkError::DataCorruption(format!(
                    "series id {} conflicts across lane segment families",
                    series.series_id
                )));
            }
            None => {
                series_by_id.insert(series.series_id, series);
            }
        }
    }

    let numeric_has_segments = !numeric.indexed_segments.is_empty();
    let blob_has_segments = !blob.indexed_segments.is_empty();

    let mut indexed_segments = numeric.indexed_segments;
    indexed_segments.append(&mut blob.indexed_segments);
    indexed_segments.sort_by_key(|segment| (segment.manifest.level, segment.manifest.segment_id));

    let replay_highwater = match (numeric_lane_enabled, blob_lane_enabled) {
        (true, true) => match (numeric_has_segments, blob_has_segments) {
            // Both lane families are configured, so one-sided segment visibility can be a
            // failed/crashed split persist. Fall back to full WAL replay to avoid skipping
            // frames needed by the missing lane.
            (true, true) => numeric.wal_replay_highwater.min(blob.wal_replay_highwater),
            _ => WalHighWatermark::default(),
        },
        (true, false) => numeric.wal_replay_highwater,
        (false, true) => blob.wal_replay_highwater,
        (false, false) => WalHighWatermark::default(),
    };

    Ok(crate::engine::segment::LoadedSegmentIndexes {
        next_segment_id: numeric.next_segment_id.max(blob.next_segment_id).max(1),
        series: series_by_id.into_values().collect(),
        indexed_segments,
        wal_replay_highwater: replay_highwater,
    })
}

struct PersistedSourceMergeCursor<'a> {
    chunk_refs: Vec<PersistedChunkRef>,
    segment_maps: &'a [Arc<PlatformMmap>],
    start: i64,
    end: i64,
    next_chunk_idx: usize,
    current_points: Vec<DataPoint>,
    next_point_idx: usize,
}

impl<'a> PersistedSourceMergeCursor<'a> {
    fn new(
        chunk_refs: Vec<PersistedChunkRef>,
        segment_maps: &'a [Arc<PlatformMmap>],
        start: i64,
        end: i64,
    ) -> Self {
        Self {
            chunk_refs,
            segment_maps,
            start,
            end,
            next_chunk_idx: 0,
            current_points: Vec::new(),
            next_point_idx: 0,
        }
    }

    fn ensure_head(&mut self) -> Result<Option<&DataPoint>> {
        loop {
            if self.next_point_idx < self.current_points.len() {
                return Ok(self.current_points.get(self.next_point_idx));
            }
            if self.next_chunk_idx >= self.chunk_refs.len() {
                return Ok(None);
            }

            let chunk_ref = self.chunk_refs[self.next_chunk_idx];
            self.next_chunk_idx = self.next_chunk_idx.saturating_add(1);
            self.current_points.clear();
            self.next_point_idx = 0;

            let payload = persisted_chunk_payload(self.segment_maps, &chunk_ref)?;
            decode_encoded_chunk_payload_in_range_into(
                EncodedChunkDescriptor {
                    lane: chunk_ref.lane,
                    ts_codec: chunk_ref.ts_codec,
                    value_codec: chunk_ref.value_codec,
                    point_count: chunk_ref.point_count as usize,
                },
                payload,
                self.start,
                self.end,
                &mut self.current_points,
            )?;
        }
    }

    fn peek_timestamp(&mut self) -> Result<Option<i64>> {
        Ok(self.ensure_head()?.map(|point| point.timestamp))
    }

    fn pop_point(&mut self) -> Result<Option<DataPoint>> {
        if self.ensure_head()?.is_none() {
            return Ok(None);
        }

        let point = self.current_points[self.next_point_idx].clone();
        self.next_point_idx = self.next_point_idx.saturating_add(1);
        Ok(Some(point))
    }
}

struct SealedSourceMergeCursor<'a> {
    chunks: Vec<&'a Chunk>,
    start: i64,
    end: i64,
    next_chunk_idx: usize,
    current_points: Vec<DataPoint>,
    next_point_idx: usize,
}

impl<'a> SealedSourceMergeCursor<'a> {
    fn new(chunks: Vec<&'a Chunk>, start: i64, end: i64) -> Self {
        Self {
            chunks,
            start,
            end,
            next_chunk_idx: 0,
            current_points: Vec::new(),
            next_point_idx: 0,
        }
    }

    fn ensure_head(&mut self) -> Result<Option<&DataPoint>> {
        loop {
            if self.next_point_idx < self.current_points.len() {
                return Ok(self.current_points.get(self.next_point_idx));
            }
            if self.next_chunk_idx >= self.chunks.len() {
                return Ok(None);
            }

            let chunk = self.chunks[self.next_chunk_idx];
            self.next_chunk_idx = self.next_chunk_idx.saturating_add(1);
            self.current_points.clear();
            self.next_point_idx = 0;
            decode_chunk_points_in_range_into(
                chunk,
                self.start,
                self.end,
                &mut self.current_points,
            )?;
        }
    }

    fn peek_timestamp(&mut self) -> Result<Option<i64>> {
        Ok(self.ensure_head()?.map(|point| point.timestamp))
    }

    fn pop_point(&mut self) -> Result<Option<DataPoint>> {
        if self.ensure_head()?.is_none() {
            return Ok(None);
        }

        let point = self.current_points[self.next_point_idx].clone();
        self.next_point_idx = self.next_point_idx.saturating_add(1);
        Ok(Some(point))
    }
}

struct ActiveSourceMergeCursor<'a> {
    points: &'a [ChunkPoint],
    start: i64,
    end: i64,
    next_point_idx: usize,
}

impl<'a> ActiveSourceMergeCursor<'a> {
    fn new(points: &'a [ChunkPoint], start: i64, end: i64) -> Self {
        Self {
            points,
            start,
            end,
            next_point_idx: 0,
        }
    }

    fn seek_in_range(&mut self) {
        while self.next_point_idx < self.points.len()
            && self.points[self.next_point_idx].ts < self.start
        {
            self.next_point_idx = self.next_point_idx.saturating_add(1);
        }
    }

    fn peek_timestamp(&mut self) -> Option<i64> {
        self.seek_in_range();
        let point = self.points.get(self.next_point_idx)?;
        (point.ts < self.end).then_some(point.ts)
    }

    fn pop_point(&mut self) -> Option<DataPoint> {
        self.seek_in_range();
        let point = self.points.get(self.next_point_idx)?;
        if point.ts >= self.end {
            return None;
        }
        self.next_point_idx = self.next_point_idx.saturating_add(1);
        Some(DataPoint::new(point.ts, point.value.clone()))
    }
}

fn merge_sorted_query_sources_into(
    persisted: &mut PersistedSourceMergeCursor<'_>,
    sealed: &mut SealedSourceMergeCursor<'_>,
    active: &mut ActiveSourceMergeCursor<'_>,
    out: &mut Vec<DataPoint>,
) -> Result<()> {
    enum Source {
        Persisted,
        Sealed,
        Active,
    }

    loop {
        let mut selected = None;
        let mut selected_ts = i64::MAX;

        if let Some(ts) = persisted.peek_timestamp()? {
            selected = Some(Source::Persisted);
            selected_ts = ts;
        }
        if let Some(ts) = sealed.peek_timestamp()? {
            if selected.is_none() || ts < selected_ts {
                selected = Some(Source::Sealed);
                selected_ts = ts;
            }
        }
        if let Some(ts) = active.peek_timestamp() {
            if selected.is_none() || ts < selected_ts {
                selected = Some(Source::Active);
            }
        }

        match selected {
            Some(Source::Persisted) => {
                if let Some(point) = persisted.pop_point()? {
                    out.push(point);
                }
            }
            Some(Source::Sealed) => {
                if let Some(point) = sealed.pop_point()? {
                    out.push(point);
                }
            }
            Some(Source::Active) => {
                if let Some(point) = active.pop_point() {
                    out.push(point);
                }
            }
            None => break,
        }
    }

    Ok(())
}

fn persisted_chunk_payload<'a>(
    persisted_segment_maps: &'a [Arc<PlatformMmap>],
    chunk_ref: &PersistedChunkRef,
) -> Result<&'a [u8]> {
    let Some(mapped_segment) = persisted_segment_maps.get(chunk_ref.segment_slot) else {
        return Err(TsinkError::DataCorruption(format!(
            "missing mapped segment slot {}",
            chunk_ref.segment_slot
        )));
    };
    let bytes = mapped_segment.as_slice();

    let offset = usize::try_from(chunk_ref.chunk_offset).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "chunk offset {} exceeds usize",
            chunk_ref.chunk_offset
        ))
    })?;
    let record_len = usize::try_from(chunk_ref.chunk_len).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "chunk length {} exceeds usize",
            chunk_ref.chunk_len
        ))
    })?;
    let record_end = offset.saturating_add(record_len);
    if record_end > bytes.len() {
        return Err(TsinkError::DataCorruption(format!(
            "chunk at offset {} length {} exceeds mapped file size {}",
            chunk_ref.chunk_offset,
            chunk_ref.chunk_len,
            bytes.len()
        )));
    }

    let record = &bytes[offset..record_end];
    if record.len() < 42 {
        return Err(TsinkError::DataCorruption(
            "chunk record too short for header".to_string(),
        ));
    }

    let mut raw = [0u8; 4];
    raw.copy_from_slice(&record[0..4]);
    let body_len = usize::try_from(u32::from_le_bytes(raw)).unwrap_or(usize::MAX);
    if body_len.saturating_add(4) != record.len() {
        return Err(TsinkError::DataCorruption(format!(
            "chunk record length mismatch at offset {}",
            chunk_ref.chunk_offset
        )));
    }

    raw.copy_from_slice(&record[38..42]);
    let payload_len = usize::try_from(u32::from_le_bytes(raw)).unwrap_or(usize::MAX);
    let payload_start = 42usize;
    let payload_end = payload_start.saturating_add(payload_len);

    if payload_end.saturating_add(4) != record.len() {
        return Err(TsinkError::DataCorruption(format!(
            "chunk payload length mismatch at offset {}",
            chunk_ref.chunk_offset
        )));
    }

    Ok(&record[payload_start..payload_end])
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

fn points_are_sorted_by_timestamp(points: &[DataPoint]) -> bool {
    points
        .windows(2)
        .all(|window| window[0].timestamp <= window[1].timestamp)
}

fn dedupe_last_value_per_timestamp(points: &mut Vec<DataPoint>) {
    if points.len() < 2 {
        return;
    }

    points.dedup_by(|current, next| {
        if current.timestamp == next.timestamp {
            // `dedup_by` removes `next`; swap first so the latest value survives.
            std::mem::swap(current, next);
            true
        } else {
            false
        }
    });
}

fn dedupe_exact_duplicate_points(points: &mut Vec<DataPoint>) {
    if points.len() < 2 {
        return;
    }

    points.dedup_by(|current, next| {
        current.timestamp == next.timestamp && current.value == next.value
    });
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

fn lane_name(lane: ValueLane) -> &'static str {
    match lane {
        ValueLane::Numeric => "numeric",
        ValueLane::Blob => "blob",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingValueFamily {
    F64,
    I64,
    U64,
    Bool,
    Blob,
}

fn value_family_for_lane(value: &Value, lane: ValueLane) -> Result<PendingValueFamily> {
    match (value, lane) {
        (Value::F64(_), ValueLane::Numeric) => Ok(PendingValueFamily::F64),
        (Value::I64(_), ValueLane::Numeric) => Ok(PendingValueFamily::I64),
        (Value::U64(_), ValueLane::Numeric) => Ok(PendingValueFamily::U64),
        (Value::Bool(_), ValueLane::Numeric) => Ok(PendingValueFamily::Bool),
        (Value::Bytes(_) | Value::String(_), ValueLane::Blob) => Ok(PendingValueFamily::Blob),
        (_, ValueLane::Numeric) => Err(TsinkError::ValueTypeMismatch {
            expected: "numeric lane value".to_string(),
            actual: value.kind().to_string(),
        }),
        (_, ValueLane::Blob) => Err(TsinkError::ValueTypeMismatch {
            expected: "blob lane value".to_string(),
            actual: value.kind().to_string(),
        }),
    }
}

fn value_family_name(family: PendingValueFamily) -> &'static str {
    match family {
        PendingValueFamily::F64 => "f64",
        PendingValueFamily::I64 => "i64",
        PendingValueFamily::U64 => "u64",
        PendingValueFamily::Bool => "bool",
        PendingValueFamily::Blob => "bytes/string",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use tempfile::TempDir;

    use super::{
        merge_loaded_segment_indexes, ChunkStorage, ChunkStorageOptions, BLOB_LANE_ROOT,
        DEFAULT_ADMISSION_POLL_INTERVAL, DEFAULT_COMPACTION_INTERVAL, NUMERIC_LANE_ROOT,
        WAL_DIR_NAME,
    };
    use crate::engine::chunk::{
        Chunk, ChunkHeader, ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane,
    };
    use crate::engine::encoder::Encoder;
    use crate::engine::segment::{
        load_segment_indexes, load_segments_for_level, SegmentWriter, WalHighWatermark,
    };
    use crate::engine::series_registry::SeriesRegistry;
    use crate::engine::wal::{FramedWal, ReplayFrame, SamplesBatchFrame, SeriesDefinitionFrame};
    use crate::wal::WalSyncMode;
    use crate::{
        DataPoint, Label, Row, SeriesMatcher, SeriesSelection, Storage, StorageBuilder,
        TimestampPrecision, TsinkError, Value,
    };

    #[test]
    fn rotates_chunks_at_configured_cap() {
        let storage = ChunkStorage::new(2, None);
        let labels = vec![Label::new("host", "a")];

        let rows = vec![
            Row::with_labels("cpu", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(3, 3.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(4, 4.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(5, 5.0)),
        ];

        storage.insert_rows(&rows).unwrap();

        let series_id = storage
            .registry
            .read()
            .resolve_existing("cpu", &labels)
            .unwrap()
            .series_id;

        let sealed = storage.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
        let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].header.point_count, 2);
        assert_eq!(chunks[1].header.point_count, 2);

        let active = storage.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
        let state = active.get(&series_id).unwrap();
        assert_eq!(state.builder.len(), 1);
    }

    #[test]
    fn select_reads_active_points_without_flushing_and_sorts_points() {
        let storage = ChunkStorage::new(2, None);
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[
                Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();

        let points = storage.select("latency", &labels, 0, 10).unwrap();
        let timestamps = points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![1, 2, 3]);
        assert_eq!(storage.select("latency", &labels, 0, 10).unwrap(), points);

        let series_id = storage
            .registry
            .read()
            .resolve_existing("latency", &labels)
            .unwrap()
            .series_id;

        let active = storage.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
        assert_eq!(active.get(&series_id).unwrap().builder.len(), 1);
        drop(active);

        let sealed = storage.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
        assert_eq!(sealed.get(&series_id).unwrap().len(), 1);
    }

    #[test]
    fn select_sorts_unsorted_active_points_without_flushing() {
        let storage = ChunkStorage::new(8, None);
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[
                Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();

        let points = storage.select("latency", &labels, 0, 10).unwrap();
        let timestamps = points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![1, 2, 3]);

        let series_id = storage
            .registry
            .read()
            .resolve_existing("latency", &labels)
            .unwrap()
            .series_id;

        let active = storage.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
        assert_eq!(active.get(&series_id).unwrap().builder.len(), 3);
        drop(active);

        let sealed = storage.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
        assert!(sealed.get(&series_id).is_none());
    }

    #[test]
    fn select_sorts_overlapping_sealed_chunks() {
        let storage = ChunkStorage::new(2, None);
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[
                Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(4, 4.0)),
            ])
            .unwrap();

        let points = storage.select("latency", &labels, 0, 10).unwrap();
        let timestamps = points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![1, 2, 3, 4]);
    }

    #[test]
    fn select_preserves_duplicate_timestamps_across_overlapping_chunks() {
        let storage = ChunkStorage::new(2, None);
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[
                Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(3, 30.0)),
            ])
            .unwrap();

        let points = storage.select("latency", &labels, 0, 10).unwrap();
        let timestamps = points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![1, 2, 3, 3]);

        let mut values_at_timestamp_three = points
            .iter()
            .filter(|point| point.timestamp == 3)
            .filter_map(|point| point.value_as_f64())
            .collect::<Vec<_>>();
        values_at_timestamp_three.sort_by(f64::total_cmp);
        assert_eq!(values_at_timestamp_three, vec![3.0, 30.0]);
    }

    #[test]
    fn select_sorts_manual_unsorted_chunk_without_payload() {
        let storage = ChunkStorage::new(4, None);
        let labels = vec![Label::new("host", "a")];

        let series_id = storage
            .registry
            .write()
            .resolve_or_insert("manual", &labels)
            .unwrap()
            .series_id;

        storage.append_sealed_chunk(
            series_id,
            Chunk {
                header: ChunkHeader {
                    series_id,
                    lane: ValueLane::Numeric,
                    point_count: 3,
                    min_ts: 1,
                    max_ts: 3,
                    ts_codec: TimestampCodecId::DeltaVarint,
                    value_codec: ValueCodecId::ConstantRle,
                },
                points: vec![
                    ChunkPoint {
                        ts: 3,
                        value: Value::F64(3.0),
                    },
                    ChunkPoint {
                        ts: 1,
                        value: Value::F64(1.0),
                    },
                    ChunkPoint {
                        ts: 2,
                        value: Value::F64(2.0),
                    },
                ],
                encoded_payload: Vec::new(),
            },
        );

        let points = storage.select("manual", &labels, 0, 10).unwrap();
        let timestamps = points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![1, 2, 3]);
    }

    #[test]
    fn select_into_reuses_output_buffer() {
        let storage = ChunkStorage::new(4, None);
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[
                Row::with_labels("cpu", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("cpu", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("cpu", labels.clone(), DataPoint::new(3, 3.0)),
            ])
            .unwrap();

        let mut out = vec![DataPoint::new(-1, -1.0)];
        storage
            .select_into("cpu", &labels, 0, 10, &mut out)
            .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].timestamp, 1);
        assert_eq!(out[1].timestamp, 2);
        assert_eq!(out[2].timestamp, 3);

        let reused_capacity = out.capacity();
        storage
            .select_into("cpu", &labels, 100, 200, &mut out)
            .unwrap();
        assert!(out.is_empty());
        assert!(out.capacity() >= reused_capacity);
    }

    #[test]
    fn rejects_lane_mismatch_for_same_series() {
        let storage = ChunkStorage::new(4, None);
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[Row::with_labels(
                "events",
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        let err = storage
            .insert_rows(&[Row::with_labels(
                "events",
                labels,
                DataPoint::new(2, "oops"),
            )])
            .unwrap_err();

        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
    }

    #[test]
    fn rejects_mixed_numeric_insert_when_wal_is_disabled() {
        let storage = StorageBuilder::new()
            .with_wal_enabled(false)
            .with_chunk_points(8)
            .build()
            .unwrap();
        let labels = vec![Label::new("host", "a")];

        let err = storage
            .insert_rows(&[
                Row::with_labels("mixed_no_wal", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("mixed_no_wal", labels.clone(), DataPoint::new(2, 2_i64)),
            ])
            .unwrap_err();

        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

        let points = storage.select("mixed_no_wal", &labels, 0, 10).unwrap();
        assert!(points.is_empty());
    }

    #[test]
    fn rejects_mixed_numeric_insert_across_calls_when_wal_is_disabled() {
        let storage = StorageBuilder::new()
            .with_wal_enabled(false)
            .with_chunk_points(8)
            .build()
            .unwrap();
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[Row::with_labels(
                "mixed_no_wal_across_calls",
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        let err = storage
            .insert_rows(&[Row::with_labels(
                "mixed_no_wal_across_calls",
                labels.clone(),
                DataPoint::new(2, 2_i64),
            )])
            .unwrap_err();

        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

        let points = storage
            .select("mixed_no_wal_across_calls", &labels, 0, 10)
            .unwrap();
        assert_eq!(points, vec![DataPoint::new(1, 1.0)]);
    }

    #[test]
    fn failed_insert_rolls_back_new_series_metadata_immediately() {
        let storage = ChunkStorage::new(4, None);
        let labels = vec![Label::new("host", "a")];

        let err = storage
            .insert_rows(&[
                Row::with_labels("phantom_metric", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("phantom_metric", labels.clone(), DataPoint::new(2, 2_i64)),
            ])
            .unwrap_err();
        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

        let has_phantom_metric = storage
            .list_metrics()
            .unwrap()
            .into_iter()
            .any(|series| series.name == "phantom_metric" && series.labels == labels);
        assert!(!has_phantom_metric);
    }

    #[test]
    fn failed_mixed_numeric_insert_does_not_resurrect_series_after_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            let err = storage
                .insert_rows(&[
                    Row::with_labels("mixed_metric", labels.clone(), DataPoint::new(1, 1.0)),
                    Row::with_labels("mixed_metric", labels.clone(), DataPoint::new(2, 2_i64)),
                ])
                .unwrap_err();
            assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

            storage.close().unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let has_mixed_metric = reopened
            .list_metrics()
            .unwrap()
            .into_iter()
            .any(|series| series.name == "mixed_metric" && series.labels == labels);
        assert!(!has_mixed_metric);

        reopened.close().unwrap();
    }

    #[test]
    fn wal_append_failure_does_not_ingest_points_or_survive_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let metric = "wal_append_atomicity";
        let oversized_value = vec![0xAB; (64 * 1024 * 1024) + 1];

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            let err = storage
                .insert_rows(&[Row::new(metric, DataPoint::new(1, oversized_value))])
                .unwrap_err();
            assert!(matches!(
                err,
                TsinkError::InvalidConfiguration(message)
                    if message.contains("WAL frame payload too large")
            ));

            let points = storage.select(metric, &[], 0, 10).unwrap();
            assert!(
                points.is_empty(),
                "failed WAL append must not expose ingested points in memory"
            );

            storage.close().unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let points = reopened.select(metric, &[], 0, 10).unwrap();
        assert!(
            points.is_empty(),
            "failed WAL append must not leave points durable after reopen"
        );

        reopened.close().unwrap();
    }

    #[test]
    fn failed_ingest_does_not_append_wal_samples() {
        let temp_dir = TempDir::new().unwrap();
        let wal =
            FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        let storage = ChunkStorage::new(2, Some(wal));
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[Row::with_labels(
                "wal_ingest_guard",
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        let sample_batches_before: usize = storage
            .wal
            .as_ref()
            .unwrap()
            .replay_frames()
            .unwrap()
            .into_iter()
            .map(|frame| match frame {
                ReplayFrame::Samples(batches) => batches.len(),
                ReplayFrame::SeriesDefinition(_) => 0,
            })
            .sum();

        let series_id = storage
            .registry
            .read()
            .resolve_existing("wal_ingest_guard", &labels)
            .unwrap()
            .series_id;
        {
            let mut active = storage.active_shard(series_id).write();
            active
                .get_mut(&series_id)
                .unwrap()
                .builder
                .append(2, Value::I64(2));
        }

        let err = storage
            .insert_rows(&[Row::with_labels(
                "wal_ingest_guard",
                labels.clone(),
                DataPoint::new(3, 3.0),
            )])
            .unwrap_err();
        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

        let sample_batches_after: usize = storage
            .wal
            .as_ref()
            .unwrap()
            .replay_frames()
            .unwrap()
            .into_iter()
            .map(|frame| match frame {
                ReplayFrame::Samples(batches) => batches.len(),
                ReplayFrame::SeriesDefinition(_) => 0,
            })
            .sum();
        assert_eq!(sample_batches_after, sample_batches_before);
    }

    #[test]
    fn list_metrics_remains_available_while_writer_waits_on_active_lock() {
        use std::sync::mpsc;
        use std::sync::Arc;
        use std::thread;

        let storage = Arc::new(
            ChunkStorage::new_with_data_path_and_options(
                8,
                None,
                None,
                None,
                1,
                ChunkStorageOptions {
                    retention_window: i64::MAX,
                    retention_enforced: false,
                    partition_window: i64::MAX,
                    max_writers: 2,
                    write_timeout: Duration::from_secs(2),
                    memory_budget_bytes: u64::MAX,
                    cardinality_limit: usize::MAX,
                    wal_size_limit_bytes: u64::MAX,
                    admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                    compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                    background_threads_enabled: true,
                },
            )
            .unwrap(),
        );

        let active_read_guards = storage
            .active_builders
            .iter()
            .map(|shard| shard.read())
            .collect::<Vec<_>>();

        let writer_storage = Arc::clone(&storage);
        let (writer_tx, writer_rx) = mpsc::channel();
        let writer = thread::spawn(move || {
            let result = writer_storage
                .insert_rows(&[Row::new("read_concurrency_metric", DataPoint::new(1, 1.0))]);
            writer_tx.send(result).unwrap();
        });

        thread::sleep(Duration::from_millis(75));

        let reader_storage = Arc::clone(&storage);
        let (reader_tx, reader_rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            let result = reader_storage.list_metrics();
            reader_tx.send(result).unwrap();
        });

        let reader_result = reader_rx
            .recv_timeout(Duration::from_millis(500))
            .expect("list_metrics should not block on in-flight WAL/ingest work");
        assert!(reader_result.is_ok());

        drop(active_read_guards);

        let writer_result = writer_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(writer_result.is_ok());

        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn writer_waiting_on_one_metric_shard_does_not_block_other_metric_shards() {
        use std::sync::mpsc;
        use std::sync::Arc;
        use std::thread;

        let storage = Arc::new(
            ChunkStorage::new_with_data_path_and_options(
                8,
                None,
                None,
                None,
                1,
                ChunkStorageOptions {
                    retention_window: i64::MAX,
                    retention_enforced: false,
                    partition_window: i64::MAX,
                    max_writers: 2,
                    write_timeout: Duration::from_secs(2),
                    memory_budget_bytes: u64::MAX,
                    cardinality_limit: usize::MAX,
                    wal_size_limit_bytes: u64::MAX,
                    admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                    compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                    background_threads_enabled: true,
                },
            )
            .unwrap(),
        );
        let labels = vec![Label::new("host", "a")];
        let metric_a = "registry_shard_metric_a";
        let metric_a_shard = ChunkStorage::registry_metric_shard_idx(metric_a);
        let metric_b = (0..1024)
            .map(|idx| format!("registry_shard_metric_b_{idx}"))
            .find(|candidate| ChunkStorage::registry_metric_shard_idx(candidate) != metric_a_shard)
            .expect("expected to find a metric mapped to a different registry shard");

        storage
            .insert_rows(&[Row::with_labels(
                metric_a,
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();
        storage
            .insert_rows(&[Row::with_labels(
                metric_b.as_str(),
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        let series_a = storage
            .registry
            .read()
            .resolve_existing(metric_a, &labels)
            .unwrap()
            .series_id;
        let active_shard_a = ChunkStorage::series_shard_idx(series_a);
        let active_read_guard = storage.active_builders[active_shard_a].read();

        let writer_a_storage = Arc::clone(&storage);
        let writer_a_labels = labels.clone();
        let writer_a_metric = metric_a.to_string();
        let (writer_a_tx, writer_a_rx) = mpsc::channel();
        let writer_a = thread::spawn(move || {
            let result = writer_a_storage.insert_rows(&[Row::with_labels(
                writer_a_metric.as_str(),
                writer_a_labels,
                DataPoint::new(2, 2.0),
            )]);
            writer_a_tx.send(result).unwrap();
        });

        thread::sleep(Duration::from_millis(75));

        let writer_b_storage = Arc::clone(&storage);
        let writer_b_labels = labels.clone();
        let writer_b_metric = metric_b.clone();
        let (writer_b_tx, writer_b_rx) = mpsc::channel();
        let writer_b = thread::spawn(move || {
            let result = writer_b_storage.insert_rows(&[Row::with_labels(
                writer_b_metric.as_str(),
                writer_b_labels,
                DataPoint::new(2, 2.0),
            )]);
            writer_b_tx.send(result).unwrap();
        });

        let writer_b_result = writer_b_rx
            .recv_timeout(Duration::from_millis(500))
            .expect("writer on a different metric shard should not block");
        assert!(writer_b_result.is_ok());
        assert!(writer_a_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err());

        drop(active_read_guard);

        let writer_a_result = writer_a_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(writer_a_result.is_ok());

        writer_a.join().unwrap();
        writer_b.join().unwrap();
    }

    #[test]
    fn concurrent_lane_mismatch_does_not_log_failed_write_to_wal() {
        use std::sync::mpsc;
        use std::sync::Arc;
        use std::sync::Barrier;
        use std::thread;
        use std::time::Instant;

        let temp_dir = TempDir::new().unwrap();
        let wal =
            FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        let storage = Arc::new(
            ChunkStorage::new_with_data_path_and_options(
                8,
                Some(wal),
                None,
                None,
                1,
                ChunkStorageOptions {
                    retention_window: i64::MAX,
                    retention_enforced: false,
                    partition_window: i64::MAX,
                    max_writers: 2,
                    write_timeout: Duration::from_secs(2),
                    memory_budget_bytes: u64::MAX,
                    cardinality_limit: usize::MAX,
                    wal_size_limit_bytes: u64::MAX,
                    admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                    compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                    background_threads_enabled: true,
                },
            )
            .unwrap(),
        );
        let labels = vec![Label::new("host", "a")];
        let start = Arc::new(Barrier::new(3));
        let (tx, rx) = mpsc::channel();

        let active_read_guards = storage
            .active_builders
            .iter()
            .map(|shard| shard.read())
            .collect::<Vec<_>>();

        let thread_storage = Arc::clone(&storage);
        let thread_labels = labels.clone();
        let thread_start = Arc::clone(&start);
        let thread_tx = tx.clone();
        let numeric_writer = thread::spawn(move || {
            thread_start.wait();
            let result = thread_storage.insert_rows(&[Row::with_labels(
                "lane_race_metric",
                thread_labels,
                DataPoint::new(1, 1.0),
            )]);
            thread_tx.send(result).unwrap();
        });

        let thread_storage = Arc::clone(&storage);
        let thread_labels = labels.clone();
        let thread_start = Arc::clone(&start);
        let blob_writer = thread::spawn(move || {
            thread_start.wait();
            let result = thread_storage.insert_rows(&[Row::with_labels(
                "lane_race_metric",
                thread_labels,
                DataPoint::new(2, "blob"),
            )]);
            tx.send(result).unwrap();
        });

        start.wait();

        let mut pre_release_sample_batches = 0usize;
        let deadline = Instant::now() + Duration::from_millis(250);
        while Instant::now() < deadline {
            pre_release_sample_batches = storage
                .wal
                .as_ref()
                .unwrap()
                .replay_frames()
                .unwrap()
                .into_iter()
                .map(|frame| match frame {
                    ReplayFrame::Samples(batches) => batches.len(),
                    ReplayFrame::SeriesDefinition(_) => 0,
                })
                .sum();
            if pre_release_sample_batches > 0 {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(pre_release_sample_batches, 0);

        drop(active_read_guards);

        let first = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let second = rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let mut ok_count = 0usize;
        let mut mismatch_count = 0usize;
        for result in [first, second] {
            match result {
                Ok(()) => ok_count += 1,
                Err(TsinkError::ValueTypeMismatch { .. }) => mismatch_count += 1,
                Err(other) => panic!("unexpected insert result: {other}"),
            }
        }
        assert_eq!(ok_count, 1);
        assert_eq!(mismatch_count, 1);

        numeric_writer.join().unwrap();
        blob_writer.join().unwrap();

        let final_sample_batches: usize = storage
            .wal
            .as_ref()
            .unwrap()
            .replay_frames()
            .unwrap()
            .into_iter()
            .map(|frame| match frame {
                ReplayFrame::Samples(batches) => batches.len(),
                ReplayFrame::SeriesDefinition(_) => 0,
            })
            .sum();
        assert_eq!(final_sample_batches, 1);
    }

    #[test]
    fn close_clears_metadata_only_wal_when_no_chunks_are_sealed() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];
        let metric = "phantom_metric";

        {
            let wal = FramedWal::open(temp_dir.path().join("wal"), WalSyncMode::PerAppend).unwrap();
            wal.append_series_definition(&SeriesDefinitionFrame {
                series_id: 42,
                metric: metric.to_string(),
                labels: labels.clone(),
            })
            .unwrap();
        }

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            let has_phantom_without_wal = storage
                .list_metrics()
                .unwrap()
                .into_iter()
                .any(|series| series.name == metric && series.labels == labels);
            assert!(!has_phantom_without_wal);

            let has_phantom_with_wal = storage
                .list_metrics_with_wal()
                .unwrap()
                .into_iter()
                .any(|series| series.name == metric && series.labels == labels);
            assert!(has_phantom_with_wal);

            storage.close().unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let has_phantom = reopened
            .list_metrics()
            .unwrap()
            .into_iter()
            .any(|series| series.name == metric && series.labels == labels);
        assert!(!has_phantom);

        let has_phantom = reopened
            .list_metrics_with_wal()
            .unwrap()
            .into_iter()
            .any(|series| series.name == metric && series.labels == labels);
        assert!(!has_phantom);

        reopened.close().unwrap();
    }

    #[test]
    fn select_uses_exact_label_match_with_postings_candidates() {
        let storage = ChunkStorage::new(2, None);
        let short = vec![Label::new("host", "a")];
        let long = vec![Label::new("host", "a"), Label::new("region", "us")];

        storage
            .insert_rows(&[
                Row::with_labels("cpu", short.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("cpu", long.clone(), DataPoint::new(1, 10.0)),
            ])
            .unwrap();

        let points_short = storage.select("cpu", &short, 0, 10).unwrap();
        let points_long = storage.select("cpu", &long, 0, 10).unwrap();

        assert_eq!(points_short, vec![DataPoint::new(1, 1.0)]);
        assert_eq!(points_long, vec![DataPoint::new(1, 10.0)]);
    }

    #[test]
    fn select_series_supports_matcher_intersections() {
        let storage = ChunkStorage::new(2, None);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "cpu",
                    vec![Label::new("host", "a"), Label::new("region", "use1")],
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "cpu",
                    vec![Label::new("host", "b"), Label::new("region", "usw2")],
                    DataPoint::new(1, 2.0),
                ),
                Row::with_labels(
                    "memory",
                    vec![Label::new("host", "a"), Label::new("region", "use1")],
                    DataPoint::new(1, 3.0),
                ),
            ])
            .unwrap();

        let selected = storage
            .select_series(
                &SeriesSelection::new()
                    .with_metric("cpu")
                    .with_matcher(SeriesMatcher::equal("host", "a"))
                    .with_matcher(SeriesMatcher::regex_match("region", "use.*")),
            )
            .unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].name, "cpu");
        assert_eq!(
            selected[0].labels,
            vec![Label::new("host", "a"), Label::new("region", "use1")]
        );

        let not_equal = storage
            .select_series(
                &SeriesSelection::new()
                    .with_metric("cpu")
                    .with_matcher(SeriesMatcher::not_equal("host", "a")),
            )
            .unwrap();
        assert_eq!(not_equal.len(), 1);
        assert_eq!(
            not_equal[0].labels,
            vec![Label::new("host", "b"), Label::new("region", "usw2")]
        );
    }

    #[test]
    fn select_series_time_range_uses_chunk_indexes() {
        let temp_dir = TempDir::new().unwrap();
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(1)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels(
                    "cpu",
                    vec![Label::new("host", "old")],
                    DataPoint::new(10, 1.0),
                ),
                Row::with_labels(
                    "cpu",
                    vec![Label::new("host", "new")],
                    DataPoint::new(200, 2.0),
                ),
            ])
            .unwrap();
        storage.close().unwrap();

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(1)
            .build()
            .unwrap();

        let selected = reopened
            .select_series(
                &SeriesSelection::new()
                    .with_metric("cpu")
                    .with_time_range(100, 300),
            )
            .unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].name, "cpu");
        assert_eq!(selected[0].labels, vec![Label::new("host", "new")]);

        reopened.close().unwrap();
    }

    #[test]
    fn select_series_rejects_invalid_regex() {
        let storage = ChunkStorage::new(2, None);
        let err = storage
            .select_series(
                &SeriesSelection::new()
                    .with_metric("cpu")
                    .with_matcher(SeriesMatcher::regex_match("host", "(")),
            )
            .unwrap_err();
        assert!(matches!(err, TsinkError::InvalidConfiguration(_)));
    }

    #[test]
    fn recovers_rows_from_wal_after_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            storage
                .insert_rows(&[
                    Row::with_labels("recover", labels.clone(), DataPoint::new(1, 1.0)),
                    Row::with_labels("recover", labels.clone(), DataPoint::new(2, 2.0)),
                    Row::with_labels("recover", labels.clone(), DataPoint::new(3, 3.0)),
                ])
                .unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let points = reopened.select("recover", &labels, 0, 10).unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0], DataPoint::new(1, 1.0));
        assert_eq!(points[2], DataPoint::new(3, 3.0));

        reopened.close().unwrap();
    }

    #[test]
    fn stale_wal_with_already_persisted_points_does_not_duplicate_query_results() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];
        let wal_dir = temp_dir.path().join(WAL_DIR_NAME);

        let stale_frames = {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            storage
                .insert_rows(&[
                    Row::with_labels("dupe_recovery", labels.clone(), DataPoint::new(1, 1.0)),
                    Row::with_labels("dupe_recovery", labels.clone(), DataPoint::new(2, 2.0)),
                    Row::with_labels("dupe_recovery", labels.clone(), DataPoint::new(3, 3.0)),
                ])
                .unwrap();

            let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
            let frames = wal.replay_frames().unwrap();
            assert!(
                !frames.is_empty(),
                "expected WAL frames before close so we can simulate crash window replay"
            );

            storage.close().unwrap();
            frames
        };

        {
            let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
            for frame in stale_frames {
                match frame {
                    ReplayFrame::SeriesDefinition(definition) => {
                        wal.append_series_definition(&definition).unwrap();
                    }
                    ReplayFrame::Samples(batches) => {
                        wal.append_samples(&batches).unwrap();
                    }
                }
            }
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let points = reopened.select("dupe_recovery", &labels, 0, 10).unwrap();
        assert_eq!(
            points,
            vec![
                DataPoint::new(1, 1.0),
                DataPoint::new(2, 2.0),
                DataPoint::new(3, 3.0)
            ]
        );

        reopened.close().unwrap();
    }

    #[test]
    fn stale_wal_with_already_persisted_nan_points_does_not_duplicate_query_results() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];
        let wal_dir = temp_dir.path().join(WAL_DIR_NAME);

        let stale_frames = {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            storage
                .insert_rows(&[
                    Row::with_labels(
                        "dupe_recovery_nan",
                        labels.clone(),
                        DataPoint::new(1, f64::NAN),
                    ),
                    Row::with_labels("dupe_recovery_nan", labels.clone(), DataPoint::new(2, 2.0)),
                ])
                .unwrap();

            let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
            let frames = wal.replay_frames().unwrap();
            assert!(
                !frames.is_empty(),
                "expected WAL frames before close so we can simulate crash window replay"
            );

            storage.close().unwrap();
            frames
        };

        {
            let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
            for frame in stale_frames {
                match frame {
                    ReplayFrame::SeriesDefinition(definition) => {
                        wal.append_series_definition(&definition).unwrap();
                    }
                    ReplayFrame::Samples(batches) => {
                        wal.append_samples(&batches).unwrap();
                    }
                }
            }
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let points = reopened
            .select("dupe_recovery_nan", &labels, 0, 10)
            .unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].timestamp, 1);
        assert!(points[0].value_as_f64().is_some_and(f64::is_nan));
        assert_eq!(points[1], DataPoint::new(2, 2.0));

        reopened.close().unwrap();
    }

    #[test]
    fn query_prefers_compacted_generation_when_compaction_crash_leaves_both_generations() {
        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
        let labels = vec![Label::new("host", "a")];
        let metric = "compaction_crash_dupe";

        let mut registry = SeriesRegistry::new();
        let series_id = registry
            .resolve_or_insert(metric, &labels)
            .unwrap()
            .series_id;

        let mut stale_l0 = HashMap::new();
        stale_l0.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(1, 1.0), (2, 2.0)],
            )],
        );
        SegmentWriter::new(&lane_path, 0, 1)
            .unwrap()
            .write_segment(&registry, &stale_l0)
            .unwrap();

        let mut compacted_l1 = HashMap::new();
        compacted_l1.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(1, 10.0), (2, 2.0)],
            )],
        );
        SegmentWriter::new(&lane_path, 1, 2)
            .unwrap()
            .write_segment(&registry, &compacted_l1)
            .unwrap();

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let points = reopened.select(metric, &labels, 0, 10).unwrap();
        assert_eq!(
            points,
            vec![DataPoint::new(1, 10.0), DataPoint::new(2, 2.0)]
        );

        reopened.close().unwrap();
    }

    #[test]
    fn background_compaction_reduces_l0_segments_while_storage_is_open() {
        use std::thread;
        use std::time::Instant;

        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
        let labels = vec![Label::new("host", "a")];

        let mut registry = SeriesRegistry::new();
        let series_id = registry
            .resolve_or_insert("background_compaction", &labels)
            .unwrap()
            .series_id;

        for segment_id in 1..=4 {
            let mut chunks = HashMap::new();
            chunks.insert(
                series_id,
                vec![make_persisted_numeric_chunk(
                    series_id,
                    &[(segment_id as i64, segment_id as f64)],
                )],
            );
            SegmentWriter::new(&lane_path, 0, segment_id)
                .unwrap()
                .write_segment(&registry, &chunks)
                .unwrap();
        }

        let storage = ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            5,
            ChunkStorageOptions {
                retention_window: i64::MAX,
                retention_enforced: false,
                partition_window: i64::MAX,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: Duration::from_millis(25),
                background_threads_enabled: true,
            },
        )
        .unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut compacted = false;

        while Instant::now() < deadline {
            let l0 = load_segments_for_level(&lane_path, 0).unwrap();
            let l1 = load_segments_for_level(&lane_path, 1).unwrap();
            if l0.len() < 4 && !l1.is_empty() {
                compacted = true;
                break;
            }
            thread::sleep(Duration::from_millis(25));
        }

        assert!(compacted, "background thread did not compact L0 into L1");
        storage.close().unwrap();
    }

    #[test]
    fn background_flush_pipeline_refreshes_persisted_index_and_evicts_sealed_chunks_while_open() {
        use std::sync::Arc;
        use std::thread;
        use std::time::Instant;

        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
        let labels = vec![Label::new("host", "a")];
        let storage = Arc::new(
            ChunkStorage::new_with_data_path_and_options(
                2,
                None,
                Some(lane_path.clone()),
                None,
                1,
                ChunkStorageOptions::default(),
            )
            .unwrap(),
        );
        storage
            .start_background_flush_thread(Duration::from_millis(25))
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("background_flush", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("background_flush", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("background_flush", labels.clone(), DataPoint::new(3, 3.0)),
            ])
            .unwrap();

        let series_id = storage
            .registry
            .read()
            .resolve_existing("background_flush", &labels)
            .unwrap()
            .series_id;

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut flushed = false;
        while Instant::now() < deadline {
            let active_len = storage
                .active_shard(series_id)
                .read()
                .get(&series_id)
                .map_or(0, |state| state.builder.len());
            let sealed_len = storage
                .sealed_shard(series_id)
                .read()
                .get(&series_id)
                .map_or(0, |chunks| chunks.len());
            let persisted_len = storage
                .persisted_index
                .read()
                .chunk_refs
                .get(&series_id)
                .map_or(0, |chunks| chunks.len());
            let l0 = load_segments_for_level(&lane_path, 0).unwrap();

            if active_len == 0 && sealed_len == 0 && persisted_len >= 2 && !l0.is_empty() {
                flushed = true;
                break;
            }

            thread::sleep(Duration::from_millis(25));
        }

        assert!(
            flushed,
            "background flush pipeline did not refresh persisted indexes and evict flushed sealed chunks"
        );
        assert_eq!(
            storage
                .select("background_flush", &labels, 0, 10)
                .unwrap()
                .len(),
            3
        );

        storage.close().unwrap();
    }

    #[test]
    fn flush_pipeline_waits_for_busy_writer_permit() {
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
        let labels = vec![Label::new("host", "a")];
        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(lane_path.clone()),
            None,
            1,
            ChunkStorageOptions {
                retention_window: i64::MAX,
                retention_enforced: false,
                partition_window: i64::MAX,
                max_writers: 2,
                write_timeout: Duration::from_millis(250),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
            },
        )
        .unwrap();

        storage
            .insert_rows(&[Row::with_labels(
                "flush_busy_writer",
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        let series_id = storage
            .registry
            .read()
            .resolve_existing("flush_busy_writer", &labels)
            .unwrap()
            .series_id;

        let active_before = storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.builder.len());
        assert_eq!(active_before, 1);

        let held_permit = storage.write_limiter.acquire();
        thread::scope(|scope| {
            let flush = scope.spawn(|| storage.flush_pipeline_once());
            thread::sleep(Duration::from_millis(50));
            drop(held_permit);
            flush.join().unwrap().unwrap();
        });

        let active_after = storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.builder.len());
        assert_eq!(active_after, 0);
        assert_eq!(
            storage
                .select("flush_busy_writer", &labels, 0, 10)
                .unwrap()
                .len(),
            1
        );
        assert!(
            !load_segments_for_level(&lane_path, 0).unwrap().is_empty(),
            "flush pipeline should persist after blocked writer permit is released"
        );

        storage.close().unwrap();
    }

    #[test]
    fn reenable_wal_after_wal_disabled_run_ignores_stale_wal_generation() {
        let temp_dir = TempDir::new().unwrap();
        let stale_labels = vec![Label::new("host", "stale")];
        let fresh_labels = vec![Label::new("host", "fresh")];

        let stale_series_id = 1;
        let wal =
            FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: stale_series_id,
            metric: "metric".to_string(),
            labels: stale_labels.clone(),
        })
        .unwrap();
        let stale_batch = SamplesBatchFrame::from_points(
            stale_series_id,
            ValueLane::Numeric,
            &[
                ChunkPoint {
                    ts: 1,
                    value: Value::F64(1.0),
                },
                ChunkPoint {
                    ts: 2,
                    value: Value::F64(2.0),
                },
            ],
        )
        .unwrap();
        wal.append_samples(&[stale_batch]).unwrap();
        drop(wal);

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_wal_enabled(false)
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            storage
                .insert_rows(&[Row::with_labels(
                    "metric",
                    fresh_labels.clone(),
                    DataPoint::new(10, 10.0),
                )])
                .unwrap();
            storage.close().unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let fresh = reopened.select("metric", &fresh_labels, 0, 20).unwrap();
        assert_eq!(fresh, vec![DataPoint::new(10, 10.0)]);
        let stale = reopened.select("metric", &stale_labels, 0, 20).unwrap();
        assert!(stale.is_empty());

        reopened.close().unwrap();
    }

    #[test]
    fn reopens_from_segment_files_without_wal() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_wal_enabled(false)
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            storage
                .insert_rows(&[
                    Row::with_labels("seg", labels.clone(), DataPoint::new(1, 1.0)),
                    Row::with_labels("seg", labels.clone(), DataPoint::new(2, 2.0)),
                    Row::with_labels("seg", labels.clone(), DataPoint::new(3, 3.0)),
                ])
                .unwrap();
            storage.close().unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let points = reopened.select("seg", &labels, 0, 10).unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0], DataPoint::new(1, 1.0));
        assert_eq!(points[2], DataPoint::new(3, 3.0));

        let segments_root = temp_dir
            .path()
            .join(NUMERIC_LANE_ROOT)
            .join("segments")
            .join("L0");
        assert!(
            segments_root.exists(),
            "numeric lane segments should exist at {:?}",
            segments_root
        );
        let mut found_segment = false;
        for entry in std::fs::read_dir(segments_root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                found_segment = true;
                assert!(path.join("manifest.bin").exists());
                assert!(path.join("chunks.bin").exists());
                assert!(path.join("chunk_index.bin").exists());
                assert!(path.join("series.bin").exists());
                assert!(path.join("postings.bin").exists());
            }
        }
        assert!(found_segment);

        reopened.close().unwrap();
    }

    #[test]
    fn isolates_numeric_and_blob_segments_and_merges_in_queries() {
        let temp_dir = TempDir::new().unwrap();
        let numeric_labels = vec![Label::new("kind", "numeric")];
        let blob_labels = vec![Label::new("kind", "blob")];

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_wal_enabled(false)
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            storage
                .insert_rows(&[
                    Row::with_labels("mix", numeric_labels.clone(), DataPoint::new(1, 1.0)),
                    Row::with_labels("mix", numeric_labels.clone(), DataPoint::new(2, 2.0)),
                    Row::with_labels("mix", blob_labels.clone(), DataPoint::new(1, "a")),
                    Row::with_labels("mix", blob_labels.clone(), DataPoint::new(2, "b")),
                ])
                .unwrap();
            storage.close().unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let numeric = reopened.select("mix", &numeric_labels, 0, 10).unwrap();
        let blob = reopened.select("mix", &blob_labels, 0, 10).unwrap();
        assert_eq!(numeric.len(), 2);
        assert_eq!(blob.len(), 2);
        assert_eq!(blob[0], DataPoint::new(1, "a"));
        assert_eq!(blob[1], DataPoint::new(2, "b"));

        let mut all = reopened.select_all("mix", 0, 10).unwrap();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].1.len(), 2);
        assert_eq!(all[1].1.len(), 2);

        let numeric_root = temp_dir
            .path()
            .join(NUMERIC_LANE_ROOT)
            .join("segments")
            .join("L0");
        let blob_root = temp_dir
            .path()
            .join(BLOB_LANE_ROOT)
            .join("segments")
            .join("L0");
        assert!(numeric_root.exists());
        assert!(blob_root.exists());

        reopened.close().unwrap();
    }

    #[test]
    fn replay_highwater_is_conservative_when_one_configured_lane_has_no_segments() {
        let temp_dir = TempDir::new().unwrap();
        let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
        let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);
        let labels = vec![Label::new("kind", "numeric")];
        let mut registry = SeriesRegistry::new();
        let series_id = registry
            .resolve_or_insert("watermark_gap", &labels)
            .unwrap()
            .series_id;

        let mut numeric_chunks = HashMap::new();
        numeric_chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(series_id, &[(1, 1.0)])],
        );
        SegmentWriter::new(&numeric_lane_path, 0, 1)
            .unwrap()
            .write_segment_with_wal_highwater(
                &registry,
                &numeric_chunks,
                WalHighWatermark {
                    segment: 3,
                    frame: 7,
                },
            )
            .unwrap();

        let loaded_numeric = load_segment_indexes(&numeric_lane_path).unwrap();
        let loaded_blob = load_segment_indexes(&blob_lane_path).unwrap();
        let merged = merge_loaded_segment_indexes(loaded_numeric, loaded_blob, true, true).unwrap();
        assert_eq!(merged.wal_replay_highwater, WalHighWatermark::default());
    }

    #[test]
    fn persist_segment_rolls_back_published_lane_when_other_lane_fails() {
        let temp_dir = TempDir::new().unwrap();
        let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
        let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);
        std::fs::write(&blob_lane_path, b"not-a-directory").unwrap();

        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(numeric_lane_path.clone()),
            Some(blob_lane_path),
            1,
            ChunkStorageOptions::default(),
        )
        .unwrap();
        let numeric_labels = vec![Label::new("kind", "numeric")];
        let blob_labels = vec![Label::new("kind", "blob")];
        storage
            .insert_rows(&[
                Row::with_labels(
                    "lane_atomicity",
                    numeric_labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "lane_atomicity",
                    blob_labels.clone(),
                    DataPoint::new(1, "a"),
                ),
            ])
            .unwrap();
        storage.flush_all_active().unwrap();

        assert!(storage.persist_segment(true).is_err());
        assert!(load_segments_for_level(&numeric_lane_path, 0)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn partition_window_rotates_chunks_before_reaching_chunk_cap() {
        let storage = ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                retention_window: i64::MAX,
                retention_enforced: false,
                partition_window: 1,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: true,
            },
        )
        .unwrap();

        let labels = vec![Label::new("host", "a")];
        storage
            .insert_rows(&[
                Row::with_labels("partitioned", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("partitioned", labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();

        let series_id = storage
            .registry
            .read()
            .resolve_existing("partitioned", &labels)
            .unwrap()
            .series_id;

        let sealed = storage.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
        let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
        assert_eq!(
            chunks.len(),
            1,
            "partition transition should seal current chunk"
        );
        assert_eq!(chunks[0].header.min_ts, 1);
        assert_eq!(chunks[0].header.max_ts, 1);

        let active = storage.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
        assert_eq!(active.get(&series_id).unwrap().builder.len(), 1);
    }

    #[test]
    fn memory_budget_spills_to_l0_and_preserves_query_results() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];
        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                retention_window: i64::MAX,
                retention_enforced: false,
                partition_window: i64::MAX,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: 512,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: true,
            },
        )
        .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("budget_metric", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("budget_metric", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("budget_metric", labels.clone(), DataPoint::new(3, 3.0)),
                Row::with_labels("budget_metric", labels.clone(), DataPoint::new(4, 4.0)),
                Row::with_labels("budget_metric", labels.clone(), DataPoint::new(5, 5.0)),
                Row::with_labels("budget_metric", labels.clone(), DataPoint::new(6, 6.0)),
            ])
            .unwrap();

        let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
        let l0_segments = load_segments_for_level(&lane_path, 0).unwrap();
        assert!(
            !l0_segments.is_empty(),
            "budget pressure should flush sealed chunks to L0 before close"
        );

        let points = storage.select("budget_metric", &labels, 0, 10).unwrap();
        assert_eq!(
            points,
            vec![
                DataPoint::new(1, 1.0),
                DataPoint::new(2, 2.0),
                DataPoint::new(3, 3.0),
                DataPoint::new(4, 4.0),
                DataPoint::new(5, 5.0),
                DataPoint::new(6, 6.0),
            ]
        );
        assert_eq!(storage.memory_budget(), 512);
        assert!(
            storage.memory_used() <= storage.memory_budget(),
            "memory usage should fall under budget after spill"
        );

        let series_id = storage
            .registry
            .read()
            .resolve_existing("budget_metric", &labels)
            .unwrap()
            .series_id;
        let sealed = storage.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
        let sealed_count = sealed
            .get(&series_id)
            .map(|chunks| chunks.len())
            .unwrap_or(0);
        assert!(
            sealed_count < 3,
            "oldest sealed chunks should be evicted after spill"
        );

        storage.close().unwrap();
    }

    #[test]
    fn memory_budget_stats_reflect_builder_configuration() {
        let storage = StorageBuilder::new()
            .with_memory_limit(1234)
            .build()
            .unwrap();

        assert_eq!(storage.memory_budget(), 1234);
        assert_eq!(storage.memory_used(), 0);
    }

    #[test]
    fn memory_budget_guard_rejects_writes_when_in_memory_budget_cannot_be_relaxed() {
        let storage = StorageBuilder::new()
            .with_wal_enabled(false)
            .with_memory_limit(1)
            .with_write_timeout(Duration::ZERO)
            .build()
            .unwrap();

        let err = storage
            .insert_rows(&[Row::new("memory_guard_metric", DataPoint::new(1, 1.0))])
            .unwrap_err();
        assert!(matches!(
            err,
            TsinkError::MemoryBudgetExceeded { budget: 1, .. }
        ));
        assert!(
            storage
                .select("memory_guard_metric", &[], 0, 10)
                .unwrap()
                .is_empty(),
            "rejected writes must not mutate in-memory state"
        );
    }

    #[test]
    fn cardinality_limit_rejects_new_series_beyond_limit() {
        let storage = StorageBuilder::new()
            .with_cardinality_limit(1)
            .build()
            .unwrap();
        let labels_a = vec![Label::new("host", "a")];
        let labels_b = vec![Label::new("host", "b")];

        storage
            .insert_rows(&[Row::with_labels(
                "cardinality_guard_metric",
                labels_a.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        let err = storage
            .insert_rows(&[Row::with_labels(
                "cardinality_guard_metric",
                labels_b.clone(),
                DataPoint::new(1, 2.0),
            )])
            .unwrap_err();
        assert!(matches!(
            err,
            TsinkError::CardinalityLimitExceeded { limit: 1, .. }
        ));

        storage
            .insert_rows(&[Row::with_labels(
                "cardinality_guard_metric",
                labels_a.clone(),
                DataPoint::new(2, 3.0),
            )])
            .unwrap();
        let points_a = storage
            .select("cardinality_guard_metric", &labels_a, 0, 10)
            .unwrap();
        assert_eq!(points_a.len(), 2);
        assert!(storage
            .select("cardinality_guard_metric", &labels_b, 0, 10)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn cardinality_limit_rejection_does_not_grow_string_dictionaries() {
        let storage = ChunkStorage::new_with_data_path_and_options(
            4,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                cardinality_limit: 1,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();
        let baseline_labels = vec![Label::new("host", "baseline")];

        storage
            .insert_rows(&[Row::with_labels(
                "cardinality_dict_guard_metric",
                baseline_labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        let (baseline_metric_len, baseline_label_name_len, baseline_label_value_len) = {
            let registry = storage.registry.read();
            (
                registry.metric_dictionary_len(),
                registry.label_name_dictionary_len(),
                registry.label_value_dictionary_len(),
            )
        };

        for attempt in 0..16 {
            let err = storage
                .insert_rows(&[Row::with_labels(
                    format!("cardinality_dict_leak_metric_{attempt}"),
                    vec![Label::new(
                        format!("dict_name_{attempt}"),
                        format!("dict_value_{attempt}"),
                    )],
                    DataPoint::new(2, attempt as f64),
                )])
                .unwrap_err();
            assert!(matches!(
                err,
                TsinkError::CardinalityLimitExceeded { limit: 1, .. }
            ));
        }

        let (metric_len, label_name_len, label_value_len) = {
            let registry = storage.registry.read();
            (
                registry.metric_dictionary_len(),
                registry.label_name_dictionary_len(),
                registry.label_value_dictionary_len(),
            )
        };

        assert_eq!(metric_len, baseline_metric_len);
        assert_eq!(label_name_len, baseline_label_name_len);
        assert_eq!(label_value_len, baseline_label_value_len);

        storage.close().unwrap();
    }

    #[test]
    fn wal_size_limit_rejects_writes_that_cannot_fit_new_frames() {
        let temp_dir = TempDir::new().unwrap();
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_write_timeout(Duration::ZERO)
            .with_wal_size_limit(1)
            .build()
            .unwrap();

        let err = storage
            .insert_rows(&[Row::new("wal_guard_metric", DataPoint::new(1, 1.0))])
            .unwrap_err();
        assert!(matches!(
            err,
            TsinkError::WalSizeLimitExceeded { limit: 1, .. }
        ));
        assert!(
            storage
                .select("wal_guard_metric", &[], 0, 10)
                .unwrap()
                .is_empty(),
            "WAL admission failure must not ingest points"
        );
    }

    #[test]
    fn chunk_storage_default_retention_enforcement_matches_builder_default() {
        let chunk_defaults = ChunkStorageOptions::default();
        let builder_default = StorageBuilder::new();
        assert_eq!(
            chunk_defaults.retention_enforced,
            builder_default.retention_enforced()
        );
    }

    #[test]
    fn retention_window_hides_points_older_than_latest_minus_window() {
        let storage = StorageBuilder::new()
            .with_retention(Duration::from_secs(1))
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::new("retention_metric", DataPoint::new(100, 1.0))])
            .unwrap();
        storage
            .insert_rows(&[Row::new("retention_metric", DataPoint::new(102, 2.0))])
            .unwrap();

        let points = storage.select("retention_metric", &[], 0, 200).unwrap();
        assert_eq!(points, vec![DataPoint::new(102, 2.0)]);
    }

    #[test]
    fn default_retention_rejects_out_of_window_writes() {
        let retention_secs = Duration::from_secs(14 * 24 * 3600).as_secs() as i64;
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::new(
                "default_retention_metric",
                DataPoint::new(retention_secs + 1, 1.0),
            )])
            .unwrap();

        let err = storage
            .insert_rows(&[Row::new("default_retention_metric", DataPoint::new(0, 0.0))])
            .unwrap_err();
        assert!(matches!(err, TsinkError::OutOfRetention { timestamp: 0 }));
    }

    #[test]
    fn explicitly_setting_default_retention_rejects_out_of_window_writes() {
        let retention = Duration::from_secs(14 * 24 * 3600);
        let storage = StorageBuilder::new()
            .with_retention(retention)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::new(
                "explicit_default_retention_metric",
                DataPoint::new(retention.as_secs() as i64 + 1, 1.0),
            )])
            .unwrap();

        let err = storage
            .insert_rows(&[Row::new(
                "explicit_default_retention_metric",
                DataPoint::new(0, 0.0),
            )])
            .unwrap_err();
        assert!(matches!(err, TsinkError::OutOfRetention { timestamp: 0 }));
    }

    #[test]
    fn disabling_retention_enforcement_never_expires_or_rejects_points() {
        let storage = StorageBuilder::new()
            .with_retention(Duration::from_secs(1))
            .with_retention_enforced(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::new("no_retention_metric", DataPoint::new(100, 1.0))])
            .unwrap();
        storage
            .insert_rows(&[Row::new("no_retention_metric", DataPoint::new(102, 2.0))])
            .unwrap();
        storage
            .insert_rows(&[Row::new("no_retention_metric", DataPoint::new(0, 0.0))])
            .unwrap();

        let points = storage.select("no_retention_metric", &[], 0, 200).unwrap();
        assert_eq!(
            points,
            vec![
                DataPoint::new(0, 0.0),
                DataPoint::new(100, 1.0),
                DataPoint::new(102, 2.0)
            ]
        );
    }

    #[test]
    fn retention_sweeper_deletes_expired_persisted_segments_across_levels() {
        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
        let labels = vec![Label::new("host", "a")];
        let mut registry = SeriesRegistry::new();
        let series_id = registry
            .resolve_or_insert("retention_swept_metric", &labels)
            .unwrap()
            .series_id;

        let mut expired_chunks = HashMap::new();
        expired_chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(1, 1.0), (2, 2.0)],
            )],
        );
        SegmentWriter::new(&lane_path, 0, 1)
            .unwrap()
            .write_segment(&registry, &expired_chunks)
            .unwrap();
        SegmentWriter::new(&lane_path, 1, 2)
            .unwrap()
            .write_segment(&registry, &expired_chunks)
            .unwrap();

        let mut retained_chunks = HashMap::new();
        retained_chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(100, 100.0), (101, 101.0)],
            )],
        );
        SegmentWriter::new(&lane_path, 2, 3)
            .unwrap()
            .write_segment(&registry, &retained_chunks)
            .unwrap();

        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .with_retention(Duration::from_secs(10))
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        assert!(load_segments_for_level(&lane_path, 0).unwrap().is_empty());
        assert!(load_segments_for_level(&lane_path, 1).unwrap().is_empty());
        let l2 = load_segments_for_level(&lane_path, 2).unwrap();
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].manifest.segment_id, 3);

        let points = storage
            .select("retention_swept_metric", &labels, 0, 200)
            .unwrap();
        assert_eq!(
            points,
            vec![DataPoint::new(100, 100.0), DataPoint::new(101, 101.0)]
        );

        storage.close().unwrap();
    }

    #[test]
    fn retention_sweep_reload_failure_keeps_existing_persisted_data_visible() {
        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
        let labels = vec![Label::new("host", "a")];
        let mut registry = SeriesRegistry::new();
        let series_id = registry
            .resolve_or_insert("retention_reload_visibility_metric", &labels)
            .unwrap()
            .series_id;

        let mut expired_chunks = HashMap::new();
        expired_chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(1, 1.0), (2, 2.0)],
            )],
        );
        SegmentWriter::new(&lane_path, 0, 1)
            .unwrap()
            .write_segment(&registry, &expired_chunks)
            .unwrap();

        let mut retained_chunks = HashMap::new();
        retained_chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(100, 100.0), (101, 101.0)],
            )],
        );
        SegmentWriter::new(&lane_path, 2, 2)
            .unwrap()
            .write_segment(&registry, &retained_chunks)
            .unwrap();

        let storage = ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            Some(lane_path.clone()),
            None,
            3,
            ChunkStorageOptions {
                retention_window: 10,
                retention_enforced: true,
                partition_window: i64::MAX,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
            },
        )
        .unwrap();
        storage
            .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap())
            .unwrap();

        let before = storage
            .select("retention_reload_visibility_metric", &labels, 0, 200)
            .unwrap();
        assert_eq!(
            before,
            vec![DataPoint::new(100, 100.0), DataPoint::new(101, 101.0)]
        );

        // Introduce an on-disk conflict that makes future index reloads fail.
        let mut conflicting_registry = SeriesRegistry::new();
        conflicting_registry
            .register_series_with_id(series_id, "retention_reload_conflict_metric", &labels)
            .unwrap();
        let mut conflicting_chunks = HashMap::new();
        conflicting_chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(series_id, &[(150, 150.0)])],
        );
        SegmentWriter::new(&lane_path, 1, 10)
            .unwrap()
            .write_segment(&conflicting_registry, &conflicting_chunks)
            .unwrap();

        let err = storage.sweep_expired_persisted_segments().unwrap_err();
        assert!(matches!(err, TsinkError::DataCorruption(_)));

        let after = storage
            .select("retention_reload_visibility_metric", &labels, 0, 200)
            .unwrap();
        assert_eq!(
            after,
            vec![DataPoint::new(100, 100.0), DataPoint::new(101, 101.0)]
        );
    }

    #[test]
    fn retention_sweeper_is_disabled_when_retention_enforcement_is_off() {
        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
        let labels = vec![Label::new("host", "a")];
        let mut registry = SeriesRegistry::new();
        let series_id = registry
            .resolve_or_insert("retention_unswept_metric", &labels)
            .unwrap()
            .series_id;

        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(1, 1.0), (2, 2.0)],
            )],
        );
        SegmentWriter::new(&lane_path, 0, 1)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();

        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .with_retention(Duration::from_secs(1))
            .with_retention_enforced(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        let l0 = load_segments_for_level(&lane_path, 0).unwrap();
        assert_eq!(l0.len(), 1);
        assert_eq!(l0[0].manifest.segment_id, 1);

        let points = storage
            .select("retention_unswept_metric", &labels, 0, 10)
            .unwrap();
        assert_eq!(points, vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]);

        storage.close().unwrap();
    }

    #[test]
    fn timestamp_precision_changes_retention_unit_conversion() {
        let seconds_storage = StorageBuilder::new()
            .with_retention(Duration::from_secs(1))
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();
        seconds_storage
            .insert_rows(&[Row::new("seconds", DataPoint::new(0, 1.0))])
            .unwrap();
        seconds_storage
            .insert_rows(&[Row::new("seconds", DataPoint::new(2, 2.0))])
            .unwrap();
        let seconds_points = seconds_storage.select("seconds", &[], 0, 10).unwrap();
        assert_eq!(seconds_points, vec![DataPoint::new(2, 2.0)]);

        let millis_storage = StorageBuilder::new()
            .with_retention(Duration::from_secs(1))
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .unwrap();
        millis_storage
            .insert_rows(&[Row::new("millis", DataPoint::new(0, 1.0))])
            .unwrap();
        millis_storage
            .insert_rows(&[Row::new("millis", DataPoint::new(2, 2.0))])
            .unwrap();
        let millis_points = millis_storage.select("millis", &[], 0, 10).unwrap();
        assert_eq!(millis_points.len(), 2);
    }

    #[test]
    fn write_limiter_respects_configured_timeout() {
        let storage = ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                retention_window: i64::MAX,
                retention_enforced: false,
                partition_window: i64::MAX,
                max_writers: 1,
                write_timeout: Duration::ZERO,
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: true,
            },
        )
        .unwrap();

        let _held_permit = storage.write_limiter.acquire();
        let err = storage
            .insert_rows(&[Row::new("write_timeout_metric", DataPoint::new(1, 1.0))])
            .unwrap_err();
        assert!(matches!(
            err,
            TsinkError::WriteTimeout {
                timeout_ms: 0,
                workers: 1
            }
        ));
    }

    #[test]
    fn admission_pressure_drain_times_out_when_another_writer_is_in_flight() {
        let temp_dir = TempDir::new().unwrap();
        let wal =
            FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        let storage = ChunkStorage::new_with_data_path_and_options(
            8,
            Some(wal),
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                retention_window: i64::MAX,
                retention_enforced: false,
                partition_window: i64::MAX,
                max_writers: 2,
                write_timeout: Duration::from_millis(100),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: 1,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: true,
            },
        )
        .unwrap();

        let _held_permit = storage.write_limiter.acquire();
        let err = storage
            .insert_rows(&[Row::new(
                "wal_pressure_drain_metric",
                DataPoint::new(1, 1.0),
            )])
            .unwrap_err();
        assert!(matches!(err, TsinkError::WriteTimeout { workers: 2, .. }));
    }

    #[test]
    fn memory_pressure_drain_times_out_when_another_writer_is_in_flight() {
        let temp_dir = TempDir::new().unwrap();
        let wal =
            FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        let storage = ChunkStorage::new_with_data_path_and_options(
            8,
            Some(wal),
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                retention_window: i64::MAX,
                retention_enforced: false,
                partition_window: i64::MAX,
                max_writers: 2,
                write_timeout: Duration::from_millis(100),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: true,
            },
        )
        .unwrap();

        storage
            .insert_rows(&[
                Row::new("memory_pressure_drain_metric", DataPoint::new(1, 1.0)),
                Row::new("memory_pressure_drain_metric", DataPoint::new(2, 2.0)),
            ])
            .unwrap();

        storage
            .memory_budget_bytes
            .store(1, std::sync::atomic::Ordering::Release);
        storage.refresh_memory_usage();

        let _held_permit = storage.write_limiter.acquire();
        let err = storage.enforce_memory_budget_if_needed().unwrap_err();
        assert!(matches!(err, TsinkError::WriteTimeout { workers: 2, .. }));
    }

    #[test]
    fn close_blocks_until_in_flight_writer_releases_permit() {
        use std::sync::mpsc;
        use std::sync::Arc;
        use std::thread;

        let storage = Arc::new(
            ChunkStorage::new_with_data_path_and_options(
                8,
                None,
                None,
                None,
                1,
                ChunkStorageOptions {
                    retention_window: i64::MAX,
                    retention_enforced: false,
                    partition_window: i64::MAX,
                    max_writers: 1,
                    write_timeout: Duration::from_secs(2),
                    memory_budget_bytes: u64::MAX,
                    cardinality_limit: usize::MAX,
                    wal_size_limit_bytes: u64::MAX,
                    admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                    compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                    background_threads_enabled: true,
                },
            )
            .unwrap(),
        );
        let labels = vec![Label::new("host", "a")];

        let held_permit = storage.write_limiter.acquire();

        let writer_storage = Arc::clone(&storage);
        let writer_labels = labels.clone();
        let (writer_tx, writer_rx) = mpsc::channel();
        let writer = thread::spawn(move || {
            let result = writer_storage.insert_rows(&[Row::with_labels(
                "close_race_metric",
                writer_labels,
                DataPoint::new(1, 1.0),
            )]);
            writer_tx.send(result).unwrap();
        });

        assert!(writer_rx.recv_timeout(Duration::from_millis(100)).is_err());

        let close_storage = Arc::clone(&storage);
        let (close_tx, close_rx) = mpsc::channel();
        let closer = thread::spawn(move || {
            let result = close_storage.close();
            close_tx.send(result).unwrap();
        });

        assert!(close_rx.recv_timeout(Duration::from_millis(100)).is_err());

        drop(held_permit);

        let close_result = close_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(close_result.is_ok());

        let writer_result = writer_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(writer_result, Err(TsinkError::StorageClosed)));

        writer.join().unwrap();
        closer.join().unwrap();
    }

    fn make_persisted_numeric_chunk(series_id: u64, points: &[(i64, f64)]) -> Chunk {
        assert!(!points.is_empty());
        let chunk_points = points
            .iter()
            .map(|(ts, value)| ChunkPoint {
                ts: *ts,
                value: Value::F64(*value),
            })
            .collect::<Vec<_>>();
        let encoded = Encoder::encode_chunk_points(&chunk_points, ValueLane::Numeric).unwrap();

        Chunk {
            header: ChunkHeader {
                series_id,
                lane: ValueLane::Numeric,
                point_count: chunk_points.len() as u16,
                min_ts: chunk_points.first().unwrap().ts,
                max_ts: chunk_points.last().unwrap().ts,
                ts_codec: encoded.ts_codec,
                value_codec: encoded.value_codec,
            },
            points: chunk_points,
            encoded_payload: encoded.payload,
        }
    }
}
