//! Runtime-agnostic async facade over sync `Storage` using dedicated worker threads.

use crate::cgroup;
use crate::wal::{WalReplayMode, WalSyncMode};
use crate::{
    AsyncResourceLimits, BatchWriteResult, DataPoint, EffectiveStorageLimits, Label, MetricSeries,
    QueryBudgetLimits, QueryCancellationToken, QueryExecution, QueryExecutionAccounting,
    QueryOptions, QueryRowsExecutionResult, QueryRowsPage, QueryRowsScanOptions, QueryWorkLimits,
    ResourceConfigurationSnapshot, ResourceLimitOverride, ResourceProfile, Result,
    RollupObservabilitySnapshot, RollupPolicy, Row, RowWriteOutcome, SelectSeriesExecutionResult,
    SeriesSelection, Storage, StorageBuilder, StorageObservabilitySnapshot, TimestampPrecision,
    TsinkError, WriteBatchLimits, WriteMode, WriteRejection, WriteRejectionCategory, WriteResult,
};
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

const STATE_OPEN: u8 = 0;
const STATE_CLOSING: u8 = 1;
const STATE_CLOSED: u8 = 2;
const DEFAULT_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_WRITE_QUEUE_BYTE_CAPACITY: usize = 64 * 1024 * 1024;
const DEFAULT_READ_QUEUE_BYTE_CAPACITY: usize = 16 * 1024 * 1024;

const READ_QUEUE_NAME: &str = "read";
const WRITE_QUEUE_NAME: &str = "write";

/// Runtime settings for the async service layer.
#[derive(Debug, Clone, Copy)]
pub struct AsyncRuntimeOptions {
    /// Number of commands that may wait in each bounded read and write queue.
    ///
    /// Zero is normalized to one.
    pub queue_capacity: usize,
    /// Maximum modeled bytes owned by write commands waiting for or occupying the write queue.
    ///
    /// The default is 64 MiB. Zero permits only write commands without owned input payloads.
    pub write_queue_byte_capacity: usize,
    /// Maximum modeled bytes owned by read commands waiting for or occupying the read queue.
    ///
    /// The default is 16 MiB. Zero permits only reads without owned input payloads.
    pub read_queue_byte_capacity: usize,
    /// Number of dedicated reader worker threads.
    ///
    /// Zero is normalized to one.
    pub read_workers: usize,
}

impl Default for AsyncRuntimeOptions {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            write_queue_byte_capacity: DEFAULT_WRITE_QUEUE_BYTE_CAPACITY,
            read_queue_byte_capacity: DEFAULT_READ_QUEUE_BYTE_CAPACITY,
            read_workers: cgroup::default_workers_limit().max(1),
        }
    }
}

impl AsyncRuntimeOptions {
    fn normalized(self) -> Self {
        Self {
            queue_capacity: self.queue_capacity.max(1),
            write_queue_byte_capacity: self.write_queue_byte_capacity,
            read_queue_byte_capacity: self.read_queue_byte_capacity,
            read_workers: self.read_workers.max(1),
        }
    }
}

/// Resource state owned by one [`AsyncStorage`] worker facade.
///
/// Queue depths count commands currently inside the bounded channels. Byte counters also include
/// commands held by producers waiting for a full channel, because those futures already own their
/// command payloads. Bytes are released when a worker receives a command, before backend work
/// starts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsyncRuntimeSnapshot {
    pub write_queue_command_capacity: usize,
    pub read_queue_command_capacity: usize,
    pub write_queue_byte_capacity: usize,
    pub read_queue_byte_capacity: usize,
    pub write_queue_depth: usize,
    pub read_queue_depth: usize,
    pub current_write_queue_bytes: usize,
    pub peak_write_queue_bytes: usize,
    pub current_read_queue_bytes: usize,
    pub peak_read_queue_bytes: usize,
    pub write_queue_byte_rejections_total: u64,
    pub read_queue_byte_rejections_total: u64,
    pub write_workers: usize,
    pub read_workers: usize,
}

#[derive(Debug)]
struct QueueByteBudget {
    queue: &'static str,
    limit: usize,
    current: AtomicUsize,
    peak: AtomicUsize,
    rejections: AtomicU64,
}

impl QueueByteBudget {
    fn new(queue: &'static str, limit: usize) -> Arc<Self> {
        Arc::new(Self {
            queue,
            limit,
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            rejections: AtomicU64::new(0),
        })
    }

    fn reserve(self: &Arc<Self>, requested: usize) -> Result<QueueByteReservation> {
        let mut current = self.current.load(Ordering::Acquire);
        loop {
            let Some(required) = current.checked_add(requested) else {
                self.record_rejection();
                return Err(TsinkError::AsyncQueuePayloadSizeOverflow { queue: self.queue });
            };
            if required > self.limit {
                self.record_rejection();
                return Err(TsinkError::AsyncQueueByteLimitExceeded {
                    queue: self.queue,
                    limit: self.limit,
                    current,
                    requested,
                });
            }
            match self.current.compare_exchange_weak(
                current,
                required,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.peak.fetch_max(required, Ordering::AcqRel);
                    return Ok(QueueByteReservation {
                        budget: Arc::clone(self),
                        bytes: Some(requested),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn record_rejection(&self) {
        let _ = self
            .rejections
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1))
            });
    }
}

#[derive(Debug)]
struct QueueByteReservation {
    budget: Arc<QueueByteBudget>,
    bytes: Option<usize>,
}

impl QueueByteReservation {
    fn release(&mut self) {
        let Some(bytes) = self.bytes.take() else {
            return;
        };
        let result =
            self.budget
                .current
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current.checked_sub(bytes)
                });
        debug_assert!(result.is_ok(), "async queue byte accounting underflow");
    }
}

impl Drop for QueueByteReservation {
    fn drop(&mut self) {
        self.release();
    }
}

struct QueuedWriteCommand {
    command: WriteCommand,
    reservation: QueueByteReservation,
}

struct QueuedReadCommand {
    command: ReadCommand,
    cancellation: QueryCancellationToken,
    reservation: QueueByteReservation,
}

type Reply<T> = async_channel::Sender<Result<T>>;

enum WriteCommand {
    InsertRows {
        rows: Vec<Row>,
        reply: Reply<()>,
    },
    InsertRowsWithResult {
        rows: Vec<Row>,
        reply: Reply<WriteResult>,
    },
    WriteBatch {
        rows: Vec<Row>,
        mode: WriteMode,
        reply: Reply<BatchWriteResult>,
    },
    Snapshot {
        path: PathBuf,
        reply: Reply<()>,
    },
    ApplyRollupPolicies {
        policies: Vec<RollupPolicy>,
        reply: Reply<RollupObservabilitySnapshot>,
    },
    TriggerRollupRun {
        reply: Reply<RollupObservabilitySnapshot>,
    },
    Close {
        reply: Reply<()>,
    },
}

enum ReadCommand {
    Select {
        metric: String,
        labels: Vec<Label>,
        start: i64,
        end: i64,
        reply: Reply<Vec<DataPoint>>,
    },
    SelectWithOptions {
        metric: String,
        options: QueryOptions,
        reply: Reply<Vec<DataPoint>>,
    },
    SelectAll {
        metric: String,
        start: i64,
        end: i64,
        reply: Reply<Vec<(Vec<Label>, Vec<DataPoint>)>>,
    },
    ListMetrics {
        reply: Reply<SelectSeriesExecutionResult>,
    },
    SelectSeries {
        selection: SeriesSelection,
        reply: Reply<SelectSeriesExecutionResult>,
    },
    ScanSeriesRows {
        series: Vec<MetricSeries>,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        reply: Reply<QueryRowsExecutionResult>,
    },
    ScanMetricRows {
        metric: String,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        reply: Reply<QueryRowsExecutionResult>,
    },
}

struct AsyncRuntime {
    storage: Arc<dyn Storage>,
    state: Arc<AtomicU8>,
    options: AsyncRuntimeOptions,
    resource_overrides: Vec<ResourceLimitOverride>,
    write_budget: Arc<QueueByteBudget>,
    read_budget: Arc<QueueByteBudget>,
    write_tx: async_channel::Sender<QueuedWriteCommand>,
    read_tx: async_channel::Sender<QueuedReadCommand>,
    worker_handles: Mutex<Vec<JoinHandle<()>>>,
}

impl AsyncRuntime {
    fn new(
        storage: Arc<dyn Storage>,
        options: AsyncRuntimeOptions,
        resource_overrides: Vec<ResourceLimitOverride>,
    ) -> Result<Self> {
        let options = options.normalized();
        let state = Arc::new(AtomicU8::new(STATE_OPEN));
        let write_budget =
            QueueByteBudget::new(WRITE_QUEUE_NAME, options.write_queue_byte_capacity);
        let read_budget = QueueByteBudget::new(READ_QUEUE_NAME, options.read_queue_byte_capacity);

        let (write_tx, write_rx) = async_channel::bounded(options.queue_capacity);
        let (read_tx, read_rx) = async_channel::bounded(options.queue_capacity);

        let mut worker_handles = Vec::with_capacity(options.read_workers + 1);

        let write_storage = Arc::clone(&storage);
        let write_state = Arc::clone(&state);
        worker_handles.push(
            thread::Builder::new()
                .name("tsink-async-write".to_string())
                .spawn(move || write_worker_loop(write_storage, write_state, write_rx))
                .map_err(|err| {
                    TsinkError::Other(format!("failed to spawn async write worker: {err}"))
                })?,
        );

        for worker_idx in 0..options.read_workers {
            let read_storage = Arc::clone(&storage);
            let read_rx = read_rx.clone();
            worker_handles.push(
                thread::Builder::new()
                    .name(format!("tsink-async-read-{worker_idx}"))
                    .spawn(move || read_worker_loop(read_storage, read_rx))
                    .map_err(|err| {
                        TsinkError::Other(format!("failed to spawn async read worker: {err}"))
                    })?,
            );
        }

        Ok(Self {
            storage,
            state,
            options,
            resource_overrides,
            write_budget,
            read_budget,
            write_tx,
            read_tx,
            worker_handles: Mutex::new(worker_handles),
        })
    }

    fn snapshot(&self) -> AsyncRuntimeSnapshot {
        AsyncRuntimeSnapshot {
            write_queue_command_capacity: self.options.queue_capacity,
            read_queue_command_capacity: self.options.queue_capacity,
            write_queue_byte_capacity: self.options.write_queue_byte_capacity,
            read_queue_byte_capacity: self.options.read_queue_byte_capacity,
            write_queue_depth: self.write_tx.len(),
            read_queue_depth: self.read_tx.len(),
            current_write_queue_bytes: self.write_budget.current.load(Ordering::Acquire),
            peak_write_queue_bytes: self.write_budget.peak.load(Ordering::Acquire),
            current_read_queue_bytes: self.read_budget.current.load(Ordering::Acquire),
            peak_read_queue_bytes: self.read_budget.peak.load(Ordering::Acquire),
            write_queue_byte_rejections_total: self.write_budget.rejections.load(Ordering::Acquire),
            read_queue_byte_rejections_total: self.read_budget.rejections.load(Ordering::Acquire),
            write_workers: 1,
            read_workers: self.options.read_workers,
        }
    }
}

impl Drop for AsyncRuntime {
    fn drop(&mut self) {
        self.write_tx.close();
        self.read_tx.close();

        let mut handles = self.worker_handles.lock();
        for handle in handles.drain(..) {
            let _ = handle.join();
        }
    }
}

/// Runtime-independent async facade over [`Storage`], backed by dedicated worker threads.
///
/// Clones share the same queues, workers, and lifecycle state. Closing any clone closes the
/// shared facade for every clone. Call [`AsyncStorage::close`] explicitly to observe storage
/// shutdown errors; dropping the facade is not a substitute for a successful close.
#[derive(Clone)]
pub struct AsyncStorage {
    runtime: Arc<AsyncRuntime>,
}

impl AsyncStorage {
    /// Starts an async facade with [`AsyncRuntimeOptions::default`] around an existing backend.
    pub fn from_storage(storage: Arc<dyn Storage>) -> Result<Self> {
        Self::from_storage_with_options(storage, AsyncRuntimeOptions::default())
    }

    /// Starts an async facade with explicit queue and reader-worker settings.
    ///
    /// Worker threads are owned by the returned facade and do not require a Tokio or async-std
    /// runtime. Zero command capacity and reader workers are normalized to one; zero byte
    /// capacities permit only commands without owned input payloads.
    pub fn from_storage_with_options(
        storage: Arc<dyn Storage>,
        options: AsyncRuntimeOptions,
    ) -> Result<Self> {
        Self::from_storage_with_options_and_overrides(storage, options, Vec::new())
    }

    fn from_storage_with_options_and_overrides(
        storage: Arc<dyn Storage>,
        options: AsyncRuntimeOptions,
        resource_overrides: Vec<ResourceLimitOverride>,
    ) -> Result<Self> {
        Ok(Self {
            runtime: Arc::new(AsyncRuntime::new(storage, options, resource_overrides)?),
        })
    }

    /// Clones the underlying synchronous storage handle.
    ///
    /// Operations made through this handle bypass the facade's queues and lifecycle guard.
    pub fn inner(&self) -> Arc<dyn Storage> {
        Arc::clone(&self.runtime.storage)
    }

    /// Consumes this facade handle and returns a clone of the synchronous storage handle.
    ///
    /// This does not close the storage or other [`AsyncStorage`] clones.
    pub fn into_inner(self) -> Arc<dyn Storage> {
        Arc::clone(&self.runtime.storage)
    }

    /// Queues a compatibility write and waits for its success or failure.
    ///
    /// Use [`AsyncStorage::insert_rows_with_result`] when the durability acknowledgement matters.
    /// Once accepted into the write queue, the write may still execute if the awaiting future is
    /// cancelled.
    pub async fn insert_rows(&self, rows: Vec<Row>) -> Result<()> {
        self.ensure_open()?;
        let payload_bytes = modeled_write_rows_bytes(&rows)?;
        self.send_write(payload_bytes, |reply| WriteCommand::InsertRows {
            rows,
            reply,
        })
        .await
    }

    /// Queues a write and returns the durability guarantee established when it succeeds.
    ///
    /// Once accepted into the write queue, the write may still execute if the awaiting future is
    /// cancelled.
    pub async fn insert_rows_with_result(&self, rows: Vec<Row>) -> Result<WriteResult> {
        self.ensure_open()?;
        let payload_bytes = modeled_write_rows_bytes(&rows)?;
        self.send_write(payload_bytes, |reply| WriteCommand::InsertRowsWithResult {
            rows,
            reply,
        })
        .await
    }

    /// Queues [`Storage::write_batch`] with an explicit failure policy and returns its outcome.
    ///
    /// The whole batch runs as one command on the serialized write worker. Once accepted into the
    /// write queue, it may still execute if the awaiting future is cancelled.
    pub async fn write_batch(&self, rows: Vec<Row>, mode: WriteMode) -> Result<BatchWriteResult> {
        if let Err(error) = self.ensure_open() {
            if !rows.is_empty() && matches!(error, TsinkError::StorageClosed) {
                let outcomes = match mode {
                    WriteMode::Atomic => {
                        let rejection = WriteRejection::new(
                            WriteRejectionCategory::StorageClosed,
                            None,
                            error.to_string(),
                        );
                        (0..rows.len())
                            .map(|index| RowWriteOutcome::rejected(index, rejection.clone()))
                            .collect()
                    }
                    WriteMode::BestEffort => (0..rows.len())
                        .map(|index| {
                            RowWriteOutcome::rejected(
                                index,
                                WriteRejection::new(
                                    WriteRejectionCategory::StorageClosed,
                                    Some(index),
                                    error.to_string(),
                                ),
                            )
                        })
                        .collect(),
                };
                return Ok(BatchWriteResult::from_outcomes(None, outcomes));
            }
            return Err(error);
        }
        let payload_bytes = modeled_write_rows_bytes(&rows)?;
        self.send_write(payload_bytes, |reply| WriteCommand::WriteBatch {
            rows,
            mode,
            reply,
        })
        .await
    }

    /// Runs [`Storage::select`] on the facade's reader worker pool.
    pub async fn select(
        &self,
        metric: impl Into<String>,
        labels: Vec<Label>,
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        self.ensure_open()?;
        let metric = metric.into();
        let payload_bytes = modeled_metric_and_labels_bytes(&metric, &labels)?;
        self.send_read(payload_bytes, |reply| ReadCommand::Select {
            metric,
            labels,
            start,
            end,
            reply,
        })
        .await
    }

    /// Runs [`Storage::select_with_options`] on the reader worker pool.
    pub async fn select_with_options(
        &self,
        metric: impl Into<String>,
        options: QueryOptions,
    ) -> Result<Vec<DataPoint>> {
        self.ensure_open()?;
        let metric = metric.into();
        let payload_bytes = modeled_query_options_bytes(&metric, &options)?;
        self.send_read(payload_bytes, |reply| ReadCommand::SelectWithOptions {
            metric,
            options,
            reply,
        })
        .await
    }

    /// Runs [`Storage::select_all`] on the reader worker pool.
    pub async fn select_all(
        &self,
        metric: impl Into<String>,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.ensure_open()?;
        let metric = metric.into();
        let payload_bytes = metric.len();
        self.send_read(payload_bytes, |reply| ReadCommand::SelectAll {
            metric,
            start,
            end,
            reply,
        })
        .await
    }

    /// Runs [`Storage::scan_series_rows`] on the reader worker pool.
    pub async fn scan_series_rows(
        &self,
        series: Vec<MetricSeries>,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        self.ensure_open()?;
        let payload_bytes = modeled_metric_series_bytes(&series)?;
        self.send_read(payload_bytes, |reply| ReadCommand::ScanSeriesRows {
            series,
            start,
            end,
            options,
            reply,
        })
        .await
        .map(QueryRowsExecutionResult::into_page)
    }

    /// Runs [`Storage::scan_metric_rows`] on the reader worker pool.
    pub async fn scan_metric_rows(
        &self,
        metric: impl Into<String>,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        self.ensure_open()?;
        let metric = metric.into();
        let payload_bytes = metric.len();
        self.send_read(payload_bytes, |reply| ReadCommand::ScanMetricRows {
            metric,
            start,
            end,
            options,
            reply,
        })
        .await
        .map(QueryRowsExecutionResult::into_page)
    }

    /// Runs [`Storage::list_metrics`] on the reader worker pool.
    pub async fn list_metrics(&self) -> Result<Vec<MetricSeries>> {
        self.ensure_open()?;
        self.send_read(0, |reply| ReadCommand::ListMetrics { reply })
            .await
            .map(SelectSeriesExecutionResult::into_series)
    }

    /// Runs [`Storage::select_series`] on the reader worker pool.
    pub async fn select_series(&self, selection: SeriesSelection) -> Result<Vec<MetricSeries>> {
        self.ensure_open()?;
        let payload_bytes = modeled_series_selection_bytes(&selection)?;
        self.send_read(payload_bytes, |reply| ReadCommand::SelectSeries {
            selection,
            reply,
        })
        .await
        .map(SelectSeriesExecutionResult::into_series)
    }

    /// Returns [`Storage::memory_used`] directly without entering a worker queue.
    pub fn memory_used(&self) -> usize {
        self.runtime.storage.memory_used()
    }

    /// Returns [`Storage::memory_budget`] directly without entering a worker queue.
    ///
    /// `usize::MAX` means no explicit memory budget is configured.
    pub fn memory_budget(&self) -> usize {
        self.runtime.storage.memory_budget()
    }

    /// Returns [`Storage::effective_storage_limits`] directly without entering a worker queue.
    pub fn effective_storage_limits(&self) -> EffectiveStorageLimits {
        self.runtime.storage.effective_storage_limits()
    }

    /// Returns the selected storage profile augmented with this facade's resolved queue limits.
    pub fn resource_configuration_snapshot(&self) -> ResourceConfigurationSnapshot {
        let mut snapshot = self.runtime.storage.resource_configuration_snapshot();
        snapshot.resolved_limits.async_runtime = Some(AsyncResourceLimits {
            queue_command_capacity: self.runtime.options.queue_capacity,
            write_queue_byte_capacity: self.runtime.options.write_queue_byte_capacity,
            read_queue_byte_capacity: self.runtime.options.read_queue_byte_capacity,
            read_workers: self.runtime.options.read_workers,
        });
        snapshot
            .overrides
            .extend(self.runtime.resource_overrides.iter().copied());
        snapshot.overrides.sort_unstable();
        snapshot.overrides.dedup();
        snapshot
    }

    /// Returns [`Storage::observability_snapshot`] directly without entering a worker queue.
    pub fn observability_snapshot(&self) -> StorageObservabilitySnapshot {
        let mut snapshot = self.runtime.storage.observability_snapshot();
        snapshot.resource_configuration = self.resource_configuration_snapshot();
        snapshot
    }

    /// Returns the async facade's queue, worker, and byte-admission state.
    pub fn async_runtime_snapshot(&self) -> AsyncRuntimeSnapshot {
        self.runtime.snapshot()
    }

    /// Queues [`Storage::apply_rollup_policies`] on the serialized write worker.
    pub async fn apply_rollup_policies(
        &self,
        policies: Vec<RollupPolicy>,
    ) -> Result<RollupObservabilitySnapshot> {
        self.ensure_open()?;
        let payload_bytes = modeled_rollup_policies_bytes(&policies)?;
        self.send_write(payload_bytes, |reply| WriteCommand::ApplyRollupPolicies {
            policies,
            reply,
        })
        .await
    }

    /// Queues [`Storage::trigger_rollup_run`] on the serialized write worker.
    pub async fn trigger_rollup_run(&self) -> Result<RollupObservabilitySnapshot> {
        self.ensure_open()?;
        self.send_write(0, |reply| WriteCommand::TriggerRollupRun { reply })
            .await
    }

    /// Queues an atomic on-disk snapshot after earlier queued writes.
    ///
    /// Backend-specific requirements from [`Storage::snapshot`] still apply.
    pub async fn snapshot(&self, path: impl AsRef<Path>) -> Result<()> {
        self.ensure_open()?;
        let path = path.as_ref().to_path_buf();
        let payload_bytes = path.as_os_str().as_encoded_bytes().len();
        self.send_write(payload_bytes, |reply| WriteCommand::Snapshot {
            path,
            reply,
        })
        .await
    }

    /// Closes the underlying storage through the serialized write worker.
    ///
    /// A successful call closes all clones of this facade. Additional facade operations, including
    /// another close, return [`TsinkError::StorageClosed`].
    pub async fn close(&self) -> Result<()> {
        if self
            .runtime
            .state
            .compare_exchange(
                STATE_OPEN,
                STATE_CLOSING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return Err(TsinkError::StorageClosed);
        }

        let mut state_guard = ClosingStateGuard::new(&self.runtime.state);
        let (reply, recv) = reply_channel();
        let reservation = self.runtime.write_budget.reserve(0)?;
        self.runtime
            .write_tx
            .send(QueuedWriteCommand {
                command: WriteCommand::Close { reply },
                reservation,
            })
            .await
            .map_err(|_| {
                state_guard.disarm();
                self.runtime.state.store(STATE_CLOSED, Ordering::SeqCst);
                runtime_stopped_error()
            })?;
        state_guard.disarm();

        recv_reply(recv).await
    }

    fn ensure_open(&self) -> Result<()> {
        if self.runtime.state.load(Ordering::SeqCst) != STATE_OPEN {
            return Err(TsinkError::StorageClosed);
        }
        Ok(())
    }

    async fn send_write<T>(
        &self,
        payload_bytes: usize,
        build: impl FnOnce(Reply<T>) -> WriteCommand,
    ) -> Result<T> {
        let reservation = self.runtime.write_budget.reserve(payload_bytes)?;
        let (reply, recv) = reply_channel();
        self.runtime
            .write_tx
            .send(QueuedWriteCommand {
                command: build(reply),
                reservation,
            })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    async fn send_read<T>(
        &self,
        payload_bytes: usize,
        build: impl FnOnce(Reply<T>) -> ReadCommand,
    ) -> Result<T> {
        let reservation = self.runtime.read_budget.reserve(payload_bytes)?;
        let cancellation = QueryCancellationToken::new();
        let mut cancellation_guard = CancelReadOnDrop::new(cancellation.clone());
        let (reply, recv) = reply_channel();
        self.runtime
            .read_tx
            .send(QueuedReadCommand {
                command: build(reply),
                cancellation,
                reservation,
            })
            .await
            .map_err(|_| runtime_stopped_error())?;
        let result = recv_reply(recv).await;
        cancellation_guard.disarm();
        result
    }
}

struct CancelReadOnDrop {
    cancellation: QueryCancellationToken,
    armed: bool,
}

impl CancelReadOnDrop {
    fn new(cancellation: QueryCancellationToken) -> Self {
        Self {
            cancellation,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelReadOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
        }
    }
}

struct ClosingStateGuard<'a> {
    state: &'a AtomicU8,
    armed: bool,
}

impl<'a> ClosingStateGuard<'a> {
    fn new(state: &'a AtomicU8) -> Self {
        Self { state, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ClosingStateGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.state.compare_exchange(
                STATE_CLOSING,
                STATE_OPEN,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        }
    }
}

/// Configures both the synchronous storage backend and its [`AsyncStorage`] worker facade.
///
/// Storage and queue settings use the finite [`ResourceProfile::Embedded`] base profile. Select
/// [`ResourceProfile::ExpertUnlimited`] explicitly for legacy storage/query defaults.
pub struct AsyncStorageBuilder {
    inner: StorageBuilder,
    async_options: AsyncRuntimeOptions,
    async_resource_overrides: BTreeSet<ResourceLimitOverride>,
}

impl Default for AsyncStorageBuilder {
    fn default() -> Self {
        let limits = crate::ResourceLimits::embedded().async_runtime;
        Self {
            inner: StorageBuilder::new(),
            async_options: AsyncRuntimeOptions {
                queue_capacity: limits.queue_command_capacity,
                write_queue_byte_capacity: limits.write_queue_byte_capacity,
                read_queue_byte_capacity: limits.read_queue_byte_capacity,
                read_workers: limits.read_workers,
            },
            async_resource_overrides: BTreeSet::new(),
        }
    }
}

impl AsyncStorageBuilder {
    /// Creates an async builder with default storage and runtime options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn apply_profile_async_limits(&mut self, profile: ResourceProfile) {
        let limits = profile
            .finite_limits()
            .map(|limits| limits.async_runtime)
            .unwrap_or_else(|| {
                let legacy = AsyncRuntimeOptions::default();
                AsyncResourceLimits {
                    queue_command_capacity: legacy.queue_capacity,
                    write_queue_byte_capacity: legacy.write_queue_byte_capacity,
                    read_queue_byte_capacity: legacy.read_queue_byte_capacity,
                    read_workers: legacy.read_workers,
                }
            });
        if !self
            .async_resource_overrides
            .contains(&ResourceLimitOverride::AsyncQueueCommands)
        {
            self.async_options.queue_capacity = limits.queue_command_capacity;
        }
        if !self
            .async_resource_overrides
            .contains(&ResourceLimitOverride::AsyncWriteQueueBytes)
        {
            self.async_options.write_queue_byte_capacity = limits.write_queue_byte_capacity;
        }
        if !self
            .async_resource_overrides
            .contains(&ResourceLimitOverride::AsyncReadQueueBytes)
        {
            self.async_options.read_queue_byte_capacity = limits.read_queue_byte_capacity;
        }
        if !self
            .async_resource_overrides
            .contains(&ResourceLimitOverride::AsyncReadWorkers)
        {
            self.async_options.read_workers = limits.read_workers;
        }
    }

    /// Selects the storage and async base profile while preserving low-level overrides.
    #[must_use]
    pub fn with_resource_profile(mut self, profile: ResourceProfile) -> Self {
        self.inner = self.inner.with_resource_profile(profile);
        self.apply_profile_async_limits(profile);
        self
    }

    /// Clears one storage or async override and reapplies the selected profile value.
    #[must_use]
    pub fn clear_resource_limit_override(mut self, field: ResourceLimitOverride) -> Self {
        self.inner = self.inner.clear_resource_limit_override(field);
        self.async_resource_overrides.remove(&field);
        let profile = self.inner.resource_profile();
        self.apply_profile_async_limits(profile);
        self
    }

    /// Clears all storage and async overrides and reapplies the selected profile.
    #[must_use]
    pub fn clear_resource_limit_overrides(mut self) -> Self {
        self.inner = self.inner.clear_resource_limit_overrides();
        self.async_resource_overrides.clear();
        let profile = self.inner.resource_profile();
        self.apply_profile_async_limits(profile);
        self
    }

    /// Sets the capacity of each bounded command queue.
    ///
    /// Zero is normalized to one.
    #[must_use]
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.async_resource_overrides
            .insert(ResourceLimitOverride::AsyncQueueCommands);
        self.async_options.queue_capacity = capacity.max(1);
        self
    }

    /// Sets the modeled byte capacity for write payloads waiting for the serialized worker.
    ///
    /// Zero permits only commands without owned input payloads. This is independent of the
    /// underlying storage engine's foreground-write transient-memory limits.
    #[must_use]
    pub fn with_write_queue_byte_capacity(mut self, bytes: usize) -> Self {
        self.async_resource_overrides
            .insert(ResourceLimitOverride::AsyncWriteQueueBytes);
        self.async_options.write_queue_byte_capacity = bytes;
        self
    }

    /// Sets the modeled byte capacity for read payloads waiting for the reader worker pool.
    ///
    /// Zero permits only commands without owned input payloads. Query result and intermediate
    /// memory remain governed by the underlying storage query budget.
    #[must_use]
    pub fn with_read_queue_byte_capacity(mut self, bytes: usize) -> Self {
        self.async_resource_overrides
            .insert(ResourceLimitOverride::AsyncReadQueueBytes);
        self.async_options.read_queue_byte_capacity = bytes;
        self
    }

    /// Sets the number of dedicated reader worker threads.
    ///
    /// Zero is normalized to one. Writes always use one serialized worker.
    #[must_use]
    pub fn with_read_workers(mut self, workers: usize) -> Self {
        self.async_resource_overrides
            .insert(ResourceLimitOverride::AsyncReadWorkers);
        self.async_options.read_workers = workers.max(1);
        self
    }

    /// Sets the local persistence path; see [`StorageBuilder::with_data_path`].
    #[must_use]
    pub fn with_data_path(mut self, path: impl AsRef<Path>) -> Self {
        self.inner = self.inner.with_data_path(path);
        self
    }

    /// Sets and enables the retention window; see [`StorageBuilder::with_retention`].
    #[must_use]
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.inner = self.inner.with_retention(retention);
        self
    }

    /// Enables or disables retention enforcement.
    #[must_use]
    pub fn with_retention_enforced(mut self, enforced: bool) -> Self {
        self.inner = self.inner.with_retention_enforced(enforced);
        self
    }

    /// Rejects samples farther than `max_future_skew` ahead of the storage clock.
    ///
    /// This is opt in; see [`StorageBuilder::with_max_future_skew`].
    #[must_use]
    pub fn with_max_future_skew(mut self, max_future_skew: Duration) -> Self {
        self.inner = self.inner.with_max_future_skew(max_future_skew);
        self
    }

    /// Sets the timestamp unit used by storage.
    #[must_use]
    pub fn with_timestamp_precision(mut self, precision: TimestampPrecision) -> Self {
        self.inner = self.inner.with_timestamp_precision(precision);
        self
    }

    /// Sets the target encoded chunk size in points.
    #[must_use]
    pub fn with_chunk_points(mut self, points: usize) -> Self {
        self.inner = self.inner.with_chunk_points(points);
        self
    }

    /// Sets the synchronous storage writer-concurrency limit.
    #[must_use]
    pub fn with_max_writers(mut self, max_writers: usize) -> Self {
        self.inner = self.inner.with_max_writers(max_writers);
        self
    }

    /// Sets how long storage waits to acquire writer permits.
    #[must_use]
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.with_write_timeout(timeout);
        self
    }

    /// Sets the width of active ingestion time partitions.
    #[must_use]
    pub fn with_partition_duration(mut self, duration: Duration) -> Self {
        self.inner = self.inner.with_partition_duration(duration);
        self
    }

    /// Sets the per-series partition-head bound.
    #[must_use]
    pub fn with_max_active_partition_heads_per_series(mut self, max_heads: usize) -> Self {
        self.inner = self
            .inner
            .with_max_active_partition_heads_per_series(max_heads);
        self
    }

    /// Sets the storage memory budget in bytes.
    ///
    /// The `Embedded` profile default is 512 MiB.
    #[must_use]
    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.inner = self.inner.with_memory_limit(bytes);
        self
    }

    /// Sets the maximum number of distinct metric-and-label series.
    ///
    /// The `Embedded` profile default is 1,000,000 series.
    #[must_use]
    pub fn with_cardinality_limit(mut self, series: usize) -> Self {
        self.inner = self.inner.with_cardinality_limit(series);
        self
    }

    /// Sets the per-series label-count limit.
    #[must_use]
    pub fn with_max_labels_per_series(mut self, labels: usize) -> Self {
        self.inner = self.inner.with_max_labels_per_series(labels);
        self
    }

    /// Sets the cumulative metric-and-label identity byte limit.
    #[must_use]
    pub fn with_max_series_identity_bytes(mut self, bytes: usize) -> Self {
        self.inner = self.inner.with_max_series_identity_bytes(bytes);
        self
    }

    /// Limits successful new-series creation during each fixed storage-clock window.
    #[must_use]
    pub fn with_series_creation_rate_limit(
        mut self,
        max_new_series: usize,
        window: Duration,
    ) -> Self {
        self.inner = self
            .inner
            .with_series_creation_rate_limit(max_new_series, window);
        self
    }

    /// Configures pre-allocation row-count and modeled-input-byte write limits.
    #[must_use]
    pub fn with_write_batch_limits(mut self, limits: WriteBatchLimits) -> Self {
        self.inner = self.inner.with_write_batch_limits(limits);
        self
    }

    /// Enables or disables the WAL for persistent storage.
    #[must_use]
    pub fn with_wal_enabled(mut self, enabled: bool) -> Self {
        self.inner = self.inner.with_wal_enabled(enabled);
        self
    }

    /// Sets the maximum on-disk WAL size in bytes.
    ///
    /// The `Embedded` profile default is 512 MiB.
    #[must_use]
    pub fn with_wal_size_limit(mut self, bytes: usize) -> Self {
        self.inner = self.inner.with_wal_size_limit(bytes);
        self
    }

    /// Sets the managed local data-directory byte limit.
    #[must_use]
    pub fn with_local_disk_limit(mut self, bytes: u64) -> Self {
        self.inner = self.inner.with_local_disk_limit(bytes);
        self
    }

    /// Sets the filesystem free-space floor for local storage.
    #[must_use]
    pub fn with_filesystem_free_headroom(mut self, bytes: u64) -> Self {
        self.inner = self.inner.with_filesystem_free_headroom(bytes);
        self
    }

    /// Reserves local-disk bytes for maintenance temporary output.
    #[must_use]
    pub fn with_maintenance_temp_reserve(mut self, bytes: u64) -> Self {
        self.inner = self.inner.with_maintenance_temp_reserve(bytes);
        self
    }

    /// Sets the userspace WAL writer-buffer capacity in bytes.
    #[must_use]
    pub fn with_wal_buffer_size(mut self, size: usize) -> Self {
        self.inner = self.inner.with_wal_buffer_size(size);
        self
    }

    /// Selects the WAL synchronization policy.
    #[must_use]
    pub fn with_wal_sync_mode(mut self, mode: WalSyncMode) -> Self {
        self.inner = self.inner.with_wal_sync_mode(mode);
        self
    }

    /// Sets the logical WAL replay policy after open-time validation.
    ///
    /// The underlying storage builder defaults to [`WalReplayMode::Strict`] and always validates
    /// the complete published WAL prefix strictly before replay. `Salvage` therefore does not
    /// provide in-place recovery of a corrupt persistent data directory.
    #[must_use]
    pub fn with_wal_replay_mode(mut self, mode: WalReplayMode) -> Self {
        self.inner = self.inner.with_wal_replay_mode(mode);
        self
    }

    /// Controls whether background durability worker failures fence service.
    ///
    /// The underlying storage builder defaults to `true`.
    #[must_use]
    pub fn with_background_fail_fast(mut self, enabled: bool) -> Self {
        self.inner = self.inner.with_background_fail_fast(enabled);
        self
    }

    /// Configures the shared query budget owned by the underlying storage instance.
    #[must_use]
    pub fn with_query_budget_limits(mut self, limits: QueryBudgetLimits) -> Self {
        self.inner = self.inner.with_query_budget_limits(limits);
        self
    }

    /// Sets the maximum logical items selected by one maintenance pass.
    #[must_use]
    pub fn with_maintenance_max_items_per_pass(mut self, max_items: usize) -> Self {
        self.inner = self.inner.with_maintenance_max_items_per_pass(max_items);
        self
    }

    /// Sets the maximum modeled bytes selected by one maintenance pass.
    #[must_use]
    pub fn with_maintenance_max_bytes_per_pass(mut self, max_bytes: u64) -> Self {
        self.inner = self.inner.with_maintenance_max_bytes_per_pass(max_bytes);
        self
    }

    /// Builds the synchronous storage backend and starts the async worker threads.
    ///
    /// Call [`AsyncStorage::close`] during host shutdown to surface storage shutdown errors.
    pub fn build(self) -> Result<AsyncStorage> {
        let storage = self.inner.build()?;
        AsyncStorage::from_storage_with_options_and_overrides(
            storage,
            self.async_options,
            self.async_resource_overrides.into_iter().collect(),
        )
    }
}

fn write_worker_loop(
    storage: Arc<dyn Storage>,
    state: Arc<AtomicU8>,
    receiver: async_channel::Receiver<QueuedWriteCommand>,
) {
    while let Ok(queued) = receiver.recv_blocking() {
        let QueuedWriteCommand {
            command,
            mut reservation,
        } = queued;
        reservation.release();
        drop(reservation);
        match command {
            WriteCommand::InsertRows { rows, reply } => {
                // Writes are side-effecting: once accepted into the queue they must run,
                // even if the caller drops/cancels the awaiting future.
                let result = storage.insert_rows(&rows);
                let _ = reply.send_blocking(result);
            }
            WriteCommand::InsertRowsWithResult { rows, reply } => {
                let result = storage.insert_rows_with_result(&rows);
                let _ = reply.send_blocking(result);
            }
            WriteCommand::WriteBatch { rows, mode, reply } => {
                let result = storage.write_batch(&rows, mode);
                let _ = reply.send_blocking(result);
            }
            WriteCommand::Snapshot { path, reply } => {
                let result = storage.snapshot(&path);
                let _ = reply.send_blocking(result);
            }
            WriteCommand::ApplyRollupPolicies { policies, reply } => {
                let result = storage.apply_rollup_policies(policies);
                let _ = reply.send_blocking(result);
            }
            WriteCommand::TriggerRollupRun { reply } => {
                let result = storage.trigger_rollup_run();
                let _ = reply.send_blocking(result);
            }
            WriteCommand::Close { reply } => {
                let result = storage.close();
                if result.is_ok() {
                    state.store(STATE_CLOSED, Ordering::SeqCst);
                } else {
                    state.store(STATE_OPEN, Ordering::SeqCst);
                }
                let _ = reply.send_blocking(result);
            }
        }
    }

    state.store(STATE_CLOSED, Ordering::SeqCst);
}

fn read_worker_loop(
    storage: Arc<dyn Storage>,
    receiver: async_channel::Receiver<QueuedReadCommand>,
) {
    while let Ok(queued) = receiver.recv_blocking() {
        let QueuedReadCommand {
            command,
            cancellation,
            mut reservation,
        } = queued;
        reservation.release();
        drop(reservation);

        if command.reply_is_closed() {
            continue;
        }
        let execution =
            match storage.begin_query_execution(QueryWorkLimits::default(), cancellation.clone()) {
                Ok(execution) => execution,
                Err(error) => {
                    command.send_error(error);
                    continue;
                }
            };

        match command {
            ReadCommand::Select {
                metric,
                labels,
                start,
                end,
                reply,
            } => {
                let result = run_read_with_execution(
                    execution,
                    &cancellation,
                    |execution| {
                        storage.select_with_execution(&metric, &labels, start, end, execution)
                    },
                    || storage.select(&metric, &labels, start, end),
                );
                let _ = reply.send_blocking(result);
            }
            ReadCommand::SelectWithOptions {
                metric,
                options,
                reply,
            } => {
                let options_with_execution = options.clone();
                let result = run_read_with_execution(
                    execution,
                    &cancellation,
                    |execution| {
                        storage.select_with_options_with_execution(
                            &metric,
                            options_with_execution,
                            execution,
                        )
                    },
                    || storage.select_with_options(&metric, options),
                );
                let _ = reply.send_blocking(result);
            }
            ReadCommand::SelectAll {
                metric,
                start,
                end,
                reply,
            } => {
                let result = run_read_with_execution(
                    execution,
                    &cancellation,
                    |execution| storage.select_all_with_execution(&metric, start, end, execution),
                    || storage.select_all(&metric, start, end),
                );
                let _ = reply.send_blocking(result);
            }
            ReadCommand::ListMetrics { reply } => {
                let result = run_read_with_execution(
                    execution,
                    &cancellation,
                    |execution| {
                        if storage.list_metrics_execution_accounting()
                            != QueryExecutionAccounting::Complete
                        {
                            return Err(incomplete_async_metadata_accounting("async_list_metrics"));
                        }
                        let detailed = storage.list_metrics_with_execution_result(execution)?;
                        validate_async_metadata_result(detailed, "async_list_metrics")
                    },
                    || {
                        storage
                            .list_metrics()
                            .map(SelectSeriesExecutionResult::unaccounted)
                    },
                );
                let _ = reply.send_blocking(result);
            }
            ReadCommand::SelectSeries { selection, reply } => {
                let result = run_read_with_execution(
                    execution,
                    &cancellation,
                    |execution| {
                        if storage.select_series_execution_accounting()
                            != QueryExecutionAccounting::Complete
                        {
                            return Err(incomplete_async_metadata_accounting(
                                "async_select_series",
                            ));
                        }
                        let detailed =
                            storage.select_series_with_execution_result(&selection, execution)?;
                        validate_async_metadata_result(detailed, "async_select_series")
                    },
                    || {
                        storage
                            .select_series(&selection)
                            .map(SelectSeriesExecutionResult::unaccounted)
                    },
                );
                let _ = reply.send_blocking(result);
            }
            ReadCommand::ScanSeriesRows {
                series,
                start,
                end,
                options,
                reply,
            } => {
                let result = run_read_with_execution(
                    execution,
                    &cancellation,
                    |execution| {
                        if storage.scan_series_rows_execution_accounting()
                            != QueryExecutionAccounting::Complete
                        {
                            return Err(incomplete_async_row_scan_accounting(
                                "async_scan_series_rows",
                            ));
                        }
                        let detailed = storage.scan_series_rows_with_execution_result(
                            &series, start, end, options, execution,
                        )?;
                        validate_async_row_scan_result(detailed, "async_scan_series_rows")
                    },
                    || {
                        storage
                            .scan_series_rows(&series, start, end, options)
                            .map(QueryRowsExecutionResult::unaccounted)
                    },
                );
                let _ = reply.send_blocking(result);
            }
            ReadCommand::ScanMetricRows {
                metric,
                start,
                end,
                options,
                reply,
            } => {
                let result = run_read_with_execution(
                    execution,
                    &cancellation,
                    |execution| {
                        if storage.scan_metric_rows_execution_accounting()
                            != QueryExecutionAccounting::Complete
                        {
                            return Err(incomplete_async_row_scan_accounting(
                                "async_scan_metric_rows",
                            ));
                        }
                        let detailed = storage.scan_metric_rows_with_execution_result(
                            &metric, start, end, options, execution,
                        )?;
                        validate_async_row_scan_result(detailed, "async_scan_metric_rows")
                    },
                    || {
                        storage
                            .scan_metric_rows(&metric, start, end, options)
                            .map(QueryRowsExecutionResult::unaccounted)
                    },
                );
                let _ = reply.send_blocking(result);
            }
        }
    }
}

fn incomplete_async_row_scan_accounting(operation: &'static str) -> TsinkError {
    TsinkError::UnsupportedOperation {
        operation,
        reason: "bounded async row scans require complete execution accounting".to_string(),
    }
}

fn incomplete_async_metadata_accounting(operation: &'static str) -> TsinkError {
    TsinkError::UnsupportedOperation {
        operation,
        reason: "bounded async metadata reads require complete execution accounting".to_string(),
    }
}

fn validate_async_metadata_result(
    mut detailed: SelectSeriesExecutionResult,
    operation: &'static str,
) -> Result<SelectSeriesExecutionResult> {
    let required =
        crate::engine::engine::modeled_metric_series_vec_retained_bytes(&detailed.series);
    let Some(reservation) = detailed.take_memory_reservation() else {
        drop(detailed);
        return Err(incomplete_async_metadata_accounting(operation));
    };
    if reservation.bytes() < required {
        let error = incomplete_async_metadata_accounting(operation);
        // `detailed` no longer owns the detached guard. Destroy the series before releasing it;
        // implicit reverse local-drop order would release `reservation` first.
        drop(detailed);
        drop(reservation);
        return Err(error);
    }
    let series = detailed.into_series();
    // The detailed result stores `series` before its guard, preserving payload-before-guard drop.
    Ok(SelectSeriesExecutionResult::accounted(series, reservation))
}

fn validate_async_row_scan_result(
    mut detailed: QueryRowsExecutionResult,
    operation: &'static str,
) -> Result<QueryRowsExecutionResult> {
    let required = crate::modeled_query_rows_retained_bytes(&detailed.page.rows);
    let Some(reservation) = detailed.take_memory_reservation() else {
        drop(detailed);
        return Err(incomplete_async_row_scan_accounting(operation));
    };
    if reservation.bytes() < required {
        let error = incomplete_async_row_scan_accounting(operation);
        // Keep the detached guard live until the page has been destroyed on every error path.
        drop(detailed);
        drop(reservation);
        return Err(error);
    }
    let page = detailed.into_page();
    // The detailed result stores `page` before its guard, preserving payload-before-guard drop.
    Ok(QueryRowsExecutionResult::accounted(page, reservation))
}

impl ReadCommand {
    fn reply_is_closed(&self) -> bool {
        match self {
            Self::Select { reply, .. } => reply.is_closed(),
            Self::SelectWithOptions { reply, .. } => reply.is_closed(),
            Self::SelectAll { reply, .. } => reply.is_closed(),
            Self::ListMetrics { reply } => reply.is_closed(),
            Self::SelectSeries { reply, .. } => reply.is_closed(),
            Self::ScanSeriesRows { reply, .. } => reply.is_closed(),
            Self::ScanMetricRows { reply, .. } => reply.is_closed(),
        }
    }

    fn send_error(self, error: TsinkError) {
        match self {
            Self::Select { reply, .. } => {
                let _ = reply.send_blocking(Err(error));
            }
            Self::SelectWithOptions { reply, .. } => {
                let _ = reply.send_blocking(Err(error));
            }
            Self::SelectAll { reply, .. } => {
                let _ = reply.send_blocking(Err(error));
            }
            Self::ListMetrics { reply } => {
                let _ = reply.send_blocking(Err(error));
            }
            Self::SelectSeries { reply, .. } => {
                let _ = reply.send_blocking(Err(error));
            }
            Self::ScanSeriesRows { reply, .. } => {
                let _ = reply.send_blocking(Err(error));
            }
            Self::ScanMetricRows { reply, .. } => {
                let _ = reply.send_blocking(Err(error));
            }
        }
    }
}

fn run_read_with_execution<T>(
    execution: Option<QueryExecution>,
    cancellation: &QueryCancellationToken,
    with_execution: impl FnOnce(&QueryExecution) -> Result<T>,
    without_execution: impl FnOnce() -> Result<T>,
) -> Result<T> {
    match execution {
        Some(execution) => {
            execution.checkpoint()?;
            let value = with_execution(&execution)?;
            execution.checkpoint()?;
            drop(execution);
            Ok(value)
        }
        None => {
            cancellation.checkpoint()?;
            let value = without_execution()?;
            cancellation.checkpoint()?;
            Ok(value)
        }
    }
}

fn checked_async_size_add(queue: &'static str, lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_add(rhs)
        .ok_or(TsinkError::AsyncQueuePayloadSizeOverflow { queue })
}

fn checked_async_size_mul(queue: &'static str, lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_mul(rhs)
        .ok_or(TsinkError::AsyncQueuePayloadSizeOverflow { queue })
}

fn modeled_write_rows_bytes(rows: &[Row]) -> Result<usize> {
    crate::modeled_write_batch_input_bytes(rows).map_err(|error| match error {
        TsinkError::WriteBatchSizeOverflow => TsinkError::AsyncQueuePayloadSizeOverflow {
            queue: WRITE_QUEUE_NAME,
        },
        other => other,
    })
}

fn modeled_labels_bytes(labels: &[Label], queue: &'static str) -> Result<usize> {
    let mut bytes = checked_async_size_mul(queue, labels.len(), std::mem::size_of::<Label>())?;
    for label in labels {
        bytes = checked_async_size_add(queue, bytes, label.name.len())?;
        bytes = checked_async_size_add(queue, bytes, label.value.len())?;
    }
    Ok(bytes)
}

fn modeled_metric_and_labels_bytes(metric: &str, labels: &[Label]) -> Result<usize> {
    checked_async_size_add(
        READ_QUEUE_NAME,
        metric.len(),
        modeled_labels_bytes(labels, READ_QUEUE_NAME)?,
    )
}

fn modeled_query_options_bytes(metric: &str, options: &QueryOptions) -> Result<usize> {
    modeled_metric_and_labels_bytes(metric, &options.labels)
}

fn modeled_metric_series_bytes(series: &[MetricSeries]) -> Result<usize> {
    let mut bytes = checked_async_size_mul(
        READ_QUEUE_NAME,
        series.len(),
        std::mem::size_of::<MetricSeries>(),
    )?;
    for entry in series {
        bytes = checked_async_size_add(READ_QUEUE_NAME, bytes, entry.name.len())?;
        bytes = checked_async_size_add(
            READ_QUEUE_NAME,
            bytes,
            modeled_labels_bytes(&entry.labels, READ_QUEUE_NAME)?,
        )?;
    }
    Ok(bytes)
}

fn modeled_series_selection_bytes(selection: &SeriesSelection) -> Result<usize> {
    let mut bytes = selection.metric.as_ref().map_or(0, String::len);
    bytes = checked_async_size_add(
        READ_QUEUE_NAME,
        bytes,
        checked_async_size_mul(
            READ_QUEUE_NAME,
            selection.matchers.len(),
            std::mem::size_of::<crate::SeriesMatcher>(),
        )?,
    )?;
    for matcher in &selection.matchers {
        bytes = checked_async_size_add(READ_QUEUE_NAME, bytes, matcher.name.len())?;
        bytes = checked_async_size_add(READ_QUEUE_NAME, bytes, matcher.value.len())?;
    }
    Ok(bytes)
}

fn modeled_rollup_policies_bytes(policies: &[RollupPolicy]) -> Result<usize> {
    let mut bytes = checked_async_size_mul(
        WRITE_QUEUE_NAME,
        policies.len(),
        std::mem::size_of::<RollupPolicy>(),
    )?;
    for policy in policies {
        bytes = checked_async_size_add(WRITE_QUEUE_NAME, bytes, policy.id.len())?;
        bytes = checked_async_size_add(WRITE_QUEUE_NAME, bytes, policy.metric.len())?;
        bytes = checked_async_size_add(
            WRITE_QUEUE_NAME,
            bytes,
            modeled_labels_bytes(&policy.match_labels, WRITE_QUEUE_NAME)?,
        )?;
    }
    Ok(bytes)
}

fn reply_channel<T>() -> (Reply<T>, async_channel::Receiver<Result<T>>) {
    async_channel::bounded(1)
}

async fn recv_reply<T>(receiver: async_channel::Receiver<Result<T>>) -> Result<T> {
    receiver.recv().await.map_err(|_| runtime_stopped_error())?
}

fn runtime_stopped_error() -> TsinkError {
    TsinkError::Other("async runtime worker stopped unexpectedly".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Barrier, Condvar, Mutex as StdMutex};

    #[test]
    fn plain_read_helper_releases_execution_before_result_handoff() {
        let budget =
            crate::QueryBudget::new(QueryBudgetLimits::default()).expect("budget should build");
        let execution = budget.begin_query().expect("query should admit");
        let cancellation = QueryCancellationToken::new();
        assert_eq!(budget.snapshot().active_queries, 1);

        let value = run_read_with_execution(
            Some(execution),
            &cancellation,
            |execution| {
                let reservation = execution.reserve_memory(1)?;
                assert_eq!(execution.snapshot().memory_reserved_bytes, 1);
                drop(reservation);
                Ok(7)
            },
            || unreachable!("the admitted query must use the execution-aware path"),
        )
        .expect("read helper should succeed");

        assert_eq!(value, 7);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn queue_byte_budget_is_exact_at_n_and_rejects_n_plus_one() {
        let budget = QueueByteBudget::new("test", 64);
        let reservation = budget.reserve(64).expect("N bytes must be admitted");
        assert_eq!(budget.current.load(Ordering::Acquire), 64);

        assert!(matches!(
            budget.reserve(1),
            Err(TsinkError::AsyncQueueByteLimitExceeded {
                queue: "test",
                limit: 64,
                current: 64,
                requested: 1,
            })
        ));
        assert_eq!(budget.rejections.load(Ordering::Acquire), 1);

        drop(reservation);
        assert_eq!(budget.current.load(Ordering::Acquire), 0);
        assert_eq!(budget.peak.load(Ordering::Acquire), 64);

        assert!(matches!(
            budget.reserve(65),
            Err(TsinkError::AsyncQueueByteLimitExceeded {
                queue: "test",
                limit: 64,
                current: 0,
                requested: 65,
            })
        ));
    }

    #[test]
    fn concurrent_queue_reservations_cannot_bypass_the_cap() {
        const PRODUCERS: usize = 16;
        let budget = QueueByteBudget::new("test", 32);
        let start = Arc::new(Barrier::new(PRODUCERS + 1));
        let release = Arc::new((StdMutex::new(false), Condvar::new()));
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let mut handles = Vec::new();

        for _ in 0..PRODUCERS {
            let budget = Arc::clone(&budget);
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            let result_tx = result_tx.clone();
            handles.push(std::thread::spawn(move || {
                start.wait();
                let reservation = budget.reserve(32).ok();
                result_tx.send(reservation.is_some()).unwrap();
                let (lock, wake) = &*release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
                drop(reservation);
            }));
        }
        drop(result_tx);

        start.wait();
        let admitted = (0..PRODUCERS)
            .map(|_| result_rx.recv().unwrap())
            .filter(|admitted| *admitted)
            .count();
        assert_eq!(admitted, 1);
        assert_eq!(budget.current.load(Ordering::Acquire), 32);
        assert_eq!(
            budget.rejections.load(Ordering::Acquire),
            (PRODUCERS - 1) as u64
        );

        let (lock, wake) = &*release;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(budget.current.load(Ordering::Acquire), 0);
    }

    #[test]
    fn queue_reservation_release_is_idempotent_and_never_underflows() {
        let budget = QueueByteBudget::new("test", 7);
        let mut reservation = budget.reserve(7).unwrap();
        reservation.release();
        reservation.release();
        drop(reservation);

        assert_eq!(budget.current.load(Ordering::Acquire), 0);
        assert_eq!(budget.peak.load(Ordering::Acquire), 7);

        let next = budget.reserve(7).unwrap();
        drop(next);
        assert_eq!(budget.current.load(Ordering::Acquire), 0);
    }

    #[test]
    fn closed_channel_send_failure_releases_queue_bytes() {
        let budget = QueueByteBudget::new(WRITE_QUEUE_NAME, 9);
        let (sender, receiver) = async_channel::bounded(1);
        drop(receiver);
        let (reply, _reply_receiver) = reply_channel();
        let command = QueuedWriteCommand {
            command: WriteCommand::TriggerRollupRun { reply },
            reservation: budget.reserve(9).unwrap(),
        };

        let error = sender
            .try_send(command)
            .expect_err("a closed channel must return the owned command");
        drop(error);
        assert_eq!(budget.current.load(Ordering::Acquire), 0);
        assert_eq!(budget.peak.load(Ordering::Acquire), 9);
    }
}
