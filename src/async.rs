//! Runtime-agnostic async facade over sync `Storage` using dedicated worker threads.

use crate::cgroup;
use crate::wal::{WalReplayMode, WalSyncMode};
use crate::{
    DataPoint, Label, MetricSeries, QueryOptions, QueryRowsPage, QueryRowsScanOptions, Result,
    RollupObservabilitySnapshot, RollupPolicy, Row, SeriesSelection, Storage, StorageBuilder,
    TimestampPrecision, TsinkError, WriteResult,
};
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

const STATE_OPEN: u8 = 0;
const STATE_CLOSING: u8 = 1;
const STATE_CLOSED: u8 = 2;
const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// Runtime settings for the async service layer.
#[derive(Debug, Clone, Copy)]
pub struct AsyncRuntimeOptions {
    /// Maximum number of in-flight requests accepted per queue.
    pub queue_capacity: usize,
    /// Number of dedicated reader worker threads.
    pub read_workers: usize,
}

impl Default for AsyncRuntimeOptions {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            read_workers: cgroup::default_workers_limit().max(1),
        }
    }
}

impl AsyncRuntimeOptions {
    fn normalized(self) -> Self {
        Self {
            queue_capacity: self.queue_capacity.max(1),
            read_workers: self.read_workers.max(1),
        }
    }
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
        reply: Reply<Vec<MetricSeries>>,
    },
    SelectSeries {
        selection: SeriesSelection,
        reply: Reply<Vec<MetricSeries>>,
    },
    ScanSeriesRows {
        series: Vec<MetricSeries>,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        reply: Reply<QueryRowsPage>,
    },
    ScanMetricRows {
        metric: String,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        reply: Reply<QueryRowsPage>,
    },
}

struct AsyncRuntime {
    storage: Arc<dyn Storage>,
    state: Arc<AtomicU8>,
    write_tx: async_channel::Sender<WriteCommand>,
    read_tx: async_channel::Sender<ReadCommand>,
    worker_handles: Mutex<Vec<JoinHandle<()>>>,
}

impl AsyncRuntime {
    fn new(storage: Arc<dyn Storage>, options: AsyncRuntimeOptions) -> Result<Self> {
        let options = options.normalized();
        let state = Arc::new(AtomicU8::new(STATE_OPEN));

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
            write_tx,
            read_tx,
            worker_handles: Mutex::new(worker_handles),
        })
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

/// Async facade for `Storage` backed by dedicated worker threads.
#[derive(Clone)]
pub struct AsyncStorage {
    runtime: Arc<AsyncRuntime>,
}

impl AsyncStorage {
    pub fn from_storage(storage: Arc<dyn Storage>) -> Result<Self> {
        Self::from_storage_with_options(storage, AsyncRuntimeOptions::default())
    }

    pub fn from_storage_with_options(
        storage: Arc<dyn Storage>,
        options: AsyncRuntimeOptions,
    ) -> Result<Self> {
        Ok(Self {
            runtime: Arc::new(AsyncRuntime::new(storage, options)?),
        })
    }

    pub fn inner(&self) -> Arc<dyn Storage> {
        Arc::clone(&self.runtime.storage)
    }

    pub fn into_inner(self) -> Arc<dyn Storage> {
        Arc::clone(&self.runtime.storage)
    }

    pub async fn insert_rows(&self, rows: Vec<Row>) -> Result<()> {
        self.ensure_open()?;
        let (reply, recv) = reply_channel();
        self.runtime
            .write_tx
            .send(WriteCommand::InsertRows { rows, reply })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    /// Inserts rows and returns the durability guarantee established when the call succeeds.
    pub async fn insert_rows_with_result(&self, rows: Vec<Row>) -> Result<WriteResult> {
        self.ensure_open()?;
        let (reply, recv) = reply_channel();
        self.runtime
            .write_tx
            .send(WriteCommand::InsertRowsWithResult { rows, reply })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    pub async fn select(
        &self,
        metric: impl Into<String>,
        labels: Vec<Label>,
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        self.ensure_open()?;
        let (reply, recv) = reply_channel();
        self.runtime
            .read_tx
            .send(ReadCommand::Select {
                metric: metric.into(),
                labels,
                start,
                end,
                reply,
            })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    pub async fn select_with_options(
        &self,
        metric: impl Into<String>,
        options: QueryOptions,
    ) -> Result<Vec<DataPoint>> {
        self.ensure_open()?;
        let (reply, recv) = reply_channel();
        self.runtime
            .read_tx
            .send(ReadCommand::SelectWithOptions {
                metric: metric.into(),
                options,
                reply,
            })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    pub async fn select_all(
        &self,
        metric: impl Into<String>,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.ensure_open()?;
        let (reply, recv) = reply_channel();
        self.runtime
            .read_tx
            .send(ReadCommand::SelectAll {
                metric: metric.into(),
                start,
                end,
                reply,
            })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    pub async fn scan_series_rows(
        &self,
        series: Vec<MetricSeries>,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        self.ensure_open()?;
        let (reply, recv) = reply_channel();
        self.runtime
            .read_tx
            .send(ReadCommand::ScanSeriesRows {
                series,
                start,
                end,
                options,
                reply,
            })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    pub async fn scan_metric_rows(
        &self,
        metric: impl Into<String>,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        self.ensure_open()?;
        let (reply, recv) = reply_channel();
        self.runtime
            .read_tx
            .send(ReadCommand::ScanMetricRows {
                metric: metric.into(),
                start,
                end,
                options,
                reply,
            })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    pub async fn list_metrics(&self) -> Result<Vec<MetricSeries>> {
        self.ensure_open()?;
        let (reply, recv) = reply_channel();
        self.runtime
            .read_tx
            .send(ReadCommand::ListMetrics { reply })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    pub async fn select_series(&self, selection: SeriesSelection) -> Result<Vec<MetricSeries>> {
        self.ensure_open()?;
        let (reply, recv) = reply_channel();
        self.runtime
            .read_tx
            .send(ReadCommand::SelectSeries { selection, reply })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    pub fn memory_used(&self) -> usize {
        self.runtime.storage.memory_used()
    }

    pub fn memory_budget(&self) -> usize {
        self.runtime.storage.memory_budget()
    }

    pub async fn apply_rollup_policies(
        &self,
        policies: Vec<RollupPolicy>,
    ) -> Result<RollupObservabilitySnapshot> {
        self.ensure_open()?;
        let (reply, recv) = reply_channel();
        self.runtime
            .write_tx
            .send(WriteCommand::ApplyRollupPolicies { policies, reply })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    pub async fn trigger_rollup_run(&self) -> Result<RollupObservabilitySnapshot> {
        self.ensure_open()?;
        let (reply, recv) = reply_channel();
        self.runtime
            .write_tx
            .send(WriteCommand::TriggerRollupRun { reply })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    /// Writes an atomic on-disk snapshot to `path`.
    pub async fn snapshot(&self, path: impl AsRef<Path>) -> Result<()> {
        self.ensure_open()?;
        let (reply, recv) = reply_channel();
        self.runtime
            .write_tx
            .send(WriteCommand::Snapshot {
                path: path.as_ref().to_path_buf(),
                reply,
            })
            .await
            .map_err(|_| runtime_stopped_error())?;
        recv_reply(recv).await
    }

    /// Close the storage. Additional operations return `StorageClosed`.
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
        self.runtime
            .write_tx
            .send(WriteCommand::Close { reply })
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

/// Builder for [`AsyncStorage`].
pub struct AsyncStorageBuilder {
    inner: StorageBuilder,
    async_options: AsyncRuntimeOptions,
}

impl Default for AsyncStorageBuilder {
    fn default() -> Self {
        Self {
            inner: StorageBuilder::new(),
            async_options: AsyncRuntimeOptions::default(),
        }
    }
}

impl AsyncStorageBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.async_options.queue_capacity = capacity.max(1);
        self
    }

    #[must_use]
    pub fn with_read_workers(mut self, workers: usize) -> Self {
        self.async_options.read_workers = workers.max(1);
        self
    }

    #[must_use]
    pub fn with_data_path(mut self, path: impl AsRef<Path>) -> Self {
        self.inner = self.inner.with_data_path(path);
        self
    }

    #[must_use]
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.inner = self.inner.with_retention(retention);
        self
    }

    #[must_use]
    pub fn with_retention_enforced(mut self, enforced: bool) -> Self {
        self.inner = self.inner.with_retention_enforced(enforced);
        self
    }

    #[must_use]
    pub fn with_timestamp_precision(mut self, precision: TimestampPrecision) -> Self {
        self.inner = self.inner.with_timestamp_precision(precision);
        self
    }

    #[must_use]
    pub fn with_chunk_points(mut self, points: usize) -> Self {
        self.inner = self.inner.with_chunk_points(points);
        self
    }

    #[must_use]
    pub fn with_max_writers(mut self, max_writers: usize) -> Self {
        self.inner = self.inner.with_max_writers(max_writers);
        self
    }

    #[must_use]
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.with_write_timeout(timeout);
        self
    }

    #[must_use]
    pub fn with_partition_duration(mut self, duration: Duration) -> Self {
        self.inner = self.inner.with_partition_duration(duration);
        self
    }

    #[must_use]
    pub fn with_max_active_partition_heads_per_series(mut self, max_heads: usize) -> Self {
        self.inner = self
            .inner
            .with_max_active_partition_heads_per_series(max_heads);
        self
    }

    #[must_use]
    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.inner = self.inner.with_memory_limit(bytes);
        self
    }

    #[must_use]
    pub fn with_cardinality_limit(mut self, series: usize) -> Self {
        self.inner = self.inner.with_cardinality_limit(series);
        self
    }

    #[must_use]
    pub fn with_wal_enabled(mut self, enabled: bool) -> Self {
        self.inner = self.inner.with_wal_enabled(enabled);
        self
    }

    #[must_use]
    pub fn with_wal_size_limit(mut self, bytes: usize) -> Self {
        self.inner = self.inner.with_wal_size_limit(bytes);
        self
    }

    #[must_use]
    pub fn with_wal_buffer_size(mut self, size: usize) -> Self {
        self.inner = self.inner.with_wal_buffer_size(size);
        self
    }

    #[must_use]
    pub fn with_wal_sync_mode(mut self, mode: WalSyncMode) -> Self {
        self.inner = self.inner.with_wal_sync_mode(mode);
        self
    }

    /// Sets WAL replay policy when corruption is encountered mid-log.
    ///
    /// The underlying storage builder defaults to [`WalReplayMode::Strict`].
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

    pub fn build(self) -> Result<AsyncStorage> {
        let storage = self.inner.build()?;
        AsyncStorage::from_storage_with_options(storage, self.async_options)
    }
}

fn write_worker_loop(
    storage: Arc<dyn Storage>,
    state: Arc<AtomicU8>,
    receiver: async_channel::Receiver<WriteCommand>,
) {
    while let Ok(command) = receiver.recv_blocking() {
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

fn read_worker_loop(storage: Arc<dyn Storage>, receiver: async_channel::Receiver<ReadCommand>) {
    while let Ok(command) = receiver.recv_blocking() {
        match command {
            ReadCommand::Select {
                metric,
                labels,
                start,
                end,
                reply,
            } => {
                if reply.is_closed() {
                    continue;
                }
                let result = storage.select(&metric, &labels, start, end);
                let _ = reply.send_blocking(result);
            }
            ReadCommand::SelectWithOptions {
                metric,
                options,
                reply,
            } => {
                if reply.is_closed() {
                    continue;
                }
                let result = storage.select_with_options(&metric, options);
                let _ = reply.send_blocking(result);
            }
            ReadCommand::SelectAll {
                metric,
                start,
                end,
                reply,
            } => {
                if reply.is_closed() {
                    continue;
                }
                let result = storage.select_all(&metric, start, end);
                let _ = reply.send_blocking(result);
            }
            ReadCommand::ListMetrics { reply } => {
                if reply.is_closed() {
                    continue;
                }
                let result = storage.list_metrics();
                let _ = reply.send_blocking(result);
            }
            ReadCommand::SelectSeries { selection, reply } => {
                if reply.is_closed() {
                    continue;
                }
                let result = storage.select_series(&selection);
                let _ = reply.send_blocking(result);
            }
            ReadCommand::ScanSeriesRows {
                series,
                start,
                end,
                options,
                reply,
            } => {
                if reply.is_closed() {
                    continue;
                }
                let result = storage.scan_series_rows(&series, start, end, options);
                let _ = reply.send_blocking(result);
            }
            ReadCommand::ScanMetricRows {
                metric,
                start,
                end,
                options,
                reply,
            } => {
                if reply.is_closed() {
                    continue;
                }
                let result = storage.scan_metric_rows(&metric, start, end, options);
                let _ = reply.send_blocking(result);
            }
        }
    }
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
