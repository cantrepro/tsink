//! Main storage implementation for tsink.

use crate::concurrency::Semaphore;
use crate::disk::DiskPartition;
use crate::list::PartitionList;
use crate::memory::MemoryPartition;
use crate::partition::SharedPartition;
use crate::wal::{DiskWal, NopWal, Wal, WalReader, WalSyncMode};
use crate::{DataPoint, Label, Result, Row, TsinkError};
use crossbeam_channel::{Sender, bounded};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread;
use std::time::Duration;
use tracing::{error, info, warn};

const WRITABLE_PARTITIONS_NUM: usize = 2;
const STORAGE_OPEN: u8 = 0;
const STORAGE_CLOSING: u8 = 1;
const STORAGE_CLOSED: u8 = 2;
const MAX_METRIC_NAME_LEN: usize = u16::MAX as usize;

/// Timestamp precision for data points.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TimestampPrecision {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

/// Storage provides thread-safe capabilities for insertion and retrieval from time-series storage.
pub trait Storage: Send + Sync {
    /// Inserts rows into the storage.
    fn insert_rows(&self, rows: &[Row]) -> Result<()>;

    /// Selects data points for a metric within the given time range.
    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>>;

    /// Selects with additional options like downsampling, aggregation, and pagination.
    fn select_with_options(&self, metric: &str, opts: QueryOptions) -> Result<Vec<DataPoint>>;

    /// Selects data points for a metric regardless of labels.
    /// Returns a map of label sets to their corresponding data points.
    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>>;

    /// Closes the storage gracefully.
    fn close(&self) -> Result<()>;
}

/// Builder for creating a Storage instance.
pub struct StorageBuilder {
    data_path: Option<PathBuf>,
    retention: Duration,
    timestamp_precision: TimestampPrecision,
    max_writers: usize,
    write_timeout: Duration,
    partition_duration: Duration,
    wal_enabled: bool,
    wal_buffer_size: usize,
    wal_sync_mode: WalSyncMode,
}

impl Default for StorageBuilder {
    fn default() -> Self {
        Self {
            data_path: None,
            retention: Duration::from_secs(14 * 24 * 3600), // 14 days
            timestamp_precision: TimestampPrecision::Nanoseconds,
            max_writers: crate::cgroup::default_workers_limit(),
            write_timeout: Duration::from_secs(30),
            partition_duration: Duration::from_secs(3600), // 1 hour
            wal_enabled: true,
            wal_buffer_size: 4096,
            wal_sync_mode: WalSyncMode::default(),
        }
    }
}

impl StorageBuilder {
    /// Creates a new StorageBuilder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    // Storage configuration methods

    /// Sets the data path for persistent storage.
    pub fn with_data_path(mut self, path: impl AsRef<Path>) -> Self {
        self.data_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the retention period.
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention = retention;
        self
    }

    /// Sets the timestamp precision.
    pub fn with_timestamp_precision(mut self, precision: TimestampPrecision) -> Self {
        self.timestamp_precision = precision;
        self
    }

    /// Sets the maximum number of concurrent writers.
    pub fn with_max_writers(mut self, max_writers: usize) -> Self {
        self.max_writers = if max_writers == 0 {
            crate::cgroup::default_workers_limit().max(1)
        } else {
            max_writers
        };
        self
    }

    /// Sets the write timeout.
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Sets the partition duration.
    pub fn with_partition_duration(mut self, duration: Duration) -> Self {
        self.partition_duration = duration;
        self
    }

    /// Enables or disables WAL.
    pub fn with_wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    /// Sets the WAL buffer size.
    pub fn with_wal_buffer_size(mut self, size: usize) -> Self {
        self.wal_buffer_size = size;
        self
    }

    /// Sets WAL fsync policy.
    pub fn with_wal_sync_mode(mut self, mode: WalSyncMode) -> Self {
        self.wal_sync_mode = mode;
        self
    }

    /// Builds the Storage instance.
    pub fn build(self) -> Result<Arc<dyn Storage>> {
        let storage = self.build_impl()?;
        Ok(storage)
    }

    fn build_impl(self) -> Result<Arc<StorageImpl>> {
        // Check file descriptor limits on Unix systems
        #[cfg(unix)]
        {
            let mut rlim = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            unsafe {
                if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
                    let required_fds = 100; // Minimum required file descriptors
                    if rlim.rlim_cur < required_fds {
                        tracing::warn!(
                            "Low file descriptor limit: {}. Consider increasing with 'ulimit -n'",
                            rlim.rlim_cur
                        );
                    }
                }
            }
        }

        let max_writers = if self.max_writers == 0 {
            crate::cgroup::default_workers_limit().max(1)
        } else {
            self.max_writers
        };

        let partition_units =
            crate::time::duration_to_units(self.partition_duration, self.timestamp_precision);
        if partition_units <= 0 {
            return Err(TsinkError::InvalidConfiguration(
                "partition_duration is too small for the configured timestamp_precision"
                    .to_string(),
            ));
        }

        if !self.retention.is_zero()
            && crate::time::duration_to_units(self.retention, self.timestamp_precision) <= 0
        {
            return Err(TsinkError::InvalidConfiguration(
                "retention is too small for the configured timestamp_precision".to_string(),
            ));
        }

        if let Some(ref data_path) = self.data_path {
            // Ensure persistent storage path exists even when WAL is disabled.
            fs::create_dir_all(data_path)?;
        }

        let use_disk_wal = self.data_path.is_some() && self.wal_enabled;

        // Create WAL based on configuration
        let wal: Arc<dyn Wal> = if let Some(ref data_path) = self.data_path {
            if use_disk_wal {
                DiskWal::new_with_sync_mode(
                    data_path.join("wal"),
                    self.wal_buffer_size,
                    self.wal_sync_mode,
                )?
            } else {
                Arc::new(NopWal)
            }
        } else {
            Arc::new(NopWal)
        };

        let storage = Arc::new(StorageImpl {
            partition_list: Arc::new(PartitionList::new()),
            data_path: self.data_path.clone(),
            partition_duration: self.partition_duration,
            retention: self.retention,
            timestamp_precision: self.timestamp_precision,
            write_timeout: self.write_timeout,
            wal: wal.clone(),
            workers_semaphore: Arc::new(Semaphore::new(max_writers)),
            lifecycle: Arc::new(AtomicU8::new(STORAGE_OPEN)),
            partition_creation_lock: Arc::new(parking_lot::Mutex::new(())),
            partition_ops_lock: Arc::new(parking_lot::RwLock::new(())),
            expiry_thread: Arc::new(parking_lot::Mutex::new(None)),
            expiry_stop_tx: Arc::new(parking_lot::Mutex::new(None)),
            flush_thread: Arc::new(parking_lot::Mutex::new(None)),
        });

        // Load existing partitions from disk if data path is set
        if let Some(ref data_path) = self.data_path {
            storage.load_disk_partitions(data_path)?;

            // Recover from WAL if it exists
            let wal_dir = data_path.join("wal");
            if use_disk_wal && wal_dir.exists() {
                storage.recover_from_wal(&wal_dir)?;
            }
        }

        // Create initial memory partition
        storage.new_partition(None)?;

        // Start background tasks
        storage.start_background_tasks();

        Ok(storage)
    }
}

/// Aggregation applied to query results or buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    None,
    Sum,
    Min,
    Max,
    Avg,
    Last,
}

/// Downsampling configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownsampleOptions {
    pub interval: i64,
}

/// Options to customize queries (aggregation, downsampling, pagination).
#[derive(Debug, Clone, PartialEq)]
pub struct QueryOptions {
    pub labels: Vec<Label>,
    pub start: i64,
    pub end: i64,
    pub aggregation: Aggregation,
    pub downsample: Option<DownsampleOptions>,
    pub limit: Option<usize>,
    pub offset: usize,
}

impl QueryOptions {
    /// Create query options for a time range.
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            labels: Vec::new(),
            start,
            end,
            aggregation: Aggregation::None,
            downsample: None,
            limit: None,
            offset: 0,
        }
    }

    /// Attach labels to filter the series.
    pub fn with_labels(mut self, labels: Vec<Label>) -> Self {
        self.labels = labels;
        self
    }

    /// Apply pagination.
    pub fn with_pagination(mut self, offset: usize, limit: Option<usize>) -> Self {
        self.offset = offset;
        self.limit = limit;
        self
    }

    /// Apply downsampling using the given interval and aggregation.
    pub fn with_downsample(mut self, interval: i64, aggregation: Aggregation) -> Self {
        self.downsample = Some(DownsampleOptions { interval });
        self.aggregation = aggregation;
        self
    }

    /// Apply aggregation without downsampling (reduces the whole series to one point).
    pub fn with_aggregation(mut self, aggregation: Aggregation) -> Self {
        self.aggregation = aggregation;
        self
    }
}

/// Main storage implementation.
struct StorageImpl {
    partition_list: Arc<PartitionList>,
    data_path: Option<PathBuf>,
    partition_duration: Duration,
    retention: Duration,
    timestamp_precision: TimestampPrecision,
    write_timeout: Duration,
    wal: Arc<dyn Wal>,
    workers_semaphore: Arc<Semaphore>,
    lifecycle: Arc<AtomicU8>,
    partition_creation_lock: Arc<parking_lot::Mutex<()>>,
    partition_ops_lock: Arc<parking_lot::RwLock<()>>,
    expiry_thread: Arc<parking_lot::Mutex<Option<thread::JoinHandle<()>>>>,
    expiry_stop_tx: Arc<parking_lot::Mutex<Option<Sender<()>>>>,
    flush_thread: Arc<parking_lot::Mutex<Option<thread::JoinHandle<()>>>>,
}

impl StorageImpl {
    fn ensure_operational(&self) -> Result<()> {
        match self.lifecycle.load(Ordering::SeqCst) {
            STORAGE_OPEN => Ok(()),
            STORAGE_CLOSING => Err(TsinkError::StorageShuttingDown),
            _ => Err(TsinkError::StorageClosed),
        }
    }

    fn validate_metric_name(metric: &str) -> Result<()> {
        if metric.is_empty() {
            return Err(TsinkError::MetricRequired);
        }
        if metric.len() > MAX_METRIC_NAME_LEN {
            return Err(TsinkError::InvalidMetricName(format!(
                "metric name too long: {} bytes (max {})",
                metric.len(),
                MAX_METRIC_NAME_LEN
            )));
        }
        Ok(())
    }

    fn validate_labels(labels: &[Label]) -> Result<()> {
        if labels.iter().all(Label::is_valid) {
            Ok(())
        } else {
            Err(TsinkError::InvalidLabel(
                "label name and value must be non-empty".to_string(),
            ))
        }
    }

    fn validate_rows(rows: &[Row]) -> Result<()> {
        for row in rows {
            Self::validate_metric_name(row.metric())?;
            Self::validate_labels(row.labels())?;
        }
        Ok(())
    }

    fn recover_from_wal(&self, wal_dir: &Path) -> Result<()> {
        let reader = WalReader::new(wal_dir)?;
        let rows = reader.read_all()?;

        if rows.is_empty() {
            return Ok(());
        }

        let mut skipped_invalid = 0usize;
        let mut valid_rows = Vec::with_capacity(rows.len());
        for row in rows {
            if Self::validate_metric_name(row.metric()).is_ok()
                && Self::validate_labels(row.labels()).is_ok()
            {
                valid_rows.push(row);
            } else {
                skipped_invalid += 1;
            }
        }

        if skipped_invalid > 0 {
            warn!(
                count = skipped_invalid,
                "Skipping WAL rows with invalid metric names or labels during recovery"
            );
        }

        if !valid_rows.is_empty() {
            info!("Recovering {} rows from WAL", valid_rows.len());
        }

        let replay_result = if valid_rows.is_empty() {
            Ok(())
        } else {
            // Insert recovered rows.
            let mut replay_result = Ok(());
            for chunk in valid_rows.chunks(1000) {
                if let Err(e) = self.insert_rows_recovery(chunk) {
                    replay_result = Err(e);
                    break;
                }
            }
            replay_result
        };

        // Always refresh the WAL after replay attempt to avoid duplicate replays on restart.
        let refresh_result = self.wal.refresh();
        match (replay_result, refresh_result) {
            (Err(replay_err), Err(refresh_err)) => {
                warn!(
                    replay_error = %replay_err,
                    refresh_error = %refresh_err,
                    "WAL replay failed and WAL refresh also failed"
                );
                Err(replay_err)
            }
            (Err(replay_err), Ok(())) => Err(replay_err),
            (Ok(()), Err(refresh_err)) => Err(refresh_err),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn insert_rows_internal(&self, rows: &[Row]) -> Result<()> {
        self.insert_rows_internal_with_mode(rows, false)
    }

    fn insert_rows_recovery(&self, rows: &[Row]) -> Result<()> {
        self.insert_rows_internal_with_mode(rows, true)
    }

    fn insert_rows_internal_with_mode(&self, rows: &[Row], recovery_mode: bool) -> Result<()> {
        Self::validate_rows(rows)?;
        self.ensure_active_head()?;
        let _partition_ops_guard = self.partition_ops_lock.read();

        let mut pending_rows = rows.to_vec();
        let mut extra_partitions = 0usize;
        // A partition can accept at least one row from a non-empty batch; cap growth by batch size.
        let max_extra_partitions = rows.len().saturating_add(WRITABLE_PARTITIONS_NUM);

        while !pending_rows.is_empty() {
            let mut remaining = pending_rows;

            for partition in self.partition_list.iter() {
                if remaining.is_empty() {
                    break;
                }

                if partition.expired() {
                    continue;
                }

                let insert_result = if recovery_mode {
                    partition.insert_rows_recovery(&remaining)
                } else {
                    partition.insert_rows(&remaining)
                };

                match insert_result {
                    Ok(outdated_rows) => remaining = outdated_rows,
                    Err(TsinkError::ReadOnlyPartition { .. }) => continue,
                    Err(e) => return Err(e),
                }
            }

            if remaining.is_empty() {
                return Ok(());
            }

            if extra_partitions >= max_extra_partitions {
                let ts = remaining
                    .iter()
                    .map(|row| row.data_point().timestamp)
                    .min()
                    .unwrap_or(0);
                return Err(TsinkError::OutOfRetention { timestamp: ts });
            }

            self.append_partition(None)?;
            if self.data_path.is_some() {
                self.schedule_flush_partitions();
            }
            extra_partitions += 1;
            pending_rows = remaining;
        }

        Ok(())
    }

    fn load_disk_partitions(&self, data_path: &Path) -> Result<()> {
        let entries = fs::read_dir(data_path)?;
        let mut partitions = Vec::new();

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir()
                && let Some(name) = path.file_name()
            {
                let name_str = name.to_string_lossy();
                if name_str.starts_with("p-") {
                    match DiskPartition::open(&path, self.retention) {
                        Ok(partition) => {
                            partitions.push(Arc::new(partition) as SharedPartition);
                        }
                        Err(TsinkError::NoDataPoints { .. }) => continue,
                        Err(TsinkError::InvalidPartition { .. }) => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        // Sort by min timestamp and insert into list
        partitions.sort_by_key(|p| p.min_timestamp());
        for partition in partitions {
            self.partition_list.insert(partition);
        }

        Ok(())
    }

    fn build_partition(&self, partition: Option<SharedPartition>) -> SharedPartition {
        if let Some(p) = partition {
            p
        } else {
            let mem_partition = Arc::new(MemoryPartition::new(
                self.wal.clone(),
                self.partition_duration,
                self.timestamp_precision,
                self.retention,
            ));

            mem_partition as SharedPartition
        }
    }

    fn new_partition(&self, partition: Option<SharedPartition>) -> Result<()> {
        let partition = self.build_partition(partition);
        // Rotate WAL first so a punctuate failure does not mutate partition state.
        self.wal.punctuate()?;
        self.partition_list.insert(partition);
        Ok(())
    }

    fn append_partition(&self, partition: Option<SharedPartition>) -> Result<()> {
        let partition = self.build_partition(partition);
        // Keep WAL segmentation consistent with partition lifecycle for recovery.
        self.wal.punctuate()?;
        self.partition_list.insert_tail(partition);
        Ok(())
    }

    fn active_partition_count(&self) -> usize {
        self.partition_list
            .iter()
            .filter(|p| p.active() && !p.expired())
            .count()
    }

    fn ensure_active_head(&self) -> Result<()> {
        if self.active_partition_count() >= WRITABLE_PARTITIONS_NUM {
            return Ok(());
        }

        let _guard = self.partition_creation_lock.lock();

        let mut created = 0usize;
        while self.active_partition_count() < WRITABLE_PARTITIONS_NUM {
            self.new_partition(None)?;
            created += 1;
        }

        if created > 0 {
            // Trigger flush in background to free older partitions
            if self.data_path.is_some() {
                self.schedule_flush_partitions();
            }
        }

        Ok(())
    }

    fn schedule_flush_partitions(&self) {
        let mut slot = self.flush_thread.lock();

        if let Some(handle) = slot.as_ref()
            && !handle.is_finished()
        {
            return;
        }

        if let Some(handle) = slot.take() {
            let _ = handle.join();
        }

        let storage = self.clone_refs();
        *slot = Some(thread::spawn(move || {
            if let Err(e) = storage.flush_partitions() {
                error!("Failed to flush partitions: {}", e);
            }
        }));
    }

    fn collect_flush_candidates(&self, skip_head: usize) -> Vec<SharedPartition> {
        let _partition_ops_guard = self.partition_ops_lock.read();
        let mut partitions_to_flush = Vec::new();
        let mut i = 0usize;

        for partition in self.partition_list.iter() {
            if i < skip_head {
                i += 1;
                continue;
            }

            if partition.size() == 0 {
                continue;
            }

            partitions_to_flush.push(partition);
        }

        partitions_to_flush
    }

    fn flush_partitions(&self) -> Result<()> {
        let partitions_to_flush = self.collect_flush_candidates(WRITABLE_PARTITIONS_NUM);

        for partition in partitions_to_flush {
            if let Some(data_path) = &self.data_path {
                if !partition.begin_flush() {
                    continue;
                }

                let prepared = match self.flush_memory_partition_to_disk(&partition, data_path) {
                    Ok(prepared) => prepared,
                    Err(e) => {
                        partition.end_flush();
                        error!("Failed to flush partition: {}", e);
                        continue;
                    }
                };

                let Some(flushed_partition) = prepared else {
                    partition.end_flush();
                    continue;
                };

                let swapped = {
                    let _partition_ops_guard = self.partition_ops_lock.write();
                    self.partition_list
                        .swap(&partition, flushed_partition.clone())
                };

                match swapped {
                    Ok(()) => {
                        // Remove oldest WAL segment only when a memory partition was persisted.
                        self.wal.remove_oldest()?;
                    }
                    Err(TsinkError::PartitionNotFound { .. }) => {
                        partition.end_flush();
                        let _ = flushed_partition.clean();
                    }
                    Err(e) => {
                        partition.end_flush();
                        let _ = flushed_partition.clean();
                        return Err(e);
                    }
                }
            } else {
                // In-memory mode - just remove old partitions.
                let remove_result = {
                    let _partition_ops_guard = self.partition_ops_lock.write();
                    self.partition_list.remove(&partition)
                };

                match remove_result {
                    Ok(()) | Err(TsinkError::PartitionNotFound { .. }) => {}
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(())
    }

    fn flush_all_partitions(&self) -> Result<()> {
        let partitions_to_flush = self.collect_flush_candidates(0);

        for partition in partitions_to_flush {
            if let Some(data_path) = &self.data_path {
                if !partition.begin_flush() {
                    continue;
                }

                let prepared = match self.flush_memory_partition_to_disk(&partition, data_path)? {
                    Some(prepared) => prepared,
                    None => {
                        partition.end_flush();
                        continue;
                    }
                };

                let swap_result = {
                    let _partition_ops_guard = self.partition_ops_lock.write();
                    self.partition_list.swap(&partition, prepared.clone())
                };

                if let Err(e) = swap_result {
                    partition.end_flush();
                    let _ = prepared.clean();
                    return Err(e);
                }
            } else {
                let remove_result = {
                    let _partition_ops_guard = self.partition_ops_lock.write();
                    self.partition_list.remove(&partition)
                };
                if let Err(e) = remove_result {
                    if matches!(e, TsinkError::PartitionNotFound { .. }) {
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    fn flush_memory_partition_to_disk(
        &self,
        partition: &SharedPartition,
        data_path: &Path,
    ) -> Result<Option<SharedPartition>> {
        if partition.size() == 0 {
            return Ok(None);
        }

        let dir_path = self.next_partition_dir(partition, data_path)?;

        match partition.flush_to_disk()? {
            Some((data, meta)) => {
                let disk_partition =
                    crate::disk::DiskPartition::create(&dir_path, meta, data, self.retention)?;
                Ok(Some(Arc::new(disk_partition) as SharedPartition))
            }
            None => {
                // Already on disk, nothing to do.
                Ok(None)
            }
        }
    }

    fn next_partition_dir(&self, partition: &SharedPartition, data_path: &Path) -> Result<PathBuf> {
        let base = format!(
            "p-{}-{}",
            partition.min_timestamp(),
            partition.max_timestamp()
        );
        let mut candidate = data_path.join(&base);
        if !candidate.exists() {
            return Ok(candidate);
        }

        for suffix in 1u64.. {
            candidate = data_path.join(format!("{base}-{suffix}"));
            if !candidate.exists() {
                return Ok(candidate);
            }
        }

        Err(TsinkError::Other(
            "unable to allocate unique partition directory".to_string(),
        ))
    }

    fn remove_expired_partitions(&self) -> Result<()> {
        let _partition_ops_guard = self.partition_ops_lock.write();
        let mut expired = Vec::new();

        for partition in self.partition_list.iter() {
            if partition.expired() {
                expired.push(partition);
            }
        }

        for partition in expired {
            self.partition_list.remove(&partition)?;
        }

        Ok(())
    }

    #[cfg(test)]
    fn expiry_check_interval(&self) -> Duration {
        Duration::from_millis(50)
    }

    #[cfg(not(test))]
    fn expiry_check_interval(&self) -> Duration {
        if self.retention.is_zero() {
            return Duration::from_secs(3600);
        }

        let interval = self.retention / 10;
        interval.clamp(Duration::from_secs(1), Duration::from_secs(3600))
    }

    fn start_background_tasks(&self) {
        let (stop_tx, stop_rx) = bounded::<()>(1);
        *self.expiry_stop_tx.lock() = Some(stop_tx);

        let storage = self.clone_refs();
        let interval = storage.expiry_check_interval();
        let handle = thread::spawn(move || {
            loop {
                match stop_rx.recv_timeout(interval) {
                    Ok(_) => break,
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        if storage.lifecycle.load(Ordering::SeqCst) != STORAGE_OPEN {
                            break;
                        }

                        if let Err(e) = storage.remove_expired_partitions() {
                            error!("Failed to remove expired partitions: {}", e);
                        }
                    }
                }
            }
        });
        *self.expiry_thread.lock() = Some(handle);
    }

    fn stop_background_tasks(&self) {
        if let Some(tx) = self.expiry_stop_tx.lock().take() {
            let _ = tx.try_send(());
        }

        if let Some(handle) = self.expiry_thread.lock().take() {
            let _ = handle.join();
        }
    }

    fn stop_flush_worker(&self) {
        if let Some(handle) = self.flush_thread.lock().take() {
            let _ = handle.join();
        }
    }

    fn clone_refs(&self) -> Arc<StorageImpl> {
        Arc::new(StorageImpl {
            partition_list: self.partition_list.clone(),
            data_path: self.data_path.clone(),
            partition_duration: self.partition_duration,
            retention: self.retention,
            timestamp_precision: self.timestamp_precision,
            write_timeout: self.write_timeout,
            wal: self.wal.clone(),
            workers_semaphore: self.workers_semaphore.clone(),
            lifecycle: self.lifecycle.clone(),
            partition_creation_lock: self.partition_creation_lock.clone(),
            partition_ops_lock: self.partition_ops_lock.clone(),
            expiry_thread: self.expiry_thread.clone(),
            expiry_stop_tx: self.expiry_stop_tx.clone(),
            flush_thread: self.flush_thread.clone(),
        })
    }
}

impl Storage for StorageImpl {
    fn insert_rows(&self, rows: &[Row]) -> Result<()> {
        self.ensure_operational()?;

        // Try to acquire worker slot with timeout
        let _guard = match self.workers_semaphore.try_acquire_for(self.write_timeout) {
            Ok(guard) => guard,
            Err(TsinkError::WriteTimeout { .. }) => {
                return match self.lifecycle.load(Ordering::SeqCst) {
                    STORAGE_CLOSING => Err(TsinkError::StorageShuttingDown),
                    STORAGE_CLOSED => Err(TsinkError::StorageClosed),
                    _ => Err(TsinkError::WriteTimeout {
                        timeout_ms: self.write_timeout.as_millis() as u64,
                        workers: self.workers_semaphore.capacity(),
                    }),
                };
            }
            Err(err) => return Err(err),
        };

        self.ensure_operational()?;
        self.insert_rows_internal(rows)
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        self.ensure_operational()?;
        Self::validate_metric_name(metric)?;
        Self::validate_labels(labels)?;

        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }

        let _partition_ops_guard = self.partition_ops_lock.read();
        let mut all_points = Vec::new();

        for partition in self.partition_list.iter() {
            if partition.size() == 0 || partition.expired() {
                continue;
            }

            match partition.select_data_points(metric, labels, start, end) {
                Ok(points) => all_points.extend(points),
                Err(TsinkError::NoDataPoints { .. }) => continue,
                Err(e) => return Err(e),
            }
        }

        all_points.sort_by_key(|p| p.timestamp);

        Ok(all_points)
    }

    fn select_with_options(&self, metric: &str, opts: QueryOptions) -> Result<Vec<DataPoint>> {
        self.ensure_operational()?;
        Self::validate_metric_name(metric)?;
        Self::validate_labels(&opts.labels)?;

        if opts.start >= opts.end {
            return Err(TsinkError::InvalidTimeRange {
                start: opts.start,
                end: opts.end,
            });
        }

        if let Some(d) = opts.downsample
            && d.interval <= 0
        {
            return Err(TsinkError::InvalidConfiguration(
                "downsample interval must be positive".to_string(),
            ));
        }

        // Reuse the existing select for raw points.
        let points = self.select(metric, &opts.labels, opts.start, opts.end)?;

        // Downsample or aggregate if requested.
        let aggregation = match (opts.downsample.is_some(), opts.aggregation) {
            (true, Aggregation::None) => Aggregation::Last, // sensible default for downsampling
            _ => opts.aggregation,
        };

        let mut processed = if let Some(downsample) = opts.downsample {
            downsample_points(
                &points,
                downsample.interval,
                aggregation,
                opts.start,
                opts.end,
            )
        } else if aggregation != Aggregation::None {
            aggregate_series(&points, aggregation)
                .into_iter()
                .collect::<Vec<DataPoint>>()
        } else {
            points
        };

        // Apply pagination.
        if opts.offset > 0 && opts.offset < processed.len() {
            processed.drain(0..opts.offset);
        } else if opts.offset >= processed.len() {
            processed.clear();
        }

        if let Some(limit) = opts.limit {
            processed.truncate(limit);
        }

        Ok(processed)
    }

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.ensure_operational()?;
        Self::validate_metric_name(metric)?;

        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }

        let _partition_ops_guard = self.partition_ops_lock.read();
        let mut results_map: std::collections::HashMap<Vec<Label>, Vec<DataPoint>> =
            std::collections::HashMap::new();

        for partition in self.partition_list.iter() {
            // Skip only if partition is truly empty (size == 0)
            if partition.size() == 0 {
                continue;
            }

            if partition.expired() {
                continue;
            }

            // Perform selection
            match partition.select_all_labels(metric, start, end) {
                Ok(partition_results) => {
                    for (labels, points) in partition_results {
                        results_map.entry(labels).or_default().extend(points);
                    }
                }
                Err(TsinkError::NoDataPoints { .. }) => continue,
                Err(e) => return Err(e),
            }
        }

        // Sort points within each label set
        let mut results: Vec<(Vec<Label>, Vec<DataPoint>)> = results_map
            .into_iter()
            .map(|(labels, mut points)| {
                points.sort_by_key(|p| p.timestamp);
                (labels, points)
            })
            .collect();

        // Sort by label sets for consistent output
        results.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(results)
    }

    fn close(&self) -> Result<()> {
        let state = self.lifecycle.compare_exchange(
            STORAGE_OPEN,
            STORAGE_CLOSING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        if state.is_err() {
            return match self.lifecycle.load(Ordering::SeqCst) {
                STORAGE_CLOSING => Err(TsinkError::StorageShuttingDown),
                _ => Err(TsinkError::StorageClosed),
            };
        }

        // Stop background workers first to avoid resource retention while closing.
        self.stop_background_tasks();

        let close_result = (|| -> Result<()> {
            // Block until all in-flight writers have completed.
            let _writer_guards = self.workers_semaphore.acquire_all(self.write_timeout)?;

            // Ensure no flush worker is racing with close-time flush/removal.
            self.stop_flush_worker();

            // Flush WAL
            self.wal.flush()?;

            // For in-memory mode, just return
            if self.data_path.is_none() {
                return Ok(());
            }

            // Create final partitions to make all writable ones read-only
            for _ in 0..WRITABLE_PARTITIONS_NUM {
                self.new_partition(None)?;
            }

            // Flush all partitions
            self.flush_all_partitions()?;

            // Remove expired partitions
            self.remove_expired_partitions()?;

            // Remove WAL
            self.wal.remove_all()?;

            Ok(())
        })();

        match close_result {
            Ok(()) => {
                self.lifecycle.store(STORAGE_CLOSED, Ordering::SeqCst);
                Ok(())
            }
            Err(err) => {
                self.lifecycle.store(STORAGE_OPEN, Ordering::SeqCst);
                self.start_background_tasks();
                Err(err)
            }
        }
    }
}

fn min_non_nan_point(points: &[DataPoint]) -> Option<DataPoint> {
    let mut best: Option<DataPoint> = None;
    for point in points.iter().copied() {
        if point.value.is_nan() {
            continue;
        }
        match best {
            Some(current) if point.value.total_cmp(&current.value).is_lt() => best = Some(point),
            None => best = Some(point),
            _ => {}
        }
    }
    best
}

fn max_non_nan_point(points: &[DataPoint]) -> Option<DataPoint> {
    let mut best: Option<DataPoint> = None;
    for point in points.iter().copied() {
        if point.value.is_nan() {
            continue;
        }
        match best {
            Some(current) if point.value.total_cmp(&current.value).is_gt() => best = Some(point),
            None => best = Some(point),
            _ => {}
        }
    }
    best
}

fn aggregate_series(points: &[DataPoint], aggregation: Aggregation) -> Option<DataPoint> {
    if points.is_empty() {
        return None;
    }

    match aggregation {
        Aggregation::None => None,
        Aggregation::Last => points.last().copied(),
        Aggregation::Sum | Aggregation::Avg => {
            let mut sum = 0.0;
            for p in points {
                sum += p.value;
            }
            let value = if aggregation == Aggregation::Avg {
                sum / points.len() as f64
            } else {
                sum
            };

            Some(DataPoint::new(points.last().unwrap().timestamp, value))
        }
        Aggregation::Min => min_non_nan_point(points).or_else(|| points.last().copied()),
        Aggregation::Max => max_non_nan_point(points).or_else(|| points.last().copied()),
    }
}

fn aggregate_bucket(
    points: &[DataPoint],
    aggregation: Aggregation,
    bucket_start: i64,
) -> Option<DataPoint> {
    if points.is_empty() {
        return None;
    }

    match aggregation {
        Aggregation::None => None,
        Aggregation::Last => points.last().copied(),
        Aggregation::Sum | Aggregation::Avg => {
            let mut sum = 0.0;
            for p in points {
                sum += p.value;
            }
            let value = if aggregation == Aggregation::Avg {
                sum / points.len() as f64
            } else {
                sum
            };
            Some(DataPoint::new(bucket_start, value))
        }
        Aggregation::Min => min_non_nan_point(points)
            .map(|p| DataPoint::new(bucket_start, p.value))
            .or_else(|| points.last().map(|p| DataPoint::new(bucket_start, p.value))),
        Aggregation::Max => max_non_nan_point(points)
            .map(|p| DataPoint::new(bucket_start, p.value))
            .or_else(|| points.last().map(|p| DataPoint::new(bucket_start, p.value))),
    }
}

fn downsample_points(
    points: &[DataPoint],
    interval: i64,
    aggregation: Aggregation,
    start: i64,
    end: i64,
) -> Vec<DataPoint> {
    if points.is_empty() {
        return Vec::new();
    }
    let mut result = Vec::new();
    let mut idx = 0;
    let mut bucket_start = start;

    while bucket_start < end {
        let bucket_end = bucket_start.saturating_add(interval);
        let mut bucket_points = Vec::new();

        while idx < points.len() && points[idx].timestamp < bucket_end {
            bucket_points.push(points[idx]);
            idx += 1;
        }

        if let Some(dp) = aggregate_bucket(&bucket_points, aggregation, bucket_start) {
            result.push(dp);
        }

        // Fast-forward if points are sparse
        if idx < points.len() && points[idx].timestamp >= bucket_end {
            let rel = points[idx].timestamp as i128 - start as i128;
            let next_bucket = start as i128 + rel.div_euclid(interval as i128) * interval as i128;
            let next_bucket = next_bucket.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
            bucket_start = std::cmp::max(bucket_end, next_bucket);
        } else {
            bucket_start = bucket_end;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn memory_mode_removes_expired_partitions_in_background() {
        let storage = StorageBuilder::new()
            .with_retention(Duration::from_millis(50))
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_partition_duration(Duration::from_millis(10))
            .build_impl()
            .unwrap();

        let row = Row::new("metric", DataPoint::new(1, 1.0));
        storage.insert_rows(&[row]).unwrap();

        let partition_with_data = storage
            .partition_list
            .iter()
            .find(|partition| partition.size() > 0)
            .expect("expected partition with data");
        let weak_partition = Arc::downgrade(&partition_with_data);
        drop(partition_with_data);

        let deadline = Instant::now() + Duration::from_millis(500);
        while weak_partition.upgrade().is_some() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }

        assert!(
            weak_partition.upgrade().is_none(),
            "expired memory partition should be dropped"
        );

        storage.close().unwrap();
    }

    #[test]
    fn build_rejects_partition_duration_too_small_for_precision() {
        let result = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_partition_duration(Duration::from_millis(1))
            .build_impl();

        assert!(matches!(result, Err(TsinkError::InvalidConfiguration(_))));
    }

    #[test]
    fn build_rejects_positive_retention_too_small_for_precision() {
        let result = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_retention(Duration::from_millis(1))
            .build_impl();

        assert!(matches!(result, Err(TsinkError::InvalidConfiguration(_))));
    }

    #[test]
    fn close_failure_keeps_storage_operational_for_retry() {
        let storage = StorageBuilder::new()
            .with_write_timeout(Duration::from_millis(1))
            .build_impl()
            .unwrap();

        let permit = storage
            .workers_semaphore
            .try_acquire_for(Duration::from_secs(1))
            .unwrap();

        let err = storage.close().unwrap_err();
        assert!(matches!(err, TsinkError::WriteTimeout { .. }));
        drop(permit);

        storage
            .insert_rows(&[Row::new("close_retry", DataPoint::new(1, 1.0))])
            .unwrap();
        storage.close().unwrap();
    }

    #[test]
    fn insert_batch_spanning_more_than_two_windows_is_accepted() {
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_partition_duration(Duration::from_secs(60))
            .build_impl()
            .unwrap();

        let rows = vec![
            Row::new("wide_span", DataPoint::new(10_000, 1.0)),
            Row::new("wide_span", DataPoint::new(9_900, 2.0)),
            Row::new("wide_span", DataPoint::new(9_700, 3.0)),
        ];

        storage.insert_rows(&rows).unwrap();
        let points = storage.select("wide_span", &[], 0, i64::MAX).unwrap();
        assert_eq!(points.len(), 3);

        storage.close().unwrap();
    }

    #[test]
    fn backfilled_rows_are_appended_to_older_partitions() {
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_partition_duration(Duration::from_secs(60))
            .build_impl()
            .unwrap();

        let rows = vec![
            Row::new("ordering", DataPoint::new(1_000, 1.0)),
            Row::new("ordering", DataPoint::new(100, 2.0)),
            Row::new("ordering", DataPoint::new(-900, 3.0)),
        ];

        storage.insert_rows(&rows).unwrap();

        let mins: Vec<i64> = storage
            .partition_list
            .iter()
            .filter(|partition| partition.size() > 0)
            .map(|partition| partition.min_timestamp())
            .collect();

        assert!(
            mins.len() >= 3,
            "expected three non-empty partitions, got {mins:?}"
        );
        assert!(
            mins.windows(2).all(|pair| pair[0] >= pair[1]),
            "partitions must stay ordered newest->oldest, got {mins:?}"
        );

        storage.close().unwrap();
    }

    #[test]
    fn insert_waiting_on_permit_returns_shutdown_error_when_closing_begins() {
        let storage = StorageBuilder::new()
            .with_max_writers(1)
            .with_write_timeout(Duration::from_millis(25))
            .build_impl()
            .unwrap();

        let permit = storage
            .workers_semaphore
            .try_acquire_for(Duration::from_secs(1))
            .unwrap();

        let storage_for_insert = storage.clone_refs();
        let handle = std::thread::spawn(move || {
            storage_for_insert.insert_rows(&[Row::new("shutdown", DataPoint::new(1, 1.0))])
        });

        std::thread::sleep(Duration::from_millis(5));
        storage.lifecycle.store(STORAGE_CLOSING, Ordering::SeqCst);

        let result = handle.join().unwrap();
        assert!(matches!(result, Err(TsinkError::StorageShuttingDown)));

        storage.lifecycle.store(STORAGE_OPEN, Ordering::SeqCst);
        drop(permit);
        storage.close().unwrap();
    }
}
