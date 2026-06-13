//! Main storage implementation for tsink.

use crate::disk::DiskPartition;
use crate::list::PartitionList;
use crate::memory::MemoryPartition;
use crate::partition::SharedPartition;
use crate::wal::{DiskWal, NopWal, Wal, WalReader};
use crate::{DataPoint, Label, Result, Row, TsinkError};
use crossbeam_channel::{Sender, bounded};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use tracing::{error, info};

const WRITABLE_PARTITIONS_NUM: usize = 2;
const CHECK_EXPIRED_INTERVAL: Duration = Duration::from_secs(3600);

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
        self.max_writers = max_writers;
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
    pub fn with_wal_buffer_size(mut self, size: i32) -> Self {
        self.wal_buffer_size = size.max(0) as usize;
        self
    }

    /// Builds the Storage instance.
    pub fn build(self) -> Result<Arc<dyn Storage>> {
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

        // Create WAL based on configuration
        let wal: Arc<dyn Wal> = if let Some(ref data_path) = self.data_path {
            if self.wal_enabled && self.wal_buffer_size > 0 {
                let wal_dir = data_path.join("wal");
                DiskWal::new(wal_dir, self.wal_buffer_size)?
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
            workers_semaphore: Arc::new(Semaphore::new(self.max_writers)),
            closing: Arc::new(AtomicBool::new(false)),
            memory_partitions: Arc::new(parking_lot::Mutex::new(Vec::new())),
            partition_creation_lock: Arc::new(parking_lot::Mutex::new(())),
        });

        // Load existing partitions from disk if data path is set
        if let Some(ref data_path) = self.data_path {
            storage.load_disk_partitions(data_path)?;

            // Recover from WAL if it exists
            let wal_dir = data_path.join("wal");
            if wal_dir.exists() {
                storage.recover_from_wal(&wal_dir)?;
            }
        }

        // Create initial memory partition
        storage.new_partition(None)?;

        // Start background tasks
        if self.data_path.is_some() {
            storage.start_background_tasks();
        }

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
    closing: Arc<AtomicBool>,
    memory_partitions: Arc<parking_lot::Mutex<Vec<Arc<MemoryPartition>>>>,
    partition_creation_lock: Arc<parking_lot::Mutex<()>>,
}

impl StorageImpl {
    fn recover_from_wal(&self, wal_dir: &Path) -> Result<()> {
        let reader = WalReader::new(wal_dir)?;
        let rows = reader.read_all()?;

        if !rows.is_empty() {
            info!("Recovering {} rows from WAL", rows.len());

            // Insert recovered rows
            for chunk in rows.chunks(1000) {
                self.insert_rows_internal(chunk)?;
            }

            // Refresh WAL after recovery
            self.wal.refresh()?;
        }

        Ok(())
    }

    fn insert_rows_internal(&self, rows: &[Row]) -> Result<()> {
        self.ensure_active_head()?;

        let mut rows_to_insert = rows.to_vec();

        for (idx, partition) in self.partition_list.iter().enumerate() {
            if idx >= WRITABLE_PARTITIONS_NUM || rows_to_insert.is_empty() {
                break;
            }

            let outdated = partition.insert_rows(&rows_to_insert)?;
            rows_to_insert = outdated;
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

    fn new_partition(&self, partition: Option<SharedPartition>) -> Result<()> {
        let partition = if let Some(p) = partition {
            p
        } else {
            let mem_partition = Arc::new(MemoryPartition::new(
                self.wal.clone(),
                self.partition_duration,
                self.timestamp_precision,
            ));

            // Store reference to memory partition
            self.memory_partitions.lock().push(mem_partition.clone());

            mem_partition as SharedPartition
        };

        self.partition_list.insert(partition);
        self.wal.punctuate()?;
        Ok(())
    }

    fn ensure_active_head(&self) -> Result<()> {
        if let Some(head) = self.partition_list.get_head()
            && head.active()
        {
            return Ok(());
        }

        let _guard = self.partition_creation_lock.lock();

        if let Some(head) = self.partition_list.get_head()
            && head.active()
        {
            return Ok(());
        }

        // Need to create a new partition
        self.new_partition(None)?;

        // Trigger flush in background
        let storage = self.clone_refs();
        thread::spawn(move || {
            if let Err(e) = storage.flush_partitions() {
                error!("Failed to flush partitions: {}", e);
            }
        });

        Ok(())
    }

    fn flush_partitions(&self) -> Result<()> {
        let mut partitions_to_flush = Vec::new();
        let mut i = 0;

        for partition in self.partition_list.iter() {
            if i < WRITABLE_PARTITIONS_NUM {
                i += 1;
                continue;
            }

            // Check if it's a memory partition that needs flushing
            if partition.min_timestamp() == 0 {
                continue;
            }

            partitions_to_flush.push(partition);
        }

        for partition in partitions_to_flush {
            if let Some(data_path) = &self.data_path {
                // Try to downcast to MemoryPartition to access its data
                let result = self.flush_memory_partition_to_disk(&partition, data_path);

                if let Err(e) = result {
                    error!("Failed to flush partition: {}", e);
                    continue;
                }

                // Remove oldest WAL segment
                self.wal.remove_oldest()?;
            } else {
                // In-memory mode - just remove old partitions
                self.partition_list.remove(&partition)?;
            }
        }

        Ok(())
    }

    fn flush_memory_partition_to_disk(
        &self,
        partition: &SharedPartition,
        data_path: &Path,
    ) -> Result<()> {
        // Check if partition has data
        if partition.min_timestamp() == 0 || partition.size() == 0 {
            return Ok(());
        }

        // Find the memory partition
        let memory_partitions = self.memory_partitions.lock();
        let mem_partition = memory_partitions.iter().find(|p| {
            let p_shared: SharedPartition = (*p).clone();
            Arc::ptr_eq(&p_shared, partition)
        });

        if let Some(mem_partition) = mem_partition {
            let dir_name = format!(
                "p-{}-{}",
                partition.min_timestamp(),
                partition.max_timestamp()
            );
            let dir_path = data_path.join(dir_name);

            // Use the memory partition's flush_to_disk method
            let disk_partition = mem_partition.flush_to_disk(&dir_path, self.retention)?;

            // Swap in partition list
            self.partition_list
                .swap(partition, Arc::new(disk_partition) as SharedPartition)?;
        }

        Ok(())
    }

    fn remove_expired_partitions(&self) -> Result<()> {
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

    fn start_background_tasks(&self) {
        let storage = self.clone_refs();

        thread::spawn(move || {
            loop {
                thread::sleep(CHECK_EXPIRED_INTERVAL);

                if storage.closing.load(Ordering::SeqCst) {
                    break;
                }

                if let Err(e) = storage.remove_expired_partitions() {
                    error!("Failed to remove expired partitions: {}", e);
                }
            }
        });
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
            closing: self.closing.clone(),
            memory_partitions: self.memory_partitions.clone(),
            partition_creation_lock: self.partition_creation_lock.clone(),
        })
    }
}

impl Storage for StorageImpl {
    fn insert_rows(&self, rows: &[Row]) -> Result<()> {
        let insert = || -> Result<()> {
            self.ensure_active_head()?;

            let mut rows_to_insert = rows.to_vec();

            for (idx, partition) in self.partition_list.iter().enumerate() {
                if idx >= WRITABLE_PARTITIONS_NUM || rows_to_insert.is_empty() {
                    break;
                }

                let outdated = partition.insert_rows(&rows_to_insert)?;
                rows_to_insert = outdated;
            }

            Ok(())
        };

        // Try to acquire worker slot with timeout
        if let Ok(_guard) = self.workers_semaphore.try_acquire(self.write_timeout) {
            insert()
        } else {
            Err(TsinkError::WriteTimeout {
                timeout_ms: self.write_timeout.as_millis() as u64,
                workers: self.workers_semaphore.capacity(),
            })
        }
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        if metric.is_empty() {
            return Err(TsinkError::MetricRequired);
        }

        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }

        let mut all_points = Vec::new();

        for partition in self.partition_list.iter() {
            // Skip only if partition is truly empty (size == 0)
            if partition.size() == 0 {
                continue; // Skip empty partition
            }

            if partition.max_timestamp() < start {
                break; // No need to continue
            }

            if partition.min_timestamp() > end {
                continue;
            }

            match partition.select_data_points(metric, labels, start, end) {
                Ok(points) => {
                    // Prepend to maintain order (newest partition first)
                    let mut combined = points;
                    combined.append(&mut all_points);
                    all_points = combined;
                }
                Err(TsinkError::NoDataPoints { .. }) => continue,
                Err(e) => return Err(e),
            }
        }

        Ok(all_points)
    }

    fn select_with_options(&self, metric: &str, opts: QueryOptions) -> Result<Vec<DataPoint>> {
        if metric.is_empty() {
            return Err(TsinkError::MetricRequired);
        }

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
        if metric.is_empty() {
            return Err(TsinkError::MetricRequired);
        }

        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }

        let mut results_map: std::collections::HashMap<Vec<Label>, Vec<DataPoint>> =
            std::collections::HashMap::new();

        for partition in self.partition_list.iter() {
            // Skip only if partition is truly empty (size == 0)
            if partition.size() == 0 {
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
        self.closing.store(true, Ordering::SeqCst);

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
        self.flush_partitions()?;

        // Remove expired partitions
        self.remove_expired_partitions()?;

        // Remove WAL
        self.wal.remove_all()?;

        Ok(())
    }
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
        Aggregation::Min => points
            .iter()
            .min_by(|a, b| {
                a.value
                    .partial_cmp(&b.value)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .copied(),
        Aggregation::Max => points
            .iter()
            .max_by(|a, b| {
                a.value
                    .partial_cmp(&b.value)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .copied(),
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
        Aggregation::Min => points
            .iter()
            .min_by(|a, b| {
                a.value
                    .partial_cmp(&b.value)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|p| DataPoint::new(bucket_start, p.value)),
        Aggregation::Max => points
            .iter()
            .max_by(|a, b| {
                a.value
                    .partial_cmp(&b.value)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|p| DataPoint::new(bucket_start, p.value)),
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
            let next_bucket = points[idx].timestamp / interval * interval;
            bucket_start = std::cmp::max(bucket_end, next_bucket);
        } else {
            bucket_start = bucket_end;
        }
    }

    result
}

/// Simple semaphore implementation for limiting concurrent workers.
struct Semaphore {
    permits: crossbeam_channel::Receiver<()>,
    returns: Sender<()>,
    capacity: usize,
}

impl Semaphore {
    fn new(capacity: usize) -> Self {
        let (tx, rx) = bounded(capacity);

        // Fill with initial permits
        for _ in 0..capacity {
            // This should never fail since we just created the channel with this capacity
            let _ = tx.send(());
        }

        Self {
            permits: rx,
            returns: tx,
            capacity,
        }
    }

    fn try_acquire(&self, timeout: Duration) -> Result<SemaphoreGuard> {
        match self.permits.recv_timeout(timeout) {
            Ok(()) => Ok(SemaphoreGuard {
                returns: self.returns.clone(),
            }),
            Err(_) => Err(TsinkError::WriteTimeout {
                timeout_ms: timeout.as_millis() as u64,
                workers: self.capacity,
            }),
        }
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

struct SemaphoreGuard {
    returns: Sender<()>,
}

impl Drop for SemaphoreGuard {
    fn drop(&mut self) {
        let _ = self.returns.send(());
    }
}
