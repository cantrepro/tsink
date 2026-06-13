//! Memory partition implementation.

use crate::disk::{DiskMetric, DiskPartition, PartitionMeta, encode_metric_key};
use crate::encoding::GorillaEncoder;
use crate::label::{marshal_metric_name, unmarshal_metric_name};
use crate::partition::{Partition, SharedPartition};
use crate::wal::Wal;
use crate::{DataPoint, Label, Result, Row, TimestampPrecision, TsinkError};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs;
use std::io::{Seek, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

/// A memory partition stores data points in heap memory.
pub struct MemoryPartition {
    /// Number of data points
    num_points: AtomicUsize,
    /// Minimum timestamp (immutable after first set)
    min_t: AtomicI64,
    /// Maximum timestamp
    max_t: AtomicI64,
    /// Metrics storage - using DashMap for concurrent access
    metrics: DashMap<Vec<u8>, Arc<MemoryMetric>>,
    /// Write-ahead log
    wal: Arc<dyn Wal>,
    /// Partition duration in the appropriate time unit
    partition_duration: i64,
    /// Timestamp precision
    #[allow(dead_code)]
    timestamp_precision: TimestampPrecision,
    /// Flag to ensure min_t is set only once
    min_t_set: AtomicUsize,
}

impl MemoryPartition {
    /// Creates a new memory partition.
    pub fn new(
        wal: Arc<dyn Wal>,
        partition_duration: Duration,
        timestamp_precision: TimestampPrecision,
    ) -> Self {
        let duration = match timestamp_precision {
            TimestampPrecision::Nanoseconds => partition_duration.as_nanos() as i64,
            TimestampPrecision::Microseconds => partition_duration.as_micros() as i64,
            TimestampPrecision::Milliseconds => partition_duration.as_millis() as i64,
            TimestampPrecision::Seconds => partition_duration.as_secs() as i64,
        };

        Self {
            num_points: AtomicUsize::new(0),
            min_t: AtomicI64::new(0),
            max_t: AtomicI64::new(0),
            metrics: DashMap::new(),
            wal,
            partition_duration: duration,
            timestamp_precision,
            min_t_set: AtomicUsize::new(0),
        }
    }

    /// Gets or creates a metric.
    fn get_or_create_metric(&self, name: Vec<u8>) -> Arc<MemoryMetric> {
        self.metrics
            .entry(name.clone())
            .or_insert_with(|| Arc::new(MemoryMetric::new(name)))
            .clone()
    }

    fn update_min_timestamp(&self, timestamp: i64) {
        loop {
            let current = self.min_t.load(Ordering::Acquire);

            if current != 0 && current <= timestamp {
                break;
            }

            let desired = if current == 0 {
                timestamp
            } else {
                timestamp.min(current)
            };

            if self
                .min_t
                .compare_exchange(current, desired, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.min_t_set.store(1, Ordering::Release);
                break;
            }
        }
    }

    /// Encodes all points in the partition to a writer and returns metadata.
    pub fn flush_to_disk(
        &self,
        dir_path: impl AsRef<Path>,
        retention: Duration,
    ) -> Result<DiskPartition> {
        let dir_path = dir_path.as_ref();

        // Create directory
        fs::create_dir_all(dir_path)?;

        // Create data file
        let data_path = dir_path.join(crate::disk::DATA_FILE_NAME);
        let mut data_file = fs::File::create(&data_path)?;

        let mut metrics_map = HashMap::new();

        for entry in self.metrics.iter() {
            let (name, metric) = (entry.key(), entry.value());

            // Get current position in file
            let offset = data_file.stream_position()?;

            // Encode metric data
            let mut encoder = GorillaEncoder::new(&mut data_file);
            metric.encode_all_points(&mut encoder)?;
            encoder.flush()?;

            // Track encoded byte size for accurate slicing
            let end = data_file.stream_position()?;
            let encoded_size = end.saturating_sub(offset);

            // Add to metadata with lossless key encoding
            let encoded_key = encode_metric_key(name);
            metrics_map.insert(
                encoded_key.clone(),
                DiskMetric {
                    name: encoded_key,
                    offset,
                    encoded_size,
                    min_timestamp: metric.min_timestamp(),
                    max_timestamp: metric.max_timestamp(),
                    num_data_points: metric.size(),
                },
            );
        }

        // Create metadata
        let meta = PartitionMeta {
            min_timestamp: Partition::min_timestamp(self),
            max_timestamp: Partition::max_timestamp(self),
            num_data_points: Partition::size(self),
            metrics: metrics_map,
            created_at: SystemTime::now(),
        };

        // Write metadata
        let meta_path = dir_path.join(crate::disk::META_FILE_NAME);
        let meta_file = fs::File::create(&meta_path)?;
        serde_json::to_writer_pretty(meta_file, &meta)?;

        // Open the created partition
        DiskPartition::open(dir_path, retention)
    }
}

impl crate::partition::Partition for MemoryPartition {
    fn insert_rows(&self, rows: &[Row]) -> Result<Vec<Row>> {
        if rows.is_empty() {
            return Err(TsinkError::Other("No rows given".to_string()));
        }

        // Normalize timestamps before touching WAL so recovery matches live writes
        let mut normalized_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let timestamp = if row.data_point().timestamp == 0 {
                let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                tracing::warn!(
                    "Replacing zero timestamp with current time {} for metric {}",
                    now,
                    row.metric()
                );
                now
            } else {
                row.data_point().timestamp
            };

            normalized_rows.push(Row::with_labels(
                row.metric().to_string(),
                row.labels().to_vec(),
                DataPoint::new(timestamp, row.data_point().value),
            ));
        }

        // Write to WAL first using normalized rows
        self.wal.append_rows(&normalized_rows)?;

        let mut outdated_rows = Vec::new();
        let mut max_timestamp = i64::MIN;
        let mut rows_added = 0usize;

        for row in &normalized_rows {
            let timestamp = row.data_point().timestamp;
            let current_min = self.min_t.load(Ordering::Acquire);
            if current_min != 0 && timestamp < current_min {
                let diff = current_min.saturating_sub(timestamp);
                if diff > self.partition_duration {
                    outdated_rows.push(row.clone()); // return normalized row
                    continue;
                }
            }

            self.update_min_timestamp(timestamp);

            if timestamp > max_timestamp {
                max_timestamp = timestamp;
            }

            // Get or create metric
            let metric_name = marshal_metric_name(row.metric(), row.labels());
            let metric = self.get_or_create_metric(metric_name);

            // Insert the point
            metric.insert_point(DataPoint::new(timestamp, row.data_point().value));
            rows_added += 1;
        }

        if !outdated_rows.is_empty() {
            tracing::debug!(
                count = outdated_rows.len(),
                partition_min = self.min_t.load(Ordering::Relaxed),
                partition_duration = self.partition_duration,
                "memory_partition_outdated_rows"
            );
        }

        // Update counters
        self.num_points.fetch_add(rows_added, Ordering::SeqCst);

        if rows_added > 0 {
            // Update max timestamp atomically with exponential backoff
            let mut retries = 0;
            loop {
                let current_max = self.max_t.load(Ordering::Acquire);
                if max_timestamp <= current_max {
                    break;
                }
                match self.max_t.compare_exchange_weak(
                    current_max,
                    max_timestamp,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(_) => {
                        retries += 1;
                        if retries <= 3 {
                            // Exponential backoff: 1, 2, 4 iterations
                            for _ in 0..(1 << (retries - 1)) {
                                std::hint::spin_loop();
                            }
                        } else {
                            // After 3 retries, yield to scheduler
                            std::thread::yield_now();
                            retries = 0; // Reset counter after yield
                        }
                    }
                }
            }
        }

        Ok(outdated_rows)
    }

    fn select_data_points(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        let metric_name = marshal_metric_name(metric, labels);

        match self.metrics.get(&metric_name) {
            Some(metric) => Ok(metric.select_points(start, end)),
            None => Ok(Vec::new()),
        }
    }

    fn select_all_labels(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        let mut results = Vec::new();

        // Iterate through all metrics to find ones matching the base metric name
        for entry in self.metrics.iter() {
            let (marshaled_name, metric_ref) = entry.pair();

            // Unmarshal the metric name to extract the base name and labels
            if let Ok((base_metric, labels)) = unmarshal_metric_name(marshaled_name) {
                // Check if this is the metric we're looking for
                if base_metric == metric {
                    let points = metric_ref.select_points(start, end);
                    if !points.is_empty() {
                        results.push((labels, points));
                    }
                }
            }
        }

        Ok(results)
    }

    fn min_timestamp(&self) -> i64 {
        self.min_t.load(Ordering::SeqCst)
    }

    fn max_timestamp(&self) -> i64 {
        self.max_t.load(Ordering::SeqCst)
    }

    fn size(&self) -> usize {
        self.num_points.load(Ordering::SeqCst)
    }

    fn active(&self) -> bool {
        let min = self.min_timestamp();
        let max = self.max_timestamp();
        if min == 0 {
            return true; // Empty partition is active
        }
        max - min + 1 < self.partition_duration
    }

    fn expired(&self) -> bool {
        false // Memory partitions don't expire
    }

    fn clean(&self) -> Result<()> {
        // Memory is automatically cleaned by dropping
        Ok(())
    }

    fn flush_to_disk(&self) -> Result<Option<(Vec<u8>, crate::disk::PartitionMeta)>> {
        // Flush WAL first
        self.wal.flush()?;

        // Create data buffer
        let mut data = Vec::new();
        let mut metrics_map = HashMap::new();

        // Encode each metric's data
        for entry in self.metrics.iter() {
            let (name, metric) = entry.pair();
            let offset = data.len() as u64;

            // Encode metric data using Gorilla compression
            let mut encoder = GorillaEncoder::new(&mut data);
            metric.encode_all_points(&mut encoder)?;
            encoder.flush()?;

            let encoded_size = (data.len() as u64).saturating_sub(offset);

            // Add to metadata
            let encoded_key = encode_metric_key(name);
            metrics_map.insert(
                encoded_key.clone(),
                DiskMetric {
                    name: encoded_key,
                    offset,
                    encoded_size,
                    min_timestamp: metric.min_timestamp(),
                    max_timestamp: metric.max_timestamp(),
                    num_data_points: metric.size(),
                },
            );
        }

        // Create partition metadata
        let meta = crate::disk::PartitionMeta {
            min_timestamp: self.min_timestamp(),
            max_timestamp: self.max_timestamp(),
            num_data_points: self.size(),
            metrics: metrics_map,
            created_at: SystemTime::now(),
        };

        Ok(Some((data, meta)))
    }
}

/// A memory metric holds ordered data points for a specific metric.
struct MemoryMetric {
    #[allow(dead_code)]
    name: Vec<u8>,
    size: AtomicUsize,
    min_timestamp: AtomicI64,
    max_timestamp: AtomicI64,
    /// In-order points
    points: RwLock<Vec<DataPoint>>,
    /// Out-of-order points to be merged later
    out_of_order_points: Mutex<Vec<DataPoint>>,
}

impl MemoryMetric {
    fn new(name: Vec<u8>) -> Self {
        Self {
            name,
            size: AtomicUsize::new(0),
            min_timestamp: AtomicI64::new(0),
            max_timestamp: AtomicI64::new(0),
            points: RwLock::new(Vec::new()),
            out_of_order_points: Mutex::new(Vec::new()),
        }
    }

    fn insert_point(&self, point: DataPoint) {
        // Check if this is the first insertion using a more reliable approach
        let is_first = self.size.load(Ordering::Acquire) == 0;

        if is_first {
            // Acquire write lock first to ensure atomicity
            let mut points = self.points.write();

            // Double-check inside the lock
            if self.size.load(Ordering::Acquire) == 0 {
                points.push(point);
                self.min_timestamp.store(point.timestamp, Ordering::Release);
                self.max_timestamp.store(point.timestamp, Ordering::Release);
                self.size.store(1, Ordering::Release);
                return;
            }
            // If we're here, another thread beat us to it
            drop(points);
        }

        // Not the first insertion - normal path
        // Use upgradeable read lock to avoid lock thrashing
        let points = self.points.upgradable_read();
        if !points.is_empty() && points[points.len() - 1].timestamp < point.timestamp {
            // Upgrade to write lock only when needed
            let mut points = parking_lot::RwLockUpgradableReadGuard::upgrade(points);
            points.push(point);
            self.max_timestamp.store(point.timestamp, Ordering::SeqCst);
            self.size.fetch_add(1, Ordering::SeqCst);
        } else {
            drop(points);
            // Out of order point
            let mut ooo_points = self.out_of_order_points.lock();
            ooo_points.push(point);
            self.size.fetch_add(1, Ordering::SeqCst);

            // Update min/max if needed
            let current_min = self.min_timestamp.load(Ordering::SeqCst);
            let current_max = self.max_timestamp.load(Ordering::SeqCst);
            if point.timestamp < current_min {
                self.min_timestamp.store(point.timestamp, Ordering::SeqCst);
            }
            if point.timestamp > current_max {
                self.max_timestamp.store(point.timestamp, Ordering::SeqCst);
            }
        }
    }

    fn select_points(&self, start: i64, end: i64) -> Vec<DataPoint> {
        let mut result = Vec::new();

        // Read in-order points first and release lock quickly
        {
            let points = self.points.read();
            for point in points.iter() {
                if point.timestamp >= start && point.timestamp < end {
                    result.push(*point);
                }
            }
        }

        // Then read out-of-order points
        {
            let ooo_points = self.out_of_order_points.lock();
            for point in ooo_points.iter() {
                if point.timestamp >= start && point.timestamp < end {
                    result.push(*point);
                }
            }
        }

        // Sort by timestamp
        result.sort_by_key(|p| p.timestamp);
        result
    }

    fn encode_all_points<W: Write>(&self, encoder: &mut GorillaEncoder<W>) -> Result<()> {
        let points = self.points.read();
        let ooo_points = self.out_of_order_points.lock();

        // Create a sorted iterator without cloning
        let mut all_points: Vec<&DataPoint> = Vec::with_capacity(points.len() + ooo_points.len());
        all_points.extend(points.iter());
        all_points.extend(ooo_points.iter());
        all_points.sort_by_key(|p| p.timestamp);

        // Encode all points in sorted order
        for point in all_points {
            encoder.encode_point(point)?;
        }

        Ok(())
    }

    fn min_timestamp(&self) -> i64 {
        self.min_timestamp.load(Ordering::SeqCst)
    }

    fn max_timestamp(&self) -> i64 {
        self.max_timestamp.load(Ordering::SeqCst)
    }

    fn size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }
}

/// Metadata for metrics in a partition.
pub struct MetricsMetadata {
    pub metrics: Vec<MetricMetadata>,
}

impl MetricsMetadata {
    #[allow(dead_code)]
    fn new() -> Self {
        Self {
            metrics: Vec::new(),
        }
    }

    #[allow(dead_code)]
    fn add_metric(&mut self, name: String, offset: u64, min_ts: i64, max_ts: i64, size: usize) {
        self.metrics.push(MetricMetadata {
            name,
            offset,
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            num_data_points: size,
        });
    }
}

/// Metadata for a single metric.
pub struct MetricMetadata {
    pub name: String,
    pub offset: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub num_data_points: usize,
}

/// Flushes a memory partition to disk.
/// This function uses the partition's flush_to_disk method to properly encode and save data.
pub fn flush_memory_partition_to_disk(
    partition: SharedPartition,
    dir_path: impl AsRef<Path>,
    retention: Duration,
) -> Result<DiskPartition> {
    // Use the partition's flush_to_disk method
    let flush_result = partition.flush_to_disk()?;

    match flush_result {
        Some((data, meta)) => {
            // Create the disk partition with the flushed data
            DiskPartition::create(dir_path, meta, data, retention)
        }
        None => {
            // This partition doesn't support flushing (e.g., already a disk partition)
            Err(TsinkError::Other(
                "Partition does not support flushing to disk".to_string(),
            ))
        }
    }
}
