//! Memory partition implementation.

use crate::disk::{DiskMetric, DiskPartition, PartitionMeta, encode_metric_key};
use crate::encoding::GorillaEncoder;
use crate::label::{marshal_metric_name, unmarshal_metric_name};
use crate::partition::{Partition, SharedPartition};
use crate::time::{duration_to_units, now_in_precision};
use crate::wal::Wal;
use crate::{DataPoint, Label, Result, Row, TimestampPrecision, TsinkError};
use dashmap::{DashMap, mapref::entry::Entry};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs;
use std::io::{Seek, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
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
    /// Retention configuration
    retention: Duration,
    /// Retention window in timestamp units
    retention_units: i64,
    /// Creation time to gate retention on wall clock
    created_at: SystemTime,
    /// Flag to ensure min_t is set only once
    min_t_set: AtomicUsize,
    /// Prevents new writes from entering while this partition is being flushed.
    flush_sealed: AtomicBool,
    /// Number of in-flight write operations currently mutating this partition.
    inflight_inserts: AtomicUsize,
}

struct InflightInsertGuard<'a> {
    counter: &'a AtomicUsize,
}

impl Drop for InflightInsertGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

impl MemoryPartition {
    /// Creates a new memory partition.
    pub fn new(
        wal: Arc<dyn Wal>,
        partition_duration: Duration,
        timestamp_precision: TimestampPrecision,
        retention: Duration,
    ) -> Self {
        let duration = duration_to_units(partition_duration, timestamp_precision);
        let retention_units = duration_to_units(retention, timestamp_precision);
        let created_at = SystemTime::now();

        Self {
            num_points: AtomicUsize::new(0),
            min_t: AtomicI64::new(0),
            max_t: AtomicI64::new(0),
            metrics: DashMap::new(),
            wal,
            partition_duration: duration,
            timestamp_precision,
            retention,
            retention_units,
            created_at,
            min_t_set: AtomicUsize::new(0),
            flush_sealed: AtomicBool::new(false),
            inflight_inserts: AtomicUsize::new(0),
        }
    }

    /// Gets or creates a metric.
    fn get_or_create_metric(&self, name: Vec<u8>) -> Arc<MemoryMetric> {
        match self.metrics.entry(name) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let metric = Arc::new(MemoryMetric::new(entry.key().clone()));
                entry.insert(metric.clone());
                metric
            }
        }
    }

    fn get_or_create_plain_metric(&self, metric: &str) -> Arc<MemoryMetric> {
        if let Some(existing) = self.metrics.get(metric.as_bytes()) {
            existing.clone()
        } else {
            self.get_or_create_metric(metric.as_bytes().to_vec())
        }
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

        data_file.sync_all()?;

        // Create metadata
        let meta = PartitionMeta {
            min_timestamp: Partition::min_timestamp(self),
            max_timestamp: Partition::max_timestamp(self),
            num_data_points: Partition::size(self),
            metrics: metrics_map,
            timestamp_precision: self.timestamp_precision,
            created_at: self.created_at,
        };

        // Write metadata atomically
        let meta_path = dir_path.join(crate::disk::META_FILE_NAME);
        crate::disk::write_meta_atomic(&meta_path, &meta)?;

        // Open the created partition
        DiskPartition::open(dir_path, retention)
    }

    fn insert_rows_impl(&self, rows: &[Row], append_wal: bool) -> Result<Vec<Row>> {
        if rows.is_empty() {
            return Err(TsinkError::Other("No rows given".to_string()));
        }

        if self.flush_sealed.load(Ordering::Acquire) {
            return Ok(rows.to_vec());
        }
        self.inflight_inserts.fetch_add(1, Ordering::AcqRel);
        if self.flush_sealed.load(Ordering::Acquire) {
            self.inflight_inserts.fetch_sub(1, Ordering::AcqRel);
            return Ok(rows.to_vec());
        }
        let _inflight_guard = InflightInsertGuard {
            counter: &self.inflight_inserts,
        };

        let mut normalized_rows = Vec::with_capacity(rows.len());
        let mut batch_max_timestamp = i64::MIN;
        let mut replace_zero_with: Option<i64> = None;

        for row in rows {
            let original = row.data_point();
            let timestamp = if original.timestamp == 0 {
                let now = *replace_zero_with
                    .get_or_insert_with(|| now_in_precision(self.timestamp_precision));
                tracing::warn!(
                    "Replacing zero timestamp with current time {} for metric {}",
                    now,
                    row.metric()
                );
                now
            } else {
                original.timestamp
            };

            batch_max_timestamp = batch_max_timestamp.max(timestamp);
            if timestamp == original.timestamp {
                normalized_rows.push(row.clone());
            } else {
                normalized_rows.push(Row::with_labels(
                    row.metric().to_string(),
                    row.labels().to_vec(),
                    DataPoint::new(timestamp, original.value),
                ));
            }
        }

        let mut accepted_rows = Vec::with_capacity(normalized_rows.len());
        let mut outdated_rows = Vec::new();
        let mut max_timestamp = i64::MIN;

        let partition_min = self.min_t.load(Ordering::Acquire);
        let allowed_min = if partition_min == 0 {
            // For a fresh partition, anchor this batch to its newest timestamp so that very old
            // rows are cascaded to older partitions instead of stretching the new head window.
            batch_max_timestamp.saturating_sub(self.partition_duration)
        } else {
            partition_min.saturating_sub(self.partition_duration)
        };

        for row in normalized_rows {
            let timestamp = row.data_point().timestamp;
            if timestamp < allowed_min {
                outdated_rows.push(row);
                continue;
            }
            max_timestamp = max_timestamp.max(timestamp);
            accepted_rows.push(row);
        }

        if accepted_rows.is_empty() {
            if !outdated_rows.is_empty() {
                tracing::debug!(
                    count = outdated_rows.len(),
                    partition_min = self.min_t.load(Ordering::Relaxed),
                    partition_duration = self.partition_duration,
                    "memory_partition_outdated_rows"
                );
            }
            return Ok(outdated_rows);
        }

        // Write only the rows this partition will actually store to WAL.
        // During recovery, WAL appends must be skipped to avoid replay duplication.
        if append_wal {
            self.wal.append_rows(&accepted_rows)?;
        }

        let mut rows_added = 0usize;

        for row in &accepted_rows {
            let timestamp = row.data_point().timestamp;

            self.update_min_timestamp(timestamp);

            // Get or create metric
            let metric = if row.labels().is_empty() {
                self.get_or_create_plain_metric(row.metric())
            } else {
                let metric_name = marshal_metric_name(row.metric(), row.labels());
                self.get_or_create_metric(metric_name)
            };

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
                if current_max != 0 && max_timestamp <= current_max {
                    break;
                }
                let desired = if current_max == 0 {
                    max_timestamp
                } else {
                    max_timestamp.max(current_max)
                };
                match self.max_t.compare_exchange_weak(
                    current_max,
                    desired,
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
}

impl crate::partition::Partition for MemoryPartition {
    fn insert_rows(&self, rows: &[Row]) -> Result<Vec<Row>> {
        self.insert_rows_impl(rows, true)
    }

    fn insert_rows_recovery(&self, rows: &[Row]) -> Result<Vec<Row>> {
        self.insert_rows_impl(rows, false)
    }

    fn select_data_points(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        let min = self.min_t.load(Ordering::Acquire);
        let max = self.max_t.load(Ordering::Acquire);
        if min != 0 && (end <= min || start > max) {
            return Ok(Vec::new());
        }

        if labels.is_empty() {
            return Ok(self
                .metrics
                .get(metric.as_bytes())
                .map_or_else(Vec::new, |m| m.select_points(start, end)));
        }

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
        max.saturating_sub(min).saturating_add(1) < self.partition_duration
    }

    fn expired(&self) -> bool {
        if self.retention_units <= 0 {
            return false;
        }

        let max_ts = self.max_timestamp();
        if max_ts == 0 {
            return false;
        }

        let cutoff =
            now_in_precision(self.timestamp_precision).saturating_sub(self.retention_units);
        let timestamp_expired = max_ts < cutoff;
        let age = self.created_at.elapsed().unwrap_or(Duration::ZERO);
        let age_expired = age > self.retention;
        let stale_by_timestamp = timestamp_expired && age >= Duration::from_secs(1);

        stale_by_timestamp || age_expired
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
            timestamp_precision: self.timestamp_precision,
            created_at: self.created_at,
        };

        Ok(Some((data, meta)))
    }

    fn begin_flush(&self) -> bool {
        if self
            .flush_sealed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }

        while self.inflight_inserts.load(Ordering::Acquire) > 0 {
            std::hint::spin_loop();
            std::thread::yield_now();
        }
        true
    }

    fn end_flush(&self) {
        self.flush_sealed.store(false, Ordering::Release);
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

    fn merge_out_of_order_points(&self) {
        let mut ooo_points = self.out_of_order_points.lock();
        if ooo_points.is_empty() {
            return;
        }

        ooo_points.sort_by_key(|p| p.timestamp);
        let incoming = std::mem::take(&mut *ooo_points);
        drop(ooo_points);

        let mut points = self.points.write();
        if points.is_empty() {
            points.extend(incoming);
            return;
        }

        if let (Some(last), Some(first_incoming)) = (points.last(), incoming.first())
            && last.timestamp <= first_incoming.timestamp
        {
            points.extend(incoming);
            return;
        }

        let mut merged = Vec::with_capacity(points.len() + incoming.len());
        let mut i = 0usize;
        let mut j = 0usize;

        while i < points.len() && j < incoming.len() {
            if points[i].timestamp <= incoming[j].timestamp {
                merged.push(points[i]);
                i += 1;
            } else {
                merged.push(incoming[j]);
                j += 1;
            }
        }

        if i < points.len() {
            merged.extend_from_slice(&points[i..]);
        }
        if j < incoming.len() {
            merged.extend_from_slice(&incoming[j..]);
        }

        *points = merged;
    }

    fn select_points(&self, start: i64, end: i64) -> Vec<DataPoint> {
        self.merge_out_of_order_points();

        let points = self.points.read();
        if points.is_empty() {
            return Vec::new();
        }

        // Points are timestamp-sorted; use binary search to avoid full scans.
        let start_idx = points.partition_point(|p| p.timestamp < start);
        let end_idx = points.partition_point(|p| p.timestamp < end);
        if start_idx >= end_idx {
            Vec::new()
        } else {
            points[start_idx..end_idx].to_vec()
        }
    }

    fn encode_all_points<W: Write>(&self, encoder: &mut GorillaEncoder<W>) -> Result<()> {
        self.merge_out_of_order_points();

        let points = self.points.read();
        for point in points.iter() {
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
