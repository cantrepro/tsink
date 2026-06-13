//! Disk partition implementation.

use crate::encoding::GorillaDecoder;
use crate::label::{marshal_metric_name, unmarshal_metric_name};
use crate::mmap::PlatformMmap;
use crate::time::{duration_to_units, now_in_precision};
use crate::{DataPoint, Label, Result, Row, TimestampPrecision, TsinkError};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Write;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

pub const DATA_FILE_NAME: &str = "data";
pub const META_FILE_NAME: &str = "meta.json";

/// Metadata for a disk partition.
#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionMeta {
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub num_data_points: usize,
    pub metrics: HashMap<String, DiskMetric>,
    #[serde(default = "default_timestamp_precision")]
    pub timestamp_precision: TimestampPrecision,
    pub created_at: SystemTime,
}

/// Metadata for a metric in a disk partition.
#[derive(Debug, Serialize, Deserialize)]
pub struct DiskMetric {
    pub name: String,
    pub offset: u64,
    #[serde(default = "default_encoded_size")]
    pub encoded_size: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub num_data_points: usize,
}

/// A disk partition stores time-series data on disk using memory-mapped files.
pub struct DiskPartition {
    dir_path: PathBuf,
    meta: PartitionMeta,
    mapped_file: RwLock<Option<PlatformMmap>>,
    retention: Duration,
    timestamp_precision: TimestampPrecision,
    retention_units: i64,
}

impl DiskPartition {
    /// Helper method to decode points from a disk metric.
    fn decode_metric_points(
        &self,
        disk_metric: &DiskMetric,
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        // Early exit if query range is completely outside metric range
        if end <= disk_metric.min_timestamp || start > disk_metric.max_timestamp {
            return Ok(Vec::new());
        }

        let mapped_guard = self.mapped_file.read();
        let mapped_file = mapped_guard
            .as_ref()
            .ok_or_else(|| TsinkError::InvalidPartition {
                id: self.dir_path.display().to_string(),
            })?;

        // Validate offset is within bounds
        let offset = disk_metric.offset as usize;
        if offset >= mapped_file.len() {
            return Err(TsinkError::InvalidOffset {
                offset: disk_metric.offset,
                max: mapped_file.len() as u64,
            });
        }

        // Create a cursor at the metric's offset with bounds checking
        let data_slice = mapped_file.as_slice();
        let mapped_len = data_slice.len();
        let encoded_size = if disk_metric.encoded_size > 0 {
            disk_metric.encoded_size as usize
        } else {
            // Fallback for older metadata without encoded size: read until file end
            mapped_len.saturating_sub(offset)
        };

        let end_offset = std::cmp::min(mapped_len, offset.saturating_add(encoded_size));
        if end_offset <= offset {
            return Err(TsinkError::DataCorruption(format!(
                "Invalid metric bounds: offset {offset}, encoded_size {encoded_size}"
            )));
        }

        let metric_data = &data_slice[offset..end_offset];

        // Decode points
        let mut decoder = GorillaDecoder::from_slice(metric_data);
        // Metadata can be corrupted; avoid preallocating attacker-controlled capacity.
        let mut points = Vec::new();

        // Must decode all points sequentially due to delta encoding
        for _ in 0..disk_metric.num_data_points {
            let point = decoder.decode_point()?;

            if point.timestamp < start {
                continue;
            }
            if point.timestamp >= end {
                break;
            }

            points.push(point);
        }

        Ok(points)
    }

    /// Opens an existing disk partition.
    pub fn open(dir_path: impl AsRef<Path>, retention: Duration) -> Result<Self> {
        let dir_path = dir_path.as_ref();

        // Read metadata
        let meta_path = dir_path.join(META_FILE_NAME);
        if !meta_path.exists() {
            return Err(TsinkError::InvalidPartition {
                id: dir_path.to_string_lossy().to_string(),
            });
        }

        let meta_file = File::open(&meta_path)?;
        let meta: PartitionMeta = serde_json::from_reader(meta_file)?;

        // Memory-map the data file
        let data_path = dir_path.join(DATA_FILE_NAME);
        let data_file = File::open(&data_path)?;

        if data_file.metadata()?.len() == 0 {
            return Err(TsinkError::NoDataPoints {
                metric: "unknown".to_string(),
                start: 0,
                end: 0,
            });
        }

        let file_len = data_file.metadata()?.len() as usize;
        let mapped_file = PlatformMmap::new_readonly(data_file, file_len)?;
        let timestamp_precision = meta.timestamp_precision;
        let retention_units = duration_to_units(retention, timestamp_precision);

        Ok(Self {
            dir_path: dir_path.to_path_buf(),
            meta,
            mapped_file: RwLock::new(Some(mapped_file)),
            retention,
            timestamp_precision,
            retention_units,
        })
    }

    /// Creates a new disk partition from memory partition data.
    pub fn create(
        dir_path: impl AsRef<Path>,
        meta: PartitionMeta,
        data: Vec<u8>,
        retention: Duration,
    ) -> Result<Self> {
        let dir_path = dir_path.as_ref();

        if dir_path.exists() {
            // Prevent accidental overwrite of an existing persisted partition.
            if fs::read_dir(dir_path)?.next().is_some() {
                return Err(TsinkError::IoWithPath {
                    path: dir_path.to_path_buf(),
                    source: io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!(
                            "partition directory already exists and is not empty: {}",
                            dir_path.display()
                        ),
                    ),
                });
            }
        } else {
            fs::create_dir_all(dir_path)?;
        }

        // Write data file
        let data_path = dir_path.join(DATA_FILE_NAME);
        {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&data_path)?;
            file.write_all(&data)?;
            file.sync_all()?;
        }

        // Write metadata file (write last to indicate valid partition)
        let meta_path = dir_path.join(META_FILE_NAME);
        write_meta_atomic(&meta_path, &meta)?;

        // Open the created partition
        Self::open(dir_path, retention)
    }
}

impl crate::partition::Partition for DiskPartition {
    fn insert_rows(&self, _rows: &[Row]) -> Result<Vec<Row>> {
        Err(TsinkError::ReadOnlyPartition {
            path: self.dir_path.clone(),
        })
    }

    fn select_data_points(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        if self.expired() {
            return Ok(Vec::new());
        }

        let metric_name = marshal_metric_name(metric, labels);
        let encoded_key = encode_metric_key(&metric_name);

        let disk_metric = self.meta.metrics.get(&encoded_key).or_else(|| {
            // Backward compatibility: fall back to plain UTF-8 metric name if present
            std::str::from_utf8(&metric_name)
                .ok()
                .and_then(|plain| self.meta.metrics.get(plain))
        });

        let Some(disk_metric) = disk_metric else {
            return Ok(Vec::new());
        };

        self.decode_metric_points(disk_metric, start, end)
    }

    fn select_all_labels(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        if self.expired() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();

        // Iterate through all metrics in metadata
        for (encoded_key, disk_metric) in &self.meta.metrics {
            // Try to unmarshal the name to extract base metric and labels
            let marshaled_bytes = decode_metric_key(encoded_key);
            let mut matched = false;

            // First try to unmarshal it as a marshaled name
            if let Ok((base_metric, labels)) = unmarshal_metric_name(&marshaled_bytes)
                && base_metric == metric
            {
                // Found a matching metric, decode its data points
                let points = self.decode_metric_points(disk_metric, start, end)?;
                if !points.is_empty() {
                    results.push((labels, points));
                    matched = true;
                }
            }

            if !matched && (encoded_key == metric || marshaled_bytes == metric.as_bytes()) {
                // It might be a plain metric key from legacy metadata.
                let points = self.decode_metric_points(disk_metric, start, end)?;
                if !points.is_empty() {
                    results.push((Vec::new(), points));
                }
            }
        }

        Ok(results)
    }

    fn min_timestamp(&self) -> i64 {
        self.meta.min_timestamp
    }

    fn max_timestamp(&self) -> i64 {
        self.meta.max_timestamp
    }

    fn size(&self) -> usize {
        self.meta.num_data_points
    }

    fn active(&self) -> bool {
        false // Disk partitions are always read-only
    }

    fn expired(&self) -> bool {
        if self.retention_units <= 0 {
            return false;
        }

        let cutoff =
            now_in_precision(self.timestamp_precision).saturating_sub(self.retention_units);
        let timestamp_expired = self.meta.max_timestamp < cutoff;
        let age = self.meta.created_at.elapsed().unwrap_or(Duration::ZERO);
        let age_expired = age > self.retention;
        let stale_by_timestamp = timestamp_expired && age >= Duration::from_secs(1);

        stale_by_timestamp || age_expired
    }

    fn clean(&self) -> Result<()> {
        // Drop the mmap before deleting files to keep behavior consistent across platforms.
        let mut mapped = self.mapped_file.write();
        let _ = mapped.take();
        drop(mapped);

        if self.dir_path.exists() {
            fs::remove_dir_all(&self.dir_path)?;
        }
        Ok(())
    }

    fn flush_to_disk(&self) -> Result<Option<(Vec<u8>, PartitionMeta)>> {
        // DiskPartition is already on disk, so return None
        Ok(None)
    }
}

/// Lossless key encoding for marshaled metric bytes.
pub(crate) fn encode_metric_key(metric: &[u8]) -> String {
    let mut out = String::with_capacity(metric.len() * 2);
    for byte in metric {
        // Lower-case hex for deterministic keys
        let _ = write!(&mut out, "{:02x}", byte);
    }
    out
}

/// Decodes a previously encoded metric key, falling back to raw UTF-8 bytes for old metadata.
pub(crate) fn decode_metric_key(key: &str) -> Vec<u8> {
    if key.len() & 1 == 0 && key.as_bytes().iter().all(|b| b.is_ascii_hexdigit()) {
        let mut out = Vec::with_capacity(key.len() / 2);
        let mut i = 0;
        while i < key.len() {
            let byte_str = &key[i..i + 2];
            if let Ok(val) = u8::from_str_radix(byte_str, 16) {
                out.push(val);
            } else {
                return key.as_bytes().to_vec();
            }
            i += 2;
        }
        return out;
    }

    key.as_bytes().to_vec()
}

const fn default_encoded_size() -> u64 {
    0
}

const fn default_timestamp_precision() -> TimestampPrecision {
    TimestampPrecision::Nanoseconds
}

pub(crate) fn write_meta_atomic(path: &Path, meta: &PartitionMeta) -> Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let file = File::create(&tmp)?;
        serde_json::to_writer_pretty(&file, meta)?;
        file.sync_all()?;
    }
    fs::rename(tmp, path)?;
    sync_parent_dir(path)?;
    Ok(())
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> Result<()> {
    Ok(())
}
