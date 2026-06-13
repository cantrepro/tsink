//! Disk partition implementation.

use crate::encoding::{Gorilla64Decoder, MetricEncoding, PointDecoder};
use crate::label::{marshal_metric_name, unmarshal_metric_name};
use crate::mmap::PlatformMmap;
use crate::time::{duration_to_units, now_in_precision};
use crate::{DataPoint, Label, Result, Row, TimestampPrecision, TsinkError};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Write;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write as IoWrite};
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
    pub timestamp_precision: TimestampPrecision,
    pub created_at: SystemTime,
}

/// Metadata for a metric in a disk partition.
#[derive(Debug, Serialize, Deserialize)]
pub struct DiskMetric {
    pub name: String,
    pub offset: u64,
    pub encoded_size: u64,
    pub encoding: MetricEncoding,
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
        if end <= disk_metric.min_timestamp || start > disk_metric.max_timestamp {
            return Ok(Vec::new());
        }

        let mapped_guard = self.mapped_file.read();
        let mapped_file = mapped_guard
            .as_ref()
            .ok_or_else(|| TsinkError::InvalidPartition {
                id: self.dir_path.display().to_string(),
            })?;

        // Guard against out-of-bounds slices when metadata is corrupt.
        let offset = disk_metric.offset as usize;
        if offset >= mapped_file.len() {
            return Err(TsinkError::InvalidOffset {
                offset: disk_metric.offset,
                max: mapped_file.len() as u64,
            });
        }

        let data_slice = mapped_file.as_slice();
        let mapped_len = data_slice.len();
        let encoded_size = usize::try_from(disk_metric.encoded_size).map_err(|_| {
            TsinkError::DataCorruption(format!(
                "invalid encoded_size {} for metric at offset {}",
                disk_metric.encoded_size, offset
            ))
        })?;
        if encoded_size == 0 {
            return Err(TsinkError::DataCorruption(format!(
                "Invalid metric bounds: offset {offset}, encoded_size {encoded_size}"
            )));
        }

        let end_offset = std::cmp::min(mapped_len, offset.saturating_add(encoded_size));
        if end_offset <= offset {
            return Err(TsinkError::DataCorruption(format!(
                "Invalid metric bounds: offset {offset}, encoded_size {encoded_size}"
            )));
        }

        let metric_data = &data_slice[offset..end_offset];

        // Metadata can be corrupted; avoid preallocating attacker-controlled capacity.
        let mut points = Vec::new();

        let max_points_from_size = encoded_size.saturating_mul(8);
        if disk_metric.num_data_points > max_points_from_size {
            return Err(TsinkError::DataCorruption(format!(
                "invalid num_data_points {} for encoded_size {}",
                disk_metric.num_data_points, encoded_size
            )));
        }

        match disk_metric.encoding {
            MetricEncoding::Typed => {
                let mut decoder = PointDecoder::from_slice(metric_data);
                for (decoded_points, _) in (0..disk_metric.num_data_points).enumerate() {
                    let point = match decoder.decode_point() {
                        Ok(point) => point,
                        Err(TsinkError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                            return Err(TsinkError::DataCorruption(format!(
                                "truncated metric stream: expected {} points, decoded {}",
                                disk_metric.num_data_points, decoded_points
                            )));
                        }
                        Err(e) => return Err(e),
                    };

                    if point.timestamp < start {
                        continue;
                    }
                    if point.timestamp >= end {
                        break;
                    }

                    points.push(point);
                }
            }
            gorilla_encoding => {
                let mut decoder = Gorilla64Decoder::from_slice(metric_data);
                for (decoded_points, _) in (0..disk_metric.num_data_points).enumerate() {
                    let (timestamp, value_bits) = match decoder.decode_point() {
                        Ok(decoded) => decoded,
                        Err(TsinkError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                            return Err(TsinkError::DataCorruption(format!(
                                "truncated metric stream: expected {} points, decoded {}",
                                disk_metric.num_data_points, decoded_points
                            )));
                        }
                        Err(e) => return Err(e),
                    };

                    if timestamp < start {
                        continue;
                    }
                    if timestamp >= end {
                        break;
                    }

                    let value = gorilla_encoding.bits_to_value(value_bits)?;
                    points.push(DataPoint::new(timestamp, value));
                }
            }
        }

        Ok(points)
    }

    /// Opens an existing disk partition.
    pub fn open(dir_path: impl AsRef<Path>, retention: Duration) -> Result<Self> {
        let dir_path = dir_path.as_ref();

        let meta_path = dir_path.join(META_FILE_NAME);
        if !meta_path.exists() {
            return Err(TsinkError::InvalidPartition {
                id: dir_path.to_string_lossy().to_string(),
            });
        }

        let meta_file = std::io::BufReader::new(File::open(&meta_path)?);
        let meta: PartitionMeta = serde_json::from_reader(meta_file)?;

        let data_path = dir_path.join(DATA_FILE_NAME);
        let data_file = File::open(&data_path)?;

        let data_metadata = data_file.metadata()?;
        let data_len = data_metadata.len();

        if data_len == 0 {
            return Err(TsinkError::NoDataPoints {
                metric: "unknown".to_string(),
                start: 0,
                end: 0,
            });
        }

        let file_len = usize::try_from(data_len).map_err(|_| TsinkError::MemoryMap {
            path: data_path.clone(),
            details: format!(
                "data file length {} exceeds usize::MAX ({}) on this platform",
                data_len,
                usize::MAX
            ),
        })?;
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

        let data_path = dir_path.join(DATA_FILE_NAME);
        {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&data_path)?;
            file.write_all(&data)?;
            file.sync_all()?;
        }

        let meta_path = dir_path.join(META_FILE_NAME);
        write_meta_atomic(&meta_path, &meta)?;

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
        let Some(disk_metric) = self.meta.metrics.get(&encoded_key) else {
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

        for (encoded_key, disk_metric) in &self.meta.metrics {
            let marshaled_bytes = decode_metric_key(encoded_key)?;
            let (base_metric, labels) = unmarshal_metric_name(&marshaled_bytes)?;
            if base_metric == metric {
                let points = self.decode_metric_points(disk_metric, start, end)?;
                if !points.is_empty() {
                    results.push((labels, points));
                }
            }
        }

        Ok(results)
    }

    fn list_metric_series(&self) -> Result<Vec<(String, Vec<Label>)>> {
        if self.expired() {
            return Ok(Vec::new());
        }

        let mut series = Vec::with_capacity(self.meta.metrics.len());

        for encoded_key in self.meta.metrics.keys() {
            let marshaled = decode_metric_key(encoded_key)?;
            let (metric, mut labels) = unmarshal_metric_name(&marshaled)?;
            labels.sort();
            series.push((metric, labels));
        }

        Ok(series)
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
        let grace = self.retention.min(Duration::from_secs(1));
        timestamp_expired && age >= grace
    }

    fn clean(&self) -> Result<()> {
        let mut mapped = self.mapped_file.write();
        let _ = mapped.take();
        drop(mapped);

        if self.dir_path.exists() {
            fs::remove_dir_all(&self.dir_path)?;
        }
        Ok(())
    }

    fn flush_to_disk(&self) -> Result<Option<(Vec<u8>, PartitionMeta)>> {
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

/// Decodes a previously encoded metric key.
pub(crate) fn decode_metric_key(key: &str) -> Result<Vec<u8>> {
    if key.len() & 1 != 0 || !key.as_bytes().iter().all(|b| b.is_ascii_hexdigit()) {
        return Err(TsinkError::DataCorruption(format!(
            "invalid encoded metric key: {key}"
        )));
    }

    let mut out = Vec::with_capacity(key.len() / 2);
    let mut i = 0;
    while i < key.len() {
        let byte_str = &key[i..i + 2];
        let val = u8::from_str_radix(byte_str, 16).map_err(|e| {
            TsinkError::DataCorruption(format!("invalid encoded metric key {key}: {e}"))
        })?;
        out.push(val);
        i += 2;
    }

    Ok(out)
}

pub(crate) fn write_meta_atomic(path: &Path, meta: &PartitionMeta) -> Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let file = File::create(&tmp)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, meta)?;
        let file = writer.into_inner().map_err(|e| e.into_error())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::PointEncoder;
    use crate::partition::Partition;
    use tempfile::TempDir;

    #[test]
    fn decode_metric_points_rejects_implausible_num_points() {
        let tmp = TempDir::new().unwrap();

        let mut encoded = Vec::new();
        let mut encoder = PointEncoder::new(&mut encoded);
        encoder.encode_point(&DataPoint::new(42, 1.23)).unwrap();
        encoder.flush().unwrap();

        let marshaled = marshal_metric_name("cpu_usage", &[]);
        let encoded_key = encode_metric_key(&marshaled);

        let mut metrics = HashMap::new();
        metrics.insert(
            encoded_key,
            DiskMetric {
                name: "cpu_usage".to_string(),
                offset: 0,
                encoded_size: encoded.len() as u64,
                encoding: MetricEncoding::Typed,
                min_timestamp: 42,
                max_timestamp: 42,
                num_data_points: usize::MAX,
            },
        );

        let meta = PartitionMeta {
            min_timestamp: 42,
            max_timestamp: 42,
            num_data_points: usize::MAX,
            metrics,
            timestamp_precision: TimestampPrecision::Milliseconds,
            created_at: SystemTime::now(),
        };

        let partition = DiskPartition::create(tmp.path(), meta, encoded, Duration::from_secs(60))
            .expect("disk partition should be created");
        let err = Partition::select_data_points(&partition, "cpu_usage", &[], 0, 100)
            .expect_err("invalid metadata should be rejected");

        assert!(matches!(err, TsinkError::DataCorruption(_)));
    }
}
