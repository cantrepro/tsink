//! Write-ahead log implementation.

use crate::label::{marshal_metric_name, unmarshal_metric_name};
use crate::{DataPoint, Result, Row, TsinkError};
use parking_lot::Mutex;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::warn;

/// WAL operation types.
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum WalOperation {
    Insert = 0,
}

/// Trait for write-ahead log implementations.
pub trait Wal: Send + Sync {
    /// Appends rows to the WAL.
    fn append_rows(&self, rows: &[Row]) -> Result<()>;

    /// Flushes buffered data to disk.
    fn flush(&self) -> Result<()>;

    /// Punctuates the WAL (creates a new segment).
    fn punctuate(&self) -> Result<()>;

    /// Removes the oldest WAL segment.
    fn remove_oldest(&self) -> Result<()>;

    /// Removes all WAL segments.
    fn remove_all(&self) -> Result<()>;

    /// Refreshes the WAL (removes all and starts fresh).
    fn refresh(&self) -> Result<()>;
}

/// No-op WAL implementation.
pub struct NopWal;

impl Wal for NopWal {
    fn append_rows(&self, _rows: &[Row]) -> Result<()> {
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn punctuate(&self) -> Result<()> {
        Ok(())
    }

    fn remove_oldest(&self) -> Result<()> {
        Ok(())
    }

    fn remove_all(&self) -> Result<()> {
        Ok(())
    }

    fn refresh(&self) -> Result<()> {
        Ok(())
    }
}

/// Disk-based WAL implementation.
pub struct DiskWal {
    dir: PathBuf,
    current_segment: Mutex<Option<WalSegment>>,
    buffer_size: usize,
    segment_index: AtomicU32,
}

impl DiskWal {
    /// Creates a new disk WAL.
    pub fn new(dir: impl AsRef<Path>, buffer_size: usize) -> Result<Arc<Self>> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Find the highest segment index to continue from there
        let max_index = Self::find_max_segment_index(&dir)?;

        Ok(Arc::new(Self {
            dir,
            current_segment: Mutex::new(None),
            buffer_size,
            segment_index: AtomicU32::new(max_index + 1),
        }))
    }

    /// Finds the maximum segment index in the directory.
    fn find_max_segment_index(dir: &Path) -> Result<u32> {
        let mut max_index = 0u32;

        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Some(name) = entry.file_name().to_str() {
                        if let Ok(index) = name.parse::<u32>() {
                            max_index = max_index.max(index);
                        }
                    }
                }
            }
        }

        Ok(max_index)
    }

    /// Creates a new segment file.
    fn create_segment_file(&self) -> Result<(PathBuf, File)> {
        let index = self.segment_index.fetch_add(1, Ordering::SeqCst);
        let path = self.dir.join(index.to_string());
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((path, file))
    }

    /// Gets the current segment writer.
    fn get_or_create_writer(&self) -> Result<()> {
        let mut current = self.current_segment.lock();

        if current.is_none() {
            let (path, file) = self.create_segment_file()?;

            let writer = if self.buffer_size > 0 {
                BufWriter::with_capacity(self.buffer_size, file)
            } else {
                BufWriter::new(file)
            };

            *current = Some(WalSegment { path, writer });
        }

        Ok(())
    }

    /// Lists all WAL segment files in order.
    fn list_segments(&self) -> Result<Vec<PathBuf>> {
        let mut segments = Vec::new();

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                if let Some(name) = path.file_name() {
                    // Check if it's a valid segment file (numeric name)
                    if name.to_string_lossy().parse::<u32>().is_ok() {
                        segments.push(path);
                    }
                }
            }
        }

        // Sort by numeric value
        segments.sort_by_key(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0)
        });

        Ok(segments)
    }
}

impl Wal for DiskWal {
    fn append_rows(&self, rows: &[Row]) -> Result<()> {
        self.get_or_create_writer()?;

        let mut current = self.current_segment.lock();
        if let Some(ref mut segment) = *current {
            for row in rows {
                // Write operation type
                segment.writer.write_all(&[WalOperation::Insert as u8])?;

                // Marshal metric name with labels (like Go version)
                let metric_name = marshal_metric_name(row.metric(), row.labels());

                // Write metric name length as varint
                let mut len_buf = [0u8; 10];
                let len_size = encode_uvarint(metric_name.len() as u64, &mut len_buf);
                segment.writer.write_all(&len_buf[..len_size])?;

                // Write metric name
                segment.writer.write_all(metric_name.as_bytes())?;

                // Write timestamp as varint
                let mut ts_buf = [0u8; 10];
                let ts_size = encode_varint(row.data_point().timestamp, &mut ts_buf);
                segment.writer.write_all(&ts_buf[..ts_size])?;

                // Write value as float64 bits encoded as uvarint
                let value_bits = row.data_point().value.to_bits();
                let mut val_buf = [0u8; 10];
                let val_size = encode_uvarint(value_bits, &mut val_buf);
                segment.writer.write_all(&val_buf[..val_size])?;
            }

            if self.buffer_size == 0 {
                segment.writer.flush()?;
            }
        }

        Ok(())
    }

    fn flush(&self) -> Result<()> {
        if let Some(ref mut segment) = *self.current_segment.lock() {
            segment.writer.flush()?;
        }
        Ok(())
    }

    fn punctuate(&self) -> Result<()> {
        // Flush current segment and create a new one
        let mut current = self.current_segment.lock();
        if let Some(ref mut segment) = *current {
            segment.writer.flush()?;
        }

        // Force creation of new segment on next write
        *current = None;
        Ok(())
    }

    fn remove_oldest(&self) -> Result<()> {
        let segments = self.list_segments()?;
        if let Some(oldest) = segments.first() {
            fs::remove_file(oldest)?;
        }
        Ok(())
    }

    fn remove_all(&self) -> Result<()> {
        let segments = self.list_segments()?;
        for segment in segments {
            fs::remove_file(segment)?;
        }

        // Clear current segment
        *self.current_segment.lock() = None;
        Ok(())
    }

    fn refresh(&self) -> Result<()> {
        self.remove_all()?;
        Ok(())
    }
}

/// A WAL segment.
struct WalSegment {
    #[allow(dead_code)]
    path: PathBuf,
    writer: BufWriter<File>,
}

/// WAL Reader for recovery.
pub struct WalReader {
    dir: PathBuf,
    rows_to_insert: Vec<Row>,
}

impl WalReader {
    /// Creates a new WAL reader.
    pub fn new(dir: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
            rows_to_insert: Vec::new(),
        })
    }

    /// Reads all WAL segments and returns the recovered rows.
    pub fn read_all(mut self) -> Result<Vec<Row>> {
        let mut segments = Vec::new();

        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries {
                let entry = entry?;
                let path = entry.path();

                if path.is_file() {
                    if let Some(name) = path.file_name() {
                        // Check if it's a valid segment file (numeric name)
                        if name.to_string_lossy().parse::<u32>().is_ok() {
                            segments.push(path);
                        }
                    }
                }
            }
        }

        // Sort by numeric value
        segments.sort_by_key(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0)
        });

        // Read each segment
        for segment_path in segments {
            if let Err(e) = self.read_segment(&segment_path) {
                // Log error but continue - segment might be partially written
                warn!("Error reading WAL segment {:?}: {}", segment_path, e);
            }
        }

        Ok(self.rows_to_insert)
    }

    /// Reads a single WAL segment.
    fn read_segment(&mut self, path: &Path) -> Result<()> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        loop {
            // Read operation type
            let mut op_buf = [0u8; 1];
            match reader.read_exact(&mut op_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let op = WalOperation::from_u8(op_buf[0]).ok_or_else(|| {
                TsinkError::Other(format!("Unknown WAL operation: {}", op_buf[0]))
            })?;

            match op {
                WalOperation::Insert => {
                    // Read metric name length
                    let metric_len = match decode_uvarint(&mut reader) {
                        Ok(len) => len as usize,
                        Err(_) => break, // Incomplete record
                    };

                    // Read metric name
                    let mut metric_buf = vec![0u8; metric_len];
                    if reader.read_exact(&mut metric_buf).is_err() {
                        break; // Incomplete record
                    }
                    let metric_name_raw = String::from_utf8_lossy(&metric_buf).to_string();

                    // Unmarshal metric name and labels
                    let (metric, labels) = unmarshal_metric_name(&metric_name_raw)?;

                    // Read timestamp
                    let timestamp = match decode_varint(&mut reader) {
                        Ok(ts) => ts,
                        Err(_) => break, // Incomplete record
                    };

                    // Read value
                    let value_bits = match decode_uvarint(&mut reader) {
                        Ok(bits) => bits,
                        Err(_) => break, // Incomplete record
                    };
                    let value = f64::from_bits(value_bits);

                    self.rows_to_insert.push(Row {
                        metric,
                        labels,
                        data_point: DataPoint::new(timestamp, value),
                    });
                }
            }
        }

        Ok(())
    }
}

impl WalOperation {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(WalOperation::Insert),
            _ => None,
        }
    }
}

// Varint encoding/decoding functions
fn encode_varint(value: i64, buf: &mut [u8]) -> usize {
    // Zigzag encode
    let uvalue = ((value << 1) ^ (value >> 63)) as u64;
    encode_uvarint(uvalue, buf)
}

fn encode_uvarint(mut value: u64, buf: &mut [u8]) -> usize {
    let mut i = 0;
    while value >= 0x80 {
        buf[i] = (value as u8) | 0x80;
        value >>= 7;
        i += 1;
    }
    buf[i] = value as u8;
    i + 1
}

fn decode_varint<R: Read>(reader: &mut R) -> Result<i64> {
    let uvalue = decode_uvarint(reader)?;
    // Zigzag decode
    Ok(((uvalue >> 1) as i64) ^ -((uvalue & 1) as i64))
}

fn decode_uvarint<R: Read>(reader: &mut R) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0;

    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;

        result |= ((byte[0] & 0x7F) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;

        if shift >= 64 {
            return Err(TsinkError::Other("Varint overflow".to_string()));
        }
    }

    Ok(result)
}
