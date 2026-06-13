//! Write-ahead log implementation.

use crate::label::{marshal_metric_name, unmarshal_metric_name};
use crate::{DataPoint, Result, Row, TsinkError};
use parking_lot::Mutex;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// WAL operation types.
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum WalOperation {
    Insert = 1,
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

const WAL_SEGMENT_EXTENSION: &str = ".wal";
const MAX_WAL_METRIC_NAME_BYTES: usize = 4 * 1024 * 1024;

/// Sync policy for WAL durability/performance tradeoffs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalSyncMode {
    /// Flush and fsync on every append call.
    PerAppend,
    /// Flush every append and fsync at most once per interval.
    Periodic(Duration),
}

impl Default for WalSyncMode {
    fn default() -> Self {
        WalSyncMode::Periodic(Duration::from_secs(1))
    }
}

fn parse_segment_index(name: &OsStr) -> Option<u32> {
    let name = name.to_str()?;
    let trimmed = name.strip_suffix(WAL_SEGMENT_EXTENSION)?.trim();
    if trimmed.is_empty() {
        return None;
    }
    if !trimmed.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    trimmed.parse::<u32>().ok()
}

/// Disk-based WAL implementation.
pub struct DiskWal {
    dir: PathBuf,
    current_segment: Mutex<Option<WalSegment>>,
    buffer_size: usize,
    segment_index: AtomicU32,
    sync_mode: WalSyncMode,
    last_sync: Mutex<Instant>,
}

impl DiskWal {
    /// Creates a new disk WAL.
    pub fn new(dir: impl AsRef<Path>, buffer_size: usize) -> Result<Arc<Self>> {
        Self::new_with_sync_mode(dir, buffer_size, WalSyncMode::default())
    }

    /// Creates a new disk WAL with an explicit sync mode.
    pub fn new_with_sync_mode(
        dir: impl AsRef<Path>,
        buffer_size: usize,
        sync_mode: WalSyncMode,
    ) -> Result<Arc<Self>> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Find the highest segment index to continue from there
        let max_index = Self::find_max_segment_index(&dir)?;

        Ok(Arc::new(Self {
            dir,
            current_segment: Mutex::new(None),
            buffer_size,
            segment_index: AtomicU32::new(max_index + 1),
            sync_mode,
            last_sync: Mutex::new(Instant::now()),
        }))
    }

    /// Finds the maximum segment index in the directory.
    fn find_max_segment_index(dir: &Path) -> Result<u32> {
        let mut max_index = 0u32;

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if entry.path().is_file()
                && let Some(index) = parse_segment_index(&entry.file_name())
            {
                max_index = max_index.max(index);
            }
        }

        Ok(max_index)
    }

    /// Creates a new segment file.
    fn create_segment_file(&self) -> Result<(PathBuf, File)> {
        let index = self.segment_index.fetch_add(1, Ordering::SeqCst);
        let path = self
            .dir
            .join(format!("{:06}{}", index, WAL_SEGMENT_EXTENSION));
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

            if path.is_file()
                && let Some(index) = path.file_name().and_then(parse_segment_index)
            {
                segments.push((index, path));
            }
        }

        // Sort by numeric value
        segments.sort_by_key(|(index, _)| *index);

        Ok(segments.into_iter().map(|(_, path)| path).collect())
    }

    fn maybe_sync_after_append(&self, writer: &mut BufWriter<File>) -> Result<()> {
        // Always flush userspace buffer so data reaches the kernel page cache.
        writer.flush()?;

        match self.sync_mode {
            WalSyncMode::PerAppend => {
                writer.get_ref().sync_all()?;
                *self.last_sync.lock() = Instant::now();
            }
            WalSyncMode::Periodic(interval) => {
                if interval.is_zero() {
                    writer.get_ref().sync_all()?;
                    *self.last_sync.lock() = Instant::now();
                    return Ok(());
                }

                let mut last_sync = self.last_sync.lock();
                if last_sync.elapsed() >= interval {
                    writer.get_ref().sync_all()?;
                    *last_sync = Instant::now();
                }
            }
        }

        Ok(())
    }

    fn sync_segment_now(&self, writer: &mut BufWriter<File>) -> Result<()> {
        writer.flush()?;
        writer.get_ref().sync_all()?;
        *self.last_sync.lock() = Instant::now();
        Ok(())
    }
}

impl Wal for DiskWal {
    fn append_rows(&self, rows: &[Row]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

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
                segment.writer.write_all(&metric_name)?;

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

            self.maybe_sync_after_append(&mut segment.writer)?;
        }

        Ok(())
    }

    fn flush(&self) -> Result<()> {
        if let Some(ref mut segment) = *self.current_segment.lock() {
            self.sync_segment_now(&mut segment.writer)?;
        }
        Ok(())
    }

    fn punctuate(&self) -> Result<()> {
        // Flush current segment and create a new one
        let mut current = self.current_segment.lock();
        if let Some(ref mut segment) = *current {
            self.sync_segment_now(&mut segment.writer)?;
        }

        // Force creation of new segment on next write
        *current = None;
        Ok(())
    }

    fn remove_oldest(&self) -> Result<()> {
        let segments = self.list_segments()?;
        // Keep at least one segment so active/unflushed WAL data is never discarded.
        if segments.len() <= 1 {
            return Ok(());
        }

        if let Some(oldest) = segments.first() {
            fs::remove_file(oldest)?;
        }
        Ok(())
    }

    fn remove_all(&self) -> Result<()> {
        // Drop the currently open writer before removing files.
        let current = self.current_segment.lock().take();
        drop(current);

        let segments = self.list_segments()?;
        for segment in segments {
            fs::remove_file(segment)?;
        }

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

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file()
                && let Some(index) = path.file_name().and_then(parse_segment_index)
            {
                segments.push((index, path));
            }
        }

        // Sort by numeric value
        segments.sort_by_key(|(index, _)| *index);

        // Read each segment
        let mut failed_segments = 0usize;
        debug!(
            segments = segments.len(),
            wal_dir = %self.dir.display(),
            "Recovering WAL segments"
        );

        for (index, segment_path) in segments {
            debug!(
                segment_index = index,
                segment = %segment_path.display(),
                "Reading WAL segment"
            );
            match self.read_segment(&segment_path) {
                Ok(_) => {}
                Err(e) => {
                    // Track failed segments for potential recovery
                    warn!("Error reading WAL segment {:?}: {}", segment_path, e);
                    failed_segments += 1;
                }
            }
        }

        if failed_segments > 0 {
            warn!(
                failed_segments,
                recovered_rows = self.rows_to_insert.len(),
                "WAL recovery completed with segment failures"
            );
        }

        Ok(self.rows_to_insert)
    }

    /// Reads a single WAL segment.
    fn read_segment(&mut self, path: &Path) -> Result<()> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();

        // Skip truly empty files; short files can still contain valid varint-encoded records.
        if file_len == 0 {
            warn!("Skipping WAL segment {:?}: file is empty", path);
            return Ok(());
        }

        let mut reader = BufReader::new(file);
        let mut bytes_read = 0u64;

        let mut corrupted_entries = 0;
        const MAX_CORRUPTED: usize = 5;

        loop {
            // Track position for error recovery
            let entry_start = bytes_read;

            // Read operation type
            let mut op_buf = [0u8; 1];
            match reader.read_exact(&mut op_buf) {
                Ok(_) => bytes_read += 1,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    warn!("WAL read error at byte {}: {}", bytes_read, e);
                    corrupted_entries += 1;
                    if corrupted_entries > MAX_CORRUPTED {
                        return Err(TsinkError::Wal {
                            operation: "segment_read".to_string(),
                            details: format!("Too many corrupted entries in {:?}", path),
                        });
                    }
                    break;
                }
            }

            let op = match WalOperation::from_u8(op_buf[0]) {
                Some(op) => op,
                None => {
                    debug!(
                        wal_segment = %path.display(),
                        byte_offset = entry_start,
                        opcode = op_buf[0],
                        "Unknown WAL opcode encountered"
                    );
                    warn!(
                        "Unknown WAL operation {} at byte {}",
                        op_buf[0], entry_start
                    );
                    corrupted_entries += 1;
                    if corrupted_entries > MAX_CORRUPTED {
                        return Err(TsinkError::Wal {
                            operation: "segment_read".to_string(),
                            details: format!("Too many corrupted entries in {:?}", path),
                        });
                    }
                    // Try to skip to next valid entry
                    continue;
                }
            };

            match op {
                WalOperation::Insert => {
                    // Read metric name length
                    let metric_len_u64 = match decode_uvarint(&mut reader) {
                        Ok(len) => len,
                        Err(_) => break, // Incomplete record
                    };

                    if metric_len_u64 > usize::MAX as u64
                        || metric_len_u64 as usize > MAX_WAL_METRIC_NAME_BYTES
                    {
                        warn!(
                            "Invalid WAL metric length {} at byte {}",
                            metric_len_u64, entry_start
                        );
                        corrupted_entries += 1;
                        if corrupted_entries > MAX_CORRUPTED {
                            return Err(TsinkError::Wal {
                                operation: "segment_read".to_string(),
                                details: format!("Too many corrupted entries in {:?}", path),
                            });
                        }
                        break;
                    }
                    let metric_len = metric_len_u64 as usize;

                    // Read metric name
                    let mut metric_buf = vec![0u8; metric_len];
                    if reader.read_exact(&mut metric_buf).is_err() {
                        break; // Incomplete record
                    }
                    // Unmarshal metric name and labels
                    let (metric, labels) = unmarshal_metric_name(&metric_buf)?;

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
            // Accept legacy opcode 0 for backward compatibility with older WAL segments.
            0 | 1 => Some(WalOperation::Insert),
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
