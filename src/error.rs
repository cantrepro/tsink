//! Error types for tsink.

use std::path::PathBuf;
use thiserror::Error;

/// Result type alias for tsink operations.
pub type Result<T> = std::result::Result<T, TsinkError>;

/// Main error type for tsink operations.
#[derive(Error, Debug)]
pub enum TsinkError {
    #[error("No data points found for metric '{metric}' in range [{start}, {end})")]
    NoDataPoints {
        metric: String,
        start: i64,
        end: i64,
    },

    #[error("Invalid timestamp range: start {start} >= end {end}")]
    InvalidTimeRange { start: i64, end: i64 },

    #[error("Metric name is required")]
    MetricRequired,

    #[error("Invalid metric name: {0}")]
    InvalidMetricName(String),

    #[error("Invalid label: {0}")]
    InvalidLabel(String),

    #[error("Partition not found for timestamp {timestamp}")]
    PartitionNotFound { timestamp: i64 },

    #[error("Invalid partition ID: {id}")]
    InvalidPartition { id: String },

    #[error("Cannot insert rows into read-only partition at {path:?}")]
    ReadOnlyPartition { path: PathBuf },

    #[error("Write timeout exceeded after {timeout_ms}ms with {workers} concurrent writers")]
    WriteTimeout { timeout_ms: u64, workers: usize },

    #[error("Storage is shutting down")]
    StorageShuttingDown,

    #[error("Storage already closed")]
    StorageClosed,

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Data corruption detected: {0}")]
    DataCorruption(String),

    #[error("Insufficient disk space: required {required} bytes, available {available} bytes")]
    InsufficientDiskSpace { required: u64, available: u64 },

    #[error("IO error at path {path:?}: {source}")]
    IoWithPath {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Bincode serialization error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("Lock poisoned for resource: {resource}")]
    LockPoisoned { resource: String },

    #[error("Channel send error for {channel}")]
    ChannelSend { channel: String },

    #[error("Channel receive error for {channel}")]
    ChannelReceive { channel: String },

    #[error("Channel timeout after {timeout_ms}ms")]
    ChannelTimeout { timeout_ms: u64 },

    #[error("Memory map error at {path:?}: {details}")]
    MemoryMap { path: PathBuf, details: String },

    #[error("Invalid offset {offset} exceeds maximum {max}")]
    InvalidOffset { offset: u64, max: u64 },

    #[error("WAL error: {operation} failed: {details}")]
    Wal { operation: String, details: String },

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Checksum mismatch: expected {expected:?}, got {actual:?}")]
    ChecksumMismatch { expected: Vec<u8>, actual: Vec<u8> },

    #[error("Data point with timestamp {timestamp} is outside the retention window")]
    OutOfRetention { timestamp: i64 },

    #[error("Other error: {0}")]
    Other(String),
}

impl<T> From<std::sync::PoisonError<T>> for TsinkError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        TsinkError::LockPoisoned {
            resource: format!("{:?}", err),
        }
    }
}

impl<T> From<crossbeam_channel::SendError<T>> for TsinkError {
    fn from(err: crossbeam_channel::SendError<T>) -> Self {
        TsinkError::ChannelSend {
            channel: format!("{:?}", err),
        }
    }
}

impl From<crossbeam_channel::RecvError> for TsinkError {
    fn from(err: crossbeam_channel::RecvError) -> Self {
        TsinkError::ChannelReceive {
            channel: format!("{:?}", err),
        }
    }
}

impl From<crossbeam_channel::RecvTimeoutError> for TsinkError {
    fn from(e: crossbeam_channel::RecvTimeoutError) -> Self {
        match e {
            crossbeam_channel::RecvTimeoutError::Timeout => TsinkError::ChannelReceive {
                channel: "recv_timeout: duration unavailable".to_string(),
            },
            crossbeam_channel::RecvTimeoutError::Disconnected => TsinkError::ChannelReceive {
                channel: "timeout: channel disconnected".to_string(),
            },
        }
    }
}
