use std::path::PathBuf;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, TsinkError>;

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

    #[error("Memory budget exceeded: budget {budget} bytes, required at least {required} bytes")]
    MemoryBudgetExceeded { budget: usize, required: usize },

    #[error(
        "Series cardinality limit exceeded: limit {limit}, current {current}, requested {requested}"
    )]
    CardinalityLimitExceeded {
        limit: usize,
        current: usize,
        requested: usize,
    },

    #[error("WAL size limit exceeded: limit {limit} bytes, required at least {required} bytes")]
    WalSizeLimitExceeded { limit: u64, required: u64 },

    #[error("Storage is shutting down")]
    StorageShuttingDown,

    #[error("Storage already closed")]
    StorageClosed,

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Unsupported operation '{operation}': {reason}")]
    UnsupportedOperation {
        operation: &'static str,
        reason: String,
    },

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

    #[error(
        "Late write at timestamp {timestamp} would open partition {partition_id} beyond the active-head limit {max_active_partition_heads_per_series} (active range {oldest_active_partition_id}..={newest_active_partition_id})"
    )]
    LateWritePartitionFanoutExceeded {
        timestamp: i64,
        partition_id: i64,
        max_active_partition_heads_per_series: usize,
        oldest_active_partition_id: i64,
        newest_active_partition_id: i64,
    },

    #[error("Unsupported aggregation '{aggregation}' for value type '{value_type}'")]
    UnsupportedAggregation {
        aggregation: String,
        value_type: String,
    },

    #[error("Value type mismatch: expected {expected}, found {actual}")]
    ValueTypeMismatch { expected: String, actual: String },

    #[error("Codec error: {0}")]
    Codec(String),

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
            crossbeam_channel::RecvTimeoutError::Timeout => {
                TsinkError::ChannelTimeout { timeout_ms: 0 }
            }
            crossbeam_channel::RecvTimeoutError::Disconnected => TsinkError::ChannelReceive {
                channel: "timeout: channel disconnected".to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TsinkError;
    use crossbeam_channel::RecvTimeoutError;
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn recv_timeout_error_timeout_maps_to_channel_timeout() {
        let err: TsinkError = RecvTimeoutError::Timeout.into();
        assert!(matches!(err, TsinkError::ChannelTimeout { timeout_ms: 0 }));
    }

    #[test]
    fn recv_timeout_error_disconnected_maps_to_channel_receive() {
        let err: TsinkError = RecvTimeoutError::Disconnected.into();
        assert!(matches!(err, TsinkError::ChannelReceive { .. }));
    }

    #[test]
    fn recv_error_maps_to_channel_receive() {
        let (tx, rx) = crossbeam_channel::bounded::<u8>(1);
        drop(tx);

        let err: TsinkError = rx.recv().unwrap_err().into();
        assert!(matches!(err, TsinkError::ChannelReceive { .. }));
    }

    #[test]
    fn send_error_maps_to_channel_send() {
        let (tx, rx) = crossbeam_channel::bounded::<u8>(1);
        drop(rx);

        let err: TsinkError = tx.send(1).unwrap_err().into();
        assert!(matches!(err, TsinkError::ChannelSend { .. }));
    }

    #[test]
    fn poison_error_maps_to_lock_poisoned() {
        let shared = Arc::new(Mutex::new(1usize));
        let shared_for_thread = Arc::clone(&shared);

        let _ = thread::spawn(move || {
            let _guard = shared_for_thread.lock().unwrap();
            panic!("intentional panic to poison mutex");
        })
        .join();

        let poison = shared.lock().unwrap_err();
        let err: TsinkError = poison.into();
        assert!(matches!(err, TsinkError::LockPoisoned { .. }));
    }
}
