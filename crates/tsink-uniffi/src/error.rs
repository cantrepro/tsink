use tsink::TsinkError;

#[derive(Debug, thiserror::Error, uniffi::Error)]
#[uniffi(flat_error)]
pub enum TsinkUniFFIError {
    #[error("NoDataPoints: {msg}")]
    NoDataPoints { msg: String },

    #[error("InvalidTimeRange: {msg}")]
    InvalidTimeRange { msg: String },

    #[error("StorageClosed: {msg}")]
    StorageClosed { msg: String },

    #[error("InvalidInput: {msg}")]
    InvalidInput { msg: String },

    #[error("IoError: {msg}")]
    IoError { msg: String },

    #[error("DataCorruption: {msg}")]
    DataCorruption { msg: String },

    #[error("ResourceExhausted: {msg}")]
    ResourceExhausted { msg: String },

    #[error("Other: {msg}")]
    Other { msg: String },
}

impl From<TsinkError> for TsinkUniFFIError {
    fn from(e: TsinkError) -> Self {
        let msg = e.to_string();
        match e {
            TsinkError::NoDataPoints { .. } => TsinkUniFFIError::NoDataPoints { msg },

            TsinkError::InvalidTimeRange { .. } => TsinkUniFFIError::InvalidTimeRange { msg },

            TsinkError::StorageClosed | TsinkError::StorageShuttingDown => {
                TsinkUniFFIError::StorageClosed { msg }
            }

            TsinkError::MetricRequired
            | TsinkError::InvalidMetricName(_)
            | TsinkError::InvalidLabel(_)
            | TsinkError::InvalidConfiguration(_)
            | TsinkError::InvalidPartition { .. }
            | TsinkError::InvalidOffset { .. }
            | TsinkError::UnsupportedAggregation { .. }
            | TsinkError::ValueTypeMismatch { .. }
            | TsinkError::OutOfRetention { .. } => TsinkUniFFIError::InvalidInput { msg },

            TsinkError::Io(_)
            | TsinkError::IoWithPath { .. }
            | TsinkError::ReadOnlyPartition { .. }
            | TsinkError::InsufficientDiskSpace { .. }
            | TsinkError::MemoryMap { .. } => TsinkUniFFIError::IoError { msg },

            TsinkError::DataCorruption(_)
            | TsinkError::ChecksumMismatch { .. }
            | TsinkError::Compression(_)
            | TsinkError::Json(_)
            | TsinkError::Bincode(_)
            | TsinkError::Utf8(_) => TsinkUniFFIError::DataCorruption { msg },

            TsinkError::MemoryBudgetExceeded { .. }
            | TsinkError::CardinalityLimitExceeded { .. }
            | TsinkError::WalSizeLimitExceeded { .. }
            | TsinkError::WriteTimeout { .. } => TsinkUniFFIError::ResourceExhausted { msg },

            TsinkError::PartitionNotFound { .. }
            | TsinkError::LockPoisoned { .. }
            | TsinkError::ChannelSend { .. }
            | TsinkError::ChannelReceive { .. }
            | TsinkError::ChannelTimeout { .. }
            | TsinkError::Wal { .. }
            | TsinkError::Codec(_)
            | TsinkError::Other(_) => TsinkUniFFIError::Other { msg },
        }
    }
}

pub type Result<T> = std::result::Result<T, TsinkUniFFIError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_mapping_no_data_points() {
        let e = TsinkError::NoDataPoints {
            metric: "cpu".into(),
            start: 0,
            end: 100,
        };
        let mapped: TsinkUniFFIError = e.into();
        assert!(matches!(mapped, TsinkUniFFIError::NoDataPoints { .. }));
    }

    #[test]
    fn test_error_mapping_invalid_time_range() {
        let e = TsinkError::InvalidTimeRange { start: 100, end: 0 };
        let mapped: TsinkUniFFIError = e.into();
        assert!(matches!(mapped, TsinkUniFFIError::InvalidTimeRange { .. }));
    }

    #[test]
    fn test_error_mapping_storage_closed() {
        let e = TsinkError::StorageClosed;
        let mapped: TsinkUniFFIError = e.into();
        assert!(matches!(mapped, TsinkUniFFIError::StorageClosed { .. }));
    }

    #[test]
    fn test_error_mapping_invalid_input() {
        let e = TsinkError::MetricRequired;
        let mapped: TsinkUniFFIError = e.into();
        assert!(matches!(mapped, TsinkUniFFIError::InvalidInput { .. }));
    }

    #[test]
    fn test_error_mapping_io() {
        let e = TsinkError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        let mapped: TsinkUniFFIError = e.into();
        assert!(matches!(mapped, TsinkUniFFIError::IoError { .. }));
    }

    #[test]
    fn test_error_mapping_resource_exhausted() {
        let e = TsinkError::MemoryBudgetExceeded {
            budget: 100,
            required: 200,
        };
        let mapped: TsinkUniFFIError = e.into();
        assert!(matches!(mapped, TsinkUniFFIError::ResourceExhausted { .. }));
    }

    #[test]
    fn test_error_mapping_data_corruption() {
        let e = TsinkError::DataCorruption("bad data".into());
        let mapped: TsinkUniFFIError = e.into();
        assert!(matches!(mapped, TsinkUniFFIError::DataCorruption { .. }));
    }

    #[test]
    fn test_error_mapping_other() {
        let e = TsinkError::Other("unknown".into());
        let mapped: TsinkUniFFIError = e.into();
        assert!(matches!(mapped, TsinkUniFFIError::Other { .. }));
    }
}
