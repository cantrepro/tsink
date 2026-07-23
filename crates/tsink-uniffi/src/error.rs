use tsink_core::TsinkError;

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

    #[error("Cancelled: {msg}")]
    Cancelled { msg: String },

    #[error("DeadlineExceeded: {msg}")]
    DeadlineExceeded { msg: String },

    #[error("Other: {msg}")]
    Other { msg: String },
}

impl From<TsinkError> for TsinkUniFFIError {
    fn from(e: TsinkError) -> Self {
        let msg = e.to_string();
        match e {
            TsinkError::QueryBudget(error) => match error {
                tsink_core::QueryBudgetError::InvalidLimits(_) => {
                    TsinkUniFFIError::InvalidInput { msg }
                }
                tsink_core::QueryBudgetError::LimitExceeded(_) => {
                    TsinkUniFFIError::ResourceExhausted { msg }
                }
                tsink_core::QueryBudgetError::Cancelled => TsinkUniFFIError::Cancelled { msg },
                tsink_core::QueryBudgetError::DeadlineExceeded => {
                    TsinkUniFFIError::DeadlineExceeded { msg }
                }
                _ => TsinkUniFFIError::Other { msg },
            },

            TsinkError::NoDataPoints { .. } => TsinkUniFFIError::NoDataPoints { msg },

            TsinkError::InvalidTimeRange { .. } => TsinkUniFFIError::InvalidTimeRange { msg },

            TsinkError::StorageClosed | TsinkError::StorageShuttingDown => {
                TsinkUniFFIError::StorageClosed { msg }
            }

            TsinkError::MetricRequired
            | TsinkError::InvalidMetricName(_)
            | TsinkError::InvalidLabel(_)
            | TsinkError::InvalidConfiguration(_)
            | TsinkError::UnsupportedOperation { .. }
            | TsinkError::InvalidPartition { .. }
            | TsinkError::InvalidOffset { .. }
            | TsinkError::UnsupportedAggregation { .. }
            | TsinkError::ValueTypeMismatch { .. }
            | TsinkError::OutOfRetention { .. }
            | TsinkError::FutureSkewExceeded { .. }
            | TsinkError::LateWritePartitionFanoutExceeded { .. }
            | TsinkError::MaintenanceWorkItemTooLarge { .. }
            | TsinkError::MaintenanceDependencyWindowExceeded { .. }
            | TsinkError::MaintenanceNamespaceLimitExceeded { .. } => {
                TsinkUniFFIError::InvalidInput { msg }
            }

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
            | TsinkError::AsyncQueueByteLimitExceeded { .. }
            | TsinkError::AsyncQueuePayloadSizeOverflow { .. }
            | TsinkError::WriteBatchRowLimitExceeded { .. }
            | TsinkError::WriteBatchInputLimitExceeded { .. }
            | TsinkError::WriteBatchSizeOverflow
            | TsinkError::CardinalityLimitExceeded { .. }
            | TsinkError::CardinalityCreationRateExceeded { .. }
            | TsinkError::WalSizeLimitExceeded { .. }
            | TsinkError::DiskQuotaExceeded { .. }
            | TsinkError::InsufficientCompactionHeadroom { .. }
            | TsinkError::WriteTimeout { .. }
            | TsinkError::LifecycleTimeout { .. } => TsinkUniFFIError::ResourceExhausted { msg },

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

#[cfg(test)]
mod query_control_tests {
    use super::*;

    #[test]
    fn query_control_errors_keep_stable_uniffi_categories() {
        let cancelled = TsinkUniFFIError::from(TsinkError::QueryBudget(
            tsink_core::QueryBudgetError::Cancelled,
        ));
        assert!(matches!(cancelled, TsinkUniFFIError::Cancelled { .. }));

        let deadline = TsinkUniFFIError::from(TsinkError::QueryBudget(
            tsink_core::QueryBudgetError::DeadlineExceeded,
        ));
        assert!(matches!(
            deadline,
            TsinkUniFFIError::DeadlineExceeded { .. }
        ));
    }

    #[test]
    fn maintenance_policy_errors_keep_stable_uniffi_category() {
        let oversized = TsinkUniFFIError::from(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "persist sealed chunks",
            limit: 1,
            required: 2,
        });
        assert!(matches!(oversized, TsinkUniFFIError::InvalidInput { .. }));

        let dependency_window =
            TsinkUniFFIError::from(TsinkError::MaintenanceDependencyWindowExceeded {
                operation: "persist sealed chunks",
                item_limit: 1,
                byte_limit: 2,
                selected_items: 1,
                selected_bytes: 2,
            });
        assert!(matches!(
            dependency_window,
            TsinkUniFFIError::InvalidInput { .. }
        ));

        let namespace = TsinkUniFFIError::from(TsinkError::MaintenanceNamespaceLimitExceeded {
            operation: "incremental series registry",
            limit: 2,
            required: 3,
        });
        assert!(matches!(namespace, TsinkUniFFIError::InvalidInput { .. }));
    }

    #[test]
    fn lifecycle_timeout_is_resource_exhaustion() {
        let error = TsinkUniFFIError::from(TsinkError::LifecycleTimeout {
            operation: "close compaction drain",
            timeout_ms: 25,
        });
        assert!(matches!(
            error,
            TsinkUniFFIError::ResourceExhausted { msg }
                if msg.contains("close compaction drain") && msg.contains("25ms")
        ));
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
    fn test_creation_rate_error_maps_to_resource_exhausted() {
        let e = TsinkError::CardinalityCreationRateExceeded {
            limit: 10,
            current: 10,
            requested: 1,
            window_nanos: 1_000_000_000,
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
