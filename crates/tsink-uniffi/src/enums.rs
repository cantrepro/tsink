use std::time::Duration;

use crate::types::{UNativeHistogram, UWriteRejection};

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UValue {
    F64 { v: f64 },
    I64 { v: i64 },
    U64 { v: u64 },
    Bool { v: bool },
    Bytes { v: Vec<u8> },
    Str { v: String },
    Histogram { v: UNativeHistogram },
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UHistogramCount {
    Int { v: u64 },
    Float { v: f64 },
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UHistogramResetHint {
    Unknown,
    Yes,
    No,
    Gauge,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UAggregation {
    None,
    Sum,
    Min,
    Max,
    Avg,
    First,
    Last,
    Count,
    Median,
    Range,
    Variance,
    StdDev,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UTimestampPrecision {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UStorageRuntimeMode {
    ReadWrite,
    ComputeOnly,
}

#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum UResourceProfile {
    Test,
    Embedded,
    Edge,
    Server,
    ExpertUnlimited,
}

#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum UResourceProfileName {
    Unreported,
    Test,
    Embedded,
    Edge,
    Server,
    Custom,
    ExpertUnlimited,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UMemoryPressureLevel {
    Normal,
    ApproachingLimit,
    Backpressured,
    Rejecting,
    Degraded,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UDiskCategory {
    Wal,
    Segments,
    Registry,
    Tombstones,
    Rollups,
    Metadata,
    Exemplars,
    Cluster,
    EdgeSync,
    ServerState,
    Temporary,
    Unknown,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum URemoteSegmentCachePolicy {
    MetadataOnly,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum USeriesMatcherOp {
    Equal,
    NotEqual,
    RegexMatch,
    RegexNoMatch,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UWalSyncMode {
    PerAppend,
    Periodic { interval: Duration },
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UWalReplayMode {
    Strict,
    Salvage,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UWriteAcknowledgement {
    Volatile,
    Appended,
    Durable,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UWriteMode {
    Atomic,
    BestEffort,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UWriteRejectionCategory {
    InvalidMetric,
    InvalidLabels,
    UnsupportedValue,
    TimestampOutOfBounds,
    BelowRetentionFloor,
    FutureSkewExceeded,
    CardinalityLimitExceeded,
    CardinalityCreationRateExceeded,
    MemoryPressure,
    DiskQuotaExceeded,
    WalQuotaExceeded,
    PolicyRejected,
    WriteTimeout,
    StorageClosed,
    StorageDegraded,
    InternalIo,
    Internal,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum URowWriteStatus {
    Accepted,
    Rejected { rejection: UWriteRejection },
}
