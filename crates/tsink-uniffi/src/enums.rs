use std::time::Duration;

#[derive(Debug, Clone, uniffi::Enum)]
pub enum UValue {
    F64 { v: f64 },
    I64 { v: i64 },
    U64 { v: u64 },
    Bool { v: bool },
    Bytes { v: Vec<u8> },
    Str { v: String },
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
