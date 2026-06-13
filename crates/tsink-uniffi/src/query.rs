use crate::enums::UAggregation;
use crate::types::{UDownsampleOptions, ULabel, USeriesMatcher};

#[derive(Debug, Clone, uniffi::Record)]
pub struct UQueryOptions {
    pub labels: Vec<ULabel>,
    pub start: i64,
    pub end: i64,
    pub aggregation: UAggregation,
    pub downsample: Option<UDownsampleOptions>,
    pub limit: Option<u64>,
    pub offset: u64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct USeriesSelection {
    pub metric: Option<String>,
    pub matchers: Vec<USeriesMatcher>,
    pub start: Option<i64>,
    pub end: Option<i64>,
}
