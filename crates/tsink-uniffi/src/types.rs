use crate::enums::{USeriesMatcherOp, UValue};

#[derive(Debug, Clone, uniffi::Record)]
pub struct ULabel {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UDataPoint {
    pub value: UValue,
    pub timestamp: i64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct URow {
    pub metric: String,
    pub labels: Vec<ULabel>,
    pub data_point: UDataPoint,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UMetricSeries {
    pub name: String,
    pub labels: Vec<ULabel>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct UDownsampleOptions {
    pub interval: i64,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct USeriesMatcher {
    pub name: String,
    pub op: USeriesMatcherOp,
    pub value: String,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct ULabeledDataPoints {
    pub labels: Vec<ULabel>,
    pub data_points: Vec<UDataPoint>,
}
