use crate::enums::*;
use crate::query::*;
use crate::types::*;

impl From<ULabel> for tsink::Label {
    fn from(u: ULabel) -> Self {
        tsink::Label::new(u.name, u.value)
    }
}

impl From<tsink::Label> for ULabel {
    fn from(l: tsink::Label) -> Self {
        ULabel {
            name: l.name,
            value: l.value,
        }
    }
}

impl From<UValue> for tsink::Value {
    fn from(u: UValue) -> Self {
        match u {
            UValue::F64 { v } => tsink::Value::F64(v),
            UValue::I64 { v } => tsink::Value::I64(v),
            UValue::U64 { v } => tsink::Value::U64(v),
            UValue::Bool { v } => tsink::Value::Bool(v),
            UValue::Bytes { v } => tsink::Value::Bytes(v),
            UValue::Str { v } => tsink::Value::String(v),
        }
    }
}

impl From<tsink::Value> for UValue {
    fn from(v: tsink::Value) -> Self {
        match v {
            tsink::Value::F64(v) => UValue::F64 { v },
            tsink::Value::I64(v) => UValue::I64 { v },
            tsink::Value::U64(v) => UValue::U64 { v },
            tsink::Value::Bool(v) => UValue::Bool { v },
            tsink::Value::Bytes(v) => UValue::Bytes { v },
            tsink::Value::String(v) => UValue::Str { v },
        }
    }
}

impl From<UDataPoint> for tsink::DataPoint {
    fn from(u: UDataPoint) -> Self {
        let value: tsink::Value = u.value.into();
        tsink::DataPoint::new(u.timestamp, value)
    }
}

impl From<tsink::DataPoint> for UDataPoint {
    fn from(dp: tsink::DataPoint) -> Self {
        UDataPoint {
            value: dp.value.into(),
            timestamp: dp.timestamp,
        }
    }
}

impl From<URow> for tsink::Row {
    fn from(u: URow) -> Self {
        let labels: Vec<tsink::Label> = u.labels.into_iter().map(Into::into).collect();
        tsink::Row::with_labels(u.metric, labels, u.data_point.into())
    }
}

impl From<UMetricSeries> for tsink::MetricSeries {
    fn from(u: UMetricSeries) -> Self {
        tsink::MetricSeries {
            name: u.name,
            labels: u.labels.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<tsink::MetricSeries> for UMetricSeries {
    fn from(ms: tsink::MetricSeries) -> Self {
        UMetricSeries {
            name: ms.name,
            labels: ms.labels.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<UAggregation> for tsink::Aggregation {
    fn from(u: UAggregation) -> Self {
        match u {
            UAggregation::None => tsink::Aggregation::None,
            UAggregation::Sum => tsink::Aggregation::Sum,
            UAggregation::Min => tsink::Aggregation::Min,
            UAggregation::Max => tsink::Aggregation::Max,
            UAggregation::Avg => tsink::Aggregation::Avg,
            UAggregation::First => tsink::Aggregation::First,
            UAggregation::Last => tsink::Aggregation::Last,
            UAggregation::Count => tsink::Aggregation::Count,
            UAggregation::Median => tsink::Aggregation::Median,
            UAggregation::Range => tsink::Aggregation::Range,
            UAggregation::Variance => tsink::Aggregation::Variance,
            UAggregation::StdDev => tsink::Aggregation::StdDev,
        }
    }
}

impl From<tsink::Aggregation> for UAggregation {
    fn from(a: tsink::Aggregation) -> Self {
        match a {
            tsink::Aggregation::None => UAggregation::None,
            tsink::Aggregation::Sum => UAggregation::Sum,
            tsink::Aggregation::Min => UAggregation::Min,
            tsink::Aggregation::Max => UAggregation::Max,
            tsink::Aggregation::Avg => UAggregation::Avg,
            tsink::Aggregation::First => UAggregation::First,
            tsink::Aggregation::Last => UAggregation::Last,
            tsink::Aggregation::Count => UAggregation::Count,
            tsink::Aggregation::Median => UAggregation::Median,
            tsink::Aggregation::Range => UAggregation::Range,
            tsink::Aggregation::Variance => UAggregation::Variance,
            tsink::Aggregation::StdDev => UAggregation::StdDev,
        }
    }
}

impl From<UTimestampPrecision> for tsink::TimestampPrecision {
    fn from(u: UTimestampPrecision) -> Self {
        match u {
            UTimestampPrecision::Nanoseconds => tsink::TimestampPrecision::Nanoseconds,
            UTimestampPrecision::Microseconds => tsink::TimestampPrecision::Microseconds,
            UTimestampPrecision::Milliseconds => tsink::TimestampPrecision::Milliseconds,
            UTimestampPrecision::Seconds => tsink::TimestampPrecision::Seconds,
        }
    }
}

impl From<tsink::TimestampPrecision> for UTimestampPrecision {
    fn from(tp: tsink::TimestampPrecision) -> Self {
        match tp {
            tsink::TimestampPrecision::Nanoseconds => UTimestampPrecision::Nanoseconds,
            tsink::TimestampPrecision::Microseconds => UTimestampPrecision::Microseconds,
            tsink::TimestampPrecision::Milliseconds => UTimestampPrecision::Milliseconds,
            tsink::TimestampPrecision::Seconds => UTimestampPrecision::Seconds,
        }
    }
}

impl From<USeriesMatcherOp> for tsink::SeriesMatcherOp {
    fn from(u: USeriesMatcherOp) -> Self {
        match u {
            USeriesMatcherOp::Equal => tsink::SeriesMatcherOp::Equal,
            USeriesMatcherOp::NotEqual => tsink::SeriesMatcherOp::NotEqual,
            USeriesMatcherOp::RegexMatch => tsink::SeriesMatcherOp::RegexMatch,
            USeriesMatcherOp::RegexNoMatch => tsink::SeriesMatcherOp::RegexNoMatch,
        }
    }
}

impl From<tsink::SeriesMatcherOp> for USeriesMatcherOp {
    fn from(op: tsink::SeriesMatcherOp) -> Self {
        match op {
            tsink::SeriesMatcherOp::Equal => USeriesMatcherOp::Equal,
            tsink::SeriesMatcherOp::NotEqual => USeriesMatcherOp::NotEqual,
            tsink::SeriesMatcherOp::RegexMatch => USeriesMatcherOp::RegexMatch,
            tsink::SeriesMatcherOp::RegexNoMatch => USeriesMatcherOp::RegexNoMatch,
        }
    }
}

impl From<UWalSyncMode> for tsink::WalSyncMode {
    fn from(u: UWalSyncMode) -> Self {
        match u {
            UWalSyncMode::PerAppend => tsink::WalSyncMode::PerAppend,
            UWalSyncMode::Periodic { interval } => tsink::WalSyncMode::Periodic(interval),
        }
    }
}

impl From<tsink::WalSyncMode> for UWalSyncMode {
    fn from(m: tsink::WalSyncMode) -> Self {
        match m {
            tsink::WalSyncMode::PerAppend => UWalSyncMode::PerAppend,
            tsink::WalSyncMode::Periodic(interval) => UWalSyncMode::Periodic { interval },
        }
    }
}

impl From<USeriesMatcher> for tsink::SeriesMatcher {
    fn from(u: USeriesMatcher) -> Self {
        tsink::SeriesMatcher::new(u.name, u.op.into(), u.value)
    }
}

impl From<UDownsampleOptions> for tsink::DownsampleOptions {
    fn from(u: UDownsampleOptions) -> Self {
        tsink::DownsampleOptions {
            interval: u.interval,
        }
    }
}

impl From<UQueryOptions> for tsink::QueryOptions {
    fn from(u: UQueryOptions) -> Self {
        let mut opts = tsink::QueryOptions::new(u.start, u.end)
            .with_labels(u.labels.into_iter().map(Into::into).collect())
            .with_aggregation(u.aggregation.into())
            .with_pagination(u.offset as usize, u.limit.map(|l| l as usize));

        if let Some(ds) = u.downsample {
            opts.downsample = Some(ds.into());
        }

        opts
    }
}

impl From<USeriesSelection> for tsink::SeriesSelection {
    fn from(u: USeriesSelection) -> Self {
        let mut sel = tsink::SeriesSelection::new();

        if let Some(metric) = u.metric {
            sel = sel.with_metric(metric);
        }

        sel = sel.with_matchers(u.matchers.into_iter().map(Into::into).collect());

        if let (Some(start), Some(end)) = (u.start, u.end) {
            sel = sel.with_time_range(start, end);
        }

        sel
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_roundtrip() {
        let ulabel = ULabel {
            name: "host".into(),
            value: "server1".into(),
        };
        let label: tsink::Label = ulabel.clone().into();
        let back: ULabel = label.into();
        assert_eq!(ulabel.name, back.name);
        assert_eq!(ulabel.value, back.value);
    }

    #[test]
    fn test_value_f64_roundtrip() {
        let uval = UValue::F64 { v: 42.5 };
        let val: tsink::Value = uval.into();
        let back: UValue = val.into();
        match back {
            UValue::F64 { v } => assert_eq!(v, 42.5),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn test_value_string_roundtrip() {
        let uval = UValue::Str { v: "hello".into() };
        let val: tsink::Value = uval.into();
        let back: UValue = val.into();
        match back {
            UValue::Str { v } => assert_eq!(v, "hello"),
            _ => panic!("expected Str"),
        }
    }

    #[test]
    fn test_datapoint_roundtrip() {
        let udp = UDataPoint {
            value: UValue::I64 { v: 100 },
            timestamp: 1234567890,
        };
        let dp: tsink::DataPoint = udp.into();
        let back: UDataPoint = dp.into();
        assert_eq!(back.timestamp, 1234567890);
        match back.value {
            UValue::I64 { v } => assert_eq!(v, 100),
            _ => panic!("expected I64"),
        }
    }

    #[test]
    fn test_aggregation_roundtrip() {
        let cases = vec![
            UAggregation::None,
            UAggregation::Sum,
            UAggregation::Min,
            UAggregation::Max,
            UAggregation::Avg,
            UAggregation::Count,
            UAggregation::StdDev,
        ];
        for uagg in cases {
            let agg: tsink::Aggregation = uagg.clone().into();
            let _back: UAggregation = agg.into();
        }
    }

    #[test]
    fn test_query_options_conversion() {
        let uqo = UQueryOptions {
            labels: vec![ULabel {
                name: "env".into(),
                value: "prod".into(),
            }],
            start: 100,
            end: 200,
            aggregation: UAggregation::Avg,
            downsample: Some(UDownsampleOptions { interval: 60 }),
            limit: Some(10),
            offset: 5,
        };
        let qo: tsink::QueryOptions = uqo.into();
        assert_eq!(qo.start, 100);
        assert_eq!(qo.end, 200);
        assert_eq!(qo.labels.len(), 1);
        assert!(qo.downsample.is_some());
        assert_eq!(qo.limit, Some(10));
        assert_eq!(qo.offset, 5);
    }

    #[test]
    fn test_series_selection_conversion() {
        let usel = USeriesSelection {
            metric: Some("cpu".into()),
            matchers: vec![USeriesMatcher {
                name: "host".into(),
                op: USeriesMatcherOp::Equal,
                value: "server1".into(),
            }],
            start: Some(0),
            end: Some(1000),
        };
        let sel: tsink::SeriesSelection = usel.into();
        assert_eq!(sel.metric, Some("cpu".into()));
        assert_eq!(sel.matchers.len(), 1);
        assert_eq!(sel.start, Some(0));
        assert_eq!(sel.end, Some(1000));
    }
}
