use crate::enums::*;
use crate::query::*;
use crate::types::*;

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn u64_to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

impl From<ULabel> for tsink_core::Label {
    fn from(u: ULabel) -> Self {
        tsink_core::Label::new(u.name, u.value)
    }
}

impl From<tsink_core::Label> for ULabel {
    fn from(l: tsink_core::Label) -> Self {
        ULabel {
            name: l.name,
            value: l.value,
        }
    }
}

impl From<UValue> for tsink_core::Value {
    fn from(u: UValue) -> Self {
        match u {
            UValue::F64 { v } => tsink_core::Value::F64(v),
            UValue::I64 { v } => tsink_core::Value::I64(v),
            UValue::U64 { v } => tsink_core::Value::U64(v),
            UValue::Bool { v } => tsink_core::Value::Bool(v),
            UValue::Bytes { v } => tsink_core::Value::Bytes(v),
            UValue::Str { v } => tsink_core::Value::String(v),
            UValue::Histogram { v } => {
                tsink_core::Value::from(tsink_core::NativeHistogram::from(v))
            }
        }
    }
}

impl From<tsink_core::Value> for UValue {
    fn from(v: tsink_core::Value) -> Self {
        match v {
            tsink_core::Value::F64(v) => UValue::F64 { v },
            tsink_core::Value::I64(v) => UValue::I64 { v },
            tsink_core::Value::U64(v) => UValue::U64 { v },
            tsink_core::Value::Bool(v) => UValue::Bool { v },
            tsink_core::Value::Bytes(v) => UValue::Bytes { v },
            tsink_core::Value::String(v) => UValue::Str { v },
            tsink_core::Value::Histogram(v) => UValue::Histogram { v: (*v).into() },
        }
    }
}

impl From<UHistogramCount> for tsink_core::HistogramCount {
    fn from(u: UHistogramCount) -> Self {
        match u {
            UHistogramCount::Int { v } => tsink_core::HistogramCount::Int(v),
            UHistogramCount::Float { v } => tsink_core::HistogramCount::Float(v),
        }
    }
}

impl From<tsink_core::HistogramCount> for UHistogramCount {
    fn from(c: tsink_core::HistogramCount) -> Self {
        match c {
            tsink_core::HistogramCount::Int(v) => UHistogramCount::Int { v },
            tsink_core::HistogramCount::Float(v) => UHistogramCount::Float { v },
        }
    }
}

impl From<UHistogramBucketSpan> for tsink_core::HistogramBucketSpan {
    fn from(u: UHistogramBucketSpan) -> Self {
        tsink_core::HistogramBucketSpan {
            offset: u.offset,
            length: u.length,
        }
    }
}

impl From<tsink_core::HistogramBucketSpan> for UHistogramBucketSpan {
    fn from(span: tsink_core::HistogramBucketSpan) -> Self {
        UHistogramBucketSpan {
            offset: span.offset,
            length: span.length,
        }
    }
}

impl From<UHistogramResetHint> for tsink_core::HistogramResetHint {
    fn from(u: UHistogramResetHint) -> Self {
        match u {
            UHistogramResetHint::Unknown => tsink_core::HistogramResetHint::Unknown,
            UHistogramResetHint::Yes => tsink_core::HistogramResetHint::Yes,
            UHistogramResetHint::No => tsink_core::HistogramResetHint::No,
            UHistogramResetHint::Gauge => tsink_core::HistogramResetHint::Gauge,
        }
    }
}

impl From<tsink_core::HistogramResetHint> for UHistogramResetHint {
    fn from(hint: tsink_core::HistogramResetHint) -> Self {
        match hint {
            tsink_core::HistogramResetHint::Unknown => UHistogramResetHint::Unknown,
            tsink_core::HistogramResetHint::Yes => UHistogramResetHint::Yes,
            tsink_core::HistogramResetHint::No => UHistogramResetHint::No,
            tsink_core::HistogramResetHint::Gauge => UHistogramResetHint::Gauge,
        }
    }
}

impl From<UNativeHistogram> for tsink_core::NativeHistogram {
    fn from(u: UNativeHistogram) -> Self {
        tsink_core::NativeHistogram {
            count: u.count.map(Into::into),
            sum: u.sum,
            schema: u.schema,
            zero_threshold: u.zero_threshold,
            zero_count: u.zero_count.map(Into::into),
            negative_spans: u.negative_spans.into_iter().map(Into::into).collect(),
            negative_deltas: u.negative_deltas,
            negative_counts: u.negative_counts,
            positive_spans: u.positive_spans.into_iter().map(Into::into).collect(),
            positive_deltas: u.positive_deltas,
            positive_counts: u.positive_counts,
            reset_hint: u.reset_hint.into(),
            custom_values: u.custom_values,
        }
    }
}

impl From<tsink_core::NativeHistogram> for UNativeHistogram {
    fn from(histogram: tsink_core::NativeHistogram) -> Self {
        UNativeHistogram {
            count: histogram.count.map(Into::into),
            sum: histogram.sum,
            schema: histogram.schema,
            zero_threshold: histogram.zero_threshold,
            zero_count: histogram.zero_count.map(Into::into),
            negative_spans: histogram
                .negative_spans
                .into_iter()
                .map(Into::into)
                .collect(),
            negative_deltas: histogram.negative_deltas,
            negative_counts: histogram.negative_counts,
            positive_spans: histogram
                .positive_spans
                .into_iter()
                .map(Into::into)
                .collect(),
            positive_deltas: histogram.positive_deltas,
            positive_counts: histogram.positive_counts,
            reset_hint: histogram.reset_hint.into(),
            custom_values: histogram.custom_values,
        }
    }
}

impl From<UDataPoint> for tsink_core::DataPoint {
    fn from(u: UDataPoint) -> Self {
        let value: tsink_core::Value = u.value.into();
        tsink_core::DataPoint::new(u.timestamp, value)
    }
}

impl From<tsink_core::DataPoint> for UDataPoint {
    fn from(dp: tsink_core::DataPoint) -> Self {
        UDataPoint {
            value: dp.value.into(),
            timestamp: dp.timestamp,
        }
    }
}

impl From<URow> for tsink_core::Row {
    fn from(u: URow) -> Self {
        let labels: Vec<tsink_core::Label> = u.labels.into_iter().map(Into::into).collect();
        tsink_core::Row::with_labels(u.metric, labels, u.data_point.into())
    }
}

impl From<tsink_core::Row> for URow {
    fn from(row: tsink_core::Row) -> Self {
        URow {
            metric: row.metric().to_string(),
            labels: row.labels().iter().cloned().map(Into::into).collect(),
            data_point: row.data_point().clone().into(),
        }
    }
}

impl From<UMetricSeries> for tsink_core::MetricSeries {
    fn from(u: UMetricSeries) -> Self {
        tsink_core::MetricSeries {
            name: u.name,
            labels: u.labels.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<tsink_core::MetricSeries> for UMetricSeries {
    fn from(ms: tsink_core::MetricSeries) -> Self {
        UMetricSeries {
            name: ms.name,
            labels: ms.labels.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<tsink_core::SeriesPoints> for USeriesPoints {
    fn from(series_points: tsink_core::SeriesPoints) -> Self {
        USeriesPoints {
            series: series_points.series.into(),
            points: series_points.points.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<UMetadataShardScope> for tsink_core::MetadataShardScope {
    fn from(scope: UMetadataShardScope) -> Self {
        tsink_core::MetadataShardScope::new(scope.shard_count, scope.shards)
    }
}

impl From<tsink_core::MetadataShardScope> for UMetadataShardScope {
    fn from(scope: tsink_core::MetadataShardScope) -> Self {
        UMetadataShardScope {
            shard_count: scope.shard_count,
            shards: scope.shards,
        }
    }
}

impl From<tsink_core::ShardWindowDigest> for UShardWindowDigest {
    fn from(digest: tsink_core::ShardWindowDigest) -> Self {
        UShardWindowDigest {
            shard: digest.shard,
            shard_count: digest.shard_count,
            window_start: digest.window_start,
            window_end: digest.window_end,
            series_count: digest.series_count,
            point_count: digest.point_count,
            fingerprint: digest.fingerprint,
        }
    }
}

impl From<UShardWindowScanOptions> for tsink_core::ShardWindowScanOptions {
    fn from(options: UShardWindowScanOptions) -> Self {
        tsink_core::ShardWindowScanOptions {
            max_series: options.max_series.map(u64_to_usize),
            max_rows: options.max_rows.map(u64_to_usize),
            row_offset: options.row_offset,
        }
    }
}

impl From<tsink_core::ShardWindowRowsPage> for UShardWindowRowsPage {
    fn from(page: tsink_core::ShardWindowRowsPage) -> Self {
        UShardWindowRowsPage {
            shard: page.shard,
            shard_count: page.shard_count,
            window_start: page.window_start,
            window_end: page.window_end,
            series_scanned: page.series_scanned,
            rows_scanned: page.rows_scanned,
            truncated: page.truncated,
            next_row_offset: page.next_row_offset,
            rows: page.rows.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<UQueryRowsScanOptions> for tsink_core::QueryRowsScanOptions {
    fn from(options: UQueryRowsScanOptions) -> Self {
        tsink_core::QueryRowsScanOptions {
            max_rows: options.max_rows.map(u64_to_usize),
            row_offset: options.row_offset,
        }
    }
}

impl From<tsink_core::QueryRowsPage> for UQueryRowsPage {
    fn from(page: tsink_core::QueryRowsPage) -> Self {
        UQueryRowsPage {
            rows_scanned: page.rows_scanned,
            truncated: page.truncated,
            next_row_offset: page.next_row_offset,
            rows: page.rows.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<UAggregation> for tsink_core::Aggregation {
    fn from(u: UAggregation) -> Self {
        match u {
            UAggregation::None => tsink_core::Aggregation::None,
            UAggregation::Sum => tsink_core::Aggregation::Sum,
            UAggregation::Min => tsink_core::Aggregation::Min,
            UAggregation::Max => tsink_core::Aggregation::Max,
            UAggregation::Avg => tsink_core::Aggregation::Avg,
            UAggregation::First => tsink_core::Aggregation::First,
            UAggregation::Last => tsink_core::Aggregation::Last,
            UAggregation::Count => tsink_core::Aggregation::Count,
            UAggregation::Median => tsink_core::Aggregation::Median,
            UAggregation::Range => tsink_core::Aggregation::Range,
            UAggregation::Variance => tsink_core::Aggregation::Variance,
            UAggregation::StdDev => tsink_core::Aggregation::StdDev,
        }
    }
}

impl From<tsink_core::Aggregation> for UAggregation {
    fn from(a: tsink_core::Aggregation) -> Self {
        match a {
            tsink_core::Aggregation::None => UAggregation::None,
            tsink_core::Aggregation::Sum => UAggregation::Sum,
            tsink_core::Aggregation::Min => UAggregation::Min,
            tsink_core::Aggregation::Max => UAggregation::Max,
            tsink_core::Aggregation::Avg => UAggregation::Avg,
            tsink_core::Aggregation::First => UAggregation::First,
            tsink_core::Aggregation::Last => UAggregation::Last,
            tsink_core::Aggregation::Count => UAggregation::Count,
            tsink_core::Aggregation::Median => UAggregation::Median,
            tsink_core::Aggregation::Range => UAggregation::Range,
            tsink_core::Aggregation::Variance => UAggregation::Variance,
            tsink_core::Aggregation::StdDev => UAggregation::StdDev,
        }
    }
}

impl From<UTimestampPrecision> for tsink_core::TimestampPrecision {
    fn from(u: UTimestampPrecision) -> Self {
        match u {
            UTimestampPrecision::Nanoseconds => tsink_core::TimestampPrecision::Nanoseconds,
            UTimestampPrecision::Microseconds => tsink_core::TimestampPrecision::Microseconds,
            UTimestampPrecision::Milliseconds => tsink_core::TimestampPrecision::Milliseconds,
            UTimestampPrecision::Seconds => tsink_core::TimestampPrecision::Seconds,
        }
    }
}

impl From<tsink_core::TimestampPrecision> for UTimestampPrecision {
    fn from(tp: tsink_core::TimestampPrecision) -> Self {
        match tp {
            tsink_core::TimestampPrecision::Nanoseconds => UTimestampPrecision::Nanoseconds,
            tsink_core::TimestampPrecision::Microseconds => UTimestampPrecision::Microseconds,
            tsink_core::TimestampPrecision::Milliseconds => UTimestampPrecision::Milliseconds,
            tsink_core::TimestampPrecision::Seconds => UTimestampPrecision::Seconds,
        }
    }
}

impl From<UStorageRuntimeMode> for tsink_core::StorageRuntimeMode {
    fn from(mode: UStorageRuntimeMode) -> Self {
        match mode {
            UStorageRuntimeMode::ReadWrite => tsink_core::StorageRuntimeMode::ReadWrite,
            UStorageRuntimeMode::ComputeOnly => tsink_core::StorageRuntimeMode::ComputeOnly,
        }
    }
}

impl From<tsink_core::StorageRuntimeMode> for UStorageRuntimeMode {
    fn from(mode: tsink_core::StorageRuntimeMode) -> Self {
        match mode {
            tsink_core::StorageRuntimeMode::ReadWrite => UStorageRuntimeMode::ReadWrite,
            tsink_core::StorageRuntimeMode::ComputeOnly => UStorageRuntimeMode::ComputeOnly,
        }
    }
}

impl From<tsink_core::MemoryPressureLevel> for UMemoryPressureLevel {
    fn from(level: tsink_core::MemoryPressureLevel) -> Self {
        match level {
            tsink_core::MemoryPressureLevel::Normal => UMemoryPressureLevel::Normal,
            tsink_core::MemoryPressureLevel::ApproachingLimit => {
                UMemoryPressureLevel::ApproachingLimit
            }
            tsink_core::MemoryPressureLevel::Backpressured => UMemoryPressureLevel::Backpressured,
            tsink_core::MemoryPressureLevel::Rejecting => UMemoryPressureLevel::Rejecting,
            tsink_core::MemoryPressureLevel::Degraded => UMemoryPressureLevel::Degraded,
            _ => UMemoryPressureLevel::Degraded,
        }
    }
}

impl From<URemoteSegmentCachePolicy> for tsink_core::RemoteSegmentCachePolicy {
    fn from(policy: URemoteSegmentCachePolicy) -> Self {
        match policy {
            URemoteSegmentCachePolicy::MetadataOnly => {
                tsink_core::RemoteSegmentCachePolicy::MetadataOnly
            }
        }
    }
}

impl From<tsink_core::RemoteSegmentCachePolicy> for URemoteSegmentCachePolicy {
    fn from(policy: tsink_core::RemoteSegmentCachePolicy) -> Self {
        match policy {
            tsink_core::RemoteSegmentCachePolicy::MetadataOnly => {
                URemoteSegmentCachePolicy::MetadataOnly
            }
        }
    }
}

impl From<USeriesMatcherOp> for tsink_core::SeriesMatcherOp {
    fn from(u: USeriesMatcherOp) -> Self {
        match u {
            USeriesMatcherOp::Equal => tsink_core::SeriesMatcherOp::Equal,
            USeriesMatcherOp::NotEqual => tsink_core::SeriesMatcherOp::NotEqual,
            USeriesMatcherOp::RegexMatch => tsink_core::SeriesMatcherOp::RegexMatch,
            USeriesMatcherOp::RegexNoMatch => tsink_core::SeriesMatcherOp::RegexNoMatch,
        }
    }
}

impl From<tsink_core::SeriesMatcherOp> for USeriesMatcherOp {
    fn from(op: tsink_core::SeriesMatcherOp) -> Self {
        match op {
            tsink_core::SeriesMatcherOp::Equal => USeriesMatcherOp::Equal,
            tsink_core::SeriesMatcherOp::NotEqual => USeriesMatcherOp::NotEqual,
            tsink_core::SeriesMatcherOp::RegexMatch => USeriesMatcherOp::RegexMatch,
            tsink_core::SeriesMatcherOp::RegexNoMatch => USeriesMatcherOp::RegexNoMatch,
        }
    }
}

impl From<UWalSyncMode> for tsink_core::WalSyncMode {
    fn from(u: UWalSyncMode) -> Self {
        match u {
            UWalSyncMode::PerAppend => tsink_core::WalSyncMode::PerAppend,
            UWalSyncMode::Periodic { interval } => tsink_core::WalSyncMode::Periodic(interval),
        }
    }
}

impl From<tsink_core::WalSyncMode> for UWalSyncMode {
    fn from(mode: tsink_core::WalSyncMode) -> Self {
        match mode {
            tsink_core::WalSyncMode::PerAppend => UWalSyncMode::PerAppend,
            tsink_core::WalSyncMode::Periodic(interval) => UWalSyncMode::Periodic { interval },
        }
    }
}

impl From<UWalReplayMode> for tsink_core::WalReplayMode {
    fn from(mode: UWalReplayMode) -> Self {
        match mode {
            UWalReplayMode::Strict => tsink_core::WalReplayMode::Strict,
            UWalReplayMode::Salvage => tsink_core::WalReplayMode::Salvage,
        }
    }
}

impl From<tsink_core::WalReplayMode> for UWalReplayMode {
    fn from(mode: tsink_core::WalReplayMode) -> Self {
        match mode {
            tsink_core::WalReplayMode::Strict => UWalReplayMode::Strict,
            tsink_core::WalReplayMode::Salvage => UWalReplayMode::Salvage,
        }
    }
}

impl From<UWriteAcknowledgement> for tsink_core::WriteAcknowledgement {
    fn from(acknowledgement: UWriteAcknowledgement) -> Self {
        match acknowledgement {
            UWriteAcknowledgement::Volatile => tsink_core::WriteAcknowledgement::Volatile,
            UWriteAcknowledgement::Appended => tsink_core::WriteAcknowledgement::Appended,
            UWriteAcknowledgement::Durable => tsink_core::WriteAcknowledgement::Durable,
        }
    }
}

impl From<tsink_core::WriteAcknowledgement> for UWriteAcknowledgement {
    fn from(acknowledgement: tsink_core::WriteAcknowledgement) -> Self {
        match acknowledgement {
            tsink_core::WriteAcknowledgement::Volatile => UWriteAcknowledgement::Volatile,
            tsink_core::WriteAcknowledgement::Appended => UWriteAcknowledgement::Appended,
            tsink_core::WriteAcknowledgement::Durable => UWriteAcknowledgement::Durable,
        }
    }
}

impl From<UWriteMode> for tsink_core::WriteMode {
    fn from(mode: UWriteMode) -> Self {
        match mode {
            UWriteMode::Atomic => tsink_core::WriteMode::Atomic,
            UWriteMode::BestEffort => tsink_core::WriteMode::BestEffort,
        }
    }
}

impl From<UWriteRejectionCategory> for tsink_core::WriteRejectionCategory {
    fn from(category: UWriteRejectionCategory) -> Self {
        match category {
            UWriteRejectionCategory::InvalidMetric => {
                tsink_core::WriteRejectionCategory::InvalidMetric
            }
            UWriteRejectionCategory::InvalidLabels => {
                tsink_core::WriteRejectionCategory::InvalidLabels
            }
            UWriteRejectionCategory::UnsupportedValue => {
                tsink_core::WriteRejectionCategory::UnsupportedValue
            }
            UWriteRejectionCategory::TimestampOutOfBounds => {
                tsink_core::WriteRejectionCategory::TimestampOutOfBounds
            }
            UWriteRejectionCategory::BelowRetentionFloor => {
                tsink_core::WriteRejectionCategory::BelowRetentionFloor
            }
            UWriteRejectionCategory::FutureSkewExceeded => {
                tsink_core::WriteRejectionCategory::FutureSkewExceeded
            }
            UWriteRejectionCategory::CardinalityLimitExceeded => {
                tsink_core::WriteRejectionCategory::CardinalityLimitExceeded
            }
            UWriteRejectionCategory::CardinalityCreationRateExceeded => {
                tsink_core::WriteRejectionCategory::CardinalityCreationRateExceeded
            }
            UWriteRejectionCategory::MemoryPressure => {
                tsink_core::WriteRejectionCategory::MemoryPressure
            }
            UWriteRejectionCategory::DiskQuotaExceeded => {
                tsink_core::WriteRejectionCategory::DiskQuotaExceeded
            }
            UWriteRejectionCategory::WalQuotaExceeded => {
                tsink_core::WriteRejectionCategory::WalQuotaExceeded
            }
            UWriteRejectionCategory::PolicyRejected => {
                tsink_core::WriteRejectionCategory::PolicyRejected
            }
            UWriteRejectionCategory::WriteTimeout => {
                tsink_core::WriteRejectionCategory::WriteTimeout
            }
            UWriteRejectionCategory::StorageClosed => {
                tsink_core::WriteRejectionCategory::StorageClosed
            }
            UWriteRejectionCategory::StorageDegraded => {
                tsink_core::WriteRejectionCategory::StorageDegraded
            }
            UWriteRejectionCategory::InternalIo => tsink_core::WriteRejectionCategory::InternalIo,
            UWriteRejectionCategory::Internal => tsink_core::WriteRejectionCategory::Internal,
        }
    }
}

impl From<tsink_core::WriteRejectionCategory> for UWriteRejectionCategory {
    fn from(category: tsink_core::WriteRejectionCategory) -> Self {
        match category {
            tsink_core::WriteRejectionCategory::InvalidMetric => {
                UWriteRejectionCategory::InvalidMetric
            }
            tsink_core::WriteRejectionCategory::InvalidLabels => {
                UWriteRejectionCategory::InvalidLabels
            }
            tsink_core::WriteRejectionCategory::UnsupportedValue => {
                UWriteRejectionCategory::UnsupportedValue
            }
            tsink_core::WriteRejectionCategory::TimestampOutOfBounds => {
                UWriteRejectionCategory::TimestampOutOfBounds
            }
            tsink_core::WriteRejectionCategory::BelowRetentionFloor => {
                UWriteRejectionCategory::BelowRetentionFloor
            }
            tsink_core::WriteRejectionCategory::FutureSkewExceeded => {
                UWriteRejectionCategory::FutureSkewExceeded
            }
            tsink_core::WriteRejectionCategory::CardinalityLimitExceeded => {
                UWriteRejectionCategory::CardinalityLimitExceeded
            }
            tsink_core::WriteRejectionCategory::CardinalityCreationRateExceeded => {
                UWriteRejectionCategory::CardinalityCreationRateExceeded
            }
            tsink_core::WriteRejectionCategory::MemoryPressure => {
                UWriteRejectionCategory::MemoryPressure
            }
            tsink_core::WriteRejectionCategory::DiskQuotaExceeded => {
                UWriteRejectionCategory::DiskQuotaExceeded
            }
            tsink_core::WriteRejectionCategory::WalQuotaExceeded => {
                UWriteRejectionCategory::WalQuotaExceeded
            }
            tsink_core::WriteRejectionCategory::PolicyRejected => {
                UWriteRejectionCategory::PolicyRejected
            }
            tsink_core::WriteRejectionCategory::WriteTimeout => {
                UWriteRejectionCategory::WriteTimeout
            }
            tsink_core::WriteRejectionCategory::StorageClosed => {
                UWriteRejectionCategory::StorageClosed
            }
            tsink_core::WriteRejectionCategory::StorageDegraded => {
                UWriteRejectionCategory::StorageDegraded
            }
            tsink_core::WriteRejectionCategory::InternalIo => UWriteRejectionCategory::InternalIo,
            tsink_core::WriteRejectionCategory::Internal => UWriteRejectionCategory::Internal,
            _ => UWriteRejectionCategory::Internal,
        }
    }
}

impl From<tsink_core::WriteRejection> for UWriteRejection {
    fn from(rejection: tsink_core::WriteRejection) -> Self {
        UWriteRejection {
            category: rejection.category.into(),
            cause_index: rejection.cause_index.map(usize_to_u64),
            message: rejection.message,
        }
    }
}

impl From<tsink_core::RowWriteStatus> for URowWriteStatus {
    fn from(status: tsink_core::RowWriteStatus) -> Self {
        match status {
            tsink_core::RowWriteStatus::Accepted => URowWriteStatus::Accepted,
            tsink_core::RowWriteStatus::Rejected(rejection) => URowWriteStatus::Rejected {
                rejection: rejection.into(),
            },
            _ => URowWriteStatus::Rejected {
                rejection: UWriteRejection {
                    category: UWriteRejectionCategory::Internal,
                    cause_index: None,
                    message: "unrecognized row write status".to_string(),
                },
            },
        }
    }
}

impl From<tsink_core::RowWriteOutcome> for URowWriteOutcome {
    fn from(outcome: tsink_core::RowWriteOutcome) -> Self {
        URowWriteOutcome {
            index: usize_to_u64(outcome.index),
            status: outcome.status.into(),
        }
    }
}

impl From<tsink_core::BatchWriteResult> for UBatchWriteResult {
    fn from(result: tsink_core::BatchWriteResult) -> Self {
        UBatchWriteResult {
            submitted: usize_to_u64(result.submitted),
            accepted: usize_to_u64(result.accepted),
            rejected: usize_to_u64(result.rejected),
            acknowledgement: result.acknowledgement.map(Into::into),
            outcomes: result.outcomes.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<tsink_core::WriteResult> for UWriteResult {
    fn from(result: tsink_core::WriteResult) -> Self {
        UWriteResult {
            acknowledgement: result.acknowledgement.into(),
        }
    }
}

impl From<tsink_core::DeleteSeriesResult> for UDeleteSeriesResult {
    fn from(result: tsink_core::DeleteSeriesResult) -> Self {
        UDeleteSeriesResult {
            matched_series: result.matched_series,
            tombstones_applied: result.tombstones_applied,
        }
    }
}

impl From<tsink_core::EffectiveStorageLimits> for UEffectiveStorageLimits {
    fn from(limits: tsink_core::EffectiveStorageLimits) -> Self {
        UEffectiveStorageLimits {
            reported_by_backend: limits.reported_by_backend,
            persistent: limits.persistent,
            wal_enabled: limits.wal_enabled,
            accounted_memory_bytes: limits.accounted_memory_bytes,
            cardinality: limits.cardinality,
            wal_bytes: limits.wal_bytes,
            local_disk_bytes: limits.local_disk_bytes,
            filesystem_free_headroom_bytes: limits.filesystem_free_headroom_bytes,
            maintenance_temp_reserve_bytes: limits.maintenance_temp_reserve_bytes,
            max_concurrent_writers: limits.max_concurrent_writers,
            write_timeout_nanos: limits.write_timeout_nanos,
            max_active_partition_heads_per_series: limits.max_active_partition_heads_per_series,
        }
    }
}

impl From<tsink_core::DiskCategory> for UDiskCategory {
    fn from(category: tsink_core::DiskCategory) -> Self {
        match category {
            tsink_core::DiskCategory::Wal => UDiskCategory::Wal,
            tsink_core::DiskCategory::Segments => UDiskCategory::Segments,
            tsink_core::DiskCategory::Registry => UDiskCategory::Registry,
            tsink_core::DiskCategory::Tombstones => UDiskCategory::Tombstones,
            tsink_core::DiskCategory::Rollups => UDiskCategory::Rollups,
            tsink_core::DiskCategory::Metadata => UDiskCategory::Metadata,
            tsink_core::DiskCategory::Exemplars => UDiskCategory::Exemplars,
            tsink_core::DiskCategory::Cluster => UDiskCategory::Cluster,
            tsink_core::DiskCategory::EdgeSync => UDiskCategory::EdgeSync,
            tsink_core::DiskCategory::ServerState => UDiskCategory::ServerState,
            tsink_core::DiskCategory::Temporary => UDiskCategory::Temporary,
            tsink_core::DiskCategory::Unknown => UDiskCategory::Unknown,
            _ => UDiskCategory::Unknown,
        }
    }
}

impl From<tsink_core::LocalDiskLimits> for ULocalDiskLimits {
    fn from(limits: tsink_core::LocalDiskLimits) -> Self {
        Self {
            max_bytes: limits.max_bytes,
            filesystem_free_headroom_bytes: limits.filesystem_free_headroom_bytes,
            maintenance_temp_reserve_bytes: limits.maintenance_temp_reserve_bytes,
        }
    }
}

impl From<tsink_core::DiskCategoryUsage> for UDiskCategoryUsage {
    fn from(usage: tsink_core::DiskCategoryUsage) -> Self {
        Self {
            category: usage.category.into(),
            bytes: usage.bytes,
        }
    }
}

impl From<tsink_core::LocalDiskBudgetSnapshot> for ULocalDiskBudgetSnapshot {
    fn from(snapshot: tsink_core::LocalDiskBudgetSnapshot) -> Self {
        Self {
            limits: snapshot.limits.into(),
            accounted_bytes: snapshot.accounted_bytes,
            reserved_bytes: snapshot.reserved_bytes,
            maintenance_reserved_bytes: snapshot.maintenance_reserved_bytes,
            unknown_bytes: snapshot.unknown_bytes,
            filesystem_available_bytes: snapshot.filesystem_available_bytes,
            over_limit: snapshot.over_limit,
            active_reservations: snapshot.active_reservations,
            rejections_total: snapshot.rejections_total,
            reconciliations_total: snapshot.reconciliations_total,
            reservation_overruns_total: snapshot.reservation_overruns_total,
            categories: snapshot.categories.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<tsink_core::MemoryObservabilitySnapshot> for UMemoryObservabilitySnapshot {
    fn from(snapshot: tsink_core::MemoryObservabilitySnapshot) -> Self {
        UMemoryObservabilitySnapshot {
            accounted_bytes: usize_to_u64(snapshot.accounted_bytes),
            estimated_accounted_bytes: usize_to_u64(snapshot.estimated_accounted_bytes),
            budgeted_bytes: usize_to_u64(snapshot.budgeted_bytes),
            excluded_bytes: usize_to_u64(snapshot.excluded_bytes),
            excluded_bytes_known: snapshot.excluded_bytes_known,
            excluded_categories: snapshot.excluded_categories,
            active_and_sealed_bytes: usize_to_u64(snapshot.active_and_sealed_bytes),
            registry_bytes: usize_to_u64(snapshot.registry_bytes),
            metadata_cache_bytes: usize_to_u64(snapshot.metadata_cache_bytes),
            persisted_index_bytes: usize_to_u64(snapshot.persisted_index_bytes),
            persisted_mmap_bytes: usize_to_u64(snapshot.persisted_mmap_bytes),
            tombstone_bytes: usize_to_u64(snapshot.tombstone_bytes),
            excluded_persisted_mmap_bytes: usize_to_u64(snapshot.excluded_persisted_mmap_bytes),
            pressure: UMemoryPressureSnapshot {
                level: snapshot.pressure.level.map(Into::into),
                approaching_limit_basis_points: snapshot.pressure.approaching_limit_basis_points,
                approaching_limit_bytes: snapshot.pressure.approaching_limit_bytes,
                active_backpressured_writers: snapshot.pressure.active_backpressured_writers,
                backpressure_events_total: snapshot.pressure.backpressure_events_total,
                rejections_total: snapshot.pressure.rejections_total,
            },
        }
    }
}

impl From<tsink_core::WalObservabilitySnapshot> for UWalObservabilitySnapshot {
    fn from(snapshot: tsink_core::WalObservabilitySnapshot) -> Self {
        UWalObservabilitySnapshot {
            enabled: snapshot.enabled,
            sync_mode: snapshot.sync_mode,
            acknowledged_writes_durable: snapshot.acknowledged_writes_durable,
            size_bytes: snapshot.size_bytes,
            segment_count: snapshot.segment_count,
            active_segment: snapshot.active_segment,
            highwater_segment: snapshot.highwater_segment,
            highwater_frame: snapshot.highwater_frame,
            durable_highwater_segment: snapshot.durable_highwater_segment,
            durable_highwater_frame: snapshot.durable_highwater_frame,
            replay_runs_total: snapshot.replay_runs_total,
            replay_frames_total: snapshot.replay_frames_total,
            replay_series_definitions_total: snapshot.replay_series_definitions_total,
            replay_sample_batches_total: snapshot.replay_sample_batches_total,
            replay_points_total: snapshot.replay_points_total,
            replay_errors_total: snapshot.replay_errors_total,
            replay_duration_nanos_total: snapshot.replay_duration_nanos_total,
            append_series_definitions_total: snapshot.append_series_definitions_total,
            append_sample_batches_total: snapshot.append_sample_batches_total,
            append_points_total: snapshot.append_points_total,
            append_bytes_total: snapshot.append_bytes_total,
            append_errors_total: snapshot.append_errors_total,
            resets_total: snapshot.resets_total,
            reset_errors_total: snapshot.reset_errors_total,
        }
    }
}

impl From<tsink_core::RetentionObservabilitySnapshot> for URetentionObservabilitySnapshot {
    fn from(snapshot: tsink_core::RetentionObservabilitySnapshot) -> Self {
        URetentionObservabilitySnapshot {
            max_observed_timestamp: snapshot.max_observed_timestamp,
            recency_reference_timestamp: snapshot.recency_reference_timestamp,
            future_skew_window: snapshot.future_skew_window,
            future_skew_points_total: snapshot.future_skew_points_total,
            future_skew_max_timestamp: snapshot.future_skew_max_timestamp,
        }
    }
}

impl From<tsink_core::FlushObservabilitySnapshot> for UFlushObservabilitySnapshot {
    fn from(snapshot: tsink_core::FlushObservabilitySnapshot) -> Self {
        UFlushObservabilitySnapshot {
            pipeline_runs_total: snapshot.pipeline_runs_total,
            pipeline_success_total: snapshot.pipeline_success_total,
            pipeline_timeout_total: snapshot.pipeline_timeout_total,
            pipeline_errors_total: snapshot.pipeline_errors_total,
            pipeline_duration_nanos_total: snapshot.pipeline_duration_nanos_total,
            admission_backpressure_delays_total: snapshot.admission_backpressure_delays_total,
            admission_backpressure_delay_nanos_total: snapshot
                .admission_backpressure_delay_nanos_total,
            admission_pressure_relief_requests_total: snapshot
                .admission_pressure_relief_requests_total,
            admission_pressure_relief_observed_total: snapshot
                .admission_pressure_relief_observed_total,
            active_flush_runs_total: snapshot.active_flush_runs_total,
            active_flush_errors_total: snapshot.active_flush_errors_total,
            active_flushed_series_total: snapshot.active_flushed_series_total,
            active_flushed_chunks_total: snapshot.active_flushed_chunks_total,
            active_flushed_points_total: snapshot.active_flushed_points_total,
            persist_runs_total: snapshot.persist_runs_total,
            persist_success_total: snapshot.persist_success_total,
            persist_noop_total: snapshot.persist_noop_total,
            persist_errors_total: snapshot.persist_errors_total,
            persisted_series_total: snapshot.persisted_series_total,
            persisted_chunks_total: snapshot.persisted_chunks_total,
            persisted_points_total: snapshot.persisted_points_total,
            persisted_segments_total: snapshot.persisted_segments_total,
            persist_duration_nanos_total: snapshot.persist_duration_nanos_total,
            evicted_sealed_chunks_total: snapshot.evicted_sealed_chunks_total,
            tier_moves_total: snapshot.tier_moves_total,
            tier_move_errors_total: snapshot.tier_move_errors_total,
            expired_segments_total: snapshot.expired_segments_total,
            hot_segments_visible: snapshot.hot_segments_visible,
            warm_segments_visible: snapshot.warm_segments_visible,
            cold_segments_visible: snapshot.cold_segments_visible,
        }
    }
}

impl From<tsink_core::CompactionObservabilitySnapshot> for UCompactionObservabilitySnapshot {
    fn from(snapshot: tsink_core::CompactionObservabilitySnapshot) -> Self {
        UCompactionObservabilitySnapshot {
            runs_total: snapshot.runs_total,
            success_total: snapshot.success_total,
            noop_total: snapshot.noop_total,
            errors_total: snapshot.errors_total,
            source_segments_total: snapshot.source_segments_total,
            output_segments_total: snapshot.output_segments_total,
            source_chunks_total: snapshot.source_chunks_total,
            output_chunks_total: snapshot.output_chunks_total,
            source_points_total: snapshot.source_points_total,
            output_points_total: snapshot.output_points_total,
            duration_nanos_total: snapshot.duration_nanos_total,
        }
    }
}

impl From<tsink_core::QueryObservabilitySnapshot> for UQueryObservabilitySnapshot {
    fn from(snapshot: tsink_core::QueryObservabilitySnapshot) -> Self {
        UQueryObservabilitySnapshot {
            select_calls_total: snapshot.select_calls_total,
            select_errors_total: snapshot.select_errors_total,
            select_duration_nanos_total: snapshot.select_duration_nanos_total,
            select_points_returned_total: snapshot.select_points_returned_total,
            select_with_options_calls_total: snapshot.select_with_options_calls_total,
            select_with_options_errors_total: snapshot.select_with_options_errors_total,
            select_with_options_duration_nanos_total: snapshot
                .select_with_options_duration_nanos_total,
            select_with_options_points_returned_total: snapshot
                .select_with_options_points_returned_total,
            select_all_calls_total: snapshot.select_all_calls_total,
            select_all_errors_total: snapshot.select_all_errors_total,
            select_all_duration_nanos_total: snapshot.select_all_duration_nanos_total,
            select_all_series_returned_total: snapshot.select_all_series_returned_total,
            select_all_points_returned_total: snapshot.select_all_points_returned_total,
            select_series_calls_total: snapshot.select_series_calls_total,
            select_series_errors_total: snapshot.select_series_errors_total,
            select_series_duration_nanos_total: snapshot.select_series_duration_nanos_total,
            select_series_returned_total: snapshot.select_series_returned_total,
            merge_path_queries_total: snapshot.merge_path_queries_total,
            merge_path_shard_snapshots_total: snapshot.merge_path_shard_snapshots_total,
            merge_path_shard_snapshot_wait_nanos_total: snapshot
                .merge_path_shard_snapshot_wait_nanos_total,
            merge_path_shard_snapshot_hold_nanos_total: snapshot
                .merge_path_shard_snapshot_hold_nanos_total,
            append_sort_path_queries_total: snapshot.append_sort_path_queries_total,
            hot_only_query_plans_total: snapshot.hot_only_query_plans_total,
            warm_tier_query_plans_total: snapshot.warm_tier_query_plans_total,
            cold_tier_query_plans_total: snapshot.cold_tier_query_plans_total,
            hot_tier_persisted_chunks_read_total: snapshot.hot_tier_persisted_chunks_read_total,
            warm_tier_persisted_chunks_read_total: snapshot.warm_tier_persisted_chunks_read_total,
            cold_tier_persisted_chunks_read_total: snapshot.cold_tier_persisted_chunks_read_total,
            warm_tier_fetch_duration_nanos_total: snapshot.warm_tier_fetch_duration_nanos_total,
            cold_tier_fetch_duration_nanos_total: snapshot.cold_tier_fetch_duration_nanos_total,
            rollup_query_plans_total: snapshot.rollup_query_plans_total,
            partial_rollup_query_plans_total: snapshot.partial_rollup_query_plans_total,
            rollup_points_read_total: snapshot.rollup_points_read_total,
        }
    }
}

impl From<URollupPolicy> for tsink_core::RollupPolicy {
    fn from(policy: URollupPolicy) -> Self {
        tsink_core::RollupPolicy {
            id: policy.id,
            metric: policy.metric,
            match_labels: policy.match_labels.into_iter().map(Into::into).collect(),
            interval: policy.interval,
            aggregation: policy.aggregation.into(),
            bucket_origin: policy.bucket_origin,
        }
    }
}

impl From<tsink_core::RollupPolicy> for URollupPolicy {
    fn from(policy: tsink_core::RollupPolicy) -> Self {
        URollupPolicy {
            id: policy.id,
            metric: policy.metric,
            match_labels: policy.match_labels.into_iter().map(Into::into).collect(),
            interval: policy.interval,
            aggregation: policy.aggregation.into(),
            bucket_origin: policy.bucket_origin,
        }
    }
}

impl From<tsink_core::RollupPolicyStatus> for URollupPolicyStatus {
    fn from(status: tsink_core::RollupPolicyStatus) -> Self {
        URollupPolicyStatus {
            policy: status.policy.into(),
            matched_series: status.matched_series,
            materialized_series: status.materialized_series,
            materialized_through: status.materialized_through,
            lag: status.lag,
            last_run_started_at_ms: status.last_run_started_at_ms,
            last_run_completed_at_ms: status.last_run_completed_at_ms,
            last_run_duration_nanos: status.last_run_duration_nanos,
            last_error: status.last_error,
        }
    }
}

impl From<tsink_core::RollupObservabilitySnapshot> for URollupObservabilitySnapshot {
    fn from(snapshot: tsink_core::RollupObservabilitySnapshot) -> Self {
        URollupObservabilitySnapshot {
            worker_runs_total: snapshot.worker_runs_total,
            worker_success_total: snapshot.worker_success_total,
            worker_errors_total: snapshot.worker_errors_total,
            policy_runs_total: snapshot.policy_runs_total,
            buckets_materialized_total: snapshot.buckets_materialized_total,
            points_materialized_total: snapshot.points_materialized_total,
            last_run_duration_nanos: snapshot.last_run_duration_nanos,
            policies: snapshot.policies.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<tsink_core::RemoteStorageObservabilitySnapshot> for URemoteStorageObservabilitySnapshot {
    fn from(snapshot: tsink_core::RemoteStorageObservabilitySnapshot) -> Self {
        URemoteStorageObservabilitySnapshot {
            enabled: snapshot.enabled,
            runtime_mode: snapshot.runtime_mode.into(),
            cache_policy: snapshot.cache_policy.into(),
            metadata_refresh_interval_ms: snapshot.metadata_refresh_interval_ms,
            mirror_hot_segments: snapshot.mirror_hot_segments,
            catalog_refreshes_total: snapshot.catalog_refreshes_total,
            catalog_refresh_errors_total: snapshot.catalog_refresh_errors_total,
            accessible: snapshot.accessible,
            last_refresh_attempt_unix_ms: snapshot.last_refresh_attempt_unix_ms,
            last_successful_refresh_unix_ms: snapshot.last_successful_refresh_unix_ms,
            consecutive_refresh_failures: snapshot.consecutive_refresh_failures,
            next_refresh_retry_unix_ms: snapshot.next_refresh_retry_unix_ms,
            backoff_active: snapshot.backoff_active,
            last_refresh_error: snapshot.last_refresh_error,
        }
    }
}

impl From<tsink_core::storage::StorageHealthSnapshot> for UStorageHealthSnapshot {
    fn from(snapshot: tsink_core::storage::StorageHealthSnapshot) -> Self {
        UStorageHealthSnapshot {
            background_errors_total: snapshot.background_errors_total,
            maintenance_errors_total: snapshot.maintenance_errors_total,
            degraded: snapshot.degraded,
            fail_fast_enabled: snapshot.fail_fast_enabled,
            fail_fast_triggered: snapshot.fail_fast_triggered,
            last_background_error: snapshot.last_background_error,
            last_maintenance_error: snapshot.last_maintenance_error,
        }
    }
}

impl From<tsink_core::StorageObservabilitySnapshot> for UStorageObservabilitySnapshot {
    fn from(snapshot: tsink_core::StorageObservabilitySnapshot) -> Self {
        UStorageObservabilitySnapshot {
            limits: snapshot.limits.into(),
            local_disk: snapshot.local_disk.map(Into::into),
            memory: snapshot.memory.into(),
            wal: snapshot.wal.into(),
            retention: snapshot.retention.into(),
            flush: snapshot.flush.into(),
            compaction: snapshot.compaction.into(),
            query: snapshot.query.into(),
            rollups: snapshot.rollups.into(),
            remote: snapshot.remote.into(),
            health: snapshot.health.into(),
        }
    }
}

impl From<USeriesMatcher> for tsink_core::SeriesMatcher {
    fn from(u: USeriesMatcher) -> Self {
        tsink_core::SeriesMatcher::new(u.name, u.op.into(), u.value)
    }
}

impl From<UDownsampleOptions> for tsink_core::DownsampleOptions {
    fn from(u: UDownsampleOptions) -> Self {
        tsink_core::DownsampleOptions {
            interval: u.interval,
        }
    }
}

impl From<UQueryOptions> for tsink_core::QueryOptions {
    fn from(u: UQueryOptions) -> Self {
        let mut opts = tsink_core::QueryOptions::new(u.start, u.end)
            .with_labels(u.labels.into_iter().map(Into::into).collect())
            .with_aggregation(u.aggregation.into())
            .with_pagination(u64_to_usize(u.offset), u.limit.map(u64_to_usize));

        if let Some(ds) = u.downsample {
            opts.downsample = Some(ds.into());
        }

        opts
    }
}

impl From<USeriesSelection> for tsink_core::SeriesSelection {
    fn from(u: USeriesSelection) -> Self {
        tsink_core::SeriesSelection {
            metric: u.metric,
            matchers: u.matchers.into_iter().map(Into::into).collect(),
            start: u.start,
            end: u.end,
        }
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
        let label: tsink_core::Label = ulabel.clone().into();
        let back: ULabel = label.into();
        assert_eq!(ulabel.name, back.name);
        assert_eq!(ulabel.value, back.value);
    }

    #[test]
    fn test_value_f64_roundtrip() {
        let uval = UValue::F64 { v: 42.5 };
        let val: tsink_core::Value = uval.into();
        let back: UValue = val.into();
        match back {
            UValue::F64 { v } => assert_eq!(v, 42.5),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn test_value_string_roundtrip() {
        let uval = UValue::Str { v: "hello".into() };
        let val: tsink_core::Value = uval.into();
        let back: UValue = val.into();
        match back {
            UValue::Str { v } => assert_eq!(v, "hello"),
            _ => panic!("expected Str"),
        }
    }

    #[test]
    fn test_value_histogram_roundtrip() {
        let histogram = UNativeHistogram {
            count: Some(UHistogramCount::Int { v: 42 }),
            sum: 12.5,
            schema: 3,
            zero_threshold: 0.001,
            zero_count: Some(UHistogramCount::Float { v: 7.5 }),
            negative_spans: vec![UHistogramBucketSpan {
                offset: -2,
                length: 1,
            }],
            negative_deltas: vec![3],
            negative_counts: vec![],
            positive_spans: vec![UHistogramBucketSpan {
                offset: 1,
                length: 2,
            }],
            positive_deltas: vec![],
            positive_counts: vec![1.25, 2.5],
            reset_hint: UHistogramResetHint::No,
            custom_values: vec![],
        };
        let uval = UValue::Histogram {
            v: histogram.clone(),
        };
        let val: tsink_core::Value = uval.into();
        let back: UValue = val.into();
        match back {
            UValue::Histogram { v } => {
                assert!(matches!(v.count, Some(UHistogramCount::Int { v: 42 })));
                assert_eq!(v.sum, histogram.sum);
                assert_eq!(v.schema, histogram.schema);
                assert_eq!(v.zero_threshold, histogram.zero_threshold);
                assert!(matches!(
                    v.zero_count,
                    Some(UHistogramCount::Float { v }) if v == 7.5
                ));
                assert_eq!(v.negative_spans.len(), histogram.negative_spans.len());
                assert_eq!(v.negative_deltas, histogram.negative_deltas);
                assert_eq!(v.negative_counts, histogram.negative_counts);
                assert_eq!(v.positive_spans.len(), histogram.positive_spans.len());
                assert_eq!(v.positive_deltas, histogram.positive_deltas);
                assert_eq!(v.positive_counts, histogram.positive_counts);
                assert!(matches!(v.reset_hint, UHistogramResetHint::No));
                assert_eq!(v.custom_values, histogram.custom_values);
            }
            _ => panic!("expected Histogram"),
        }
    }

    #[test]
    fn test_datapoint_roundtrip() {
        let udp = UDataPoint {
            value: UValue::I64 { v: 100 },
            timestamp: 1234567890,
        };
        let dp: tsink_core::DataPoint = udp.into();
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
            let agg: tsink_core::Aggregation = uagg.clone().into();
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
        let qo: tsink_core::QueryOptions = uqo.into();
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
        let sel: tsink_core::SeriesSelection = usel.into();
        assert_eq!(sel.metric, Some("cpu".into()));
        assert_eq!(sel.matchers.len(), 1);
        assert_eq!(sel.start, Some(0));
        assert_eq!(sel.end, Some(1000));
    }

    #[test]
    fn test_partial_series_selection_range_is_preserved() {
        let usel = USeriesSelection {
            metric: None,
            matchers: Vec::new(),
            start: Some(100),
            end: None,
        };
        let sel: tsink_core::SeriesSelection = usel.into();
        assert_eq!(sel.start, Some(100));
        assert_eq!(sel.end, None);
    }
}
