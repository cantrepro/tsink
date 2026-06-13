use crate::enums::*;
use crate::query::*;
use crate::types::*;

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn u64_to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

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
            UValue::Histogram { v } => tsink::Value::from(tsink::NativeHistogram::from(v)),
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
            tsink::Value::Histogram(v) => UValue::Histogram { v: (*v).into() },
        }
    }
}

impl From<UHistogramCount> for tsink::HistogramCount {
    fn from(u: UHistogramCount) -> Self {
        match u {
            UHistogramCount::Int { v } => tsink::HistogramCount::Int(v),
            UHistogramCount::Float { v } => tsink::HistogramCount::Float(v),
        }
    }
}

impl From<tsink::HistogramCount> for UHistogramCount {
    fn from(c: tsink::HistogramCount) -> Self {
        match c {
            tsink::HistogramCount::Int(v) => UHistogramCount::Int { v },
            tsink::HistogramCount::Float(v) => UHistogramCount::Float { v },
        }
    }
}

impl From<UHistogramBucketSpan> for tsink::HistogramBucketSpan {
    fn from(u: UHistogramBucketSpan) -> Self {
        tsink::HistogramBucketSpan {
            offset: u.offset,
            length: u.length,
        }
    }
}

impl From<tsink::HistogramBucketSpan> for UHistogramBucketSpan {
    fn from(span: tsink::HistogramBucketSpan) -> Self {
        UHistogramBucketSpan {
            offset: span.offset,
            length: span.length,
        }
    }
}

impl From<UHistogramResetHint> for tsink::HistogramResetHint {
    fn from(u: UHistogramResetHint) -> Self {
        match u {
            UHistogramResetHint::Unknown => tsink::HistogramResetHint::Unknown,
            UHistogramResetHint::Yes => tsink::HistogramResetHint::Yes,
            UHistogramResetHint::No => tsink::HistogramResetHint::No,
            UHistogramResetHint::Gauge => tsink::HistogramResetHint::Gauge,
        }
    }
}

impl From<tsink::HistogramResetHint> for UHistogramResetHint {
    fn from(hint: tsink::HistogramResetHint) -> Self {
        match hint {
            tsink::HistogramResetHint::Unknown => UHistogramResetHint::Unknown,
            tsink::HistogramResetHint::Yes => UHistogramResetHint::Yes,
            tsink::HistogramResetHint::No => UHistogramResetHint::No,
            tsink::HistogramResetHint::Gauge => UHistogramResetHint::Gauge,
        }
    }
}

impl From<UNativeHistogram> for tsink::NativeHistogram {
    fn from(u: UNativeHistogram) -> Self {
        tsink::NativeHistogram {
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

impl From<tsink::NativeHistogram> for UNativeHistogram {
    fn from(histogram: tsink::NativeHistogram) -> Self {
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

impl From<tsink::Row> for URow {
    fn from(row: tsink::Row) -> Self {
        URow {
            metric: row.metric().to_string(),
            labels: row.labels().iter().cloned().map(Into::into).collect(),
            data_point: row.data_point().clone().into(),
        }
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

impl From<tsink::SeriesPoints> for USeriesPoints {
    fn from(series_points: tsink::SeriesPoints) -> Self {
        USeriesPoints {
            series: series_points.series.into(),
            points: series_points.points.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<UMetadataShardScope> for tsink::MetadataShardScope {
    fn from(scope: UMetadataShardScope) -> Self {
        tsink::MetadataShardScope::new(scope.shard_count, scope.shards)
    }
}

impl From<tsink::MetadataShardScope> for UMetadataShardScope {
    fn from(scope: tsink::MetadataShardScope) -> Self {
        UMetadataShardScope {
            shard_count: scope.shard_count,
            shards: scope.shards,
        }
    }
}

impl From<tsink::ShardWindowDigest> for UShardWindowDigest {
    fn from(digest: tsink::ShardWindowDigest) -> Self {
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

impl From<UShardWindowScanOptions> for tsink::ShardWindowScanOptions {
    fn from(options: UShardWindowScanOptions) -> Self {
        tsink::ShardWindowScanOptions {
            max_series: options.max_series.map(u64_to_usize),
            max_rows: options.max_rows.map(u64_to_usize),
            row_offset: options.row_offset,
        }
    }
}

impl From<tsink::ShardWindowRowsPage> for UShardWindowRowsPage {
    fn from(page: tsink::ShardWindowRowsPage) -> Self {
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

impl From<UQueryRowsScanOptions> for tsink::QueryRowsScanOptions {
    fn from(options: UQueryRowsScanOptions) -> Self {
        tsink::QueryRowsScanOptions {
            max_rows: options.max_rows.map(u64_to_usize),
            row_offset: options.row_offset,
        }
    }
}

impl From<tsink::QueryRowsPage> for UQueryRowsPage {
    fn from(page: tsink::QueryRowsPage) -> Self {
        UQueryRowsPage {
            rows_scanned: page.rows_scanned,
            truncated: page.truncated,
            next_row_offset: page.next_row_offset,
            rows: page.rows.into_iter().map(Into::into).collect(),
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

impl From<UStorageRuntimeMode> for tsink::StorageRuntimeMode {
    fn from(mode: UStorageRuntimeMode) -> Self {
        match mode {
            UStorageRuntimeMode::ReadWrite => tsink::StorageRuntimeMode::ReadWrite,
            UStorageRuntimeMode::ComputeOnly => tsink::StorageRuntimeMode::ComputeOnly,
        }
    }
}

impl From<tsink::StorageRuntimeMode> for UStorageRuntimeMode {
    fn from(mode: tsink::StorageRuntimeMode) -> Self {
        match mode {
            tsink::StorageRuntimeMode::ReadWrite => UStorageRuntimeMode::ReadWrite,
            tsink::StorageRuntimeMode::ComputeOnly => UStorageRuntimeMode::ComputeOnly,
        }
    }
}

impl From<URemoteSegmentCachePolicy> for tsink::RemoteSegmentCachePolicy {
    fn from(policy: URemoteSegmentCachePolicy) -> Self {
        match policy {
            URemoteSegmentCachePolicy::MetadataOnly => {
                tsink::RemoteSegmentCachePolicy::MetadataOnly
            }
        }
    }
}

impl From<tsink::RemoteSegmentCachePolicy> for URemoteSegmentCachePolicy {
    fn from(policy: tsink::RemoteSegmentCachePolicy) -> Self {
        match policy {
            tsink::RemoteSegmentCachePolicy::MetadataOnly => {
                URemoteSegmentCachePolicy::MetadataOnly
            }
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
    fn from(mode: tsink::WalSyncMode) -> Self {
        match mode {
            tsink::WalSyncMode::PerAppend => UWalSyncMode::PerAppend,
            tsink::WalSyncMode::Periodic(interval) => UWalSyncMode::Periodic { interval },
        }
    }
}

impl From<UWalReplayMode> for tsink::WalReplayMode {
    fn from(mode: UWalReplayMode) -> Self {
        match mode {
            UWalReplayMode::Strict => tsink::WalReplayMode::Strict,
            UWalReplayMode::Salvage => tsink::WalReplayMode::Salvage,
        }
    }
}

impl From<tsink::WalReplayMode> for UWalReplayMode {
    fn from(mode: tsink::WalReplayMode) -> Self {
        match mode {
            tsink::WalReplayMode::Strict => UWalReplayMode::Strict,
            tsink::WalReplayMode::Salvage => UWalReplayMode::Salvage,
        }
    }
}

impl From<UWriteAcknowledgement> for tsink::WriteAcknowledgement {
    fn from(acknowledgement: UWriteAcknowledgement) -> Self {
        match acknowledgement {
            UWriteAcknowledgement::Volatile => tsink::WriteAcknowledgement::Volatile,
            UWriteAcknowledgement::Appended => tsink::WriteAcknowledgement::Appended,
            UWriteAcknowledgement::Durable => tsink::WriteAcknowledgement::Durable,
        }
    }
}

impl From<tsink::WriteAcknowledgement> for UWriteAcknowledgement {
    fn from(acknowledgement: tsink::WriteAcknowledgement) -> Self {
        match acknowledgement {
            tsink::WriteAcknowledgement::Volatile => UWriteAcknowledgement::Volatile,
            tsink::WriteAcknowledgement::Appended => UWriteAcknowledgement::Appended,
            tsink::WriteAcknowledgement::Durable => UWriteAcknowledgement::Durable,
        }
    }
}

impl From<tsink::WriteResult> for UWriteResult {
    fn from(result: tsink::WriteResult) -> Self {
        UWriteResult {
            acknowledgement: result.acknowledgement.into(),
        }
    }
}

impl From<tsink::DeleteSeriesResult> for UDeleteSeriesResult {
    fn from(result: tsink::DeleteSeriesResult) -> Self {
        UDeleteSeriesResult {
            matched_series: result.matched_series,
            tombstones_applied: result.tombstones_applied,
        }
    }
}

impl From<tsink::MemoryObservabilitySnapshot> for UMemoryObservabilitySnapshot {
    fn from(snapshot: tsink::MemoryObservabilitySnapshot) -> Self {
        UMemoryObservabilitySnapshot {
            budgeted_bytes: usize_to_u64(snapshot.budgeted_bytes),
            excluded_bytes: usize_to_u64(snapshot.excluded_bytes),
            active_and_sealed_bytes: usize_to_u64(snapshot.active_and_sealed_bytes),
            registry_bytes: usize_to_u64(snapshot.registry_bytes),
            metadata_cache_bytes: usize_to_u64(snapshot.metadata_cache_bytes),
            persisted_index_bytes: usize_to_u64(snapshot.persisted_index_bytes),
            persisted_mmap_bytes: usize_to_u64(snapshot.persisted_mmap_bytes),
            tombstone_bytes: usize_to_u64(snapshot.tombstone_bytes),
            excluded_persisted_mmap_bytes: usize_to_u64(snapshot.excluded_persisted_mmap_bytes),
        }
    }
}

impl From<tsink::WalObservabilitySnapshot> for UWalObservabilitySnapshot {
    fn from(snapshot: tsink::WalObservabilitySnapshot) -> Self {
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

impl From<tsink::RetentionObservabilitySnapshot> for URetentionObservabilitySnapshot {
    fn from(snapshot: tsink::RetentionObservabilitySnapshot) -> Self {
        URetentionObservabilitySnapshot {
            max_observed_timestamp: snapshot.max_observed_timestamp,
            recency_reference_timestamp: snapshot.recency_reference_timestamp,
            future_skew_window: snapshot.future_skew_window,
            future_skew_points_total: snapshot.future_skew_points_total,
            future_skew_max_timestamp: snapshot.future_skew_max_timestamp,
        }
    }
}

impl From<tsink::FlushObservabilitySnapshot> for UFlushObservabilitySnapshot {
    fn from(snapshot: tsink::FlushObservabilitySnapshot) -> Self {
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

impl From<tsink::CompactionObservabilitySnapshot> for UCompactionObservabilitySnapshot {
    fn from(snapshot: tsink::CompactionObservabilitySnapshot) -> Self {
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

impl From<tsink::QueryObservabilitySnapshot> for UQueryObservabilitySnapshot {
    fn from(snapshot: tsink::QueryObservabilitySnapshot) -> Self {
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

impl From<URollupPolicy> for tsink::RollupPolicy {
    fn from(policy: URollupPolicy) -> Self {
        tsink::RollupPolicy {
            id: policy.id,
            metric: policy.metric,
            match_labels: policy.match_labels.into_iter().map(Into::into).collect(),
            interval: policy.interval,
            aggregation: policy.aggregation.into(),
            bucket_origin: policy.bucket_origin,
        }
    }
}

impl From<tsink::RollupPolicy> for URollupPolicy {
    fn from(policy: tsink::RollupPolicy) -> Self {
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

impl From<tsink::RollupPolicyStatus> for URollupPolicyStatus {
    fn from(status: tsink::RollupPolicyStatus) -> Self {
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

impl From<tsink::RollupObservabilitySnapshot> for URollupObservabilitySnapshot {
    fn from(snapshot: tsink::RollupObservabilitySnapshot) -> Self {
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

impl From<tsink::RemoteStorageObservabilitySnapshot> for URemoteStorageObservabilitySnapshot {
    fn from(snapshot: tsink::RemoteStorageObservabilitySnapshot) -> Self {
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

impl From<tsink::storage::StorageHealthSnapshot> for UStorageHealthSnapshot {
    fn from(snapshot: tsink::storage::StorageHealthSnapshot) -> Self {
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

impl From<tsink::StorageObservabilitySnapshot> for UStorageObservabilitySnapshot {
    fn from(snapshot: tsink::StorageObservabilitySnapshot) -> Self {
        UStorageObservabilitySnapshot {
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
            .with_pagination(u64_to_usize(u.offset), u.limit.map(u64_to_usize));

        if let Some(ds) = u.downsample {
            opts.downsample = Some(ds.into());
        }

        opts
    }
}

impl From<USeriesSelection> for tsink::SeriesSelection {
    fn from(u: USeriesSelection) -> Self {
        tsink::SeriesSelection {
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
        let val: tsink::Value = uval.into();
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

    #[test]
    fn test_partial_series_selection_range_is_preserved() {
        let usel = USeriesSelection {
            metric: None,
            matchers: Vec::new(),
            start: Some(100),
            end: None,
        };
        let sel: tsink::SeriesSelection = usel.into();
        assert_eq!(sel.start, Some(100));
        assert_eq!(sel.end, None);
    }
}
