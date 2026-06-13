use std::sync::atomic::Ordering;
use std::time::Instant;

use super::{
    apply_offset_limit_in_place, dedupe_last_value_per_timestamp, elapsed_nanos_u64,
    saturating_u64_from_usize, ChunkStorage, PersistedTierFetchStats, RawSeriesPagination,
};
use crate::query_aggregation::{
    aggregate_series, downsample_points, downsample_points_with_custom,
    downsample_points_with_origin,
};
use crate::validation::{validate_labels, validate_metric};
use crate::{
    Aggregation, DataPoint, Label, MetricSeries, QueryOptions, Result, SeriesPoints, TsinkError,
};

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn select_api(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        self.observability
            .query
            .select_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<DataPoint>> {
            let context = self.series_query_context();
            self.ensure_open()?;
            Self::validate_select_request(metric, labels, start, end)?;
            self.request_background_persisted_refresh_if_needed();
            let plan = context.query_tier_plan(start, end);

            let mut out = Vec::new();
            let stats = context.select_into(metric, labels, start, end, plan, &mut out)?;
            self.record_query_tier_plan(plan);
            self.record_persisted_tier_fetch_stats(stats);
            Ok(out)
        })();

        self.observability
            .query
            .select_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(points) => {
                self.observability
                    .query
                    .select_points_returned_total
                    .fetch_add(saturating_u64_from_usize(points.len()), Ordering::Relaxed);
                Ok(points)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    pub(in crate::engine::storage_engine) fn select_into_api(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        let context = self.series_query_context();
        self.ensure_open()?;
        Self::validate_select_request(metric, labels, start, end)?;
        self.request_background_persisted_refresh_if_needed();
        let plan = context.query_tier_plan(start, end);
        let stats = context.select_into(metric, labels, start, end, plan, out)?;
        self.record_query_tier_plan(plan);
        self.record_persisted_tier_fetch_stats(stats);
        Ok(())
    }

    pub(in crate::engine::storage_engine) fn select_with_options_api(
        &self,
        metric: &str,
        opts: QueryOptions,
    ) -> Result<Vec<DataPoint>> {
        self.observability
            .query
            .select_with_options_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<DataPoint>> {
            let context = self.series_query_context();
            self.ensure_open()?;
            validate_metric(metric)?;
            validate_labels(&opts.labels)?;
            self.request_background_persisted_refresh_if_needed();

            if opts.start >= opts.end {
                return Err(TsinkError::InvalidTimeRange {
                    start: opts.start,
                    end: opts.end,
                });
            }

            if let Some(downsample) = opts.downsample {
                if downsample.interval <= 0 {
                    return Err(TsinkError::InvalidConfiguration(
                        "downsample interval must be positive".to_string(),
                    ));
                }
            }
            let plan = context.query_tier_plan(opts.start, opts.end);
            self.record_query_tier_plan(plan);

            let aggregation = match (opts.downsample.is_some(), opts.aggregation) {
                (true, Aggregation::None) => Aggregation::Last,
                _ => opts.aggregation,
            };

            if opts.limit == Some(0) {
                return Ok(Vec::new());
            }

            if opts.custom_aggregation.is_none()
                && opts.downsample.is_none()
                && aggregation == Aggregation::None
            {
                let page = context.select_raw_series_page(
                    metric,
                    &opts.labels,
                    opts.start,
                    opts.end,
                    plan,
                    RawSeriesPagination::new(saturating_u64_from_usize(opts.offset), opts.limit),
                )?;
                self.record_persisted_tier_fetch_stats(page.stats);
                return Ok(page.points);
            }

            let mut processed = if let Some(custom) = opts.custom_aggregation {
                let mut points = Vec::new();
                let stats = context.select_into(
                    metric,
                    &opts.labels,
                    opts.start,
                    opts.end,
                    plan,
                    &mut points,
                )?;
                self.record_persisted_tier_fetch_stats(stats);
                if let Some(downsample) = opts.downsample {
                    downsample_points_with_custom(
                        &points,
                        downsample.interval,
                        custom.as_ref(),
                        opts.start,
                        opts.end,
                    )?
                } else {
                    custom
                        .aggregate_series(&points)?
                        .into_iter()
                        .collect::<Vec<DataPoint>>()
                }
            } else {
                let _visibility_guard = context.visibility_read_fence();
                let mut rollup_stats = PersistedTierFetchStats::default();
                let mut used_rollup = false;
                let mut partial_rollup = false;
                let mut rollup_points_read = 0usize;
                let mut processed = None;

                if let Some(downsample) = opts.downsample {
                    if let Some(candidate) = context.rollup_query_candidate(
                        metric,
                        &opts.labels,
                        downsample.interval,
                        aggregation,
                        opts.start,
                        opts.end,
                    ) {
                        let covered_end = candidate.materialized_through.min(opts.end);
                        if covered_end > opts.start {
                            let rollup_plan = context.query_tier_plan(opts.start, covered_end);
                            let mut rollup_points = Vec::new();
                            rollup_stats.accumulate(context.select_into(
                                candidate.metric.as_str(),
                                &opts.labels,
                                opts.start,
                                covered_end,
                                rollup_plan,
                                &mut rollup_points,
                            )?);
                            rollup_points.sort_by_key(|point| point.timestamp);
                            dedupe_last_value_per_timestamp(&mut rollup_points);

                            used_rollup = true;
                            partial_rollup = covered_end < opts.end;
                            rollup_points_read = rollup_points.len();

                            if partial_rollup {
                                let tail_plan = context.query_tier_plan(covered_end, opts.end);
                                let mut raw_tail = Vec::new();
                                rollup_stats.accumulate(context.select_into(
                                    metric,
                                    &opts.labels,
                                    covered_end,
                                    opts.end,
                                    tail_plan,
                                    &mut raw_tail,
                                )?);
                                let mut tail_points = downsample_points_with_origin(
                                    &raw_tail,
                                    downsample.interval,
                                    aggregation,
                                    candidate.policy.bucket_origin,
                                    covered_end,
                                    opts.end,
                                )?;
                                rollup_points.append(&mut tail_points);
                            }

                            processed = Some(rollup_points);
                        }
                    }
                }

                let mut processed = if let Some(processed) = processed {
                    context.record_rollup_query_use(rollup_points_read, partial_rollup);
                    self.record_persisted_tier_fetch_stats(rollup_stats);
                    processed
                } else {
                    let mut points = Vec::new();
                    let stats = context.select_into(
                        metric,
                        &opts.labels,
                        opts.start,
                        opts.end,
                        plan,
                        &mut points,
                    )?;
                    self.record_persisted_tier_fetch_stats(stats);
                    if let Some(downsample) = opts.downsample {
                        downsample_points(
                            &points,
                            downsample.interval,
                            aggregation,
                            opts.start,
                            opts.end,
                        )?
                    } else if aggregation != Aggregation::None {
                        aggregate_series(&points, aggregation)?
                            .into_iter()
                            .collect::<Vec<DataPoint>>()
                    } else {
                        points
                    }
                };

                if used_rollup {
                    processed.sort_by_key(|point| point.timestamp);
                    dedupe_last_value_per_timestamp(&mut processed);
                }
                processed
            };

            apply_offset_limit_in_place(
                &mut processed,
                saturating_u64_from_usize(opts.offset),
                opts.limit,
            );

            Ok(processed)
        })();

        self.observability
            .query
            .select_with_options_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(points) => {
                self.observability
                    .query
                    .select_with_options_points_returned_total
                    .fetch_add(saturating_u64_from_usize(points.len()), Ordering::Relaxed);
                Ok(points)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_with_options_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    pub(in crate::engine::storage_engine) fn select_many_api(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
    ) -> Result<Vec<SeriesPoints>> {
        let context = self.series_query_context();
        self.ensure_open()?;
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        self.request_background_persisted_refresh_if_needed();

        for item in series {
            validate_metric(&item.name)?;
            validate_labels(&item.labels)?;
        }
        let resolved = context.resolve_series_batch(series);
        let plan = context.query_tier_plan(start, end);

        let mut out = Vec::with_capacity(resolved.len());
        let mut persisted_stats = PersistedTierFetchStats::default();
        for (series, series_id) in resolved {
            let points = match series_id {
                Some(series_id) => {
                    let (points, stats) =
                        context.collect_points_for_series(series_id, start, end, plan)?;
                    persisted_stats.accumulate(stats);
                    points
                }
                None => Vec::new(),
            };
            out.push(SeriesPoints { series, points });
        }
        self.record_query_tier_plan(plan);
        self.record_persisted_tier_fetch_stats(persisted_stats);
        Ok(out)
    }

    pub(in crate::engine::storage_engine) fn select_all_api(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.observability
            .query
            .select_all_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            let context = self.series_query_context();
            self.ensure_open()?;
            validate_metric(metric)?;
            self.request_background_persisted_refresh_if_needed();

            if start >= end {
                return Err(TsinkError::InvalidTimeRange { start, end });
            }

            let mut series_with_labels = context.resolved_series_for_metric(metric);
            if series_with_labels.is_empty() {
                return Ok(Vec::new());
            }
            series_with_labels.sort_by(|a, b| a.1.labels.cmp(&b.1.labels));
            let plan = context.query_tier_plan(start, end);

            let mut persisted_stats = PersistedTierFetchStats::default();
            let mut out = Vec::new();
            for (series_id, series) in series_with_labels {
                let (points, stats) =
                    context.collect_points_for_series(series_id, start, end, plan)?;
                if points.is_empty() {
                    continue;
                }
                persisted_stats.accumulate(stats);
                out.push((series.labels, points));
            }
            self.record_query_tier_plan(plan);
            self.record_persisted_tier_fetch_stats(persisted_stats);
            Ok(out)
        })();

        self.observability
            .query
            .select_all_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(series) => {
                let points_returned = series.iter().map(|(_, points)| points.len()).sum::<usize>();
                self.observability
                    .query
                    .select_all_series_returned_total
                    .fetch_add(saturating_u64_from_usize(series.len()), Ordering::Relaxed);
                self.observability
                    .query
                    .select_all_points_returned_total
                    .fetch_add(
                        saturating_u64_from_usize(points_returned),
                        Ordering::Relaxed,
                    );
                Ok(series)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_all_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    pub(super) fn validate_select_request(
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<()> {
        validate_metric(metric)?;
        validate_labels(labels)?;
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        Ok(())
    }
}
