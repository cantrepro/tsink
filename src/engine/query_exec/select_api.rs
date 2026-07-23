use std::sync::atomic::Ordering;
use std::time::Instant;

use super::{
    apply_offset_limit_in_place, dedupe_last_value_per_timestamp, elapsed_nanos_u64,
    modeled_point_output_upper_bound_bytes, modeled_point_result_upper_bound_bytes,
    modeled_points_retained_bytes, modeled_vec_capacity_bytes, saturating_u64_from_usize,
    value_heap_bytes, ChunkStorage, PersistedTierFetchStats, RawSeriesPagination,
};
use crate::query_aggregation::{
    aggregate_series, downsample_points, downsample_points_with_custom,
    downsample_points_with_origin,
};
use crate::validation::{validate_labels, validate_metric};
use crate::QueryExecution;
use crate::{
    Aggregation, DataPoint, Label, MetricSeries, QueryOptions, Result, SeriesPoints, TsinkError,
};

fn modeled_vec_growth_capacity_upper(elements: usize) -> usize {
    if elements == 0 {
        return 0;
    }
    elements
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .max(4)
}

fn downsample_output_count_upper(
    points: &[DataPoint],
    interval: i64,
    origin: i64,
    start: i64,
    end: i64,
) -> usize {
    if points.is_empty() || interval <= 0 || start >= end {
        return 0;
    }
    let mut idx = 0usize;
    let mut buckets = 0usize;
    while idx < points.len() && points[idx].timestamp < start {
        idx = idx.saturating_add(1);
    }
    while idx < points.len() && points[idx].timestamp < end {
        buckets = buckets.saturating_add(1);
        let bucket_start = crate::query_aggregation::bucket_start_for_origin(
            points[idx].timestamp,
            origin,
            interval,
        );
        let bucket_end = bucket_start.saturating_add(interval);
        while idx < points.len()
            && points[idx].timestamp < end
            && points[idx].timestamp < bucket_end
        {
            idx = idx.saturating_add(1);
        }
    }
    buckets
}

fn builtin_aggregation_scratch_bytes(points: usize, aggregation: Aggregation) -> u64 {
    if matches!(
        aggregation,
        Aggregation::Sum
            | Aggregation::Avg
            | Aggregation::Median
            | Aggregation::Range
            | Aggregation::Variance
            | Aggregation::StdDev
    ) {
        saturating_u64_from_usize(points)
            .saturating_mul(u64::try_from(std::mem::size_of::<f64>()).unwrap_or(u64::MAX))
    } else {
        0
    }
}

fn modeled_points_value_heap_bytes(points: &[DataPoint]) -> u64 {
    points.iter().fold(0u64, |bytes, point| {
        bytes.saturating_add(u64::try_from(value_heap_bytes(&point.value)).unwrap_or(u64::MAX))
    })
}

fn modeled_string_allocation_bytes(value: &str) -> u64 {
    if value.is_empty() {
        0
    } else {
        u64::try_from(value.len())
            .unwrap_or(u64::MAX)
            .saturating_add(super::QUERY_COLLECTION_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_metric_series_retained_bytes(series: &MetricSeries) -> u64 {
    modeled_string_allocation_bytes(&series.name)
        .saturating_add(modeled_vec_capacity_bytes::<Label>(series.labels.len()))
        .saturating_add(series.labels.iter().fold(0u64, |bytes, label| {
            bytes
                .saturating_add(modeled_string_allocation_bytes(&label.name))
                .saturating_add(modeled_string_allocation_bytes(&label.value))
        }))
}

fn modeled_series_points_identity_returned_bytes(series: &MetricSeries) -> u64 {
    u64::try_from(std::mem::size_of::<SeriesPoints>())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(series.name.len()).unwrap_or(u64::MAX))
        .saturating_add(super::modeled_labels_bytes(&series.labels))
}

fn reserve_retained_points(
    execution: &QueryExecution,
    points: &Vec<DataPoint>,
) -> Result<crate::QueryMemoryReservation> {
    execution.checkpoint()?;
    execution.observe_intermediate_vector_size(saturating_u64_from_usize(points.len()))?;
    execution
        .reserve_memory(modeled_points_retained_bytes(points))
        .map_err(Into::into)
}

fn execute_builtin_point_transform(
    execution: &QueryExecution,
    points: &Vec<DataPoint>,
    aggregation: Aggregation,
    output_elements_upper: usize,
    transform: impl FnOnce() -> Result<Vec<DataPoint>>,
) -> Result<(Vec<DataPoint>, crate::QueryMemoryReservation)> {
    execution.checkpoint()?;
    let _input_reservation = reserve_retained_points(execution, points)?;
    execution.ensure_samples_returned(saturating_u64_from_usize(output_elements_upper))?;
    execution.ensure_returned_bytes(modeled_point_result_upper_bound_bytes(
        points,
        output_elements_upper,
    ))?;
    execution.observe_intermediate_vector_size(saturating_u64_from_usize(output_elements_upper))?;
    let output_capacity_upper = modeled_vec_growth_capacity_upper(output_elements_upper);
    let mut output_reservation = execution.reserve_memory(
        modeled_point_output_upper_bound_bytes(points, output_capacity_upper),
    )?;
    let _scratch_reservation =
        execution.reserve_memory(builtin_aggregation_scratch_bytes(points.len(), aggregation))?;

    let output = transform()?;
    output_reservation.resize(modeled_points_retained_bytes(&output))?;
    Ok((output, output_reservation))
}

fn execute_custom_point_transform(
    execution: &QueryExecution,
    points: &Vec<DataPoint>,
    output_elements_upper: usize,
    transform: impl FnOnce(&mut dyn FnMut(&DataPoint) -> Result<()>) -> Result<Vec<DataPoint>>,
) -> Result<(Vec<DataPoint>, crate::QueryMemoryReservation)> {
    execution.checkpoint()?;
    let _input_reservation = reserve_retained_points(execution, points)?;
    execution.ensure_samples_returned(saturating_u64_from_usize(output_elements_upper))?;
    // Caller-provided aggregators may allocate arbitrary value payloads internally. The fixed
    // tsink result slots are preflighted here; returned payload bytes are admitted by `admit`
    // before tsink transfers them into its result vector.
    execution.ensure_returned_bytes(
        saturating_u64_from_usize(output_elements_upper)
            .saturating_mul(u64::try_from(std::mem::size_of::<DataPoint>()).unwrap_or(u64::MAX)),
    )?;
    execution.observe_intermediate_vector_size(saturating_u64_from_usize(output_elements_upper))?;
    let output_capacity_upper = modeled_vec_growth_capacity_upper(output_elements_upper);
    let fixed_output_bytes = modeled_vec_capacity_bytes::<DataPoint>(output_capacity_upper);
    let fixed_result_bytes = saturating_u64_from_usize(output_elements_upper)
        .saturating_mul(u64::try_from(std::mem::size_of::<DataPoint>()).unwrap_or(u64::MAX));
    let mut output_reservation = execution.reserve_memory(fixed_output_bytes)?;
    let mut transferred_heap_bytes = 0u64;
    let mut admit = |point: &DataPoint| -> Result<()> {
        execution.checkpoint()?;
        transferred_heap_bytes = transferred_heap_bytes
            .saturating_add(u64::try_from(value_heap_bytes(&point.value)).unwrap_or(u64::MAX));
        execution
            .ensure_returned_bytes(fixed_result_bytes.saturating_add(transferred_heap_bytes))?;
        output_reservation.resize(fixed_output_bytes.saturating_add(transferred_heap_bytes))?;
        Ok(())
    };

    let output = transform(&mut admit)?;
    output_reservation.resize(modeled_points_retained_bytes(&output))?;
    Ok((output, output_reservation))
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn select_api(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> Result<Vec<DataPoint>> {
        self.observability
            .query
            .select_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<(Vec<DataPoint>, crate::QueryMemoryReservation)> {
            execution.checkpoint()?;
            let context = self.series_query_context();
            self.ensure_open()?;
            Self::validate_select_request(metric, labels, start, end)?;
            self.request_background_persisted_refresh_if_needed();
            let plan = context.query_tier_plan(start, end);

            let mut out = Vec::new();
            let stats = context.select_into(
                metric,
                labels,
                start,
                end,
                plan,
                &mut out,
                Some(execution),
                true,
            )?;
            self.record_query_tier_plan(plan);
            self.record_persisted_tier_fetch_stats(stats);
            let reservation = reserve_retained_points(execution, &out)?;
            Ok((out, reservation))
        })()
        .and_then(|(points, _reservation)| {
            self.charge_point_query_result(execution, 0, 0, &points)?;
            Ok(points)
        });

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
        execution: &QueryExecution,
    ) -> Result<()> {
        let context = self.series_query_context();
        self.ensure_open()?;
        Self::validate_select_request(metric, labels, start, end)?;
        self.request_background_persisted_refresh_if_needed();
        let plan = context.query_tier_plan(start, end);
        execution.checkpoint()?;
        let stats =
            context.select_into(metric, labels, start, end, plan, out, Some(execution), true)?;
        self.record_query_tier_plan(plan);
        self.record_persisted_tier_fetch_stats(stats);
        let _reservation = reserve_retained_points(execution, out)?;
        self.charge_point_query_result(execution, 0, 0, out)
    }

    pub(in crate::engine::storage_engine) fn select_with_options_api(
        &self,
        metric: &str,
        opts: QueryOptions,
        execution: &QueryExecution,
    ) -> Result<Vec<DataPoint>> {
        self.observability
            .query
            .select_with_options_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<(Vec<DataPoint>, crate::QueryMemoryReservation)> {
            execution.checkpoint()?;
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
                let points = Vec::new();
                let reservation = reserve_retained_points(execution, &points)?;
                return Ok((points, reservation));
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
                    Some(execution),
                    true,
                )?;
                self.record_persisted_tier_fetch_stats(page.stats);
                let reservation = reserve_retained_points(execution, &page.points)?;
                return Ok((page.points, reservation));
            }

            let (mut processed, mut processed_reservation) = if let Some(custom) =
                opts.custom_aggregation
            {
                let mut points = Vec::new();
                let stats = context.select_into(
                    metric,
                    &opts.labels,
                    opts.start,
                    opts.end,
                    plan,
                    &mut points,
                    Some(execution),
                    false,
                )?;
                self.record_persisted_tier_fetch_stats(stats);
                if let Some(downsample) = opts.downsample {
                    let output_upper = downsample_output_count_upper(
                        &points,
                        downsample.interval,
                        opts.start,
                        opts.start,
                        opts.end,
                    );
                    execute_custom_point_transform(execution, &points, output_upper, |admit| {
                        downsample_points_with_custom(
                            &points,
                            downsample.interval,
                            custom.as_ref(),
                            opts.start,
                            opts.end,
                            |point| admit(point),
                        )
                    })?
                } else {
                    execute_custom_point_transform(execution, &points, 1, |admit| {
                        let mut output = Vec::new();
                        if let Some(point) = custom.aggregate_series(&points)? {
                            admit(&point)?;
                            output.push(point);
                        }
                        Ok(output)
                    })?
                }
            } else {
                let _visibility_guard = context.visibility_read_fence();
                let mut rollup_stats = PersistedTierFetchStats::default();
                let mut used_rollup = false;
                let mut partial_rollup = false;
                let mut rollup_points_read = 0usize;
                let mut processed: Option<(Vec<DataPoint>, crate::QueryMemoryReservation)> = None;

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
                                Some(execution),
                                false,
                            )?);
                            rollup_points.sort_by_key(|point| point.timestamp);
                            dedupe_last_value_per_timestamp(&mut rollup_points);
                            let mut rollup_reservation =
                                reserve_retained_points(execution, &rollup_points)?;

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
                                    Some(execution),
                                    false,
                                )?);
                                let (mut tail_points, _tail_reservation) =
                                    execute_builtin_point_transform(
                                        execution,
                                        &raw_tail,
                                        aggregation,
                                        downsample_output_count_upper(
                                            &raw_tail,
                                            downsample.interval,
                                            candidate.policy.bucket_origin,
                                            covered_end,
                                            opts.end,
                                        ),
                                        || {
                                            downsample_points_with_origin(
                                                &raw_tail,
                                                downsample.interval,
                                                aggregation,
                                                candidate.policy.bucket_origin,
                                                covered_end,
                                                opts.end,
                                            )
                                        },
                                    )?;
                                let combined_len =
                                    rollup_points.len().saturating_add(tail_points.len());
                                let combined_capacity =
                                    modeled_vec_growth_capacity_upper(combined_len);
                                let combined_heap = modeled_points_value_heap_bytes(&rollup_points)
                                    .saturating_add(modeled_points_value_heap_bytes(&tail_points));
                                rollup_reservation.resize(
                                    modeled_vec_capacity_bytes::<DataPoint>(combined_capacity)
                                        .saturating_add(combined_heap),
                                )?;
                                rollup_points.append(&mut tail_points);
                                rollup_reservation
                                    .resize(modeled_points_retained_bytes(&rollup_points))?;
                            }

                            processed = Some((rollup_points, rollup_reservation));
                        }
                    }
                }

                let (mut processed, mut processed_reservation) = if let Some(processed) = processed
                {
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
                        Some(execution),
                        false,
                    )?;
                    self.record_persisted_tier_fetch_stats(stats);
                    if let Some(downsample) = opts.downsample {
                        let output_upper = downsample_output_count_upper(
                            &points,
                            downsample.interval,
                            opts.start,
                            opts.start,
                            opts.end,
                        );
                        execute_builtin_point_transform(
                            execution,
                            &points,
                            aggregation,
                            output_upper,
                            || {
                                downsample_points(
                                    &points,
                                    downsample.interval,
                                    aggregation,
                                    opts.start,
                                    opts.end,
                                )
                            },
                        )?
                    } else if aggregation != Aggregation::None {
                        execute_builtin_point_transform(execution, &points, aggregation, 1, || {
                            Ok(aggregate_series(&points, aggregation)?
                                .into_iter()
                                .collect::<Vec<DataPoint>>())
                        })?
                    } else {
                        let reservation = reserve_retained_points(execution, &points)?;
                        (points, reservation)
                    }
                };

                if used_rollup {
                    processed.sort_by_key(|point| point.timestamp);
                    dedupe_last_value_per_timestamp(&mut processed);
                    processed_reservation.resize(modeled_points_retained_bytes(&processed))?;
                }
                (processed, processed_reservation)
            };

            apply_offset_limit_in_place(
                &mut processed,
                saturating_u64_from_usize(opts.offset),
                opts.limit,
            );
            processed_reservation.resize(modeled_points_retained_bytes(&processed))?;

            Ok((processed, processed_reservation))
        })()
        .and_then(|(points, _reservation)| {
            self.charge_point_query_result(execution, 0, 0, &points)?;
            Ok(points)
        });

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
        execution: &QueryExecution,
    ) -> Result<Vec<SeriesPoints>> {
        let context = self.series_query_context();
        execution.checkpoint()?;
        self.ensure_open()?;
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        self.request_background_persisted_refresh_if_needed();

        for item in series {
            validate_metric(&item.name)?;
            validate_labels(&item.labels)?;
        }
        let identity_returned_bytes = series.iter().fold(0u64, |bytes, item| {
            bytes.saturating_add(modeled_series_points_identity_returned_bytes(item))
        });
        execution.ensure_returned_bytes(identity_returned_bytes)?;
        let identity_retained_bytes = modeled_vec_capacity_bytes::<SeriesPoints>(series.len())
            .saturating_add(series.iter().fold(0u64, |bytes, item| {
                bytes.saturating_add(modeled_metric_series_retained_bytes(item))
            }));
        let mut output_reservation = execution.reserve_memory(identity_retained_bytes)?;
        // Charge identity bytes before `resolve_series_batch` clones metric and label strings.
        execution.charge_returned_bytes(identity_returned_bytes)?;
        let resolved = context.resolve_series_batch(series);
        let plan = context.query_tier_plan(start, end);

        let mut out = Vec::with_capacity(resolved.len());
        let mut retained_point_bytes = 0u64;
        let mut persisted_stats = PersistedTierFetchStats::default();
        for (series, series_id) in resolved {
            execution.checkpoint()?;
            let points = match series_id {
                Some(series_id) => {
                    let (points, stats) = context.collect_points_for_series(
                        series_id,
                        start,
                        end,
                        plan,
                        Some(execution),
                        true,
                    )?;
                    persisted_stats.accumulate(stats);
                    points
                }
                None => Vec::new(),
            };
            retained_point_bytes =
                retained_point_bytes.saturating_add(modeled_points_retained_bytes(&points));
            output_reservation
                .resize(identity_retained_bytes.saturating_add(retained_point_bytes))?;
            self.charge_point_query_result(execution, 0, 0, &points)?;
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
        execution: &QueryExecution,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.observability
            .query
            .select_all_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            execution.checkpoint()?;
            let context = self.series_query_context();
            self.ensure_open()?;
            validate_metric(metric)?;
            self.request_background_persisted_refresh_if_needed();

            if start >= end {
                return Err(TsinkError::InvalidTimeRange { start, end });
            }

            let registry = self.catalog.registry.read();
            let registry_series_count = registry.series_count();
            let mut identity_reservation = execution.reserve_memory(
                modeled_vec_capacity_bytes::<super::SeriesId>(registry_series_count),
            )?;
            // The ID vector is admitted against the full registry count before postings are
            // materialized. Exact identity shapes are then measured without constructing strings.
            let series_ids = registry.series_ids_for_metric(metric);
            let mut identity_returned_bytes = 0u64;
            let mut identity_retained_bytes = 0u64;
            for series_id in &series_ids {
                execution.checkpoint()?;
                let Some((metric_bytes, label_count, label_text_bytes)) =
                    registry.decoded_series_key_shape(*series_id)
                else {
                    continue;
                };
                identity_returned_bytes = identity_returned_bytes.saturating_add(
                    super::modeled_metric_series_shape_bytes(
                        metric_bytes,
                        label_count,
                        label_text_bytes,
                    ),
                );
                identity_retained_bytes = identity_retained_bytes.saturating_add(
                    super::modeled_metric_series_shape_retained_bytes(
                        metric_bytes,
                        label_count,
                        label_text_bytes,
                    ),
                );
            }
            execution.ensure_returned_bytes(identity_returned_bytes)?;
            let output_identity_bytes =
                modeled_vec_capacity_bytes::<(Vec<Label>, Vec<DataPoint>)>(series_ids.len())
                    .saturating_add(
                        modeled_vec_capacity_bytes::<(super::SeriesId, MetricSeries)>(
                            series_ids.len(),
                        ),
                    )
                    .saturating_add(identity_retained_bytes);
            identity_reservation.resize(
                modeled_vec_capacity_bytes::<super::SeriesId>(series_ids.capacity())
                    .saturating_add(output_identity_bytes),
            )?;
            // Charge metric and label identity bytes before decoding clones their strings.
            execution.charge_returned_bytes(identity_returned_bytes)?;
            let mut series_with_labels = Vec::with_capacity(series_ids.len());
            for series_id in series_ids {
                execution.checkpoint()?;
                let Some(series_key) = registry.decode_series_key(series_id) else {
                    continue;
                };
                series_with_labels.push((
                    series_id,
                    MetricSeries {
                        name: series_key.metric,
                        labels: series_key.labels,
                    },
                ));
            }
            drop(registry);
            if series_with_labels.is_empty() {
                return Ok(Vec::new());
            }
            series_with_labels.sort_by(|a, b| a.1.labels.cmp(&b.1.labels));
            let plan = context.query_tier_plan(start, end);

            let mut persisted_stats = PersistedTierFetchStats::default();
            let mut out = Vec::with_capacity(series_with_labels.len());
            let mut retained_point_bytes = 0u64;
            for (series_id, series) in series_with_labels {
                execution.checkpoint()?;
                let (points, stats) = context.collect_points_for_series(
                    series_id,
                    start,
                    end,
                    plan,
                    Some(execution),
                    true,
                )?;
                if points.is_empty() {
                    continue;
                }
                retained_point_bytes =
                    retained_point_bytes.saturating_add(modeled_points_retained_bytes(&points));
                identity_reservation.resize(
                    modeled_vec_capacity_bytes::<super::SeriesId>(registry_series_count)
                        .saturating_add(output_identity_bytes)
                        .saturating_add(retained_point_bytes),
                )?;
                self.charge_point_query_result(execution, 0, 0, &points)?;
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
