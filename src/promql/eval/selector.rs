use std::collections::BTreeSet;

use crate::{
    DataPoint, Label, MetricSeries, QueryExecution, QueryExecutionAccounting,
    QueryMemoryReservation, SelectSeriesExecutionResult, SeriesMatcher, SeriesPoints,
    SeriesSelection, TsinkError,
};

use crate::promql::ast::{MatchOp, MatrixSelector, VectorSelector};
use crate::promql::error::{PromqlError, Result};
use crate::promql::types::{is_stale_nan_value, value_to_f64, PromqlValue, Sample, Series};

use super::time::duration_to_units;
use super::{resolve_at_modifier, Engine, QueryParams};

pub(crate) struct PreparedPromqlMatchers<'a> {
    matchers: &'a [crate::promql::ast::LabelMatcher],
    regexes: Vec<Option<crate::query_matcher::ExecutionBoundedRegex>>,
    _slots_reservation: QueryMemoryReservation,
}

impl<'a> PreparedPromqlMatchers<'a> {
    pub(crate) fn new(
        matchers: &'a [crate::promql::ast::LabelMatcher],
        execution: &QueryExecution,
    ) -> Result<Self> {
        crate::query_matcher::validate_matcher_shapes(
            matchers.len(),
            matchers
                .iter()
                .map(|matcher| (matcher.name.as_str(), matcher.value.as_str())),
        )
        .map_err(|error| PromqlError::Parse(error.to_string()))?;
        execution.checkpoint().map_err(TsinkError::from)?;
        let slots_bytes = super::modeled_vec_capacity_bytes::<
            Option<crate::query_matcher::ExecutionBoundedRegex>,
        >(matchers.len());
        let slots_reservation = execution
            .reserve_memory(slots_bytes)
            .map_err(TsinkError::from)?;
        let mut regexes = Vec::with_capacity(matchers.len());
        for matcher in matchers {
            execution.checkpoint().map_err(TsinkError::from)?;
            let regex = match matcher.op {
                MatchOp::RegexMatch | MatchOp::RegexNoMatch => Some(
                    crate::query_matcher::prepare_bounded_regex_with_execution(
                        &matcher.value,
                        crate::query_matcher::RegexAnchoring::Anchored,
                        execution,
                    )
                    .map_err(promql_regex_preparation_error)?,
                ),
                MatchOp::Equal | MatchOp::NotEqual => None,
            };
            regexes.push(regex);
        }
        Ok(Self {
            matchers,
            regexes,
            _slots_reservation: slots_reservation,
        })
    }

    pub(crate) fn matches(&self, metric: &str, labels: &[Label]) -> bool {
        self.matches_where(metric, labels, |_| true)
    }

    pub(crate) fn matches_metric_name(&self, metric: &str) -> bool {
        self.matches_where(metric, &[], |matcher| matcher.name == "__name__")
    }

    fn matches_where(
        &self,
        metric: &str,
        labels: &[Label],
        include: impl Fn(&crate::promql::ast::LabelMatcher) -> bool,
    ) -> bool {
        for (matcher, prepared_regex) in self.matchers.iter().zip(&self.regexes) {
            if !include(matcher) {
                continue;
            }
            let actual = if matcher.name == "__name__" {
                metric
            } else {
                labels
                    .iter()
                    .find(|label| label.name == matcher.name)
                    .map(|label| label.value.as_str())
                    .unwrap_or("")
            };

            let matched = match matcher.op {
                MatchOp::Equal => actual == matcher.value,
                MatchOp::NotEqual => actual != matcher.value,
                MatchOp::RegexMatch => prepared_regex
                    .as_ref()
                    .expect("regex matcher was prepared")
                    .regex()
                    .is_match(actual),
                MatchOp::RegexNoMatch => !prepared_regex
                    .as_ref()
                    .expect("negative regex matcher was prepared")
                    .regex()
                    .is_match(actual),
            };
            if !matched {
                return false;
            }
        }
        true
    }
}

fn promql_regex_preparation_error(
    error: crate::query_matcher::BoundedRegexPreparationError,
) -> PromqlError {
    match error {
        crate::query_matcher::BoundedRegexPreparationError::Regex(error) => {
            PromqlError::Regex(error.to_string())
        }
        crate::query_matcher::BoundedRegexPreparationError::Query(error) => {
            PromqlError::Storage(TsinkError::from(error))
        }
    }
}

pub(super) struct RetainedMetricRows {
    pub(super) rows: super::MetricPrefetchRows,
    reservation: Option<QueryMemoryReservation>,
}

impl RetainedMetricRows {
    fn unreserved(rows: super::MetricPrefetchRows) -> Self {
        Self {
            rows,
            reservation: None,
        }
    }

    pub(super) fn into_parts(self) -> (super::MetricPrefetchRows, Option<QueryMemoryReservation>) {
        (self.rows, self.reservation)
    }
}

pub(crate) fn eval_vector_selector(
    engine: &Engine,
    selector: &VectorSelector,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    let base_eval = resolve_at_modifier(
        selector.at.as_ref(),
        params,
        engine.timestamp_units_per_second(),
    )?;
    let offset = duration_to_units(selector.offset, engine.timestamp_units_per_second());
    let eval_at = base_eval.saturating_sub(offset);
    let start = eval_at.saturating_sub(engine.default_lookback_delta());
    let end = eval_at.saturating_add(1);

    let mut out = Vec::new();

    if let Some(metric) = &selector.metric_name {
        if let Some(labels) = exact_equal_labels(metric, &selector.matchers) {
            if has_exact_series(engine, metric, &labels, params)? {
                let points = load_exact_series_points_with_retained_execution(
                    engine, metric, &labels, start, end, params,
                )?;
                if let Some(point) = points
                    .iter()
                    .filter(|point| point.timestamp >= start && point.timestamp < end)
                    .max_by_key(|point| point.timestamp)
                {
                    if !is_stale_nan_value(&point.value) {
                        params.reserve_sample(metric, &labels, point.value.as_histogram())?;
                    }
                }
                if let Some(sample) = latest_point_as_sample(metric, labels, points, start, end) {
                    out.push(sample);
                }
                return Ok(PromqlValue::InstantVector(out));
            }
        }

        let prepared_matchers = PreparedPromqlMatchers::new(&selector.matchers, params.execution)?;
        collect_instant_samples_for_metric(
            engine,
            metric,
            &prepared_matchers,
            start,
            end,
            params,
            &mut out,
        )?;
    } else {
        let prepared_matchers = PreparedPromqlMatchers::new(&selector.matchers, params.execution)?;
        for metric in candidate_metrics(engine, selector, params)? {
            collect_instant_samples_for_metric(
                engine,
                &metric,
                &prepared_matchers,
                start,
                end,
                params,
                &mut out,
            )?;
        }
    }

    Ok(PromqlValue::InstantVector(out))
}

pub(crate) fn eval_matrix_selector(
    engine: &Engine,
    selector: &MatrixSelector,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    let base_eval = resolve_at_modifier(
        selector.vector.at.as_ref(),
        params,
        engine.timestamp_units_per_second(),
    )?;
    let offset = duration_to_units(selector.vector.offset, engine.timestamp_units_per_second());
    let eval_at = base_eval.saturating_sub(offset);
    let range = duration_to_units(selector.range, engine.timestamp_units_per_second());
    let start = eval_at.saturating_sub(range);
    let end = eval_at.saturating_add(1);

    let mut out = Vec::new();

    if let Some(metric) = &selector.vector.metric_name {
        if let Some(labels) = exact_equal_labels(metric, &selector.vector.matchers) {
            if has_exact_series(engine, metric, &labels, params)? {
                let points = load_exact_series_points_with_retained_execution(
                    engine, metric, &labels, start, end, params,
                )?;
                params.reserve_range_series_upper(metric, &labels, &points)?;
                let samples = points
                    .into_iter()
                    .filter(|p| p.timestamp > start && p.timestamp < end)
                    .filter(|p| !is_stale_nan_value(&p.value))
                    .fold(Series::new(metric.clone(), labels), |mut series, point| {
                        if let Some(histogram) = point.value.as_histogram() {
                            series
                                .histograms
                                .push((point.timestamp, Box::new(histogram.clone())));
                        } else {
                            series
                                .samples
                                .push((point.timestamp, value_to_f64(&point.value)));
                        }
                        series
                    });
                if !samples.samples.is_empty() || !samples.histograms.is_empty() {
                    out.push(samples);
                }
                return Ok(PromqlValue::RangeVector(out));
            }
        }

        let prepared_matchers =
            PreparedPromqlMatchers::new(&selector.vector.matchers, params.execution)?;
        collect_range_series_for_metric(
            engine,
            metric,
            &prepared_matchers,
            start,
            end,
            params,
            &mut out,
        )?;
    } else {
        let prepared_matchers =
            PreparedPromqlMatchers::new(&selector.vector.matchers, params.execution)?;
        for metric in candidate_metrics_for_matrix(engine, selector, params)? {
            collect_range_series_for_metric(
                engine,
                &metric,
                &prepared_matchers,
                start,
                end,
                params,
                &mut out,
            )?;
        }
    }

    Ok(PromqlValue::RangeVector(out))
}

fn candidate_metrics(
    engine: &Engine,
    selector: &VectorSelector,
    params: &QueryParams<'_>,
) -> Result<Vec<String>> {
    if let Some(metric) = &selector.metric_name {
        return Ok(vec![metric.clone()]);
    }

    let mut selected = select_series_with_retained_execution_result(
        engine,
        &selection_from_promql_matchers(&selector.matchers),
        params.execution,
    )?;
    params.reserve_metric_names(selected.series.iter().map(|series| series.name.as_str()))?;
    let mut metrics = BTreeSet::new();
    for series in std::mem::take(&mut selected.series) {
        metrics.insert(series.name);
    }
    Ok(metrics.into_iter().collect())
}

fn candidate_metrics_for_matrix(
    engine: &Engine,
    selector: &MatrixSelector,
    params: &QueryParams<'_>,
) -> Result<Vec<String>> {
    if let Some(metric) = &selector.vector.metric_name {
        return Ok(vec![metric.clone()]);
    }

    let mut selected = select_series_with_retained_execution_result(
        engine,
        &selection_from_promql_matchers(&selector.vector.matchers),
        params.execution,
    )?;
    params.reserve_metric_names(selected.series.iter().map(|series| series.name.as_str()))?;
    let mut metrics = BTreeSet::new();
    for series in std::mem::take(&mut selected.series) {
        metrics.insert(series.name);
    }
    Ok(metrics.into_iter().collect())
}

pub(super) fn select_series_with_retained_execution_result(
    engine: &Engine,
    selection: &SeriesSelection,
    execution: &QueryExecution,
) -> Result<SelectSeriesExecutionResult> {
    let accounting = engine.storage().select_series_execution_accounting();
    let bounded =
        engine.storage().query_budget().is_some() || execution.limits() != Default::default();
    if bounded && accounting != QueryExecutionAccounting::Complete {
        return Err(TsinkError::UnsupportedOperation {
            operation: "bounded PromQL metadata selection",
            reason: "storage does not provide complete select_series execution accounting"
                .to_string(),
        }
        .into());
    }

    let mut selected = engine
        .storage()
        .select_series_with_execution_result(selection, execution)?;
    if accounting == QueryExecutionAccounting::Complete {
        let reservation = selected.take_memory_reservation().ok_or_else(|| {
            TsinkError::Other(
                "completely accounted PromQL metadata selection omitted its result reservation"
                    .to_string(),
            )
        })?;
        if !selected.series.is_empty() && reservation.bytes() == 0 {
            return Err(TsinkError::Other(
                "completely accounted PromQL metadata selection retained zero bytes for a non-empty result"
                    .to_string(),
            )
            .into());
        }
        return Ok(SelectSeriesExecutionResult::accounted(
            std::mem::take(&mut selected.series),
            reservation,
        ));
    }
    Ok(selected)
}

fn load_exact_series_points_with_retained_execution(
    engine: &Engine,
    metric: &str,
    labels: &[Label],
    start: i64,
    end: i64,
    params: &QueryParams<'_>,
) -> Result<Vec<DataPoint>> {
    let accounting = engine.storage().select_many_execution_accounting();
    let bounded = engine.storage().query_budget().is_some()
        || params.execution.limits() != Default::default();
    if bounded && accounting != QueryExecutionAccounting::Complete {
        return Err(TsinkError::UnsupportedOperation {
            operation: "bounded PromQL point selection",
            reason: "storage does not provide complete select_many execution accounting"
                .to_string(),
        }
        .into());
    }

    let selector = MetricSeries {
        name: metric.to_string(),
        labels: labels.to_vec(),
    };
    params.execution.checkpoint().map_err(TsinkError::from)?;
    let mut selected = engine.storage().select_many_with_execution_result(
        std::slice::from_ref(&selector),
        start,
        end,
        params.execution,
    )?;
    params.execution.checkpoint().map_err(TsinkError::from)?;
    if selected.series.len() != 1 || selected.series[0].series != selector {
        return Err(TsinkError::Other(
            "PromQL exact-series selection returned identities or ordering outside the request"
                .to_string(),
        )
        .into());
    }

    if accounting == QueryExecutionAccounting::Complete {
        let matched = selected.matched_selectors.take().ok_or_else(|| {
            TsinkError::Other(
                "completely accounted PromQL batch selection omitted selector-existence bits"
                    .to_string(),
            )
        })?;
        if matched.len() != 1 {
            return Err(TsinkError::Other(format!(
                "completely accounted PromQL exact-series selection returned {} existence bits",
                matched.len()
            ))
            .into());
        }
        if !matched[0] && !selected.series[0].points.is_empty() {
            return Err(TsinkError::Other(
                "PromQL exact-series selection returned points for a missing selector".to_string(),
            )
            .into());
        }
        let reservation = selected.take_memory_reservation().ok_or_else(|| {
            TsinkError::Other(
                "completely accounted PromQL batch selection omitted its result reservation"
                    .to_string(),
            )
        })?;
        if reservation.bytes() == 0 {
            return Err(TsinkError::Other(
                "completely accounted PromQL batch selection retained zero bytes for a non-empty result"
                    .to_string(),
            )
            .into());
        }
        params.memory.adopt(reservation);
    }

    Ok(selected
        .series
        .pop()
        .expect("validated one exact-series result")
        .points)
}

fn collect_instant_samples_for_metric(
    engine: &Engine,
    metric: &str,
    prepared_matchers: &PreparedPromqlMatchers<'_>,
    start: i64,
    end: i64,
    params: &QueryParams<'_>,
    out: &mut Vec<Sample>,
) -> Result<()> {
    let mut all_series = fetch_metric_series(engine, metric, start, end, params)?;
    for (labels, points) in all_series.rows.drain(..) {
        if !prepared_matchers.matches(metric, &labels) {
            continue;
        }
        if let Some(point) = latest_instant_point(points, start, end) {
            params.reserve_sample(metric, &labels, point.value.as_histogram())?;
            if let Some(histogram) = point.value.as_histogram() {
                out.push(Sample::from_histogram(
                    metric.to_string(),
                    labels,
                    point.timestamp,
                    histogram.clone(),
                ));
            } else {
                out.push(Sample::from_float(
                    metric.to_string(),
                    labels,
                    point.timestamp,
                    value_to_f64(&point.value),
                ));
            }
        }
    }

    Ok(())
}

fn collect_range_series_for_metric(
    engine: &Engine,
    metric: &str,
    prepared_matchers: &PreparedPromqlMatchers<'_>,
    start: i64,
    end: i64,
    params: &QueryParams<'_>,
    out: &mut Vec<Series>,
) -> Result<()> {
    let mut all_series = fetch_metric_series(engine, metric, start, end, params)?;
    for (labels, points) in all_series.rows.drain(..) {
        if !prepared_matchers.matches(metric, &labels) {
            continue;
        }

        params.reserve_range_series_upper(metric, &labels, &points)?;

        let series = points
            .into_iter()
            .filter(|p| p.timestamp > start && p.timestamp < end)
            .filter(|p| !is_stale_nan_value(&p.value))
            .fold(
                Series::new(metric.to_string(), labels),
                |mut series, point| {
                    if let Some(histogram) = point.value.as_histogram() {
                        series
                            .histograms
                            .push((point.timestamp, Box::new(histogram.clone())));
                    } else {
                        series
                            .samples
                            .push((point.timestamp, value_to_f64(&point.value)));
                    }
                    series
                },
            );

        if !series.samples.is_empty() || !series.histograms.is_empty() {
            out.push(series);
        }
    }

    Ok(())
}

fn fetch_metric_series(
    engine: &Engine,
    metric: &str,
    start: i64,
    end: i64,
    params: &QueryParams<'_>,
) -> Result<RetainedMetricRows> {
    if let Some(cache) = params.prefetch {
        if let Some(all) = cache.get(metric) {
            params.memory.reserve(
                params.execution,
                super::modeled_vec_capacity_bytes::<(Vec<Label>, Vec<DataPoint>)>(all.len()),
            )?;
            let mut filtered = Vec::new();
            for (labels, points) in all {
                params.checkpoint()?;
                let selected_count = points
                    .iter()
                    .filter(|point| point.timestamp >= start && point.timestamp < end)
                    .count();
                if selected_count == 0 {
                    continue;
                }
                let retained_bytes = super::modeled_labels_bytes(labels)
                    .saturating_add(super::modeled_vec_capacity_bytes::<DataPoint>(
                        selected_count,
                    ))
                    .saturating_add(
                        points
                            .iter()
                            .filter(|point| point.timestamp >= start && point.timestamp < end)
                            .fold(0u64, |bytes, point| {
                                bytes.saturating_add(super::modeled_value_heap_bytes(&point.value))
                            }),
                    );
                params.memory.reserve(params.execution, retained_bytes)?;
                filtered.push((
                    labels.clone(),
                    points
                        .iter()
                        .filter(|point| point.timestamp >= start && point.timestamp < end)
                        .cloned()
                        .collect(),
                ));
            }
            return Ok(RetainedMetricRows::unreserved(filtered));
        }
    }

    load_metric_rows_with_retained_execution(engine, metric, start, end, params.execution)
}

pub(super) fn load_metric_rows_with_retained_execution(
    engine: &Engine,
    metric: &str,
    start: i64,
    end: i64,
    execution: &QueryExecution,
) -> Result<RetainedMetricRows> {
    let selection = SeriesSelection::new()
        .with_metric(metric.to_string())
        .with_time_range(start, end);
    let mut metadata = select_series_with_retained_execution_result(engine, &selection, execution)?;
    if metadata.series.is_empty() {
        return Ok(RetainedMetricRows::unreserved(Vec::new()));
    }

    let mut selectors = std::mem::take(&mut metadata.series);
    selectors.sort_unstable_by(|left, right| {
        left.labels
            .cmp(&right.labels)
            .then_with(|| left.name.cmp(&right.name))
    });

    let accounting = engine.storage().select_many_execution_accounting();
    let bounded =
        engine.storage().query_budget().is_some() || execution.limits() != Default::default();
    if bounded && accounting != QueryExecutionAccounting::Complete {
        return Err(TsinkError::UnsupportedOperation {
            operation: "bounded PromQL point selection",
            reason: "storage does not provide complete select_many execution accounting"
                .to_string(),
        }
        .into());
    }

    execution.checkpoint().map_err(TsinkError::from)?;
    let mut selected = engine
        .storage()
        .select_many_with_execution_result(&selectors, start, end, execution)?;
    execution.checkpoint().map_err(TsinkError::from)?;
    if selected.series.len() != selectors.len()
        || selected
            .series
            .iter()
            .zip(&selectors)
            .any(|(item, selector)| item.series != *selector)
    {
        return Err(TsinkError::Other(
            "PromQL batch selection returned identities or ordering outside the request"
                .to_string(),
        )
        .into());
    }

    let mut result_reservation = match accounting {
        QueryExecutionAccounting::Complete => {
            let matched = selected.matched_selectors.take().ok_or_else(|| {
                TsinkError::Other(
                    "completely accounted PromQL batch selection omitted selector-existence bits"
                        .to_string(),
                )
            })?;
            if matched.len() != selectors.len() {
                return Err(TsinkError::Other(format!(
                    "completely accounted PromQL batch selection returned {} existence bits for {} selectors",
                    matched.len(),
                    selectors.len()
                ))
                .into());
            }
            let reservation = selected.take_memory_reservation().ok_or_else(|| {
                TsinkError::Other(
                    "completely accounted PromQL batch selection omitted its result reservation"
                        .to_string(),
                )
            })?;
            if !selected.series.is_empty() && reservation.bytes() == 0 {
                return Err(TsinkError::Other(
                    "completely accounted PromQL batch selection retained zero bytes for a non-empty result"
                        .to_string(),
                )
                .into());
            }
            Some(reservation)
        }
        QueryExecutionAccounting::Unaccounted => None,
    };

    execution
        .observe_intermediate_vector_size(u64::try_from(selected.series.len()).unwrap_or(u64::MAX))
        .map_err(TsinkError::from)?;
    let output_slots =
        super::modeled_vec_capacity_bytes::<super::LabelPoints>(selected.series.len());
    let mut output_reservation = execution
        .reserve_memory(output_slots)
        .map_err(TsinkError::from)?;
    let mut rows = Vec::with_capacity(selected.series.len());
    for SeriesPoints { series, points } in std::mem::take(&mut selected.series) {
        execution.checkpoint().map_err(TsinkError::from)?;
        if points.is_empty() {
            continue;
        }
        rows.push((series.labels, points));
    }

    let retained_bytes = super::modeled_prefetch_rows_bytes(&rows);
    let reservation = match result_reservation.as_mut() {
        Some(reservation) => {
            reservation
                .resize(retained_bytes)
                .map_err(TsinkError::from)?;
            drop(output_reservation);
            result_reservation.expect("complete accounting retained a reservation")
        }
        None => {
            output_reservation
                .resize(retained_bytes)
                .map_err(TsinkError::from)?;
            output_reservation
        }
    };

    Ok(RetainedMetricRows {
        rows,
        reservation: Some(reservation),
    })
}

fn latest_point_as_sample(
    metric: &str,
    labels: Vec<Label>,
    points: Vec<DataPoint>,
    start: i64,
    end: i64,
) -> Option<Sample> {
    latest_instant_point(points, start, end).map(|point| {
        if let Some(histogram) = point.value.as_histogram() {
            Sample::from_histogram(
                metric.to_string(),
                labels,
                point.timestamp,
                histogram.clone(),
            )
        } else {
            Sample::from_float(
                metric.to_string(),
                labels,
                point.timestamp,
                value_to_f64(&point.value),
            )
        }
    })
}

fn latest_instant_point(points: Vec<DataPoint>, start: i64, end: i64) -> Option<DataPoint> {
    let mut latest: Option<DataPoint> = None;
    for point in points {
        if point.timestamp < start || point.timestamp >= end {
            continue;
        }
        if latest
            .as_ref()
            .is_some_and(|current| current.timestamp >= point.timestamp)
        {
            continue;
        }
        latest = Some(point);
    }

    match latest {
        Some(point) if is_stale_nan_value(&point.value) => None,
        other => other,
    }
}

fn exact_equal_labels(
    metric: &str,
    matchers: &[crate::promql::ast::LabelMatcher],
) -> Option<Vec<Label>> {
    if matchers.is_empty() {
        return None;
    }

    let mut out = Vec::new();
    for matcher in matchers {
        if matcher.name == "__name__" {
            if matcher.op != MatchOp::Equal || matcher.value != metric {
                return None;
            }
            continue;
        }

        if matcher.op != MatchOp::Equal {
            return None;
        }

        out.push(Label::new(matcher.name.clone(), matcher.value.clone()));
    }
    if out.is_empty() {
        return None;
    }
    out.sort();
    Some(out)
}

fn selection_from_promql_matchers(
    matchers: &[crate::promql::ast::LabelMatcher],
) -> SeriesSelection {
    let mut selection = SeriesSelection::new();
    for matcher in matchers {
        let op = match matcher.op {
            MatchOp::Equal => crate::storage::SeriesMatcherOp::Equal,
            MatchOp::NotEqual => crate::storage::SeriesMatcherOp::NotEqual,
            MatchOp::RegexMatch => crate::storage::SeriesMatcherOp::RegexMatch,
            MatchOp::RegexNoMatch => crate::storage::SeriesMatcherOp::RegexNoMatch,
        };
        selection = selection.with_matcher(SeriesMatcher::new(
            matcher.name.clone(),
            op,
            matcher.value.clone(),
        ));
    }
    selection
}

fn has_exact_series(
    engine: &Engine,
    metric: &str,
    labels: &[Label],
    params: &QueryParams<'_>,
) -> Result<bool> {
    let mut selection = SeriesSelection::new().with_metric(metric.to_string());
    for label in labels {
        selection = selection.with_matcher(SeriesMatcher::equal(&label.name, &label.value));
    }

    let selected =
        select_series_with_retained_execution_result(engine, &selection, params.execution)?;
    for series in &selected.series {
        if labels_equal_unordered(&series.labels, labels) {
            return Ok(true);
        }
    }

    Ok(false)
}

fn labels_equal_unordered(left: &[Label], right: &[Label]) -> bool {
    left.len() == right.len()
        && left.iter().all(|label| {
            left.iter().filter(|candidate| *candidate == label).count()
                == right.iter().filter(|candidate| *candidate == label).count()
        })
}
