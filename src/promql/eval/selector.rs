use std::collections::BTreeSet;

use crate::{DataPoint, Label, SeriesMatcher, SeriesSelection};
use regex::Regex;

use crate::promql::ast::{MatchOp, MatrixSelector, VectorSelector};
use crate::promql::error::{PromqlError, Result};
use crate::promql::types::{is_stale_nan_value, value_to_f64, PromqlValue, Sample, Series};

use super::time::duration_to_units;
use super::{resolve_at_modifier, Engine, QueryParams};

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
            if has_exact_series(engine, metric, &labels)? {
                let points = engine.storage().select(metric, &labels, start, end)?;
                if let Some(sample) = latest_point_as_sample(metric, labels, points, start, end) {
                    out.push(sample);
                }
                return Ok(PromqlValue::InstantVector(out));
            }
        }

        collect_instant_samples_for_metric(engine, metric, selector, start, end, params, &mut out)?;
    } else {
        for metric in candidate_metrics(engine, selector)? {
            collect_instant_samples_for_metric(
                engine, &metric, selector, start, end, params, &mut out,
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
            if has_exact_series(engine, metric, &labels)? {
                let points = engine.storage().select(metric, &labels, start, end)?;
                let samples = points
                    .into_iter()
                    .filter(|p| p.timestamp >= start && p.timestamp < end)
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

        collect_range_series_for_metric(engine, metric, selector, start, end, params, &mut out)?;
    } else {
        for metric in candidate_metrics_for_matrix(engine, selector)? {
            collect_range_series_for_metric(
                engine, &metric, selector, start, end, params, &mut out,
            )?;
        }
    }

    Ok(PromqlValue::RangeVector(out))
}

fn candidate_metrics(engine: &Engine, selector: &VectorSelector) -> Result<Vec<String>> {
    if let Some(metric) = &selector.metric_name {
        return Ok(vec![metric.clone()]);
    }

    let all = engine
        .storage()
        .select_series(&selection_from_promql_matchers(&selector.matchers))?;
    let mut metrics = BTreeSet::new();
    for series in all {
        metrics.insert(series.name);
    }
    Ok(metrics.into_iter().collect())
}

fn candidate_metrics_for_matrix(engine: &Engine, selector: &MatrixSelector) -> Result<Vec<String>> {
    if let Some(metric) = &selector.vector.metric_name {
        return Ok(vec![metric.clone()]);
    }

    let all = engine
        .storage()
        .select_series(&selection_from_promql_matchers(&selector.vector.matchers))?;
    let mut metrics = BTreeSet::new();
    for series in all {
        metrics.insert(series.name);
    }
    Ok(metrics.into_iter().collect())
}

fn collect_instant_samples_for_metric(
    engine: &Engine,
    metric: &str,
    selector: &VectorSelector,
    start: i64,
    end: i64,
    params: &QueryParams<'_>,
    out: &mut Vec<Sample>,
) -> Result<()> {
    let all_series = fetch_metric_series(engine, metric, start, end, params)?;
    for (labels, points) in all_series {
        if !matchers_match(metric, &labels, &selector.matchers)? {
            continue;
        }
        if let Some(point) = latest_instant_point(points, start, end) {
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
    selector: &MatrixSelector,
    start: i64,
    end: i64,
    params: &QueryParams<'_>,
    out: &mut Vec<Series>,
) -> Result<()> {
    let all_series = fetch_metric_series(engine, metric, start, end, params)?;
    for (labels, points) in all_series {
        if !matchers_match(metric, &labels, &selector.vector.matchers)? {
            continue;
        }

        let series = points
            .into_iter()
            .filter(|p| p.timestamp >= start && p.timestamp < end)
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
) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
    if let Some(cache) = params.prefetch {
        if let Some(all) = cache.get(metric) {
            let filtered = all
                .iter()
                .filter_map(|(labels, points)| {
                    let points = points
                        .iter()
                        .filter(|p| p.timestamp >= start && p.timestamp < end)
                        .cloned()
                        .collect::<Vec<_>>();
                    if points.is_empty() {
                        None
                    } else {
                        Some((labels.clone(), points))
                    }
                })
                .collect();
            return Ok(filtered);
        }
    }

    engine
        .storage()
        .select_all(metric, start, end)
        .map_err(Into::into)
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

fn has_exact_series(engine: &Engine, metric: &str, labels: &[Label]) -> Result<bool> {
    let mut expected = labels.to_vec();
    expected.sort();

    let mut selection = SeriesSelection::new().with_metric(metric.to_string());
    for label in labels {
        selection = selection.with_matcher(SeriesMatcher::equal(&label.name, &label.value));
    }

    for series in engine.storage().select_series(&selection)? {
        let mut labels = series.labels;
        labels.sort();
        if labels == expected {
            return Ok(true);
        }
    }

    Ok(false)
}

pub(crate) fn matchers_match(
    metric: &str,
    labels: &[Label],
    matchers: &[crate::promql::ast::LabelMatcher],
) -> Result<bool> {
    for matcher in matchers {
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
            MatchOp::RegexMatch => anchored_regex(&matcher.value)?.is_match(actual),
            MatchOp::RegexNoMatch => !anchored_regex(&matcher.value)?.is_match(actual),
        };

        if !matched {
            return Ok(false);
        }
    }

    Ok(true)
}

fn anchored_regex(pattern: &str) -> Result<Regex> {
    let anchored = format!("^(?:{pattern})$");
    Regex::new(&anchored).map_err(PromqlError::from)
}
