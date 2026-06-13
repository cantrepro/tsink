use std::collections::BTreeMap;

use crate::{label::canonical_series_identity, Label};
use chrono::{Datelike, TimeZone, Timelike, Utc};
use regex::Regex;

use crate::promql::ast::{CallExpr, Expr, LabelMatcher, MatchOp};
use crate::promql::error::{PromqlError, Result};
use crate::promql::types::{
    histogram_count_value, histogram_counter_reset_detected, histogram_quantile_native,
    histogram_scale, histogram_sub, is_stale_nan_value, PromqlValue, Sample,
};

use super::time::duration_to_units;
use super::{resolve_at_modifier, selector::matchers_match, Engine, QueryParams};

pub(crate) fn eval_call(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    let func = call.func.to_ascii_lowercase();

    match func.as_str() {
        "rate" => eval_rate_like(engine, call, params, RateKind::Rate),
        "irate" => eval_rate_like(engine, call, params, RateKind::Irate),
        "increase" => eval_rate_like(engine, call, params, RateKind::Increase),
        "delta" => eval_rate_like(engine, call, params, RateKind::Delta),
        "idelta" => eval_rate_like(engine, call, params, RateKind::Idelta),
        "changes" => eval_rate_like(engine, call, params, RateKind::Changes),
        "resets" => eval_rate_like(engine, call, params, RateKind::Resets),
        "avg_over_time" => eval_over_time(engine, call, params, OverTimeKind::Avg),
        "sum_over_time" => eval_over_time(engine, call, params, OverTimeKind::Sum),
        "min_over_time" => eval_over_time(engine, call, params, OverTimeKind::Min),
        "max_over_time" => eval_over_time(engine, call, params, OverTimeKind::Max),
        "count_over_time" => eval_over_time(engine, call, params, OverTimeKind::Count),
        "last_over_time" => eval_over_time(engine, call, params, OverTimeKind::Last),
        "present_over_time" => eval_over_time(engine, call, params, OverTimeKind::Present),
        "stddev_over_time" => eval_over_time(engine, call, params, OverTimeKind::Stddev),
        "stdvar_over_time" => eval_over_time(engine, call, params, OverTimeKind::Stdvar),
        "mad_over_time" => eval_mad_over_time(engine, call, params),
        "quantile_over_time" => eval_quantile_over_time(engine, call, params),
        "deriv" => eval_deriv(engine, call, params),
        "histogram_avg" => eval_histogram_native_unary(engine, call, params),
        "histogram_count" => eval_histogram_native_unary(engine, call, params),
        "histogram_sum" => eval_histogram_native_unary(engine, call, params),
        "histogram_stddev" => eval_histogram_native_unary(engine, call, params),
        "histogram_stdvar" => eval_histogram_native_unary(engine, call, params),
        "histogram_fraction" => eval_histogram_fraction(engine, call, params),
        "histogram_quantile" => eval_histogram_quantile(engine, call, params),
        "double_exponential_smoothing" | "holt_winters" => {
            eval_double_exponential_smoothing(engine, call, params)
        }
        "count_scalar" => eval_count_scalar(engine, call, params),
        "pi" => eval_pi(engine, call, params),
        "sgn" => eval_unary_map(engine, call, params, |v| {
            if v.is_nan() {
                f64::NAN
            } else {
                v.signum()
            }
        }),
        "acos" => eval_unary_map(engine, call, params, |v| v.acos()),
        "acosh" => eval_unary_map(engine, call, params, |v| v.acosh()),
        "asin" => eval_unary_map(engine, call, params, |v| v.asin()),
        "asinh" => eval_unary_map(engine, call, params, |v| v.asinh()),
        "atan" => eval_unary_map(engine, call, params, |v| v.atan()),
        "atanh" => eval_unary_map(engine, call, params, |v| v.atanh()),
        "cos" => eval_unary_map(engine, call, params, |v| v.cos()),
        "cosh" => eval_unary_map(engine, call, params, |v| v.cosh()),
        "sin" => eval_unary_map(engine, call, params, |v| v.sin()),
        "sinh" => eval_unary_map(engine, call, params, |v| v.sinh()),
        "tan" => eval_unary_map(engine, call, params, |v| v.tan()),
        "tanh" => eval_unary_map(engine, call, params, |v| v.tanh()),
        "deg" => eval_unary_map(engine, call, params, |v| v.to_degrees()),
        "rad" => eval_unary_map(engine, call, params, |v| v.to_radians()),
        "predict_linear" => eval_predict_linear(engine, call, params),
        "minute" => eval_time_component(engine, call, params, |dt| dt.minute() as f64),
        "hour" => eval_time_component(engine, call, params, |dt| dt.hour() as f64),
        "day_of_week" => eval_time_component(engine, call, params, |dt| {
            dt.weekday().num_days_from_sunday() as f64
        }),
        "day_of_month" => eval_time_component(engine, call, params, |dt| dt.day() as f64),
        "day_of_year" => eval_time_component(engine, call, params, |dt| dt.ordinal() as f64),
        "days_in_month" => eval_time_component(engine, call, params, |dt| days_in_month(dt) as f64),
        "month" => eval_time_component(engine, call, params, |dt| dt.month() as f64),
        "year" => eval_time_component(engine, call, params, |dt| dt.year() as f64),
        "abs" => eval_unary_map(engine, call, params, |v| v.abs()),
        "ceil" => eval_unary_map(engine, call, params, |v| v.ceil()),
        "floor" => eval_unary_map(engine, call, params, |v| v.floor()),
        "round" => eval_round(engine, call, params),
        "sqrt" => eval_unary_map(engine, call, params, |v| v.sqrt()),
        "exp" => eval_unary_map(engine, call, params, |v| v.exp()),
        "ln" => eval_unary_map(engine, call, params, |v| v.ln()),
        "log2" => eval_unary_map(engine, call, params, |v| v.log2()),
        "log10" => eval_unary_map(engine, call, params, |v| v.log10()),
        "clamp" => eval_clamp(engine, call, params),
        "clamp_min" => eval_clamp_min_max(engine, call, params, true),
        "clamp_max" => eval_clamp_min_max(engine, call, params, false),
        "scalar" => eval_scalar(engine, call, params),
        "vector" => eval_vector(engine, call, params),
        "time" => eval_time(engine, call, params),
        "timestamp" => eval_timestamp(engine, call, params),
        "sort" => eval_sort(engine, call, params, false),
        "sort_desc" => eval_sort(engine, call, params, true),
        "sort_by_label" => eval_sort_by_label(engine, call, params, false),
        "sort_by_label_desc" => eval_sort_by_label(engine, call, params, true),
        "info" => eval_info(engine, call, params),
        "absent" => eval_absent(engine, call, params),
        "absent_over_time" => eval_absent_over_time(engine, call, params),
        "drop_common_labels" => eval_drop_common_labels(engine, call, params),
        "label_replace" => eval_label_replace(engine, call, params),
        "label_join" => eval_label_join(engine, call, params),
        _ => Err(PromqlError::UnknownFunction(call.func.clone())),
    }
}

#[derive(Clone, Copy, Debug)]
enum RateKind {
    Rate,
    Irate,
    Increase,
    Delta,
    Idelta,
    Changes,
    Resets,
}

fn range_series_has_histograms(series: &crate::promql::types::Series) -> bool {
    !series.histograms.is_empty()
}

fn ensure_histogram_free_series(series: &crate::promql::types::Series, func: &str) -> Result<()> {
    if range_series_has_histograms(series) {
        return Err(PromqlError::Eval(format!(
            "{func} does not support histogram samples yet"
        )));
    }
    Ok(())
}

fn ensure_histogram_free_vector(vector: &[Sample], func: &str) -> Result<()> {
    if vector.iter().any(|sample| sample.histogram.is_some()) {
        return Err(PromqlError::Eval(format!(
            "{func} does not support histogram samples yet"
        )));
    }
    Ok(())
}

fn eval_rate_like(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
    kind: RateKind,
) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let window = range_window(&call.args[0], engine, params)?;
    let arg = engine.eval(&call.args[0], params)?;
    let series = expect_range_vector(arg, &call.func)?;

    let mut out = Vec::new();
    for mut s in series {
        if range_series_has_histograms(&s) {
            if !s.samples.is_empty() {
                return Err(PromqlError::Eval(format!(
                    "{} does not support mixed float and histogram samples in the same series",
                    call.func
                )));
            }

            let Some(histogram) = eval_histogram_rate_like_series(
                &s.histograms,
                window,
                kind,
                engine.timestamp_units_per_second(),
            )?
            else {
                continue;
            };

            out.push(Sample::from_histogram(
                std::mem::take(&mut s.metric),
                std::mem::take(&mut s.labels),
                params.eval_time,
                histogram,
            ));
            continue;
        }

        let value = match kind {
            RateKind::Rate => extrapolated_delta(
                &s.samples,
                window,
                true,
                true,
                engine.timestamp_units_per_second(),
            ),
            RateKind::Irate => irate_value(&s.samples, engine.timestamp_units_per_second()),
            RateKind::Increase => extrapolated_delta(
                &s.samples,
                window,
                true,
                false,
                engine.timestamp_units_per_second(),
            ),
            RateKind::Delta => extrapolated_delta(
                &s.samples,
                window,
                false,
                false,
                engine.timestamp_units_per_second(),
            ),
            RateKind::Idelta => idelta_value(&s.samples),
            RateKind::Changes => changes_value(&s.samples),
            RateKind::Resets => resets_value(&s.samples),
        };
        let Some(value) = value else {
            continue;
        };

        out.push(Sample::from_float(
            std::mem::take(&mut s.metric),
            std::mem::take(&mut s.labels),
            params.eval_time,
            value,
        ));
    }

    Ok(PromqlValue::InstantVector(out))
}

fn irate_value(samples: &[(i64, f64)], units_per_second: i64) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let a = samples[samples.len() - 2];
    let b = samples[samples.len() - 1];
    let dt = b.0.saturating_sub(a.0);
    if dt <= 0 {
        return None;
    }
    let mut dv = b.1 - a.1;
    if dv < 0.0 {
        dv = b.1;
    }
    Some(dv / (dt as f64 / units_per_second as f64))
}

fn counter_increase_value(samples: &[(i64, f64)]) -> Option<f64> {
    let mut total = 0.0;
    let mut prev = samples[0].1;
    for &(_, value) in &samples[1..] {
        if value >= prev {
            total += value - prev;
        } else {
            total += value;
        }
        prev = value;
    }
    Some(total)
}

fn sample_delta_value(samples: &[(i64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    Some(samples.last()?.1 - samples.first()?.1)
}

fn idelta_value(samples: &[(i64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    Some(samples[samples.len() - 1].1 - samples[samples.len() - 2].1)
}

fn extrapolated_delta(
    samples: &[(i64, f64)],
    window: Option<(i64, i64)>,
    is_counter: bool,
    is_rate: bool,
    units_per_second: i64,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }

    let (range_start, range_end) = window?;
    let sampled_interval = samples.last()?.0.saturating_sub(samples.first()?.0);
    if sampled_interval <= 0 {
        return None;
    }

    let mut result = if is_counter {
        counter_increase_value(samples)?
    } else {
        sample_delta_value(samples)?
    };

    let average_duration_between_samples = sampled_interval as f64 / (samples.len() - 1) as f64;
    let mut duration_to_start = (samples.first()?.0.saturating_sub(range_start)) as f64;
    let duration_to_end = (range_end.saturating_sub(samples.last()?.0)) as f64;

    if is_counter && result > 0.0 && samples.first()?.1 >= 0.0 {
        let duration_to_zero = sampled_interval as f64 * (samples.first()?.1 / result);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }

    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut extrapolated_interval = sampled_interval as f64;

    if duration_to_start < extrapolation_threshold {
        extrapolated_interval += duration_to_start;
    } else {
        extrapolated_interval += average_duration_between_samples / 2.0;
    }

    if duration_to_end < extrapolation_threshold {
        extrapolated_interval += duration_to_end;
    } else {
        extrapolated_interval += average_duration_between_samples / 2.0;
    }

    result *= extrapolated_interval / sampled_interval as f64;

    if is_rate {
        let range_duration = range_end.saturating_sub(range_start);
        if range_duration <= 0 {
            return None;
        }
        result /= range_duration as f64 / units_per_second as f64;
    }

    Some(result)
}

fn eval_histogram_rate_like_series(
    samples: &[(i64, Box<crate::NativeHistogram>)],
    window: Option<(i64, i64)>,
    kind: RateKind,
    units_per_second: i64,
) -> Result<Option<crate::NativeHistogram>> {
    match kind {
        RateKind::Rate => {
            histogram_extrapolated_delta(samples, window, true, true, units_per_second)
        }
        RateKind::Increase => {
            histogram_extrapolated_delta(samples, window, true, false, units_per_second)
        }
        _ => Err(PromqlError::Eval(format!(
            "{:?} does not support histogram samples yet",
            kind
        ))),
    }
}

fn histogram_extrapolated_delta(
    samples: &[(i64, Box<crate::NativeHistogram>)],
    window: Option<(i64, i64)>,
    is_counter: bool,
    is_rate: bool,
    units_per_second: i64,
) -> Result<Option<crate::NativeHistogram>> {
    if samples.len() < 2 {
        return Ok(None);
    }
    if is_counter
        && samples
            .iter()
            .any(|(_, histogram)| matches!(histogram.reset_hint, crate::HistogramResetHint::Gauge))
    {
        return Err(PromqlError::Eval(
            "counter-style histogram functions do not support gauge histograms yet".to_string(),
        ));
    }

    let (range_start, range_end) = match window {
        Some(window) => window,
        None => return Ok(None),
    };

    let sampled_interval = samples
        .last()
        .map(|(ts, _)| *ts)
        .unwrap_or_default()
        .saturating_sub(samples.first().map(|(ts, _)| *ts).unwrap_or_default());
    if sampled_interval <= 0 {
        return Ok(None);
    }

    let mut result = if is_counter {
        histogram_counter_increase_value(samples)?
    } else {
        histogram_sub(&samples.last().expect("len checked").1, &samples[0].1)
            .map_err(PromqlError::Eval)?
    };

    let average_duration_between_samples = sampled_interval as f64 / (samples.len() - 1) as f64;
    let duration_to_start = (samples
        .first()
        .map(|(ts, _)| *ts)
        .unwrap_or(range_start)
        .saturating_sub(range_start)) as f64;
    let duration_to_end =
        (range_end.saturating_sub(samples.last().map(|(ts, _)| *ts).unwrap_or(range_end))) as f64;
    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut extrapolated_interval = sampled_interval as f64;

    if duration_to_start < extrapolation_threshold {
        extrapolated_interval += duration_to_start;
    } else {
        extrapolated_interval += average_duration_between_samples / 2.0;
    }
    if duration_to_end < extrapolation_threshold {
        extrapolated_interval += duration_to_end;
    } else {
        extrapolated_interval += average_duration_between_samples / 2.0;
    }

    result = histogram_scale(&result, extrapolated_interval / sampled_interval as f64)
        .map_err(PromqlError::Eval)?;

    if is_rate {
        let range_duration = range_end.saturating_sub(range_start);
        if range_duration <= 0 {
            return Ok(None);
        }
        result = histogram_scale(&result, units_per_second as f64 / range_duration as f64)
            .map_err(PromqlError::Eval)?;
    }

    Ok(Some(result))
}

fn histogram_counter_increase_value(
    samples: &[(i64, Box<crate::NativeHistogram>)],
) -> Result<crate::NativeHistogram> {
    let mut total = histogram_sub(&samples[1].1, &samples[0].1).map_err(PromqlError::Eval)?;
    if histogram_counter_reset_detected(&samples[0].1, &samples[1].1).map_err(PromqlError::Eval)? {
        total = samples[1].1.as_ref().clone();
    }

    let mut prev = samples[1].1.as_ref().clone();
    for (_, histogram) in &samples[2..] {
        if histogram_counter_reset_detected(&prev, histogram).map_err(PromqlError::Eval)? {
            total = crate::promql::types::histogram_add(&total, histogram)
                .map_err(PromqlError::Eval)?;
        } else {
            total = crate::promql::types::histogram_add(
                &total,
                &histogram_sub(histogram, &prev).map_err(PromqlError::Eval)?,
            )
            .map_err(PromqlError::Eval)?;
        }
        prev = histogram.as_ref().clone();
    }
    Ok(total)
}

fn changes_value(samples: &[(i64, f64)]) -> Option<f64> {
    let mut iter = samples.iter();
    let (_, mut prev) = iter.next().copied()?;

    let mut total = 0.0;
    for &(_, value) in iter {
        if value != prev {
            total += 1.0;
        }
        prev = value;
    }

    Some(total)
}

fn resets_value(samples: &[(i64, f64)]) -> Option<f64> {
    let mut iter = samples.iter();
    let (_, mut prev) = iter.next().copied()?;

    let mut total = 0.0;
    for &(_, value) in iter {
        if value < prev {
            total += 1.0;
        }
        prev = value;
    }

    Some(total)
}

fn range_window(
    expr: &Expr,
    engine: &Engine,
    params: &QueryParams<'_>,
) -> Result<Option<(i64, i64)>> {
    match expr {
        Expr::MatrixSelector(selector) => {
            let base_eval = resolve_at_modifier(
                selector.vector.at.as_ref(),
                params,
                engine.timestamp_units_per_second(),
            )?;
            let offset =
                duration_to_units(selector.vector.offset, engine.timestamp_units_per_second());
            let eval_at = base_eval.saturating_sub(offset);
            let range = duration_to_units(selector.range, engine.timestamp_units_per_second());
            Ok(Some((eval_at.saturating_sub(range), eval_at)))
        }
        Expr::Subquery(subquery) => {
            let base_eval = resolve_at_modifier(
                subquery.at.as_ref(),
                params,
                engine.timestamp_units_per_second(),
            )?;
            let offset = duration_to_units(subquery.offset, engine.timestamp_units_per_second());
            let eval_at = base_eval.saturating_sub(offset);
            let range = duration_to_units(subquery.range, engine.timestamp_units_per_second());
            Ok(Some((eval_at.saturating_sub(range), eval_at)))
        }
        Expr::Paren(inner) => range_window(inner, engine, params),
        _ => Ok(None),
    }
}

#[derive(Clone, Copy)]
enum OverTimeKind {
    Avg,
    Sum,
    Min,
    Max,
    Count,
    Last,
    Present,
    Stddev,
    Stdvar,
}

fn eval_over_time(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
    kind: OverTimeKind,
) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let arg = engine.eval(&call.args[0], params)?;
    let series = expect_range_vector(arg, &call.func)?;

    let mut out = Vec::new();
    for mut s in series {
        ensure_histogram_free_series(&s, &call.func)?;
        if s.samples.is_empty() {
            continue;
        }

        let values = s.samples.iter().map(|(_, v)| *v);
        let value = match kind {
            OverTimeKind::Avg => {
                let mut sum = 0.0;
                let mut count = 0.0;
                for v in values {
                    sum += v;
                    count += 1.0;
                }
                sum / count
            }
            OverTimeKind::Sum => values.sum(),
            OverTimeKind::Min => values.fold(f64::INFINITY, f64::min),
            OverTimeKind::Max => values.fold(f64::NEG_INFINITY, f64::max),
            OverTimeKind::Count => s.samples.len() as f64,
            OverTimeKind::Last => s
                .samples
                .last()
                .map(|(_, value)| *value)
                .unwrap_or(f64::NAN),
            OverTimeKind::Present => 1.0,
            OverTimeKind::Stddev => variance(&s.samples).sqrt(),
            OverTimeKind::Stdvar => variance(&s.samples),
        };

        out.push(Sample {
            metric: std::mem::take(&mut s.metric),
            labels: std::mem::take(&mut s.labels),
            timestamp: params.eval_time,
            value,
            histogram: None,
        });
    }

    Ok(PromqlValue::InstantVector(out))
}

fn variance(samples: &[(i64, f64)]) -> f64 {
    let count = samples.len() as f64;
    if count == 0.0 {
        return f64::NAN;
    }

    let mean = samples.iter().map(|(_, value)| *value).sum::<f64>() / count;
    samples
        .iter()
        .map(|(_, value)| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f64>()
        / count
}

fn quantile(values: &mut [f64], phi: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    if phi.is_nan() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }

    values.sort_by(|a, b| a.total_cmp(b));
    if values.len() == 1 {
        return values[0];
    }

    let rank = phi * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        return values[lower];
    }

    let weight = rank - lower as f64;
    values[lower] + (values[upper] - values[lower]) * weight
}

fn deriv_value(samples: &[(i64, f64)], units_per_second: i64) -> Option<f64> {
    regression(samples, units_per_second).map(|(slope, _)| slope)
}

fn regression(samples: &[(i64, f64)], units_per_second: i64) -> Option<(f64, f64)> {
    if samples.len() < 2 {
        return None;
    }
    if samples.iter().any(|(_, value)| !value.is_finite()) {
        return Some((f64::NAN, f64::NAN));
    }

    let first_ts = samples.first()?.0;
    let scale = units_per_second as f64;

    let mut count = 0.0;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xy = 0.0;
    let mut sum_x2 = 0.0;

    for &(timestamp, value) in samples {
        let x = (timestamp - first_ts) as f64 / scale;
        count += 1.0;
        sum_x += x;
        sum_y += value;
        sum_xy += x * value;
        sum_x2 += x * x;
    }

    let denominator = count * sum_x2 - sum_x * sum_x;
    if denominator == 0.0 {
        return None;
    }

    let slope = (count * sum_xy - sum_x * sum_y) / denominator;
    let intercept = (sum_y - slope * sum_x) / count;
    Some((slope, intercept))
}

fn eval_quantile_over_time(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    expect_arg_count(call, 2)?;
    let phi = expect_scalar(engine.eval(&call.args[0], params)?, &call.func)?;
    let arg = engine.eval(&call.args[1], params)?;
    let series = expect_range_vector(arg, &call.func)?;

    let mut out = Vec::new();
    for mut s in series {
        ensure_histogram_free_series(&s, &call.func)?;
        let mut values = s
            .samples
            .iter()
            .map(|(_, value)| *value)
            .collect::<Vec<_>>();
        if values.is_empty() {
            continue;
        }

        out.push(Sample {
            metric: std::mem::take(&mut s.metric),
            labels: std::mem::take(&mut s.labels),
            timestamp: params.eval_time,
            value: quantile(&mut values, phi),
            histogram: None,
        });
    }

    Ok(PromqlValue::InstantVector(out))
}

fn eval_mad_over_time(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let arg = engine.eval(&call.args[0], params)?;
    let series = expect_range_vector(arg, &call.func)?;

    let mut out = Vec::new();
    for mut s in series {
        ensure_histogram_free_series(&s, &call.func)?;
        if s.samples.is_empty() {
            continue;
        }

        let mut values = s
            .samples
            .iter()
            .map(|(_, value)| *value)
            .collect::<Vec<_>>();
        let median = quantile(&mut values, 0.5);
        let mut deviations = s
            .samples
            .iter()
            .map(|(_, value)| (*value - median).abs())
            .collect::<Vec<_>>();
        let mad = quantile(&mut deviations, 0.5);

        out.push(Sample {
            metric: std::mem::take(&mut s.metric),
            labels: std::mem::take(&mut s.labels),
            timestamp: params.eval_time,
            value: mad,
            histogram: None,
        });
    }

    Ok(PromqlValue::InstantVector(out))
}

fn eval_deriv(engine: &Engine, call: &CallExpr, params: &QueryParams<'_>) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let arg = engine.eval(&call.args[0], params)?;
    let series = expect_range_vector(arg, &call.func)?;

    let mut out = Vec::new();
    for mut s in series {
        ensure_histogram_free_series(&s, &call.func)?;
        let Some(value) = deriv_value(&s.samples, engine.timestamp_units_per_second()) else {
            continue;
        };

        out.push(Sample {
            metric: std::mem::take(&mut s.metric),
            labels: std::mem::take(&mut s.labels),
            timestamp: params.eval_time,
            value,
            histogram: None,
        });
    }

    Ok(PromqlValue::InstantVector(out))
}

fn eval_predict_linear(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    expect_arg_count(call, 2)?;
    let seconds_ahead = expect_scalar(engine.eval(&call.args[1], params)?, &call.func)?;
    let arg = engine.eval(&call.args[0], params)?;
    let series = expect_range_vector(arg, &call.func)?;

    let units_per_second = engine.timestamp_units_per_second();
    let mut out = Vec::new();
    for mut s in series {
        ensure_histogram_free_series(&s, &call.func)?;
        let Some((slope, intercept)) = regression(&s.samples, units_per_second) else {
            continue;
        };
        let origin = s
            .samples
            .first()
            .map(|(ts, _)| *ts)
            .unwrap_or(params.eval_time) as f64
            / units_per_second as f64;
        let eval_time = params.eval_time as f64 / units_per_second as f64;
        let x = eval_time + seconds_ahead - origin;
        let value = slope * x + intercept;

        out.push(Sample {
            metric: std::mem::take(&mut s.metric),
            labels: std::mem::take(&mut s.labels),
            timestamp: params.eval_time,
            value,
            histogram: None,
        });
    }

    Ok(PromqlValue::InstantVector(out))
}

fn eval_double_exponential_smoothing(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    expect_arg_count(call, 3)?;
    let smoothing_factor = expect_scalar(engine.eval(&call.args[1], params)?, &call.func)?;
    let trend_factor = expect_scalar(engine.eval(&call.args[2], params)?, &call.func)?;
    if !(0.0..=1.0).contains(&smoothing_factor) || !(0.0..=1.0).contains(&trend_factor) {
        return Err(PromqlError::Eval(
            "double_exponential_smoothing parameters must be within [0, 1]".to_string(),
        ));
    }

    let arg = engine.eval(&call.args[0], params)?;
    let series = expect_range_vector(arg, &call.func)?;

    let mut out = Vec::new();
    for mut s in series {
        ensure_histogram_free_series(&s, &call.func)?;
        let Some(value) =
            double_exponential_smoothing_value(&s.samples, smoothing_factor, trend_factor)
        else {
            continue;
        };

        out.push(Sample {
            metric: std::mem::take(&mut s.metric),
            labels: std::mem::take(&mut s.labels),
            timestamp: params.eval_time,
            value,
            histogram: None,
        });
    }

    Ok(PromqlValue::InstantVector(out))
}

fn double_exponential_smoothing_value(
    samples: &[(i64, f64)],
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    if samples.iter().any(|(_, value)| !value.is_finite()) {
        return Some(f64::NAN);
    }

    let mut level = samples[0].1;
    let mut trend = samples[1].1 - samples[0].1;

    for &(_, value) in &samples[1..] {
        let previous_level = level;
        level = smoothing_factor * value + (1.0 - smoothing_factor) * (level + trend);
        trend = trend_factor * (level - previous_level) + (1.0 - trend_factor) * trend;
    }

    Some(level + trend)
}

#[derive(Clone)]
struct HistogramBucketGroup {
    metric: String,
    labels: Vec<Label>,
    timestamp: i64,
    buckets: Vec<(f64, f64)>,
}

fn eval_histogram_quantile(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    expect_arg_count(call, 2)?;
    let phi = expect_scalar(engine.eval(&call.args[0], params)?, &call.func)?;
    let vector = expect_instant_vector(engine.eval(&call.args[1], params)?, &call.func)?;

    let mut groups: BTreeMap<Vec<u8>, HistogramBucketGroup> = BTreeMap::new();
    let mut out = Vec::new();
    for sample in vector {
        if let Some(histogram) = sample.histogram() {
            let value = histogram_quantile_native(phi, histogram).map_err(PromqlError::Eval)?;
            out.push(Sample::from_float(
                sample.metric,
                sample.labels,
                sample.timestamp,
                value,
            ));
            continue;
        }
        let mut upper_bound = None;
        let mut labels = Vec::with_capacity(sample.labels.len());
        for label in sample.labels {
            if label.name == "le" {
                upper_bound = parse_histogram_bucket_bound(&label.value);
            } else {
                labels.push(label);
            }
        }

        let Some(upper_bound) = upper_bound else {
            continue;
        };

        labels.sort();
        let metric = histogram_metric_name(&sample.metric);
        let key = histogram_group_key(&metric, &labels);
        let entry = groups.entry(key).or_insert_with(|| HistogramBucketGroup {
            metric,
            labels,
            timestamp: sample.timestamp,
            buckets: Vec::new(),
        });
        entry.timestamp = entry.timestamp.max(sample.timestamp);
        entry.buckets.push((upper_bound, sample.value));
    }

    for (_, mut group) in groups {
        let value = histogram_quantile_value(phi, &mut group.buckets);
        out.push(Sample::from_float(
            group.metric,
            group.labels,
            group.timestamp,
            value,
        ));
    }

    Ok(PromqlValue::InstantVector(out))
}

fn eval_histogram_native_unary(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let vector = expect_instant_vector(engine.eval(&call.args[0], params)?, &call.func)?;
    let func = call.func.to_ascii_lowercase();
    let mut out = Vec::new();
    for sample in vector {
        let Some(histogram) = sample.histogram() else {
            continue;
        };
        let value = match func.as_str() {
            "histogram_count" => histogram_count_value(histogram),
            "histogram_sum" => histogram.sum,
            "histogram_avg" => {
                let count = histogram_count_value(histogram);
                if count == 0.0 {
                    f64::NAN
                } else {
                    histogram.sum / count
                }
            }
            other => {
                return Err(PromqlError::Eval(format!(
                    "{other} does not support native histogram samples yet"
                )))
            }
        };
        out.push(Sample::from_float(
            sample.metric,
            sample.labels,
            sample.timestamp,
            value,
        ));
    }
    Ok(PromqlValue::InstantVector(out))
}

fn eval_histogram_fraction(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    expect_arg_count(call, 3)?;
    let _lower = expect_scalar(engine.eval(&call.args[0], params)?, &call.func)?;
    let _upper = expect_scalar(engine.eval(&call.args[1], params)?, &call.func)?;
    let vector = expect_instant_vector(engine.eval(&call.args[2], params)?, &call.func)?;
    if vector.iter().any(|sample| sample.histogram.is_some()) {
        return Err(PromqlError::Eval(
            "histogram_fraction does not support native histogram samples yet".to_string(),
        ));
    }
    Ok(PromqlValue::InstantVector(Vec::new()))
}

fn eval_count_scalar(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let value = engine.eval(&call.args[0], params)?;
    let vector = expect_instant_vector(value, &call.func)?;
    let ts = vector
        .iter()
        .map(|sample| sample.timestamp)
        .max()
        .unwrap_or(params.eval_time);
    Ok(PromqlValue::Scalar(vector.len() as f64, ts))
}

fn eval_pi(engine: &Engine, call: &CallExpr, params: &QueryParams<'_>) -> Result<PromqlValue> {
    expect_arg_count(call, 0)?;
    let _ = engine;
    Ok(PromqlValue::Scalar(std::f64::consts::PI, params.eval_time))
}

fn eval_time_component(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
    component: impl Fn(chrono::DateTime<Utc>) -> f64,
) -> Result<PromqlValue> {
    let mut vector = instant_vector_arg_or_default(engine, call, params)?;
    ensure_histogram_free_vector(&vector, &call.func)?;
    for sample in &mut vector {
        sample.value = timestamp_component(sample.value, &component);
    }
    Ok(PromqlValue::InstantVector(vector))
}

fn instant_vector_arg_or_default(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<Vec<Sample>> {
    match call.args.len() {
        0 => Ok(vec![Sample {
            metric: String::new(),
            labels: Vec::new(),
            timestamp: params.eval_time,
            value: params.eval_time as f64 / engine.timestamp_units_per_second() as f64,
            histogram: None,
        }]),
        1 => match engine.eval(&call.args[0], params)? {
            PromqlValue::InstantVector(vector) => Ok(vector),
            PromqlValue::Scalar(value, ts) => Ok(vec![Sample {
                metric: String::new(),
                labels: Vec::new(),
                timestamp: ts,
                value,
                histogram: None,
            }]),
            other => Err(PromqlError::Type(format!(
                "{} expects scalar or instant vector, got {other:?}",
                call.func
            ))),
        },
        got => Err(PromqlError::ArgumentCount {
            func: call.func.clone(),
            expected: "0 or 1".to_string(),
            got,
        }),
    }
}

fn timestamp_component(
    timestamp_seconds: f64,
    component: &impl Fn(chrono::DateTime<Utc>) -> f64,
) -> f64 {
    if !timestamp_seconds.is_finite() {
        return f64::NAN;
    }

    let secs = timestamp_seconds.trunc();
    if secs < i64::MIN as f64 || secs > i64::MAX as f64 {
        return f64::NAN;
    }

    match Utc.timestamp_opt(secs as i64, 0).single() {
        Some(dt) => component(dt),
        None => f64::NAN,
    }
}

fn days_in_month(dt: chrono::DateTime<Utc>) -> u32 {
    let year = dt.year();
    let month = dt.month();
    let next_month = if month == 12 {
        Utc.with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0).single()
    } else {
        Utc.with_ymd_and_hms(year, month + 1, 1, 0, 0, 0).single()
    };

    next_month
        .map(|next| (next - chrono::Duration::days(1)).day())
        .unwrap_or(31)
}

fn parse_histogram_bucket_bound(value: &str) -> Option<f64> {
    if value.eq_ignore_ascii_case("+inf") || value.eq_ignore_ascii_case("inf") {
        Some(f64::INFINITY)
    } else if value.eq_ignore_ascii_case("-inf") {
        Some(f64::NEG_INFINITY)
    } else {
        value.parse::<f64>().ok()
    }
}

fn histogram_metric_name(metric: &str) -> String {
    metric.strip_suffix("_bucket").unwrap_or(metric).to_string()
}

fn histogram_group_key(metric: &str, labels: &[Label]) -> Vec<u8> {
    canonical_series_identity(metric, labels)
}

fn histogram_quantile_value(phi: f64, buckets: &mut [(f64, f64)]) -> f64 {
    if phi.is_nan() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }
    if buckets.len() < 2 {
        return f64::NAN;
    }

    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    let last_upper = buckets.last().map(|bucket| bucket.0).unwrap_or(f64::NAN);
    if !last_upper.is_infinite() || !last_upper.is_sign_positive() {
        return f64::NAN;
    }

    for idx in 1..buckets.len() {
        if buckets[idx].1 < buckets[idx - 1].1 {
            buckets[idx].1 = buckets[idx - 1].1;
        }
    }

    let total = buckets.last().map(|bucket| bucket.1).unwrap_or(0.0);
    if !total.is_finite() || total <= 0.0 {
        return f64::NAN;
    }

    let rank = phi * total;
    if rank >= total {
        return buckets[buckets.len() - 2].0;
    }

    let mut prev_upper = 0.0;
    let mut prev_count = 0.0;
    for (idx, (upper, count)) in buckets.iter().copied().enumerate() {
        if rank <= count {
            if idx == 0 {
                if upper <= 0.0 || count <= 0.0 {
                    return upper;
                }
                return upper * (rank / count);
            }

            let bucket_count = count - prev_count;
            if bucket_count <= 0.0 {
                return upper;
            }

            return prev_upper + (upper - prev_upper) * ((rank - prev_count) / bucket_count);
        }
        prev_upper = upper;
        prev_count = count;
    }

    buckets[buckets.len() - 2].0
}

fn eval_unary_map(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
    map: impl Fn(f64) -> f64,
) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let value = engine.eval(&call.args[0], params)?;
    map_scalar_or_vector(value, map)
}

fn eval_round(engine: &Engine, call: &CallExpr, params: &QueryParams<'_>) -> Result<PromqlValue> {
    if !(1..=2).contains(&call.args.len()) {
        return Err(PromqlError::ArgumentCount {
            func: call.func.clone(),
            expected: "1 or 2".to_string(),
            got: call.args.len(),
        });
    }

    let value = engine.eval(&call.args[0], params)?;
    let to_nearest = if call.args.len() == 2 {
        expect_scalar(engine.eval(&call.args[1], params)?, &call.func)?
    } else {
        1.0
    };

    if to_nearest == 0.0 || !to_nearest.is_finite() {
        return map_scalar_or_vector(value, |_| f64::NAN);
    }

    map_scalar_or_vector(value, |v| (v / to_nearest).round() * to_nearest)
}

fn eval_clamp(engine: &Engine, call: &CallExpr, params: &QueryParams<'_>) -> Result<PromqlValue> {
    expect_arg_count(call, 3)?;
    let value = engine.eval(&call.args[0], params)?;
    let min = expect_scalar(engine.eval(&call.args[1], params)?, &call.func)?;
    let max = expect_scalar(engine.eval(&call.args[2], params)?, &call.func)?;
    if min > max {
        return match value {
            PromqlValue::Scalar(_, _) => Ok(PromqlValue::Scalar(f64::NAN, params.eval_time)),
            PromqlValue::InstantVector(_) => Ok(PromqlValue::InstantVector(Vec::new())),
            other => Err(PromqlError::Type(format!(
                "function expects scalar or instant vector, got {other:?}"
            ))),
        };
    }
    map_scalar_or_vector(value, |v| v.clamp(min, max))
}

fn eval_clamp_min_max(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
    min_side: bool,
) -> Result<PromqlValue> {
    expect_arg_count(call, 2)?;
    let value = engine.eval(&call.args[0], params)?;
    let bound = expect_scalar(engine.eval(&call.args[1], params)?, &call.func)?;
    if min_side {
        map_scalar_or_vector(value, |v| v.max(bound))
    } else {
        map_scalar_or_vector(value, |v| v.min(bound))
    }
}

fn eval_scalar(engine: &Engine, call: &CallExpr, params: &QueryParams<'_>) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let value = engine.eval(&call.args[0], params)?;
    match value {
        PromqlValue::Scalar(v, ts) => Ok(PromqlValue::Scalar(v, ts)),
        PromqlValue::InstantVector(vector) => {
            if vector.len() == 1 {
                if vector[0].histogram.is_some() {
                    Err(PromqlError::Eval(
                        "scalar() does not support histogram samples".to_string(),
                    ))
                } else {
                    Ok(PromqlValue::Scalar(vector[0].value, vector[0].timestamp))
                }
            } else {
                Ok(PromqlValue::Scalar(f64::NAN, params.eval_time))
            }
        }
        other => Err(PromqlError::Type(format!(
            "scalar() expects scalar or instant vector, got {other:?}"
        ))),
    }
}

fn eval_vector(engine: &Engine, call: &CallExpr, params: &QueryParams<'_>) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let value = engine.eval(&call.args[0], params)?;
    let (v, ts) = expect_scalar_with_ts(value, &call.func)?;
    Ok(PromqlValue::InstantVector(vec![Sample {
        metric: String::new(),
        labels: Vec::new(),
        timestamp: ts,
        value: v,
        histogram: None,
    }]))
}

fn eval_time(engine: &Engine, call: &CallExpr, params: &QueryParams<'_>) -> Result<PromqlValue> {
    expect_arg_count(call, 0)?;
    let seconds = params.eval_time as f64 / engine.timestamp_units_per_second() as f64;
    Ok(PromqlValue::Scalar(seconds, params.eval_time))
}

fn eval_timestamp(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let value = engine.eval(&call.args[0], params)?;
    let mut vector = expect_instant_vector(value, &call.func)?;
    let units_per_second = engine.timestamp_units_per_second() as f64;
    for sample in &mut vector {
        sample.histogram = None;
        sample.value = sample.timestamp as f64 / units_per_second;
    }
    Ok(PromqlValue::InstantVector(vector))
}

fn eval_sort(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
    desc: bool,
) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let value = engine.eval(&call.args[0], params)?;
    let mut vector = expect_instant_vector(value, &call.func)?;
    ensure_histogram_free_vector(&vector, &call.func)?;
    vector.sort_by(|a, b| {
        let ord = a
            .value
            .partial_cmp(&b.value)
            .unwrap_or(std::cmp::Ordering::Equal);
        if desc {
            ord.reverse()
        } else {
            ord
        }
    });
    Ok(PromqlValue::InstantVector(vector))
}

fn eval_sort_by_label(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
    desc: bool,
) -> Result<PromqlValue> {
    if call.args.len() < 2 {
        return Err(PromqlError::ArgumentCount {
            func: call.func.clone(),
            expected: "at least 2".to_string(),
            got: call.args.len(),
        });
    }

    let mut vector = expect_instant_vector(engine.eval(&call.args[0], params)?, &call.func)?;
    ensure_histogram_free_vector(&vector, &call.func)?;
    let mut label_names = Vec::with_capacity(call.args.len() - 1);
    for arg in &call.args[1..] {
        label_names.push(expect_string(engine.eval(arg, params)?, &call.func)?);
    }

    vector.sort_by(|a, b| {
        for label_name in &label_names {
            let av = label_value_for_sort(a, label_name);
            let bv = label_value_for_sort(b, label_name);
            let ord = natural_cmp(&av, &bv);
            if !ord.is_eq() {
                return if desc { ord.reverse() } else { ord };
            }
        }

        let tie = full_series_sort_key(a).cmp(&full_series_sort_key(b));
        if desc {
            tie.reverse()
        } else {
            tie
        }
    });
    Ok(PromqlValue::InstantVector(vector))
}

fn eval_drop_common_labels(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let mut vector = expect_instant_vector(engine.eval(&call.args[0], params)?, &call.func)?;
    if vector.is_empty() {
        return Ok(PromqlValue::InstantVector(vector));
    }

    let mut common = BTreeMap::new();
    for label in &vector[0].labels {
        common.insert(label.name.clone(), label.value.clone());
    }

    for sample in &vector[1..] {
        common.retain(|name, value| {
            sample
                .labels
                .iter()
                .find(|label| label.name == *name)
                .is_some_and(|label| label.value == *value)
        });
    }

    for sample in &mut vector {
        sample
            .labels
            .retain(|label| !common.contains_key(&label.name));
    }

    Ok(PromqlValue::InstantVector(vector))
}

fn eval_info(engine: &Engine, call: &CallExpr, params: &QueryParams<'_>) -> Result<PromqlValue> {
    if !(1..=2).contains(&call.args.len()) {
        return Err(PromqlError::ArgumentCount {
            func: call.func.clone(),
            expected: "1 or 2".to_string(),
            got: call.args.len(),
        });
    }

    let mut vector = expect_instant_vector(engine.eval(&call.args[0], params)?, &call.func)?;
    let info_matchers = if call.args.len() == 2 {
        info_selector_matchers(&call.args[1])?
    } else {
        vec![LabelMatcher {
            name: "__name__".to_string(),
            op: MatchOp::Equal,
            value: "target_info".to_string(),
        }]
    };

    let range_start = params
        .eval_time
        .saturating_sub(engine.default_lookback_delta());
    let range_end = params.eval_time.saturating_add(1);
    let info_series = load_info_series(engine, &info_matchers, range_start, range_end)?;

    for sample in &mut vector {
        let Some(job) = label_value(sample, "job") else {
            continue;
        };
        let Some(instance) = label_value(sample, "instance") else {
            continue;
        };

        let mut extra_labels = info_series
            .iter()
            .filter(|series| series.job == job && series.instance == instance)
            .flat_map(|series| series.data_labels.iter().cloned())
            .collect::<Vec<_>>();
        extra_labels.sort();
        extra_labels.dedup_by(|a, b| a.name == b.name);

        for label in extra_labels {
            set_label(&mut sample.labels, &label.name, &label.value);
        }
    }

    Ok(PromqlValue::InstantVector(vector))
}

fn eval_absent(engine: &Engine, call: &CallExpr, params: &QueryParams<'_>) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let value = engine.eval(&call.args[0], params)?;
    let vector = expect_instant_vector(value, &call.func)?;
    if !vector.is_empty() {
        return Ok(PromqlValue::InstantVector(Vec::new()));
    }

    Ok(PromqlValue::InstantVector(vec![Sample {
        metric: String::new(),
        labels: absent_labels_from_expr(&call.args[0]),
        timestamp: params.eval_time,
        value: 1.0,
        histogram: None,
    }]))
}

fn eval_absent_over_time(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let value = engine.eval(&call.args[0], params)?;
    let vector = expect_range_vector(value, &call.func)?;
    if !vector.is_empty() {
        return Ok(PromqlValue::InstantVector(Vec::new()));
    }

    Ok(PromqlValue::InstantVector(vec![Sample {
        metric: String::new(),
        labels: absent_labels_from_expr(&call.args[0]),
        timestamp: params.eval_time,
        value: 1.0,
        histogram: None,
    }]))
}

fn eval_label_replace(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    expect_arg_count(call, 5)?;
    let vector = expect_instant_vector(engine.eval(&call.args[0], params)?, &call.func)?;
    let dst = expect_string(engine.eval(&call.args[1], params)?, &call.func)?;
    let replacement = expect_string(engine.eval(&call.args[2], params)?, &call.func)?;
    let src = expect_string(engine.eval(&call.args[3], params)?, &call.func)?;
    let pattern = expect_string(engine.eval(&call.args[4], params)?, &call.func)?;

    let regex = Regex::new(&pattern).map_err(PromqlError::from)?;
    let mut out = Vec::with_capacity(vector.len());

    for mut sample in vector {
        let src_val = sample
            .labels
            .iter()
            .find(|l| l.name == src)
            .map(|l| l.value.clone())
            .unwrap_or_default();

        if regex.is_match(&src_val) {
            let replaced = regex
                .replace_all(&src_val, replacement.as_str())
                .to_string();
            set_label(&mut sample.labels, &dst, &replaced);
        }

        out.push(sample);
    }

    Ok(PromqlValue::InstantVector(out))
}

fn eval_label_join(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    if call.args.len() < 4 {
        return Err(PromqlError::ArgumentCount {
            func: call.func.clone(),
            expected: "at least 4".to_string(),
            got: call.args.len(),
        });
    }

    let mut vector = expect_instant_vector(engine.eval(&call.args[0], params)?, &call.func)?;
    let dst = expect_string(engine.eval(&call.args[1], params)?, &call.func)?;
    let sep = expect_string(engine.eval(&call.args[2], params)?, &call.func)?;

    let mut src_labels = Vec::new();
    for arg in &call.args[3..] {
        src_labels.push(expect_string(engine.eval(arg, params)?, &call.func)?);
    }

    for sample in &mut vector {
        let joined = src_labels
            .iter()
            .map(|name| {
                sample
                    .labels
                    .iter()
                    .find(|l| l.name == *name)
                    .map(|l| l.value.clone())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join(&sep);

        set_label(&mut sample.labels, &dst, &joined);
    }

    Ok(PromqlValue::InstantVector(vector))
}

fn map_scalar_or_vector(value: PromqlValue, map: impl Fn(f64) -> f64) -> Result<PromqlValue> {
    match value {
        PromqlValue::Scalar(v, ts) => Ok(PromqlValue::Scalar(map(v), ts)),
        PromqlValue::InstantVector(mut vector) => {
            ensure_histogram_free_vector(&vector, "function")?;
            for sample in &mut vector {
                sample.value = map(sample.value);
            }
            Ok(PromqlValue::InstantVector(vector))
        }
        other => Err(PromqlError::Type(format!(
            "function expects scalar or instant vector, got {other:?}"
        ))),
    }
}

fn expect_arg_count(call: &CallExpr, expected: usize) -> Result<()> {
    if call.args.len() != expected {
        return Err(PromqlError::ArgumentCount {
            func: call.func.clone(),
            expected: expected.to_string(),
            got: call.args.len(),
        });
    }
    Ok(())
}

fn expect_scalar(value: PromqlValue, func: &str) -> Result<f64> {
    match value {
        PromqlValue::Scalar(v, _) => Ok(v),
        other => Err(PromqlError::Type(format!(
            "{func} expects scalar argument, got {other:?}"
        ))),
    }
}

fn expect_scalar_with_ts(value: PromqlValue, func: &str) -> Result<(f64, i64)> {
    match value {
        PromqlValue::Scalar(v, ts) => Ok((v, ts)),
        other => Err(PromqlError::Type(format!(
            "{func} expects scalar argument, got {other:?}"
        ))),
    }
}

fn expect_string(value: PromqlValue, func: &str) -> Result<String> {
    match value {
        PromqlValue::String(v, _) => Ok(v),
        other => Err(PromqlError::Type(format!(
            "{func} expects string argument, got {other:?}"
        ))),
    }
}

fn expect_instant_vector(value: PromqlValue, func: &str) -> Result<Vec<Sample>> {
    match value {
        PromqlValue::InstantVector(v) => Ok(v),
        other => Err(PromqlError::Type(format!(
            "{func} expects instant vector, got {other:?}"
        ))),
    }
}

fn expect_range_vector(
    value: PromqlValue,
    func: &str,
) -> Result<Vec<crate::promql::types::Series>> {
    match value {
        PromqlValue::RangeVector(v) => Ok(v),
        other => Err(PromqlError::Type(format!(
            "{func} expects range vector, got {other:?}"
        ))),
    }
}

fn absent_labels_from_expr(expr: &Expr) -> Vec<Label> {
    match expr {
        Expr::VectorSelector(selector) => absent_labels_from_matchers(&selector.matchers),
        Expr::MatrixSelector(selector) => absent_labels_from_matchers(&selector.vector.matchers),
        Expr::Subquery(subquery) => absent_labels_from_expr(&subquery.expr),
        Expr::Paren(inner) => absent_labels_from_expr(inner),
        _ => Vec::new(),
    }
}

fn absent_labels_from_matchers(matchers: &[crate::promql::ast::LabelMatcher]) -> Vec<Label> {
    let mut labels = Vec::new();
    for matcher in matchers {
        if matcher.op != MatchOp::Equal || matcher.name == "__name__" {
            continue;
        }
        set_label(&mut labels, &matcher.name, &matcher.value);
    }
    labels
}

fn label_value_for_sort(sample: &Sample, name: &str) -> String {
    if name == "__name__" {
        return sample.metric.clone();
    }

    sample
        .labels
        .iter()
        .find(|label| label.name == name)
        .map(|label| label.value.clone())
        .unwrap_or_default()
}

fn full_series_sort_key(sample: &Sample) -> String {
    let mut labels = sample.labels.clone();
    labels.sort();
    format!(
        "{}\u{1f}{}",
        sample.metric,
        labels
            .iter()
            .map(|label| format!("{}={}", label.name, label.value))
            .collect::<Vec<_>>()
            .join("\u{1f}")
    )
}

#[derive(Clone)]
struct InfoSeries {
    job: String,
    instance: String,
    metric: String,
    timestamp: i64,
    data_labels: Vec<Label>,
}

fn info_selector_matchers(expr: &Expr) -> Result<Vec<LabelMatcher>> {
    match expr {
        Expr::VectorSelector(selector)
            if selector.metric_name.is_none() && selector.offset == 0 =>
        {
            Ok(selector.matchers.clone())
        }
        Expr::Paren(inner) => info_selector_matchers(inner),
        _ => Err(PromqlError::Type(
            "info() second argument must be a label matcher selector like {k=\"v\"}".to_string(),
        )),
    }
}

fn load_info_series(
    engine: &Engine,
    matchers: &[LabelMatcher],
    start: i64,
    end: i64,
) -> Result<Vec<InfoSeries>> {
    let metric_matchers = matchers
        .iter()
        .filter(|matcher| matcher.name == "__name__")
        .cloned()
        .collect::<Vec<_>>();
    let data_matchers = matchers
        .iter()
        .filter(|matcher| matcher.name != "__name__")
        .cloned()
        .collect::<Vec<_>>();

    let metrics = candidate_info_metrics(engine, &metric_matchers)?;
    let mut selected: BTreeMap<(String, String, String), InfoSeries> = BTreeMap::new();

    for metric in metrics {
        for (labels, points) in engine.storage().select_all(&metric, start, end)? {
            let Some(point) = latest_point_for_info(points, start, end) else {
                continue;
            };
            if !matchers_match(&metric, &labels, matchers)? {
                continue;
            }

            let Some(job) = labels
                .iter()
                .find(|label| label.name == "job")
                .map(|label| label.value.clone())
            else {
                continue;
            };
            let Some(instance) = labels
                .iter()
                .find(|label| label.name == "instance")
                .map(|label| label.value.clone())
            else {
                continue;
            };

            let mut data_labels = labels
                .into_iter()
                .filter(|label| label.name != "job" && label.name != "instance")
                .filter(|label| {
                    if data_matchers.is_empty() {
                        return true;
                    }
                    data_matchers
                        .iter()
                        .any(|matcher| matcher.name == label.name)
                })
                .collect::<Vec<_>>();
            data_labels.sort();

            let key = (metric.clone(), job.clone(), instance.clone());
            let candidate = InfoSeries {
                job,
                instance,
                metric: metric.clone(),
                timestamp: point.timestamp,
                data_labels,
            };

            match selected.get(&key) {
                Some(current) if current.timestamp >= candidate.timestamp => {}
                _ => {
                    selected.insert(key, candidate);
                }
            }
        }
    }

    let mut out = selected.into_values().collect::<Vec<_>>();
    out.sort_by(|a, b| a.metric.cmp(&b.metric));
    Ok(out)
}

fn candidate_info_metrics(
    engine: &Engine,
    metric_matchers: &[LabelMatcher],
) -> Result<Vec<String>> {
    if metric_matchers.is_empty() {
        return Ok(vec!["target_info".to_string()]);
    }

    let mut metrics = engine
        .storage()
        .list_metrics()?
        .into_iter()
        .map(|series| series.name)
        .collect::<Vec<_>>();
    metrics.sort();
    metrics.dedup();
    let mut out = Vec::new();
    for metric in metrics {
        if matchers_match(&metric, &[], metric_matchers)? {
            out.push(metric);
        }
    }
    Ok(out)
}

fn latest_point_for_info(
    points: Vec<crate::DataPoint>,
    start: i64,
    end: i64,
) -> Option<crate::DataPoint> {
    let mut latest = None;
    for point in points {
        if point.timestamp < start || point.timestamp >= end {
            continue;
        }
        if latest
            .as_ref()
            .is_some_and(|current: &crate::DataPoint| current.timestamp >= point.timestamp)
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

fn label_value(sample: &Sample, name: &str) -> Option<String> {
    sample
        .labels
        .iter()
        .find(|label| label.name == name)
        .map(|label| label.value.clone())
}

fn natural_cmp(lhs: &str, rhs: &str) -> std::cmp::Ordering {
    let left = lhs.as_bytes();
    let right = rhs.as_bytes();
    let mut i = 0;
    let mut j = 0;

    while i < left.len() && j < right.len() {
        let a = left[i];
        let b = right[j];

        if a.is_ascii_digit() && b.is_ascii_digit() {
            let start_i = i;
            while i < left.len() && left[i].is_ascii_digit() {
                i += 1;
            }
            let start_j = j;
            while j < right.len() && right[j].is_ascii_digit() {
                j += 1;
            }

            let lhs_digits = &lhs[start_i..i];
            let rhs_digits = &rhs[start_j..j];
            let lhs_trimmed = lhs_digits.trim_start_matches('0');
            let rhs_trimmed = rhs_digits.trim_start_matches('0');

            let lhs_norm = if lhs_trimmed.is_empty() {
                "0"
            } else {
                lhs_trimmed
            };
            let rhs_norm = if rhs_trimmed.is_empty() {
                "0"
            } else {
                rhs_trimmed
            };

            let ord = lhs_norm
                .len()
                .cmp(&rhs_norm.len())
                .then_with(|| lhs_norm.cmp(rhs_norm))
                .then_with(|| lhs_digits.len().cmp(&rhs_digits.len()).reverse());
            if !ord.is_eq() {
                return ord;
            }
            continue;
        }

        let ord = a.cmp(&b);
        if !ord.is_eq() {
            return ord;
        }
        i += 1;
        j += 1;
    }

    left.len().cmp(&right.len())
}

fn set_label(labels: &mut Vec<Label>, name: &str, value: &str) {
    if let Some(label) = labels.iter_mut().find(|l| l.name == name) {
        label.value = value.to_string();
        return;
    }
    labels.push(Label::new(name.to_string(), value.to_string()));
    labels.sort();
}
