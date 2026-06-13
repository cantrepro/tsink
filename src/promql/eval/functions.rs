use crate::Label;
use regex::Regex;

use crate::promql::ast::CallExpr;
use crate::promql::error::{PromqlError, Result};
use crate::promql::types::{PromqlValue, Sample};

use super::{Engine, QueryParams};

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
        "avg_over_time" => eval_over_time(engine, call, params, OverTimeKind::Avg),
        "sum_over_time" => eval_over_time(engine, call, params, OverTimeKind::Sum),
        "min_over_time" => eval_over_time(engine, call, params, OverTimeKind::Min),
        "max_over_time" => eval_over_time(engine, call, params, OverTimeKind::Max),
        "count_over_time" => eval_over_time(engine, call, params, OverTimeKind::Count),
        "abs" => eval_unary_map(engine, call, params, |v| v.abs()),
        "ceil" => eval_unary_map(engine, call, params, |v| v.ceil()),
        "floor" => eval_unary_map(engine, call, params, |v| v.floor()),
        "round" => eval_unary_map(engine, call, params, |v| v.round()),
        "clamp" => eval_clamp(engine, call, params),
        "clamp_min" => eval_clamp_min_max(engine, call, params, true),
        "clamp_max" => eval_clamp_min_max(engine, call, params, false),
        "scalar" => eval_scalar(engine, call, params),
        "vector" => eval_vector(engine, call, params),
        "time" => eval_time(engine, call, params),
        "timestamp" => eval_timestamp(engine, call, params),
        "sort" => eval_sort(engine, call, params, false),
        "sort_desc" => eval_sort(engine, call, params, true),
        "label_replace" => eval_label_replace(engine, call, params),
        "label_join" => eval_label_join(engine, call, params),
        _ => Err(PromqlError::UnknownFunction(call.func.clone())),
    }
}

#[derive(Clone, Copy)]
enum RateKind {
    Rate,
    Irate,
    Increase,
}

fn eval_rate_like(
    engine: &Engine,
    call: &CallExpr,
    params: &QueryParams<'_>,
    kind: RateKind,
) -> Result<PromqlValue> {
    expect_arg_count(call, 1)?;
    let arg = engine.eval(&call.args[0], params)?;
    let series = expect_range_vector(arg, &call.func)?;

    let mut out = Vec::new();
    for mut s in series {
        let value = match kind {
            RateKind::Rate => rate_value(&s.samples, engine.timestamp_units_per_second()),
            RateKind::Irate => irate_value(&s.samples, engine.timestamp_units_per_second()),
            RateKind::Increase => increase_value(&s.samples),
        };
        let Some(value) = value else {
            continue;
        };

        out.push(Sample {
            metric: std::mem::take(&mut s.metric),
            labels: std::mem::take(&mut s.labels),
            timestamp: params.eval_time,
            value,
        });
    }

    Ok(PromqlValue::InstantVector(out))
}

fn rate_value(samples: &[(i64, f64)], units_per_second: i64) -> Option<f64> {
    let increase = increase_value(samples)?;
    let first = samples.first()?.0;
    let last = samples.last()?.0;
    let duration = last.saturating_sub(first);
    if duration <= 0 {
        return None;
    }
    Some(increase / (duration as f64 / units_per_second as f64))
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

fn increase_value(samples: &[(i64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }

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

#[derive(Clone, Copy)]
enum OverTimeKind {
    Avg,
    Sum,
    Min,
    Max,
    Count,
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
        };

        out.push(Sample {
            metric: std::mem::take(&mut s.metric),
            labels: std::mem::take(&mut s.labels),
            timestamp: params.eval_time,
            value,
        });
    }

    Ok(PromqlValue::InstantVector(out))
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

fn eval_clamp(engine: &Engine, call: &CallExpr, params: &QueryParams<'_>) -> Result<PromqlValue> {
    expect_arg_count(call, 3)?;
    let value = engine.eval(&call.args[0], params)?;
    let mut min = expect_scalar(engine.eval(&call.args[1], params)?, &call.func)?;
    let mut max = expect_scalar(engine.eval(&call.args[2], params)?, &call.func)?;
    if min > max {
        std::mem::swap(&mut min, &mut max);
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
                Ok(PromqlValue::Scalar(vector[0].value, vector[0].timestamp))
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

fn set_label(labels: &mut Vec<Label>, name: &str, value: &str) {
    if let Some(label) = labels.iter_mut().find(|l| l.name == name) {
        label.value = value.to_string();
        return;
    }
    labels.push(Label::new(name.to_string(), value.to_string()));
    labels.sort();
}
