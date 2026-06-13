use std::collections::{BTreeMap, BTreeSet};

use crate::{label::canonical_series_identity, Label};
use xxhash_rust::xxh64::xxh64;

use crate::promql::ast::{AggregationExpr, AggregationOp, Grouping};
use crate::promql::error::{PromqlError, Result};
use crate::promql::types::{histogram_add, histogram_scale, PromqlValue, Sample};

use super::{Engine, QueryParams};

pub(crate) fn eval_aggregation(
    engine: &Engine,
    expr: &AggregationExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    let input = engine.eval(&expr.expr, params)?;
    let PromqlValue::InstantVector(samples) = input else {
        return Err(PromqlError::Type(
            "aggregation expects an instant vector".to_string(),
        ));
    };

    if samples.is_empty() {
        return Ok(PromqlValue::InstantVector(Vec::new()));
    }

    let mut groups: BTreeMap<Vec<u8>, (Vec<Label>, Vec<Sample>)> = BTreeMap::new();
    for sample in samples {
        let labels = grouped_labels(&sample.labels, expr.grouping.as_ref());
        let key = labels_identity(&labels);
        let entry = groups.entry(key).or_insert_with(|| (labels, Vec::new()));
        entry.1.push(sample);
    }

    let mut out = Vec::new();
    match expr.op {
        AggregationOp::Sum
        | AggregationOp::Avg
        | AggregationOp::Min
        | AggregationOp::Max
        | AggregationOp::Count
        | AggregationOp::Group
        | AggregationOp::Stddev
        | AggregationOp::Stdvar => {
            for (_, (labels, group_samples)) in groups {
                let timestamp = max_timestamp(&group_samples, params.eval_time);
                match aggregate_group(expr.op, &group_samples)? {
                    AggregationValue::Float(value) => {
                        out.push(Sample::from_float(String::new(), labels, timestamp, value))
                    }
                    AggregationValue::Histogram(histogram) => out.push(Sample::from_histogram(
                        String::new(),
                        labels,
                        timestamp,
                        *histogram,
                    )),
                }
            }
        }
        AggregationOp::Quantile => {
            let phi = aggregation_quantile(engine, expr, params)?;
            for (_, (labels, group_samples)) in groups {
                if group_samples
                    .iter()
                    .any(|sample| sample.histogram.is_some())
                {
                    return Err(unsupported_histogram_aggregation("quantile"));
                }
                let timestamp = max_timestamp(&group_samples, params.eval_time);
                let value = quantile_of_samples(&group_samples, phi);
                out.push(Sample {
                    metric: String::new(),
                    labels,
                    timestamp,
                    value,
                    histogram: None,
                });
            }
        }
        AggregationOp::CountValues => {
            let label_name = aggregation_label_name(engine, expr, params)?;
            for (_, (labels, group_samples)) in groups {
                if group_samples
                    .iter()
                    .any(|sample| sample.histogram.is_some())
                {
                    return Err(unsupported_histogram_aggregation("count_values"));
                }
                let timestamp = max_timestamp(&group_samples, params.eval_time);
                let mut counts = BTreeMap::new();
                for sample in group_samples {
                    *counts
                        .entry(sample_value_label(sample.value))
                        .or_insert(0usize) += 1;
                }

                for (value_label, count) in counts {
                    let mut labels = labels.clone();
                    set_label(&mut labels, &label_name, &value_label);
                    out.push(Sample {
                        metric: String::new(),
                        labels,
                        timestamp,
                        value: count as f64,
                        histogram: None,
                    });
                }
            }
        }
        AggregationOp::TopK | AggregationOp::BottomK => {
            let k = aggregation_k(engine, expr, params)?;
            for (_, (_, mut group_samples)) in groups {
                if group_samples
                    .iter()
                    .any(|sample| sample.histogram.is_some())
                {
                    return Err(unsupported_histogram_aggregation(
                        if expr.op == AggregationOp::TopK {
                            "topk"
                        } else {
                            "bottomk"
                        },
                    ));
                }
                if expr.op == AggregationOp::TopK {
                    group_samples.sort_by(|a, b| topk_value_cmp(a.value, b.value));
                } else {
                    group_samples.sort_by(|a, b| bottomk_value_cmp(a.value, b.value));
                }

                for sample in group_samples.into_iter().take(k) {
                    out.push(sample);
                }
            }
        }
        AggregationOp::LimitK | AggregationOp::LimitRatio => {
            for (_, (_, mut group_samples)) in groups {
                group_samples.sort_by_key(sample_hash);
                let selected = match expr.op {
                    AggregationOp::LimitK => {
                        let k = aggregation_k_like(engine, expr, params, "limitk")?;
                        select_limitk(group_samples, k)
                    }
                    AggregationOp::LimitRatio => {
                        let ratio = aggregation_ratio(engine, expr, params)?;
                        select_limit_ratio(group_samples, ratio)?
                    }
                    _ => unreachable!(),
                };

                for sample in selected {
                    out.push(sample);
                }
            }
        }
    }

    Ok(PromqlValue::InstantVector(out))
}

fn max_timestamp(samples: &[Sample], fallback: i64) -> i64 {
    samples
        .iter()
        .map(|sample| sample.timestamp)
        .max()
        .unwrap_or(fallback)
}

fn aggregation_k(
    engine: &Engine,
    expr: &AggregationExpr,
    params: &QueryParams<'_>,
) -> Result<usize> {
    let param = expr
        .param
        .as_ref()
        .ok_or_else(|| PromqlError::Eval("topk/bottomk requires a parameter".to_string()))?;

    let value = engine.eval(param, params)?;
    match value {
        PromqlValue::Scalar(v, _) => {
            if !v.is_finite() || v <= 0.0 {
                Ok(0)
            } else {
                Ok(v.floor() as usize)
            }
        }
        _ => Err(PromqlError::Type(
            "topk/bottomk parameter must evaluate to scalar".to_string(),
        )),
    }
}

fn aggregation_k_like(
    engine: &Engine,
    expr: &AggregationExpr,
    params: &QueryParams<'_>,
    name: &str,
) -> Result<usize> {
    let param = expr
        .param
        .as_ref()
        .ok_or_else(|| PromqlError::Eval(format!("{name} requires a parameter")))?;

    let value = engine.eval(param, params)?;
    match value {
        PromqlValue::Scalar(v, _) => {
            if !v.is_finite() || v <= 0.0 {
                Ok(0)
            } else {
                Ok(v.floor() as usize)
            }
        }
        _ => Err(PromqlError::Type(format!(
            "{name} parameter must evaluate to scalar"
        ))),
    }
}

fn aggregation_quantile(
    engine: &Engine,
    expr: &AggregationExpr,
    params: &QueryParams<'_>,
) -> Result<f64> {
    let param = expr
        .param
        .as_ref()
        .ok_or_else(|| PromqlError::Eval("quantile requires a parameter".to_string()))?;

    match engine.eval(param, params)? {
        PromqlValue::Scalar(v, _) => Ok(v),
        _ => Err(PromqlError::Type(
            "quantile parameter must evaluate to scalar".to_string(),
        )),
    }
}

fn aggregation_label_name(
    engine: &Engine,
    expr: &AggregationExpr,
    params: &QueryParams<'_>,
) -> Result<String> {
    let param = expr
        .param
        .as_ref()
        .ok_or_else(|| PromqlError::Eval("count_values requires a parameter".to_string()))?;

    match engine.eval(param, params)? {
        PromqlValue::String(v, _) => Ok(v),
        _ => Err(PromqlError::Type(
            "count_values parameter must evaluate to string".to_string(),
        )),
    }
}

fn aggregation_ratio(
    engine: &Engine,
    expr: &AggregationExpr,
    params: &QueryParams<'_>,
) -> Result<f64> {
    let param = expr
        .param
        .as_ref()
        .ok_or_else(|| PromqlError::Eval("limit_ratio requires a parameter".to_string()))?;

    match engine.eval(param, params)? {
        PromqlValue::Scalar(v, _) => Ok(v),
        _ => Err(PromqlError::Type(
            "limit_ratio parameter must evaluate to scalar".to_string(),
        )),
    }
}

enum AggregationValue {
    Float(f64),
    Histogram(Box<crate::NativeHistogram>),
}

fn aggregate_group(op: AggregationOp, samples: &[Sample]) -> Result<AggregationValue> {
    let histogram_count = samples
        .iter()
        .filter(|sample| sample.histogram.is_some())
        .count();
    let float_count = samples.len().saturating_sub(histogram_count);

    match op {
        AggregationOp::Sum | AggregationOp::Avg => {
            if histogram_count > 0 && float_count > 0 {
                return Err(PromqlError::Eval(format!(
                    "{} does not support mixed float and histogram samples in the same aggregation group",
                    aggregation_name(op)
                )));
            }
            if histogram_count > 0 {
                let mut histograms = samples
                    .iter()
                    .filter_map(|sample| sample.histogram())
                    .cloned();
                let Some(mut aggregate) = histograms.next() else {
                    return Ok(AggregationValue::Float(f64::NAN));
                };
                for histogram in histograms {
                    aggregate = histogram_add(&aggregate, &histogram).map_err(PromqlError::Eval)?;
                }
                if op == AggregationOp::Avg {
                    aggregate = histogram_scale(&aggregate, 1.0 / histogram_count as f64)
                        .map_err(PromqlError::Eval)?;
                }
                Ok(AggregationValue::Histogram(Box::new(aggregate)))
            } else if op == AggregationOp::Sum {
                Ok(AggregationValue::Float(
                    samples.iter().map(|sample| sample.value).sum(),
                ))
            } else {
                let sum: f64 = samples.iter().map(|sample| sample.value).sum();
                Ok(AggregationValue::Float(sum / samples.len() as f64))
            }
        }
        AggregationOp::Min => {
            if histogram_count > 0 {
                return Err(unsupported_histogram_aggregation("min"));
            }
            Ok(AggregationValue::Float(
                samples
                    .iter()
                    .map(|sample| sample.value)
                    .reduce(min_ignore_nan)
                    .unwrap_or(f64::NAN),
            ))
        }
        AggregationOp::Max => {
            if histogram_count > 0 {
                return Err(unsupported_histogram_aggregation("max"));
            }
            Ok(AggregationValue::Float(
                samples
                    .iter()
                    .map(|sample| sample.value)
                    .reduce(max_ignore_nan)
                    .unwrap_or(f64::NAN),
            ))
        }
        AggregationOp::Count => Ok(AggregationValue::Float(samples.len() as f64)),
        AggregationOp::Group => Ok(AggregationValue::Float(1.0)),
        AggregationOp::Stddev => {
            if histogram_count > 0 {
                return Err(unsupported_histogram_aggregation("stddev"));
            }
            Ok(AggregationValue::Float(variance_of_samples(samples).sqrt()))
        }
        AggregationOp::Stdvar => {
            if histogram_count > 0 {
                return Err(unsupported_histogram_aggregation("stdvar"));
            }
            Ok(AggregationValue::Float(variance_of_samples(samples)))
        }
        AggregationOp::TopK
        | AggregationOp::BottomK
        | AggregationOp::LimitK
        | AggregationOp::LimitRatio
        | AggregationOp::CountValues
        | AggregationOp::Quantile => Ok(AggregationValue::Float(f64::NAN)),
    }
}

fn aggregation_name(op: AggregationOp) -> &'static str {
    match op {
        AggregationOp::Sum => "sum",
        AggregationOp::Avg => "avg",
        AggregationOp::Min => "min",
        AggregationOp::Max => "max",
        AggregationOp::Count => "count",
        AggregationOp::Group => "group",
        AggregationOp::CountValues => "count_values",
        AggregationOp::Quantile => "quantile",
        AggregationOp::Stddev => "stddev",
        AggregationOp::Stdvar => "stdvar",
        AggregationOp::LimitK => "limitk",
        AggregationOp::LimitRatio => "limit_ratio",
        AggregationOp::TopK => "topk",
        AggregationOp::BottomK => "bottomk",
    }
}

fn unsupported_histogram_aggregation(name: &str) -> PromqlError {
    PromqlError::Eval(format!("{name} does not support histogram samples yet"))
}

fn variance_of_samples(samples: &[Sample]) -> f64 {
    let count = samples.len() as f64;
    if count == 0.0 {
        return f64::NAN;
    }

    let mean = samples.iter().map(|sample| sample.value).sum::<f64>() / count;
    samples
        .iter()
        .map(|sample| {
            let delta = sample.value - mean;
            delta * delta
        })
        .sum::<f64>()
        / count
}

fn quantile_of_samples(samples: &[Sample], phi: f64) -> f64 {
    let mut values = samples
        .iter()
        .map(|sample| sample.value)
        .collect::<Vec<_>>();
    quantile(&mut values, phi)
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

    values.sort_by(|a, b| quantile_value_cmp(*a, *b));
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

fn grouped_labels(labels: &[Label], grouping: Option<&Grouping>) -> Vec<Label> {
    let mut out = match grouping {
        None => Vec::new(),
        Some(grouping) if grouping.without => {
            let excluded: BTreeSet<&str> =
                grouping.labels.iter().map(|label| label.as_str()).collect();
            labels
                .iter()
                .filter(|label| !excluded.contains(label.name.as_str()))
                .cloned()
                .collect::<Vec<_>>()
        }
        Some(grouping) => {
            let included: BTreeSet<&str> =
                grouping.labels.iter().map(|label| label.as_str()).collect();
            labels
                .iter()
                .filter(|label| included.contains(label.name.as_str()))
                .cloned()
                .collect::<Vec<_>>()
        }
    };

    out.sort();
    out
}

fn labels_identity(labels: &[Label]) -> Vec<u8> {
    canonical_series_identity("", labels)
}

fn sample_value_label(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "+Inf".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        value.to_string()
    }
}

fn set_label(labels: &mut Vec<Label>, name: &str, value: &str) {
    if let Some(label) = labels.iter_mut().find(|label| label.name == name) {
        label.value = value.to_string();
    } else {
        labels.push(Label::new(name.to_string(), value.to_string()));
        labels.sort();
    }
}

fn min_ignore_nan(lhs: f64, rhs: f64) -> f64 {
    match (lhs.is_nan(), rhs.is_nan()) {
        (true, true) => f64::NAN,
        (true, false) => rhs,
        (false, true) => lhs,
        (false, false) => lhs.min(rhs),
    }
}

fn max_ignore_nan(lhs: f64, rhs: f64) -> f64 {
    match (lhs.is_nan(), rhs.is_nan()) {
        (true, true) => f64::NAN,
        (true, false) => rhs,
        (false, true) => lhs,
        (false, false) => lhs.max(rhs),
    }
}

fn quantile_value_cmp(lhs: f64, rhs: f64) -> std::cmp::Ordering {
    match (lhs.is_nan(), rhs.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => lhs.total_cmp(&rhs),
    }
}

fn topk_value_cmp(lhs: f64, rhs: f64) -> std::cmp::Ordering {
    match (lhs.is_nan(), rhs.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => rhs.total_cmp(&lhs),
    }
}

fn bottomk_value_cmp(lhs: f64, rhs: f64) -> std::cmp::Ordering {
    match (lhs.is_nan(), rhs.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => lhs.total_cmp(&rhs),
    }
}

fn select_limitk(samples: Vec<Sample>, k: usize) -> Vec<Sample> {
    samples.into_iter().take(k).collect()
}

fn select_limit_ratio(samples: Vec<Sample>, ratio: f64) -> Result<Vec<Sample>> {
    if !ratio.is_finite() || ratio == 0.0 || !(-1.0..=1.0).contains(&ratio) {
        return Err(PromqlError::Eval(
            "limit_ratio parameter must be within [-1, 1] and not equal to 0".to_string(),
        ));
    }

    let len = samples.len();
    if len == 0 {
        return Ok(Vec::new());
    }

    let take = ((len as f64) * ratio.abs()).floor() as usize;
    if ratio > 0.0 {
        Ok(samples.into_iter().take(take).collect())
    } else {
        Ok(samples.into_iter().rev().take(take).collect())
    }
}

fn sample_hash(sample: &Sample) -> u64 {
    xxh64(
        &canonical_series_identity(&sample.metric, &sample.labels),
        0,
    )
}
