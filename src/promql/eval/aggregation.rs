use std::collections::{BTreeMap, BTreeSet};

use crate::Label;

use crate::promql::ast::{AggregationExpr, AggregationOp, Grouping};
use crate::promql::error::{PromqlError, Result};
use crate::promql::types::{PromqlValue, Sample};

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

    let mut groups: BTreeMap<String, (Vec<Label>, Vec<Sample>)> = BTreeMap::new();
    for sample in samples {
        let labels = grouped_labels(&sample.labels, expr.grouping.as_ref());
        let key = labels_key(&labels);
        let entry = groups.entry(key).or_insert_with(|| (labels, Vec::new()));
        entry.1.push(sample);
    }

    let mut out = Vec::new();
    match expr.op {
        AggregationOp::Sum
        | AggregationOp::Avg
        | AggregationOp::Min
        | AggregationOp::Max
        | AggregationOp::Count => {
            for (_, (labels, group_samples)) in groups {
                let timestamp = group_samples
                    .iter()
                    .map(|s| s.timestamp)
                    .max()
                    .unwrap_or(params.eval_time);
                let value = aggregate_group(expr.op, &group_samples);
                out.push(Sample {
                    metric: String::new(),
                    labels,
                    timestamp,
                    value,
                });
            }
        }
        AggregationOp::TopK | AggregationOp::BottomK => {
            let k = aggregation_k(engine, expr, params)?;
            for (_, (labels, mut group_samples)) in groups {
                if expr.op == AggregationOp::TopK {
                    group_samples.sort_by(|a, b| {
                        b.value
                            .partial_cmp(&a.value)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                } else {
                    group_samples.sort_by(|a, b| {
                        a.value
                            .partial_cmp(&b.value)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                }

                for mut sample in group_samples.into_iter().take(k) {
                    sample.metric.clear();
                    if expr.grouping.is_some() {
                        sample.labels = labels.clone();
                    }
                    out.push(sample);
                }
            }
        }
    }

    Ok(PromqlValue::InstantVector(out))
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

fn aggregate_group(op: AggregationOp, samples: &[Sample]) -> f64 {
    match op {
        AggregationOp::Sum => samples.iter().map(|s| s.value).sum(),
        AggregationOp::Avg => {
            let sum: f64 = samples.iter().map(|s| s.value).sum();
            sum / samples.len() as f64
        }
        AggregationOp::Min => samples
            .iter()
            .map(|s| s.value)
            .fold(f64::INFINITY, f64::min),
        AggregationOp::Max => samples
            .iter()
            .map(|s| s.value)
            .fold(f64::NEG_INFINITY, f64::max),
        AggregationOp::Count => samples.len() as f64,
        AggregationOp::TopK | AggregationOp::BottomK => f64::NAN,
    }
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

fn labels_key(labels: &[Label]) -> String {
    labels
        .iter()
        .map(|l| format!("{}={}", l.name, l.value))
        .collect::<Vec<_>>()
        .join("\u{1f}")
}
