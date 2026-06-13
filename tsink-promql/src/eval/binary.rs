use std::collections::{BTreeMap, BTreeSet};

use tsink::Label;

use crate::ast::{BinaryExpr, BinaryOp, VectorMatching};
use crate::error::{PromqlError, Result};
use crate::types::{PromqlValue, Sample};

use super::{Engine, QueryParams};

pub(crate) fn eval_binary(
    engine: &Engine,
    expr: &BinaryExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    let lhs = engine.eval(&expr.lhs, params)?;
    let rhs = engine.eval(&expr.rhs, params)?;

    match (lhs, rhs) {
        (PromqlValue::Scalar(lv, lts), PromqlValue::Scalar(rv, rts)) => {
            let ts = lts.max(rts);
            eval_scalar_scalar(expr.op, lv, rv, expr.return_bool, ts)
        }
        (PromqlValue::InstantVector(samples), PromqlValue::Scalar(s, _)) => {
            eval_vector_scalar(expr.op, samples, s, true, expr.return_bool)
        }
        (PromqlValue::Scalar(s, _), PromqlValue::InstantVector(samples)) => {
            eval_vector_scalar(expr.op, samples, s, false, expr.return_bool)
        }
        (PromqlValue::InstantVector(lhs), PromqlValue::InstantVector(rhs)) => {
            eval_vector_vector(expr, lhs, rhs)
        }
        (l, r) => Err(PromqlError::Type(format!(
            "binary operator cannot combine {l:?} and {r:?}"
        ))),
    }
}

fn eval_scalar_scalar(
    op: BinaryOp,
    lhs: f64,
    rhs: f64,
    return_bool: bool,
    ts: i64,
) -> Result<PromqlValue> {
    if op.is_set() {
        return Err(PromqlError::Type(
            "set operators require vector operands".to_string(),
        ));
    }

    if op.is_comparison() {
        let matched = compare(op, lhs, rhs)?;
        let value = if return_bool {
            if matched {
                1.0
            } else {
                0.0
            }
        } else if matched {
            lhs
        } else {
            f64::NAN
        };
        return Ok(PromqlValue::Scalar(value, ts));
    }

    let value = arithmetic(op, lhs, rhs)?;
    Ok(PromqlValue::Scalar(value, ts))
}

fn eval_vector_scalar(
    op: BinaryOp,
    mut samples: Vec<Sample>,
    scalar: f64,
    vector_on_lhs: bool,
    return_bool: bool,
) -> Result<PromqlValue> {
    if op.is_set() {
        return Err(PromqlError::Type(
            "set operators require vector-vector operands".to_string(),
        ));
    }

    let mut out = Vec::with_capacity(samples.len());
    for mut sample in samples.drain(..) {
        let (lhs, rhs) = if vector_on_lhs {
            (sample.value, scalar)
        } else {
            (scalar, sample.value)
        };

        if op.is_comparison() {
            let matched = compare(op, lhs, rhs)?;
            if return_bool {
                sample.value = if matched { 1.0 } else { 0.0 };
                out.push(sample);
            } else if matched {
                out.push(sample);
            }
            continue;
        }

        sample.value = arithmetic(op, lhs, rhs)?;
        out.push(sample);
    }

    Ok(PromqlValue::InstantVector(out))
}

fn eval_vector_vector(
    expr: &BinaryExpr,
    lhs: Vec<Sample>,
    rhs: Vec<Sample>,
) -> Result<PromqlValue> {
    if expr.op.is_set() {
        return eval_set_op(expr.op, lhs, rhs, expr.matching.as_ref());
    }

    let rhs_map = build_sample_map(&rhs, expr.matching.as_ref());
    let mut out = Vec::new();

    for mut sample in lhs {
        let key = sample_key(&sample, expr.matching.as_ref());
        let Some(rhs_sample) = rhs_map.get(&key) else {
            continue;
        };

        if expr.op.is_comparison() {
            let matched = compare(expr.op, sample.value, rhs_sample.value)?;
            if expr.return_bool {
                sample.metric.clear();
                sample.value = if matched { 1.0 } else { 0.0 };
                out.push(sample);
            } else if matched {
                out.push(sample);
            }
        } else {
            sample.metric.clear();
            sample.value = arithmetic(expr.op, sample.value, rhs_sample.value)?;
            out.push(sample);
        }
    }

    Ok(PromqlValue::InstantVector(out))
}

fn eval_set_op(
    op: BinaryOp,
    lhs: Vec<Sample>,
    rhs: Vec<Sample>,
    matching: Option<&VectorMatching>,
) -> Result<PromqlValue> {
    let rhs_map = build_sample_map(&rhs, matching);

    let mut out = Vec::new();
    match op {
        BinaryOp::And => {
            for sample in lhs {
                let key = sample_key(&sample, matching);
                if rhs_map.contains_key(&key) {
                    out.push(sample);
                }
            }
        }
        BinaryOp::Or => {
            let mut seen = BTreeSet::new();
            for sample in lhs {
                let key = sample_key(&sample, matching);
                seen.insert(key.clone());
                out.push(sample);
            }
            for sample in rhs {
                let key = sample_key(&sample, matching);
                if !seen.contains(&key) {
                    out.push(sample);
                }
            }
        }
        BinaryOp::Unless => {
            for sample in lhs {
                let key = sample_key(&sample, matching);
                if !rhs_map.contains_key(&key) {
                    out.push(sample);
                }
            }
        }
        _ => {
            return Err(PromqlError::Eval(format!(
                "operator {:?} is not a set operator",
                op
            )));
        }
    }

    Ok(PromqlValue::InstantVector(out))
}

fn build_sample_map(
    samples: &[Sample],
    matching: Option<&VectorMatching>,
) -> BTreeMap<String, Sample> {
    let mut out = BTreeMap::new();
    for sample in samples {
        out.entry(sample_key(sample, matching))
            .or_insert_with(|| sample.clone());
    }
    out
}

fn sample_key(sample: &Sample, matching: Option<&VectorMatching>) -> String {
    let mut labels = sample.labels.clone();
    labels.sort();

    match matching {
        None => key_from_labels(&labels),
        Some(m) if m.on => {
            let wanted: BTreeSet<&str> = m.labels.iter().map(|s| s.as_str()).collect();
            let selected = labels
                .into_iter()
                .filter(|l| wanted.contains(l.name.as_str()))
                .collect::<Vec<_>>();
            key_from_labels(&selected)
        }
        Some(m) => {
            let ignored: BTreeSet<&str> = m.labels.iter().map(|s| s.as_str()).collect();
            let selected = labels
                .into_iter()
                .filter(|l| !ignored.contains(l.name.as_str()))
                .collect::<Vec<_>>();
            key_from_labels(&selected)
        }
    }
}

fn key_from_labels(labels: &[Label]) -> String {
    labels
        .iter()
        .map(|l| format!("{}={}", l.name, l.value))
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

fn arithmetic(op: BinaryOp, lhs: f64, rhs: f64) -> Result<f64> {
    match op {
        BinaryOp::Add => Ok(lhs + rhs),
        BinaryOp::Sub => Ok(lhs - rhs),
        BinaryOp::Mul => Ok(lhs * rhs),
        BinaryOp::Div => Ok(lhs / rhs),
        BinaryOp::Mod => Ok(lhs % rhs),
        BinaryOp::Pow => Ok(lhs.powf(rhs)),
        _ => Err(PromqlError::Type(format!(
            "operator {:?} is not an arithmetic operator",
            op
        ))),
    }
}

fn compare(op: BinaryOp, lhs: f64, rhs: f64) -> Result<bool> {
    match op {
        BinaryOp::Eq => Ok(lhs == rhs),
        BinaryOp::NotEq => Ok(lhs != rhs),
        BinaryOp::Lt => Ok(lhs < rhs),
        BinaryOp::Gt => Ok(lhs > rhs),
        BinaryOp::Lte => Ok(lhs <= rhs),
        BinaryOp::Gte => Ok(lhs >= rhs),
        _ => Err(PromqlError::Type(format!(
            "operator {:?} is not a comparison operator",
            op
        ))),
    }
}
