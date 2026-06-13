use std::collections::{BTreeMap, BTreeSet};

use crate::{label::canonical_series_identity, Label};

use crate::promql::ast::{BinaryExpr, BinaryOp, VectorMatchCardinality, VectorMatching};
use crate::promql::error::{PromqlError, Result};
use crate::promql::types::{PromqlValue, Sample};

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
        if !return_bool {
            return Err(PromqlError::Type(
                "scalar-to-scalar comparison requires the bool modifier".to_string(),
            ));
        }
        let matched = compare(op, lhs, rhs)?;
        let value = if matched { 1.0 } else { 0.0 };
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
    if samples.iter().any(|sample| sample.histogram.is_some()) {
        return Err(PromqlError::Eval(format!(
            "binary operator '{}' does not support histogram samples yet",
            binary_op_name(op)
        )));
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
                sample.metric.clear();
                sample.value = if matched { 1.0 } else { 0.0 };
                out.push(sample);
            } else if matched {
                out.push(sample);
            }
            continue;
        }

        sample.metric.clear();
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
        if expr
            .matching
            .as_ref()
            .is_some_and(|m| m.cardinality != VectorMatchCardinality::OneToOne)
        {
            return Err(PromqlError::Type(
                "group_left/group_right modifiers cannot be used with set operators".to_string(),
            ));
        }
        return eval_set_op(expr.op, lhs, rhs, expr.matching.as_ref());
    }
    if lhs.iter().any(|sample| sample.histogram.is_some())
        || rhs.iter().any(|sample| sample.histogram.is_some())
    {
        return Err(PromqlError::Eval(format!(
            "binary operator '{}' does not support histogram samples yet",
            binary_op_name(expr.op)
        )));
    }

    ensure_matching_cardinality(&lhs, &rhs, expr.matching.as_ref())?;
    let lhs_groups = build_sample_groups(&lhs, expr.matching.as_ref());
    let rhs_groups = build_sample_groups(&rhs, expr.matching.as_ref());
    let mut out = Vec::new();

    match matching_cardinality(expr.matching.as_ref()) {
        VectorMatchCardinality::OneToOne | VectorMatchCardinality::ManyToOne => {
            for lhs_sample in lhs {
                let key = sample_key(&lhs_sample, expr.matching.as_ref());
                let Some(rhs_sample) = rhs_groups.get(&key).and_then(|samples| samples.first())
                else {
                    continue;
                };

                if let Some(result) = combine_vector_samples(expr, &lhs_sample, rhs_sample, false)?
                {
                    out.push(result);
                }
            }
        }
        VectorMatchCardinality::OneToMany => {
            for rhs_sample in rhs {
                let key = sample_key(&rhs_sample, expr.matching.as_ref());
                let Some(lhs_sample) = lhs_groups.get(&key).and_then(|samples| samples.first())
                else {
                    continue;
                };

                if let Some(result) = combine_vector_samples(expr, lhs_sample, &rhs_sample, true)? {
                    out.push(result);
                }
            }
        }
    }

    Ok(PromqlValue::InstantVector(out))
}

fn ensure_matching_cardinality(
    lhs: &[Sample],
    rhs: &[Sample],
    matching: Option<&VectorMatching>,
) -> Result<()> {
    let lhs_counts = build_sample_count_map(lhs, matching);
    let rhs_counts = build_sample_count_map(rhs, matching);

    match matching_cardinality(matching) {
        VectorMatchCardinality::OneToOne => {
            for (key, lhs_count) in &lhs_counts {
                if *lhs_count > 1 && rhs_counts.get(key).copied().unwrap_or(0) > 0 {
                    return Err(PromqlError::Eval(
                        "many-to-many matching not allowed: matching labels must be unique on one side"
                            .to_string(),
                    ));
                }
            }

            for (key, rhs_count) in &rhs_counts {
                if *rhs_count > 1 && lhs_counts.get(key).copied().unwrap_or(0) > 0 {
                    return Err(PromqlError::Eval(
                        "many-to-many matching not allowed: matching labels must be unique on one side"
                            .to_string(),
                    ));
                }
            }
        }
        VectorMatchCardinality::ManyToOne => {
            for (key, rhs_count) in &rhs_counts {
                if *rhs_count > 1 && lhs_counts.get(key).copied().unwrap_or(0) > 0 {
                    return Err(PromqlError::Eval(
                        "group_left requires unique matches on the right-hand side".to_string(),
                    ));
                }
            }
        }
        VectorMatchCardinality::OneToMany => {
            for (key, lhs_count) in &lhs_counts {
                if *lhs_count > 1 && rhs_counts.get(key).copied().unwrap_or(0) > 0 {
                    return Err(PromqlError::Eval(
                        "group_right requires unique matches on the left-hand side".to_string(),
                    ));
                }
            }
        }
    }

    Ok(())
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

fn combine_vector_samples(
    expr: &BinaryExpr,
    lhs: &Sample,
    rhs: &Sample,
    result_on_rhs: bool,
) -> Result<Option<Sample>> {
    let mut sample = if result_on_rhs {
        rhs.clone()
    } else {
        lhs.clone()
    };

    if expr.op.is_comparison() {
        let matched = compare(expr.op, lhs.value, rhs.value)?;
        if expr.return_bool {
            sample.metric.clear();
            sample.value = if matched { 1.0 } else { 0.0 };
        } else if !matched {
            return Ok(None);
        } else if !result_on_rhs && expr.matching.as_ref().is_some_and(|m| m.on) {
            sample.metric.clear();
        }
    } else {
        sample.metric.clear();
        sample.value = arithmetic(expr.op, lhs.value, rhs.value)?;
    }

    if let Some(matching) = expr.matching.as_ref() {
        if result_on_rhs {
            include_labels_from_one_side(&mut sample.labels, lhs, &matching.include_labels);
        } else if matching.cardinality == VectorMatchCardinality::ManyToOne {
            include_labels_from_one_side(&mut sample.labels, rhs, &matching.include_labels);
        }
    }

    Ok(Some(sample))
}

fn build_sample_map(
    samples: &[Sample],
    matching: Option<&VectorMatching>,
) -> BTreeMap<Vec<u8>, Sample> {
    let mut out = BTreeMap::new();
    for sample in samples {
        out.entry(sample_key(sample, matching))
            .or_insert_with(|| sample.clone());
    }
    out
}

fn build_sample_groups(
    samples: &[Sample],
    matching: Option<&VectorMatching>,
) -> BTreeMap<Vec<u8>, Vec<Sample>> {
    let mut out = BTreeMap::new();
    for sample in samples {
        out.entry(sample_key(sample, matching))
            .or_insert_with(Vec::new)
            .push(sample.clone());
    }
    out
}

fn build_sample_count_map(
    samples: &[Sample],
    matching: Option<&VectorMatching>,
) -> BTreeMap<Vec<u8>, usize> {
    let mut out = BTreeMap::new();
    for sample in samples {
        *out.entry(sample_key(sample, matching)).or_insert(0) += 1;
    }
    out
}

fn sample_key(sample: &Sample, matching: Option<&VectorMatching>) -> Vec<u8> {
    match matching {
        None => canonical_series_identity("", &sample.labels),
        Some(m) if m.on => {
            let wanted: BTreeSet<&str> = m.labels.iter().map(|s| s.as_str()).collect();
            let selected = sample
                .labels
                .iter()
                .filter(|l| wanted.contains(l.name.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            canonical_series_identity("", &selected)
        }
        Some(m) => {
            let ignored: BTreeSet<&str> = m.labels.iter().map(|s| s.as_str()).collect();
            let selected = sample
                .labels
                .iter()
                .filter(|l| !ignored.contains(l.name.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            canonical_series_identity("", &selected)
        }
    }
}

fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::Atan2 => "atan2",
        BinaryOp::Pow => "^",
        BinaryOp::Eq => "==",
        BinaryOp::NotEq => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::Gt => ">",
        BinaryOp::Lte => "<=",
        BinaryOp::Gte => ">=",
        BinaryOp::And => "and",
        BinaryOp::Or => "or",
        BinaryOp::Unless => "unless",
    }
}

fn matching_cardinality(matching: Option<&VectorMatching>) -> VectorMatchCardinality {
    matching
        .map(|matching| matching.cardinality)
        .unwrap_or(VectorMatchCardinality::OneToOne)
}

fn include_labels_from_one_side(base: &mut Vec<Label>, source: &Sample, labels: &[String]) {
    for label_name in labels {
        if let Some(value) = source
            .labels
            .iter()
            .find(|label| label.name == *label_name)
            .map(|label| label.value.clone())
        {
            set_label(base, label_name, &value);
        }
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

fn arithmetic(op: BinaryOp, lhs: f64, rhs: f64) -> Result<f64> {
    match op {
        BinaryOp::Add => Ok(lhs + rhs),
        BinaryOp::Sub => Ok(lhs - rhs),
        BinaryOp::Mul => Ok(lhs * rhs),
        BinaryOp::Div => Ok(lhs / rhs),
        BinaryOp::Mod => Ok(lhs % rhs),
        BinaryOp::Atan2 => Ok(lhs.atan2(rhs)),
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
