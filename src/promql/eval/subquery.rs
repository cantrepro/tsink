use std::collections::BTreeMap;

use crate::promql::ast::SubqueryExpr;
use crate::promql::error::{PromqlError, Result};
use crate::promql::types::{PromqlValue, Series};

use super::time::{duration_to_units, step_times};
use super::{resolve_at_modifier, Engine, QueryParams};

pub(crate) fn eval_subquery(
    engine: &Engine,
    subquery: &SubqueryExpr,
    params: &QueryParams<'_>,
) -> Result<PromqlValue> {
    let base_eval = resolve_at_modifier(
        subquery.at.as_ref(),
        params,
        engine.timestamp_units_per_second(),
    )?;
    let offset = duration_to_units(subquery.offset, engine.timestamp_units_per_second());
    let eval_at = base_eval.saturating_sub(offset);
    let range = duration_to_units(subquery.range, engine.timestamp_units_per_second());
    let step = match subquery.step {
        Some(step) => duration_to_units(step, engine.timestamp_units_per_second()),
        None => params
            .query_step
            .unwrap_or_else(|| engine.default_subquery_step()),
    };

    if step <= 0 {
        return Err(PromqlError::Eval(
            "subquery resolution must be positive".to_string(),
        ));
    }

    let start = eval_at.saturating_sub(range);
    let mut out: BTreeMap<(String, Vec<crate::Label>), Series> = BTreeMap::new();
    for ts in step_times(start, eval_at, step) {
        let inner_params = QueryParams {
            eval_time: ts,
            prefetch: params.prefetch,
            query_start: params.query_start,
            query_end: params.query_end,
            query_step: params.query_step,
        };
        let value = engine.eval(&subquery.expr, &inner_params)?;
        match value {
            PromqlValue::Scalar(v, _) => {
                out.entry((String::new(), Vec::new()))
                    .or_insert_with(|| Series::new(String::new(), Vec::new()))
                    .samples
                    .push((ts, v));
            }
            PromqlValue::InstantVector(samples) => {
                for sample in samples {
                    let metric = sample.metric;
                    let labels = sample.labels;
                    let histogram = sample.histogram;
                    let value = sample.value;
                    let mut labels = labels;
                    labels.sort();
                    let entry = out
                        .entry((metric.clone(), labels.clone()))
                        .or_insert_with(|| Series::new(metric, labels));
                    if let Some(histogram) = histogram {
                        entry.histograms.push((ts, histogram));
                    } else {
                        entry.samples.push((ts, value));
                    }
                }
            }
            other => {
                return Err(PromqlError::Type(format!(
                    "subquery expects instant vector or scalar expression, got {other:?}"
                )));
            }
        }
    }

    let series = out.into_values().collect();
    Ok(PromqlValue::RangeVector(series))
}
