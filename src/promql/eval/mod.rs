mod aggregation;
mod binary;
mod functions;
mod selector;
pub mod time;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::promql::ast::{Expr, MatrixSelector, UnaryOp, VectorSelector};
use crate::promql::error::{PromqlError, Result};
use crate::promql::types::{PromqlValue, Series};
use crate::{DataPoint, Label, Storage, TimestampPrecision};

use self::time::{duration_to_units, step_times};

const DEFAULT_LOOKBACK_DELTA_MS: i64 = 5 * 60 * 1_000;

pub struct Engine {
    storage: Arc<dyn Storage>,
    default_lookback_delta: i64,
    timestamp_units_per_second: i64,
}

#[derive(Clone)]
pub(crate) struct QueryParams<'a> {
    pub eval_time: i64,
    pub prefetch: Option<&'a PrefetchCache>,
}

#[derive(Clone, Default)]
pub(crate) struct PrefetchCache {
    by_metric: HashMap<String, Vec<(Vec<Label>, Vec<DataPoint>)>>,
}

impl PrefetchCache {
    fn insert(&mut self, metric: String, data: Vec<(Vec<Label>, Vec<DataPoint>)>) {
        self.by_metric.insert(metric, data);
    }

    pub(crate) fn get(&self, metric: &str) -> Option<&Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.by_metric.get(metric)
    }
}

impl Engine {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self::with_precision(storage, TimestampPrecision::Nanoseconds)
    }

    pub fn with_precision(storage: Arc<dyn Storage>, precision: TimestampPrecision) -> Self {
        let units_per_second = match precision {
            TimestampPrecision::Seconds => 1,
            TimestampPrecision::Milliseconds => 1_000,
            TimestampPrecision::Microseconds => 1_000_000,
            TimestampPrecision::Nanoseconds => 1_000_000_000,
        };

        Self {
            storage,
            default_lookback_delta: duration_to_units(DEFAULT_LOOKBACK_DELTA_MS, units_per_second),
            timestamp_units_per_second: units_per_second,
        }
    }

    pub fn instant_query(&self, query_str: &str, time: i64) -> Result<PromqlValue> {
        let expr = crate::promql::parse(query_str)?;
        let params = QueryParams {
            eval_time: time,
            prefetch: None,
        };
        self.eval(&expr, &params)
    }

    pub fn range_query(
        &self,
        query_str: &str,
        start: i64,
        end: i64,
        step: i64,
    ) -> Result<PromqlValue> {
        if step <= 0 {
            return Err(PromqlError::Eval("range step must be positive".to_string()));
        }
        if start > end {
            return Err(PromqlError::Eval(format!(
                "invalid range: start ({start}) is greater than end ({end})"
            )));
        }

        let expr = crate::promql::parse(query_str)?;
        let prefetch = self.build_prefetch_cache(&expr, start, end)?;

        let mut out: BTreeMap<(String, Vec<Label>), Vec<(i64, f64)>> = BTreeMap::new();
        for ts in step_times(start, end, step) {
            let params = QueryParams {
                eval_time: ts,
                prefetch: Some(&prefetch),
            };
            let val = self.eval(&expr, &params)?;
            Self::append_step_value(&mut out, ts, val);
        }

        let series = out
            .into_iter()
            .map(|((metric, labels), samples)| Series {
                metric,
                labels,
                samples,
            })
            .collect();

        Ok(PromqlValue::RangeVector(series))
    }

    fn append_step_value(
        out: &mut BTreeMap<(String, Vec<Label>), Vec<(i64, f64)>>,
        step_ts: i64,
        value: PromqlValue,
    ) {
        match value {
            PromqlValue::Scalar(v, _) => {
                out.entry((String::new(), Vec::new()))
                    .or_default()
                    .push((step_ts, v));
            }
            PromqlValue::InstantVector(samples) => {
                for sample in samples {
                    let mut labels = sample.labels;
                    labels.sort();
                    out.entry((sample.metric, labels))
                        .or_default()
                        .push((step_ts, sample.value));
                }
            }
            PromqlValue::RangeVector(series) => {
                for mut s in series {
                    if let Some((_, v)) = s.samples.pop() {
                        s.labels.sort();
                        out.entry((s.metric, s.labels))
                            .or_default()
                            .push((step_ts, v));
                    }
                }
            }
            PromqlValue::String(_, _) => {}
        }
    }

    pub(crate) fn eval(&self, expr: &Expr, params: &QueryParams<'_>) -> Result<PromqlValue> {
        match expr {
            Expr::NumberLiteral(v) => Ok(PromqlValue::Scalar(*v, params.eval_time)),
            Expr::StringLiteral(v) => Ok(PromqlValue::String(v.clone(), params.eval_time)),
            Expr::Paren(expr) => self.eval(expr, params),
            Expr::VectorSelector(selector) => {
                selector::eval_vector_selector(self, selector, params)
            }
            Expr::MatrixSelector(selector) => {
                selector::eval_matrix_selector(self, selector, params)
            }
            Expr::Unary(unary) => {
                let value = self.eval(&unary.expr, params)?;
                match unary.op {
                    UnaryOp::Neg => Ok(negate_value(value)),
                }
            }
            Expr::Binary(binary) => binary::eval_binary(self, binary, params),
            Expr::Aggregation(agg) => aggregation::eval_aggregation(self, agg, params),
            Expr::Call(call) => functions::eval_call(self, call, params),
        }
    }

    pub(crate) fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    pub(crate) fn default_lookback_delta(&self) -> i64 {
        self.default_lookback_delta
    }

    pub(crate) fn timestamp_units_per_second(&self) -> i64 {
        self.timestamp_units_per_second
    }

    fn build_prefetch_cache(&self, expr: &Expr, start: i64, end: i64) -> Result<PrefetchCache> {
        let mut metrics = BTreeSet::new();
        let mut max_window = self.default_lookback_delta;
        self.collect_prefetch_requirements(expr, &mut metrics, &mut max_window);

        let fetch_start = start.saturating_sub(max_window);
        let fetch_end = end.saturating_add(1);

        let mut cache = PrefetchCache::default();
        for metric in metrics {
            let data = self.storage.select_all(&metric, fetch_start, fetch_end)?;
            cache.insert(metric, data);
        }

        Ok(cache)
    }

    fn collect_prefetch_requirements(
        &self,
        expr: &Expr,
        metrics: &mut BTreeSet<String>,
        max_window: &mut i64,
    ) {
        match expr {
            Expr::VectorSelector(VectorSelector {
                metric_name,
                offset,
                ..
            }) => {
                if let Some(metric) = metric_name {
                    metrics.insert(metric.clone());
                }
                let offset = duration_to_units(*offset, self.timestamp_units_per_second);
                *max_window = (*max_window).max(self.default_lookback_delta.saturating_add(offset));
            }
            Expr::MatrixSelector(MatrixSelector { vector, range }) => {
                if let Some(metric) = &vector.metric_name {
                    metrics.insert(metric.clone());
                }
                let offset = duration_to_units(vector.offset, self.timestamp_units_per_second);
                let range = duration_to_units(*range, self.timestamp_units_per_second);
                *max_window = (*max_window).max(
                    self.default_lookback_delta
                        .saturating_add(offset)
                        .saturating_add(range),
                );
            }
            Expr::Unary(u) => self.collect_prefetch_requirements(&u.expr, metrics, max_window),
            Expr::Binary(b) => {
                self.collect_prefetch_requirements(&b.lhs, metrics, max_window);
                self.collect_prefetch_requirements(&b.rhs, metrics, max_window);
            }
            Expr::Aggregation(a) => {
                if let Some(param) = &a.param {
                    self.collect_prefetch_requirements(param, metrics, max_window);
                }
                self.collect_prefetch_requirements(&a.expr, metrics, max_window);
            }
            Expr::Call(c) => {
                for arg in &c.args {
                    self.collect_prefetch_requirements(arg, metrics, max_window);
                }
            }
            Expr::Paren(inner) => self.collect_prefetch_requirements(inner, metrics, max_window),
            Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
        }
    }
}

fn negate_value(value: PromqlValue) -> PromqlValue {
    match value {
        PromqlValue::Scalar(v, ts) => PromqlValue::Scalar(-v, ts),
        PromqlValue::InstantVector(mut samples) => {
            for s in &mut samples {
                s.value = -s.value;
            }
            PromqlValue::InstantVector(samples)
        }
        PromqlValue::RangeVector(mut series) => {
            for s in &mut series {
                for (_, v) in &mut s.samples {
                    *v = -*v;
                }
            }
            PromqlValue::RangeVector(series)
        }
        PromqlValue::String(v, ts) => PromqlValue::String(v, ts),
    }
}
