mod aggregation;
mod binary;
mod functions;
mod selector;
mod subquery;
pub mod time;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::promql::ast::{AtModifier, Expr, MatrixSelector, SubqueryExpr, UnaryOp, VectorSelector};
use crate::promql::error::{PromqlError, Result};
use crate::promql::types::{PromqlValue, Series};
use crate::{DataPoint, Label, Storage, TimestampPrecision};

use self::time::{duration_to_units, step_times};

const DEFAULT_LOOKBACK_DELTA_MS: i64 = 5 * 60 * 1_000;
const DEFAULT_SUBQUERY_STEP_MS: i64 = 60 * 1_000;

type LabelPoints = (Vec<Label>, Vec<DataPoint>);
type MetricPrefetchRows = Vec<LabelPoints>;
type RangeSeriesKey = (String, Vec<Label>);
type RangeSeriesAccumulator = Series;

pub struct Engine {
    storage: Arc<dyn Storage>,
    default_lookback_delta: i64,
    timestamp_units_per_second: i64,
}

#[derive(Clone)]
pub(crate) struct QueryParams<'a> {
    pub eval_time: i64,
    pub prefetch: Option<&'a PrefetchCache>,
    pub query_start: i64,
    pub query_end: i64,
    pub query_step: Option<i64>,
}

#[derive(Clone, Default)]
pub(crate) struct PrefetchCache {
    by_metric: HashMap<String, MetricPrefetchRows>,
}

impl PrefetchCache {
    fn insert(&mut self, metric: String, data: MetricPrefetchRows) {
        self.by_metric.insert(metric, data);
    }

    pub(crate) fn get(&self, metric: &str) -> Option<&MetricPrefetchRows> {
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
            query_start: time,
            query_end: time,
            query_step: None,
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
        let prefetch = if self.supports_prefetch(&expr) {
            Some(self.build_prefetch_cache(&expr, start, end)?)
        } else {
            None
        };

        let mut out: BTreeMap<RangeSeriesKey, RangeSeriesAccumulator> = BTreeMap::new();
        for ts in step_times(start, end, step) {
            let params = QueryParams {
                eval_time: ts,
                prefetch: prefetch.as_ref(),
                query_start: start,
                query_end: end,
                query_step: Some(step),
            };
            let val = self.eval(&expr, &params)?;
            Self::append_step_value(&mut out, ts, val);
        }

        let series = out.into_values().collect();

        Ok(PromqlValue::RangeVector(series))
    }

    fn append_step_value(
        out: &mut BTreeMap<RangeSeriesKey, RangeSeriesAccumulator>,
        step_ts: i64,
        value: PromqlValue,
    ) {
        match value {
            PromqlValue::Scalar(v, _) => {
                out.entry((String::new(), Vec::new()))
                    .or_insert_with(|| Series::new(String::new(), Vec::new()))
                    .samples
                    .push((step_ts, v));
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
                        entry.histograms.push((step_ts, histogram));
                    } else {
                        entry.samples.push((step_ts, value));
                    }
                }
            }
            PromqlValue::RangeVector(series) => {
                for mut series in series {
                    let latest_float = series.samples.last().cloned();
                    let latest_histogram = series.histograms.last().cloned();
                    let Some(latest_point) = latest_series_point(latest_float, latest_histogram)
                    else {
                        continue;
                    };
                    series.labels.sort();
                    let entry = out
                        .entry((series.metric.clone(), series.labels.clone()))
                        .or_insert_with(|| Series::new(series.metric, series.labels));
                    match latest_point {
                        LatestSeriesPoint::Float(value) => entry.samples.push((step_ts, value)),
                        LatestSeriesPoint::Histogram(histogram) => {
                            entry.histograms.push((step_ts, histogram))
                        }
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
            Expr::Subquery(subquery) => subquery::eval_subquery(self, subquery, params),
            Expr::Unary(unary) => {
                let value = self.eval(&unary.expr, params)?;
                match unary.op {
                    UnaryOp::Pos => Ok(value),
                    UnaryOp::Neg => negate_value(value),
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

    pub(crate) fn default_subquery_step(&self) -> i64 {
        duration_to_units(DEFAULT_SUBQUERY_STEP_MS, self.timestamp_units_per_second)
    }

    fn build_prefetch_cache(&self, expr: &Expr, start: i64, end: i64) -> Result<PrefetchCache> {
        let mut metrics = BTreeSet::new();
        let mut max_past_window = self.default_lookback_delta;
        let mut max_future_window = 0;
        self.collect_prefetch_requirements(
            expr,
            &mut metrics,
            &mut max_past_window,
            &mut max_future_window,
        );

        let fetch_start = start.saturating_sub(max_past_window);
        let fetch_end = end.saturating_add(max_future_window).saturating_add(1);

        let mut cache = PrefetchCache::default();
        for metric in metrics {
            let data = self.storage.select_all(&metric, fetch_start, fetch_end)?;
            cache.insert(metric, data);
        }

        Ok(cache)
    }

    fn supports_prefetch(&self, expr: &Expr) -> bool {
        !expr_uses_dynamic_time(expr)
    }

    fn collect_prefetch_requirements(
        &self,
        expr: &Expr,
        metrics: &mut BTreeSet<String>,
        max_past_window: &mut i64,
        max_future_window: &mut i64,
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
                let past = self.default_lookback_delta.saturating_add(offset.max(0));
                let future = if offset < 0 {
                    offset.saturating_neg()
                } else {
                    0
                };
                *max_past_window = (*max_past_window).max(past);
                *max_future_window = (*max_future_window).max(future);
            }
            Expr::MatrixSelector(MatrixSelector { vector, range }) => {
                if let Some(metric) = &vector.metric_name {
                    metrics.insert(metric.clone());
                }
                let offset = duration_to_units(vector.offset, self.timestamp_units_per_second);
                let range = duration_to_units(*range, self.timestamp_units_per_second);
                let past = range.saturating_add(offset.max(0));
                let future = if offset < 0 {
                    offset.saturating_neg()
                } else {
                    0
                };
                *max_past_window = (*max_past_window).max(past);
                *max_future_window = (*max_future_window).max(future);
            }
            Expr::Unary(u) => self.collect_prefetch_requirements(
                &u.expr,
                metrics,
                max_past_window,
                max_future_window,
            ),
            Expr::Binary(b) => {
                self.collect_prefetch_requirements(
                    &b.lhs,
                    metrics,
                    max_past_window,
                    max_future_window,
                );
                self.collect_prefetch_requirements(
                    &b.rhs,
                    metrics,
                    max_past_window,
                    max_future_window,
                );
            }
            Expr::Aggregation(a) => {
                if let Some(param) = &a.param {
                    self.collect_prefetch_requirements(
                        param,
                        metrics,
                        max_past_window,
                        max_future_window,
                    );
                }
                self.collect_prefetch_requirements(
                    &a.expr,
                    metrics,
                    max_past_window,
                    max_future_window,
                );
            }
            Expr::Call(c) => {
                for arg in &c.args {
                    self.collect_prefetch_requirements(
                        arg,
                        metrics,
                        max_past_window,
                        max_future_window,
                    );
                }
            }
            Expr::Subquery(SubqueryExpr { expr, .. }) => self.collect_prefetch_requirements(
                expr,
                metrics,
                max_past_window,
                max_future_window,
            ),
            Expr::Paren(inner) => self.collect_prefetch_requirements(
                inner,
                metrics,
                max_past_window,
                max_future_window,
            ),
            Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
        }
    }
}

fn expr_uses_dynamic_time(expr: &Expr) -> bool {
    match expr {
        Expr::VectorSelector(selector) => selector.at.is_some(),
        Expr::MatrixSelector(selector) => selector.vector.at.is_some(),
        Expr::Subquery(_) => true,
        Expr::Unary(unary) => expr_uses_dynamic_time(&unary.expr),
        Expr::Binary(binary) => {
            expr_uses_dynamic_time(&binary.lhs) || expr_uses_dynamic_time(&binary.rhs)
        }
        Expr::Aggregation(aggregation) => {
            aggregation
                .param
                .as_ref()
                .is_some_and(|param| expr_uses_dynamic_time(param))
                || expr_uses_dynamic_time(&aggregation.expr)
        }
        Expr::Call(call) => call.args.iter().any(expr_uses_dynamic_time),
        Expr::Paren(inner) => expr_uses_dynamic_time(inner),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => false,
    }
}

pub(crate) fn resolve_at_modifier(
    at: Option<&AtModifier>,
    params: &QueryParams<'_>,
    units_per_second: i64,
) -> Result<i64> {
    match at {
        None => Ok(params.eval_time),
        Some(AtModifier::Start) => Ok(params.query_start),
        Some(AtModifier::End) => Ok(params.query_end),
        Some(AtModifier::Timestamp(timestamp)) => timestamp_to_units(*timestamp, units_per_second),
    }
}

pub(crate) fn timestamp_to_units(timestamp: f64, units_per_second: i64) -> Result<i64> {
    if !timestamp.is_finite() {
        return Err(PromqlError::Eval(
            "@ timestamp must be a finite numeric value".to_string(),
        ));
    }

    let scaled = timestamp * units_per_second as f64;
    if !scaled.is_finite() || scaled < i64::MIN as f64 || scaled > i64::MAX as f64 {
        return Err(PromqlError::Eval(
            "@ timestamp is out of supported range".to_string(),
        ));
    }

    Ok(scaled.round() as i64)
}

fn negate_value(value: PromqlValue) -> Result<PromqlValue> {
    match value {
        PromqlValue::Scalar(v, ts) => Ok(PromqlValue::Scalar(-v, ts)),
        PromqlValue::InstantVector(mut samples) => {
            for s in &mut samples {
                if s.histogram.is_some() {
                    return Err(PromqlError::Eval(
                        "unary '-' does not support histogram samples yet".to_string(),
                    ));
                }
                s.value = -s.value;
            }
            Ok(PromqlValue::InstantVector(samples))
        }
        PromqlValue::RangeVector(mut series) => {
            for s in &mut series {
                if !s.histograms.is_empty() {
                    return Err(PromqlError::Eval(
                        "unary '-' does not support histogram samples yet".to_string(),
                    ));
                }
                for (_, v) in &mut s.samples {
                    *v = -*v;
                }
            }
            Ok(PromqlValue::RangeVector(series))
        }
        PromqlValue::String(v, ts) => Ok(PromqlValue::String(v, ts)),
    }
}

enum LatestSeriesPoint {
    Float(f64),
    Histogram(Box<crate::NativeHistogram>),
}

fn latest_series_point(
    latest_float: Option<(i64, f64)>,
    latest_histogram: Option<(i64, Box<crate::NativeHistogram>)>,
) -> Option<LatestSeriesPoint> {
    match (latest_float, latest_histogram) {
        (Some((float_ts, value)), Some((hist_ts, histogram))) => {
            if hist_ts > float_ts {
                Some(LatestSeriesPoint::Histogram(histogram))
            } else {
                Some(LatestSeriesPoint::Float(value))
            }
        }
        (Some((_, value)), None) => Some(LatestSeriesPoint::Float(value)),
        (None, Some((_, histogram))) => Some(LatestSeriesPoint::Histogram(histogram)),
        (None, None) => None,
    }
}
