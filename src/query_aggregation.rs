use crate::value::{i64_to_f64_exact, u64_to_f64_exact};
use crate::{Aggregation, BytesAggregation, DataPoint, Result, TsinkError, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumericDomain {
    F64,
    I64,
    U64,
    Mixed,
}

fn aggregation_name(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::None => "none",
        Aggregation::Sum => "sum",
        Aggregation::Min => "min",
        Aggregation::Max => "max",
        Aggregation::Avg => "avg",
        Aggregation::First => "first",
        Aggregation::Last => "last",
        Aggregation::Count => "count",
        Aggregation::Median => "median",
        Aggregation::Range => "range",
        Aggregation::Variance => "variance",
        Aggregation::StdDev => "stddev",
    }
}

fn is_nan_f64(value: &Value) -> bool {
    matches!(value, Value::F64(v) if v.is_nan())
}

fn numeric_domain(points: &[DataPoint], aggregation: Aggregation) -> Result<Option<NumericDomain>> {
    let mut has_f64 = false;
    let mut has_i64 = false;
    let mut has_u64 = false;

    for point in points {
        match &point.value {
            Value::F64(v) => {
                if !v.is_nan() {
                    has_f64 = true;
                }
            }
            Value::I64(_) => has_i64 = true,
            Value::U64(_) => has_u64 = true,
            _ => {
                return Err(TsinkError::UnsupportedAggregation {
                    aggregation: aggregation_name(aggregation).to_string(),
                    value_type: point.value.kind().to_string(),
                });
            }
        }
    }

    let count = has_f64 as u8 + has_i64 as u8 + has_u64 as u8;
    if count == 0 {
        Ok(None)
    } else if count > 1 {
        Ok(Some(NumericDomain::Mixed))
    } else if has_f64 {
        Ok(Some(NumericDomain::F64))
    } else if has_i64 {
        Ok(Some(NumericDomain::I64))
    } else {
        Ok(Some(NumericDomain::U64))
    }
}

fn numeric_values_f64(points: &[DataPoint], aggregation: Aggregation) -> Result<Vec<f64>> {
    let mut values = Vec::with_capacity(points.len());
    for point in points {
        match &point.value {
            Value::F64(v) if v.is_nan() => {}
            Value::F64(v) => values.push(*v),
            Value::I64(v) => {
                let Some(value) = i64_to_f64_exact(*v) else {
                    return Err(TsinkError::UnsupportedAggregation {
                        aggregation: aggregation_name(aggregation).to_string(),
                        value_type: format!(
                            "{} (cannot be represented exactly as f64)",
                            point.value.kind()
                        ),
                    });
                };
                values.push(value);
            }
            Value::U64(v) => {
                let Some(value) = u64_to_f64_exact(*v) else {
                    return Err(TsinkError::UnsupportedAggregation {
                        aggregation: aggregation_name(aggregation).to_string(),
                        value_type: format!(
                            "{} (cannot be represented exactly as f64)",
                            point.value.kind()
                        ),
                    });
                };
                values.push(value);
            }
            _ => {
                return Err(TsinkError::UnsupportedAggregation {
                    aggregation: aggregation_name(aggregation).to_string(),
                    value_type: point.value.kind().to_string(),
                });
            }
        }
    }
    Ok(values)
}

fn integral_f64_to_i128(value: f64) -> Option<i128> {
    if !value.is_finite() {
        return None;
    }

    let bits = value.to_bits();
    let sign = (bits >> 63) != 0;
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1u64 << 52) - 1);

    let magnitude = if exponent_bits == 0 {
        if fraction == 0 {
            0u128
        } else {
            return None;
        }
    } else {
        let exponent = exponent_bits - 1023;
        let significand = ((1u64 << 52) | fraction) as u128;

        if exponent < 0 {
            return None;
        }

        if exponent >= 52 {
            let shift = (exponent - 52) as u32;
            significand.checked_shl(shift)?
        } else {
            let shift = (52 - exponent) as u32;
            let mask = (1u128 << shift) - 1;
            if (significand & mask) != 0 {
                return None;
            }
            significand >> shift
        }
    };

    if sign {
        let min_abs = (i128::MAX as u128) + 1;
        if magnitude == min_abs {
            Some(i128::MIN)
        } else if magnitude <= i128::MAX as u128 {
            Some(-(magnitude as i128))
        } else {
            None
        }
    } else if magnitude <= i128::MAX as u128 {
        Some(magnitude as i128)
    } else {
        None
    }
}

fn cmp_f64_with_i128(value: f64, integer: i128) -> std::cmp::Ordering {
    if value.is_nan() {
        return value.total_cmp(&(integer as f64));
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        };
    }

    let rounded = integer as f64;
    let ord = value.total_cmp(&rounded);
    if !ord.is_eq() {
        return ord;
    }

    integral_f64_to_i128(value)
        .map(|parsed| parsed.cmp(&integer))
        .unwrap_or(std::cmp::Ordering::Equal)
}

fn value_cmp(lhs: &Value, rhs: &Value) -> Result<std::cmp::Ordering> {
    match (lhs, rhs) {
        (Value::F64(a), Value::F64(b)) => Ok(a.total_cmp(b)),
        (Value::F64(a), Value::I64(b)) => Ok(cmp_f64_with_i128(*a, *b as i128)),
        (Value::F64(a), Value::U64(b)) => Ok(cmp_f64_with_i128(*a, *b as i128)),
        (Value::I64(a), Value::F64(b)) => Ok(cmp_f64_with_i128(*b, *a as i128).reverse()),
        (Value::I64(a), Value::I64(b)) => Ok(a.cmp(b)),
        (Value::I64(a), Value::U64(b)) => Ok((*a as i128).cmp(&(*b as i128))),
        (Value::U64(a), Value::F64(b)) => Ok(cmp_f64_with_i128(*b, *a as i128).reverse()),
        (Value::U64(a), Value::I64(b)) => Ok((*a as i128).cmp(&(*b as i128))),
        (Value::U64(a), Value::U64(b)) => Ok(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
        (Value::Bytes(a), Value::Bytes(b)) => Ok(a.cmp(b)),
        (Value::String(a), Value::String(b)) => Ok(a.cmp(b)),
        (Value::Histogram(_), _) | (_, Value::Histogram(_)) => Err(TsinkError::ValueTypeMismatch {
            expected: "comparable scalar value".to_string(),
            actual: "histogram".to_string(),
        }),
        _ => Err(TsinkError::ValueTypeMismatch {
            expected: lhs.kind().to_string(),
            actual: rhs.kind().to_string(),
        }),
    }
}

fn min_point(points: &[DataPoint]) -> Result<Option<DataPoint>> {
    let mut best: Option<DataPoint> = None;
    for point in points {
        if is_nan_f64(&point.value) {
            continue;
        }
        if matches!(point.value, Value::Histogram(_)) {
            return Err(TsinkError::UnsupportedAggregation {
                aggregation: aggregation_name(Aggregation::Min).to_string(),
                value_type: point.value.kind().to_string(),
            });
        }

        match &best {
            Some(current) if value_cmp(&point.value, &current.value)?.is_lt() => {
                best = Some(point.clone())
            }
            None => best = Some(point.clone()),
            _ => {}
        }
    }
    Ok(best)
}

fn max_point(points: &[DataPoint]) -> Result<Option<DataPoint>> {
    let mut best: Option<DataPoint> = None;
    for point in points {
        if is_nan_f64(&point.value) {
            continue;
        }
        if matches!(point.value, Value::Histogram(_)) {
            return Err(TsinkError::UnsupportedAggregation {
                aggregation: aggregation_name(Aggregation::Max).to_string(),
                value_type: point.value.kind().to_string(),
            });
        }

        match &best {
            Some(current) if value_cmp(&point.value, &current.value)?.is_gt() => {
                best = Some(point.clone())
            }
            None => best = Some(point.clone()),
            _ => {}
        }
    }
    Ok(best)
}

fn sum_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let Some(domain) = numeric_domain(points, Aggregation::Sum)? else {
        return Ok(None);
    };

    let value = match domain {
        NumericDomain::F64 => {
            let sum: f64 = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::F64(v) if !v.is_nan() => Some(*v),
                    _ => None,
                })
                .sum();
            Value::F64(sum)
        }
        NumericDomain::I64 => {
            let sum = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::I64(v) => Some(*v as i128),
                    _ => None,
                })
                .sum::<i128>()
                .clamp(i64::MIN as i128, i64::MAX as i128) as i64;
            Value::I64(sum)
        }
        NumericDomain::U64 => {
            let sum = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::U64(v) => Some(*v as u128),
                    _ => None,
                })
                .sum::<u128>()
                .min(u64::MAX as u128) as u64;
            Value::U64(sum)
        }
        NumericDomain::Mixed => {
            let sum: f64 = numeric_values_f64(points, Aggregation::Sum)?
                .into_iter()
                .sum();
            Value::F64(sum)
        }
    };

    Ok(Some(value))
}

fn average_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let values = numeric_values_f64(points, Aggregation::Avg)?;
    if values.is_empty() {
        return Ok(None);
    }
    let sum: f64 = values.iter().sum();
    Ok(Some(Value::F64(sum / values.len() as f64)))
}

fn median_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let mut values = numeric_values_f64(points, Aggregation::Median)?;
    if values.is_empty() {
        return Ok(None);
    }

    values.sort_by(|a, b| a.total_cmp(b));
    let mid = values.len() / 2;
    let median = if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    };

    Ok(Some(Value::F64(median)))
}

fn range_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let Some(domain) = numeric_domain(points, Aggregation::Range)? else {
        return Ok(None);
    };

    let value = match domain {
        NumericDomain::F64 => {
            let values: Vec<f64> = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::F64(v) if !v.is_nan() => Some(*v),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(None);
            }
            let min = values
                .iter()
                .copied()
                .min_by(|a, b| a.total_cmp(b))
                .unwrap();
            let max = values
                .iter()
                .copied()
                .max_by(|a, b| a.total_cmp(b))
                .unwrap();
            Value::F64(max - min)
        }
        NumericDomain::I64 => {
            let values: Vec<i64> = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::I64(v) => Some(*v),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(None);
            }
            let min = values.iter().min().copied().unwrap() as i128;
            let max = values.iter().max().copied().unwrap() as i128;
            Value::I64((max - min).clamp(i64::MIN as i128, i64::MAX as i128) as i64)
        }
        NumericDomain::U64 => {
            let values: Vec<u64> = points
                .iter()
                .filter_map(|p| match &p.value {
                    Value::U64(v) => Some(*v),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(None);
            }
            let min = values.iter().min().copied().unwrap();
            let max = values.iter().max().copied().unwrap();
            Value::U64(max.saturating_sub(min))
        }
        NumericDomain::Mixed => {
            // Preserve integer precision when the mixed set is i64/u64 only.
            let mut int_min: Option<i128> = None;
            let mut int_max: Option<i128> = None;
            let mut has_f64 = false;

            for point in points {
                match &point.value {
                    Value::I64(v) => {
                        let value = *v as i128;
                        int_min = Some(int_min.map_or(value, |min| min.min(value)));
                        int_max = Some(int_max.map_or(value, |max| max.max(value)));
                    }
                    Value::U64(v) => {
                        let value = *v as i128;
                        int_min = Some(int_min.map_or(value, |min| min.min(value)));
                        int_max = Some(int_max.map_or(value, |max| max.max(value)));
                    }
                    Value::F64(v) if v.is_nan() => {}
                    Value::F64(_) => {
                        has_f64 = true;
                        break;
                    }
                    _ => {
                        return Err(TsinkError::UnsupportedAggregation {
                            aggregation: aggregation_name(Aggregation::Range).to_string(),
                            value_type: point.value.kind().to_string(),
                        });
                    }
                }
            }

            if !has_f64 {
                let (Some(min), Some(max)) = (int_min, int_max) else {
                    return Ok(None);
                };
                return Ok(Some(Value::F64((max - min) as f64)));
            }

            let values = numeric_values_f64(points, Aggregation::Range)?;
            if values.is_empty() {
                return Ok(None);
            }
            let min = values
                .iter()
                .copied()
                .min_by(|a, b| a.total_cmp(b))
                .unwrap();
            let max = values
                .iter()
                .copied()
                .max_by(|a, b| a.total_cmp(b))
                .unwrap();
            Value::F64(max - min)
        }
    };

    Ok(Some(value))
}

fn population_variance_numeric(points: &[DataPoint]) -> Result<Option<Value>> {
    let values = numeric_values_f64(points, Aggregation::Variance)?;
    if values.is_empty() {
        return Ok(None);
    }

    let mut count = 0.0f64;
    let mut mean = 0.0f64;
    let mut m2 = 0.0f64;

    for value in values {
        count += 1.0;
        let delta = value - mean;
        mean += delta / count;
        let delta2 = value - mean;
        m2 += delta * delta2;
    }

    Ok(Some(Value::F64(m2 / count)))
}

pub(crate) fn aggregate_series(
    points: &[DataPoint],
    aggregation: Aggregation,
) -> Result<Option<DataPoint>> {
    let Some(last) = points.last() else {
        return Ok(None);
    };

    let aggregated = match aggregation {
        Aggregation::None => None,
        Aggregation::First => return Ok(points.first().cloned()),
        Aggregation::Last => return Ok(points.last().cloned()),
        Aggregation::Count => Some(DataPoint::new(
            last.timestamp,
            Value::U64(
                points
                    .iter()
                    .filter(|point| !is_nan_f64(&point.value))
                    .count() as u64,
            ),
        )),
        Aggregation::Sum => sum_numeric(points)?.map(|value| DataPoint::new(last.timestamp, value)),
        Aggregation::Avg => {
            average_numeric(points)?.map(|value| DataPoint::new(last.timestamp, value))
        }
        Aggregation::Min => min_point(points)?,
        Aggregation::Max => max_point(points)?,
        Aggregation::Median => {
            median_numeric(points)?.map(|value| DataPoint::new(last.timestamp, value))
        }
        Aggregation::Range => {
            range_numeric(points)?.map(|value| DataPoint::new(last.timestamp, value))
        }
        Aggregation::Variance => {
            population_variance_numeric(points)?.map(|value| DataPoint::new(last.timestamp, value))
        }
        Aggregation::StdDev => population_variance_numeric(points)?
            .and_then(|value| match value {
                Value::F64(v) => Some(Value::F64(v.sqrt())),
                _ => None,
            })
            .map(|value| DataPoint::new(last.timestamp, value)),
    };

    Ok(aggregated)
}

fn aggregate_bucket(
    points: &[DataPoint],
    aggregation: Aggregation,
    bucket_start: i64,
) -> Result<Option<DataPoint>> {
    if points.is_empty() {
        return Ok(None);
    }

    let aggregated = match aggregation {
        Aggregation::None => None,
        Aggregation::First => points
            .first()
            .map(|point| DataPoint::new(bucket_start, point.value.clone())),
        Aggregation::Last => points
            .last()
            .map(|point| DataPoint::new(bucket_start, point.value.clone())),
        Aggregation::Count => Some(DataPoint::new(
            bucket_start,
            Value::U64(
                points
                    .iter()
                    .filter(|point| !is_nan_f64(&point.value))
                    .count() as u64,
            ),
        )),
        Aggregation::Sum => sum_numeric(points)?.map(|value| DataPoint::new(bucket_start, value)),
        Aggregation::Avg => {
            average_numeric(points)?.map(|value| DataPoint::new(bucket_start, value))
        }
        Aggregation::Min => {
            min_point(points)?.map(|point| DataPoint::new(bucket_start, point.value))
        }
        Aggregation::Max => {
            max_point(points)?.map(|point| DataPoint::new(bucket_start, point.value))
        }
        Aggregation::Median => {
            median_numeric(points)?.map(|value| DataPoint::new(bucket_start, value))
        }
        Aggregation::Range => {
            range_numeric(points)?.map(|value| DataPoint::new(bucket_start, value))
        }
        Aggregation::Variance => {
            population_variance_numeric(points)?.map(|value| DataPoint::new(bucket_start, value))
        }
        Aggregation::StdDev => population_variance_numeric(points)?
            .and_then(|value| match value {
                Value::F64(v) => Some(Value::F64(v.sqrt())),
                _ => None,
            })
            .map(|value| DataPoint::new(bucket_start, value)),
    };

    Ok(aggregated)
}

pub(crate) fn bucket_start_for_origin(ts: i64, origin: i64, interval: i64) -> i64 {
    let rel = ts as i128 - origin as i128;
    let bucket = origin as i128 + rel.div_euclid(interval as i128) * interval as i128;
    bucket.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

pub(crate) fn downsample_points(
    points: &[DataPoint],
    interval: i64,
    aggregation: Aggregation,
    start: i64,
    end: i64,
) -> Result<Vec<DataPoint>> {
    downsample_points_with_origin(points, interval, aggregation, start, start, end)
}

pub(crate) fn downsample_points_with_origin(
    points: &[DataPoint],
    interval: i64,
    aggregation: Aggregation,
    origin: i64,
    start: i64,
    end: i64,
) -> Result<Vec<DataPoint>> {
    if points.is_empty() || interval <= 0 || start >= end {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    let mut idx = 0;

    while idx < points.len() && points[idx].timestamp < start {
        idx += 1;
    }

    while idx < points.len() {
        if points[idx].timestamp >= end {
            break;
        }

        let bucket_start = bucket_start_for_origin(points[idx].timestamp, origin, interval);
        let bucket_end = bucket_start.saturating_add(interval);
        let bucket_begin = idx;

        while idx < points.len() {
            let ts = points[idx].timestamp;
            if ts >= end || ts >= bucket_end {
                break;
            }
            idx += 1;
        }

        if let Some(dp) = aggregate_bucket(&points[bucket_begin..idx], aggregation, bucket_start)? {
            result.push(dp);
        }
    }

    Ok(result)
}

pub(crate) fn downsample_points_with_custom(
    points: &[DataPoint],
    interval: i64,
    aggregation: &dyn BytesAggregation,
    start: i64,
    end: i64,
) -> Result<Vec<DataPoint>> {
    if points.is_empty() || interval <= 0 || start >= end {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    let mut idx = 0;

    while idx < points.len() && points[idx].timestamp < start {
        idx += 1;
    }

    while idx < points.len() {
        if points[idx].timestamp >= end {
            break;
        }

        let bucket_start = bucket_start_for_origin(points[idx].timestamp, start, interval);
        let bucket_end = bucket_start.saturating_add(interval);
        let bucket_begin = idx;

        while idx < points.len() {
            let ts = points[idx].timestamp;
            if ts >= end || ts >= bucket_end {
                break;
            }
            idx += 1;
        }

        if let Some(dp) = aggregation.aggregate_bucket(&points[bucket_begin..idx], bucket_start)? {
            result.push(dp);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::{aggregate_bucket, aggregate_series};
    use crate::{Aggregation, DataPoint, TsinkError, Value};

    #[test]
    fn mixed_i64_u64_min_max_use_integer_order_for_large_values() {
        let larger = i64::MAX;
        let smaller = (i64::MAX as u64).saturating_sub(1);

        let min_points = vec![
            DataPoint::new(1, Value::I64(larger)),
            DataPoint::new(2, Value::U64(smaller)),
        ];
        let min = aggregate_series(&min_points, Aggregation::Min)
            .unwrap()
            .unwrap();
        assert_eq!(min.value, Value::U64(smaller));

        let max_points = vec![
            DataPoint::new(1, Value::U64(smaller)),
            DataPoint::new(2, Value::I64(larger)),
        ];
        let max = aggregate_series(&max_points, Aggregation::Max)
            .unwrap()
            .unwrap();
        assert_eq!(max.value, Value::I64(larger));
    }

    #[test]
    fn mixed_i64_u64_range_preserves_unit_difference_at_high_values() {
        let points = vec![
            DataPoint::new(1, Value::I64(i64::MAX - 1)),
            DataPoint::new(2, Value::U64(i64::MAX as u64)),
        ];

        let range = aggregate_series(&points, Aggregation::Range)
            .unwrap()
            .unwrap();
        assert_eq!(range.value, Value::F64(1.0));
    }

    #[test]
    fn mixed_i64_f64_min_max_preserve_order_for_large_values() {
        let larger_f64 = i64::MAX as f64;
        let smaller_i64 = i64::MAX - 1;

        let min_points = vec![
            DataPoint::new(1, Value::F64(larger_f64)),
            DataPoint::new(2, Value::I64(smaller_i64)),
        ];
        let min = aggregate_series(&min_points, Aggregation::Min)
            .unwrap()
            .unwrap();
        assert_eq!(min.value, Value::I64(smaller_i64));

        let max_points = vec![
            DataPoint::new(1, Value::I64(smaller_i64)),
            DataPoint::new(2, Value::F64(larger_f64)),
        ];
        let max = aggregate_series(&max_points, Aggregation::Max)
            .unwrap()
            .unwrap();
        assert_eq!(max.value, Value::F64(larger_f64));
    }

    #[test]
    fn mixed_u64_f64_min_max_preserve_order_for_large_values() {
        let larger_f64 = u64::MAX as f64;
        let smaller_u64 = u64::MAX - 1;

        let min_points = vec![
            DataPoint::new(1, Value::F64(larger_f64)),
            DataPoint::new(2, Value::U64(smaller_u64)),
        ];
        let min = aggregate_series(&min_points, Aggregation::Min)
            .unwrap()
            .unwrap();
        assert_eq!(min.value, Value::U64(smaller_u64));

        let max_points = vec![
            DataPoint::new(1, Value::U64(smaller_u64)),
            DataPoint::new(2, Value::F64(larger_f64)),
        ];
        let max = aggregate_series(&max_points, Aggregation::Max)
            .unwrap()
            .unwrap();
        assert_eq!(max.value, Value::F64(larger_f64));
    }

    #[test]
    fn variance_rejects_non_representable_large_integers() {
        let points = vec![
            DataPoint::new(1, Value::I64(i64::MAX - 1)),
            DataPoint::new(2, Value::I64(i64::MAX)),
        ];

        let err = aggregate_series(&points, Aggregation::Variance).unwrap_err();
        assert!(matches!(err, TsinkError::UnsupportedAggregation { .. }));
        assert!(err
            .to_string()
            .contains("cannot be represented exactly as f64"));
    }

    #[test]
    fn all_nan_numeric_aggregations_return_none_for_series() {
        let points = vec![
            DataPoint::new(1, Value::F64(f64::NAN)),
            DataPoint::new(2, Value::F64(f64::NAN)),
        ];

        for aggregation in [
            Aggregation::Sum,
            Aggregation::Avg,
            Aggregation::Min,
            Aggregation::Max,
            Aggregation::Median,
            Aggregation::Range,
            Aggregation::Variance,
            Aggregation::StdDev,
        ] {
            assert!(aggregate_series(&points, aggregation).unwrap().is_none());
        }
    }

    #[test]
    fn all_nan_numeric_aggregations_return_none_for_bucket() {
        let points = vec![
            DataPoint::new(1, Value::F64(f64::NAN)),
            DataPoint::new(2, Value::F64(f64::NAN)),
        ];

        for aggregation in [
            Aggregation::Sum,
            Aggregation::Avg,
            Aggregation::Min,
            Aggregation::Max,
            Aggregation::Median,
            Aggregation::Range,
            Aggregation::Variance,
            Aggregation::StdDev,
        ] {
            assert!(aggregate_bucket(&points, aggregation, 1_000)
                .unwrap()
                .is_none());
        }
    }
}
