//! Typed value model and extensibility traits.

use crate::{DataPoint, Result, TsinkError};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::iter::Sum;
use std::ops::{Div, Sub};

fn f64_eq(lhs: f64, rhs: f64) -> bool {
    lhs == rhs || (lhs.is_nan() && rhs.is_nan())
}

fn f64_slice_eq(lhs: &[f64], rhs: &[f64]) -> bool {
    lhs.len() == rhs.len()
        && lhs
            .iter()
            .zip(rhs)
            .all(|(left, right)| f64_eq(*left, *right))
}

/// Count representation for a native histogram sample.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HistogramCount {
    Int(u64),
    Float(f64),
}

impl PartialEq for HistogramCount {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int(lhs), Self::Int(rhs)) => lhs == rhs,
            (Self::Float(lhs), Self::Float(rhs)) => f64_eq(*lhs, *rhs),
            _ => false,
        }
    }
}

impl Eq for HistogramCount {}

impl HistogramCount {
    pub fn as_f64(&self) -> f64 {
        match self {
            Self::Int(value) => *value as f64,
            Self::Float(value) => *value,
        }
    }
}

/// Sparse bucket span used by native histogram payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistogramBucketSpan {
    pub offset: i32,
    pub length: u32,
}

/// Reset hint attached to a native histogram sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistogramResetHint {
    Unknown,
    Yes,
    No,
    Gauge,
}

/// First-class native histogram payload stored inside a data point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeHistogram {
    pub count: Option<HistogramCount>,
    pub sum: f64,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: Option<HistogramCount>,
    pub negative_spans: Vec<HistogramBucketSpan>,
    pub negative_deltas: Vec<i64>,
    pub negative_counts: Vec<f64>,
    pub positive_spans: Vec<HistogramBucketSpan>,
    pub positive_deltas: Vec<i64>,
    pub positive_counts: Vec<f64>,
    pub reset_hint: HistogramResetHint,
    pub custom_values: Vec<f64>,
}

impl PartialEq for NativeHistogram {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count
            && f64_eq(self.sum, other.sum)
            && self.schema == other.schema
            && f64_eq(self.zero_threshold, other.zero_threshold)
            && self.zero_count == other.zero_count
            && self.negative_spans == other.negative_spans
            && self.negative_deltas == other.negative_deltas
            && f64_slice_eq(&self.negative_counts, &other.negative_counts)
            && self.positive_spans == other.positive_spans
            && self.positive_deltas == other.positive_deltas
            && f64_slice_eq(&self.positive_counts, &other.positive_counts)
            && self.reset_hint == other.reset_hint
            && f64_slice_eq(&self.custom_values, &other.custom_values)
    }
}

impl Eq for NativeHistogram {}

/// Typed payload value stored in a data point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    F64(f64),
    I64(i64),
    U64(u64),
    Bool(bool),
    Bytes(Vec<u8>),
    String(String),
    Histogram(Box<NativeHistogram>),
}

const F64_EXACT_INT_BITS: u32 = f64::MANTISSA_DIGITS;

fn u64_is_exact_in_f64(value: u64) -> bool {
    if value <= (1u64 << F64_EXACT_INT_BITS) {
        return true;
    }

    let bit_len = u64::BITS - value.leading_zeros();
    let shift = bit_len - F64_EXACT_INT_BITS;
    let mask = (1u64 << shift) - 1;
    (value & mask) == 0
}

pub(crate) fn u64_to_f64_exact(value: u64) -> Option<f64> {
    if u64_is_exact_in_f64(value) {
        Some(value as f64)
    } else {
        None
    }
}

pub(crate) fn i64_to_f64_exact(value: i64) -> Option<f64> {
    if u64_is_exact_in_f64(value.unsigned_abs()) {
        Some(value as f64)
    } else {
        None
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::F64(a), Value::F64(b)) => f64_eq(*a, *b),
            (Value::I64(a), Value::I64(b)) => a == b,
            (Value::U64(a), Value::U64(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Histogram(a), Value::Histogram(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Value {
    pub fn kind(&self) -> &'static str {
        match self {
            Value::F64(_) => "f64",
            Value::I64(_) => "i64",
            Value::U64(_) => "u64",
            Value::Bool(_) => "bool",
            Value::Bytes(_) => "bytes",
            Value::String(_) => "string",
            Value::Histogram(_) => "histogram",
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::F64(v) => Some(*v),
            Value::I64(v) => i64_to_f64_exact(*v),
            Value::U64(v) => u64_to_f64_exact(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::I64(v) => Some(*v),
            Value::U64(v) => i64::try_from(*v).ok(),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::U64(v) => Some(*v),
            Value::I64(v) => u64::try_from(*v).ok(),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v.as_str()),
            _ => None,
        }
    }

    pub fn as_histogram(&self) -> Option<&NativeHistogram> {
        match self {
            Value::Histogram(histogram) => Some(histogram.as_ref()),
            _ => None,
        }
    }

    /// Encodes a user type into raw bytes using the provided codec.
    pub fn encode_with<T, C: Codec<Item = T>>(value: &T, codec: &C) -> Result<Self> {
        Ok(Value::Bytes(codec.encode(value)?))
    }

    /// Decodes raw bytes into a user type using the provided codec.
    pub fn decode_with<T, C: Codec<Item = T>>(&self, codec: &C) -> Result<T> {
        match self {
            Value::Bytes(bytes) => codec.decode(bytes),
            other => Err(TsinkError::ValueTypeMismatch {
                expected: "bytes".to_string(),
                actual: other.kind().to_string(),
            }),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::F64(v) => write!(f, "{v}"),
            Value::I64(v) => write!(f, "{v}"),
            Value::U64(v) => write!(f, "{v}"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::Bytes(v) => write!(f, "bytes(len={})", v.len()),
            Value::String(v) => write!(f, "{v}"),
            Value::Histogram(_) => write!(f, "histogram"),
        }
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::F64(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::I64(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::I64(value as i64)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::U64(value)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Value::U64(value as u64)
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        Value::U64(value as u64)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::Bytes(value)
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Value::Bytes(value.to_vec())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl From<NativeHistogram> for Value {
    fn from(value: NativeHistogram) -> Self {
        Value::Histogram(Box::new(value))
    }
}

impl PartialEq<f64> for Value {
    fn eq(&self, other: &f64) -> bool {
        self.as_f64().is_some_and(|v| v == *other)
    }
}

impl PartialEq<Value> for f64 {
    fn eq(&self, other: &Value) -> bool {
        other == self
    }
}

impl Sub<f64> for &Value {
    type Output = f64;

    fn sub(self, rhs: f64) -> Self::Output {
        self.as_f64().unwrap_or(f64::NAN) - rhs
    }
}

impl Div<f64> for &Value {
    type Output = f64;

    fn div(self, rhs: f64) -> Self::Output {
        self.as_f64().unwrap_or(f64::NAN) / rhs
    }
}

impl Sum<Value> for f64 {
    fn sum<I: Iterator<Item = Value>>(iter: I) -> Self {
        iter.filter_map(|value| value.as_f64()).sum()
    }
}

impl<'a> Sum<&'a Value> for f64 {
    fn sum<I: Iterator<Item = &'a Value>>(iter: I) -> Self {
        iter.filter_map(|value| value.as_f64()).sum()
    }
}

/// Codec for encoding/decoding a user type to raw bytes.
pub trait Codec: Send + Sync {
    type Item: Clone + Send + Sync + 'static;

    fn encode(&self, value: &Self::Item) -> Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> Result<Self::Item>;
}

/// Aggregator for a decoded user type.
pub trait Aggregator<T>: Send + Sync {
    fn aggregate(&self, values: &[T]) -> Option<T>;
}

/// Object-safe server-side bytes aggregation adapter.
pub trait BytesAggregation: Send + Sync {
    fn aggregate_series(&self, points: &[DataPoint]) -> Result<Option<DataPoint>>;
    fn aggregate_bucket(
        &self,
        points: &[DataPoint],
        bucket_start: i64,
    ) -> Result<Option<DataPoint>>;
}

/// Adapter that bridges `Codec` + typed `Aggregator` to server-side bytes aggregation.
#[derive(Debug)]
pub struct CodecAggregator<C, A> {
    codec: C,
    aggregator: A,
}

impl<C, A> CodecAggregator<C, A> {
    pub fn new(codec: C, aggregator: A) -> Self {
        Self { codec, aggregator }
    }
}

impl<C, A> CodecAggregator<C, A>
where
    C: Codec,
    A: Aggregator<C::Item>,
{
    fn decode_values(&self, points: &[DataPoint]) -> Result<Vec<C::Item>> {
        let mut values = Vec::with_capacity(points.len());
        for point in points {
            match &point.value {
                Value::Bytes(bytes) => values.push(self.codec.decode(bytes)?),
                other => {
                    return Err(TsinkError::ValueTypeMismatch {
                        expected: "bytes".to_string(),
                        actual: other.kind().to_string(),
                    });
                }
            }
        }
        Ok(values)
    }
}

impl<C, A> BytesAggregation for CodecAggregator<C, A>
where
    C: Codec + 'static,
    A: Aggregator<C::Item> + 'static,
{
    fn aggregate_series(&self, points: &[DataPoint]) -> Result<Option<DataPoint>> {
        if points.is_empty() {
            return Ok(None);
        }

        let values = self.decode_values(points)?;
        let Some(output) = self.aggregator.aggregate(&values) else {
            return Ok(None);
        };
        let encoded = self.codec.encode(&output)?;

        Ok(Some(DataPoint::new(
            points.last().map(|p| p.timestamp).unwrap_or(0),
            Value::Bytes(encoded),
        )))
    }

    fn aggregate_bucket(
        &self,
        points: &[DataPoint],
        bucket_start: i64,
    ) -> Result<Option<DataPoint>> {
        if points.is_empty() {
            return Ok(None);
        }

        let values = self.decode_values(points)?;
        let Some(output) = self.aggregator.aggregate(&values) else {
            return Ok(None);
        };
        let encoded = self.codec.encode(&output)?;

        Ok(Some(DataPoint::new(bucket_start, Value::Bytes(encoded))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct U32Codec;

    impl Codec for U32Codec {
        type Item = u32;

        fn encode(&self, value: &Self::Item) -> Result<Vec<u8>> {
            Ok(value.to_le_bytes().to_vec())
        }

        fn decode(&self, bytes: &[u8]) -> Result<Self::Item> {
            if bytes.len() != 4 {
                return Err(TsinkError::Codec(format!(
                    "expected 4 bytes for u32, got {}",
                    bytes.len()
                )));
            }
            let mut arr = [0u8; 4];
            arr.copy_from_slice(bytes);
            Ok(u32::from_le_bytes(arr))
        }
    }

    struct SumU32;

    impl Aggregator<u32> for SumU32 {
        fn aggregate(&self, values: &[u32]) -> Option<u32> {
            Some(values.iter().sum())
        }
    }

    #[test]
    fn value_accessors_and_kind_cover_all_variants() {
        let histogram = NativeHistogram {
            count: Some(HistogramCount::Int(3)),
            sum: 1.5,
            schema: 1,
            zero_threshold: 0.0,
            zero_count: Some(HistogramCount::Float(f64::NAN)),
            negative_spans: vec![HistogramBucketSpan {
                offset: -1,
                length: 2,
            }],
            negative_deltas: vec![1, -2],
            negative_counts: vec![1.0, f64::NAN],
            positive_spans: vec![HistogramBucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_deltas: vec![3],
            positive_counts: vec![2.5],
            reset_hint: HistogramResetHint::No,
            custom_values: vec![0.25, f64::NAN],
        };
        let cases = [
            (Value::F64(1.5), "f64"),
            (Value::I64(-7), "i64"),
            (Value::U64(42), "u64"),
            (Value::Bool(true), "bool"),
            (Value::Bytes(vec![1, 2]), "bytes"),
            (Value::String("x".to_string()), "string"),
            (Value::from(histogram.clone()), "histogram"),
        ];

        for (value, expected_kind) in cases {
            assert_eq!(value.kind(), expected_kind);
        }

        assert_eq!(Value::F64(1.5).as_f64(), Some(1.5));
        assert_eq!(Value::I64(-7).as_i64(), Some(-7));
        assert_eq!(Value::U64(42).as_u64(), Some(42));
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Bytes(vec![1, 2]).as_bytes(), Some(&[1, 2][..]));
        assert_eq!(Value::String("x".to_string()).as_str(), Some("x"));
        assert_eq!(
            Value::from(histogram.clone()).as_histogram(),
            Some(&histogram)
        );
    }

    #[test]
    fn as_f64_rejects_non_representable_large_integers() {
        assert_eq!(Value::I64(i64::MAX).as_f64(), None);
        assert_eq!(Value::I64(i64::MAX - 1).as_f64(), None);
        assert_eq!(Value::U64(u64::MAX).as_f64(), None);

        assert_eq!(Value::I64(i64::MIN).as_f64(), Some(i64::MIN as f64));
        assert_eq!(Value::U64(1u64 << 63).as_f64(), Some((1u64 << 63) as f64));
    }

    #[test]
    fn value_equality_treats_nan_values_as_equal() {
        assert_eq!(Value::F64(f64::NAN), Value::F64(f64::NAN));
        assert_ne!(Value::F64(f64::NAN), Value::F64(1.0));

        let left = NativeHistogram {
            count: Some(HistogramCount::Float(f64::NAN)),
            sum: f64::NAN,
            schema: 4,
            zero_threshold: f64::NAN,
            zero_count: Some(HistogramCount::Float(f64::NAN)),
            negative_spans: vec![HistogramBucketSpan {
                offset: -2,
                length: 1,
            }],
            negative_deltas: vec![1],
            negative_counts: vec![f64::NAN],
            positive_spans: vec![HistogramBucketSpan {
                offset: 1,
                length: 2,
            }],
            positive_deltas: vec![2, 3],
            positive_counts: vec![f64::NAN, 4.0],
            reset_hint: HistogramResetHint::Gauge,
            custom_values: vec![f64::NAN],
        };
        let right = left.clone();

        assert_eq!(Value::from(left), Value::from(right));
    }

    #[test]
    fn encode_decode_with_codec_roundtrips() {
        let codec = U32Codec;
        let value = Value::encode_with(&1234u32, &codec).unwrap();
        let decoded = value.decode_with(&codec).unwrap();
        assert_eq!(decoded, 1234);
    }

    #[test]
    fn decode_with_rejects_non_bytes_values() {
        let codec = U32Codec;
        let err = Value::I64(10).decode_with(&codec).unwrap_err();
        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
    }

    #[test]
    fn sum_over_value_iter_uses_numeric_coercion() {
        let values = vec![Value::I64(1), Value::U64(2), Value::F64(3.5)];
        let sum: f64 = values.into_iter().sum();
        assert!((sum - 6.5).abs() < 1e-12);
    }

    #[test]
    fn sum_over_value_iter_ignores_non_numeric_values() {
        let values = vec![
            Value::I64(1),
            Value::String("x".to_string()),
            Value::Bool(true),
            Value::F64(2.5),
        ];
        let sum: f64 = values.into_iter().sum();
        assert!((sum - 3.5).abs() < 1e-12);
    }

    #[test]
    fn sum_over_value_ref_iter_ignores_non_numeric_values() {
        let values = [
            Value::U64(2),
            Value::Bytes(vec![1, 2, 3]),
            Value::String("y".to_string()),
            Value::F64(1.5),
        ];
        let sum: f64 = values.iter().sum();
        assert!((sum - 3.5).abs() < 1e-12);
    }

    #[test]
    fn codec_aggregator_aggregates_series_and_buckets() {
        let adapter = CodecAggregator::new(U32Codec, SumU32);
        let points = [
            DataPoint::new(100, Value::Bytes(10u32.to_le_bytes().to_vec())),
            DataPoint::new(110, Value::Bytes(20u32.to_le_bytes().to_vec())),
        ];

        let series = adapter.aggregate_series(&points).unwrap().unwrap();
        let bucket = adapter.aggregate_bucket(&points, 1000).unwrap().unwrap();

        let codec = U32Codec;
        assert_eq!(series.timestamp, 110);
        assert_eq!(series.value.decode_with(&codec).unwrap(), 30);
        assert_eq!(bucket.timestamp, 1000);
        assert_eq!(bucket.value.decode_with(&codec).unwrap(), 30);
    }

    #[test]
    fn codec_aggregator_rejects_non_bytes_payloads() {
        let adapter = CodecAggregator::new(U32Codec, SumU32);
        let err = adapter
            .aggregate_series(&[DataPoint::new(1, Value::I64(1))])
            .unwrap_err();
        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
    }
}
