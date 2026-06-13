//! Typed value model and extensibility traits.

use crate::{DataPoint, Result, TsinkError};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::iter::Sum;
use std::ops::{Div, Sub};

/// Typed payload value stored in a data point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    F64(f64),
    I64(i64),
    U64(u64),
    Bool(bool),
    Bytes(Vec<u8>),
    String(String),
}

impl Value {
    /// Returns the value kind name.
    pub fn kind(&self) -> &'static str {
        match self {
            Value::F64(_) => "f64",
            Value::I64(_) => "i64",
            Value::U64(_) => "u64",
            Value::Bool(_) => "bool",
            Value::Bytes(_) => "bytes",
            Value::String(_) => "string",
        }
    }

    /// Returns the value converted to f64 when numeric.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::F64(v) => Some(*v),
            Value::I64(v) => Some(*v as f64),
            Value::U64(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Returns the value as i64 when exactly representable.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::I64(v) => Some(*v),
            Value::U64(v) => i64::try_from(*v).ok(),
            _ => None,
        }
    }

    /// Returns the value as u64 when exactly representable.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::U64(v) => Some(*v),
            Value::I64(v) => u64::try_from(*v).ok(),
            _ => None,
        }
    }

    /// Returns the value as bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the value as a borrowed byte slice.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    /// Returns the value as a borrowed string slice.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v.as_str()),
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
        iter.fold(0.0, |acc, value| acc + value.as_f64().unwrap_or(f64::NAN))
    }
}

impl<'a> Sum<&'a Value> for f64 {
    fn sum<I: Iterator<Item = &'a Value>>(iter: I) -> Self {
        iter.fold(0.0, |acc, value| acc + value.as_f64().unwrap_or(f64::NAN))
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
        let cases = [
            (Value::F64(1.5), "f64"),
            (Value::I64(-7), "i64"),
            (Value::U64(42), "u64"),
            (Value::Bool(true), "bool"),
            (Value::Bytes(vec![1, 2]), "bytes"),
            (Value::String("x".to_string()), "string"),
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
    fn codec_aggregator_aggregates_series_and_buckets() {
        let adapter = CodecAggregator::new(U32Codec, SumU32);
        let points = vec![
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
