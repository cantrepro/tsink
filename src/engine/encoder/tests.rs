use super::{decode_values_f64_xor, decode_values_f64_xor_range, encode_values_f64_xor, Encoder};
use crate::engine::chunk::{ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane};
use crate::{
    DataPoint, HistogramBucketSpan, HistogramCount, HistogramResetHint, NativeHistogram,
    TsinkError, Value,
};

fn chunk_points(ts: &[i64], values: Vec<Value>) -> Vec<ChunkPoint> {
    ts.iter()
        .copied()
        .zip(values)
        .map(|(ts, value)| ChunkPoint { ts, value })
        .collect()
}

fn sample_histogram() -> NativeHistogram {
    NativeHistogram {
        count: Some(HistogramCount::Int(42)),
        sum: 17.5,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(HistogramCount::Int(7)),
        negative_spans: vec![HistogramBucketSpan {
            offset: -1,
            length: 1,
        }],
        negative_deltas: vec![2],
        negative_counts: vec![],
        positive_spans: vec![HistogramBucketSpan {
            offset: 0,
            length: 2,
        }],
        positive_deltas: vec![3, 1],
        positive_counts: vec![],
        reset_hint: HistogramResetHint::No,
        custom_values: vec![0.25, 0.5],
    }
}

#[test]
fn chooses_fixed_step_for_regular_timestamps() {
    let timestamps = (0..64).map(|idx| 1_000 + idx * 10).collect::<Vec<_>>();
    let values = (0..64)
        .map(|idx| Value::I64(idx as i64))
        .collect::<Vec<_>>();
    let points = chunk_points(&timestamps, values);

    let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();
    assert_eq!(encoded.ts_codec, TimestampCodecId::FixedStepRle);
}

#[test]
fn chooses_constant_codec_for_constant_values() {
    let points = chunk_points(
        &[1, 2, 3, 4],
        vec![Value::I64(7), Value::I64(7), Value::I64(7), Value::I64(7)],
    );

    let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();
    assert_eq!(encoded.value_codec, ValueCodecId::ConstantRle);
}

#[test]
fn roundtrip_f64_values() {
    let points = vec![
        DataPoint::new(1, 1.25),
        DataPoint::new(3, 1.75),
        DataPoint::new(5, 2.5),
        DataPoint::new(8, -0.25),
    ];

    let encoded = Encoder::encode(&points).unwrap();
    let decoded = Encoder::decode(&encoded).unwrap();
    assert_eq!(decoded, points);
}

#[test]
fn gorilla_f64_bitpacking_reduces_payload_for_smooth_series() {
    let mut bits = 1_000.0f64.to_bits();
    let values = (0..256)
        .map(|idx| {
            if idx > 0 {
                bits = bits.wrapping_add(1);
            }
            Value::F64(f64::from_bits(bits))
        })
        .collect::<Vec<_>>();
    let timestamps = (0..256).map(|idx| idx as i64).collect::<Vec<_>>();
    let points = chunk_points(&timestamps, values);

    let payload = encode_values_f64_xor(&points).unwrap();
    assert!(
        payload.len() < points.len().saturating_mul(8),
        "expected Gorilla bitpacking to beat raw 8-byte/value XOR, got {} bytes for {} values",
        payload.len(),
        points.len()
    );

    let decoded = decode_values_f64_xor(&payload, points.len()).unwrap();
    let expected = points
        .iter()
        .map(|point| point.value.clone())
        .collect::<Vec<_>>();
    assert_eq!(decoded, expected);
}

#[test]
fn gorilla_f64_range_decode_matches_full_decode_slice() {
    let values = (0..128)
        .map(|idx| Value::F64(1_000.0 + (idx as f64) * 0.25))
        .collect::<Vec<_>>();
    let timestamps = (0..128).map(|idx| idx as i64).collect::<Vec<_>>();
    let points = chunk_points(&timestamps, values);

    let payload = encode_values_f64_xor(&points).unwrap();
    let full = decode_values_f64_xor(&payload, points.len()).unwrap();
    let start = 11usize;
    let end = 97usize;
    let ranged = decode_values_f64_xor_range(&payload, points.len(), start, end).unwrap();

    assert_eq!(ranged, full[start..end].to_vec());
}

#[test]
fn roundtrip_i64_values() {
    let points = vec![
        DataPoint::new(10, Value::I64(-5)),
        DataPoint::new(11, Value::I64(-4)),
        DataPoint::new(17, Value::I64(2)),
        DataPoint::new(18, Value::I64(2)),
    ];

    let encoded = Encoder::encode(&points).unwrap();
    let decoded = Encoder::decode(&encoded).unwrap();
    assert_eq!(decoded, points);
}

#[test]
fn roundtrip_u64_values() {
    let points = vec![
        DataPoint::new(100, Value::U64(1000)),
        DataPoint::new(102, Value::U64(900)),
        DataPoint::new(103, Value::U64(1900)),
        DataPoint::new(108, Value::U64(1900)),
    ];

    let encoded = Encoder::encode(&points).unwrap();
    let decoded = Encoder::decode(&encoded).unwrap();
    assert_eq!(decoded, points);
}

#[test]
fn roundtrip_u64_values_with_large_deltas() {
    let points = vec![
        DataPoint::new(1, Value::U64(0)),
        DataPoint::new(2, Value::U64(u64::MAX)),
        DataPoint::new(3, Value::U64(0)),
        DataPoint::new(4, Value::U64(1u64 << 63)),
        DataPoint::new(5, Value::U64(0)),
    ];

    let encoded = Encoder::encode(&points).unwrap();
    assert_eq!(encoded.value_codec, ValueCodecId::DeltaBitpackU64);

    let decoded = Encoder::decode(&encoded).unwrap();
    assert_eq!(decoded, points);
}

#[test]
fn roundtrip_bool_values() {
    let points = vec![
        DataPoint::new(1, Value::Bool(true)),
        DataPoint::new(2, Value::Bool(false)),
        DataPoint::new(3, Value::Bool(true)),
        DataPoint::new(4, Value::Bool(false)),
        DataPoint::new(5, Value::Bool(true)),
    ];

    let encoded = Encoder::encode(&points).unwrap();
    let decoded = Encoder::decode(&encoded).unwrap();
    assert_eq!(decoded, points);
}

#[test]
fn roundtrip_blob_values() {
    let points = vec![
        DataPoint::new(1, Value::Bytes(b"abc".to_vec())),
        DataPoint::new(2, Value::String("xyz".to_string())),
        DataPoint::new(3, Value::Bytes(vec![0, 1, 2, 3])),
        DataPoint::new(4, Value::String("longer-payload".to_string())),
    ];

    let encoded = Encoder::encode(&points).unwrap();
    assert_eq!(encoded.value_codec, ValueCodecId::BytesDeltaBlock);

    let decoded = Encoder::decode(&encoded).unwrap();
    assert_eq!(decoded, points);
}

#[test]
fn roundtrip_histogram_values() {
    let histogram = sample_histogram();
    let points = vec![
        DataPoint::new(1, Value::from(histogram.clone())),
        DataPoint::new(2, Value::from(histogram.clone())),
    ];

    let encoded = Encoder::encode(&points).unwrap();
    assert_eq!(encoded.lane, ValueLane::Blob);

    let decoded = Encoder::decode(&encoded).unwrap();
    assert_eq!(decoded, points);
}

#[test]
fn rejects_timestamp_delta_overflow() {
    let points = vec![
        DataPoint::new(i64::MIN, Value::I64(1)),
        DataPoint::new(i64::MAX, Value::I64(2)),
    ];

    let err = Encoder::encode(&points).unwrap_err();
    assert!(matches!(err, TsinkError::Codec(_)));
}

#[test]
fn falls_back_to_delta_varint_when_delta_of_delta_overflows() {
    let points = vec![
        DataPoint::new(0, Value::I64(1)),
        DataPoint::new(i64::MAX, Value::I64(2)),
        DataPoint::new(-1, Value::I64(3)),
    ];

    let encoded = Encoder::encode(&points).unwrap();
    assert_eq!(encoded.ts_codec, TimestampCodecId::DeltaVarint);

    let decoded = Encoder::decode(&encoded).unwrap();
    assert_eq!(decoded, points);
}

#[test]
fn rejects_i64_delta_overflow() {
    let points = vec![
        DataPoint::new(1, Value::I64(i64::MIN)),
        DataPoint::new(2, Value::I64(i64::MAX)),
    ];

    let err = Encoder::encode(&points).unwrap_err();
    assert!(matches!(err, TsinkError::Codec(_)));
}
