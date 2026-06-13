use tempfile::TempDir;
use tsink::{
    Aggregation, Aggregator, Codec, DataPoint, HistogramBucketSpan, HistogramCount,
    HistogramResetHint, NativeHistogram, QueryOptions, Row, StorageBuilder, TsinkError, Value,
};

#[derive(Clone)]
struct U32Codec;

impl Codec for U32Codec {
    type Item = u32;

    fn encode(&self, value: &Self::Item) -> Result<Vec<u8>, TsinkError> {
        Ok(value.to_le_bytes().to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> Result<Self::Item, TsinkError> {
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

fn sample_histogram() -> NativeHistogram {
    NativeHistogram {
        count: Some(HistogramCount::Int(42)),
        sum: 17.5,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(HistogramCount::Int(7)),
        negative_spans: vec![HistogramBucketSpan {
            offset: -2,
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
fn rejects_mixed_numeric_types_within_series() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let err = storage
        .insert_rows(&[
            Row::new("mixed_metric", DataPoint::new(1, 1.0f64)),
            Row::new("mixed_metric", DataPoint::new(2, 2i64)),
        ])
        .unwrap_err();
    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
}

#[test]
fn typed_values_roundtrip_after_persistence() {
    let temp_dir = TempDir::new().unwrap();
    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::new("f64", DataPoint::new(1, 1.25f64)),
                Row::new("i64", DataPoint::new(1, -10i64)),
                Row::new("u64", DataPoint::new(1, 10u64)),
                Row::new("bool", DataPoint::new(1, true)),
                Row::new("bytes", DataPoint::new(1, Value::Bytes(vec![7, 8]))),
                Row::new("string", DataPoint::new(1, "hello")),
                Row::new(
                    "histogram",
                    DataPoint::new(1, Value::from(sample_histogram())),
                ),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    assert_eq!(
        storage.select("f64", &[], 0, 10).unwrap()[0].value,
        Value::F64(1.25)
    );
    assert_eq!(
        storage.select("i64", &[], 0, 10).unwrap()[0].value,
        Value::I64(-10)
    );
    assert_eq!(
        storage.select("u64", &[], 0, 10).unwrap()[0].value,
        Value::U64(10)
    );
    assert_eq!(
        storage.select("bool", &[], 0, 10).unwrap()[0].value,
        Value::Bool(true)
    );
    assert_eq!(
        storage.select("bytes", &[], 0, 10).unwrap()[0].value,
        Value::Bytes(vec![7, 8])
    );
    assert_eq!(
        storage.select("string", &[], 0, 10).unwrap()[0].value,
        Value::String("hello".to_string())
    );
    assert_eq!(
        storage.select("histogram", &[], 0, 10).unwrap()[0].value,
        Value::from(sample_histogram())
    );
}

#[test]
fn blob_family_survives_restart_but_rejects_histograms() {
    let temp_dir = TempDir::new().unwrap();
    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .build()
            .unwrap();
        storage
            .insert_rows(&[Row::new(
                "blob_family_restart",
                DataPoint::new(1, Value::Bytes(vec![7, 8])),
            )])
            .unwrap();
        storage.close().unwrap();
    }

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();
    storage
        .insert_rows(&[Row::new("blob_family_restart", DataPoint::new(2, "hello"))])
        .unwrap();

    let err = storage
        .insert_rows(&[Row::new(
            "blob_family_restart",
            DataPoint::new(3, Value::from(sample_histogram())),
        )])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::ValueTypeMismatch { expected, actual }
            if expected == "bytes/string" && actual == "histogram"
    ));

    let points = storage.select("blob_family_restart", &[], 0, 10).unwrap();
    assert_eq!(points.len(), 2);
    assert_eq!(points[0].value, Value::Bytes(vec![7, 8]));
    assert_eq!(points[1].value, Value::String("hello".to_string()));

    storage.close().unwrap();
}

#[test]
fn typed_aggregations_cover_integer_bool_and_string_rules() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::new("ints", DataPoint::new(1, -1i64)),
            Row::new("ints", DataPoint::new(2, 3i64)),
            Row::new("unsigned", DataPoint::new(1, 2u64)),
            Row::new("unsigned", DataPoint::new(2, 4u64)),
            Row::new("bools", DataPoint::new(1, true)),
            Row::new("bools", DataPoint::new(2, false)),
            Row::new("strings", DataPoint::new(1, "beta")),
            Row::new("strings", DataPoint::new(2, "alpha")),
        ])
        .unwrap();

    let sum_i64 = storage
        .select_with_options(
            "ints",
            QueryOptions::new(0, 10).with_aggregation(Aggregation::Sum),
        )
        .unwrap();
    assert_eq!(sum_i64[0].value, Value::I64(2));

    let avg_i64 = storage
        .select_with_options(
            "ints",
            QueryOptions::new(0, 10).with_aggregation(Aggregation::Avg),
        )
        .unwrap();
    assert_eq!(avg_i64[0].value, Value::F64(1.0));

    let sum_u64 = storage
        .select_with_options(
            "unsigned",
            QueryOptions::new(0, 10).with_aggregation(Aggregation::Sum),
        )
        .unwrap();
    assert_eq!(sum_u64[0].value, Value::U64(6));

    let min_bool = storage
        .select_with_options(
            "bools",
            QueryOptions::new(0, 10).with_aggregation(Aggregation::Min),
        )
        .unwrap();
    let max_bool = storage
        .select_with_options(
            "bools",
            QueryOptions::new(0, 10).with_aggregation(Aggregation::Max),
        )
        .unwrap();
    assert_eq!(min_bool[0].value, Value::Bool(false));
    assert_eq!(max_bool[0].value, Value::Bool(true));

    let min_str = storage
        .select_with_options(
            "strings",
            QueryOptions::new(0, 10).with_aggregation(Aggregation::Min),
        )
        .unwrap();
    let max_str = storage
        .select_with_options(
            "strings",
            QueryOptions::new(0, 10).with_aggregation(Aggregation::Max),
        )
        .unwrap();
    assert_eq!(min_str[0].value, Value::String("alpha".to_string()));
    assert_eq!(max_str[0].value, Value::String("beta".to_string()));
}

#[test]
fn unsupported_and_mixed_type_aggregations_return_errors() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::new("str_metric", DataPoint::new(1, "a")),
            Row::new("str_metric", DataPoint::new(2, "b")),
            Row::new("mixed_metric", DataPoint::new(1, 1i64)),
        ])
        .unwrap();

    let unsupported = storage
        .select_with_options(
            "str_metric",
            QueryOptions::new(0, 10).with_aggregation(Aggregation::Sum),
        )
        .unwrap_err();
    assert!(matches!(
        unsupported,
        TsinkError::UnsupportedAggregation { .. }
    ));

    let mismatch = storage
        .insert_rows(&[Row::new("mixed_metric", DataPoint::new(2, "x"))])
        .unwrap_err();
    assert!(matches!(mismatch, TsinkError::ValueTypeMismatch { .. }));

    storage
        .insert_rows(&[Row::new(
            "hist_metric",
            DataPoint::new(1, Value::from(sample_histogram())),
        )])
        .unwrap();

    let histogram_sum = storage
        .select_with_options(
            "hist_metric",
            QueryOptions::new(0, 10).with_aggregation(Aggregation::Sum),
        )
        .unwrap_err();
    assert!(matches!(
        histogram_sum,
        TsinkError::UnsupportedAggregation { .. }
    ));
}

#[test]
fn custom_bytes_aggregation_supports_downsampling() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();
    let codec = U32Codec;

    storage
        .insert_rows(&[
            Row::new(
                "bytes_ds",
                DataPoint::new(1_000, Value::encode_with(&10u32, &codec).unwrap()),
            ),
            Row::new(
                "bytes_ds",
                DataPoint::new(1_500, Value::encode_with(&20u32, &codec).unwrap()),
            ),
            Row::new(
                "bytes_ds",
                DataPoint::new(3_000, Value::encode_with(&7u32, &codec).unwrap()),
            ),
        ])
        .unwrap();

    let points = storage
        .select_with_options(
            "bytes_ds",
            QueryOptions::new(1_000, 5_000)
                .with_downsample(2_000, Aggregation::Last)
                .with_custom_bytes_aggregation(U32Codec, SumU32),
        )
        .unwrap();

    assert_eq!(points.len(), 2);
    assert_eq!(points[0].timestamp, 1_000);
    assert_eq!(points[1].timestamp, 3_000);
    assert_eq!(points[0].value.decode_with(&codec).unwrap(), 30);
    assert_eq!(points[1].value.decode_with(&codec).unwrap(), 7);
}

#[test]
fn custom_bytes_aggregation_rejects_non_bytes_streams() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();
    storage
        .insert_rows(&[Row::new("not_bytes", DataPoint::new(1, 1.0f64))])
        .unwrap();

    let err = storage
        .select_with_options(
            "not_bytes",
            QueryOptions::new(0, 10).with_custom_bytes_aggregation(U32Codec, SumU32),
        )
        .unwrap_err();
    assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
}
