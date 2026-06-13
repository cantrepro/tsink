use tempfile::TempDir;
use tsink::{
    Aggregation, Aggregator, Codec, DataPoint, QueryOptions, Row, StorageBuilder, TsinkError, Value,
};

#[test]
fn test_downsample_average() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![
        Row::new("ds", DataPoint::new(1_000, 1.0)),
        Row::new("ds", DataPoint::new(2_000, 2.0)),
        Row::new("ds", DataPoint::new(3_000, 3.0)),
        Row::new("ds", DataPoint::new(4_500, 1.0)),
    ];
    storage.insert_rows(&rows).unwrap();

    let opts = QueryOptions::new(1_000, 5_000).with_downsample(2_000, Aggregation::Avg);
    let points = storage.select_with_options("ds", opts).unwrap();

    assert_eq!(points.len(), 2);
    assert_eq!(points[0].timestamp, 1_000);
    assert!((points[0].value_as_f64().unwrap_or(f64::NAN) - 1.5).abs() < 1e-9);
    assert_eq!(points[1].timestamp, 3_000);
    assert!((points[1].value_as_f64().unwrap_or(f64::NAN) - 2.0).abs() < 1e-9);
}

#[test]
fn test_aggregation_sum_whole_series() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![
        Row::new("agg", DataPoint::new(100, 1.0)),
        Row::new("agg", DataPoint::new(200, 2.0)),
        Row::new("agg", DataPoint::new(300, 3.5)),
    ];
    storage.insert_rows(&rows).unwrap();

    let opts = QueryOptions::new(0, 1_000).with_aggregation(Aggregation::Sum);
    let points = storage.select_with_options("agg", opts).unwrap();

    assert_eq!(points.len(), 1);
    assert_eq!(points[0].timestamp, 300);
    assert!((points[0].value_as_f64().unwrap_or(f64::NAN) - 6.5).abs() < 1e-9);
}

#[test]
fn test_limit_and_offset() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    for i in 0..5 {
        let ts = (i + 1) as i64 * 1_000;
        storage
            .insert_rows(&[Row::new("page", DataPoint::new(ts, i as f64))])
            .unwrap();
    }

    let opts = QueryOptions::new(0, 10_000).with_pagination(2, Some(2));
    let points = storage.select_with_options("page", opts).unwrap();

    assert_eq!(points.len(), 2);
    assert_eq!(points[0].timestamp, 3_000);
    assert_eq!(points[1].timestamp, 4_000);
}

#[test]
fn test_downsample_fast_forward_aligns_to_query_start() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![
        Row::new("ds_align", DataPoint::new(1_100, 1.0)),
        Row::new("ds_align", DataPoint::new(5_100, 2.0)),
    ];
    storage.insert_rows(&rows).unwrap();

    // start is not interval-aligned; buckets should be [1000,3000), [3000,5000), [5000,7000)
    let opts = QueryOptions::new(1_000, 7_000).with_downsample(2_000, Aggregation::Avg);
    let points = storage.select_with_options("ds_align", opts).unwrap();

    assert_eq!(points.len(), 2);
    assert_eq!(points[0].timestamp, 1_000);
    assert_eq!(points[1].timestamp, 5_000);
    assert!((points[0].value_as_f64().unwrap_or(f64::NAN) - 1.0).abs() < 1e-9);
    assert!((points[1].value_as_f64().unwrap_or(f64::NAN) - 2.0).abs() < 1e-9);
}

#[test]
fn test_min_max_aggregation_ignores_nan_when_possible() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![
        Row::new("nan_agg", DataPoint::new(1_000, f64::NAN)),
        Row::new("nan_agg", DataPoint::new(2_000, 3.0)),
        Row::new("nan_agg", DataPoint::new(3_000, 1.5)),
    ];
    storage.insert_rows(&rows).unwrap();

    let min_points = storage
        .select_with_options(
            "nan_agg",
            QueryOptions::new(0, 4_000).with_aggregation(Aggregation::Min),
        )
        .unwrap();
    assert_eq!(min_points.len(), 1);
    assert_eq!(min_points[0].value_as_f64().unwrap_or(f64::NAN), 1.5);

    let max_points = storage
        .select_with_options(
            "nan_agg",
            QueryOptions::new(0, 4_000).with_aggregation(Aggregation::Max),
        )
        .unwrap();
    assert_eq!(max_points.len(), 1);
    assert_eq!(max_points[0].value_as_f64().unwrap_or(f64::NAN), 3.0);
}

#[test]
fn test_sum_avg_aggregation_ignores_nan_when_possible() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![
        Row::new("nan_sum_avg", DataPoint::new(1_000, f64::NAN)),
        Row::new("nan_sum_avg", DataPoint::new(2_000, 3.0)),
        Row::new("nan_sum_avg", DataPoint::new(3_000, 1.5)),
    ];
    storage.insert_rows(&rows).unwrap();

    let sum_points = storage
        .select_with_options(
            "nan_sum_avg",
            QueryOptions::new(0, 4_000).with_aggregation(Aggregation::Sum),
        )
        .unwrap();
    assert_eq!(sum_points.len(), 1);
    assert_eq!(sum_points[0].timestamp, 3_000);
    assert!((sum_points[0].value_as_f64().unwrap_or(f64::NAN) - 4.5).abs() < 1e-9);

    let avg_points = storage
        .select_with_options(
            "nan_sum_avg",
            QueryOptions::new(0, 4_000).with_aggregation(Aggregation::Avg),
        )
        .unwrap();
    assert_eq!(avg_points.len(), 1);
    assert_eq!(avg_points[0].timestamp, 3_000);
    assert!((avg_points[0].value_as_f64().unwrap_or(f64::NAN) - 2.25).abs() < 1e-9);
}

#[test]
fn test_downsample_sum_avg_ignores_nan_when_possible() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![
        Row::new("nan_downsample_sum_avg", DataPoint::new(1_000, 1.0)),
        Row::new("nan_downsample_sum_avg", DataPoint::new(1_500, f64::NAN)),
        Row::new("nan_downsample_sum_avg", DataPoint::new(1_900, 2.0)),
    ];
    storage.insert_rows(&rows).unwrap();

    let sum_points = storage
        .select_with_options(
            "nan_downsample_sum_avg",
            QueryOptions::new(1_000, 3_000).with_downsample(2_000, Aggregation::Sum),
        )
        .unwrap();
    assert_eq!(sum_points.len(), 1);
    assert_eq!(sum_points[0].timestamp, 1_000);
    assert!((sum_points[0].value_as_f64().unwrap_or(f64::NAN) - 3.0).abs() < 1e-9);

    let avg_points = storage
        .select_with_options(
            "nan_downsample_sum_avg",
            QueryOptions::new(1_000, 3_000).with_downsample(2_000, Aggregation::Avg),
        )
        .unwrap();
    assert_eq!(avg_points.len(), 1);
    assert_eq!(avg_points[0].timestamp, 1_000);
    assert!((avg_points[0].value_as_f64().unwrap_or(f64::NAN) - 1.5).abs() < 1e-9);
}

#[test]
fn test_aggregation_first_whole_series() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![
        Row::new("agg_first", DataPoint::new(100, 10.0)),
        Row::new("agg_first", DataPoint::new(200, 20.0)),
        Row::new("agg_first", DataPoint::new(300, 30.0)),
    ];
    storage.insert_rows(&rows).unwrap();

    let points = storage
        .select_with_options(
            "agg_first",
            QueryOptions::new(0, 1_000).with_aggregation(Aggregation::First),
        )
        .unwrap();

    assert_eq!(points.len(), 1);
    assert_eq!(points[0].timestamp, 100);
    assert_eq!(points[0].value_as_f64().unwrap_or(f64::NAN), 10.0);
}

#[test]
fn test_aggregation_count_median_whole_series_ignore_nan() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![
        Row::new("agg_count_median", DataPoint::new(1_000, 10.0)),
        Row::new("agg_count_median", DataPoint::new(2_000, f64::NAN)),
        Row::new("agg_count_median", DataPoint::new(3_000, 30.0)),
        Row::new("agg_count_median", DataPoint::new(4_000, 20.0)),
    ];
    storage.insert_rows(&rows).unwrap();

    let count_points = storage
        .select_with_options(
            "agg_count_median",
            QueryOptions::new(0, 5_000).with_aggregation(Aggregation::Count),
        )
        .unwrap();
    assert_eq!(count_points.len(), 1);
    assert_eq!(count_points[0].timestamp, 4_000);
    assert_eq!(count_points[0].value_as_f64().unwrap_or(f64::NAN), 3.0);

    let median_points = storage
        .select_with_options(
            "agg_count_median",
            QueryOptions::new(0, 5_000).with_aggregation(Aggregation::Median),
        )
        .unwrap();
    assert_eq!(median_points.len(), 1);
    assert_eq!(median_points[0].timestamp, 4_000);
    assert_eq!(median_points[0].value_as_f64().unwrap_or(f64::NAN), 20.0);
}

#[test]
fn test_downsample_first_last_count_median() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![
        Row::new("ds_new_aggs", DataPoint::new(1_000, 10.0)),
        Row::new("ds_new_aggs", DataPoint::new(1_400, 20.0)),
        Row::new("ds_new_aggs", DataPoint::new(1_800, 30.0)),
        Row::new("ds_new_aggs", DataPoint::new(3_100, 100.0)),
        Row::new("ds_new_aggs", DataPoint::new(3_200, f64::NAN)),
        Row::new("ds_new_aggs", DataPoint::new(3_300, 200.0)),
    ];
    storage.insert_rows(&rows).unwrap();

    let first_points = storage
        .select_with_options(
            "ds_new_aggs",
            QueryOptions::new(1_000, 5_000).with_downsample(2_000, Aggregation::First),
        )
        .unwrap();
    assert_eq!(first_points.len(), 2);
    assert_eq!(first_points[0], DataPoint::new(1_000, 10.0));
    assert_eq!(first_points[1], DataPoint::new(3_000, 100.0));

    let last_points = storage
        .select_with_options(
            "ds_new_aggs",
            QueryOptions::new(1_000, 5_000).with_downsample(2_000, Aggregation::Last),
        )
        .unwrap();
    assert_eq!(last_points.len(), 2);
    assert_eq!(last_points[0], DataPoint::new(1_000, 30.0));
    assert_eq!(last_points[1], DataPoint::new(3_000, 200.0));

    let count_points = storage
        .select_with_options(
            "ds_new_aggs",
            QueryOptions::new(1_000, 5_000).with_downsample(2_000, Aggregation::Count),
        )
        .unwrap();
    assert_eq!(count_points.len(), 2);
    assert_eq!(count_points[0], DataPoint::new(1_000, 3u64));
    assert_eq!(count_points[1], DataPoint::new(3_000, 2u64));

    let median_points = storage
        .select_with_options(
            "ds_new_aggs",
            QueryOptions::new(1_000, 5_000).with_downsample(2_000, Aggregation::Median),
        )
        .unwrap();
    assert_eq!(median_points.len(), 2);
    assert_eq!(median_points[0], DataPoint::new(1_000, 20.0));
    assert_eq!(median_points[1], DataPoint::new(3_000, 150.0));
}

#[test]
fn test_aggregation_range_variance_stddev_whole_series_ignore_nan() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![
        Row::new("agg_range_var_std", DataPoint::new(1_000, 1.0)),
        Row::new("agg_range_var_std", DataPoint::new(2_000, f64::NAN)),
        Row::new("agg_range_var_std", DataPoint::new(3_000, 2.0)),
        Row::new("agg_range_var_std", DataPoint::new(4_000, 3.0)),
    ];
    storage.insert_rows(&rows).unwrap();

    let range_points = storage
        .select_with_options(
            "agg_range_var_std",
            QueryOptions::new(0, 5_000).with_aggregation(Aggregation::Range),
        )
        .unwrap();
    assert_eq!(range_points.len(), 1);
    assert_eq!(range_points[0].timestamp, 4_000);
    assert!((range_points[0].value_as_f64().unwrap_or(f64::NAN) - 2.0).abs() < 1e-12);

    let variance_points = storage
        .select_with_options(
            "agg_range_var_std",
            QueryOptions::new(0, 5_000).with_aggregation(Aggregation::Variance),
        )
        .unwrap();
    assert_eq!(variance_points.len(), 1);
    assert_eq!(variance_points[0].timestamp, 4_000);
    assert!((variance_points[0].value_as_f64().unwrap_or(f64::NAN) - (2.0 / 3.0)).abs() < 1e-12);

    let stddev_points = storage
        .select_with_options(
            "agg_range_var_std",
            QueryOptions::new(0, 5_000).with_aggregation(Aggregation::StdDev),
        )
        .unwrap();
    assert_eq!(stddev_points.len(), 1);
    assert_eq!(stddev_points[0].timestamp, 4_000);
    assert!(
        (stddev_points[0].value_as_f64().unwrap_or(f64::NAN) - (2.0f64 / 3.0).sqrt()).abs() < 1e-12
    );
}

#[test]
fn test_downsample_range_variance_stddev() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![
        Row::new("ds_range_var_std", DataPoint::new(1_000, 1.0)),
        Row::new("ds_range_var_std", DataPoint::new(1_100, 2.0)),
        Row::new("ds_range_var_std", DataPoint::new(1_900, 3.0)),
        Row::new("ds_range_var_std", DataPoint::new(3_100, 10.0)),
        Row::new("ds_range_var_std", DataPoint::new(3_200, f64::NAN)),
        Row::new("ds_range_var_std", DataPoint::new(3_300, 14.0)),
    ];
    storage.insert_rows(&rows).unwrap();

    let range_points = storage
        .select_with_options(
            "ds_range_var_std",
            QueryOptions::new(1_000, 5_000).with_downsample(2_000, Aggregation::Range),
        )
        .unwrap();
    assert_eq!(range_points.len(), 2);
    assert_eq!(range_points[0], DataPoint::new(1_000, 2.0));
    assert_eq!(range_points[1], DataPoint::new(3_000, 4.0));

    let variance_points = storage
        .select_with_options(
            "ds_range_var_std",
            QueryOptions::new(1_000, 5_000).with_downsample(2_000, Aggregation::Variance),
        )
        .unwrap();
    assert_eq!(variance_points.len(), 2);
    assert_eq!(variance_points[0].timestamp, 1_000);
    assert_eq!(variance_points[1].timestamp, 3_000);
    assert!((variance_points[0].value_as_f64().unwrap_or(f64::NAN) - (2.0 / 3.0)).abs() < 1e-12);
    assert!((variance_points[1].value_as_f64().unwrap_or(f64::NAN) - 4.0).abs() < 1e-12);

    let stddev_points = storage
        .select_with_options(
            "ds_range_var_std",
            QueryOptions::new(1_000, 5_000).with_downsample(2_000, Aggregation::StdDev),
        )
        .unwrap();
    assert_eq!(stddev_points.len(), 2);
    assert_eq!(stddev_points[0].timestamp, 1_000);
    assert_eq!(stddev_points[1].timestamp, 3_000);
    assert!(
        (stddev_points[0].value_as_f64().unwrap_or(f64::NAN) - (2.0f64 / 3.0).sqrt()).abs() < 1e-12
    );
    assert!((stddev_points[1].value_as_f64().unwrap_or(f64::NAN) - 2.0).abs() < 1e-12);
}

#[test]
fn test_custom_bytes_aggregation_with_codec_and_aggregator() {
    struct I32Codec;
    impl Codec for I32Codec {
        type Item = i32;

        fn encode(&self, value: &Self::Item) -> Result<Vec<u8>, TsinkError> {
            Ok(value.to_le_bytes().to_vec())
        }

        fn decode(&self, bytes: &[u8]) -> Result<Self::Item, TsinkError> {
            if bytes.len() != 4 {
                return Err(TsinkError::Codec(format!(
                    "expected 4 bytes for i32, got {}",
                    bytes.len()
                )));
            }
            let mut arr = [0u8; 4];
            arr.copy_from_slice(bytes);
            Ok(i32::from_le_bytes(arr))
        }
    }

    struct SumI32;
    impl Aggregator<i32> for SumI32 {
        fn aggregate(&self, values: &[i32]) -> Option<i32> {
            Some(values.iter().sum())
        }
    }

    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let codec = I32Codec;
    let rows = vec![
        Row::new(
            "custom_bytes",
            DataPoint::new(1_000, Value::encode_with(&10, &codec).unwrap()),
        ),
        Row::new(
            "custom_bytes",
            DataPoint::new(2_000, Value::encode_with(&20, &codec).unwrap()),
        ),
    ];
    storage.insert_rows(&rows).unwrap();

    let points = storage
        .select_with_options(
            "custom_bytes",
            QueryOptions::new(0, 3_000).with_custom_bytes_aggregation(I32Codec, SumI32),
        )
        .unwrap();

    assert_eq!(points.len(), 1);
    let decoded = points[0].value.decode_with(&codec).unwrap();
    assert_eq!(decoded, 30);
}
