use tempfile::TempDir;
use tsink::{Aggregation, DataPoint, QueryOptions, Row, StorageBuilder};

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
    assert!((points[0].value - 1.5).abs() < 1e-9);
    assert_eq!(points[1].timestamp, 3_000);
    assert!((points[1].value - 2.0).abs() < 1e-9);
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
    assert!((points[0].value - 6.5).abs() < 1e-9);
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
