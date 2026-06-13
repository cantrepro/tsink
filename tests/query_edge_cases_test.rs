use tempfile::TempDir;
use tsink::{DataPoint, Label, Row, StorageBuilder, TsinkError};

#[test]
fn test_query_empty_database() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Query on empty database
    let points = storage.select("nonexistent", &[], 1, 1000).unwrap();
    assert_eq!(points.len(), 0);
}

#[test]
fn test_query_with_extreme_timestamps() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Insert with extreme timestamps
    let rows = vec![
        Row::new("extreme", DataPoint::new(i64::MIN + 1, 1.0)),
        Row::new("extreme", DataPoint::new(1, 2.0)),
        Row::new("extreme", DataPoint::new(i64::MAX - 1, 3.0)),
    ];
    for row in rows {
        storage.insert_rows(&[row]).unwrap();
    }

    // Query with full range
    let points = storage.select("extreme", &[], i64::MIN, i64::MAX).unwrap();
    assert_eq!(points.len(), 3);

    // Query with extreme ranges
    let points = storage
        .select("extreme", &[], i64::MIN, i64::MIN + 2)
        .unwrap();
    assert_eq!(points.len(), 1);

    let points = storage
        .select("extreme", &[], i64::MAX - 2, i64::MAX)
        .unwrap();
    assert_eq!(points.len(), 1);
}

#[test]
fn test_query_boundary_conditions() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Insert boundary test data
    let rows = vec![
        Row::new("boundary", DataPoint::new(100, 1.0)),
        Row::new("boundary", DataPoint::new(200, 2.0)),
        Row::new("boundary", DataPoint::new(300, 3.0)),
    ];
    storage.insert_rows(&rows).unwrap();

    // Test exclusive end boundary
    let points = storage.select("boundary", &[], 100, 200).unwrap();
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].timestamp, 100);

    // Test inclusive start boundary
    let points = storage.select("boundary", &[], 200, 301).unwrap();
    assert_eq!(points.len(), 2);

    // Test exact match
    let points = storage.select("boundary", &[], 200, 201).unwrap();
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].timestamp, 200);

    // Test no overlap
    let points = storage.select("boundary", &[], 201, 299).unwrap();
    assert_eq!(points.len(), 0);
}

#[test]
fn test_query_with_nan_and_infinity() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Insert special float values
    let rows = vec![
        Row::new("special", DataPoint::new(100, f64::NAN)),
        Row::new("special", DataPoint::new(200, f64::INFINITY)),
        Row::new("special", DataPoint::new(300, f64::NEG_INFINITY)),
        Row::new("special", DataPoint::new(400, 0.0)),
        Row::new("special", DataPoint::new(500, -0.0)),
    ];
    storage.insert_rows(&rows).unwrap();

    let points = storage.select("special", &[], 1, 1000).unwrap();
    assert_eq!(points.len(), 5);

    // Check special values are preserved
    assert!(points[0].value.is_nan());
    assert!(points[1].value.is_infinite() && points[1].value > 0.0);
    assert!(points[2].value.is_infinite() && points[2].value < 0.0);
}

#[test]
fn test_query_with_duplicate_timestamps() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Insert duplicate timestamps
    let rows = vec![
        Row::new("duplicates", DataPoint::new(100, 1.0)),
        Row::new("duplicates", DataPoint::new(100, 2.0)),
        Row::new("duplicates", DataPoint::new(100, 3.0)),
        Row::new("duplicates", DataPoint::new(200, 4.0)),
    ];
    storage.insert_rows(&rows).unwrap();

    let points = storage.select("duplicates", &[], 100, 201).unwrap();
    assert_eq!(points.len(), 4);

    // All duplicate timestamps should be preserved
    let count_100 = points.iter().filter(|p| p.timestamp == 100).count();
    assert_eq!(count_100, 3);
}

#[test]
fn test_query_after_partition_rotation() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_partition_duration(std::time::Duration::from_millis(100)) // Small partition to force rotation
        .build()
        .unwrap();

    // Insert enough data to cause partition rotation
    for i in 0..50 {
        let rows = vec![Row::new(
            "rotation",
            DataPoint::new((i + 1) as i64 * 1000, i as f64),
        )];
        storage.insert_rows(&rows).unwrap();
    }

    // Query across partitions
    let points = storage.select("rotation", &[], 1, 51000).unwrap();
    assert_eq!(points.len(), 50);

    // Verify order is maintained
    for i in 0..49 {
        assert!(points[i].timestamp <= points[i + 1].timestamp);
    }
}

#[test]
fn test_query_with_complex_labels() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Test with special characters in labels
    let labels_sets = [
        vec![Label::new("key", "value with spaces")],
        vec![Label::new("key", "value/with/slashes")],
        vec![Label::new("key", "value=with=equals")],
        vec![Label::new("key", "value,with,commas")],
        vec![Label::new("key", "value\"with\"quotes")],
        vec![
            Label::new("key1", "value1"),
            Label::new("key2", "value2"),
            Label::new("key3", "value3"),
        ], // Multiple labels
    ];

    for (i, labels) in labels_sets.iter().enumerate() {
        let rows = vec![Row::with_labels(
            "labeled",
            labels.clone(),
            DataPoint::new((i + 1) as i64, i as f64),
        )];
        storage.insert_rows(&rows).unwrap();
    }

    // Query each label set
    for (i, labels) in labels_sets.iter().enumerate() {
        let points = storage.select("labeled", labels, 1, 100).unwrap();
        assert!(
            !points.is_empty(),
            "Failed to find data for label set {}",
            i
        );
    }
}

#[test]
fn test_query_rejects_invalid_labels() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let err = storage
        .insert_rows(&[Row::with_labels(
            "invalid_label_insert",
            vec![Label::new("", "value")],
            DataPoint::new(1, 1.0),
        )])
        .unwrap_err();
    assert!(matches!(err, TsinkError::InvalidLabel(_)));

    let err = storage
        .select(
            "invalid_label_select",
            &[Label::new("key", "")],
            1,
            i64::MAX,
        )
        .unwrap_err();
    assert!(matches!(err, TsinkError::InvalidLabel(_)));
}

#[test]
fn test_query_range_completely_outside_data() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Insert data in range 1000-2000
    for i in 10..20 {
        let rows = vec![Row::new(
            "range_test",
            DataPoint::new(i as i64 * 100, i as f64),
        )];
        storage.insert_rows(&rows).unwrap();
    }

    // Query before all data
    let points = storage.select("range_test", &[], 1, 900).unwrap();
    assert_eq!(points.len(), 0);

    // Query after all data
    let points = storage.select("range_test", &[], 2100, 3000).unwrap();
    assert_eq!(points.len(), 0);

    // Query with range containing all data
    let points = storage.select("range_test", &[], 1, 3000).unwrap();
    assert_eq!(points.len(), 10);
}
