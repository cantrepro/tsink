use tempfile::TempDir;
use tsink::{DataPoint, Label, Row, StorageBuilder};

#[test]
fn test_select_all_with_multiple_labels() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Insert data points with different label combinations
    // Same metric "cpu_usage" but with different labels
    let rows1 = vec![
        Row::with_labels(
            "cpu_usage",
            vec![
                Label::new("host", "server1"),
                Label::new("region", "us-west"),
            ],
            DataPoint::new(1000, 10.0),
        ),
        Row::with_labels(
            "cpu_usage",
            vec![
                Label::new("host", "server1"),
                Label::new("region", "us-west"),
            ],
            DataPoint::new(2000, 20.0),
        ),
    ];
    storage.insert_rows(&rows1).unwrap();

    let rows2 = vec![
        Row::with_labels(
            "cpu_usage",
            vec![
                Label::new("host", "server2"),
                Label::new("region", "us-east"),
            ],
            DataPoint::new(1500, 15.0),
        ),
        Row::with_labels(
            "cpu_usage",
            vec![
                Label::new("host", "server2"),
                Label::new("region", "us-east"),
            ],
            DataPoint::new(2500, 25.0),
        ),
    ];
    storage.insert_rows(&rows2).unwrap();

    let rows3 = vec![Row::with_labels(
        "cpu_usage",
        vec![Label::new("host", "server3")], // Only one label
        DataPoint::new(1200, 12.0),
    )];
    storage.insert_rows(&rows3).unwrap();

    // Query all cpu_usage metrics regardless of labels
    let all_results = storage.select_all("cpu_usage", 0, 3000).unwrap();

    // We should have 3 different label combinations
    assert_eq!(
        all_results.len(),
        3,
        "Should have 3 different label combinations"
    );

    // Count total points
    let total_points: usize = all_results.iter().map(|(_, points)| points.len()).sum();
    assert_eq!(total_points, 5, "Should have 5 total data points");

    // Verify we can find each label combination
    let mut found_server1 = false;
    let mut found_server2 = false;
    let mut found_server3 = false;

    for (labels, points) in &all_results {
        // Check for server1 with region
        if labels
            .iter()
            .any(|l| l.name == "host" && l.value == "server1")
        {
            found_server1 = true;
            assert_eq!(points.len(), 2);
            assert!(
                labels
                    .iter()
                    .any(|l| l.name == "region" && l.value == "us-west")
            );
        }
        // Check for server2 with region
        else if labels
            .iter()
            .any(|l| l.name == "host" && l.value == "server2")
        {
            found_server2 = true;
            assert_eq!(points.len(), 2);
            assert!(
                labels
                    .iter()
                    .any(|l| l.name == "region" && l.value == "us-east")
            );
        }
        // Check for server3 (only host label)
        else if labels
            .iter()
            .any(|l| l.name == "host" && l.value == "server3")
        {
            found_server3 = true;
            assert_eq!(points.len(), 1);
            assert_eq!(labels.len(), 1);
        }
    }

    assert!(found_server1, "Should find server1 data");
    assert!(found_server2, "Should find server2 data");
    assert!(found_server3, "Should find server3 data");
}

#[test]
fn test_select_all_no_labels() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Insert data points without labels
    let rows = vec![
        Row::new("temperature", DataPoint::new(1000, 20.0)),
        Row::new("temperature", DataPoint::new(2000, 22.0)),
        Row::new("temperature", DataPoint::new(3000, 21.0)),
    ];
    storage.insert_rows(&rows).unwrap();

    // Query all temperature metrics
    let all_results = storage.select_all("temperature", 0, 4000).unwrap();

    assert_eq!(
        all_results.len(),
        1,
        "Should have 1 label combination (no labels)"
    );

    let (labels, points) = &all_results[0];
    assert!(labels.is_empty(), "Should have no labels");
    assert_eq!(points.len(), 3, "Should have 3 data points");
}

#[test]
fn test_select_all_mixed_labels_and_no_labels() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Insert some points with labels and some without
    let rows1 = vec![
        Row::new("requests", DataPoint::new(1000, 100.0)),
        Row::new("requests", DataPoint::new(2000, 110.0)),
    ];
    storage.insert_rows(&rows1).unwrap();

    let rows2 = vec![Row::with_labels(
        "requests",
        vec![Label::new("endpoint", "/api/v1")],
        DataPoint::new(1500, 150.0),
    )];
    storage.insert_rows(&rows2).unwrap();

    // Query all
    let all_results = storage.select_all("requests", 0, 3000).unwrap();

    assert_eq!(
        all_results.len(),
        2,
        "Should have 2 different label combinations"
    );

    // Verify we have both labeled and unlabeled data
    let mut found_unlabeled = false;
    let mut found_labeled = false;

    for (labels, points) in &all_results {
        if labels.is_empty() {
            found_unlabeled = true;
            assert_eq!(points.len(), 2);
        } else {
            found_labeled = true;
            assert_eq!(points.len(), 1);
            assert_eq!(labels[0].name, "endpoint");
            assert_eq!(labels[0].value, "/api/v1");
        }
    }

    assert!(found_unlabeled, "Should find unlabeled data");
    assert!(found_labeled, "Should find labeled data");
}

#[test]
fn test_select_all_nonexistent_metric() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Insert some data
    let rows = vec![Row::new("existing_metric", DataPoint::new(1000, 1.0))];
    storage.insert_rows(&rows).unwrap();

    // Query a non-existent metric
    let all_results = storage.select_all("nonexistent", 0, 2000).unwrap();

    assert!(
        all_results.is_empty(),
        "Should return empty results for non-existent metric"
    );
}

#[test]
fn test_select_all_time_range_filtering() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    // Insert data points at different times
    let rows = vec![
        Row::with_labels(
            "metric",
            vec![Label::new("type", "A")],
            DataPoint::new(1000, 10.0),
        ),
        Row::with_labels(
            "metric",
            vec![Label::new("type", "A")],
            DataPoint::new(2000, 20.0),
        ),
        Row::with_labels(
            "metric",
            vec![Label::new("type", "A")],
            DataPoint::new(3000, 30.0),
        ),
        Row::with_labels(
            "metric",
            vec![Label::new("type", "B")],
            DataPoint::new(1500, 15.0),
        ),
        Row::with_labels(
            "metric",
            vec![Label::new("type", "B")],
            DataPoint::new(2500, 25.0),
        ),
    ];
    storage.insert_rows(&rows).unwrap();

    // Query with a time range that excludes some points
    let all_results = storage.select_all("metric", 1200, 2700).unwrap();

    assert_eq!(all_results.len(), 2, "Should have both label sets");

    for (labels, points) in &all_results {
        if labels.iter().any(|l| l.value == "A") {
            // Type A should have point at 2000 only
            assert_eq!(points.len(), 1);
            assert_eq!(points[0].timestamp, 2000);
        } else if labels.iter().any(|l| l.value == "B") {
            // Type B should have points at 1500 and 2500
            assert_eq!(points.len(), 2);
            assert_eq!(points[0].timestamp, 1500);
            assert_eq!(points[1].timestamp, 2500);
        }
    }
}
