use tempfile::TempDir;
use tsink::{DataPoint, Row, StorageBuilder};

#[test]
fn test_simple_insert() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    let rows = vec![Row::new("test_metric", DataPoint::new(1000, 1.0))];
    storage.insert_rows(&rows).unwrap();

    let points = storage.select("test_metric", &[], 500, 1500).unwrap();
    println!("Found {} points", points.len());
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].timestamp, 1000);
    assert_eq!(points[0].value_as_f64().unwrap_or(f64::NAN), 1.0);
}

#[test]
fn test_multiple_inserts() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .build()
        .unwrap();

    for i in 0..10 {
        let rows = vec![Row::new(
            "test_metric",
            DataPoint::new((i + 1) * 1000, i as f64),
        )];
        storage.insert_rows(&rows).unwrap();
    }

    match storage.select("test_metric", &[], 500, 11000) {
        Ok(points) => {
            println!("Found {} points", points.len());
            assert_eq!(points.len(), 10);
        }
        Err(e) => {
            panic!("Select failed: {:?}", e);
        }
    }
}
