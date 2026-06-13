use super::*;

#[test]
fn select_uses_exact_label_match_with_postings_candidates() {
    let storage = ChunkStorage::new(2, None);
    let short = vec![Label::new("host", "a")];
    let long = vec![Label::new("host", "a"), Label::new("region", "us")];

    storage
        .insert_rows(&[
            Row::with_labels("cpu", short.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("cpu", long.clone(), DataPoint::new(1, 10.0)),
        ])
        .unwrap();

    let points_short = storage.select("cpu", &short, 0, 10).unwrap();
    let points_long = storage.select("cpu", &long, 0, 10).unwrap();

    assert_eq!(points_short, vec![DataPoint::new(1, 1.0)]);
    assert_eq!(points_long, vec![DataPoint::new(1, 10.0)]);
}

#[test]
fn select_series_supports_matcher_intersections() {
    let storage = ChunkStorage::new(2, None);
    storage
        .insert_rows(&[
            Row::with_labels(
                "cpu",
                vec![Label::new("host", "a"), Label::new("region", "use1")],
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "cpu",
                vec![Label::new("host", "b"), Label::new("region", "usw2")],
                DataPoint::new(1, 2.0),
            ),
            Row::with_labels(
                "memory",
                vec![Label::new("host", "a"), Label::new("region", "use1")],
                DataPoint::new(1, 3.0),
            ),
        ])
        .unwrap();

    let selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("host", "a"))
                .with_matcher(SeriesMatcher::regex_match("region", "use.*")),
        )
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].name, "cpu");
    assert_eq!(
        selected[0].labels,
        vec![Label::new("host", "a"), Label::new("region", "use1")]
    );

    let not_equal = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::not_equal("host", "a")),
        )
        .unwrap();
    assert_eq!(not_equal.len(), 1);
    assert_eq!(
        not_equal[0].labels,
        vec![Label::new("host", "b"), Label::new("region", "usw2")]
    );
}

#[test]
fn select_series_time_range_uses_chunk_indexes() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(1)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "cpu",
                vec![Label::new("host", "old")],
                DataPoint::new(10, 1.0),
            ),
            Row::with_labels(
                "cpu",
                vec![Label::new("host", "new")],
                DataPoint::new(200, 2.0),
            ),
        ])
        .unwrap();
    storage.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(1)
        .build()
        .unwrap();

    let selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(100, 300),
        )
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].name, "cpu");
    assert_eq!(selected[0].labels, vec![Label::new("host", "new")]);

    reopened.close().unwrap();
}

#[test]
fn select_series_rejects_invalid_regex() {
    let storage = ChunkStorage::new(2, None);
    let err = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_match("host", "(")),
        )
        .unwrap_err();
    assert!(matches!(err, TsinkError::InvalidConfiguration(_)));
}
