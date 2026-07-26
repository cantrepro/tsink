use std::collections::BTreeSet;

use tsink::promql::types::PromqlValue;
use tsink::{DataPoint, ResourceProfile, Row, RowWriteStatus, TimestampPrecision, TsinkError};
use tsink_test::{TsinkTestDb, TsinkTestDbMode, MAX_DIAGNOSTIC_MESSAGE_BYTES};

#[test]
fn temporary_close_restart_preserves_data_and_close_is_idempotent() {
    let mut db = TsinkTestDb::builder()
        .temporary()
        .timestamp_precision(TimestampPrecision::Seconds)
        .start()
        .unwrap();
    assert_eq!(db.mode(), TsinkTestDbMode::Temporary);
    assert_eq!(db.resource_profile(), ResourceProfile::Test);
    let data_path = db.data_path().unwrap().to_path_buf();
    assert!(data_path.is_dir());

    let result = db
        .write_atomic(&[Row::new("restart_metric", DataPoint::new(10, 7.0))])
        .unwrap();
    assert_eq!(result.submitted, 1);
    assert_eq!(result.accepted, 1);
    assert_eq!(result.rejected, 0);
    assert!(matches!(
        result.outcomes[0].status,
        RowWriteStatus::Accepted
    ));

    db.restart().unwrap();
    assert_eq!(db.data_path(), Some(data_path.as_path()));
    let points = db.select("restart_metric", &[], 0, 11).unwrap();
    assert_eq!(points, vec![DataPoint::new(10, 7.0)]);

    db.close().unwrap();
    assert!(db.is_closed());
    assert!(!data_path.exists());
    db.close().unwrap();
}

#[test]
fn explicit_persistent_directory_reopens_across_fixture_values() {
    let parent = tempfile::tempdir().unwrap();
    let data_path = parent.path().join("persistent-db");
    {
        let mut db = TsinkTestDb::builder()
            .persistent_directory(&data_path)
            .timestamp_precision(TimestampPrecision::Seconds)
            .start()
            .unwrap();
        assert_eq!(db.mode(), TsinkTestDbMode::PersistentDirectory);
        db.write_atomic(&[Row::new("persistent_metric", DataPoint::new(20, 9.0))])
            .unwrap();
        db.close().unwrap();
        assert!(data_path.is_dir());
    }

    let mut reopened = TsinkTestDb::builder()
        .persistent_directory(&data_path)
        .timestamp_precision(TimestampPrecision::Seconds)
        .start()
        .unwrap();
    assert_eq!(
        reopened.select("persistent_metric", &[], 0, 21).unwrap(),
        vec![DataPoint::new(20, 9.0)]
    );
    reopened.close().unwrap();
    assert!(data_path.is_dir());
}

#[test]
fn parallel_temporary_fixtures_have_independent_roots_and_ids() {
    let fixtures = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                let mut db = TsinkTestDb::builder().temporary().start().unwrap();
                let id = db.diagnostic_id().to_string();
                let path = db.data_path().unwrap().to_path_buf();
                assert!(path.is_dir());
                db.close().unwrap();
                assert!(!path.exists());
                (id, path)
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        fixtures
            .iter()
            .map(|(id, _)| id)
            .collect::<BTreeSet<_>>()
            .len(),
        fixtures.len()
    );
    assert_eq!(
        fixtures
            .iter()
            .map(|(_, path)| path)
            .collect::<BTreeSet<_>>()
            .len(),
        fixtures.len()
    );
}

#[test]
fn direct_promql_helpers_use_explicit_instant_and_range_times() {
    let mut db = TsinkTestDb::builder()
        .in_memory()
        .timestamp_precision(TimestampPrecision::Seconds)
        .start()
        .unwrap();
    db.write_atomic(&[
        Row::new("promql_metric", DataPoint::new(10, 1.0)),
        Row::new("promql_metric", DataPoint::new(20, 2.0)),
    ])
    .unwrap();

    let instant = db.promql_instant("promql_metric", 20).unwrap();
    let samples = instant.as_instant_vector().unwrap();
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].metric, "promql_metric");
    assert_eq!(samples[0].timestamp, 20);
    assert_eq!(samples[0].value, 2.0);

    let range = db.promql_range("promql_metric", 10, 20, 10).unwrap();
    let PromqlValue::RangeVector(series) = range else {
        panic!("expected range vector");
    };
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].metric, "promql_metric");
    assert_eq!(series[0].samples, vec![(10, 1.0), (20, 2.0)]);

    let error = db.restart().unwrap_err();
    assert!(matches!(
        error,
        TsinkError::UnsupportedOperation {
            operation: "tsink_test_restart",
            ..
        }
    ));
    assert!(!db.is_closed());
    db.close().unwrap();
}

#[test]
fn diagnostics_are_bounded_and_keep_only_the_newest_operations() {
    let mut db = TsinkTestDb::builder()
        .in_memory()
        .diagnostic_capacity(2)
        .start()
        .unwrap();
    db.write_atomic(&[]).unwrap();
    let invalid_query = "(".repeat(4_096);
    assert!(db.promql_instant(&invalid_query, 1).is_err());

    let diagnostics = db.diagnostics();
    assert_eq!(diagnostics.len(), 2);
    assert_eq!(diagnostics[0].operation, "write_atomic");
    assert_eq!(diagnostics[1].operation, "promql_instant");
    assert!(diagnostics
        .iter()
        .all(|entry| entry.message.len() <= MAX_DIAGNOSTIC_MESSAGE_BYTES));
    assert!(db.diagnostic_dump().contains(db.diagnostic_id()));

    db.close().unwrap();
}

#[test]
fn invalid_diagnostic_capacity_fails_before_starting_storage() {
    let error = TsinkTestDb::builder()
        .diagnostic_capacity(0)
        .start()
        .unwrap_err();
    assert!(matches!(error, TsinkError::InvalidConfiguration(_)));
}
