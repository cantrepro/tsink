use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use super::super::SeriesRegistry;
use super::assert_memory_usage_reconciled;
use crate::Label;

#[test]
fn postings_track_all_series_for_same_label_pair() {
    let registry = SeriesRegistry::new();

    let a = registry
        .resolve_or_insert(
            "cpu",
            &[Label::new("host", "h1"), Label::new("region", "use1")],
        )
        .unwrap();
    let b = registry
        .resolve_or_insert(
            "cpu",
            &[Label::new("host", "h2"), Label::new("region", "use1")],
        )
        .unwrap();

    let postings = registry.postings_for_label("region", "use1").unwrap();
    assert_eq!(postings.len(), 2);
    assert!(postings.contains(a.series_id));
    assert!(postings.contains(b.series_id));
}

#[test]
fn missing_label_postings_are_unretained_and_refresh_after_new_series_changes() {
    let registry = SeriesRegistry::new();
    registry
        .resolve_or_insert("cpu", &[Label::new("host", "a"), Label::new("job", "api")])
        .unwrap();
    let missing_before = registry
        .resolve_or_insert("cpu", &[Label::new("host", "b")])
        .unwrap();
    let job_name_id = registry.label_name_id("job").unwrap();

    let memory_before_query = registry.memory_usage_bytes();
    let initial_missing = registry.missing_label_postings_for_id(job_name_id);
    assert_eq!(initial_missing.len(), 1);
    assert!(initial_missing.contains(missing_before.series_id));
    assert_eq!(registry.memory_usage_bytes(), memory_before_query);

    let missing_after = registry
        .resolve_or_insert("cpu", &[Label::new("host", "c")])
        .unwrap();
    let with_job = registry
        .resolve_or_insert(
            "cpu",
            &[Label::new("host", "d"), Label::new("job", "worker")],
        )
        .unwrap();

    let memory_before_refresh = registry.memory_usage_bytes();
    let refreshed_missing = registry.missing_label_postings_for_id(job_name_id);
    assert_eq!(refreshed_missing.len(), 2);
    assert!(refreshed_missing.contains(missing_before.series_id));
    assert!(refreshed_missing.contains(missing_after.series_id));
    assert!(!refreshed_missing.contains(with_job.series_id));
    assert_eq!(registry.memory_usage_bytes(), memory_before_refresh);
    assert_memory_usage_reconciled(&registry);
}

#[test]
fn missing_label_postings_refresh_after_rollback_without_retained_state() {
    let registry = SeriesRegistry::new();
    registry
        .resolve_or_insert("cpu", &[Label::new("host", "a"), Label::new("job", "api")])
        .unwrap();
    let missing_before = registry
        .resolve_or_insert("cpu", &[Label::new("host", "b")])
        .unwrap();
    let job_name_id = registry.label_name_id("job").unwrap();
    assert!(registry
        .missing_label_postings_for_id(job_name_id)
        .contains(missing_before.series_id));

    let rolled_back = registry
        .resolve_or_insert("cpu", &[Label::new("host", "c")])
        .unwrap();
    let memory_before_query = registry.memory_usage_bytes();
    let missing_with_rolled_back = registry.missing_label_postings_for_id(job_name_id);
    assert!(missing_with_rolled_back.contains(rolled_back.series_id));
    assert_eq!(
        missing_with_rolled_back.iter().collect::<Vec<_>>(),
        vec![missing_before.series_id, rolled_back.series_id],
        "the uncached result should surface new series before rollback",
    );
    assert_eq!(registry.memory_usage_bytes(), memory_before_query);

    registry.rollback_created_series(std::slice::from_ref(&rolled_back));
    let memory_before_refresh = registry.memory_usage_bytes();
    let refreshed_missing = registry.missing_label_postings_for_id(job_name_id);
    assert_eq!(refreshed_missing.len(), 1);
    assert!(refreshed_missing.contains(missing_before.series_id));
    assert!(!refreshed_missing.contains(rolled_back.series_id));
    assert_eq!(registry.memory_usage_bytes(), memory_before_refresh);
    assert_memory_usage_reconciled(&registry);
}

#[test]
fn missing_label_queries_do_not_block_unrelated_new_series_registration() {
    let registry = Arc::new(SeriesRegistry::new());
    registry
        .resolve_or_insert(
            "metric_a",
            &[Label::new("host", "a"), Label::new("job", "api")],
        )
        .unwrap();
    registry
        .resolve_or_insert("metric_a", &[Label::new("host", "b")])
        .unwrap();
    let job_name_id = registry.label_name_id("job").unwrap();
    let missing_job = registry.missing_label_postings_for_id(job_name_id);
    assert_eq!(missing_job.len(), 1);

    let job_shard = SeriesRegistry::label_postings_shard_idx(job_name_id);
    let _job_guard = registry.label_postings_shards[job_shard].read();

    let writer_registry = Arc::clone(&registry);
    let (tx, rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let result = writer_registry.resolve_or_insert(
            "metric_b",
            &[Label::new("rack", "r1"), Label::new("zone", "use1")],
        );
        tx.send(result).unwrap();
    });

    let result = rx
        .recv_timeout(Duration::from_millis(500))
        .expect("unrelated postings shards should not serialize new-series inserts");
    assert!(result.unwrap().created);
    writer.join().unwrap();
}

#[test]
fn metric_postings_preflight_stabilizes_ids_during_concurrent_registration() {
    let registry = Arc::new(SeriesRegistry::new());
    let first = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let (preflight_entered_tx, preflight_entered_rx) = mpsc::channel();
    let (release_preflight_tx, release_preflight_rx) = mpsc::channel();
    let query_registry = Arc::clone(&registry);
    let query = thread::spawn(move || {
        query_registry.series_ids_for_metric_with_preflight("cpu", |count| {
            preflight_entered_tx.send(count).unwrap();
            release_preflight_rx.recv().unwrap();
            Ok::<(), ()>(())
        })
    });
    assert_eq!(preflight_entered_rx.recv().unwrap(), 1);

    let (writer_started_tx, writer_started_rx) = mpsc::channel();
    let (writer_finished_tx, writer_finished_rx) = mpsc::channel();
    let writer_registry = Arc::clone(&registry);
    let writer = thread::spawn(move || {
        writer_started_tx.send(()).unwrap();
        let result = writer_registry.resolve_or_insert("cpu", &[Label::new("host", "b")]);
        writer_finished_tx.send(result).unwrap();
    });
    writer_started_rx.recv().unwrap();
    assert!(
        writer_finished_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "registration for the same metric must wait for snapshot materialization",
    );

    release_preflight_tx.send(()).unwrap();
    let admitted_ids = query.join().unwrap().unwrap();
    assert_eq!(admitted_ids, vec![first]);
    assert!(
        writer_finished_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap()
            .unwrap()
            .created
    );
    writer.join().unwrap();
    assert_eq!(registry.series_count_for_metric("cpu"), 2);
}

#[test]
fn metric_postings_pages_seek_after_the_exclusive_cursor() {
    let registry = SeriesRegistry::new();
    let mut expected = Vec::new();
    for host in 0..5 {
        expected.push(
            registry
                .resolve_or_insert("cpu", &[Label::new("host", host.to_string())])
                .unwrap()
                .series_id,
        );
    }
    registry
        .resolve_or_insert("disk", &[Label::new("host", "unrelated")])
        .unwrap();

    let (first, more) = registry.series_ids_for_metric_after("cpu", None, 2);
    assert_eq!(first, expected[..2]);
    assert!(more);

    let (second, more) = registry.series_ids_for_metric_after("cpu", first.last().copied(), 2);
    assert_eq!(second, expected[2..4]);
    assert!(more);

    let (last, more) = registry.series_ids_for_metric_after("cpu", second.last().copied(), 2);
    assert_eq!(last, expected[4..]);
    assert!(!more);

    let (finished, more) = registry.series_ids_for_metric_after("cpu", Some(u64::MAX), 2);
    assert!(finished.is_empty());
    assert!(!more);

    let exact = (0..4)
        .map(|host| {
            registry
                .resolve_or_insert("memory", &[Label::new("host", host.to_string())])
                .unwrap()
                .series_id
        })
        .collect::<Vec<_>>();
    let (first, more) = registry.series_ids_for_metric_after("memory", None, 2);
    assert_eq!(first, exact[..2]);
    assert!(more);
    let (second, more) = registry.series_ids_for_metric_after("memory", first.last().copied(), 2);
    assert_eq!(second, exact[2..]);
    assert!(
        more,
        "a full page must not peek past the configured posting-inspection limit"
    );
    let (terminal, more) =
        registry.series_ids_for_metric_after("memory", second.last().copied(), 2);
    assert!(terminal.is_empty());
    assert!(!more);
}
