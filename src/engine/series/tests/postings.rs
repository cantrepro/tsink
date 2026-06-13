use std::sync::atomic::Ordering;
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
fn missing_label_cache_invalidates_after_new_series_changes() {
    let registry = SeriesRegistry::new();
    registry
        .resolve_or_insert("cpu", &[Label::new("host", "a"), Label::new("job", "api")])
        .unwrap();
    let missing_before = registry
        .resolve_or_insert("cpu", &[Label::new("host", "b")])
        .unwrap();
    let job_name_id = registry.label_name_id("job").unwrap();

    let cached_missing = registry.missing_label_postings_for_id(job_name_id);
    assert_eq!(cached_missing.len(), 1);
    assert!(cached_missing.contains(missing_before.series_id));
    let initial_generation = registry.postings_generation.load(Ordering::Acquire);

    let missing_after = registry
        .resolve_or_insert("cpu", &[Label::new("host", "c")])
        .unwrap();
    let with_job = registry
        .resolve_or_insert(
            "cpu",
            &[Label::new("host", "d"), Label::new("job", "worker")],
        )
        .unwrap();

    let shard_idx = SeriesRegistry::label_postings_shard_idx(job_name_id);
    let cached_after_writes = registry.label_postings_shards[shard_idx]
        .read()
        .label_name_states
        .get(&job_name_id)
        .and_then(|state| state.missing_cache.clone())
        .unwrap();
    assert_eq!(
        cached_after_writes.bitmap.iter().collect::<Vec<_>>(),
        vec![missing_before.series_id],
        "new series invalidates the cache instead of mutating unrelated postings shards",
    );
    assert_eq!(cached_after_writes.postings_generation, initial_generation);
    assert!(registry.postings_generation.load(Ordering::Acquire) > initial_generation);

    let refreshed_missing = registry.missing_label_postings_for_id(job_name_id);
    assert_eq!(refreshed_missing.len(), 2);
    assert!(refreshed_missing.contains(missing_before.series_id));
    assert!(refreshed_missing.contains(missing_after.series_id));
    assert!(!refreshed_missing.contains(with_job.series_id));
    let refreshed_cache = registry.label_postings_shards[shard_idx]
        .read()
        .label_name_states
        .get(&job_name_id)
        .and_then(|state| state.missing_cache.clone())
        .unwrap();
    assert_eq!(
        refreshed_cache.postings_generation,
        registry.postings_generation.load(Ordering::Acquire),
    );
    assert_memory_usage_reconciled(&registry);
}

#[test]
fn rollback_invalidates_missing_label_cache_for_untouched_shards() {
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
    let missing_with_rolled_back = registry.missing_label_postings_for_id(job_name_id);
    assert!(missing_with_rolled_back.contains(rolled_back.series_id));
    let shard_idx = SeriesRegistry::label_postings_shard_idx(job_name_id);
    let cached_before_rollback = registry.label_postings_shards[shard_idx]
        .read()
        .label_name_states
        .get(&job_name_id)
        .and_then(|state| state.missing_cache.clone())
        .unwrap();
    assert_eq!(
        cached_before_rollback.bitmap.iter().collect::<Vec<_>>(),
        vec![missing_before.series_id, rolled_back.series_id],
        "refreshing the cache should surface new series before rollback",
    );
    let rollback_generation = registry.postings_generation.load(Ordering::Acquire);

    registry.rollback_created_series(std::slice::from_ref(&rolled_back));

    let cached_after_rollback = registry.label_postings_shards[shard_idx]
        .read()
        .label_name_states
        .get(&job_name_id)
        .and_then(|state| state.missing_cache.clone())
        .unwrap();
    assert_eq!(
        cached_after_rollback.bitmap.iter().collect::<Vec<_>>(),
        vec![missing_before.series_id, rolled_back.series_id],
        "rollback should invalidate the cache rather than mutating an unrelated shard in place",
    );
    assert_eq!(
        cached_after_rollback.postings_generation,
        rollback_generation
    );
    assert!(registry.postings_generation.load(Ordering::Acquire) > rollback_generation);

    let refreshed_missing = registry.missing_label_postings_for_id(job_name_id);
    assert_eq!(refreshed_missing.len(), 1);
    assert!(refreshed_missing.contains(missing_before.series_id));
    assert!(!refreshed_missing.contains(rolled_back.series_id));
    assert_memory_usage_reconciled(&registry);
}

#[test]
fn active_missing_label_caches_do_not_block_unrelated_new_series_registration() {
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
