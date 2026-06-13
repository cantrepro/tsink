use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use super::*;
use crate::label::stable_series_identity_hash;
use crate::storage::{
    metric_series_matches_shard_scope, MetadataShardScope, QueryOptions, QueryRowsScanOptions,
    ShardWindowScanOptions,
};

fn labels_for_shard(metric: &str, shard_count: u32, target_shard: u32, prefix: &str) -> Vec<Label> {
    (0..4096u32)
        .map(|idx| vec![Label::new("host", format!("{prefix}-{idx}"))])
        .find(|labels| {
            (stable_series_identity_hash(metric, labels) % u64::from(shard_count)) as u32
                == target_shard
        })
        .unwrap()
}

fn labels_not_in_shard(
    metric: &str,
    shard_count: u32,
    excluded_shard: u32,
    prefix: &str,
) -> Vec<Label> {
    (0..4096u32)
        .map(|idx| vec![Label::new("host", format!("{prefix}-{idx}"))])
        .find(|labels| {
            (stable_series_identity_hash(metric, labels) % u64::from(shard_count)) as u32
                != excluded_shard
        })
        .unwrap()
}

fn labels_for_shard_with_job(
    metric: &str,
    shard_count: u32,
    target_shard: u32,
    prefix: &str,
    job: Option<&str>,
) -> Vec<Label> {
    (0..4096u32)
        .map(|idx| {
            let mut labels = vec![Label::new("host", format!("{prefix}-{idx}"))];
            if let Some(job) = job {
                labels.push(Label::new("job", job));
            }
            labels
        })
        .find(|labels| {
            (stable_series_identity_hash(metric, labels) % u64::from(shard_count)) as u32
                == target_shard
        })
        .unwrap()
}

fn labels_not_in_shard_with_job(
    metric: &str,
    shard_count: u32,
    excluded_shard: u32,
    prefix: &str,
    job: Option<&str>,
) -> Vec<Label> {
    (0..4096u32)
        .map(|idx| {
            let mut labels = vec![Label::new("host", format!("{prefix}-{idx}"))];
            if let Some(job) = job {
                labels.push(Label::new("job", job));
            }
            labels
        })
        .find(|labels| {
            (stable_series_identity_hash(metric, labels) % u64::from(shard_count)) as u32
                != excluded_shard
        })
        .unwrap()
}

fn runtime_metadata_delta_series_ids(storage: &ChunkStorage) -> Vec<u64> {
    storage
        .persisted
        .persisted_index
        .read()
        .runtime_metadata_delta_series_ids
        .iter()
        .collect()
}

fn persistent_numeric_storage_with_metadata_shards(
    root: &Path,
    timestamp_precision: TimestampPrecision,
    chunk_point_cap: usize,
    metadata_shard_count: u32,
) -> ChunkStorage {
    super::persistent_numeric_storage_with_metadata_shards(
        root,
        timestamp_precision,
        chunk_point_cap,
        metadata_shard_count,
    )
}

fn live_numeric_storage(
    root: &Path,
    timestamp_precision: TimestampPrecision,
    chunk_point_cap: usize,
    current_time_override: i64,
    retention: Duration,
    metadata_shard_count: Option<u32>,
) -> ChunkStorage {
    super::live_numeric_storage_with_config(
        root,
        timestamp_precision,
        chunk_point_cap,
        current_time_override,
        retention,
        metadata_shard_count,
    )
}

fn summary_needs_exact_scan(
    summary: &super::super::state::SeriesVisibilitySummary,
    start: i64,
    end: i64,
) -> bool {
    if summary
        .latest_visible_timestamp
        .is_none_or(|latest| latest < start)
    {
        return false;
    }

    let mut overlaps_coarse = false;
    for range in &summary.ranges {
        if range.max_ts < start || range.min_ts >= end {
            continue;
        }
        if range.exact {
            return false;
        }
        overlaps_coarse = true;
    }

    if overlaps_coarse {
        return true;
    }
    if !summary.truncated_before_floor {
        return false;
    }
    summary
        .exhaustive_floor_inclusive
        .is_none_or(|floor| start < floor)
}

fn legacy_postings_contributors_memory_usage_bytes<K>(
    contributors: &BTreeMap<K, Vec<usize>>,
    key_bytes: impl Fn(&K) -> usize,
) -> usize {
    let mut bytes = 0usize;
    for (key, slots) in contributors {
        bytes = bytes
            .saturating_add(std::mem::size_of::<(K, Vec<usize>)>())
            .saturating_add(key_bytes(key))
            .saturating_add(
                slots
                    .capacity()
                    .saturating_mul(std::mem::size_of::<usize>()),
            );
    }
    bytes
}

fn legacy_removed_persisted_metadata_bytes(
    loaded: &crate::engine::segment::LoadedSegmentIndexes,
) -> usize {
    let mut bytes = loaded.indexed_segments.iter().fold(0usize, |acc, segment| {
        acc.saturating_add(segment.postings.memory_usage_bytes())
    });

    let mut segment_roots_by_slot = HashMap::new();
    let mut merged_metric_segments = BTreeMap::<String, Vec<usize>>::new();
    let mut merged_label_name_segments = BTreeMap::<String, Vec<usize>>::new();
    let mut merged_label_segments = BTreeMap::<(String, String), Vec<usize>>::new();
    for (slot, segment) in loaded.indexed_segments.iter().enumerate() {
        segment_roots_by_slot.insert(slot, segment.root.clone());
        for metric in segment.postings.metric_postings.keys() {
            merged_metric_segments
                .entry(metric.clone())
                .or_default()
                .push(slot);
        }
        for label_name in segment.postings.label_name_postings.keys() {
            merged_label_name_segments
                .entry(label_name.clone())
                .or_default()
                .push(slot);
        }
        for label in segment.postings.label_postings.keys() {
            merged_label_segments
                .entry(label.clone())
                .or_default()
                .push(slot);
        }
    }

    bytes = bytes.saturating_add(ChunkStorage::hash_map_memory_usage_bytes::<
        usize,
        std::path::PathBuf,
    >(&segment_roots_by_slot));
    for root in segment_roots_by_slot.values() {
        bytes = bytes.saturating_add(root.as_os_str().len());
    }

    bytes = bytes.saturating_add(legacy_postings_contributors_memory_usage_bytes(
        &merged_metric_segments,
        |metric| metric.capacity(),
    ));
    bytes = bytes.saturating_add(legacy_postings_contributors_memory_usage_bytes(
        &merged_label_name_segments,
        |label_name| label_name.capacity(),
    ));
    bytes = bytes.saturating_add(legacy_postings_contributors_memory_usage_bytes(
        &merged_label_segments,
        |(label_name, label_value)| label_name.capacity().saturating_add(label_value.capacity()),
    ));
    bytes
}

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
fn select_series_racing_revival_write_does_not_prune_materialized_series() {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(live_numeric_storage(
        temp_dir.path(),
        TimestampPrecision::Seconds,
        8,
        20,
        Duration::from_secs(5),
        None,
    ));
    let metric = "select_series_revival_race";
    let labels = vec![Label::new("host", "a")];
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(20, 1.0),
        )])
        .unwrap();
    storage.set_current_time_override(30);

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(metric, &labels)
        .unwrap()
        .series_id;
    assert_eq!(storage.materialized_series_snapshot(), vec![series_id]);

    let entered_hook = Arc::new(Barrier::new(2));
    let release_hook = Arc::new(Barrier::new(2));
    storage.set_metadata_live_series_pre_prune_hook({
        let entered_hook = Arc::clone(&entered_hook);
        let release_hook = Arc::clone(&release_hook);
        move || {
            entered_hook.wait();
            release_hook.wait();
        }
    });

    let reader_storage = Arc::clone(&storage);
    let reader_metric = metric.to_string();
    let reader = thread::spawn(move || {
        reader_storage.select_series(
            &SeriesSelection::new()
                .with_metric(&reader_metric)
                .with_matcher(SeriesMatcher::equal("host", "a")),
        )
    });

    entered_hook.wait();
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(30, 2.0),
        )])
        .unwrap();
    release_hook.wait();

    let reader_result = reader.join().unwrap();
    assert!(reader_result.is_ok());
    storage.clear_metadata_live_series_pre_prune_hook();

    assert_eq!(storage.materialized_series_snapshot(), vec![series_id]);
    assert_eq!(
        storage
            .select_series(
                &SeriesSelection::new()
                    .with_metric(metric)
                    .with_matcher(SeriesMatcher::equal("host", "a")),
            )
            .unwrap(),
        vec![MetricSeries {
            name: metric.to_string(),
            labels,
        }],
    );
}

#[test]
fn select_series_treats_missing_labels_as_empty_strings() {
    let storage = ChunkStorage::new(2, None);
    let missing_zone = vec![Label::new("host", "a")];
    let with_zone = vec![Label::new("host", "b"), Label::new("zone", "use1")];

    storage
        .insert_rows(&[
            Row::with_labels("cpu", missing_zone.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("cpu", with_zone.clone(), DataPoint::new(1, 2.0)),
        ])
        .unwrap();

    let equal_empty = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("zone", "")),
        )
        .unwrap();
    assert_eq!(equal_empty.len(), 1);
    assert_eq!(equal_empty[0].labels, missing_zone);

    let not_equal_empty = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::not_equal("zone", "")),
        )
        .unwrap();
    assert_eq!(not_equal_empty.len(), 1);
    assert_eq!(not_equal_empty[0].labels, with_zone);
}

#[test]
fn select_series_handles_regex_matchers_that_match_or_reject_empty_labels() {
    let storage = ChunkStorage::new(2, None);
    let missing_zone = vec![Label::new("host", "a")];
    let with_zone = vec![Label::new("host", "b"), Label::new("zone", "use1")];

    storage
        .insert_rows(&[
            Row::with_labels("cpu", missing_zone.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("cpu", with_zone.clone(), DataPoint::new(1, 2.0)),
        ])
        .unwrap();

    let regex_matches_empty = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_match("zone", ".*")),
        )
        .unwrap();
    assert_eq!(regex_matches_empty.len(), 2);

    let regex_rejects_empty = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_no_match("zone", ".*")),
        )
        .unwrap();
    assert!(regex_rejects_empty.is_empty());

    let regex_requires_present_value = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_match("zone", ".+")),
        )
        .unwrap();
    assert_eq!(regex_requires_present_value.len(), 1);
    assert_eq!(regex_requires_present_value[0].labels, with_zone);

    let regex_keeps_missing = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_no_match("zone", ".+")),
        )
        .unwrap();
    assert_eq!(regex_keeps_missing.len(), 1);
    assert_eq!(regex_keeps_missing[0].labels, missing_zone);
}

#[test]
fn select_series_exact_seeded_regex_filter_avoids_live_series_snapshot() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let storage = ChunkStorage::new(2, None);
    let target_labels = vec![
        Label::new("instance", "instance-0128"),
        Label::new("job", "job-08"),
        Label::new("tenant", "tenant-00"),
    ];

    let rows = (0..256usize)
        .map(|idx| {
            let labels = if idx == 128 {
                target_labels.clone()
            } else {
                vec![
                    Label::new("instance", format!("instance-{idx:04}")),
                    Label::new("job", format!("job-{:02}", idx % 32)),
                    Label::new("tenant", format!("tenant-{:02}", idx % 8)),
                ]
            };
            Row::with_labels("cpu", labels, DataPoint::new(idx as i64, idx as f64))
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let snapshots = Arc::new(AtomicUsize::new(0));
    let all_series_postings = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_live_series_snapshot_hook({
        let snapshots = Arc::clone(&snapshots);
        move || {
            snapshots.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_all_series_postings_hook({
        let all_series_postings = Arc::clone(&all_series_postings);
        move || {
            all_series_postings.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_matcher(SeriesMatcher::regex_match("job", "job-(08|09)"))
                .with_matcher(SeriesMatcher::equal("instance", "instance-0128")),
        )
        .unwrap();

    assert_eq!(
        selected,
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels: target_labels,
        }],
    );
    assert_eq!(
        snapshots.load(Ordering::SeqCst),
        0,
        "exact postings seeds should avoid snapshotting all live series for regex refinements",
    );

    storage.clear_metadata_live_series_snapshot_hook();
}

#[test]
fn select_series_exact_seeded_negative_filter_avoids_live_series_snapshot() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let storage = ChunkStorage::new(2, None);
    let target_labels = vec![
        Label::new("instance", "instance-0064"),
        Label::new("job", "job-07"),
        Label::new("tenant", "tenant-00"),
    ];

    let rows = (0..192usize)
        .map(|idx| {
            let labels = if idx == 64 {
                target_labels.clone()
            } else {
                vec![
                    Label::new("instance", format!("instance-{idx:04}")),
                    Label::new("job", format!("job-{:02}", idx % 24)),
                    Label::new("tenant", format!("tenant-{:02}", idx % 6)),
                ]
            };
            Row::with_labels("cpu", labels, DataPoint::new(idx as i64, idx as f64))
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let snapshots = Arc::new(AtomicUsize::new(0));
    let all_series_postings = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_live_series_snapshot_hook({
        let snapshots = Arc::clone(&snapshots);
        move || {
            snapshots.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_all_series_postings_hook({
        let all_series_postings = Arc::clone(&all_series_postings);
        move || {
            all_series_postings.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_matcher(SeriesMatcher::equal("instance", "instance-0064"))
                .with_matcher(SeriesMatcher::not_equal("job", "job-99")),
        )
        .unwrap();

    assert_eq!(
        selected,
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels: target_labels,
        }],
    );
    assert_eq!(
        snapshots.load(Ordering::SeqCst),
        0,
        "exact postings seeds should avoid snapshotting all live series for negative refinements",
    );

    storage.clear_metadata_live_series_snapshot_hook();
}

#[test]
fn select_series_broad_regex_filter_avoids_live_series_snapshot() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let storage = ChunkStorage::new(2, None);
    let rows = (0..128usize)
        .map(|idx| {
            Row::with_labels(
                "cpu",
                vec![
                    Label::new("instance", format!("instance-{idx:04}")),
                    Label::new("job", format!("job-{:02}", idx % 32)),
                    Label::new("tenant", format!("tenant-{:02}", idx % 4)),
                ],
                DataPoint::new(1_000 + idx as i64, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let snapshots = Arc::new(AtomicUsize::new(0));
    let all_series_postings = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_live_series_snapshot_hook({
        let snapshots = Arc::clone(&snapshots);
        move || {
            snapshots.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_all_series_postings_hook({
        let all_series_postings = Arc::clone(&all_series_postings);
        move || {
            all_series_postings.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = storage
        .select_series(
            &SeriesSelection::new().with_matcher(SeriesMatcher::regex_match("job", "job-(08|09)")),
        )
        .unwrap();

    assert_eq!(selected.len(), 8);
    assert!(selected.iter().all(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "job")
            .is_some_and(|label| label.value == "job-08" || label.value == "job-09")
    }));
    assert_eq!(
        snapshots.load(Ordering::SeqCst),
        0,
        "broad regex selectors should seed from indexed label presence instead of snapshotting all live series",
    );
    assert_eq!(
        all_series_postings.load(Ordering::SeqCst),
        0,
        "broad regex selectors should not materialize the global all-series bitmap",
    );

    storage.clear_metadata_live_series_snapshot_hook();
    storage.clear_metadata_all_series_postings_hook();
}

#[test]
fn select_series_broad_negative_regex_filter_avoids_live_series_snapshot() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let storage = ChunkStorage::new(2, None);
    let rows = (0..64usize)
        .map(|idx| {
            let mut labels = vec![Label::new("instance", format!("instance-{idx:04}"))];
            if idx % 2 == 0 {
                labels.push(Label::new("job", format!("job-{:02}", idx % 8)));
            }
            Row::with_labels(
                "cpu",
                labels,
                DataPoint::new(1_000 + idx as i64, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let snapshots = Arc::new(AtomicUsize::new(0));
    let all_series_seeds = Arc::new(AtomicUsize::new(0));
    let all_series_postings = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_live_series_snapshot_hook({
        let snapshots = Arc::clone(&snapshots);
        move || {
            snapshots.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_all_series_seed_hook({
        let all_series_seeds = Arc::clone(&all_series_seeds);
        move || {
            all_series_seeds.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_all_series_postings_hook({
        let all_series_postings = Arc::clone(&all_series_postings);
        move || {
            all_series_postings.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_no_match("job", "job-(03|04)")),
        )
        .unwrap();

    assert_eq!(selected.len(), 56);
    assert!(selected.iter().all(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "job")
            .is_none_or(|label| label.value != "job-03" && label.value != "job-04")
    }));
    assert_eq!(
        snapshots.load(Ordering::SeqCst),
        0,
        "broad negative selectors should use indexed candidates instead of snapshotting all live series",
    );
    assert_eq!(
        all_series_seeds.load(Ordering::SeqCst),
        0,
        "broad negative selectors should not fall back to an all-series seed",
    );
    assert_eq!(
        all_series_postings.load(Ordering::SeqCst),
        0,
        "broad negative selectors should stay on indexed rejection paths instead of materializing all-series postings",
    );

    storage.clear_metadata_live_series_snapshot_hook();
    storage.clear_metadata_all_series_seed_hook();
    storage.clear_metadata_all_series_postings_hook();
}

#[test]
fn select_series_without_metric_or_matchers_reports_all_series_seed() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let storage = ChunkStorage::new(2, None);
    storage
        .insert_rows(&[
            Row::with_labels(
                "cpu",
                vec![Label::new("instance", "instance-0000")],
                DataPoint::new(1_000, 1.0),
            ),
            Row::with_labels(
                "mem",
                vec![Label::new("instance", "instance-0001")],
                DataPoint::new(1_001, 2.0),
            ),
            Row::with_labels(
                "disk",
                vec![Label::new("instance", "instance-0002")],
                DataPoint::new(1_002, 3.0),
            ),
        ])
        .unwrap();

    let all_series_seeds = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_all_series_seed_hook({
        let all_series_seeds = Arc::clone(&all_series_seeds);
        move || {
            all_series_seeds.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = storage.select_series(&SeriesSelection::new()).unwrap();

    assert_eq!(selected.len(), 3);
    assert_eq!(
        all_series_seeds.load(Ordering::SeqCst),
        1,
        "unseeded selectors should report the all-series metadata fallback",
    );

    storage.clear_metadata_all_series_seed_hook();
}

#[test]
fn list_metrics_high_cardinality_avoids_live_series_snapshot() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let storage = ChunkStorage::new(2, None);
    let series_count = 8_192usize;
    let rows = (0..series_count)
        .map(|idx| {
            let mut labels = vec![
                Label::new("instance", format!("instance-{idx:06}")),
                Label::new("tenant", format!("tenant-{:02}", idx % 8)),
            ];
            if idx % 3 != 0 {
                labels.push(Label::new("job", format!("job-{:02}", idx % 32)));
            }
            Row::with_labels(
                "cpu",
                labels,
                DataPoint::new(1_000 + idx as i64, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let snapshots = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_live_series_snapshot_hook({
        let snapshots = Arc::clone(&snapshots);
        move || {
            snapshots.fetch_add(1, Ordering::SeqCst);
        }
    });

    let listed = storage.list_metrics().unwrap();

    assert_eq!(listed.len(), series_count);
    assert_eq!(
        snapshots.load(Ordering::SeqCst),
        0,
        "list_metrics should page through materialized metadata instead of snapshotting all live series",
    );

    storage.clear_metadata_live_series_snapshot_hook();
}

#[test]
fn select_series_broad_empty_matching_regex_avoids_all_series_seed() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let storage = ChunkStorage::new(2, None);
    let rows = (0..96usize)
        .map(|idx| {
            let mut labels = vec![Label::new("instance", format!("instance-{idx:04}"))];
            if idx % 3 != 0 {
                labels.push(Label::new("job", format!("job-{:02}", idx % 12)));
            }
            Row::with_labels(
                "cpu",
                labels,
                DataPoint::new(1_000 + idx as i64, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let all_series_seeds = Arc::new(AtomicUsize::new(0));
    let all_series_postings = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_all_series_seed_hook({
        let all_series_seeds = Arc::clone(&all_series_seeds);
        move || {
            all_series_seeds.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_all_series_postings_hook({
        let all_series_postings = Arc::clone(&all_series_postings);
        move || {
            all_series_postings.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_match("job", ".*")),
        )
        .unwrap();

    assert_eq!(selected.len(), rows.len());
    assert_eq!(
        all_series_seeds.load(Ordering::SeqCst),
        0,
        "empty-matching broad regex selectors should seed from indexed postings instead of all-series fallback",
    );
    assert_eq!(
        all_series_postings.load(Ordering::SeqCst),
        0,
        "empty-matching broad regex selectors should not materialize the global all-series bitmap",
    );

    storage.clear_metadata_all_series_seed_hook();
    storage.clear_metadata_all_series_postings_hook();
}

#[test]
fn select_series_metric_negative_regex_stays_on_indexed_metadata_path() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let storage = ChunkStorage::new(2, None);
    storage
        .insert_rows(&[
            Row::with_labels(
                "cpu",
                vec![Label::new("instance", "instance-0000")],
                DataPoint::new(1_000, 1.0),
            ),
            Row::with_labels(
                "mem",
                vec![Label::new("instance", "instance-0001")],
                DataPoint::new(1_001, 2.0),
            ),
            Row::with_labels(
                "disk",
                vec![Label::new("instance", "instance-0002")],
                DataPoint::new(1_002, 3.0),
            ),
            Row::with_labels(
                "io",
                vec![Label::new("instance", "instance-0003")],
                DataPoint::new(1_003, 4.0),
            ),
        ])
        .unwrap();

    let all_series_seeds = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_all_series_seed_hook({
        let all_series_seeds = Arc::clone(&all_series_seeds);
        move || {
            all_series_seeds.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_matcher(SeriesMatcher::regex_no_match("__name__", "cpu|mem")),
        )
        .unwrap();

    let mut selected_metrics = selected
        .iter()
        .map(|series| series.name.clone())
        .collect::<Vec<_>>();
    selected_metrics.sort();

    assert_eq!(selected_metrics, vec!["disk".to_string(), "io".to_string()]);
    assert_eq!(
        all_series_seeds.load(Ordering::SeqCst),
        0,
        "metric negative-regex selectors should stay on indexed metadata paths",
    );

    storage.clear_metadata_all_series_seed_hook();
}

#[test]
fn select_series_high_cardinality_broad_empty_and_negative_matchers_preserve_empty_label_semantics()
{
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);
    let series_count = 8_192usize;
    let rows = (0..series_count)
        .map(|idx| {
            let mut labels = vec![
                Label::new("instance", format!("instance-{idx:06}")),
                Label::new("tenant", format!("tenant-{:02}", idx % 8)),
            ];
            if idx % 4 != 0 {
                labels.push(Label::new("job", format!("job-{idx:04}")));
            }
            Row::with_labels(
                "cpu",
                labels,
                DataPoint::new(1_000 + idx as i64, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let snapshots = Arc::new(AtomicUsize::new(0));
    let all_series_seeds = Arc::new(AtomicUsize::new(0));
    let all_series_postings = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_live_series_snapshot_hook({
        let snapshots = Arc::clone(&snapshots);
        move || {
            snapshots.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_all_series_seed_hook({
        let all_series_seeds = Arc::clone(&all_series_seeds);
        move || {
            all_series_seeds.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_all_series_postings_hook({
        let all_series_postings = Arc::clone(&all_series_postings);
        move || {
            all_series_postings.fetch_add(1, Ordering::SeqCst);
        }
    });

    let missing_selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("job", "")),
        )
        .unwrap();
    let broad_exact_selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("tenant", "tenant-03")),
        )
        .unwrap();
    let regex_selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_match("job", "job-2[0-9][0-9][0-9]")),
        )
        .unwrap();
    let empty_regex_selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_match("job", ".*")),
        )
        .unwrap();
    let negative_selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_no_match("job", "job-1[0-9][0-9][0-9]")),
        )
        .unwrap();

    let missing_count = rows.iter().filter(|row| row.labels().len() == 2).count();
    let broad_exact_count = rows
        .iter()
        .filter(|row| {
            row.labels()
                .iter()
                .find(|label| label.name == "tenant")
                .is_some_and(|label| label.value == "tenant-03")
        })
        .count();
    let regex_match_count = rows
        .iter()
        .filter(|row| {
            row.labels()
                .iter()
                .find(|label| label.name == "job")
                .is_some_and(|label| label.value.starts_with("job-2"))
        })
        .count();
    let excluded_present_count = rows
        .iter()
        .filter(|row| {
            row.labels()
                .iter()
                .find(|label| label.name == "job")
                .is_some_and(|label| label.value.starts_with("job-1"))
        })
        .count();

    assert_eq!(missing_selected.len(), missing_count);
    assert!(missing_selected
        .iter()
        .all(|series| { series.labels.iter().all(|label| label.name != "job") }));
    assert_eq!(broad_exact_selected.len(), broad_exact_count);
    assert!(broad_exact_selected.iter().all(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "tenant")
            .is_some_and(|label| label.value == "tenant-03")
    }));
    assert_eq!(regex_selected.len(), regex_match_count);
    assert!(regex_selected.iter().all(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "job")
            .is_some_and(|label| label.value.starts_with("job-2"))
    }));
    assert_eq!(empty_regex_selected.len(), series_count);
    assert_eq!(
        negative_selected.len(),
        series_count - excluded_present_count
    );
    assert!(negative_selected.iter().all(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "job")
            .is_none_or(|label| !label.value.starts_with("job-1"))
    }));
    assert_eq!(
        snapshots.load(Ordering::SeqCst),
        0,
        "high-cardinality broad regex and negative selectors should stay on indexed metadata paths",
    );
    assert_eq!(
        all_series_seeds.load(Ordering::SeqCst),
        0,
        "high-cardinality broad selectors should not fall back to an all-series seed",
    );
    assert_eq!(
        all_series_postings.load(Ordering::SeqCst),
        0,
        "high-cardinality broad selectors should not materialize the global all-series bitmap",
    );

    storage.clear_metadata_live_series_snapshot_hook();
    storage.clear_metadata_all_series_seed_hook();
    storage.clear_metadata_all_series_postings_hook();
}

#[test]
fn select_series_metric_broad_negative_regex_uses_bounded_candidate_scan() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let storage = ChunkStorage::new(2, None);
    let series_count = 8_192usize;
    let rows = (0..series_count)
        .map(|idx| {
            let mut labels = vec![Label::new("instance", format!("instance-{idx:06}"))];
            if idx % 4 != 0 {
                labels.push(Label::new("job", format!("job-{idx:04}")));
            }
            Row::with_labels(
                "cpu",
                labels,
                DataPoint::new(1_000 + idx as i64, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let direct_candidate_scans = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_direct_candidate_scan_hook({
        let direct_candidate_scans = Arc::clone(&direct_candidate_scans);
        move || {
            direct_candidate_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_no_match("job", "job-1[0-9][0-9][0-9]")),
        )
        .unwrap();

    let excluded_present_count = rows
        .iter()
        .filter(|row| {
            row.labels()
                .iter()
                .find(|label| label.name == "job")
                .is_some_and(|label| label.value.starts_with("job-1"))
        })
        .count();
    assert_eq!(selected.len(), series_count - excluded_present_count);
    assert!(selected.iter().all(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "job")
            .is_none_or(|label| !label.value.starts_with("job-1"))
    }));
    assert!(
        direct_candidate_scans.load(Ordering::SeqCst) > 0,
        "metric-seeded broad negative selectors should fall back to bounded candidate scans instead of scanning every postings bucket",
    );

    storage.clear_metadata_direct_candidate_scan_hook();
}

#[test]
fn mixed_runtime_high_cardinality_broad_negative_regex_uses_persisted_postings_with_bounded_candidate_scan(
) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let series_count = 8_192usize;

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);
        let rows = (0..series_count)
            .map(|idx| {
                let mut labels = vec![Label::new("instance", format!("instance-{idx:06}"))];
                if idx % 4 != 0 {
                    labels.push(Label::new("job", format!("job-{idx:04}")));
                }
                Row::with_labels(
                    "cpu",
                    labels,
                    DataPoint::new(1_000 + idx as i64, idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);
    reopened
        .insert_rows(&[
            Row::with_labels(
                "cpu",
                vec![
                    Label::new("instance", "live-a"),
                    Label::new("job", "job-live"),
                ],
                DataPoint::new(20_000, 1.0),
            ),
            Row::with_labels(
                "cpu",
                vec![
                    Label::new("instance", "live-b"),
                    Label::new("job", "job-1999"),
                ],
                DataPoint::new(20_001, 2.0),
            ),
            Row::with_labels(
                "cpu",
                vec![Label::new("instance", "live-c")],
                DataPoint::new(20_002, 3.0),
            ),
        ])
        .unwrap();

    let persisted_postings_uses = Arc::new(AtomicUsize::new(0));
    let direct_candidate_scans = Arc::new(AtomicUsize::new(0));
    let all_series_seeds = Arc::new(AtomicUsize::new(0));
    let live_series_snapshots = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_persisted_postings_hook({
        let persisted_postings_uses = Arc::clone(&persisted_postings_uses);
        move || {
            persisted_postings_uses.fetch_add(1, Ordering::SeqCst);
        }
    });
    reopened.set_metadata_direct_candidate_scan_hook({
        let direct_candidate_scans = Arc::clone(&direct_candidate_scans);
        move || {
            direct_candidate_scans.fetch_add(1, Ordering::SeqCst);
        }
    });
    reopened.set_metadata_all_series_seed_hook({
        let all_series_seeds = Arc::clone(&all_series_seeds);
        move || {
            all_series_seeds.fetch_add(1, Ordering::SeqCst);
        }
    });
    reopened.set_metadata_live_series_snapshot_hook({
        let live_series_snapshots = Arc::clone(&live_series_snapshots);
        move || {
            live_series_snapshots.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_no_match("job", "job-1[0-9][0-9][0-9]")),
        )
        .unwrap();

    let excluded_present_count = (0..series_count)
        .filter(|idx| idx % 4 != 0 && (1000..2000).contains(idx))
        .count();
    assert_eq!(selected.len(), series_count - excluded_present_count + 2);
    assert!(selected.iter().all(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "job")
            .is_none_or(|label| !label.value.starts_with("job-1"))
    }));
    assert!(selected.iter().any(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "instance")
            .is_some_and(|label| label.value == "live-a")
    }));
    assert!(selected.iter().any(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "instance")
            .is_some_and(|label| label.value == "live-c")
    }));
    assert!(!selected.iter().any(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "instance")
            .is_some_and(|label| label.value == "live-b")
    }));
    assert!(
        persisted_postings_uses.load(Ordering::SeqCst) > 0,
        "mixed persisted+live broad selectors should keep using persisted postings for the persisted majority",
    );
    assert!(
        direct_candidate_scans.load(Ordering::SeqCst) > 0,
        "mixed persisted+live broad selectors should use bounded candidate scans instead of scanning every postings bucket",
    );
    assert_eq!(
        all_series_seeds.load(Ordering::SeqCst),
        0,
        "mixed persisted+live broad selectors should not fall back to an all-series seed",
    );
    assert_eq!(
        live_series_snapshots.load(Ordering::SeqCst),
        0,
        "mixed persisted+live broad selectors should not snapshot all live metadata series",
    );

    reopened.clear_metadata_persisted_postings_hook();
    reopened.clear_metadata_direct_candidate_scan_hook();
    reopened.clear_metadata_all_series_seed_hook();
    reopened.clear_metadata_live_series_snapshot_hook();
    let _ = reopened.close();
}

#[test]
fn reopened_storage_high_cardinality_broad_matchers_use_persisted_postings_without_all_series_seed()
{
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let series_count = 8_192usize;

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);
        let rows = (0..series_count)
            .map(|idx| {
                let mut labels = vec![Label::new("instance", format!("instance-{idx:06}"))];
                if idx % 4 != 0 {
                    labels.push(Label::new("job", format!("job-{idx:04}")));
                }
                Row::with_labels(
                    "cpu",
                    labels,
                    DataPoint::new(1_000 + idx as i64, idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);
    let persisted_postings_uses = Arc::new(AtomicUsize::new(0));
    let all_series_seeds = Arc::new(AtomicUsize::new(0));
    let live_series_snapshots = Arc::new(AtomicUsize::new(0));
    let all_series_postings = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_persisted_postings_hook({
        let persisted_postings_uses = Arc::clone(&persisted_postings_uses);
        move || {
            persisted_postings_uses.fetch_add(1, Ordering::SeqCst);
        }
    });
    reopened.set_metadata_all_series_seed_hook({
        let all_series_seeds = Arc::clone(&all_series_seeds);
        move || {
            all_series_seeds.fetch_add(1, Ordering::SeqCst);
        }
    });
    reopened.set_metadata_live_series_snapshot_hook({
        let live_series_snapshots = Arc::clone(&live_series_snapshots);
        move || {
            live_series_snapshots.fetch_add(1, Ordering::SeqCst);
        }
    });
    reopened.set_metadata_all_series_postings_hook({
        let all_series_postings = Arc::clone(&all_series_postings);
        move || {
            all_series_postings.fetch_add(1, Ordering::SeqCst);
        }
    });

    let regex_selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_match("job", "job-2[0-9][0-9][0-9]")),
        )
        .unwrap();
    let empty_regex_selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_match("job", ".*")),
        )
        .unwrap();
    let negative_selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_no_match("job", "job-1[0-9][0-9][0-9]")),
        )
        .unwrap();

    let missing_count = (0..series_count).filter(|idx| idx % 4 == 0).count();
    let regex_match_count = (0..series_count)
        .filter(|idx| idx % 4 != 0 && (2000..3000).contains(idx))
        .count();
    let excluded_present_count = (0..series_count)
        .filter(|idx| idx % 4 != 0 && (1000..2000).contains(idx))
        .count();

    assert_eq!(regex_selected.len(), regex_match_count);
    assert!(regex_selected.iter().all(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "job")
            .is_some_and(|label| label.value.starts_with("job-2"))
    }));
    assert_eq!(empty_regex_selected.len(), series_count);
    assert_eq!(
        negative_selected.len(),
        series_count - excluded_present_count
    );
    assert!(negative_selected.iter().all(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "job")
            .is_none_or(|label| !label.value.starts_with("job-1"))
    }));
    assert!(
        negative_selected.len() > missing_count,
        "negative matcher should include missing-label series as empty-string matches",
    );
    assert!(
        persisted_postings_uses.load(Ordering::SeqCst) > 0,
        "reopened high-cardinality broad selectors should keep using persisted postings",
    );
    assert_eq!(
        all_series_seeds.load(Ordering::SeqCst),
        0,
        "reopened high-cardinality broad selectors should not fall back to all-series seeds",
    );
    assert_eq!(
        live_series_snapshots.load(Ordering::SeqCst),
        0,
        "reopened high-cardinality broad selectors should not need live-series snapshots",
    );
    assert_eq!(
        all_series_postings.load(Ordering::SeqCst),
        0,
        "reopened high-cardinality broad selectors should not materialize global all-series postings",
    );

    reopened.clear_metadata_persisted_postings_hook();
    reopened.clear_metadata_all_series_seed_hook();
    reopened.clear_metadata_live_series_snapshot_hook();
    reopened.clear_metadata_all_series_postings_hook();
    let _ = reopened.close();
}

#[test]
fn reopened_storage_high_cardinality_exact_and_missing_matchers_stay_on_indexed_paths() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let series_count = 8_192usize;
    let target_instance = "instance-004096";

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);
        let rows = (0..series_count)
            .map(|idx| {
                let mut labels = vec![
                    Label::new("instance", format!("instance-{idx:06}")),
                    Label::new("tenant", format!("tenant-{:02}", idx % 8)),
                ];
                if idx % 4 != 0 {
                    labels.push(Label::new("job", format!("job-{:02}", idx % 32)));
                }
                Row::with_labels(
                    "cpu",
                    labels,
                    DataPoint::new(1_000 + idx as i64, idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);
    let persisted_postings_uses = Arc::new(AtomicUsize::new(0));
    let all_series_seeds = Arc::new(AtomicUsize::new(0));
    let live_series_snapshots = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_persisted_postings_hook({
        let persisted_postings_uses = Arc::clone(&persisted_postings_uses);
        move || {
            persisted_postings_uses.fetch_add(1, Ordering::SeqCst);
        }
    });
    reopened.set_metadata_all_series_seed_hook({
        let all_series_seeds = Arc::clone(&all_series_seeds);
        move || {
            all_series_seeds.fetch_add(1, Ordering::SeqCst);
        }
    });
    reopened.set_metadata_live_series_snapshot_hook({
        let live_series_snapshots = Arc::clone(&live_series_snapshots);
        move || {
            live_series_snapshots.fetch_add(1, Ordering::SeqCst);
        }
    });

    let exact_selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("instance", target_instance)),
        )
        .unwrap();
    let broad_exact_selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("tenant", "tenant-03")),
        )
        .unwrap();
    let missing_selected = reopened
        .select_series(&SeriesSelection::new().with_matcher(SeriesMatcher::equal("job", "")))
        .unwrap();

    let missing_count = (0..series_count).filter(|idx| idx % 4 == 0).count();
    let broad_exact_count = (0..series_count).filter(|idx| idx % 8 == 3).count();
    assert_eq!(exact_selected.len(), 1);
    assert_eq!(
        exact_selected[0]
            .labels
            .iter()
            .find(|label| label.name == "instance")
            .map(|label| label.value.as_str()),
        Some(target_instance),
    );
    assert_eq!(broad_exact_selected.len(), broad_exact_count);
    assert!(broad_exact_selected.iter().all(|series| {
        series
            .labels
            .iter()
            .find(|label| label.name == "tenant")
            .is_some_and(|label| label.value == "tenant-03")
    }));
    assert_eq!(missing_selected.len(), missing_count);
    assert!(missing_selected
        .iter()
        .all(|series| series.labels.iter().all(|label| label.name != "job")));
    assert!(
        persisted_postings_uses.load(Ordering::SeqCst) > 0,
        "reopened high-cardinality exact and missing-label selectors should keep using persisted postings",
    );
    assert_eq!(
        all_series_seeds.load(Ordering::SeqCst),
        0,
        "reopened high-cardinality exact and missing-label selectors should not fall back to all-series seeds",
    );
    assert_eq!(
        live_series_snapshots.load(Ordering::SeqCst),
        0,
        "reopened high-cardinality exact and missing-label selectors should not need live-series snapshots",
    );

    reopened.clear_metadata_persisted_postings_hook();
    reopened.clear_metadata_all_series_seed_hook();
    reopened.clear_metadata_live_series_snapshot_hook();
    let _ = reopened.close();
}

#[test]
fn reopened_storage_uses_merged_persisted_postings_for_runtime_metadata_selection() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);
        let rows = (0..128usize)
            .map(|idx| {
                Row::with_labels(
                    "cpu",
                    vec![
                        Label::new("instance", format!("instance-{idx:04}")),
                        Label::new("job", format!("job-{:02}", idx % 32)),
                        Label::new("tenant", format!("tenant-{:02}", idx % 4)),
                    ],
                    DataPoint::new(1_000 + idx as i64, idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);
    assert!(
        !reopened
            .persisted
            .persisted_index
            .read()
            .merged_postings
            .metric_postings
            .is_empty(),
        "reopened storage should retain the merged persisted planner index after load",
    );

    let persisted_postings_uses = Arc::new(AtomicUsize::new(0));
    let all_series_seeds = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_persisted_postings_hook({
        let persisted_postings_uses = Arc::clone(&persisted_postings_uses);
        move || {
            persisted_postings_uses.fetch_add(1, Ordering::SeqCst);
        }
    });
    reopened.set_metadata_all_series_seed_hook({
        let all_series_seeds = Arc::clone(&all_series_seeds);
        move || {
            all_series_seeds.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_match("job", "job-(08|09)")),
        )
        .unwrap();

    assert_eq!(selected.len(), 8);
    assert!(
        persisted_postings_uses.load(Ordering::SeqCst) > 0,
        "reopened metadata selection should consult retained persisted postings",
    );
    assert_eq!(
        all_series_seeds.load(Ordering::SeqCst),
        0,
        "retained persisted postings should avoid falling back to all-series seeds after reopen",
    );

    reopened.clear_metadata_persisted_postings_hook();
    reopened.clear_metadata_all_series_seed_hook();
    let _ = reopened.close();
}

#[test]
fn mixed_runtime_metadata_selection_keeps_persisted_postings_during_ingest() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);
        let rows = (0..32usize)
            .map(|idx| {
                Row::with_labels(
                    "cpu",
                    vec![
                        Label::new("instance", format!("instance-{idx:04}")),
                        Label::new("job", format!("job-{:02}", idx % 8)),
                        Label::new("tenant", format!("tenant-{:02}", idx % 4)),
                    ],
                    DataPoint::new(1_000 + idx as i64, idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);

    reopened
        .delete_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("instance", "instance-0011")),
        )
        .unwrap();

    reopened
        .insert_rows(&[
            Row::with_labels(
                "cpu",
                vec![
                    Label::new("instance", "live-a"),
                    Label::new("job", "job-03"),
                    Label::new("tenant", "tenant-live"),
                ],
                DataPoint::new(10_000, 100.0),
            ),
            Row::with_labels(
                "cpu",
                vec![
                    Label::new("instance", "live-b"),
                    Label::new("job", "job-03"),
                    Label::new("tenant", "tenant-live"),
                ],
                DataPoint::new(10_001, 101.0),
            ),
            Row::with_labels(
                "cpu",
                vec![
                    Label::new("instance", "live-c"),
                    Label::new("job", "job-04"),
                    Label::new("tenant", "tenant-live"),
                ],
                DataPoint::new(10_002, 102.0),
            ),
        ])
        .unwrap();

    let persisted_postings_uses = Arc::new(AtomicUsize::new(0));
    let all_series_seeds = Arc::new(AtomicUsize::new(0));
    let live_series_snapshots = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_persisted_postings_hook({
        let persisted_postings_uses = Arc::clone(&persisted_postings_uses);
        move || {
            persisted_postings_uses.fetch_add(1, Ordering::SeqCst);
        }
    });
    reopened.set_metadata_all_series_seed_hook({
        let all_series_seeds = Arc::clone(&all_series_seeds);
        move || {
            all_series_seeds.fetch_add(1, Ordering::SeqCst);
        }
    });
    reopened.set_metadata_live_series_snapshot_hook({
        let live_series_snapshots = Arc::clone(&live_series_snapshots);
        move || {
            live_series_snapshots.fetch_add(1, Ordering::SeqCst);
        }
    });

    let exact_selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("job", "job-03")),
        )
        .unwrap();
    let regex_selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_match("job", "job-(03|04)")),
        )
        .unwrap();
    let negative_selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::not_equal("job", "job-03")),
        )
        .unwrap();

    fn label_value<'a>(series: &'a MetricSeries, name: &str) -> Option<&'a str> {
        series
            .labels
            .iter()
            .find(|label| label.name == name)
            .map(|label| label.value.as_str())
    }

    assert_eq!(exact_selected.len(), 5);
    assert!(exact_selected
        .iter()
        .all(|series| label_value(series, "job") == Some("job-03")));
    assert!(exact_selected
        .iter()
        .any(|series| label_value(series, "instance") == Some("live-a")));
    assert!(exact_selected
        .iter()
        .any(|series| label_value(series, "instance") == Some("live-b")));
    assert!(!exact_selected
        .iter()
        .any(|series| label_value(series, "instance") == Some("instance-0011")));

    assert_eq!(regex_selected.len(), 10);
    assert!(regex_selected
        .iter()
        .all(|series| { matches!(label_value(series, "job"), Some("job-03") | Some("job-04")) }));
    assert!(regex_selected
        .iter()
        .any(|series| label_value(series, "instance") == Some("live-c")));

    assert_eq!(negative_selected.len(), 29);
    assert!(negative_selected
        .iter()
        .all(|series| label_value(series, "job") != Some("job-03")));
    assert!(!negative_selected
        .iter()
        .any(|series| label_value(series, "instance") == Some("instance-0011")));

    assert!(
        persisted_postings_uses.load(Ordering::SeqCst) > 0,
        "mixed persisted+live metadata selection should keep using persisted postings during ingest",
    );
    assert_eq!(
        all_series_seeds.load(Ordering::SeqCst),
        0,
        "mixed persisted+live metadata selection should not fall back to all-series seeds",
    );
    assert_eq!(
        live_series_snapshots.load(Ordering::SeqCst),
        0,
        "mixed persisted+live metadata selection should not need a live-series snapshot",
    );

    reopened.clear_metadata_persisted_postings_hook();
    reopened.clear_metadata_all_series_seed_hook();
    reopened.clear_metadata_live_series_snapshot_hook();
    let _ = reopened.close();
}

#[test]
fn runtime_metadata_delta_series_ids_track_ingest_flush_persist_and_delete() {
    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);

    let persisted_labels = vec![
        Label::new("instance", "persisted-a"),
        Label::new("job", "job-a"),
    ];
    let other_persisted_labels = vec![
        Label::new("instance", "persisted-b"),
        Label::new("job", "job-b"),
    ];
    storage
        .insert_rows(&[
            Row::with_labels("cpu", persisted_labels.clone(), DataPoint::new(1_000, 1.0)),
            Row::with_labels(
                "cpu",
                other_persisted_labels.clone(),
                DataPoint::new(1_001, 2.0),
            ),
        ])
        .unwrap();

    let registry = storage.catalog.registry.read();
    let persisted_series_id = registry
        .resolve_existing("cpu", &persisted_labels)
        .unwrap()
        .series_id;
    let other_persisted_series_id = registry
        .resolve_existing("cpu", &other_persisted_labels)
        .unwrap()
        .series_id;
    drop(registry);

    assert_eq!(
        runtime_metadata_delta_series_ids(&storage),
        vec![persisted_series_id, other_persisted_series_id],
    );

    storage.flush_all_active().unwrap();
    assert_eq!(
        runtime_metadata_delta_series_ids(&storage),
        vec![persisted_series_id, other_persisted_series_id],
    );

    assert!(storage.persist_segment().unwrap());
    assert!(
        runtime_metadata_delta_series_ids(&storage).is_empty(),
        "persisted series should leave the live-not-yet-persisted delta once postings cover them",
    );

    let live_only_labels = vec![Label::new("instance", "live-c"), Label::new("job", "job-c")];
    storage
        .insert_rows(&[
            Row::with_labels("cpu", persisted_labels.clone(), DataPoint::new(2_000, 3.0)),
            Row::with_labels("cpu", live_only_labels.clone(), DataPoint::new(2_001, 4.0)),
        ])
        .unwrap();

    let live_only_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("cpu", &live_only_labels)
        .unwrap()
        .series_id;
    assert_eq!(
        runtime_metadata_delta_series_ids(&storage),
        vec![live_only_series_id],
        "new live-only series should enter the delta while additional writes to persisted series should not",
    );

    storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("instance", "live-c")),
        )
        .unwrap();
    assert!(
        runtime_metadata_delta_series_ids(&storage).is_empty(),
        "fully deleted live-only series should be pruned from the delta",
    );

    storage.close().unwrap();
}

#[test]
fn runtime_metadata_delta_series_ids_refresh_reconcile_persisted_removals_and_reloads() {
    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);

    let labels = vec![
        Label::new("instance", "refresh-a"),
        Label::new("job", "job-r"),
    ];
    storage
        .insert_rows(&[Row::with_labels(
            "cpu",
            labels.clone(),
            DataPoint::new(1_000, 1.0),
        )])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("cpu", &labels)
        .unwrap()
        .series_id;

    storage.flush_all_active().unwrap();
    assert!(storage.persist_segment().unwrap());
    assert!(
        runtime_metadata_delta_series_ids(&storage).is_empty(),
        "persisted series should not remain in the delta after flush publish",
    );

    storage
        .insert_rows(&[Row::with_labels(
            "cpu",
            labels.clone(),
            DataPoint::new(2_000, 2.0),
        )])
        .unwrap();
    assert!(
        runtime_metadata_delta_series_ids(&storage).is_empty(),
        "additional live data for an already persisted series should still use persisted postings",
    );

    let roots = storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert!(!roots.is_empty());

    storage.remove_persisted_segment_roots(&roots).unwrap();
    assert_eq!(
        runtime_metadata_delta_series_ids(&storage),
        vec![series_id],
        "removing persisted coverage while live data remains should move the series back into the delta",
    );
    assert_eq!(
        storage
            .select_series(
                &SeriesSelection::new()
                    .with_metric("cpu")
                    .with_matcher(SeriesMatcher::equal("instance", "refresh-a")),
            )
            .unwrap()
            .len(),
        1,
        "metadata selection should still see the live series while persisted roots are absent",
    );

    let loaded = load_segment_indexes(temp_dir.path().join(NUMERIC_LANE_ROOT)).unwrap();
    storage
        .add_persisted_segments_from_loaded(loaded.indexed_segments)
        .unwrap();
    assert!(
        runtime_metadata_delta_series_ids(&storage).is_empty(),
        "reloading persisted roots should remove the series from the delta again",
    );

    storage.close().unwrap();
}

#[test]
fn runtime_segment_adoption_uses_merged_persisted_postings_for_metadata_selection() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);
        let rows = (0..96usize)
            .map(|idx| {
                Row::with_labels(
                    "cpu",
                    vec![
                        Label::new("instance", format!("instance-{idx:04}")),
                        Label::new("job", format!("job-{:02}", idx % 24)),
                        Label::new("tenant", format!("tenant-{:02}", idx % 6)),
                    ],
                    DataPoint::new(1_000 + idx as i64, idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
    }

    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let loaded = load_segment_indexes(&lane_path).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(lane_path),
        None,
        loaded.next_segment_id,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: i64::MAX,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            retention_enforced: true,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: false,
            background_fail_fast: false,
            metadata_shard_count: None,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: Duration::from_secs(5),
            tiered_storage: None,
            #[cfg(test)]
            current_time_override: None,
        },
    )
    .unwrap();
    storage
        .add_persisted_segments_from_loaded(loaded.indexed_segments)
        .unwrap();

    assert!(
        !storage
            .persisted
            .persisted_index
            .read()
            .merged_postings
            .label_postings
            .is_empty(),
        "runtime segment adoption should seed the merged persisted postings planner state",
    );

    let persisted_postings_uses = Arc::new(AtomicUsize::new(0));
    let all_series_seeds = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_persisted_postings_hook({
        let persisted_postings_uses = Arc::clone(&persisted_postings_uses);
        move || {
            persisted_postings_uses.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_all_series_seed_hook({
        let all_series_seeds = Arc::clone(&all_series_seeds);
        move || {
            all_series_seeds.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_match("job", "job-(07|08)")),
        )
        .unwrap();

    assert_eq!(selected.len(), 8);
    assert!(
        persisted_postings_uses.load(Ordering::SeqCst) > 0,
        "runtime-adopted segments should expose persisted postings to the metadata planner",
    );
    assert_eq!(
        all_series_seeds.load(Ordering::SeqCst),
        0,
        "runtime-adopted persisted postings should avoid all-series metadata seeds",
    );

    storage.clear_metadata_persisted_postings_hook();
    storage.clear_metadata_all_series_seed_hook();
    storage.close().unwrap();
}

#[test]
fn reopened_large_history_metadata_snapshots_drop_legacy_duplicate_postings_bytes() {
    let temp_dir = TempDir::new().unwrap();
    let series_count = 2_048usize;
    let segment_count = 4usize;

    let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);
    for segment_idx in 0..segment_count {
        let rows = (0..series_count)
            .map(|idx| {
                Row::with_labels(
                    "cpu",
                    vec![
                        Label::new("instance", format!("instance-{idx:05}")),
                        Label::new("job", format!("job-{:02}", idx % 64)),
                        Label::new("tenant", format!("tenant-{:02}", idx % 8)),
                    ],
                    DataPoint::new(
                        10_000 * segment_idx as i64 + idx as i64,
                        (segment_idx * series_count + idx) as f64,
                    ),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
        assert!(
            storage.persist_segment().unwrap(),
            "each history batch should publish a persisted segment",
        );
    }
    storage.close().unwrap();

    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let loaded = load_segment_indexes(&lane_path).unwrap();
    assert!(
        loaded.indexed_segments.len() >= segment_count,
        "expected multiple persisted segments for the duplicated-postings regression",
    );
    let legacy_removed_bytes = legacy_removed_persisted_metadata_bytes(&loaded);

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);
    let startup_snapshot = reopened.memory_observability_snapshot();

    reopened
        .insert_rows(&[
            Row::with_labels(
                "cpu",
                vec![Label::new("instance", "live-missing-job")],
                DataPoint::new(100_000, 1.0),
            ),
            Row::with_labels(
                "cpu",
                vec![
                    Label::new("instance", "live-with-job"),
                    Label::new("job", "job-live"),
                ],
                DataPoint::new(100_001, 2.0),
            ),
        ])
        .unwrap();
    let selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("job", "")),
        )
        .unwrap();
    assert_eq!(
        selected.len(),
        1,
        "steady-state query should still see the live missing-label series",
    );
    let steady_state_snapshot = reopened.memory_observability_snapshot();

    let legacy_startup_bytes = startup_snapshot
        .persisted_index_bytes
        .saturating_add(legacy_removed_bytes);
    let legacy_steady_state_bytes = steady_state_snapshot
        .persisted_index_bytes
        .saturating_add(legacy_removed_bytes);

    assert!(
        legacy_removed_bytes >= 512 * 1024,
        "expected the removed duplicate runtime metadata to be material, observed only {legacy_removed_bytes} bytes",
    );
    assert!(
        startup_snapshot.persisted_index_bytes * 4 <= legacy_startup_bytes * 3,
        "startup persisted-index memory should drop materially after removing duplicated segment postings (actual={}, legacy={legacy_startup_bytes})",
        startup_snapshot.persisted_index_bytes,
    );
    assert!(
        steady_state_snapshot.persisted_index_bytes * 4 <= legacy_steady_state_bytes * 3,
        "steady-state persisted-index memory should stay materially below the legacy duplicated footprint (actual={}, legacy={legacy_steady_state_bytes})",
        steady_state_snapshot.persisted_index_bytes,
    );

    reopened.close().unwrap();
}

#[test]
fn persisted_merged_postings_update_across_segment_removal_and_re_adoption() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();

    let series_a = registry
        .resolve_or_insert(
            "cpu",
            &[Label::new("host", "a"), Label::new("job", "job-01")],
        )
        .unwrap()
        .series_id;
    let series_b = registry
        .resolve_or_insert(
            "cpu",
            &[Label::new("host", "b"), Label::new("job", "job-02")],
        )
        .unwrap()
        .series_id;
    let series_c = registry
        .resolve_or_insert("cpu", &[Label::new("host", "c")])
        .unwrap()
        .series_id;

    let first_writer = SegmentWriter::new(&numeric_lane_path, 0, 1).unwrap();
    first_writer
        .write_segment(
            &registry,
            &HashMap::from([
                (
                    series_a,
                    vec![make_persisted_numeric_chunk(series_a, &[(10, 1.0)])],
                ),
                (
                    series_b,
                    vec![make_persisted_numeric_chunk(series_b, &[(11, 2.0)])],
                ),
            ]),
        )
        .unwrap();
    let first_root = first_writer.layout().root.clone();

    let second_writer = SegmentWriter::new(&numeric_lane_path, 0, 2).unwrap();
    second_writer
        .write_segment(
            &registry,
            &HashMap::from([
                (
                    series_b,
                    vec![make_persisted_numeric_chunk(series_b, &[(20, 3.0)])],
                ),
                (
                    series_c,
                    vec![make_persisted_numeric_chunk(series_c, &[(21, 4.0)])],
                ),
            ]),
        )
        .unwrap();
    let second_root = second_writer.layout().root.clone();

    let loaded = load_segment_indexes(&numeric_lane_path).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(numeric_lane_path),
        None,
        loaded.next_segment_id,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: i64::MAX,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            retention_enforced: true,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: false,
            background_fail_fast: false,
            metadata_shard_count: None,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: Duration::from_secs(5),
            tiered_storage: None,
            #[cfg(test)]
            current_time_override: None,
        },
    )
    .unwrap();
    storage
        .add_persisted_segments_from_loaded(loaded.indexed_segments)
        .unwrap();

    let assert_summary = |storage: &ChunkStorage,
                          expected_metric: &[u64],
                          expected_job_02: &[u64],
                          expected_missing_job: &[u64]| {
        let persisted_index = storage.persisted.persisted_index.read();
        assert_eq!(persisted_index.merged_postings.metric_postings.len(), 1);
        assert_eq!(
            persisted_index
                .merged_postings
                .series_id_postings_for_metric("cpu")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected_metric,
        );
        assert_eq!(
            persisted_index
                .merged_postings
                .postings_for_label("job", "job-02")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected_job_02,
        );
        assert_eq!(
            persisted_index
                .merged_postings
                .missing_label_postings_for_name("job")
                .iter()
                .collect::<Vec<_>>(),
            expected_missing_job,
        );
    };

    let count_selected = |storage: &ChunkStorage, selection: SeriesSelection| {
        storage.select_series(&selection).unwrap().len()
    };

    assert_summary(
        &storage,
        &[series_a, series_b, series_c],
        &[series_b],
        &[series_c],
    );
    assert_eq!(
        count_selected(
            &storage,
            SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("job", "job-02")),
        ),
        1,
    );
    assert_eq!(
        count_selected(
            &storage,
            SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("job", "")),
        ),
        1,
    );

    storage
        .remove_persisted_segment_roots(std::slice::from_ref(&second_root))
        .unwrap();
    assert_summary(&storage, &[series_a, series_b], &[series_b], &[]);
    assert_eq!(
        count_selected(
            &storage,
            SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("job", "job-02")),
        ),
        1,
    );
    assert_eq!(
        count_selected(
            &storage,
            SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("job", "")),
        ),
        0,
    );

    storage
        .add_persisted_segments_from_loaded(vec![load_segment_index(&second_root).unwrap()])
        .unwrap();
    assert_summary(
        &storage,
        &[series_a, series_b, series_c],
        &[series_b],
        &[series_c],
    );
    assert_eq!(
        count_selected(
            &storage,
            SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("job", "")),
        ),
        1,
    );

    storage
        .remove_persisted_segment_roots(std::slice::from_ref(&first_root))
        .unwrap();
    assert_summary(&storage, &[series_b, series_c], &[series_b], &[series_c]);
    assert_eq!(
        count_selected(
            &storage,
            SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("job", "job-01")),
        ),
        0,
    );

    storage.close().unwrap();
}

#[test]
fn select_series_time_range_uses_chunk_indexes() {
    let temp_dir = TempDir::new().unwrap();
    let now = 1_000;
    let storage = StorageBuilder::new()
        .with_current_time_override_for_tests(now)
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
        .with_current_time_override_for_tests(now)
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

    let _ = reopened.close();
}

#[test]
fn select_series_time_range_uses_visibility_summary_when_visible_cache_entries_are_missing() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let storage = ChunkStorage::new(4, None);
    let labels = vec![Label::new("host", "summary-cache-miss")];

    storage
        .insert_rows(&[
            Row::with_labels("cpu", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("cpu", &labels)
        .unwrap()
        .series_id;
    storage
        .visibility
        .series_visible_max_timestamps
        .write()
        .remove(&series_id);
    storage
        .visibility
        .series_visible_bounded_max_timestamps
        .write()
        .remove(&series_id);

    let summary_uses = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_time_range_summary_hook({
        let summary_uses = Arc::clone(&summary_uses);
        move || {
            summary_uses.fetch_add(1, Ordering::SeqCst);
        }
    });

    assert_eq!(
        storage
            .select_series(
                &SeriesSelection::new()
                    .with_metric("cpu")
                    .with_time_range(2, 3),
            )
            .unwrap(),
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels: labels.clone(),
        }],
    );
    assert!(
        summary_uses.load(Ordering::SeqCst) > 0,
        "time-range metadata selection should use maintained visibility summaries even when the visible cache entry is missing",
    );
    assert!(
        storage
            .visibility
            .series_visible_max_timestamps
            .read()
            .get(&series_id)
            .is_none(),
        "cache misses should not require a foreground visibility refresh to answer metadata reads",
    );

    storage.clear_metadata_time_range_summary_hook();
}

#[test]
fn select_series_time_range_uses_visible_timestamp_cache_before_persisted_decode() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);

        storage
            .insert_rows(&[
                Row::with_labels("cpu", labels.clone(), DataPoint::new(100, 1.0)),
                Row::with_labels("cpu", labels.clone(), DataPoint::new(500, 2.0)),
            ])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);

    reopened
        .delete_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("host", "a"))
                .with_time_range(500, 501),
        )
        .unwrap();

    let persisted_decodes = Arc::new(AtomicUsize::new(0));
    reopened.set_query_persisted_chunk_decode_hook({
        let persisted_decodes = Arc::clone(&persisted_decodes);
        move || {
            persisted_decodes.fetch_add(1, Ordering::SeqCst);
        }
    });

    assert_eq!(
        reopened
            .select_series(&SeriesSelection::new().with_metric("cpu"))
            .unwrap(),
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels: labels.clone(),
        }],
    );
    assert!(reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(200, 300),
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        persisted_decodes.load(Ordering::SeqCst),
        0,
        "visible max timestamp cache should reject out-of-range persisted series before decode",
    );

    reopened.clear_query_persisted_chunk_decode_hook();
    let _ = reopened.close();
}

#[test]
fn select_series_time_range_sees_encoded_only_sealed_chunks_before_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "sealed-only")];
    let storage = live_numeric_storage(
        temp_dir.path(),
        TimestampPrecision::Seconds,
        4,
        100,
        Duration::from_secs(14 * 24 * 3600),
        None,
    );

    storage
        .insert_rows(&[
            Row::with_labels("cpu", labels.clone(), DataPoint::new(10, 1.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(20, 2.0)),
        ])
        .unwrap();
    storage.flush_all_active().unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("cpu", &labels)
        .unwrap()
        .series_id;
    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 1);
    assert!(chunks[0].points.is_empty());
    assert!(!chunks[0].encoded_payload.is_empty());
    drop(sealed);

    assert_eq!(
        storage
            .select_series(
                &SeriesSelection::new()
                    .with_metric("cpu")
                    .with_time_range(15, 25),
            )
            .unwrap(),
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels,
        }],
    );

    storage.close().unwrap();
}

#[test]
fn select_series_in_shards_time_range_sees_encoded_only_sealed_chunks_before_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let shard_count = 8;
    let target_shard = 3;
    let target_labels = labels_for_shard("cpu", shard_count, target_shard, "sealed-target");
    let other_labels = labels_not_in_shard("cpu", shard_count, target_shard, "sealed-other");
    let storage = live_numeric_storage(
        temp_dir.path(),
        TimestampPrecision::Seconds,
        4,
        100,
        Duration::from_secs(14 * 24 * 3600),
        Some(shard_count),
    );

    storage
        .insert_rows(&[
            Row::with_labels("cpu", target_labels.clone(), DataPoint::new(10, 1.0)),
            Row::with_labels("cpu", target_labels.clone(), DataPoint::new(20, 2.0)),
            Row::with_labels("cpu", other_labels, DataPoint::new(200, 3.0)),
        ])
        .unwrap();
    storage.flush_all_active().unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("cpu", &target_labels)
        .unwrap()
        .series_id;
    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 1);
    assert!(chunks[0].points.is_empty());
    assert!(!chunks[0].encoded_payload.is_empty());
    drop(sealed);

    let selection = SeriesSelection::new()
        .with_metric("cpu")
        .with_time_range(15, 25);
    let scope = MetadataShardScope::new(shard_count, vec![target_shard]);
    assert_eq!(
        storage.select_series_in_shards(&selection, &scope).unwrap(),
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels: target_labels,
        }],
    );

    storage.close().unwrap();
}

#[test]
fn select_series_time_range_keeps_mixed_active_and_sealed_series_visible_across_retention_and_tombstones(
) {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "mixed")];
    let storage = live_numeric_storage(
        temp_dir.path(),
        TimestampPrecision::Seconds,
        2,
        36,
        Duration::from_secs(5),
        None,
    );

    storage
        .insert_rows(&[
            Row::with_labels("cpu", labels.clone(), DataPoint::new(34, 1.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(36, 2.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(39, 3.0)),
        ])
        .unwrap();
    storage.set_current_time_override(40);

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("cpu", &labels)
        .unwrap()
        .series_id;
    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(chunks.len(), 1);
    assert!(chunks[0].points.is_empty());
    assert!(!chunks[0].encoded_payload.is_empty());
    drop(sealed);

    storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("host", "mixed"))
                .with_time_range(39, 40),
        )
        .unwrap();

    assert!(storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(0, 35),
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        storage
            .select_series(
                &SeriesSelection::new()
                    .with_metric("cpu")
                    .with_time_range(35, 40),
            )
            .unwrap(),
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels: labels.clone(),
        }],
    );
    assert!(storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(38, 40),
        )
        .unwrap()
        .is_empty());

    storage.close().unwrap();
}

#[test]
fn select_series_time_range_respects_tombstones_exactly_for_persisted_series() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

        storage
            .insert_rows(&[
                Row::with_labels("cpu", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("cpu", labels.clone(), DataPoint::new(3, 2.0)),
            ])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

    reopened
        .delete_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("host", "a"))
                .with_time_range(3, 4),
        )
        .unwrap();

    assert!(reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(2, 4),
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        reopened
            .select_series(
                &SeriesSelection::new()
                    .with_metric("cpu")
                    .with_time_range(0, 2),
            )
            .unwrap(),
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels,
        }],
    );

    let _ = reopened.close();
}

#[test]
fn select_series_time_range_respects_retention_cutoff_exactly() {
    let ingest_now = 10;
    let query_now = 40;
    let labels = vec![Label::new("host", "a")];
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = StorageBuilder::new()
            .with_current_time_override_for_tests(ingest_now)
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_retention(Duration::from_secs(5))
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::with_labels(
                "cpu",
                labels.clone(),
                DataPoint::new(10, 1.0),
            )])
            .unwrap();
        storage
            .insert_rows(&[Row::with_labels(
                "cpu",
                labels.clone(),
                DataPoint::new(40, 2.0),
            )])
            .unwrap();
    }

    let storage = StorageBuilder::new()
        .with_current_time_override_for_tests(query_now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_retention(Duration::from_secs(5))
        .build()
        .unwrap();

    assert!(storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(0, 20),
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        storage
            .select_series(
                &SeriesSelection::new()
                    .with_metric("cpu")
                    .with_time_range(35, 45),
            )
            .unwrap(),
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels,
        }],
    );

    storage.close().unwrap();
}

#[test]
fn select_series_time_range_stays_decode_free_across_many_series_and_segments() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let series_count = 128usize;
    let history_segments = 32usize;

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

        for segment_offset in 0..history_segments {
            let ts = 10 + segment_offset as i64;
            let rows = (0..series_count)
                .map(|series_idx| {
                    Row::with_labels(
                        "cpu",
                        vec![Label::new("host", format!("host-{series_idx:04}"))],
                        DataPoint::new(ts, series_idx as f64),
                    )
                })
                .collect::<Vec<_>>();
            storage.insert_rows(&rows).unwrap();
            storage.flush_all_active().unwrap();
            assert!(storage.persist_segment_with_outcome().unwrap().persisted);
        }

        storage.close().unwrap();
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

    let persisted_decodes = Arc::new(AtomicUsize::new(0));
    reopened.set_query_persisted_chunk_decode_hook({
        let persisted_decodes = Arc::clone(&persisted_decodes);
        move || {
            persisted_decodes.fetch_add(1, Ordering::SeqCst);
        }
    });

    assert!(reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(10_000, 20_000),
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        persisted_decodes.load(Ordering::SeqCst),
        0,
        "long-history metadata queries should reject stale series without persisted timestamp decode",
    );

    reopened.clear_query_persisted_chunk_decode_hook();
    let _ = reopened.close();
}

#[test]
fn select_series_time_range_uses_timestamp_search_index_for_partial_persisted_overlap() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "gap")];

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 8);
        storage
            .insert_rows(&[
                Row::with_labels("cpu", labels.clone(), DataPoint::new(10, 1.0)),
                Row::with_labels("cpu", labels.clone(), DataPoint::new(100, 2.0)),
                Row::with_labels("cpu", labels.clone(), DataPoint::new(200, 3.0)),
            ])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
        storage.close().unwrap();
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 8);

    let persisted_decodes = Arc::new(AtomicUsize::new(0));
    reopened.set_query_persisted_chunk_decode_hook({
        let persisted_decodes = Arc::clone(&persisted_decodes);
        move || {
            persisted_decodes.fetch_add(1, Ordering::SeqCst);
        }
    });

    assert!(reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(50, 90),
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        reopened
            .select_series(
                &SeriesSelection::new()
                    .with_metric("cpu")
                    .with_time_range(90, 101),
            )
            .unwrap(),
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels,
        }],
    );
    assert_eq!(
        persisted_decodes.load(Ordering::SeqCst),
        0,
        "partial overlapping persisted chunks should use the timestamp search index instead of full timestamp decode",
    );

    reopened.clear_query_persisted_chunk_decode_hook();
    reopened.close().unwrap();
}

#[test]
fn reconcile_live_metadata_indexes_uses_timestamp_search_index_for_bounded_overlap() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let now = -800;
    let labels = vec![Label::new("host", "bounded-gap")];
    let open_storage = || {
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
                retention_enforced: true,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: Some(now),
            },
        )
        .unwrap()
    };

    {
        let storage = open_storage();
        storage
            .insert_rows(&[
                Row::with_labels("cpu", labels.clone(), DataPoint::new(10, 1.0)),
                Row::with_labels("cpu", labels.clone(), DataPoint::new(100, 2.0)),
                Row::with_labels("cpu", labels.clone(), DataPoint::new(200, 3.0)),
            ])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
        storage.close().unwrap();
    }

    let reopened = open_storage();
    reopened.load_tombstones_index().unwrap();
    reopened
        .apply_loaded_segment_indexes(
            load_segment_indexes(temp_dir.path().join(NUMERIC_LANE_ROOT)).unwrap(),
            false,
        )
        .unwrap();

    let persisted_decodes = Arc::new(AtomicUsize::new(0));
    reopened.set_query_persisted_chunk_decode_hook({
        let persisted_decodes = Arc::clone(&persisted_decodes);
        move || {
            persisted_decodes.fetch_add(1, Ordering::SeqCst);
        }
    });

    reopened.reconcile_live_metadata_indexes().unwrap();
    assert_eq!(
        reopened.bounded_recency_reference_timestamp(),
        Some(100),
        "bounded recency should fall back to the latest timestamp at or below the future-skew cutoff",
    );
    assert_eq!(
        persisted_decodes.load(Ordering::SeqCst),
        0,
        "bounded overlap cache repair should use the timestamp search index instead of full timestamp decode",
    );

    reopened.clear_query_persisted_chunk_decode_hook();
    reopened.close().unwrap();
}

#[test]
fn reconcile_live_metadata_indexes_uses_chunk_bounds_without_persisted_decode() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "bench")];
    let history_segments = 128usize;

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

        for segment_offset in 0..history_segments {
            storage
                .insert_rows(&[Row::with_labels(
                    "cpu",
                    labels.clone(),
                    DataPoint::new(10 + segment_offset as i64, segment_offset as f64),
                )])
                .unwrap();
            storage.flush_all_active().unwrap();
            assert!(storage.persist_segment_with_outcome().unwrap().persisted);
        }

        storage.close().unwrap();
    }

    let reopened = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);
    reopened.load_tombstones_index().unwrap();
    reopened
        .apply_loaded_segment_indexes(
            load_segment_indexes(temp_dir.path().join(NUMERIC_LANE_ROOT)).unwrap(),
            false,
        )
        .unwrap();

    let persisted_decodes = Arc::new(AtomicUsize::new(0));
    reopened.set_query_persisted_chunk_decode_hook({
        let persisted_decodes = Arc::clone(&persisted_decodes);
        move || {
            persisted_decodes.fetch_add(1, Ordering::SeqCst);
        }
    });

    reopened.reconcile_live_metadata_indexes().unwrap();
    assert_eq!(
        persisted_decodes.load(Ordering::SeqCst),
        0,
        "visible timestamp cache refresh should reuse persisted chunk bounds across long history",
    );

    reopened.clear_query_persisted_chunk_decode_hook();
    reopened.close().unwrap();
}

#[test]
fn select_series_time_range_segment_prune_keeps_overlap_bitmap_bounded_to_candidates() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let total_series = 512usize;
    let matching_series = 4usize;
    let recent_segments = 40usize;
    let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

    let rows = (0..total_series)
        .map(|series_idx| {
            let tenant = if series_idx < matching_series {
                "tenant-target".to_string()
            } else {
                format!("tenant-{series_idx:04}")
            };
            Row::with_labels(
                "cpu",
                vec![
                    Label::new("host", format!("host-{series_idx:04}")),
                    Label::new("tenant", tenant),
                ],
                DataPoint::new(10, series_idx as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();
    storage.flush_all_active().unwrap();
    assert!(storage.persist_segment_with_outcome().unwrap().persisted);

    for segment_offset in 0..recent_segments {
        let ts = 1_000 + (segment_offset as i64 * 10);
        let rows = (0..total_series)
            .map(|series_idx| {
                let tenant = if series_idx < matching_series {
                    "tenant-target".to_string()
                } else {
                    format!("tenant-{series_idx:04}")
                };
                Row::with_labels(
                    "cpu",
                    vec![
                        Label::new("host", format!("host-{series_idx:04}")),
                        Label::new("tenant", tenant),
                    ],
                    DataPoint::new(ts, series_idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
    }

    let segment_prunes = Arc::new(AtomicUsize::new(0));
    let segment_spills = Arc::new(AtomicUsize::new(0));
    let persisted_exact_scans = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_time_range_segment_prune_hook({
        let segment_prunes = Arc::clone(&segment_prunes);
        move || {
            segment_prunes.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_time_range_segment_spill_hook({
        let segment_spills = Arc::clone(&segment_spills);
        move || {
            segment_spills.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_time_range_persisted_exact_scan_hook({
        let persisted_exact_scans = Arc::clone(&persisted_exact_scans);
        move || {
            persisted_exact_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let mut selected_hosts = storage
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("tenant", "tenant-target"))
                .with_time_range(0, 20),
        )
        .unwrap()
        .into_iter()
        .map(|series| {
            series
                .labels
                .into_iter()
                .find(|label| label.name == "host")
                .unwrap()
                .value
        })
        .collect::<Vec<_>>();
    selected_hosts.sort();

    let expected_hosts = (0..matching_series)
        .map(|series_idx| format!("host-{series_idx:04}"))
        .collect::<Vec<_>>();
    assert_eq!(selected_hosts, expected_hosts);
    assert!(
        segment_prunes.load(Ordering::SeqCst) > 0,
        "time-range pruning should still consult overlapping persisted segments for exact-scan candidates",
    );
    assert_eq!(
        segment_spills.load(Ordering::SeqCst),
        0,
        "time-range pruning should not materialize segment series outside the already-selected candidate subset",
    );
    assert!(
        persisted_exact_scans.load(Ordering::SeqCst) <= matching_series,
        "persisted exact scans should stay bounded by the matched candidate subset",
    );

    storage.clear_metadata_time_range_segment_prune_hook();
    storage.clear_metadata_time_range_segment_spill_hook();
    storage.clear_metadata_time_range_persisted_exact_scan_hook();
}

#[test]
fn select_series_time_range_segment_prunes_persisted_exact_scans_at_high_cardinality() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let total_series = 128usize;
    let matching_series = 4usize;
    let recent_segments = 40usize;

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

        let old_rows = (0..matching_series)
            .map(|series_idx| {
                Row::with_labels(
                    "cpu",
                    vec![Label::new("host", format!("host-{series_idx:04}"))],
                    DataPoint::new(10, series_idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&old_rows).unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);

        for segment_offset in 0..recent_segments {
            let ts = 1_000 + (segment_offset as i64 * 10);
            let rows = (0..total_series)
                .map(|series_idx| {
                    Row::with_labels(
                        "cpu",
                        vec![Label::new("host", format!("host-{series_idx:04}"))],
                        DataPoint::new(ts, series_idx as f64),
                    )
                })
                .collect::<Vec<_>>();
            storage.insert_rows(&rows).unwrap();
            storage.flush_all_active().unwrap();
            assert!(storage.persist_segment_with_outcome().unwrap().persisted);
        }

        storage.close().unwrap();
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

    let segment_prunes = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_time_range_segment_prune_hook({
        let segment_prunes = Arc::clone(&segment_prunes);
        move || {
            segment_prunes.fetch_add(1, Ordering::SeqCst);
        }
    });
    let persisted_exact_scans = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_time_range_persisted_exact_scan_hook({
        let persisted_exact_scans = Arc::clone(&persisted_exact_scans);
        move || {
            persisted_exact_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let mut selected_hosts = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(0, 20),
        )
        .unwrap()
        .into_iter()
        .map(|series| {
            series
                .labels
                .into_iter()
                .find(|label| label.name == "host")
                .unwrap()
                .value
        })
        .collect::<Vec<_>>();
    selected_hosts.sort();

    let expected_hosts = (0..matching_series)
        .map(|series_idx| format!("host-{series_idx:04}"))
        .collect::<Vec<_>>();
    let exact_scan_candidates = reopened
        .visibility
        .series_visibility_summaries
        .read()
        .values()
        .filter(|summary| summary_needs_exact_scan(summary, 0, 20))
        .count();
    assert_eq!(selected_hosts, expected_hosts);
    assert!(
        segment_prunes.load(Ordering::SeqCst) > 0,
        "time-range metadata reads should consult persisted segment overlap pruning before exact scans (segment_prunes={}, exact_scans={})",
        segment_prunes.load(Ordering::SeqCst),
        persisted_exact_scans.load(Ordering::SeqCst),
    );
    assert!(
        persisted_exact_scans.load(Ordering::SeqCst) < exact_scan_candidates,
        "segment pruning should reduce persisted exact scans below the pre-prune exact-scan candidate count (candidates={}, exact_scans={})",
        exact_scan_candidates,
        persisted_exact_scans.load(Ordering::SeqCst),
    );

    reopened.clear_metadata_time_range_segment_prune_hook();
    reopened.clear_metadata_time_range_persisted_exact_scan_hook();
    reopened.close().unwrap();
}

#[test]
fn select_series_time_range_segment_series_summaries_prune_truncated_old_history() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let total_series = 128usize;
    let matching_series = 4usize;
    let recent_segments = 40usize;

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

        let mixed_rows = (0..total_series)
            .map(|series_idx| {
                let ts = if series_idx < matching_series {
                    10
                } else {
                    1_000
                };
                Row::with_labels(
                    "cpu",
                    vec![Label::new("host", format!("host-{series_idx:04}"))],
                    DataPoint::new(ts, series_idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&mixed_rows).unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);

        for segment_offset in 0..recent_segments {
            let ts = 2_000 + (segment_offset as i64 * 10);
            let rows = (0..total_series)
                .map(|series_idx| {
                    Row::with_labels(
                        "cpu",
                        vec![Label::new("host", format!("host-{series_idx:04}"))],
                        DataPoint::new(ts, series_idx as f64),
                    )
                })
                .collect::<Vec<_>>();
            storage.insert_rows(&rows).unwrap();
            storage.flush_all_active().unwrap();
            assert!(storage.persist_segment_with_outcome().unwrap().persisted);
        }

        storage.close().unwrap();
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

    let segment_prunes = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_time_range_segment_prune_hook({
        let segment_prunes = Arc::clone(&segment_prunes);
        move || {
            segment_prunes.fetch_add(1, Ordering::SeqCst);
        }
    });
    let persisted_exact_scans = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_time_range_persisted_exact_scan_hook({
        let persisted_exact_scans = Arc::clone(&persisted_exact_scans);
        move || {
            persisted_exact_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let mut selected_hosts = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(0, 20),
        )
        .unwrap()
        .into_iter()
        .map(|series| {
            series
                .labels
                .into_iter()
                .find(|label| label.name == "host")
                .unwrap()
                .value
        })
        .collect::<Vec<_>>();
    selected_hosts.sort();

    let expected_hosts = (0..matching_series)
        .map(|series_idx| format!("host-{series_idx:04}"))
        .collect::<Vec<_>>();
    let exact_scan_candidates = reopened
        .visibility
        .series_visibility_summaries
        .read()
        .values()
        .filter(|summary| summary_needs_exact_scan(summary, 0, 20))
        .count();
    assert_eq!(selected_hosts, expected_hosts);
    assert_eq!(
        exact_scan_candidates, total_series,
        "the maintained summary should still treat the truncated old window as needing exact scans before segment-series pruning runs",
    );
    assert!(
        segment_prunes.load(Ordering::SeqCst) > 0,
        "time-range metadata reads should still consult persisted segment pruning before exact scans (segment_prunes={}, exact_scans={})",
        segment_prunes.load(Ordering::SeqCst),
        persisted_exact_scans.load(Ordering::SeqCst),
    );
    assert_eq!(
        persisted_exact_scans.load(Ordering::SeqCst),
        matching_series,
        "per-series segment summaries should bound exact persisted scans to the truly overlapping series in the mixed old segment",
    );

    reopened.clear_metadata_time_range_segment_prune_hook();
    reopened.clear_metadata_time_range_persisted_exact_scan_hook();
    reopened.close().unwrap();
}

#[test]
fn select_series_time_range_segment_prune_preserves_tombstone_exactness() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "tombstoned")];
    let recent_segments = 40usize;

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

        storage
            .insert_rows(&[Row::with_labels(
                "cpu",
                labels.clone(),
                DataPoint::new(10, 1.0),
            )])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);

        for segment_offset in 0..recent_segments {
            storage
                .insert_rows(&[Row::with_labels(
                    "cpu",
                    labels.clone(),
                    DataPoint::new(
                        1_000 + (segment_offset as i64 * 10),
                        2.0 + segment_offset as f64,
                    ),
                )])
                .unwrap();
            storage.flush_all_active().unwrap();
            assert!(storage.persist_segment_with_outcome().unwrap().persisted);
        }

        storage.close().unwrap();
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);
    reopened
        .delete_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::equal("host", "tombstoned"))
                .with_time_range(0, 20),
        )
        .unwrap();

    let segment_prunes = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_time_range_segment_prune_hook({
        let segment_prunes = Arc::clone(&segment_prunes);
        move || {
            segment_prunes.fetch_add(1, Ordering::SeqCst);
        }
    });
    let persisted_exact_scans = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_time_range_persisted_exact_scan_hook({
        let persisted_exact_scans = Arc::clone(&persisted_exact_scans);
        move || {
            persisted_exact_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    assert!(reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(0, 20),
        )
        .unwrap()
        .is_empty());
    assert!(
        segment_prunes.load(Ordering::SeqCst) > 0,
        "segment pruning should still run before persisted exact scans for tombstoned history (segment_prunes={}, exact_scans={})",
        segment_prunes.load(Ordering::SeqCst),
        persisted_exact_scans.load(Ordering::SeqCst),
    );
    assert_eq!(
        persisted_exact_scans.load(Ordering::SeqCst),
        1,
        "series present in overlapping persisted segments must still take the exact tombstone-aware path",
    );

    reopened.clear_metadata_time_range_segment_prune_hook();
    reopened.clear_metadata_time_range_persisted_exact_scan_hook();
    reopened.close().unwrap();
}

#[test]
fn select_series_time_range_segment_prune_respects_retention_cutoff() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let total_series = 64usize;
    let matching_series = 4usize;
    let query_now = 2_000;
    let retention = Duration::from_secs(1_000);

    {
        let storage = live_numeric_storage(
            temp_dir.path(),
            TimestampPrecision::Seconds,
            1,
            query_now,
            retention,
            None,
        );

        let retained_rows = (0..matching_series)
            .map(|series_idx| {
                Row::with_labels(
                    "cpu",
                    vec![Label::new("host", format!("host-{series_idx:04}"))],
                    DataPoint::new(1_004, series_idx as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&retained_rows).unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);

        for segment_offset in 0..40usize {
            let ts = 1_015 + (segment_offset as i64 * 10);
            let rows = (0..total_series)
                .map(|series_idx| {
                    Row::with_labels(
                        "cpu",
                        vec![Label::new("host", format!("host-{series_idx:04}"))],
                        DataPoint::new(ts, series_idx as f64),
                    )
                })
                .collect::<Vec<_>>();
            storage.insert_rows(&rows).unwrap();
            storage.flush_all_active().unwrap();
            assert!(storage.persist_segment_with_outcome().unwrap().persisted);
        }

        storage.close().unwrap();
    }

    let reopened = live_numeric_storage(
        temp_dir.path(),
        TimestampPrecision::Seconds,
        1,
        query_now,
        retention,
        None,
    );
    reopened.load_tombstones_index().unwrap();
    reopened
        .apply_loaded_segment_indexes(
            load_segment_indexes(temp_dir.path().join(NUMERIC_LANE_ROOT)).unwrap(),
            false,
        )
        .unwrap();
    reopened.reconcile_live_metadata_indexes().unwrap();

    let segment_prunes = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_time_range_segment_prune_hook({
        let segment_prunes = Arc::clone(&segment_prunes);
        move || {
            segment_prunes.fetch_add(1, Ordering::SeqCst);
        }
    });
    let persisted_exact_scans = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_time_range_persisted_exact_scan_hook({
        let persisted_exact_scans = Arc::clone(&persisted_exact_scans);
        move || {
            persisted_exact_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let mut selected_hosts = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(0, 1_005),
        )
        .unwrap()
        .into_iter()
        .map(|series| {
            series
                .labels
                .into_iter()
                .find(|label| label.name == "host")
                .unwrap()
                .value
        })
        .collect::<Vec<_>>();
    selected_hosts.sort();

    let expected_hosts = (0..matching_series)
        .map(|series_idx| format!("host-{series_idx:04}"))
        .collect::<Vec<_>>();
    let effective_start = query_now - retention.as_secs() as i64;
    let exact_scan_candidates = reopened
        .visibility
        .series_visibility_summaries
        .read()
        .values()
        .filter(|summary| summary_needs_exact_scan(summary, effective_start, 1_005))
        .count();
    assert_eq!(selected_hosts, expected_hosts);
    assert!(
        segment_prunes.load(Ordering::SeqCst) > 0,
        "retention-aware time-range selection should prune persisted segments using the effective retained range (segment_prunes={}, exact_scans={})",
        segment_prunes.load(Ordering::SeqCst),
        persisted_exact_scans.load(Ordering::SeqCst),
    );
    assert!(
        persisted_exact_scans.load(Ordering::SeqCst) < exact_scan_candidates,
        "retention pruning should reduce persisted exact scans below the pre-prune exact-scan candidate count (candidates={}, exact_scans={})",
        exact_scan_candidates,
        persisted_exact_scans.load(Ordering::SeqCst),
    );

    reopened.clear_metadata_time_range_segment_prune_hook();
    reopened.clear_metadata_time_range_persisted_exact_scan_hook();
    reopened.close().unwrap();
}

#[test]
fn select_series_time_range_bucket_prunes_truncated_gap_history_at_high_cardinality() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let total_series = 128usize;
    let matching_series = 4usize;
    let history_segments = 40usize;

    {
        let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

        for segment_offset in 0..history_segments {
            let recent_ts = 1_000 + (segment_offset as i64 * 10);
            let mut rows = Vec::with_capacity(
                total_series
                    .saturating_mul(2)
                    .saturating_add(matching_series),
            );
            for series_idx in 0..total_series {
                let labels = vec![Label::new("host", format!("host-{series_idx:04}"))];
                rows.push(Row::with_labels(
                    "cpu",
                    labels.clone(),
                    DataPoint::new(10, series_idx as f64),
                ));
                rows.push(Row::with_labels(
                    "cpu",
                    labels.clone(),
                    DataPoint::new(recent_ts, series_idx as f64),
                ));
                if series_idx < matching_series {
                    rows.push(Row::with_labels(
                        "cpu",
                        labels,
                        DataPoint::new(500, series_idx as f64),
                    ));
                }
            }

            storage.insert_rows(&rows).unwrap();
            storage.flush_all_active().unwrap();
            assert!(storage.persist_segment_with_outcome().unwrap().persisted);
        }

        storage.close().unwrap();
    }

    let reopened =
        reopen_persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);

    let segment_prunes = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_time_range_segment_prune_hook({
        let segment_prunes = Arc::clone(&segment_prunes);
        move || {
            segment_prunes.fetch_add(1, Ordering::SeqCst);
        }
    });
    let persisted_exact_scans = Arc::new(AtomicUsize::new(0));
    reopened.set_metadata_time_range_persisted_exact_scan_hook({
        let persisted_exact_scans = Arc::clone(&persisted_exact_scans);
        move || {
            persisted_exact_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let mut selected_hosts = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_time_range(500, 520),
        )
        .unwrap()
        .into_iter()
        .map(|series| {
            series
                .labels
                .into_iter()
                .find(|label| label.name == "host")
                .unwrap()
                .value
        })
        .collect::<Vec<_>>();
    selected_hosts.sort();

    let expected_hosts = (0..matching_series)
        .map(|series_idx| format!("host-{series_idx:04}"))
        .collect::<Vec<_>>();
    let exact_scan_candidates = reopened
        .visibility
        .series_visibility_summaries
        .read()
        .values()
        .filter(|summary| summary_needs_exact_scan(summary, 500, 520))
        .count();
    assert_eq!(selected_hosts, expected_hosts);
    assert_eq!(
        exact_scan_candidates, total_series,
        "all series should still require persisted exact scans before segment bucket pruning runs",
    );
    assert!(
        segment_prunes.load(Ordering::SeqCst) > 0,
        "truncated gap history should still consult persisted segment pruning before exact scans (segment_prunes={}, exact_scans={})",
        segment_prunes.load(Ordering::SeqCst),
        persisted_exact_scans.load(Ordering::SeqCst),
    );
    assert_eq!(
        persisted_exact_scans.load(Ordering::SeqCst),
        matching_series,
        "segment bucket pruning should bound persisted exact scans to the series that actually have a chunk in the narrow query window",
    );

    reopened.clear_metadata_time_range_segment_prune_hook();
    reopened.clear_metadata_time_range_persisted_exact_scan_hook();
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

#[test]
fn shard_scoped_metadata_apis_match_full_metadata_filtering() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let shard_count = 8;
    let target_shard = 3;

    let target_labels = labels_for_shard("cpu", shard_count, target_shard, "target");
    let late_target_labels = labels_for_shard("cpu", shard_count, target_shard, "late");
    let other_labels = labels_not_in_shard("cpu", shard_count, target_shard, "other");

    let storage = persistent_numeric_storage_with_metadata_shards(
        temp_dir.path(),
        TimestampPrecision::Seconds,
        2,
        shard_count,
    );

    storage
        .insert_rows(&[
            Row::with_labels("cpu", target_labels.clone(), DataPoint::new(10, 1.0)),
            Row::with_labels("cpu", late_target_labels.clone(), DataPoint::new(200, 4.0)),
            Row::with_labels("cpu", other_labels.clone(), DataPoint::new(20, 2.0)),
            Row::with_labels("memory", target_labels.clone(), DataPoint::new(30, 3.0)),
        ])
        .unwrap();

    let fallback_scans = Arc::new(AtomicUsize::new(0));
    storage.set_shard_metadata_fallback_scan_hook({
        let fallback_scans = Arc::clone(&fallback_scans);
        move || {
            fallback_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let scope = MetadataShardScope::new(shard_count, vec![target_shard]);

    let expected_metrics = storage
        .list_metrics()
        .unwrap()
        .into_iter()
        .filter(|series| metric_series_matches_shard_scope(series, &scope))
        .collect::<Vec<_>>();
    let scoped_metrics = storage.list_metrics_in_shards(&scope).unwrap();
    assert_eq!(scoped_metrics, expected_metrics);

    let selection = SeriesSelection::new().with_metric("cpu");
    let expected_series = storage
        .select_series(&selection)
        .unwrap()
        .into_iter()
        .filter(|series| metric_series_matches_shard_scope(series, &scope))
        .collect::<Vec<_>>();
    let scoped_series = storage.select_series_in_shards(&selection, &scope).unwrap();
    assert_eq!(scoped_series, expected_series);

    let time_filtered_selection = SeriesSelection::new()
        .with_metric("cpu")
        .with_time_range(0, 100);
    let expected_time_filtered_series = storage
        .select_series(&time_filtered_selection)
        .unwrap()
        .into_iter()
        .filter(|series| metric_series_matches_shard_scope(series, &scope))
        .collect::<Vec<_>>();
    let scoped_time_filtered_series = storage
        .select_series_in_shards(&time_filtered_selection, &scope)
        .unwrap();
    assert_eq!(scoped_time_filtered_series, expected_time_filtered_series);

    assert_eq!(
        fallback_scans.load(Ordering::SeqCst),
        0,
        "exact indexed shard scopes should not fall back to a full metadata scan",
    );

    storage.clear_shard_metadata_fallback_scan_hook();
}

#[test]
fn shard_scoped_broad_matchers_match_full_metadata_filtering_without_fallback_scan() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let shard_count = 8;
    let target_shard = 3;

    let target_job_08 = labels_for_shard_with_job(
        "cpu",
        shard_count,
        target_shard,
        "target-job-08",
        Some("job-08"),
    );
    let target_job_09 = labels_for_shard_with_job(
        "cpu",
        shard_count,
        target_shard,
        "target-job-09",
        Some("job-09"),
    );
    let target_job_03 = labels_for_shard_with_job(
        "cpu",
        shard_count,
        target_shard,
        "target-job-03",
        Some("job-03"),
    );
    let target_missing =
        labels_for_shard_with_job("cpu", shard_count, target_shard, "target-missing", None);
    let other_job_08 = labels_not_in_shard_with_job(
        "cpu",
        shard_count,
        target_shard,
        "other-job-08",
        Some("job-08"),
    );
    let other_missing =
        labels_not_in_shard_with_job("cpu", shard_count, target_shard, "other-missing", None);

    let storage = persistent_numeric_storage_with_metadata_shards(
        temp_dir.path(),
        TimestampPrecision::Seconds,
        2,
        shard_count,
    );

    storage
        .insert_rows(&[
            Row::with_labels("cpu", target_job_08, DataPoint::new(10, 1.0)),
            Row::with_labels("cpu", target_job_09, DataPoint::new(11, 2.0)),
            Row::with_labels("cpu", target_job_03, DataPoint::new(12, 3.0)),
            Row::with_labels("cpu", target_missing, DataPoint::new(13, 4.0)),
            Row::with_labels("cpu", other_job_08, DataPoint::new(14, 5.0)),
            Row::with_labels("cpu", other_missing, DataPoint::new(15, 6.0)),
        ])
        .unwrap();

    let fallback_scans = Arc::new(AtomicUsize::new(0));
    storage.set_shard_metadata_fallback_scan_hook({
        let fallback_scans = Arc::clone(&fallback_scans);
        move || {
            fallback_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let scope = MetadataShardScope::new(shard_count, vec![target_shard]);
    let regex_selection = SeriesSelection::new()
        .with_metric("cpu")
        .with_matcher(SeriesMatcher::regex_match("job", "job-(08|09)"));
    let empty_regex_selection = SeriesSelection::new()
        .with_metric("cpu")
        .with_matcher(SeriesMatcher::regex_match("job", ".*"));
    let negative_selection = SeriesSelection::new()
        .with_metric("cpu")
        .with_matcher(SeriesMatcher::regex_no_match("job", "job-(03|04)"));

    let expected_regex = storage
        .select_series(&regex_selection)
        .unwrap()
        .into_iter()
        .filter(|series| metric_series_matches_shard_scope(series, &scope))
        .collect::<Vec<_>>();
    let expected_empty_regex = storage
        .select_series(&empty_regex_selection)
        .unwrap()
        .into_iter()
        .filter(|series| metric_series_matches_shard_scope(series, &scope))
        .collect::<Vec<_>>();
    let expected_negative = storage
        .select_series(&negative_selection)
        .unwrap()
        .into_iter()
        .filter(|series| metric_series_matches_shard_scope(series, &scope))
        .collect::<Vec<_>>();

    assert_eq!(
        storage
            .select_series_in_shards(&regex_selection, &scope)
            .unwrap(),
        expected_regex,
    );
    assert_eq!(
        storage
            .select_series_in_shards(&empty_regex_selection, &scope)
            .unwrap(),
        expected_empty_regex,
    );
    assert_eq!(
        storage
            .select_series_in_shards(&negative_selection, &scope)
            .unwrap(),
        expected_negative,
    );
    assert_eq!(
        fallback_scans.load(Ordering::SeqCst),
        0,
        "broad shard-scoped selectors should remain on indexed metadata paths",
    );

    storage.clear_shard_metadata_fallback_scan_hook();
}

#[test]
fn shard_scoped_candidate_planning_prunes_deleted_scope_series_before_bounded_regex_scan() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let shard_count = 8;
    let target_shard = 3;
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        None,
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            metadata_shard_count: Some(shard_count),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    let retained_labels =
        labels_for_shard_with_job("cpu", shard_count, target_shard, "kept", Some("job-live"));
    let excluded_labels = labels_for_shard_with_job(
        "cpu",
        shard_count,
        target_shard,
        "excluded",
        Some("job-1000"),
    );
    let deleted_labels = labels_for_shard_with_job(
        "cpu",
        shard_count,
        target_shard,
        "deleted",
        Some("job-live"),
    );
    let off_scope_labels =
        labels_not_in_shard_with_job("cpu", shard_count, target_shard, "other", Some("job-live"));
    let scope = MetadataShardScope::new(shard_count, vec![target_shard]);

    storage
        .insert_rows(&[
            Row::with_labels("cpu", retained_labels.clone(), DataPoint::new(1_000, 1.0)),
            Row::with_labels("cpu", excluded_labels.clone(), DataPoint::new(1_001, 2.0)),
            Row::with_labels("cpu", deleted_labels.clone(), DataPoint::new(1_002, 3.0)),
            Row::with_labels("cpu", off_scope_labels, DataPoint::new(1_003, 4.0)),
        ])
        .unwrap();
    storage
        .delete_series(&SeriesSelection::new().with_metric("cpu").with_matcher(
            SeriesMatcher::equal("host", deleted_labels[0].value.clone()),
        ))
        .unwrap();

    let direct_candidate_scans = Arc::new(AtomicUsize::new(0));
    let all_series_seeds = Arc::new(AtomicUsize::new(0));
    let live_series_snapshots = Arc::new(AtomicUsize::new(0));
    storage.set_metadata_direct_candidate_scan_hook({
        let direct_candidate_scans = Arc::clone(&direct_candidate_scans);
        move || {
            direct_candidate_scans.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_all_series_seed_hook({
        let all_series_seeds = Arc::clone(&all_series_seeds);
        move || {
            all_series_seeds.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage.set_metadata_live_series_snapshot_hook({
        let live_series_snapshots = Arc::clone(&live_series_snapshots);
        move || {
            live_series_snapshots.fetch_add(1, Ordering::SeqCst);
        }
    });

    let selected = storage
        .select_series_in_shards(
            &SeriesSelection::new()
                .with_metric("cpu")
                .with_matcher(SeriesMatcher::regex_no_match("job", "job-1[0-9][0-9][0-9]")),
            &scope,
        )
        .unwrap();

    assert_eq!(
        selected,
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels: retained_labels,
        }]
    );
    assert!(
        direct_candidate_scans.load(Ordering::SeqCst) > 0,
        "shard-scoped broad selectors should still use bounded candidate scans",
    );
    assert_eq!(
        all_series_seeds.load(Ordering::SeqCst),
        0,
        "deleted shard-scoped series should be pruned from the scope instead of forcing a global metadata seed",
    );
    assert_eq!(
        live_series_snapshots.load(Ordering::SeqCst),
        0,
        "deleted shard-scoped series should not force a full live-series snapshot",
    );

    storage.clear_metadata_direct_candidate_scan_hook();
    storage.clear_metadata_all_series_seed_hook();
    storage.clear_metadata_live_series_snapshot_hook();
}

#[test]
fn empty_indexed_shard_scope_returns_empty_without_fallback_scan() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let shard_count = 8;
    let target_shard = 3;
    let other_labels = labels_not_in_shard("cpu", shard_count, target_shard, "other");

    let storage = persistent_numeric_storage_with_metadata_shards(
        temp_dir.path(),
        TimestampPrecision::Seconds,
        2,
        shard_count,
    );

    storage
        .insert_rows(&[
            Row::with_labels("cpu", other_labels.clone(), DataPoint::new(10, 1.0)),
            Row::with_labels("memory", other_labels, DataPoint::new(20, 2.0)),
        ])
        .unwrap();

    let fallback_scans = Arc::new(AtomicUsize::new(0));
    storage.set_shard_metadata_fallback_scan_hook({
        let fallback_scans = Arc::clone(&fallback_scans);
        move || {
            fallback_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let scope = MetadataShardScope::new(shard_count, vec![target_shard]);
    assert!(storage.list_metrics_in_shards(&scope).unwrap().is_empty());
    assert!(storage
        .select_series_in_shards(&SeriesSelection::new().with_metric("cpu"), &scope)
        .unwrap()
        .is_empty());
    assert_eq!(
        fallback_scans.load(Ordering::SeqCst),
        0,
        "empty indexed shard scopes should short-circuit instead of scanning all metadata",
    );

    storage.clear_shard_metadata_fallback_scan_hook();
}

#[test]
fn mismatched_shard_count_scopes_return_unsupported_without_fallback_scan() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let indexed_shard_count = 8;
    let requested_shard_count = 16;
    let target_shard = 5;
    let target_labels = labels_for_shard("cpu", requested_shard_count, target_shard, "target");

    let storage = persistent_numeric_storage_with_metadata_shards(
        temp_dir.path(),
        TimestampPrecision::Seconds,
        2,
        indexed_shard_count,
    );

    storage
        .insert_rows(&[Row::with_labels(
            "cpu",
            target_labels.clone(),
            DataPoint::new(10, 1.0),
        )])
        .unwrap();

    let fallback_scans = Arc::new(AtomicUsize::new(0));
    storage.set_shard_metadata_fallback_scan_hook({
        let fallback_scans = Arc::clone(&fallback_scans);
        move || {
            fallback_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let scope = MetadataShardScope::new(requested_shard_count, vec![target_shard]);
    let list_err = storage.list_metrics_in_shards(&scope).unwrap_err();
    assert!(matches!(
        list_err,
        TsinkError::UnsupportedOperation {
            operation: "list_metrics_in_shards",
            ..
        }
    ));
    assert!(list_err
        .to_string()
        .contains("requested shard_count 16 to match indexed shard_count 8"));

    let select_err = storage
        .select_series_in_shards(&SeriesSelection::new().with_metric("cpu"), &scope)
        .unwrap_err();
    assert!(matches!(
        select_err,
        TsinkError::UnsupportedOperation {
            operation: "select_series_in_shards",
            ..
        }
    ));
    assert!(select_err
        .to_string()
        .contains("requested shard_count 16 to match indexed shard_count 8"));

    assert_eq!(
        fallback_scans.load(Ordering::SeqCst),
        0,
        "mismatched shard geometry should return an explicit bounded error instead of scanning all metadata",
    );

    storage.clear_shard_metadata_fallback_scan_hook();
}

#[test]
fn disabled_shard_index_returns_unsupported_without_fallback_scan() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let shard_count = 8;
    let target_shard = 3;
    let target_labels = labels_for_shard("cpu", shard_count, target_shard, "target");

    let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 2);
    storage
        .insert_rows(&[Row::with_labels(
            "cpu",
            target_labels,
            DataPoint::new(10, 1.0),
        )])
        .unwrap();

    let fallback_scans = Arc::new(AtomicUsize::new(0));
    storage.set_shard_metadata_fallback_scan_hook({
        let fallback_scans = Arc::clone(&fallback_scans);
        move || {
            fallback_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let scope = MetadataShardScope::new(shard_count, vec![target_shard]);

    let list_err = storage.list_metrics_in_shards(&scope).unwrap_err();
    assert!(matches!(
        list_err,
        TsinkError::UnsupportedOperation {
            operation: "list_metrics_in_shards",
            ..
        }
    ));
    assert!(list_err
        .to_string()
        .contains("metadata shard indexing to be enabled"));

    let select_err = storage
        .select_series_in_shards(&SeriesSelection::new().with_metric("cpu"), &scope)
        .unwrap_err();
    assert!(matches!(
        select_err,
        TsinkError::UnsupportedOperation {
            operation: "select_series_in_shards",
            ..
        }
    ));
    assert!(select_err
        .to_string()
        .contains("metadata shard indexing to be enabled"));

    assert_eq!(
        fallback_scans.load(Ordering::SeqCst),
        0,
        "disabled shard indexing should return an explicit bounded error instead of scanning all metadata",
    );

    storage.clear_shard_metadata_fallback_scan_hook();
}

#[test]
fn retention_sweep_reconciles_live_metadata_indexes_and_shard_scoped_reads() {
    let temp_dir = TempDir::new().unwrap();
    let shard_count = 8;
    let target_shard = 3;
    let expired_labels = labels_for_shard("cpu", shard_count, target_shard, "expired");
    let live_labels = labels_not_in_shard("cpu", shard_count, target_shard, "live");
    let scope = MetadataShardScope::new(shard_count, vec![target_shard]);

    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        None,
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: 5,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            retention_enforced: true,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: false,
            background_fail_fast: false,
            metadata_shard_count: Some(shard_count),
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: Duration::from_secs(5),
            tiered_storage: None,
            #[cfg(test)]
            current_time_override: Some(10),
        },
    )
    .unwrap();

    storage
        .insert_rows(&[Row::with_labels(
            "cpu",
            expired_labels.clone(),
            DataPoint::new(10, 1.0),
        )])
        .unwrap();
    storage.flush_all_active().unwrap();
    let expired_persist = storage.persist_segment_with_outcome().unwrap();
    assert!(expired_persist.persisted);

    storage.set_current_time_override(40);
    storage
        .insert_rows(&[Row::with_labels(
            "cpu",
            live_labels.clone(),
            DataPoint::new(40, 2.0),
        )])
        .unwrap();

    storage.flush_all_active().unwrap();
    let persist_outcome = storage.persist_segment_with_outcome().unwrap();
    assert!(persist_outcome.persisted);

    assert_eq!(storage.materialized_series_snapshot().len(), 2);
    assert_eq!(storage.sweep_expired_persisted_segments().unwrap(), 1);
    assert_eq!(storage.materialized_series_snapshot().len(), 1);

    assert_eq!(
        storage.list_metrics().unwrap(),
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels: live_labels.clone(),
        }]
    );
    assert_eq!(
        storage
            .select_series(&SeriesSelection::new().with_metric("cpu"))
            .unwrap(),
        vec![MetricSeries {
            name: "cpu".to_string(),
            labels: live_labels,
        }]
    );
    assert!(storage.list_metrics_in_shards(&scope).unwrap().is_empty());
    assert!(storage
        .select_series_in_shards(&SeriesSelection::new().with_metric("cpu"), &scope)
        .unwrap()
        .is_empty());
}

#[test]
fn compute_shard_window_digest_scopes_to_target_shard_and_time_window() {
    let temp_dir = TempDir::new().unwrap();
    let shard_count = 8;
    let target_shard = 3;
    let target_labels = labels_for_shard("cpu", shard_count, target_shard, "target-a");
    let target_labels_outside_window =
        labels_for_shard("cpu", shard_count, target_shard, "target-b");
    let other_labels = labels_not_in_shard("cpu", shard_count, target_shard, "other");

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_metadata_shard_count(shard_count)
        .with_current_time_override_for_tests(300)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels("cpu", target_labels.clone(), DataPoint::new(10, 1.0)),
            Row::with_labels("cpu", target_labels.clone(), DataPoint::new(20, 2.0)),
            Row::with_labels(
                "cpu",
                target_labels_outside_window.clone(),
                DataPoint::new(200, 3.0),
            ),
            Row::with_labels("cpu", other_labels.clone(), DataPoint::new(15, 4.0)),
        ])
        .unwrap();

    let digest_a = storage
        .compute_shard_window_digest(target_shard, shard_count, 0, 100)
        .unwrap();
    let digest_b = storage
        .compute_shard_window_digest(target_shard, shard_count, 0, 100)
        .unwrap();

    assert_eq!(digest_a, digest_b);
    assert_eq!(digest_a.series_count, 1);
    assert_eq!(digest_a.point_count, 2);
}

#[test]
fn shard_window_public_reads_record_query_observability_for_persisted_reads() {
    let temp_dir = TempDir::new().unwrap();
    let shard_count = 8;
    let target_shard = 3;
    let target_a = labels_for_shard("cpu", shard_count, target_shard, "target-a");
    let target_b = labels_for_shard("cpu", shard_count, target_shard, "target-b");
    let other = labels_not_in_shard("cpu", shard_count, target_shard, "other");
    let storage = persistent_numeric_storage_with_metadata_shards(
        temp_dir.path(),
        TimestampPrecision::Seconds,
        1,
        shard_count,
    );

    for (labels, timestamp, value) in [
        (target_a.clone(), 10, 1.0),
        (target_b.clone(), 20, 2.0),
        (other, 15, 3.0),
    ] {
        storage
            .insert_rows(&[Row::with_labels(
                "cpu",
                labels,
                DataPoint::new(timestamp, value),
            )])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
    }

    let before_digest = storage.observability_snapshot();
    let digest = storage
        .compute_shard_window_digest(target_shard, shard_count, 0, 100)
        .unwrap();
    let after_digest = storage.observability_snapshot();

    assert_eq!(digest.series_count, 2);
    assert_eq!(digest.point_count, 2);
    assert_eq!(
        after_digest.query.hot_only_query_plans_total,
        before_digest.query.hot_only_query_plans_total + 1,
    );
    assert_eq!(
        after_digest.query.hot_tier_persisted_chunks_read_total,
        before_digest.query.hot_tier_persisted_chunks_read_total + 2,
    );
    assert_eq!(
        after_digest.query.warm_tier_persisted_chunks_read_total,
        before_digest.query.warm_tier_persisted_chunks_read_total,
    );
    assert_eq!(
        after_digest.query.cold_tier_persisted_chunks_read_total,
        before_digest.query.cold_tier_persisted_chunks_read_total,
    );

    let before_scan = after_digest;
    let page = storage
        .scan_shard_window_rows(
            target_shard,
            shard_count,
            0,
            100,
            ShardWindowScanOptions::default(),
        )
        .unwrap();
    let after_scan = storage.observability_snapshot();

    assert_eq!(page.series_scanned, 2);
    assert_eq!(page.rows_scanned, 2);
    assert!(!page.truncated);
    assert_eq!(page.rows.len(), 2);
    assert!(page
        .rows
        .iter()
        .any(|row| row.labels() == target_a.as_slice()));
    assert!(page
        .rows
        .iter()
        .any(|row| row.labels() == target_b.as_slice()));
    assert_eq!(
        after_scan.query.hot_only_query_plans_total,
        before_scan.query.hot_only_query_plans_total + 1,
    );
    assert_eq!(
        after_scan.query.hot_tier_persisted_chunks_read_total,
        before_scan.query.hot_tier_persisted_chunks_read_total + 2,
    );
    assert_eq!(
        after_scan.query.warm_tier_persisted_chunks_read_total,
        before_scan.query.warm_tier_persisted_chunks_read_total,
    );
    assert_eq!(
        after_scan.query.cold_tier_persisted_chunks_read_total,
        before_scan.query.cold_tier_persisted_chunks_read_total,
    );

    let _ = storage.close();
}

#[test]
fn scan_shard_window_rows_paginates_deterministically_with_row_offset() {
    let temp_dir = TempDir::new().unwrap();
    let shard_count = 8;
    let target_shard = 3;
    let series_a = labels_for_shard("cpu", shard_count, target_shard, "a");
    let series_b = labels_for_shard("cpu", shard_count, target_shard, "b");

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_metadata_shard_count(shard_count)
        .with_current_time_override_for_tests(300)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels("cpu", series_a.clone(), DataPoint::new(20, 1.0)),
            Row::with_labels("cpu", series_b.clone(), DataPoint::new(10, 2.0)),
            Row::with_labels("cpu", series_b.clone(), DataPoint::new(30, 3.0)),
        ])
        .unwrap();

    let first_page = storage
        .scan_shard_window_rows(
            target_shard,
            shard_count,
            0,
            100,
            ShardWindowScanOptions {
                max_rows: Some(2),
                ..ShardWindowScanOptions::default()
            },
        )
        .unwrap();
    assert_eq!(first_page.series_scanned, 2);
    assert_eq!(first_page.rows_scanned, 2);
    assert!(first_page.truncated);
    let next_offset = first_page.next_row_offset.unwrap();
    assert_eq!(first_page.rows.len(), 2);
    assert_eq!(first_page.rows[0].metric(), "cpu");
    assert_eq!(first_page.rows[0].labels(), series_a.as_slice());
    assert_eq!(first_page.rows[0].data_point(), &DataPoint::new(20, 1.0));
    assert_eq!(first_page.rows[1].labels(), series_b.as_slice());
    assert_eq!(first_page.rows[1].data_point(), &DataPoint::new(10, 2.0));

    let second_page = storage
        .scan_shard_window_rows(
            target_shard,
            shard_count,
            0,
            100,
            ShardWindowScanOptions {
                row_offset: Some(next_offset),
                ..ShardWindowScanOptions::default()
            },
        )
        .unwrap();
    assert_eq!(second_page.series_scanned, 1);
    assert_eq!(second_page.rows_scanned, 1);
    assert!(!second_page.truncated);
    assert_eq!(second_page.next_row_offset, None);
    assert_eq!(second_page.rows.len(), 1);
    assert_eq!(second_page.rows[0].labels(), series_b.as_slice());
    assert_eq!(second_page.rows[0].data_point(), &DataPoint::new(30, 3.0));
}

#[test]
fn shard_window_reads_are_stable_for_delimiter_collision_series_across_insert_orders() {
    let left_labels = vec![Label::new("job", "api,zone=west|prod\u{1f}blue")];
    let right_labels = vec![
        Label::new("job", "api"),
        Label::new("zone", "west|prod\u{1f}blue"),
    ];

    let build_storage = |path| {
        StorageBuilder::new()
            .with_data_path(path)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_metadata_shard_count(1)
            .with_current_time_override_for_tests(300)
            .build()
            .unwrap()
    };

    let temp_dir_a = TempDir::new().unwrap();
    let temp_dir_b = TempDir::new().unwrap();
    let storage_a = build_storage(temp_dir_a.path());
    let storage_b = build_storage(temp_dir_b.path());

    storage_a
        .insert_rows(&[
            Row::with_labels("cpu", left_labels.clone(), DataPoint::new(10, 1.0)),
            Row::with_labels("cpu", right_labels.clone(), DataPoint::new(20, 2.0)),
        ])
        .unwrap();
    storage_b
        .insert_rows(&[
            Row::with_labels("cpu", right_labels.clone(), DataPoint::new(20, 2.0)),
            Row::with_labels("cpu", left_labels.clone(), DataPoint::new(10, 1.0)),
        ])
        .unwrap();

    let digest_a = storage_a.compute_shard_window_digest(0, 1, 0, 100).unwrap();
    let digest_b = storage_b.compute_shard_window_digest(0, 1, 0, 100).unwrap();
    assert_eq!(digest_a, digest_b);

    let page_a = storage_a
        .scan_shard_window_rows(0, 1, 0, 100, ShardWindowScanOptions::default())
        .unwrap();
    let page_b = storage_b
        .scan_shard_window_rows(0, 1, 0, 100, ShardWindowScanOptions::default())
        .unwrap();

    let snapshot = |page: &crate::ShardWindowRowsPage| {
        page.rows
            .iter()
            .map(|row| {
                (
                    row.metric().to_string(),
                    row.labels().to_vec(),
                    row.data_point().clone(),
                )
            })
            .collect::<Vec<_>>()
    };

    assert_eq!(page_a.series_scanned, 2);
    assert_eq!(page_b.series_scanned, 2);
    assert_eq!(snapshot(&page_a), snapshot(&page_b));
}

#[test]
fn select_many_returns_points_in_request_order() {
    let storage = ChunkStorage::new(2, None);
    let series_a = MetricSeries {
        name: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    };
    let series_b = MetricSeries {
        name: "cpu".to_string(),
        labels: vec![Label::new("host", "b")],
    };
    let missing = MetricSeries {
        name: "cpu".to_string(),
        labels: vec![Label::new("host", "missing")],
    };

    storage
        .insert_rows(&[
            Row::with_labels(
                series_a.name.clone(),
                series_a.labels.clone(),
                DataPoint::new(10, 1.0),
            ),
            Row::with_labels(
                series_b.name.clone(),
                series_b.labels.clone(),
                DataPoint::new(20, 2.0),
            ),
        ])
        .unwrap();

    let selected = storage
        .select_many(
            &[series_b.clone(), missing.clone(), series_a.clone()],
            0,
            30,
        )
        .unwrap();

    assert_eq!(
        selected,
        vec![
            SeriesPoints {
                series: series_b,
                points: vec![DataPoint::new(20, 2.0)],
            },
            SeriesPoints {
                series: missing,
                points: Vec::new(),
            },
            SeriesPoints {
                series: series_a,
                points: vec![DataPoint::new(10, 1.0)],
            },
        ]
    );
}

#[test]
fn select_into_and_select_many_record_query_observability_for_persisted_reads() {
    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);
    let series_a = MetricSeries {
        name: "cpu".to_string(),
        labels: vec![Label::new("host", "a")],
    };
    let series_b = MetricSeries {
        name: "cpu".to_string(),
        labels: vec![Label::new("host", "b")],
    };
    let missing = MetricSeries {
        name: "cpu".to_string(),
        labels: vec![Label::new("host", "missing")],
    };

    for (series, timestamp, value) in [(series_a.clone(), 10, 1.0), (series_b.clone(), 20, 2.0)] {
        storage
            .insert_rows(&[Row::with_labels(
                series.name,
                series.labels,
                DataPoint::new(timestamp, value),
            )])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
    }

    let before_select_into = storage.observability_snapshot();
    let mut out = vec![DataPoint::new(-1, -1.0)];
    storage
        .select_into("cpu", &series_a.labels, 0, 100, &mut out)
        .unwrap();
    let after_select_into = storage.observability_snapshot();

    assert_eq!(out, vec![DataPoint::new(10, 1.0)]);
    assert_eq!(
        after_select_into.query.hot_only_query_plans_total,
        before_select_into.query.hot_only_query_plans_total + 1,
    );
    assert_eq!(
        after_select_into.query.hot_tier_persisted_chunks_read_total,
        before_select_into
            .query
            .hot_tier_persisted_chunks_read_total
            + 1,
    );
    assert_eq!(
        after_select_into
            .query
            .warm_tier_persisted_chunks_read_total,
        before_select_into
            .query
            .warm_tier_persisted_chunks_read_total,
    );
    assert_eq!(
        after_select_into
            .query
            .cold_tier_persisted_chunks_read_total,
        before_select_into
            .query
            .cold_tier_persisted_chunks_read_total,
    );

    let before_select_many = after_select_into;
    let selected = storage
        .select_many(
            &[series_b.clone(), missing.clone(), series_a.clone()],
            0,
            100,
        )
        .unwrap();
    let after_select_many = storage.observability_snapshot();

    assert_eq!(
        selected,
        vec![
            SeriesPoints {
                series: series_b,
                points: vec![DataPoint::new(20, 2.0)],
            },
            SeriesPoints {
                series: missing,
                points: Vec::new(),
            },
            SeriesPoints {
                series: series_a,
                points: vec![DataPoint::new(10, 1.0)],
            },
        ]
    );
    assert_eq!(
        after_select_many.query.hot_only_query_plans_total,
        before_select_many.query.hot_only_query_plans_total + 1,
    );
    assert_eq!(
        after_select_many.query.hot_tier_persisted_chunks_read_total,
        before_select_many
            .query
            .hot_tier_persisted_chunks_read_total
            + 2,
    );
    assert_eq!(
        after_select_many
            .query
            .warm_tier_persisted_chunks_read_total,
        before_select_many
            .query
            .warm_tier_persisted_chunks_read_total,
    );
    assert_eq!(
        after_select_many
            .query
            .cold_tier_persisted_chunks_read_total,
        before_select_many
            .query
            .cold_tier_persisted_chunks_read_total,
    );

    let _ = storage.close();
}

#[test]
fn select_with_options_zero_limit_records_query_plan_once() {
    let storage = ChunkStorage::new(2, None);
    let labels = vec![Label::new("host", "zero-limit")];
    storage
        .insert_rows(&[Row::with_labels(
            "cpu",
            labels.clone(),
            DataPoint::new(10, 1.0),
        )])
        .unwrap();

    let before = storage.observability_snapshot();
    let selected = storage
        .select_with_options(
            "cpu",
            QueryOptions::new(0, 100)
                .with_labels(labels)
                .with_pagination(0, Some(0)),
        )
        .unwrap();
    let after = storage.observability_snapshot();

    assert!(selected.is_empty());
    assert_eq!(
        after.query.select_with_options_calls_total,
        before.query.select_with_options_calls_total + 1,
    );
    assert_eq!(
        after.query.select_with_options_points_returned_total,
        before.query.select_with_options_points_returned_total,
    );
    assert_eq!(
        after.query.hot_only_query_plans_total,
        before.query.hot_only_query_plans_total + 1,
    );
    assert_eq!(
        after.query.warm_tier_query_plans_total,
        before.query.warm_tier_query_plans_total,
    );
    assert_eq!(
        after.query.cold_tier_query_plans_total,
        before.query.cold_tier_query_plans_total,
    );
}

#[test]
fn select_with_options_pagination_stops_before_decoding_full_persisted_history() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let total_points = 64usize;
    let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);
    for ts in 0..total_points {
        storage
            .insert_rows(&[Row::with_labels(
                "cpu",
                vec![Label::new("host", "paged")],
                DataPoint::new(ts as i64, ts as f64),
            )])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
    }

    assert_eq!(
        storage
            .select("cpu", &[Label::new("host", "paged")], 0, 1_000)
            .unwrap()
            .len(),
        total_points,
    );

    let persisted_decodes = Arc::new(AtomicUsize::new(0));
    storage.set_query_persisted_chunk_decode_hook({
        let persisted_decodes = Arc::clone(&persisted_decodes);
        move || {
            persisted_decodes.fetch_add(1, Ordering::SeqCst);
        }
    });

    let before = storage.observability_snapshot();
    let selected = storage
        .select_with_options(
            "cpu",
            QueryOptions::new(0, 1_000)
                .with_labels(vec![Label::new("host", "paged")])
                .with_pagination(10, Some(5)),
        )
        .unwrap();
    let after = storage.observability_snapshot();
    assert_eq!(
        selected,
        (10..15)
            .map(|ts| DataPoint::new(ts, ts as f64))
            .collect::<Vec<_>>()
    );
    assert!(
        persisted_decodes.load(Ordering::SeqCst) < total_points,
        "paginated select should stop before decoding every persisted chunk",
    );
    assert_eq!(
        after.query.select_with_options_calls_total,
        before.query.select_with_options_calls_total + 1,
    );
    assert_eq!(
        after.query.select_with_options_points_returned_total,
        before.query.select_with_options_points_returned_total + 5,
    );
    assert_eq!(
        after.query.hot_only_query_plans_total,
        before.query.hot_only_query_plans_total + 1,
    );
    assert_eq!(
        after.query.warm_tier_query_plans_total,
        before.query.warm_tier_query_plans_total,
    );
    assert_eq!(
        after.query.cold_tier_query_plans_total,
        before.query.cold_tier_query_plans_total,
    );

    storage.clear_query_persisted_chunk_decode_hook();
    let _ = storage.close();
}

#[test]
fn scan_metric_rows_paginates_without_materializing_every_metric_series() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let total_series = 12usize;
    let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);
    for series_idx in 0..total_series {
        storage
            .insert_rows(&[Row::with_labels(
                "cpu",
                vec![Label::new("host", format!("host-{series_idx:04}"))],
                DataPoint::new(10, series_idx as f64),
            )])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
    }

    let persisted_decodes = Arc::new(AtomicUsize::new(0));
    storage.set_query_persisted_chunk_decode_hook({
        let persisted_decodes = Arc::clone(&persisted_decodes);
        move || {
            persisted_decodes.fetch_add(1, Ordering::SeqCst);
        }
    });

    let page = storage
        .scan_metric_rows(
            "cpu",
            0,
            100,
            QueryRowsScanOptions {
                max_rows: Some(3),
                ..QueryRowsScanOptions::default()
            },
        )
        .unwrap();
    assert_eq!(page.rows_scanned, 3);
    assert!(page.truncated);
    assert_eq!(page.next_row_offset, Some(3));
    assert_eq!(page.rows.len(), 3);
    assert_eq!(page.rows[0].labels(), &[Label::new("host", "host-0000")]);
    assert_eq!(page.rows[1].labels(), &[Label::new("host", "host-0001")]);
    assert_eq!(page.rows[2].labels(), &[Label::new("host", "host-0002")]);
    assert!(
        persisted_decodes.load(Ordering::SeqCst) < total_series,
        "paged metric scans should stop before decoding every persisted series",
    );

    storage.clear_query_persisted_chunk_decode_hook();
    let _ = storage.close();
}

#[test]
fn scan_series_rows_paginates_in_request_order_without_full_selection_materialization() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let total_series = 8usize;
    let requested = [5usize, 2, 7, 1, 6];
    let storage = persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Seconds, 1);
    for series_idx in 0..total_series {
        storage
            .insert_rows(&[Row::with_labels(
                "cpu",
                vec![Label::new("host", format!("host-{series_idx:04}"))],
                DataPoint::new(10, series_idx as f64),
            )])
            .unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
    }

    let persisted_decodes = Arc::new(AtomicUsize::new(0));
    storage.set_query_persisted_chunk_decode_hook({
        let persisted_decodes = Arc::clone(&persisted_decodes);
        move || {
            persisted_decodes.fetch_add(1, Ordering::SeqCst);
        }
    });

    let request = requested
        .iter()
        .map(|series_idx| MetricSeries {
            name: "cpu".to_string(),
            labels: vec![Label::new("host", format!("host-{series_idx:04}"))],
        })
        .collect::<Vec<_>>();
    let page = storage
        .scan_series_rows(
            &request,
            0,
            100,
            QueryRowsScanOptions {
                max_rows: Some(2),
                ..QueryRowsScanOptions::default()
            },
        )
        .unwrap();

    assert_eq!(page.rows_scanned, 2);
    assert!(page.truncated);
    assert_eq!(page.next_row_offset, Some(2));
    assert_eq!(page.rows.len(), 2);
    assert_eq!(page.rows[0].labels(), &[Label::new("host", "host-0005")]);
    assert_eq!(page.rows[1].labels(), &[Label::new("host", "host-0002")]);
    assert!(
        persisted_decodes.load(Ordering::SeqCst) < request.len(),
        "paged series scans should stop before decoding the full selection",
    );

    storage.clear_query_persisted_chunk_decode_hook();
    let _ = storage.close();
}
