use tempfile::TempDir;

use super::super::{SeriesKey, SeriesRegistry, SeriesValueFamily};
use super::assert_memory_usage_reconciled;
use crate::Label;

#[test]
fn incremental_memory_usage_matches_recomputed_totals_across_mutation_and_reload_paths() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("series_index.bin");
    let registry = SeriesRegistry::new();

    let fresh = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a"), Label::new("job", "api")])
        .unwrap();
    registry
        .assign_series_value_family_if_missing(fresh.series_id, SeriesValueFamily::F64)
        .unwrap();
    assert_memory_usage_reconciled(&registry);

    let missing_job = registry
        .resolve_or_insert("cpu", &[Label::new("host", "b")])
        .unwrap();
    let job_name_id = registry.label_name_id("job").unwrap();
    let missing = registry.missing_label_postings_for_id(job_name_id);
    assert!(missing.contains(missing_job.series_id));
    assert_memory_usage_reconciled(&registry);

    let replayed = registry
        .register_series_with_id(42, "historical", &[Label::new("host", "archive")])
        .unwrap();
    assert!(replayed.created);
    registry
        .record_series_value_family(replayed.series_id, SeriesValueFamily::I64)
        .unwrap();
    assert_memory_usage_reconciled(&registry);

    let rolled_back = registry
        .resolve_or_insert(
            "phantom_metric",
            &[Label::new("host", "c"), Label::new("zone", "use1")],
        )
        .unwrap();
    registry
        .assign_series_value_family_if_missing(rolled_back.series_id, SeriesValueFamily::F64)
        .unwrap();
    assert_memory_usage_reconciled(&registry);
    registry.rollback_created_series(std::slice::from_ref(&rolled_back));
    assert_memory_usage_reconciled(&registry);

    registry.persist_to_path(&snapshot_path).unwrap();
    let loaded = SeriesRegistry::load_from_path(&snapshot_path).unwrap();
    assert_memory_usage_reconciled(&loaded);
}

#[test]
fn rollback_created_series_removes_series_without_rewinding_ids_or_dictionaries() {
    let registry = SeriesRegistry::new();
    registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap();

    let baseline_series = registry.series_count();
    let baseline_metric_dict_len = registry.metric_dictionary_len();
    let baseline_label_name_dict_len = registry.label_name_dictionary_len();
    let baseline_label_value_dict_len = registry.label_value_dictionary_len();

    let phantom_labels = vec![Label::new("zone", "use1"), Label::new("env", "prod")];
    let created = registry
        .resolve_or_insert("phantom_metric", &phantom_labels)
        .unwrap();
    assert!(created.created);
    assert!(registry
        .resolve_existing("phantom_metric", &phantom_labels)
        .is_some());

    registry.rollback_created_series(std::slice::from_ref(&created));

    assert_eq!(registry.series_count(), baseline_series);
    assert!(registry.metric_dictionary_len() >= baseline_metric_dict_len);
    assert!(registry.label_name_dictionary_len() >= baseline_label_name_dict_len);
    assert!(registry.label_value_dictionary_len() >= baseline_label_value_dict_len);
    assert!(registry
        .resolve_existing("phantom_metric", &phantom_labels)
        .is_none());

    let next = registry
        .resolve_or_insert("cpu", &[Label::new("host", "b")])
        .unwrap();
    assert!(next.series_id > created.series_id);
}

#[test]
fn estimate_new_series_memory_growth_matches_real_registry_delta_with_active_missing_cache() {
    let registry = SeriesRegistry::new();
    registry
        .resolve_or_insert("baseline_metric", &[Label::new("host", "baseline")])
        .unwrap();
    registry
        .resolve_or_insert(
            "cache_seed",
            &[Label::new("host", "seed"), Label::new("job", "api")],
        )
        .unwrap();
    registry
        .resolve_or_insert("cache_seed", &[Label::new("host", "missing")])
        .unwrap();
    let job_name_id = registry.label_name_id("job").unwrap();
    let seeded_missing = registry.missing_label_postings_for_id(job_name_id);
    assert_eq!(seeded_missing.len(), 2);

    let estimate_keys = vec![
        SeriesKey {
            metric: "cpu".to_string(),
            labels: vec![Label::new("host", "a"), Label::new("rack", "r1")],
        },
        SeriesKey {
            metric: "cpu".to_string(),
            labels: vec![Label::new("rack", "r1"), Label::new("host", "b")],
        },
        SeriesKey {
            metric: "disk".to_string(),
            labels: vec![Label::new("host", "c"), Label::new("zone", "use1")],
        },
        SeriesKey {
            metric: "disk".to_string(),
            labels: vec![Label::new("host", "c"), Label::new("zone", "use1")],
        },
    ];

    let estimated = registry
        .estimate_new_series_memory_growth_bytes(&estimate_keys)
        .unwrap();
    let before = registry.memory_usage_bytes();

    for key in &estimate_keys {
        registry
            .resolve_or_insert(&key.metric, &key.labels)
            .unwrap();
    }

    let after = registry.memory_usage_bytes();
    assert_eq!(estimated, after.saturating_sub(before));
}
