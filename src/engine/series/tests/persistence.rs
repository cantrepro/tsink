use tempfile::TempDir;

use super::super::{LoadedSeriesRegistry, SeriesRegistry};
use crate::{Label, TsinkError};

#[test]
fn persisted_registry_index_uses_zstd_when_it_shrinks() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("series_index.bin");
    let registry = SeriesRegistry::new();

    for series_idx in 0..512u64 {
        registry
            .resolve_or_insert(
                "cpu",
                &[
                    Label::new("tenant", format!("t{}", series_idx % 32)),
                    Label::new("host", format!("h{}", series_idx % 128)),
                    Label::new("series", format!("s{series_idx}")),
                ],
            )
            .unwrap();
    }

    registry.persist_to_path(&path).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
    assert_ne!(
        flags & crate::engine::binio::FILE_FLAG_ZSTD_BODY,
        0,
        "registry snapshot should be compressed when it materially shrinks"
    );

    let loaded = SeriesRegistry::load_from_path(&path).unwrap();
    assert_eq!(loaded.series_count(), registry.series_count());
    assert_eq!(
        loaded.metric_dictionary_len(),
        registry.metric_dictionary_len()
    );
    assert_eq!(
        loaded.label_name_dictionary_len(),
        registry.label_name_dictionary_len()
    );
    assert_eq!(
        loaded.label_value_dictionary_len(),
        registry.label_value_dictionary_len()
    );
}

#[test]
fn persist_to_path_returns_error_when_parent_sync_fails() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("series_index.bin");
    let registry = SeriesRegistry::new();
    registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap();

    let _guard = crate::engine::fs_utils::fail_directory_sync_once(
        path.parent().unwrap().to_path_buf(),
        "injected parent directory sync failure",
    );
    let err = registry
        .persist_to_path(&path)
        .expect_err("parent directory sync failure must be surfaced");
    assert!(
        err.to_string()
            .contains("injected parent directory sync failure"),
        "unexpected error: {err:?}"
    );

    assert!(
        path.exists(),
        "persisted file should remain available for retry"
    );
    let loaded = SeriesRegistry::load_from_path(&path).unwrap();
    assert_eq!(loaded.series_count(), 1);
}

#[test]
fn merge_from_imports_incremental_series_without_rebinding_ids() {
    let base = SeriesRegistry::new();
    base.resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap();

    let delta = SeriesRegistry::new();
    delta
        .register_series_with_id(42, "cpu", &[Label::new("host", "b")])
        .unwrap();

    let created = base.merge_from(&delta).unwrap();
    assert_eq!(created, 1);
    assert_eq!(
        base.resolve_existing("cpu", &[Label::new("host", "b")])
            .unwrap()
            .series_id,
        42
    );
}

#[test]
fn load_persisted_state_merges_checkpoint_and_legacy_incremental_sidecar() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("series_index.bin");
    let delta_path = SeriesRegistry::incremental_path(&snapshot_path);

    let full = SeriesRegistry::new();
    let base = full
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;
    let delta = full
        .resolve_or_insert("cpu", &[Label::new("host", "b")])
        .unwrap()
        .series_id;

    let checkpoint = full.series_subset([base]).unwrap();
    let incremental = full.series_subset([delta]).unwrap();
    checkpoint.persist_to_path(&snapshot_path).unwrap();
    incremental.persist_to_path(&delta_path).unwrap();

    let LoadedSeriesRegistry {
        registry,
        delta_series_count,
    } = SeriesRegistry::load_persisted_state(&snapshot_path)
        .unwrap()
        .expect("checkpoint or sidecar should load");

    assert_eq!(delta_series_count, 1);
    assert_eq!(registry.series_count(), 2);
    assert!(registry
        .resolve_existing("cpu", &[Label::new("host", "a")])
        .is_some());
    assert!(registry
        .resolve_existing("cpu", &[Label::new("host", "b")])
        .is_some());
}

#[test]
fn load_persisted_state_merges_legacy_and_segmented_incremental_sidecars() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("series_index.bin");
    let delta_path = SeriesRegistry::incremental_path(&snapshot_path);

    let full = SeriesRegistry::new();
    let checkpoint = full
        .resolve_or_insert("cpu", &[Label::new("host", "checkpoint")])
        .unwrap()
        .series_id;
    let legacy_delta = full
        .resolve_or_insert("cpu", &[Label::new("host", "legacy")])
        .unwrap()
        .series_id;
    let segmented_delta = full
        .resolve_or_insert("cpu", &[Label::new("host", "segmented")])
        .unwrap()
        .series_id;

    full.series_subset([checkpoint])
        .unwrap()
        .persist_to_path(&snapshot_path)
        .unwrap();
    full.series_subset([legacy_delta])
        .unwrap()
        .persist_to_path(&delta_path)
        .unwrap();
    full.series_subset([segmented_delta])
        .unwrap()
        .persist_incremental_to_snapshot_path(&snapshot_path)
        .unwrap();

    let LoadedSeriesRegistry {
        registry,
        delta_series_count,
    } = SeriesRegistry::load_persisted_state(&snapshot_path)
        .unwrap()
        .expect("checkpoint plus both incremental sidecars should load");

    assert_eq!(delta_series_count, 2);
    assert_eq!(registry.series_count(), 3);
    assert!(registry
        .resolve_existing("cpu", &[Label::new("host", "checkpoint")])
        .is_some());
    assert!(registry
        .resolve_existing("cpu", &[Label::new("host", "legacy")])
        .is_some());
    assert!(registry
        .resolve_existing("cpu", &[Label::new("host", "segmented")])
        .is_some());
}

#[test]
fn startup_registry_admission_is_aggregate_across_every_incremental_file() {
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("series_index.bin");
    let full = SeriesRegistry::new();
    let base_id = full
        .resolve_or_insert("cpu", &[Label::new("host", "base")])
        .unwrap()
        .series_id;
    let first_delta_id = full
        .resolve_or_insert("cpu", &[Label::new("host", "delta-a")])
        .unwrap()
        .series_id;
    let second_delta_id = full
        .resolve_or_insert("blob", &[Label::new("host", "delta-b")])
        .unwrap()
        .series_id;
    full.series_subset([base_id])
        .unwrap()
        .persist_to_path(&snapshot_path)
        .unwrap();
    full.series_subset([first_delta_id])
        .unwrap()
        .persist_incremental_to_snapshot_path(&snapshot_path)
        .unwrap();
    full.series_subset([second_delta_id])
        .unwrap()
        .persist_to_path(
            &SeriesRegistry::incremental_dir(&snapshot_path).join("delta-0000000000000001.bin"),
        )
        .unwrap();

    let mut exact_threshold = 0usize;
    let loaded = SeriesRegistry::load_persisted_state_with_startup_admission(
        &snapshot_path,
        usize::MAX,
        |additional| {
            exact_threshold = exact_threshold.saturating_add(additional);
            Ok(())
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(loaded.registry.series_count(), 3);
    assert_eq!(loaded.delta_series_count, 2);
    assert!(exact_threshold > 0);

    let mut exact_reserved = 0usize;
    let exact = SeriesRegistry::load_persisted_state_with_startup_admission(
        &snapshot_path,
        exact_threshold,
        |additional| {
            let required = exact_reserved.saturating_add(additional);
            if required > exact_threshold {
                return Err(TsinkError::MemoryBudgetExceeded {
                    budget: exact_threshold,
                    required,
                });
            }
            exact_reserved = required;
            Ok(())
        },
    )
    .expect("the exact aggregate startup threshold must succeed")
    .unwrap();
    assert_eq!(exact.registry.series_count(), 3);
    assert_eq!(exact_reserved, exact_threshold);

    let one_less = exact_threshold - 1;
    let mut rejected_reserved = 0usize;
    let err = SeriesRegistry::load_persisted_state_with_startup_admission(
        &snapshot_path,
        one_less,
        |additional| {
            let required = rejected_reserved.saturating_add(additional);
            if required > one_less {
                return Err(TsinkError::MemoryBudgetExceeded {
                    budget: one_less,
                    required,
                });
            }
            rejected_reserved = required;
            Ok(())
        },
    )
    .expect_err("one byte below the aggregate threshold must fail");
    assert!(matches!(
        err,
        TsinkError::MemoryBudgetExceeded { budget, required }
            if budget == one_less && required == exact_threshold
    ));
}
