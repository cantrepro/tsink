use tempfile::TempDir;

use super::*;

#[test]
fn persist_rollup_state_returns_error_when_parent_sync_fails() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join(ROLLUP_DIR_NAME)
        .join(ROLLUP_STATE_FILE_NAME);
    let checkpoints = HashMap::from([(
        "policy-a".to_string(),
        BTreeMap::from([("cpu{host=\"a\"}".to_string(), 42)]),
    )]);
    let generations = HashMap::from([("policy-a".to_string(), 3)]);
    let pending_materializations = HashMap::from([(
        "policy-a".to_string(),
        BTreeMap::from([(
            "cpu{host=\"a\"}".to_string(),
            PendingRollupMaterialization {
                checkpoint: 40,
                materialized_through: 42,
                generation: 3,
            },
        )]),
    )]);
    let pending_delete_invalidations = vec![PendingRollupDeleteInvalidation {
        tombstone: TombstoneRange { start: 10, end: 20 },
        series_ids: vec![7],
        affected_policy_ids: vec!["policy-a".to_string()],
    }];

    let _guard = crate::engine::fs_utils::fail_directory_sync_once(
        path.parent().unwrap().to_path_buf(),
        "injected parent directory sync failure",
    );
    let err = persist_rollup_state(
        Some(&path),
        &checkpoints,
        &generations,
        &pending_materializations,
        &pending_delete_invalidations,
    )
    .expect_err("parent directory sync failure must be surfaced");
    assert!(
        err.to_string()
            .contains("injected parent directory sync failure"),
        "unexpected error: {err:?}"
    );

    assert!(
        path.exists(),
        "persisted rollup state should remain for retry"
    );
    let loaded = load_rollup_state(Some(&path)).unwrap();
    assert_eq!(loaded.checkpoints, checkpoints);
    assert_eq!(loaded.generations, generations);
    assert_eq!(loaded.pending_materializations, pending_materializations);
    assert_eq!(
        loaded.pending_delete_invalidations,
        pending_delete_invalidations
    );
}
