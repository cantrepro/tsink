use tempfile::TempDir;

use super::policy::{encode_rollup_policies, load_rollup_policies};
use super::runtime::encode_rollup_state;
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

#[test]
fn snapshot_quota_failure_restores_both_files_before_reopen() {
    fn policy(id: &str, interval: i64) -> RollupPolicy {
        RollupPolicy {
            id: id.to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        }
    }

    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let stable_policy = policy("stable-policy", 1_000);
    let mut original_policy = policy("updated-policy", 2_000);
    let mut updated_policy = policy("updated-policy", 4_000);
    let large_policy_match = (0..32)
        .map(|index| {
            Label::new(
                format!("dimension_{index:02}"),
                format!("policy-value-{index:02}-with-enough-bytes-for-second-file-admission"),
            )
        })
        .collect::<Vec<_>>();
    original_policy.match_labels = large_policy_match.clone();
    updated_policy.match_labels = large_policy_match;

    // Keep state small and policy definitions large. With exactly one candidate-state payload of
    // temporary headroom, state.json is published first and policies.json is then rejected. This
    // exercises cross-file rollback after the first publication, not only first-file preflight.
    let stable_checkpoints = (0..2)
        .map(|index| {
            (
                format!("cpu_usage{{host=\"host-{index:03}\",region=\"west\"}}"),
                i64::from(index) * 1_000,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let original_snapshot = RollupRuntimeSnapshot {
        policies: vec![stable_policy.clone(), original_policy.clone()],
        checkpoints: HashMap::from([
            (stable_policy.id.clone(), stable_checkpoints),
            (
                original_policy.id.clone(),
                BTreeMap::from([("cpu_usage{host=\"target\"}".to_string(), 8_000)]),
            ),
        ]),
        pending_materializations: HashMap::new(),
        pending_delete_invalidations: Vec::new(),
        generations: HashMap::from([
            (stable_policy.id.clone(), 0),
            (original_policy.id.clone(), 3),
        ]),
        policy_stats: BTreeMap::from([
            (stable_policy.id.clone(), PolicyRunState::default()),
            (original_policy.id.clone(), PolicyRunState::default()),
        ]),
    };

    let initial_runtime = RollupRuntimeState::new_with_disk_budget(Some(data_path.clone()), None);
    let initial_store = RollupStateStoreContext {
        state: &initial_runtime,
    };
    initial_store.persist_snapshot(&original_snapshot).unwrap();
    initial_store.install_snapshot(original_snapshot.clone());

    let policies_path = data_path
        .join(ROLLUP_DIR_NAME)
        .join(ROLLUP_POLICIES_FILE_NAME);
    let state_path = data_path.join(ROLLUP_DIR_NAME).join(ROLLUP_STATE_FILE_NAME);
    let original_policy_bytes = fs::read(&policies_path).unwrap();
    let original_state_bytes = fs::read(&state_path).unwrap();
    let original_total =
        u64::try_from(original_policy_bytes.len() + original_state_bytes.len()).unwrap();

    let mut updated_policies = vec![stable_policy, updated_policy];
    updated_policies.sort_by(|left, right| left.id.cmp(&right.id));
    let candidate = initial_store.next_snapshot_for_policies(updated_policies);
    let encoded_policies = encode_rollup_policies(&candidate.policies).unwrap();
    let encoded_state = encode_rollup_state(
        &candidate.checkpoints,
        &candidate.generations,
        &candidate.pending_materializations,
        &candidate.pending_delete_invalidations,
    )
    .unwrap();
    assert!(
        encoded_policies.len() > original_state_bytes.len(),
        "test requires the second policy reservation to exceed the old state bytes left as headroom"
    );

    let budget = crate::LocalDiskBudget::open(
        &data_path,
        crate::LocalDiskLimits {
            max_bytes: Some(original_total + u64::try_from(encoded_state.len()).unwrap()),
            ..crate::LocalDiskLimits::default()
        },
    )
    .unwrap();
    drop(initial_runtime);
    let budgeted_storage = ChunkStorage::new_with_data_path_and_options_and_disk_budget(
        8,
        None,
        Some(data_path.join("numeric")),
        Some(data_path.join("blob")),
        1,
        ChunkStorageOptions::default(),
        Some(Arc::clone(&budget)),
    )
    .unwrap();
    budgeted_storage.load_rollup_runtime_state().unwrap();

    let interruption_observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    budgeted_storage.set_rollup_state_persist_hook({
        let interruption_observed = Arc::clone(&interruption_observed);
        let policies_path = policies_path.clone();
        let state_path = state_path.clone();
        let expected_policies = original_snapshot.policies.clone();
        let expected_checkpoints = candidate.checkpoints.clone();
        let expected_generations = candidate.generations.clone();
        move || {
            // This hook runs after candidate state is durable and before candidate policies are
            // attempted. Treat loading these files as an interruption-point reopen: the old
            // policy definitions must still pair with invalidating candidate state, never with
            // checkpoints from their superseded definitions.
            assert_eq!(
                load_rollup_policies(Some(&policies_path)).unwrap(),
                expected_policies
            );
            let interrupted_state = load_rollup_state(Some(&state_path)).unwrap();
            assert_eq!(interrupted_state.checkpoints, expected_checkpoints);
            assert_eq!(interrupted_state.generations, expected_generations);
            interruption_observed.store(true, Ordering::SeqCst);
            Ok(())
        }
    });

    let err = budgeted_storage
        .apply_rollup_policies(candidate.policies.clone())
        .expect_err("policy replacement should exceed the deliberately tiny quota");
    budgeted_storage.clear_rollup_state_persist_hook();
    assert!(interruption_observed.load(Ordering::SeqCst));
    assert!(
        matches!(
            err,
            TsinkError::DiskQuotaExceeded { requested, .. }
                if requested == u64::try_from(encoded_policies.len()).unwrap()
        ),
        "quota error should retain its typed variant after successful rollback: {err:?}"
    );
    assert_eq!(
        budgeted_storage
            .rollup_state_store_context()
            .policies_snapshot(),
        original_snapshot.policies,
        "the public failed apply must not publish the candidate in memory"
    );
    assert_eq!(fs::read(&policies_path).unwrap(), original_policy_bytes);
    assert_eq!(fs::read(&state_path).unwrap(), original_state_bytes);
    let disk = budget.snapshot();
    assert_eq!(disk.accounted_bytes, original_total);
    assert_eq!(disk.active_reservations, 0);
    assert_eq!(disk.reserved_bytes, 0);
    assert_eq!(disk.rejections_total, 1);

    drop(budgeted_storage);
    let reopened_storage = ChunkStorage::new_with_data_path_and_options_and_disk_budget(
        8,
        None,
        Some(data_path.join("numeric")),
        Some(data_path.join("blob")),
        1,
        ChunkStorageOptions::default(),
        None,
    )
    .unwrap();
    reopened_storage.load_rollup_runtime_state().unwrap();
    assert_eq!(
        reopened_storage
            .rollup_state_store_context()
            .policies_snapshot(),
        original_snapshot.policies
    );
    let reopened_state = load_rollup_state(Some(&state_path)).unwrap();
    assert_eq!(reopened_state.checkpoints, original_snapshot.checkpoints);
    assert_eq!(reopened_state.generations, original_snapshot.generations);
    assert_eq!(
        reopened_state.pending_materializations,
        original_snapshot.pending_materializations
    );
    assert_eq!(
        reopened_state.pending_delete_invalidations,
        original_snapshot.pending_delete_invalidations
    );
}

#[test]
fn failed_policy_rollback_retains_candidate_state_for_safe_reopen() {
    fn policy(interval: i64) -> RollupPolicy {
        RollupPolicy {
            id: "cpu-policy".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        }
    }

    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let labels = vec![Label::new("host", "a")];
    let source_key = source_series_key("cpu_usage", &labels);
    let original_policy = policy(1_000);
    let updated_policy = policy(2_000);
    let original_snapshot = RollupRuntimeSnapshot {
        policies: vec![original_policy.clone()],
        checkpoints: HashMap::from([(
            original_policy.id.clone(),
            BTreeMap::from([(source_key.clone(), 4_000)]),
        )]),
        pending_materializations: HashMap::new(),
        pending_delete_invalidations: Vec::new(),
        generations: HashMap::from([(original_policy.id.clone(), 7)]),
        policy_stats: BTreeMap::from([(original_policy.id.clone(), PolicyRunState::default())]),
    };

    let runtime = RollupRuntimeState::new_with_disk_budget(Some(data_path.clone()), None);
    let store = RollupStateStoreContext { state: &runtime };
    store.persist_snapshot(&original_snapshot).unwrap();
    store.install_snapshot(original_snapshot.clone());
    let candidate = store.next_snapshot_for_policies(vec![updated_policy.clone()]);
    assert!(!candidate.checkpoints.contains_key(&updated_policy.id));
    assert_eq!(candidate.generations.get(&updated_policy.id), Some(&8));

    let candidate_publish_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rollback_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    runtime.set_policy_persist_hook({
        let candidate_publish_seen = Arc::clone(&candidate_publish_seen);
        let rollback_seen = Arc::clone(&rollback_seen);
        move |point| match point {
            RollupPolicyPersistHookPoint::CandidatePublished => {
                candidate_publish_seen.store(true, Ordering::SeqCst);
                Err(TsinkError::Other(
                    "injected candidate policy post-publication failure".to_string(),
                ))
            }
            RollupPolicyPersistHookPoint::RollbackStarting => {
                rollback_seen.store(true, Ordering::SeqCst);
                Err(TsinkError::Other(
                    "injected predecessor policy rollback failure".to_string(),
                ))
            }
        }
    });

    let err = store
        .persist_snapshot(&candidate)
        .expect_err("late policy publication and rollback failures must be indeterminate");
    runtime.clear_policy_persist_hook();
    assert!(candidate_publish_seen.load(Ordering::SeqCst));
    assert!(rollback_seen.load(Ordering::SeqCst));
    assert!(
        err.to_string()
            .contains("injected predecessor policy rollback failure"),
        "unexpected persistence error: {err:?}"
    );
    assert!(
        err.to_string()
            .contains("candidate invalidating state retained"),
        "error must explain the conservative state decision: {err:?}"
    );

    let policies_path = data_path
        .join(ROLLUP_DIR_NAME)
        .join(ROLLUP_POLICIES_FILE_NAME);
    let state_path = data_path.join(ROLLUP_DIR_NAME).join(ROLLUP_STATE_FILE_NAME);
    assert_eq!(
        load_rollup_policies(Some(&policies_path)).unwrap(),
        candidate.policies,
        "the injected rollback failure leaves the already-published candidate policies"
    );
    let durable_state = load_rollup_state(Some(&state_path)).unwrap();
    assert_eq!(durable_state.checkpoints, candidate.checkpoints);
    assert_eq!(durable_state.generations, candidate.generations);
    assert!(
        !durable_state
            .checkpoints
            .get(&updated_policy.id)
            .is_some_and(|entries| entries.contains_key(&source_key)),
        "candidate policies must never be paired with the predecessor checkpoint"
    );

    drop(runtime);
    let reopened = ChunkStorage::new_with_data_path_and_options_and_disk_budget(
        8,
        None,
        Some(data_path.join("numeric")),
        Some(data_path.join("blob")),
        1,
        ChunkStorageOptions::default(),
        None,
    )
    .unwrap();
    reopened.load_rollup_runtime_state().unwrap();
    Storage::insert_rows(
        &reopened,
        &[Row::with_labels(
            "cpu_usage",
            labels.clone(),
            DataPoint::new(0, 1.0),
        )],
    )
    .unwrap();
    assert_eq!(
        reopened.rollup_state_store_context().policies_snapshot(),
        candidate.policies
    );
    assert!(
        reopened
            .rollup_query_candidate(
                "cpu_usage",
                &labels,
                updated_policy.interval,
                updated_policy.aggregation,
                0,
                4_000,
            )
            .is_none(),
        "fresh reopen must fall back to raw data without the predecessor checkpoint"
    );
    reopened.close().unwrap();
}
