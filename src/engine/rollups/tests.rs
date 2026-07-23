use tempfile::TempDir;

use super::policy::{encode_rollup_policies, load_rollup_policies};
use super::runtime::encode_rollup_state_with_epoch;
use super::state_journal::RollupSourceStateEvent;
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
fn source_state_event_is_durable_before_the_caller_mutates_memory() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let state_path = data_path.join(ROLLUP_DIR_NAME).join(ROLLUP_STATE_FILE_NAME);
    let checkpoints = HashMap::from([(
        "policy-a".to_string(),
        BTreeMap::from([("cpu{host=\"a\"}".to_string(), 10)]),
    )]);
    let generations = HashMap::from([("policy-a".to_string(), 0)]);
    persist_rollup_state(
        Some(&state_path),
        &checkpoints,
        &generations,
        &HashMap::new(),
        &[],
    )
    .unwrap();

    let runtime = RollupRuntimeState::new_with_disk_budget(Some(data_path), None);
    *runtime.checkpoints.write() = checkpoints;
    *runtime.generations.write() = generations;
    let store = RollupStateStoreContext { state: &runtime };
    let pending = PendingRollupMaterialization {
        checkpoint: 10,
        materialized_through: 20,
        generation: 0,
    };
    store
        .persist_source_state_event(RollupSourceStateEvent::pending(
            0,
            "policy-a",
            "cpu{host=\"a\"}",
            0,
            Some(10),
            &pending,
        ))
        .unwrap();

    assert!(
        runtime.pending_materializations.read().is_empty(),
        "the persistence primitive must not publish the caller's in-memory mutation"
    );
    let reloaded = load_rollup_state(Some(&state_path)).unwrap();
    assert_eq!(
        reloaded.pending_materializations["policy-a"]["cpu{host=\"a\"}"], pending,
        "a successful return must already be restart-durable"
    );
}

#[test]
fn full_state_snapshot_rejects_before_unbounded_flattening() {
    let entries = (0..=ROLLUP_STATE_SNAPSHOT_MAX_ITEMS)
        .map(|index| (format!("cpu{{series=\"{index}\"}}"), index as i64))
        .collect::<BTreeMap<_, _>>();
    let checkpoints = HashMap::from([("policy-a".to_string(), entries)]);
    let error =
        encode_rollup_state_with_epoch(&checkpoints, &HashMap::new(), &HashMap::new(), &[], 1)
            .expect_err("a policy/delete full snapshot must stop at its pre-allocation item bound");
    assert!(matches!(
        error,
        TsinkError::MaintenanceDependencyWindowExceeded {
            operation: "rollup state snapshot encoding",
            item_limit: ROLLUP_STATE_SNAPSHOT_MAX_ITEMS,
            selected_items,
            ..
        } if selected_items == ROLLUP_STATE_SNAPSHOT_MAX_ITEMS + 1
    ));

    let runtime = RollupRuntimeState::new_with_disk_budget(None, None);
    *runtime.checkpoints.write() = checkpoints;
    let capture_error = (RollupStateStoreContext { state: &runtime })
        .capture_snapshot()
        .expect_err("live policy/delete capture must apply the same guard before cloning maps");
    assert!(matches!(
        capture_error,
        TsinkError::MaintenanceDependencyWindowExceeded {
            operation: "rollup state snapshot encoding",
            ..
        }
    ));
}

#[test]
fn policy_payload_rejects_before_clone_or_json_encoding() {
    let policies = (0..=ROLLUP_STATE_SNAPSHOT_MAX_ITEMS)
        .map(|index| RollupPolicy {
            id: format!("policy-{index}"),
            metric: "cpu".to_string(),
            match_labels: Vec::new(),
            interval: 1,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        })
        .collect::<Vec<_>>();
    let error = super::runtime::ensure_rollup_policies_within_limits(&policies)
        .expect_err("the policy vector must stop at the shared snapshot item bound");
    assert!(matches!(
        error,
        TsinkError::MaintenanceDependencyWindowExceeded {
            operation: "rollup state snapshot encoding",
            item_limit: ROLLUP_STATE_SNAPSHOT_MAX_ITEMS,
            selected_items,
            ..
        } if selected_items == ROLLUP_STATE_SNAPSHOT_MAX_ITEMS + 1
    ));
}

#[test]
fn policy_load_rejects_oversized_file_before_read_or_decode() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("policies.json");
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES as u64 + 1)
        .unwrap();
    let error = load_rollup_policies(Some(&path))
        .expect_err("policy startup must enforce the same finite byte envelope before reading");
    assert!(error.to_string().contains("bounded decode limit"));
}

fn test_policy(interval: i64) -> RollupPolicy {
    RollupPolicy {
        id: "cpu-policy".to_string(),
        metric: "cpu_usage".to_string(),
        match_labels: Vec::new(),
        interval,
        aggregation: Aggregation::Avg,
        bucket_origin: 0,
    }
}

fn original_rollup_snapshot(policy: &RollupPolicy) -> RollupRuntimeSnapshot {
    RollupRuntimeSnapshot {
        policies: vec![policy.clone()],
        checkpoints: HashMap::from([(
            policy.id.clone(),
            BTreeMap::from([("cpu_usage{host=\"a\"}".to_string(), 4_000)]),
        )]),
        pending_materializations: HashMap::new(),
        pending_delete_invalidations: Vec::new(),
        generations: HashMap::from([(policy.id.clone(), 7)]),
        policy_stats: BTreeMap::from([(policy.id.clone(), PolicyRunState::default())]),
    }
}

fn persist_initial_snapshot(
    data_path: &Path,
) -> (
    RollupRuntimeSnapshot,
    RollupRuntimeSnapshot,
    Vec<u8>,
    Vec<u8>,
) {
    let original_policy = test_policy(1_000);
    let original = original_rollup_snapshot(&original_policy);
    let runtime = RollupRuntimeState::new_with_disk_budget(Some(data_path.to_path_buf()), None);
    let store = RollupStateStoreContext { state: &runtime };
    store
        .persist_snapshot(&original)
        .unwrap()
        .report_cleanup_debt();
    store.install_snapshot(original.clone());
    let candidate = store
        .next_snapshot_for_policies(vec![test_policy(2_000)])
        .unwrap();
    let policies_path = data_path
        .join(ROLLUP_DIR_NAME)
        .join(ROLLUP_POLICIES_FILE_NAME);
    let state_path = data_path.join(ROLLUP_DIR_NAME).join(ROLLUP_STATE_FILE_NAME);
    let original_policies = fs::read(policies_path).unwrap();
    let original_state = fs::read(state_path).unwrap();
    (original, candidate, original_policies, original_state)
}

fn open_budgeted_rollup_storage(
    data_path: &Path,
    budget: Arc<crate::LocalDiskBudget>,
) -> ChunkStorage {
    let storage = ChunkStorage::new_with_data_path_and_options_and_disk_budget(
        8,
        None,
        Some(data_path.join("numeric")),
        Some(data_path.join("blob")),
        1,
        ChunkStorageOptions::default(),
        Some(budget),
    )
    .unwrap();
    storage.load_rollup_runtime_state().unwrap();
    storage
}

fn assert_no_rollup_reservation(budget: &crate::LocalDiskBudget, expected_bytes: u64) {
    let snapshot = budget.snapshot();
    assert_eq!(snapshot.accounted_bytes, expected_bytes);
    assert_eq!(snapshot.active_reservations, 0);
    assert_eq!(snapshot.reserved_bytes, 0);
}

#[test]
fn snapshot_quota_failure_rejects_combined_peak_before_publication() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let (original, candidate, original_policies, original_state) =
        persist_initial_snapshot(&data_path);
    let policies_path = data_path
        .join(ROLLUP_DIR_NAME)
        .join(ROLLUP_POLICIES_FILE_NAME);
    let state_path = data_path.join(ROLLUP_DIR_NAME).join(ROLLUP_STATE_FILE_NAME);
    let encoded_policies = encode_rollup_policies(&candidate.policies).unwrap();
    let encoded_state = encode_rollup_state_with_epoch(
        &candidate.checkpoints,
        &candidate.generations,
        &candidate.pending_materializations,
        &candidate.pending_delete_invalidations,
        2,
    )
    .unwrap();
    let original_total = u64::try_from(original_policies.len() + original_state.len()).unwrap();
    let candidate_bytes = u64::try_from(encoded_policies.len() + encoded_state.len()).unwrap();
    let probe_budget =
        crate::LocalDiskBudget::open(&data_path, crate::LocalDiskLimits::default()).unwrap();
    let entry_allowance = probe_budget
        .snapshot_restore_entry_staging_allowance_bytes()
        .unwrap();
    drop(probe_budget);
    let candidate_peak = candidate_bytes + 2 * entry_allowance;
    let budget = crate::LocalDiskBudget::open(
        &data_path,
        crate::LocalDiskLimits {
            // Either staged file fits. The complete pair is one byte over the remaining quota and
            // must be rejected before the state-first publication closure is entered.
            max_bytes: Some(original_total + candidate_peak - 1),
            ..crate::LocalDiskLimits::default()
        },
    )
    .unwrap();
    let storage = open_budgeted_rollup_storage(&data_path, Arc::clone(&budget));
    let publication_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    storage.set_rollup_state_persist_hook({
        let publication_started = Arc::clone(&publication_started);
        move || {
            publication_started.store(true, Ordering::SeqCst);
            Ok(())
        }
    });

    let error = storage
        .apply_rollup_policies(candidate.policies.clone())
        .expect_err("the combined replacement peak must exceed the quota");
    storage.clear_rollup_state_persist_hook();

    assert!(matches!(
        error,
        TsinkError::DiskQuotaExceeded { requested, .. } if requested == candidate_peak
    ));
    assert!(!publication_started.load(Ordering::SeqCst));
    assert_eq!(fs::read(&policies_path).unwrap(), original_policies);
    assert_eq!(fs::read(&state_path).unwrap(), original_state);
    assert_eq!(
        storage.rollup_state_store_context().policies_snapshot(),
        original.policies
    );
    assert_no_rollup_reservation(&budget, original_total);
    assert_eq!(budget.snapshot().rejections_total, 1);
    assert_eq!(
        fs::read_dir(data_path.join(ROLLUP_DIR_NAME))
            .unwrap()
            .count(),
        2
    );
}

#[test]
fn interruption_after_invalidating_state_is_safe_and_reconciled_on_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let (original, candidate, original_policies, _) = persist_initial_snapshot(&data_path);
    let policies_path = data_path
        .join(ROLLUP_DIR_NAME)
        .join(ROLLUP_POLICIES_FILE_NAME);
    let state_path = data_path.join(ROLLUP_DIR_NAME).join(ROLLUP_STATE_FILE_NAME);
    let candidate_state = encode_rollup_state_with_epoch(
        &candidate.checkpoints,
        &candidate.generations,
        &candidate.pending_materializations,
        &candidate.pending_delete_invalidations,
        2,
    )
    .unwrap();
    let budget =
        crate::LocalDiskBudget::open(&data_path, crate::LocalDiskLimits::default()).unwrap();
    let storage = open_budgeted_rollup_storage(&data_path, Arc::clone(&budget));
    storage.set_rollup_state_persist_hook(|| {
        Err(TsinkError::Other(
            "injected interruption after durable invalidating state".to_string(),
        ))
    });

    let error = storage
        .apply_rollup_policies(candidate.policies.clone())
        .expect_err("the interruption must leave the apply outcome indeterminate");
    storage.clear_rollup_state_persist_hook();

    assert!(error.to_string().contains("indeterminate"));
    assert!(error
        .to_string()
        .contains("injected interruption after durable invalidating state"));
    assert_eq!(fs::read(&policies_path).unwrap(), original_policies);
    assert_eq!(fs::read(&state_path).unwrap(), candidate_state);
    assert_eq!(
        storage.rollup_state_store_context().policies_snapshot(),
        original.policies
    );
    let expected_bytes = u64::try_from(original_policies.len() + candidate_state.len()).unwrap();
    assert_no_rollup_reservation(&budget, expected_bytes);
    assert_eq!(
        fs::read_dir(data_path.join(ROLLUP_DIR_NAME))
            .unwrap()
            .count(),
        2
    );

    drop(storage);
    let reopened = open_budgeted_rollup_storage(&data_path, Arc::clone(&budget));
    assert_eq!(
        reopened.rollup_state_store_context().policies_snapshot(),
        original.policies
    );
    let reopened_state = load_rollup_state(Some(&state_path)).unwrap();
    assert_eq!(reopened_state.checkpoints, candidate.checkpoints);
    assert_eq!(reopened_state.generations, candidate.generations);
    assert!(
        !reopened_state.checkpoints.contains_key("cpu-policy"),
        "old policies paired with candidate state must fall back to raw data"
    );
}

#[test]
fn indeterminate_candidate_publication_fences_retry_from_stale_predecessor() {
    use std::sync::atomic::AtomicUsize;

    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let (original, candidate, _, _) = persist_initial_snapshot(&data_path);
    let policies_path = data_path
        .join(ROLLUP_DIR_NAME)
        .join(ROLLUP_POLICIES_FILE_NAME);
    let state_path = data_path.join(ROLLUP_DIR_NAME).join(ROLLUP_STATE_FILE_NAME);
    let candidate_policies = encode_rollup_policies(&candidate.policies).unwrap();
    let candidate_state = encode_rollup_state_with_epoch(
        &candidate.checkpoints,
        &candidate.generations,
        &candidate.pending_materializations,
        &candidate.pending_delete_invalidations,
        2,
    )
    .unwrap();
    let budget =
        crate::LocalDiskBudget::open(&data_path, crate::LocalDiskLimits::default()).unwrap();
    let storage = open_budgeted_rollup_storage(&data_path, Arc::clone(&budget));
    let rollup_dir = fs::canonicalize(data_path.join(ROLLUP_DIR_NAME)).unwrap();
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let _sync_failure = crate::engine::fs_utils::fail_directory_sync_matching_once(
        {
            let sync_calls = Arc::clone(&sync_calls);
            move |candidate_path| {
                candidate_path == rollup_dir && sync_calls.fetch_add(1, Ordering::SeqCst) == 1
            }
        },
        "injected candidate policies parent sync failure",
    );

    let error = storage
        .apply_rollup_policies(candidate.policies.clone())
        .expect_err("the second publication sync failure must be indeterminate");

    assert!(error.to_string().contains("indeterminate"));
    assert!(error
        .to_string()
        .contains("injected candidate policies parent sync failure"));
    assert_eq!(fs::read(&state_path).unwrap(), candidate_state);
    assert_eq!(fs::read(&policies_path).unwrap(), candidate_policies);
    assert_eq!(
        storage.rollup_state_store_context().policies_snapshot(),
        original.policies,
        "an indeterminate apply must not silently change the in-memory policy set"
    );
    let expected_bytes = u64::try_from(candidate_state.len() + candidate_policies.len()).unwrap();
    assert_no_rollup_reservation(&budget, expected_bytes);

    // Memory still contains predecessor O while the visible/durable files may contain candidate
    // A. Retrying O would otherwise preserve O's checkpoints in the next state-first file and
    // briefly pair them with A's policy definition. The fence must reject that retry before its
    // state publication crash point.
    let retry_state_published = Arc::new(std::sync::atomic::AtomicBool::new(false));
    storage.set_rollup_state_persist_hook({
        let retry_state_published = Arc::clone(&retry_state_published);
        move || {
            retry_state_published.store(true, Ordering::SeqCst);
            Err(TsinkError::Other(
                "injected retry interruption after state publication".to_string(),
            ))
        }
    });
    let retry_error = storage
        .apply_rollup_policies(original.policies.clone())
        .expect_err("a retry derived from the stale predecessor must remain fenced");
    storage.clear_rollup_state_persist_hook();
    assert!(retry_error.to_string().contains("fenced"));
    assert!(retry_error.to_string().contains("reopen"));
    assert!(!retry_state_published.load(Ordering::SeqCst));
    let pipeline_error = storage
        .trigger_rollup_run()
        .expect_err("background materialization must share the indeterminate-publication fence");
    assert!(pipeline_error.to_string().contains("fenced"));
    assert_eq!(fs::read(&state_path).unwrap(), candidate_state);
    assert_eq!(fs::read(&policies_path).unwrap(), candidate_policies);
    assert_no_rollup_reservation(&budget, expected_bytes);

    drop(storage);
    let reopened = open_budgeted_rollup_storage(&data_path, Arc::clone(&budget));
    assert_eq!(
        reopened.rollup_state_store_context().policies_snapshot(),
        candidate.policies
    );
    let reopened_state = load_rollup_state(Some(&state_path)).unwrap();
    assert_eq!(reopened_state.checkpoints, candidate.checkpoints);
    assert_eq!(reopened_state.generations, candidate.generations);
}

#[test]
fn committed_pair_surfaces_postcommit_debt_after_installing_candidate() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let (_, candidate, _, _) = persist_initial_snapshot(&data_path);
    let policies_path = data_path
        .join(ROLLUP_DIR_NAME)
        .join(ROLLUP_POLICIES_FILE_NAME);
    let state_path = data_path.join(ROLLUP_DIR_NAME).join(ROLLUP_STATE_FILE_NAME);
    let candidate_policies = encode_rollup_policies(&candidate.policies).unwrap();
    let candidate_state = encode_rollup_state_with_epoch(
        &candidate.checkpoints,
        &candidate.generations,
        &candidate.pending_materializations,
        &candidate.pending_delete_invalidations,
        2,
    )
    .unwrap();
    let budget =
        crate::LocalDiskBudget::open(&data_path, crate::LocalDiskLimits::default()).unwrap();
    let storage = open_budgeted_rollup_storage(&data_path, Arc::clone(&budget));
    storage
        .rollups
        .runtime
        .set_policy_persist_hook(|point| match point {
            RollupPolicyPersistHookPoint::CandidatePublished => Err(TsinkError::Other(
                "injected postcommit rollup cleanup failure".to_string(),
            )),
        });

    let result = storage
        .apply_rollup_policies(candidate.policies.clone())
        .expect("a known committed pair must not be reported as rejected");
    storage.rollups.runtime.clear_policy_persist_hook();

    assert!(result.policies.iter().any(|policy| {
        policy
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("postcommit rollup snapshot cleanup debt"))
    }));
    assert_eq!(fs::read(&state_path).unwrap(), candidate_state);
    assert_eq!(fs::read(&policies_path).unwrap(), candidate_policies);
    assert_eq!(
        storage.rollup_state_store_context().policies_snapshot(),
        candidate.policies,
        "a known committed pair must be installed before postcommit debt is returned"
    );
    let expected_bytes = u64::try_from(candidate_state.len() + candidate_policies.len()).unwrap();
    assert_no_rollup_reservation(&budget, expected_bytes);

    drop(storage);
    let reopened = open_budgeted_rollup_storage(&data_path, Arc::clone(&budget));
    assert_eq!(
        reopened.rollup_state_store_context().policies_snapshot(),
        candidate.policies
    );
}
