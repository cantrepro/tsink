use super::policy::{encode_rollup_policies, load_rollup_policies};
use super::*;
use crate::engine::series::SeriesKey;

impl RollupRuntimeState {
    pub(in crate::engine) fn new_with_disk_budget(
        data_path: Option<PathBuf>,
        local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    ) -> Self {
        let dir_path = data_path.map(|path| path.join(ROLLUP_DIR_NAME));
        let policies_path = dir_path
            .as_ref()
            .map(|dir_path| dir_path.join(ROLLUP_POLICIES_FILE_NAME));
        let state_path = dir_path
            .as_ref()
            .map(|dir_path| dir_path.join(ROLLUP_STATE_FILE_NAME));
        Self {
            dir_path,
            policies_path,
            state_path,
            local_disk_budget,
            snapshot_publication_fenced: AtomicBool::new(false),
            snapshot_visibility: RwLock::new(()),
            policies: RwLock::new(Vec::new()),
            checkpoints: RwLock::new(HashMap::new()),
            pending_materializations: RwLock::new(HashMap::new()),
            pending_delete_invalidations: RwLock::new(Vec::new()),
            generations: RwLock::new(HashMap::new()),
            journal_epoch: AtomicU64::new(0),
            policy_stats: RwLock::new(BTreeMap::new()),
            #[cfg(test)]
            test_hooks: RollupTestHooks::default(),
        }
    }

    pub(in crate::engine) fn dir_path(&self) -> Option<&Path> {
        self.dir_path.as_deref()
    }

    fn policies_path(&self) -> Option<&Path> {
        self.policies_path.as_deref()
    }

    fn state_path(&self) -> Option<&Path> {
        self.state_path.as_deref()
    }

    #[cfg(test)]
    pub(super) fn invoke_policy_start_hook(&self, policy: &RollupPolicy) {
        let hook = self.test_hooks.policy_start_hook.read().clone();
        if let Some(hook) = hook {
            hook(policy);
        }
    }

    #[cfg(test)]
    pub(super) fn set_policy_start_hook<F>(&self, hook: F)
    where
        F: Fn(&RollupPolicy) + Send + Sync + 'static,
    {
        *self.test_hooks.policy_start_hook.write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(super) fn clear_policy_start_hook(&self) {
        *self.test_hooks.policy_start_hook.write() = None;
    }

    #[cfg(test)]
    pub(super) fn invoke_source_read_hook(
        &self,
        policy: &RollupPolicy,
        series_id: SeriesId,
    ) -> Result<()> {
        let hook = self.test_hooks.source_read_hook.read().clone();
        if let Some(hook) = hook {
            hook(policy, series_id)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn set_source_read_hook<F>(&self, hook: F)
    where
        F: Fn(&RollupPolicy, SeriesId) -> Result<()> + Send + Sync + 'static,
    {
        *self.test_hooks.source_read_hook.write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(super) fn clear_source_read_hook(&self) {
        *self.test_hooks.source_read_hook.write() = None;
    }

    #[cfg(test)]
    fn invoke_state_persist_hook(&self) -> Result<()> {
        let hook = self.test_hooks.state_persist_hook.read().clone();
        if let Some(hook) = hook {
            hook()?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn set_state_persist_hook<F>(&self, hook: F)
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        *self.test_hooks.state_persist_hook.write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(super) fn clear_state_persist_hook(&self) {
        *self.test_hooks.state_persist_hook.write() = None;
    }

    #[cfg(test)]
    fn invoke_policy_persist_hook(&self, point: RollupPolicyPersistHookPoint) -> Result<()> {
        let hook = self.test_hooks.policy_persist_hook.read().clone();
        if let Some(hook) = hook {
            hook(point)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn set_policy_persist_hook<F>(&self, hook: F)
    where
        F: Fn(RollupPolicyPersistHookPoint) -> Result<()> + Send + Sync + 'static,
    {
        *self.test_hooks.policy_persist_hook.write() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(super) fn clear_policy_persist_hook(&self) {
        *self.test_hooks.policy_persist_hook.write() = None;
    }
}

pub(super) fn load_rollup_state(path: Option<&Path>) -> Result<LoadedRollupState> {
    let Some(path) = path else {
        return Ok(LoadedRollupState::default());
    };
    let bytes = match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                || !metadata.file_type().is_file()
            {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state path is link-like or not a regular file: {}",
                    path.display()
                )));
            }
            if metadata.len() > ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES as u64 {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state snapshot size {} exceeds the bounded decode limit {ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES}",
                    metadata.len()
                )));
            }
            let mut file = fs::File::open(path)?;
            Some(crate::engine::binio::read_to_end_bounded(
                &mut file,
                ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES,
                usize::try_from(metadata.len())
                    .unwrap_or(ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES)
                    .min(ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES),
                "rollup state snapshot",
            )?)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source: err,
            });
        }
    };

    let file = if let Some(bytes) = bytes {
        let file: PersistedRollupStateFile = serde_json::from_slice(&bytes)?;
        if file.magic != ROLLUP_STATE_MAGIC || file.version != ROLLUP_SCHEMA_VERSION {
            return Err(TsinkError::DataCorruption(format!(
                "unsupported rollup state file {}",
                path.display()
            )));
        }
        Some(file)
    } else {
        None
    };

    let journal_epoch = file.as_ref().map_or(0, |file| file.journal_epoch);

    let mut checkpoints = HashMap::<String, BTreeMap<String, i64>>::new();
    for checkpoint in file
        .as_ref()
        .map(|file| file.checkpoints.as_slice())
        .unwrap_or_default()
        .iter()
        .cloned()
    {
        checkpoints
            .entry(checkpoint.policy_id)
            .or_default()
            .insert(checkpoint.source_key, checkpoint.materialized_through);
    }
    let generations = file
        .as_ref()
        .map(|file| file.generations.as_slice())
        .unwrap_or_default()
        .iter()
        .cloned()
        .map(|entry| (entry.policy_id, entry.generation))
        .collect::<HashMap<_, _>>();
    let mut pending_materializations =
        HashMap::<String, BTreeMap<String, PendingRollupMaterialization>>::new();
    for pending in file
        .as_ref()
        .map(|file| file.pending_materializations.as_slice())
        .unwrap_or_default()
        .iter()
        .cloned()
    {
        pending_materializations
            .entry(pending.policy_id)
            .or_default()
            .insert(
                pending.source_key,
                PendingRollupMaterialization {
                    checkpoint: pending.checkpoint,
                    materialized_through: pending.materialized_through,
                    generation: pending.generation,
                },
            );
    }
    let mut pending_delete_invalidations = file
        .as_ref()
        .map(|file| file.pending_delete_invalidations.as_slice())
        .unwrap_or_default()
        .iter()
        .cloned()
        .map(|pending| PendingRollupDeleteInvalidation {
            tombstone: pending.tombstone,
            series_ids: pending.series_ids,
            affected_policy_ids: pending.affected_policy_ids,
        })
        .collect::<Vec<_>>();
    normalize_pending_delete_invalidations(&mut pending_delete_invalidations);

    let mut state = LoadedRollupState {
        journal_epoch,
        checkpoints,
        pending_materializations,
        pending_delete_invalidations,
        generations,
    };
    super::state_journal::load_rollup_state_journal(path.parent(), journal_epoch, &mut state)?;
    Ok(state)
}

#[cfg(test)]
pub(in crate::engine::storage_engine) fn load_rollup_state_json(
    path: &Path,
) -> Result<serde_json::Value> {
    let state = load_rollup_state(Some(path))?;
    let encoded = encode_rollup_state_with_epoch(
        &state.checkpoints,
        &state.generations,
        &state.pending_materializations,
        &state.pending_delete_invalidations,
        state.journal_epoch,
    )?;
    Ok(serde_json::from_slice(&encoded)?)
}

#[cfg(test)]
pub(super) fn persist_rollup_state(
    path: Option<&Path>,
    checkpoints: &HashMap<String, BTreeMap<String, i64>>,
    generations: &HashMap<String, u64>,
    pending_materializations: &HashMap<String, BTreeMap<String, PendingRollupMaterialization>>,
    pending_delete_invalidations: &[PendingRollupDeleteInvalidation],
) -> Result<()> {
    persist_rollup_state_budgeted(
        path,
        checkpoints,
        generations,
        pending_materializations,
        pending_delete_invalidations,
        0,
        None,
    )
}

fn persist_rollup_state_budgeted(
    path: Option<&Path>,
    checkpoints: &HashMap<String, BTreeMap<String, i64>>,
    generations: &HashMap<String, u64>,
    pending_materializations: &HashMap<String, BTreeMap<String, PendingRollupMaterialization>>,
    pending_delete_invalidations: &[PendingRollupDeleteInvalidation],
    journal_epoch: u64,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let Some(path) = path else {
        if checkpoints.is_empty()
            && generations.is_empty()
            && pending_materializations.is_empty()
            && pending_delete_invalidations.is_empty()
        {
            return Ok(());
        }
        return Err(TsinkError::InvalidConfiguration(
            "rollup state requires persistent storage".to_string(),
        ));
    };

    let encoded = encode_rollup_state_with_epoch(
        checkpoints,
        generations,
        pending_materializations,
        pending_delete_invalidations,
        journal_epoch,
    )?;
    persist_encoded_rollup_state_budgeted(
        path,
        &encoded,
        local_disk_budget,
        crate::DiskReservationKind::Growth,
    )
}

fn charge_rollup_snapshot_item(
    selected_items: &mut usize,
    selected_bytes: &mut usize,
    modeled_bytes: usize,
) -> Result<()> {
    *selected_items = selected_items.saturating_add(1);
    *selected_bytes = selected_bytes.saturating_add(modeled_bytes);
    if *selected_items > ROLLUP_STATE_SNAPSHOT_MAX_ITEMS
        || *selected_bytes > ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES
    {
        return Err(TsinkError::MaintenanceDependencyWindowExceeded {
            operation: "rollup state snapshot encoding",
            item_limit: ROLLUP_STATE_SNAPSHOT_MAX_ITEMS,
            byte_limit: ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES as u64,
            selected_items: *selected_items,
            selected_bytes: u64::try_from(*selected_bytes).unwrap_or(u64::MAX),
        });
    }
    Ok(())
}

fn ensure_rollup_snapshot_within_limits(
    policies: &[RollupPolicy],
    checkpoints: &HashMap<String, BTreeMap<String, i64>>,
    generations: &HashMap<String, u64>,
    pending_materializations: &HashMap<String, BTreeMap<String, PendingRollupMaterialization>>,
    pending_delete_invalidations: &[PendingRollupDeleteInvalidation],
) -> Result<()> {
    let mut selected_items = 0usize;
    let mut selected_bytes = 512usize;
    for policy in policies {
        charge_rollup_snapshot_item(
            &mut selected_items,
            &mut selected_bytes,
            policy
                .id
                .len()
                .saturating_add(policy.metric.len())
                .saturating_mul(6)
                .saturating_add(192),
        )?;
        for label in &policy.match_labels {
            charge_rollup_snapshot_item(
                &mut selected_items,
                &mut selected_bytes,
                label
                    .name
                    .len()
                    .saturating_add(label.value.len())
                    .saturating_mul(6)
                    .saturating_add(64),
            )?;
        }
    }
    for (policy_id, entries) in checkpoints {
        for source_key in entries.keys() {
            charge_rollup_snapshot_item(
                &mut selected_items,
                &mut selected_bytes,
                policy_id
                    .len()
                    .saturating_add(source_key.len())
                    .saturating_mul(6)
                    .saturating_add(128),
            )?;
        }
    }
    for policy_id in generations.keys() {
        charge_rollup_snapshot_item(
            &mut selected_items,
            &mut selected_bytes,
            policy_id.len().saturating_mul(6).saturating_add(96),
        )?;
    }
    for (policy_id, entries) in pending_materializations {
        for source_key in entries.keys() {
            charge_rollup_snapshot_item(
                &mut selected_items,
                &mut selected_bytes,
                policy_id
                    .len()
                    .saturating_add(source_key.len())
                    .saturating_mul(6)
                    .saturating_add(192),
            )?;
        }
    }
    for pending in pending_delete_invalidations {
        charge_rollup_snapshot_item(&mut selected_items, &mut selected_bytes, 192)?;
        for _ in &pending.series_ids {
            charge_rollup_snapshot_item(&mut selected_items, &mut selected_bytes, 32)?;
        }
        for policy_id in &pending.affected_policy_ids {
            charge_rollup_snapshot_item(
                &mut selected_items,
                &mut selected_bytes,
                policy_id.len().saturating_mul(6).saturating_add(32),
            )?;
        }
    }
    Ok(())
}

pub(super) fn ensure_rollup_policies_within_limits(policies: &[RollupPolicy]) -> Result<()> {
    ensure_rollup_snapshot_within_limits(
        policies,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        &[],
    )
}

pub(super) fn encode_rollup_state_with_epoch(
    checkpoints: &HashMap<String, BTreeMap<String, i64>>,
    generations: &HashMap<String, u64>,
    pending_materializations: &HashMap<String, BTreeMap<String, PendingRollupMaterialization>>,
    pending_delete_invalidations: &[PendingRollupDeleteInvalidation],
    journal_epoch: u64,
) -> Result<Vec<u8>> {
    ensure_rollup_snapshot_within_limits(
        &[],
        checkpoints,
        generations,
        pending_materializations,
        pending_delete_invalidations,
    )?;
    let mut flattened = Vec::new();
    let mut policy_ids = checkpoints.keys().cloned().collect::<Vec<_>>();
    policy_ids.sort();
    for policy_id in policy_ids {
        let Some(entries) = checkpoints.get(&policy_id) else {
            continue;
        };
        for (source_key, materialized_through) in entries {
            flattened.push(PersistedRollupCheckpoint {
                policy_id: policy_id.clone(),
                source_key: source_key.clone(),
                materialized_through: *materialized_through,
            });
        }
    }

    let mut persisted_generations = generations
        .iter()
        .map(|(policy_id, generation)| PersistedRollupGeneration {
            policy_id: policy_id.clone(),
            generation: *generation,
        })
        .collect::<Vec<_>>();
    persisted_generations.sort_by(|left, right| left.policy_id.cmp(&right.policy_id));

    let mut persisted_pending = Vec::new();
    let mut pending_policy_ids = pending_materializations.keys().cloned().collect::<Vec<_>>();
    pending_policy_ids.sort();
    for policy_id in pending_policy_ids {
        let Some(entries) = pending_materializations.get(&policy_id) else {
            continue;
        };
        for (source_key, pending) in entries {
            persisted_pending.push(PersistedPendingRollupMaterialization {
                policy_id: policy_id.clone(),
                source_key: source_key.clone(),
                checkpoint: pending.checkpoint,
                materialized_through: pending.materialized_through,
                generation: pending.generation,
            });
        }
    }

    let mut normalized_pending_delete_invalidations = pending_delete_invalidations.to_vec();
    normalize_pending_delete_invalidations(&mut normalized_pending_delete_invalidations);
    let persisted_pending_delete_invalidations = normalized_pending_delete_invalidations
        .into_iter()
        .map(|pending| PersistedPendingRollupDeleteInvalidation {
            tombstone: pending.tombstone,
            series_ids: pending.series_ids,
            affected_policy_ids: pending.affected_policy_ids,
        })
        .collect::<Vec<_>>();

    let payload = PersistedRollupStateFile {
        magic: ROLLUP_STATE_MAGIC.to_string(),
        version: ROLLUP_SCHEMA_VERSION,
        journal_epoch,
        checkpoints: flattened,
        pending_materializations: persisted_pending,
        pending_delete_invalidations: persisted_pending_delete_invalidations,
        generations: persisted_generations,
    };
    Ok(serde_json::to_vec_pretty(&payload)?)
}

fn persist_encoded_rollup_state_budgeted(
    path: &Path,
    encoded: &[u8],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    write_file_atomically_and_sync_parent_budgeted(
        path,
        encoded,
        local_disk_budget,
        crate::DiskCategory::Rollups,
        reservation_kind,
    )
}

fn with_staged_rollup_snapshot_replacements<T, F>(
    replacements: &[crate::ManagedFileReplacement<'_>],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
    publish: F,
) -> Result<T>
where
    F: FnOnce(&mut crate::StagedManagedFileReplacements) -> Result<T>,
{
    match (local_disk_budget, reservation_kind) {
        (Some(budget), crate::DiskReservationKind::Growth) => budget
            .with_staged_managed_file_replacements(
                replacements,
                crate::DiskCategory::Rollups,
                publish,
            ),
        (Some(budget), crate::DiskReservationKind::Recovery) => budget
            .with_staged_managed_file_replacements_for_recovery(
                replacements,
                crate::DiskCategory::Rollups,
                publish,
            ),
        (Some(_), crate::DiskReservationKind::Maintenance) => {
            Err(TsinkError::InvalidConfiguration(
                "rollup snapshot pairs do not support maintenance admission".to_string(),
            ))
        }
        (None, _) => crate::with_staged_file_replacements(replacements, publish),
    }
}

pub(super) fn normalize_pending_delete_invalidations(
    pending_delete_invalidations: &mut Vec<PendingRollupDeleteInvalidation>,
) {
    for pending in pending_delete_invalidations.iter_mut() {
        pending.series_ids.sort_unstable();
        pending.series_ids.dedup();
        pending.affected_policy_ids.sort();
        pending.affected_policy_ids.dedup();
    }
    pending_delete_invalidations.retain(|pending| {
        pending.tombstone.start < pending.tombstone.end
            && !pending.series_ids.is_empty()
            && !pending.affected_policy_ids.is_empty()
    });
    pending_delete_invalidations.sort_by(|left, right| {
        (
            left.tombstone.start,
            left.tombstone.end,
            &left.series_ids,
            &left.affected_policy_ids,
        )
            .cmp(&(
                right.tombstone.start,
                right.tombstone.end,
                &right.series_ids,
                &right.affected_policy_ids,
            ))
    });
    pending_delete_invalidations.dedup();
}

pub(super) fn build_pending_delete_invalidation(
    tombstone: TombstoneRange,
    series_ids: &[SeriesId],
    affected_policy_ids: &BTreeSet<String>,
) -> Option<PendingRollupDeleteInvalidation> {
    let pending = PendingRollupDeleteInvalidation {
        tombstone,
        series_ids: series_ids.to_vec(),
        affected_policy_ids: affected_policy_ids.iter().cloned().collect(),
    };
    let mut entries = vec![pending];
    normalize_pending_delete_invalidations(&mut entries);
    entries.pop()
}

fn tombstone_range_is_fully_covered(
    pending_range: TombstoneRange,
    applied_ranges: &[TombstoneRange],
) -> bool {
    let mut covered_until = pending_range.start;
    for applied_range in applied_ranges {
        if applied_range.end <= covered_until {
            continue;
        }
        if applied_range.start > covered_until {
            return false;
        }
        covered_until = applied_range.end;
        if covered_until >= pending_range.end {
            return true;
        }
    }
    covered_until >= pending_range.end
}

fn pending_delete_is_committed(
    tombstones: &TombstoneMap,
    pending: &PendingRollupDeleteInvalidation,
) -> bool {
    pending.series_ids.iter().all(|series_id| {
        tombstones
            .get(series_id)
            .is_some_and(|ranges| tombstone_range_is_fully_covered(pending.tombstone, ranges))
    })
}

fn tombstone_range_overlaps_window(range: TombstoneRange, start: i64, end: i64) -> bool {
    range.start < end && range.end > start
}

pub(super) fn pending_delete_blocks_rollup_candidate(
    pending_delete_invalidations: &[PendingRollupDeleteInvalidation],
    series_id: SeriesId,
    policy_id: &str,
    start: i64,
    end: i64,
) -> bool {
    pending_delete_invalidations.iter().any(|pending| {
        pending.series_ids.binary_search(&series_id).is_ok()
            && pending
                .affected_policy_ids
                .binary_search_by(|candidate| candidate.as_str().cmp(policy_id))
                .is_ok()
            && tombstone_range_overlaps_window(pending.tombstone, start, end)
    })
}

pub(super) fn invalidate_rollup_state_for_policy_ids(
    checkpoints: &mut HashMap<String, BTreeMap<String, i64>>,
    generations: &mut HashMap<String, u64>,
    pending_materializations: &mut HashMap<String, BTreeMap<String, PendingRollupMaterialization>>,
    policy_stats: Option<&mut BTreeMap<String, PolicyRunState>>,
    affected_policy_ids: &BTreeSet<String>,
) {
    if affected_policy_ids.is_empty() {
        return;
    }

    for policy_id in affected_policy_ids {
        let generation = generations.entry(policy_id.clone()).or_insert(0);
        *generation = generation.saturating_add(1);
    }
    for policy_id in affected_policy_ids {
        checkpoints.remove(policy_id);
        pending_materializations.remove(policy_id);
    }
    if let Some(policy_stats) = policy_stats {
        for policy_id in affected_policy_ids {
            let state = policy_stats.entry(policy_id.clone()).or_default();
            state.matched_series = 0;
            state.materialized_series = 0;
            state.materialized_through = None;
            state.source_traversal_complete = false;
            state.last_error = None;
        }
    }
}

pub(super) fn pending_materialized_through(
    pending_materializations: &HashMap<String, BTreeMap<String, PendingRollupMaterialization>>,
    generations: &HashMap<String, u64>,
    policy_id: &str,
    source_key: &str,
) -> Option<i64> {
    let current_generation = generations.get(policy_id).copied().unwrap_or(0);
    pending_materializations
        .get(policy_id)
        .and_then(|entries| entries.get(source_key))
        .filter(|pending| pending.generation == current_generation)
        .map(|pending| pending.materialized_through)
}

impl<'a> RollupStateStoreContext<'a> {
    pub(super) fn dir_path(self) -> Option<&'a Path> {
        self.state.dir_path()
    }

    pub(super) fn ensure_snapshot_mutations_unfenced(self) -> Result<()> {
        if self
            .state
            .snapshot_publication_fenced
            .load(Ordering::Acquire)
        {
            return Err(TsinkError::Other(
                "rollup snapshot mutations are fenced after an indeterminate publication; reopen storage to reload the durable state/policy pair before retrying"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn policies_snapshot(self) -> Vec<RollupPolicy> {
        self.state.policies.read().clone()
    }

    pub(super) fn checkpoints_snapshot(self) -> HashMap<String, BTreeMap<String, i64>> {
        self.state.checkpoints.read().clone()
    }

    pub(super) fn pending_materializations_snapshot(
        self,
    ) -> HashMap<String, BTreeMap<String, PendingRollupMaterialization>> {
        self.state.pending_materializations.read().clone()
    }

    pub(super) fn generations_snapshot(self) -> HashMap<String, u64> {
        self.state.generations.read().clone()
    }

    pub(super) fn policy_stats_snapshot(self) -> BTreeMap<String, PolicyRunState> {
        self.state.policy_stats.read().clone()
    }

    fn load_policies(self) -> Result<Vec<RollupPolicy>> {
        load_rollup_policies(self.state.policies_path())
    }

    fn load_state(self) -> Result<LoadedRollupState> {
        load_rollup_state(self.state.state_path())
    }

    #[cfg(test)]
    pub(super) fn invoke_policy_start_hook(self, policy: &RollupPolicy) {
        self.state.invoke_policy_start_hook(policy);
    }

    #[cfg(test)]
    pub(super) fn set_policy_start_hook<F>(self, hook: F)
    where
        F: Fn(&RollupPolicy) + Send + Sync + 'static,
    {
        self.state.set_policy_start_hook(hook);
    }

    #[cfg(test)]
    pub(super) fn clear_policy_start_hook(self) {
        self.state.clear_policy_start_hook();
    }

    #[cfg(test)]
    pub(super) fn set_state_persist_hook<F>(self, hook: F)
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        self.state.set_state_persist_hook(hook);
    }

    #[cfg(test)]
    pub(super) fn clear_state_persist_hook(self) {
        self.state.clear_state_persist_hook();
    }

    pub(super) fn persist_state_snapshot(
        self,
        checkpoints: &HashMap<String, BTreeMap<String, i64>>,
        generations: &HashMap<String, u64>,
        pending_materializations: &HashMap<String, BTreeMap<String, PendingRollupMaterialization>>,
        pending_delete_invalidations: &[PendingRollupDeleteInvalidation],
    ) -> Result<()> {
        self.ensure_snapshot_mutations_unfenced()?;
        let journal_epoch = self.state.journal_epoch.load(Ordering::Acquire);
        #[cfg(test)]
        self.state.invoke_state_persist_hook()?;
        persist_rollup_state_budgeted(
            self.state.state_path(),
            checkpoints,
            generations,
            pending_materializations,
            pending_delete_invalidations,
            journal_epoch,
            self.state.local_disk_budget.as_ref(),
        )?;
        super::state_journal::cleanup_rollup_state_journal(
            self.state.dir_path(),
            self.state.local_disk_budget.as_ref(),
        )
    }

    pub(super) fn persist_source_state_event(
        self,
        event: super::state_journal::RollupSourceStateEvent,
    ) -> Result<()> {
        self.ensure_snapshot_mutations_unfenced()?;
        #[cfg(test)]
        self.state.invoke_state_persist_hook()?;
        super::state_journal::persist_rollup_source_state_event(
            self.state.dir_path(),
            event,
            self.state.local_disk_budget.as_ref(),
        )
    }

    pub(super) fn capture_snapshot(self) -> Result<RollupRuntimeSnapshot> {
        let _visibility = self.state.snapshot_visibility.read();
        let policies = self.state.policies.read();
        let checkpoints = self.state.checkpoints.read();
        let pending_materializations = self.state.pending_materializations.read();
        let pending_delete_invalidations = self.state.pending_delete_invalidations.read();
        let generations = self.state.generations.read();
        let policy_stats = self.state.policy_stats.read();
        ensure_rollup_snapshot_within_limits(
            &policies,
            &checkpoints,
            &generations,
            &pending_materializations,
            &pending_delete_invalidations,
        )?;
        Ok(RollupRuntimeSnapshot {
            policies: policies.clone(),
            checkpoints: checkpoints.clone(),
            pending_materializations: pending_materializations.clone(),
            pending_delete_invalidations: pending_delete_invalidations.clone(),
            generations: generations.clone(),
            policy_stats: policy_stats.clone(),
        })
    }

    pub(super) fn persist_snapshot(
        self,
        snapshot: &RollupRuntimeSnapshot,
    ) -> Result<RollupSnapshotPersistenceOutcome> {
        self.persist_snapshot_with_kind(snapshot, crate::DiskReservationKind::Growth)
    }

    fn persist_snapshot_with_kind(
        self,
        snapshot: &RollupRuntimeSnapshot,
        reservation_kind: crate::DiskReservationKind,
    ) -> Result<RollupSnapshotPersistenceOutcome> {
        self.ensure_snapshot_mutations_unfenced()?;

        ensure_rollup_snapshot_within_limits(
            &snapshot.policies,
            &snapshot.checkpoints,
            &snapshot.generations,
            &snapshot.pending_materializations,
            &snapshot.pending_delete_invalidations,
        )?;
        let next_epoch = self
            .state
            .journal_epoch
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or_else(|| TsinkError::Other("rollup state journal epoch exhausted".to_string()))?;
        let encoded_policies = encode_rollup_policies(&snapshot.policies)?;
        let encoded_state = encode_rollup_state_with_epoch(
            &snapshot.checkpoints,
            &snapshot.generations,
            &snapshot.pending_materializations,
            &snapshot.pending_delete_invalidations,
            next_epoch,
        )?;

        let Some(policies_path) = self.state.policies_path() else {
            if snapshot.policies.is_empty()
                && snapshot.checkpoints.is_empty()
                && snapshot.generations.is_empty()
                && snapshot.pending_materializations.is_empty()
                && snapshot.pending_delete_invalidations.is_empty()
            {
                return Ok(RollupSnapshotPersistenceOutcome::Clean);
            }
            return Err(TsinkError::InvalidConfiguration(
                "rollup snapshots require persistent storage".to_string(),
            ));
        };
        let Some(state_path) = self.state.state_path() else {
            return Err(TsinkError::InvalidConfiguration(
                "rollup snapshots require persistent storage".to_string(),
            ));
        };

        // These two files form one logical snapshot. Admit and fully stage the complete pair
        // before either target is replaced. State is the conservative, invalidating half and must
        // remain first: an interruption can therefore expose either the predecessor pair, the
        // predecessor policies with candidate invalidation state, or the complete candidate pair.
        // Every one of those combinations is safe on reopen; candidate policies can never become
        // durable while predecessor checkpoints remain reusable. If publication is indeterminate,
        // the fence below also prevents a later snapshot from being derived from stale in-memory
        // predecessor state before reopen resolves which pair became durable.
        let replacements = [
            crate::ManagedFileReplacement::new(state_path, &encoded_state),
            crate::ManagedFileReplacement::new(policies_path, &encoded_policies),
        ];
        let mut state_published = false;
        let mut state_durable = false;
        let mut policies_published = false;
        let mut policies_durable = false;
        let mut publication_ambiguous = false;
        // Arm the fence before staging begins. An unwind then remains conservative even if it
        // happens after a rename but before the publication outcome can be classified below.
        // Ordinary pre-publication failures and proven complete commits clear it again.
        self.state
            .snapshot_publication_fenced
            .store(true, Ordering::Release);
        let persist_result = with_staged_rollup_snapshot_replacements(
            &replacements,
            self.state.local_disk_budget.as_ref(),
            reservation_kind,
            |staged| {
                let state_result = staged.publish(0);
                state_published = staged.is_published(0);
                publication_ambiguous |= staged.publication_ambiguous();
                state_result?;
                state_durable = true;
                self.state
                    .journal_epoch
                    .store(next_epoch, Ordering::Release);

                #[cfg(test)]
                self.state.invoke_state_persist_hook()?;

                let policies_result = staged.publish(1);
                policies_published = staged.is_published(1);
                publication_ambiguous |= staged.publication_ambiguous();
                policies_result?;
                policies_durable = true;

                #[cfg(test)]
                self.state
                    .invoke_policy_persist_hook(RollupPolicyPersistHookPoint::CandidatePublished)?;
                Ok(())
            },
        );

        match persist_result {
            Ok(()) => {
                let cleanup_result = super::state_journal::cleanup_rollup_state_journal(
                    self.state.dir_path(),
                    self.state.local_disk_budget.as_ref(),
                );
                self.state
                    .snapshot_publication_fenced
                    .store(false, Ordering::Release);
                match cleanup_result {
                    Ok(()) => Ok(RollupSnapshotPersistenceOutcome::Clean),
                    Err(error) => Ok(
                        RollupSnapshotPersistenceOutcome::CommittedWithCleanupDebt(error),
                    ),
                }
            }
            Err(error) if state_durable && policies_durable => {
                self.state
                    .snapshot_publication_fenced
                    .store(false, Ordering::Release);
                Ok(RollupSnapshotPersistenceOutcome::CommittedWithCleanupDebt(
                    error,
                ))
            }
            Err(error)
                if state_published || policies_published || publication_ambiguous =>
            {
                Err(TsinkError::Other(format!(
                    "rollup snapshot publication outcome is indeterminate after ordered state-first publication; the durable invalidation ordering remains safe for reopen, and further in-process mutations are fenced: {error}"
                )))
            }
            Err(error) => {
                self.state
                    .snapshot_publication_fenced
                    .store(false, Ordering::Release);
                Err(error)
            }
        }
    }

    pub(super) fn install_snapshot(self, snapshot: RollupRuntimeSnapshot) {
        // Query selection holds the matching read guard while it reads the component locks. Keep
        // the whole in-memory replacement invisible until every component is installed, and put
        // policies last so even non-query readers see the conservative invalidating state first.
        let _visibility = self.state.snapshot_visibility.write();
        *self.state.checkpoints.write() = snapshot.checkpoints;
        *self.state.pending_materializations.write() = snapshot.pending_materializations;
        *self.state.pending_delete_invalidations.write() = snapshot.pending_delete_invalidations;
        *self.state.generations.write() = snapshot.generations;
        *self.state.policy_stats.write() = snapshot.policy_stats;
        *self.state.policies.write() = snapshot.policies;
    }

    pub(super) fn next_snapshot_for_policies(
        self,
        policies: Vec<RollupPolicy>,
    ) -> Result<RollupRuntimeSnapshot> {
        ensure_rollup_policies_within_limits(&policies)?;
        let mut snapshot = self.capture_snapshot()?;
        let current_policies = snapshot
            .policies
            .iter()
            .cloned()
            .map(|policy| (policy.id.clone(), policy))
            .collect::<HashMap<_, _>>();
        let active_policy_ids = policies
            .iter()
            .map(|policy| policy.id.clone())
            .collect::<BTreeSet<_>>();
        let updated_policy_ids = policies
            .iter()
            .filter_map(|policy| {
                current_policies
                    .get(&policy.id)
                    .filter(|current| *current != policy)
                    .map(|_| policy.id.clone())
            })
            .collect::<BTreeSet<_>>();

        snapshot.policies = policies;
        snapshot
            .checkpoints
            .retain(|policy_id, _| active_policy_ids.contains(policy_id));
        snapshot
            .pending_materializations
            .retain(|policy_id, _| active_policy_ids.contains(policy_id));
        for pending in &mut snapshot.pending_delete_invalidations {
            pending
                .affected_policy_ids
                .retain(|policy_id| active_policy_ids.contains(policy_id));
        }
        normalize_pending_delete_invalidations(&mut snapshot.pending_delete_invalidations);
        snapshot
            .generations
            .retain(|policy_id, _| active_policy_ids.contains(policy_id));
        snapshot
            .policy_stats
            .retain(|policy_id, _| active_policy_ids.contains(policy_id));

        for policy_id in &updated_policy_ids {
            snapshot.checkpoints.remove(policy_id);
            snapshot.pending_materializations.remove(policy_id);
            snapshot
                .policy_stats
                .insert(policy_id.clone(), PolicyRunState::default());
        }

        for policy in &snapshot.policies {
            let generation = snapshot.generations.entry(policy.id.clone()).or_insert(0);
            if updated_policy_ids.contains(&policy.id) {
                *generation = generation.saturating_add(1);
            }
            snapshot.policy_stats.entry(policy.id.clone()).or_default();
        }

        ensure_rollup_snapshot_within_limits(
            &snapshot.policies,
            &snapshot.checkpoints,
            &snapshot.generations,
            &snapshot.pending_materializations,
            &snapshot.pending_delete_invalidations,
        )?;
        Ok(snapshot)
    }
}

impl<'a> RollupRegistryReadContext<'a> {
    pub(super) fn has_series_for_metric(self, metric: &str) -> bool {
        self.registry.read().has_series_for_metric(metric)
    }

    pub(super) fn series_ids_for_metric_after(
        self,
        metric: &str,
        after: Option<SeriesId>,
        limit: usize,
    ) -> (Vec<SeriesId>, bool) {
        self.registry
            .read()
            .series_ids_for_metric_after(metric, after, limit)
    }

    pub(super) fn resolve_existing_series_id(
        self,
        metric: &str,
        labels: &[Label],
    ) -> Option<SeriesId> {
        self.registry
            .read()
            .resolve_existing(metric, labels)
            .map(|resolution| resolution.series_id)
    }

    pub(super) fn decode_series_key(self, series_id: SeriesId) -> Option<SeriesKey> {
        self.registry.read().decode_series_key(series_id)
    }
}

impl<'a> RollupDeleteRepairContext<'a> {
    fn repair_pending_delete_invalidations_on_load(
        self,
        state: &mut LoadedRollupState,
        active_policy_ids: &BTreeSet<String>,
    ) -> bool {
        for pending in state.pending_delete_invalidations.iter_mut() {
            pending
                .affected_policy_ids
                .retain(|policy_id| active_policy_ids.contains(policy_id));
        }
        normalize_pending_delete_invalidations(&mut state.pending_delete_invalidations);

        if state.pending_delete_invalidations.is_empty() {
            return false;
        }

        let tombstones = self.tombstones.read();
        let mut repaired_policy_ids = BTreeSet::new();
        for pending in &state.pending_delete_invalidations {
            if pending_delete_is_committed(&tombstones, pending) {
                repaired_policy_ids.extend(pending.affected_policy_ids.iter().cloned());
            }
        }
        drop(tombstones);

        let had_pending_delete_invalidations = !state.pending_delete_invalidations.is_empty();
        state.pending_delete_invalidations.clear();
        if !repaired_policy_ids.is_empty() {
            invalidate_rollup_state_for_policy_ids(
                &mut state.checkpoints,
                &mut state.generations,
                &mut state.pending_materializations,
                None,
                &repaired_policy_ids,
            );
        }
        had_pending_delete_invalidations
    }

    fn load_runtime_state(self) -> Result<()> {
        let policies = self.store.load_policies()?;
        let mut state = self.store.load_state()?;
        let active_ids = policies
            .iter()
            .map(|policy| policy.id.clone())
            .collect::<BTreeSet<_>>();
        state
            .checkpoints
            .retain(|policy_id, _| active_ids.contains(policy_id));
        state
            .generations
            .retain(|policy_id, _| active_ids.contains(policy_id));
        state
            .pending_materializations
            .retain(|policy_id, _| active_ids.contains(policy_id));
        for policy in &policies {
            state.generations.entry(policy.id.clone()).or_insert(0);
        }
        state.pending_materializations.retain(|policy_id, entries| {
            let current_generation = state.generations.get(policy_id).copied().unwrap_or(0);
            entries.retain(|_, pending| {
                pending.generation == current_generation
                    && pending.materialized_through > pending.checkpoint
            });
            !entries.is_empty()
        });
        let repaired_pending_delete_invalidations =
            self.repair_pending_delete_invalidations_on_load(&mut state, &active_ids);
        if repaired_pending_delete_invalidations {
            // The repaired state is installed in memory below even if persisting the cleanup
            // fails here; leaving the on-disk marker in place keeps the recovery retry-safe.
            let _ = self.store.persist_state_snapshot(
                &state.checkpoints,
                &state.generations,
                &state.pending_materializations,
                &state.pending_delete_invalidations,
            );
        }

        let LoadedRollupState {
            journal_epoch,
            checkpoints,
            pending_materializations,
            pending_delete_invalidations,
            generations,
        } = state;

        self.store
            .state
            .journal_epoch
            .store(journal_epoch, Ordering::Release);
        *self.store.state.policies.write() = policies.clone();
        *self.store.state.checkpoints.write() = checkpoints;
        *self.store.state.pending_materializations.write() = pending_materializations;
        *self.store.state.pending_delete_invalidations.write() = pending_delete_invalidations;
        *self.store.state.generations.write() = generations;
        let mut policy_stats = self.store.state.policy_stats.write();
        policy_stats.clear();
        for policy in policies {
            policy_stats.insert(policy.id, PolicyRunState::default());
        }
        Ok(())
    }
}

impl<'a> RollupInvalidationContext<'a> {
    fn affected_rollup_policy_ids_for_series(self, series_ids: &[SeriesId]) -> BTreeSet<String> {
        if series_ids.is_empty() {
            return BTreeSet::new();
        }

        let policies = self.store.policies_snapshot();
        if policies.is_empty() {
            return BTreeSet::new();
        }

        let mut affected_policy_ids = BTreeSet::new();
        for series_id in series_ids {
            let Some(series) = self.registry.decode_series_key(*series_id) else {
                continue;
            };
            if is_internal_rollup_metric(&series.metric) {
                continue;
            }

            for policy in &policies {
                if policy_matches_source(policy, &series.metric, &series.labels) {
                    affected_policy_ids.insert(policy.id.clone());
                }
            }
        }

        affected_policy_ids
    }

    fn stage_pending_rollup_delete_invalidation(
        self,
        tombstone: TombstoneRange,
        series_ids: &[SeriesId],
        affected_policy_ids: &BTreeSet<String>,
    ) -> Result<()> {
        let Some(pending_delete) =
            build_pending_delete_invalidation(tombstone, series_ids, affected_policy_ids)
        else {
            return Ok(());
        };

        let mut snapshot = self.store.capture_snapshot()?;
        snapshot.pending_delete_invalidations.push(pending_delete);
        normalize_pending_delete_invalidations(&mut snapshot.pending_delete_invalidations);
        let persistence = self.store.persist_snapshot(&snapshot)?;
        self.store.install_snapshot(snapshot);
        persistence.report_cleanup_debt();
        Ok(())
    }

    fn finalize_pending_rollup_delete_invalidation(
        self,
        tombstone: TombstoneRange,
        series_ids: &[SeriesId],
        affected_policy_ids: &BTreeSet<String>,
    ) -> Result<()> {
        let Some(pending_delete) =
            build_pending_delete_invalidation(tombstone, series_ids, affected_policy_ids)
        else {
            return Ok(());
        };

        let mut snapshot = self.store.capture_snapshot()?;
        invalidate_rollup_state_for_policy_ids(
            &mut snapshot.checkpoints,
            &mut snapshot.generations,
            &mut snapshot.pending_materializations,
            Some(&mut snapshot.policy_stats),
            affected_policy_ids,
        );
        snapshot
            .pending_delete_invalidations
            .retain(|entry| entry != &pending_delete);

        let persist_result = self.store.persist_snapshot(&snapshot);
        self.store.install_snapshot(snapshot);
        persist_result.map(RollupSnapshotPersistenceOutcome::report_cleanup_debt)
    }

    fn cancel_pending_rollup_delete_invalidation(
        self,
        tombstone: TombstoneRange,
        series_ids: &[SeriesId],
        affected_policy_ids: &BTreeSet<String>,
    ) -> Result<()> {
        let Some(pending_delete) =
            build_pending_delete_invalidation(tombstone, series_ids, affected_policy_ids)
        else {
            return Ok(());
        };

        let mut snapshot = self.store.capture_snapshot()?;
        let previous_len = snapshot.pending_delete_invalidations.len();
        snapshot
            .pending_delete_invalidations
            .retain(|entry| entry != &pending_delete);
        if snapshot.pending_delete_invalidations.len() == previous_len {
            return Ok(());
        }

        // Cancellation restores the pre-delete state after tombstone persistence rejected the
        // operation. It is recovery work, so it must remain possible while the logical data root
        // is at or above its normal-growth limit.
        let persistence = self
            .store
            .persist_snapshot_with_kind(&snapshot, crate::DiskReservationKind::Recovery)?;
        self.store.install_snapshot(snapshot);
        persistence.report_cleanup_debt();
        Ok(())
    }

    fn invalidate_rollup_policy_ids(self, affected_policy_ids: &BTreeSet<String>) -> Result<bool> {
        if affected_policy_ids.is_empty() {
            return Ok(false);
        }

        let mut snapshot = self.store.capture_snapshot()?;
        invalidate_rollup_state_for_policy_ids(
            &mut snapshot.checkpoints,
            &mut snapshot.generations,
            &mut snapshot.pending_materializations,
            Some(&mut snapshot.policy_stats),
            affected_policy_ids,
        );
        let persistence = self.store.persist_snapshot(&snapshot)?;
        self.store.install_snapshot(snapshot);
        persistence.report_cleanup_debt();
        Ok(true)
    }
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn with_rollup_run_lock<R>(
        &self,
        f: impl FnOnce() -> R,
    ) -> R {
        let _run_guard = self.rollup_run_coordination_context().run_lock.lock();
        f()
    }

    pub(in crate::engine) fn load_rollup_runtime_state(&self) -> Result<()> {
        self.rollup_delete_repair_context().load_runtime_state()
    }

    pub(in crate::engine) fn affected_rollup_policy_ids_for_series(
        &self,
        series_ids: &[SeriesId],
    ) -> BTreeSet<String> {
        self.rollup_invalidation_context()
            .affected_rollup_policy_ids_for_series(series_ids)
    }

    pub(in crate::engine) fn stage_pending_rollup_delete_invalidation(
        &self,
        tombstone: TombstoneRange,
        series_ids: &[SeriesId],
        affected_policy_ids: &BTreeSet<String>,
    ) -> Result<()> {
        self.rollup_invalidation_context()
            .stage_pending_rollup_delete_invalidation(tombstone, series_ids, affected_policy_ids)
    }

    pub(in crate::engine) fn finalize_pending_rollup_delete_invalidation(
        &self,
        tombstone: TombstoneRange,
        series_ids: &[SeriesId],
        affected_policy_ids: &BTreeSet<String>,
    ) -> Result<()> {
        self.rollup_invalidation_context()
            .finalize_pending_rollup_delete_invalidation(tombstone, series_ids, affected_policy_ids)
    }

    pub(in crate::engine) fn cancel_pending_rollup_delete_invalidation(
        &self,
        tombstone: TombstoneRange,
        series_ids: &[SeriesId],
        affected_policy_ids: &BTreeSet<String>,
    ) -> Result<()> {
        self.rollup_invalidation_context()
            .cancel_pending_rollup_delete_invalidation(tombstone, series_ids, affected_policy_ids)
    }

    pub(in crate::engine) fn invalidate_rollup_policy_ids(
        &self,
        affected_policy_ids: &BTreeSet<String>,
    ) -> Result<bool> {
        self.rollup_invalidation_context()
            .invalidate_rollup_policy_ids(affected_policy_ids)
    }
}
