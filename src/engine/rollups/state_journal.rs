//! Bounded persistence journal for per-source rollup checkpoint state.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::*;
use crate::engine::binio::{
    append_u16, append_u32, append_u64, checksum32, read_array, read_to_end_bounded, read_u16,
    read_u32, read_u64,
};
use crate::engine::fs_utils::{
    collect_directory_entries_bounded, remove_path_if_exists_and_sync_parent_budgeted,
    rename_and_sync_parents, write_file_atomically_and_sync_parent_budgeted,
    MAX_RECOVERY_NAMESPACE_ENTRIES,
};

const ROLLUP_STATE_JOURNAL_ACTIVE_FILE_NAME: &str = "state-journal-active.bin";
const ROLLUP_STATE_JOURNAL_SEGMENT_PREFIX: &str = "state-journal-";
const ROLLUP_STATE_JOURNAL_SEGMENT_SUFFIX: &str = ".bin";
const ROLLUP_STATE_JOURNAL_MAGIC: [u8; 4] = *b"RSJN";
const ROLLUP_STATE_JOURNAL_VERSION: u16 = 1;
const ROLLUP_STATE_JOURNAL_HEADER_LEN: usize = 24;
const ROLLUP_STATE_JOURNAL_MAX_EVENTS: usize = 1_024;
const ROLLUP_STATE_JOURNAL_MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
const ROLLUP_STATE_JOURNAL_MAX_GENERATIONS: usize = 1_024;

#[derive(Debug, Clone, Copy)]
struct RollupStateJournalLimits {
    max_events: usize,
    max_payload_bytes: usize,
    max_generations: usize,
}

impl Default for RollupStateJournalLimits {
    fn default() -> Self {
        Self {
            max_events: ROLLUP_STATE_JOURNAL_MAX_EVENTS,
            max_payload_bytes: ROLLUP_STATE_JOURNAL_MAX_PAYLOAD_BYTES,
            max_generations: ROLLUP_STATE_JOURNAL_MAX_GENERATIONS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct JournalPendingRollupMaterialization {
    checkpoint: i64,
    materialized_through: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RollupSourceStateEvent {
    epoch: u64,
    policy_id: String,
    source_key: String,
    generation: u64,
    checkpoint: Option<i64>,
    pending: Option<JournalPendingRollupMaterialization>,
}

impl RollupSourceStateEvent {
    pub(super) fn pending(
        epoch: u64,
        policy_id: &str,
        source_key: &str,
        generation: u64,
        checkpoint: Option<i64>,
        pending: &PendingRollupMaterialization,
    ) -> Self {
        Self {
            epoch,
            policy_id: policy_id.to_string(),
            source_key: source_key.to_string(),
            generation,
            checkpoint,
            pending: Some(JournalPendingRollupMaterialization {
                checkpoint: pending.checkpoint,
                materialized_through: pending.materialized_through,
            }),
        }
    }

    pub(super) fn completed(
        epoch: u64,
        policy_id: &str,
        source_key: &str,
        generation: u64,
        materialized_through: i64,
    ) -> Self {
        Self {
            epoch,
            policy_id: policy_id.to_string(),
            source_key: source_key.to_string(),
            generation,
            checkpoint: Some(materialized_through),
            pending: None,
        }
    }

    fn modeled_bytes(&self) -> usize {
        self.policy_id
            .len()
            .saturating_add(self.source_key.len())
            .saturating_mul(6)
            .saturating_add(256)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RollupStateJournalHeader {
    event_count: usize,
    payload_len: usize,
    payload_crc32: u32,
}

#[derive(Debug)]
struct RollupStateJournalDiscovery {
    sealed: Vec<(u64, PathBuf)>,
    active: Option<PathBuf>,
    entry_count: usize,
}

fn journal_dir_path(rollup_dir: &Path) -> PathBuf {
    rollup_dir.to_path_buf()
}

fn active_journal_path(journal_dir: &Path) -> PathBuf {
    journal_dir.join(ROLLUP_STATE_JOURNAL_ACTIVE_FILE_NAME)
}

fn parse_segment_generation(name: &str) -> Option<u64> {
    let encoded = name
        .strip_prefix(ROLLUP_STATE_JOURNAL_SEGMENT_PREFIX)?
        .strip_suffix(ROLLUP_STATE_JOURNAL_SEGMENT_SUFFIX)?;
    if encoded.len() != 16 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(encoded, 16).ok()
}

fn discover_journal(journal_dir: &Path) -> Result<RollupStateJournalDiscovery> {
    let metadata = match fs::symlink_metadata(journal_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RollupStateJournalDiscovery {
                sealed: Vec::new(),
                active: None,
                entry_count: 0,
            });
        }
        Err(error) => return Err(error.into()),
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal path is link-like or not a directory: {}",
            journal_dir.display()
        )));
    }

    let entries = collect_directory_entries_bounded(
        journal_dir,
        MAX_RECOVERY_NAMESPACE_ENTRIES,
        "rollup state journal discovery",
    )?;
    let entry_count = entries.len();
    let mut sealed = Vec::new();
    let mut active = None;
    for entry in entries {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let recognized = name == ROLLUP_STATE_JOURNAL_ACTIVE_FILE_NAME
            || parse_segment_generation(name).is_some();
        if !recognized {
            continue;
        }
        let file_type = entry.file_type()?;
        if !file_type.is_file() || file_type.is_symlink() {
            return Err(TsinkError::DataCorruption(format!(
                "recognized rollup state journal entry is not a regular file: {}",
                entry.path().display()
            )));
        }
        if name == ROLLUP_STATE_JOURNAL_ACTIVE_FILE_NAME {
            active = Some(entry.path());
        } else if let Some(generation) = parse_segment_generation(name) {
            sealed.push((generation, entry.path()));
        }
    }
    sealed.sort_by_key(|(generation, _)| *generation);
    Ok(RollupStateJournalDiscovery {
        sealed,
        active,
        entry_count,
    })
}

fn parse_header(
    bytes: &[u8],
    limits: RollupStateJournalLimits,
) -> Result<RollupStateJournalHeader> {
    if bytes.len() < ROLLUP_STATE_JOURNAL_HEADER_LEN {
        return Err(TsinkError::DataCorruption(
            "rollup state journal header is truncated".to_string(),
        ));
    }
    let mut position = 0usize;
    if read_array::<4>(bytes, &mut position)? != ROLLUP_STATE_JOURNAL_MAGIC {
        return Err(TsinkError::DataCorruption(
            "rollup state journal magic mismatch".to_string(),
        ));
    }
    let version = read_u16(bytes, &mut position)?;
    if version != ROLLUP_STATE_JOURNAL_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported rollup state journal version {version}"
        )));
    }
    let flags = read_u16(bytes, &mut position)?;
    if flags != 0 {
        return Err(TsinkError::DataCorruption(format!(
            "invalid rollup state journal flags {flags:#06x}"
        )));
    }
    let event_count = usize::try_from(read_u32(bytes, &mut position)?).map_err(|_| {
        TsinkError::DataCorruption(
            "rollup state journal event count does not fit this platform".to_string(),
        )
    })?;
    let payload_len = usize::try_from(read_u64(bytes, &mut position)?).map_err(|_| {
        TsinkError::DataCorruption(
            "rollup state journal payload length does not fit this platform".to_string(),
        )
    })?;
    let payload_crc32 = read_u32(bytes, &mut position)?;
    debug_assert_eq!(position, ROLLUP_STATE_JOURNAL_HEADER_LEN);
    if event_count > limits.max_events || payload_len > limits.max_payload_bytes {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal header exceeds its bounded generation limits: {event_count} events, {payload_len} payload bytes"
        )));
    }
    Ok(RollupStateJournalHeader {
        event_count,
        payload_len,
        payload_crc32,
    })
}

fn encode_events(
    events: &[RollupSourceStateEvent],
    limits: RollupStateJournalLimits,
) -> Result<Option<Vec<u8>>> {
    if events.len() > limits.max_events {
        return Ok(None);
    }
    let payload = serde_json::to_vec(events)?;
    if payload.len() > limits.max_payload_bytes {
        return Ok(None);
    }
    let event_count = u32::try_from(events.len()).map_err(|_| {
        TsinkError::InvalidConfiguration("rollup state journal event count exceeds u32".to_string())
    })?;
    let payload_len = u64::try_from(payload.len()).map_err(|_| {
        TsinkError::InvalidConfiguration(
            "rollup state journal payload length exceeds u64".to_string(),
        )
    })?;
    let mut encoded = Vec::with_capacity(ROLLUP_STATE_JOURNAL_HEADER_LEN + payload.len());
    encoded.extend_from_slice(&ROLLUP_STATE_JOURNAL_MAGIC);
    append_u16(&mut encoded, ROLLUP_STATE_JOURNAL_VERSION);
    append_u16(&mut encoded, 0);
    append_u32(&mut encoded, event_count);
    append_u64(&mut encoded, payload_len);
    append_u32(&mut encoded, checksum32(&payload));
    debug_assert_eq!(encoded.len(), ROLLUP_STATE_JOURNAL_HEADER_LEN);
    encoded.extend_from_slice(&payload);
    Ok(Some(encoded))
}

fn read_events(
    path: &Path,
    limits: RollupStateJournalLimits,
) -> Result<Vec<RollupSourceStateEvent>> {
    let metadata = fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_file()
    {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal is link-like or not a regular file: {}",
            path.display()
        )));
    }
    let max_stored_bytes = ROLLUP_STATE_JOURNAL_HEADER_LEN
        .checked_add(limits.max_payload_bytes)
        .ok_or_else(|| {
            TsinkError::InvalidConfiguration("rollup state journal read limit overflow".to_string())
        })?;
    if metadata.len() > max_stored_bytes as u64 {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal file size {} exceeds the bounded generation limit {max_stored_bytes}",
            metadata.len()
        )));
    }
    let initial_capacity = usize::try_from(metadata.len())
        .unwrap_or(max_stored_bytes)
        .min(max_stored_bytes);
    let mut file = File::open(path)?;
    let bytes = read_to_end_bounded(
        &mut file,
        max_stored_bytes,
        initial_capacity,
        "rollup state journal generation",
    )?;
    let header = parse_header(&bytes, limits)?;
    let expected_len = ROLLUP_STATE_JOURNAL_HEADER_LEN
        .checked_add(header.payload_len)
        .ok_or_else(|| {
            TsinkError::DataCorruption("rollup state journal length overflow".to_string())
        })?;
    if expected_len != bytes.len() {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal length mismatch: header declares {expected_len} bytes, file has {}",
            bytes.len()
        )));
    }
    let payload = &bytes[ROLLUP_STATE_JOURNAL_HEADER_LEN..];
    let actual_crc32 = checksum32(payload);
    if actual_crc32 != header.payload_crc32 {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal checksum mismatch: expected {:08x}, got {actual_crc32:08x}",
            header.payload_crc32
        )));
    }
    let events: Vec<RollupSourceStateEvent> = serde_json::from_slice(payload)?;
    if events.len() != header.event_count {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal event count mismatch: header declares {}, payload contains {}",
            header.event_count,
            events.len()
        )));
    }
    Ok(events)
}

fn compact_events(
    events: impl IntoIterator<Item = RollupSourceStateEvent>,
    epoch: u64,
) -> Vec<RollupSourceStateEvent> {
    let mut latest = BTreeMap::<(String, String), RollupSourceStateEvent>::new();
    for event in events {
        if event.epoch != epoch {
            continue;
        }
        latest.insert((event.policy_id.clone(), event.source_key.clone()), event);
    }
    latest.into_values().collect()
}

fn apply_event(state: &mut LoadedRollupState, event: RollupSourceStateEvent, epoch: u64) {
    if event.epoch != epoch
        || state
            .generations
            .get(&event.policy_id)
            .copied()
            .unwrap_or(0)
            != event.generation
    {
        return;
    }

    if let Some(checkpoint) = event.checkpoint {
        state
            .checkpoints
            .entry(event.policy_id.clone())
            .or_default()
            .insert(event.source_key.clone(), checkpoint);
    } else if let Some(entries) = state.checkpoints.get_mut(&event.policy_id) {
        entries.remove(&event.source_key);
        if entries.is_empty() {
            state.checkpoints.remove(&event.policy_id);
        }
    }

    if let Some(pending) = event.pending {
        state
            .pending_materializations
            .entry(event.policy_id.clone())
            .or_default()
            .insert(
                event.source_key.clone(),
                PendingRollupMaterialization {
                    checkpoint: pending.checkpoint,
                    materialized_through: pending.materialized_through,
                    generation: event.generation,
                },
            );
    } else if let Some(entries) = state.pending_materializations.get_mut(&event.policy_id) {
        entries.remove(&event.source_key);
        if entries.is_empty() {
            state.pending_materializations.remove(&event.policy_id);
        }
    }
}

pub(super) fn load_rollup_state_journal(
    rollup_dir: Option<&Path>,
    epoch: u64,
    state: &mut LoadedRollupState,
) -> Result<()> {
    let Some(rollup_dir) = rollup_dir else {
        return Ok(());
    };
    let journal_dir = journal_dir_path(rollup_dir);
    let discovery = discover_journal(&journal_dir)?;
    if discovery.sealed.len() + usize::from(discovery.active.is_some())
        > ROLLUP_STATE_JOURNAL_MAX_GENERATIONS
    {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal has more than {ROLLUP_STATE_JOURNAL_MAX_GENERATIONS} recognized generations"
        )));
    }
    for (_, path) in discovery.sealed {
        for event in read_events(&path, RollupStateJournalLimits::default())? {
            apply_event(state, event, epoch);
        }
    }
    if let Some(path) = discovery.active {
        for event in read_events(&path, RollupStateJournalLimits::default())? {
            apply_event(state, event, epoch);
        }
    }
    Ok(())
}

fn write_events(
    path: &Path,
    events: &[RollupSourceStateEvent],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    limits: RollupStateJournalLimits,
) -> Result<()> {
    let Some(encoded) = encode_events(events, limits)? else {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "rollup state journal generation",
            limit: u64::try_from(limits.max_payload_bytes).unwrap_or(u64::MAX),
            required: u64::try_from(events.iter().fold(0usize, |total, event| {
                total.saturating_add(event.modeled_bytes())
            }))
            .unwrap_or(u64::MAX),
        });
    };
    write_file_atomically_and_sync_parent_budgeted(
        path,
        &encoded,
        local_disk_budget,
        crate::DiskCategory::Rollups,
        crate::DiskReservationKind::Growth,
    )
}

fn compact_or_prune_one_generation_pair(
    discovery: &RollupStateJournalDiscovery,
    epoch: u64,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    limits: RollupStateJournalLimits,
) -> Result<bool> {
    let Some((_, oldest_path)) = discovery.sealed.first() else {
        return Ok(false);
    };
    let oldest = read_events(oldest_path, limits)?;
    let oldest_current = compact_events(oldest, epoch);
    if oldest_current.is_empty() {
        remove_path_if_exists_and_sync_parent_budgeted(
            oldest_path,
            local_disk_budget,
            crate::DiskCategory::Rollups,
        )?;
        return Ok(true);
    }

    let Some((_, newer_path)) = discovery.sealed.get(1) else {
        return Ok(false);
    };
    let newer = read_events(newer_path, limits)?;
    let compacted = compact_events(oldest_current.into_iter().chain(newer), epoch);
    let Some(encoded) = encode_events(&compacted, limits)? else {
        return Ok(false);
    };

    // Publish the combined newer generation before removing the older input. If cleanup is
    // interrupted, replay sees `older, combined`; source-state records are replacements, so that
    // duplicate prefix is equivalent to replaying the combined generation once.
    write_file_atomically_and_sync_parent_budgeted(
        newer_path,
        &encoded,
        local_disk_budget,
        crate::DiskCategory::Rollups,
        crate::DiskReservationKind::Growth,
    )?;
    remove_path_if_exists_and_sync_parent_budgeted(
        oldest_path,
        local_disk_budget,
        crate::DiskCategory::Rollups,
    )?;
    Ok(true)
}

fn persist_event_with_limits(
    rollup_dir: &Path,
    event: RollupSourceStateEvent,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    limits: RollupStateJournalLimits,
) -> Result<()> {
    if limits.max_events == 0 || limits.max_payload_bytes == 0 || limits.max_generations == 0 {
        return Err(TsinkError::InvalidConfiguration(
            "rollup state journal limits must be non-zero".to_string(),
        ));
    }
    if event.modeled_bytes() > limits.max_payload_bytes {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "rollup source state journal record",
            limit: u64::try_from(limits.max_payload_bytes).unwrap_or(u64::MAX),
            required: u64::try_from(event.modeled_bytes()).unwrap_or(u64::MAX),
        });
    }
    if encode_events(std::slice::from_ref(&event), limits)?.is_none() {
        return Err(TsinkError::MaintenanceWorkItemTooLarge {
            operation: "rollup source state journal record",
            limit: u64::try_from(limits.max_payload_bytes).unwrap_or(u64::MAX),
            required: u64::try_from(event.modeled_bytes()).unwrap_or(u64::MAX),
        });
    }

    let journal_dir = journal_dir_path(rollup_dir);
    if local_disk_budget.is_some()
        && matches!(
            fs::symlink_metadata(&journal_dir),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound
        )
    {
        return Err(TsinkError::InvalidConfiguration(
            "managed rollup state journal requires the durable rollup directory to be published first"
                .to_string(),
        ));
    }
    let mut discovery = discover_journal(&journal_dir)?;
    let active_path = active_journal_path(&journal_dir);
    if discovery.entry_count >= MAX_RECOVERY_NAMESPACE_ENTRIES {
        return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
            operation: "rollup state journal directory",
            limit: MAX_RECOVERY_NAMESPACE_ENTRIES,
            required: discovery.entry_count.saturating_add(1),
        });
    }
    if let Some(path) = discovery.active.as_deref() {
        let active_events = read_events(path, limits)?;
        let compacted = compact_events(
            active_events.into_iter().chain([event.clone()]),
            event.epoch,
        );
        if encode_events(&compacted, limits)?.is_some() {
            return write_events(path, &compacted, local_disk_budget, limits);
        }
    } else {
        let recognized = discovery.sealed.len();
        if recognized >= limits.max_generations
            && compact_or_prune_one_generation_pair(
                &discovery,
                event.epoch,
                local_disk_budget,
                limits,
            )?
        {
            discovery = discover_journal(&journal_dir)?;
        }
        if discovery.sealed.len() >= limits.max_generations {
            return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "rollup state journal",
                limit: limits.max_generations,
                required: discovery.sealed.len().saturating_add(1),
            });
        }
        return write_events(
            &active_path,
            std::slice::from_ref(&event),
            local_disk_budget,
            limits,
        );
    }

    if compact_or_prune_one_generation_pair(&discovery, event.epoch, local_disk_budget, limits)? {
        discovery = discover_journal(&journal_dir)?;
    }

    let recognized_generations = discovery.sealed.len() + usize::from(discovery.active.is_some());
    if recognized_generations >= limits.max_generations {
        return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
            operation: "rollup state journal",
            limit: limits.max_generations,
            required: recognized_generations.saturating_add(1),
        });
    }
    if discovery.entry_count >= MAX_RECOVERY_NAMESPACE_ENTRIES {
        return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
            operation: "rollup state journal directory",
            limit: MAX_RECOVERY_NAMESPACE_ENTRIES,
            required: discovery.entry_count.saturating_add(1),
        });
    }

    let next_generation = discovery
        .sealed
        .last()
        .map(|(generation, _)| generation.saturating_add(1))
        .unwrap_or(1);
    if next_generation == u64::MAX
        && discovery
            .sealed
            .last()
            .is_some_and(|(generation, _)| *generation == u64::MAX)
    {
        return Err(TsinkError::Other(
            "rollup state journal generation number exhausted".to_string(),
        ));
    }
    let sealed_path = journal_dir.join(format!(
        "{ROLLUP_STATE_JOURNAL_SEGMENT_PREFIX}{next_generation:016x}{ROLLUP_STATE_JOURNAL_SEGMENT_SUFFIX}"
    ));
    rename_and_sync_parents(&active_path, &sealed_path)?;
    write_events(
        &active_path,
        std::slice::from_ref(&event),
        local_disk_budget,
        limits,
    )
}

pub(super) fn persist_rollup_source_state_event(
    rollup_dir: Option<&Path>,
    event: RollupSourceStateEvent,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let Some(rollup_dir) = rollup_dir else {
        return Err(TsinkError::InvalidConfiguration(
            "rollup state requires persistent storage".to_string(),
        ));
    };
    persist_event_with_limits(
        rollup_dir,
        event,
        local_disk_budget,
        RollupStateJournalLimits::default(),
    )
}

pub(super) fn cleanup_rollup_state_journal(
    rollup_dir: Option<&Path>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let Some(rollup_dir) = rollup_dir else {
        return Ok(());
    };
    let journal_dir = journal_dir_path(rollup_dir);
    let discovery = discover_journal(&journal_dir)?;
    if discovery.sealed.len() + usize::from(discovery.active.is_some())
        > ROLLUP_STATE_JOURNAL_MAX_GENERATIONS
    {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal cleanup exceeds its {ROLLUP_STATE_JOURNAL_MAX_GENERATIONS}-generation bound"
        )));
    }
    for (_, path) in discovery.sealed {
        remove_path_if_exists_and_sync_parent_budgeted(
            &path,
            local_disk_budget,
            crate::DiskCategory::Rollups,
        )?;
    }
    if let Some(path) = discovery.active {
        remove_path_if_exists_and_sync_parent_budgeted(
            &path,
            local_disk_budget,
            crate::DiskCategory::Rollups,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn completed_event(epoch: u64, source_key: &str, checkpoint: i64) -> RollupSourceStateEvent {
        RollupSourceStateEvent::completed(epoch, "policy-a", source_key, 0, checkpoint)
    }

    fn load_test_state(rollup_dir: &Path, epoch: u64) -> LoadedRollupState {
        let mut state = LoadedRollupState {
            journal_epoch: epoch,
            generations: HashMap::from([("policy-a".to_string(), 0)]),
            ..LoadedRollupState::default()
        };
        load_rollup_state_journal(Some(rollup_dir), epoch, &mut state).unwrap();
        state
    }

    fn checkpoint(state: &LoadedRollupState, source_key: &str) -> Option<i64> {
        state
            .checkpoints
            .get("policy-a")
            .and_then(|entries| entries.get(source_key))
            .copied()
    }

    #[test]
    fn pending_and_completion_events_overlay_legacy_state_without_rewriting_it() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let state_path = rollup_dir.join(ROLLUP_STATE_FILE_NAME);
        let checkpoints = HashMap::from([(
            "policy-a".to_string(),
            BTreeMap::from([("cpu{host=\"a\"}".to_string(), 10)]),
        )]);
        let generations = HashMap::from([("policy-a".to_string(), 0)]);
        super::super::runtime::persist_rollup_state(
            Some(&state_path),
            &checkpoints,
            &generations,
            &HashMap::new(),
            &[],
        )
        .unwrap();
        let legacy_bytes = fs::read(&state_path).unwrap();
        let pending = PendingRollupMaterialization {
            checkpoint: 10,
            materialized_through: 20,
            generation: 0,
        };
        persist_rollup_source_state_event(
            Some(&rollup_dir),
            RollupSourceStateEvent::pending(
                0,
                "policy-a",
                "cpu{host=\"a\"}",
                0,
                Some(10),
                &pending,
            ),
            None,
        )
        .unwrap();

        let staged = super::super::runtime::load_rollup_state(Some(&state_path)).unwrap();
        assert_eq!(checkpoint(&staged, "cpu{host=\"a\"}"), Some(10));
        assert_eq!(
            staged.pending_materializations["policy-a"]["cpu{host=\"a\"}"],
            pending
        );
        assert_eq!(fs::read(&state_path).unwrap(), legacy_bytes);

        let completion = completed_event(0, "cpu{host=\"a\"}", 20);
        persist_rollup_source_state_event(Some(&rollup_dir), completion.clone(), None).unwrap();
        // Retrying after an ambiguous publication is idempotent: the active generation keeps the
        // latest complete source-state replacement rather than appending duplicate history.
        persist_rollup_source_state_event(Some(&rollup_dir), completion, None).unwrap();

        let completed = super::super::runtime::load_rollup_state(Some(&state_path)).unwrap();
        assert_eq!(checkpoint(&completed, "cpu{host=\"a\"}"), Some(20));
        assert!(completed.pending_materializations.is_empty());
        assert_eq!(fs::read(&state_path).unwrap(), legacy_bytes);
    }

    #[test]
    fn epoch_change_prevents_old_journal_from_resurrecting_readded_policy_state() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        persist_rollup_source_state_event(
            Some(&rollup_dir),
            completed_event(1, "cpu{host=\"old\"}", 90),
            None,
        )
        .unwrap();

        let state_path = rollup_dir.join(ROLLUP_STATE_FILE_NAME);
        let generations = HashMap::from([("policy-a".to_string(), 0)]);
        let encoded = super::super::runtime::encode_rollup_state_with_epoch(
            &HashMap::new(),
            &generations,
            &HashMap::new(),
            &[],
            3,
        )
        .unwrap();
        write_file_atomically_and_sync_parent_budgeted(
            &state_path,
            &encoded,
            None,
            crate::DiskCategory::Rollups,
            crate::DiskReservationKind::Growth,
        )
        .unwrap();

        let loaded = super::super::runtime::load_rollup_state(Some(&state_path)).unwrap();
        assert_eq!(loaded.journal_epoch, 3);
        assert!(loaded.checkpoints.is_empty());
        assert!(loaded.pending_materializations.is_empty());
    }

    #[test]
    fn bounded_rollover_compacts_repeated_source_replacements() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        let limits = RollupStateJournalLimits {
            max_events: 2,
            max_payload_bytes: 64 * 1024,
            max_generations: 3,
        };
        let epoch = 7;
        for event in [
            completed_event(epoch, "a", 1),
            completed_event(epoch, "b", 1),
            completed_event(epoch, "a", 2),
            completed_event(epoch, "b", 2),
            completed_event(epoch, "c", 1),
            completed_event(epoch, "d", 1),
            completed_event(epoch, "e", 1),
        ] {
            persist_event_with_limits(&rollup_dir, event, None, limits).unwrap();
        }

        let discovery = discover_journal(&journal_dir_path(&rollup_dir)).unwrap();
        assert_eq!(
            discovery.sealed.len() + usize::from(discovery.active.is_some()),
            3,
            "pair compaction must make room before the bounded namespace grows"
        );
        let loaded = load_test_state(&rollup_dir, epoch);
        assert_eq!(checkpoint(&loaded, "a"), Some(2));
        assert_eq!(checkpoint(&loaded, "b"), Some(2));
        assert_eq!(checkpoint(&loaded, "c"), Some(1));
        assert_eq!(checkpoint(&loaded, "d"), Some(1));
        assert_eq!(checkpoint(&loaded, "e"), Some(1));
    }

    #[test]
    fn bounded_namespace_rejects_distinct_live_generations_before_rollover() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        let limits = RollupStateJournalLimits {
            max_events: 1,
            max_payload_bytes: 64 * 1024,
            max_generations: 2,
        };
        persist_event_with_limits(&rollup_dir, completed_event(4, "a", 1), None, limits).unwrap();
        persist_event_with_limits(&rollup_dir, completed_event(4, "b", 1), None, limits).unwrap();
        let error =
            persist_event_with_limits(&rollup_dir, completed_event(4, "c", 1), None, limits)
                .expect_err("the third distinct generation must stop before namespace growth");
        assert!(matches!(
            error,
            TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "rollup state journal",
                limit: 2,
                required: 3,
            }
        ));
        let loaded = load_test_state(&rollup_dir, 4);
        assert_eq!(checkpoint(&loaded, "a"), Some(1));
        assert_eq!(checkpoint(&loaded, "b"), Some(1));
        assert_eq!(checkpoint(&loaded, "c"), None);
    }

    #[test]
    fn ambiguous_active_publication_replays_once_and_retry_is_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        let journal_dir = journal_dir_path(&rollup_dir);
        fs::create_dir_all(&journal_dir).unwrap();
        let _failure = crate::engine::fs_utils::fail_directory_sync_once(
            journal_dir.clone(),
            "injected rollup journal parent sync failure",
        );
        let event = completed_event(8, "a", 11);
        let error = persist_rollup_source_state_event(Some(&rollup_dir), event.clone(), None)
            .expect_err("a post-rename parent sync failure must be surfaced");
        assert!(error
            .to_string()
            .contains("injected rollup journal parent sync failure"));
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 8), "a"), Some(11));

        persist_rollup_source_state_event(Some(&rollup_dir), event, None).unwrap();
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 8), "a"), Some(11));
        let events = read_events(
            &active_journal_path(&journal_dir),
            RollupStateJournalLimits::default(),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn journal_rollover_and_compaction_leave_disk_accounting_exact() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let limits = RollupStateJournalLimits {
            max_events: 2,
            max_payload_bytes: 64 * 1024,
            max_generations: 3,
        };
        for event in [
            completed_event(9, "a", 1),
            completed_event(9, "b", 1),
            completed_event(9, "a", 2),
            completed_event(9, "b", 2),
            completed_event(9, "c", 1),
            completed_event(9, "d", 1),
            completed_event(9, "e", 1),
        ] {
            persist_event_with_limits(&rollup_dir, event, Some(&budget), limits).unwrap();
        }

        let journal_dir = journal_dir_path(&rollup_dir);
        let stored_bytes = fs::read_dir(&journal_dir)
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .sum::<u64>();
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, stored_bytes);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
    }

    #[test]
    fn first_journal_file_respects_tiny_managed_disk_headroom() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let event = completed_event(10, "a", 1);
        let encoded = encode_events(
            std::slice::from_ref(&event),
            RollupStateJournalLimits::default(),
        )
        .unwrap()
        .unwrap();
        let required = encoded.len() as u64;
        let budget = crate::LocalDiskBudget::open(
            temp_dir.path(),
            crate::LocalDiskLimits {
                max_bytes: Some(required - 1),
                ..crate::LocalDiskLimits::default()
            },
        )
        .unwrap();

        let error = persist_rollup_source_state_event(Some(&rollup_dir), event, Some(&budget))
            .expect_err("the first journal file must reserve its complete encoded size");
        assert!(matches!(
            error,
            TsinkError::DiskQuotaExceeded { requested, .. } if requested == required
        ));
        assert!(!active_journal_path(&rollup_dir).exists());
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
    }
}
