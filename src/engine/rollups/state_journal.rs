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
#[cfg(test)]
use crate::engine::fs_utils::rename_and_sync_parents;
#[cfg(test)]
use crate::engine::fs_utils::write_file_atomically_and_sync_parent_budgeted;
use crate::engine::fs_utils::{
    collect_directory_entries_bounded, remove_empty_dir_if_exists, remove_file_if_exists,
    rename_noreplace_and_sync_parents, rename_path_noreplace, sync_parent_dir,
    write_file_atomically_and_sync_parent_budgeted_with_reconciliation_memory_limit,
    write_tmp_and_sync, MAX_RECOVERY_NAMESPACE_ENTRIES,
};

const ROLLUP_STATE_JOURNAL_ACTIVE_FILE_NAME: &str = "state-journal-active.bin";
const ROLLUP_STATE_JOURNAL_SEGMENT_PREFIX: &str = "state-journal-";
const ROLLUP_STATE_JOURNAL_SEGMENT_SUFFIX: &str = ".bin";
const ROLLUP_STATE_JOURNAL_BATCH_PREFIX: &str = "state-journal-batch-";
const ROLLUP_STATE_JOURNAL_BATCH_SUFFIX: &str = ".d";
const ROLLUP_STATE_JOURNAL_EVENT_PREFIX: &str = "event-";
const ROLLUP_STATE_JOURNAL_EVENT_SUFFIX: &str = ".bin";
const ROLLUP_STATE_JOURNAL_CLEANUP_PREFIX: &str = ".state-journal-cleanup-";
const ROLLUP_STATE_JOURNAL_CLEANUP_SUFFIX: &str = ".d";
const ROLLUP_STATE_JOURNAL_MAGIC: [u8; 4] = *b"RSJN";
const ROLLUP_STATE_JOURNAL_VERSION: u16 = 1;
const ROLLUP_STATE_JOURNAL_HEADER_LEN: usize = 24;
const ROLLUP_STATE_JOURNAL_MAX_EVENTS: usize = 1_024;
const ROLLUP_STATE_JOURNAL_MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
const ROLLUP_STATE_JOURNAL_MAX_GENERATIONS: usize = 1_024;

#[cfg(test)]
thread_local! {
    static JOURNAL_EVENT_VEC_MATERIALIZATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_journal_event_vec_materializations() {
    JOURNAL_EVENT_VEC_MATERIALIZATIONS.with(|count| count.set(0));
}

#[cfg(test)]
fn journal_event_vec_materializations() -> u64 {
    JOURNAL_EVENT_VEC_MATERIALIZATIONS.with(std::cell::Cell::get)
}

#[derive(Debug, Clone, Copy)]
struct RollupStateJournalLimits {
    max_events: usize,
    max_payload_bytes: usize,
    max_generations: usize,
    max_namespace_entries: usize,
}

impl Default for RollupStateJournalLimits {
    fn default() -> Self {
        Self {
            max_events: ROLLUP_STATE_JOURNAL_MAX_EVENTS,
            max_payload_bytes: ROLLUP_STATE_JOURNAL_MAX_PAYLOAD_BYTES,
            max_generations: ROLLUP_STATE_JOURNAL_MAX_GENERATIONS,
            max_namespace_entries: MAX_RECOVERY_NAMESPACE_ENTRIES,
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
    top_level_entry_count: usize,
    entry_count: usize,
    owned_top_level_temporaries: Vec<PathBuf>,
    owned_event_temporaries: Vec<(PathBuf, Vec<PathBuf>, usize)>,
    owned_cleanup_directories: Vec<(PathBuf, Vec<PathBuf>)>,
}

impl RollupStateJournalDiscovery {
    fn sealed_generation_count(&self) -> usize {
        self.sealed
            .iter()
            .map(|(generation, _)| *generation)
            .fold((None, 0usize), |(previous, count), generation| {
                if previous == Some(generation) {
                    (previous, count)
                } else {
                    (Some(generation), count.saturating_add(1))
                }
            })
            .1
    }

    fn generation_count(&self) -> usize {
        self.sealed_generation_count() + usize::from(self.active.is_some())
    }
}

const ROLLUP_STATE_JOURNAL_MEMORY_FIXED_BYTES: usize = 4 * 1024;
const ROLLUP_STATE_JOURNAL_MODELED_PATH_COPIES: usize = 8;
const ROLLUP_STATE_JOURNAL_MODELED_PAYLOAD_COPIES: usize = 6;
const ROLLUP_STATE_JOURNAL_MODELED_EVENT_COLLECTION_COPIES: usize = 4;
const ROLLUP_STATE_JOURNAL_MODELED_ENTRY_OVERHEAD_BYTES: usize = 256;

fn rollup_state_journal_memory_model_overflow() -> TsinkError {
    TsinkError::Other("rollup state journal memory model overflow".to_string())
}

fn add_rollup_state_journal_modeled_bytes(total: &mut usize, added: usize) -> Result<()> {
    *total = total
        .checked_add(added)
        .ok_or_else(rollup_state_journal_memory_model_overflow)?;
    Ok(())
}

fn modeled_rollup_state_journal_discovery_bytes(
    top_level_entries: usize,
    observed_entries: usize,
    retained_path_bytes: usize,
    retained_name_bytes: usize,
) -> Result<usize> {
    let mut required = ROLLUP_STATE_JOURNAL_MEMORY_FIXED_BYTES;
    add_rollup_state_journal_modeled_bytes(
        &mut required,
        top_level_entries
            .checked_mul(std::mem::size_of::<fs::DirEntry>())
            .ok_or_else(rollup_state_journal_memory_model_overflow)?,
    )?;
    let per_entry_structures = ROLLUP_STATE_JOURNAL_MODELED_PATH_COPIES
        .checked_mul(std::mem::size_of::<PathBuf>())
        .and_then(|bytes| bytes.checked_add(ROLLUP_STATE_JOURNAL_MODELED_ENTRY_OVERHEAD_BYTES))
        .ok_or_else(rollup_state_journal_memory_model_overflow)?;
    add_rollup_state_journal_modeled_bytes(
        &mut required,
        observed_entries
            .checked_mul(per_entry_structures)
            .ok_or_else(rollup_state_journal_memory_model_overflow)?,
    )?;
    add_rollup_state_journal_modeled_bytes(
        &mut required,
        retained_path_bytes
            .checked_add(retained_name_bytes)
            .and_then(|bytes| bytes.checked_mul(ROLLUP_STATE_JOURNAL_MODELED_PATH_COPIES))
            .ok_or_else(rollup_state_journal_memory_model_overflow)?,
    )?;
    Ok(required)
}

fn admit_rollup_state_journal_required_memory(
    memory_limit_bytes: usize,
    retained_memory_bytes: usize,
    journal_required_bytes: usize,
) -> Result<()> {
    let required = retained_memory_bytes
        .checked_add(journal_required_bytes)
        .ok_or_else(rollup_state_journal_memory_model_overflow)?;
    crate::disk_budget::admit_startup_memory(memory_limit_bytes, required)
}

/// Admits one complete journal discovery/read/cleanup peak before recovery can mutate the
/// namespace. The scan itself streams directory entries; retained discovery paths, cleanup-plan
/// duplicates, decoded generations, compaction keys, JSON payloads, and atomic-write buffers are
/// conservatively represented by fixed copy factors below. Actual namespace and stored-payload
/// sizes keep an empty journal usable under a small custom budget, while the independent hard
/// namespace and frame limits remain the upper bounds on work.
fn admit_rollup_state_journal_memory(
    journal_dir: &Path,
    limits: RollupStateJournalLimits,
    memory_limit_bytes: usize,
    retained_memory_bytes: usize,
) -> Result<usize> {
    if memory_limit_bytes == usize::MAX {
        return Ok(0);
    }
    let metadata = match fs::symlink_metadata(journal_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: journal_dir.to_path_buf(),
                source,
            })
        }
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        // Discovery owns the more specific fail-closed error. This preflight must not follow a
        // link-like path merely to estimate memory.
        return Ok(0);
    }

    let mut observed_entries = 0usize;
    let mut top_level_entries = 0usize;
    let mut retained_path_bytes = 0usize;
    let mut retained_name_bytes = 0usize;
    let mut largest_generation_payload = 0usize;
    let mut largest_generation_events = 0usize;
    admit_rollup_state_journal_required_memory(
        memory_limit_bytes,
        retained_memory_bytes,
        ROLLUP_STATE_JOURNAL_MEMORY_FIXED_BYTES,
    )?;
    for entry in fs::read_dir(journal_dir).map_err(|source| TsinkError::IoWithPath {
        path: journal_dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: journal_dir.to_path_buf(),
            source,
        })?;
        observed_entries = observed_entries
            .checked_add(1)
            .ok_or_else(rollup_state_journal_memory_model_overflow)?;
        top_level_entries = top_level_entries
            .checked_add(1)
            .ok_or_else(rollup_state_journal_memory_model_overflow)?;
        if observed_entries > limits.max_namespace_entries {
            return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "rollup state journal memory preflight",
                limit: limits.max_namespace_entries,
                required: observed_entries,
            });
        }
        let name = entry.file_name();
        let name_bytes = name.as_encoded_bytes().len();
        add_rollup_state_journal_modeled_bytes(&mut retained_name_bytes, name_bytes)?;
        let path_bytes = journal_dir
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .checked_add(name_bytes)
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or_else(rollup_state_journal_memory_model_overflow)?;
        add_rollup_state_journal_modeled_bytes(&mut retained_path_bytes, path_bytes)?;
        admit_rollup_state_journal_required_memory(
            memory_limit_bytes,
            retained_memory_bytes,
            modeled_rollup_state_journal_discovery_bytes(
                top_level_entries,
                observed_entries,
                retained_path_bytes,
                retained_name_bytes,
            )?,
        )?;
        let path = journal_dir.join(&name);
        let Some(name) = name.to_str() else {
            continue;
        };
        let segment = parse_segment_generation(name).is_some()
            || name == ROLLUP_STATE_JOURNAL_ACTIVE_FILE_NAME;
        let batch = parse_batch_generation(name).is_some();
        let cleanup = parse_cleanup_generation(name).is_some();
        let entry_metadata =
            fs::symlink_metadata(&path).map_err(|source| TsinkError::IoWithPath {
                path: path.clone(),
                source,
            })?;
        if segment && entry_metadata.file_type().is_file() {
            let payload = usize::try_from(
                entry_metadata
                    .len()
                    .saturating_sub(ROLLUP_STATE_JOURNAL_HEADER_LEN as u64),
            )
            .unwrap_or(limits.max_payload_bytes)
            .min(limits.max_payload_bytes);
            largest_generation_payload = largest_generation_payload.max(payload);
            if payload > 0 {
                largest_generation_events = largest_generation_events.max(limits.max_events);
            }
        } else if (batch || cleanup)
            && entry_metadata.file_type().is_dir()
            && !crate::engine::fs_utils::is_link_or_reparse_point(&entry_metadata)
        {
            let mut batch_payload = 0usize;
            let mut batch_events = 0usize;
            for child in fs::read_dir(&path).map_err(|source| TsinkError::IoWithPath {
                path: path.clone(),
                source,
            })? {
                let child = child.map_err(|source| TsinkError::IoWithPath {
                    path: path.clone(),
                    source,
                })?;
                observed_entries = observed_entries
                    .checked_add(1)
                    .ok_or_else(rollup_state_journal_memory_model_overflow)?;
                if observed_entries > limits.max_namespace_entries {
                    return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                        operation: "rollup state journal memory preflight",
                        limit: limits.max_namespace_entries,
                        required: observed_entries,
                    });
                }
                let child_name = child.file_name();
                let child_name_bytes = child_name.as_encoded_bytes().len();
                add_rollup_state_journal_modeled_bytes(&mut retained_name_bytes, child_name_bytes)?;
                let child_path_bytes = path
                    .as_os_str()
                    .as_encoded_bytes()
                    .len()
                    .checked_add(child_name_bytes)
                    .and_then(|bytes| bytes.checked_add(1))
                    .ok_or_else(rollup_state_journal_memory_model_overflow)?;
                add_rollup_state_journal_modeled_bytes(&mut retained_path_bytes, child_path_bytes)?;
                admit_rollup_state_journal_required_memory(
                    memory_limit_bytes,
                    retained_memory_bytes,
                    modeled_rollup_state_journal_discovery_bytes(
                        top_level_entries,
                        observed_entries,
                        retained_path_bytes,
                        retained_name_bytes,
                    )?,
                )?;
                let child_path = path.join(&child_name);
                if batch
                    && child_name
                        .to_str()
                        .and_then(parse_batch_event_sequence)
                        .is_some()
                {
                    let child_metadata = fs::symlink_metadata(&child_path).map_err(|source| {
                        TsinkError::IoWithPath {
                            path: child_path.clone(),
                            source,
                        }
                    })?;
                    if child_metadata.file_type().is_file() {
                        let payload = usize::try_from(
                            child_metadata
                                .len()
                                .saturating_sub(ROLLUP_STATE_JOURNAL_HEADER_LEN as u64),
                        )
                        .unwrap_or(limits.max_payload_bytes)
                        .min(limits.max_payload_bytes);
                        batch_payload = batch_payload
                            .saturating_add(payload)
                            .min(limits.max_payload_bytes);
                        batch_events = batch_events.saturating_add(1).min(limits.max_events);
                    }
                }
            }
            if batch {
                largest_generation_payload = largest_generation_payload.max(batch_payload);
                largest_generation_events = largest_generation_events.max(batch_events);
            }
        }
    }

    let mut required = modeled_rollup_state_journal_discovery_bytes(
        top_level_entries,
        observed_entries,
        retained_path_bytes,
        retained_name_bytes,
    )?;
    add_rollup_state_journal_modeled_bytes(
        &mut required,
        largest_generation_payload
            .checked_mul(ROLLUP_STATE_JOURNAL_MODELED_PAYLOAD_COPIES)
            .ok_or_else(rollup_state_journal_memory_model_overflow)?,
    )?;
    add_rollup_state_journal_modeled_bytes(
        &mut required,
        largest_generation_events
            .checked_mul(std::mem::size_of::<RollupSourceStateEvent>())
            .and_then(|bytes| {
                bytes.checked_mul(ROLLUP_STATE_JOURNAL_MODELED_EVENT_COLLECTION_COPIES)
            })
            .ok_or_else(rollup_state_journal_memory_model_overflow)?,
    )?;
    admit_rollup_state_journal_required_memory(
        memory_limit_bytes,
        retained_memory_bytes,
        required,
    )?;
    Ok(required)
}

fn journal_dir_path(rollup_dir: &Path) -> PathBuf {
    rollup_dir.to_path_buf()
}

fn with_serialized_rollup_state_journal_mutation<T>(
    journal_dir: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    if let Some(budget) = local_disk_budget {
        if budget.governs_entry(journal_dir)? {
            return budget.with_serialized_managed_file_mutation(operation);
        }
    }
    operation()
}

#[cfg(test)]
fn active_journal_path(journal_dir: &Path) -> PathBuf {
    journal_dir.join(ROLLUP_STATE_JOURNAL_ACTIVE_FILE_NAME)
}

fn parse_segment_generation(name: &str) -> Option<u64> {
    let encoded = name
        .strip_prefix(ROLLUP_STATE_JOURNAL_SEGMENT_PREFIX)?
        .strip_suffix(ROLLUP_STATE_JOURNAL_SEGMENT_SUFFIX)?;
    if encoded.len() != 16 || !encoded.bytes().all(is_ascii_lower_hex_digit) {
        return None;
    }
    u64::from_str_radix(encoded, 16).ok()
}

fn parse_batch_generation(name: &str) -> Option<u64> {
    let encoded = name
        .strip_prefix(ROLLUP_STATE_JOURNAL_BATCH_PREFIX)?
        .strip_suffix(ROLLUP_STATE_JOURNAL_BATCH_SUFFIX)?;
    if encoded.len() != 16 || !encoded.bytes().all(is_ascii_lower_hex_digit) {
        return None;
    }
    u64::from_str_radix(encoded, 16).ok()
}

fn parse_batch_event_sequence(name: &str) -> Option<u64> {
    let encoded = name
        .strip_prefix(ROLLUP_STATE_JOURNAL_EVENT_PREFIX)?
        .strip_suffix(ROLLUP_STATE_JOURNAL_EVENT_SUFFIX)?;
    if encoded.len() != 16 || !encoded.bytes().all(is_ascii_lower_hex_digit) {
        return None;
    }
    u64::from_str_radix(encoded, 16).ok()
}

fn parse_cleanup_generation(name: &str) -> Option<u64> {
    let encoded = name
        .strip_prefix(ROLLUP_STATE_JOURNAL_CLEANUP_PREFIX)?
        .strip_suffix(ROLLUP_STATE_JOURNAL_CLEANUP_SUFFIX)?;
    if encoded.len() != 16 || !encoded.bytes().all(is_ascii_lower_hex_digit) {
        return None;
    }
    u64::from_str_radix(encoded, 16).ok()
}

fn is_ascii_lower_hex_digit(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn sealed_journal_path(journal_dir: &Path, generation: u64) -> PathBuf {
    journal_dir.join(format!(
        "{ROLLUP_STATE_JOURNAL_SEGMENT_PREFIX}{generation:016x}{ROLLUP_STATE_JOURNAL_SEGMENT_SUFFIX}"
    ))
}

fn batch_journal_path(journal_dir: &Path, generation: u64) -> PathBuf {
    journal_dir.join(format!(
        "{ROLLUP_STATE_JOURNAL_BATCH_PREFIX}{generation:016x}{ROLLUP_STATE_JOURNAL_BATCH_SUFFIX}"
    ))
}

fn batch_event_path(batch_dir: &Path, sequence: u64) -> PathBuf {
    batch_dir.join(format!(
        "{ROLLUP_STATE_JOURNAL_EVENT_PREFIX}{sequence:016x}{ROLLUP_STATE_JOURNAL_EVENT_SUFFIX}"
    ))
}

fn cleanup_journal_path(journal_dir: &Path, generation: u64) -> PathBuf {
    journal_dir.join(format!(
        "{ROLLUP_STATE_JOURNAL_CLEANUP_PREFIX}{generation:016x}{ROLLUP_STATE_JOURNAL_CLEANUP_SUFFIX}"
    ))
}

fn generation_number_exhausted_error() -> TsinkError {
    TsinkError::UnsupportedOperation {
        operation: "rollup state journal generation allocation",
        reason: "the monotonic u64 generation namespace is exhausted".to_string(),
    }
}

#[cfg(test)]
fn discover_journal(journal_dir: &Path) -> Result<RollupStateJournalDiscovery> {
    discover_journal_with_namespace_limit(journal_dir, MAX_RECOVERY_NAMESPACE_ENTRIES)
}

#[cfg(test)]
fn discover_journal_with_namespace_limit(
    journal_dir: &Path,
    max_namespace_entries: usize,
) -> Result<RollupStateJournalDiscovery> {
    discover_journal_inner(journal_dir, max_namespace_entries, false)
}

fn discover_journal_for_event_temp_recovery(
    journal_dir: &Path,
    max_namespace_entries: usize,
) -> Result<RollupStateJournalDiscovery> {
    discover_journal_inner(journal_dir, max_namespace_entries, true)
}

fn discover_journal_inner(
    journal_dir: &Path,
    max_namespace_entries: usize,
    admit_owned_event_temporaries: bool,
) -> Result<RollupStateJournalDiscovery> {
    let metadata = match fs::symlink_metadata(journal_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RollupStateJournalDiscovery {
                sealed: Vec::new(),
                active: None,
                top_level_entry_count: 0,
                entry_count: 0,
                owned_top_level_temporaries: Vec::new(),
                owned_event_temporaries: Vec::new(),
                owned_cleanup_directories: Vec::new(),
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
        max_namespace_entries,
        "rollup state journal discovery",
    )?;
    let top_level_entry_count = entries.len();
    let mut entry_count = top_level_entry_count;
    let mut sealed = Vec::new();
    let mut active = None;
    let mut owned_top_level_temporaries = Vec::new();
    let mut owned_event_temporaries = Vec::new();
    let mut owned_cleanup_directories = Vec::new();
    for entry in entries {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let owned_top_level_temporary = crate::disk_budget::atomic_write_temp_target_name(name)
            .is_some_and(|target| {
                target == ROLLUP_STATE_JOURNAL_ACTIVE_FILE_NAME
                    || parse_segment_generation(target).is_some()
            });
        if owned_top_level_temporary {
            let metadata =
                fs::symlink_metadata(entry.path()).map_err(|source| TsinkError::IoWithPath {
                    path: entry.path(),
                    source,
                })?;
            if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                || !metadata.file_type().is_file()
            {
                return Err(TsinkError::DataCorruption(format!(
                    "owned rollup state journal atomic temporary is link-like or not a regular file: {}",
                    entry.path().display()
                )));
            }
            if admit_owned_event_temporaries {
                owned_top_level_temporaries.push(entry.path());
            }
            continue;
        }
        let segment_generation = parse_segment_generation(name);
        let batch_generation = parse_batch_generation(name);
        let cleanup_generation = parse_cleanup_generation(name);
        let recognized = name == ROLLUP_STATE_JOURNAL_ACTIVE_FILE_NAME
            || segment_generation.is_some()
            || batch_generation.is_some()
            || cleanup_generation.is_some();
        if !recognized {
            continue;
        }
        let entry_metadata =
            fs::symlink_metadata(entry.path()).map_err(|source| TsinkError::IoWithPath {
                path: entry.path(),
                source,
            })?;
        let file_type = entry_metadata.file_type();
        let expected_type = if batch_generation.is_some() || cleanup_generation.is_some() {
            file_type.is_dir()
        } else {
            file_type.is_file()
        };
        if !expected_type || crate::engine::fs_utils::is_link_or_reparse_point(&entry_metadata) {
            return Err(TsinkError::DataCorruption(format!(
                "recognized rollup state journal entry has the wrong type: {}",
                entry.path().display()
            )));
        }
        if cleanup_generation.is_some() {
            let mut cleanup_children = Vec::new();
            for child in fs::read_dir(entry.path()).map_err(|source| TsinkError::IoWithPath {
                path: entry.path(),
                source,
            })? {
                let child = child.map_err(|source| TsinkError::IoWithPath {
                    path: entry.path(),
                    source,
                })?;
                entry_count = entry_count.checked_add(1).ok_or_else(|| {
                    TsinkError::Other("rollup state journal namespace count overflow".to_string())
                })?;
                if entry_count > max_namespace_entries {
                    return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                        operation: "rollup state journal recovery discovery",
                        limit: max_namespace_entries,
                        required: entry_count,
                    });
                }
                let child_name = child.file_name();
                let Some(child_name) = child_name.to_str() else {
                    return Err(TsinkError::DataCorruption(format!(
                        "rollup state journal cleanup directory contains a non-UTF-8 child: {}",
                        child.path().display()
                    )));
                };
                let canonical_event = parse_batch_event_sequence(child_name).is_some();
                let canonical_temporary =
                    crate::disk_budget::atomic_write_temp_target_name(child_name)
                        .and_then(parse_batch_event_sequence)
                        .is_some();
                if !canonical_event && !canonical_temporary {
                    return Err(TsinkError::DataCorruption(format!(
                        "rollup state journal cleanup directory contains an unknown or lookalike child: {}",
                        child.path().display()
                    )));
                }
                let metadata = fs::symlink_metadata(child.path()).map_err(|source| {
                    TsinkError::IoWithPath {
                        path: child.path(),
                        source,
                    }
                })?;
                if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                    || !metadata.file_type().is_file()
                {
                    return Err(TsinkError::DataCorruption(format!(
                        "rollup state journal cleanup child is link-like or not a regular file: {}",
                        child.path().display()
                    )));
                }
                cleanup_children.push(child.path());
            }
            if admit_owned_event_temporaries {
                owned_cleanup_directories.push((entry.path(), cleanup_children));
            }
        } else if name == ROLLUP_STATE_JOURNAL_ACTIVE_FILE_NAME {
            active = Some(entry.path());
        } else if let Some(generation) = segment_generation.or(batch_generation) {
            if batch_generation.is_some() {
                let children =
                    fs::read_dir(entry.path()).map_err(|source| TsinkError::IoWithPath {
                        path: entry.path(),
                        source,
                    })?;
                let mut sequences = Vec::new();
                let mut batch_temporaries = Vec::new();
                for child in children {
                    let child = child.map_err(|source| TsinkError::IoWithPath {
                        path: entry.path(),
                        source,
                    })?;
                    entry_count = entry_count.checked_add(1).ok_or_else(|| {
                        TsinkError::Other(
                            "rollup state journal namespace count overflow".to_string(),
                        )
                    })?;
                    if entry_count > max_namespace_entries {
                        return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                            operation: "rollup state journal discovery",
                            limit: max_namespace_entries,
                            required: entry_count,
                        });
                    }
                    let child_name = child.file_name();
                    let Some(child_name) = child_name.to_str() else {
                        return Err(TsinkError::DataCorruption(format!(
                            "rollup state journal batch contains a non-UTF-8 child; refusing cleanup or mutation before operator inspection: {}",
                            child.path().display()
                        )));
                    };
                    let sequence = parse_batch_event_sequence(child_name);
                    let owned_temporary =
                        crate::disk_budget::atomic_write_temp_target_name(child_name)
                            .and_then(parse_batch_event_sequence)
                            .is_some();
                    if sequence.is_none() && !owned_temporary {
                        return Err(TsinkError::DataCorruption(format!(
                            "rollup state journal batch contains an unknown or lookalike child; refusing cleanup or mutation before operator inspection: {}",
                            child.path().display()
                        )));
                    }
                    let child_metadata = fs::symlink_metadata(child.path()).map_err(|source| {
                        TsinkError::IoWithPath {
                            path: child.path(),
                            source,
                        }
                    })?;
                    if crate::engine::fs_utils::is_link_or_reparse_point(&child_metadata)
                        || !child_metadata.file_type().is_file()
                    {
                        return Err(TsinkError::DataCorruption(format!(
                            "recognized rollup state journal event is link-like or not a regular file: {}",
                            child.path().display()
                        )));
                    }
                    if let Some(sequence) = sequence {
                        sequences.push(sequence);
                    } else if admit_owned_event_temporaries {
                        batch_temporaries.push(child.path());
                    }
                }
                let batch_entry_count = sequences
                    .len()
                    .checked_add(batch_temporaries.len())
                    .ok_or_else(|| {
                        TsinkError::Other(
                            "rollup state journal batch entry count overflow".to_string(),
                        )
                    })?;
                if !batch_temporaries.is_empty() {
                    owned_event_temporaries.push((
                        entry.path(),
                        batch_temporaries,
                        batch_entry_count,
                    ));
                }
                sequences.sort_unstable();
                if sequences.len() > ROLLUP_STATE_JOURNAL_MAX_EVENTS {
                    return Err(TsinkError::DataCorruption(format!(
                        "rollup state journal batch exceeds its {ROLLUP_STATE_JOURNAL_MAX_EVENTS}-event bound: {}",
                        entry.path().display()
                    )));
                }
                for (expected, observed) in sequences.into_iter().enumerate() {
                    let expected = u64::try_from(expected).map_err(|_| {
                        TsinkError::DataCorruption(
                            "rollup state journal batch sequence exceeds u64".to_string(),
                        )
                    })?;
                    if observed != expected {
                        return Err(TsinkError::DataCorruption(format!(
                            "rollup state journal batch has non-contiguous event sequence {observed}, expected {expected}: {}",
                            entry.path().display()
                        )));
                    }
                }
            }
            sealed.push((generation, entry.path()));
        }
    }
    sealed.sort_by(|left, right| {
        left.0.cmp(&right.0).then_with(|| {
            let left_batch = left
                .1
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(parse_batch_generation)
                .is_some();
            let right_batch = right
                .1
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(parse_batch_generation)
                .is_some();
            right_batch.cmp(&left_batch)
        })
    });
    let mut group_start = 0usize;
    while group_start < sealed.len() {
        let generation = sealed[group_start].0;
        let mut group_end = group_start + 1;
        while group_end < sealed.len() && sealed[group_end].0 == generation {
            group_end += 1;
        }
        let group = &sealed[group_start..group_end];
        if group.len() > 2
            || (group.len() == 2
                && !(parse_batch_generation(
                    group[0]
                        .1
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or_default(),
                )
                .is_some()
                    && parse_segment_generation(
                        group[1]
                            .1
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or_default(),
                    )
                    .is_some()))
        {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal generation {generation} has an invalid set of recognized entries"
            )));
        }
        group_start = group_end;
    }
    Ok(RollupStateJournalDiscovery {
        sealed,
        active,
        top_level_entry_count,
        entry_count,
        owned_top_level_temporaries,
        owned_event_temporaries,
        owned_cleanup_directories,
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

fn admit_rollup_state_journal_encoding(
    events: &[RollupSourceStateEvent],
    memory_limit_bytes: usize,
    retained_memory_bytes: usize,
) -> Result<()> {
    let modeled_events = events.iter().try_fold(0usize, |total, event| {
        total
            .checked_add(event.modeled_bytes())
            .ok_or_else(rollup_state_journal_memory_model_overflow)
    })?;
    let required = modeled_events
        .checked_mul(2)
        .and_then(|bytes| {
            events
                .len()
                .checked_mul(std::mem::size_of::<RollupSourceStateEvent>())
                .and_then(|events| bytes.checked_add(events))
        })
        .and_then(|bytes| bytes.checked_add(ROLLUP_STATE_JOURNAL_MEMORY_FIXED_BYTES))
        .ok_or_else(rollup_state_journal_memory_model_overflow)?;
    admit_rollup_state_journal_required_memory(memory_limit_bytes, retained_memory_bytes, required)
}

fn preflight_event_array(payload: &[u8]) -> Result<usize> {
    struct EventArrayVisitor;

    impl<'de> serde::de::Visitor<'de> for EventArrayVisitor {
        type Value = usize;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an array of rollup state journal events")
        }

        fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<usize, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut count = 0usize;
            while sequence.next_element::<RollupSourceStateEvent>()?.is_some() {
                count = count.checked_add(1).ok_or_else(|| {
                    serde::de::Error::custom("rollup state journal event count overflow")
                })?;
            }
            Ok(count)
        }
    }

    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    let count = serde::de::Deserializer::deserialize_seq(&mut deserializer, EventArrayVisitor)?;
    deserializer.end()?;
    Ok(count)
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
    let event_count = preflight_event_array(payload)?;
    if event_count != header.event_count {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal event count mismatch: header declares {}, payload contains {}",
            header.event_count, event_count
        )));
    }
    #[cfg(test)]
    JOURNAL_EVENT_VEC_MATERIALIZATIONS.with(|count| count.set(count.get().saturating_add(1)));
    let events: Vec<RollupSourceStateEvent> = serde_json::from_slice(payload)?;
    Ok(events)
}

fn read_generation_events(
    path: &Path,
    limits: RollupStateJournalLimits,
    global_entry_count: &mut usize,
) -> Result<Vec<RollupSourceStateEvent>> {
    let metadata = fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata) {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal generation is link-like: {}",
            path.display()
        )));
    }
    if metadata.file_type().is_file() {
        return read_events(path, limits);
    }
    if !metadata.file_type().is_dir() {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal generation is not a file or directory: {}",
            path.display()
        )));
    }

    let mut event_paths = Vec::new();
    let entries = fs::read_dir(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })?;
        *global_entry_count = global_entry_count.checked_add(1).ok_or_else(|| {
            TsinkError::Other("rollup state journal namespace count overflow".to_string())
        })?;
        if *global_entry_count > limits.max_namespace_entries {
            return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "rollup state journal replay",
                limit: limits.max_namespace_entries,
                required: *global_entry_count,
            });
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal batch contains a late non-UTF-8 child: {}",
                entry.path().display()
            )));
        };
        let sequence = parse_batch_event_sequence(name);
        let owned_temporary = crate::disk_budget::atomic_write_temp_target_name(name)
            .and_then(parse_batch_event_sequence)
            .is_some();
        if sequence.is_none() && !owned_temporary {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal batch contains a late unknown or lookalike child: {}",
                entry.path().display()
            )));
        }
        let entry_metadata =
            fs::symlink_metadata(entry.path()).map_err(|source| TsinkError::IoWithPath {
                path: entry.path(),
                source,
            })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&entry_metadata)
            || !entry_metadata.file_type().is_file()
        {
            return Err(TsinkError::DataCorruption(format!(
                "recognized rollup state journal event is not a regular file: {}",
                entry.path().display()
            )));
        }
        if let Some(sequence) = sequence {
            if event_paths.len() >= limits.max_events {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state journal batch exceeds its {}-event bound: {}",
                    limits.max_events,
                    path.display()
                )));
            }
            event_paths.push((sequence, entry.path()));
        }
    }
    event_paths.sort_by_key(|(sequence, _)| *sequence);
    if event_paths.len() > limits.max_events {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal batch exceeds its {}-event bound: {}",
            limits.max_events,
            event_paths.len()
        )));
    }

    let mut events = Vec::with_capacity(event_paths.len());
    let mut total_payload_bytes = 0usize;
    for (index, (sequence, event_path)) in event_paths.into_iter().enumerate() {
        let expected_sequence = u64::try_from(index).map_err(|_| {
            TsinkError::DataCorruption(
                "rollup state journal event sequence exceeds u64".to_string(),
            )
        })?;
        if sequence != expected_sequence {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal batch has non-contiguous event sequence {sequence}, expected {expected_sequence}: {}",
                path.display()
            )));
        }
        let event_metadata =
            fs::symlink_metadata(&event_path).map_err(|source| TsinkError::IoWithPath {
                path: event_path.clone(),
                source,
            })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&event_metadata)
            || !event_metadata.file_type().is_file()
        {
            return Err(TsinkError::DataCorruption(format!(
                "recognized rollup state journal event changed type during replay: {}",
                event_path.display()
            )));
        }
        let stored_bytes = event_metadata.len();
        let payload_bytes = stored_bytes
            .checked_sub(ROLLUP_STATE_JOURNAL_HEADER_LEN as u64)
            .ok_or_else(|| {
                TsinkError::DataCorruption(format!(
                    "rollup state journal event is shorter than its header: {}",
                    event_path.display()
                ))
            })?;
        total_payload_bytes = total_payload_bytes
            .checked_add(usize::try_from(payload_bytes).map_err(|_| {
                TsinkError::DataCorruption(
                    "rollup state journal batch payload does not fit this platform".to_string(),
                )
            })?)
            .ok_or_else(|| {
                TsinkError::DataCorruption(
                    "rollup state journal batch payload length overflow".to_string(),
                )
            })?;
        if total_payload_bytes > limits.max_payload_bytes {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal batch exceeds its {}-byte payload bound",
                limits.max_payload_bytes
            )));
        }
        let mut event = read_events(&event_path, limits)?;
        if event.len() != 1 {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal batch event file contains {} events instead of one: {}",
                event.len(),
                event_path.display()
            )));
        }
        events.push(event.pop().expect("one checked journal event"));
    }
    Ok(events)
}

struct RollupStateJournalBatchCleanupPlan {
    // The global preflight deliberately retains no open identity handle. Execution rescans the
    // exact children and then holds one identity handle through unlink; a path replacement with
    // identical admitted children between those phases is the portable late-mutation boundary.
    directory: PathBuf,
    detached_directory: PathBuf,
    event_paths: Vec<PathBuf>,
    detached_event_paths: Vec<PathBuf>,
}

impl RollupStateJournalBatchCleanupPlan {
    fn remove(self) -> Result<bool> {
        // Hold at most one directory identity handle while executing a plan. Keeping all handles
        // from the global preflight alive could exceed common process handle limits at the
        // 1,024-generation bound.
        let directory_identity = capture_plain_directory_identity(
            &self.directory,
            "rollup state journal batch cleanup",
        )?;
        let mut observed_paths = Vec::new();
        observed_paths
            .try_reserve_exact(self.event_paths.len())
            .map_err(|_| {
                TsinkError::Other(
                    "rollup state journal batch cleanup rescan allocation failed".to_string(),
                )
            })?;
        for entry in fs::read_dir(&self.directory).map_err(|source| TsinkError::IoWithPath {
            path: self.directory.clone(),
            source,
        })? {
            let entry = entry.map_err(|source| TsinkError::IoWithPath {
                path: self.directory.clone(),
                source,
            })?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state journal batch changed to contain a non-UTF-8 child: {}",
                    entry.path().display()
                )));
            };
            if parse_batch_event_sequence(name).is_none() {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state journal batch changed to contain an unknown or lookalike child: {}",
                    entry.path().display()
                )));
            }
            let metadata =
                fs::symlink_metadata(entry.path()).map_err(|source| TsinkError::IoWithPath {
                    path: entry.path(),
                    source,
                })?;
            if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                || !metadata.file_type().is_file()
            {
                return Err(TsinkError::DataCorruption(format!(
                    "recognized rollup state journal event changed type: {}",
                    entry.path().display()
                )));
            }
            if observed_paths.len() >= self.event_paths.len() {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state journal batch gained a child after cleanup planning: {}",
                    self.directory.display()
                )));
            }
            observed_paths.push(entry.path());
        }
        observed_paths.sort();
        let mut expected_paths = self.event_paths.clone();
        expected_paths.sort();
        if observed_paths != expected_paths {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal batch contents changed after cleanup planning: {}",
                self.directory.display()
            )));
        }
        ensure_plain_directory_identity(
            &self.directory,
            &directory_identity,
            "rollup state journal batch cleanup",
        )?;
        rename_noreplace_and_sync_parents(&self.directory, &self.detached_directory)?;
        drop(directory_identity);

        // After the durable rename this generation is inert: replay cannot observe a partially
        // deleted prefix. Any error below leaves an exact detached directory for bounded restart
        // recovery instead of a malformed live batch.
        RollupStateJournalDetachedCleanupPlan {
            directory: self.detached_directory,
            child_paths: self.detached_event_paths,
        }
        .remove()?;
        Ok(true)
    }
}

struct RollupStateJournalDetachedCleanupPlan {
    directory: PathBuf,
    child_paths: Vec<PathBuf>,
}

impl RollupStateJournalDetachedCleanupPlan {
    fn remove(self) -> Result<bool> {
        let directory_identity = capture_plain_directory_identity(
            &self.directory,
            "rollup state journal detached cleanup",
        )?;
        let observed_paths =
            scan_detached_cleanup_children(&self.directory, self.child_paths.len())?;
        let mut expected_paths = self.child_paths;
        let mut observed_paths = observed_paths;
        expected_paths.sort();
        observed_paths.sort();
        if observed_paths != expected_paths {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal detached cleanup contents changed after planning: {}",
                self.directory.display()
            )));
        }
        ensure_plain_directory_identity(
            &self.directory,
            &directory_identity,
            "rollup state journal detached cleanup",
        )?;
        let mut mutated = false;
        for path in observed_paths {
            ensure_plain_directory_identity(
                &self.directory,
                &directory_identity,
                "rollup state journal detached cleanup",
            )?;
            mutated |= remove_expected_regular_file_and_sync_parent(&path)?;
        }
        ensure_plain_directory_identity(
            &self.directory,
            &directory_identity,
            "rollup state journal detached cleanup",
        )?;
        let removed_directory = remove_empty_dir_if_exists(&self.directory).map_err(|source| {
            TsinkError::IoWithPath {
                path: self.directory.clone(),
                source,
            }
        })?;
        if removed_directory {
            sync_parent_dir(&self.directory)?;
        }
        Ok(mutated || removed_directory)
    }
}

fn scan_detached_cleanup_children(directory: &Path, max_expected: usize) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    paths.try_reserve_exact(max_expected).map_err(|_| {
        TsinkError::Other(
            "rollup state journal detached cleanup rescan allocation failed".to_string(),
        )
    })?;
    for entry in fs::read_dir(directory).map_err(|source| TsinkError::IoWithPath {
        path: directory.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: directory.to_path_buf(),
            source,
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal detached cleanup contains a non-UTF-8 child: {}",
                entry.path().display()
            )));
        };
        let canonical_event = parse_batch_event_sequence(name).is_some();
        let canonical_temporary = crate::disk_budget::atomic_write_temp_target_name(name)
            .and_then(parse_batch_event_sequence)
            .is_some();
        if !canonical_event && !canonical_temporary {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal detached cleanup contains an unknown or lookalike child: {}",
                entry.path().display()
            )));
        }
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|source| TsinkError::IoWithPath {
                path: entry.path(),
                source,
            })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_file()
        {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal detached cleanup child changed type: {}",
                entry.path().display()
            )));
        }
        if paths.len() >= max_expected {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal detached cleanup gained a child after planning: {}",
                directory.display()
            )));
        }
        paths.push(entry.path());
    }
    Ok(paths)
}

struct RollupStateJournalEventTemporaryCleanupPlan {
    directory: PathBuf,
    temporary_paths: Vec<PathBuf>,
    planned_entry_count: usize,
}

impl RollupStateJournalEventTemporaryCleanupPlan {
    fn remove(self) -> Result<bool> {
        let directory_identity = capture_plain_directory_identity(
            &self.directory,
            "rollup state journal event temporary cleanup",
        )?;
        let mut observed_temporaries = Vec::new();
        let mut sequences = Vec::new();
        observed_temporaries
            .try_reserve_exact(self.temporary_paths.len())
            .map_err(|_| {
                TsinkError::Other(
                    "rollup state journal event temporary cleanup allocation failed".to_string(),
                )
            })?;
        sequences
            .try_reserve_exact(
                self.planned_entry_count
                    .saturating_sub(self.temporary_paths.len()),
            )
            .map_err(|_| {
                TsinkError::Other(
                    "rollup state journal event sequence cleanup allocation failed".to_string(),
                )
            })?;
        for entry in fs::read_dir(&self.directory).map_err(|source| TsinkError::IoWithPath {
            path: self.directory.clone(),
            source,
        })? {
            let entry = entry.map_err(|source| TsinkError::IoWithPath {
                path: self.directory.clone(),
                source,
            })?;
            if sequences.len().saturating_add(observed_temporaries.len())
                >= self.planned_entry_count
            {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state journal batch gained a child after temporary cleanup planning: {}",
                    self.directory.display()
                )));
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state journal batch contains a non-UTF-8 child during temporary cleanup: {}",
                    entry.path().display()
                )));
            };
            if let Some(sequence) = parse_batch_event_sequence(name) {
                sequences.push(sequence);
            } else if crate::disk_budget::atomic_write_temp_target_name(name)
                .and_then(parse_batch_event_sequence)
                .is_some()
            {
                observed_temporaries.push(entry.path());
            } else {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state journal batch contains an unknown or lookalike child during temporary cleanup: {}",
                    entry.path().display()
                )));
            }
            let metadata =
                fs::symlink_metadata(entry.path()).map_err(|source| TsinkError::IoWithPath {
                    path: entry.path(),
                    source,
                })?;
            if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
                || !metadata.file_type().is_file()
            {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state journal batch child changed type during temporary cleanup: {}",
                    entry.path().display()
                )));
            }
        }
        if sequences.len().saturating_add(observed_temporaries.len()) != self.planned_entry_count {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal batch lost a child after temporary cleanup planning: {}",
                self.directory.display()
            )));
        }
        sequences.sort_unstable();
        for (expected, observed) in sequences.into_iter().enumerate() {
            if observed != u64::try_from(expected).unwrap_or(u64::MAX) {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state journal batch sequence changed during temporary cleanup: {}",
                    self.directory.display()
                )));
            }
        }
        observed_temporaries.sort();
        let mut expected_temporaries = self.temporary_paths;
        expected_temporaries.sort();
        if observed_temporaries != expected_temporaries {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal event temporaries changed after recovery planning: {}",
                self.directory.display()
            )));
        }
        ensure_plain_directory_identity(
            &self.directory,
            &directory_identity,
            "rollup state journal event temporary cleanup",
        )?;
        let mut mutated = false;
        for path in observed_temporaries {
            ensure_plain_directory_identity(
                &self.directory,
                &directory_identity,
                "rollup state journal event temporary cleanup",
            )?;
            mutated |= remove_expected_regular_file_and_sync_parent(&path)?;
        }
        Ok(mutated)
    }
}

enum RollupStateJournalCleanupOperation {
    File(PathBuf),
    Batch(RollupStateJournalBatchCleanupPlan),
    Detached(RollupStateJournalDetachedCleanupPlan),
    EventTemporaries(RollupStateJournalEventTemporaryCleanupPlan),
}

impl RollupStateJournalCleanupOperation {
    fn governed_path(&self) -> &Path {
        match self {
            Self::File(path) => path,
            Self::Batch(plan) => &plan.directory,
            Self::Detached(plan) => &plan.directory,
            Self::EventTemporaries(plan) => &plan.directory,
        }
    }

    fn remove(self) -> Result<bool> {
        match self {
            Self::File(path) => remove_expected_regular_file_and_sync_parent(&path),
            Self::Batch(plan) => plan.remove(),
            Self::Detached(plan) => plan.remove(),
            Self::EventTemporaries(plan) => plan.remove(),
        }
    }
}

fn capture_plain_directory_identity(path: &Path, operation: &str) -> Result<same_file::Handle> {
    let identity = same_file::Handle::from_path(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    ensure_plain_directory_identity(path, &identity, operation)?;
    Ok(identity)
}

fn ensure_plain_directory_identity(
    path: &Path,
    expected: &same_file::Handle,
    operation: &str,
) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "{operation} root is link-like or not a directory: {}",
            path.display()
        )));
    }
    let current = same_file::Handle::from_path(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if &current != expected {
        return Err(TsinkError::DataCorruption(format!(
            "refusing {operation} because the batch directory identity changed: {}",
            path.display()
        )));
    }
    Ok(())
}

fn remove_expected_regular_file_and_sync_parent(path: &Path) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_file()
    {
        return Err(TsinkError::DataCorruption(format!(
            "refusing to remove a rollup state journal entry that became link-like or non-regular: {}",
            path.display()
        )));
    }
    let removed = remove_file_if_exists(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if removed {
        sync_parent_dir(path)?;
    }
    Ok(removed)
}

fn plan_rollup_state_journal_batch_cleanup(
    directory: &Path,
    global_entry_count: &mut usize,
    max_namespace_entries: usize,
) -> Result<RollupStateJournalBatchCleanupPlan> {
    let generation = directory
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(parse_batch_generation)
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "rollup state journal batch cleanup target has a non-canonical name: {}",
                directory.display()
            ))
        })?;
    let journal_dir = directory.parent().ok_or_else(|| {
        TsinkError::DataCorruption(format!(
            "rollup state journal batch has no parent: {}",
            directory.display()
        ))
    })?;
    let detached_directory = cleanup_journal_path(journal_dir, generation);
    if crate::engine::fs_utils::path_exists_no_follow(&detached_directory)? {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal detached cleanup target already exists and requires recovery: {}",
            detached_directory.display()
        )));
    }
    let metadata = fs::symlink_metadata(directory).map_err(|source| TsinkError::IoWithPath {
        path: directory.to_path_buf(),
        source,
    })?;
    if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
        || !metadata.file_type().is_dir()
    {
        return Err(TsinkError::DataCorruption(format!(
            "recognized rollup state journal batch is link-like or not a directory: {}",
            directory.display()
        )));
    }
    let directory_identity =
        capture_plain_directory_identity(directory, "rollup state journal batch cleanup planning")?;

    let entries = fs::read_dir(directory).map_err(|source| TsinkError::IoWithPath {
        path: directory.to_path_buf(),
        source,
    })?;
    let mut event_paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: directory.to_path_buf(),
            source,
        })?;
        *global_entry_count = global_entry_count.checked_add(1).ok_or_else(|| {
            TsinkError::Other("rollup state journal namespace count overflow".to_string())
        })?;
        if *global_entry_count > max_namespace_entries {
            return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "rollup state journal cleanup",
                limit: max_namespace_entries,
                required: *global_entry_count,
            });
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal batch contains a non-UTF-8 child; refusing cleanup before operator inspection: {}",
                entry.path().display()
            )));
        };
        if parse_batch_event_sequence(name).is_none() {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal batch contains an unknown or lookalike child; refusing cleanup before operator inspection: {}",
                entry.path().display()
            )));
        }
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|source| TsinkError::IoWithPath {
                path: entry.path(),
                source,
            })?;
        if crate::engine::fs_utils::is_link_or_reparse_point(&metadata)
            || !metadata.file_type().is_file()
        {
            return Err(TsinkError::DataCorruption(format!(
                "recognized rollup state journal event is link-like or not a regular file: {}",
                entry.path().display()
            )));
        }
        event_paths.push(entry.path());
    }
    event_paths.sort_by_key(|path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .and_then(parse_batch_event_sequence)
            .unwrap_or(u64::MAX)
    });
    if event_paths.len() > ROLLUP_STATE_JOURNAL_MAX_EVENTS {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal batch exceeds its {ROLLUP_STATE_JOURNAL_MAX_EVENTS}-event bound: {}",
            directory.display()
        )));
    }
    for (expected, path) in event_paths.iter().enumerate() {
        let expected = u64::try_from(expected).map_err(|_| {
            TsinkError::DataCorruption(
                "rollup state journal batch sequence exceeds u64".to_string(),
            )
        })?;
        let observed = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(parse_batch_event_sequence)
            .expect("planned journal event has a parsed sequence");
        if observed != expected {
            return Err(TsinkError::DataCorruption(format!(
                "rollup state journal batch has non-contiguous event sequence {observed}, expected {expected}: {}",
                directory.display()
            )));
        }
    }
    ensure_plain_directory_identity(
        directory,
        &directory_identity,
        "rollup state journal batch cleanup planning",
    )?;
    let mut detached_event_paths = Vec::new();
    detached_event_paths
        .try_reserve_exact(event_paths.len())
        .map_err(|_| {
            TsinkError::Other(
                "rollup state journal detached cleanup planning allocation failed".to_string(),
            )
        })?;
    for path in &event_paths {
        detached_event_paths.push(
            detached_directory.join(
                path.file_name()
                    .expect("planned journal event has a final component"),
            ),
        );
    }
    Ok(RollupStateJournalBatchCleanupPlan {
        directory: directory.to_path_buf(),
        detached_directory,
        event_paths,
        detached_event_paths,
    })
}

fn remove_rollup_state_journal_paths_batched(
    operations: Vec<RollupStateJournalCleanupOperation>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    operation: &str,
    memory_limit_bytes: usize,
) -> Result<()> {
    let mut reservation = None;
    let mut governed_mutation_or_ambiguity = false;
    let operation_result = (|| -> Result<()> {
        for cleanup in operations {
            let governed = match local_disk_budget {
                Some(budget) => budget.governs_entry(cleanup.governed_path())?,
                None => false,
            };
            if governed && reservation.is_none() {
                reservation = Some(
                    local_disk_budget
                        .expect("governed rollup journal cleanup requires a disk budget")
                        .reserve(
                            crate::DiskCategory::Rollups,
                            0,
                            crate::DiskReservationKind::Recovery,
                        )?,
                );
            }
            match cleanup.remove() {
                Ok(removed) => governed_mutation_or_ambiguity |= governed && removed,
                Err(err) => {
                    governed_mutation_or_ambiguity |= governed;
                    return Err(err);
                }
            }
        }
        Ok(())
    })();

    let settlement_result = reservation.map_or(Ok(()), |reservation| reservation.commit(0, 0));
    if settlement_result.is_err() {
        governed_mutation_or_ambiguity = true;
    }
    let reconciliation_result = if governed_mutation_or_ambiguity {
        local_disk_budget
            .expect("governed rollup journal cleanup requires a disk budget")
            .reconcile_when_idle_with_memory_limit(memory_limit_bytes)
            .map(|_| ())
    } else {
        Ok(())
    };
    let mut errors = Vec::new();
    if let Err(err) = &operation_result {
        errors.push(format!("cleanup failed: {err}"));
    }
    if let Err(err) = &settlement_result {
        errors.push(format!("disk settlement failed: {err}"));
    }
    if let Err(err) = &reconciliation_result {
        errors.push(format!("disk reconciliation failed: {err}"));
    }
    if errors.is_empty() {
        Ok(())
    } else if errors.len() == 1 {
        match (operation_result, settlement_result, reconciliation_result) {
            (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => Err(err),
            _ => unreachable!("one recorded cleanup error must have a matching failed result"),
        }
    } else {
        Err(TsinkError::Other(format!(
            "{operation} failed: {}",
            errors.join("; ")
        )))
    }
}

fn recover_rollup_state_journal_artifacts(
    journal_dir: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    limits: RollupStateJournalLimits,
    memory_limit_bytes: usize,
    retained_memory_bytes: usize,
) -> Result<RollupStateJournalDiscovery> {
    admit_rollup_state_journal_memory(
        journal_dir,
        limits,
        memory_limit_bytes,
        retained_memory_bytes,
    )?;
    let mut discovery =
        discover_journal_for_event_temp_recovery(journal_dir, limits.max_namespace_entries)?;
    let top_temporary_count = discovery.owned_top_level_temporaries.len();
    let event_temporary_count = discovery
        .owned_event_temporaries
        .iter()
        .map(|(_, paths, _)| paths.len())
        .sum::<usize>();
    let detached_directory_count = discovery.owned_cleanup_directories.len();
    let detached_child_count = discovery
        .owned_cleanup_directories
        .iter()
        .map(|(_, paths)| paths.len())
        .sum::<usize>();
    if top_temporary_count == 0 && event_temporary_count == 0 && detached_directory_count == 0 {
        return Ok(discovery);
    }

    let operation_count = top_temporary_count
        .checked_add(discovery.owned_event_temporaries.len())
        .and_then(|count| count.checked_add(detached_directory_count))
        .ok_or_else(|| {
            TsinkError::Other("rollup state journal recovery plan overflow".to_string())
        })?;
    let mut operations = Vec::new();
    operations.try_reserve_exact(operation_count).map_err(|_| {
        TsinkError::Other("rollup state journal recovery plan allocation failed".to_string())
    })?;
    operations.extend(
        std::mem::take(&mut discovery.owned_top_level_temporaries)
            .into_iter()
            .map(RollupStateJournalCleanupOperation::File),
    );
    operations.extend(
        std::mem::take(&mut discovery.owned_event_temporaries)
            .into_iter()
            .map(|(directory, temporary_paths, planned_entry_count)| {
                RollupStateJournalCleanupOperation::EventTemporaries(
                    RollupStateJournalEventTemporaryCleanupPlan {
                        directory,
                        temporary_paths,
                        planned_entry_count,
                    },
                )
            }),
    );
    operations.extend(
        std::mem::take(&mut discovery.owned_cleanup_directories)
            .into_iter()
            .map(|(directory, child_paths)| {
                RollupStateJournalCleanupOperation::Detached(
                    RollupStateJournalDetachedCleanupPlan {
                        directory,
                        child_paths,
                    },
                )
            }),
    );
    remove_rollup_state_journal_paths_batched(
        operations,
        local_disk_budget,
        "rollup state journal crash-artifact recovery",
        memory_limit_bytes,
    )?;

    let removed_entries = top_temporary_count
        .checked_add(event_temporary_count)
        .and_then(|count| count.checked_add(detached_directory_count))
        .and_then(|count| count.checked_add(detached_child_count))
        .ok_or_else(|| {
            TsinkError::Other("rollup state journal recovery count overflow".to_string())
        })?;
    discovery.entry_count = discovery
        .entry_count
        .checked_sub(removed_entries)
        .ok_or_else(|| {
            TsinkError::Other("rollup state journal recovery count underflow".to_string())
        })?;
    discovery.top_level_entry_count = discovery
        .top_level_entry_count
        .checked_sub(top_temporary_count.saturating_add(detached_directory_count))
        .ok_or_else(|| {
            TsinkError::Other("rollup state journal top-level recovery underflow".to_string())
        })?;
    Ok(discovery)
}

fn recover_published_batch_shadows(
    journal_dir: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    limits: RollupStateJournalLimits,
    discovery: RollupStateJournalDiscovery,
    memory_limit_bytes: usize,
    retained_memory_bytes: usize,
) -> Result<RollupStateJournalDiscovery> {
    let mut operations = Vec::new();
    operations
        .try_reserve_exact(discovery.sealed_generation_count())
        .map_err(|_| {
            TsinkError::Other(
                "rollup state journal published-batch recovery plan allocation failed".to_string(),
            )
        })?;
    let mut replay_entry_count = discovery.top_level_entry_count;
    let mut cleanup_entry_count = discovery.top_level_entry_count;
    let mut group_start = 0usize;
    while group_start < discovery.sealed.len() {
        let generation = discovery.sealed[group_start].0;
        let mut group_end = group_start + 1;
        while group_end < discovery.sealed.len() && discovery.sealed[group_end].0 == generation {
            group_end += 1;
        }
        let group = &discovery.sealed[group_start..group_end];
        if group.len() == 2 {
            let batch = &group[0].1;
            let shadow = &group[1].1;
            let batch_events = read_generation_events(batch, limits, &mut replay_entry_count)?;
            let shadow_events = read_events(shadow, limits)?;
            if batch_events != shadow_events {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state journal generation {generation} batch and packed shadow have different contents"
                )));
            }
            operations.push(RollupStateJournalCleanupOperation::Batch(
                plan_rollup_state_journal_batch_cleanup(
                    batch,
                    &mut cleanup_entry_count,
                    limits.max_namespace_entries,
                )?,
            ));
        }
        group_start = group_end;
    }
    if operations.is_empty() {
        return Ok(discovery);
    }

    // Publishing the regular shadow is the commit point. Removing the same-generation batch is
    // entry-neutral because it first renames the live directory into its deterministic inert
    // cleanup name, so restart can finish this step even at the exact namespace ceiling.
    remove_rollup_state_journal_paths_batched(
        operations,
        local_disk_budget,
        "rollup state journal published-batch recovery",
        memory_limit_bytes,
    )?;
    recover_rollup_state_journal_artifacts(
        journal_dir,
        local_disk_budget,
        limits,
        memory_limit_bytes,
        retained_memory_bytes,
    )
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

pub(super) fn event_applies(
    event: &RollupSourceStateEvent,
    epoch: u64,
    generations: &HashMap<String, u64>,
) -> bool {
    if event.epoch != epoch
        || generations.get(&event.policy_id).copied().unwrap_or(0) != event.generation
    {
        return false;
    }
    true
}

pub(super) fn preflight_event_envelope_usage(
    usage: RollupStateEnvelopeUsage,
    checkpoints: &HashMap<String, BTreeMap<String, i64>>,
    pending_materializations: &HashMap<String, BTreeMap<String, PendingRollupMaterialization>>,
    generations: &HashMap<String, u64>,
    event: &RollupSourceStateEvent,
    epoch: u64,
    operation: &'static str,
) -> Result<RollupStateEnvelopeUsage> {
    if !event_applies(event, epoch, generations) {
        return Ok(usage);
    }
    let checkpoint_exists = checkpoints
        .get(&event.policy_id)
        .is_some_and(|entries| entries.contains_key(&event.source_key));
    let pending_exists = pending_materializations
        .get(&event.policy_id)
        .is_some_and(|entries| entries.contains_key(&event.source_key));
    let checkpoint_bytes = event
        .policy_id
        .len()
        .saturating_add(event.source_key.len())
        .saturating_mul(6)
        .saturating_add(128);
    let pending_bytes = event
        .policy_id
        .len()
        .saturating_add(event.source_key.len())
        .saturating_mul(6)
        .saturating_add(192);
    let remove_checkpoint = checkpoint_exists && event.checkpoint.is_none();
    let add_checkpoint = !checkpoint_exists && event.checkpoint.is_some();
    let remove_pending = pending_exists && event.pending.is_none();
    let add_pending = !pending_exists && event.pending.is_some();
    usage.transition(
        usize::from(remove_checkpoint).saturating_add(usize::from(remove_pending)),
        usize::from(remove_checkpoint)
            .saturating_mul(checkpoint_bytes)
            .saturating_add(usize::from(remove_pending).saturating_mul(pending_bytes)),
        usize::from(add_checkpoint).saturating_add(usize::from(add_pending)),
        usize::from(add_checkpoint)
            .saturating_mul(checkpoint_bytes)
            .saturating_add(usize::from(add_pending).saturating_mul(pending_bytes)),
        operation,
    )
}

pub(super) fn apply_event_to_maps(
    checkpoints: &mut HashMap<String, BTreeMap<String, i64>>,
    pending_materializations: &mut HashMap<String, BTreeMap<String, PendingRollupMaterialization>>,
    event: RollupSourceStateEvent,
) {
    // Apply removals before additions so a net-bounded replacement never transiently grows past
    // the preflighted envelope.
    if event.checkpoint.is_none() {
        if let Some(entries) = checkpoints.get_mut(&event.policy_id) {
            entries.remove(&event.source_key);
            if entries.is_empty() {
                checkpoints.remove(&event.policy_id);
            }
        }
    }
    if event.pending.is_none() {
        if let Some(entries) = pending_materializations.get_mut(&event.policy_id) {
            entries.remove(&event.source_key);
            if entries.is_empty() {
                pending_materializations.remove(&event.policy_id);
            }
        }
    }

    if let Some(checkpoint) = event.checkpoint {
        checkpoints
            .entry(event.policy_id.clone())
            .or_default()
            .insert(event.source_key.clone(), checkpoint);
    }

    if let Some(pending) = event.pending {
        pending_materializations
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
    }
}

fn apply_event(state: &mut LoadedRollupState, event: RollupSourceStateEvent, epoch: u64) {
    if !event_applies(&event, epoch, &state.generations) {
        return;
    }
    apply_event_to_maps(
        &mut state.checkpoints,
        &mut state.pending_materializations,
        event,
    );
}

pub(super) fn load_rollup_state_journal(
    rollup_dir: Option<&Path>,
    epoch: u64,
    state: &mut LoadedRollupState,
    envelope_usage: RollupStateEnvelopeUsage,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    memory_limit_bytes: usize,
) -> Result<RollupStateEnvelopeUsage> {
    let Some(rollup_dir) = rollup_dir else {
        return Ok(envelope_usage);
    };
    let journal_dir = journal_dir_path(rollup_dir);
    with_serialized_rollup_state_journal_mutation(&journal_dir, local_disk_budget, || {
        load_rollup_state_journal_locked(
            &journal_dir,
            epoch,
            state,
            envelope_usage,
            local_disk_budget,
            memory_limit_bytes,
        )
    })
}

fn load_rollup_state_journal_locked(
    journal_dir: &Path,
    epoch: u64,
    state: &mut LoadedRollupState,
    mut envelope_usage: RollupStateEnvelopeUsage,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    memory_limit_bytes: usize,
) -> Result<RollupStateEnvelopeUsage> {
    let limits = RollupStateJournalLimits::default();
    let journal_required = admit_rollup_state_journal_memory(
        journal_dir,
        limits,
        memory_limit_bytes,
        envelope_usage.modeled_bytes,
    )?;
    let discovery = recover_rollup_state_journal_artifacts(
        journal_dir,
        local_disk_budget,
        limits,
        memory_limit_bytes,
        envelope_usage.modeled_bytes,
    )?;
    let discovery = recover_published_batch_shadows(
        journal_dir,
        local_disk_budget,
        limits,
        discovery,
        memory_limit_bytes,
        envelope_usage.modeled_bytes,
    )?;
    if discovery.generation_count() > ROLLUP_STATE_JOURNAL_MAX_GENERATIONS {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal has more than {ROLLUP_STATE_JOURNAL_MAX_GENERATIONS} recognized generations"
        )));
    }
    let mut global_entry_count = discovery.top_level_entry_count;
    for (_, path) in discovery.sealed {
        for event in read_generation_events(&path, limits, &mut global_entry_count)? {
            let next_usage = preflight_event_envelope_usage(
                envelope_usage,
                &state.checkpoints,
                &state.pending_materializations,
                &state.generations,
                &event,
                epoch,
                "rollup source state journal replay",
            )?;
            admit_rollup_state_journal_required_memory(
                memory_limit_bytes,
                next_usage.modeled_bytes,
                journal_required,
            )?;
            envelope_usage = next_usage;
            apply_event(state, event, epoch);
        }
    }
    if let Some(path) = discovery.active {
        for event in read_events(&path, limits)? {
            let next_usage = preflight_event_envelope_usage(
                envelope_usage,
                &state.checkpoints,
                &state.pending_materializations,
                &state.generations,
                &event,
                epoch,
                "rollup source state journal replay",
            )?;
            admit_rollup_state_journal_required_memory(
                memory_limit_bytes,
                next_usage.modeled_bytes,
                journal_required,
            )?;
            envelope_usage = next_usage;
            apply_event(state, event, epoch);
        }
    }
    Ok(envelope_usage)
}

#[cfg(test)]
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

fn create_journal_batch_directory(
    path: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let mut reservation = None;
    if let Some(budget) = local_disk_budget {
        if budget.governs_entry(path)? {
            reservation = Some(budget.reserve(
                crate::DiskCategory::Rollups,
                budget.snapshot_restore_entry_staging_allowance_bytes()?,
                crate::DiskReservationKind::Growth,
            )?);
        }
    }

    let creation_result = fs::create_dir(path)
        .map_err(|source| TsinkError::IoWithPath {
            path: path.to_path_buf(),
            source,
        })
        .and_then(|()| sync_parent_dir(path));
    let settlement_result = reservation.map_or(Ok(()), |reservation| reservation.commit(0, 0));
    match (creation_result, settlement_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
        (Err(creation_err), Err(settlement_err)) => Err(TsinkError::Other(format!(
            "rollup state journal batch directory creation failed: {creation_err}; disk settlement failed: {settlement_err}"
        ))),
    }
}

fn write_new_journal_event(
    path: &Path,
    encoded: &[u8],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
    publication_may_be_ambiguous: Option<&mut bool>,
    memory_limit_bytes: usize,
    retained_memory_bytes: usize,
) -> Result<()> {
    let write_peak = encoded
        .len()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(ROLLUP_STATE_JOURNAL_MEMORY_FIXED_BYTES))
        .ok_or_else(rollup_state_journal_memory_model_overflow)?;
    admit_rollup_state_journal_required_memory(
        memory_limit_bytes,
        retained_memory_bytes,
        write_peak,
    )?;
    if crate::engine::fs_utils::path_exists_no_follow(path)? {
        return Err(TsinkError::DataCorruption(format!(
            "refusing to overwrite an existing rollup state journal event: {}",
            path.display()
        )));
    }

    let mut reservation = None;
    let encoded_bytes = u64::try_from(encoded.len()).map_err(|_| {
        TsinkError::Other("rollup state journal event size exceeds u64".to_string())
    })?;
    if let Some(budget) = local_disk_budget {
        if budget.governs_entry(path)? {
            let admitted_bytes = encoded_bytes
                .checked_add(budget.snapshot_restore_entry_staging_allowance_bytes()?)
                .ok_or_else(|| {
                    TsinkError::Other(
                        "rollup state journal event staging admission overflow".to_string(),
                    )
                })?;
            reservation = Some(budget.reserve(
                crate::DiskCategory::Rollups,
                admitted_bytes,
                reservation_kind,
            )?);
        }
    }

    let write_result = match write_tmp_and_sync(path, encoded) {
        Ok(temporary) => match rename_path_noreplace(&temporary, path) {
            Ok(()) => {
                if let Some(publication_may_be_ambiguous) = publication_may_be_ambiguous {
                    *publication_may_be_ambiguous = true;
                }
                sync_parent_dir(path)
            }
            Err(publication_err) => {
                let cleanup_result = remove_file_if_exists(&temporary)
                    .map_err(TsinkError::from)
                    .and_then(|removed| {
                        if removed {
                            sync_parent_dir(&temporary)
                        } else {
                            Ok(())
                        }
                    });
                match cleanup_result {
                    Ok(()) => Err(publication_err),
                    Err(cleanup_err) => Err(TsinkError::Other(format!(
                        "{publication_err}; temporary cleanup failed: {cleanup_err}"
                    ))),
                }
            }
        },
        Err(err) => Err(err),
    };

    let governed = reservation.is_some();
    let settlement_result = match (&write_result, reservation) {
        (Ok(()), Some(reservation)) => reservation.commit(encoded_bytes, 0),
        (Err(_), Some(reservation)) => {
            reservation.commit_as(crate::DiskCategory::Temporary, encoded_bytes, 0)
        }
        (_, None) => Ok(()),
    };
    let reconciliation_result = if governed && (write_result.is_err() || settlement_result.is_err())
    {
        local_disk_budget
            .expect("governed journal event failure requires its disk budget")
            .reconcile_when_idle_with_memory_limit(memory_limit_bytes)
            .map(|_| ())
    } else {
        Ok(())
    };
    match (write_result, settlement_result, reconciliation_result) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(err), Ok(()), Ok(())) | (Ok(()), Err(err), Ok(())) | (Ok(()), Ok(()), Err(err)) => {
            Err(err)
        }
        (write, settlement, reconciliation) => {
            let mut errors = Vec::new();
            if let Err(err) = write {
                errors.push(format!("event publication failed: {err}"));
            }
            if let Err(err) = settlement {
                errors.push(format!("disk settlement failed: {err}"));
            }
            if let Err(err) = reconciliation {
                errors.push(format!("disk reconciliation failed: {err}"));
            }
            Err(TsinkError::Other(format!(
                "rollup state journal event publication failed: {}",
                errors.join("; ")
            )))
        }
    }
}

pub(super) struct RollupStateJournalWriter {
    journal_dir: Option<PathBuf>,
    local_disk_budget: Option<Arc<crate::LocalDiskBudget>>,
    memory_limit_bytes: usize,
    retained_memory_bytes: usize,
    limits: RollupStateJournalLimits,
    generation_count: usize,
    namespace_entry_count: usize,
    next_generation: Option<u64>,
    legacy_active: Option<PathBuf>,
    batch_dir: Option<PathBuf>,
    batch_event_count: usize,
    batch_payload_bytes: usize,
    event_publication_may_be_ambiguous: bool,
}

impl RollupStateJournalWriter {
    #[cfg(test)]
    fn begin_with_limits(
        rollup_dir: Option<&Path>,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
        limits: RollupStateJournalLimits,
    ) -> Result<Self> {
        Self::begin_with_limits_and_memory_limit(
            rollup_dir,
            local_disk_budget,
            limits,
            usize::MAX,
            0,
        )
    }

    fn begin_with_limits_and_memory_limit(
        rollup_dir: Option<&Path>,
        local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
        limits: RollupStateJournalLimits,
        memory_limit_bytes: usize,
        retained_memory_bytes: usize,
    ) -> Result<Self> {
        if limits.max_events == 0
            || limits.max_payload_bytes == 0
            || limits.max_generations == 0
            || limits.max_namespace_entries < 3
            || limits.max_namespace_entries > MAX_RECOVERY_NAMESPACE_ENTRIES
        {
            return Err(TsinkError::InvalidConfiguration(
                "rollup state journal limits are invalid".to_string(),
            ));
        }
        let Some(rollup_dir) = rollup_dir else {
            return Ok(Self {
                journal_dir: None,
                local_disk_budget: local_disk_budget.cloned(),
                memory_limit_bytes,
                retained_memory_bytes,
                limits,
                generation_count: 0,
                namespace_entry_count: 0,
                next_generation: Some(1),
                legacy_active: None,
                batch_dir: None,
                batch_event_count: 0,
                batch_payload_bytes: 0,
                event_publication_may_be_ambiguous: false,
            });
        };
        let journal_dir = journal_dir_path(rollup_dir);
        with_serialized_rollup_state_journal_mutation(&journal_dir, local_disk_budget, || {
            let discovery = recover_rollup_state_journal_artifacts(
                &journal_dir,
                local_disk_budget,
                limits,
                memory_limit_bytes,
                retained_memory_bytes,
            )?;
            let discovery = recover_published_batch_shadows(
                &journal_dir,
                local_disk_budget,
                limits,
                discovery,
                memory_limit_bytes,
                retained_memory_bytes,
            )?;
            let generation_count = discovery.generation_count();
            if generation_count > limits.max_generations {
                return Err(TsinkError::DataCorruption(format!(
                    "rollup state journal has more than {} recognized generations",
                    limits.max_generations
                )));
            }
            let next_generation = discovery
                .sealed
                .last()
                .map_or(Some(1), |(generation, _)| generation.checked_add(1));
            Ok(Self {
                journal_dir: Some(journal_dir.clone()),
                local_disk_budget: local_disk_budget.cloned(),
                memory_limit_bytes,
                retained_memory_bytes,
                limits,
                generation_count,
                namespace_entry_count: discovery.entry_count,
                next_generation,
                legacy_active: discovery.active,
                batch_dir: None,
                batch_event_count: 0,
                batch_payload_bytes: 0,
                event_publication_may_be_ambiguous: false,
            })
        })
    }

    fn take_next_generation(&mut self) -> Result<u64> {
        let generation = self
            .next_generation
            .ok_or_else(generation_number_exhausted_error)?;
        self.next_generation = generation.checked_add(1);
        Ok(generation)
    }

    fn refresh_discovery(&mut self) -> Result<RollupStateJournalDiscovery> {
        let journal_dir = self.journal_dir.as_deref().ok_or_else(|| {
            TsinkError::InvalidConfiguration("rollup state requires persistent storage".to_string())
        })?;
        let discovery = recover_rollup_state_journal_artifacts(
            journal_dir,
            self.local_disk_budget.as_ref(),
            self.limits,
            self.memory_limit_bytes,
            self.retained_memory_bytes,
        )?;
        self.generation_count = discovery.generation_count();
        self.namespace_entry_count = discovery.entry_count;
        self.next_generation = discovery
            .sealed
            .last()
            .map_or(Some(1), |(generation, _)| generation.checked_add(1));
        self.legacy_active.clone_from(&discovery.active);
        if self.batch_dir.as_ref().is_some_and(|current| {
            !discovery
                .sealed
                .iter()
                .any(|(_, candidate)| candidate == current)
        }) {
            self.batch_dir = None;
            self.batch_event_count = 0;
            self.batch_payload_bytes = 0;
        }
        Ok(discovery)
    }

    fn pack_one_batch_generation(&mut self) -> Result<bool> {
        let discovery = self.refresh_discovery()?;
        let Some((generation, batch_dir)) = discovery.sealed.iter().find(|(_, path)| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(parse_batch_generation)
                .is_some()
        }) else {
            return Ok(false);
        };
        let generation = *generation;
        let batch_dir = batch_dir.clone();
        let mut global_entry_count = discovery.top_level_entry_count;
        let events = read_generation_events(&batch_dir, self.limits, &mut global_entry_count)?;
        let target = sealed_journal_path(
            self.journal_dir
                .as_deref()
                .expect("persistent writer has a journal directory"),
            generation,
        );
        if events.is_empty() {
            if crate::engine::fs_utils::path_exists_no_follow(&target)? {
                return Err(TsinkError::DataCorruption(format!(
                    "empty rollup state journal batch unexpectedly collides with a packed shadow: {}",
                    target.display()
                )));
            }
        } else {
            admit_rollup_state_journal_encoding(
                &events,
                self.memory_limit_bytes,
                self.retained_memory_bytes,
            )?;
            let encoded = encode_events(&events, self.limits)?.ok_or_else(|| {
                TsinkError::MaintenanceWorkItemTooLarge {
                    operation: "rollup state journal batch packing",
                    limit: u64::try_from(self.limits.max_payload_bytes).unwrap_or(u64::MAX),
                    required: u64::try_from(events.iter().fold(0usize, |total, event| {
                        total.saturating_add(event.modeled_bytes())
                    }))
                    .unwrap_or(u64::MAX),
                }
            })?;
            if crate::engine::fs_utils::path_exists_no_follow(&target)? {
                if read_events(&target, self.limits)? != events {
                    return Err(TsinkError::DataCorruption(format!(
                        "rollup state journal batch packing collision has different contents: {}",
                        target.display()
                    )));
                }
            } else {
                write_new_journal_event(
                    &target,
                    &encoded,
                    self.local_disk_budget.as_ref(),
                    crate::DiskReservationKind::Maintenance,
                    None,
                    self.memory_limit_bytes,
                    self.retained_memory_bytes,
                )?;
            }
        }

        let cleanup_discovery = self.refresh_discovery()?;
        let mut cleanup_entry_count = cleanup_discovery.top_level_entry_count;
        let plan = plan_rollup_state_journal_batch_cleanup(
            &batch_dir,
            &mut cleanup_entry_count,
            self.limits.max_namespace_entries,
        )?;
        if self.batch_dir.as_deref() == Some(batch_dir.as_path()) {
            self.batch_dir = None;
            self.batch_event_count = 0;
            self.batch_payload_bytes = 0;
        }
        remove_rollup_state_journal_paths_batched(
            vec![RollupStateJournalCleanupOperation::Batch(plan)],
            self.local_disk_budget.as_ref(),
            "rollup state journal batch packing cleanup",
            self.memory_limit_bytes,
        )?;
        self.refresh_discovery()?;
        Ok(true)
    }

    fn compact_or_prune_one_pair(
        &mut self,
        epoch: u64,
        allow_atomic_publication: bool,
    ) -> Result<bool> {
        loop {
            let discovery = self.refresh_discovery()?;
            if discovery.generation_count() < 2 {
                return Ok(false);
            }
            if compact_or_prune_one_generation_pair(
                &discovery,
                epoch,
                allow_atomic_publication,
                self.local_disk_budget.as_ref(),
                self.limits,
                self.memory_limit_bytes,
                self.retained_memory_bytes,
            )? {
                self.refresh_discovery()?;
                return Ok(true);
            }
            let last_sealed = discovery.sealed.last();
            if let (Some((_, last_sealed)), Some(active)) =
                (last_sealed, discovery.active.as_deref())
            {
                let last_is_batch = last_sealed
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(parse_batch_generation)
                    .is_some();
                if !last_is_batch
                    && generation_pair_can_compact(
                        last_sealed,
                        active,
                        epoch,
                        self.limits,
                        self.memory_limit_bytes,
                        self.retained_memory_bytes,
                    )?
                {
                    let generation = self.take_next_generation()?;
                    let sealed = sealed_journal_path(
                        self.journal_dir
                            .as_deref()
                            .expect("persistent writer has a journal directory"),
                        generation,
                    );
                    rename_noreplace_and_sync_parents(active, &sealed)?;
                    self.legacy_active = None;
                    self.refresh_discovery()?;
                    continue;
                }
            }
            let has_batch = discovery.sealed.iter().any(|(_, path)| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(parse_batch_generation)
                    .is_some()
            });
            if has_batch {
                if !allow_atomic_publication {
                    return Ok(false);
                }
                if !self.pack_one_batch_generation()? {
                    return Ok(false);
                }
                continue;
            }
            return Ok(false);
        }
    }

    fn make_namespace_room(&mut self, additional: usize, epoch: u64) -> Result<()> {
        loop {
            let required = self
                .namespace_entry_count
                .checked_add(additional)
                .ok_or_else(|| {
                    TsinkError::Other("rollup state journal namespace count overflow".to_string())
                })?;
            // Keep one recognized-entry slot available for crash-safe batch packing.
            if required < self.limits.max_namespace_entries {
                return Ok(());
            }
            if self.namespace_entry_count < self.limits.max_namespace_entries
                && self.pack_one_batch_generation()?
            {
                continue;
            }
            if self.compact_or_prune_one_pair(
                epoch,
                self.namespace_entry_count < self.limits.max_namespace_entries,
            )? {
                continue;
            }
            return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "rollup state journal directory",
                limit: self.limits.max_namespace_entries,
                required,
            });
        }
    }

    fn preflight_rotation_generations(&self) -> Result<()> {
        let first = self
            .next_generation
            .ok_or_else(generation_number_exhausted_error)?;
        if self.legacy_active.is_some() && first.checked_add(1).is_none() {
            return Err(generation_number_exhausted_error());
        }
        Ok(())
    }

    fn ensure_batch_for_payload(&mut self, payload_bytes: usize, epoch: u64) -> Result<()> {
        if self.batch_dir.is_some()
            && self.batch_event_count < self.limits.max_events
            && payload_bytes <= self.limits.max_payload_bytes - self.batch_payload_bytes
        {
            return Ok(());
        }
        if payload_bytes > self.limits.max_payload_bytes {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: "rollup source state journal record",
                limit: u64::try_from(self.limits.max_payload_bytes).unwrap_or(u64::MAX),
                required: u64::try_from(payload_bytes).unwrap_or(u64::MAX),
            });
        }
        self.preflight_rotation_generations()?;
        while self.generation_count >= self.limits.max_generations {
            if !self.compact_or_prune_one_pair(
                epoch,
                self.namespace_entry_count < self.limits.max_namespace_entries,
            )? {
                return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
                    operation: "rollup state journal",
                    limit: self.limits.max_generations,
                    required: self.generation_count.saturating_add(1),
                });
            }
        }
        self.make_namespace_room(2, epoch)?;

        let journal_dir = self.journal_dir.clone().ok_or_else(|| {
            TsinkError::InvalidConfiguration("rollup state requires persistent storage".to_string())
        })?;
        if let Some(active) = self.legacy_active.clone() {
            let generation = self.take_next_generation()?;
            let sealed = sealed_journal_path(&journal_dir, generation);
            rename_noreplace_and_sync_parents(&active, &sealed)?;
            self.legacy_active = None;
        }
        let generation = self.take_next_generation()?;
        let batch_dir = batch_journal_path(&journal_dir, generation);
        create_journal_batch_directory(&batch_dir, self.local_disk_budget.as_ref())?;
        self.generation_count = self.generation_count.checked_add(1).ok_or_else(|| {
            TsinkError::Other("rollup state journal generation count overflow".to_string())
        })?;
        self.namespace_entry_count =
            self.namespace_entry_count.checked_add(1).ok_or_else(|| {
                TsinkError::Other("rollup state journal namespace count overflow".to_string())
            })?;
        self.batch_dir = Some(batch_dir);
        self.batch_event_count = 0;
        self.batch_payload_bytes = 0;
        Ok(())
    }

    pub(super) fn persist(&mut self, event: RollupSourceStateEvent) -> Result<()> {
        let journal_dir = self.journal_dir.clone();
        let local_disk_budget = self.local_disk_budget.clone();
        let Some(journal_dir) = journal_dir else {
            return self.persist_locked(event);
        };
        with_serialized_rollup_state_journal_mutation(
            &journal_dir,
            local_disk_budget.as_ref(),
            || self.persist_locked(event),
        )
    }

    fn persist_locked(&mut self, event: RollupSourceStateEvent) -> Result<()> {
        self.event_publication_may_be_ambiguous = false;
        if event.modeled_bytes() > self.limits.max_payload_bytes {
            return Err(TsinkError::MaintenanceWorkItemTooLarge {
                operation: "rollup source state journal record",
                limit: u64::try_from(self.limits.max_payload_bytes).unwrap_or(u64::MAX),
                required: u64::try_from(event.modeled_bytes()).unwrap_or(u64::MAX),
            });
        }
        admit_rollup_state_journal_encoding(
            std::slice::from_ref(&event),
            self.memory_limit_bytes,
            self.retained_memory_bytes,
        )?;
        let encoded =
            encode_events(std::slice::from_ref(&event), self.limits)?.ok_or_else(|| {
                TsinkError::MaintenanceWorkItemTooLarge {
                    operation: "rollup source state journal record",
                    limit: u64::try_from(self.limits.max_payload_bytes).unwrap_or(u64::MAX),
                    required: u64::try_from(event.modeled_bytes()).unwrap_or(u64::MAX),
                }
            })?;
        let payload_bytes = encoded
            .len()
            .checked_sub(ROLLUP_STATE_JOURNAL_HEADER_LEN)
            .expect("encoded rollup event includes its fixed header");
        self.ensure_batch_for_payload(payload_bytes, event.epoch)?;
        self.make_namespace_room(1, event.epoch)?;
        if self.batch_dir.is_none() {
            self.ensure_batch_for_payload(payload_bytes, event.epoch)?;
        }
        let sequence = u64::try_from(self.batch_event_count).map_err(|_| {
            TsinkError::Other("rollup state journal event sequence exceeds u64".to_string())
        })?;
        let path = batch_event_path(
            self.batch_dir
                .as_deref()
                .expect("batch capacity creates a journal directory"),
            sequence,
        );
        write_new_journal_event(
            &path,
            &encoded,
            self.local_disk_budget.as_ref(),
            crate::DiskReservationKind::Growth,
            Some(&mut self.event_publication_may_be_ambiguous),
            self.memory_limit_bytes,
            self.retained_memory_bytes,
        )?;
        self.batch_event_count = self.batch_event_count.checked_add(1).ok_or_else(|| {
            TsinkError::Other("rollup state journal event count overflow".to_string())
        })?;
        self.batch_payload_bytes = self
            .batch_payload_bytes
            .checked_add(payload_bytes)
            .ok_or_else(|| {
                TsinkError::Other("rollup state journal batch payload overflow".to_string())
            })?;
        self.namespace_entry_count =
            self.namespace_entry_count.checked_add(1).ok_or_else(|| {
                TsinkError::Other("rollup state journal namespace count overflow".to_string())
            })?;
        self.event_publication_may_be_ambiguous = false;
        Ok(())
    }

    pub(super) fn persist_with_retained_memory(
        &mut self,
        event: RollupSourceStateEvent,
        operation_retained_memory_bytes: usize,
        steady_state_retained_memory_bytes: usize,
    ) -> Result<()> {
        let previous_retained_memory_bytes = self.retained_memory_bytes;
        self.retained_memory_bytes = operation_retained_memory_bytes;
        let result = self.persist(event);
        if result.is_ok() {
            self.retained_memory_bytes = steady_state_retained_memory_bytes;
        } else if !self.event_publication_may_be_ambiguous {
            self.retained_memory_bytes = previous_retained_memory_bytes;
        }
        result
    }

    pub(super) fn event_publication_may_be_ambiguous(&self) -> bool {
        self.event_publication_may_be_ambiguous
    }
}

#[cfg(test)]
pub(super) fn begin_rollup_state_journal_writer(
    rollup_dir: Option<&Path>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<RollupStateJournalWriter> {
    RollupStateJournalWriter::begin_with_limits(
        rollup_dir,
        local_disk_budget,
        RollupStateJournalLimits::default(),
    )
}

pub(super) fn begin_rollup_state_journal_writer_with_memory_limit(
    rollup_dir: Option<&Path>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    memory_limit_bytes: usize,
    retained_memory_bytes: usize,
) -> Result<RollupStateJournalWriter> {
    RollupStateJournalWriter::begin_with_limits_and_memory_limit(
        rollup_dir,
        local_disk_budget,
        RollupStateJournalLimits::default(),
        memory_limit_bytes,
        retained_memory_bytes,
    )
}

fn compact_or_prune_one_generation_pair(
    discovery: &RollupStateJournalDiscovery,
    epoch: u64,
    allow_atomic_rewrite: bool,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    limits: RollupStateJournalLimits,
    memory_limit_bytes: usize,
    retained_memory_bytes: usize,
) -> Result<bool> {
    let mut paths = Vec::<&Path>::new();
    let mut last_generation = None;
    for (generation, path) in &discovery.sealed {
        if last_generation == Some(*generation) {
            continue;
        }
        last_generation = Some(*generation);
        paths.push(path);
    }
    let mut global_entry_count = discovery.top_level_entry_count;
    for (index, oldest_path) in paths.iter().copied().enumerate() {
        let oldest_is_batch = oldest_path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(parse_batch_generation)
            .is_some();
        if oldest_is_batch {
            continue;
        }
        let oldest_current = compact_events(
            read_generation_events(oldest_path, limits, &mut global_entry_count)?,
            epoch,
        );
        if oldest_current.is_empty() {
            remove_rollup_state_journal_paths_batched(
                vec![RollupStateJournalCleanupOperation::File(
                    oldest_path.to_path_buf(),
                )],
                local_disk_budget,
                "rollup state journal stale-generation pruning",
                memory_limit_bytes,
            )?;
            return Ok(true);
        }
        // Rewriting the newer regular generation uses an atomic sibling temporary. Stale
        // generations above remain entry-decreasing and may be pruned at exact capacity, but a
        // crash-safe replacement may begin only while the shared recovery namespace has one free
        // slot for that temporary.
        if !allow_atomic_rewrite || discovery.entry_count >= limits.max_namespace_entries {
            continue;
        }

        let Some(newer_path) = paths.get(index.saturating_add(1)).copied() else {
            continue;
        };
        let newer_is_batch = newer_path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(parse_batch_generation)
            .is_some();
        if newer_is_batch {
            continue;
        }
        let newer = read_generation_events(newer_path, limits, &mut global_entry_count)?;
        let compacted = compact_events(oldest_current.into_iter().chain(newer), epoch);
        admit_rollup_state_journal_encoding(&compacted, memory_limit_bytes, retained_memory_bytes)?;
        let Some(encoded) = encode_events(&compacted, limits)? else {
            continue;
        };

        // Publish the combined newer generation before removing the older input. If cleanup is
        // interrupted, replay sees `older, combined`; source-state records are replacements, so
        // that duplicate prefix is equivalent to replaying the combined generation once.
        write_file_atomically_and_sync_parent_budgeted_with_reconciliation_memory_limit(
            newer_path,
            &encoded,
            local_disk_budget,
            crate::DiskCategory::Rollups,
            crate::DiskReservationKind::Maintenance,
            memory_limit_bytes,
        )?;
        remove_rollup_state_journal_paths_batched(
            vec![RollupStateJournalCleanupOperation::File(
                oldest_path.to_path_buf(),
            )],
            local_disk_budget,
            "rollup state journal adjacent-generation cleanup",
            memory_limit_bytes,
        )?;
        return Ok(true);
    }
    Ok(false)
}

fn generation_pair_can_compact(
    oldest_path: &Path,
    newer_path: &Path,
    epoch: u64,
    limits: RollupStateJournalLimits,
    memory_limit_bytes: usize,
    retained_memory_bytes: usize,
) -> Result<bool> {
    let mut global_entry_count = 0;
    let oldest_current = compact_events(
        read_generation_events(oldest_path, limits, &mut global_entry_count)?,
        epoch,
    );
    if oldest_current.is_empty() {
        return Ok(true);
    }
    let compacted = compact_events(
        oldest_current.into_iter().chain(read_generation_events(
            newer_path,
            limits,
            &mut global_entry_count,
        )?),
        epoch,
    );
    admit_rollup_state_journal_encoding(&compacted, memory_limit_bytes, retained_memory_bytes)?;
    Ok(encode_events(&compacted, limits)?.is_some())
}

#[cfg(test)]
fn persist_event_with_limits(
    rollup_dir: &Path,
    event: RollupSourceStateEvent,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    limits: RollupStateJournalLimits,
) -> Result<()> {
    if limits.max_events == 0
        || limits.max_payload_bytes == 0
        || limits.max_generations == 0
        || limits.max_namespace_entries == 0
        || limits.max_namespace_entries > MAX_RECOVERY_NAMESPACE_ENTRIES
    {
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
    let mut discovery =
        discover_journal_with_namespace_limit(&journal_dir, limits.max_namespace_entries)?;
    let active_path = active_journal_path(&journal_dir);
    if discovery.entry_count >= limits.max_namespace_entries {
        return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
            operation: "rollup state journal directory",
            limit: limits.max_namespace_entries,
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
                discovery.entry_count < limits.max_namespace_entries,
                local_disk_budget,
                limits,
                usize::MAX,
                0,
            )?
        {
            discovery =
                discover_journal_with_namespace_limit(&journal_dir, limits.max_namespace_entries)?;
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

    if compact_or_prune_one_generation_pair(
        &discovery,
        event.epoch,
        discovery.entry_count < limits.max_namespace_entries,
        local_disk_budget,
        limits,
        usize::MAX,
        0,
    )? {
        discovery =
            discover_journal_with_namespace_limit(&journal_dir, limits.max_namespace_entries)?;
    }

    let recognized_generations = discovery.sealed.len() + usize::from(discovery.active.is_some());
    if recognized_generations >= limits.max_generations {
        return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
            operation: "rollup state journal",
            limit: limits.max_generations,
            required: recognized_generations.saturating_add(1),
        });
    }
    if discovery.entry_count >= limits.max_namespace_entries {
        return Err(TsinkError::MaintenanceNamespaceLimitExceeded {
            operation: "rollup state journal directory",
            limit: limits.max_namespace_entries,
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
        return Err(generation_number_exhausted_error());
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

#[cfg(test)]
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

#[cfg(test)]
pub(super) fn cleanup_rollup_state_journal(
    rollup_dir: Option<&Path>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    cleanup_rollup_state_journal_with_memory_limit(rollup_dir, local_disk_budget, usize::MAX, 0)
}

pub(super) fn cleanup_rollup_state_journal_with_memory_limit(
    rollup_dir: Option<&Path>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    memory_limit_bytes: usize,
    retained_memory_bytes: usize,
) -> Result<()> {
    let Some(rollup_dir) = rollup_dir else {
        return Ok(());
    };
    let journal_dir = journal_dir_path(rollup_dir);
    with_serialized_rollup_state_journal_mutation(&journal_dir, local_disk_budget, || {
        cleanup_rollup_state_journal_locked(
            &journal_dir,
            local_disk_budget,
            memory_limit_bytes,
            retained_memory_bytes,
        )
    })
}

fn cleanup_rollup_state_journal_locked(
    journal_dir: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    memory_limit_bytes: usize,
    retained_memory_bytes: usize,
) -> Result<()> {
    let discovery = recover_rollup_state_journal_artifacts(
        journal_dir,
        local_disk_budget,
        RollupStateJournalLimits::default(),
        memory_limit_bytes,
        retained_memory_bytes,
    )?;
    if discovery.generation_count() > ROLLUP_STATE_JOURNAL_MAX_GENERATIONS {
        return Err(TsinkError::DataCorruption(format!(
            "rollup state journal cleanup exceeds its {ROLLUP_STATE_JOURNAL_MAX_GENERATIONS}-generation bound"
        )));
    }

    // Preflight every recognized batch before the first mutation. That makes an unknown,
    // lookalike, link-like, or wrong-type child an operator-visible corruption error without
    // partially cleaning otherwise valid generations.
    let mut operations = Vec::new();
    operations
        .try_reserve_exact(discovery.sealed.len() + usize::from(discovery.active.is_some()))
        .map_err(|_| {
            TsinkError::Other("rollup state journal cleanup plan allocation failed".to_string())
        })?;
    let mut global_entry_count = discovery.top_level_entry_count;
    for path in discovery
        .sealed
        .into_iter()
        .map(|(_, path)| path)
        .chain(discovery.active)
    {
        let is_batch = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(parse_batch_generation)
            .is_some();
        if is_batch {
            operations.push(RollupStateJournalCleanupOperation::Batch(
                plan_rollup_state_journal_batch_cleanup(
                    &path,
                    &mut global_entry_count,
                    MAX_RECOVERY_NAMESPACE_ENTRIES,
                )?,
            ));
        } else {
            operations.push(RollupStateJournalCleanupOperation::File(path));
        }
    }

    // A full snapshot makes every recognized journal generation obsolete at once. Hold one
    // zero-byte Recovery reservation across the complete cleanup so a managed directory with many
    // generations needs one exact terminal scan instead of one full-root scan per entry.
    remove_rollup_state_journal_paths_batched(
        operations,
        local_disk_budget,
        "rollup state journal cleanup",
        memory_limit_bytes,
    )
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
        let envelope_usage = super::super::runtime::rollup_state_envelope_usage(
            &[],
            &state.checkpoints,
            &state.generations,
            &state.pending_materializations,
            &state.pending_delete_invalidations,
        )
        .unwrap();
        load_rollup_state_journal(
            Some(rollup_dir),
            epoch,
            &mut state,
            envelope_usage,
            None,
            usize::MAX,
        )
        .unwrap();
        state
    }

    fn checkpoint(state: &LoadedRollupState, source_key: &str) -> Option<i64> {
        state
            .checkpoints
            .get("policy-a")
            .and_then(|entries| entries.get(source_key))
            .copied()
    }

    fn journal_file_bytes(rollup_dir: &Path) -> u64 {
        fs::read_dir(rollup_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .map(|path| {
                if path.is_dir() {
                    fs::read_dir(path)
                        .unwrap()
                        .map(|entry| entry.unwrap().metadata().unwrap().len())
                        .sum()
                } else {
                    fs::metadata(path).unwrap().len()
                }
            })
            .sum()
    }

    fn write_raw_event_frame(path: &Path, declared_count: u32, payload: &[u8]) {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&ROLLUP_STATE_JOURNAL_MAGIC);
        append_u16(&mut encoded, ROLLUP_STATE_JOURNAL_VERSION);
        append_u16(&mut encoded, 0);
        append_u32(&mut encoded, declared_count);
        append_u64(&mut encoded, u64::try_from(payload.len()).unwrap());
        append_u32(&mut encoded, checksum32(payload));
        encoded.extend_from_slice(payload);
        fs::write(path, encoded).unwrap();
    }

    #[test]
    fn event_array_preflight_checks_schema_then_declared_count_before_vec_materialization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("journal.bin");
        let payload = serde_json::to_vec(&vec![
            completed_event(1, "a", 1),
            completed_event(1, "b", 2),
        ])
        .unwrap();
        write_raw_event_frame(&path, 1, &payload);
        reset_journal_event_vec_materializations();
        let error = read_events(&path, RollupStateJournalLimits::default()).unwrap_err();
        assert!(matches!(error, TsinkError::DataCorruption(_)));
        assert!(error.to_string().contains("event count mismatch"));
        assert_eq!(journal_event_vec_materializations(), 0);

        let mut malformed = serde_json::to_vec(&vec![completed_event(1, "a", 1)]).unwrap();
        malformed.pop();
        malformed.extend_from_slice(br#",{"epoch":}]"#);
        write_raw_event_frame(&path, 1, &malformed);
        let error = read_events(&path, RollupStateJournalLimits::default()).unwrap_err();
        assert!(matches!(error, TsinkError::Json(_)));
        assert_eq!(journal_event_vec_materializations(), 0);
    }

    #[test]
    fn writer_batches_events_and_restart_replays_with_exact_accounting() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let before = budget.snapshot();
        let lock_attempts_before = budget.managed_file_mutation_lock_attempts_for_test();
        let mut writer =
            begin_rollup_state_journal_writer(Some(&rollup_dir), Some(&budget)).unwrap();
        writer.persist(completed_event(12, "a", 1)).unwrap();
        writer.persist(completed_event(12, "b", 2)).unwrap();
        writer.persist(completed_event(12, "a", 3)).unwrap();
        assert_eq!(
            budget.managed_file_mutation_lock_attempts_for_test(),
            lock_attempts_before + 4,
            "writer discovery and each event publication must take the shared managed mutation lock exactly once"
        );

        let discovery = discover_journal(&rollup_dir).unwrap();
        assert_eq!(discovery.generation_count(), 1);
        let batch = discovery.sealed[0].1.clone();
        assert_eq!(fs::read_dir(&batch).unwrap().count(), 3);
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 12), "a"), Some(3));
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 12), "b"), Some(2));
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.reconciliations_total, before.reconciliations_total);
        assert_eq!(snapshot.accounted_bytes, journal_file_bytes(&rollup_dir));
        assert_eq!(snapshot.active_reservations, 0);

        drop(writer);
        let mut restarted =
            begin_rollup_state_journal_writer(Some(&rollup_dir), Some(&budget)).unwrap();
        restarted.persist(completed_event(12, "b", 4)).unwrap();
        assert_eq!(discover_journal(&rollup_dir).unwrap().generation_count(), 2);
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 12), "a"), Some(3));
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 12), "b"), Some(4));
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, journal_file_bytes(&rollup_dir));
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(
            budget.managed_file_mutation_lock_attempts_for_test(),
            lock_attempts_before + 6,
        );
        cleanup_rollup_state_journal(Some(&rollup_dir), Some(&budget)).unwrap();
        assert_eq!(
            budget.managed_file_mutation_lock_attempts_for_test(),
            lock_attempts_before + 7,
            "full journal cleanup must share the same mutation lock without reacquiring it"
        );
    }

    #[test]
    fn writer_packs_and_compacts_before_small_namespace_or_generation_rejection() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let limits = RollupStateJournalLimits {
            max_events: 2,
            max_payload_bytes: 64 * 1024,
            max_generations: 3,
            max_namespace_entries: 5,
        };
        let mut writer =
            RollupStateJournalWriter::begin_with_limits(Some(&rollup_dir), None, limits).unwrap();
        for checkpoint in 1..=12 {
            writer
                .persist(completed_event(13, "same-source", checkpoint))
                .unwrap();
        }
        let discovery =
            discover_journal_with_namespace_limit(&rollup_dir, limits.max_namespace_entries)
                .unwrap();
        assert!(discovery.generation_count() <= limits.max_generations);
        assert!(discovery.entry_count < limits.max_namespace_entries);
        assert_eq!(
            checkpoint(&load_test_state(&rollup_dir, 13), "same-source"),
            Some(12)
        );
    }

    #[test]
    fn compaction_searches_later_adjacent_pairs_when_oldest_pair_is_full() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let limits = RollupStateJournalLimits {
            max_events: 2,
            max_payload_bytes: 64 * 1024,
            max_generations: 3,
            max_namespace_entries: 32,
        };
        write_events(
            &sealed_journal_path(&rollup_dir, 1),
            &[completed_event(40, "a", 1), completed_event(40, "b", 1)],
            None,
            limits,
        )
        .unwrap();
        write_events(
            &sealed_journal_path(&rollup_dir, 2),
            &[completed_event(40, "c", 1), completed_event(40, "d", 1)],
            None,
            limits,
        )
        .unwrap();
        write_events(
            &sealed_journal_path(&rollup_dir, 3),
            &[completed_event(40, "c", 2), completed_event(40, "d", 2)],
            None,
            limits,
        )
        .unwrap();

        let mut writer =
            RollupStateJournalWriter::begin_with_limits(Some(&rollup_dir), None, limits).unwrap();
        writer.persist(completed_event(40, "e", 1)).unwrap();
        assert!(sealed_journal_path(&rollup_dir, 1).is_file());
        assert!(!sealed_journal_path(&rollup_dir, 2).exists());
        let loaded = load_test_state(&rollup_dir, 40);
        assert_eq!(checkpoint(&loaded, "a"), Some(1));
        assert_eq!(checkpoint(&loaded, "c"), Some(2));
        assert_eq!(checkpoint(&loaded, "e"), Some(1));
    }

    #[test]
    fn compaction_rejects_without_mutation_when_every_adjacent_pair_is_full() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let limits = RollupStateJournalLimits {
            max_events: 2,
            max_payload_bytes: 64 * 1024,
            max_generations: 3,
            max_namespace_entries: 32,
        };
        for (generation, left, right) in [(1, "a", "b"), (2, "c", "d"), (3, "e", "f")] {
            write_events(
                &sealed_journal_path(&rollup_dir, generation),
                &[completed_event(41, left, 1), completed_event(41, right, 1)],
                None,
                limits,
            )
            .unwrap();
        }
        let before = (1..=3)
            .map(|generation| fs::read(sealed_journal_path(&rollup_dir, generation)).unwrap())
            .collect::<Vec<_>>();
        let mut writer =
            RollupStateJournalWriter::begin_with_limits(Some(&rollup_dir), None, limits).unwrap();
        let error = writer
            .persist(completed_event(41, "g", 1))
            .expect_err("all bounded adjacent unions remain too large");
        assert!(matches!(
            error,
            TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "rollup state journal",
                limit: 3,
                ..
            }
        ));
        let after = (1..=3)
            .map(|generation| fs::read(sealed_journal_path(&rollup_dir, generation)).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(after, before);
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 41), "g"), None);
    }

    #[test]
    fn regular_pair_rewrite_requires_one_free_namespace_slot() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let limits = RollupStateJournalLimits {
            max_events: 2,
            max_payload_bytes: 64 * 1024,
            max_generations: 2,
            max_namespace_entries: 3,
        };
        let oldest = sealed_journal_path(&rollup_dir, 1);
        let newer = sealed_journal_path(&rollup_dir, 2);
        write_events(&oldest, &[completed_event(42, "a", 1)], None, limits).unwrap();
        write_events(&newer, &[completed_event(42, "a", 2)], None, limits).unwrap();
        let operator_entry = rollup_dir.join("operator.keep");
        fs::write(&operator_entry, b"keep").unwrap();
        let saturated = discover_journal_with_namespace_limit(&rollup_dir, 3).unwrap();
        assert_eq!(saturated.entry_count, limits.max_namespace_entries);
        let oldest_before = fs::read(&oldest).unwrap();
        let newer_before = fs::read(&newer).unwrap();

        assert!(!compact_or_prune_one_generation_pair(
            &saturated,
            42,
            false,
            None,
            limits,
            usize::MAX,
            0,
        )
        .unwrap());
        assert_eq!(fs::read(&oldest).unwrap(), oldest_before);
        assert_eq!(fs::read(&newer).unwrap(), newer_before);
        assert_eq!(fs::read(&operator_entry).unwrap(), b"keep");

        fs::remove_file(&operator_entry).unwrap();
        let one_free_slot = discover_journal_with_namespace_limit(&rollup_dir, 3).unwrap();
        assert_eq!(one_free_slot.entry_count, limits.max_namespace_entries - 1);
        assert!(compact_or_prune_one_generation_pair(
            &one_free_slot,
            42,
            true,
            None,
            limits,
            usize::MAX,
            0,
        )
        .unwrap());
        assert!(!oldest.exists());
        assert_eq!(
            read_events(&newer, limits).unwrap(),
            vec![completed_event(42, "a", 2)]
        );
    }

    #[test]
    fn stale_regular_generation_can_be_pruned_at_exact_namespace_capacity() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let limits = RollupStateJournalLimits {
            max_events: 2,
            max_payload_bytes: 64 * 1024,
            max_generations: 2,
            max_namespace_entries: 3,
        };
        let stale = sealed_journal_path(&rollup_dir, 1);
        let current = sealed_journal_path(&rollup_dir, 2);
        write_events(&stale, &[completed_event(41, "stale", 1)], None, limits).unwrap();
        write_events(&current, &[completed_event(42, "current", 2)], None, limits).unwrap();
        fs::write(rollup_dir.join("operator.keep"), b"keep").unwrap();
        let saturated = discover_journal_with_namespace_limit(&rollup_dir, 3).unwrap();
        assert_eq!(saturated.entry_count, limits.max_namespace_entries);

        assert!(compact_or_prune_one_generation_pair(
            &saturated,
            42,
            false,
            None,
            limits,
            usize::MAX,
            0,
        )
        .unwrap());
        assert!(!stale.exists());
        assert!(current.is_file());
    }

    #[test]
    fn crash_temporary_from_one_free_slot_is_recovered_at_exact_capacity() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let limits = RollupStateJournalLimits {
            max_events: 2,
            max_payload_bytes: 64 * 1024,
            max_generations: 2,
            max_namespace_entries: 5,
        };
        let oldest = sealed_journal_path(&rollup_dir, 1);
        let newer = sealed_journal_path(&rollup_dir, 2);
        write_events(&oldest, &[completed_event(42, "a", 1)], None, limits).unwrap();
        write_events(&newer, &[completed_event(42, "a", 2)], None, limits).unwrap();
        fs::write(rollup_dir.join("operator-a.keep"), b"a").unwrap();
        fs::write(rollup_dir.join("operator-b.keep"), b"b").unwrap();
        assert_eq!(
            discover_journal_with_namespace_limit(&rollup_dir, 5)
                .unwrap()
                .entry_count,
            limits.max_namespace_entries - 1
        );
        let temporary = exact_atomic_temporary_for(&newer, "0000000000000001");
        fs::write(&temporary, b"interrupted regular-pair rewrite").unwrap();
        assert_eq!(fs::read_dir(&rollup_dir).unwrap().count(), 5);

        let writer =
            RollupStateJournalWriter::begin_with_limits(Some(&rollup_dir), None, limits).unwrap();
        assert!(!temporary.exists());
        assert!(oldest.is_file());
        assert!(newer.is_file());
        assert_eq!(
            writer.namespace_entry_count,
            limits.max_namespace_entries - 1
        );
    }

    #[test]
    fn namespace_limit_is_shared_exactly_across_top_level_and_batch_children() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let batch = batch_journal_path(&rollup_dir, 1);
        fs::create_dir(&batch).unwrap();
        for sequence in 0..3 {
            let encoded = encode_events(
                &[completed_event(
                    14,
                    &format!("s{sequence}"),
                    sequence as i64,
                )],
                RollupStateJournalLimits::default(),
            )
            .unwrap()
            .unwrap();
            fs::write(batch_event_path(&batch, sequence), encoded).unwrap();
        }
        assert_eq!(
            discover_journal_with_namespace_limit(&rollup_dir, 4)
                .unwrap()
                .entry_count,
            4
        );
        let mut before_entries = fs::read_dir(&batch)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        before_entries.sort();
        let saturated_limits = RollupStateJournalLimits {
            max_namespace_entries: 4,
            ..RollupStateJournalLimits::default()
        };
        let mut saturated =
            RollupStateJournalWriter::begin_with_limits(Some(&rollup_dir), None, saturated_limits)
                .unwrap();
        let error = saturated
            .persist(completed_event(14, "new", 9))
            .expect_err("inherited saturation must not publish a max-plus-one pack shadow");
        assert!(matches!(
            error,
            TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "rollup state journal directory",
                limit: 4,
                ..
            }
        ));
        let mut after_entries = fs::read_dir(&batch)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        after_entries.sort();
        assert_eq!(after_entries, before_entries);
        assert!(!sealed_journal_path(&rollup_dir, 1).exists());
        fs::write(rollup_dir.join("operator.keep"), b"x").unwrap();
        let error = discover_journal_with_namespace_limit(&rollup_dir, 4)
            .expect_err("top-level plus nested N+1 must exceed one shared bound");
        assert!(matches!(
            error,
            TsinkError::MaintenanceNamespaceLimitExceeded {
                limit: 4,
                required: 5,
                ..
            }
        ));
    }

    #[test]
    fn legacy_active_nonce_exhaustion_is_detected_before_any_rename() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let active = active_journal_path(&rollup_dir);
        let sealed = sealed_journal_path(&rollup_dir, u64::MAX - 1);
        write_events(
            &sealed,
            &[completed_event(15, "sealed", 1)],
            None,
            RollupStateJournalLimits::default(),
        )
        .unwrap();
        write_events(
            &active,
            &[completed_event(15, "active", 2)],
            None,
            RollupStateJournalLimits::default(),
        )
        .unwrap();
        let active_bytes = fs::read(&active).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        let error = writer
            .persist(completed_event(15, "new", 3))
            .expect_err("sealing active plus creating a batch needs two monotonic nonces");
        assert!(matches!(
            error,
            TsinkError::UnsupportedOperation {
                operation: "rollup state journal generation allocation",
                ..
            }
        ));
        assert_eq!(fs::read(&active).unwrap(), active_bytes);
        assert!(sealed.is_file());
        assert!(!sealed_journal_path(&rollup_dir, u64::MAX).exists());
    }

    #[test]
    fn writer_compacts_legacy_active_pair_without_losing_generation_progress() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let limits = RollupStateJournalLimits {
            max_events: 1,
            max_payload_bytes: 64 * 1024,
            max_generations: 2,
            max_namespace_entries: 16,
        };
        write_events(
            &sealed_journal_path(&rollup_dir, 7),
            &[completed_event(22, "same", 1)],
            None,
            limits,
        )
        .unwrap();
        write_events(
            &active_journal_path(&rollup_dir),
            &[completed_event(22, "same", 2)],
            None,
            limits,
        )
        .unwrap();
        let mut writer =
            RollupStateJournalWriter::begin_with_limits(Some(&rollup_dir), None, limits).unwrap();
        writer.persist(completed_event(22, "same", 3)).unwrap();
        assert!(!active_journal_path(&rollup_dir).exists());
        assert!(sealed_journal_path(&rollup_dir, 8).is_file());
        assert_eq!(
            checkpoint(&load_test_state(&rollup_dir, 22), "same"),
            Some(3)
        );
    }

    #[test]
    fn uncompactable_legacy_active_pair_rejects_before_rename() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let limits = RollupStateJournalLimits {
            max_events: 1,
            max_payload_bytes: 64 * 1024,
            max_generations: 2,
            max_namespace_entries: 16,
        };
        let sealed = sealed_journal_path(&rollup_dir, 7);
        let active = active_journal_path(&rollup_dir);
        write_events(&sealed, &[completed_event(23, "a", 1)], None, limits).unwrap();
        write_events(&active, &[completed_event(23, "b", 1)], None, limits).unwrap();
        let active_bytes = fs::read(&active).unwrap();
        let mut writer =
            RollupStateJournalWriter::begin_with_limits(Some(&rollup_dir), None, limits).unwrap();
        let error = writer
            .persist(completed_event(23, "c", 1))
            .expect_err("three distinct max-one-event generations cannot be compacted");
        assert!(matches!(
            error,
            TsinkError::MaintenanceNamespaceLimitExceeded {
                operation: "rollup state journal",
                limit: 2,
                ..
            }
        ));
        assert_eq!(fs::read(active).unwrap(), active_bytes);
        assert!(sealed.is_file());
        assert!(!sealed_journal_path(&rollup_dir, 8).exists());
    }

    #[test]
    fn adjacent_combined_generation_replays_safely_before_oldest_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let limits = RollupStateJournalLimits::default();
        let oldest = sealed_journal_path(&rollup_dir, 1);
        let newer = sealed_journal_path(&rollup_dir, 2);
        write_events(&oldest, &[completed_event(24, "a", 1)], None, limits).unwrap();
        write_events(&newer, &[completed_event(24, "b", 2)], None, limits).unwrap();
        write_events(
            &newer,
            &[completed_event(24, "a", 1), completed_event(24, "b", 2)],
            None,
            limits,
        )
        .unwrap();
        let loaded = load_test_state(&rollup_dir, 24);
        assert_eq!(checkpoint(&loaded, "a"), Some(1));
        assert_eq!(checkpoint(&loaded, "b"), Some(2));
        assert!(oldest.is_file());
        assert!(newer.is_file());
    }

    #[test]
    fn batch_cleanup_rejects_unknown_children_before_mutation() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer.persist(completed_event(16, "a", 1)).unwrap();
        let batch = discover_journal(&rollup_dir).unwrap().sealed[0].1.clone();
        let event = batch_event_path(&batch, 0);
        let unknown = batch.join("operator.keep");
        fs::write(&unknown, b"preserve").unwrap();

        let error = cleanup_rollup_state_journal(Some(&rollup_dir), None)
            .expect_err("an unknown batch child must require operator intervention");
        assert!(matches!(error, TsinkError::DataCorruption(_)));
        assert!(event.is_file());
        assert_eq!(fs::read(unknown).unwrap(), b"preserve");
    }

    #[test]
    fn planned_batch_cleanup_rescans_changed_contents_and_preserves_late_child() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer.persist(completed_event(17, "a", 1)).unwrap();
        let batch = discover_journal(&rollup_dir).unwrap().sealed[0].1.clone();
        let mut count = 1;
        let plan = plan_rollup_state_journal_batch_cleanup(
            &batch,
            &mut count,
            MAX_RECOVERY_NAMESPACE_ENTRIES,
        )
        .unwrap();
        let original = rollup_dir.join("original-batch");
        fs::rename(&batch, &original).unwrap();
        fs::create_dir(&batch).unwrap();
        let replacement = batch.join("replacement.keep");
        fs::write(&replacement, b"replacement").unwrap();
        let error = plan
            .remove()
            .expect_err("replacement contents outside the plan must fail closed on rescan");
        assert!(matches!(error, TsinkError::DataCorruption(_)));
        assert_eq!(fs::read(&replacement).unwrap(), b"replacement");
        assert!(batch_event_path(&original, 0).is_file());

        fs::remove_file(&replacement).unwrap();
        fs::remove_dir(&batch).unwrap();
        fs::rename(&original, &batch).unwrap();
        let mut count = 1;
        let late_plan = plan_rollup_state_journal_batch_cleanup(
            &batch,
            &mut count,
            MAX_RECOVERY_NAMESPACE_ENTRIES,
        )
        .unwrap();
        let late = batch_event_path(&batch, 1);
        fs::write(
            &late,
            encode_events(
                &[completed_event(17, "late", 2)],
                RollupStateJournalLimits::default(),
            )
            .unwrap()
            .unwrap(),
        )
        .unwrap();
        late_plan
            .remove()
            .expect_err("a late child must prevent final directory removal");
        assert!(batch_event_path(&batch, 0).is_file());
        assert!(late.is_file());
    }

    #[test]
    fn packed_shadow_replays_with_its_batch_and_restart_finishes_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer.persist(completed_event(18, "a", 1)).unwrap();
        writer.persist(completed_event(18, "b", 2)).unwrap();
        let discovery = discover_journal(&rollup_dir).unwrap();
        let (generation, batch) = discovery.sealed[0].clone();
        let mut count = discovery.top_level_entry_count;
        let events =
            read_generation_events(&batch, RollupStateJournalLimits::default(), &mut count)
                .unwrap();
        let shadow = sealed_journal_path(&rollup_dir, generation);
        fs::write(
            &shadow,
            encode_events(&events, RollupStateJournalLimits::default())
                .unwrap()
                .unwrap(),
        )
        .unwrap();

        let shadowed = discover_journal(&rollup_dir).unwrap();
        assert_eq!(shadowed.sealed.len(), 2);
        assert_eq!(shadowed.generation_count(), 1);
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 18), "a"), Some(1));
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 18), "b"), Some(2));

        drop(writer);
        let _restarted = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        assert!(!batch.exists());
        assert!(shadow.is_file());
        assert_eq!(discover_journal(&rollup_dir).unwrap().sealed.len(), 1);
    }

    fn exact_atomic_temporary_for(path: &Path, nonce: &str) -> PathBuf {
        path.parent().unwrap().join(format!(
            ".{}.tmp-{}-{nonce}",
            path.file_name().unwrap().to_string_lossy(),
            std::process::id()
        ))
    }

    #[test]
    fn exact_batch_event_temporary_is_replay_inert_and_recovered() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer.persist(completed_event(31, "a", 1)).unwrap();
        drop(writer);
        let discovery = discover_journal(&rollup_dir).unwrap();
        let batch = discovery.sealed[0].1.clone();
        let temporary =
            exact_atomic_temporary_for(&batch_event_path(&batch, 1), "0000000000000001");
        fs::write(&temporary, b"interrupted publication").unwrap();

        let discovery = discover_journal(&rollup_dir).unwrap();
        let mut count = discovery.top_level_entry_count;
        let events =
            read_generation_events(&batch, RollupStateJournalLimits::default(), &mut count)
                .unwrap();
        assert_eq!(events, vec![completed_event(31, "a", 1)]);
        assert!(temporary.is_file());

        let _restarted = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        assert!(!temporary.exists());
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 31), "a"), Some(1));
    }

    #[test]
    fn malformed_batch_event_temporary_is_rejected_and_preserved() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer.persist(completed_event(32, "a", 1)).unwrap();
        drop(writer);
        let batch = discover_journal(&rollup_dir).unwrap().sealed[0].1.clone();
        let malformed =
            exact_atomic_temporary_for(&batch_event_path(&batch, 1), "000000000000000A");
        fs::write(&malformed, b"operator-owned lookalike").unwrap();
        let error = begin_rollup_state_journal_writer(Some(&rollup_dir), None)
            .err()
            .expect("uppercase nonce is not an owned atomic temporary");
        assert!(matches!(error, TsinkError::DataCorruption(_)));
        assert_eq!(fs::read(malformed).unwrap(), b"operator-owned lookalike");
    }

    #[test]
    fn exact_top_level_atomic_temporary_is_recovered_but_wrong_type_fails_closed() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let target = sealed_journal_path(&rollup_dir, 7);
        let temporary = exact_atomic_temporary_for(&target, "0000000000000001");
        fs::write(&temporary, b"shadow staging").unwrap();
        let _writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        assert!(!temporary.exists());

        let wrong_type = exact_atomic_temporary_for(&target, "0000000000000002");
        fs::create_dir(&wrong_type).unwrap();
        let sentinel = wrong_type.join("sentinel.keep");
        fs::write(&sentinel, b"preserve").unwrap();
        let error = begin_rollup_state_journal_writer(Some(&rollup_dir), None)
            .err()
            .expect("an exact owned temporary with the wrong type must fail closed");
        assert!(matches!(error, TsinkError::DataCorruption(_)));
        assert_eq!(fs::read(sentinel).unwrap(), b"preserve");
    }

    #[test]
    fn configured_memory_rejects_discovery_before_artifact_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let target = sealed_journal_path(&rollup_dir, 7);
        let temporary = exact_atomic_temporary_for(&target, "0000000000000001");
        fs::write(&temporary, b"interrupted publication").unwrap();
        let limits = RollupStateJournalLimits::default();
        let required = admit_rollup_state_journal_memory(&rollup_dir, limits, usize::MAX - 1, 0)
            .expect("a large finite budget must expose the modeled peak");
        assert!(required > 0);

        let error = RollupStateJournalWriter::begin_with_limits_and_memory_limit(
            Some(&rollup_dir),
            None,
            limits,
            required - 1,
            0,
        )
        .err()
        .expect("one byte below the modeled discovery peak must reject");
        assert!(matches!(
            error,
            TsinkError::MemoryBudgetExceeded {
                budget,
                required: observed,
            } if budget == required - 1 && observed == required
        ));
        assert_eq!(fs::read(&temporary).unwrap(), b"interrupted publication");

        RollupStateJournalWriter::begin_with_limits_and_memory_limit(
            Some(&rollup_dir),
            None,
            limits,
            required,
            0,
        )
        .unwrap();
        assert!(!temporary.exists());
    }

    #[test]
    fn detached_partial_batch_is_replay_inert_and_recovered() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer.persist(completed_event(33, "a", 1)).unwrap();
        writer.persist(completed_event(33, "b", 2)).unwrap();
        drop(writer);
        let (generation, batch) = discover_journal(&rollup_dir).unwrap().sealed[0].clone();
        let detached = cleanup_journal_path(&rollup_dir, generation);
        fs::rename(&batch, &detached).unwrap();
        fs::remove_file(batch_event_path(&detached, 0)).unwrap();
        let exact_temp =
            exact_atomic_temporary_for(&batch_event_path(&detached, 2), "0000000000000001");
        fs::write(&exact_temp, b"partial").unwrap();

        let loaded = load_test_state(&rollup_dir, 33);
        assert_eq!(checkpoint(&loaded, "a"), None);
        assert_eq!(checkpoint(&loaded, "b"), None);
        assert!(!detached.exists());
    }

    #[test]
    fn detached_cleanup_namespace_and_child_ownership_fail_closed() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let detached = cleanup_journal_path(&rollup_dir, 1);
        fs::create_dir(&detached).unwrap();
        let event = batch_event_path(&detached, 0);
        fs::write(&event, b"event").unwrap();
        let temporary =
            exact_atomic_temporary_for(&batch_event_path(&detached, 1), "0000000000000001");
        fs::write(&temporary, b"temporary").unwrap();
        assert_eq!(
            discover_journal_with_namespace_limit(&rollup_dir, 3)
                .unwrap()
                .entry_count,
            3
        );
        let operator = rollup_dir.join("operator.keep");
        fs::write(&operator, b"preserve").unwrap();
        let error = discover_journal_with_namespace_limit(&rollup_dir, 3)
            .expect_err("top plus detached children N+1 must exceed the shared cap");
        assert!(matches!(
            error,
            TsinkError::MaintenanceNamespaceLimitExceeded {
                limit: 3,
                required: 4,
                ..
            }
        ));
        fs::remove_file(&operator).unwrap();

        let malformed =
            exact_atomic_temporary_for(&batch_event_path(&detached, 2), "000000000000000A");
        fs::write(&malformed, b"lookalike").unwrap();
        let error = begin_rollup_state_journal_writer(Some(&rollup_dir), None)
            .err()
            .expect("malformed detached child must require operator inspection");
        assert!(matches!(error, TsinkError::DataCorruption(_)));
        assert_eq!(fs::read(&malformed).unwrap(), b"lookalike");
        fs::remove_file(&malformed).unwrap();

        fs::remove_file(&event).unwrap();
        fs::create_dir(&event).unwrap();
        let sentinel = event.join("sentinel.keep");
        fs::write(&sentinel, b"preserve").unwrap();
        let error = begin_rollup_state_journal_writer(Some(&rollup_dir), None)
            .err()
            .expect("detached canonical child with wrong type must fail closed");
        assert!(matches!(error, TsinkError::DataCorruption(_)));
        assert_eq!(fs::read(sentinel).unwrap(), b"preserve");
    }

    #[test]
    fn partially_deleted_detached_batch_replays_only_its_durable_shadow() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer.persist(completed_event(331, "a", 1)).unwrap();
        writer.persist(completed_event(331, "b", 2)).unwrap();
        drop(writer);
        let discovery = discover_journal(&rollup_dir).unwrap();
        let (generation, batch) = discovery.sealed[0].clone();
        let mut count = discovery.top_level_entry_count;
        let events =
            read_generation_events(&batch, RollupStateJournalLimits::default(), &mut count)
                .unwrap();
        write_events(
            &sealed_journal_path(&rollup_dir, generation),
            &events,
            None,
            RollupStateJournalLimits::default(),
        )
        .unwrap();
        let detached = cleanup_journal_path(&rollup_dir, generation);
        fs::rename(&batch, &detached).unwrap();
        fs::remove_file(batch_event_path(&detached, 0)).unwrap();

        let loaded = load_test_state(&rollup_dir, 331);
        assert_eq!(checkpoint(&loaded, "a"), Some(1));
        assert_eq!(checkpoint(&loaded, "b"), Some(2));
        assert!(!detached.exists());
        assert!(sealed_journal_path(&rollup_dir, generation).is_file());
    }

    #[test]
    fn same_epoch_snapshot_ignores_detached_cleanup_suffix_on_restart() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(ROLLUP_DIR_NAME);
        fs::create_dir_all(&rollup_dir).unwrap();
        let state_path = rollup_dir.join(ROLLUP_STATE_FILE_NAME);
        super::super::runtime::persist_rollup_state(
            Some(&state_path),
            &HashMap::from([(
                "policy-a".to_string(),
                BTreeMap::from([("a".to_string(), 9)]),
            )]),
            &HashMap::from([("policy-a".to_string(), 0)]),
            &HashMap::new(),
            &[],
        )
        .unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer.persist(completed_event(0, "a", 1)).unwrap();
        drop(writer);
        let (generation, batch) = discover_journal(&rollup_dir).unwrap().sealed[0].clone();
        let detached = cleanup_journal_path(&rollup_dir, generation);
        fs::rename(&batch, &detached).unwrap();

        let loaded = super::super::runtime::load_rollup_state(Some(&state_path)).unwrap();
        assert_eq!(checkpoint(&loaded, "a"), Some(9));
        assert!(!detached.exists());
    }

    #[test]
    fn batch_detach_sync_failure_never_deletes_children_and_restart_recovers() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer.persist(completed_event(34, "a", 1)).unwrap();
        drop(writer);
        let (generation, batch) = discover_journal(&rollup_dir).unwrap().sealed[0].clone();
        let mut count = 1;
        let plan = plan_rollup_state_journal_batch_cleanup(
            &batch,
            &mut count,
            MAX_RECOVERY_NAMESPACE_ENTRIES,
        )
        .unwrap();
        let _failure = crate::engine::fs_utils::fail_directory_sync_once(
            rollup_dir.clone(),
            "injected batch detach parent sync failure",
        );
        let error = plan
            .remove()
            .expect_err("rename sync ambiguity must stop before child deletion");
        assert!(error
            .to_string()
            .contains("injected batch detach parent sync failure"));
        let detached = cleanup_journal_path(&rollup_dir, generation);
        assert!(!batch.exists());
        assert!(batch_event_path(&detached, 0).is_file());

        let loaded = load_test_state(&rollup_dir, 34);
        assert_eq!(checkpoint(&loaded, "a"), None);
        assert!(!detached.exists());
    }

    #[test]
    fn full_cleanup_detaches_batch_before_same_generation_shadow() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer.persist(completed_event(341, "a", 1)).unwrap();
        drop(writer);
        let discovery = discover_journal(&rollup_dir).unwrap();
        let (generation, batch) = discovery.sealed[0].clone();
        let mut count = discovery.top_level_entry_count;
        let events =
            read_generation_events(&batch, RollupStateJournalLimits::default(), &mut count)
                .unwrap();
        let shadow = sealed_journal_path(&rollup_dir, generation);
        write_events(&shadow, &events, None, RollupStateJournalLimits::default()).unwrap();
        let _failure = crate::engine::fs_utils::fail_directory_sync_once(
            rollup_dir.clone(),
            "injected ordered full cleanup detach failure",
        );
        let error = cleanup_rollup_state_journal(Some(&rollup_dir), None)
            .expect_err("batch detach ambiguity must stop before shadow unlink");
        assert!(error
            .to_string()
            .contains("injected ordered full cleanup detach failure"));
        let detached = cleanup_journal_path(&rollup_dir, generation);
        assert!(batch_event_path(&detached, 0).is_file());
        assert!(shadow.is_file());

        let loaded = load_test_state(&rollup_dir, 341);
        assert_eq!(checkpoint(&loaded, "a"), Some(1));
        assert!(!detached.exists());
    }

    #[test]
    fn same_generation_batch_shadow_mismatch_rejects_before_state_mutation() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer.persist(completed_event(35, "a", 1)).unwrap();
        drop(writer);
        let (generation, batch) = discover_journal(&rollup_dir).unwrap().sealed[0].clone();
        let shadow = sealed_journal_path(&rollup_dir, generation);
        write_events(
            &shadow,
            &[completed_event(35, "a", 2)],
            None,
            RollupStateJournalLimits::default(),
        )
        .unwrap();
        let mut state = LoadedRollupState {
            journal_epoch: 35,
            generations: HashMap::from([("policy-a".to_string(), 0)]),
            ..LoadedRollupState::default()
        };
        let usage = super::super::runtime::rollup_state_envelope_usage(
            &[],
            &state.checkpoints,
            &state.generations,
            &state.pending_materializations,
            &state.pending_delete_invalidations,
        )
        .unwrap();
        let error =
            load_rollup_state_journal(Some(&rollup_dir), 35, &mut state, usage, None, usize::MAX)
                .expect_err("mismatched same-generation representations must fail closed");
        assert!(matches!(error, TsinkError::DataCorruption(_)));
        assert!(state.checkpoints.is_empty());
        assert!(batch.is_dir());
        assert!(shadow.is_file());
    }

    #[test]
    fn saturated_identical_batch_shadow_pair_finishes_without_new_namespace_entry() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let batch = batch_journal_path(&rollup_dir, 1);
        fs::create_dir(&batch).unwrap();
        let events = vec![completed_event(36, "a", 1), completed_event(36, "b", 2)];
        for (sequence, event) in events.iter().enumerate() {
            fs::write(
                batch_event_path(&batch, sequence as u64),
                encode_events(
                    std::slice::from_ref(event),
                    RollupStateJournalLimits::default(),
                )
                .unwrap()
                .unwrap(),
            )
            .unwrap();
        }
        write_events(
            &sealed_journal_path(&rollup_dir, 1),
            &events,
            None,
            RollupStateJournalLimits::default(),
        )
        .unwrap();
        let limits = RollupStateJournalLimits {
            max_namespace_entries: 4,
            ..RollupStateJournalLimits::default()
        };
        assert_eq!(
            discover_journal_with_namespace_limit(&rollup_dir, 4)
                .unwrap()
                .entry_count,
            4
        );
        let writer =
            RollupStateJournalWriter::begin_with_limits(Some(&rollup_dir), None, limits).unwrap();
        assert!(!batch.exists());
        assert_eq!(writer.namespace_entry_count, 1);
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 36), "b"), Some(2));
    }

    #[test]
    fn publication_enforces_combined_snapshot_item_envelope_incrementally() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(ROLLUP_DIR_NAME);
        fs::create_dir_all(&rollup_dir).unwrap();
        let runtime =
            RollupRuntimeState::new_with_disk_budget(Some(temp_dir.path().to_path_buf()), None);
        let checkpoints = (0..(ROLLUP_STATE_SNAPSHOT_MAX_ITEMS - 2))
            .map(|index| (format!("source-{index:05}"), index as i64))
            .collect::<BTreeMap<_, _>>();
        *runtime.checkpoints.write() = HashMap::from([("policy-a".to_string(), checkpoints)]);
        *runtime.generations.write() = HashMap::from([("policy-a".to_string(), 0)]);
        let usage = super::super::runtime::rollup_state_envelope_usage(
            &[],
            &runtime.checkpoints.read(),
            &runtime.generations.read(),
            &runtime.pending_materializations.read(),
            &runtime.pending_delete_invalidations.read(),
        )
        .unwrap();
        assert_eq!(usage.items, ROLLUP_STATE_SNAPSHOT_MAX_ITEMS - 1);
        *runtime.state_envelope_usage.lock() = usage;
        let store = RollupStateStoreContext { state: &runtime };
        let mut writer = store.begin_source_state_journal_writer().unwrap();

        store
            .persist_source_state_event(&mut writer, completed_event(0, "new-a", 1))
            .unwrap();
        assert_eq!(
            runtime.state_envelope_usage.lock().items,
            ROLLUP_STATE_SNAPSHOT_MAX_ITEMS
        );
        let batch = discover_journal(&rollup_dir).unwrap().sealed[0].1.clone();
        assert_eq!(fs::read_dir(&batch).unwrap().count(), 1);

        let error = store
            .persist_source_state_event(&mut writer, completed_event(0, "new-b", 2))
            .expect_err("N+1 must be rejected before journal or memory growth");
        assert!(matches!(
            error,
            TsinkError::MaintenanceDependencyWindowExceeded {
                operation: "rollup source state journal publication",
                selected_items,
                ..
            } if selected_items == ROLLUP_STATE_SNAPSHOT_MAX_ITEMS + 1
        ));
        assert_eq!(fs::read_dir(&batch).unwrap().count(), 1);
        assert!(!runtime
            .checkpoints
            .read()
            .get("policy-a")
            .unwrap()
            .contains_key("new-b"));
        assert!(!runtime.snapshot_publication_fenced.load(Ordering::Acquire));

        store
            .persist_source_state_event(&mut writer, completed_event(0, "source-00000", 99))
            .unwrap();
        assert_eq!(fs::read_dir(&batch).unwrap().count(), 2);
        assert_eq!(
            runtime.state_envelope_usage.lock().items,
            ROLLUP_STATE_SNAPSHOT_MAX_ITEMS
        );

        store
            .persist_source_state_event(
                &mut writer,
                RollupSourceStateEvent {
                    epoch: 0,
                    policy_id: "policy-a".to_string(),
                    source_key: "new-a".to_string(),
                    generation: 0,
                    checkpoint: None,
                    pending: None,
                },
            )
            .unwrap();
        assert_eq!(
            runtime.state_envelope_usage.lock().items,
            ROLLUP_STATE_SNAPSHOT_MAX_ITEMS - 1
        );
        assert!(!runtime
            .checkpoints
            .read()
            .get("policy-a")
            .unwrap()
            .contains_key("new-a"));
        store
            .persist_source_state_event(&mut writer, completed_event(0, "new-b", 2))
            .unwrap();
        assert_eq!(
            runtime.state_envelope_usage.lock().items,
            ROLLUP_STATE_SNAPSHOT_MAX_ITEMS
        );
    }

    #[test]
    fn definite_event_quota_rejection_does_not_fence_and_can_be_retried() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(ROLLUP_DIR_NAME);
        fs::create_dir_all(&rollup_dir).unwrap();
        let event = completed_event(0, "a", 1);
        let encoded = encode_events(
            std::slice::from_ref(&event),
            RollupStateJournalLimits::default(),
        )
        .unwrap()
        .unwrap();
        let probe_budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let entry_allowance = probe_budget
            .snapshot_restore_entry_staging_allowance_bytes()
            .unwrap();
        drop(probe_budget);

        let filler = temp_dir.path().join("quota-filler");
        fs::write(&filler, vec![0; encoded.len()]).unwrap();
        let encoded_bytes = u64::try_from(encoded.len()).unwrap();
        let budget = crate::LocalDiskBudget::open(
            temp_dir.path(),
            crate::LocalDiskLimits {
                max_bytes: Some(entry_allowance.checked_add(encoded_bytes).unwrap()),
                ..crate::LocalDiskLimits::default()
            },
        )
        .unwrap();
        let runtime = RollupRuntimeState::new_with_disk_budget(
            Some(temp_dir.path().to_path_buf()),
            Some(Arc::clone(&budget)),
        );
        let store = RollupStateStoreContext { state: &runtime };
        let mut writer = store.begin_source_state_journal_writer().unwrap();

        let error = store
            .persist_source_state_event(&mut writer, event.clone())
            .expect_err("event reservation must fail while the filler consumes headroom");
        assert!(matches!(error, TsinkError::DiskQuotaExceeded { .. }));
        assert!(!writer.event_publication_may_be_ambiguous());
        assert!(!runtime.snapshot_publication_fenced.load(Ordering::Acquire));
        assert!(runtime.checkpoints.read().is_empty());

        fs::remove_file(&filler).unwrap();
        budget.reconcile_when_idle().unwrap();
        store
            .persist_source_state_event(&mut writer, event)
            .unwrap();
        assert_eq!(runtime.checkpoints.read()["policy-a"]["a"], 1);
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 0), "a"), Some(1));
    }

    #[test]
    fn post_rename_event_sync_failure_fences_stale_runtime_state() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(ROLLUP_DIR_NAME);
        fs::create_dir_all(&rollup_dir).unwrap();
        let runtime =
            RollupRuntimeState::new_with_disk_budget(Some(temp_dir.path().to_path_buf()), None);
        let store = RollupStateStoreContext { state: &runtime };
        let mut writer = store.begin_source_state_journal_writer().unwrap();
        store
            .persist_source_state_event(&mut writer, completed_event(0, "a", 1))
            .unwrap();
        let batch = discover_journal(&journal_dir_path(&rollup_dir))
            .unwrap()
            .sealed[0]
            .1
            .clone();
        let _sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
            batch,
            "injected rollup event parent sync failure",
        );

        let error = store
            .persist_source_state_event(&mut writer, completed_event(0, "a", 2))
            .expect_err("a post-rename event sync failure must fence stale memory");
        assert!(error
            .to_string()
            .contains("injected rollup event parent sync failure"));
        assert!(writer.event_publication_may_be_ambiguous());
        assert!(runtime.snapshot_publication_fenced.load(Ordering::Acquire));
        assert_eq!(runtime.checkpoints.read()["policy-a"]["a"], 1);
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 0), "a"), Some(2));

        let retry_error = store
            .persist_source_state_event(&mut writer, completed_event(0, "a", 2))
            .expect_err("fenced stale memory must not be used for an in-process retry");
        assert!(retry_error.to_string().contains("fenced"));
    }

    #[test]
    fn shrinking_replacement_uses_pre_event_memory_peak_at_exact_boundary() {
        let event = completed_event(0, "source-with-pending-state", 10);
        let pending = PendingRollupMaterialization {
            checkpoint: 0,
            materialized_through: 10,
            generation: 0,
        };
        let checkpoints = HashMap::from([(
            "policy-a".to_string(),
            BTreeMap::from([("source-with-pending-state".to_string(), 0)]),
        )]);
        let pending_materializations = HashMap::from([(
            "policy-a".to_string(),
            BTreeMap::from([("source-with-pending-state".to_string(), pending)]),
        )]);
        let generations = HashMap::from([("policy-a".to_string(), 0)]);
        let current_usage = super::super::runtime::rollup_state_envelope_usage(
            &[],
            &checkpoints,
            &generations,
            &pending_materializations,
            &[],
        )
        .unwrap();
        let next_usage = preflight_event_envelope_usage(
            current_usage,
            &checkpoints,
            &pending_materializations,
            &generations,
            &event,
            0,
            "shrinking replacement test",
        )
        .unwrap();
        assert!(next_usage.modeled_bytes < current_usage.modeled_bytes);

        let encoding_scratch = event
            .modeled_bytes()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<RollupSourceStateEvent>()))
            .and_then(|bytes| bytes.checked_add(ROLLUP_STATE_JOURNAL_MEMORY_FIXED_BYTES))
            .unwrap();
        let encoded = encode_events(
            std::slice::from_ref(&event),
            RollupStateJournalLimits::default(),
        )
        .unwrap()
        .unwrap();
        let write_scratch = encoded
            .len()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(ROLLUP_STATE_JOURNAL_MEMORY_FIXED_BYTES))
            .unwrap();
        assert!(encoding_scratch >= write_scratch);
        let exact_limit = current_usage
            .modeled_bytes
            .checked_add(encoding_scratch)
            .unwrap();

        let run = |memory_limit_bytes: usize| {
            let temp_dir = TempDir::new().unwrap();
            let rollup_dir = temp_dir.path().join(ROLLUP_DIR_NAME);
            fs::create_dir_all(&rollup_dir).unwrap();
            let runtime = RollupRuntimeState::new_with_disk_budget_and_memory_limit(
                Some(temp_dir.path().to_path_buf()),
                None,
                memory_limit_bytes,
            );
            *runtime.checkpoints.write() = checkpoints.clone();
            *runtime.pending_materializations.write() = pending_materializations.clone();
            *runtime.generations.write() = generations.clone();
            *runtime.state_envelope_usage.lock() = current_usage;
            let store = RollupStateStoreContext { state: &runtime };
            let mut writer = store.begin_source_state_journal_writer().unwrap();
            let result = store.persist_source_state_event(&mut writer, event.clone());
            (temp_dir, runtime, writer, result)
        };

        let (_temp_dir, rejected_runtime, rejected_writer, error) = run(exact_limit - 1);
        let error = error.expect_err("N-1 must reject while the larger pre-event maps are live");
        assert!(matches!(
            error,
            TsinkError::MemoryBudgetExceeded { budget, required }
                if budget == exact_limit - 1 && required == exact_limit
        ));
        assert!(rejected_runtime.pending_materializations.read()["policy-a"]
            .contains_key("source-with-pending-state"));
        assert_eq!(
            rejected_writer.retained_memory_bytes,
            current_usage.modeled_bytes
        );

        let (_temp_dir, accepted_runtime, accepted_writer, result) = run(exact_limit);
        result.expect("exact pre-event peak must be admitted");
        assert!(accepted_runtime.pending_materializations.read().is_empty());
        assert_eq!(*accepted_runtime.state_envelope_usage.lock(), next_usage);
        assert_eq!(
            accepted_writer.retained_memory_bytes,
            next_usage.modeled_bytes
        );
    }

    #[test]
    fn replay_rejects_item_n_plus_one_before_map_growth_and_accepts_exact_n() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer
            .persist(completed_event(42, "journal-new", 1))
            .unwrap();
        drop(writer);

        let checkpoints = (0..(ROLLUP_STATE_SNAPSHOT_MAX_ITEMS - 1))
            .map(|index| (format!("source-{index:05}"), index as i64))
            .collect::<BTreeMap<_, _>>();
        let mut state = LoadedRollupState {
            journal_epoch: 42,
            checkpoints: HashMap::from([("policy-a".to_string(), checkpoints)]),
            generations: HashMap::from([("policy-a".to_string(), 0)]),
            ..LoadedRollupState::default()
        };
        let usage = super::super::runtime::rollup_state_envelope_usage(
            &[],
            &state.checkpoints,
            &state.generations,
            &state.pending_materializations,
            &state.pending_delete_invalidations,
        )
        .unwrap();
        assert_eq!(usage.items, ROLLUP_STATE_SNAPSHOT_MAX_ITEMS);
        let error =
            load_rollup_state_journal(Some(&rollup_dir), 42, &mut state, usage, None, usize::MAX)
                .expect_err("journal replay N+1 must be bounded");
        assert!(matches!(
            error,
            TsinkError::MaintenanceDependencyWindowExceeded {
                operation: "rollup source state journal replay",
                ..
            }
        ));
        assert!(!state
            .checkpoints
            .get("policy-a")
            .unwrap()
            .contains_key("journal-new"));

        state
            .checkpoints
            .get_mut("policy-a")
            .unwrap()
            .remove("source-00000");
        let usage = super::super::runtime::rollup_state_envelope_usage(
            &[],
            &state.checkpoints,
            &state.generations,
            &state.pending_materializations,
            &state.pending_delete_invalidations,
        )
        .unwrap();
        load_rollup_state_journal(Some(&rollup_dir), 42, &mut state, usage, None, usize::MAX)
            .unwrap();
        assert_eq!(checkpoint(&state, "journal-new"), Some(1));
        let exact = super::super::runtime::rollup_state_envelope_usage(
            &[],
            &state.checkpoints,
            &state.generations,
            &state.pending_materializations,
            &state.pending_delete_invalidations,
        )
        .unwrap();
        assert_eq!(exact.items, ROLLUP_STATE_SNAPSHOT_MAX_ITEMS);
    }

    #[test]
    fn envelope_byte_boundary_and_net_replacement_are_exact() {
        let at_limit = RollupStateEnvelopeUsage {
            items: ROLLUP_STATE_SNAPSHOT_MAX_ITEMS,
            modeled_bytes: ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES,
        };
        assert_eq!(
            at_limit
                .transition(1, 256, 1, 256, "test rollup envelope")
                .unwrap(),
            at_limit
        );
        let error = at_limit
            .transition(0, 0, 0, 1, "test rollup envelope")
            .expect_err("modeled-byte N+1 must be rejected");
        assert!(matches!(
            error,
            TsinkError::MaintenanceDependencyWindowExceeded {
                operation: "test rollup envelope",
                selected_bytes,
                ..
            } if selected_bytes == (ROLLUP_STATE_SNAPSHOT_MAX_MODELED_BYTES as u64) + 1
        ));
    }

    #[test]
    fn concurrent_distinct_source_publications_serialize_retained_usage() {
        let temp_dir = TempDir::new().unwrap();
        let runtime = Arc::new(RollupRuntimeState::new_with_disk_budget(None, None));
        *runtime.generations.write() = HashMap::from([("policy-a".to_string(), 0)]);
        *runtime.state_envelope_usage.lock() = super::super::runtime::rollup_state_envelope_usage(
            &[],
            &runtime.checkpoints.read(),
            &runtime.generations.read(),
            &runtime.pending_materializations.read(),
            &runtime.pending_delete_invalidations.read(),
        )
        .unwrap();

        let journal_a = temp_dir.path().join("journal-a");
        let journal_b = temp_dir.path().join("journal-b");
        fs::create_dir(&journal_a).unwrap();
        fs::create_dir(&journal_b).unwrap();
        let writer_a = begin_rollup_state_journal_writer(Some(&journal_a), None).unwrap();
        let writer_b = begin_rollup_state_journal_writer(Some(&journal_b), None).unwrap();
        let hook_entered = Arc::new(std::sync::Barrier::new(2));
        let hook_release = Arc::new(std::sync::Barrier::new(2));
        let hook_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        runtime.set_state_persist_hook({
            let hook_entered = Arc::clone(&hook_entered);
            let hook_release = Arc::clone(&hook_release);
            let hook_calls = Arc::clone(&hook_calls);
            move || {
                if hook_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    hook_entered.wait();
                    hook_release.wait();
                }
                Ok(())
            }
        });

        let first_runtime = Arc::clone(&runtime);
        let first = std::thread::spawn(move || {
            let store = RollupStateStoreContext {
                state: &first_runtime,
            };
            let mut writer = writer_a;
            store.persist_source_state_event(&mut writer, completed_event(0, "a", 1))
        });
        hook_entered.wait();
        let (second_started_tx, second_started_rx) = std::sync::mpsc::channel();
        let second_runtime = Arc::clone(&runtime);
        let second = std::thread::spawn(move || {
            let store = RollupStateStoreContext {
                state: &second_runtime,
            };
            let mut writer = writer_b;
            second_started_tx.send(()).unwrap();
            store.persist_source_state_event(&mut writer, completed_event(0, "b", 2))
        });
        second_started_rx.recv().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(25));
        assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
        hook_release.wait();
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        assert_eq!(hook_calls.load(Ordering::SeqCst), 2);
        let checkpoints = runtime.checkpoints.read();
        assert_eq!(checkpoints["policy-a"]["a"], 1);
        assert_eq!(checkpoints["policy-a"]["b"], 2);
        let exact = super::super::runtime::rollup_state_envelope_usage(
            &[],
            &checkpoints,
            &runtime.generations.read(),
            &runtime.pending_materializations.read(),
            &runtime.pending_delete_invalidations.read(),
        )
        .unwrap();
        assert_eq!(*runtime.state_envelope_usage.lock(), exact);
    }

    #[test]
    fn source_publication_and_full_install_serialize_without_deadlock() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(ROLLUP_DIR_NAME);
        fs::create_dir_all(&rollup_dir).unwrap();
        let runtime = Arc::new(RollupRuntimeState::new_with_disk_budget(
            Some(temp_dir.path().to_path_buf()),
            None,
        ));
        *runtime.generations.write() = HashMap::from([("policy-a".to_string(), 0)]);
        *runtime.state_envelope_usage.lock() = super::super::runtime::rollup_state_envelope_usage(
            &[],
            &runtime.checkpoints.read(),
            &runtime.generations.read(),
            &runtime.pending_materializations.read(),
            &runtime.pending_delete_invalidations.read(),
        )
        .unwrap();
        let writer = (RollupStateStoreContext { state: &runtime })
            .begin_source_state_journal_writer()
            .unwrap();
        let hook_entered = Arc::new(std::sync::Barrier::new(2));
        let hook_release = Arc::new(std::sync::Barrier::new(2));
        runtime.set_state_persist_hook({
            let hook_entered = Arc::clone(&hook_entered);
            let hook_release = Arc::clone(&hook_release);
            move || {
                hook_entered.wait();
                hook_release.wait();
                Ok(())
            }
        });

        let source_runtime = Arc::clone(&runtime);
        let source = std::thread::spawn(move || {
            let store = RollupStateStoreContext {
                state: &source_runtime,
            };
            let mut writer = writer;
            store.persist_source_state_event(&mut writer, completed_event(0, "source", 1))
        });
        hook_entered.wait();
        let snapshot = RollupRuntimeSnapshot {
            policies: Vec::new(),
            checkpoints: HashMap::from([(
                "policy-a".to_string(),
                BTreeMap::from([("installed".to_string(), 9)]),
            )]),
            pending_materializations: HashMap::new(),
            pending_delete_invalidations: Vec::new(),
            generations: HashMap::from([("policy-a".to_string(), 0)]),
            policy_stats: BTreeMap::new(),
        };
        let install_runtime = Arc::clone(&runtime);
        let (installed_tx, installed_rx) = std::sync::mpsc::channel();
        let install = std::thread::spawn(move || {
            (RollupStateStoreContext {
                state: &install_runtime,
            })
            .install_snapshot(snapshot);
            installed_tx.send(()).unwrap();
        });
        assert!(matches!(
            installed_rx.recv_timeout(std::time::Duration::from_millis(25)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        hook_release.wait();
        source.join().unwrap().unwrap();
        install.join().unwrap();
        installed_rx.recv().unwrap();
        let checkpoints = runtime.checkpoints.read();
        assert_eq!(checkpoints["policy-a"]["installed"], 9);
        assert!(!checkpoints["policy-a"].contains_key("source"));
        let exact = super::super::runtime::rollup_state_envelope_usage(
            &runtime.policies.read(),
            &checkpoints,
            &runtime.generations.read(),
            &runtime.pending_materializations.read(),
            &runtime.pending_delete_invalidations.read(),
        )
        .unwrap();
        assert_eq!(*runtime.state_envelope_usage.lock(), exact);
    }

    #[test]
    fn full_policy_install_recomputes_exact_combined_envelope() {
        let runtime = RollupRuntimeState::new_with_disk_budget(None, None);
        let checkpoints = (0..(ROLLUP_STATE_SNAPSHOT_MAX_ITEMS - 2))
            .map(|index| (format!("source-{index:05}"), index as i64))
            .collect::<BTreeMap<_, _>>();
        *runtime.checkpoints.write() = HashMap::from([("policy-a".to_string(), checkpoints)]);
        *runtime.generations.write() = HashMap::from([("policy-a".to_string(), 0)]);
        *runtime.state_envelope_usage.lock() = super::super::runtime::rollup_state_envelope_usage(
            &[],
            &runtime.checkpoints.read(),
            &runtime.generations.read(),
            &runtime.pending_materializations.read(),
            &runtime.pending_delete_invalidations.read(),
        )
        .unwrap();
        let store = RollupStateStoreContext { state: &runtime };
        let policy = RollupPolicy {
            id: "policy-a".to_string(),
            metric: "cpu".to_string(),
            match_labels: Vec::new(),
            interval: 1_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        };
        let snapshot = store
            .next_snapshot_for_policies(vec![policy.clone()])
            .unwrap();
        let exact = super::super::runtime::rollup_state_envelope_usage(
            &snapshot.policies,
            &snapshot.checkpoints,
            &snapshot.generations,
            &snapshot.pending_materializations,
            &snapshot.pending_delete_invalidations,
        )
        .unwrap();
        assert_eq!(exact.items, ROLLUP_STATE_SNAPSHOT_MAX_ITEMS);
        store.install_snapshot(snapshot);
        assert_eq!(*runtime.state_envelope_usage.lock(), exact);

        let mut over = policy;
        over.match_labels.push(Label::new("host", "a"));
        let error = super::super::runtime::rollup_state_envelope_usage(
            &[over],
            &runtime.checkpoints.read(),
            &runtime.generations.read(),
            &runtime.pending_materializations.read(),
            &runtime.pending_delete_invalidations.read(),
        )
        .expect_err("one policy label beyond exact N must be rejected");
        assert!(matches!(
            error,
            TsinkError::MaintenanceDependencyWindowExceeded {
                selected_items,
                ..
            } if selected_items == ROLLUP_STATE_SNAPSHOT_MAX_ITEMS + 1
        ));
    }

    #[test]
    fn governed_event_parent_sync_failure_reconciles_once_and_restart_is_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let mut writer =
            begin_rollup_state_journal_writer(Some(&rollup_dir), Some(&budget)).unwrap();
        writer.persist(completed_event(19, "a", 1)).unwrap();
        let batch = discover_journal(&rollup_dir).unwrap().sealed[0].1.clone();
        let before = budget.snapshot();
        let _failure = crate::engine::fs_utils::fail_directory_sync_once(
            batch,
            "injected batch event parent sync failure",
        );
        let retry_event = completed_event(19, "a", 2);
        let error = writer
            .persist(retry_event.clone())
            .expect_err("post-rename event parent sync failure must be surfaced");
        assert!(error
            .to_string()
            .contains("injected batch event parent sync failure"));
        assert_eq!(
            budget.snapshot().reconciliations_total,
            before.reconciliations_total + 1
        );
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 19), "a"), Some(2));

        drop(writer);
        let mut restarted =
            begin_rollup_state_journal_writer(Some(&rollup_dir), Some(&budget)).unwrap();
        restarted.persist(retry_event).unwrap();
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 19), "a"), Some(2));
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, journal_file_bytes(&rollup_dir));
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
    }

    #[test]
    fn interrupted_batch_directory_publication_is_empty_and_restart_safe() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let before = budget.snapshot();
        let _failure = crate::engine::fs_utils::fail_directory_sync_once(
            rollup_dir.clone(),
            "injected batch directory parent sync failure",
        );
        let mut writer =
            begin_rollup_state_journal_writer(Some(&rollup_dir), Some(&budget)).unwrap();
        let error = writer
            .persist(completed_event(25, "a", 1))
            .expect_err("batch directory parent sync failure must be surfaced");
        assert!(error
            .to_string()
            .contains("injected batch directory parent sync failure"));
        let discovery = discover_journal(&rollup_dir).unwrap();
        assert_eq!(discovery.generation_count(), 1);
        assert_eq!(fs::read_dir(&discovery.sealed[0].1).unwrap().count(), 0);
        let failed = budget.snapshot();
        assert_eq!(failed.accounted_bytes, 0);
        assert_eq!(failed.active_reservations, 0);
        assert_eq!(failed.reconciliations_total, before.reconciliations_total);

        drop(writer);
        let mut restarted =
            begin_rollup_state_journal_writer(Some(&rollup_dir), Some(&budget)).unwrap();
        restarted.persist(completed_event(25, "a", 1)).unwrap();
        assert_eq!(checkpoint(&load_test_state(&rollup_dir, 25), "a"), Some(1));
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, journal_file_bytes(&rollup_dir));
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
    }

    #[test]
    fn governed_batch_snapshot_cleanup_reconciles_once() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let mut writer =
            begin_rollup_state_journal_writer(Some(&rollup_dir), Some(&budget)).unwrap();
        for checkpoint in 1..=4 {
            writer
                .persist(completed_event(20, &format!("s{checkpoint}"), checkpoint))
                .unwrap();
        }
        drop(writer);
        let before = budget.snapshot();
        cleanup_rollup_state_journal(Some(&rollup_dir), Some(&budget)).unwrap();
        let after = budget.snapshot();
        assert_eq!(
            after.reconciliations_total,
            before.reconciliations_total + 1
        );
        assert_eq!(after.accounted_bytes, 0);
        assert_eq!(after.active_reservations, 0);
        assert!(discover_journal(&rollup_dir).unwrap().sealed.is_empty());
    }

    #[test]
    fn uppercase_managed_name_lookalike_is_preserved_by_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let lookalike = rollup_dir.join("state-journal-batch-000000000000000A.d");
        fs::create_dir(&lookalike).unwrap();
        let sentinel = lookalike.join("operator.keep");
        fs::write(&sentinel, b"preserved").unwrap();
        cleanup_rollup_state_journal(Some(&rollup_dir), None).unwrap();
        assert_eq!(fs::read(sentinel).unwrap(), b"preserved");
    }

    #[test]
    fn exact_event_type_swap_fails_closed_without_recursive_deletion() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let mut writer = begin_rollup_state_journal_writer(Some(&rollup_dir), None).unwrap();
        writer.persist(completed_event(21, "a", 1)).unwrap();
        let batch = discover_journal(&rollup_dir).unwrap().sealed[0].1.clone();
        let event = batch_event_path(&batch, 0);
        fs::remove_file(&event).unwrap();
        fs::create_dir(&event).unwrap();
        let sentinel = event.join("sentinel.keep");
        fs::write(&sentinel, b"preserve").unwrap();
        let error = cleanup_rollup_state_journal(Some(&rollup_dir), None)
            .expect_err("an exact event path changed to a directory must fail closed");
        assert!(matches!(error, TsinkError::DataCorruption(_)));
        assert_eq!(fs::read(sentinel).unwrap(), b"preserve");
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
            max_namespace_entries: MAX_RECOVERY_NAMESPACE_ENTRIES,
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
            max_namespace_entries: MAX_RECOVERY_NAMESPACE_ENTRIES,
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
            max_namespace_entries: MAX_RECOVERY_NAMESPACE_ENTRIES,
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
    fn governed_full_snapshot_cleanup_batches_generations_into_one_reconciliation() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let sealed_one = rollup_dir.join(format!(
            "{ROLLUP_STATE_JOURNAL_SEGMENT_PREFIX}{:016x}{ROLLUP_STATE_JOURNAL_SEGMENT_SUFFIX}",
            1
        ));
        let sealed_two = rollup_dir.join(format!(
            "{ROLLUP_STATE_JOURNAL_SEGMENT_PREFIX}{:016x}{ROLLUP_STATE_JOURNAL_SEGMENT_SUFFIX}",
            2
        ));
        let active = active_journal_path(&rollup_dir);
        let unknown = rollup_dir.join("operator-owned.keep");
        fs::write(&sealed_one, b"sealed-one").unwrap();
        fs::write(&sealed_two, b"sealed-generation-two").unwrap();
        fs::write(&active, b"active-generation").unwrap();
        fs::write(&unknown, b"preserved").unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let before = budget.snapshot();
        assert_eq!(before.reconciliations_total, 1);

        cleanup_rollup_state_journal(Some(&rollup_dir), Some(&budget)).unwrap();

        assert!(!sealed_one.exists());
        assert!(!sealed_two.exists());
        assert!(!active.exists());
        assert!(unknown.is_file());
        let snapshot = budget.snapshot();
        assert_eq!(
            snapshot.reconciliations_total,
            before.reconciliations_total + 1
        );
        assert_eq!(
            snapshot.accounted_bytes,
            u64::try_from(b"preserved".len()).unwrap()
        );
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.maintenance_reserved_bytes, 0);
    }

    #[test]
    fn governed_full_snapshot_cleanup_reconciles_after_post_unlink_sync_failure() {
        let temp_dir = TempDir::new().unwrap();
        let rollup_dir = temp_dir.path().join(".rollups");
        fs::create_dir_all(&rollup_dir).unwrap();
        let sealed_one = rollup_dir.join(format!(
            "{ROLLUP_STATE_JOURNAL_SEGMENT_PREFIX}{:016x}{ROLLUP_STATE_JOURNAL_SEGMENT_SUFFIX}",
            1
        ));
        let sealed_two = rollup_dir.join(format!(
            "{ROLLUP_STATE_JOURNAL_SEGMENT_PREFIX}{:016x}{ROLLUP_STATE_JOURNAL_SEGMENT_SUFFIX}",
            2
        ));
        let active = active_journal_path(&rollup_dir);
        fs::write(&sealed_one, b"removed-before-sync-failure").unwrap();
        fs::write(&sealed_two, b"sealed-two-retained").unwrap();
        fs::write(&active, b"active-retained").unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let before = budget.snapshot();
        let _sync_failure = crate::engine::fs_utils::fail_directory_sync_once(
            rollup_dir.clone(),
            "injected rollup journal cleanup sync failure",
        );

        let err = cleanup_rollup_state_journal(Some(&rollup_dir), Some(&budget))
            .expect_err("the committed unlink must retain its synchronization error");

        assert!(matches!(
            err,
            TsinkError::Other(ref message)
                if message == "injected rollup journal cleanup sync failure"
        ));
        assert!(!sealed_one.exists());
        assert!(sealed_two.is_file());
        assert!(active.is_file());
        let snapshot = budget.snapshot();
        assert_eq!(
            snapshot.accounted_bytes,
            u64::try_from(b"sealed-two-retained".len() + b"active-retained".len()).unwrap()
        );
        assert_eq!(
            snapshot.reconciliations_total,
            before.reconciliations_total + 1
        );
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.maintenance_reserved_bytes, 0);
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
