use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::engine::fs_utils::{
    remove_path_if_exists_and_sync_parent_budgeted, write_file_atomically_and_sync_parent_budgeted,
};
use crate::engine::series::SeriesId;
use crate::{Result, TsinkError};

pub(crate) const TOMBSTONES_FILE_NAME: &str = "tombstones.json";
const TOMBSTONES_FILE_VERSION: u16 = 1;
const TOMBSTONE_STORE_VERSION: u16 = 2;
const TOMBSTONE_STORE_SHARD_COUNT: usize = 256;
const TOMBSTONE_STORE_MAGIC: &[u8; 8] = b"TSINKTM2";
const TOMBSTONE_SHARD_MAGIC: &[u8; 8] = b"TSINKTS2";
const TOMBSTONE_STORE_DIR_SUFFIX: &str = ".store";
const TOMBSTONE_SHARDS_DIR_NAME: &str = "shards";

static TOMBSTONE_SHARD_FILE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
type TombstoneManifestRollbackHook = dyn Fn(&Path) -> Result<()> + Send + Sync + 'static;

#[cfg(test)]
pub(crate) struct TombstoneManifestRollbackHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for TombstoneManifestRollbackHookGuard {
    fn drop(&mut self) {
        *tombstone_manifest_rollback_hook_slot()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = None;
    }
}

#[cfg(test)]
fn tombstone_manifest_rollback_hook_slot(
) -> &'static std::sync::Mutex<Option<Arc<TombstoneManifestRollbackHook>>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<Arc<TombstoneManifestRollbackHook>>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn tombstone_manifest_rollback_test_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(test)]
fn invoke_tombstone_manifest_rollback_hook(path: &Path) -> Result<()> {
    let hook = tombstone_manifest_rollback_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    match hook {
        Some(hook) => hook(path),
        None => Ok(()),
    }
}

#[cfg(test)]
pub(crate) fn fail_tombstone_manifest_rollback_once(
    path: PathBuf,
    message: impl Into<String>,
) -> TombstoneManifestRollbackHookGuard {
    use std::sync::atomic::AtomicBool;

    let lock = tombstone_manifest_rollback_test_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let failed = Arc::new(AtomicBool::new(false));
    let message = message.into();
    *tombstone_manifest_rollback_hook_slot()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = Some(Arc::new(move |candidate| {
        if candidate == path && !failed.swap(true, Ordering::SeqCst) {
            return Err(TsinkError::Other(message.clone()));
        }
        Ok(())
    }));
    TombstoneManifestRollbackHookGuard { _lock: lock }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TombstoneRange {
    pub(crate) start: i64,
    pub(crate) end: i64,
}

pub(crate) type TombstoneMap = HashMap<SeriesId, Vec<TombstoneRange>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TombstoneFileV1 {
    version: u16,
    entries: Vec<TombstoneSeriesEntryV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TombstoneSeriesEntryV1 {
    series_id: SeriesId,
    ranges: Vec<TombstoneRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TombstoneStoreManifestV2 {
    version: u16,
    shard_count: u16,
    shards: Vec<Option<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TombstoneShardFileV2 {
    version: u16,
    entries: Vec<TombstoneSeriesEntryV1>,
}

struct PreparedTombstoneShard {
    file_name: String,
    path: PathBuf,
    payload: Vec<u8>,
}

struct PreparedTombstoneStoreUpdate {
    path: PathBuf,
    previous_bytes: Option<Vec<u8>>,
    previous_manifest: Option<TombstoneStoreManifestV2>,
    next_manifest: TombstoneStoreManifestV2,
    next_manifest_payload: Vec<u8>,
    new_shards: Vec<PreparedTombstoneShard>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TombstonePersistenceCertainty {
    DefinitivelyClean,
    Indeterminate,
}

#[derive(Debug)]
pub(crate) struct TombstonePersistenceError {
    error: TsinkError,
    certainty: TombstonePersistenceCertainty,
}

pub(crate) type TombstonePersistenceResult<T> = std::result::Result<T, TombstonePersistenceError>;

impl TombstonePersistenceError {
    pub(crate) fn definitively_clean(error: TsinkError) -> Self {
        Self {
            error,
            certainty: TombstonePersistenceCertainty::DefinitivelyClean,
        }
    }

    pub(crate) fn indeterminate(error: TsinkError) -> Self {
        Self {
            error,
            certainty: TombstonePersistenceCertainty::Indeterminate,
        }
    }

    pub(crate) fn is_definitively_clean(&self) -> bool {
        self.certainty == TombstonePersistenceCertainty::DefinitivelyClean
    }

    pub(crate) fn into_tsink_error(self) -> TsinkError {
        self.error
    }
}

impl From<TsinkError> for TombstonePersistenceError {
    fn from(error: TsinkError) -> Self {
        Self::definitively_clean(error)
    }
}

pub(crate) fn merge_tombstone_range(ranges: &mut Vec<TombstoneRange>, new_range: TombstoneRange) {
    ranges.push(new_range);
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<TombstoneRange> = Vec::with_capacity(ranges.len());
    for range in ranges.drain(..) {
        if let Some(last) = merged.last_mut() {
            if last.end >= range.start {
                if range.end > last.end {
                    last.end = range.end;
                }
                continue;
            }
        }
        merged.push(range);
    }
    *ranges = merged;
}

pub(crate) fn timestamp_is_tombstoned(timestamp: i64, ranges: &[TombstoneRange]) -> bool {
    let idx = ranges.partition_point(|range| range.start <= timestamp);
    if idx == 0 {
        return false;
    }
    timestamp < ranges[idx - 1].end
}

pub(crate) fn interval_fully_tombstoned(
    start_inclusive: i64,
    end_inclusive: i64,
    ranges: &[TombstoneRange],
) -> bool {
    if start_inclusive > end_inclusive {
        return false;
    }

    let idx = ranges.partition_point(|range| range.start <= start_inclusive);
    if idx == 0 {
        return false;
    }

    ranges[idx - 1].end > end_inclusive
}

fn normalize_tombstone_map(map: &mut TombstoneMap) -> Result<()> {
    for ranges in map.values_mut() {
        *ranges = normalize_tombstone_ranges(ranges.iter().copied())?;
    }
    map.retain(|_, ranges| !ranges.is_empty());
    Ok(())
}

fn normalize_tombstone_ranges(
    ranges: impl IntoIterator<Item = TombstoneRange>,
) -> Result<Vec<TombstoneRange>> {
    let mut normalized = Vec::new();
    for range in ranges {
        if range.start >= range.end {
            return Err(TsinkError::DataCorruption(
                "invalid tombstone range: start must be strictly less than end".to_string(),
            ));
        }
        merge_tombstone_range(&mut normalized, range);
    }
    Ok(normalized)
}

fn tombstone_store_dir(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| TOMBSTONES_FILE_NAME.to_string());
    path.with_file_name(format!("{file_name}{TOMBSTONE_STORE_DIR_SUFFIX}"))
}

fn tombstone_shards_dir(path: &Path) -> PathBuf {
    tombstone_store_dir(path).join(TOMBSTONE_SHARDS_DIR_NAME)
}

fn tombstone_shard_index(series_id: SeriesId) -> usize {
    (series_id as usize) % TOMBSTONE_STORE_SHARD_COUNT
}

fn validate_store_manifest(manifest: &TombstoneStoreManifestV2) -> Result<()> {
    if manifest.version != TOMBSTONE_STORE_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported tombstone store version {}",
            manifest.version
        )));
    }
    if usize::from(manifest.shard_count) != TOMBSTONE_STORE_SHARD_COUNT {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported tombstone shard count {}",
            manifest.shard_count
        )));
    }
    if manifest.shards.len() != TOMBSTONE_STORE_SHARD_COUNT {
        return Err(TsinkError::DataCorruption(format!(
            "invalid tombstone manifest shard count {}",
            manifest.shards.len()
        )));
    }
    for (shard_index, file_name) in manifest.shards.iter().enumerate() {
        if let Some(file_name) = file_name {
            validate_tombstone_shard_file_name(shard_index, file_name)?;
        }
    }
    Ok(())
}

fn validate_tombstone_shard_file_name(shard_index: usize, file_name: &str) -> Result<()> {
    let expected_prefix = format!("shard-{shard_index:03}-");
    let valid = file_name
        .strip_prefix(&expected_prefix)
        .and_then(|value| value.strip_suffix(".bin"))
        .is_some_and(|nonce| {
            nonce.len() == 16
                && nonce
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        });
    if valid {
        Ok(())
    } else {
        Err(TsinkError::DataCorruption(format!(
            "invalid tombstone shard file name for manifest slot {shard_index}: {file_name:?}"
        )))
    }
}

fn encode_store_manifest(manifest: &TombstoneStoreManifestV2) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(TOMBSTONE_STORE_MAGIC.len() + 64);
    payload.extend_from_slice(TOMBSTONE_STORE_MAGIC);
    payload.extend_from_slice(&bincode::serialize(manifest)?);
    Ok(payload)
}

fn decode_store_manifest(bytes: &[u8]) -> Result<Option<TombstoneStoreManifestV2>> {
    if !bytes.starts_with(TOMBSTONE_STORE_MAGIC) {
        return Ok(None);
    }
    let manifest: TombstoneStoreManifestV2 =
        bincode::deserialize(&bytes[TOMBSTONE_STORE_MAGIC.len()..])?;
    validate_store_manifest(&manifest)?;
    Ok(Some(manifest))
}

fn encode_shard(entries: Vec<TombstoneSeriesEntryV1>) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(TOMBSTONE_SHARD_MAGIC.len() + 128);
    payload.extend_from_slice(TOMBSTONE_SHARD_MAGIC);
    payload.extend_from_slice(&bincode::serialize(&TombstoneShardFileV2 {
        version: TOMBSTONE_STORE_VERSION,
        entries,
    })?);
    Ok(payload)
}

fn decode_shard(bytes: &[u8]) -> Result<TombstoneShardFileV2> {
    if !bytes.starts_with(TOMBSTONE_SHARD_MAGIC) {
        return Err(TsinkError::DataCorruption(
            "invalid tombstone shard header".to_string(),
        ));
    }
    let shard: TombstoneShardFileV2 = bincode::deserialize(&bytes[TOMBSTONE_SHARD_MAGIC.len()..])?;
    if shard.version != TOMBSTONE_STORE_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported tombstone shard version {}",
            shard.version
        )));
    }
    Ok(shard)
}

fn empty_store_manifest() -> TombstoneStoreManifestV2 {
    TombstoneStoreManifestV2 {
        version: TOMBSTONE_STORE_VERSION,
        shard_count: TOMBSTONE_STORE_SHARD_COUNT as u16,
        shards: vec![None; TOMBSTONE_STORE_SHARD_COUNT],
    }
}

fn shard_entries_from_map(mut map: TombstoneMap) -> Vec<TombstoneSeriesEntryV1> {
    let mut entries = map
        .drain()
        .map(|(series_id, ranges)| TombstoneSeriesEntryV1 { series_id, ranges })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.series_id);
    entries
}

fn read_shard_map(path: &Path) -> Result<TombstoneMap> {
    let metadata = std::fs::symlink_metadata(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.file_type().is_file() {
        return Err(TsinkError::DataCorruption(format!(
            "tombstone shard must be a regular file and may not be a symlink: {}",
            path.display()
        )));
    }
    let bytes = std::fs::read(path).map_err(|source| TsinkError::IoWithPath {
        path: path.to_path_buf(),
        source,
    })?;
    let shard = decode_shard(&bytes)?;
    let mut tombstones = TombstoneMap::new();
    for entry in shard.entries {
        tombstones.insert(entry.series_id, entry.ranges);
    }
    normalize_tombstone_map(&mut tombstones)?;
    Ok(tombstones)
}

fn load_sharded_tombstones(
    path: &Path,
    manifest: TombstoneStoreManifestV2,
) -> Result<TombstoneMap> {
    validate_store_manifest(&manifest)?;
    let shards_dir = tombstone_shards_dir(path);
    let mut tombstones = TombstoneMap::new();
    for file_name in manifest.shards.into_iter().flatten() {
        let shard_path = shards_dir.join(file_name);
        let loaded = read_shard_map(&shard_path)?;
        for (series_id, ranges) in loaded {
            tombstones.insert(series_id, ranges);
        }
    }
    normalize_tombstone_map(&mut tombstones)?;
    Ok(tombstones)
}

fn allocate_tombstone_shard_file_name(path: &Path, shard_index: usize) -> Result<String> {
    for _ in 0..256 {
        let file_name = format!(
            "shard-{shard_index:03}-{:016x}.bin",
            TOMBSTONE_SHARD_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let shard_path = tombstone_shards_dir(path).join(&file_name);
        match std::fs::symlink_metadata(&shard_path) {
            Ok(_) => continue,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(file_name),
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: shard_path,
                    source,
                })
            }
        }
    }

    Err(TsinkError::Other(format!(
        "failed to allocate a unique tombstone shard file for shard {shard_index} beneath {}",
        tombstone_shards_dir(path).display()
    )))
}

fn prepare_shard_file(
    path: &Path,
    shard_index: usize,
    map: TombstoneMap,
) -> Result<Option<PreparedTombstoneShard>> {
    if map.is_empty() {
        return Ok(None);
    }

    let file_name = allocate_tombstone_shard_file_name(path, shard_index)?;
    let shard_path = tombstone_shards_dir(path).join(&file_name);
    let payload = encode_shard(shard_entries_from_map(map))?;
    Ok(Some(PreparedTombstoneShard {
        file_name,
        path: shard_path,
        payload,
    }))
}

fn write_shard_file(
    path: &Path,
    shard_index: usize,
    map: TombstoneMap,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
) -> Result<Option<String>> {
    let Some(prepared) = prepare_shard_file(path, shard_index, map)? else {
        return Ok(None);
    };
    write_file_atomically_and_sync_parent_budgeted(
        &prepared.path,
        &prepared.payload,
        local_disk_budget,
        crate::DiskCategory::Tombstones,
        reservation_kind,
    )?;
    Ok(Some(prepared.file_name))
}

fn cleanup_replaced_shards(
    path: &Path,
    previous: &[Option<String>],
    next: &[Option<String>],
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let shards_dir = tombstone_shards_dir(path);
    for (shard_index, file_name) in previous.iter().enumerate() {
        if let Some(file_name) = file_name {
            validate_tombstone_shard_file_name(shard_index, file_name)?;
        }
    }
    for (shard_index, file_name) in next.iter().enumerate() {
        if let Some(file_name) = file_name {
            validate_tombstone_shard_file_name(shard_index, file_name)?;
        }
    }
    let keep = next.iter().flatten().cloned().collect::<HashSet<_>>();
    for file_name in previous.iter().flatten() {
        if keep.contains(file_name) {
            continue;
        }
        let shard_path = shards_dir.join(file_name);
        match std::fs::symlink_metadata(&shard_path) {
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(TsinkError::DataCorruption(format!(
                    "obsolete tombstone shard must be a regular file and may not be a symlink: {}",
                    shard_path.display()
                )))
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(TsinkError::IoWithPath {
                    path: shard_path,
                    source,
                })
            }
        }
        remove_path_if_exists_and_sync_parent_budgeted(
            &shard_path,
            local_disk_budget,
            crate::DiskCategory::Tombstones,
        )?;
    }
    Ok(())
}

fn remove_tombstone_store(
    path: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let store_dir = tombstone_store_dir(path);
    remove_path_if_exists_and_sync_parent_budgeted(
        path,
        local_disk_budget,
        crate::DiskCategory::Tombstones,
    )?;
    remove_path_if_exists_and_sync_parent_budgeted(
        &store_dir,
        local_disk_budget,
        crate::DiskCategory::Tombstones,
    )?;
    Ok(())
}

fn write_full_sharded_store(
    path: &Path,
    tombstones: &TombstoneMap,
    previous_manifest: Option<&TombstoneStoreManifestV2>,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
) -> Result<()> {
    let mut shards = vec![TombstoneMap::new(); TOMBSTONE_STORE_SHARD_COUNT];
    for (&series_id, ranges) in tombstones {
        shards[tombstone_shard_index(series_id)].insert(series_id, ranges.clone());
    }

    let mut manifest = empty_store_manifest();
    for (shard_index, shard_map) in shards.into_iter().enumerate() {
        manifest.shards[shard_index] = write_shard_file(
            path,
            shard_index,
            shard_map,
            local_disk_budget,
            reservation_kind,
        )?;
    }

    let payload = encode_store_manifest(&manifest)?;
    write_file_atomically_and_sync_parent_budgeted(
        path,
        &payload,
        local_disk_budget,
        crate::DiskCategory::Tombstones,
        reservation_kind,
    )?;
    if let Some(previous_manifest) = previous_manifest {
        cleanup_replaced_shards(
            path,
            &previous_manifest.shards,
            &manifest.shards,
            local_disk_budget,
        )?;
    }
    Ok(())
}

fn load_store_manifest_from_bytes(bytes: &[u8]) -> Result<Option<TombstoneStoreManifestV2>> {
    match decode_store_manifest(bytes)? {
        Some(manifest) => Ok(Some(manifest)),
        None => Ok(None),
    }
}

fn load_legacy_tombstones_from_bytes(bytes: &[u8]) -> Result<TombstoneMap> {
    let snapshot: TombstoneFileV1 = serde_json::from_slice(bytes)?;
    if snapshot.version != TOMBSTONES_FILE_VERSION {
        return Err(TsinkError::DataCorruption(format!(
            "unsupported tombstone index version {}",
            snapshot.version
        )));
    }

    let mut tombstones = TombstoneMap::new();
    for entry in snapshot.entries {
        for range in entry.ranges {
            merge_tombstone_range(tombstones.entry(entry.series_id).or_default(), range);
        }
    }
    normalize_tombstone_map(&mut tombstones)?;
    Ok(tombstones)
}

pub(crate) fn load_tombstones(path: &Path) -> Result<TombstoneMap> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(TombstoneMap::new()),
        Err(err) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source: err,
            })
        }
    };

    if let Some(manifest) = load_store_manifest_from_bytes(&bytes)? {
        return load_sharded_tombstones(path, manifest);
    }
    load_legacy_tombstones_from_bytes(&bytes)
}

#[cfg(test)]
pub(crate) fn persist_tombstones(path: &Path, tombstones: &TombstoneMap) -> Result<()> {
    persist_tombstones_with_disk_budget_and_kind(
        path,
        tombstones,
        None,
        crate::DiskReservationKind::Maintenance,
    )
}

pub(crate) fn persist_tombstones_with_disk_budget_and_kind(
    path: &Path,
    tombstones: &TombstoneMap,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
    reservation_kind: crate::DiskReservationKind,
) -> Result<()> {
    let mut normalized = tombstones.clone();
    normalize_tombstone_map(&mut normalized)?;
    if normalized.is_empty() {
        return remove_tombstone_store(path, local_disk_budget);
    }
    let previous_manifest = match std::fs::read(path) {
        Ok(bytes) => load_store_manifest_from_bytes(&bytes)?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source: err,
            })
        }
    };
    write_full_sharded_store(
        path,
        &normalized,
        previous_manifest.as_ref(),
        local_disk_budget,
        reservation_kind,
    )
}

fn prepare_tombstone_store_update(
    path: &Path,
    normalized_updates: &TombstoneMap,
) -> Result<PreparedTombstoneStoreUpdate> {
    let previous_bytes = match std::fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    let previous_manifest = previous_bytes
        .as_deref()
        .map(load_store_manifest_from_bytes)
        .transpose()?
        .flatten();

    let (next_manifest, new_shards) = if let Some(previous_manifest) = &previous_manifest {
        let mut next_manifest = previous_manifest.clone();
        let mut updates_by_shard = vec![TombstoneMap::new(); TOMBSTONE_STORE_SHARD_COUNT];
        for (&series_id, ranges) in normalized_updates {
            updates_by_shard[tombstone_shard_index(series_id)].insert(series_id, ranges.clone());
        }

        let mut new_shards = Vec::new();
        for (shard_index, shard_updates) in updates_by_shard.into_iter().enumerate() {
            if shard_updates.is_empty() {
                continue;
            }

            let mut shard_map = match &previous_manifest.shards[shard_index] {
                Some(file_name) => read_shard_map(&tombstone_shards_dir(path).join(file_name))?,
                None => TombstoneMap::new(),
            };
            for (series_id, ranges) in shard_updates {
                if ranges.is_empty() {
                    shard_map.remove(&series_id);
                } else {
                    shard_map.insert(series_id, ranges);
                }
            }
            normalize_tombstone_map(&mut shard_map)?;
            let prepared = prepare_shard_file(path, shard_index, shard_map)?;
            next_manifest.shards[shard_index] =
                prepared.as_ref().map(|shard| shard.file_name.clone());
            if let Some(prepared) = prepared {
                new_shards.push(prepared);
            }
        }
        (next_manifest, new_shards)
    } else {
        let mut merged = load_tombstones(path)?;
        for (&series_id, ranges) in normalized_updates {
            merged.insert(series_id, ranges.clone());
        }
        normalize_tombstone_map(&mut merged)?;

        let mut shards = vec![TombstoneMap::new(); TOMBSTONE_STORE_SHARD_COUNT];
        for (series_id, ranges) in merged {
            shards[tombstone_shard_index(series_id)].insert(series_id, ranges);
        }

        let mut next_manifest = empty_store_manifest();
        let mut new_shards = Vec::new();
        for (shard_index, shard_map) in shards.into_iter().enumerate() {
            let prepared = prepare_shard_file(path, shard_index, shard_map)?;
            next_manifest.shards[shard_index] =
                prepared.as_ref().map(|shard| shard.file_name.clone());
            if let Some(prepared) = prepared {
                new_shards.push(prepared);
            }
        }
        (next_manifest, new_shards)
    };

    let next_manifest_payload = encode_store_manifest(&next_manifest)?;
    Ok(PreparedTombstoneStoreUpdate {
        path: path.to_path_buf(),
        previous_bytes,
        previous_manifest,
        next_manifest,
        next_manifest_payload,
        new_shards,
    })
}

fn checked_add_payload_bytes(total: &mut u64, bytes: usize, description: &str) -> Result<()> {
    let bytes = u64::try_from(bytes).map_err(|_| {
        TsinkError::Other(format!(
            "{description} exceeds the supported tombstone transaction byte range"
        ))
    })?;
    *total = total.checked_add(bytes).ok_or_else(|| {
        TsinkError::Other(format!(
            "tombstone transaction byte total overflowed while adding {description}"
        ))
    })?;
    Ok(())
}

fn tombstone_transaction_budget_bytes(
    plans: &[PreparedTombstoneStoreUpdate],
    budget: &Arc<crate::LocalDiskBudget>,
    include_rollback_copy: bool,
) -> Result<u64> {
    let mut bytes = 0u64;
    for plan in plans {
        if !budget.governs_entry(&plan.path)? {
            continue;
        }
        budget.validate_managed_file_path(&plan.path)?;
        for shard in &plan.new_shards {
            budget.validate_managed_file_path(&shard.path)?;
            checked_add_payload_bytes(&mut bytes, shard.payload.len(), "new tombstone shard")?;
        }
        checked_add_payload_bytes(
            &mut bytes,
            plan.next_manifest_payload.len(),
            "new tombstone manifest",
        )?;
        if include_rollback_copy {
            if let Some(previous_bytes) = &plan.previous_bytes {
                checked_add_payload_bytes(
                    &mut bytes,
                    previous_bytes.len(),
                    "tombstone manifest rollback copy",
                )?;
            }
        }
    }
    Ok(bytes)
}

fn prepare_tombstone_transaction_directories(
    plans: &[PreparedTombstoneStoreUpdate],
    budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let mut prepared = HashSet::new();
    for plan in plans {
        for shard in &plan.new_shards {
            let Some(parent) = shard.path.parent() else {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "tombstone shard path has no parent: {}",
                    shard.path.display()
                )));
            };
            if !prepared.insert(parent.to_path_buf()) {
                continue;
            }
            match budget {
                Some(budget) if budget.governs_entry(&shard.path)? => {
                    budget.create_dir_all_and_sync_parents(parent)?;
                }
                _ => {
                    crate::engine::fs_utils::create_dir_all_and_sync_parents(parent)?;
                }
            }
        }
    }
    Ok(())
}

fn remove_new_tombstone_shards(
    plans: &[PreparedTombstoneStoreUpdate],
    retain_plan_indices: &HashSet<usize>,
) -> Result<()> {
    let mut errors = Vec::new();
    for (plan_index, plan) in plans.iter().enumerate().rev() {
        if retain_plan_indices.contains(&plan_index) {
            continue;
        }
        for shard in plan.new_shards.iter().rev() {
            if let Err(err) =
                crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&shard.path)
            {
                errors.push(format!("remove {} failed: {err}", shard.path.display()));
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(TsinkError::Other(errors.join("; ")))
    }
}

fn rollback_tombstone_transaction(
    plans: &[PreparedTombstoneStoreUpdate],
    attempted_manifests: usize,
) -> Result<()> {
    let mut errors = Vec::new();
    let mut retain_plan_indices = HashSet::new();
    for plan_index in (0..attempted_manifests).rev() {
        let plan = &plans[plan_index];
        let restore = || match &plan.previous_bytes {
            Some(previous_bytes) => write_file_atomically_and_sync_parent_budgeted(
                &plan.path,
                previous_bytes,
                None,
                crate::DiskCategory::Tombstones,
                crate::DiskReservationKind::Recovery,
            ),
            None => crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&plan.path),
        };
        #[cfg(test)]
        let result = invoke_tombstone_manifest_rollback_hook(&plan.path).and_then(|()| restore());
        #[cfg(not(test))]
        let result = restore();
        if let Err(err) = result {
            // A failed restore leaves the current manifest indeterminate. Candidate shards for
            // this plan may still be live, so retain all of them. Startup orphan cleanup can
            // reclaim any that a subsequently readable manifest proves are unreachable.
            retain_plan_indices.insert(plan_index);
            errors.push(format!(
                "restore tombstone manifest {} failed: {err}",
                plan.path.display()
            ));
        }
    }
    if let Err(err) = remove_new_tombstone_shards(plans, &retain_plan_indices) {
        errors.push(format!("remove staged tombstone shards failed: {err}"));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(TsinkError::Other(errors.join("; ")))
    }
}

fn finish_failed_tombstone_transaction(
    plans: &[PreparedTombstoneStoreUpdate],
    attempted_manifests: usize,
    reservation: Option<crate::disk_budget::DiskReservation>,
    budget: Option<&Arc<crate::LocalDiskBudget>>,
    operation_error: TsinkError,
) -> TombstonePersistenceError {
    let rollback_result = rollback_tombstone_transaction(plans, attempted_manifests);
    drop(reservation);
    let reconciliation_result = budget
        .map(|budget| budget.reconcile_when_idle().map(|_| ()))
        .unwrap_or(Ok(()));

    match (rollback_result, reconciliation_result) {
        (Ok(()), Ok(())) => TombstonePersistenceError::definitively_clean(operation_error),
        (rollback_result, reconciliation_result) => {
            let mut errors = vec![format!("transaction failed: {operation_error}")];
            if let Err(err) = rollback_result {
                errors.push(format!("rollback failed: {err}"));
            }
            if let Err(err) = reconciliation_result {
                errors.push(format!("disk reconciliation failed: {err}"));
            }
            TombstonePersistenceError::indeterminate(TsinkError::Other(format!(
                "tombstone persistence transaction failed: {}",
                errors.join("; ")
            )))
        }
    }
}

fn persist_tombstone_updates_transactionally(
    paths: &[PathBuf],
    normalized_updates: &TombstoneMap,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> TombstonePersistenceResult<()> {
    if normalized_updates.is_empty() || paths.is_empty() {
        return Ok(());
    }

    // Prepare every lane before reserving or mutating the filesystem. This catches corrupt prior
    // manifests and computes the complete transaction peak before any shard becomes durable.
    let plans = paths
        .iter()
        .map(|path| prepare_tombstone_store_update(path, normalized_updates))
        .collect::<Result<Vec<_>>>()?;
    let peak_bytes = local_disk_budget
        .map(|budget| tombstone_transaction_budget_bytes(&plans, budget, true))
        .transpose()?
        .unwrap_or(0);
    let final_charge = local_disk_budget
        .map(|budget| tombstone_transaction_budget_bytes(&plans, budget, false))
        .transpose()?
        .unwrap_or(0);
    let mut reservation = if peak_bytes == 0 {
        None
    } else {
        local_disk_budget
            .map(|budget| {
                budget.reserve(
                    crate::DiskCategory::Tombstones,
                    peak_bytes,
                    crate::DiskReservationKind::Maintenance,
                )
            })
            .transpose()?
    };

    if let Err(err) = prepare_tombstone_transaction_directories(&plans, local_disk_budget) {
        return Err(finish_failed_tombstone_transaction(
            &plans,
            0,
            reservation,
            local_disk_budget,
            err,
        ));
    }

    for plan in &plans {
        for shard in &plan.new_shards {
            if let Err(err) = write_file_atomically_and_sync_parent_budgeted(
                &shard.path,
                &shard.payload,
                None,
                crate::DiskCategory::Tombstones,
                crate::DiskReservationKind::Maintenance,
            ) {
                return Err(finish_failed_tombstone_transaction(
                    &plans,
                    0,
                    reservation,
                    local_disk_budget,
                    err,
                ));
            }
        }
    }

    for (manifest_index, plan) in plans.iter().enumerate() {
        if let Err(err) = write_file_atomically_and_sync_parent_budgeted(
            &plan.path,
            &plan.next_manifest_payload,
            None,
            crate::DiskCategory::Tombstones,
            crate::DiskReservationKind::Maintenance,
        ) {
            return Err(finish_failed_tombstone_transaction(
                &plans,
                manifest_index + 1,
                reservation,
                local_disk_budget,
                err,
            ));
        }
    }

    if let Some(reservation) = reservation.take() {
        if let Err(err) = reservation.commit(final_charge, 0).and_then(|()| {
            local_disk_budget
                .expect("a tombstone transaction reservation requires a budget")
                .reconcile_when_idle()
                .map(|_| ())
        }) {
            return Err(finish_failed_tombstone_transaction(
                &plans,
                plans.len(),
                None,
                local_disk_budget,
                err,
            ));
        }
    }

    // Manifest publication is the commit point. Old shards are unreachable now; cleanup is
    // best-effort because returning an error after this point would falsely imply that the delete
    // did not commit. Reconciliation before and after cleanup keeps any survivor conservatively
    // charged rather than leaking unaccounted quota.
    let mut cleanup_errors = Vec::new();
    for plan in &plans {
        if let Some(previous_manifest) = &plan.previous_manifest {
            if let Err(err) = cleanup_replaced_shards(
                &plan.path,
                &previous_manifest.shards,
                &plan.next_manifest.shards,
                None,
            ) {
                cleanup_errors.push(format!("{}: {err}", plan.path.display()));
            }
        }
    }
    if let Some(budget) = local_disk_budget {
        if let Err(err) = budget.reconcile_when_idle() {
            cleanup_errors.push(format!("disk reconciliation: {err}"));
        }
    }
    if !cleanup_errors.is_empty() {
        tracing::warn!(
            errors = %cleanup_errors.join("; "),
            "committed tombstone transaction left conservatively-accounted cleanup work"
        );
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn persist_tombstone_updates(path: &Path, updates: &TombstoneMap) -> Result<()> {
    persist_tombstone_updates_with_disk_budget(path, updates, None)
}

#[cfg(test)]
pub(crate) fn persist_tombstone_updates_with_disk_budget(
    path: &Path,
    updates: &TombstoneMap,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    let mut normalized_updates = updates.clone();
    normalize_tombstone_map(&mut normalized_updates)?;
    persist_tombstone_updates_transactionally(
        &[path.to_path_buf()],
        &normalized_updates,
        local_disk_budget,
    )
    .map_err(TombstonePersistenceError::into_tsink_error)
}

#[cfg(test)]
pub(crate) fn persist_tombstone_updates_across_paths_with_disk_budget(
    paths: &[PathBuf],
    updates: &TombstoneMap,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<()> {
    persist_tombstone_updates_across_paths_with_disk_budget_outcome(
        paths,
        updates,
        local_disk_budget,
    )
    .map_err(TombstonePersistenceError::into_tsink_error)
}

pub(crate) fn persist_tombstone_updates_across_paths_with_disk_budget_outcome(
    paths: &[PathBuf],
    updates: &TombstoneMap,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> TombstonePersistenceResult<()> {
    let mut normalized_updates = updates.clone();
    normalize_tombstone_map(&mut normalized_updates)
        .map_err(TombstonePersistenceError::definitively_clean)?;
    persist_tombstone_updates_transactionally(paths, &normalized_updates, local_disk_budget)
}

fn is_owned_tombstone_shard_name(name: &str) -> bool {
    let Some(value) = name
        .strip_prefix("shard-")
        .and_then(|value| value.strip_suffix(".bin"))
    else {
        return false;
    };
    let Some((shard, nonce)) = value.split_once('-') else {
        return false;
    };
    shard.len() == 3
        && shard.bytes().all(|byte| byte.is_ascii_digit())
        && shard
            .parse::<usize>()
            .is_ok_and(|shard_index| shard_index < TOMBSTONE_STORE_SHARD_COUNT)
        && nonce.len() == 16
        && nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn cleanup_unreferenced_tombstone_shards(
    path: &Path,
    local_disk_budget: Option<&Arc<crate::LocalDiskBudget>>,
) -> Result<u64> {
    let referenced = match std::fs::read(path) {
        Ok(bytes) => match load_store_manifest_from_bytes(&bytes)? {
            Some(manifest) => manifest
                .shards
                .into_iter()
                .flatten()
                .collect::<HashSet<_>>(),
            None => {
                // An existing non-V2 file implies no shard references only when it is a fully
                // valid legacy V1 snapshot. Unrecognized bytes may be a damaged V2 manifest, so
                // fail closed and retain every candidate shard for explicit recovery.
                load_legacy_tombstones_from_bytes(&bytes)?;
                HashSet::new()
            }
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => HashSet::new(),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    let shards_dir = tombstone_shards_dir(path);
    let entries = match std::fs::read_dir(&shards_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => {
            return Err(TsinkError::IoWithPath {
                path: shards_dir,
                source,
            })
        }
    };

    let mut removed = 0u64;
    for entry in entries {
        let entry = entry.map_err(|source| TsinkError::IoWithPath {
            path: shards_dir.clone(),
            source,
        })?;
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if referenced.contains(&file_name) || !is_owned_tombstone_shard_name(&file_name) {
            continue;
        }
        let entry_path = entry.path();
        let file_type = entry.file_type().map_err(|source| TsinkError::IoWithPath {
            path: entry_path.clone(),
            source,
        })?;
        if !file_type.is_file() {
            return Err(TsinkError::DataCorruption(format!(
                "owned tombstone shard namespace contains a non-regular file or symlink: {}",
                entry_path.display()
            )));
        }
        remove_path_if_exists_and_sync_parent_budgeted(
            &entry_path,
            local_disk_budget,
            crate::DiskCategory::Tombstones,
        )?;
        removed = removed.saturating_add(1);
    }
    Ok(removed)
}

#[cfg(test)]
pub(crate) fn tombstone_transaction_peak_bytes_for_test(
    paths: &[PathBuf],
    updates: &TombstoneMap,
    budget: &Arc<crate::LocalDiskBudget>,
) -> Result<u64> {
    let mut normalized_updates = updates.clone();
    normalize_tombstone_map(&mut normalized_updates)?;
    let plans = paths
        .iter()
        .map(|path| prepare_tombstone_store_update(path, &normalized_updates))
        .collect::<Result<Vec<_>>>()?;
    tombstone_transaction_budget_bytes(&plans, budget, true)
}

#[cfg(test)]
pub(crate) fn tombstone_store_sidecar_path(path: &Path) -> PathBuf {
    tombstone_store_dir(path)
}

#[cfg(test)]
pub(crate) fn referenced_tombstone_shard_files(path: &Path) -> Result<Vec<Option<String>>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source: err,
            })
        }
    };

    Ok(load_store_manifest_from_bytes(&bytes)?
        .map(|manifest| manifest.shards)
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;

    fn regular_file_count_beneath(path: &Path) -> usize {
        let Ok(entries) = std::fs::read_dir(path) else {
            return 0;
        };
        entries
            .map(|entry| entry.unwrap().path())
            .map(|path| {
                if path.is_dir() {
                    regular_file_count_beneath(&path)
                } else {
                    usize::from(path.is_file())
                }
            })
            .sum()
    }

    #[test]
    fn persist_tombstones_returns_error_when_parent_sync_fails() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join(TOMBSTONES_FILE_NAME);
        let mut tombstones = TombstoneMap::new();
        tombstones.insert(7, vec![TombstoneRange { start: 10, end: 20 }]);

        let _guard = crate::engine::fs_utils::fail_directory_sync_once(
            path.parent().unwrap().to_path_buf(),
            "injected parent directory sync failure",
        );
        let err = persist_tombstones(&path, &tombstones)
            .expect_err("parent directory sync failure must be surfaced");
        assert!(
            err.to_string()
                .contains("injected parent directory sync failure"),
            "unexpected error: {err:?}"
        );

        assert!(
            path.exists(),
            "persisted tombstones should remain for retry"
        );
        assert_eq!(load_tombstones(&path).unwrap(), tombstones);
    }

    #[test]
    fn persist_tombstone_updates_rewrites_only_changed_shards() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join(TOMBSTONES_FILE_NAME);
        let mut first = TombstoneMap::new();
        first.insert(1, vec![TombstoneRange { start: 10, end: 20 }]);
        persist_tombstone_updates(&path, &first).unwrap();

        let shard_files_after_first = referenced_tombstone_shard_files(&path).unwrap();
        assert_eq!(
            shard_files_after_first.iter().flatten().count(),
            1,
            "first update should only materialize one shard"
        );

        let mut second = TombstoneMap::new();
        second.insert(2, vec![TombstoneRange { start: 30, end: 40 }]);
        persist_tombstone_updates(&path, &second).unwrap();

        let shard_files_after_second = referenced_tombstone_shard_files(&path).unwrap();
        assert_eq!(
            shard_files_after_second.iter().flatten().count(),
            2,
            "second update should only add the newly touched shard"
        );
        assert_eq!(
            shard_files_after_second[tombstone_shard_index(1)],
            shard_files_after_first[tombstone_shard_index(1)],
            "untouched shard should not be rewritten"
        );
        assert_ne!(
            shard_files_after_second[tombstone_shard_index(2)],
            shard_files_after_first[tombstone_shard_index(2)],
            "changed shard should get a new file"
        );
        assert_eq!(
            load_tombstones(&path).unwrap(),
            HashMap::from([
                (1, vec![TombstoneRange { start: 10, end: 20 }]),
                (2, vec![TombstoneRange { start: 30, end: 40 }]),
            ])
        );
    }

    #[test]
    fn tombstone_transaction_preflights_the_complete_multi_lane_quota() {
        let temp_dir = TempDir::new().unwrap();
        std::fs::write(temp_dir.path().join("existing.bin"), vec![0u8; 128]).unwrap();
        let budget = crate::LocalDiskBudget::open(
            temp_dir.path(),
            crate::LocalDiskLimits {
                max_bytes: Some(129),
                ..crate::LocalDiskLimits::default()
            },
        )
        .unwrap();
        let paths = [
            temp_dir
                .path()
                .join("lane_numeric")
                .join(TOMBSTONES_FILE_NAME),
            temp_dir.path().join("lane_blob").join(TOMBSTONES_FILE_NAME),
        ];
        let updates = TombstoneMap::from([(7, vec![TombstoneRange { start: 10, end: 20 }])]);
        let before = budget.snapshot();

        let err = persist_tombstone_updates_across_paths_with_disk_budget(
            &paths,
            &updates,
            Some(&budget),
        )
        .expect_err("the complete multi-lane transaction must be rejected before mutation");
        assert!(
            matches!(
                &err,
                TsinkError::DiskQuotaExceeded { .. }
                    | TsinkError::InsufficientCompactionHeadroom { .. }
            ),
            "unexpected error: {err:?}"
        );

        for path in &paths {
            assert!(!path.exists());
            assert!(!tombstone_store_dir(path).exists());
            assert!(load_tombstones(path).unwrap().is_empty());
        }
        let after = budget.snapshot();
        assert_eq!(after.accounted_bytes, before.accounted_bytes);
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);

        drop(budget);
        let reopened = crate::LocalDiskBudget::open(
            temp_dir.path(),
            crate::LocalDiskLimits {
                max_bytes: Some(129),
                ..crate::LocalDiskLimits::default()
            },
        )
        .unwrap();
        assert_eq!(reopened.snapshot().accounted_bytes, before.accounted_bytes);
        for path in &paths {
            assert!(load_tombstones(path).unwrap().is_empty());
        }
    }

    #[test]
    fn second_lane_manifest_failure_restores_every_previous_lane() {
        let temp_dir = TempDir::new().unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let numeric_path = temp_dir
            .path()
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let blob_path = temp_dir.path().join("lane_blob").join(TOMBSTONES_FILE_NAME);
        let paths = [numeric_path.clone(), blob_path.clone()];
        let initial = TombstoneMap::from([(1, vec![TombstoneRange { start: 10, end: 20 }])]);
        persist_tombstone_updates_across_paths_with_disk_budget(&paths, &initial, Some(&budget))
            .unwrap();
        let numeric_manifest_before = std::fs::read(&numeric_path).unwrap();
        let blob_manifest_before = std::fs::read(&blob_path).unwrap();
        let numeric_shards_before = referenced_tombstone_shard_files(&numeric_path).unwrap();
        let blob_shards_before = referenced_tombstone_shard_files(&blob_path).unwrap();
        let before = budget.snapshot();

        let blob_lane_path = blob_path.parent().unwrap().to_path_buf();
        let _guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
            {
                let numeric_path = numeric_path.clone();
                let numeric_manifest_before = numeric_manifest_before.clone();
                move |candidate| {
                    candidate == blob_lane_path
                        && std::fs::read(&numeric_path)
                            .is_ok_and(|bytes| bytes != numeric_manifest_before)
                }
            },
            "injected second-lane tombstone manifest failure",
        );
        let updates = TombstoneMap::from([(2, vec![TombstoneRange { start: 30, end: 40 }])]);
        let result = persist_tombstone_updates_across_paths_with_disk_budget(
            &paths,
            &updates,
            Some(&budget),
        );
        assert!(
            result.is_err(),
            "the injected second-lane manifest failure must be surfaced"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("injected second-lane tombstone manifest failure"),
            "unexpected error: {err:?}"
        );

        assert_eq!(
            std::fs::read(&numeric_path).unwrap(),
            numeric_manifest_before
        );
        assert_eq!(std::fs::read(&blob_path).unwrap(), blob_manifest_before);
        assert_eq!(load_tombstones(&numeric_path).unwrap(), initial);
        assert_eq!(load_tombstones(&blob_path).unwrap(), initial);
        assert_eq!(
            referenced_tombstone_shard_files(&numeric_path).unwrap(),
            numeric_shards_before
        );
        assert_eq!(
            referenced_tombstone_shard_files(&blob_path).unwrap(),
            blob_shards_before
        );
        let after = budget.snapshot();
        assert_eq!(after.accounted_bytes, before.accounted_bytes);
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);
        assert_eq!(
            regular_file_count_beneath(&tombstone_store_dir(&numeric_path)),
            1
        );
        assert_eq!(
            regular_file_count_beneath(&tombstone_store_dir(&blob_path)),
            1
        );

        drop(_guard);
        drop(budget);
        let reopened =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        assert_eq!(reopened.snapshot().accounted_bytes, before.accounted_bytes);
        assert_eq!(load_tombstones(&numeric_path).unwrap(), initial);
        assert_eq!(load_tombstones(&blob_path).unwrap(), initial);
    }

    #[test]
    fn partial_multi_shard_failure_removes_every_staged_shard() {
        let temp_dir = TempDir::new().unwrap();
        let budget =
            crate::LocalDiskBudget::open(temp_dir.path(), crate::LocalDiskLimits::default())
                .unwrap();
        let path = temp_dir
            .path()
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let paths = [path.clone()];
        let initial = TombstoneMap::from([(1, vec![TombstoneRange { start: 10, end: 20 }])]);
        persist_tombstone_updates_across_paths_with_disk_budget(&paths, &initial, Some(&budget))
            .unwrap();
        let manifest_before = std::fs::read(&path).unwrap();
        let shards_before = referenced_tombstone_shard_files(&path).unwrap();
        let before = budget.snapshot();

        let shard_syncs = Arc::new(AtomicUsize::new(0));
        let shards_dir = tombstone_shards_dir(&path);
        let _guard = crate::engine::fs_utils::fail_directory_sync_matching_once(
            {
                let shard_syncs = Arc::clone(&shard_syncs);
                move |candidate| {
                    candidate == shards_dir && shard_syncs.fetch_add(1, Ordering::SeqCst) == 1
                }
            },
            "injected second tombstone shard failure",
        );
        let updates = TombstoneMap::from([
            (2, vec![TombstoneRange { start: 30, end: 40 }]),
            (3, vec![TombstoneRange { start: 50, end: 60 }]),
        ]);
        let err = persist_tombstone_updates_across_paths_with_disk_budget(
            &paths,
            &updates,
            Some(&budget),
        )
        .expect_err("the injected second-shard failure must be surfaced");
        assert!(
            err.to_string()
                .contains("injected second tombstone shard failure"),
            "unexpected error: {err:?}"
        );

        assert_eq!(std::fs::read(&path).unwrap(), manifest_before);
        assert_eq!(load_tombstones(&path).unwrap(), initial);
        assert_eq!(
            referenced_tombstone_shard_files(&path).unwrap(),
            shards_before
        );
        assert_eq!(regular_file_count_beneath(&tombstone_store_dir(&path)), 1);
        let after = budget.snapshot();
        assert_eq!(after.accounted_bytes, before.accounted_bytes);
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);
    }

    #[test]
    fn unmanaged_tombstone_ancestry_sync_failure_is_surfaced_without_local_quota_charge() {
        let temp_dir = TempDir::new().unwrap();
        let local_root = temp_dir.path().join("local");
        let external_root = temp_dir.path().join("object-store");
        let budget = crate::LocalDiskBudget::open(
            &local_root,
            crate::LocalDiskLimits {
                max_bytes: Some(1),
                ..crate::LocalDiskLimits::default()
            },
        )
        .unwrap();
        let path = external_root
            .join("hot")
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let updates = TombstoneMap::from([(7, vec![TombstoneRange { start: 10, end: 20 }])]);
        let before = budget.snapshot();

        let guard = crate::engine::fs_utils::fail_directory_sync_once(
            external_root.clone(),
            "injected external tombstone ancestry sync failure",
        );
        let err = persist_tombstone_updates_across_paths_with_disk_budget(
            std::slice::from_ref(&path),
            &updates,
            Some(&budget),
        )
        .expect_err("external tombstone ancestry must be synchronized before shard publication");
        assert!(
            err.to_string()
                .contains("injected external tombstone ancestry sync failure"),
            "unexpected error: {err:?}"
        );
        assert!(!path.exists());
        assert_eq!(regular_file_count_beneath(&tombstone_store_dir(&path)), 0);
        assert_eq!(budget.snapshot().accounted_bytes, before.accounted_bytes);

        drop(guard);
        persist_tombstone_updates_across_paths_with_disk_budget(
            std::slice::from_ref(&path),
            &updates,
            Some(&budget),
        )
        .unwrap();
        assert_eq!(load_tombstones(&path).unwrap(), updates);
        let after = budget.snapshot();
        assert_eq!(after.accounted_bytes, before.accounted_bytes);
        assert_eq!(after.reserved_bytes, 0);
        assert_eq!(after.active_reservations, 0);
    }

    #[test]
    fn corrupt_v2_manifest_magic_preserves_all_candidate_shards() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let expected = TombstoneMap::from([(7, vec![TombstoneRange { start: 10, end: 20 }])]);
        persist_tombstone_updates(&path, &expected).unwrap();

        let manifest_before = std::fs::read(&path).unwrap();
        let shard_file_name = referenced_tombstone_shard_files(&path)
            .unwrap()
            .into_iter()
            .flatten()
            .next()
            .unwrap();
        let shard_path = tombstone_shards_dir(&path).join(shard_file_name);
        let shard_before = std::fs::read(&shard_path).unwrap();
        let mut corrupt_manifest = manifest_before.clone();
        corrupt_manifest[0] ^= 0x01;
        std::fs::write(&path, corrupt_manifest).unwrap();

        cleanup_unreferenced_tombstone_shards(&path, None)
            .expect_err("unrecognized manifest bytes must fail closed before orphan cleanup");
        assert_eq!(std::fs::read(&shard_path).unwrap(), shard_before);
        load_tombstones(&path).expect_err("the corrupt manifest must remain visible to recovery");

        std::fs::write(&path, manifest_before).unwrap();
        assert_eq!(load_tombstones(&path).unwrap(), expected);
    }

    #[test]
    fn manifest_rejects_traversal_and_absolute_shard_names_before_read_or_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join("lane_numeric");
        std::fs::create_dir_all(&lane_path).unwrap();
        let path = lane_path.join(TOMBSTONES_FILE_NAME);
        let outside = temp_dir.path().join("outside-sentinel.bin");
        std::fs::write(&outside, b"outside-sentinel").unwrap();
        let malicious_names = [
            "../../outside-sentinel.bin".to_string(),
            outside.to_string_lossy().into_owned(),
        ];

        for malicious_name in malicious_names {
            let mut manifest = empty_store_manifest();
            manifest.shards[7] = Some(malicious_name);
            std::fs::write(&path, encode_store_manifest(&manifest).unwrap()).unwrap();

            let load_err = load_tombstones(&path)
                .expect_err("an untrusted manifest shard path must not be read");
            assert!(matches!(load_err, TsinkError::DataCorruption(_)));
            let update_err = persist_tombstone_updates(
                &path,
                &TombstoneMap::from([(7, vec![TombstoneRange { start: 10, end: 20 }])]),
            )
            .expect_err("an untrusted manifest shard path must not reach obsolete cleanup");
            assert!(matches!(update_err, TsinkError::DataCorruption(_)));
            assert_eq!(std::fs::read(&outside).unwrap(), b"outside-sentinel");
            assert!(!tombstone_store_dir(&path).exists());
        }
    }

    #[cfg(unix)]
    #[test]
    fn referenced_and_orphan_tombstone_shard_symlinks_are_rejected_without_following() {
        use std::os::unix::fs::symlink;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join("lane_numeric")
            .join(TOMBSTONES_FILE_NAME);
        let shards_dir = tombstone_shards_dir(&path);
        std::fs::create_dir_all(&shards_dir).unwrap();
        let external = temp_dir.path().join("external-shard.bin");
        let external_payload = encode_shard(vec![TombstoneSeriesEntryV1 {
            series_id: 7,
            ranges: vec![TombstoneRange { start: 10, end: 20 }],
        }])
        .unwrap();
        std::fs::write(&external, &external_payload).unwrap();
        let file_name = "shard-007-0000000000000001.bin";
        let shard_path = shards_dir.join(file_name);
        symlink(&external, &shard_path).unwrap();
        let mut manifest = empty_store_manifest();
        manifest.shards[7] = Some(file_name.to_string());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, encode_store_manifest(&manifest).unwrap()).unwrap();

        let err = load_tombstones(&path)
            .expect_err("a referenced shard symlink must not be followed outside the store");
        assert!(matches!(err, TsinkError::DataCorruption(_)));
        assert_eq!(std::fs::read(&external).unwrap(), external_payload);
        assert!(std::fs::symlink_metadata(&shard_path)
            .unwrap()
            .file_type()
            .is_symlink());

        std::fs::remove_file(&path).unwrap();
        let cleanup_err = cleanup_unreferenced_tombstone_shards(&path, None)
            .expect_err("an orphan shard symlink must be rejected rather than followed or removed");
        assert!(matches!(cleanup_err, TsinkError::DataCorruption(_)));
        assert_eq!(std::fs::read(&external).unwrap(), external_payload);
        assert!(std::fs::symlink_metadata(&shard_path)
            .unwrap()
            .file_type()
            .is_symlink());
    }
}
