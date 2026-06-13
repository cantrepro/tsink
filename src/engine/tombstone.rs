use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::engine::fs_utils::{
    path_exists_no_follow, remove_path_if_exists, sync_dir, sync_parent_dir,
    write_file_atomically_and_sync_parent,
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
    Ok(())
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

fn write_shard_file(path: &Path, shard_index: usize, map: TombstoneMap) -> Result<Option<String>> {
    if map.is_empty() {
        return Ok(None);
    }

    let file_name = format!(
        "shard-{shard_index:03}-{:016x}.bin",
        TOMBSTONE_SHARD_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let shard_path = tombstone_shards_dir(path).join(&file_name);
    let payload = encode_shard(shard_entries_from_map(map))?;
    write_file_atomically_and_sync_parent(&shard_path, &payload)?;
    Ok(Some(file_name))
}

fn cleanup_replaced_shards(
    path: &Path,
    previous: &[Option<String>],
    next: &[Option<String>],
) -> Result<()> {
    let shards_dir = tombstone_shards_dir(path);
    let keep = next.iter().flatten().cloned().collect::<HashSet<_>>();
    let mut removed_any = false;
    for file_name in previous.iter().flatten() {
        if keep.contains(file_name) {
            continue;
        }
        let shard_path = shards_dir.join(file_name);
        let existed = path_exists_no_follow(&shard_path)?;
        remove_path_if_exists(&shard_path)?;
        removed_any |= existed;
    }
    if removed_any {
        sync_dir(&shards_dir)?;
    }
    Ok(())
}

fn remove_tombstone_store(path: &Path) -> Result<()> {
    let store_dir = tombstone_store_dir(path);
    let manifest_exists = path_exists_no_follow(path)?;
    let store_exists = path_exists_no_follow(&store_dir)?;
    remove_path_if_exists(path)?;
    remove_path_if_exists(&store_dir)?;
    if manifest_exists || store_exists {
        sync_parent_dir(path)?;
    }
    Ok(())
}

fn write_full_sharded_store(
    path: &Path,
    tombstones: &TombstoneMap,
    previous_manifest: Option<&TombstoneStoreManifestV2>,
) -> Result<()> {
    let mut shards = vec![TombstoneMap::new(); TOMBSTONE_STORE_SHARD_COUNT];
    for (&series_id, ranges) in tombstones {
        shards[tombstone_shard_index(series_id)].insert(series_id, ranges.clone());
    }

    let mut manifest = empty_store_manifest();
    for (shard_index, shard_map) in shards.into_iter().enumerate() {
        manifest.shards[shard_index] = write_shard_file(path, shard_index, shard_map)?;
    }

    let payload = encode_store_manifest(&manifest)?;
    write_file_atomically_and_sync_parent(path, &payload)?;
    if let Some(previous_manifest) = previous_manifest {
        cleanup_replaced_shards(path, &previous_manifest.shards, &manifest.shards)?;
    }
    Ok(())
}

fn load_store_manifest_from_bytes(bytes: &[u8]) -> Result<Option<TombstoneStoreManifestV2>> {
    match decode_store_manifest(bytes)? {
        Some(manifest) => Ok(Some(manifest)),
        None => Ok(None),
    }
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

    let snapshot: TombstoneFileV1 = serde_json::from_slice(&bytes)?;
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

pub(crate) fn persist_tombstones(path: &Path, tombstones: &TombstoneMap) -> Result<()> {
    let mut normalized = tombstones.clone();
    normalize_tombstone_map(&mut normalized)?;
    if normalized.is_empty() {
        return remove_tombstone_store(path);
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
    write_full_sharded_store(path, &normalized, previous_manifest.as_ref())
}

pub(crate) fn persist_tombstone_updates(path: &Path, updates: &TombstoneMap) -> Result<()> {
    let mut normalized_updates = updates.clone();
    normalize_tombstone_map(&mut normalized_updates)?;
    if normalized_updates.is_empty() {
        return Ok(());
    }

    let current_bytes = match std::fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            return Err(TsinkError::IoWithPath {
                path: path.to_path_buf(),
                source: err,
            })
        }
    };

    let previous_manifest = current_bytes
        .as_deref()
        .map(load_store_manifest_from_bytes)
        .transpose()?
        .flatten();

    if previous_manifest.is_none() {
        let mut merged = load_tombstones(path)?;
        for (series_id, ranges) in normalized_updates {
            merged.insert(series_id, ranges);
        }
        normalize_tombstone_map(&mut merged)?;
        return write_full_sharded_store(path, &merged, None);
    }

    let previous_manifest = previous_manifest.unwrap_or_else(empty_store_manifest);
    let mut next_manifest = previous_manifest.clone();
    let mut updates_by_shard = vec![TombstoneMap::new(); TOMBSTONE_STORE_SHARD_COUNT];
    for (series_id, ranges) in normalized_updates {
        updates_by_shard[tombstone_shard_index(series_id)].insert(series_id, ranges);
    }

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
        next_manifest.shards[shard_index] = write_shard_file(path, shard_index, shard_map)?;
    }

    let payload = encode_store_manifest(&next_manifest)?;
    write_file_atomically_and_sync_parent(path, &payload)?;
    cleanup_replaced_shards(path, &previous_manifest.shards, &next_manifest.shards)
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
    use tempfile::TempDir;

    use super::*;

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
}
